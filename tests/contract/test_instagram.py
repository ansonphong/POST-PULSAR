"""Offline contracts for the official Instagram Graph API adapter."""

from __future__ import annotations

import hashlib
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path
from urllib.parse import parse_qs

import httpx
import pytest
import respx

from post_pulsar.config import SecretValue
from post_pulsar.media import (
    MediaMetadata,
    MediaSafetyError,
    MediaWarning,
    PreparedMedia,
    PreparedMediaItem,
    PublicURLVerification,
    StagedMedia,
)
from post_pulsar.platforms.base import (
    AdapterContractError,
    ArtifactCheckpoint,
    PreparedPublication,
    PublicationRequest,
    PublicationSnapshot,
)
from post_pulsar.platforms.http import HTTPPolicy, PlatformHTTPClient
from post_pulsar.platforms.instagram import (
    GRAPH_API_VERSION,
    GRAPH_BASE_URL,
    REQUIRED_SCOPES,
    InstagramAdapter,
    InstagramAdapterError,
    PublicMediaCleaner,
)
from post_pulsar.state import (
    ArtifactRecord,
    BundleFileSnapshot,
    DeliveryRecord,
    ProfileTargetSnapshot,
    StateRepository,
    TargetSnapshot,
)

GRAPH = GRAPH_BASE_URL
NOW = datetime(2026, 9, 5, 12, tzinfo=UTC)


def _snapshot(
    profile_id: str = "ansonphong",
    user_id: str = "17841400000000000",
    *,
    media_directory: str = "/tmp/post-pulsar-instagram-contract",
    processing_timeout_seconds: float = 300.0,
) -> PublicationSnapshot:
    return PublicationSnapshot(
        profile_id=profile_id,
        bundle_key=11,
        bundle_id="post",
        bundle_fingerprint="b" * 64,
        source_bucket="QUEUE",
        target=TargetSnapshot(
            platform="instagram",
            expected_remote_user_id=user_id,
            expected_username=profile_id,
            token_env_var=(f"POST_PULSAR_INSTAGRAM_{profile_id.upper()}_ACCESS_TOKEN"),
            api_version=GRAPH_API_VERSION,
            adapter_version=1,
            request_settings={
                "timeout": 30,
                "media_base_url": "https://media.example.test/post-pulsar/",
                "media_directory": media_directory,
                "processing_timeout_seconds": processing_timeout_seconds,
            },
        ),
    )


def _staged(snapshot: PublicationSnapshot, ordinal: int, kind: str) -> StagedMedia:
    suffix = "mp4" if kind == "video" else "jpg"
    mime = "video/mp4" if kind == "video" else "image/jpeg"
    digest = hashlib.sha256(f"media-{ordinal}".encode()).hexdigest()
    filename = (
        f"{snapshot.profile_id}-{snapshot.source_bucket}-"
        f"{snapshot.bundle_fingerprint}-{ordinal}.{suffix}"
    )
    return StagedMedia(
        profile_id=snapshot.profile_id,
        source_bucket=snapshot.source_bucket,
        bundle_id=snapshot.bundle_id,
        bundle_fingerprint=snapshot.bundle_fingerprint,
        source_name=f"post-{ordinal}.{suffix}",
        staging_kind="instagram_public",
        relative_path=Path(filename),
        sha256=digest,
        mime_type=mime,
        size_bytes=100 + ordinal,
        cleanup_policy="known_terminal_hash_match",
        public_url=f"https://media.example.test/post-pulsar/{filename}",
        source_sha256="a" * 64,
    )


def _media(
    snapshot: PublicationSnapshot,
    *,
    kinds: tuple[str, ...] = ("image",),
    warnings: tuple[MediaWarning, ...] = (),
) -> PreparedMedia:
    items: list[PreparedMediaItem] = []
    for ordinal, kind in enumerate(kinds):
        public = _staged(snapshot, ordinal, kind)
        private = StagedMedia(
            profile_id=snapshot.profile_id,
            source_bucket=snapshot.source_bucket,
            bundle_id=snapshot.bundle_id,
            bundle_fingerprint=snapshot.bundle_fingerprint,
            source_name=public.source_name,
            staging_kind="private",
            relative_path=Path(
                snapshot.profile_id,
                snapshot.source_bucket,
                snapshot.bundle_fingerprint,
                public.source_name,
            ),
            sha256=public.source_sha256 or "a" * 64,
            mime_type=public.mime_type,
            size_bytes=public.size_bytes,
            cleanup_policy="known_terminal_hash_match",
        )
        metadata = MediaMetadata(
            kind=kind,  # type: ignore[arg-type]
            format_name="MP4" if kind == "video" else "JPEG",
            mime_type=public.mime_type,
            width=1080,
            height=1080,
        )
        items.append(PreparedMediaItem(public.source_name, metadata, private, public))
    return PreparedMedia(
        profile_id=snapshot.profile_id,
        source_bucket=snapshot.source_bucket,
        bundle_id=snapshot.bundle_id,
        bundle_fingerprint=snapshot.bundle_fingerprint,
        targets=("instagram",),
        items=tuple(items),
        warnings=warnings,
    )


def _request(
    *,
    text: str = "hello",
    kinds: tuple[str, ...] = ("image",),
    snapshot: PublicationSnapshot | None = None,
    warnings: tuple[MediaWarning, ...] = (),
) -> PublicationRequest:
    bound = snapshot or _snapshot()
    return PublicationRequest(
        bound, _media(bound, kinds=kinds, warnings=warnings), text, None
    )


def _with_public_staging(
    publication: PublicationRequest, staged: StagedMedia
) -> PublicationRequest:
    item = replace(publication.media.items[0], public=staged)
    media = replace(publication.media, items=(item,))
    return replace(publication, media=media)


def _proof(staged: StagedMedia) -> PublicURLVerification:
    assert staged.public_url is not None
    return PublicURLVerification(
        staged.public_url, 0, staged.sha256, staged.mime_type, staged.size_bytes
    )


def _delivery(phase: str = "processing") -> DeliveryRecord:
    return DeliveryRecord(
        bundle_key=11,
        platform="instagram",
        status="in_flight",
        phase=phase,  # type: ignore[arg-type]
        attempt_count=1,
        consecutive_failures=0,
        safe_to_retry=True,
        next_attempt_at=None,
        remote_id=None,
        error_code=None,
        error_message=None,
        revision=2,
    )


class _Writer:
    def __init__(self) -> None:
        self.phases: list[str] = []
        self.artifacts: list[ArtifactRecord] = []
        self.transitions: list[tuple[str, int, str]] = []
        self.warnings: list[tuple[str, dict[str, object]]] = []

    def advance_phase(self, phase: str) -> DeliveryRecord:
        self.phases.append(phase)
        return _delivery(phase)

    def checkpoint_artifact(self, checkpoint: ArtifactCheckpoint) -> ArtifactRecord:
        artifact = ArtifactRecord(
            bundle_key=11,
            platform="instagram",
            attempt_count=1,
            kind=checkpoint.kind,
            ordinal=checkpoint.ordinal,
            external_id=checkpoint.external_id,
            relative_path=checkpoint.relative_path,
            sha256=checkpoint.sha256,
            expires_at=checkpoint.expires_at,
            processing_metadata=checkpoint.processing_metadata,
        )
        self.artifacts.append(artifact)
        return artifact

    def transition_artifact_processing(
        self, current: ArtifactRecord, checkpoint: ArtifactCheckpoint
    ) -> ArtifactRecord:
        assert checkpoint.external_id == current.external_id
        updated = replace(current, processing_metadata=checkpoint.processing_metadata)
        self.transitions.append(
            (
                current.kind,
                current.ordinal,
                str(checkpoint.processing_metadata["state"]),
            )
        )
        for index, artifact in enumerate(self.artifacts):
            if artifact == current:
                self.artifacts[index] = updated
                break
        else:
            self.artifacts.append(updated)
        return updated

    def record_warning(self, code: str, details: dict[str, object]) -> None:
        self.warnings.append((code, details))


class _RepositoryWriter:
    def __init__(
        self,
        repository: StateRepository,
        bundle_key: int,
        claim_token: str,
        attempt_count: int,
    ) -> None:
        self.repository = repository
        self.bundle_key = bundle_key
        self.claim_token = claim_token
        self.attempt_count = attempt_count

    def advance_phase(self, phase: str) -> DeliveryRecord:
        return self.repository.advance_delivery_phase(
            self.bundle_key,
            "instagram",
            phase,  # type: ignore[arg-type]
            claim_token=self.claim_token,
            attempt_count=self.attempt_count,
        )

    def checkpoint_artifact(self, checkpoint: ArtifactCheckpoint) -> ArtifactRecord:
        return self.repository.checkpoint_artifact(
            self.bundle_key,
            "instagram",
            kind=checkpoint.kind,
            ordinal=checkpoint.ordinal,
            external_id=checkpoint.external_id,
            relative_path=checkpoint.relative_path,
            sha256=checkpoint.sha256,
            expires_at=checkpoint.expires_at,
            processing_metadata=checkpoint.processing_metadata,
            claim_token=self.claim_token,
            attempt_count=self.attempt_count,
        )

    def transition_artifact_processing(
        self, current: ArtifactRecord, checkpoint: ArtifactCheckpoint
    ) -> ArtifactRecord:
        assert checkpoint.external_id is not None
        return self.repository.transition_artifact_processing(
            self.bundle_key,
            "instagram",
            kind=checkpoint.kind,
            ordinal=checkpoint.ordinal,
            external_id=checkpoint.external_id,
            expected_processing_metadata=current.processing_metadata,
            processing_metadata=checkpoint.processing_metadata,
            claim_token=self.claim_token,
            attempt_count=self.attempt_count,
        )


def _client(
    snapshot: PublicationSnapshot, token: str = "instagram-token"
) -> PlatformHTTPClient:
    return PlatformHTTPClient(snapshot, SecretValue(token), base_url=GRAPH)


def _identity(router: respx.MockRouter, snapshot: PublicationSnapshot) -> None:
    router.get(f"{GRAPH}/{GRAPH_API_VERSION}/me").mock(
        return_value=httpx.Response(
            200,
            json={
                "user_id": snapshot.target.expected_remote_user_id,
                "username": snapshot.target.expected_username,
            },
        )
    )


def _quota(
    router: respx.MockRouter,
    snapshot: PublicationSnapshot,
    *,
    usage: int = 4,
    total: int = 100,
    duration: int = 86_400,
) -> respx.Route:
    return router.get(
        f"{GRAPH}/{GRAPH_API_VERSION}/"
        f"{snapshot.target.expected_remote_user_id}/content_publishing_limit"
    ).mock(
        return_value=httpx.Response(
            200,
            json={
                "data": [
                    {
                        "quota_usage": usage,
                        "config": {
                            "quota_total": total,
                            "quota_duration": duration,
                        },
                    }
                ]
            },
        )
    )


def _adapter(
    snapshot: PublicationSnapshot,
    client: PlatformHTTPClient,
    *,
    public_cleaner: PublicMediaCleaner | None = None,
    **kwargs: object,
) -> InstagramAdapter:
    return InstagramAdapter(
        snapshot,
        client,
        public_verifier=_proof,
        public_cleaner=public_cleaner or _ignore_cleanup,
        clock=lambda: NOW,
        sleeper=lambda _seconds: None,
        **kwargs,  # type: ignore[arg-type]
    )


def _ignore_cleanup(_staged: StagedMedia, _outcome: str) -> bool:
    return True


@pytest.mark.parametrize(
    ("caption", "error_code"),
    [
        ("a" * 2_201, "instagram_caption_too_long"),
        (
            " ".join(f"#tag{index}" for index in range(31)),
            "instagram_too_many_hashtags",
        ),
        (
            " ".join(f"@user{index}" for index in range(21)),
            "instagram_too_many_mentions",
        ),
    ],
)
def test_caption_limits_reject_before_mutation_or_checkpoint(
    caption: str, error_code: str
) -> None:
    snapshot = _snapshot()
    writer = _Writer()
    with respx.mock(assert_all_called=True) as router:
        _identity(router, snapshot)
        client = _client(snapshot)
        adapter = _adapter(snapshot, client)
        issues = adapter.preflight(_request(text=caption, snapshot=snapshot))
        with pytest.raises(InstagramAdapterError) as caught:
            adapter.prepare(
                _request(text=caption, snapshot=snapshot),
                prior=None,
                checkpoints=writer,
            )
        adapter.close()

    assert [issue.code for issue in issues if issue.severity == "error"] == [error_code]
    assert caught.value.code == error_code
    assert writer.artifacts == []
    assert writer.phases == []
    assert all(call.request.method == "GET" for call in router.calls)


def test_caption_exact_boundaries_and_nfc_payload_are_allowed() -> None:
    snapshot = _snapshot()
    # Keep each independent boundary exact; the combined caption would exceed 2,200.
    cases = (
        "e\u0301" + "a" * 2_198,
        " ".join(f"#h{index}" for index in range(30)),
        " ".join(f"@u{index}" for index in range(20)),
    )
    for index, value in enumerate(cases):
        writer = _Writer()
        with respx.mock(assert_all_called=True) as router:
            _identity(router, snapshot)
            _quota(router, snapshot)
            media = router.post(
                f"{GRAPH}/{GRAPH_API_VERSION}/"
                f"{snapshot.target.expected_remote_user_id}/media"
            ).mock(return_value=httpx.Response(200, json={"id": f"1800{index}"}))
            router.get(f"{GRAPH}/{GRAPH_API_VERSION}/1800{index}").mock(
                return_value=httpx.Response(200, json={"status_code": "FINISHED"})
            )
            adapter = _adapter(snapshot, _client(snapshot))
            prepared = adapter.prepare(
                _request(text=value, snapshot=snapshot),
                prior=None,
                checkpoints=writer,
            )
            adapter.close()
        assert prepared.attempt_count == 1
        form = parse_qs(media.calls[0].request.content.decode())
        assert len(form["caption"][0]) <= 2_200
        if index == 0:
            assert form["caption"][0].startswith("é")


def test_identity_and_protected_quota_shapes_are_exact_and_header_only() -> None:
    snapshot = _snapshot()
    token = "bearer-never-query"
    with respx.mock(assert_all_called=True) as router:
        _identity(router, snapshot)
        quota = _quota(router, snapshot, usage=37, total=40, duration=3_600)
        adapter = _adapter(snapshot, _client(snapshot, token))
        assert adapter.preflight(_request(snapshot=snapshot)) == ()
        adapter.close()

    assert REQUIRED_SCOPES == (
        "instagram_business_basic",
        "instagram_business_content_publish",
    )
    assert dict(quota.calls[0].request.url.params) == {"fields": "quota_usage,config"}
    for call in router.calls:
        assert call.request.headers["Authorization"] == f"Bearer {token}"
        assert "access_token" not in call.request.url.params
        assert "account_type" not in call.request.url.params.get("fields", "")


def test_quota_exhaustion_uses_returned_config_without_container_mutation() -> None:
    snapshot = _snapshot()
    with respx.mock(assert_all_called=True) as router:
        _identity(router, snapshot)
        _quota(router, snapshot, usage=7, total=7, duration=123)
        adapter = _adapter(snapshot, _client(snapshot))
        issues = adapter.preflight(_request(snapshot=snapshot))
        adapter.close()

    assert [issue.code for issue in issues] == ["instagram_quota_exhausted"]
    assert all(call.request.method == "GET" for call in router.calls)


@pytest.mark.parametrize(
    "payload",
    [
        {},
        {"data": []},
        {"data": [{"quota_usage": True, "config": {}}]},
        {
            "data": [
                {
                    "quota_usage": 1,
                    "config": {"quota_total": 0, "quota_duration": 86_400},
                }
            ]
        },
    ],
)
def test_malformed_protected_quota_fails_closed(payload: object) -> None:
    snapshot = _snapshot()
    with respx.mock(assert_all_called=True) as router:
        _identity(router, snapshot)
        router.get(
            f"{GRAPH}/{GRAPH_API_VERSION}/"
            f"{snapshot.target.expected_remote_user_id}/content_publishing_limit"
        ).mock(return_value=httpx.Response(200, json=payload))
        adapter = _adapter(snapshot, _client(snapshot))
        with pytest.raises(InstagramAdapterError) as caught:
            adapter.preflight(_request(snapshot=snapshot))
        adapter.close()
    assert caught.value.code in {
        "instagram_quota_invalid",
        "instagram_response_invalid",
    }


@pytest.mark.parametrize(
    "kinds",
    [("image",) * 11, ("image", "video"), ("video", "video")],
)
def test_invalid_media_shape_rejects_before_quota_container_or_checkpoint(
    kinds: tuple[str, ...],
) -> None:
    snapshot = _snapshot()
    writer = _Writer()
    with respx.mock(assert_all_called=True) as router:
        _identity(router, snapshot)
        adapter = _adapter(snapshot, _client(snapshot))
        with pytest.raises(InstagramAdapterError) as caught:
            adapter.prepare(
                _request(snapshot=snapshot, kinds=kinds),
                prior=None,
                checkpoints=writer,
            )
        adapter.close()
    assert caught.value.code == "instagram_media_shape_invalid"
    assert writer.phases == [] and writer.artifacts == []
    assert all(call.request.method == "GET" for call in router.calls)


def test_ten_image_carousel_boundary_passes_read_only_preflight() -> None:
    snapshot = _snapshot()
    with respx.mock(assert_all_called=True) as router:
        _identity(router, snapshot)
        _quota(router, snapshot)
        adapter = _adapter(snapshot, _client(snapshot))
        issues = adapter.preflight(_request(snapshot=snapshot, kinds=("image",) * 10))
        adapter.close()
    assert issues == ()
    assert all(call.request.method == "GET" for call in router.calls)


def test_wrong_account_blocks_before_any_container_mutation() -> None:
    snapshot = _snapshot()
    with respx.mock(assert_all_called=True) as router:
        router.get(f"{GRAPH}/{GRAPH_API_VERSION}/me").mock(
            return_value=httpx.Response(
                200, json={"user_id": "999", "username": "attacker"}
            )
        )
        adapter = _adapter(snapshot, _client(snapshot))
        with pytest.raises(InstagramAdapterError) as caught:
            adapter.prepare(
                _request(snapshot=snapshot), prior=None, checkpoints=_Writer()
            )
        adapter.close()

    assert caught.value.code == "instagram_identity_mismatch"
    assert all(call.request.method == "GET" for call in router.calls)


def test_single_image_checkpoints_staging_container_expiry_and_publishes_once() -> None:
    snapshot = _snapshot()
    writer = _Writer()
    cleanup: list[tuple[str, str]] = []

    def cleaner(staged: StagedMedia, outcome: str) -> bool:
        cleanup.append((staged.source_name, outcome))
        return True

    with respx.mock(assert_all_called=True) as router:
        _identity(router, snapshot)
        _quota(router, snapshot)
        create = router.post(
            f"{GRAPH}/{GRAPH_API_VERSION}/"
            f"{snapshot.target.expected_remote_user_id}/media"
        ).mock(return_value=httpx.Response(200, json={"id": "18001"}))
        status = router.get(f"{GRAPH}/{GRAPH_API_VERSION}/18001").mock(
            return_value=httpx.Response(200, json={"status_code": "FINISHED"})
        )
        publish = router.post(
            f"{GRAPH}/{GRAPH_API_VERSION}/"
            f"{snapshot.target.expected_remote_user_id}/media_publish"
        ).mock(return_value=httpx.Response(200, json={"id": "19001"}))
        adapter = _adapter(snapshot, _client(snapshot), public_cleaner=cleaner)
        prepared = adapter.prepare(
            _request(snapshot=snapshot), prior=None, checkpoints=writer
        )
        result = adapter.commit(prepared, delivery=_delivery("final_dispatch_started"))
        with pytest.raises(AdapterContractError, match="already attempted"):
            adapter.commit(prepared, delivery=_delivery("final_dispatch_started"))
        adapter.close()

    assert result.outcome == "published" and result.remote_id == "19001"
    assert parse_qs(create.calls[0].request.content.decode()) == {
        "image_url": [
            f"https://media.example.test/post-pulsar/ansonphong-QUEUE-{'b' * 64}-0.jpg"
        ],
        "caption": ["hello"],
    }
    assert dict(status.calls[0].request.url.params) == {"fields": "status_code,status"}
    assert parse_qs(publish.calls[0].request.content.decode()) == {
        "creation_id": ["18001"]
    }
    assert publish.call_count == 1
    assert writer.phases == ["processing", "ready"]
    assert [(item.kind, item.ordinal) for item in writer.artifacts] == [
        ("staged_public", 0),
        ("instagram_parent_container", 0),
    ]
    assert writer.artifacts[-1].expires_at == NOW + timedelta(hours=23, minutes=45)
    assert writer.transitions == [("instagram_parent_container", 0, "FINISHED")]
    assert cleanup == [("post-0.jpg", "published")]


def test_carousel_creates_and_checkpoints_each_child_before_parent() -> None:
    snapshot = _snapshot()
    writer = _Writer()
    ids = iter(("18101", "18102", "18103"))
    with respx.mock(assert_all_called=True) as router:
        _identity(router, snapshot)
        _quota(router, snapshot)

        def create_response(_request: httpx.Request) -> httpx.Response:
            return httpx.Response(200, json={"id": next(ids)})

        create = router.post(
            f"{GRAPH}/{GRAPH_API_VERSION}/"
            f"{snapshot.target.expected_remote_user_id}/media"
        ).mock(side_effect=create_response)
        for container_id in ("18101", "18102", "18103"):
            router.get(f"{GRAPH}/{GRAPH_API_VERSION}/{container_id}").mock(
                return_value=httpx.Response(200, json={"status_code": "FINISHED"})
            )
        adapter = _adapter(snapshot, _client(snapshot))
        prepared = adapter.prepare(
            _request(snapshot=snapshot, kinds=("image", "image")),
            prior=None,
            checkpoints=writer,
        )
        adapter.close()

    forms = [parse_qs(call.request.content.decode()) for call in create.calls]
    assert forms[0]["is_carousel_item"] == ["true"]
    assert forms[1]["is_carousel_item"] == ["true"]
    assert forms[2] == {
        "media_type": ["CAROUSEL"],
        "children": ["18101,18102"],
        "caption": ["hello"],
    }
    assert [(item.kind, item.ordinal) for item in prepared.artifacts] == [
        ("instagram_child_container", 0),
        ("instagram_child_container", 1),
        ("instagram_parent_container", 0),
        ("staged_public", 0),
        ("staged_public", 1),
    ]


def test_durable_prior_containers_resume_without_duplicate_creation() -> None:
    snapshot = _snapshot()
    publication = _request(snapshot=snapshot)
    with respx.mock(assert_all_called=True) as router:
        _identity(router, snapshot)
        _quota(router, snapshot)
        create = router.post(
            f"{GRAPH}/{GRAPH_API_VERSION}/"
            f"{snapshot.target.expected_remote_user_id}/media"
        ).mock(return_value=httpx.Response(200, json={"id": "18111"}))
        router.get(f"{GRAPH}/{GRAPH_API_VERSION}/18111").mock(
            return_value=httpx.Response(200, json={"status_code": "FINISHED"})
        )
        first_adapter = _adapter(snapshot, _client(snapshot))
        first = first_adapter.prepare(publication, prior=None, checkpoints=_Writer())
        first_adapter.close()
        assert create.call_count == 1

    resumed_writer = _Writer()
    with respx.mock(assert_all_called=True) as router:
        _identity(router, snapshot)
        _quota(router, snapshot)
        resumed_adapter = _adapter(snapshot, _client(snapshot))
        resumed = resumed_adapter.prepare(
            publication, prior=first, checkpoints=resumed_writer
        )
        resumed_adapter.close()

    assert resumed.artifacts == first.artifacts
    assert resumed_writer.artifacts == []
    assert all("/18111" not in call.request.url.path for call in router.calls)


def test_reel_uses_public_video_reels_and_share_to_feed_true() -> None:
    snapshot = _snapshot()
    with respx.mock(assert_all_called=True) as router:
        _identity(router, snapshot)
        _quota(router, snapshot)
        create = router.post(
            f"{GRAPH}/{GRAPH_API_VERSION}/"
            f"{snapshot.target.expected_remote_user_id}/media"
        ).mock(return_value=httpx.Response(200, json={"id": "18201"}))
        router.get(f"{GRAPH}/{GRAPH_API_VERSION}/18201").mock(
            return_value=httpx.Response(200, json={"status_code": "FINISHED"})
        )
        adapter = _adapter(snapshot, _client(snapshot))
        adapter.prepare(
            _request(snapshot=snapshot, kinds=("video",)),
            prior=None,
            checkpoints=_Writer(),
        )
        adapter.close()

    form = parse_qs(create.calls[0].request.content.decode())
    assert form["media_type"] == ["REELS"]
    assert form["share_to_feed"] == ["true"]
    assert form["video_url"][0].endswith(".mp4")


@pytest.mark.parametrize(
    ("status_code", "classification"),
    [
        ("ERROR", "safe_pre_final"),
        ("EXPIRED", "safe_pre_final"),
        ("PUBLISHED", "ambiguous"),
        ("UNKNOWN", "safe_pre_final"),
    ],
)
def test_prepare_maps_container_statuses_without_publish(
    status_code: str, classification: str
) -> None:
    snapshot = _snapshot()
    cleanup: list[str] = []

    def cleaner(_staged: StagedMedia, outcome: str) -> bool:
        cleanup.append(outcome)
        return True

    with respx.mock(assert_all_called=True) as router:
        _identity(router, snapshot)
        _quota(router, snapshot)
        router.post(
            f"{GRAPH}/{GRAPH_API_VERSION}/"
            f"{snapshot.target.expected_remote_user_id}/media"
        ).mock(return_value=httpx.Response(200, json={"id": "18301"}))
        router.get(f"{GRAPH}/{GRAPH_API_VERSION}/18301").mock(
            return_value=httpx.Response(200, json={"status_code": status_code})
        )
        adapter = _adapter(snapshot, _client(snapshot), public_cleaner=cleaner)
        with pytest.raises(InstagramAdapterError) as caught:
            adapter.prepare(
                _request(snapshot=snapshot), prior=None, checkpoints=_Writer()
            )
        adapter.close()

    assert caught.value.retry_classification == classification
    assert all("media_publish" not in call.request.url.path for call in router.calls)
    expected_cleanup = ["failed"] if status_code in {"ERROR", "EXPIRED"} else []
    assert cleanup == expected_cleanup


def test_in_progress_polling_is_bounded_and_resumable() -> None:
    snapshot = _snapshot()
    with respx.mock(assert_all_called=True) as router:
        _identity(router, snapshot)
        _quota(router, snapshot)
        router.post(
            f"{GRAPH}/{GRAPH_API_VERSION}/"
            f"{snapshot.target.expected_remote_user_id}/media"
        ).mock(return_value=httpx.Response(200, json={"id": "18401"}))
        status = router.get(f"{GRAPH}/{GRAPH_API_VERSION}/18401").mock(
            return_value=httpx.Response(200, json={"status_code": "IN_PROGRESS"})
        )
        adapter = _adapter(snapshot, _client(snapshot), max_status_polls=3)
        with pytest.raises(InstagramAdapterError) as caught:
            adapter.prepare(
                _request(snapshot=snapshot), prior=None, checkpoints=_Writer()
            )
        adapter.close()

    assert caught.value.code == "instagram_container_timeout"
    assert caught.value.retry_classification == "safe_pre_final"
    assert status.call_count == 3


def test_container_creation_rejection_is_sanitized_and_pre_final() -> None:
    snapshot = _snapshot()
    token = "container-token-never-render"
    with respx.mock(assert_all_called=True) as router:
        _identity(router, snapshot)
        _quota(router, snapshot)
        router.post(
            f"{GRAPH}/{GRAPH_API_VERSION}/"
            f"{snapshot.target.expected_remote_user_id}/media"
        ).mock(return_value=httpx.Response(400, content=f"private {token}".encode()))
        adapter = _adapter(snapshot, _client(snapshot, token))
        with pytest.raises(AdapterContractError) as caught:
            adapter.prepare(
                _request(snapshot=snapshot), prior=None, checkpoints=_Writer()
            )
        adapter.close()
    assert token not in str(caught.value)
    assert token not in repr(caught.value)
    assert all("media_publish" not in call.request.url.path for call in router.calls)


@pytest.mark.parametrize(
    "status_code", ["PUBLISHED", "FINISHED", "ERROR", "EXPIRED", "UNKNOWN"]
)
def test_uncertain_media_publish_maps_read_only_status_and_never_repeats(
    status_code: str,
) -> None:
    snapshot = _snapshot()
    writer = _Writer()
    cleanup: list[str] = []

    def cleaner(_staged: StagedMedia, outcome: str) -> bool:
        cleanup.append(outcome)
        return True

    with respx.mock(assert_all_called=True) as router:
        _identity(router, snapshot)
        _quota(router, snapshot)
        router.post(
            f"{GRAPH}/{GRAPH_API_VERSION}/"
            f"{snapshot.target.expected_remote_user_id}/media"
        ).mock(return_value=httpx.Response(200, json={"id": "18501"}))
        status = router.get(f"{GRAPH}/{GRAPH_API_VERSION}/18501").mock(
            side_effect=[
                httpx.Response(200, json={"status_code": "FINISHED"}),
                httpx.Response(200, json={"status_code": status_code}),
            ]
        )
        publish = router.post(
            f"{GRAPH}/{GRAPH_API_VERSION}/"
            f"{snapshot.target.expected_remote_user_id}/media_publish"
        ).mock(return_value=httpx.Response(503, json={"error": "private"}))
        adapter = _adapter(snapshot, _client(snapshot), public_cleaner=cleaner)
        prepared = adapter.prepare(
            _request(snapshot=snapshot), prior=None, checkpoints=writer
        )
        result = adapter.commit(prepared, delivery=_delivery("final_dispatch_started"))
        with pytest.raises(AdapterContractError, match="already attempted"):
            adapter.commit(prepared, delivery=_delivery("final_dispatch_started"))
        adapter.close()

    assert result.outcome == "ambiguous"
    assert result.remote_id is None
    assert publish.call_count == 1
    assert status.call_count == 2
    assert any(item.kind == "instagram_parent_container" for item in writer.artifacts)
    assert any(item.kind == "staged_public" for item in writer.artifacts)
    assert cleanup == []


def test_known_final_rejection_is_permanent_and_one_shot() -> None:
    snapshot = _snapshot()
    cleanup: list[str] = []

    def cleaner(_staged: StagedMedia, outcome: str) -> bool:
        cleanup.append(outcome)
        return True

    with respx.mock(assert_all_called=True) as router:
        _identity(router, snapshot)
        _quota(router, snapshot)
        router.post(
            f"{GRAPH}/{GRAPH_API_VERSION}/"
            f"{snapshot.target.expected_remote_user_id}/media"
        ).mock(return_value=httpx.Response(200, json={"id": "18601"}))
        router.get(f"{GRAPH}/{GRAPH_API_VERSION}/18601").mock(
            return_value=httpx.Response(200, json={"status_code": "FINISHED"})
        )
        publish = router.post(
            f"{GRAPH}/{GRAPH_API_VERSION}/"
            f"{snapshot.target.expected_remote_user_id}/media_publish"
        ).mock(return_value=httpx.Response(400, json={"error": "rejected"}))
        adapter = _adapter(snapshot, _client(snapshot), public_cleaner=cleaner)
        prepared = adapter.prepare(
            _request(snapshot=snapshot), prior=None, checkpoints=_Writer()
        )
        result = adapter.commit(prepared, delivery=_delivery("final_dispatch_started"))
        adapter.close()

    assert result.outcome == "failed"
    assert result.retry_classification == "permanent"
    assert publish.call_count == 1
    assert cleanup == ["failed"]


def test_uncertain_in_progress_status_polling_stops_without_second_publish() -> None:
    snapshot = _snapshot()
    with respx.mock(assert_all_called=True) as router:
        _identity(router, snapshot)
        _quota(router, snapshot)
        router.post(
            f"{GRAPH}/{GRAPH_API_VERSION}/"
            f"{snapshot.target.expected_remote_user_id}/media"
        ).mock(return_value=httpx.Response(200, json={"id": "18611"}))
        status = router.get(f"{GRAPH}/{GRAPH_API_VERSION}/18611").mock(
            side_effect=[
                httpx.Response(200, json={"status_code": "FINISHED"}),
                httpx.Response(200, json={"status_code": "IN_PROGRESS"}),
                httpx.Response(200, json={"status_code": "IN_PROGRESS"}),
                httpx.Response(200, json={"status_code": "IN_PROGRESS"}),
            ]
        )
        publish = router.post(
            f"{GRAPH}/{GRAPH_API_VERSION}/"
            f"{snapshot.target.expected_remote_user_id}/media_publish"
        ).mock(return_value=httpx.Response(503, json={}))
        adapter = _adapter(snapshot, _client(snapshot), max_status_polls=3)
        prepared = adapter.prepare(
            _request(snapshot=snapshot), prior=None, checkpoints=_Writer()
        )
        result = adapter.commit(prepared, delivery=_delivery("final_dispatch_started"))
        adapter.close()
    assert result.outcome == "ambiguous"
    assert result.error_code == "instagram_publish_status_timeout"
    assert status.call_count == 4
    assert publish.call_count == 1


def test_success_status_with_missing_media_id_remains_ambiguous() -> None:
    snapshot = _snapshot()
    with respx.mock(assert_all_called=True) as router:
        _identity(router, snapshot)
        _quota(router, snapshot)
        router.post(
            f"{GRAPH}/{GRAPH_API_VERSION}/"
            f"{snapshot.target.expected_remote_user_id}/media"
        ).mock(return_value=httpx.Response(200, json={"id": "18621"}))
        status = router.get(f"{GRAPH}/{GRAPH_API_VERSION}/18621").mock(
            side_effect=[
                httpx.Response(200, json={"status_code": "FINISHED"}),
                httpx.Response(200, json={"status_code": "PUBLISHED"}),
            ]
        )
        publish = router.post(
            f"{GRAPH}/{GRAPH_API_VERSION}/"
            f"{snapshot.target.expected_remote_user_id}/media_publish"
        ).mock(return_value=httpx.Response(200, json={"unexpected": "shape"}))
        adapter = _adapter(snapshot, _client(snapshot))
        prepared = adapter.prepare(
            _request(snapshot=snapshot), prior=None, checkpoints=_Writer()
        )
        result = adapter.commit(prepared, delivery=_delivery("final_dispatch_started"))
        adapter.close()
    assert result.outcome == "ambiguous"
    assert result.remote_id is None
    assert result.error_code == "instagram_publish_status_published"
    assert status.call_count == 2
    assert publish.call_count == 1


def test_warning_persistence_and_cross_profile_staging_isolation() -> None:
    snapshot = _snapshot()
    warning = MediaWarning(
        snapshot.profile_id,
        snapshot.source_bucket,
        snapshot.bundle_id,
        None,
        "instagram_alt_text_unsupported",
        "Instagram publishing does not preserve source alt text.",
    )
    writer = _Writer()
    with respx.mock(assert_all_called=True) as router:
        _identity(router, snapshot)
        _quota(router, snapshot)
        router.post(
            f"{GRAPH}/{GRAPH_API_VERSION}/"
            f"{snapshot.target.expected_remote_user_id}/media"
        ).mock(return_value=httpx.Response(200, json={"id": "18701"}))
        router.get(f"{GRAPH}/{GRAPH_API_VERSION}/18701").mock(
            return_value=httpx.Response(200, json={"status_code": "FINISHED"})
        )
        adapter = _adapter(snapshot, _client(snapshot))
        adapter.prepare(
            _request(snapshot=snapshot, warnings=(warning,)),
            prior=None,
            checkpoints=writer,
        )
        adapter.close()
    assert writer.warnings[0][0] == "instagram_alt_text_unsupported"

    request = _request(snapshot=snapshot)
    foreign = _staged(_snapshot("other", "17841400000000001"), 0, "image")
    forged_item = PreparedMediaItem(
        request.media.items[0].source_name,
        request.media.items[0].metadata,
        request.media.items[0].private,
        foreign,
    )
    forged_media = PreparedMedia(
        snapshot.profile_id,
        snapshot.source_bucket,
        snapshot.bundle_id,
        snapshot.bundle_fingerprint,
        ("instagram",),
        (forged_item,),
        (),
    )
    forged = PublicationRequest(snapshot, forged_media, "hello", None)
    with respx.mock(assert_all_called=True) as router:
        _identity(router, snapshot)
        adapter = _adapter(snapshot, _client(snapshot))
        with pytest.raises(AdapterContractError, match="another publication"):
            adapter.prepare(forged, prior=None, checkpoints=_Writer())
        adapter.close()
    assert all(call.request.method == "GET" for call in router.calls)


def test_public_verification_failure_has_no_container_mutation() -> None:
    snapshot = _snapshot()

    def reject(_staged: StagedMedia) -> PublicURLVerification:
        raise MediaSafetyError("private path omitted")

    with respx.mock(assert_all_called=True) as router:
        _identity(router, snapshot)
        _quota(router, snapshot)
        adapter = InstagramAdapter(
            snapshot,
            _client(snapshot),
            public_verifier=reject,
            public_cleaner=_ignore_cleanup,
            clock=lambda: NOW,
            sleeper=lambda _seconds: None,
        )
        issues = adapter.preflight(_request(snapshot=snapshot))
        writer = _Writer()
        with pytest.raises(InstagramAdapterError) as caught:
            adapter.prepare(_request(snapshot=snapshot), prior=None, checkpoints=writer)
        adapter.close()
    assert [issue.code for issue in issues] == ["instagram_public_media_unavailable"]
    assert caught.value.code == "instagram_public_media_unavailable"
    assert all(call.request.method == "GET" for call in router.calls)
    assert writer.phases == [] and writer.artifacts == []


def test_public_verifier_proof_must_match_profile_staging_bytes() -> None:
    snapshot = _snapshot()

    def mismatch(staged: StagedMedia) -> PublicURLVerification:
        assert staged.public_url is not None
        return PublicURLVerification(
            staged.public_url, 0, "f" * 64, staged.mime_type, staged.size_bytes
        )

    with respx.mock(assert_all_called=True) as router:
        _identity(router, snapshot)
        _quota(router, snapshot)
        adapter = InstagramAdapter(
            snapshot,
            _client(snapshot),
            public_verifier=mismatch,
            public_cleaner=_ignore_cleanup,
            clock=lambda: NOW,
            sleeper=lambda _seconds: None,
        )
        issues = adapter.preflight(_request(snapshot=snapshot))
        writer = _Writer()
        with pytest.raises(InstagramAdapterError) as caught:
            adapter.prepare(_request(snapshot=snapshot), prior=None, checkpoints=writer)
        adapter.close()
    assert [issue.code for issue in issues] == ["instagram_public_media_mismatch"]
    assert caught.value.code == "instagram_public_media_mismatch"
    assert all(call.request.method == "GET" for call in router.calls)
    assert writer.phases == [] and writer.artifacts == []


def test_preflight_rejects_same_hash_url_from_another_profile_path() -> None:
    snapshot = _snapshot()
    publication = _request(snapshot=snapshot)
    staged = publication.media.items[0].public
    assert staged is not None
    foreign_url = (
        "https://media.example.test/post-pulsar/"
        f"other-QUEUE-{snapshot.bundle_fingerprint}-0.jpg"
    )
    forged = _with_public_staging(publication, replace(staged, public_url=foreign_url))
    writer = _Writer()

    with respx.mock(assert_all_called=True) as router:
        _identity(router, snapshot)
        adapter = _adapter(snapshot, _client(snapshot))
        issues = adapter.preflight(forged)
        with pytest.raises(InstagramAdapterError) as caught:
            adapter.prepare(forged, prior=None, checkpoints=writer)
        adapter.close()

    assert [issue.code for issue in issues] == [
        "instagram_public_media_identity_invalid"
    ]
    assert caught.value.code == "instagram_public_media_identity_invalid"
    assert writer.phases == [] and writer.artifacts == []
    assert all(call.request.method == "GET" for call in router.calls)


def test_preflight_rejects_same_hash_redirect_to_another_profile_path() -> None:
    snapshot = _snapshot()

    def cross_profile_redirect(staged: StagedMedia) -> PublicURLVerification:
        return PublicURLVerification(
            (
                "https://media.example.test/post-pulsar/"
                f"other-QUEUE-{staged.bundle_fingerprint}-0.jpg"
            ),
            1,
            staged.sha256,
            staged.mime_type,
            staged.size_bytes,
        )

    with respx.mock(assert_all_called=True) as router:
        _identity(router, snapshot)
        _quota(router, snapshot)
        adapter = InstagramAdapter(
            snapshot,
            _client(snapshot),
            public_verifier=cross_profile_redirect,
            public_cleaner=_ignore_cleanup,
            clock=lambda: NOW,
            sleeper=lambda _seconds: None,
        )
        issues = adapter.preflight(_request(snapshot=snapshot))
        adapter.close()

    assert [issue.code for issue in issues] == [
        "instagram_public_media_identity_invalid"
    ]
    assert all(call.request.method == "GET" for call in router.calls)


def test_successful_preflight_is_reverified_immediately_before_creation() -> None:
    snapshot = _snapshot()
    verified: list[str] = []

    def verifier(staged: StagedMedia) -> PublicURLVerification:
        verified.append(staged.source_name)
        return _proof(staged)

    with respx.mock(assert_all_called=True) as router:
        _identity(router, snapshot)
        _quota(router, snapshot)
        router.post(
            f"{GRAPH}/{GRAPH_API_VERSION}/"
            f"{snapshot.target.expected_remote_user_id}/media"
        ).mock(return_value=httpx.Response(200, json={"id": "18801"}))
        router.get(f"{GRAPH}/{GRAPH_API_VERSION}/18801").mock(
            return_value=httpx.Response(200, json={"status_code": "FINISHED"})
        )
        adapter = InstagramAdapter(
            snapshot,
            _client(snapshot),
            public_verifier=verifier,
            public_cleaner=_ignore_cleanup,
            clock=lambda: NOW,
            sleeper=lambda _seconds: None,
        )
        assert adapter.preflight(_request(snapshot=snapshot)) == ()
        adapter.prepare(_request(snapshot=snapshot), prior=None, checkpoints=_Writer())
        adapter.close()

    assert verified == ["post-0.jpg", "post-0.jpg"]


@pytest.mark.parametrize(
    ("status_code", "exists_after"),
    [("ERROR", False), ("PUBLISHED", True)],
)
def test_prepublication_cleanup_only_for_known_terminal_status(
    tmp_path: Path, status_code: str, exists_after: bool
) -> None:
    snapshot = _snapshot(media_directory=str(tmp_path))
    publication = _request(snapshot=snapshot)
    staged = publication.media.items[0].public
    assert staged is not None
    content = b"public-stage"
    staged = replace(
        staged, sha256=hashlib.sha256(content).hexdigest(), size_bytes=len(content)
    )
    publication = _with_public_staging(publication, staged)
    staged_path = tmp_path / staged.relative_path
    staged_path.write_bytes(content)

    with respx.mock(assert_all_called=True) as router:
        _identity(router, snapshot)
        _quota(router, snapshot)
        router.post(
            f"{GRAPH}/{GRAPH_API_VERSION}/"
            f"{snapshot.target.expected_remote_user_id}/media"
        ).mock(return_value=httpx.Response(200, json={"id": "18811"}))
        router.get(f"{GRAPH}/{GRAPH_API_VERSION}/18811").mock(
            return_value=httpx.Response(200, json={"status_code": status_code})
        )
        adapter = InstagramAdapter(
            snapshot,
            _client(snapshot),
            public_verifier=_proof,
            clock=lambda: NOW,
            sleeper=lambda _seconds: None,
        )
        with pytest.raises(InstagramAdapterError):
            adapter.prepare(publication, prior=None, checkpoints=_Writer())
        adapter.close()

    assert staged_path.exists() is exists_after


@pytest.mark.parametrize(
    ("actual_content", "exists_after"),
    [(b"expected", False), (b"tampered", True)],
)
def test_published_cleanup_is_hash_guarded(
    tmp_path: Path, actual_content: bytes, exists_after: bool
) -> None:
    snapshot = _snapshot(media_directory=str(tmp_path))
    publication = _request(snapshot=snapshot)
    staged = publication.media.items[0].public
    assert staged is not None
    expected = b"expected"
    staged = replace(
        staged, sha256=hashlib.sha256(expected).hexdigest(), size_bytes=len(expected)
    )
    publication = _with_public_staging(publication, staged)
    staged_path = tmp_path / staged.relative_path
    staged_path.write_bytes(actual_content)

    with respx.mock(assert_all_called=True) as router:
        _identity(router, snapshot)
        _quota(router, snapshot)
        router.post(
            f"{GRAPH}/{GRAPH_API_VERSION}/"
            f"{snapshot.target.expected_remote_user_id}/media"
        ).mock(return_value=httpx.Response(200, json={"id": "18821"}))
        router.get(f"{GRAPH}/{GRAPH_API_VERSION}/18821").mock(
            return_value=httpx.Response(200, json={"status_code": "FINISHED"})
        )
        router.post(
            f"{GRAPH}/{GRAPH_API_VERSION}/"
            f"{snapshot.target.expected_remote_user_id}/media_publish"
        ).mock(return_value=httpx.Response(200, json={"id": "19821"}))
        adapter = InstagramAdapter(
            snapshot,
            _client(snapshot),
            public_verifier=_proof,
            clock=lambda: NOW,
            sleeper=lambda _seconds: None,
        )
        prepared = adapter.prepare(publication, prior=None, checkpoints=_Writer())
        result = adapter.commit(prepared, delivery=_delivery("final_dispatch_started"))
        adapter.close()

    assert result.outcome == "published"
    assert staged_path.exists() is exists_after
    if exists_after:
        assert staged_path.read_bytes() == actual_content


def test_processing_poll_uses_frozen_timeout_and_bounds_final_sleep() -> None:
    snapshot = _snapshot(processing_timeout_seconds=90.0)
    elapsed = 0.0
    sleeps: list[float] = []

    def monotonic() -> float:
        return elapsed

    def sleeper(seconds: float) -> None:
        nonlocal elapsed
        sleeps.append(seconds)
        elapsed += seconds

    with respx.mock(assert_all_called=True) as router:
        _identity(router, snapshot)
        _quota(router, snapshot)
        router.post(
            f"{GRAPH}/{GRAPH_API_VERSION}/"
            f"{snapshot.target.expected_remote_user_id}/media"
        ).mock(return_value=httpx.Response(200, json={"id": "18831"}))
        status = router.get(f"{GRAPH}/{GRAPH_API_VERSION}/18831").mock(
            return_value=httpx.Response(200, json={"status_code": "IN_PROGRESS"})
        )
        adapter = InstagramAdapter(
            snapshot,
            _client(snapshot),
            public_verifier=_proof,
            public_cleaner=_ignore_cleanup,
            clock=lambda: NOW,
            monotonic=monotonic,
            sleeper=sleeper,
        )
        with pytest.raises(InstagramAdapterError) as caught:
            adapter.prepare(
                _request(snapshot=snapshot), prior=None, checkpoints=_Writer()
            )
        adapter.close()

    assert caught.value.code == "instagram_container_timeout"
    assert sleeps == [60.0, 30.0]
    assert status.call_count == 2


def test_resume_does_not_reset_frozen_container_processing_deadline() -> None:
    snapshot = _snapshot(processing_timeout_seconds=300.0)
    publication = _request(snapshot=snapshot)
    first_writer = _Writer()
    with respx.mock(assert_all_called=True) as router:
        _identity(router, snapshot)
        _quota(router, snapshot)
        router.post(
            f"{GRAPH}/{GRAPH_API_VERSION}/"
            f"{snapshot.target.expected_remote_user_id}/media"
        ).mock(return_value=httpx.Response(200, json={"id": "18841"}))
        router.get(f"{GRAPH}/{GRAPH_API_VERSION}/18841").mock(
            return_value=httpx.Response(200, json={"status_code": "IN_PROGRESS"})
        )
        first = _adapter(snapshot, _client(snapshot), max_status_polls=1)
        with pytest.raises(InstagramAdapterError):
            first.prepare(publication, prior=None, checkpoints=first_writer)
        first.close()

    prior = PreparedPublication(
        publication,
        1,
        tuple(
            sorted(first_writer.artifacts, key=lambda item: (item.kind, item.ordinal))
        ),
    )
    sleeps: list[float] = []
    with respx.mock(assert_all_called=True) as router:
        _identity(router, snapshot)
        _quota(router, snapshot)
        resumed = InstagramAdapter(
            snapshot,
            _client(snapshot),
            public_verifier=_proof,
            public_cleaner=_ignore_cleanup,
            clock=lambda: NOW + timedelta(seconds=301),
            monotonic=lambda: 10.0,
            sleeper=sleeps.append,
        )
        with pytest.raises(InstagramAdapterError) as caught:
            resumed.prepare(publication, prior=prior, checkpoints=_Writer())
        resumed.close()

    assert caught.value.code == "instagram_container_timeout"
    assert all("/18841" not in call.request.url.path for call in router.calls)
    assert sleeps == []


def test_timeout_retains_real_staging_and_restart_reuses_container(
    tmp_path: Path,
) -> None:
    snapshot = _snapshot(media_directory=str(tmp_path))
    publication = _request(snapshot=snapshot)
    staged = publication.media.items[0].public
    assert staged is not None
    payload = b"public-timeout-stage"
    staged = replace(
        staged, sha256=hashlib.sha256(payload).hexdigest(), size_bytes=len(payload)
    )
    publication = _with_public_staging(publication, staged)
    staged_path = tmp_path / staged.relative_path
    staged_path.write_bytes(payload)
    first_writer = _Writer()

    with respx.mock(assert_all_called=True) as router:
        _identity(router, snapshot)
        _quota(router, snapshot)
        create = router.post(
            f"{GRAPH}/{GRAPH_API_VERSION}/"
            f"{snapshot.target.expected_remote_user_id}/media"
        ).mock(return_value=httpx.Response(200, json={"id": "18851"}))
        router.get(f"{GRAPH}/{GRAPH_API_VERSION}/18851").mock(
            return_value=httpx.Response(200, json={"status_code": "IN_PROGRESS"})
        )
        first = InstagramAdapter(
            snapshot,
            _client(snapshot),
            public_verifier=_proof,
            clock=lambda: NOW,
            sleeper=lambda _seconds: None,
            max_status_polls=1,
        )
        with pytest.raises(InstagramAdapterError, match="did not become ready"):
            first.prepare(publication, prior=None, checkpoints=first_writer)
        first.close()
    assert create.call_count == 1
    assert staged_path.read_bytes() == payload

    prior = PreparedPublication(
        publication,
        1,
        tuple(
            sorted(first_writer.artifacts, key=lambda item: (item.kind, item.ordinal))
        ),
    )
    resumed_writer = _Writer()
    with respx.mock(assert_all_called=True) as router:
        _identity(router, snapshot)
        _quota(router, snapshot)
        status = router.get(f"{GRAPH}/{GRAPH_API_VERSION}/18851").mock(
            return_value=httpx.Response(200, json={"status_code": "FINISHED"})
        )
        publish = router.post(
            f"{GRAPH}/{GRAPH_API_VERSION}/"
            f"{snapshot.target.expected_remote_user_id}/media_publish"
        ).mock(return_value=httpx.Response(200, json={"id": "19851"}))
        resumed = InstagramAdapter(
            snapshot,
            _client(snapshot),
            public_verifier=_proof,
            clock=lambda: NOW,
            sleeper=lambda _seconds: None,
        )
        prepared = resumed.prepare(publication, prior=prior, checkpoints=resumed_writer)
        result = resumed.commit(prepared, delivery=_delivery("final_dispatch_started"))
        resumed.close()

    assert status.call_count == 1
    assert publish.call_count == 1
    assert result.outcome == "published"
    assert not staged_path.exists()


def test_network_uncertainty_after_container_creation_retains_real_staging(
    tmp_path: Path,
) -> None:
    snapshot = _snapshot(media_directory=str(tmp_path))
    publication = _request(snapshot=snapshot)
    staged = publication.media.items[0].public
    assert staged is not None
    payload = b"public-network-stage"
    staged = replace(
        staged, sha256=hashlib.sha256(payload).hexdigest(), size_bytes=len(payload)
    )
    publication = _with_public_staging(publication, staged)
    staged_path = tmp_path / staged.relative_path
    staged_path.write_bytes(payload)

    with respx.mock(assert_all_called=True) as router:
        _identity(router, snapshot)
        _quota(router, snapshot)
        router.post(
            f"{GRAPH}/{GRAPH_API_VERSION}/"
            f"{snapshot.target.expected_remote_user_id}/media"
        ).mock(return_value=httpx.Response(200, json={"id": "18861"}))
        router.get(f"{GRAPH}/{GRAPH_API_VERSION}/18861").mock(
            side_effect=httpx.ConnectError("offline")
        )
        client = PlatformHTTPClient(
            snapshot,
            SecretValue("instagram-token"),
            base_url=GRAPH,
            policy=HTTPPolicy(max_pre_final_attempts=1),
        )
        adapter = InstagramAdapter(
            snapshot,
            client,
            public_verifier=_proof,
            clock=lambda: NOW,
            sleeper=lambda _seconds: None,
        )
        with pytest.raises(AdapterContractError, match="network request failed"):
            adapter.prepare(publication, prior=None, checkpoints=_Writer())
        adapter.close()

    assert staged_path.read_bytes() == payload


def test_uncertain_container_creation_response_retains_real_staging(
    tmp_path: Path,
) -> None:
    snapshot = _snapshot(media_directory=str(tmp_path))
    publication = _request(snapshot=snapshot)
    staged = publication.media.items[0].public
    assert staged is not None
    payload = b"public-create-uncertain"
    staged = replace(
        staged, sha256=hashlib.sha256(payload).hexdigest(), size_bytes=len(payload)
    )
    publication = _with_public_staging(publication, staged)
    staged_path = tmp_path / staged.relative_path
    staged_path.write_bytes(payload)

    with respx.mock(assert_all_called=True) as router:
        _identity(router, snapshot)
        _quota(router, snapshot)
        router.post(
            f"{GRAPH}/{GRAPH_API_VERSION}/"
            f"{snapshot.target.expected_remote_user_id}/media"
        ).mock(return_value=httpx.Response(200, json={}))
        adapter = InstagramAdapter(
            snapshot,
            _client(snapshot),
            public_verifier=_proof,
            clock=lambda: NOW,
            sleeper=lambda _seconds: None,
        )
        with pytest.raises(AdapterContractError):
            adapter.prepare(publication, prior=None, checkpoints=_Writer())
        adapter.close()

    assert staged_path.read_bytes() == payload


def test_finished_transition_survives_crash_and_restart_skips_status() -> None:
    class CrashAfterFinished(_Writer):
        def transition_artifact_processing(
            self, current: ArtifactRecord, checkpoint: ArtifactCheckpoint
        ) -> ArtifactRecord:
            updated = super().transition_artifact_processing(current, checkpoint)
            if checkpoint.processing_metadata["state"] == "FINISHED":
                raise RuntimeError("crash after durable transition")
            return updated

    snapshot = _snapshot()
    publication = _request(snapshot=snapshot)
    writer = CrashAfterFinished()
    with respx.mock(assert_all_called=True) as router:
        _identity(router, snapshot)
        _quota(router, snapshot)
        router.post(
            f"{GRAPH}/{GRAPH_API_VERSION}/"
            f"{snapshot.target.expected_remote_user_id}/media"
        ).mock(return_value=httpx.Response(200, json={"id": "18871"}))
        status = router.get(f"{GRAPH}/{GRAPH_API_VERSION}/18871").mock(
            return_value=httpx.Response(200, json={"status_code": "FINISHED"})
        )
        adapter = _adapter(snapshot, _client(snapshot))
        with pytest.raises(RuntimeError, match="crash after durable transition"):
            adapter.prepare(publication, prior=None, checkpoints=writer)
        adapter.close()
    assert status.call_count == 1

    prior = PreparedPublication(
        publication,
        1,
        tuple(sorted(writer.artifacts, key=lambda item: (item.kind, item.ordinal))),
    )
    with respx.mock(assert_all_called=True) as router:
        _identity(router, snapshot)
        _quota(router, snapshot)
        resumed = _adapter(snapshot, _client(snapshot))
        prepared = resumed.prepare(publication, prior=prior, checkpoints=_Writer())
        resumed.close()
    assert _artifact_state(prepared, "instagram_parent_container") == "FINISHED"
    assert all("/18871" not in call.request.url.path for call in router.calls)


def test_real_state_repository_accepts_instagram_processing_transitions(
    tmp_path: Path,
) -> None:
    original = _snapshot(media_directory=str(tmp_path / "public"))
    repository = StateRepository(tmp_path / "state.sqlite3", clock=lambda: NOW)
    stored_target = replace(
        original.target, request_settings=dict(original.target.request_settings)
    )
    repository.register_profile(
        original.profile_id,
        tmp_path / "accounts" / original.profile_id,
        (
            ProfileTargetSnapshot(
                "instagram",
                original.target.expected_remote_user_id,
                original.target.expected_username,
                original.target.token_env_var,
                dict(original.target.request_settings),
            ),
        ),
        config_hash="c" * 64,
    )
    bundle_key = repository.add_bundle(
        profile_id=original.profile_id,
        bundle_id=original.bundle_id,
        fingerprint=original.bundle_fingerprint,
        source_bucket=original.source_bucket,
        files=(
            BundleFileSnapshot(
                "post-0.jpg", "media", 0, "image", "image/jpeg", 100, "a" * 64
            ),
        ),
        targets=(stored_target,),
    )
    snapshot = replace(original, bundle_key=bundle_key)
    claimed = repository.claim_delivery(bundle_key, "instagram", "claim-instagram")
    writer = _RepositoryWriter(
        repository, bundle_key, "claim-instagram", claimed.attempt_count
    )

    with respx.mock(assert_all_called=True) as router:
        _identity(router, snapshot)
        _quota(router, snapshot)
        router.post(
            f"{GRAPH}/{GRAPH_API_VERSION}/"
            f"{snapshot.target.expected_remote_user_id}/media"
        ).mock(return_value=httpx.Response(200, json={"id": "18881"}))
        router.get(f"{GRAPH}/{GRAPH_API_VERSION}/18881").mock(
            side_effect=[
                httpx.Response(200, json={"status_code": "IN_PROGRESS"}),
                httpx.Response(200, json={"status_code": "FINISHED"}),
            ]
        )
        adapter = _adapter(snapshot, _client(snapshot))
        prepared = adapter.prepare(
            _request(snapshot=snapshot), prior=None, checkpoints=writer
        )
        adapter.close()

    assert _artifact_state(prepared, "instagram_parent_container") == "FINISHED"
    durable = repository.list_delivery_artifacts(
        bundle_key, "instagram", attempt_count=claimed.attempt_count
    )
    assert (
        next(
            item for item in durable if item.kind == "instagram_parent_container"
        ).processing_metadata["state"]
        == "FINISHED"
    )
    assert repository.get_delivery(bundle_key, "instagram").phase == "ready"


def test_prepare_status_retry_after_cannot_overrun_monotonic_deadline() -> None:
    snapshot = _snapshot(processing_timeout_seconds=5.0)
    elapsed = 0.0

    def monotonic() -> float:
        return elapsed

    def sleep(seconds: float) -> None:
        nonlocal elapsed
        elapsed += seconds

    with respx.mock(assert_all_called=True) as router:
        _identity(router, snapshot)
        _quota(router, snapshot)
        router.post(
            f"{GRAPH}/{GRAPH_API_VERSION}/"
            f"{snapshot.target.expected_remote_user_id}/media"
        ).mock(return_value=httpx.Response(200, json={"id": "18891"}))
        status = router.get(f"{GRAPH}/{GRAPH_API_VERSION}/18891").mock(
            return_value=httpx.Response(429, headers={"Retry-After": "60"})
        )
        client = PlatformHTTPClient(
            snapshot,
            SecretValue("instagram-token"),
            base_url=GRAPH,
            policy=HTTPPolicy(max_pre_final_attempts=3, max_retry_after_seconds=60),
            sleeper=sleep,
        )
        adapter = InstagramAdapter(
            snapshot,
            client,
            public_verifier=_proof,
            public_cleaner=_ignore_cleanup,
            clock=lambda: NOW,
            monotonic=monotonic,
            sleeper=sleep,
        )
        with pytest.raises(InstagramAdapterError) as caught:
            adapter.prepare(
                _request(snapshot=snapshot), prior=None, checkpoints=_Writer()
            )
        adapter.close()

    assert caught.value.code == "instagram_container_timeout"
    assert caught.value.retry_classification == "safe_pre_final"
    assert elapsed == 5.0
    assert status.call_count == 1


def test_uncertain_publish_retry_after_cannot_overrun_monotonic_deadline() -> None:
    snapshot = _snapshot(processing_timeout_seconds=5.0)
    elapsed = 0.0

    def monotonic() -> float:
        return elapsed

    def sleep(seconds: float) -> None:
        nonlocal elapsed
        elapsed += seconds

    with respx.mock(assert_all_called=True) as router:
        _identity(router, snapshot)
        _quota(router, snapshot)
        router.post(
            f"{GRAPH}/{GRAPH_API_VERSION}/"
            f"{snapshot.target.expected_remote_user_id}/media"
        ).mock(return_value=httpx.Response(200, json={"id": "18901"}))
        status = router.get(f"{GRAPH}/{GRAPH_API_VERSION}/18901").mock(
            side_effect=[
                httpx.Response(200, json={"status_code": "FINISHED"}),
                httpx.Response(429, headers={"Retry-After": "60"}),
            ]
        )
        publish = router.post(
            f"{GRAPH}/{GRAPH_API_VERSION}/"
            f"{snapshot.target.expected_remote_user_id}/media_publish"
        ).mock(return_value=httpx.Response(503))
        client = PlatformHTTPClient(
            snapshot,
            SecretValue("instagram-token"),
            base_url=GRAPH,
            policy=HTTPPolicy(max_pre_final_attempts=3, max_retry_after_seconds=60),
            sleeper=sleep,
        )
        adapter = InstagramAdapter(
            snapshot,
            client,
            public_verifier=_proof,
            public_cleaner=_ignore_cleanup,
            clock=lambda: NOW,
            monotonic=monotonic,
            sleeper=sleep,
        )
        prepared = adapter.prepare(
            _request(snapshot=snapshot), prior=None, checkpoints=_Writer()
        )
        result = adapter.commit(prepared, delivery=_delivery("final_dispatch_started"))
        adapter.close()

    assert result.outcome == "ambiguous"
    assert result.error_code == "instagram_publish_status_timeout"
    assert elapsed == 5.0
    assert status.call_count == 2
    assert publish.call_count == 1


def test_late_finished_after_zero_retry_after_is_not_checkpointed() -> None:
    snapshot = _snapshot(processing_timeout_seconds=5.0)
    elapsed = 0.0
    status_responses = 0

    def monotonic() -> float:
        return elapsed

    def slow_finished(_request: httpx.Request) -> httpx.Response:
        nonlocal elapsed
        elapsed = 6.0
        return httpx.Response(200, json={"status_code": "FINISHED"})

    def status_response(request: httpx.Request) -> httpx.Response:
        nonlocal status_responses
        status_responses += 1
        if status_responses == 1:
            return httpx.Response(429, headers={"Retry-After": "0"})
        return slow_finished(request)

    writer = _Writer()
    with respx.mock(assert_all_called=True) as router:
        _identity(router, snapshot)
        _quota(router, snapshot)
        router.post(
            f"{GRAPH}/{GRAPH_API_VERSION}/"
            f"{snapshot.target.expected_remote_user_id}/media"
        ).mock(return_value=httpx.Response(200, json={"id": "18911"}))
        status = router.get(f"{GRAPH}/{GRAPH_API_VERSION}/18911").mock(
            side_effect=status_response
        )
        client = PlatformHTTPClient(
            snapshot,
            SecretValue("instagram-token"),
            base_url=GRAPH,
            policy=HTTPPolicy(max_pre_final_attempts=2),
            sleeper=lambda _seconds: None,
        )
        adapter = InstagramAdapter(
            snapshot,
            client,
            public_verifier=_proof,
            public_cleaner=_ignore_cleanup,
            clock=lambda: NOW,
            monotonic=monotonic,
            sleeper=lambda _seconds: None,
        )
        with pytest.raises(InstagramAdapterError) as caught:
            adapter.prepare(_request(snapshot=snapshot), prior=None, checkpoints=writer)
        adapter.close()

    assert caught.value.code == "instagram_container_timeout"
    assert status.call_count == 2
    assert writer.transitions == []


def _artifact_state(prepared: PreparedPublication, kind: str) -> object:
    return next(
        artifact for artifact in prepared.artifacts if artifact.kind == kind
    ).processing_metadata["state"]
