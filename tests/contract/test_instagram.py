"""Offline contracts for the official Instagram Graph API adapter."""

from __future__ import annotations

import hashlib
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
from post_pulsar.platforms.http import PlatformHTTPClient
from post_pulsar.platforms.instagram import (
    GRAPH_API_VERSION,
    GRAPH_BASE_URL,
    REQUIRED_SCOPES,
    InstagramAdapter,
    InstagramAdapterError,
)
from post_pulsar.state import ArtifactRecord, DeliveryRecord, TargetSnapshot

GRAPH = GRAPH_BASE_URL
NOW = datetime(2026, 9, 5, 12, tzinfo=UTC)


def _snapshot(
    profile_id: str = "ansonphong",
    user_id: str = "17841400000000000",
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

    def record_warning(self, code: str, details: dict[str, object]) -> None:
        self.warnings.append((code, details))


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
    **kwargs: object,
) -> InstagramAdapter:
    return InstagramAdapter(
        snapshot,
        client,
        public_verifier=_proof,
        clock=lambda: NOW,
        sleeper=lambda _seconds: None,
        **kwargs,  # type: ignore[arg-type]
    )


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
        adapter = _adapter(snapshot, _client(snapshot))
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
        status = router.get(f"{GRAPH}/{GRAPH_API_VERSION}/18111").mock(
            return_value=httpx.Response(200, json={"status_code": "FINISHED"})
        )
        resumed_adapter = _adapter(snapshot, _client(snapshot))
        resumed = resumed_adapter.prepare(
            publication, prior=first, checkpoints=resumed_writer
        )
        resumed_adapter.close()

    assert resumed.artifacts == first.artifacts
    assert resumed_writer.artifacts == []
    assert status.call_count == 1


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
        adapter = _adapter(snapshot, _client(snapshot))
        with pytest.raises(InstagramAdapterError) as caught:
            adapter.prepare(
                _request(snapshot=snapshot), prior=None, checkpoints=_Writer()
            )
        adapter.close()

    assert caught.value.retry_classification == classification
    assert all("media_publish" not in call.request.url.path for call in router.calls)


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
        adapter = _adapter(snapshot, _client(snapshot))
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


def test_known_final_rejection_is_permanent_and_one_shot() -> None:
    snapshot = _snapshot()
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
        adapter = _adapter(snapshot, _client(snapshot))
        prepared = adapter.prepare(
            _request(snapshot=snapshot), prior=None, checkpoints=_Writer()
        )
        result = adapter.commit(prepared, delivery=_delivery("final_dispatch_started"))
        adapter.close()

    assert result.outcome == "failed"
    assert result.retry_classification == "permanent"
    assert publish.call_count == 1


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
            clock=lambda: NOW,
            sleeper=lambda _seconds: None,
        )
        with pytest.raises(InstagramAdapterError) as caught:
            adapter.prepare(
                _request(snapshot=snapshot), prior=None, checkpoints=_Writer()
            )
        adapter.close()
    assert caught.value.code == "instagram_public_media_unavailable"
    assert all(call.request.method == "GET" for call in router.calls)


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
            clock=lambda: NOW,
            sleeper=lambda _seconds: None,
        )
        with pytest.raises(InstagramAdapterError) as caught:
            adapter.prepare(
                _request(snapshot=snapshot), prior=None, checkpoints=_Writer()
            )
        adapter.close()
    assert caught.value.code == "instagram_public_media_mismatch"
    assert all(call.request.method == "GET" for call in router.calls)
