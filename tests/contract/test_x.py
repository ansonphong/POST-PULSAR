from __future__ import annotations

import hashlib
import json
import re
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import httpx
import pytest

from post_pulsar.config import SecretValue
from post_pulsar.media import (
    MediaKind,
    MediaMetadata,
    PreparedMedia,
    PreparedMediaItem,
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
from post_pulsar.platforms.x import XAdapter, XAdapterError
from post_pulsar.state import (
    ArtifactRecord,
    BundleFileSnapshot,
    DeliveryPhase,
    DeliveryRecord,
    ProfileTargetSnapshot,
    StateRepository,
    TargetSnapshot,
)

NOW = datetime(2026, 9, 5, 12, tzinfo=UTC)
FINGERPRINT = "a" * 64


class Clock:
    def __init__(self) -> None:
        self.now = NOW
        self.elapsed = 0.0
        self.sleeps: list[float] = []

    def __call__(self) -> datetime:
        return self.now

    def sleep(self, seconds: float) -> None:
        self.sleeps.append(seconds)
        self.now += timedelta(seconds=seconds)
        self.elapsed += seconds

    def monotonic(self) -> float:
        return self.elapsed


class Writer:
    def __init__(self, *, attempt: int = 1) -> None:
        self.attempt = attempt
        self.events: list[tuple[str, object]] = []
        self.records: dict[tuple[str, int], ArtifactRecord] = {}

    def delivery(self, phase: DeliveryPhase) -> DeliveryRecord:
        return DeliveryRecord(
            1,
            "x",
            "in_flight",
            phase,
            self.attempt,
            0,
            False,
            None,
            None,
            None,
            None,
            1,
        )

    def advance_phase(self, phase: DeliveryPhase) -> DeliveryRecord:
        self.events.append(("phase", phase))
        return self.delivery(phase)

    def checkpoint_artifact(self, checkpoint: ArtifactCheckpoint) -> ArtifactRecord:
        self.events.append(("checkpoint", checkpoint))
        key = (checkpoint.kind, checkpoint.ordinal)
        record = ArtifactRecord(
            1,
            "x",
            self.attempt,
            checkpoint.kind,
            checkpoint.ordinal,
            checkpoint.external_id,
            checkpoint.relative_path,
            checkpoint.sha256,
            checkpoint.expires_at,
            checkpoint.processing_metadata,
        )
        existing = self.records.get(key)
        if existing is not None and existing != record:
            raise AssertionError("checkpoint changed")
        self.records[key] = record
        return record

    def transition_artifact_processing(
        self, current: ArtifactRecord, checkpoint: ArtifactCheckpoint
    ) -> ArtifactRecord:
        self.events.append(("transition", checkpoint))
        key = (checkpoint.kind, checkpoint.ordinal)
        if (
            self.records.get(key) != current
            or checkpoint.external_id != current.external_id
        ):
            raise AssertionError("processing transition changed artifact identity")
        record = ArtifactRecord(
            current.bundle_key,
            current.platform,
            current.attempt_count,
            current.kind,
            current.ordinal,
            current.external_id,
            current.relative_path,
            current.sha256,
            checkpoint.expires_at,
            checkpoint.processing_metadata,
        )
        self.records[key] = record
        return record

    def replace_expired_artifact(
        self, current: ArtifactRecord, checkpoint: ArtifactCheckpoint
    ) -> ArtifactRecord:
        self.events.append(("replace", checkpoint))
        key = (checkpoint.kind, checkpoint.ordinal)
        if self.records.get(key) != current:
            raise AssertionError("expired replacement changed artifact identity")
        record = ArtifactRecord(
            current.bundle_key,
            current.platform,
            current.attempt_count,
            current.kind,
            current.ordinal,
            checkpoint.external_id,
            checkpoint.relative_path,
            checkpoint.sha256,
            checkpoint.expires_at,
            checkpoint.processing_metadata,
        )
        self.records[key] = record
        return record


class RepositoryWriter:
    def __init__(
        self,
        repository: StateRepository,
        *,
        bundle_key: int,
        claim_token: str,
        attempt_count: int,
    ) -> None:
        self.repository = repository
        self.bundle_key = bundle_key
        self.claim_token = claim_token
        self.attempt_count = attempt_count

    def advance_phase(self, phase: DeliveryPhase) -> DeliveryRecord:
        return self.repository.advance_delivery_phase(
            self.bundle_key,
            "x",
            phase,
            claim_token=self.claim_token,
            attempt_count=self.attempt_count,
        )

    def checkpoint_artifact(self, checkpoint: ArtifactCheckpoint) -> ArtifactRecord:
        return self.repository.checkpoint_artifact(
            self.bundle_key,
            "x",
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
            "x",
            kind=checkpoint.kind,
            ordinal=checkpoint.ordinal,
            external_id=checkpoint.external_id,
            expected_processing_metadata=current.processing_metadata,
            processing_metadata=checkpoint.processing_metadata,
            expected_expires_at=current.expires_at,
            expires_at=checkpoint.expires_at,
            claim_token=self.claim_token,
            attempt_count=self.attempt_count,
        )

    def replace_expired_artifact(
        self, current: ArtifactRecord, checkpoint: ArtifactCheckpoint
    ) -> ArtifactRecord:
        assert current.external_id is not None and checkpoint.external_id is not None
        assert checkpoint.expires_at is not None
        return self.repository.replace_expired_artifact(
            self.bundle_key,
            "x",
            kind=checkpoint.kind,
            ordinal=checkpoint.ordinal,
            expected_external_id=current.external_id,
            external_id=checkpoint.external_id,
            expires_at=checkpoint.expires_at,
            processing_metadata=checkpoint.processing_metadata,
            claim_token=self.claim_token,
            attempt_count=self.attempt_count,
        )


def snapshot(*, settings: dict[str, object] | None = None) -> PublicationSnapshot:
    return PublicationSnapshot(
        "alpha",
        1,
        "cat",
        FINGERPRINT,
        "QUEUE",
        TargetSnapshot(
            "x",
            "12345",
            "expected_user",
            "POST_PULSAR_X_ALPHA_USER_ACCESS_TOKEN",
            "2",
            1,
            settings or {},
        ),
    )


def media(tmp_path: Path, kinds: tuple[MediaKind, ...]) -> PreparedMedia:
    items: list[PreparedMediaItem] = []
    for ordinal, kind in enumerate(kinds):
        payload = (f"media-{ordinal}-" * 3).encode()
        digest = hashlib.sha256(payload).hexdigest()
        extension = (
            "mp4" if kind == "video" else "gif" if kind == "animated_gif" else "jpg"
        )
        mime = (
            "video/mp4"
            if kind == "video"
            else "image/gif"
            if kind == "animated_gif"
            else "image/jpeg"
        )
        relative = (
            Path("alpha/QUEUE")
            / FINGERPRINT
            / f"{ordinal + 1:02d}-{digest}.{extension}"
        )
        destination = tmp_path / relative
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_bytes(payload)
        staged = StagedMedia(
            "alpha",
            "QUEUE",
            "cat",
            FINGERPRINT,
            f"cat_{ordinal + 1}.{extension}",
            "private",
            relative,
            digest,
            mime,
            len(payload),
            "known_terminal_hash_match",
        )
        metadata = MediaMetadata(
            kind,
            extension,
            mime,
            640,
            480,
            duration_seconds=2.0 if kind != "image" else None,
        )
        items.append(PreparedMediaItem(staged.source_name, metadata, staged, None))
    return PreparedMedia("alpha", "QUEUE", "cat", FINGERPRINT, ("x",), tuple(items), ())


def adapter(
    tmp_path: Path,
    handler: Any,
    *,
    settings: dict[str, object] | None = None,
    clock: Clock | None = None,
) -> XAdapter:
    snap = snapshot(settings=settings)
    timer = clock or Clock()
    client = PlatformHTTPClient(
        snap,
        SecretValue("secret-token"),
        base_url="https://api.x.com",
        policy=HTTPPolicy(max_pre_final_attempts=2, retry_backoff_seconds=0),
        sleeper=timer.sleep,
        clock=timer,
        transport=httpx.MockTransport(handler),
    )
    return XAdapter(
        snap,
        client,
        private_staging_directory=tmp_path,
        clock=timer,
        monotonic=timer.monotonic,
        sleeper=timer.sleep,
    )


def request(
    tmp_path: Path,
    kinds: tuple[MediaKind, ...],
    *,
    text: str | None = "hello",
    alt: str | None = None,
    settings: dict[str, object] | None = None,
) -> PublicationRequest:
    return PublicationRequest(
        snapshot(settings=settings), media(tmp_path, kinds), text, alt
    )


def identity_response() -> httpx.Response:
    return httpx.Response(
        200, json={"data": {"id": "12345", "username": "expected_user"}}
    )


def test_wrong_identity_blocks_before_any_mutation(tmp_path: Path) -> None:
    calls: list[str] = []

    def handler(req: httpx.Request) -> httpx.Response:
        calls.append(req.url.path)
        return httpx.Response(200, json={"data": {"id": "999", "username": "intruder"}})

    post = request(tmp_path, ("image",))
    with adapter(tmp_path, handler) as target:  # noqa: SIM117 - ordered cleanup
        with pytest.raises(AdapterContractError, match="identity"):
            target.prepare(post, prior=None, checkpoints=Writer())
    assert calls == ["/2/users/me"]


def test_weighted_text_and_alt_text_preflight_boundaries(tmp_path: Path) -> None:
    target = adapter(tmp_path, lambda _req: identity_response())
    assert (
        target.preflight(
            request(tmp_path, ("image",), text="界" * 140, alt="e\u0301" * 1000)
        )
        == ()
    )
    # Canonical twitter-text UnicodeDirectionalMarkerCounterTest fixture: this
    # visually contains a URL, but the directional controls/domain make its
    # official weighted length 31 rather than the transformed URL length 23.
    directional = "\u2066\u202ahttp://foobar.پاکستان/\u202c\u2069"
    assert target.preflight(request(tmp_path, (), text=directional)) == ()
    assert [
        issue.code
        for issue in target.preflight(
            request(tmp_path, (), text="a" * 250 + directional)
        )
    ] == ["x_text_too_long"]
    directional_punctuation = "http://test.co\u202c,"
    assert (
        target.preflight(
            request(tmp_path, (), text="a" * 253 + " " + directional_punctuation)
        )
        == ()
    )
    assert [
        issue.code
        for issue in target.preflight(
            request(tmp_path, (), text="a" * 254 + " " + directional_punctuation)
        )
    ] == ["x_text_too_long"]
    assert target.preflight(request(tmp_path, (), text="\r\n" * 141)) == ()
    issues = target.preflight(
        request(tmp_path, ("image",), text="界" * 141, alt="a" * 1001)
    )
    assert {issue.code for issue in issues} == {
        "x_text_too_long",
        "x_alt_text_too_long",
    }
    assert target.preflight(request(tmp_path, (), text="👨‍👩‍👧‍👦" * 140)) == ()
    emoji_issues = target.preflight(request(tmp_path, (), text="👨‍👩‍👧‍👦" * 141))
    assert [issue.code for issue in emoji_issues] == ["x_text_too_long"]
    assert (
        target.preflight(
            request(
                tmp_path,
                (),
                text="a" * 256 + " https://example.com/a/very/long/url",
            )
        )
        == ()
    )
    for kind in ("video", "animated_gif"):
        issues = target.preflight(request(tmp_path, (kind,), alt="description"))
        assert [issue.code for issue in issues] == ["x_alt_text_requires_images"]
    target.close()


def test_simple_images_metadata_and_final_payload_use_v2_only(tmp_path: Path) -> None:
    seen: list[tuple[str, str]] = []
    ids = iter(("7001", "7002"))

    def handler(req: httpx.Request) -> httpx.Response:
        seen.append((req.method, req.url.path))
        if req.url.path == "/2/users/me":
            return identity_response()
        if req.url.path == "/2/media/upload":
            assert req.method == "POST" and "command" not in req.url.params
            return httpx.Response(
                200, json={"data": {"id": next(ids), "expires_after_secs": 3600}}
            )
        if req.url.path == "/2/media/metadata":
            body = json.loads(req.content)
            assert body["metadata"]["alt_text"]["text"] == "Café"
            assert body["id"] in {"7001", "7002"}
            return httpx.Response(200, json={})
        if req.url.path == "/2/tweets":
            assert json.loads(req.content) == {
                "text": "hello",
                "media": {"media_ids": ["7001", "7002"]},
            }
            return httpx.Response(201, json={"data": {"id": "9001"}})
        raise AssertionError(req.url)

    publication = request(tmp_path, ("image", "image"), alt="Cafe\u0301")
    writer = Writer()
    with adapter(tmp_path, handler) as target:
        prepared = target.prepare(publication, prior=None, checkpoints=writer)
        first_upload = next(
            i for i, item in enumerate(seen) if item == ("POST", "/2/media/upload")
        )
        x_checkpoints = [
            i
            for i, event in enumerate(writer.events)
            if event[0] == "checkpoint"
            and isinstance(event[1], ArtifactCheckpoint)
            and event[1].kind == "x_media_id"
        ]
        assert x_checkpoints and writer.events[-1] == ("phase", "ready")
        result = target.commit(
            prepared, delivery=writer.delivery("final_dispatch_started")
        )
    assert first_upload >= 1
    assert result.outcome == "published" and result.remote_id == "9001"
    assert all(path not in {"/1.1/media/upload.json"} for _, path in seen)


def test_chunked_video_checkpoint_order_status_params_and_retry_after(
    tmp_path: Path,
) -> None:
    clock = Clock()
    events: list[str] = []
    append_indices: list[int] = []
    status_calls = 0

    def handler(req: httpx.Request) -> httpx.Response:
        nonlocal status_calls
        assert req.headers["Authorization"] == "Bearer secret-token"
        if req.url.path == "/2/users/me":
            return identity_response()
        if req.url.path == "/2/media/upload/initialize":
            events.append("initialize")
            assert json.loads(req.content) == {
                "media_category": "tweet_video",
                "media_type": "video/mp4",
                "total_bytes": 24,
                "shared": False,
            }
            return httpx.Response(
                202, json={"data": {"id": "8001", "expires_after_secs": 3600}}
            )
        if req.url.path == "/2/media/upload/8001/append":
            events.append("append")
            match = re.search(rb'name="segment_index"\r\n\r\n([0-9]+)', req.content)
            assert match is not None
            segment_index = int(match.group(1))
            append_indices.append(segment_index)
            source = b"media-0-" * 3
            assert source[segment_index * 4 : (segment_index + 1) * 4] in req.content
            return httpx.Response(204)
        if req.url.path == "/2/media/upload/8001/finalize":
            events.append("finalize")
            return httpx.Response(
                200,
                json={
                    "data": {
                        "id": "8001",
                        "expires_after_secs": 120,
                        "processing_info": {"state": "pending", "check_after_secs": 1},
                    }
                },
            )
        if req.url.path == "/2/media/upload":
            events.append("status")
            assert req.method == "GET"
            assert dict(req.url.params) == {"command": "STATUS", "media_id": "8001"}
            status_calls += 1
            if status_calls == 1:
                return httpx.Response(429, headers={"Retry-After": "2"})
            return httpx.Response(
                200,
                json={
                    "data": {"id": "8001", "processing_info": {"state": "succeeded"}}
                },
            )
        raise AssertionError(req.url)

    settings: dict[str, object] = {
        "chunk_size_bytes": 4,
        "processing_timeout_seconds": 30,
    }
    publication = request(tmp_path, ("video",), settings=settings)
    writer = Writer()
    with adapter(tmp_path, handler, settings=settings, clock=clock) as target:
        prepared = target.prepare(publication, prior=None, checkpoints=writer)
    checkpoint_index = next(
        i
        for i, event in enumerate(writer.events)
        if event[0] == "checkpoint"
        and isinstance(event[1], ArtifactCheckpoint)
        and event[1].kind == "x_media_id"
    )
    assert checkpoint_index > 0
    assert events == ["initialize"] + ["append"] * 6 + ["finalize", "status", "status"]
    assert append_indices == list(range(6))
    assert clock.sleeps == [1.0, 2.0]
    assert next(
        a for a in prepared.artifacts if a.kind == "x_media_id"
    ).expires_at == NOW + timedelta(seconds=60)
    transitions = [
        event.processing_metadata["state"]
        for kind, event in writer.events
        if kind == "transition" and isinstance(event, ArtifactCheckpoint)
    ]
    assert transitions == [
        "appending",
        "appending",
        "appending",
        "appending",
        "appending",
        "appending",
        "finalizing",
        "finalized",
        "pending",
        "succeeded",
    ]


@pytest.mark.parametrize(
    ("processing_info", "expected_classification", "message"),
    [
        ({"state": "failed"}, "permanent", "processing failed"),
        ({"state": "unknown"}, "safe_pre_final", "response is invalid"),
    ],
)
def test_x_processing_failures_have_explicit_retry_classification(
    tmp_path: Path,
    processing_info: dict[str, object],
    expected_classification: str,
    message: str,
) -> None:
    def handler(req: httpx.Request) -> httpx.Response:
        if req.url.path == "/2/users/me":
            return identity_response()
        if req.url.path == "/2/media/upload/initialize":
            return httpx.Response(
                202, json={"data": {"id": "8051", "expires_after_secs": 3600}}
            )
        if req.url.path == "/2/media/upload/8051/append":
            return httpx.Response(204)
        if req.url.path == "/2/media/upload/8051/finalize":
            return httpx.Response(
                200,
                json={
                    "data": {
                        "id": "8051",
                        "processing_info": processing_info,
                    }
                },
            )
        raise AssertionError(req.url)

    settings: dict[str, object] = {
        "chunk_size_bytes": 4,
        "processing_timeout_seconds": 30,
    }
    publication = request(tmp_path, ("video",), settings=settings)
    with adapter(tmp_path, handler, settings=settings) as target:  # noqa: SIM117
        with pytest.raises(XAdapterError, match=message) as caught:
            target.prepare(publication, prior=None, checkpoints=Writer())

    assert caught.value.retry_classification == expected_classification


def test_final_create_ambiguity_is_returned_and_never_repeated(tmp_path: Path) -> None:
    tweet_calls = 0

    def handler(req: httpx.Request) -> httpx.Response:
        nonlocal tweet_calls
        if req.url.path == "/2/users/me":
            return identity_response()
        if req.url.path == "/2/tweets":
            tweet_calls += 1
            return httpx.Response(503)
        raise AssertionError(req.url)

    publication = request(tmp_path, (), text="text only")
    writer = Writer()
    with adapter(tmp_path, handler) as target:
        prepared = target.prepare(publication, prior=None, checkpoints=writer)
        delivery = writer.delivery("final_dispatch_started")
        result = target.commit(prepared, delivery=delivery)
        assert result.outcome == "ambiguous"
        with pytest.raises(AdapterContractError, match="already attempted"):
            target.commit(prepared, delivery=delivery)
    assert tweet_calls == 1


def test_unexpired_media_is_reused_but_new_attempt_reuploads_expired_media(
    tmp_path: Path,
) -> None:
    clock = Clock()
    upload_ids = iter(("7001", "7002"))
    upload_calls = 0

    def handler(req: httpx.Request) -> httpx.Response:
        nonlocal upload_calls
        if req.url.path == "/2/users/me":
            return identity_response()
        if req.url.path == "/2/media/upload":
            upload_calls += 1
            return httpx.Response(
                200,
                json={
                    "data": {
                        "id": next(upload_ids),
                        "expires_after_secs": 3600,
                    }
                },
            )
        raise AssertionError(req.url)

    publication = request(tmp_path, ("image",))
    writer_one = Writer(attempt=1)
    with adapter(tmp_path, handler, clock=clock) as first:
        prepared_one = first.prepare(
            publication,
            prior=None,
            checkpoints=writer_one,
        )
        prepared_reused = first.prepare(
            publication,
            prior=prepared_one,
            checkpoints=writer_one,
        )
    assert upload_calls == 1
    assert prepared_reused.artifacts == prepared_one.artifacts

    clock.now += timedelta(hours=2)
    writer_two = Writer(attempt=2)
    with adapter(tmp_path, handler, clock=clock) as second:
        prepared_two = second.prepare(
            publication,
            prior=prepared_one,
            checkpoints=writer_two,
        )
        clock.now += timedelta(hours=2)
        with pytest.raises(AdapterContractError, match="checkpoint"):
            second.commit(
                prepared_two,
                delivery=writer_two.delivery("final_dispatch_started"),
            )
    assert upload_calls == 2
    second_id = next(
        item.external_id for item in prepared_two.artifacts if item.kind == "x_media_id"
    )
    assert second_id == "7002"


def test_same_attempt_expired_media_is_atomically_replaced_before_final(
    tmp_path: Path,
) -> None:
    clock = Clock()
    ids = iter(("7101", "7102"))

    def handler(req: httpx.Request) -> httpx.Response:
        if req.url.path == "/2/users/me":
            return identity_response()
        if req.url.path == "/2/media/upload":
            return httpx.Response(
                200,
                json={"data": {"id": next(ids), "expires_after_secs": 3600}},
            )
        raise AssertionError(req.url)

    publication = request(tmp_path, ("image",))
    writer = Writer()
    with adapter(tmp_path, handler, clock=clock) as target:
        first = target.prepare(publication, prior=None, checkpoints=writer)
        clock.now += timedelta(hours=2)
        clock.elapsed += 7200
        replaced = target.prepare(publication, prior=first, checkpoints=writer)
    media_record = next(
        item for item in replaced.artifacts if item.kind == "x_media_id"
    )
    assert media_record.external_id == "7102"
    assert any(event[0] == "replace" for event in writer.events)


@pytest.mark.parametrize(
    ("state", "next_segment", "expected_appends", "expects_finalize", "expects_status"),
    [
        ("initialized", 0, [0, 1, 2, 3, 4, 5], True, False),
        ("appending", 3, [3, 4, 5], True, False),
        ("finalizing", 6, [], False, True),
        ("finalized", 6, [], False, True),
        ("pending", 6, [], False, True),
        ("in_progress", 6, [], False, True),
        ("succeeded", 6, [], False, False),
    ],
)
def test_chunked_resume_never_repeats_completed_remote_steps(
    tmp_path: Path,
    state: str,
    next_segment: int,
    expected_appends: list[int],
    expects_finalize: bool,
    expects_status: bool,
) -> None:
    append_indices: list[int] = []
    finalize_calls = 0
    status_calls = 0

    def handler(req: httpx.Request) -> httpx.Response:
        nonlocal finalize_calls, status_calls
        if req.url.path == "/2/users/me":
            return identity_response()
        if req.url.path == "/2/media/upload/8201/append":
            match = re.search(rb'name="segment_index"\r\n\r\n([0-9]+)', req.content)
            assert match is not None
            append_indices.append(int(match.group(1)))
            return httpx.Response(204)
        if req.url.path == "/2/media/upload/8201/finalize":
            finalize_calls += 1
            return httpx.Response(200, json={"data": {"id": "8201"}})
        if req.url.path == "/2/media/upload":
            status_calls += 1
            assert dict(req.url.params) == {"command": "STATUS", "media_id": "8201"}
            return httpx.Response(
                200,
                json={
                    "data": {
                        "id": "8201",
                        "processing_info": {"state": "succeeded"},
                    }
                },
            )
        raise AssertionError(req.url)

    settings: dict[str, object] = {"chunk_size_bytes": 4}
    publication = request(tmp_path, ("video",), settings=settings)
    item = publication.media.items[0]
    writer = Writer()
    staged = writer.checkpoint_artifact(
        ArtifactCheckpoint(
            "staged_private",
            0,
            relative_path=item.private.relative_path.as_posix(),
            sha256=item.private.sha256,
        )
    )
    remote = writer.checkpoint_artifact(
        ArtifactCheckpoint(
            "x_media_id",
            0,
            external_id="8201",
            expires_at=NOW + timedelta(hours=1),
            processing_metadata={
                "state": state,
                "next_segment_index": next_segment,
                "source_sha256": item.private.sha256,
                "media_type": item.private.mime_type,
            },
        )
    )
    prior = PreparedPublication(
        publication,
        1,
        tuple(sorted((staged, remote), key=lambda a: (a.kind, a.ordinal))),
    )
    with adapter(tmp_path, handler, settings=settings) as target:
        prepared = target.prepare(publication, prior=prior, checkpoints=writer)
    assert append_indices == expected_appends
    assert finalize_calls == int(expects_finalize)
    assert status_calls == int(expects_status)
    final_remote = next(
        item for item in prepared.artifacts if item.kind == "x_media_id"
    )
    assert final_remote.processing_metadata["state"] == "succeeded"


def test_resume_before_finalize_status_404_performs_finalize_once(
    tmp_path: Path,
) -> None:
    status_calls = 0
    finalize_calls = 0

    def handler(req: httpx.Request) -> httpx.Response:
        nonlocal status_calls, finalize_calls
        if req.url.path == "/2/users/me":
            return identity_response()
        if req.url.path == "/2/media/upload":
            status_calls += 1
            return httpx.Response(404)
        if req.url.path == "/2/media/upload/8251/finalize":
            finalize_calls += 1
            return httpx.Response(200, json={"data": {"id": "8251"}})
        raise AssertionError(req.url)

    settings: dict[str, object] = {"chunk_size_bytes": 4}
    publication = request(tmp_path, ("video",), settings=settings)
    item = publication.media.items[0]
    writer = Writer()
    staged = writer.checkpoint_artifact(
        ArtifactCheckpoint(
            "staged_private",
            0,
            relative_path=item.private.relative_path.as_posix(),
            sha256=item.private.sha256,
        )
    )
    remote = writer.checkpoint_artifact(
        ArtifactCheckpoint(
            "x_media_id",
            0,
            external_id="8251",
            expires_at=NOW + timedelta(hours=1),
            processing_metadata={
                "state": "finalizing",
                "next_segment_index": 6,
                "source_sha256": item.private.sha256,
                "media_type": item.private.mime_type,
            },
        )
    )
    prior = PreparedPublication(
        publication,
        1,
        tuple(sorted((staged, remote), key=lambda value: (value.kind, value.ordinal))),
    )
    with adapter(tmp_path, handler, settings=settings) as target:
        prepared = target.prepare(publication, prior=prior, checkpoints=writer)
    assert status_calls == 1
    assert finalize_calls == 1
    final_remote = next(
        artifact for artifact in prepared.artifacts if artifact.kind == "x_media_id"
    )
    assert final_remote.processing_metadata["state"] == "succeeded"


def test_real_state_writer_accepts_x_checkpoint_contract(tmp_path: Path) -> None:
    timer = Clock()
    settings: dict[str, object] = {
        "chunk_size_bytes": 4,
        "processing_timeout_seconds": 30,
    }
    repository = StateRepository(tmp_path / "state.sqlite3", clock=timer)
    repository.register_profile(
        "alpha",
        tmp_path / "accounts/alpha",
        (
            ProfileTargetSnapshot(
                "x",
                "12345",
                "expected_user",
                "POST_PULSAR_X_ALPHA_USER_ACCESS_TOKEN",
                settings,
            ),
        ),
        config_hash="b" * 64,
    )
    bundle_key = repository.add_bundle(
        profile_id="alpha",
        bundle_id="cat",
        fingerprint=FINGERPRINT,
        source_bucket="QUEUE",
        files=(
            BundleFileSnapshot(
                "cat_1.mp4", "media", 1, "video", "video/mp4", 24, "c" * 64
            ),
        ),
        targets=(
            TargetSnapshot(
                "x",
                "12345",
                "expected_user",
                "POST_PULSAR_X_ALPHA_USER_ACCESS_TOKEN",
                "2",
                1,
                settings,
            ),
        ),
    )
    delivery = repository.claim_delivery(bundle_key, "x", "claim-x-real")
    writer = RepositoryWriter(
        repository,
        bundle_key=bundle_key,
        claim_token="claim-x-real",
        attempt_count=delivery.attempt_count,
    )

    def handler(req: httpx.Request) -> httpx.Response:
        if req.url.path == "/2/users/me":
            return identity_response()
        if req.url.path == "/2/media/upload/initialize":
            return httpx.Response(
                202, json={"data": {"id": "7301", "expires_after_secs": 3600}}
            )
        if req.url.path == "/2/media/upload/7301/append":
            return httpx.Response(204)
        if req.url.path == "/2/media/upload/7301/finalize":
            return httpx.Response(
                200, json={"data": {"id": "7301", "expires_after_secs": 120}}
            )
        raise AssertionError(req.url)

    publication = request(tmp_path, ("video",), settings=settings)
    with adapter(tmp_path, handler, settings=settings, clock=timer) as target:
        prepared = target.prepare(publication, prior=None, checkpoints=writer)
    assert [item.kind for item in prepared.artifacts] == [
        "staged_private",
        "x_media_id",
    ]
    remote = next(item for item in prepared.artifacts if item.kind == "x_media_id")
    assert remote.expires_at == NOW + timedelta(seconds=60)
    assert remote.processing_metadata["state"] == "succeeded"
    assert (
        remote.processing_metadata["processing_deadline"]
        == "2026-09-05T12:00:30.000000Z"
    )
    assert repository.list_delivery_artifacts(bundle_key, "x")[-1] == remote
    assert repository.get_delivery(bundle_key, "x").phase == "ready"


@pytest.mark.parametrize(
    ("processing_state", "next_segment_index"),
    [("pending", 6), ("succeeded", 5)],
)
def test_commit_rejects_unfinished_chunked_artifact_from_real_state(
    tmp_path: Path, processing_state: str, next_segment_index: int
) -> None:
    timer = Clock()
    repository = StateRepository(tmp_path / "pending.sqlite3", clock=timer)
    repository.register_profile(
        "alpha",
        tmp_path / "accounts/alpha",
        (
            ProfileTargetSnapshot(
                "x",
                "12345",
                "expected_user",
                "POST_PULSAR_X_ALPHA_USER_ACCESS_TOKEN",
                {"chunk_size_bytes": 4},
            ),
        ),
        config_hash="b" * 64,
    )
    bundle_key = repository.add_bundle(
        profile_id="alpha",
        bundle_id="cat",
        fingerprint=FINGERPRINT,
        source_bucket="QUEUE",
        files=(
            BundleFileSnapshot(
                "cat_1.mp4", "media", 1, "video", "video/mp4", 24, "c" * 64
            ),
        ),
        targets=(
            TargetSnapshot(
                "x",
                "12345",
                "expected_user",
                "POST_PULSAR_X_ALPHA_USER_ACCESS_TOKEN",
                "2",
                1,
                {"chunk_size_bytes": 4},
            ),
        ),
    )
    claimed = repository.claim_delivery(bundle_key, "x", "claim-x-pending")
    writer = RepositoryWriter(
        repository,
        bundle_key=bundle_key,
        claim_token="claim-x-pending",
        attempt_count=claimed.attempt_count,
    )
    processing = writer.advance_phase("processing")
    publication = request(tmp_path, ("video",), settings={"chunk_size_bytes": 4})
    item = publication.media.items[0]
    staged = writer.checkpoint_artifact(
        ArtifactCheckpoint(
            "staged_private",
            0,
            relative_path=item.private.relative_path.as_posix(),
            sha256=item.private.sha256,
        )
    )
    pending = writer.checkpoint_artifact(
        ArtifactCheckpoint(
            "x_media_id",
            0,
            external_id="7351",
            expires_at=NOW + timedelta(hours=1),
            processing_metadata={
                "state": processing_state,
                "next_segment_index": next_segment_index,
                "source_sha256": item.private.sha256,
                "media_type": item.private.mime_type,
            },
        )
    )
    writer.advance_phase("ready")
    final = writer.advance_phase("final_dispatch_started")
    prepared = PreparedPublication(
        publication,
        processing.attempt_count,
        tuple(sorted((pending, staged), key=lambda item: (item.kind, item.ordinal))),
    )
    calls: list[str] = []

    def handler(req: httpx.Request) -> httpx.Response:
        calls.append(req.url.path)
        if req.url.path == "/2/users/me":
            return identity_response()
        raise AssertionError("final create must not run for unfinished media")

    with (
        adapter(
            tmp_path, handler, settings={"chunk_size_bytes": 4}, clock=timer
        ) as target,
        pytest.raises(AdapterContractError, match="not ready"),
    ):
        target.commit(prepared, delivery=final)
    assert calls == ["/2/users/me"]


def test_processing_deadline_is_checkpointed_and_not_reset_on_resume(
    tmp_path: Path,
) -> None:
    timer = Clock()
    settings: dict[str, object] = {
        "chunk_size_bytes": 4,
        "processing_timeout_seconds": 5,
    }
    publication = request(tmp_path, ("video",), settings=settings)
    writer = Writer()
    status_calls = 0

    def handler(req: httpx.Request) -> httpx.Response:
        nonlocal status_calls
        if req.url.path == "/2/users/me":
            return identity_response()
        if req.url.path == "/2/media/upload/initialize":
            return httpx.Response(
                202, json={"data": {"id": "8361", "expires_after_secs": 3600}}
            )
        if req.url.path == "/2/media/upload/8361/append":
            return httpx.Response(204)
        if req.url.path == "/2/media/upload/8361/finalize":
            return httpx.Response(
                200,
                json={
                    "data": {
                        "id": "8361",
                        "processing_info": {
                            "state": "pending",
                            "check_after_secs": 60,
                        },
                    }
                },
            )
        if req.url.path == "/2/media/upload":
            status_calls += 1
            return httpx.Response(
                200,
                json={
                    "data": {
                        "id": "8361",
                        "processing_info": {"state": "in_progress"},
                    }
                },
            )
        raise AssertionError(req.url)

    with adapter(  # noqa: SIM117 - ordered cleanup
        tmp_path, handler, settings=settings, clock=timer
    ) as first:
        with pytest.raises(XAdapterError, match="timed out") as first_error:
            first.prepare(publication, prior=None, checkpoints=writer)
    assert first_error.value.retry_classification == "safe_pre_final"
    remote = writer.records[("x_media_id", 0)]
    assert (
        remote.processing_metadata["processing_deadline"]
        == "2026-09-05T12:00:05.000000Z"
    )
    prior = PreparedPublication(
        publication,
        1,
        tuple(
            sorted(writer.records.values(), key=lambda item: (item.kind, item.ordinal))
        ),
    )
    with adapter(  # noqa: SIM117 - ordered cleanup
        tmp_path, handler, settings=settings, clock=timer
    ) as resumed:
        with pytest.raises(XAdapterError, match="timed out") as resumed_error:
            resumed.prepare(publication, prior=prior, checkpoints=Writer())
    assert resumed_error.value.retry_classification == "safe_pre_final"
    assert status_calls == 0


def test_processing_retry_after_cannot_overrun_monotonic_deadline(
    tmp_path: Path,
) -> None:
    timer = Clock()
    status_calls = 0

    def handler(req: httpx.Request) -> httpx.Response:
        nonlocal status_calls
        if req.url.path == "/2/users/me":
            return identity_response()
        if req.url.path == "/2/media/upload/initialize":
            return httpx.Response(
                202, json={"data": {"id": "8401", "expires_after_secs": 3600}}
            )
        if req.url.path == "/2/media/upload/8401/append":
            return httpx.Response(204)
        if req.url.path == "/2/media/upload/8401/finalize":
            return httpx.Response(
                200,
                json={
                    "data": {
                        "id": "8401",
                        "processing_info": {"state": "pending", "check_after_secs": 1},
                    }
                },
            )
        if req.url.path == "/2/media/upload":
            status_calls += 1
            if status_calls == 1:
                return httpx.Response(429, headers={"Retry-After": "60"})
            return httpx.Response(
                200,
                json={
                    "data": {
                        "id": "8401",
                        "processing_info": {"state": "succeeded"},
                    }
                },
            )
        raise AssertionError(req.url)

    settings: dict[str, object] = {
        "chunk_size_bytes": 4,
        "processing_timeout_seconds": 5,
    }
    publication = request(tmp_path, ("video",), settings=settings)
    with adapter(  # noqa: SIM117 - ordered cleanup
        tmp_path, handler, settings=settings, clock=timer
    ) as target:
        with pytest.raises(XAdapterError, match="timed out") as caught:
            target.prepare(publication, prior=None, checkpoints=Writer())
    assert caught.value.retry_classification == "safe_pre_final"
    assert timer.elapsed == 5
    assert status_calls == 1
