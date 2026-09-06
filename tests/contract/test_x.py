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
from post_pulsar.platforms.x import XAdapter
from post_pulsar.state import (
    ArtifactRecord,
    DeliveryPhase,
    DeliveryRecord,
    TargetSnapshot,
)


NOW = datetime(2026, 9, 5, 12, tzinfo=UTC)
FINGERPRINT = "a" * 64


class Clock:
    def __init__(self) -> None:
        self.now = NOW
        self.sleeps: list[float] = []

    def __call__(self) -> datetime:
        return self.now

    def sleep(self, seconds: float) -> None:
        self.sleeps.append(seconds)
        self.now += timedelta(seconds=seconds)


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
    with adapter(tmp_path, handler) as target:
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
            append_indices.append(int(match.group(1)))
            return httpx.Response(204)
        if req.url.path == "/2/media/upload/8001/finalize":
            events.append("finalize")
            return httpx.Response(
                200,
                json={
                    "data": {
                        "id": "8001",
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
    ).expires_at == NOW + timedelta(seconds=3540)


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
