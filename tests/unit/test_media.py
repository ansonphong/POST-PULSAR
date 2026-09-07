"""Safe media inspection, staging, and public verification tests."""

from __future__ import annotations

import hashlib
import json
import os
import socket
import subprocess
from collections.abc import Callable
from contextlib import contextmanager
from dataclasses import FrozenInstanceError, replace
from io import BytesIO
from pathlib import Path
from typing import Any

import httpx
import pytest
from PIL import Image
from pytest_socket import SocketBlockedError

import post_pulsar.media as media_module
from post_pulsar.config import InstagramSettings
from post_pulsar.content import PublishableBundle, scan_account_root
from post_pulsar.media import (
    MediaSafetyError,
    StagedMedia,
    cleanup_checkpointed_staging,
    cleanup_staged_media,
    open_verified_private_media,
    prepare_bundle_media,
    verify_public_media_url,
)


class MockPinnedTransportFactory:
    def __init__(self, handler: Callable[[httpx.Request], httpx.Response]) -> None:
        self.handler = handler
        self.calls: list[tuple[str, str, int]] = []

    def __call__(
        self, connect_ip: str, server_hostname: str, port: int
    ) -> httpx.BaseTransport:
        self.calls.append((connect_ip, server_hostname, port))
        return httpx.MockTransport(self.handler)


def _image_bytes(
    image_format: str = "PNG",
    *,
    size: tuple[int, int] = (640, 640),
    mode: str = "RGB",
) -> bytes:
    output = BytesIO()
    color: tuple[int, ...] = (10, 20, 30, 128) if mode == "RGBA" else (10, 20, 30)
    Image.new(mode, size, color).save(output, format=image_format)
    return output.getvalue()


def _animated_gif_bytes() -> bytes:
    output = BytesIO()
    frames = [Image.new("RGB", (64, 64), color) for color in ("red", "blue")]
    frames[0].save(
        output,
        format="GIF",
        save_all=True,
        append_images=frames[1:],
        duration=100,
        loop=0,
    )
    return output.getvalue()


def _bundle(
    tmp_path: Path, names: dict[str, bytes], bucket: str = "QUEUE"
) -> PublishableBundle:
    directory = tmp_path / "account" / bucket / "post"
    directory.mkdir(parents=True)
    for name, data in names.items():
        (directory / name).write_bytes(data)
    (directory / ".ready").write_bytes(b"")
    scan = scan_account_root(tmp_path / "account")
    assert scan.issues == ()
    return scan.bundles[0]


def _instagram(tmp_path: Path) -> InstagramSettings:
    return InstagramSettings(
        enabled=True,
        expected_remote_user_id="12345",
        expected_username="ansonphong",
        token_env_var="POST_PULSAR_INSTAGRAM_ANSONPHONG_ACCESS_TOKEN",
        media_directory=tmp_path / "public",
        media_base_url="https://media.example.test/post-pulsar/",
        request_timeout_seconds=5.0,
        processing_timeout_seconds=60.0,
    )


def _video_probe(**overrides: object) -> dict[str, object]:
    video: dict[str, object] = {
        "codec_type": "video",
        "codec_name": "h264",
        "profile": "High",
        "pix_fmt": "yuv420p",
        "width": 1024,
        "height": 1024,
        "r_frame_rate": "30/1",
        "avg_frame_rate": "30/1",
        "bit_rate": "1000000",
    }
    audio: dict[str, object] = {
        "codec_type": "audio",
        "codec_name": "aac",
        "profile": "LC",
        "sample_rate": "48000",
        "bit_rate": "128000",
    }
    format_data: dict[str, object] = {
        "format_name": "mov,mp4,m4a,3gp,3g2,mj2",
        "duration": "10.0",
        "size": "24",
    }
    for key, value in overrides.items():
        if key.startswith("video_"):
            video[key.removeprefix("video_")] = value
        elif key.startswith("audio_"):
            audio[key.removeprefix("audio_")] = value
        else:
            format_data[key] = value
    return {"format": format_data, "streams": [video, audio]}


def _runner_for(document: object) -> Any:
    def runner(argv: list[str], **kwargs: object) -> subprocess.CompletedProcess[str]:
        assert argv[0] == "ffprobe"
        assert argv[-1].endswith(".mp4")
        assert kwargs["shell"] is False
        assert kwargs["timeout"] == 7.0
        assert kwargs["text"] is True
        return subprocess.CompletedProcess(argv, 0, json.dumps(document), "")

    return runner


def _public_descriptor(tmp_path: Path, body: bytes) -> StagedMedia:
    root = tmp_path / "public"
    root.mkdir()
    fingerprint = "f" * 64
    digest = hashlib.sha256(body).hexdigest()
    path = root / f"ansonphong-QUEUE-{fingerprint}-01-{digest}.jpg"
    path.write_bytes(body)
    return StagedMedia(
        profile_id="ansonphong",
        source_bucket="QUEUE",
        bundle_id="post",
        bundle_fingerprint=fingerprint,
        source_name="post.jpg",
        staging_kind="instagram_public",
        relative_path=Path(path.name),
        sha256=digest,
        mime_type="image/jpeg",
        size_bytes=len(body),
        cleanup_policy="known_terminal_hash_match",
        public_url=f"https://media.example.test/post-pulsar/{path.name}",
        source_sha256=digest,
    )


def _public_resolver(
    host: str, port: int, **kwargs: object
) -> list[tuple[object, ...]]:
    assert host == "media.example.test"
    assert port == 443
    assert kwargs["type"] == socket.SOCK_STREAM
    return [(socket.AF_INET, socket.SOCK_STREAM, 6, "", ("93.184.216.34", port))]


def test_socket_denial_sentinel_has_no_host_exception() -> None:
    with pytest.raises(SocketBlockedError):
        socket.create_connection(("media.example.test", 443), timeout=0.01)


def test_x_image_is_verified_and_staged_from_the_admitted_stream(
    tmp_path: Path,
) -> None:
    original = _image_bytes("PNG")
    bundle = _bundle(tmp_path, {"post.png": original})

    prepared = prepare_bundle_media(
        bundle,
        profile_id="ansonphong",
        targets=("x",),
        private_staging_directory=tmp_path / "private",
    )

    assert prepared.profile_id == "ansonphong"
    assert prepared.source_bucket == "QUEUE"
    assert prepared.bundle_fingerprint == bundle.fingerprint
    assert prepared.warnings == ()
    assert len(prepared.items) == 1
    item = prepared.items[0]
    assert item.metadata.format_name == "PNG"
    assert (item.metadata.width, item.metadata.height) == (640, 640)
    assert item.private.mime_type == "image/png"
    with open_verified_private_media(
        item.private,
        tmp_path / "private",
        expected_profile_id="ansonphong",
        expected_bucket="QUEUE",
    ) as verified:
        assert verified.stream.read() == original
        assert verified.sha256 == item.private.sha256
        assert verified.size_bytes == len(original)
    assert item.public is None
    assert item.private.relative_path.parts[:2] == ("ansonphong", "QUEUE")
    private_path = tmp_path / "private" / item.private.relative_path
    assert private_path.stat().st_mode & 0o222 == 0
    with pytest.raises(MediaSafetyError, match="verified stream"):
        item.private.absolute_path(tmp_path / "private")
    with pytest.raises(FrozenInstanceError):
        item.private.sha256 = "changed"  # type: ignore[misc]


def test_open_descriptor_is_not_reused_across_profiles(tmp_path: Path) -> None:
    bundle = _bundle(tmp_path, {"post.jpg": _image_bytes("JPEG")})
    first = prepare_bundle_media(
        bundle,
        profile_id="ansonphong",
        targets=("x",),
        private_staging_directory=tmp_path / "private",
    )
    second = prepare_bundle_media(
        bundle,
        profile_id="360hextile",
        targets=("x",),
        private_staging_directory=tmp_path / "private",
    )

    assert first.items[0].private.relative_path != second.items[0].private.relative_path
    with pytest.raises(  # noqa: SIM117 - error can be raised during inner cleanup
        MediaSafetyError, match="profile"
    ):
        with open_verified_private_media(
            first.items[0].private,
            tmp_path / "private",
            expected_profile_id="360hextile",
        ) as verified:
            verified.stream.read(0)


@pytest.mark.parametrize("replacement", ["regular", "symlink"])
def test_verified_private_stream_rejects_path_replacement_before_open(
    tmp_path: Path, replacement: str
) -> None:
    original = _image_bytes("PNG")
    prepared = prepare_bundle_media(
        _bundle(tmp_path, {"post.png": original}),
        profile_id="ansonphong",
        targets=("x",),
        private_staging_directory=tmp_path / "private",
    )
    staged = prepared.items[0].private
    path = tmp_path / "private" / staged.relative_path
    path.unlink()
    if replacement == "regular":
        path.write_bytes(_image_bytes("PNG", size=(700, 700)))
    else:
        outside = tmp_path / "outside.png"
        outside.write_bytes(original)
        path.symlink_to(outside)

    with (
        pytest.raises(
            MediaSafetyError, match=r"opened safely|identity or hash|unsafe component"
        ),
        open_verified_private_media(staged, tmp_path / "private") as verified,
    ):
        verified.stream.read()


def test_verified_private_stream_keeps_open_identity_across_path_swap(
    tmp_path: Path,
) -> None:
    original = _image_bytes("PNG")
    prepared = prepare_bundle_media(
        _bundle(tmp_path, {"post.png": original}),
        profile_id="ansonphong",
        targets=("x",),
        private_staging_directory=tmp_path / "private",
    )
    staged = prepared.items[0].private
    path = tmp_path / "private" / staged.relative_path

    with pytest.raises(  # noqa: SIM117 - error is raised during inner cleanup
        MediaSafetyError, match="consumed completely and unchanged"
    ):
        with open_verified_private_media(staged, tmp_path / "private") as verified:
            captured = path.with_name("captured-original.png")
            path.rename(captured)
            path.write_bytes(_image_bytes("PNG", size=(700, 700)))
            assert verified.stream.read() == original


def test_verified_private_stream_rejects_incomplete_consumption(tmp_path: Path) -> None:
    original = _image_bytes("PNG")
    prepared = prepare_bundle_media(
        _bundle(tmp_path, {"post.png": original}),
        profile_id="ansonphong",
        targets=("x",),
        private_staging_directory=tmp_path / "private",
    )

    with pytest.raises(  # noqa: SIM117 - error is raised during inner cleanup
        MediaSafetyError, match="consumed completely"
    ):
        with open_verified_private_media(
            prepared.items[0].private, tmp_path / "private"
        ) as verified:
            assert verified.stream.read(16) == original[:16]


def test_verified_private_stream_detects_same_inode_rewrite_with_restored_mtime(
    tmp_path: Path,
) -> None:
    original = _image_bytes("PNG")
    prepared = prepare_bundle_media(
        _bundle(tmp_path, {"post.png": original}),
        profile_id="ansonphong",
        targets=("x",),
        private_staging_directory=tmp_path / "private",
    )
    staged = prepared.items[0].private
    path = tmp_path / "private" / staged.relative_path
    timestamps = path.stat()

    with pytest.raises(  # noqa: SIM117 - error is raised during inner cleanup
        MediaSafetyError, match="consumed completely and unchanged"
    ):
        with open_verified_private_media(staged, tmp_path / "private") as verified:
            assert verified.stream.read() == original
            path.chmod(0o600)
            with path.open("r+b") as mutable:
                mutable.seek(0)
                mutable.write(b"X")
                mutable.flush()
                os.fsync(mutable.fileno())
            os.utime(path, ns=(timestamps.st_atime_ns, timestamps.st_mtime_ns))


@pytest.mark.parametrize("replacement", ["regular", "symlink"])
def test_replace_or_symlink_swap_fails_closed(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, replacement: str
) -> None:
    original = _image_bytes("PNG")
    changed = _image_bytes("PNG", size=(700, 700))
    bundle = _bundle(tmp_path, {"post.png": original})
    source_path = bundle.content.images[0]
    real_copy = media_module._copy_stream

    def swap_then_copy(source: Any, destination: Any, digest: Any) -> int:
        moved = source_path.with_suffix(".original")
        source_path.rename(moved)
        if replacement == "regular":
            source_path.write_bytes(changed)
        else:
            outside = tmp_path / "outside.png"
            outside.write_bytes(changed)
            source_path.symlink_to(outside)
        return real_copy(source, destination, digest)

    monkeypatch.setattr(media_module, "_copy_stream", swap_then_copy)

    with pytest.raises(MediaSafetyError, match="changed"):
        prepare_bundle_media(
            bundle,
            profile_id="ansonphong",
            targets=("x",),
            private_staging_directory=tmp_path / "private",
        )

    assert not tuple((tmp_path / "private").rglob("*.*"))


def test_symlink_source_is_rejected_without_leaking_path(tmp_path: Path) -> None:
    bundle = _bundle(tmp_path, {"post.jpg": _image_bytes("JPEG")})
    source = bundle.content.images[0]
    outside = tmp_path / "secret-name.jpg"
    outside.write_bytes(_image_bytes("JPEG"))
    source.unlink()
    source.symlink_to(outside)

    with pytest.raises(MediaSafetyError) as caught:
        prepare_bundle_media(
            bundle,
            profile_id="ansonphong",
            targets=("x",),
            private_staging_directory=tmp_path / "private",
        )

    assert "secret-name" not in str(caught.value)
    assert str(source) not in str(caught.value)


def test_media_changed_after_admission_cannot_keep_old_bundle_fingerprint(
    tmp_path: Path,
) -> None:
    bundle = _bundle(tmp_path, {"post.png": _image_bytes("PNG")})
    bundle.content.images[0].write_bytes(_image_bytes("PNG", size=(700, 700)))

    with pytest.raises(MediaSafetyError, match="fingerprint"):
        prepare_bundle_media(
            bundle,
            profile_id="ansonphong",
            targets=("x",),
            private_staging_directory=tmp_path / "private",
        )


def test_caption_changed_after_admission_invalidates_preparation(
    tmp_path: Path,
) -> None:
    bundle = _bundle(
        tmp_path,
        {"post.jpg": _image_bytes("JPEG"), "post.txt": b"original caption"},
    )
    caption = next(
        member for member in bundle.content.members if member.suffix == ".txt"
    )
    caption.write_bytes(b"changed caption")

    with pytest.raises(MediaSafetyError, match="fingerprint"):
        prepare_bundle_media(
            bundle,
            profile_id="ansonphong",
            targets=("x",),
            private_staging_directory=tmp_path / "private",
        )


def test_instagram_carousel_is_normalized_to_common_metadata_free_jpegs(
    tmp_path: Path,
) -> None:
    bundle = _bundle(
        tmp_path,
        {
            "post-1.png": _image_bytes("PNG", size=(500, 900), mode="RGBA"),
            "post-2.jpg": _image_bytes("JPEG", size=(1600, 800)),
            "post-alt.txt": b"shared alt text",
        },
    )

    prepared = prepare_bundle_media(
        bundle,
        profile_id="ansonphong",
        targets=("instagram",),
        private_staging_directory=tmp_path / "private",
        instagram=_instagram(tmp_path),
    )

    assert [warning.code for warning in prepared.warnings] == [
        "instagram_alt_text_unsupported",
        "instagram_image_normalized",
        "instagram_image_normalized",
    ]
    public = [item.public for item in prepared.items]
    assert all(item is not None for item in public)
    dimensions: list[tuple[int, int]] = []
    for descriptor in public:
        assert descriptor is not None
        assert descriptor.relative_path.parent == Path(".")
        assert descriptor.public_url is not None
        assert descriptor.public_url.endswith(descriptor.relative_path.name)
        with Image.open(descriptor.absolute_path(tmp_path / "public")) as image:
            assert image.format == "JPEG"
            assert "exif" not in image.info
            dimensions.append(image.size)
            assert 320 <= image.width <= 1440
    for item in prepared.items:
        assert item.public is not None
        assert item.public.source_sha256 == item.private.sha256
    assert dimensions[0] == dimensions[1]
    width, height = dimensions[0]
    assert 4 * height <= 5 * width
    assert 100 * width <= 191 * height


@pytest.mark.parametrize(
    ("targets", "names", "match"),
    [
        (
            ("x",),
            {f"post-{index}.jpg": _image_bytes("JPEG") for index in range(1, 6)},
            "four",
        ),
        (("instagram",), {"post.gif": _image_bytes("GIF")}, "GIF"),
        (
            ("instagram",),
            {f"post-{index}.jpg": _image_bytes("JPEG") for index in range(1, 12)},
            "ten",
        ),
    ],
)
def test_platform_image_count_and_format_limits_are_enforced(
    tmp_path: Path,
    targets: tuple[str, ...],
    names: dict[str, bytes],
    match: str,
) -> None:
    bundle = _bundle(tmp_path, names)

    with pytest.raises(MediaSafetyError, match=match):
        prepare_bundle_media(
            bundle,
            profile_id="ansonphong",
            targets=targets,
            private_staging_directory=tmp_path / "private",
            instagram=_instagram(tmp_path) if "instagram" in targets else None,
        )


def test_corrupt_or_mislabelled_image_is_rejected(tmp_path: Path) -> None:
    bundle = _bundle(tmp_path, {"post.jpg": b"not an image"})

    with pytest.raises(MediaSafetyError, match="decode"):
        prepare_bundle_media(
            bundle,
            profile_id="ansonphong",
            targets=("x",),
            private_staging_directory=tmp_path / "private",
        )


def test_code_owned_image_size_and_animated_gif_limits_are_enforced(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    image_bundle = _bundle(tmp_path / "image", {"post.jpg": _image_bytes("JPEG")})
    monkeypatch.setattr(media_module, "_X_IMAGE_MAX_BYTES", 1)
    with pytest.raises(MediaSafetyError, match="5 MiB"):
        prepare_bundle_media(
            image_bundle,
            profile_id="ansonphong",
            targets=("x",),
            private_staging_directory=tmp_path / "private-image",
        )

    gif_bundle = _bundle(tmp_path / "gif", {"post.gif": _animated_gif_bytes()})
    monkeypatch.setattr(media_module, "_X_IMAGE_MAX_BYTES", 5 * 1024 * 1024)
    monkeypatch.setattr(media_module, "_X_GIF_MAX_FRAMES", 1)
    with pytest.raises(MediaSafetyError, match="frame count"):
        prepare_bundle_media(
            gif_bundle,
            profile_id="ansonphong",
            targets=("x",),
            private_staging_directory=tmp_path / "private-gif",
        )


def test_x_animated_gif_must_be_the_only_media_item(tmp_path: Path) -> None:
    bundle = _bundle(
        tmp_path,
        {"post-1.gif": _animated_gif_bytes(), "post-2.jpg": _image_bytes("JPEG")},
    )

    with pytest.raises(MediaSafetyError, match="sole media item"):
        prepare_bundle_media(
            bundle,
            profile_id="ansonphong",
            targets=("x",),
            private_staging_directory=tmp_path / "private",
        )


def test_staging_root_symlink_is_rejected(tmp_path: Path) -> None:
    bundle = _bundle(tmp_path, {"post.jpg": _image_bytes("JPEG")})
    real_root = tmp_path / "real-staging"
    real_root.mkdir()
    staging_link = tmp_path / "staging-link"
    staging_link.symlink_to(real_root, target_is_directory=True)

    with pytest.raises(MediaSafetyError, match="unsafe component"):
        prepare_bundle_media(
            bundle,
            profile_id="ansonphong",
            targets=("x",),
            private_staging_directory=staging_link,
        )


def test_staging_generation_uses_held_parent_not_swapped_path(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    bundle = _bundle(tmp_path, {"post.jpg": _image_bytes("JPEG")})
    private_root = tmp_path / "private"
    destination = private_root / "ansonphong" / "QUEUE" / bundle.fingerprint
    held = destination.with_name(f"{destination.name}-held")
    outside = tmp_path / "outside-stage"
    outside.mkdir()
    real_open = media_module._open_directory_nofollow
    swapped = False

    @contextmanager
    def swap_after_open(path: Path) -> Any:
        nonlocal swapped
        with real_open(path) as descriptor:
            if path == destination and not swapped:
                swapped = True
                path.rename(held)
                path.symlink_to(outside, target_is_directory=True)
            yield descriptor

    monkeypatch.setattr(media_module, "_open_directory_nofollow", swap_after_open)

    with pytest.raises(MediaSafetyError):
        prepare_bundle_media(
            bundle,
            profile_id="ansonphong",
            targets=("x",),
            private_staging_directory=private_root,
        )

    assert tuple(outside.iterdir()) == ()
    assert any(held.iterdir())


def test_ffprobe_uses_bounded_argv_and_video_metadata_is_enforced(
    tmp_path: Path,
) -> None:
    body = b"\x00\x00\x00\x18ftypmp42" + b"0" * 12
    bundle = _bundle(tmp_path, {"post.mp4": body}, bucket="REELS")

    prepared = prepare_bundle_media(
        bundle,
        profile_id="ansonphong",
        targets=("x", "instagram"),
        private_staging_directory=tmp_path / "private",
        instagram=_instagram(tmp_path),
        ffprobe_timeout_seconds=7.0,
        process_runner=_runner_for(_video_probe()),
    )

    item = prepared.items[0]
    assert item.metadata.kind == "video"
    assert item.metadata.codec == "h264"
    assert item.metadata.audio_codec == "aac"
    assert item.metadata.duration_seconds == 10.0
    assert item.public is not None
    assert item.public.mime_type == "video/mp4"
    assert item.public.sha256 == item.private.sha256
    assert item.public.source_sha256 == item.private.sha256
    assert item.public.absolute_path(tmp_path / "public").read_bytes() == body


def test_instagram_normalization_never_reopens_swapped_private_path(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    bundle = _bundle(tmp_path, {"post.jpg": _image_bytes("JPEG")})
    private_root = tmp_path / "private"
    private_path: Path | None = None
    real_normalize = media_module._normalized_jpeg

    def swap_during_normalize(source_stream: Any, dimensions: tuple[int, int]) -> bytes:
        assert private_path is not None
        captured = private_path.with_name("captured-private.jpg")
        private_path.rename(captured)
        private_path.write_bytes(_image_bytes("JPEG", size=(700, 700)))
        return real_normalize(source_stream, dimensions)

    def remember_private(result: Any, root: Path) -> Any:
        nonlocal private_path
        private_path = root / result.descriptor.relative_path
        return result

    real_stage_source = media_module._stage_source

    def stage_and_remember(*args: Any, **kwargs: Any) -> Any:
        result = real_stage_source(*args, **kwargs)
        if kwargs.get("staging_kind") == "private":
            remember_private(result, private_root)
        return result

    monkeypatch.setattr(media_module, "_stage_source", stage_and_remember)
    monkeypatch.setattr(media_module, "_normalized_jpeg", swap_during_normalize)

    with pytest.raises(MediaSafetyError, match="consumed completely and unchanged"):
        prepare_bundle_media(
            bundle,
            profile_id="ansonphong",
            targets=("instagram",),
            private_staging_directory=private_root,
            instagram=_instagram(tmp_path),
        )

    assert not (tmp_path / "public").exists()


def test_public_video_copy_uses_held_verified_private_stream(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    body = b"\x00\x00\x00\x18ftypmp42" + b"0" * 12
    bundle = _bundle(tmp_path, {"post.mp4": body}, bucket="REELS")
    private_root = tmp_path / "private"
    real_copy = media_module._copy_stream
    copy_count = 0

    def swap_before_public_copy(source: Any, destination: Any, digest: Any) -> int:
        nonlocal copy_count
        copy_count += 1
        if copy_count == 2:
            candidates = tuple(private_root.rglob("*.mp4"))
            assert len(candidates) == 1
            private_path = candidates[0]
            private_path.rename(private_path.with_name("captured-private.mp4"))
            private_path.write_bytes(b"\x00\x00\x00\x18ftypmp42changed")
        return real_copy(source, destination, digest)

    monkeypatch.setattr(media_module, "_copy_stream", swap_before_public_copy)

    with pytest.raises(MediaSafetyError, match="consumed completely and unchanged"):
        prepare_bundle_media(
            bundle,
            profile_id="ansonphong",
            targets=("instagram",),
            private_staging_directory=private_root,
            instagram=_instagram(tmp_path),
            process_runner=_runner_for(_video_probe()),
            ffprobe_timeout_seconds=7.0,
        )

    public_root = tmp_path / "public"
    assert public_root.exists()
    assert tuple(public_root.iterdir()) == ()


def test_public_video_context_entry_failures_do_not_leak_file_descriptors(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    descriptor_root = Path("/proc/self/fd")
    if not descriptor_root.is_dir():
        pytest.skip("file descriptor accounting requires procfs")
    body = b"\x00\x00\x00\x18ftypmp42" + b"0" * 12
    bundle = _bundle(tmp_path, {"post.mp4": body}, bucket="REELS")

    class EntryFailure:
        def __enter__(self) -> None:
            raise MediaSafetyError("forced verified-open entry failure")

        def __exit__(
            self,
            exception_type: object,
            exception: object,
            traceback: object,
        ) -> None:
            return None

    def fail_verified_open(*_args: object, **_kwargs: object) -> EntryFailure:
        return EntryFailure()

    monkeypatch.setattr(media_module, "open_verified_private_media", fail_verified_open)
    baseline = len(tuple(descriptor_root.iterdir()))

    for _attempt in range(32):
        with pytest.raises(MediaSafetyError, match="forced verified-open"):
            prepare_bundle_media(
                bundle,
                profile_id="ansonphong",
                targets=("instagram",),
                private_staging_directory=tmp_path / "private",
                instagram=_instagram(tmp_path),
                process_runner=_runner_for(_video_probe()),
                ffprobe_timeout_seconds=7.0,
            )

    assert len(tuple(descriptor_root.iterdir())) == baseline
    assert tuple((tmp_path / "public").iterdir()) == ()


@pytest.mark.parametrize(
    ("failure", "match"),
    [
        (FileNotFoundError(), "unavailable"),
        (subprocess.TimeoutExpired(["ffprobe"], 1), "timed out"),
        (subprocess.CompletedProcess([], 1, "", "/private/operator/path"), "failed"),
        (subprocess.CompletedProcess([], 0, "not-json", ""), "malformed"),
        (subprocess.CompletedProcess([], 0, "{" + "x" * 1_100_000, ""), "too large"),
    ],
)
def test_ffprobe_failures_are_explicit_and_sanitized(
    tmp_path: Path,
    failure: BaseException | subprocess.CompletedProcess[str],
    match: str,
) -> None:
    bundle = _bundle(
        tmp_path, {"post.mp4": b"\x00\x00\x00\x18ftypmp42data"}, bucket="REELS"
    )

    def runner(*args: object, **kwargs: object) -> subprocess.CompletedProcess[str]:
        if isinstance(failure, BaseException):
            raise failure
        return failure

    with pytest.raises(MediaSafetyError, match=match) as caught:
        prepare_bundle_media(
            bundle,
            profile_id="ansonphong",
            targets=("x",),
            private_staging_directory=tmp_path / "private",
            process_runner=runner,
        )

    assert "/private/operator/path" not in str(caught.value)


@pytest.mark.parametrize(
    ("targets", "probe", "match"),
    [
        (("x",), _video_probe(duration="141"), "duration"),
        (("x",), _video_probe(video_pix_fmt="yuv444p"), "pixel"),
        (("x",), _video_probe(video_width=2000), "dimensions"),
        (("instagram",), _video_probe(video_codec_name="vp9"), "codec"),
        (("instagram",), _video_probe(video_avg_frame_rate="61/1"), "frame rate"),
        (("instagram",), _video_probe(audio_sample_rate="44100"), "48 kHz"),
        (("instagram",), _video_probe(video_bit_rate="25000001"), "bitrate"),
    ],
)
def test_video_platform_limits_are_enforced(
    tmp_path: Path, targets: tuple[str, ...], probe: dict[str, object], match: str
) -> None:
    bundle = _bundle(
        tmp_path, {"post.mp4": b"\x00\x00\x00\x18ftypmp42data"}, bucket="REELS"
    )

    with pytest.raises(MediaSafetyError, match=match):
        prepare_bundle_media(
            bundle,
            profile_id="ansonphong",
            targets=targets,
            private_staging_directory=tmp_path / "private",
            instagram=_instagram(tmp_path) if "instagram" in targets else None,
            process_runner=_runner_for(probe),
            ffprobe_timeout_seconds=7.0,
        )


@pytest.mark.parametrize(
    "probe",
    [
        _video_probe(video_bit_rate=None),
        _video_probe(audio_bit_rate=None),
    ],
)
def test_instagram_video_missing_bitrate_metadata_fails_closed(
    tmp_path: Path, probe: dict[str, object]
) -> None:
    bundle = _bundle(
        tmp_path, {"post.mp4": b"\x00\x00\x00\x18ftypmp42data"}, bucket="REELS"
    )

    with pytest.raises(MediaSafetyError, match="bitrate metadata is required"):
        prepare_bundle_media(
            bundle,
            profile_id="ansonphong",
            targets=("instagram",),
            private_staging_directory=tmp_path / "private",
            instagram=_instagram(tmp_path),
            process_runner=_runner_for(probe),
            ffprobe_timeout_seconds=7.0,
        )


def test_public_url_verification_streams_exact_bytes_with_mocked_transport(
    tmp_path: Path,
) -> None:
    body = _image_bytes("JPEG")
    descriptor = _public_descriptor(tmp_path, body)
    factory = MockPinnedTransportFactory(
        lambda request: httpx.Response(
            200,
            headers={"content-type": "image/jpeg"},
            content=body,
            request=request,
        )
    )

    verified = verify_public_media_url(
        descriptor,
        media_base_url="https://media.example.test/post-pulsar/",
        resolver=_public_resolver,
        transport_factory=factory,
    )

    assert verified.sha256 == descriptor.sha256
    assert verified.size_bytes == descriptor.size_bytes
    assert factory.calls == [("93.184.216.34", "media.example.test", 443)]


def test_default_pinned_backend_connects_to_ip_not_resolved_hostname() -> None:
    calls: list[tuple[str, int]] = []
    sentinel = object()

    class FakeBackend:
        def connect_tcp(self, host: str, port: int, **_kwargs: object) -> object:
            calls.append((host, port))
            return sentinel

    backend = media_module._PinnedNetworkBackend(
        "93.184.216.34", "media.example.test", 443
    )
    backend._delegate = FakeBackend()  # type: ignore[assignment]

    connected = backend.connect_tcp("media.example.test", 443)

    assert connected is sentinel
    assert calls == [("93.184.216.34", 443)]
    with pytest.raises(OSError, match="origin"):
        backend.connect_tcp("rebound.example.test", 443)


def test_public_url_redirect_is_revalidated_and_bounded(tmp_path: Path) -> None:
    body = _image_bytes("JPEG")
    descriptor = _public_descriptor(tmp_path, body)
    public_url = descriptor.public_url
    assert public_url is not None
    redirected = public_url + "-final"
    seen_requests: list[tuple[str, str]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen_requests.append((str(request.url), request.headers["host"]))
        if str(request.url) == public_url:
            return httpx.Response(
                302, headers={"location": redirected}, request=request
            )
        return httpx.Response(
            200,
            headers={"content-type": "image/jpeg"},
            content=body,
            request=request,
        )

    factory = MockPinnedTransportFactory(handler)
    verified = verify_public_media_url(
        replace(descriptor, public_url=public_url),
        media_base_url="https://media.example.test/post-pulsar/",
        resolver=_public_resolver,
        transport_factory=factory,
        max_redirects=1,
    )

    assert verified.redirects == 1
    assert factory.calls == [
        ("93.184.216.34", "media.example.test", 443),
        ("93.184.216.34", "media.example.test", 443),
    ]
    assert seen_requests == [
        (public_url, "media.example.test"),
        (redirected, "media.example.test"),
    ]


@pytest.mark.parametrize("failure", ["private_dns", "escape", "mime", "body"])
def test_public_url_rejects_private_redirect_or_mismatched_response(
    tmp_path: Path, failure: str
) -> None:
    body = _image_bytes("JPEG")
    descriptor = _public_descriptor(tmp_path, body)
    response_body = body + b"extra" if failure == "body" else body
    content_type = "text/plain" if failure == "mime" else "image/jpeg"
    location = "https://evil.example/escaped.jpg" if failure == "escape" else None

    def handler(request: httpx.Request) -> httpx.Response:
        if location is not None:
            return httpx.Response(302, headers={"location": location})
        return httpx.Response(
            200, headers={"content-type": content_type}, content=response_body
        )

    def resolver(host: str, port: int, **kwargs: object) -> list[tuple[object, ...]]:
        if failure == "private_dns":
            return [(socket.AF_INET, socket.SOCK_STREAM, 6, "", ("127.0.0.1", port))]
        return _public_resolver(host, port, **kwargs)

    with pytest.raises(MediaSafetyError):
        verify_public_media_url(
            descriptor,
            media_base_url="https://media.example.test/post-pulsar/",
            resolver=resolver,
            transport_factory=MockPinnedTransportFactory(handler),
        )


def test_public_url_requires_a_matching_media_signature(tmp_path: Path) -> None:
    body = b"not-a-jpeg"
    descriptor = _public_descriptor(tmp_path, body)
    factory = MockPinnedTransportFactory(
        lambda _request: httpx.Response(
            200,
            headers={"content-type": "image/jpeg"},
            content=body,
        )
    )

    with pytest.raises(MediaSafetyError, match="signature"):
        verify_public_media_url(
            descriptor,
            media_base_url="https://media.example.test/post-pulsar/",
            resolver=_public_resolver,
            transport_factory=factory,
        )


def test_public_url_rejects_an_unsafe_base_even_with_public_dns(tmp_path: Path) -> None:
    body = _image_bytes("JPEG")
    descriptor = _public_descriptor(tmp_path, body)
    factory = MockPinnedTransportFactory(
        lambda _request: httpx.Response(
            200,
            headers={"content-type": "image/jpeg"},
            content=body,
        )
    )

    public_url = descriptor.public_url
    assert public_url is not None
    with pytest.raises(MediaSafetyError, match="base URL"):
        verify_public_media_url(
            replace(
                descriptor,
                public_url=public_url.replace("media.example.test", "93.184.216.34"),
            ),
            media_base_url="https://93.184.216.34/post-pulsar/",
            resolver=_public_resolver,
            transport_factory=factory,
        )


def test_cleanup_is_hash_guarded_and_retains_ambiguous_evidence(tmp_path: Path) -> None:
    body = _image_bytes("JPEG")
    descriptor = _public_descriptor(tmp_path, body)
    path = descriptor.absolute_path(tmp_path / "public")

    assert not cleanup_staged_media(
        descriptor, tmp_path / "public", outcome="ambiguous"
    )
    assert path.exists()
    path.write_bytes(b"different")
    with pytest.raises(MediaSafetyError, match="hash"):
        cleanup_staged_media(descriptor, tmp_path / "public", outcome="failed")
    path.write_bytes(body)
    assert cleanup_staged_media(descriptor, tmp_path / "public", outcome="published")
    assert not path.exists()


def test_checkpointed_cleanup_is_path_and_hash_bound_without_metadata(
    tmp_path: Path,
) -> None:
    body = _image_bytes("JPEG")
    descriptor = _public_descriptor(tmp_path, body)
    path = descriptor.absolute_path(tmp_path / "public")

    assert not cleanup_checkpointed_staging(
        descriptor.relative_path,
        descriptor.sha256,
        tmp_path / "public",
        outcome="ambiguous",
    )
    path.write_bytes(b"changed")
    with pytest.raises(MediaSafetyError, match="hash"):
        cleanup_checkpointed_staging(
            descriptor.relative_path,
            descriptor.sha256,
            tmp_path / "public",
            outcome="failed",
        )
    with pytest.raises(MediaSafetyError, match="relative"):
        cleanup_checkpointed_staging(
            "../outside.jpg",
            descriptor.sha256,
            tmp_path / "public",
            outcome="failed",
        )
    path.write_bytes(body)
    assert cleanup_checkpointed_staging(
        descriptor.relative_path,
        descriptor.sha256,
        tmp_path / "public",
        outcome="published",
    )


def test_cleanup_rejects_a_symlink_swap_without_touching_its_target(
    tmp_path: Path,
) -> None:
    body = _image_bytes("JPEG")
    descriptor = _public_descriptor(tmp_path, body)
    path = descriptor.absolute_path(tmp_path / "public")
    outside = tmp_path / "outside.jpg"
    outside.write_bytes(body)
    path.unlink()
    path.symlink_to(outside)

    with pytest.raises(MediaSafetyError, match="regular file"):
        cleanup_staged_media(descriptor, tmp_path / "public", outcome="failed")

    assert outside.read_bytes() == body


def test_cleanup_quarantines_open_identity_before_hash_and_unlink(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    body = _image_bytes("JPEG")
    descriptor = _public_descriptor(tmp_path, body)
    path = descriptor.absolute_path(tmp_path / "public")
    replacement = b"replacement-must-survive"
    real_rename = os.rename
    swapped = False

    def rename_then_replace(
        source: str,
        destination: str,
        *,
        src_dir_fd: int | None = None,
        dst_dir_fd: int | None = None,
    ) -> None:
        nonlocal swapped
        real_rename(
            source,
            destination,
            src_dir_fd=src_dir_fd,
            dst_dir_fd=dst_dir_fd,
        )
        if not swapped:
            swapped = True
            path.write_bytes(replacement)

    monkeypatch.setattr(os, "rename", rename_then_replace)

    assert cleanup_staged_media(descriptor, tmp_path / "public", outcome="published")
    assert path.read_bytes() == replacement
    quarantine = tmp_path / "public" / ".post-pulsar-cleanup"
    assert quarantine.stat().st_mode & 0o077 == 0
    assert tuple(quarantine.iterdir()) == ()
