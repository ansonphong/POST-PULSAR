"""Safe media inspection, staging, and public verification tests."""

from __future__ import annotations

import hashlib
import json
import socket
import subprocess
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
    cleanup_staged_media,
    prepare_bundle_media,
    verify_public_media_url,
)


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
    assert item.private.absolute_path(tmp_path / "private").read_bytes() == original
    assert item.public is None
    assert item.private.relative_path.parts[:2] == ("ansonphong", "QUEUE")
    assert item.private.absolute_path(tmp_path / "private").stat().st_mode & 0o222 == 0
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
    with pytest.raises(MediaSafetyError, match="profile"):
        first.items[0].private.absolute_path(
            tmp_path / "private", expected_profile_id="360hextile"
        )


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
    ratios: list[float] = []
    for descriptor in public:
        assert descriptor is not None
        assert descriptor.relative_path.parent == Path(".")
        assert descriptor.public_url is not None
        assert descriptor.public_url.endswith(descriptor.relative_path.name)
        with Image.open(descriptor.absolute_path(tmp_path / "public")) as image:
            assert image.format == "JPEG"
            assert "exif" not in image.info
            ratios.append(image.width / image.height)
            assert 320 <= image.width <= 1440
    assert ratios[0] == pytest.approx(ratios[1], abs=0.002)
    assert 4 / 5 <= ratios[0] <= 1.91


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
    assert item.public.absolute_path(tmp_path / "public").read_bytes() == body


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


def test_public_url_verification_streams_exact_bytes_with_mocked_transport(
    tmp_path: Path,
) -> None:
    body = _image_bytes("JPEG")
    descriptor = _public_descriptor(tmp_path, body)
    transport = httpx.MockTransport(
        lambda request: httpx.Response(
            200, headers={"content-type": "image/jpeg"}, content=body
        )
    )

    with httpx.Client(transport=transport) as client:
        verified = verify_public_media_url(
            descriptor,
            media_base_url="https://media.example.test/post-pulsar/",
            client=client,
            resolver=_public_resolver,
        )

    assert verified.sha256 == descriptor.sha256
    assert verified.size_bytes == descriptor.size_bytes


def test_public_url_redirect_is_revalidated_and_bounded(tmp_path: Path) -> None:
    body = _image_bytes("JPEG")
    descriptor = _public_descriptor(tmp_path, body)
    redirected = descriptor.public_url + "-final"

    def handler(request: httpx.Request) -> httpx.Response:
        if str(request.url) == descriptor.public_url:
            return httpx.Response(302, headers={"location": redirected})
        return httpx.Response(200, headers={"content-type": "image/jpeg"}, content=body)

    with httpx.Client(transport=httpx.MockTransport(handler)) as client:
        verified = verify_public_media_url(
            replace(descriptor, public_url=redirected.removesuffix("-final")),
            media_base_url="https://media.example.test/post-pulsar/",
            client=client,
            resolver=_public_resolver,
            max_redirects=1,
        )

    assert verified.redirects == 1


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

    with httpx.Client(transport=httpx.MockTransport(handler)) as client:
        with pytest.raises(MediaSafetyError):
            verify_public_media_url(
                descriptor,
                media_base_url="https://media.example.test/post-pulsar/",
                client=client,
                resolver=resolver,
            )


def test_public_url_requires_a_matching_media_signature(tmp_path: Path) -> None:
    body = b"not-a-jpeg"
    descriptor = _public_descriptor(tmp_path, body)
    transport = httpx.MockTransport(
        lambda _request: httpx.Response(
            200,
            headers={"content-type": "image/jpeg"},
            content=body,
        )
    )

    with httpx.Client(transport=transport) as client:
        with pytest.raises(MediaSafetyError, match="signature"):
            verify_public_media_url(
                descriptor,
                media_base_url="https://media.example.test/post-pulsar/",
                client=client,
                resolver=_public_resolver,
            )


def test_public_url_rejects_an_unsafe_base_even_with_public_dns(tmp_path: Path) -> None:
    body = _image_bytes("JPEG")
    descriptor = _public_descriptor(tmp_path, body)
    transport = httpx.MockTransport(
        lambda _request: httpx.Response(
            200,
            headers={"content-type": "image/jpeg"},
            content=body,
        )
    )

    with httpx.Client(transport=transport) as client:
        with pytest.raises(MediaSafetyError, match="base URL"):
            verify_public_media_url(
                replace(
                    descriptor,
                    public_url=descriptor.public_url.replace(
                        "media.example.test", "93.184.216.34"
                    )
                    if descriptor.public_url
                    else None,
                ),
                media_base_url="https://93.184.216.34/post-pulsar/",
                client=client,
                resolver=_public_resolver,
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
