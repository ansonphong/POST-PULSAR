"""Fail-closed media inspection, staging, and public URL verification."""

from __future__ import annotations

import hashlib
import ipaddress
import json
import math
import os
import re
import secrets
import socket
import stat
import subprocess
import tempfile
import unicodedata
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, replace
from fractions import Fraction
from io import BytesIO
from pathlib import Path
from typing import BinaryIO, Final, Literal, Protocol, TypeAlias, cast
from urllib.parse import SplitResult, quote, unquote, urljoin, urlsplit

import httpx
from PIL import Image, ImageOps, UnidentifiedImageError

from post_pulsar.config import InstagramSettings
from post_pulsar.content import PublishableBundle, SourceBucket

MediaTarget: TypeAlias = Literal["x", "instagram"]
MediaKind: TypeAlias = Literal["image", "video", "animated_gif"]
StagingKind: TypeAlias = Literal["private", "instagram_public"]
CleanupPolicy: TypeAlias = Literal["known_terminal_hash_match"]
TerminalOutcome: TypeAlias = Literal["published", "failed", "ambiguous"]

_PROFILE_ID_RE: Final = re.compile(r"[a-z0-9][a-z0-9-]{0,31}\Z")
_BUNDLE_ID_RE: Final = re.compile(r"[A-Za-z0-9][A-Za-z0-9_-]{0,63}\Z")
_SHA256_RE: Final = re.compile(r"[0-9a-f]{64}\Z")
_TARGETS: Final = frozenset({"x", "instagram"})
_BUCKETS: Final = frozenset({"QUEUE", "RANDOM", "REELS"})
_COPY_CHUNK_BYTES: Final = 1024 * 1024
_MAX_FFPROBE_JSON_BYTES: Final = 1024 * 1024
_DEFAULT_PUBLIC_RESPONSE_BYTES: Final = 1024 * 1024 * 1024 + 1
_CONTENT_FINGERPRINT_DOMAIN: Final = b"POST-PULSAR-CONTENT-BUNDLE\x00V1\x00"

_X_IMAGE_MAX_BYTES: Final = 5 * 1024 * 1024
_X_GIF_MAX_BYTES: Final = 15 * 1024 * 1024
_X_GIF_MAX_WIDTH: Final = 1280
_X_GIF_MAX_HEIGHT: Final = 1080
_X_GIF_MAX_FRAMES: Final = 350
_X_GIF_MAX_AGGREGATE_PIXELS: Final = 300_000_000
_X_VIDEO_MAX_BYTES: Final = 512 * 1024 * 1024
_INSTAGRAM_IMAGE_MAX_BYTES: Final = 8 * 1024 * 1024
_INSTAGRAM_VIDEO_MAX_BYTES: Final = 1024 * 1024 * 1024

_IMAGE_MIME: Final[dict[str, str]] = {
    "JPEG": "image/jpeg",
    "PNG": "image/png",
    "GIF": "image/gif",
}
_REDIRECT_STATUS: Final = frozenset({301, 302, 303, 307, 308})


class MediaSafetyError(ValueError):
    """A deterministic media error that never includes an operator path."""


class ProcessRunner(Protocol):
    """The bounded subprocess seam used by video inspection tests."""

    def __call__(
        self,
        argv: list[str],
        *,
        capture_output: bool,
        text: bool,
        timeout: float,
        check: bool,
        shell: bool,
    ) -> subprocess.CompletedProcess[str]: ...


class Resolver(Protocol):
    """The DNS seam used to validate every HTTP hop."""

    def __call__(
        self, host: str, port: int, **kwargs: object
    ) -> Sequence[tuple[object, ...]]: ...


class _Hasher(Protocol):
    def update(self, data: bytes) -> object: ...


@dataclass(frozen=True, slots=True)
class MediaWarning:
    """A stable warning suitable for later allow-listed persistence."""

    profile_id: str
    source_bucket: SourceBucket
    bundle_id: str
    source_name: str | None
    code: str
    message: str


@dataclass(frozen=True, slots=True)
class MediaMetadata:
    """Decoded characteristics used for platform preflight decisions."""

    kind: MediaKind
    format_name: str
    mime_type: str
    width: int
    height: int
    frame_count: int | None = None
    duration_seconds: float | None = None
    frame_rate: float | None = None
    codec: str | None = None
    pixel_format: str | None = None
    video_bitrate: int | None = None
    audio_codec: str | None = None
    audio_profile: str | None = None
    audio_sample_rate: int | None = None
    audio_bitrate: int | None = None


@dataclass(frozen=True, slots=True)
class StagedMedia:
    """An immutable identity for one private or public local artifact."""

    profile_id: str
    source_bucket: SourceBucket
    bundle_id: str
    bundle_fingerprint: str
    source_name: str
    staging_kind: StagingKind
    relative_path: Path
    sha256: str
    mime_type: str
    size_bytes: int
    cleanup_policy: CleanupPolicy
    public_url: str | None = None

    def __post_init__(self) -> None:
        """Reject forged or cross-profile descriptor identities."""
        if not _PROFILE_ID_RE.fullmatch(self.profile_id):
            raise MediaSafetyError("staged media profile identity is invalid")
        if self.source_bucket not in _BUCKETS:
            raise MediaSafetyError("staged media source bucket is invalid")
        if not _BUNDLE_ID_RE.fullmatch(self.bundle_id):
            raise MediaSafetyError("staged media bundle identity is invalid")
        if not _SHA256_RE.fullmatch(self.bundle_fingerprint):
            raise MediaSafetyError("staged media bundle fingerprint is invalid")
        if not _SHA256_RE.fullmatch(self.sha256) or self.size_bytes <= 0:
            raise MediaSafetyError("staged media content identity is invalid")
        if Path(self.source_name).name != self.source_name or not self.source_name:
            raise MediaSafetyError("staged media source name is invalid")
        _validate_relative_descriptor(self)

    def absolute_path(
        self,
        staging_root: str | os.PathLike[str],
        *,
        expected_profile_id: str | None = None,
        expected_bucket: SourceBucket | None = None,
    ) -> Path:
        """Resolve beneath the supplied root while enforcing descriptor ownership."""
        if expected_profile_id is not None and expected_profile_id != self.profile_id:
            raise MediaSafetyError("staged media belongs to a different profile")
        if expected_bucket is not None and expected_bucket != self.source_bucket:
            raise MediaSafetyError("staged media belongs to a different source bucket")
        return _descriptor_path(Path(staging_root), self.relative_path)


@dataclass(frozen=True, slots=True)
class PreparedMediaItem:
    """Verified media with the only paths adapters are permitted to consume."""

    source_name: str
    metadata: MediaMetadata
    private: StagedMedia
    public: StagedMedia | None


@dataclass(frozen=True, slots=True)
class PreparedMedia:
    """A deterministic, profile-scoped preparation result."""

    profile_id: str
    source_bucket: SourceBucket
    bundle_id: str
    bundle_fingerprint: str
    targets: tuple[MediaTarget, ...]
    items: tuple[PreparedMediaItem, ...]
    warnings: tuple[MediaWarning, ...]


@dataclass(frozen=True, slots=True)
class PublicURLVerification:
    """Proof that the configured URL served the exact staged bytes."""

    final_url: str
    redirects: int
    sha256: str
    mime_type: str
    size_bytes: int


@dataclass(frozen=True, slots=True)
class _StagedResult:
    descriptor: StagedMedia
    created: bool


def prepare_bundle_media(
    bundle: PublishableBundle,
    *,
    profile_id: str,
    targets: Iterable[str],
    private_staging_directory: str | os.PathLike[str],
    instagram: InstagramSettings | None = None,
    ffprobe_executable: str = "ffprobe",
    ffprobe_timeout_seconds: float = 15.0,
    process_runner: ProcessRunner | None = None,
) -> PreparedMedia:
    """Capture, decode, constrain, and stage every media member in *bundle*."""
    _validate_identity(profile_id, bundle)
    normalized_targets = _normalize_targets(targets)
    if "instagram" in normalized_targets and instagram is None:
        raise MediaSafetyError("Instagram staging settings are required")
    if not normalized_targets:
        raise MediaSafetyError("at least one media target is required")

    sources = bundle.content.images
    if bundle.content.video is not None:
        sources = (bundle.content.video,)
    if not sources:
        raise MediaSafetyError("bundle contains no media")
    if "x" in normalized_targets and len(sources) > 4:
        raise MediaSafetyError("X accepts no more than four images")
    if "instagram" in normalized_targets and len(sources) > 10:
        raise MediaSafetyError("Instagram accepts no more than ten images")

    private_root = Path(private_staging_directory)
    runner = _run_process if process_runner is None else process_runner
    staged_results: list[_StagedResult] = []
    private_items: list[tuple[Path, StagedMedia, MediaMetadata]] = []
    public_items: list[StagedMedia | None] = []
    warnings: list[MediaWarning] = []
    try:
        for index, source in enumerate(sources, start=1):
            result = _stage_source(
                source,
                root=private_root,
                profile_id=profile_id,
                bucket=bundle.bucket,
                bundle_id=bundle.bundle_id,
                bundle_fingerprint=bundle.fingerprint,
                ordinal=index,
                staging_kind="private",
                mime_type="application/octet-stream",
                public_url=None,
            )
            staged_results.append(result)
            descriptor = result.descriptor
            staged_path = descriptor.absolute_path(
                private_root,
                expected_profile_id=profile_id,
                expected_bucket=bundle.bucket,
            )
            if bundle.content.video is None:
                metadata = _inspect_image(staged_path)
            else:
                metadata = _inspect_video(
                    staged_path,
                    descriptor.size_bytes,
                    executable=ffprobe_executable,
                    timeout_seconds=ffprobe_timeout_seconds,
                    runner=runner,
                )
            descriptor = replace(descriptor, mime_type=metadata.mime_type)
            private_items.append((source, descriptor, metadata))

        _verify_bundle_fingerprint(bundle, private_items, private_root)
        _validate_platform_media(private_items, normalized_targets)

        if "instagram" in normalized_targets:
            instagram_settings = cast(InstagramSettings, instagram)
            if bundle.content.alt_text is not None:
                warnings.append(
                    MediaWarning(
                        profile_id,
                        bundle.bucket,
                        bundle.bundle_id,
                        None,
                        "instagram_alt_text_unsupported",
                        "Instagram publishing does not preserve source alt text.",
                    )
                )
            if bundle.content.video is None:
                public_results, normalize_warnings = _normalize_instagram_images(
                    private_items,
                    private_root=private_root,
                    profile_id=profile_id,
                    bucket=bundle.bucket,
                    bundle_id=bundle.bundle_id,
                    bundle_fingerprint=bundle.fingerprint,
                    settings=instagram_settings,
                )
                staged_results.extend(public_results)
                public_items.extend(item.descriptor for item in public_results)
                warnings.extend(normalize_warnings)
            else:
                source, private_descriptor, _metadata = private_items[0]
                raw_public_result = _stage_source(
                    private_descriptor.absolute_path(private_root),
                    root=instagram_settings.media_directory,
                    profile_id=profile_id,
                    bucket=bundle.bucket,
                    bundle_id=bundle.bundle_id,
                    bundle_fingerprint=bundle.fingerprint,
                    ordinal=1,
                    staging_kind="instagram_public",
                    mime_type=private_descriptor.mime_type,
                    public_url=_public_url(
                        instagram_settings.media_base_url,
                        _public_filename(
                            profile_id,
                            bundle.bucket,
                            bundle.fingerprint,
                            1,
                            private_descriptor.sha256,
                            source.suffix.lower(),
                        ),
                    ),
                    forced_filename=_public_filename(
                        profile_id,
                        bundle.bucket,
                        bundle.fingerprint,
                        1,
                        private_descriptor.sha256,
                        source.suffix.lower(),
                    ),
                )
                public_result = _StagedResult(
                    replace(raw_public_result.descriptor, source_name=source.name),
                    raw_public_result.created,
                )
                staged_results.append(public_result)
                public_items.append(public_result.descriptor)
        else:
            public_items.extend(None for _item in private_items)
    except Exception:
        _remove_new_staging(staged_results, private_root, instagram)
        raise

    items = tuple(
        PreparedMediaItem(
            source_name=source.name,
            metadata=metadata,
            private=private_descriptor,
            public=public_descriptor,
        )
        for (source, private_descriptor, metadata), public_descriptor in zip(
            private_items, public_items, strict=True
        )
    )
    return PreparedMedia(
        profile_id=profile_id,
        source_bucket=bundle.bucket,
        bundle_id=bundle.bundle_id,
        bundle_fingerprint=bundle.fingerprint,
        targets=normalized_targets,
        items=items,
        warnings=tuple(warnings),
    )


def verify_public_media_url(
    staged: StagedMedia,
    *,
    media_base_url: str,
    client: httpx.Client,
    resolver: Resolver | None = None,
    timeout_seconds: float = 15.0,
    max_redirects: int = 3,
    max_response_bytes: int = _DEFAULT_PUBLIC_RESPONSE_BYTES,
) -> PublicURLVerification:
    """Verify an exact public staging URL without trusting redirects or DNS."""
    if staged.staging_kind != "instagram_public" or staged.public_url is None:
        raise MediaSafetyError("public URL verification requires public staging")
    if max_redirects < 0 or max_redirects > 10:
        raise MediaSafetyError("public URL redirect limit is invalid")
    if max_response_bytes <= 0:
        raise MediaSafetyError("public URL response limit is invalid")
    dns_resolver = _resolve_addresses if resolver is None else resolver
    current = staged.public_url
    redirects = 0
    while True:
        _validate_url_beneath_base(current, media_base_url)
        _validate_public_dns(current, dns_resolver)
        try:
            with client.stream(
                "GET",
                current,
                headers={"Accept": staged.mime_type},
                follow_redirects=False,
                timeout=timeout_seconds,
            ) as response:
                if response.status_code in _REDIRECT_STATUS:
                    if redirects >= max_redirects:
                        raise MediaSafetyError("public media redirect limit exceeded")
                    location = response.headers.get("location")
                    if not location:
                        raise MediaSafetyError("public media redirect has no location")
                    current = urljoin(current, location)
                    redirects += 1
                    continue
                if response.status_code != 200:
                    raise MediaSafetyError("public media URL did not return HTTP 200")
                mime_type = (
                    response.headers.get("content-type", "")
                    .split(";", 1)[0]
                    .strip()
                    .casefold()
                )
                if mime_type != staged.mime_type.casefold():
                    raise MediaSafetyError("public media MIME type does not match")
                digest = hashlib.sha256()
                size = 0
                first = bytearray()
                tail = bytearray()
                byte_limit = min(max_response_bytes, staged.size_bytes)
                for chunk in response.iter_bytes():
                    size += len(chunk)
                    if size > byte_limit:
                        raise MediaSafetyError(
                            "public media response exceeded byte limit"
                        )
                    digest.update(chunk)
                    if len(first) < 16:
                        first.extend(chunk[: 16 - len(first)])
                    tail.extend(chunk)
                    if len(tail) > 16:
                        del tail[:-16]
        except MediaSafetyError:
            raise
        except httpx.HTTPError:
            raise MediaSafetyError("public media request failed safely") from None
        if size != staged.size_bytes or digest.hexdigest() != staged.sha256:
            raise MediaSafetyError("public media size or hash does not match staging")
        _validate_signature(staged.mime_type, bytes(first), bytes(tail))
        return PublicURLVerification(
            final_url=current,
            redirects=redirects,
            sha256=digest.hexdigest(),
            mime_type=mime_type,
            size_bytes=size,
        )


def cleanup_staged_media(
    staged: StagedMedia,
    staging_root: str | os.PathLike[str],
    *,
    outcome: TerminalOutcome,
) -> bool:
    """Remove only a hash-matched artifact after a known terminal result."""
    if outcome == "ambiguous":
        return False
    if outcome not in {"published", "failed"}:
        raise MediaSafetyError("unknown staging cleanup outcome")
    path = staged.absolute_path(staging_root)
    try:
        metadata = os.lstat(path)
    except FileNotFoundError:
        return False
    except OSError:
        raise MediaSafetyError("staged media metadata could not be read") from None
    if not stat.S_ISREG(metadata.st_mode):
        raise MediaSafetyError("staged media is not a regular file")
    digest, size = _hash_regular(path)
    if digest != staged.sha256 or size != staged.size_bytes:
        raise MediaSafetyError("staged media hash does not match cleanup descriptor")
    try:
        path.unlink()
    except OSError:
        raise MediaSafetyError("staged media could not be removed safely") from None
    return True


def _validate_identity(profile_id: str, bundle: PublishableBundle) -> None:
    if not _PROFILE_ID_RE.fullmatch(profile_id):
        raise MediaSafetyError("profile ID is invalid")
    if bundle.bucket not in _BUCKETS:
        raise MediaSafetyError("source bucket is invalid")
    if not _BUNDLE_ID_RE.fullmatch(bundle.bundle_id):
        raise MediaSafetyError("bundle ID is invalid")
    if not _SHA256_RE.fullmatch(bundle.fingerprint):
        raise MediaSafetyError("bundle fingerprint is invalid")


def _normalize_targets(targets: Iterable[str]) -> tuple[MediaTarget, ...]:
    selected = set(targets)
    unknown = selected - _TARGETS
    if unknown:
        raise MediaSafetyError("unsupported media target")
    return tuple(
        target
        for target in cast(tuple[MediaTarget, ...], ("x", "instagram"))
        if target in selected
    )


def _stage_source(
    source_path: Path,
    *,
    root: Path,
    profile_id: str,
    bucket: SourceBucket,
    bundle_id: str,
    bundle_fingerprint: str,
    ordinal: int,
    staging_kind: StagingKind,
    mime_type: str,
    public_url: str | None,
    forced_filename: str | None = None,
) -> _StagedResult:
    destination_directory = _stage_directory(
        root, profile_id, bucket, bundle_fingerprint, staging_kind
    )
    temporary = destination_directory / f".capture-{secrets.token_hex(16)}"
    try:
        source, source_before = _open_regular(source_path)
    except OSError:
        raise MediaSafetyError("source media could not be opened safely") from None
    try:
        digest = hashlib.sha256()
        descriptor = os.open(
            temporary,
            os.O_WRONLY | os.O_CREAT | os.O_EXCL,
            0o600,
        )
        try:
            with os.fdopen(descriptor, "wb") as destination:
                size = _copy_stream(source, destination, digest)
                destination.flush()
                os.fsync(destination.fileno())
        except Exception:
            _safe_unlink_temporary(temporary)
            raise
        try:
            source_opened_after = os.fstat(source.fileno())
            source_after = os.lstat(source_path)
        except OSError:
            _safe_unlink_temporary(temporary)
            raise MediaSafetyError("source media changed during capture") from None
        if not _unchanged_source(source_before, source_opened_after, source_after):
            _safe_unlink_temporary(temporary)
            raise MediaSafetyError("source media changed during capture")
    finally:
        source.close()

    sha256 = digest.hexdigest()
    if size <= 0:
        _safe_unlink_temporary(temporary)
        raise MediaSafetyError("source media is empty")
    suffix = source_path.suffix.lower()
    filename = forced_filename or (
        f"{ordinal:02d}-{bundle_fingerprint}-{sha256}{suffix}"
    )
    final_path = destination_directory / filename
    file_mode = 0o400 if staging_kind == "private" else 0o444
    created = _install_exclusive(temporary, final_path, sha256, size, file_mode)
    relative_path = final_path.relative_to(root.absolute())
    return _StagedResult(
        StagedMedia(
            profile_id=profile_id,
            source_bucket=bucket,
            bundle_id=bundle_id,
            bundle_fingerprint=bundle_fingerprint,
            source_name=source_path.name,
            staging_kind=staging_kind,
            relative_path=relative_path,
            sha256=sha256,
            mime_type=mime_type,
            size_bytes=size,
            cleanup_policy="known_terminal_hash_match",
            public_url=public_url,
        ),
        created,
    )


def _stage_bytes(
    data: bytes,
    *,
    root: Path,
    profile_id: str,
    bucket: SourceBucket,
    bundle_id: str,
    bundle_fingerprint: str,
    source_name: str,
    ordinal: int,
    mime_type: str,
    filename: str,
    public_url: str,
) -> _StagedResult:
    directory = _stage_directory(
        root, profile_id, bucket, bundle_fingerprint, "instagram_public"
    )
    temporary = directory / f".capture-{secrets.token_hex(16)}"
    descriptor = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    try:
        with os.fdopen(descriptor, "wb") as destination:
            destination.write(data)
            destination.flush()
            os.fsync(destination.fileno())
    except Exception:
        _safe_unlink_temporary(temporary)
        raise
    digest = hashlib.sha256(data).hexdigest()
    final_path = directory / filename
    created = _install_exclusive(temporary, final_path, digest, len(data), 0o444)
    return _StagedResult(
        StagedMedia(
            profile_id,
            bucket,
            bundle_id,
            bundle_fingerprint,
            source_name,
            "instagram_public",
            final_path.relative_to(root.absolute()),
            digest,
            mime_type,
            len(data),
            "known_terminal_hash_match",
            public_url,
        ),
        created,
    )


def _stage_directory(
    root: Path,
    profile_id: str,
    bucket: SourceBucket,
    fingerprint: str,
    staging_kind: StagingKind,
) -> Path:
    root_path = root.expanduser().absolute()
    _ensure_real_directory(root_path, 0o700 if staging_kind == "private" else 0o755)
    try:
        root_path.chmod(0o700 if staging_kind == "private" else 0o755)
    except OSError:
        raise MediaSafetyError(
            "staging directory permissions could not be secured"
        ) from None
    if staging_kind == "private":
        directory = root_path / profile_id / bucket / fingerprint
        _ensure_real_directory(directory, 0o700)
        try:
            directory.chmod(0o700)
        except OSError:
            raise MediaSafetyError(
                "private staging directory permissions could not be secured"
            ) from None
        return directory
    return root_path


def _ensure_real_directory(path: Path, mode: int) -> None:
    missing: list[Path] = []
    current = path
    while True:
        try:
            metadata = os.lstat(current)
        except FileNotFoundError:
            missing.append(current)
            parent = current.parent
            if parent == current:
                raise MediaSafetyError("staging directory has no safe parent") from None
            current = parent
            continue
        except OSError:
            raise MediaSafetyError(
                "staging directory metadata could not be read"
            ) from None
        if stat.S_ISLNK(metadata.st_mode) or not stat.S_ISDIR(metadata.st_mode):
            raise MediaSafetyError("staging path contains an unsafe component")
        break
    for directory in reversed(missing):
        try:
            directory.mkdir(mode=mode)
        except FileExistsError:
            existing = os.lstat(directory)
            if stat.S_ISLNK(existing.st_mode) or not stat.S_ISDIR(existing.st_mode):
                raise MediaSafetyError("staging path contains an unsafe component")
        metadata = os.lstat(directory)
        if stat.S_ISLNK(metadata.st_mode) or not stat.S_ISDIR(metadata.st_mode):
            raise MediaSafetyError("staging path contains an unsafe component")


def _open_regular(path: Path) -> tuple[BinaryIO, os.stat_result]:
    before = os.lstat(path)
    if not stat.S_ISREG(before.st_mode):
        raise OSError("not regular")
    no_follow = getattr(os, "O_NOFOLLOW", 0)

    def opener(filename: str, flags: int) -> int:
        return os.open(filename, flags | no_follow)

    source = open(path, "rb", opener=opener)
    try:
        opened = os.fstat(source.fileno())
        after = os.lstat(path)
    except OSError:
        source.close()
        raise
    if not (
        stat.S_ISREG(opened.st_mode)
        and stat.S_ISREG(after.st_mode)
        and _same_identity(before, opened)
        and _same_identity(opened, after)
    ):
        source.close()
        raise OSError("source changed")
    return source, opened


def _copy_stream(source: BinaryIO, destination: BinaryIO, digest: _Hasher) -> int:
    size = 0
    while chunk := source.read(_COPY_CHUNK_BYTES):
        destination.write(chunk)
        digest.update(chunk)
        size += len(chunk)
    return size


def _unchanged_source(
    before: os.stat_result, opened_after: os.stat_result, path_after: os.stat_result
) -> bool:
    return (
        stat.S_ISREG(opened_after.st_mode)
        and stat.S_ISREG(path_after.st_mode)
        and _same_identity(before, opened_after)
        and _same_identity(opened_after, path_after)
        and before.st_size == opened_after.st_size == path_after.st_size
        and before.st_mtime_ns == opened_after.st_mtime_ns == path_after.st_mtime_ns
        and before.st_ctime_ns == opened_after.st_ctime_ns == path_after.st_ctime_ns
    )


def _same_identity(left: os.stat_result, right: os.stat_result) -> bool:
    return (
        (left.st_dev, left.st_ino) != (0, 0)
        and (right.st_dev, right.st_ino) != (0, 0)
        and os.path.samestat(left, right)
    )


def _install_exclusive(
    temporary: Path,
    final_path: Path,
    expected_hash: str,
    expected_size: int,
    file_mode: int,
) -> bool:
    try:
        os.link(temporary, final_path, follow_symlinks=False)
    except FileExistsError:
        try:
            metadata = os.lstat(final_path)
        except OSError:
            _safe_unlink_temporary(temporary)
            raise MediaSafetyError("existing staging artifact is unsafe") from None
        if not stat.S_ISREG(metadata.st_mode):
            _safe_unlink_temporary(temporary)
            raise MediaSafetyError("existing staging artifact is unsafe")
        actual_hash, actual_size = _hash_regular(final_path)
        if actual_hash != expected_hash or actual_size != expected_size:
            _safe_unlink_temporary(temporary)
            raise MediaSafetyError("staging name collision has different bytes")
        _safe_unlink_temporary(temporary)
        return False
    except OSError:
        _safe_unlink_temporary(temporary)
        raise MediaSafetyError(
            "staging artifact could not be installed safely"
        ) from None
    _safe_unlink_temporary(temporary)
    try:
        final_path.chmod(file_mode)
    except OSError:
        final_path.unlink(missing_ok=True)
        raise MediaSafetyError(
            "staging artifact permissions could not be secured"
        ) from None
    return True


def _safe_unlink_temporary(path: Path) -> None:
    try:
        metadata = os.lstat(path)
    except FileNotFoundError:
        return
    except OSError:
        return
    if stat.S_ISREG(metadata.st_mode):
        try:
            path.unlink()
        except OSError:
            return


def _hash_regular(path: Path) -> tuple[str, int]:
    try:
        source, before = _open_regular(path)
    except OSError:
        raise MediaSafetyError("staged media could not be read safely") from None
    digest = hashlib.sha256()
    size = 0
    try:
        while chunk := source.read(_COPY_CHUNK_BYTES):
            digest.update(chunk)
            size += len(chunk)
        opened_after = os.fstat(source.fileno())
        path_after = os.lstat(path)
        if not _unchanged_source(before, opened_after, path_after):
            raise MediaSafetyError("staged media changed while hashing")
    finally:
        source.close()
    return digest.hexdigest(), size


def _verify_bundle_fingerprint(
    bundle: PublishableBundle,
    private_items: list[tuple[Path, StagedMedia, MediaMetadata]],
    private_root: Path,
) -> None:
    staged_paths = {
        source: descriptor.absolute_path(
            private_root,
            expected_profile_id=descriptor.profile_id,
            expected_bucket=bundle.bucket,
        )
        for source, descriptor, _metadata in private_items
    }
    digest = hashlib.sha256()
    digest.update(_CONTENT_FINGERPRINT_DOMAIN)
    digest.update(len(bundle.content.members).to_bytes(8, "big"))
    for member in bundle.content.members:
        role = _content_role(bundle, member)
        _hash_field(digest, role.encode("ascii"))
        name = unicodedata.normalize("NFC", member.name).encode("utf-8")
        _hash_field(digest, name)
        captured_path = staged_paths.get(member, member)
        _hash_file_into(digest, captured_path)
    if digest.hexdigest() != bundle.fingerprint:
        raise MediaSafetyError("bundle fingerprint changed before media preparation")


def _content_role(bundle: PublishableBundle, member: Path) -> str:
    if member in bundle.content.images:
        if member.stem == bundle.bundle_id:
            return "image:0"
        position = bundle.content.images.index(member) + 1
        return f"image:{position}"
    if member == bundle.content.video:
        return "video"
    if member.name == f"{bundle.bundle_id}-alt.txt":
        return "alt_text"
    return "caption"


def _hash_field(digest: _Hasher, value: bytes) -> None:
    digest.update(len(value).to_bytes(8, "big"))
    digest.update(value)


def _hash_file_into(digest: _Hasher, path: Path) -> None:
    try:
        source, before = _open_regular(path)
    except OSError:
        raise MediaSafetyError("bundle member could not be captured safely") from None
    try:
        digest.update(before.st_size.to_bytes(8, "big"))
        while chunk := source.read(_COPY_CHUNK_BYTES):
            digest.update(chunk)
        opened_after = os.fstat(source.fileno())
        path_after = os.lstat(path)
        if not _unchanged_source(before, opened_after, path_after):
            raise MediaSafetyError("bundle member changed during preparation")
    except OSError:
        raise MediaSafetyError("bundle member changed during preparation") from None
    finally:
        source.close()


def _inspect_image(path: Path) -> MediaMetadata:
    try:
        with Image.open(path) as candidate:
            candidate.verify()
        with Image.open(path) as candidate:
            format_name = candidate.format or ""
            width, height = candidate.size
            frame_count = getattr(candidate, "n_frames", 1)
            candidate.seek(frame_count - 1)
            candidate.load()
    except (OSError, UnidentifiedImageError, Image.DecompressionBombError):
        raise MediaSafetyError("image could not be decoded safely") from None
    if format_name not in _IMAGE_MIME or width <= 0 or height <= 0:
        raise MediaSafetyError("decoded image format is unsupported")
    animated = frame_count > 1
    if animated and format_name != "GIF":
        raise MediaSafetyError("only GIF animation is supported")
    return MediaMetadata(
        kind="animated_gif" if animated else "image",
        format_name=format_name,
        mime_type=_IMAGE_MIME[format_name],
        width=width,
        height=height,
        frame_count=frame_count,
    )


def _inspect_video(
    path: Path,
    size_bytes: int,
    *,
    executable: str,
    timeout_seconds: float,
    runner: ProcessRunner,
) -> MediaMetadata:
    if not executable or os.path.basename(executable) != executable:
        raise MediaSafetyError("ffprobe executable name is invalid")
    if not math.isfinite(timeout_seconds) or timeout_seconds <= 0:
        raise MediaSafetyError("ffprobe timeout is invalid")
    argv = [
        executable,
        "-v",
        "error",
        "-show_entries",
        (
            "format=format_name,duration,size:"
            "stream=codec_type,codec_name,profile,pix_fmt,width,height,"
            "r_frame_rate,avg_frame_rate,bit_rate,sample_rate"
        ),
        "-of",
        "json",
        os.fspath(path),
    ]
    try:
        result = runner(
            argv,
            capture_output=True,
            text=True,
            timeout=timeout_seconds,
            check=False,
            shell=False,
        )
    except FileNotFoundError:
        raise MediaSafetyError("ffprobe is unavailable") from None
    except subprocess.TimeoutExpired:
        raise MediaSafetyError("ffprobe timed out") from None
    except OSError:
        raise MediaSafetyError("ffprobe could not start") from None
    if result.returncode != 0:
        raise MediaSafetyError("ffprobe failed without usable metadata")
    if not isinstance(result.stdout, str):
        raise MediaSafetyError("ffprobe returned malformed metadata")
    encoded = result.stdout.encode("utf-8", errors="replace")
    if len(encoded) > _MAX_FFPROBE_JSON_BYTES:
        raise MediaSafetyError("ffprobe metadata is too large")
    try:
        document = json.loads(result.stdout)
    except (json.JSONDecodeError, TypeError):
        raise MediaSafetyError("ffprobe returned malformed metadata") from None
    if not isinstance(document, dict):
        raise MediaSafetyError("ffprobe returned malformed metadata")
    metadata = _video_metadata(cast(Mapping[str, object], document), size_bytes)
    if path.suffix.casefold() == ".mov":
        metadata = replace(metadata, mime_type="video/quicktime")
    return metadata


def _run_process(
    argv: list[str],
    *,
    capture_output: bool,
    text: bool,
    timeout: float,
    check: bool,
    shell: bool,
) -> subprocess.CompletedProcess[str]:
    del capture_output, text
    with (
        tempfile.TemporaryFile() as stdout_file,
        tempfile.TemporaryFile() as stderr_file,
    ):
        result = subprocess.run(
            argv,
            stdout=stdout_file,
            stderr=stderr_file,
            timeout=timeout,
            check=check,
            shell=shell,
        )
        stdout_file.seek(0)
        stderr_file.seek(0)
        stdout = stdout_file.read(_MAX_FFPROBE_JSON_BYTES + 1).decode(
            "utf-8", errors="replace"
        )
        stderr = stderr_file.read(_MAX_FFPROBE_JSON_BYTES + 1).decode(
            "utf-8", errors="replace"
        )
    return subprocess.CompletedProcess(argv, result.returncode, stdout, stderr)


def _video_metadata(document: Mapping[str, object], size_bytes: int) -> MediaMetadata:
    format_data = document.get("format")
    streams = document.get("streams")
    if not isinstance(format_data, dict) or not isinstance(streams, list):
        raise MediaSafetyError("ffprobe returned malformed metadata")
    video_streams = [
        stream
        for stream in streams
        if isinstance(stream, dict) and stream.get("codec_type") == "video"
    ]
    audio_streams = [
        stream
        for stream in streams
        if isinstance(stream, dict) and stream.get("codec_type") == "audio"
    ]
    if len(video_streams) != 1 or len(audio_streams) > 1:
        raise MediaSafetyError(
            "video must contain one video stream and at most one audio stream"
        )
    video = cast(dict[str, object], video_streams[0])
    audio = cast(dict[str, object] | None, audio_streams[0] if audio_streams else None)
    format_name = _required_string(format_data, "format_name")
    duration = _positive_float(format_data, "duration")
    width = _positive_integer(video, "width")
    height = _positive_integer(video, "height")
    frame_rate = _frame_rate(video)
    codec = _required_string(video, "codec_name").casefold()
    pixel_format = _required_string(video, "pix_fmt").casefold()
    video_bitrate = _optional_integer(video, "bit_rate")
    audio_codec = _optional_string(audio, "codec_name")
    audio_profile = _optional_string(audio, "profile")
    sample_rate = _optional_integer(audio, "sample_rate")
    audio_bitrate = _optional_integer(audio, "bit_rate")
    mime_type = "video/quicktime" if "quicktime" in format_name else "video/mp4"
    return MediaMetadata(
        kind="video",
        format_name=format_name,
        mime_type=mime_type,
        width=width,
        height=height,
        duration_seconds=duration,
        frame_rate=frame_rate,
        codec=codec,
        pixel_format=pixel_format,
        video_bitrate=video_bitrate,
        audio_codec=audio_codec.casefold() if audio_codec else None,
        audio_profile=audio_profile,
        audio_sample_rate=sample_rate,
        audio_bitrate=audio_bitrate,
    )


def _required_string(values: Mapping[str, object], key: str) -> str:
    value = values.get(key)
    if not isinstance(value, str) or not value:
        raise MediaSafetyError("ffprobe returned malformed metadata")
    return value


def _optional_string(values: Mapping[str, object] | None, key: str) -> str | None:
    if values is None:
        return None
    value = values.get(key)
    if value is None:
        return None
    if not isinstance(value, str) or not value:
        raise MediaSafetyError("ffprobe returned malformed metadata")
    return value


def _positive_float(values: Mapping[str, object], key: str) -> float:
    value = values.get(key)
    try:
        result = float(cast(str | int | float, value))
    except (TypeError, ValueError, OverflowError):
        raise MediaSafetyError("ffprobe returned malformed metadata") from None
    if not math.isfinite(result) or result <= 0:
        raise MediaSafetyError("ffprobe returned malformed metadata")
    return result


def _positive_integer(values: Mapping[str, object], key: str) -> int:
    value = values.get(key)
    if isinstance(value, bool):
        raise MediaSafetyError("ffprobe returned malformed metadata")
    try:
        result = int(cast(str | int, value))
    except (TypeError, ValueError, OverflowError):
        raise MediaSafetyError("ffprobe returned malformed metadata") from None
    if result <= 0:
        raise MediaSafetyError("ffprobe returned malformed metadata")
    return result


def _optional_integer(values: Mapping[str, object] | None, key: str) -> int | None:
    if values is None:
        return None
    value = values.get(key)
    if value is None or value == "":
        return None
    return _positive_integer(values, key)


def _frame_rate(video: Mapping[str, object]) -> float:
    raw = video.get("avg_frame_rate") or video.get("r_frame_rate")
    if not isinstance(raw, str):
        raise MediaSafetyError("ffprobe returned malformed metadata")
    try:
        result = float(Fraction(raw))
    except (ValueError, ZeroDivisionError, OverflowError):
        raise MediaSafetyError("ffprobe returned malformed metadata") from None
    if not math.isfinite(result) or result <= 0:
        raise MediaSafetyError("ffprobe returned malformed metadata")
    return result


def _validate_platform_media(
    items: list[tuple[Path, StagedMedia, MediaMetadata]],
    targets: tuple[MediaTarget, ...],
) -> None:
    for _source, descriptor, metadata in items:
        if "x" in targets:
            _validate_x(descriptor, metadata)
        if "instagram" in targets:
            _validate_instagram(descriptor, metadata)


def _validate_x(descriptor: StagedMedia, metadata: MediaMetadata) -> None:
    if metadata.kind == "image":
        if metadata.format_name not in _IMAGE_MIME:
            raise MediaSafetyError("X image format is unsupported")
        if descriptor.size_bytes > _X_IMAGE_MAX_BYTES:
            raise MediaSafetyError("X image exceeds the 5 MiB limit")
        return
    if metadata.kind == "animated_gif":
        frames = cast(int, metadata.frame_count)
        if descriptor.size_bytes > _X_GIF_MAX_BYTES:
            raise MediaSafetyError("X animated GIF exceeds the 15 MiB limit")
        if metadata.width > _X_GIF_MAX_WIDTH or metadata.height > _X_GIF_MAX_HEIGHT:
            raise MediaSafetyError("X animated GIF dimensions exceed the limit")
        if frames > _X_GIF_MAX_FRAMES:
            raise MediaSafetyError("X animated GIF frame count exceeds the limit")
        if metadata.width * metadata.height * frames > _X_GIF_MAX_AGGREGATE_PIXELS:
            raise MediaSafetyError("X animated GIF aggregate pixels exceed the limit")
        return
    duration = cast(float, metadata.duration_seconds)
    frame_rate = cast(float, metadata.frame_rate)
    if descriptor.size_bytes > _X_VIDEO_MAX_BYTES:
        raise MediaSafetyError("X video exceeds the 512 MiB limit")
    if Path(descriptor.source_name).suffix.casefold() != ".mp4":
        raise MediaSafetyError("X video container must be MP4")
    if not (0.5 <= duration <= 140):
        raise MediaSafetyError("X video duration is outside the supported range")
    if metadata.codec != "h264":
        raise MediaSafetyError("X video codec must be H.264")
    if metadata.audio_codec is not None and (
        metadata.audio_codec != "aac"
        or (metadata.audio_profile or "").casefold() not in {"lc", "aac lc"}
    ):
        raise MediaSafetyError("X audio codec must be AAC-LC")
    if metadata.pixel_format not in {"yuv420p", "yuvj420p"}:
        raise MediaSafetyError("X video pixel format must be YUV420")
    if not (32 <= metadata.width <= 1280 and 32 <= metadata.height <= 1024):
        raise MediaSafetyError("X video dimensions are outside the supported range")
    if not (1 / 3 <= metadata.width / metadata.height <= 3):
        raise MediaSafetyError("X video aspect ratio is outside the supported range")
    if frame_rate > 60:
        raise MediaSafetyError("X video frame rate exceeds 60 fps")
    if not ({"mp4", "mov"} & set(metadata.format_name.casefold().split(","))):
        raise MediaSafetyError("X video container must be MP4")


def _validate_instagram(descriptor: StagedMedia, metadata: MediaMetadata) -> None:
    if metadata.kind != "video":
        if metadata.format_name == "GIF":
            raise MediaSafetyError("Instagram does not accept GIF source images")
        if metadata.format_name not in {"JPEG", "PNG"}:
            raise MediaSafetyError("Instagram image format is unsupported")
        return
    duration = cast(float, metadata.duration_seconds)
    frame_rate = cast(float, metadata.frame_rate)
    if descriptor.size_bytes > _INSTAGRAM_VIDEO_MAX_BYTES:
        raise MediaSafetyError("Instagram Reel exceeds the 1 GiB limit")
    if not (3 <= duration <= 900):
        raise MediaSafetyError("Instagram Reel duration is outside the supported range")
    if metadata.codec not in {"h264", "hevc"}:
        raise MediaSafetyError("Instagram Reel video codec must be H.264 or HEVC")
    if metadata.audio_codec is not None and metadata.audio_codec != "aac":
        raise MediaSafetyError("Instagram Reel audio codec must be AAC")
    if metadata.audio_codec is not None and metadata.audio_sample_rate != 48_000:
        raise MediaSafetyError("Instagram Reel audio must use 48 kHz")
    if not (23 <= frame_rate <= 60):
        raise MediaSafetyError("Instagram Reel frame rate must be 23 to 60 fps")
    if metadata.width > 1920:
        raise MediaSafetyError("Instagram Reel horizontal dimensions exceed 1920")
    if metadata.video_bitrate is not None and metadata.video_bitrate > 25_000_000:
        raise MediaSafetyError("Instagram Reel video bitrate exceeds 25 Mbps")
    if metadata.audio_bitrate is not None and metadata.audio_bitrate > 128_000:
        raise MediaSafetyError("Instagram Reel audio bitrate exceeds 128 kbps")
    names = set(metadata.format_name.casefold().split(","))
    if not ({"mp4", "mov", "quicktime"} & names):
        raise MediaSafetyError("Instagram Reel container must be MP4 or MOV")


def _normalize_instagram_images(
    items: list[tuple[Path, StagedMedia, MediaMetadata]],
    *,
    private_root: Path,
    profile_id: str,
    bucket: SourceBucket,
    bundle_id: str,
    bundle_fingerprint: str,
    settings: InstagramSettings,
) -> tuple[list[_StagedResult], list[MediaWarning]]:
    first_metadata = items[0][2]
    ratio = min(1.91, max(4 / 5, first_metadata.width / first_metadata.height))
    results: list[_StagedResult] = []
    warnings: list[MediaWarning] = []
    for ordinal, (source, private, _metadata) in enumerate(items, start=1):
        source_private_path = private.absolute_path(
            private_root,
            expected_profile_id=profile_id,
            expected_bucket=bucket,
        )
        normalized = _normalized_jpeg(source_private_path, ratio)
        digest = hashlib.sha256(normalized).hexdigest()
        filename = _public_filename(
            profile_id, bucket, bundle_fingerprint, ordinal, digest, ".jpg"
        )
        result = _stage_bytes(
            normalized,
            root=settings.media_directory,
            profile_id=profile_id,
            bucket=bucket,
            bundle_id=bundle_id,
            bundle_fingerprint=bundle_fingerprint,
            source_name=source.name,
            ordinal=ordinal,
            mime_type="image/jpeg",
            filename=filename,
            public_url=_public_url(settings.media_base_url, filename),
        )
        results.append(result)
        if digest != private.sha256:
            warnings.append(
                MediaWarning(
                    profile_id,
                    bucket,
                    bundle_id,
                    source.name,
                    "instagram_image_normalized",
                    "Instagram image was normalized to a metadata-free JPEG.",
                )
            )
    return results, warnings


def _normalized_jpeg(path: Path, ratio: float) -> bytes:
    try:
        with Image.open(path) as source:
            oriented = ImageOps.exif_transpose(source)
            oriented.load()
            width = min(1440, max(320, oriented.width))
            height = max(1, round(width / ratio))
            if oriented.mode in {"RGBA", "LA"} or "transparency" in oriented.info:
                rgba = oriented.convert("RGBA")
                white = Image.new("RGBA", rgba.size, "white")
                white.alpha_composite(rgba)
                rgb = white.convert("RGB")
            else:
                rgb = oriented.convert("RGB")
            normalized = ImageOps.fit(
                rgb,
                (width, height),
                method=Image.Resampling.LANCZOS,
                centering=(0.5, 0.5),
            )
            for quality in range(90, 54, -5):
                output = BytesIO()
                normalized.save(
                    output,
                    format="JPEG",
                    quality=quality,
                    optimize=True,
                    progressive=False,
                    subsampling=2,
                )
                data = output.getvalue()
                if len(data) <= _INSTAGRAM_IMAGE_MAX_BYTES:
                    return data
    except OSError:
        raise MediaSafetyError("Instagram image normalization failed") from None
    raise MediaSafetyError("Instagram normalized image exceeds the 8 MiB limit")


def _public_filename(
    profile_id: str,
    bucket: SourceBucket,
    fingerprint: str,
    ordinal: int,
    sha256: str,
    suffix: str,
) -> str:
    safe_suffix = suffix if suffix in {".jpg", ".mp4", ".mov"} else ".mp4"
    return f"{profile_id}-{bucket}-{fingerprint}-{ordinal:02d}-{sha256}{safe_suffix}"


def _public_url(base_url: str, filename: str) -> str:
    if "/" in filename or "\\" in filename:
        raise MediaSafetyError("public staging filename is unsafe")
    return f"{base_url}{quote(filename, safe='')}"


def _descriptor_path(root: Path, relative_path: Path) -> Path:
    if (
        relative_path.is_absolute()
        or not relative_path.parts
        or any(part in {"", ".", ".."} for part in relative_path.parts)
        or "\\" in relative_path.as_posix()
    ):
        raise MediaSafetyError("staged media relative path is unsafe")
    root_path = root.expanduser().absolute()
    _assert_real_directory(root_path)
    candidate = root_path.joinpath(*relative_path.parts)
    if candidate.parts[: len(root_path.parts)] != root_path.parts:
        raise MediaSafetyError("staged media escaped its configured root")
    return candidate


def _validate_relative_descriptor(staged: StagedMedia) -> None:
    relative = staged.relative_path
    if (
        relative.is_absolute()
        or not relative.parts
        or any(part in {"", ".", ".."} for part in relative.parts)
        or "\\" in relative.as_posix()
    ):
        raise MediaSafetyError("staged media relative path is unsafe")
    if staged.staging_kind == "private":
        expected = (
            staged.profile_id,
            staged.source_bucket,
            staged.bundle_fingerprint,
        )
        if len(relative.parts) != 4 or relative.parts[:3] != expected:
            raise MediaSafetyError(
                "private staged media identity does not match its path"
            )
        if staged.public_url is not None:
            raise MediaSafetyError("private staged media cannot have a public URL")
        return
    prefix = f"{staged.profile_id}-{staged.source_bucket}-{staged.bundle_fingerprint}-"
    if len(relative.parts) != 1 or not relative.name.startswith(prefix):
        raise MediaSafetyError("public staged media identity does not match its path")
    if staged.public_url is None:
        raise MediaSafetyError("public staged media requires a URL")


def _assert_real_directory(path: Path) -> None:
    current = path
    checked: list[Path] = []
    while True:
        checked.append(current)
        parent = current.parent
        if parent == current:
            break
        current = parent
    for component in reversed(checked):
        try:
            metadata = os.lstat(component)
        except OSError:
            raise MediaSafetyError("staging root is unavailable") from None
        if stat.S_ISLNK(metadata.st_mode) or not stat.S_ISDIR(metadata.st_mode):
            raise MediaSafetyError("staging root contains an unsafe component")


def _validate_url_beneath_base(candidate: str, base: str) -> None:
    parsed_base = urlsplit(base)
    parsed = urlsplit(candidate)
    _validate_public_base(base, parsed_base)
    base_port = parsed_base.port or 443
    port = parsed.port or 443
    encoded_segment = parsed.path[len(parsed_base.path) :]
    decoded_segment = encoded_segment
    for _index in range(3):
        next_segment = unquote(decoded_segment)
        if next_segment == decoded_segment:
            break
        decoded_segment = next_segment
    if (
        parsed_base.scheme.casefold() != "https"
        or parsed.scheme.casefold() != "https"
        or parsed.username is not None
        or parsed.password is not None
        or "?" in candidate
        or "#" in candidate
        or parsed.query
        or parsed.fragment
        or parsed.hostname is None
        or parsed_base.hostname is None
        or parsed.hostname.rstrip(".").casefold()
        != parsed_base.hostname.rstrip(".").casefold()
        or port != base_port
        or not parsed_base.path.endswith("/")
        or not parsed.path.startswith(parsed_base.path)
        or "/" in encoded_segment
        or "\\" in decoded_segment
        or "/" in decoded_segment
        or decoded_segment in {"", ".", ".."}
    ):
        raise MediaSafetyError("public media URL escaped its configured HTTPS base")


def _validate_public_base(base: str, base_parts: SplitResult) -> None:
    hostname = base_parts.hostname
    if (
        base_parts.scheme.casefold() != "https"
        or base_parts.username is not None
        or base_parts.password is not None
        or "?" in base
        or "#" in base
        or hostname is None
        or not base_parts.path.endswith("/")
    ):
        raise MediaSafetyError("public media base URL is unsafe")
    normalized_host = hostname.rstrip(".").casefold()
    try:
        ipaddress.ip_address(normalized_host)
    except ValueError:
        if normalized_host == "localhost" or normalized_host.endswith(".localhost"):
            raise MediaSafetyError("public media base URL is unsafe") from None
    else:
        raise MediaSafetyError("public media base URL is unsafe")
    decoded_path = base_parts.path
    for _index in range(3):
        next_path = unquote(decoded_path)
        if next_path == decoded_path:
            break
        decoded_path = next_path
    if "\\" in decoded_path or any(
        segment in {".", ".."} for segment in decoded_path.split("/")
    ):
        raise MediaSafetyError("public media base URL is unsafe")


def _resolve_addresses(
    host: str, port: int, **kwargs: object
) -> Sequence[tuple[object, ...]]:
    return cast(
        "Sequence[tuple[object, ...]]",
        socket.getaddrinfo(host, port, type=cast(int, kwargs.get("type", 0))),
    )


def _validate_public_dns(url: str, resolver: Resolver) -> None:
    parsed = urlsplit(url)
    host = cast(str, parsed.hostname)
    port = parsed.port or 443
    try:
        answers = resolver(host, port, type=socket.SOCK_STREAM)
    except OSError:
        raise MediaSafetyError("public media hostname could not be resolved") from None
    if not answers:
        raise MediaSafetyError("public media hostname returned no addresses")
    for answer in answers:
        try:
            socket_address = cast(tuple[object, ...], answer[4])
            address = str(socket_address[0]).split("%", 1)[0]
            parsed_address = ipaddress.ip_address(address)
        except (IndexError, TypeError, ValueError):
            raise MediaSafetyError("public media DNS response was malformed") from None
        if not parsed_address.is_global:
            raise MediaSafetyError(
                "public media hostname resolved to a nonpublic address"
            )


def _validate_signature(mime_type: str, first: bytes, tail: bytes) -> None:
    valid = False
    if mime_type == "image/jpeg":
        valid = first.startswith(b"\xff\xd8\xff") and tail.endswith(b"\xff\xd9")
    elif mime_type == "image/png":
        valid = first.startswith(b"\x89PNG\r\n\x1a\n")
    elif mime_type == "image/gif":
        valid = first.startswith((b"GIF87a", b"GIF89a"))
    elif mime_type in {"video/mp4", "video/quicktime"}:
        valid = len(first) >= 8 and first[4:8] == b"ftyp"
    if not valid:
        raise MediaSafetyError("public media signature does not match MIME type")


def _remove_new_staging(
    results: list[_StagedResult],
    private_root: Path,
    instagram: InstagramSettings | None,
) -> None:
    for result in reversed(results):
        if not result.created:
            continue
        root = (
            private_root
            if result.descriptor.staging_kind == "private"
            else cast(InstagramSettings, instagram).media_directory
        )
        path = result.descriptor.absolute_path(root)
        try:
            metadata = os.lstat(path)
            if stat.S_ISREG(metadata.st_mode):
                path.unlink()
        except OSError:
            continue


__all__ = [
    "MediaMetadata",
    "MediaSafetyError",
    "MediaWarning",
    "PreparedMedia",
    "PreparedMediaItem",
    "PublicURLVerification",
    "StagedMedia",
    "cleanup_staged_media",
    "prepare_bundle_media",
    "verify_public_media_url",
]
