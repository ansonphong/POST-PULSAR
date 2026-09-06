"""Exact, deterministic content-bundle discovery for the POST PULSAR inbox."""

from __future__ import annotations

import hashlib
import os
import re
import stat
import unicodedata
from collections import defaultdict
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import BinaryIO, Final, Literal, Protocol, TypeAlias, cast

IssueSeverity = Literal["error", "warning"]
SourceBucket: TypeAlias = Literal["QUEUE", "RANDOM", "REELS"]

_BUNDLE_ID_RE: Final = re.compile(r"[A-Za-z0-9](?:[A-Za-z0-9_-]{0,62}[A-Za-z0-9])?\Z")
_ORDINAL_RE: Final = re.compile(r"(?P<bundle_id>.+)-(?P<ordinal>[0-9]+)\Z")
_AMBIGUOUS_ID_RE: Final = re.compile(r"(?:-alt|-[0-9]+)\Z", re.IGNORECASE)
_IMAGE_EXTENSIONS: Final = frozenset({".jpg", ".jpeg", ".png", ".gif"})
_VIDEO_EXTENSIONS: Final = frozenset({".mp4", ".mov"})
_SUPPORTED_EXTENSIONS: Final = _IMAGE_EXTENSIONS | _VIDEO_EXTENSIONS | {".txt"}
_WINDOWS_DEVICE_NAMES: Final = frozenset(
    {"con", "prn", "aux", "nul"}
    | {f"com{number}" for number in range(1, 10)}
    | {f"lpt{number}" for number in range(1, 10)}
)
_FINGERPRINT_DOMAIN: Final = b"POST-PULSAR-CONTENT-BUNDLE\x00V1\x00"
_READ_CHUNK_SIZE: Final = 1024 * 1024
_PUBLISHABLE_BUCKETS: Final[tuple[SourceBucket, ...]] = (
    "QUEUE",
    "RANDOM",
    "REELS",
)


@dataclass(frozen=True, slots=True)
class ContentBundle:
    """A validated bundle whose exact source membership cannot change."""

    bundle_id: str
    caption: str | None
    alt_text: str | None
    images: tuple[Path, ...]
    video: Path | None
    members: tuple[Path, ...]
    member_snapshots: tuple[ContentMemberSnapshot, ...]
    fingerprint: str


@dataclass(frozen=True, slots=True)
class ContentMemberSnapshot:
    """Hash and semantics captured from the same bytes used by the fingerprint."""

    relative_name: str
    role: str
    ordinal: int | None
    media_kind: str | None
    mime_type: str | None
    size_bytes: int
    sha256: str


@dataclass(frozen=True, slots=True)
class InboxIssue:
    """A stable, operator-safe inbox diagnostic."""

    severity: IssueSeverity
    code: str
    message: str
    bundle_id: str | None = None
    member: Path | None = None


@dataclass(frozen=True, slots=True)
class InboxScan:
    """The immutable result of one deterministic inbox scan."""

    bundles: tuple[ContentBundle, ...]
    issues: tuple[InboxIssue, ...]


@dataclass(frozen=True, slots=True)
class PublishableBundle:
    """An exact semantic bundle admitted beneath one editorial bucket."""

    bucket: SourceBucket
    directory: Path
    ready_marker: Path
    content: ContentBundle

    @property
    def bundle_id(self) -> str:
        """Return the stable semantic bundle ID."""
        return self.content.bundle_id

    @property
    def fingerprint(self) -> str:
        """Return the fingerprint that deliberately excludes ``.ready``."""
        return self.content.fingerprint


@dataclass(frozen=True, slots=True)
class AccountScan:
    """One fail-closed scan of all publishable buckets for an account root."""

    bundles: tuple[PublishableBundle, ...]
    issues: tuple[InboxIssue, ...]

    def for_bucket(self, bucket: SourceBucket) -> tuple[PublishableBundle, ...]:
        """Return deterministic candidates for one supported bucket."""
        return tuple(item for item in self.bundles if item.bucket == bucket)


@dataclass(frozen=True, slots=True)
class _Member:
    path: Path
    bundle_id: str
    role: Literal["caption", "alt_text", "image", "video"]
    ordinal: int | None = None


class _Hasher(Protocol):
    def update(self, data: bytes) -> object:
        """Add bytes to the digest state."""


def scan_inbox(directory: str | os.PathLike[str]) -> InboxScan:
    """Discover every valid exact bundle under *directory* without using globs."""

    supplied_root = Path(directory).expanduser()
    if supplied_root.is_symlink():
        msg = "The inbox directory itself must not be a symbolic link."
        raise ValueError(msg)
    root = supplied_root.resolve(strict=True)
    if not root.is_dir():
        raise NotADirectoryError(root)

    issues: list[InboxIssue] = []
    invalid_bundle_keys: set[str] = set()
    candidates: dict[str, list[_Member]] = defaultdict(list)
    spellings: dict[str, set[str]] = defaultdict(set)
    unsupported: list[Path] = []

    paths = sorted(
        (path for path in root.iterdir() if not path.name.startswith(".")),
        key=lambda path: (path.name.casefold(), path.name),
    )
    collision_names = _find_casefold_filename_collisions(paths)

    for path in paths:
        potential_id = _potential_bundle_id(path.name)
        if path.name.casefold() in collision_names:
            if potential_id is not None:
                invalid_bundle_keys.add(potential_id.casefold())
            issues.append(
                _issue(
                    "error",
                    "casefold_filename_collision",
                    path,
                    potential_id,
                    "Filename collides case-insensitively with another inbox entry.",
                )
            )
            continue

        try:
            mode = path.stat(follow_symlinks=False).st_mode
        except OSError:
            if potential_id is not None:
                invalid_bundle_keys.add(potential_id.casefold())
            issues.append(
                _issue(
                    "error",
                    "unreadable_entry",
                    path,
                    potential_id,
                    "Inbox entry metadata could not be read.",
                )
            )
            continue
        if stat.S_ISLNK(mode):
            if potential_id is not None:
                invalid_bundle_keys.add(potential_id.casefold())
            issues.append(
                _issue(
                    "error",
                    "symlink_entry",
                    path,
                    potential_id,
                    "Symbolic links are not accepted as bundle members.",
                )
            )
            continue
        if not stat.S_ISREG(mode):
            if potential_id is not None:
                invalid_bundle_keys.add(potential_id.casefold())
            issues.append(
                _issue(
                    "error",
                    "non_regular_entry",
                    path,
                    potential_id,
                    "Only regular files are accepted as bundle members.",
                )
            )
            continue

        parsed, parse_issue = _parse_member(path)
        if parse_issue is not None:
            issues.append(parse_issue)
            if parse_issue.bundle_id is not None:
                invalid_bundle_keys.add(parse_issue.bundle_id.casefold())
            continue
        if parsed is None:
            unsupported.append(path)
            continue

        key = parsed.bundle_id.casefold()
        candidates[key].append(parsed)
        spellings[key].add(parsed.bundle_id)

    for key, names in spellings.items():
        if len(names) <= 1:
            continue
        invalid_bundle_keys.add(key)
        first = min(candidates[key], key=lambda member: member.path.name).path
        issues.append(
            _issue(
                "error",
                "casefold_bundle_id_collision",
                first,
                min(names),
                "Bundle ID collides case-insensitively with another spelling.",
            )
        )

    known_keys = set(candidates)
    for path in unsupported:
        potential_id = _potential_bundle_id(path.name)
        alias_key = potential_id.casefold() if potential_id is not None else None
        if alias_key is not None and alias_key in known_keys:
            invalid_bundle_keys.add(alias_key)
            issues.append(
                _issue(
                    "error",
                    "unsupported_bundle_alias",
                    path,
                    min(
                        spellings[alias_key],
                        key=lambda value: (value.casefold(), value),
                    ),
                    "Unsupported file aliases a recognized bundle role.",
                )
            )
        else:
            issues.append(
                _issue(
                    "warning",
                    "unsupported_file",
                    path,
                    None,
                    "Unsupported visible file was ignored.",
                )
            )

    bundles: list[ContentBundle] = []
    for key in sorted(candidates):
        if key in invalid_bundle_keys:
            continue
        bundle, bundle_issues = _build_bundle(candidates[key])
        issues.extend(bundle_issues)
        if bundle is not None:
            bundles.append(bundle)

    bundles.sort(key=lambda bundle: (bundle.bundle_id.casefold(), bundle.bundle_id))
    issues.sort(key=_issue_sort_key)
    return InboxScan(bundles=tuple(bundles), issues=tuple(issues))


def scan_account_root(
    directory: str | os.PathLike[str],
    *,
    buckets: tuple[SourceBucket, ...] | None = None,
) -> AccountScan:
    """Discover ready bundle directories in all or selected publishable buckets."""

    supplied_root = Path(directory).expanduser()
    if supplied_root.is_symlink():
        msg = "The account root itself must not be a symbolic link."
        raise ValueError(msg)
    root = supplied_root.resolve(strict=True)
    if not root.is_dir():
        raise NotADirectoryError(root)
    root_before = os.lstat(root)
    if not stat.S_ISDIR(root_before.st_mode):
        raise NotADirectoryError(root)

    selected_buckets = _PUBLISHABLE_BUCKETS if buckets is None else buckets
    if not selected_buckets or any(
        bucket not in _PUBLISHABLE_BUCKETS for bucket in selected_buckets
    ):
        raise ValueError("publishable bucket selection is invalid")
    selected_buckets = tuple(dict.fromkeys(selected_buckets))

    issues: list[InboxIssue] = []
    candidates: list[PublishableBundle] = []
    bucket_snapshots: list[tuple[Path, os.stat_result]] = []
    misplaced_ready = root / ".ready"
    misplaced_ready_exists = False
    try:
        os.lstat(misplaced_ready)
    except FileNotFoundError:
        misplaced_ready_exists = False
    except OSError:
        issues.append(
            _bucket_issue(
                "unreadable_bundle_entry",
                misplaced_ready,
                None,
                "Account-root entry metadata could not be read.",
            )
        )
    else:
        misplaced_ready_exists = True
    if misplaced_ready_exists:
        issues.append(
            _bucket_issue(
                "misplaced_ready_marker",
                misplaced_ready,
                None,
                "The ready marker is valid only inside a bundle directory.",
            )
        )
    for bucket in selected_buckets:
        bucket_path = root / bucket
        try:
            bucket_before = os.lstat(bucket_path)
        except FileNotFoundError:
            continue
        except OSError:
            issues.append(
                _bucket_issue(
                    "unreadable_bucket",
                    bucket_path,
                    None,
                    "Publishable bucket metadata could not be read.",
                )
            )
            continue
        if stat.S_ISLNK(bucket_before.st_mode) or not stat.S_ISDIR(
            bucket_before.st_mode
        ):
            issues.append(
                _bucket_issue(
                    "unsafe_bucket",
                    bucket_path,
                    None,
                    "A publishable bucket must be a real directory.",
                )
            )
            continue
        bucket_snapshots.append((bucket_path, bucket_before))

        try:
            entries = sorted(
                bucket_path.iterdir(),
                key=lambda path: (path.name.casefold(), path.name),
            )
        except OSError:
            issues.append(
                _bucket_issue(
                    "unreadable_bucket",
                    bucket_path,
                    None,
                    "Publishable bucket contents could not be enumerated.",
                )
            )
            continue
        collisions = _find_casefold_filename_collisions(entries)
        for bundle_directory in entries:
            if bundle_directory.name.casefold() in collisions:
                issues.append(
                    _bucket_issue(
                        "casefold_bundle_directory_collision",
                        bundle_directory,
                        bundle_directory.name,
                        "Bundle directory collides case-insensitively in its bucket.",
                    )
                )
                continue
            candidate, candidate_issues = _scan_bundle_directory(
                bucket, bundle_directory
            )
            issues.extend(candidate_issues)
            if candidate is not None:
                candidates.append(candidate)

    by_id: dict[str, list[PublishableBundle]] = defaultdict(list)
    for candidate in candidates:
        by_id[candidate.bundle_id.casefold()].append(candidate)
    for duplicates in by_id.values():
        if len(duplicates) <= 1:
            continue
        first = min(duplicates, key=lambda item: (item.bucket, item.bundle_id))
        issues.append(
            _bucket_issue(
                "duplicate_bundle_across_buckets",
                first.directory,
                first.bundle_id,
                "A bundle ID may appear in only one publishable bucket.",
            )
        )

    issues.extend(_scan_generation_issues(root, root_before, bucket_snapshots))
    issues.sort(key=_issue_sort_key)
    if any(issue.severity == "error" for issue in issues):
        return AccountScan(bundles=(), issues=tuple(issues))
    bucket_order = {name: index for index, name in enumerate(selected_buckets)}
    candidates.sort(
        key=lambda item: (
            bucket_order[item.bucket],
            item.bundle_id.casefold(),
            item.bundle_id,
        )
    )
    return AccountScan(bundles=tuple(candidates), issues=tuple(issues))


def _scan_generation_issues(
    root: Path,
    root_before: os.stat_result,
    bucket_snapshots: list[tuple[Path, os.stat_result]],
) -> tuple[InboxIssue, ...]:
    issues: list[InboxIssue] = []
    try:
        root_after = os.lstat(root)
    except OSError:
        root_after = None
    if root_after is None or not (
        stat.S_ISDIR(root_after.st_mode)
        and _same_file_identity(root_before, root_after)
    ):
        issues.append(
            _bucket_issue(
                "account_root_changed",
                root,
                None,
                "Account root changed during publishable-bucket discovery.",
            )
        )

    for bucket_path, bucket_before in bucket_snapshots:
        try:
            bucket_after = os.lstat(bucket_path)
        except OSError:
            bucket_after = None
        if bucket_after is None or not (
            stat.S_ISDIR(bucket_after.st_mode)
            and _same_file_identity(bucket_before, bucket_after)
        ):
            issues.append(
                _bucket_issue(
                    "bucket_changed",
                    bucket_path,
                    None,
                    "Publishable bucket changed during discovery.",
                )
            )
    return tuple(issues)


def _scan_bundle_directory(
    bucket: SourceBucket, bundle_directory: Path
) -> tuple[PublishableBundle | None, tuple[InboxIssue, ...]]:
    bundle_id = bundle_directory.name
    issues: list[InboxIssue] = []
    try:
        directory_before = os.lstat(bundle_directory)
    except OSError:
        return None, (
            _bucket_issue(
                "unreadable_bundle_directory",
                bundle_directory,
                bundle_id,
                "Bundle directory metadata could not be read.",
            ),
        )
    if stat.S_ISLNK(directory_before.st_mode):
        return None, (
            _bucket_issue(
                "symlink_bundle_directory",
                bundle_directory,
                bundle_id,
                "Symbolic links cannot be publishable bundle directories.",
            ),
        )
    if not stat.S_ISDIR(directory_before.st_mode):
        return None, (
            _bucket_issue(
                "non_directory_bundle_entry",
                bundle_directory,
                bundle_id,
                "Publishable buckets contain only bundle directories.",
            ),
        )
    if not _valid_bundle_id(bundle_id):
        return None, (
            _bucket_issue(
                "invalid_bundle_directory_id",
                bundle_directory,
                bundle_id,
                "Bundle directory name is not a portable bundle ID.",
            ),
        )

    ready_marker = bundle_directory / ".ready"
    ready_before: os.stat_result | None = None
    try:
        entries = tuple(bundle_directory.iterdir())
    except OSError:
        return None, (
            _bucket_issue(
                "unreadable_bundle_directory",
                bundle_directory,
                bundle_id,
                "Bundle directory contents could not be enumerated.",
            ),
        )
    for entry in entries:
        try:
            entry_stat = os.lstat(entry)
        except OSError:
            issues.append(
                _bucket_issue(
                    "unreadable_bundle_entry",
                    entry,
                    bundle_id,
                    "Bundle entry metadata could not be read.",
                )
            )
            continue
        if entry.name == ".ready":
            if stat.S_ISREG(entry_stat.st_mode) and entry_stat.st_size == 0:
                ready_before = entry_stat
            else:
                issues.append(
                    _bucket_issue(
                        "unsafe_ready_marker",
                        entry,
                        bundle_id,
                        "The canonical ready marker must be an empty regular file.",
                    )
                )
            continue
        if stat.S_ISDIR(entry_stat.st_mode):
            issues.append(
                _bucket_issue(
                    "nested_bundle_entry",
                    entry,
                    bundle_id,
                    "Bundle directories cannot contain nested directories.",
                )
            )
        elif entry.name.startswith("."):
            issues.append(
                _bucket_issue(
                    "unexpected_operational_entry",
                    entry,
                    bundle_id,
                    "Only the canonical .ready operational entry is accepted.",
                )
            )
    if ready_before is None and not any(
        issue.code == "unsafe_ready_marker" for issue in issues
    ):
        issues.append(
            _bucket_issue(
                "missing_ready_marker",
                ready_marker,
                bundle_id,
                "Publishable bundles require a canonical .ready marker.",
            )
        )
    if issues:
        return None, tuple(issues)

    try:
        flat_scan = scan_inbox(bundle_directory)
    except (OSError, ValueError):
        return None, (
            _bucket_issue(
                "bundle_directory_changed",
                bundle_directory,
                bundle_id,
                "Bundle directory changed during discovery.",
            ),
        )
    if flat_scan.issues:
        return None, flat_scan.issues
    if len(flat_scan.bundles) != 1:
        return None, (
            _bucket_issue(
                "bundle_count_mismatch",
                bundle_directory,
                bundle_id,
                "A bundle directory must contain exactly one semantic bundle.",
            ),
        )
    content = flat_scan.bundles[0]
    if content.bundle_id != bundle_id:
        return None, (
            _bucket_issue(
                "bundle_id_mismatch",
                bundle_directory,
                bundle_id,
                "Container and parsed semantic bundle IDs must match exactly.",
            ),
        )
    if bucket == "REELS" and content.video is None:
        return None, (
            _bucket_issue(
                "reels_requires_video",
                bundle_directory,
                bundle_id,
                "REELS accepts exactly one video bundle.",
            ),
        )

    try:
        directory_after = os.lstat(bundle_directory)
        ready_after = os.lstat(ready_marker)
    except OSError:
        return None, (
            _bucket_issue(
                "ready_marker_changed",
                ready_marker,
                bundle_id,
                "Ready marker or bundle directory changed during discovery.",
            ),
        )
    if not (
        stat.S_ISDIR(directory_after.st_mode)
        and stat.S_ISREG(ready_after.st_mode)
        and ready_after.st_size == 0
        and _same_file_identity(directory_before, directory_after)
        and _same_file_identity(cast(os.stat_result, ready_before), ready_after)
    ):
        return None, (
            _bucket_issue(
                "ready_marker_changed",
                ready_marker,
                bundle_id,
                "Ready marker or bundle directory changed during discovery.",
            ),
        )
    return (
        PublishableBundle(
            bucket=bucket,
            directory=bundle_directory,
            ready_marker=ready_marker,
            content=content,
        ),
        (),
    )


def _find_casefold_filename_collisions(paths: list[Path]) -> set[str]:
    by_name: dict[str, int] = defaultdict(int)
    for path in paths:
        by_name[path.name.casefold()] += 1
    return {name for name, count in by_name.items() if count > 1}


def _parse_member(path: Path) -> tuple[_Member | None, InboxIssue | None]:
    suffix = path.suffix
    stem = path.stem
    if suffix not in _SUPPORTED_EXTENSIONS:
        return None, None

    alt_base = stem[:-4] if stem.casefold().endswith("-alt") else None
    ordinal_match = _ORDINAL_RE.fullmatch(stem)

    if suffix == ".txt" and alt_base is not None:
        return _validated_member(path, alt_base, "alt_text")
    if suffix == ".txt" and ordinal_match is not None:
        bundle_id = ordinal_match.group("bundle_id")
        return None, _issue(
            "error",
            "ambiguous_filename",
            path,
            bundle_id,
            "A numbered role must use a supported image extension.",
        )
    if suffix == ".txt":
        return _validated_member(path, stem, "caption")

    if suffix in _IMAGE_EXTENSIONS and alt_base is not None:
        return None, _issue(
            "error",
            "ambiguous_filename",
            path,
            alt_base,
            "The -alt role must use the .txt extension.",
        )
    if suffix in _IMAGE_EXTENSIONS and ordinal_match is not None:
        bundle_id = ordinal_match.group("bundle_id")
        ordinal = int(ordinal_match.group("ordinal"))
        return _validated_member(path, bundle_id, "image", ordinal)
    if suffix in _IMAGE_EXTENSIONS:
        return _validated_member(path, stem, "image")

    if alt_base is not None:
        return None, _issue(
            "error",
            "ambiguous_filename",
            path,
            alt_base,
            "The -alt role must use the .txt extension.",
        )
    if ordinal_match is not None:
        bundle_id = ordinal_match.group("bundle_id")
        return None, _issue(
            "error",
            "numbered_video",
            path,
            bundle_id,
            "Video filenames cannot use a numeric sequence suffix.",
        )
    return _validated_member(path, stem, "video")


def _validated_member(
    path: Path,
    bundle_id: str,
    role: Literal["caption", "alt_text", "image", "video"],
    ordinal: int | None = None,
) -> tuple[_Member | None, InboxIssue | None]:
    if not _valid_bundle_id(bundle_id):
        return None, _issue(
            "error",
            "invalid_bundle_id",
            path,
            bundle_id,
            "Bundle ID is ambiguous, reserved, or not portable.",
        )
    return _Member(path, bundle_id, role, ordinal), None


def _valid_bundle_id(bundle_id: str) -> bool:
    return bool(
        _BUNDLE_ID_RE.fullmatch(bundle_id)
        and not _AMBIGUOUS_ID_RE.search(bundle_id)
        and bundle_id.casefold() not in _WINDOWS_DEVICE_NAMES
    )


def _potential_bundle_id(filename: str) -> str | None:
    path = Path(filename)
    stem = path.stem
    if not stem:
        return None
    if stem.casefold().endswith("-alt"):
        return stem[:-4] or None
    ordinal_match = _ORDINAL_RE.fullmatch(stem)
    if ordinal_match is not None:
        return ordinal_match.group("bundle_id")
    return stem


def _build_bundle(
    entries: list[_Member],
) -> tuple[ContentBundle | None, tuple[InboxIssue, ...]]:
    bundle_id = entries[0].bundle_id
    issues: list[InboxIssue] = []
    by_role: dict[str, list[_Member]] = defaultdict(list)
    for entry in entries:
        by_role[entry.role].append(entry)

    for text_role in ("caption", "alt_text"):
        if len(by_role[text_role]) > 1:
            issues.append(
                _bundle_issue(
                    "duplicate_text_role",
                    bundle_id,
                    by_role[text_role][0].path,
                    f"Bundle contains more than one {text_role} file.",
                )
            )

    images = by_role["image"]
    videos = by_role["video"]
    numbered = [entry for entry in images if entry.ordinal is not None]
    unnumbered = [entry for entry in images if entry.ordinal is None]

    if videos and images:
        issues.append(
            _bundle_issue(
                "mixed_media_kinds",
                bundle_id,
                min((*videos, *images), key=lambda item: item.path.name).path,
                "Images and video cannot be mixed in one bundle.",
            )
        )
    if len(videos) > 1:
        issues.append(
            _bundle_issue(
                "duplicate_media_role",
                bundle_id,
                videos[0].path,
                "Bundle contains more than one video.",
            )
        )
    if numbered and unnumbered:
        issues.append(
            _bundle_issue(
                "mixed_numbered_media",
                bundle_id,
                min(images, key=lambda item: item.path.name).path,
                "Numbered and unnumbered images cannot be mixed.",
            )
        )
    if len(unnumbered) > 1:
        issues.append(
            _bundle_issue(
                "duplicate_media_role",
                bundle_id,
                unnumbered[0].path,
                "Bundle contains more than one unnumbered image.",
            )
        )
    ordinals = [cast(int, entry.ordinal) for entry in numbered]
    if len(ordinals) != len(set(ordinals)):
        issues.append(
            _bundle_issue(
                "duplicate_media_role",
                bundle_id,
                numbered[0].path,
                "Image sequence contains a duplicate ordinal.",
            )
        )
    elif ordinals and sorted(ordinals) != list(range(1, len(ordinals) + 1)):
        issues.append(
            _bundle_issue(
                "image_sequence_gap",
                bundle_id,
                numbered[0].path,
                "Image sequence must be contiguous and begin at 1.",
            )
        )
    if not images and not videos:
        issues.append(
            _bundle_issue(
                "missing_media",
                bundle_id,
                entries[0].path,
                "Bundle contains no supported media.",
            )
        )

    text: dict[str, str | None] = {"caption": None, "alt_text": None}
    captured_text_bytes: dict[Path, bytes] = {}
    for role in ("caption", "alt_text"):
        role_entries = by_role[role]
        if len(role_entries) != 1:
            continue
        entry = role_entries[0]
        try:
            captured = _read_regular_bytes(entry.path)
            captured_text_bytes[entry.path] = captured
            value = captured.decode("utf-8")
        except UnicodeDecodeError:
            issues.append(
                _bundle_issue(
                    "invalid_text_encoding",
                    bundle_id,
                    entry.path,
                    "Text roles must contain valid UTF-8.",
                )
            )
            continue
        except OSError:
            issues.append(
                _bundle_issue(
                    "unreadable_member",
                    bundle_id,
                    entry.path,
                    "Bundle member could not be read safely.",
                )
            )
            continue
        value = value.strip()
        if not value:
            issues.append(
                _bundle_issue(
                    "empty_text",
                    bundle_id,
                    entry.path,
                    "Text roles cannot be empty or whitespace-only.",
                )
            )
        else:
            text[role] = value

    if issues:
        return None, tuple(issues)

    ordered_images = tuple(
        entry.path
        for entry in sorted(
            images,
            key=lambda entry: (
                entry.ordinal if entry.ordinal is not None else 0,
                entry.path.name,
            ),
        )
    )
    caption_entries = by_role["caption"]
    alt_entries = by_role["alt_text"]
    ordered_entries = [*caption_entries, *alt_entries]
    ordered_entries.extend(
        sorted(
            images,
            key=lambda entry: (
                entry.ordinal if entry.ordinal is not None else 0,
                entry.path.name,
            ),
        )
    )
    ordered_entries.extend(videos)
    members = tuple(entry.path for entry in ordered_entries)
    try:
        fingerprint, member_snapshots = _fingerprint(
            ordered_entries, captured_text_bytes
        )
    except OSError:
        issue = _bundle_issue(
            "unreadable_member",
            bundle_id,
            ordered_entries[0].path,
            "Bundle member changed or could not be read safely.",
        )
        return None, (issue,)

    return (
        ContentBundle(
            bundle_id=bundle_id,
            caption=text["caption"],
            alt_text=text["alt_text"],
            images=ordered_images,
            video=videos[0].path if videos else None,
            members=members,
            member_snapshots=member_snapshots,
            fingerprint=fingerprint,
        ),
        (),
    )


def _fingerprint(
    entries: list[_Member], captured_bytes: Mapping[Path, bytes]
) -> tuple[str, tuple[ContentMemberSnapshot, ...]]:
    digest = hashlib.sha256()
    snapshots: list[ContentMemberSnapshot] = []
    digest.update(_FINGERPRINT_DOMAIN)
    digest.update(len(entries).to_bytes(8, "big"))
    for entry in entries:
        role_label: str = entry.role
        if role_label == "image":
            position = entry.ordinal if entry.ordinal is not None else 0
            role_label = f"image:{position}"
        _hash_field(digest, role_label.encode("ascii"))
        normalized_name = unicodedata.normalize("NFC", entry.path.name)
        _hash_field(digest, normalized_name.encode("utf-8"))
        captured = captured_bytes.get(entry.path)
        if captured is not None:
            digest.update(len(captured).to_bytes(8, "big"))
            digest.update(captured)
            member_sha = hashlib.sha256(captured).hexdigest()
            size = len(captured)
        else:
            member_digest = hashlib.sha256()
            with _open_regular(entry.path) as source:
                before = os.fstat(source.fileno())
                size = before.st_size
                digest.update(size.to_bytes(8, "big"))
                observed_size = 0
                while chunk := source.read(_READ_CHUNK_SIZE):
                    digest.update(chunk)
                    member_digest.update(chunk)
                    observed_size += len(chunk)
                _verify_stable_read(entry.path, source, before, observed_size)
            member_sha = member_digest.hexdigest()
        media_kind = entry.role if entry.role in {"image", "video"} else None
        snapshots.append(
            ContentMemberSnapshot(
                relative_name=entry.path.name,
                role=entry.role,
                ordinal=entry.ordinal,
                media_kind=media_kind,
                mime_type=_member_mime_type(entry),
                size_bytes=size,
                sha256=member_sha,
            )
        )
    return digest.hexdigest(), tuple(snapshots)


def _member_mime_type(entry: _Member) -> str | None:
    if entry.role in {"caption", "alt_text"}:
        return "text/plain"
    return {
        ".jpg": "image/jpeg",
        ".jpeg": "image/jpeg",
        ".png": "image/png",
        ".gif": "image/gif",
        ".mp4": "video/mp4",
        ".mov": "video/quicktime",
    }.get(entry.path.suffix.casefold())


def _hash_field(digest: _Hasher, value: bytes) -> None:
    digest.update(len(value).to_bytes(8, "big"))
    digest.update(value)


def _read_regular_bytes(path: Path) -> bytes:
    with _open_regular(path) as source:
        before = os.fstat(source.fileno())
        value = source.read()
        _verify_stable_read(path, source, before, len(value))
        return value


def _verify_stable_read(
    path: Path,
    source: BinaryIO,
    before: os.stat_result,
    observed_size: int,
) -> None:
    """Bind byte count and post-read identity to the same held stream."""

    opened_after = os.fstat(source.fileno())
    path_after = os.lstat(path)
    if not (
        stat.S_ISREG(opened_after.st_mode)
        and stat.S_ISREG(path_after.st_mode)
        and _same_file_identity(before, opened_after)
        and _same_file_identity(opened_after, path_after)
        and observed_size == before.st_size == opened_after.st_size == path_after.st_size
        and before.st_mtime_ns == opened_after.st_mtime_ns == path_after.st_mtime_ns
        and before.st_ctime_ns == opened_after.st_ctime_ns == path_after.st_ctime_ns
    ):
        raise OSError("bundle member changed while being captured")


def _open_regular(path: Path) -> BinaryIO:
    before = os.lstat(path)
    if not stat.S_ISREG(before.st_mode):
        raise OSError("bundle member is not a regular file")

    no_follow = getattr(os, "O_NOFOLLOW", None)

    def opener(filename: str, flags: int) -> int:
        guarded_flags = flags if no_follow is None else flags | no_follow
        return os.open(filename, guarded_flags)

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
        and _same_file_identity(before, opened)
        and _same_file_identity(opened, after)
    ):
        source.close()
        raise OSError("bundle member changed or is not a regular file")
    return source


def _same_file_identity(left: os.stat_result, right: os.stat_result) -> bool:
    """Compare open-path identities, failing closed if a platform has no identity."""

    left_identity = (left.st_dev, left.st_ino)
    right_identity = (right.st_dev, right.st_ino)
    return (
        left_identity != (0, 0)
        and right_identity != (0, 0)
        and os.path.samestat(left, right)
    )


def _issue(
    severity: IssueSeverity,
    code: str,
    member: Path,
    bundle_id: str | None,
    message: str,
) -> InboxIssue:
    return InboxIssue(severity, code, message, bundle_id, member)


def _bundle_issue(code: str, bundle_id: str, member: Path, message: str) -> InboxIssue:
    return _issue("error", code, member, bundle_id, message)


def _bucket_issue(
    code: str, member: Path, bundle_id: str | None, message: str
) -> InboxIssue:
    return _issue("error", code, member, bundle_id, message)


def _issue_sort_key(issue: InboxIssue) -> tuple[str, str, int, str, str]:
    member_name = issue.member.name if issue.member is not None else ""
    severity_order = 0 if issue.severity == "error" else 1
    return (
        member_name.casefold(),
        member_name,
        severity_order,
        issue.code,
        issue.bundle_id or "",
    )


__all__ = [
    "AccountScan",
    "ContentBundle",
    "ContentMemberSnapshot",
    "InboxIssue",
    "InboxScan",
    "PublishableBundle",
    "scan_account_root",
    "scan_inbox",
]
