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
from typing import BinaryIO, Final, Literal, Protocol

IssueSeverity = Literal["error", "warning"]

_BUNDLE_ID_RE: Final = re.compile(
    r"[A-Za-z0-9](?:[A-Za-z0-9_-]{0,62}[A-Za-z0-9])?\Z"
)
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


@dataclass(frozen=True, slots=True)
class ContentBundle:
    """A validated bundle whose exact source membership cannot change."""

    bundle_id: str
    caption: str | None
    alt_text: str | None
    images: tuple[Path, ...]
    video: Path | None
    members: tuple[Path, ...]
    fingerprint: str


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
        if path.name.casefold() in collision_names:
            potential_id = _potential_bundle_id(path.name)
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

        if path.is_symlink():
            potential_id = _potential_bundle_id(path.name)
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

        try:
            mode = path.stat(follow_symlinks=False).st_mode
        except OSError:
            issues.append(
                _issue(
                    "error",
                    "unreadable_entry",
                    path,
                    None,
                    "Inbox entry metadata could not be read.",
                )
            )
            continue
        if not stat.S_ISREG(mode):
            potential_id = _potential_bundle_id(path.name)
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
        key = potential_id.casefold() if potential_id is not None else None
        if key is not None and key in known_keys:
            invalid_bundle_keys.add(key)
            issues.append(
                _issue(
                    "error",
                    "unsupported_bundle_alias",
                    path,
                    min(spellings[key], key=lambda value: (value.casefold(), value)),
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
    ordinals = [entry.ordinal for entry in numbered]
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
        fingerprint = _fingerprint(ordered_entries, captured_text_bytes)
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
            fingerprint=fingerprint,
        ),
        (),
    )


def _fingerprint(
    entries: list[_Member], captured_bytes: Mapping[Path, bytes]
) -> str:
    digest = hashlib.sha256()
    digest.update(_FINGERPRINT_DOMAIN)
    digest.update(len(entries).to_bytes(8, "big"))
    for entry in entries:
        role = entry.role
        if role == "image":
            position = entry.ordinal if entry.ordinal is not None else 0
            role = f"image:{position}"
        _hash_field(digest, role.encode("ascii"))
        normalized_name = unicodedata.normalize("NFC", entry.path.name)
        _hash_field(digest, normalized_name.encode("utf-8"))
        captured = captured_bytes.get(entry.path)
        if captured is not None:
            digest.update(len(captured).to_bytes(8, "big"))
            digest.update(captured)
            continue
        with _open_regular(entry.path) as source:
            size = os.fstat(source.fileno()).st_size
            digest.update(size.to_bytes(8, "big"))
            while chunk := source.read(_READ_CHUNK_SIZE):
                digest.update(chunk)
    return digest.hexdigest()


def _hash_field(digest: _Hasher, value: bytes) -> None:
    digest.update(len(value).to_bytes(8, "big"))
    digest.update(value)


def _read_regular_bytes(path: Path) -> bytes:
    with _open_regular(path) as source:
        return source.read()


def _open_regular(path: Path) -> BinaryIO:
    def opener(filename: str, flags: int) -> int:
        no_follow = getattr(os, "O_NOFOLLOW", 0)
        return os.open(filename, flags | no_follow)

    source = open(path, "rb", opener=opener)
    try:
        is_regular = stat.S_ISREG(os.fstat(source.fileno()).st_mode)
    except OSError:
        source.close()
        raise
    if not is_regular:
        source.close()
        raise OSError("bundle member is not a regular file")
    return source


def _issue(
    severity: IssueSeverity,
    code: str,
    member: Path,
    bundle_id: str | None,
    message: str,
) -> InboxIssue:
    return InboxIssue(severity, code, message, bundle_id, member)


def _bundle_issue(
    code: str, bundle_id: str, member: Path, message: str
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


__all__ = ["ContentBundle", "InboxIssue", "InboxScan", "scan_inbox"]
