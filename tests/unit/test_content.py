from __future__ import annotations

from dataclasses import FrozenInstanceError
from pathlib import Path

import pytest

import post_pulsar.content as content_module
from post_pulsar.content import ContentBundle, InboxScan, scan_inbox


def _write(directory: Path, name: str, content: bytes = b"media") -> Path:
    path = directory / name
    path.write_bytes(content)
    return path


def _codes(scan: InboxScan) -> list[str]:
    return [issue.code for issue in scan.issues]


def test_scan_builds_exact_immutable_bundles_without_prefix_collisions(
    tmp_path: Path,
) -> None:
    _write(tmp_path, "cat.txt", b"cat caption\n")
    _write(tmp_path, "cat-alt.txt", b"cat alt\n")
    _write(tmp_path, "cat-1.png", b"one")
    _write(tmp_path, "cat-2.jpg", b"two")
    _write(tmp_path, "catalog.txt", b"catalog caption")
    _write(tmp_path, "catalog.jpg", b"catalog image")

    scan = scan_inbox(tmp_path)

    assert scan.issues == ()
    assert tuple(bundle.bundle_id for bundle in scan.bundles) == ("cat", "catalog")
    cat = scan.bundles[0]
    assert cat.caption == "cat caption"
    assert cat.alt_text == "cat alt"
    assert tuple(path.name for path in cat.images) == ("cat-1.png", "cat-2.jpg")
    assert cat.video is None
    assert tuple(path.name for path in cat.members) == (
        "cat.txt",
        "cat-alt.txt",
        "cat-1.png",
        "cat-2.jpg",
    )
    assert all(path.is_absolute() for path in cat.members)
    with pytest.raises(FrozenInstanceError):
        cat.caption = "changed"  # type: ignore[misc]


def test_fingerprint_is_versioned_deterministic_and_content_sensitive(
    tmp_path: Path,
) -> None:
    image = _write(tmp_path, "post.jpg", b"original")
    first = scan_inbox(tmp_path).bundles[0]
    second = scan_inbox(tmp_path).bundles[0]

    assert first.fingerprint == second.fingerprint
    assert len(first.fingerprint) == 64

    image.write_bytes(b"changed")
    changed = scan_inbox(tmp_path).bundles[0]
    assert changed.fingerprint != first.fingerprint

    image.write_bytes(b"original")
    image.rename(tmp_path / "renamed.jpg")
    renamed = scan_inbox(tmp_path).bundles[0]
    assert renamed.fingerprint != first.fingerprint


def test_fingerprint_hashes_the_same_text_bytes_used_by_bundle(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    caption = _write(tmp_path, "post.txt", b"captured caption")
    _write(tmp_path, "post.jpg", b"image")
    expected = scan_inbox(tmp_path).bundles[0]
    original_read = content_module._read_regular_bytes

    def capture_then_replace(path: Path) -> bytes:
        captured = original_read(path)
        if path == caption:
            path.write_bytes(b"replacement caption")
        return captured

    monkeypatch.setattr(content_module, "_read_regular_bytes", capture_then_replace)

    raced = scan_inbox(tmp_path).bundles[0]

    assert raced.caption == "captured caption"
    assert raced.fingerprint == expected.fingerprint


def test_single_video_and_optional_text_are_parsed(tmp_path: Path) -> None:
    _write(tmp_path, "clip.txt", b"caption")
    video = _write(tmp_path, "clip.mp4", b"video")

    bundle = scan_inbox(tmp_path).bundles[0]

    assert isinstance(bundle, ContentBundle)
    assert bundle.images == ()
    assert bundle.video == video.resolve()
    assert tuple(path.name for path in bundle.members) == ("clip.txt", "clip.mp4")


@pytest.mark.parametrize(
    "filename",
    [
        "CON.jpg",
        "com1.jpg",
        "LPT9.jpg",
        "-leading.jpg",
        "trailing-.jpg",
        "under_.jpg",
        "has.dot.jpg",
        "naive\N{LATIN SMALL LETTER I WITH DIAERESIS}.jpg",
        f"{'a' * 65}.jpg",
    ],
)
def test_nonportable_or_reserved_bundle_ids_are_rejected(
    tmp_path: Path, filename: str
) -> None:
    _write(tmp_path, filename)

    scan = scan_inbox(tmp_path)

    assert scan.bundles == ()
    assert "invalid_bundle_id" in _codes(scan)


@pytest.mark.parametrize("filename", ["topic-alt.jpg", "topic-12.txt"])
def test_ambiguous_role_names_are_rejected(tmp_path: Path, filename: str) -> None:
    _write(tmp_path, filename)

    scan = scan_inbox(tmp_path)

    assert scan.bundles == ()
    assert "ambiguous_filename" in _codes(scan)


def test_casefold_filename_collision_rejects_the_bundle(tmp_path: Path) -> None:
    _write(tmp_path, "post.txt", b"one")
    _write(tmp_path, "POST.TXT", b"two")
    _write(tmp_path, "post.jpg")

    scan = scan_inbox(tmp_path)

    assert scan.bundles == ()
    assert "casefold_filename_collision" in _codes(scan)


def test_casefold_bundle_id_collision_rejects_both_spellings(tmp_path: Path) -> None:
    _write(tmp_path, "post.txt", b"caption")
    _write(tmp_path, "POST.jpg")

    scan = scan_inbox(tmp_path)

    assert scan.bundles == ()
    assert "casefold_bundle_id_collision" in _codes(scan)


@pytest.mark.parametrize(
    ("names", "code"),
    [
        (("post-1.jpg", "post-3.png"), "image_sequence_gap"),
        (("post.jpg", "post-1.png"), "mixed_numbered_media"),
        (("post.jpg", "post.mp4"), "mixed_media_kinds"),
        (("post-1.mp4",), "numbered_video"),
        (("post.jpg", "post.png"), "duplicate_media_role"),
        (("post-1.jpg", "post-1.png"), "duplicate_media_role"),
    ],
)
def test_invalid_media_role_combinations_are_rejected(
    tmp_path: Path, names: tuple[str, ...], code: str
) -> None:
    for name in names:
        _write(tmp_path, name)

    scan = scan_inbox(tmp_path)

    assert scan.bundles == ()
    assert code in _codes(scan)


@pytest.mark.parametrize("name", ["post.txt", "post-alt.txt"])
def test_empty_text_roles_reject_the_bundle(tmp_path: Path, name: str) -> None:
    _write(tmp_path, name, b" \r\n\t")
    _write(tmp_path, "post.jpg")

    scan = scan_inbox(tmp_path)

    assert scan.bundles == ()
    assert "empty_text" in _codes(scan)


def test_non_utf8_text_rejects_the_bundle(tmp_path: Path) -> None:
    _write(tmp_path, "post.txt", b"\xff")
    _write(tmp_path, "post.jpg")

    scan = scan_inbox(tmp_path)

    assert scan.bundles == ()
    assert "invalid_text_encoding" in _codes(scan)


def test_unsupported_alias_invalidates_only_the_exact_bundle(tmp_path: Path) -> None:
    _write(tmp_path, "cat.jpg")
    _write(tmp_path, "cat.webp")
    _write(tmp_path, "catalog.jpg")
    _write(tmp_path, "catalog-notes.md")

    scan = scan_inbox(tmp_path)

    assert tuple(bundle.bundle_id for bundle in scan.bundles) == ("catalog",)
    assert "unsupported_bundle_alias" in _codes(scan)
    assert "unsupported_file" in _codes(scan)


def test_extensionless_exact_id_alias_invalidates_bundle(tmp_path: Path) -> None:
    _write(tmp_path, "cat.jpg")
    alias = _write(tmp_path, "cat", b"unsupported")

    scan = scan_inbox(tmp_path)

    assert scan.bundles == ()
    assert [(issue.code, issue.member) for issue in scan.issues] == [
        ("unsupported_bundle_alias", alias)
    ]


def test_visible_unsupported_files_warn_but_never_form_candidates(
    tmp_path: Path,
) -> None:
    _write(tmp_path, "notes.md", b"ignored")
    _write(tmp_path, ".DS_Store", b"hidden")

    scan = scan_inbox(tmp_path)

    assert scan.bundles == ()
    assert [(issue.severity, issue.code) for issue in scan.issues] == [
        ("warning", "unsupported_file")
    ]


def test_symlink_is_rejected_and_never_followed(tmp_path: Path) -> None:
    target = _write(tmp_path, "outside.bin", b"secret")
    (tmp_path / "post.jpg").symlink_to(target)

    scan = scan_inbox(tmp_path)

    assert scan.bundles == ()
    assert "symlink_entry" in _codes(scan)


def test_issue_order_is_deterministic(tmp_path: Path) -> None:
    _write(tmp_path, "z.md")
    _write(tmp_path, "a.md")

    first = scan_inbox(tmp_path)
    second = scan_inbox(tmp_path)

    assert first.issues == second.issues
    assert [issue.member.name for issue in first.issues] == ["a.md", "z.md"]
