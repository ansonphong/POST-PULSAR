"""Profile identity and publishable-bucket discovery tests."""

from __future__ import annotations

import dataclasses
import os
from pathlib import Path

import pytest

import post_pulsar.content as content_module
from post_pulsar.config import ConfigurationError, load_local_settings
from post_pulsar.content import scan_account_root, scan_inbox


def _write_config(tmp_path: Path, profiles: str) -> Path:
    path = tmp_path / "post-pulsar.toml"
    path.write_text(
        f"""
[app]
state_directory = ".post-pulsar"
log_file = ".post-pulsar/post_pulsar.log"
deployment_mode = "hardened"
control_host = "::1"
control_port = 8765
agent_capability_file = ".post-pulsar/agent-capability"
operator_verifier_file = ".post-pulsar/operator-verifier"
bootstrap_file = ".post-pulsar/bootstrap.json"
endpoint_record_file = ".post-pulsar/endpoint.json"

{profiles}
""".strip()
        + "\n",
        encoding="utf-8",
    )
    return path


def _x_profile(
    profile_id: str,
    root: str,
    remote_id: str,
    username: str,
    env_var: str,
) -> str:
    return f"""
[[profiles]]
profile_id = "{profile_id}"
account_root = "{root}"
timezone = "America/Vancouver"

[profiles.x]
enabled = false
expected_remote_user_id = "{remote_id}"
expected_username = "{username}"
token_env_var = "{env_var}"
"""


def _member(directory: Path, name: str, value: bytes = b"media") -> Path:
    directory.mkdir(parents=True, exist_ok=True)
    path = directory / name
    path.write_bytes(value)
    return path


def _ready_bundle(root: Path, bucket: str, bundle_id: str, media: str) -> Path:
    directory = root / bucket / bundle_id
    _member(directory, media)
    _member(directory, ".ready", b"")
    return directory


def _codes(scan: object) -> list[str]:
    return [issue.code for issue in scan.issues]  # type: ignore[attr-defined]


def test_two_profiles_have_isolated_roots_identities_and_secret_references(
    tmp_path: Path,
) -> None:
    profiles = _x_profile(
        "ansonphong",
        "accounts/ansonphong",
        "10001",
        "ansonphong",
        "POST_PULSAR_X_ANSONPHONG_USER_ACCESS_TOKEN",
    ) + _x_profile(
        "360hextile",
        "accounts/360hextile",
        "10002",
        "360hextile",
        "POST_PULSAR_X_360HEXTILE_USER_ACCESS_TOKEN",
    )

    settings = load_local_settings(_write_config(tmp_path, profiles))

    assert tuple(profile.profile_id for profile in settings.profiles) == (
        "ansonphong",
        "360hextile",
    )
    assert (
        settings.profile("360hextile").account_root
        == (tmp_path / "accounts/360hextile").resolve()
    )
    with pytest.raises(dataclasses.FrozenInstanceError):
        settings.profiles[0].timezone = "UTC"  # type: ignore[misc]


@pytest.mark.parametrize(
    ("second_id", "second_root", "second_remote", "second_env", "match"),
    [
        (
            "ansonphong",
            "accounts/two",
            "10002",
            "POST_PULSAR_X_TWO_USER_ACCESS_TOKEN",
            "profile_id",
        ),
        (
            "second",
            "accounts/ANSONPHONG/sub",
            "10002",
            "POST_PULSAR_X_TWO_USER_ACCESS_TOKEN",
            "overlap",
        ),
        (
            "second",
            "accounts/two",
            "10001",
            "POST_PULSAR_X_TWO_USER_ACCESS_TOKEN",
            "remote identity",
        ),
        (
            "second",
            "accounts/two",
            "10002",
            "POST_PULSAR_X_ANSONPHONG_USER_ACCESS_TOKEN",
            "environment variable",
        ),
    ],
)
def test_profile_identity_root_remote_and_environment_collisions_are_rejected(
    tmp_path: Path,
    second_id: str,
    second_root: str,
    second_remote: str,
    second_env: str,
    match: str,
) -> None:
    profiles = _x_profile(
        "ansonphong",
        "accounts/ansonphong",
        "10001",
        "ansonphong",
        "POST_PULSAR_X_ANSONPHONG_USER_ACCESS_TOKEN",
    ) + _x_profile(second_id, second_root, second_remote, "second", second_env)

    with pytest.raises(ConfigurationError, match=match):
        load_local_settings(_write_config(tmp_path, profiles))


@pytest.mark.parametrize(
    ("profile_id", "timezone", "env_var", "match"),
    [
        ("Upper", "UTC", "POST_PULSAR_X_UPPER_USER_ACCESS_TOKEN", "profile_id"),
        ("valid", "Mars/Olympus", "POST_PULSAR_X_VALID_USER_ACCESS_TOKEN", "timezone"),
        ("valid", "UTC", "HOME", "token_env_var"),
        (
            "valid",
            "UTC",
            "POST_PULSAR_INSTAGRAM_VALID_ACCESS_TOKEN",
            "token_env_var",
        ),
    ],
)
def test_profile_ids_timezones_and_token_references_are_strict(
    tmp_path: Path,
    profile_id: str,
    timezone: str,
    env_var: str,
    match: str,
) -> None:
    profile = _x_profile(
        profile_id,
        "accounts/valid",
        "10001",
        "valid",
        env_var,
    ).replace('timezone = "America/Vancouver"', f'timezone = "{timezone}"')

    with pytest.raises(ConfigurationError, match=match):
        load_local_settings(_write_config(tmp_path, profile))


def test_existing_symlink_account_root_is_rejected(tmp_path: Path) -> None:
    outside = tmp_path / "outside"
    outside.mkdir()
    accounts = tmp_path / "accounts"
    accounts.mkdir()
    (accounts / "ansonphong").symlink_to(outside, target_is_directory=True)
    profile = _x_profile(
        "ansonphong",
        "accounts/ansonphong",
        "10001",
        "ansonphong",
        "POST_PULSAR_X_ANSONPHONG_USER_ACCESS_TOKEN",
    )

    with pytest.raises(ConfigurationError, match="symbolic link"):
        load_local_settings(_write_config(tmp_path, profile))


def test_publishable_buckets_require_ready_and_exclude_drafts(tmp_path: Path) -> None:
    root = tmp_path / "accounts/ansonphong"
    draft = _ready_bundle(root / "DRAFTS", "QUEUE", "draft", "draft.jpg")
    queue = _ready_bundle(root, "QUEUE", "Zulu", "Zulu.jpg")
    _ready_bundle(root, "QUEUE", "alpha", "alpha.jpg")
    _ready_bundle(root, "RANDOM", "Zulu-random", "Zulu-random.png")
    _ready_bundle(root, "RANDOM", "alpha-random", "alpha-random.png")
    _ready_bundle(root, "REELS", "Zulu-reel", "Zulu-reel.mp4")
    _ready_bundle(root, "REELS", "alpha-reel", "alpha-reel.mp4")

    scan = scan_account_root(root)

    assert scan.issues == ()
    assert [item.bundle_id for item in scan.for_bucket("QUEUE")] == [
        "alpha",
        "Zulu",
    ]
    assert [item.bundle_id for item in scan.for_bucket("RANDOM")] == [
        "alpha-random",
        "Zulu-random",
    ]
    assert [item.bundle_id for item in scan.for_bucket("REELS")] == [
        "alpha-reel",
        "Zulu-reel",
    ]
    assert all(item.directory != draft for item in scan.bundles)
    queued = next(item for item in scan.bundles if item.directory == queue)
    assert ".ready" not in [member.name for member in queued.content.members]
    assert queued.content.fingerprint == scan_inbox(queue).bundles[0].fingerprint


@pytest.mark.parametrize(
    ("setup", "code"),
    [
        ("missing_ready", "missing_ready_marker"),
        ("ready_symlink", "unsafe_ready_marker"),
        ("nested", "nested_bundle_entry"),
        ("container_mismatch", "bundle_id_mismatch"),
        ("extra_bundle", "bundle_count_mismatch"),
        ("reel_image", "reels_requires_video"),
    ],
)
def test_invalid_publishable_shapes_are_not_candidates(
    tmp_path: Path, setup: str, code: str
) -> None:
    root = tmp_path / "account"
    directory = root / ("REELS" if setup == "reel_image" else "QUEUE") / "post"
    _member(directory, "post.jpg")
    if setup != "missing_ready":
        _member(directory, ".ready", b"")
    if setup == "ready_symlink":
        (directory / ".ready").unlink()
        (directory / ".ready").symlink_to(directory / "post.jpg")
    elif setup == "nested":
        (directory / "nested").mkdir()
    elif setup == "container_mismatch":
        (directory / "post.jpg").rename(directory / "other.jpg")
    elif setup == "extra_bundle":
        _member(directory, "other.png")

    scan = scan_account_root(root)

    assert scan.bundles == ()
    assert code in _codes(scan)


def test_case_colliding_and_symlink_bundle_directories_are_rejected(
    tmp_path: Path,
) -> None:
    root = tmp_path / "account"
    _ready_bundle(root, "QUEUE", "Post", "Post.jpg")
    _ready_bundle(root, "QUEUE", "post", "post.jpg")
    target = _ready_bundle(root, "RANDOM", "target", "target.jpg")
    (root / "RANDOM/link").symlink_to(target, target_is_directory=True)

    scan = scan_account_root(root)

    assert scan.bundles == ()
    assert "casefold_bundle_directory_collision" in _codes(scan)
    assert "symlink_bundle_directory" in _codes(scan)


def test_ready_marker_disappearing_during_scan_fails_closed(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = tmp_path / "account"
    directory = _ready_bundle(root, "QUEUE", "post", "post.jpg")
    ready = directory / ".ready"
    original_lstat = content_module.os.lstat
    ready_stats = 0

    def remove_ready(path: os.PathLike[str] | str) -> os.stat_result:
        nonlocal ready_stats
        result = original_lstat(path)
        if Path(path) == ready:
            ready_stats += 1
            if ready_stats == 1:
                ready.unlink()
        return result

    monkeypatch.setattr(content_module.os, "lstat", remove_ready)

    scan = scan_account_root(root)

    assert scan.bundles == ()
    assert "ready_marker_changed" in _codes(scan)


def test_ready_marker_at_account_root_is_rejected(tmp_path: Path) -> None:
    root = tmp_path / "account"
    _ready_bundle(root, "QUEUE", "post", "post.jpg")
    _member(root, ".ready", b"")

    scan = scan_account_root(root)

    assert scan.bundles == ()
    assert "misplaced_ready_marker" in _codes(scan)


@pytest.mark.parametrize(
    ("replace", "code"),
    [("root", "account_root_changed"), ("bucket", "bucket_changed")],
)
def test_account_scan_rejects_root_or_bucket_generation_replacement(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    replace: str,
    code: str,
) -> None:
    root = tmp_path / "account"
    _ready_bundle(root, "QUEUE", "post", "post.jpg")
    original_scan = content_module._scan_bundle_directory
    replaced = False

    def scan_then_replace(
        bucket: str, bundle_directory: Path
    ) -> tuple[object, tuple[object, ...]]:
        nonlocal replaced
        result = original_scan(bucket, bundle_directory)  # type: ignore[arg-type]
        if not replaced:
            replaced = True
            target = root if replace == "root" else root / "QUEUE"
            old = tmp_path / f"old-{replace}"
            target.rename(old)
            target.mkdir(parents=True)
        return result

    monkeypatch.setattr(content_module, "_scan_bundle_directory", scan_then_replace)

    scan = scan_account_root(root)

    assert replaced
    assert scan.bundles == ()
    assert code in _codes(scan)
