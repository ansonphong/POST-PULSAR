"""Crash-safe legacy PHONG-BOT layout migration tests."""

from __future__ import annotations

import hashlib
import json
import os
import sqlite3
import stat
from collections.abc import Callable
from pathlib import Path

import pytest
from filelock import FileLock

from post_pulsar.config import ProfileSettings
from post_pulsar.locking import LockContentionError, LockManager
from post_pulsar.migration import (
    MigrationError,
    MigrationReport,
    migrate_legacy_layout,
)
from post_pulsar.state import SCHEMA_VERSION, StateRepository

CONFIG_HASH = "c" * 64


def _resign_journal(document: dict[str, object]) -> None:
    plan = {
        key: value
        for key, value in document.items()
        if key not in {"version", "plan_sha256"}
    }
    payload = json.dumps(
        plan, sort_keys=True, separators=(",", ":"), ensure_ascii=True
    ).encode()
    document["plan_sha256"] = hashlib.sha256(payload).hexdigest()


def _profile(tmp_path: Path) -> ProfileSettings:
    return ProfileSettings(
        "operator",
        tmp_path / "accounts/operator",
        "UTC",
        None,
        None,
    )


def _legacy(tmp_path: Path) -> Path:
    root = tmp_path / "legacy-posts"
    root.mkdir()
    (root / "cat.jpg").write_bytes(b"cat-image")
    (root / "cat.txt").write_text("cat caption", encoding="utf-8")
    posted = root / "posted"
    posted.mkdir()
    (posted / "dog.jpg").write_bytes(b"dog-image")
    (root / "config.json").write_text("do-not-read", encoding="utf-8")
    return root


def _run(
    tmp_path: Path,
    *,
    mode: str,
    fault: Callable[[str], None] | None = None,
) -> MigrationReport:
    return migrate_legacy_layout(
        selected_profile_id="operator",
        profile=_profile(tmp_path),
        legacy_posts_directory=tmp_path / "legacy-posts",
        state_directory=tmp_path / "state",
        config_hash=CONFIG_HASH,
        mode=mode,
        fault_injector=fault,
    )


def test_dry_run_reports_exact_changes_and_performs_zero_writes(tmp_path: Path) -> None:
    root = _legacy(tmp_path)
    before = {
        path.relative_to(root).as_posix(): path.read_bytes()
        for path in root.rglob("*")
        if path.is_file()
    }

    report = _run(tmp_path, mode="dry-run")

    assert report.mode == "dry-run"
    assert report.phase == "planned"
    assert [(item.bundle_id, item.disposition) for item in report.items] == [
        ("cat", "active"),
        ("dog", "archived"),
    ]
    assert "config.json" in report.untouched
    assert report.required_inputs == (
        "profile:operator",
        "official_credentials_not_migrated",
        "remote_identities_not_inferred",
    )
    assert not (tmp_path / "state").exists()
    assert not _profile(tmp_path).account_root.exists()
    assert before == {
        path.relative_to(root).as_posix(): path.read_bytes()
        for path in root.rglob("*")
        if path.is_file()
    }


def test_apply_moves_only_valid_bundles_ready_last_and_infers_no_delivery(
    tmp_path: Path,
) -> None:
    root = _legacy(tmp_path)

    report = _run(tmp_path, mode="apply")

    active = _profile(tmp_path).account_root / "RANDOM/cat"
    archived = _profile(tmp_path).account_root / "POSTED/RANDOM/dog"
    assert {item.name for item in active.iterdir()} == {"cat.jpg", "cat.txt", ".ready"}
    assert {item.name for item in archived.iterdir()} == {"dog.jpg", ".ready"}
    assert (root / "config.json").read_text(encoding="utf-8") == "do-not-read"
    assert not (root / "cat.jpg").exists()
    assert not (root / "posted/dog.jpg").exists()
    assert report.phase == "complete"

    database = tmp_path / "state/post_pulsar.sqlite3"
    with StateRepository(database) as repository:
        assert repository.schema_version == SCHEMA_VERSION
        assert (
            repository.get_profile("operator").account_root
            == _profile(tmp_path).account_root
        )
    with sqlite3.connect(database) as connection:
        assert connection.execute("SELECT COUNT(*) FROM deliveries").fetchone()[0] == 0
        assert connection.execute("PRAGMA foreign_key_check").fetchall() == []
    journal = tmp_path / "state/legacy-migration-v1.json"
    assert stat.S_IMODE(journal.stat().st_mode) == 0o600


@pytest.mark.parametrize(
    "boundary",
    [
        "after_journal_created",
        "after_state_ready",
        "after_member_copied:cat:cat.jpg",
        "after_ready_installed:cat",
        "after_bundle_installed:cat",
        "after_source_removed:cat:cat.jpg",
    ],
)
def test_fault_restart_resumes_idempotently(tmp_path: Path, boundary: str) -> None:
    _legacy(tmp_path)
    fired = False

    def crash(candidate: str) -> None:
        nonlocal fired
        if candidate == boundary and not fired:
            fired = True
            raise RuntimeError("injected crash")

    with pytest.raises(RuntimeError, match="injected"):
        _run(tmp_path, mode="apply", fault=crash)

    first = _run(tmp_path, mode="apply")
    second = _run(tmp_path, mode="apply")
    assert first.phase == second.phase == "complete"
    assert (_profile(tmp_path).account_root / "RANDOM/cat/.ready").is_file()
    assert (_profile(tmp_path).account_root / "POSTED/RANDOM/dog/.ready").is_file()


def test_conflicting_destination_fails_before_state_or_source_mutation(
    tmp_path: Path,
) -> None:
    root = _legacy(tmp_path)
    conflict = _profile(tmp_path).account_root / "RANDOM/cat"
    conflict.mkdir(parents=True)
    (conflict / "other").write_text("conflict", encoding="utf-8")

    with pytest.raises(MigrationError, match="destination"):
        _run(tmp_path, mode="apply")

    assert (root / "cat.jpg").read_bytes() == b"cat-image"
    assert not (tmp_path / "state/post_pulsar.sqlite3").exists()
    assert (conflict / "other").read_text(encoding="utf-8") == "conflict"


def test_unknown_database_schema_is_byte_for_byte_untouched(tmp_path: Path) -> None:
    _legacy(tmp_path)
    database = tmp_path / "state/post_pulsar.sqlite3"
    database.parent.mkdir()
    with sqlite3.connect(database) as connection:
        connection.execute("PRAGMA user_version = 99")
    before = database.read_bytes()

    with pytest.raises(MigrationError, match="schema"):
        _run(tmp_path, mode="apply")

    assert database.read_bytes() == before
    assert not (tmp_path / "state/legacy-migration-v1.json").exists()


def test_dry_run_rejects_unknown_database_without_writes(tmp_path: Path) -> None:
    root = _legacy(tmp_path)
    database = tmp_path / "state/post_pulsar.sqlite3"
    database.parent.mkdir()
    with sqlite3.connect(database) as connection:
        connection.execute("PRAGMA user_version = 99")
    before_database = database.read_bytes()
    before_source = (root / "cat.jpg").read_bytes()

    with pytest.raises(MigrationError, match="schema"):
        _run(tmp_path, mode="dry-run")

    assert database.read_bytes() == before_database
    assert (root / "cat.jpg").read_bytes() == before_source
    assert not (tmp_path / "state/legacy-migration-v1.json").exists()


def test_current_database_is_backed_up_owner_only_before_import(tmp_path: Path) -> None:
    _legacy(tmp_path)
    database = tmp_path / "state/post_pulsar.sqlite3"
    with StateRepository(database):
        pass

    _run(tmp_path, mode="apply")

    backup = tmp_path / "state/post_pulsar.pre-migration-v1.sqlite3"
    assert backup.is_file()
    assert stat.S_IMODE(backup.stat().st_mode) == 0o600
    with sqlite3.connect(backup) as connection:
        assert connection.execute("PRAGMA user_version").fetchone()[0] == SCHEMA_VERSION


@pytest.mark.parametrize("boundary", ["after_journal_created", "after_state_ready"])
def test_current_database_backup_binding_resumes_after_crash(
    tmp_path: Path, boundary: str
) -> None:
    _legacy(tmp_path)
    database = tmp_path / "state/post_pulsar.sqlite3"
    with StateRepository(database):
        pass

    def crash(candidate: str) -> None:
        if candidate == boundary:
            raise RuntimeError("injected crash")

    with pytest.raises(RuntimeError, match="injected"):
        _run(tmp_path, mode="apply", fault=crash)

    report = _run(tmp_path, mode="apply")

    assert report.phase == "complete"
    assert (tmp_path / "state/post_pulsar.pre-migration-v1.sqlite3").is_file()


@pytest.mark.parametrize("mode", ["dry-run", "apply"])
def test_foreign_preexisting_backup_is_rejected_before_mutation(
    tmp_path: Path, mode: str
) -> None:
    root = _legacy(tmp_path)
    state = tmp_path / "state"
    database = state / "post_pulsar.sqlite3"
    with StateRepository(database):
        pass
    foreign = tmp_path / "foreign.sqlite3"
    with StateRepository(foreign) as repository:
        repository.register_profile(
            "foreign", tmp_path / "foreign-account", (), config_hash="f" * 64
        )
    backup = state / "post_pulsar.pre-migration-v1.sqlite3"
    backup.write_bytes(foreign.read_bytes())
    backup.chmod(0o600)
    before_database = database.read_bytes()
    before_backup = backup.read_bytes()

    with pytest.raises(MigrationError, match="backup"):
        _run(tmp_path, mode=mode)

    assert database.read_bytes() == before_database
    assert backup.read_bytes() == before_backup
    assert (root / "cat.jpg").exists()
    assert not (state / "legacy-migration-v1.json").exists()


def test_symlinked_member_and_profile_mismatch_fail_without_external_mutation(
    tmp_path: Path,
) -> None:
    root = _legacy(tmp_path)
    outside = tmp_path / "outside.jpg"
    outside.write_bytes(b"outside")
    (root / "cat.jpg").unlink()
    (root / "cat.jpg").symlink_to(outside)

    with pytest.raises(MigrationError):
        _run(tmp_path, mode="apply")
    assert outside.read_bytes() == b"outside"

    with pytest.raises(MigrationError, match="profile"):
        migrate_legacy_layout(
            selected_profile_id="someone-else",
            profile=_profile(tmp_path),
            legacy_posts_directory=root,
            state_directory=tmp_path / "state-two",
            config_hash=CONFIG_HASH,
            mode="dry-run",
        )


def test_daemon_instance_contention_stops_before_migration(tmp_path: Path) -> None:
    root = _legacy(tmp_path)
    locks = LockManager(tmp_path / "state")
    with locks.acquire_instance():
        with pytest.raises(LockContentionError):
            _run(tmp_path, mode="apply")

    assert (root / "cat.jpg").exists()
    assert not (tmp_path / "state/post_pulsar.sqlite3").exists()


def test_profile_contention_stops_before_cutover_and_lease_is_released(
    tmp_path: Path,
) -> None:
    root = _legacy(tmp_path)
    state = tmp_path / "state"
    LockManager(state)
    external = FileLock(state / "locks/profiles/operator.lock")
    external.acquire(timeout=0)
    try:
        with pytest.raises(LockContentionError):
            _run(tmp_path, mode="apply")
    finally:
        external.release()

    assert (root / "cat.jpg").exists()
    assert not (state / "post_pulsar.sqlite3").exists()
    assert not (state / "legacy-migration-v1.json").exists()

    _run(tmp_path, mode="apply")
    locks = LockManager(state)
    with locks.acquire_instance() as instance:
        with locks.acquire_maintenance(instance) as maintenance:
            with locks.acquire_profiles(
                instance, ("operator",), maintenance=maintenance
            ):
                pass


def test_journal_does_not_contain_legacy_secret_file_contents(tmp_path: Path) -> None:
    root = _legacy(tmp_path)
    secret = "legacy-password-" + hashlib.sha256(os.urandom(16)).hexdigest()
    (root / "config.json").write_text(secret, encoding="utf-8")

    _run(tmp_path, mode="apply")

    journal = (tmp_path / "state/legacy-migration-v1.json").read_text("utf-8")
    assert secret not in journal


def test_completed_rerun_is_read_only_and_idempotent(tmp_path: Path) -> None:
    _legacy(tmp_path)
    first = _run(tmp_path, mode="apply")
    journal = tmp_path / "state/legacy-migration-v1.json"
    before = (journal.read_bytes(), journal.stat().st_mtime_ns)

    second = _run(tmp_path, mode="apply")

    assert first == second
    assert (journal.read_bytes(), journal.stat().st_mtime_ns) == before


def test_completed_dry_run_is_noop_without_sqlite_sidecars_or_mtime_changes(
    tmp_path: Path,
) -> None:
    _legacy(tmp_path)
    applied = _run(tmp_path, mode="apply")
    state = tmp_path / "state"
    database = state / "post_pulsar.sqlite3"
    shm = Path(f"{database}-shm")
    if shm.exists():
        shm.unlink()
    before_entries = tuple(
        sorted(path.relative_to(state).as_posix() for path in state.rglob("*"))
    )
    before_metadata = {
        relative: (
            (state / relative).stat(follow_symlinks=False).st_mode,
            (state / relative).stat(follow_symlinks=False).st_size,
            (state / relative).stat(follow_symlinks=False).st_mtime_ns,
        )
        for relative in before_entries
    }
    state_mtime = state.stat().st_mtime_ns

    report = _run(tmp_path, mode="dry-run")

    assert report.mode == "dry-run"
    assert report.phase == applied.phase == "complete"
    assert report.items == applied.items
    assert report.database_changes == ("no_changes_completed_migration",)
    assert (
        tuple(sorted(path.relative_to(state).as_posix() for path in state.rglob("*")))
        == before_entries
    )
    assert {
        relative: (
            (state / relative).stat(follow_symlinks=False).st_mode,
            (state / relative).stat(follow_symlinks=False).st_size,
            (state / relative).stat(follow_symlinks=False).st_mtime_ns,
        )
        for relative in before_entries
    } == before_metadata
    assert state.stat().st_mtime_ns == state_mtime
    assert not shm.exists()


def test_dry_run_rejects_wal_only_state_without_touching_source_or_sidecars(
    tmp_path: Path,
) -> None:
    root = _legacy(tmp_path)
    state = tmp_path / "state"
    database = state / "post_pulsar.sqlite3"
    with StateRepository(database):
        pass
    repository = StateRepository(database)
    try:
        repository.register_profile(
            "foreign", tmp_path / "foreign-account", (), config_hash="f" * 64
        )
        with sqlite3.connect(database) as authoritative:
            assert (
                authoritative.execute("SELECT COUNT(*) FROM profiles").fetchone()[0]
                == 1
            )
        immutable_uri = f"file:{database}?mode=ro&immutable=1"
        with sqlite3.connect(immutable_uri, uri=True) as stale:
            assert stale.execute("SELECT COUNT(*) FROM profiles").fetchone()[0] == 0
        wal = Path(f"{database}-wal")
        assert wal.stat().st_size > 0
        before_source = _filesystem_snapshot(root)
        before_state = _filesystem_snapshot(state)

        with pytest.raises(MigrationError, match="active WAL"):
            _run(tmp_path, mode="dry-run")

        assert _filesystem_snapshot(root) == before_source
        assert _filesystem_snapshot(state) == before_state
    finally:
        repository.close()


def _filesystem_snapshot(
    root: Path,
) -> tuple[int, dict[str, tuple[int, int, int, bytes | None]]]:
    entries: dict[str, tuple[int, int, int, bytes | None]] = {}
    for path in sorted(root.rglob("*")):
        metadata = path.stat(follow_symlinks=False)
        entries[path.relative_to(root).as_posix()] = (
            metadata.st_mode,
            metadata.st_size,
            metadata.st_mtime_ns,
            path.read_bytes() if stat.S_ISREG(metadata.st_mode) else None,
        )
    return root.stat(follow_symlinks=False).st_mtime_ns, entries


def test_nonempty_current_database_is_rejected_before_import(tmp_path: Path) -> None:
    root = _legacy(tmp_path)
    database = tmp_path / "state/post_pulsar.sqlite3"
    foreign_root = tmp_path / "accounts/foreign"
    with StateRepository(database) as repository:
        repository.register_profile("foreign", foreign_root, (), config_hash="f" * 64)
    before = database.read_bytes()

    with pytest.raises(MigrationError, match="not empty"):
        _run(tmp_path, mode="apply")

    assert database.read_bytes() == before
    assert (root / "cat.jpg").exists()
    assert not (tmp_path / "state/legacy-migration-v1.json").exists()


def test_tampered_journal_paths_are_rejected_before_mutation(tmp_path: Path) -> None:
    root = _legacy(tmp_path)

    def crash(boundary: str) -> None:
        if boundary == "after_journal_created":
            raise RuntimeError("injected crash")

    with pytest.raises(RuntimeError, match="injected"):
        _run(tmp_path, mode="apply", fault=crash)
    journal = tmp_path / "state/legacy-migration-v1.json"
    document = json.loads(journal.read_text("utf-8"))
    document["items"][0]["destination_relative"] = "RANDOM/other"
    journal.write_text(json.dumps(document), encoding="utf-8")
    journal.chmod(0o600)

    with pytest.raises(MigrationError, match="journal"):
        _run(tmp_path, mode="apply")

    assert (root / "cat.jpg").exists()
    assert not (tmp_path / "state/post_pulsar.sqlite3").exists()


def test_journal_member_manifest_is_rebound_before_any_mutation(tmp_path: Path) -> None:
    root = _legacy(tmp_path)

    def crash(boundary: str) -> None:
        if boundary == "after_journal_created":
            raise RuntimeError("injected crash")

    with pytest.raises(RuntimeError, match="injected"):
        _run(tmp_path, mode="apply", fault=crash)
    journal = tmp_path / "state/legacy-migration-v1.json"
    document = json.loads(journal.read_text("utf-8"))
    config = root / "config.json"
    document["items"][0]["members"].append(
        {
            "relative_name": "config.json",
            "role": "caption",
            "sha256": hashlib.sha256(config.read_bytes()).hexdigest(),
            "size_bytes": config.stat().st_size,
        }
    )
    _resign_journal(document)
    journal.write_text(json.dumps(document), encoding="utf-8")
    journal.chmod(0o600)

    with pytest.raises(MigrationError, match="journal"):
        _run(tmp_path, mode="apply")

    assert config.read_text("utf-8") == "do-not-read"
    assert not tuple(_profile(tmp_path).account_root.rglob("config.json"))
    assert not (tmp_path / "state/post_pulsar.sqlite3").exists()


def test_resigned_journal_cannot_alias_a_member_from_another_bundle(
    tmp_path: Path,
) -> None:
    root = _legacy(tmp_path)
    catalog = root / "catalog.jpg"
    catalog.write_bytes(b"catalog-image")

    def crash(boundary: str) -> None:
        if boundary == "after_journal_created":
            raise RuntimeError("injected crash")

    with pytest.raises(RuntimeError, match="injected"):
        _run(tmp_path, mode="apply", fault=crash)
    journal = tmp_path / "state/legacy-migration-v1.json"
    document = json.loads(journal.read_text("utf-8"))
    cat_item = next(item for item in document["items"] if item["bundle_id"] == "cat")
    cat_item["members"].append(
        {
            "relative_name": "catalog.jpg",
            "role": "image",
            "sha256": hashlib.sha256(catalog.read_bytes()).hexdigest(),
            "size_bytes": catalog.stat().st_size,
        }
    )
    cat_item["members"].sort(key=lambda member: member["relative_name"])
    _resign_journal(document)
    journal.write_text(json.dumps(document), encoding="utf-8")
    journal.chmod(0o600)

    with pytest.raises(MigrationError, match="manifest"):
        _run(tmp_path, mode="apply")

    assert catalog.read_bytes() == b"catalog-image"
    assert not tuple(
        path
        for path in _profile(tmp_path).account_root.rglob("catalog.jpg")
        if path.parent.name.startswith(".cat.") or path.parent.name == "cat"
    )
    assert not (tmp_path / "state/post_pulsar.sqlite3").exists()


def test_journal_unknown_fields_are_rejected_before_mutation(tmp_path: Path) -> None:
    root = _legacy(tmp_path)

    def crash(boundary: str) -> None:
        if boundary == "after_journal_created":
            raise RuntimeError("injected crash")

    with pytest.raises(RuntimeError, match="injected"):
        _run(tmp_path, mode="apply", fault=crash)
    journal = tmp_path / "state/legacy-migration-v1.json"
    document = json.loads(journal.read_text("utf-8"))
    document["unexpected"] = "ignored-by-old-parser"
    _resign_journal(document)
    journal.write_text(json.dumps(document), encoding="utf-8")
    journal.chmod(0o600)

    with pytest.raises(MigrationError, match="journal"):
        _run(tmp_path, mode="apply")

    assert (root / "cat.jpg").exists()
    assert not (tmp_path / "state/post_pulsar.sqlite3").exists()


def test_journal_unknown_member_role_is_rejected_before_mutation(
    tmp_path: Path,
) -> None:
    root = _legacy(tmp_path)

    def crash(boundary: str) -> None:
        if boundary == "after_journal_created":
            raise RuntimeError("injected crash")

    with pytest.raises(RuntimeError, match="injected"):
        _run(tmp_path, mode="apply", fault=crash)
    journal = tmp_path / "state/legacy-migration-v1.json"
    document = json.loads(journal.read_text("utf-8"))
    document["items"][0]["members"][0]["role"] = "credential"
    _resign_journal(document)
    journal.write_text(json.dumps(document), encoding="utf-8")
    journal.chmod(0o600)

    with pytest.raises(MigrationError, match="journal"):
        _run(tmp_path, mode="apply")

    assert (root / "cat.jpg").exists()
    assert not (tmp_path / "state/post_pulsar.sqlite3").exists()


def test_partial_copy_checkpoint_is_repaired_on_restart(tmp_path: Path) -> None:
    _legacy(tmp_path)

    def crash(boundary: str) -> None:
        if boundary == "after_journal_created":
            raise RuntimeError("injected crash")

    with pytest.raises(RuntimeError, match="injected"):
        _run(tmp_path, mode="apply", fault=crash)
    document = json.loads(
        (tmp_path / "state/legacy-migration-v1.json").read_text("utf-8")
    )
    item = document["items"][0]
    member = item["members"][0]
    staging = (
        _profile(tmp_path).account_root
        / "RANDOM"
        / f".{item['bundle_id']}.{item['fingerprint']}.migration"
    )
    staging.mkdir(parents=True)
    name_hash = hashlib.sha256(member["relative_name"].encode()).hexdigest()
    temporary = staging / f".copy-{member['sha256']}-{name_hash}.tmp"
    temporary.write_bytes(b"partial")

    report = _run(tmp_path, mode="apply")

    assert report.phase == "complete"
    assert (_profile(tmp_path).account_root / "RANDOM/cat/.ready").is_file()


def test_resume_destination_conflict_fails_before_database_mutation(
    tmp_path: Path,
) -> None:
    root = _legacy(tmp_path)

    def crash(boundary: str) -> None:
        if boundary == "after_journal_created":
            raise RuntimeError("injected crash")

    with pytest.raises(RuntimeError, match="injected"):
        _run(tmp_path, mode="apply", fault=crash)
    document = json.loads(
        (tmp_path / "state/legacy-migration-v1.json").read_text("utf-8")
    )
    item = document["items"][0]
    staging = (
        _profile(tmp_path).account_root
        / "RANDOM"
        / f".{item['bundle_id']}.{item['fingerprint']}.migration"
    )
    staging.mkdir(parents=True)
    (staging / "foreign").write_bytes(b"conflict")

    with pytest.raises(MigrationError, match="staging membership"):
        _run(tmp_path, mode="apply")

    assert (root / "cat.jpg").exists()
    assert not (tmp_path / "state/post_pulsar.sqlite3").exists()


def test_symlinked_account_or_state_root_fails_without_writing_outside(
    tmp_path: Path,
) -> None:
    root = _legacy(tmp_path)
    outside_account = tmp_path / "outside-account"
    outside_account.mkdir()
    (_profile(tmp_path).account_root.parent).mkdir()
    _profile(tmp_path).account_root.symlink_to(
        outside_account, target_is_directory=True
    )

    with pytest.raises(MigrationError, match="account destination path is unsafe"):
        _run(tmp_path, mode="apply")

    assert list(outside_account.iterdir()) == []
    _profile(tmp_path).account_root.unlink()
    outside_state = tmp_path / "outside-state"
    outside_state.mkdir()
    (tmp_path / "state").symlink_to(outside_state, target_is_directory=True)

    with pytest.raises(MigrationError, match="state destination path is unsafe"):
        _run(tmp_path, mode="apply")

    assert list(outside_state.iterdir()) == []
    assert (root / "cat.jpg").exists()
