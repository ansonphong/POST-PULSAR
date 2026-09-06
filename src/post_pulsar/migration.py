"""Explicit, journaled migration from the legacy flat PHONG-BOT posts layout."""

from __future__ import annotations

import hashlib
import json
import os
import re
import sqlite3
import stat
import tempfile
from collections.abc import Mapping
from contextlib import contextmanager
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import BinaryIO, Final, Literal, Protocol, cast
from urllib.parse import quote

from post_pulsar.archive import (  # noqa: PLC2701
    _atomic_noreplace,
    _DirectoryGuard,
    _fsync_guard,
)
from post_pulsar.config import ProfileSettings
from post_pulsar.content import ContentBundle, InboxIssue, scan_inbox
from post_pulsar.locking import LockManager
from post_pulsar.state import (
    _APPLICATION_ID,
    _REQUIRED_TABLES,
    SCHEMA_VERSION,
    AdmissionRecord,
    ProfileTargetSnapshot,
    StateRepository,
    _expected_schema_manifest_digest,
    _schema_manifest_digest,
)

MigrationMode = Literal["dry-run", "apply"]
MigrationDisposition = Literal["active", "archived"]

_PROFILE_RE: Final = re.compile(r"[a-z0-9][a-z0-9-]{0,31}\Z")
_BUNDLE_RE: Final = re.compile(r"[A-Za-z0-9](?:[A-Za-z0-9_-]{0,62}[A-Za-z0-9])?\Z")
_SHA256_RE: Final = re.compile(r"[0-9a-f]{64}\Z")
_JOURNAL_NAME: Final = "legacy-migration-v1.json"
_BACKUP_NAME: Final = "post_pulsar.pre-migration-v1.sqlite3"
_DATABASE_NAME: Final = "post_pulsar.sqlite3"
_JOURNAL_VERSION: Final = 1
_COPY_BYTES: Final = 1024 * 1024


class MigrationError(RuntimeError):
    """A bounded migration failure that never includes legacy file contents."""


class MigrationFaultInjector(Protocol):
    """Crash-test seam invoked only after durable migration boundaries."""

    def __call__(self, boundary: str) -> None: ...


@dataclass(frozen=True, slots=True)
class MigrationMember:
    """One immutable legacy source member."""

    relative_name: str
    sha256: str
    size_bytes: int


@dataclass(frozen=True, slots=True)
class MigrationItem:
    """One exact legacy bundle and its explicit disposition."""

    bundle_id: str
    disposition: MigrationDisposition
    source_relative: str
    destination_relative: str
    fingerprint: str
    members: tuple[MigrationMember, ...]


@dataclass(frozen=True, slots=True)
class MigrationReport:
    """Secret-free deterministic migration result for CLI or service callers."""

    profile_id: str
    mode: MigrationMode
    phase: str
    items: tuple[MigrationItem, ...]
    untouched: tuple[str, ...]
    issues: tuple[str, ...]
    required_inputs: tuple[str, ...]
    database_changes: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class _MigrationPlan:
    profile_id: str
    legacy_root: str
    account_root: str
    state_directory: str
    config_hash: str
    phase: str
    items: tuple[MigrationItem, ...]
    untouched: tuple[str, ...]
    issues: tuple[str, ...]


def migrate_legacy_layout(
    *,
    selected_profile_id: str,
    profile: ProfileSettings,
    legacy_posts_directory: str | os.PathLike[str],
    state_directory: str | os.PathLike[str],
    config_hash: str,
    mode: MigrationMode | str,
    fault_injector: MigrationFaultInjector | object | None = None,
) -> MigrationReport:
    """Plan or apply one explicit legacy-flat-layout migration."""

    normalized_mode = _validate_request(selected_profile_id, profile, config_hash, mode)
    legacy_root = _verified_directory(Path(legacy_posts_directory), "legacy posts")
    account_root = profile.account_root.expanduser().absolute()
    state_root = Path(state_directory).expanduser().absolute()
    _reject_symlink_components(account_root, "account destination")
    _reject_symlink_components(state_root, "state destination")
    _reject_path_overlap(legacy_root, account_root, "account destination")
    _reject_path_overlap(legacy_root, state_root, "state destination")
    journal_path = state_root / _JOURNAL_NAME

    journal_existed = journal_path.exists()
    if journal_existed:
        plan = _load_plan(journal_path)
        _assert_plan_binding(
            plan,
            selected_profile_id,
            legacy_root,
            account_root,
            state_root,
            config_hash,
        )
    else:
        plan = _build_plan(
            selected_profile_id,
            legacy_root,
            account_root,
            state_root,
            config_hash,
        )
        _validate_destinations(plan, allow_existing=False)

    if normalized_mode == "dry-run":
        return _report(plan, "dry-run")

    injector = cast(MigrationFaultInjector | None, fault_injector)
    locks = LockManager(state_root)
    with locks.acquire_instance() as instance:
        with locks.acquire_maintenance(instance):
            database = state_root / _DATABASE_NAME
            database_existed = database.exists()
            _inspect_database(database)
            _validate_state_compatibility(database, plan, resuming=journal_existed)
            _validate_destinations(plan, allow_existing=journal_existed)
            if plan.phase == "complete":
                _validate_completed(plan, database, legacy_root, account_root)
                return _report(plan, "apply")
            if not journal_existed:
                _write_plan(journal_path, plan)
                _inject(injector, "after_journal_created")
            if database_existed:
                _ensure_backup(database, state_root / _BACKUP_NAME)

            with StateRepository(database) as repository:
                repository.register_profile(
                    profile.profile_id,
                    account_root,
                    _profile_targets(profile),
                    config_hash=config_hash,
                )
                if plan.phase == "planned":
                    plan = _replace_phase(plan, "state_ready")
                    _write_plan(journal_path, plan)
                    _inject(injector, "after_state_ready")
                _apply_items(
                    plan,
                    repository,
                    legacy_root,
                    account_root,
                    selected_profile_id,
                    injector,
                )

            _validate_current_state(database, selected_profile_id, len(plan.items))
            plan = _replace_phase(plan, "complete")
            _write_plan(journal_path, plan)
            _inject(injector, "after_migration_complete")
    return _report(plan, "apply")


def _validate_request(
    selected_profile_id: str,
    profile: ProfileSettings,
    config_hash: str,
    mode: str,
) -> MigrationMode:
    if (
        not _PROFILE_RE.fullmatch(selected_profile_id)
        or selected_profile_id != profile.profile_id
    ):
        raise MigrationError("migration requires one matching explicit profile")
    if not _SHA256_RE.fullmatch(config_hash):
        raise MigrationError("migration configuration identity is invalid")
    if mode not in {"dry-run", "apply"}:
        raise MigrationError("migration mode must be dry-run or apply")
    return cast(MigrationMode, mode)


def _build_plan(
    profile_id: str,
    legacy_root: Path,
    account_root: Path,
    state_root: Path,
    config_hash: str,
) -> _MigrationPlan:
    active, active_issues = _scan_legacy_location(
        legacy_root, excluded_directory="posted"
    )
    posted = legacy_root / "posted"
    if posted.exists():
        posted_root = _verified_directory(posted, "legacy posted")
        archived, archived_issues = _scan_legacy_location(posted_root)
    else:
        archived, archived_issues = (), ()

    unsafe = tuple(
        issue
        for issue in (*active_issues, *archived_issues)
        if issue.severity == "error"
    )
    if unsafe:
        raise MigrationError("legacy layout contains unsafe or partial bundles")
    if not active and not archived:
        raise MigrationError("legacy layout contains no exact valid bundles")

    seen: set[str] = set()
    items: list[MigrationItem] = []
    for disposition, source_root, bundles in (
        ("active", legacy_root, active),
        ("archived", posted, archived),
    ):
        for bundle in bundles:
            key = bundle.bundle_id.casefold()
            if key in seen:
                raise MigrationError("legacy bundle identity appears more than once")
            seen.add(key)
            destination = (
                Path("RANDOM") / bundle.bundle_id
                if disposition == "active"
                else Path("POSTED") / "RANDOM" / bundle.bundle_id
            )
            members = tuple(
                _capture_member(path, source_root)
                for path in sorted(
                    bundle.members, key=lambda item: (item.name.casefold(), item.name)
                )
            )
            if (
                _fingerprint_from_source(source_root, bundle.bundle_id)
                != bundle.fingerprint
            ):
                raise MigrationError("legacy bundle changed during migration planning")
            items.append(
                MigrationItem(
                    bundle.bundle_id,
                    cast(MigrationDisposition, disposition),
                    source_root.relative_to(legacy_root).as_posix()
                    if source_root != legacy_root
                    else "root",
                    destination.as_posix(),
                    bundle.fingerprint,
                    members,
                )
            )

    known = {
        (
            legacy_root
            / ("" if item.source_relative == "root" else item.source_relative)
            / member.relative_name
        )
        .relative_to(legacy_root)
        .as_posix()
        for item in items
        for member in item.members
    }
    untouched = tuple(
        sorted(
            (
                path.relative_to(legacy_root).as_posix()
                for path in legacy_root.rglob("*")
                if path.is_file()
                and path.relative_to(legacy_root).as_posix() not in known
            ),
            key=lambda value: (value.casefold(), value),
        )
    )
    issues = tuple(
        sorted(
            {issue.code for issue in (*active_issues, *archived_issues)},
        )
    )
    items.sort(
        key=lambda item: (item.disposition, item.bundle_id.casefold(), item.bundle_id)
    )
    return _MigrationPlan(
        profile_id,
        str(legacy_root),
        str(account_root),
        str(state_root),
        config_hash,
        "planned",
        tuple(items),
        untouched,
        issues,
    )


def _scan_legacy_location(
    directory: Path, *, excluded_directory: str | None = None
) -> tuple[tuple[ContentBundle, ...], tuple[InboxIssue, ...]]:
    scan = scan_inbox(directory)
    issues = tuple(
        issue
        for issue in scan.issues
        if not (
            excluded_directory is not None
            and issue.member is not None
            and issue.member.name == excluded_directory
            and issue.member.parent == directory
        )
    )
    return scan.bundles, issues


def _capture_member(path: Path, root: Path) -> MigrationMember:
    if path.parent != root:
        raise MigrationError("legacy bundle member escaped its flat source")
    digest, size, _identity = _hash_regular(root, path.name)
    return MigrationMember(path.name, digest, size)


def _fingerprint_from_source(root: Path, bundle_id: str) -> str:
    scan = scan_inbox(root)
    matches = tuple(bundle for bundle in scan.bundles if bundle.bundle_id == bundle_id)
    if len(matches) != 1:
        raise MigrationError("legacy bundle changed during migration planning")
    return matches[0].fingerprint


def _validate_destinations(plan: _MigrationPlan, *, allow_existing: bool) -> None:
    account_root = Path(plan.account_root)
    for item in plan.items:
        final = account_root / item.destination_relative
        staging = final.parent / f".{item.bundle_id}.{item.fingerprint}.migration"
        if not allow_existing and (os.path.lexists(final) or os.path.lexists(staging)):
            raise MigrationError("migration destination already exists")
        for candidate in (final, staging):
            if os.path.lexists(candidate):
                metadata = os.lstat(candidate)
                if stat.S_ISLNK(metadata.st_mode) or not stat.S_ISDIR(metadata.st_mode):
                    raise MigrationError("migration destination is unsafe")
        if allow_existing and os.path.lexists(final) and os.path.lexists(staging):
            raise MigrationError("migration has conflicting destination checkpoints")
        if allow_existing and os.path.lexists(final):
            _verify_destination(final, item, require_ready=True)
        if allow_existing and os.path.lexists(staging):
            _validate_staging_checkpoint(staging, item)


def _validate_staging_checkpoint(staging: Path, item: MigrationItem) -> None:
    """Reject foreign resume files while allowing one bounded partial copy."""

    member_names = {member.relative_name for member in item.members}
    temporary_names = {
        f".copy-{member.sha256}-{_name_digest(member.relative_name)}.tmp"
        for member in item.members
    }
    with _guard(staging) as guard:
        names = _list_names(guard)
        if not names.issubset(member_names | temporary_names | {".ready"}):
            raise MigrationError("migration staging membership conflicts")
        for member in item.members:
            if member.relative_name in names:
                _verify_member(guard, member)
        for temporary in names & temporary_names:
            metadata = _stat_at(guard, temporary)
            if not stat.S_ISREG(metadata.st_mode):
                raise MigrationError("migration partial copy is unsafe")
        if ".ready" in names:
            ready = _stat_at(guard, ".ready")
            if not stat.S_ISREG(ready.st_mode) or ready.st_size != 0:
                raise MigrationError("migration ready marker is invalid")


def _apply_items(
    plan: _MigrationPlan,
    repository: StateRepository,
    legacy_root: Path,
    account_root: Path,
    profile_id: str,
    injector: MigrationFaultInjector | None,
) -> None:
    _ensure_directory(account_root)
    for item in plan.items:
        _apply_item(item, repository, legacy_root, account_root, profile_id, injector)


def _apply_item(
    item: MigrationItem,
    repository: StateRepository,
    legacy_root: Path,
    account_root: Path,
    profile_id: str,
    injector: MigrationFaultInjector | None,
) -> None:
    source_root = (
        legacy_root
        if item.source_relative == "root"
        else legacy_root / item.source_relative
    )
    destination = account_root / item.destination_relative
    _ensure_directory(destination.parent)
    staging = destination.parent / f".{item.bundle_id}.{item.fingerprint}.migration"
    journal = repository.start_admission(
        profile_id=profile_id,
        bucket="RANDOM",
        bundle_id=item.bundle_id,
        fingerprint=item.fingerprint,
        source_path=f"legacy/{item.source_relative}/{item.bundle_id}",
        destination_path=item.destination_relative,
        intent_id=None,
    )

    if journal.phase == "installed":
        _verify_destination(destination, item, require_ready=True)
        _remove_sources(source_root, item, injector)
        return

    destination_exists = os.path.lexists(destination)
    if destination_exists:
        target = destination
    else:
        if not os.path.lexists(staging):
            staging.mkdir(mode=0o700)
            _fsync_path(destination.parent)
        target = staging
    _copy_members(source_root, target, item, journal, repository, injector)
    ready_already_installed = os.path.lexists(target / ".ready")
    _verify_destination(target, item, require_ready=ready_already_installed)
    _install_ready(target)
    _inject(injector, f"after_ready_installed:{item.bundle_id}")
    journal = repository.get_admission(journal.journal_id)
    if journal.phase not in {"ready_installed", "installed"}:
        journal = repository.advance_admission(
            journal.journal_id,
            "ready_installed",
            expected_revision=journal.revision,
        )
    _verify_destination(target, item, require_ready=True)
    if not destination_exists and journal.phase != "installed":
        with _guard(destination.parent) as parent:
            _atomic_noreplace(parent, staging.name, parent, destination.name)
            _fsync_guard(parent)
        _inject(injector, f"after_bundle_installed:{item.bundle_id}")

    journal = repository.get_admission(journal.journal_id)
    if journal.phase == "copying":
        journal = repository.advance_admission(
            journal.journal_id,
            "ready_installed",
            expected_revision=journal.revision,
        )
    if journal.phase == "ready_installed":
        repository.advance_admission(
            journal.journal_id,
            "installed",
            expected_revision=journal.revision,
        )
    _verify_destination(destination, item, require_ready=True)
    _remove_sources(source_root, item, injector)


def _copy_members(
    source_root: Path,
    target: Path,
    item: MigrationItem,
    journal: AdmissionRecord,
    repository: StateRepository,
    injector: MigrationFaultInjector | None,
) -> None:
    states = {
        member.relative_name: member
        for member in repository.list_admission_members(journal.journal_id)
    }
    expected_names = {member.relative_name for member in item.members}
    if set(states) - expected_names:
        raise MigrationError("migration admission member set conflicts")
    with _guard(source_root) as source, _guard(target) as destination:
        for member in item.members:
            state = states.get(member.relative_name)
            if state is None:
                state = repository.checkpoint_admission_member(
                    journal.journal_id,
                    relative_name=member.relative_name,
                    sha256=member.sha256,
                    size_bytes=member.size_bytes,
                    phase="planned",
                )
            if not _name_exists(destination, member.relative_name):
                _copy_member(
                    source,
                    destination,
                    member,
                    repair_partial=state.phase == "planned",
                )
                _inject(
                    injector,
                    f"after_member_copied:{item.bundle_id}:{member.relative_name}",
                )
            _verify_member(destination, member)
            if state.phase == "planned":
                state = repository.checkpoint_admission_member(
                    journal.journal_id,
                    relative_name=member.relative_name,
                    sha256=member.sha256,
                    size_bytes=member.size_bytes,
                    phase="copied",
                )
            if state.phase == "copied":
                repository.checkpoint_admission_member(
                    journal.journal_id,
                    relative_name=member.relative_name,
                    sha256=member.sha256,
                    size_bytes=member.size_bytes,
                    phase="verified",
                )
    completed = repository.list_admission_members(journal.journal_id)
    if {member.relative_name for member in completed} != expected_names or any(
        member.phase != "verified" for member in completed
    ):
        raise MigrationError("migration admission member set is incomplete")


def _copy_member(
    source: _DirectoryGuard,
    destination: _DirectoryGuard,
    member: MigrationMember,
    *,
    repair_partial: bool,
) -> None:
    temporary = f".copy-{member.sha256}-{_name_digest(member.relative_name)}.tmp"
    if _name_exists(destination, temporary):
        try:
            _verify_member(destination, member, name=temporary)
        except MigrationError:
            if not repair_partial:
                raise
            metadata = _stat_at(destination, temporary)
            if not stat.S_ISREG(metadata.st_mode):
                raise MigrationError("migration partial copy is unsafe") from None
            _unlink_at(destination, temporary)
    if not _name_exists(destination, temporary):
        _write_temporary_copy(source, destination, member, temporary)
    _atomic_noreplace(destination, temporary, destination, member.relative_name)
    _fsync_guard(destination)


def _write_temporary_copy(
    source: _DirectoryGuard,
    destination: _DirectoryGuard,
    member: MigrationMember,
    temporary: str,
) -> None:
    source_file, identity = _open_regular(source, member.relative_name)
    try:
        descriptor = _open_exclusive(destination, temporary)
        with os.fdopen(descriptor, "wb") as output:
            digest = hashlib.sha256()
            size = 0
            while chunk := source_file.read(_COPY_BYTES):
                output.write(chunk)
                digest.update(chunk)
                size += len(chunk)
            output.flush()
            os.fsync(output.fileno())
        named = _stat_at(source, member.relative_name)
        if (
            not os.path.samestat(identity, os.fstat(source_file.fileno()))
            or not os.path.samestat(identity, named)
            or digest.hexdigest() != member.sha256
            or size != member.size_bytes
        ):
            raise MigrationError("legacy member changed during verified copy")
    finally:
        source_file.close()
    _fsync_guard(destination)
    _verify_member(destination, member, name=temporary)


def _verify_destination(
    destination: Path,
    item: MigrationItem,
    *,
    require_ready: bool,
) -> None:
    with _guard(destination) as guard:
        expected = {member.relative_name for member in item.members}
        if require_ready:
            expected.add(".ready")
        if _list_names(guard) != expected:
            raise MigrationError("migration destination membership conflicts")
        for member in item.members:
            _verify_member(guard, member)
        if require_ready:
            ready = _stat_at(guard, ".ready")
            if not stat.S_ISREG(ready.st_mode) or ready.st_size != 0:
                raise MigrationError("migration ready marker is invalid")
    scan = scan_inbox(destination)
    matches = tuple(
        bundle for bundle in scan.bundles if bundle.bundle_id == item.bundle_id
    )
    if scan.issues or len(matches) != 1 or matches[0].fingerprint != item.fingerprint:
        raise MigrationError("migration destination fingerprint conflicts")


def _install_ready(destination: Path) -> None:
    with _guard(destination) as guard:
        if _name_exists(guard, ".ready"):
            ready = _stat_at(guard, ".ready")
            if not stat.S_ISREG(ready.st_mode) or ready.st_size != 0:
                raise MigrationError("migration ready marker is invalid")
            return
        flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0)
        descriptor = (
            os.open(".ready", flags, 0o600, dir_fd=guard.fd)
            if guard.fd is not None and os.open in os.supports_dir_fd
            else os.open(guard.path / ".ready", flags, 0o600)
        )
        os.fsync(descriptor)
        os.close(descriptor)
        _fsync_guard(guard)


def _remove_sources(
    source_root: Path,
    item: MigrationItem,
    injector: MigrationFaultInjector | None,
) -> None:
    with _guard(source_root) as source:
        for member in item.members:
            quarantine = f".post-pulsar-migrate-{_name_digest(item.bundle_id + ':' + member.relative_name)}"
            source_exists = _name_exists(source, member.relative_name)
            quarantine_exists = _name_exists(source, quarantine)
            if source_exists and quarantine_exists:
                source_identity = _verify_member(source, member)
                quarantine_identity = _verify_member(source, member, name=quarantine)
                if not os.path.samestat(source_identity, quarantine_identity):
                    raise MigrationError("legacy source removal checkpoint conflicts")
            elif source_exists:
                identity = _verify_member(source, member)
                _link_at(source, member.relative_name, quarantine)
                linked = _stat_at(source, quarantine)
                if not os.path.samestat(identity, linked):
                    raise MigrationError("legacy source identity changed")
                quarantine_exists = True
            if source_exists:
                _unlink_at(source, member.relative_name)
                _inject(
                    injector,
                    f"after_source_removed:{item.bundle_id}:{member.relative_name}",
                )
            if quarantine_exists:
                _verify_member(source, member, name=quarantine)
                _unlink_at(source, quarantine)


def _profile_targets(profile: ProfileSettings) -> tuple[ProfileTargetSnapshot, ...]:
    targets: list[ProfileTargetSnapshot] = []
    if profile.x is not None:
        targets.append(
            ProfileTargetSnapshot(
                "x",
                profile.x.expected_remote_user_id,
                profile.x.expected_username,
                profile.x.token_env_var,
                {
                    "request_timeout_seconds": profile.x.request_timeout_seconds,
                    "processing_timeout_seconds": profile.x.processing_timeout_seconds,
                    "chunk_size_bytes": profile.x.chunk_size_bytes,
                },
            )
        )
    if profile.instagram is not None:
        targets.append(
            ProfileTargetSnapshot(
                "instagram",
                profile.instagram.expected_remote_user_id,
                profile.instagram.expected_username,
                profile.instagram.token_env_var,
                {
                    "media_directory": str(profile.instagram.media_directory),
                    "media_base_url": profile.instagram.media_base_url,
                    "request_timeout_seconds": profile.instagram.request_timeout_seconds,
                    "processing_timeout_seconds": profile.instagram.processing_timeout_seconds,
                },
            )
        )
    return tuple(targets)


def _inspect_database(database: Path) -> None:
    if not os.path.lexists(database):
        return
    metadata = os.lstat(database)
    if stat.S_ISLNK(metadata.st_mode) or not stat.S_ISREG(metadata.st_mode):
        raise MigrationError("migration database path is unsafe")
    try:
        with _read_only_database(database) as connection:
            version = int(connection.execute("PRAGMA user_version").fetchone()[0])
            application_id = int(
                connection.execute("PRAGMA application_id").fetchone()[0]
            )
            tables = {
                str(row[0])
                for row in connection.execute(
                    "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"
                )
            }
            if (
                version != SCHEMA_VERSION
                or application_id != _APPLICATION_ID
                or not _REQUIRED_TABLES.issubset(tables)
                or _schema_manifest_digest(connection)
                != _expected_schema_manifest_digest()
            ):
                raise MigrationError(
                    "existing database schema is not current and canonical"
                )
    except MigrationError:
        raise
    except sqlite3.DatabaseError:
        raise MigrationError(
            "existing database schema could not be validated"
        ) from None


def _validate_state_compatibility(
    database: Path, plan: _MigrationPlan, *, resuming: bool
) -> None:
    """Reject unrelated current state before the migration writes anything."""

    if not database.exists():
        return
    with _read_only_database(database) as connection:
        profiles = connection.execute(
            "SELECT profile_id, account_root, config_hash FROM profiles "
            "ORDER BY profile_id COLLATE NOCASE"
        ).fetchall()
        bundle_count = int(
            connection.execute("SELECT COUNT(*) FROM bundles").fetchone()[0]
        )
        delivery_count = int(
            connection.execute("SELECT COUNT(*) FROM deliveries").fetchone()[0]
        )
        admissions = connection.execute(
            "SELECT profile_id, bucket, bundle_id, fingerprint, source_path, "
            "destination_path FROM admission_journals ORDER BY bundle_id COLLATE NOCASE"
        ).fetchall()

    if not resuming:
        if profiles or bundle_count or delivery_count or admissions:
            raise MigrationError("existing current database is not empty")
        return
    if bundle_count or delivery_count or len(profiles) > 1:
        raise MigrationError("migration resume state contains unrelated records")
    if profiles:
        profile_id, account_root, config_hash = profiles[0]
        if (
            str(profile_id) != plan.profile_id
            or str(account_root) != plan.account_root
            or str(config_hash) != plan.config_hash
        ):
            raise MigrationError("migration resume profile binding conflicts")

    expected = {
        (
            plan.profile_id,
            "RANDOM",
            item.bundle_id,
            item.fingerprint,
            f"legacy/{item.source_relative}/{item.bundle_id}",
            item.destination_relative,
        )
        for item in plan.items
    }
    actual = {tuple(str(value) for value in row) for row in admissions}
    if not actual.issubset(expected):
        raise MigrationError("migration resume admissions conflict")


def _ensure_backup(database: Path, backup: Path) -> None:
    if backup.exists():
        if stat.S_IMODE(backup.stat().st_mode) != 0o600:
            raise MigrationError("migration backup permissions are unsafe")
        _inspect_database(backup)
        return
    descriptor, temporary_name = tempfile.mkstemp(
        prefix=".migration-backup-", dir=backup.parent
    )
    os.close(descriptor)
    temporary = Path(temporary_name)
    try:
        temporary.chmod(0o600)
        with (
            _read_only_database(database) as source,
            sqlite3.connect(temporary) as destination,
        ):
            source.backup(destination)
        with temporary.open("rb") as copied:
            os.fsync(copied.fileno())
        os.link(temporary, backup, follow_symlinks=False)
        backup.chmod(0o600)
        _fsync_path(backup.parent)
    except FileExistsError:
        if not backup.is_file():
            raise MigrationError("migration backup destination conflicts") from None
    finally:
        temporary.unlink(missing_ok=True)


def _validate_current_state(database: Path, profile_id: str, item_count: int) -> None:
    _inspect_database(database)
    with _read_only_database(database) as connection:
        if connection.execute("PRAGMA foreign_key_check").fetchall():
            raise MigrationError("migration state foreign keys are invalid")
        profiles = connection.execute(
            "SELECT COUNT(*) FROM profiles WHERE profile_id = ?", (profile_id,)
        ).fetchone()[0]
        deliveries = connection.execute("SELECT COUNT(*) FROM deliveries").fetchone()[0]
        admissions = connection.execute(
            "SELECT COUNT(*) FROM admission_journals WHERE profile_id = ? AND phase = 'installed'",
            (profile_id,),
        ).fetchone()[0]
        if profiles != 1 or deliveries != 0 or admissions != item_count:
            raise MigrationError("migration state counts are invalid")


def _validate_completed(
    plan: _MigrationPlan,
    database: Path,
    legacy_root: Path,
    account_root: Path,
) -> None:
    """Prove a completed journal remains complete without mutating state."""

    _validate_current_state(database, plan.profile_id, len(plan.items))
    for item in plan.items:
        _verify_destination(
            account_root / item.destination_relative, item, require_ready=True
        )
        source_root = (
            legacy_root
            if item.source_relative == "root"
            else legacy_root / item.source_relative
        )
        with _guard(source_root) as source:
            for member in item.members:
                quarantine = ".post-pulsar-migrate-" + _name_digest(
                    item.bundle_id + ":" + member.relative_name
                )
                if _name_exists(source, member.relative_name) or _name_exists(
                    source, quarantine
                ):
                    raise MigrationError("completed migration source still exists")


@contextmanager
def _read_only_database(path: Path):  # type: ignore[no-untyped-def]
    uri = f"file:{quote(str(path))}?mode=ro"
    connection = sqlite3.connect(uri, uri=True)
    try:
        yield connection
    finally:
        connection.close()


def _write_plan(path: Path, plan: _MigrationPlan) -> None:
    document = {"version": _JOURNAL_VERSION, **asdict(plan)}
    payload = json.dumps(
        document, sort_keys=True, separators=(",", ":"), ensure_ascii=True
    ).encode()
    descriptor, temporary_name = tempfile.mkstemp(
        prefix=".migration-journal-", dir=path.parent
    )
    temporary = Path(temporary_name)
    try:
        os.fchmod(descriptor, 0o600)
        with os.fdopen(descriptor, "wb") as output:
            output.write(payload)
            output.flush()
            os.fsync(output.fileno())
        if path.exists() and stat.S_IMODE(path.stat().st_mode) != 0o600:
            raise MigrationError("migration journal permissions are unsafe")
        os.replace(temporary, path)
        path.chmod(0o600)
        _fsync_path(path.parent)
    finally:
        temporary.unlink(missing_ok=True)


def _load_plan(path: Path) -> _MigrationPlan:
    metadata = os.lstat(path)
    if (
        stat.S_ISLNK(metadata.st_mode)
        or not stat.S_ISREG(metadata.st_mode)
        or stat.S_IMODE(metadata.st_mode) != 0o600
        or metadata.st_size > 1024 * 1024
    ):
        raise MigrationError("migration journal is unsafe")
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(data, Mapping) or data.get("version") != _JOURNAL_VERSION:
            raise ValueError
        raw_items = data["items"]
        if not isinstance(raw_items, list):
            raise ValueError
        items: list[MigrationItem] = []
        for raw_item in raw_items:
            if not isinstance(raw_item, Mapping) or not isinstance(
                raw_item.get("members"), list
            ):
                raise ValueError
            members = tuple(
                MigrationMember(
                    str(member["relative_name"]),
                    str(member["sha256"]),
                    int(member["size_bytes"]),
                )
                for member in raw_item["members"]
                if isinstance(member, Mapping)
            )
            if len(members) != len(raw_item["members"]):
                raise ValueError
            disposition = str(raw_item["disposition"])
            if disposition not in {"active", "archived"}:
                raise ValueError
            items.append(
                MigrationItem(
                    str(raw_item["bundle_id"]),
                    cast(MigrationDisposition, disposition),
                    str(raw_item["source_relative"]),
                    str(raw_item["destination_relative"]),
                    str(raw_item["fingerprint"]),
                    members,
                )
            )
        plan = _MigrationPlan(
            str(data["profile_id"]),
            str(data["legacy_root"]),
            str(data["account_root"]),
            str(data["state_directory"]),
            str(data["config_hash"]),
            str(data["phase"]),
            tuple(items),
            tuple(str(value) for value in data["untouched"]),
            tuple(str(value) for value in data["issues"]),
        )
    except (KeyError, TypeError, ValueError, json.JSONDecodeError, UnicodeError):
        raise MigrationError("migration journal is invalid") from None
    _validate_loaded_plan(plan)
    return plan


def _validate_loaded_plan(plan: _MigrationPlan) -> None:
    if plan.phase not in {"planned", "state_ready", "complete"}:
        raise MigrationError("migration journal phase is invalid")
    if not _PROFILE_RE.fullmatch(plan.profile_id) or not _SHA256_RE.fullmatch(
        plan.config_hash
    ):
        raise MigrationError("migration journal identity is invalid")
    seen: set[str] = set()
    for item in plan.items:
        expected_source = "root" if item.disposition == "active" else "posted"
        expected_destination = (
            f"RANDOM/{item.bundle_id}"
            if item.disposition == "active"
            else f"POSTED/RANDOM/{item.bundle_id}"
        )
        if (
            not item.members
            or not _BUNDLE_RE.fullmatch(item.bundle_id)
            or not _SHA256_RE.fullmatch(item.fingerprint)
            or item.bundle_id.casefold() in seen
            or item.source_relative != expected_source
            or item.destination_relative != expected_destination
        ):
            raise MigrationError("migration journal item is invalid")
        seen.add(item.bundle_id.casefold())
        names: set[str] = set()
        for member in item.members:
            if (
                Path(member.relative_name).name != member.relative_name
                or member.relative_name in {"", ".", "..", ".ready"}
                or member.relative_name.casefold() in names
                or not _SHA256_RE.fullmatch(member.sha256)
                or member.size_bytes <= 0
            ):
                raise MigrationError("migration journal member is invalid")
            names.add(member.relative_name.casefold())
    for value in plan.untouched:
        path = Path(value)
        if not value or path.is_absolute() or ".." in path.parts:
            raise MigrationError("migration journal untouched path is invalid")


def _assert_plan_binding(
    plan: _MigrationPlan,
    profile_id: str,
    legacy_root: Path,
    account_root: Path,
    state_root: Path,
    config_hash: str,
) -> None:
    if (
        plan.profile_id != profile_id
        or plan.legacy_root != str(legacy_root)
        or plan.account_root != str(account_root)
        or plan.state_directory != str(state_root)
        or plan.config_hash != config_hash
    ):
        raise MigrationError(
            "migration journal belongs to another explicit profile or layout"
        )


def _replace_phase(plan: _MigrationPlan, phase: str) -> _MigrationPlan:
    return _MigrationPlan(
        plan.profile_id,
        plan.legacy_root,
        plan.account_root,
        plan.state_directory,
        plan.config_hash,
        phase,
        plan.items,
        plan.untouched,
        plan.issues,
    )


def _report(plan: _MigrationPlan, mode: MigrationMode) -> MigrationReport:
    phase = plan.phase if mode == "apply" else "planned"
    return MigrationReport(
        plan.profile_id,
        mode,
        phase,
        plan.items,
        plan.untouched,
        plan.issues,
        (
            f"profile:{plan.profile_id}",
            "official_credentials_not_migrated",
            "remote_identities_not_inferred",
        ),
        (
            "create_current_schema_state",
            "register_explicit_profile_without_delivery_outcomes",
            f"journal_{len(plan.items)}_exact_bundles",
        ),
    )


def _verified_directory(path: Path, label: str) -> Path:
    absolute = path.expanduser().absolute()
    _reject_symlink_components(absolute, label)
    try:
        metadata = os.lstat(absolute)
    except OSError:
        raise MigrationError(f"{label} directory is unavailable") from None
    if stat.S_ISLNK(metadata.st_mode) or not stat.S_ISDIR(metadata.st_mode):
        raise MigrationError(f"{label} directory is unsafe")
    return absolute


def _reject_symlink_components(path: Path, label: str) -> None:
    current = Path(path.anchor)
    for component in path.relative_to(current).parts:
        current /= component
        try:
            metadata = os.lstat(current)
        except FileNotFoundError:
            return
        except OSError:
            raise MigrationError(f"{label} path is unavailable") from None
        if stat.S_ISLNK(metadata.st_mode) or not stat.S_ISDIR(metadata.st_mode):
            raise MigrationError(f"{label} path is unsafe")


def _reject_path_overlap(source: Path, destination: Path, label: str) -> None:
    source_parts = tuple(part.casefold() for part in source.parts)
    destination_parts = tuple(part.casefold() for part in destination.parts)
    shorter = min(len(source_parts), len(destination_parts))
    if source_parts[:shorter] == destination_parts[:shorter]:
        raise MigrationError(f"{label} overlaps the legacy source")


def _ensure_directory(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True, mode=0o700)
    current = Path(path.anchor)
    for component in path.relative_to(current).parts:
        current /= component
        metadata = os.lstat(current)
        if stat.S_ISLNK(metadata.st_mode) or not stat.S_ISDIR(metadata.st_mode):
            raise MigrationError("migration destination path is unsafe")


@contextmanager
def _guard(path: Path):  # type: ignore[no-untyped-def]
    try:
        guard = _DirectoryGuard.open(path)
    except (OSError, RuntimeError):
        raise MigrationError("migration directory identity is unsafe") from None
    try:
        yield guard
    finally:
        guard.close()


def _open_regular(
    directory: _DirectoryGuard, name: str
) -> tuple[BinaryIO, os.stat_result]:
    before = _stat_at(directory, name)
    if not stat.S_ISREG(before.st_mode):
        raise MigrationError("legacy member is not a regular file")
    flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0)
    descriptor = (
        os.open(name, flags, dir_fd=directory.fd)
        if directory.fd is not None and os.open in os.supports_dir_fd
        else os.open(directory.path / name, flags)
    )
    source = os.fdopen(descriptor, "rb")
    opened = os.fstat(source.fileno())
    if not os.path.samestat(before, opened):
        source.close()
        raise MigrationError("legacy member identity changed")
    return source, opened


def _hash_regular(root: Path, name: str) -> tuple[str, int, os.stat_result]:
    with _guard(root) as directory:
        source, identity = _open_regular(directory, name)
        digest = hashlib.sha256()
        size = 0
        try:
            while chunk := source.read(_COPY_BYTES):
                digest.update(chunk)
                size += len(chunk)
            named = _stat_at(directory, name)
            if not os.path.samestat(identity, named):
                raise MigrationError("legacy member identity changed")
        finally:
            source.close()
    return digest.hexdigest(), size, identity


def _verify_member(
    directory: _DirectoryGuard,
    member: MigrationMember,
    *,
    name: str | None = None,
) -> os.stat_result:
    source, identity = _open_regular(directory, name or member.relative_name)
    digest = hashlib.sha256()
    size = 0
    try:
        while chunk := source.read(_COPY_BYTES):
            digest.update(chunk)
            size += len(chunk)
        named = _stat_at(directory, name or member.relative_name)
        if not os.path.samestat(identity, named):
            raise MigrationError("migration member identity changed")
    finally:
        source.close()
    if digest.hexdigest() != member.sha256 or size != member.size_bytes:
        raise MigrationError("migration member hash or size conflicts")
    return identity


def _stat_at(directory: _DirectoryGuard, name: str) -> os.stat_result:
    directory.validate()
    return (
        os.stat(name, dir_fd=directory.fd, follow_symlinks=False)
        if directory.fd is not None
        else os.stat(directory.path / name, follow_symlinks=False)
    )


def _name_exists(directory: _DirectoryGuard, name: str) -> bool:
    try:
        _stat_at(directory, name)
    except FileNotFoundError:
        return False
    return True


def _list_names(directory: _DirectoryGuard) -> set[str]:
    directory.validate()
    return set(os.listdir(directory.fd if directory.fd is not None else directory.path))


def _open_exclusive(directory: _DirectoryGuard, name: str) -> int:
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0)
    return (
        os.open(name, flags, 0o600, dir_fd=directory.fd)
        if directory.fd is not None and os.open in os.supports_dir_fd
        else os.open(directory.path / name, flags, 0o600)
    )


def _link_at(directory: _DirectoryGuard, source: str, destination: str) -> None:
    if directory.fd is not None and os.link in os.supports_dir_fd:
        os.link(
            source,
            destination,
            src_dir_fd=directory.fd,
            dst_dir_fd=directory.fd,
            follow_symlinks=False,
        )
    else:
        os.link(
            directory.path / source,
            directory.path / destination,
            follow_symlinks=False,
        )
    _fsync_guard(directory)


def _unlink_at(directory: _DirectoryGuard, name: str) -> None:
    if directory.fd is not None and os.unlink in os.supports_dir_fd:
        os.unlink(name, dir_fd=directory.fd)
    else:
        (directory.path / name).unlink()
    _fsync_guard(directory)


def _name_digest(value: str) -> str:
    return hashlib.sha256(value.encode()).hexdigest()


def _fsync_path(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _inject(injector: MigrationFaultInjector | None, boundary: str) -> None:
    if injector is not None:
        injector(boundary)


__all__ = [
    "MigrationDisposition",
    "MigrationError",
    "MigrationFaultInjector",
    "MigrationItem",
    "MigrationMember",
    "MigrationMode",
    "MigrationReport",
    "migrate_legacy_layout",
]
