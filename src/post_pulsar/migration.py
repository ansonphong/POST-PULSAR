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
MigrationMemberRole = Literal["caption", "alt_text", "image", "video"]

_PROFILE_RE: Final = re.compile(r"[a-z0-9][a-z0-9-]{0,31}\Z")
_BUNDLE_RE: Final = re.compile(r"[A-Za-z0-9](?:[A-Za-z0-9_-]{0,62}[A-Za-z0-9])?\Z")
_SHA256_RE: Final = re.compile(r"[0-9a-f]{64}\Z")
_JOURNAL_NAME: Final = "legacy-migration-v1.json"
_BACKUP_NAME: Final = "post_pulsar.pre-migration-v1.sqlite3"
_DATABASE_NAME: Final = "post_pulsar.sqlite3"
_JOURNAL_VERSION: Final = 2
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
    role: MigrationMemberRole
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
    database_sha256: str | None
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

    journal_existed = os.path.lexists(journal_path)
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
        _rebind_semantic_plan(plan, legacy_root, account_root)
    else:
        plan = _build_plan(
            selected_profile_id,
            legacy_root,
            account_root,
            state_root,
            config_hash,
        )
        _validate_destinations(plan, allow_existing=False)

    database = state_root / _DATABASE_NAME
    backup = state_root / _BACKUP_NAME
    if normalized_mode == "dry-run":
        plan = _preflight_state(plan, database, backup, resuming=journal_existed)
        completed_noop = plan.phase == "complete"
        if completed_noop:
            _validate_completed(plan, database, legacy_root, account_root)
        return _report(plan, "dry-run", completed_noop=completed_noop)

    injector = cast(MigrationFaultInjector | None, fault_injector)
    locks = LockManager(state_root)
    with locks.acquire_instance() as instance:
        with locks.acquire_maintenance(instance) as maintenance:
            with locks.acquire_profiles(
                instance, (selected_profile_id,), maintenance=maintenance
            ):
                database_existed = os.path.lexists(database)
                if journal_existed:
                    _validate_destinations(plan, allow_existing=True)
                    _recover_bound_wal(database, backup, plan, profile)
                plan = _preflight_state(
                    plan, database, backup, resuming=journal_existed
                )
                if plan.phase == "complete":
                    _validate_completed(plan, database, legacy_root, account_root)
                    return _report(plan, "apply")
                if not journal_existed:
                    _write_plan(journal_path, plan)
                    _inject(injector, "after_journal_created")
                if database_existed and plan.database_sha256 is not None:
                    _ensure_backup(database, backup, plan.database_sha256)

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
        None,
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
    return MigrationMember(path.name, _member_role(path.name), digest, size)


def _member_role(name: str) -> MigrationMemberRole:
    path = Path(name)
    extension = path.suffix.casefold()
    if extension == ".txt":
        return "alt_text" if path.stem.casefold().endswith("-alt") else "caption"
    if extension in {".jpg", ".jpeg", ".png", ".gif"}:
        return "image"
    if extension in {".mp4", ".mov"}:
        return "video"
    raise MigrationError("migration journal member role is invalid")


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


def _rebind_semantic_plan(
    plan: _MigrationPlan, legacy_root: Path, account_root: Path
) -> None:
    """Prove every journal item is still an exact parser-produced bundle."""

    scans: dict[str, tuple[ContentBundle, ...]] = {}
    represented: dict[str, set[str]] = {"root": set(), "posted": set()}
    for source_relative in ("root", "posted"):
        source = legacy_root if source_relative == "root" else legacy_root / "posted"
        if source_relative == "posted" and not os.path.lexists(source):
            scans[source_relative] = ()
            continue
        bundles, _issues = _scan_legacy_location(
            source,
            excluded_directory="posted" if source_relative == "root" else None,
        )
        scans[source_relative] = bundles

    expected_by_source = {
        (item.source_relative, item.bundle_id): item for item in plan.items
    }
    for source_relative, bundles in scans.items():
        for bundle in bundles:
            key = (source_relative, bundle.bundle_id)
            item = expected_by_source.get(key)
            if item is None:
                raise MigrationError("migration journal omits a source bundle")
            represented[source_relative].add(bundle.bundle_id)
            _compare_bundle_manifest(bundle, item, legacy_root)

    for item in plan.items:
        destination = account_root / item.destination_relative
        if os.path.lexists(destination):
            _verify_destination(destination, item, require_ready=True)
            continue
        if item.bundle_id not in represented[item.source_relative]:
            raise MigrationError("migration journal bundle no longer matches source")


def _compare_bundle_manifest(
    bundle: ContentBundle, item: MigrationItem, legacy_root: Path
) -> None:
    source = (
        legacy_root
        if item.source_relative == "root"
        else legacy_root / item.source_relative
    )
    expected = tuple(
        (member.relative_name, member.role, member.sha256, member.size_bytes)
        for member in item.members
    )
    actual = tuple(
        (
            path.name,
            _member_role(path.name),
            *_hash_regular(source, path.name)[:2],
        )
        for path in sorted(
            bundle.members,
            key=lambda candidate: (candidate.name.casefold(), candidate.name),
        )
    )
    if bundle.fingerprint != item.fingerprint or actual != expected:
        raise MigrationError("migration journal member manifest conflicts with source")


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
            _inspect_database_connection(connection)
    except MigrationError:
        raise
    except sqlite3.DatabaseError:
        raise MigrationError(
            "existing database schema could not be validated"
        ) from None


def _inspect_database_connection(connection: sqlite3.Connection) -> None:
    version = int(connection.execute("PRAGMA user_version").fetchone()[0])
    application_id = int(connection.execute("PRAGMA application_id").fetchone()[0])
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
        or _schema_manifest_digest(connection) != _expected_schema_manifest_digest()
    ):
        raise MigrationError("existing database schema is not current and canonical")


def _recover_bound_wal(
    database: Path,
    backup: Path,
    plan: _MigrationPlan,
    profile: ProfileSettings,
) -> None:
    """Checkpoint only authoritative WAL state proven to belong to this journal."""

    if not os.path.lexists(database):
        return
    wal = Path(f"{database}-wal")
    wal_before = _immutable_file_snapshot(wal, required=False)
    if wal_before is None or wal_before[3] == 0:
        return
    if plan.database_sha256 is None:
        if os.path.lexists(backup):
            raise MigrationError("migration recovery backup binding conflicts")
    else:
        if not os.path.lexists(backup):
            raise MigrationError("migration recovery backup checkpoint is missing")
        _validate_backup(backup, plan.database_sha256)
    database_before = _immutable_file_snapshot(database, required=True)
    if database_before is None:  # pragma: no cover - required=True is fail closed
        raise MigrationError("migration database is unavailable")
    shm = Path(f"{database}-shm")
    shm_before = _immutable_file_snapshot(shm, required=False)
    with tempfile.TemporaryDirectory(
        prefix=".migration-wal-inspection-", dir=database.parent
    ) as temporary_directory:
        inspection_database = Path(temporary_directory) / database.name
        _copy_database_snapshot(database, inspection_database, database_before)
        _copy_database_snapshot(
            wal,
            Path(f"{inspection_database}-wal"),
            wal_before,
        )
        inspection_uri = f"file:{quote(str(inspection_database))}?mode=rw"
        try:
            inspection = sqlite3.connect(
                inspection_uri, uri=True, isolation_level=None, timeout=0
            )
            try:
                _inspect_database_connection(inspection)
                _validate_bound_recovery_state(inspection, plan, profile)
            finally:
                inspection.close()
        except MigrationError:
            raise
        except sqlite3.DatabaseError:
            raise MigrationError(
                "migration recovery WAL could not be validated"
            ) from None
    if (
        _immutable_file_snapshot(database, required=True) != database_before
        or _immutable_file_snapshot(wal, required=True) != wal_before
        or _immutable_file_snapshot(shm, required=False) != shm_before
    ):
        raise MigrationError("migration recovery database changed during validation")
    uri = f"file:{quote(str(database))}?mode=rw"
    try:
        connection = sqlite3.connect(uri, uri=True, isolation_level=None, timeout=0)
        try:
            checkpoint = connection.execute(
                "PRAGMA wal_checkpoint(TRUNCATE)"
            ).fetchone()
            if checkpoint is None or int(checkpoint[0]) != 0:
                raise MigrationError("migration recovery WAL remains busy")
        finally:
            connection.close()
    except MigrationError:
        raise
    except sqlite3.DatabaseError:
        raise MigrationError("migration recovery WAL could not be validated") from None
    remaining = _immutable_file_snapshot(wal, required=False)
    if remaining is not None and remaining[3] > 0:
        raise MigrationError("migration recovery WAL could not be checkpointed")


def _copy_database_snapshot(
    source: Path,
    destination: Path,
    expected: tuple[int, int, int, int, int],
) -> None:
    flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0)
    try:
        descriptor = os.open(source, flags)
        opened = os.fstat(descriptor)
        with (
            os.fdopen(descriptor, "rb") as input_file,
            destination.open("xb") as output,
        ):
            while chunk := input_file.read(_COPY_BYTES):
                output.write(chunk)
            output.flush()
            os.fsync(output.fileno())
        named = os.lstat(source)
    except (FileExistsError, FileNotFoundError, OSError):
        raise MigrationError("migration recovery database snapshot is unsafe") from None
    opened_snapshot = (
        opened.st_dev,
        opened.st_ino,
        opened.st_mode,
        opened.st_size,
        opened.st_mtime_ns,
    )
    named_snapshot = (
        named.st_dev,
        named.st_ino,
        named.st_mode,
        named.st_size,
        named.st_mtime_ns,
    )
    if opened_snapshot != expected or named_snapshot != expected:
        raise MigrationError("migration recovery database changed during snapshot")
    destination.chmod(0o600)


def _validate_bound_recovery_state(
    connection: sqlite3.Connection,
    plan: _MigrationPlan,
    profile: ProfileSettings,
) -> None:
    if connection.execute("PRAGMA foreign_key_check").fetchall():
        raise MigrationError("migration recovery state foreign keys are invalid")
    forbidden_tables = (
        "bundles",
        "target_snapshots",
        "bundle_files",
        "deliveries",
        "delivery_artifacts",
        "events",
        "schedules",
        "schedule_runs",
        "run_requests",
        "confirmation_intents",
        "selection_counters",
    )
    if any(
        int(connection.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0])
        for table in forbidden_tables
    ):
        raise MigrationError("migration recovery state contains unrelated records")
    pause_rows = connection.execute(
        "SELECT singleton, paused, revision FROM pause_state"
    ).fetchall()
    if [tuple(int(value) for value in row) for row in pause_rows] != [(1, 0, 1)]:
        raise MigrationError("migration recovery pause state conflicts")

    profiles = connection.execute(
        "SELECT profile_id, account_root, config_hash FROM profiles"
    ).fetchall()
    if len(profiles) > 1:
        raise MigrationError("migration recovery profile binding conflicts")
    if profiles:
        stored_profile = tuple(str(value) for value in profiles[0])
        expected_profile = (plan.profile_id, plan.account_root, plan.config_hash)
        if stored_profile != expected_profile:
            raise MigrationError("migration recovery profile binding conflicts")
        _validate_recovery_targets(connection, profile)
    elif plan.phase != "planned":
        raise MigrationError("migration recovery profile checkpoint is missing")
    elif connection.execute("SELECT COUNT(*) FROM profile_targets").fetchone()[0]:
        raise MigrationError("migration recovery target binding conflicts")

    expected_items = {item.bundle_id.casefold(): item for item in plan.items}
    admissions = connection.execute(
        "SELECT journal_id, profile_id, bucket, bundle_id, fingerprint, source_path, "
        "destination_path, intent_id, phase FROM admission_journals"
    ).fetchall()
    if admissions and not profiles:
        raise MigrationError("migration recovery admission has no profile")
    seen: set[str] = set()
    for row in admissions:
        journal_id = int(row[0])
        bundle_id = str(row[3])
        key = bundle_id.casefold()
        item = expected_items.get(key)
        if item is None or key in seen:
            raise MigrationError("migration recovery admission conflicts")
        seen.add(key)
        expected_identity = (
            plan.profile_id,
            "RANDOM",
            item.bundle_id,
            item.fingerprint,
            f"legacy/{item.source_relative}/{item.bundle_id}",
            item.destination_relative,
            None,
        )
        actual_identity = (*tuple(str(value) for value in row[1:7]), row[7])
        if actual_identity != expected_identity:
            raise MigrationError("migration recovery admission conflicts")
        phase = str(row[8])
        if phase not in {"started", "copying", "ready_installed", "installed"}:
            raise MigrationError("migration recovery admission phase conflicts")
        _validate_recovery_members(connection, journal_id, item, phase)
        _validate_recovery_filesystem_checkpoint(plan, item, phase)


def _validate_recovery_targets(
    connection: sqlite3.Connection, profile: ProfileSettings
) -> None:
    expected: dict[str, tuple[str, str, str, dict[str, object]]] = {
        target.platform: (
            target.expected_remote_user_id,
            target.expected_username,
            target.token_env_var,
            dict(target.request_settings),
        )
        for target in _profile_targets(profile)
    }
    rows = connection.execute(
        "SELECT platform, expected_remote_user_id, expected_username, token_env_var, "
        "request_settings_json, request_settings_sha256 FROM profile_targets"
    ).fetchall()
    if len(rows) != len(expected):
        raise MigrationError("migration recovery target binding conflicts")
    for row in rows:
        platform = str(row[0])
        target = expected.get(platform)
        settings_text = str(row[4])
        try:
            settings = json.loads(settings_text)
        except json.JSONDecodeError:
            raise MigrationError(
                "migration recovery target binding conflicts"
            ) from None
        if (
            target is None
            or tuple(str(value) for value in row[1:4]) != target[:3]
            or settings != target[3]
            or hashlib.sha256(settings_text.encode()).hexdigest() != str(row[5])
        ):
            raise MigrationError("migration recovery target binding conflicts")


def _validate_recovery_members(
    connection: sqlite3.Connection,
    journal_id: int,
    item: MigrationItem,
    admission_phase: str,
) -> None:
    expected = {member.relative_name.casefold(): member for member in item.members}
    rows = connection.execute(
        "SELECT relative_name, sha256, size_bytes, phase FROM admission_members "
        "WHERE journal_id = ?",
        (journal_id,),
    ).fetchall()
    seen: set[str] = set()
    phases: list[str] = []
    for row in rows:
        name = str(row[0])
        key = name.casefold()
        member = expected.get(key)
        phase = str(row[3])
        if (
            member is None
            or key in seen
            or name != member.relative_name
            or str(row[1]) != member.sha256
            or int(row[2]) != member.size_bytes
            or phase not in {"planned", "copied", "verified"}
        ):
            raise MigrationError("migration recovery admission member conflicts")
        seen.add(key)
        phases.append(phase)
    if admission_phase == "started" and rows:
        raise MigrationError("migration recovery admission member phase conflicts")
    if admission_phase == "copying" and not rows:
        raise MigrationError("migration recovery admission member phase conflicts")
    if admission_phase in {"ready_installed", "installed"} and (
        seen != set(expected) or any(phase != "verified" for phase in phases)
    ):
        raise MigrationError("migration recovery admission member phase conflicts")


def _validate_recovery_filesystem_checkpoint(
    plan: _MigrationPlan, item: MigrationItem, admission_phase: str
) -> None:
    destination = Path(plan.account_root) / item.destination_relative
    staging = destination.parent / f".{item.bundle_id}.{item.fingerprint}.migration"
    if admission_phase == "installed":
        if not os.path.lexists(destination):
            raise MigrationError("migration recovery installed destination is missing")
        _verify_destination(destination, item, require_ready=True)
    elif admission_phase == "ready_installed":
        target = destination if os.path.lexists(destination) else staging
        if not os.path.lexists(target):
            raise MigrationError("migration recovery ready destination is missing")
        _verify_destination(target, item, require_ready=True)


def _preflight_state(
    plan: _MigrationPlan,
    database: Path,
    backup: Path,
    *,
    resuming: bool,
) -> _MigrationPlan:
    """Run identical read-only state checks for dry-run and apply."""

    _inspect_database(database)
    if not resuming:
        database_sha256 = (
            _database_content_digest(database, label="database")
            if os.path.lexists(database)
            else None
        )
        plan = _replace_database_digest(plan, database_sha256)
    elif plan.database_sha256 is not None and not _SHA256_RE.fullmatch(
        plan.database_sha256
    ):
        raise MigrationError("migration database binding is invalid")
    _validate_state_compatibility(database, plan, resuming=resuming)
    _validate_destinations(plan, allow_existing=resuming)
    if os.path.lexists(backup):
        if plan.database_sha256 is None:
            raise MigrationError("migration backup has no database binding")
        _validate_backup(backup, plan.database_sha256)
    return plan


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


def _ensure_backup(database: Path, backup: Path, expected_digest: str) -> None:
    if os.path.lexists(backup):
        _validate_backup(backup, expected_digest)
        return
    if _database_content_digest(database, label="database") != expected_digest:
        raise MigrationError("migration database changed before backup")
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
        _validate_backup(temporary, expected_digest)
        os.link(temporary, backup, follow_symlinks=False)
        backup.chmod(0o600)
        _fsync_path(backup.parent)
        _validate_backup(backup, expected_digest)
    except FileExistsError:
        _validate_backup(backup, expected_digest)
    finally:
        temporary.unlink(missing_ok=True)


def _validate_backup(backup: Path, expected_digest: str) -> None:
    try:
        metadata = os.lstat(backup)
    except OSError:
        raise MigrationError("migration backup is unavailable") from None
    if (
        stat.S_ISLNK(metadata.st_mode)
        or not stat.S_ISREG(metadata.st_mode)
        or stat.S_IMODE(metadata.st_mode) != 0o600
        or (hasattr(os, "geteuid") and metadata.st_uid != os.geteuid())
    ):
        raise MigrationError("migration backup permissions or identity are unsafe")
    _inspect_database(backup)
    if (
        _database_content_digest(backup, label="backup", require_owner_only=True)
        != expected_digest
    ):
        raise MigrationError(
            "migration backup does not match the pre-migration database"
        )


def _database_content_digest(
    database: Path, *, label: str, require_owner_only: bool = False
) -> str:
    """Hash canonical schema and rows while proving the named file stayed stable."""

    try:
        before = os.lstat(database)
        flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0)
        descriptor = os.open(database, flags)
    except OSError:
        raise MigrationError(f"migration {label} identity is unsafe") from None
    try:
        opened = os.fstat(descriptor)
        if (
            stat.S_ISLNK(before.st_mode)
            or not stat.S_ISREG(before.st_mode)
            or not os.path.samestat(before, opened)
            or (
                require_owner_only
                and (
                    stat.S_IMODE(opened.st_mode) != 0o600
                    or (hasattr(os, "geteuid") and opened.st_uid != os.geteuid())
                )
            )
        ):
            raise MigrationError(f"migration {label} identity is unsafe")
        digest = hashlib.sha256()
        with _read_only_database(database) as connection:
            connection.execute("BEGIN")
            for statement in connection.iterdump():
                encoded = statement.encode("utf-8")
                digest.update(len(encoded).to_bytes(8, "big"))
                digest.update(encoded)
            connection.rollback()
        after = os.lstat(database)
        if not os.path.samestat(opened, after) or (
            require_owner_only
            and (
                stat.S_IMODE(after.st_mode) != 0o600
                or (hasattr(os, "geteuid") and after.st_uid != os.geteuid())
            )
        ):
            raise MigrationError(f"migration {label} identity changed")
        return digest.hexdigest()
    except (OSError, sqlite3.DatabaseError, UnicodeError):
        raise MigrationError(f"migration {label} could not be authenticated") from None
    finally:
        os.close(descriptor)


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
    database_before = _immutable_file_snapshot(path, required=True)
    wal = Path(f"{path}-wal")
    shm = Path(f"{path}-shm")
    wal_before = _immutable_file_snapshot(wal, required=False)
    shm_before = _immutable_file_snapshot(shm, required=False)
    if wal_before is not None and wal_before[3] > 0:
        raise MigrationError(
            "migration database has an active WAL and cannot be inspected read-only"
        )
    uri = f"file:{quote(str(path))}?mode=ro&immutable=1"
    connection = sqlite3.connect(uri, uri=True)
    try:
        yield connection
    except BaseException:
        connection.close()
        raise
    else:
        connection.close()
        if (
            _immutable_file_snapshot(path, required=True) != database_before
            or _immutable_file_snapshot(wal, required=False) != wal_before
            or _immutable_file_snapshot(shm, required=False) != shm_before
        ):
            raise MigrationError(
                "migration database changed during immutable inspection"
            )


def _immutable_file_snapshot(
    path: Path, *, required: bool
) -> tuple[int, int, int, int, int] | None:
    try:
        metadata = os.lstat(path)
    except FileNotFoundError:
        if required:
            raise MigrationError("migration database is unavailable") from None
        return None
    except OSError:
        raise MigrationError("migration database sidecar is unsafe") from None
    if stat.S_ISLNK(metadata.st_mode) or not stat.S_ISREG(metadata.st_mode):
        raise MigrationError("migration database sidecar is unsafe")
    return (
        metadata.st_dev,
        metadata.st_ino,
        metadata.st_mode,
        metadata.st_size,
        metadata.st_mtime_ns,
    )


def _write_plan(path: Path, plan: _MigrationPlan) -> None:
    plan_document = asdict(plan)
    document = {
        "version": _JOURNAL_VERSION,
        "plan_sha256": _canonical_document_digest(plan_document),
        **plan_document,
    }
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
        descriptor = os.open(path, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0))
        with os.fdopen(descriptor, "rb") as source:
            opened = os.fstat(source.fileno())
            payload = source.read(1024 * 1024 + 1)
        after = os.lstat(path)
        if (
            not os.path.samestat(metadata, opened)
            or not os.path.samestat(opened, after)
            or len(payload) > 1024 * 1024
        ):
            raise ValueError
        data = json.loads(payload.decode("utf-8"))
        root_keys = {
            "version",
            "plan_sha256",
            "profile_id",
            "legacy_root",
            "account_root",
            "state_directory",
            "config_hash",
            "database_sha256",
            "phase",
            "items",
            "untouched",
            "issues",
        }
        if (
            not isinstance(data, Mapping)
            or set(data) != root_keys
            or data.get("version") != _JOURNAL_VERSION
            or not isinstance(data.get("plan_sha256"), str)
            or any(
                not isinstance(data[key], str)
                for key in {
                    "profile_id",
                    "legacy_root",
                    "account_root",
                    "state_directory",
                    "config_hash",
                    "phase",
                }
            )
            or (
                data["database_sha256"] is not None
                and not isinstance(data["database_sha256"], str)
            )
            or not isinstance(data["items"], list)
            or not isinstance(data["untouched"], list)
            or any(not isinstance(value, str) for value in data["untouched"])
            or not isinstance(data["issues"], list)
            or any(not isinstance(value, str) for value in data["issues"])
        ):
            raise ValueError
        plan_document = {
            key: data[key] for key in root_keys - {"version", "plan_sha256"}
        }
        if (
            not hashlib.sha256(
                json.dumps(
                    plan_document,
                    sort_keys=True,
                    separators=(",", ":"),
                    ensure_ascii=True,
                ).encode()
            ).hexdigest()
            == data["plan_sha256"]
        ):
            raise ValueError
        raw_items = data["items"]
        if not isinstance(raw_items, list):
            raise ValueError
        items: list[MigrationItem] = []
        for raw_item in raw_items:
            if (
                not isinstance(raw_item, Mapping)
                or set(raw_item)
                != {
                    "bundle_id",
                    "disposition",
                    "source_relative",
                    "destination_relative",
                    "fingerprint",
                    "members",
                }
                or not isinstance(raw_item.get("members"), list)
                or any(
                    not isinstance(raw_item[key], str)
                    for key in {
                        "bundle_id",
                        "disposition",
                        "source_relative",
                        "destination_relative",
                        "fingerprint",
                    }
                )
            ):
                raise ValueError
            members_list: list[MigrationMember] = []
            for member in raw_item["members"]:
                if (
                    not isinstance(member, Mapping)
                    or set(member) != {"relative_name", "role", "sha256", "size_bytes"}
                    or not isinstance(member["relative_name"], str)
                    or not isinstance(member["role"], str)
                    or member["role"] not in {"caption", "alt_text", "image", "video"}
                    or not isinstance(member["sha256"], str)
                    or not isinstance(member["size_bytes"], int)
                    or isinstance(member["size_bytes"], bool)
                ):
                    raise ValueError
                members_list.append(
                    MigrationMember(
                        member["relative_name"],
                        cast(MigrationMemberRole, member["role"]),
                        member["sha256"],
                        member["size_bytes"],
                    )
                )
            members = tuple(members_list)
            disposition = raw_item["disposition"]
            if disposition not in {"active", "archived"}:
                raise ValueError
            items.append(
                MigrationItem(
                    raw_item["bundle_id"],
                    cast(MigrationDisposition, disposition),
                    raw_item["source_relative"],
                    raw_item["destination_relative"],
                    raw_item["fingerprint"],
                    members,
                )
            )
        plan = _MigrationPlan(
            data["profile_id"],
            data["legacy_root"],
            data["account_root"],
            data["state_directory"],
            data["config_hash"],
            (
                str(data["database_sha256"])
                if data["database_sha256"] is not None
                else None
            ),
            data["phase"],
            tuple(items),
            tuple(str(value) for value in data["untouched"]),
            tuple(str(value) for value in data["issues"]),
        )
    except (
        KeyError,
        OSError,
        TypeError,
        ValueError,
        json.JSONDecodeError,
        UnicodeError,
    ):
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
    if plan.database_sha256 is not None and not _SHA256_RE.fullmatch(
        plan.database_sha256
    ):
        raise MigrationError("migration journal database binding is invalid")
    if not plan.items or plan.items != tuple(
        sorted(
            plan.items,
            key=lambda item: (
                item.disposition,
                item.bundle_id.casefold(),
                item.bundle_id,
            ),
        )
    ):
        raise MigrationError("migration journal item ordering is invalid")
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
        if item.members != tuple(
            sorted(
                item.members,
                key=lambda member: (
                    member.relative_name.casefold(),
                    member.relative_name,
                ),
            )
        ):
            raise MigrationError("migration journal member ordering is invalid")
        for member in item.members:
            if (
                Path(member.relative_name).name != member.relative_name
                or member.relative_name in {"", ".", "..", ".ready"}
                or member.relative_name.casefold() in names
                or not _SHA256_RE.fullmatch(member.sha256)
                or member.size_bytes <= 0
                or member.role != _member_role(member.relative_name)
            ):
                raise MigrationError("migration journal member is invalid")
            names.add(member.relative_name.casefold())
    for value in plan.untouched:
        path = Path(value)
        if not value or path.is_absolute() or ".." in path.parts:
            raise MigrationError("migration journal untouched path is invalid")
    if plan.untouched != tuple(
        sorted(set(plan.untouched), key=lambda value: (value.casefold(), value))
    ) or plan.issues != tuple(sorted(set(plan.issues))):
        raise MigrationError("migration journal diagnostics are invalid")


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
        plan.database_sha256,
        phase,
        plan.items,
        plan.untouched,
        plan.issues,
    )


def _replace_database_digest(
    plan: _MigrationPlan, database_sha256: str | None
) -> _MigrationPlan:
    return _MigrationPlan(
        plan.profile_id,
        plan.legacy_root,
        plan.account_root,
        plan.state_directory,
        plan.config_hash,
        database_sha256,
        plan.phase,
        plan.items,
        plan.untouched,
        plan.issues,
    )


def _canonical_document_digest(document: Mapping[str, object]) -> str:
    payload = json.dumps(
        document, sort_keys=True, separators=(",", ":"), ensure_ascii=True
    ).encode()
    return hashlib.sha256(payload).hexdigest()


def _report(
    plan: _MigrationPlan, mode: MigrationMode, *, completed_noop: bool = False
) -> MigrationReport:
    database_changes = (
        ("no_changes_completed_migration",)
        if completed_noop
        else (
            "create_current_schema_state",
            "register_explicit_profile_without_delivery_outcomes",
            f"journal_{len(plan.items)}_exact_bundles",
        )
    )
    return MigrationReport(
        plan.profile_id,
        mode,
        plan.phase,
        plan.items,
        plan.untouched,
        plan.issues,
        (
            f"profile:{plan.profile_id}",
            "official_credentials_not_migrated",
            "remote_identities_not_inferred",
        ),
        database_changes,
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
