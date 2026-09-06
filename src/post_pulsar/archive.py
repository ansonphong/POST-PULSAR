"""Exact-member archival with durable checkpoints and crash convergence."""

from __future__ import annotations

import errno
import ctypes
import hashlib
import os
import stat
import sys
import unicodedata
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import BinaryIO, Final, Protocol

from post_pulsar.locking import LockLease, LockManager
from post_pulsar.state import BundleFileSnapshot, BundleRecord, StateRepository

_CHUNK_BYTES: Final = 1024 * 1024
_FINGERPRINT_DOMAIN: Final = b"POST-PULSAR-CONTENT-BUNDLE\x00V1\x00"
_EMPTY_SHA256: Final = hashlib.sha256(b"").hexdigest()
_READY_MEMBER: Final = BundleFileSnapshot(
    ".ready", "ready", None, None, None, 0, _EMPTY_SHA256
)
_RENAME_NOREPLACE: Final = 1


@dataclass(slots=True)
class _DirectoryGuard:
    """Pinned no-follow directory chain used to detect component replacement."""

    path: Path
    _fds: tuple[int, ...]
    _names: tuple[str, ...]
    _identities: tuple[os.stat_result, ...]
    _fallback_paths: tuple[Path, ...] = ()

    @classmethod
    def open(cls, path: Path) -> _DirectoryGuard:
        absolute = path.absolute()
        flags = (
            os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) | getattr(os, "O_NOFOLLOW", 0)
        )
        if os.open in os.supports_dir_fd and os.stat in os.supports_dir_fd:
            anchor = Path(absolute.anchor)
            fds: list[int] = []
            names: list[str] = []
            identities: list[os.stat_result] = []
            try:
                root_fd = os.open(anchor, flags)
                fds.append(root_fd)
                names.append("")
                identities.append(os.fstat(root_fd))
                for component in absolute.relative_to(anchor).parts:
                    before = os.stat(component, dir_fd=fds[-1], follow_symlinks=False)
                    if stat.S_ISLNK(before.st_mode) or not stat.S_ISDIR(before.st_mode):
                        raise ArchiveSafetyError(
                            "archive path contains an unsafe directory"
                        )
                    descriptor = os.open(component, flags, dir_fd=fds[-1])
                    opened = os.fstat(descriptor)
                    if not os.path.samestat(before, opened):
                        os.close(descriptor)
                        raise ArchiveSafetyError("archive directory identity changed")
                    fds.append(descriptor)
                    names.append(component)
                    identities.append(opened)
            except Exception:
                for descriptor in reversed(fds):
                    os.close(descriptor)
                raise
            guard = cls(absolute, tuple(fds), tuple(names), tuple(identities))
            guard.validate()
            return guard

        current = Path(absolute.anchor)
        paths = [current]
        identities = [current.stat(follow_symlinks=False)]
        for component in absolute.relative_to(current).parts:
            current /= component
            metadata = current.stat(follow_symlinks=False)
            if stat.S_ISLNK(metadata.st_mode) or not stat.S_ISDIR(metadata.st_mode):
                raise ArchiveSafetyError("archive path contains an unsafe directory")
            paths.append(current)
            identities.append(metadata)
        return cls(absolute, (), (), tuple(identities), tuple(paths))

    @property
    def fd(self) -> int | None:
        return self._fds[-1] if self._fds else None

    @property
    def identity(self) -> os.stat_result:
        return self._identities[-1]

    def validate(self) -> None:
        if self._fds:
            for index, descriptor in enumerate(self._fds):
                opened = os.fstat(descriptor)
                if not os.path.samestat(opened, self._identities[index]):
                    raise ArchiveSafetyError("archive directory identity changed")
                if index:
                    named = os.stat(
                        self._names[index],
                        dir_fd=self._fds[index - 1],
                        follow_symlinks=False,
                    )
                    if not os.path.samestat(named, opened):
                        raise ArchiveSafetyError("archive directory identity changed")
            return
        for path, identity in zip(self._fallback_paths, self._identities, strict=True):
            current = path.stat(follow_symlinks=False)
            if (
                stat.S_ISLNK(current.st_mode)
                or not stat.S_ISDIR(current.st_mode)
                or not os.path.samestat(current, identity)
            ):
                raise ArchiveSafetyError("archive directory identity changed")

    def close(self) -> None:
        for descriptor in reversed(self._fds):
            os.close(descriptor)
        self._fds = ()


class ArchiveError(RuntimeError):
    """Base class for sanitized archive failures."""


class ArchiveSafetyError(ArchiveError):
    """Archive state is conflicting or unsafe and requires operator attention."""


class ArchiveFaultInjector(Protocol):
    def __call__(self, boundary: str) -> None: ...


class ArchiveManager:
    """Drive one exact archive transaction while required leases are already held."""

    def __init__(
        self,
        repository: StateRepository,
        locks: LockManager,
        *,
        fault_injector: ArchiveFaultInjector | None = None,
        replace: Callable[[Path, Path], None] = os.replace,
    ) -> None:
        self._repository = repository
        self._locks = locks
        self._fault = fault_injector
        self._replace = replace

    def archive_bundle(
        self,
        bundle_key: int,
        *,
        instance_lease: LockLease,
        profile_lease: LockLease,
    ) -> BundleRecord:
        self._locks.require_instance(instance_lease)
        bundle = self._repository.get_bundle(bundle_key)
        self._locks.require_profile(profile_lease, bundle.profile_id)
        try:
            if bundle.status == "archived":
                self._verify_recorded_archive(bundle)
                return bundle
            if bundle.status == "active":
                bundle = self._repository.begin_archiving(
                    bundle_key, expected_revision=bundle.revision
                )
                self._inject("after_begin_archiving")
            if bundle.status != "archiving":
                raise ArchiveSafetyError("bundle is not eligible for archival")
            return self._recover(bundle)
        except ArchiveSafetyError as exc:
            current = self._repository.get_bundle(bundle_key)
            if current.status in {"active", "archiving"}:
                self._repository.block_bundle(
                    bundle_key,
                    "archive_conflict",
                    expected_revision=current.revision,
                )
            raise exc
        except OSError:
            current = self._repository.get_bundle(bundle_key)
            if current.status in {"active", "archiving"}:
                self._repository.block_bundle(
                    bundle_key,
                    "archive_io_failure",
                    expected_revision=current.revision,
                )
            raise ArchiveSafetyError(
                "archive filesystem operation failed safely"
            ) from None

    def _recover(self, bundle: BundleRecord) -> BundleRecord:
        manifest = self._manifest(bundle.bundle_key)
        guards: list[_DirectoryGuard] = []
        try:
            account = self._open_guard(bundle.profile_root_snapshot, guards)
            source_bucket = self._open_child_guard(
                account, bundle.source_bucket, guards
            )
            source_path = source_bucket.path / bundle.bundle_id
            posted = self._ensure_child_guard(account, "POSTED", guards)
            archive_parent = self._ensure_child_guard(
                posted, bundle.source_bucket, guards
            )
            staging_name = f".{bundle.bundle_id}.{bundle.fingerprint}.archiving"
            final_name = bundle.bundle_id
            staging_path = archive_parent.path / staging_name
            final_path = archive_parent.path / final_name
            self._recover_empty_directory_quarantine(
                source_bucket, bundle.bundle_id, guards
            )
            self._recover_empty_directory_quarantine(
                archive_parent, staging_name, guards
            )
            source = self._optional_guard(source_path, guards)
            staging = self._optional_guard(staging_path, guards)
            final = self._optional_guard(final_path, guards)

            self._inject("after_archive_guards")
            _validate_guards(account, source_bucket, posted, archive_parent)
            _validate_optional_guards(source, staging, final)

            if final is not None:
                self._verify_complete(final, bundle, manifest)
                if staging is not None:
                    self._remove_redundant_container(staging, archive_parent, manifest)
                if source is not None:
                    self._remove_redundant_container(source, source_bucket, manifest)
                self._checkpoint_complete(bundle, final.path, manifest)
                return self._finish(bundle, final.path)

            if staging is None:
                _mkdir_child(archive_parent, staging_name)
                staging = self._open_child_guard(archive_parent, staging_name, guards)
                self._inject("after_staging_directory")

            self._validate_partial_locations(source, staging, manifest)
            for member in (*manifest, _ready_snapshot(bundle)):
                self._converge_member(source, staging, member)
                self._inject(f"after_member_move:{member.relative_name}")
                self._repository.checkpoint_archive_member(
                    bundle.bundle_key,
                    member.relative_name,
                    sha256=member.sha256,
                )
                self._inject(f"after_member_checkpoint:{member.relative_name}")

            if source is not None:
                self._remove_empty_container(source, source_bucket)
            self._verify_complete(staging, bundle, manifest)
            self._inject("before_final_rename")
            _validate_guards(account, source_bucket, posted, archive_parent, staging)
            _atomic_noreplace(archive_parent, staging_name, archive_parent, final_name)
            _fsync_guard(archive_parent)
            final = self._open_child_guard(archive_parent, final_name, guards)
            if not os.path.samestat(staging.identity, final.identity):
                raise ArchiveSafetyError("archive final directory identity changed")
            self._verify_complete(final, bundle, manifest)
            self._inject("after_final_rename")
            return self._finish(bundle, final.path)
        finally:
            for guard in reversed(guards):
                guard.close()

    def _finish(self, bundle: BundleRecord, final: Path) -> BundleRecord:
        relative = final.relative_to(bundle.profile_root_snapshot).as_posix()
        archived = self._repository.mark_archived(
            bundle.bundle_key, relative, expected_revision=bundle.revision
        )
        self._inject("after_mark_archived")
        return archived

    def _checkpoint_complete(
        self,
        bundle: BundleRecord,
        final: Path,
        manifest: tuple[BundleFileSnapshot, ...],
    ) -> None:
        del final
        for member in (*manifest, _ready_snapshot(bundle)):
            self._repository.checkpoint_archive_member(
                bundle.bundle_key, member.relative_name, sha256=member.sha256
            )

    def _converge_member(
        self,
        source: _DirectoryGuard | None,
        staging: _DirectoryGuard,
        member: BundleFileSnapshot,
    ) -> None:
        staged_exists = _name_exists(staging, member.relative_name)
        source_quarantine = _source_quarantine_name(member)
        deletion_quarantine = _deletion_quarantine_name(source_quarantine)
        source_name: str | None = None
        if source is not None:
            present = tuple(
                name
                for name in (
                    member.relative_name,
                    source_quarantine,
                    deletion_quarantine,
                )
                if _name_exists(source, name)
            )
            if len(present) > 1:
                raise ArchiveSafetyError("source archive member identity conflicts")
            source_name = present[0] if present else None

        if source_name == deletion_quarantine:
            if source is None:
                raise ArchiveSafetyError("archive deletion checkpoint is inconsistent")
            if not staged_exists:
                raise ArchiveSafetyError("archive deletion checkpoint is inconsistent")
            _verify_member_at(staging, member.relative_name, member)
            _unlink_verified_quarantine(source, source_name, member)
            self._remove_verified_copy_temp(staging, member)
            return

        if staged_exists:
            _verify_member_at(staging, member.relative_name, member)
            self._remove_verified_copy_temp(staging, member)
            if source is not None and source_name is not None:
                _delete_verified(source, source_name, member)
            return

        if source_name == member.relative_name:
            if source is None:
                raise ArchiveSafetyError("exact archive member is missing")
            _verify_member_at(source, source_name, member)
            _atomic_noreplace(source, source_name, source, source_quarantine)
            _fsync_guard(source)
            source_name = source_quarantine
            _verify_member_at(source, source_name, member)
            self._inject(f"after_member_reservation:{member.relative_name}")

        if source is None or source_name is None:
            raise ArchiveSafetyError("exact archive member is missing")

        try:
            _verify_member_at(source, source_name, member)
            if self._replace is os.replace:
                _atomic_noreplace(source, source_name, staging, member.relative_name)
            else:
                self._replace(
                    source.path / source_name,
                    staging.path / member.relative_name,
                )
        except OSError as exc:
            if exc.errno != errno.EXDEV:
                _restore_source_member(source, source_name, member)
                raise
            try:
                self._copy_across_filesystems(source, source_name, staging, member)
            except (ArchiveSafetyError, OSError):
                _restore_source_member(source, source_name, member)
                raise
        except ArchiveSafetyError:
            _restore_source_member(source, source_name, member)
            raise
        else:
            _fsync_guard(staging)
            _fsync_guard(source)
        _verify_member_at(staging, member.relative_name, member)

    def _copy_across_filesystems(
        self,
        source: _DirectoryGuard,
        source_name: str,
        staging: _DirectoryGuard,
        member: BundleFileSnapshot,
    ) -> None:
        temporary = _copy_temp_name(member)
        if _name_exists(staging, temporary):
            _verify_member_at(staging, temporary, member)
        else:
            source_file, _identity = _open_regular_at(source, source_name)
            try:
                with os.fdopen(
                    _open_exclusive_at(staging, temporary),
                    "wb",
                ) as destination:
                    self._inject(f"after_exdev_temp_created:{member.relative_name}")
                    while chunk := source_file.read(_CHUNK_BYTES):
                        destination.write(chunk)
                    destination.flush()
                    os.fsync(destination.fileno())
            finally:
                source_file.close()
            self._inject(f"after_exdev_temp_fsync:{member.relative_name}")
            _verify_member_at(staging, temporary, member)
            self._inject(f"after_exdev_temp_verify:{member.relative_name}")
        _link_noreplace(staging, temporary, member.relative_name)
        self._inject(f"after_exdev_install:{member.relative_name}")
        _fsync_guard(staging)
        self._inject(f"after_exdev_install_fsync:{member.relative_name}")
        _verify_member_at(staging, member.relative_name, member)
        self._inject(f"after_exdev_staged_verify:{member.relative_name}")
        _delete_verified(source, source_name, member)
        self._inject(f"after_exdev_source_remove:{member.relative_name}")
        _fsync_guard(source)
        self._inject(f"after_exdev_source_fsync:{member.relative_name}")
        _delete_verified(staging, temporary, member)
        _fsync_guard(staging)
        self._inject(f"after_exdev_temp_remove:{member.relative_name}")

    def _remove_verified_copy_temp(
        self, staging: _DirectoryGuard, member: BundleFileSnapshot
    ) -> None:
        temporary = _copy_temp_name(member)
        deletion = _deletion_quarantine_name(temporary)
        if _name_exists(staging, deletion):
            _unlink_verified_quarantine(staging, deletion, member)
            return
        if not _name_exists(staging, temporary):
            return
        _delete_verified(staging, temporary, member)

    def _validate_partial_locations(
        self,
        source: _DirectoryGuard | None,
        staging: _DirectoryGuard,
        manifest: tuple[BundleFileSnapshot, ...],
    ) -> None:
        members = (*manifest, _READY_MEMBER)
        source_names = _list_names(source) if source is not None else set()
        staging_names = _list_names(staging)
        source_allowed: set[str] = set()
        staging_allowed: set[str] = set()
        for member in members:
            source_quarantine = _source_quarantine_name(member)
            member_source_names = {
                member.relative_name,
                source_quarantine,
                _deletion_quarantine_name(source_quarantine),
            }
            source_allowed.update(member_source_names)
            temporary = _copy_temp_name(member)
            member_staging_names = {
                member.relative_name,
                temporary,
                _deletion_quarantine_name(temporary),
            }
            staging_allowed.update(member_staging_names)
            locations = {
                name
                for name in member_source_names | member_staging_names
                if name in source_names or name in staging_names
            }
            if not locations:
                raise ArchiveSafetyError("archive container membership is incomplete")
        if not source_names <= source_allowed or not staging_names <= staging_allowed:
            raise ArchiveSafetyError("archive container has unexpected members")

    def _verify_complete(
        self,
        container: _DirectoryGuard,
        bundle: BundleRecord,
        manifest: tuple[BundleFileSnapshot, ...],
    ) -> None:
        container.validate()
        expected = {member.relative_name for member in manifest} | {".ready"}
        names = _list_names(container)
        if names != expected:
            raise ArchiveSafetyError("archive destination membership conflicts")
        for member in manifest:
            _verify_member_at(container, member.relative_name, member)
        _verify_member_at(container, ".ready", _ready_snapshot(bundle))
        if _content_fingerprint(container, manifest) != bundle.fingerprint:
            raise ArchiveSafetyError("archive fingerprint does not match durable state")

    def _remove_redundant_container(
        self,
        container: _DirectoryGuard,
        parent: _DirectoryGuard,
        manifest: tuple[BundleFileSnapshot, ...],
    ) -> None:
        allowed = {member.relative_name: member for member in manifest}
        allowed[".ready"] = _READY_MEMBER
        names = _list_names(container)
        recognized: set[str] = set()
        for name, member in allowed.items():
            deletion = _deletion_quarantine_name(name)
            present = {
                candidate for candidate in (name, deletion) if candidate in names
            }
            if len(present) > 1:
                raise ArchiveSafetyError("redundant archive container conflicts")
            recognized.update(present)
            if deletion in present:
                _unlink_verified_quarantine(container, deletion, member)
            elif name in present:
                _delete_verified(container, name, member)
        if names != recognized:
            raise ArchiveSafetyError("redundant archive container conflicts")
        self._remove_empty_container(container, parent)

    def _remove_empty_container(
        self, container: _DirectoryGuard, parent: _DirectoryGuard
    ) -> None:
        container.validate()
        parent.validate()
        if _list_names(container):
            raise ArchiveSafetyError("source container has unexpected members")
        quarantine = _directory_quarantine_name(container.path.name)
        if _name_exists(parent, quarantine):
            raise ArchiveSafetyError("archive directory quarantine conflicts")
        _atomic_noreplace(parent, container.path.name, parent, quarantine)
        _fsync_guard(parent)
        self._inject(f"after_directory_quarantine:{container.path.name}")
        moved = _DirectoryGuard.open(parent.path / quarantine)
        try:
            if not os.path.samestat(container.identity, moved.identity):
                raise ArchiveSafetyError("archive directory identity changed")
            if _list_names(moved):
                raise ArchiveSafetyError("redundant archive container conflicts")
            _rmdir_child(parent, quarantine, moved.identity)
            _fsync_guard(parent)
        finally:
            moved.close()

    def _recover_empty_directory_quarantine(
        self,
        parent: _DirectoryGuard,
        original_name: str,
        guards: list[_DirectoryGuard],
    ) -> None:
        quarantine = _directory_quarantine_name(original_name)
        if not _name_exists(parent, quarantine):
            return
        if _name_exists(parent, original_name):
            raise ArchiveSafetyError("archive directory quarantine conflicts")
        moved = self._open_child_guard(parent, quarantine, guards)
        if _list_names(moved):
            raise ArchiveSafetyError("archive directory quarantine conflicts")
        _rmdir_child(parent, quarantine, moved.identity)
        _fsync_guard(parent)

    def _verify_recorded_archive(self, bundle: BundleRecord) -> None:
        if bundle.archive_path is None:
            raise ArchiveSafetyError("archived bundle has no durable archive path")
        relative = Path(bundle.archive_path)
        if relative.is_absolute() or ".." in relative.parts:
            raise ArchiveSafetyError("durable archive path is unsafe")
        expected = Path("POSTED") / bundle.source_bucket / bundle.bundle_id
        if relative != expected:
            raise ArchiveSafetyError("durable archive path is not canonical")
        final = _DirectoryGuard.open(bundle.profile_root_snapshot / relative)
        try:
            self._verify_complete(final, bundle, self._manifest(bundle.bundle_key))
        finally:
            final.close()

    def _open_guard(self, path: Path, guards: list[_DirectoryGuard]) -> _DirectoryGuard:
        guard = _DirectoryGuard.open(path)
        guards.append(guard)
        return guard

    def _optional_guard(
        self, path: Path, guards: list[_DirectoryGuard]
    ) -> _DirectoryGuard | None:
        if not _lexists(path):
            return None
        return self._open_guard(path, guards)

    def _open_child_guard(
        self,
        parent: _DirectoryGuard,
        name: str,
        guards: list[_DirectoryGuard],
    ) -> _DirectoryGuard:
        _validate_child_name(name)
        parent.validate()
        guard = self._open_guard(parent.path / name, guards)
        parent.validate()
        return guard

    def _ensure_child_guard(
        self,
        parent: _DirectoryGuard,
        name: str,
        guards: list[_DirectoryGuard],
    ) -> _DirectoryGuard:
        _validate_child_name(name)
        if not _name_exists(parent, name):
            _mkdir_child(parent, name)
        return self._open_child_guard(parent, name, guards)

    def _manifest(self, bundle_key: int) -> tuple[BundleFileSnapshot, ...]:
        manifest = self._repository.list_bundle_files(bundle_key)
        if not manifest:
            raise ArchiveSafetyError("archive manifest is empty")
        names: set[str] = set()
        for member in manifest:
            name = Path(member.relative_name)
            if (
                name.name != member.relative_name
                or member.relative_name == ".ready"
                or member.relative_name.casefold() in names
            ):
                raise ArchiveSafetyError("archive manifest member path is unsafe")
            names.add(member.relative_name.casefold())
        return tuple(sorted(manifest, key=_semantic_order))

    def _inject(self, boundary: str) -> None:
        if self._fault is not None:
            self._fault(boundary)


def _ready_snapshot(bundle: BundleRecord) -> BundleFileSnapshot:
    if bundle.ready_marker_name != ".ready":
        raise ArchiveSafetyError("ready marker snapshot is invalid")
    return _READY_MEMBER


def _semantic_order(member: BundleFileSnapshot) -> tuple[int, int, str]:
    roles = {"caption": 0, "alt_text": 1, "image": 2, "video": 3}
    return (roles.get(member.role, 99), member.ordinal or 0, member.relative_name)


def _validate_child_name(name: str) -> None:
    if Path(name).name != name or name in {"", ".", ".."}:
        raise ArchiveSafetyError("archive directory name is unsafe")


def _lexists(path: Path) -> bool:
    return os.path.lexists(path)


def _validate_guards(*guards: _DirectoryGuard) -> None:
    for guard in guards:
        guard.validate()


def _validate_optional_guards(*guards: _DirectoryGuard | None) -> None:
    for guard in guards:
        if guard is not None:
            guard.validate()


def _stat_at(directory: _DirectoryGuard, name: str) -> os.stat_result:
    directory.validate()
    if directory.fd is not None:
        return os.stat(name, dir_fd=directory.fd, follow_symlinks=False)
    return (directory.path / name).stat(follow_symlinks=False)


def _name_exists(directory: _DirectoryGuard, name: str) -> bool:
    try:
        _stat_at(directory, name)
    except FileNotFoundError:
        return False
    return True


def _list_names(directory: _DirectoryGuard | None) -> set[str]:
    if directory is None:
        return set()
    directory.validate()
    if directory.fd is not None:
        return set(os.listdir(directory.fd))
    return {entry.name for entry in directory.path.iterdir()}


def _mkdir_child(parent: _DirectoryGuard, name: str) -> None:
    _validate_child_name(name)
    parent.validate()
    if parent.fd is not None and os.mkdir in os.supports_dir_fd:
        os.mkdir(name, mode=0o700, dir_fd=parent.fd)
    else:
        (parent.path / name).mkdir(mode=0o700)
    _fsync_guard(parent)
    parent.validate()


def _open_regular_at(
    directory: _DirectoryGuard, name: str
) -> tuple[BinaryIO, os.stat_result]:
    before = _stat_at(directory, name)
    if not stat.S_ISREG(before.st_mode):
        raise ArchiveSafetyError("archive member is not a regular file")
    flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0)
    if directory.fd is not None and os.open in os.supports_dir_fd:
        descriptor = os.open(name, flags, dir_fd=directory.fd)
    else:
        descriptor = os.open(directory.path / name, flags)
    source = os.fdopen(descriptor, "rb")
    opened = os.fstat(source.fileno())
    if not os.path.samestat(before, opened):
        source.close()
        raise ArchiveSafetyError("archive member identity changed")
    return source, opened


def _hash_file_at(
    directory: _DirectoryGuard, name: str
) -> tuple[str, int, os.stat_result]:
    source, before = _open_regular_at(directory, name)
    digest = hashlib.sha256()
    size = 0
    try:
        while chunk := source.read(_CHUNK_BYTES):
            digest.update(chunk)
            size += len(chunk)
        after = os.fstat(source.fileno())
        named = _stat_at(directory, name)
        if not (os.path.samestat(before, after) and os.path.samestat(after, named)):
            raise ArchiveSafetyError("archive member identity changed")
    finally:
        source.close()
    return digest.hexdigest(), size, before


def _verify_member_at(
    directory: _DirectoryGuard, name: str, member: BundleFileSnapshot
) -> os.stat_result:
    digest, size, identity = _hash_file_at(directory, name)
    if digest != member.sha256 or size != member.size_bytes:
        raise ArchiveSafetyError("archive member hash or size conflicts")
    return identity


def _name_digest(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _source_quarantine_name(member: BundleFileSnapshot) -> str:
    return f".archive-source-{_name_digest(member.relative_name)}.quarantine"


def _deletion_quarantine_name(name: str) -> str:
    return f".archive-delete-{_name_digest(name)}.quarantine"


def _directory_quarantine_name(name: str) -> str:
    return f".archive-delete-dir-{_name_digest(name)}.quarantine"


def _copy_temp_name(member: BundleFileSnapshot) -> str:
    return f".copy-{member.sha256}-{_name_digest(member.relative_name)}.tmp"


def _open_exclusive_at(directory: _DirectoryGuard, name: str) -> int:
    directory.validate()
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0)
    if directory.fd is not None and os.open in os.supports_dir_fd:
        return os.open(name, flags, 0o600, dir_fd=directory.fd)
    return os.open(directory.path / name, flags, 0o600)


def _atomic_noreplace(
    source_parent: _DirectoryGuard,
    source_name: str,
    destination_parent: _DirectoryGuard,
    destination_name: str,
) -> None:
    """Atomically rename one entry and never replace an existing destination."""

    source_parent.validate()
    destination_parent.validate()
    if sys.platform.startswith("linux"):
        if source_parent.fd is None or destination_parent.fd is None:
            raise ArchiveSafetyError("atomic no-replace rename is unavailable")
        library = ctypes.CDLL(None, use_errno=True)
        try:
            renameat2 = library.renameat2
        except AttributeError:
            raise ArchiveSafetyError(
                "atomic no-replace rename is unavailable"
            ) from None
        result = int(
            renameat2(
                source_parent.fd,
                os.fsencode(source_name),
                destination_parent.fd,
                os.fsencode(destination_name),
                _RENAME_NOREPLACE,
            )
        )
        if result != 0:
            error = ctypes.get_errno()
            if error in {errno.EEXIST, errno.ENOTEMPTY}:
                raise ArchiveSafetyError("archive destination already exists")
            if error in {
                errno.ENOSYS,
                errno.EINVAL,
                getattr(errno, "EOPNOTSUPP", errno.ENOSYS),
            }:
                raise ArchiveSafetyError("atomic no-replace rename is unavailable")
            raise OSError(error, os.strerror(error))
    elif os.name == "nt":
        try:
            os.rename(
                source_parent.path / source_name,
                destination_parent.path / destination_name,
            )
        except FileExistsError:
            raise ArchiveSafetyError("archive destination already exists") from None
    else:
        raise ArchiveSafetyError("atomic no-replace rename is unavailable")
    source_parent.validate()
    destination_parent.validate()


def _link_noreplace(
    directory: _DirectoryGuard, source_name: str, destination_name: str
) -> None:
    directory.validate()
    if directory.fd is not None and os.link in os.supports_dir_fd:
        os.link(
            source_name,
            destination_name,
            src_dir_fd=directory.fd,
            dst_dir_fd=directory.fd,
            follow_symlinks=False,
        )
    else:
        os.link(
            directory.path / source_name,
            directory.path / destination_name,
            follow_symlinks=False,
        )
    directory.validate()


def _unlink_at(directory: _DirectoryGuard, name: str) -> None:
    directory.validate()
    if directory.fd is not None and os.unlink in os.supports_dir_fd:
        os.unlink(name, dir_fd=directory.fd)
    else:
        (directory.path / name).unlink()
    _fsync_guard(directory)


def _unlink_verified_quarantine(
    directory: _DirectoryGuard, name: str, member: BundleFileSnapshot
) -> None:
    identity = _verify_member_at(directory, name, member)
    current = _stat_at(directory, name)
    if not os.path.samestat(identity, current):
        raise ArchiveSafetyError("archive member identity changed")
    _unlink_at(directory, name)


def _delete_verified(
    directory: _DirectoryGuard, name: str, member: BundleFileSnapshot
) -> None:
    _verify_member_at(directory, name, member)
    quarantine = _deletion_quarantine_name(name)
    if _name_exists(directory, quarantine):
        raise ArchiveSafetyError("archive deletion quarantine conflicts")
    _atomic_noreplace(directory, name, directory, quarantine)
    _fsync_guard(directory)
    _unlink_verified_quarantine(directory, quarantine, member)


def _restore_source_member(
    source: _DirectoryGuard, source_name: str, member: BundleFileSnapshot
) -> None:
    """Restore an admitted source quarantine after a safe pre-install failure."""

    if source_name != _source_quarantine_name(member):
        return
    if not _name_exists(source, source_name):
        return
    _verify_member_at(source, source_name, member)
    if _name_exists(source, member.relative_name):
        raise ArchiveSafetyError("source restoration destination conflicts")
    _atomic_noreplace(source, source_name, source, member.relative_name)
    _fsync_guard(source)


def _rmdir_child(parent: _DirectoryGuard, name: str, expected: os.stat_result) -> None:
    current = _stat_at(parent, name)
    if not os.path.samestat(current, expected) or not stat.S_ISDIR(current.st_mode):
        raise ArchiveSafetyError("archive directory identity changed")
    if parent.fd is not None and os.rmdir in os.supports_dir_fd:
        os.rmdir(name, dir_fd=parent.fd)
    else:
        (parent.path / name).rmdir()


def _content_fingerprint(
    container: _DirectoryGuard, manifest: Sequence[BundleFileSnapshot]
) -> str:
    digest = hashlib.sha256()
    digest.update(_FINGERPRINT_DOMAIN)
    digest.update(len(manifest).to_bytes(8, "big"))
    for member in manifest:
        role = member.role
        if role == "image":
            role = f"image:{member.ordinal or 0}"
        _hash_field(digest, role.encode("ascii"))
        _hash_field(
            digest, unicodedata.normalize("NFC", member.relative_name).encode("utf-8")
        )
        source, identity = _open_regular_at(container, member.relative_name)
        try:
            digest.update(member.size_bytes.to_bytes(8, "big"))
            while chunk := source.read(_CHUNK_BYTES):
                digest.update(chunk)
            after = os.fstat(source.fileno())
            named = _stat_at(container, member.relative_name)
            if not (
                os.path.samestat(identity, after) and os.path.samestat(after, named)
            ):
                raise ArchiveSafetyError("archive member identity changed")
        finally:
            source.close()
    return digest.hexdigest()


def _hash_field(digest: "hashlib._Hash", value: bytes) -> None:
    digest.update(len(value).to_bytes(8, "big"))
    digest.update(value)


def _fsync_guard(directory: _DirectoryGuard) -> None:
    directory.validate()
    if directory.fd is not None:
        os.fsync(directory.fd)
        return
    descriptor = os.open(directory.path, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


__all__ = [
    "ArchiveError",
    "ArchiveFaultInjector",
    "ArchiveManager",
    "ArchiveSafetyError",
]
