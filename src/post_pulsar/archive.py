"""Exact-member archival with durable checkpoints and crash convergence."""

from __future__ import annotations

import errno
import hashlib
import os
import stat
import unicodedata
from collections.abc import Callable, Sequence
from pathlib import Path
from typing import BinaryIO, Final, Protocol

from post_pulsar.locking import LockLease, LockManager
from post_pulsar.state import BundleFileSnapshot, BundleRecord, StateRepository

_CHUNK_BYTES: Final = 1024 * 1024
_FINGERPRINT_DOMAIN: Final = b"POST-PULSAR-CONTENT-BUNDLE\x00V1\x00"
_RESERVATION_BYTES: Final = b"POST-PULSAR-ARCHIVE-RESERVATION\x00V1"
_EMPTY_SHA256: Final = hashlib.sha256(b"").hexdigest()
_READY_MEMBER: Final = BundleFileSnapshot(
    ".ready", "ready", None, None, None, 0, _EMPTY_SHA256
)


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
        account_root = _verified_directory(bundle.profile_root_snapshot)
        source_bucket = _verified_child_directory(account_root, bundle.source_bucket)
        source = source_bucket / bundle.bundle_id
        posted = _ensure_child_directory(account_root, "POSTED")
        archive_parent = _ensure_child_directory(posted, bundle.source_bucket)
        staging = archive_parent / f".{bundle.bundle_id}.{bundle.fingerprint}.archiving"
        final = archive_parent / bundle.bundle_id

        if _lexists(final):
            self._verify_complete(final, bundle, manifest)
            if _lexists(staging):
                self._remove_redundant_container(staging, manifest)
            if _lexists(source):
                self._remove_redundant_container(source, manifest)
            self._checkpoint_complete(bundle, final, manifest)
            return self._finish(bundle, final)

        if not _lexists(staging):
            staging.mkdir(mode=0o700)
            _fsync_directory(archive_parent)
            self._inject("after_staging_directory")
        else:
            _verified_directory(staging)

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

        if _lexists(source):
            if tuple(source.iterdir()):
                raise ArchiveSafetyError("source container has unexpected members")
            source.rmdir()
            _fsync_directory(source.parent)
        self._verify_complete(staging, bundle, manifest)
        self._inject("before_final_rename")
        if _lexists(final):
            raise ArchiveSafetyError("archive destination appeared unexpectedly")
        os.rename(staging, final)
        _fsync_directory(archive_parent)
        self._inject("after_final_rename")
        return self._finish(bundle, final)

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
        self, source: Path, staging: Path, member: BundleFileSnapshot
    ) -> None:
        source_path = source / member.relative_name
        staged_path = staging / member.relative_name
        source_exists = _lexists(source_path)
        staged_exists = _lexists(staged_path)
        if source_exists and staged_exists:
            _verify_member(source_path, member)
            _verify_member(staged_path, member)
            self._remove_verified_copy_temp(staging, member)
            source_path.unlink()
            _fsync_directory(source)
            return
        if staged_exists:
            _verify_member(staged_path, member)
            self._remove_verified_copy_temp(staging, member)
            return
        if not source_exists:
            raise ArchiveSafetyError("exact archive member is missing")
        _verify_member(source_path, member)
        reservation = _reserve_destination(staged_path)
        self._inject(f"after_member_reservation:{member.relative_name}")
        try:
            self._replace(source_path, staged_path)
        except OSError as exc:
            if exc.errno != errno.EXDEV:
                raise
            _remove_reservation(staged_path, reservation)
            self._copy_across_filesystems(source_path, staged_path, member)
        else:
            _fsync_directory(staging)
            _fsync_directory(source)
        _verify_member(staged_path, member)

    def _copy_across_filesystems(
        self,
        source_path: Path,
        staged_path: Path,
        member: BundleFileSnapshot,
    ) -> None:
        temporary = _copy_temp_path(staged_path.parent, member)
        if _lexists(temporary):
            _verify_member(temporary, member)
        else:
            source, _identity = _open_regular(source_path)
            try:
                with os.fdopen(
                    os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600),
                    "wb",
                ) as destination:
                    self._inject(f"after_exdev_temp_created:{member.relative_name}")
                    while chunk := source.read(_CHUNK_BYTES):
                        destination.write(chunk)
                    destination.flush()
                    os.fsync(destination.fileno())
            finally:
                source.close()
            self._inject(f"after_exdev_temp_fsync:{member.relative_name}")
            _verify_member(temporary, member)
            self._inject(f"after_exdev_temp_verify:{member.relative_name}")
        os.link(temporary, staged_path, follow_symlinks=False)
        self._inject(f"after_exdev_install:{member.relative_name}")
        _fsync_directory(staged_path.parent)
        self._inject(f"after_exdev_install_fsync:{member.relative_name}")
        _verify_member(staged_path, member)
        self._inject(f"after_exdev_staged_verify:{member.relative_name}")
        source_path.unlink()
        self._inject(f"after_exdev_source_remove:{member.relative_name}")
        _fsync_directory(source_path.parent)
        self._inject(f"after_exdev_source_fsync:{member.relative_name}")
        temporary.unlink()
        _fsync_directory(staged_path.parent)
        self._inject(f"after_exdev_temp_remove:{member.relative_name}")

    def _remove_verified_copy_temp(
        self, staging: Path, member: BundleFileSnapshot
    ) -> None:
        temporary = _copy_temp_path(staging, member)
        if not _lexists(temporary):
            return
        _verify_member(temporary, member)
        temporary.unlink()
        _fsync_directory(staging)

    def _validate_partial_locations(
        self,
        source: Path,
        staging: Path,
        manifest: tuple[BundleFileSnapshot, ...],
    ) -> None:
        expected = {member.relative_name for member in manifest} | {".ready"}
        copy_temps = {
            _copy_temp_path(staging, member).name
            for member in (*manifest, _READY_MEMBER)
        }
        observed: set[str] = set()
        for container in (source, staging):
            if not _lexists(container):
                continue
            _verified_directory(container)
            names = {entry.name for entry in container.iterdir()}
            allowed = expected | (copy_temps if container == staging else set())
            if not names <= allowed:
                raise ArchiveSafetyError("archive container has unexpected members")
            observed.update(names & expected)
        if observed != expected:
            raise ArchiveSafetyError("archive container membership is incomplete")

    def _verify_complete(
        self,
        container: Path,
        bundle: BundleRecord,
        manifest: tuple[BundleFileSnapshot, ...],
    ) -> None:
        _verified_directory(container)
        expected = {member.relative_name for member in manifest} | {".ready"}
        names = {entry.name for entry in container.iterdir()}
        if names != expected:
            raise ArchiveSafetyError("archive destination membership conflicts")
        for member in manifest:
            _verify_member(container / member.relative_name, member)
        _verify_member(container / ".ready", _ready_snapshot(bundle))
        if _content_fingerprint(container, manifest) != bundle.fingerprint:
            raise ArchiveSafetyError("archive fingerprint does not match durable state")

    def _remove_redundant_container(
        self, container: Path, manifest: tuple[BundleFileSnapshot, ...]
    ) -> None:
        _verified_directory(container)
        allowed = {member.relative_name: member for member in manifest}
        allowed[".ready"] = BundleFileSnapshot(
            ".ready", "ready", None, None, None, 0, _EMPTY_SHA256
        )
        for entry in tuple(container.iterdir()):
            member = allowed.get(entry.name)
            if member is None:
                raise ArchiveSafetyError("redundant archive container conflicts")
            _verify_member(entry, member)
        for entry in tuple(container.iterdir()):
            entry.unlink()
        container.rmdir()
        _fsync_directory(container.parent)

    def _verify_recorded_archive(self, bundle: BundleRecord) -> None:
        if bundle.archive_path is None:
            raise ArchiveSafetyError("archived bundle has no durable archive path")
        relative = Path(bundle.archive_path)
        if relative.is_absolute() or ".." in relative.parts:
            raise ArchiveSafetyError("durable archive path is unsafe")
        final = bundle.profile_root_snapshot / relative
        self._verify_complete(final, bundle, self._manifest(bundle.bundle_key))

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


def _verified_directory(path: Path) -> Path:
    metadata = path.stat(follow_symlinks=False)
    if stat.S_ISLNK(metadata.st_mode) or not stat.S_ISDIR(metadata.st_mode):
        raise ArchiveSafetyError("archive path contains an unsafe directory")
    return path


def _verified_child_directory(parent: Path, name: str) -> Path:
    if Path(name).name != name or name in {"", ".", ".."}:
        raise ArchiveSafetyError("archive directory name is unsafe")
    _verified_directory(parent)
    return _verified_directory(parent / name)


def _ensure_child_directory(parent: Path, name: str) -> Path:
    if Path(name).name != name or name in {"", ".", ".."}:
        raise ArchiveSafetyError("archive directory name is unsafe")
    _verified_directory(parent)
    child = parent / name
    try:
        child.mkdir(mode=0o700)
    except FileExistsError:
        pass
    child = _verified_directory(child)
    _fsync_directory(parent)
    return child


def _lexists(path: Path) -> bool:
    return os.path.lexists(path)


def _open_regular(path: Path) -> tuple[BinaryIO, os.stat_result]:
    before = path.stat(follow_symlinks=False)
    if not stat.S_ISREG(before.st_mode):
        raise ArchiveSafetyError("archive member is not a regular file")
    descriptor = os.open(path, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0))
    source = os.fdopen(descriptor, "rb")
    opened = os.fstat(source.fileno())
    if not os.path.samestat(before, opened):
        source.close()
        raise ArchiveSafetyError("archive member identity changed")
    return source, opened


def _reserve_destination(path: Path) -> os.stat_result:
    descriptor = os.open(
        path,
        os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0),
        0o600,
    )
    with os.fdopen(descriptor, "wb") as reserved:
        reserved.write(_RESERVATION_BYTES)
        reserved.flush()
        os.fsync(reserved.fileno())
        identity = os.fstat(reserved.fileno())
    _fsync_directory(path.parent)
    return identity


def _remove_reservation(path: Path, identity: os.stat_result) -> None:
    current = path.stat(follow_symlinks=False)
    if not os.path.samestat(current, identity):
        raise ArchiveSafetyError("archive reservation identity changed")
    digest, size = _hash_file(path)
    if digest != hashlib.sha256(_RESERVATION_BYTES).hexdigest() or size != len(
        _RESERVATION_BYTES
    ):
        raise ArchiveSafetyError("archive reservation content changed")
    path.unlink()
    _fsync_directory(path.parent)


def _hash_file(path: Path) -> tuple[str, int]:
    source, before = _open_regular(path)
    digest = hashlib.sha256()
    size = 0
    try:
        while chunk := source.read(_CHUNK_BYTES):
            digest.update(chunk)
            size += len(chunk)
        after = os.fstat(source.fileno())
        named = path.stat(follow_symlinks=False)
        if not (os.path.samestat(before, after) and os.path.samestat(after, named)):
            raise ArchiveSafetyError("archive member identity changed")
    finally:
        source.close()
    return digest.hexdigest(), size


def _verify_member(path: Path, member: BundleFileSnapshot) -> None:
    digest, size = _hash_file(path)
    if digest != member.sha256 or size != member.size_bytes:
        raise ArchiveSafetyError("archive member hash or size conflicts")


def _copy_temp_path(parent: Path, member: BundleFileSnapshot) -> Path:
    return parent / f".copy-{member.sha256}.tmp"


def _content_fingerprint(
    container: Path, manifest: Sequence[BundleFileSnapshot]
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
        source, _identity = _open_regular(container / member.relative_name)
        try:
            digest.update(member.size_bytes.to_bytes(8, "big"))
            while chunk := source.read(_CHUNK_BYTES):
                digest.update(chunk)
        finally:
            source.close()
    return digest.hexdigest()


def _hash_field(digest: "hashlib._Hash", value: bytes) -> None:
    digest.update(len(value).to_bytes(8, "big"))
    digest.update(value)


def _fsync_directory(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
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
