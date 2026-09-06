"""Crash-journaled promotion of exact DRAFTS members into publishable buckets."""

from __future__ import annotations

import ctypes
import errno
import hashlib
import os
import shutil
import stat
import sys
from collections.abc import Callable
from pathlib import Path
from typing import Final, cast

from post_pulsar.content import SourceBucket, scan_inbox
from post_pulsar.state import (
    AdmissionRecord,
    ConflictError,
    StateError,
    StateRepository,
)

_BUCKETS: Final = frozenset({"QUEUE", "RANDOM", "REELS"})
_CHUNK: Final = 1024 * 1024


class AdmissionError(RuntimeError):
    """A draft could not be promoted without weakening source guarantees."""


class DraftAdmissionService:
    """Copy immutable draft bytes and make readiness visible as the final step."""

    def __init__(
        self,
        repository: StateRepository,
        account_root: str | Path,
        *,
        fault_injector: Callable[[str], None] | None = None,
    ) -> None:
        self._repository = repository
        supplied = Path(account_root)
        if supplied.is_symlink():
            raise AdmissionError("account root is unsafe")
        self._root = supplied.resolve(strict=True)
        if not self._root.is_dir():
            raise AdmissionError("account root is not a directory")
        self._fault = fault_injector

    def admit(
        self,
        *,
        profile_id: str,
        bucket: SourceBucket,
        bundle_id: str,
        expected_fingerprint: str,
        intent_id: str,
    ) -> AdmissionRecord:
        if bucket not in _BUCKETS:
            raise AdmissionError("admission bucket is invalid")
        drafts = self._root / "DRAFTS"
        try:
            scan = scan_inbox(drafts)
        except (OSError, ValueError):
            raise AdmissionError("DRAFTS could not be inspected safely") from None
        candidates = [item for item in scan.bundles if item.bundle_id == bundle_id]
        if len(candidates) != 1 or any(
            issue.bundle_id == bundle_id and issue.severity == "error"
            for issue in scan.issues
        ):
            raise AdmissionError("draft identity is missing or ambiguous")
        draft = candidates[0]
        if draft.fingerprint != expected_fingerprint:
            raise AdmissionError("draft fingerprint differs from approved preview")
        try:
            intent = self._repository.get_confirmation_intent(intent_id)
        except StateError:
            raise AdmissionError("admission confirmation intent is invalid") from None
        if (
            intent.state != "consumed"
            or intent.action != "admit_draft"
            or intent.profile_id != profile_id
            or intent.fingerprint != expected_fingerprint
            or intent.arguments != {"bucket": bucket, "bundle_id": bundle_id}
        ):
            raise AdmissionError("admission confirmation binding is invalid")

        bucket_root = self._root / bucket
        _secure_directory(bucket_root)
        destination = bucket_root / bundle_id
        temporary = bucket_root / f".admitting-{bundle_id}-{expected_fingerprint[:16]}"
        journal = self._repository.start_admission(
            profile_id=profile_id,
            bucket=bucket,
            bundle_id=bundle_id,
            fingerprint=expected_fingerprint,
            source_path="DRAFTS",
            destination_path=f"{bucket}/{bundle_id}",
            intent_id=intent_id,
        )
        self._inject("after_journal")
        if journal.phase == "installed":
            self._verify_installed(destination, expected_fingerprint)
            return journal
        if destination.exists():
            if journal.phase == "ready_installed":
                self._verify_installed(destination, expected_fingerprint)
                return self._repository.advance_admission(
                    journal.journal_id, "installed", expected_revision=journal.revision
                )
            raise ConflictError("admission destination already exists")
        if temporary.exists():
            if temporary.is_symlink() or not temporary.is_dir():
                raise AdmissionError("admission staging path is unsafe")
            shutil.rmtree(temporary)
        temporary.mkdir(mode=0o700)
        _fsync_directory(bucket_root)

        snapshots = {item.relative_name: item for item in draft.member_snapshots}
        for member in draft.members:
            snapshot = snapshots[member.name]
            self._repository.checkpoint_admission_member(
                journal.journal_id,
                relative_name=member.name,
                sha256=snapshot.sha256,
                size_bytes=snapshot.size_bytes,
                phase="planned",
            )
            self._inject(f"after_member_planned:{member.name}")
            _copy_exact(
                member, temporary / member.name, snapshot.sha256, snapshot.size_bytes
            )
            self._inject(f"after_member_copied:{member.name}")
            self._repository.checkpoint_admission_member(
                journal.journal_id,
                relative_name=member.name,
                sha256=snapshot.sha256,
                size_bytes=snapshot.size_bytes,
                phase="copied",
            )
            self._repository.checkpoint_admission_member(
                journal.journal_id,
                relative_name=member.name,
                sha256=snapshot.sha256,
                size_bytes=snapshot.size_bytes,
                phase="verified",
            )
            self._inject(f"after_member_verified:{member.name}")
        copied_scan = scan_inbox(temporary)
        if (
            len(copied_scan.bundles) != 1
            or copied_scan.bundles[0].fingerprint != expected_fingerprint
        ):
            raise AdmissionError(
                "copied draft failed semantic fingerprint verification"
            )
        self._inject("before_ready_install")
        marker = temporary / ".ready"
        descriptor = os.open(
            marker, os.O_WRONLY | os.O_CREAT | os.O_EXCL | _no_follow(), 0o600
        )
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
        _fsync_directory(temporary)
        journal = self._repository.get_admission(journal.journal_id)
        journal = self._repository.advance_admission(
            journal.journal_id, "ready_installed", expected_revision=journal.revision
        )
        self._inject("after_ready_install")
        self._inject("before_atomic_rename")
        _rename_no_replace(temporary, destination)
        _fsync_directory(bucket_root)
        self._inject("after_atomic_rename")
        journal = self._repository.advance_admission(
            journal.journal_id, "installed", expected_revision=journal.revision
        )
        return journal

    def recover(self) -> tuple[AdmissionRecord, ...]:
        """Finish post-rename journals and roll back incomplete hidden copies."""
        recovered: list[AdmissionRecord] = []
        for journal in self._repository.list_recoverable_admissions():
            profile = self._repository.get_profile(journal.profile_id)
            if profile.account_root.resolve(strict=False) != self._root:
                continue
            destination = self._root / journal.destination_path
            temporary = (
                destination.parent
                / f".admitting-{journal.bundle_id}-{journal.fingerprint[:16]}"
            )
            if destination.exists() and journal.phase == "ready_installed":
                self._verify_installed(destination, journal.fingerprint)
                journal = self._repository.advance_admission(
                    journal.journal_id, "installed", expected_revision=journal.revision
                )
            elif (
                temporary.exists()
                and not temporary.is_symlink()
                and journal.phase == "ready_installed"
            ):
                self._verify_installed(temporary, journal.fingerprint)
                _rename_no_replace(temporary, destination)
                _fsync_directory(destination.parent)
                journal = self._repository.advance_admission(
                    journal.journal_id, "installed", expected_revision=journal.revision
                )
            else:
                if temporary.exists() and not temporary.is_symlink():
                    shutil.rmtree(temporary)
                journal = self._repository.advance_admission(
                    journal.journal_id,
                    "rolled_back",
                    expected_revision=journal.revision,
                )
            recovered.append(journal)
        return tuple(recovered)

    def _inject(self, boundary: str) -> None:
        if self._fault is not None:
            self._fault(boundary)

    @staticmethod
    def _verify_installed(destination: Path, fingerprint: str) -> None:
        if destination.is_symlink() or not destination.is_dir():
            raise AdmissionError("installed admission destination is unsafe")
        scan = scan_inbox(destination)
        if len(scan.bundles) != 1 or scan.bundles[0].fingerprint != fingerprint:
            raise AdmissionError("installed admission fingerprint is invalid")


def _copy_exact(source: Path, destination: Path, sha256: str, size_bytes: int) -> None:
    before = os.stat(source, follow_symlinks=False)
    if not stat.S_ISREG(before.st_mode):
        raise AdmissionError("draft member is not a regular file")
    flags = os.O_RDONLY | _no_follow()
    source_fd = os.open(source, flags)
    target_fd = os.open(
        destination, os.O_WRONLY | os.O_CREAT | os.O_EXCL | _no_follow(), 0o600
    )
    digest = hashlib.sha256()
    total = 0
    try:
        opened = os.fstat(source_fd)
        if not os.path.samestat(before, opened):
            raise AdmissionError("draft member changed before copy")
        while True:
            chunk = os.read(source_fd, _CHUNK)
            if not chunk:
                break
            digest.update(chunk)
            total += len(chunk)
            remaining = memoryview(chunk)
            while remaining:
                written = os.write(target_fd, remaining)
                if written <= 0:
                    raise AdmissionError("draft member copy could not make progress")
                remaining = remaining[written:]
        os.fsync(target_fd)
        after = os.stat(source, follow_symlinks=False)
        if not os.path.samestat(before, after):
            raise AdmissionError("draft member changed during copy")
    finally:
        os.close(target_fd)
        os.close(source_fd)
    if total != size_bytes or digest.hexdigest() != sha256:
        raise AdmissionError("draft member bytes differ from approved preview")


def _secure_directory(path: Path) -> None:
    from post_pulsar.secure_files import SecureFileError, secure_directory

    try:
        secure_directory(path)
    except (OSError, SecureFileError):
        raise AdmissionError("admission directory is unsafe") from None


def _fsync_directory(path: Path) -> None:
    from post_pulsar.secure_files import fsync_directory

    fsync_directory(path)


def _no_follow() -> int:
    return cast(int, getattr(os, "O_NOFOLLOW", 0))


def _rename_no_replace(source: Path, destination: Path) -> None:
    """Atomically install a directory without replacing an existing entry."""

    if os.name == "nt":
        try:
            os.rename(source, destination)
        except FileExistsError:
            raise ConflictError("admission destination already exists") from None
        return
    libc = ctypes.CDLL(None, use_errno=True)
    if sys.platform == "darwin":
        renamex = libc.renamex_np
        renamex.argtypes = (ctypes.c_char_p, ctypes.c_char_p, ctypes.c_uint)
        renamex.restype = ctypes.c_int
        result = renamex(os.fsencode(source), os.fsencode(destination), 4)
        if result == 0:
            return
        if ctypes.get_errno() in {errno.EEXIST, errno.ENOTEMPTY}:
            raise ConflictError("admission destination already exists")
        raise AdmissionError("atomic admission install failed")
    renameat2 = getattr(libc, "renameat2", None)
    if renameat2 is None:
        raise AdmissionError("atomic no-replace rename is unavailable")
    renameat2.argtypes = (
        ctypes.c_int,
        ctypes.c_char_p,
        ctypes.c_int,
        ctypes.c_char_p,
        ctypes.c_uint,
    )
    renameat2.restype = ctypes.c_int
    result = renameat2(-100, os.fsencode(source), -100, os.fsencode(destination), 1)
    if result == 0:
        return
    error = ctypes.get_errno()
    if error in {errno.EEXIST, errno.ENOTEMPTY}:
        raise ConflictError("admission destination already exists")
    raise AdmissionError("atomic admission install failed")


__all__ = ["AdmissionError", "DraftAdmissionService"]
