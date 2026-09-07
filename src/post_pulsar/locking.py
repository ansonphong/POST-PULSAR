"""Ordered process and profile leases for POST PULSAR operations."""

from __future__ import annotations

import os
import re
import stat
from dataclasses import dataclass
from pathlib import Path
from typing import Final, Literal

from filelock import FileLock, Timeout

type LockKind = Literal["instance", "maintenance", "profiles"]

_PROFILE_RE: Final = re.compile(r"[a-z0-9][a-z0-9-]{0,31}\Z")


class LockError(RuntimeError):
    """Base class for safe lock failures."""


class LockContentionError(LockError):
    """Another process owns a requested lease."""


class LockOrderError(LockError):
    """A caller attempted to violate the global lease order."""


@dataclass(frozen=True, slots=True, eq=False)
class LockLease:
    """An already-acquired lease that can be passed into application services."""

    _manager: LockManager
    kind: LockKind
    resources: tuple[str, ...]
    _locks: tuple[FileLock, ...]
    active: bool = True

    def __enter__(self) -> LockLease:
        if not self.active:
            raise LockOrderError("released lock lease cannot be reused")
        return self

    def __exit__(self, *_exception: object) -> None:
        self.release()

    def release(self) -> None:
        self._manager._release(self)


class LockManager:
    """Acquire leases in instance, maintenance, then sorted-profile order."""

    def __init__(self, state_directory: str | os.PathLike[str]) -> None:
        state_root = Path(state_directory).expanduser().absolute()
        _secure_directory(state_root)
        root = state_root / "locks"
        _secure_directory(root)
        self._root = root
        self._profiles = root / "profiles"
        _secure_directory(self._profiles)
        self._held: list[LockLease] = []
        self._issued: dict[
            int, tuple[LockKind, tuple[str, ...], tuple[FileLock, ...]]
        ] = {}

    def acquire_instance(self) -> LockLease:
        if self._held:
            raise LockOrderError("instance lease must be acquired first")
        return self._acquire("instance", ("instance",), (self._root / "instance.lock",))

    def acquire_maintenance(self, instance: LockLease) -> LockLease:
        self.require_instance(instance)
        if any(lease.kind in {"maintenance", "profiles"} for lease in self._held):
            raise LockOrderError("maintenance lease must precede profile leases")
        return self._acquire(
            "maintenance", ("maintenance",), (self._root / "maintenance.lock",)
        )

    def acquire_profiles(
        self,
        instance: LockLease,
        profile_ids: tuple[str, ...] | list[str],
        *,
        maintenance: LockLease | None = None,
    ) -> LockLease:
        self.require_instance(instance)
        if maintenance is not None:
            self._require(maintenance, "maintenance")
        if any(lease.kind == "profiles" for lease in self._held):
            raise LockOrderError("profile leases cannot be nested or reacquired")
        normalized = tuple(
            sorted(set(profile_ids), key=lambda item: (item.casefold(), item))
        )
        if not normalized or any(
            not _PROFILE_RE.fullmatch(item) for item in normalized
        ):
            raise LockOrderError("profile lease identity is invalid")
        paths = tuple(
            self._profiles / f"{profile_id}.lock" for profile_id in normalized
        )
        return self._acquire("profiles", normalized, paths)

    def require_instance(self, lease: LockLease) -> None:
        self._require(lease, "instance")

    def require_profile(self, lease: LockLease, profile_id: str) -> None:
        self._require(lease, "profiles")
        if profile_id not in lease.resources:
            raise LockOrderError("profile lease does not own the requested profile")

    def _require(self, lease: LockLease, kind: LockKind) -> None:
        if lease._manager is not self or not lease.active:
            raise LockOrderError("required already-held lease is invalid")
        held_index = next(
            (index for index, candidate in enumerate(self._held) if candidate is lease),
            None,
        )
        issued = self._issued.get(id(lease))
        if held_index is None or issued is None:
            raise LockOrderError("required already-held lease is invalid")
        issued_kind, issued_resources, issued_locks = issued
        if (
            lease.kind != kind
            or lease.kind != issued_kind
            or lease.resources != issued_resources
            or len(lease._locks) != len(issued_locks)
            or any(
                current is not original
                for current, original in zip(lease._locks, issued_locks, strict=True)
            )
        ):
            raise LockOrderError("required already-held lease was mutated")
        stack = tuple(candidate.kind for candidate in self._held)
        if stack not in {
            ("instance",),
            ("instance", "maintenance"),
            ("instance", "profiles"),
            ("instance", "maintenance", "profiles"),
        }:
            raise LockOrderError("required already-held lease order is invalid")
        if kind == "instance" and held_index != 0:
            raise LockOrderError("required already-held lease order is invalid")
        if kind == "maintenance" and held_index != 1:
            raise LockOrderError("required already-held lease order is invalid")
        if kind == "profiles" and held_index != len(self._held) - 1:
            raise LockOrderError("required already-held lease order is invalid")

    def _acquire(
        self, kind: LockKind, resources: tuple[str, ...], paths: tuple[Path, ...]
    ) -> LockLease:
        acquired: list[FileLock] = []
        try:
            for path in paths:
                _reject_symlink(path)
                lock = FileLock(path)
                lock.acquire(timeout=0)
                acquired.append(lock)
                try:
                    path.chmod(0o600)
                except OSError:
                    raise LockError(
                        "lock file permissions could not be secured"
                    ) from None
        except Timeout:
            for lock in reversed(acquired):
                lock.release()
            raise LockContentionError(
                "another POST PULSAR process owns the lock"
            ) from None
        except Exception:
            for lock in reversed(acquired):
                lock.release()
            raise
        lease = LockLease(self, kind, resources, tuple(acquired))
        self._held.append(lease)
        self._issued[id(lease)] = (kind, resources, tuple(acquired))
        return lease

    def _release(self, lease: LockLease) -> None:
        if not lease.active:
            if id(lease) in self._issued:
                raise LockOrderError("required already-held lease was mutated")
            return
        issued = self._issued.get(id(lease))
        if issued is None:
            raise LockOrderError("required already-held lease is invalid")
        self._require(lease, issued[0])
        if not self._held or self._held[-1] is not lease:
            raise LockOrderError("lock leases must be released in reverse order")
        for lock in reversed(lease._locks):
            lock.release()
        object.__setattr__(lease, "active", False)
        self._held.pop()
        self._issued.pop(id(lease), None)


def _secure_directory(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True, mode=0o700)
    metadata = path.lstat()
    if stat.S_ISLNK(metadata.st_mode) or not stat.S_ISDIR(metadata.st_mode):
        raise LockError("lock directory is unsafe")
    try:
        path.chmod(0o700)
    except OSError:
        raise LockError("lock directory permissions could not be secured") from None


def _reject_symlink(path: Path) -> None:
    try:
        metadata = path.lstat()
    except FileNotFoundError:
        return
    if stat.S_ISLNK(metadata.st_mode) or not stat.S_ISREG(metadata.st_mode):
        raise LockError("lock file is unsafe")


__all__ = [
    "LockContentionError",
    "LockError",
    "LockLease",
    "LockManager",
    "LockOrderError",
]
