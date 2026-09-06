"""Secretless, owner-only discovery record for local control clients."""

from __future__ import annotations

import json
import os
import re
import secrets
import stat
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Final, cast

BOOTSTRAP_SCHEMA: Final = "post-pulsar.bootstrap/v1"
SERVICE_MODES: Final = frozenset(
    {"manual", "systemd-user", "launchd-agent", "windows-service", "windows-task"}
)
_INSTALLATION_RE: Final = re.compile(r"[0-9a-f]{32}\Z")
_SERVICE_ID_RE: Final = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.@:-]{0,127}\Z")


class BootstrapError(RuntimeError):
    """A bootstrap record is unsafe or violates its closed schema."""


@dataclass(frozen=True, slots=True)
class BootstrapRecord:
    """Validated non-secret paths and installed service discovery metadata."""

    installation_id: str
    endpoint_record: Path
    agent_capability: Path
    service_mode: str
    service_identifier: str
    schema: str = BOOTSTRAP_SCHEMA

    def document(self) -> dict[str, object]:
        return {
            "schema": self.schema,
            "installation_id": self.installation_id,
            "endpoint_record": str(self.endpoint_record),
            "agent_capability": str(self.agent_capability),
            "service": {
                "mode": self.service_mode,
                "identifier": self.service_identifier,
            },
        }


def validate_bootstrap(
    value: BootstrapRecord | Mapping[str, object],
) -> BootstrapRecord:
    """Validate the exact checked-in schema without accepting command text."""
    if isinstance(value, BootstrapRecord):
        record = value
    else:
        if set(value) != {
            "schema",
            "installation_id",
            "endpoint_record",
            "agent_capability",
            "service",
        }:
            raise BootstrapError("bootstrap schema is invalid")
        service = value.get("service")
        if not isinstance(service, dict) or set(service) != {"mode", "identifier"}:
            raise BootstrapError("bootstrap service schema is invalid")
        try:
            record = BootstrapRecord(
                schema=cast(str, value["schema"]),
                installation_id=cast(str, value["installation_id"]),
                endpoint_record=Path(cast(str, value["endpoint_record"])),
                agent_capability=Path(cast(str, value["agent_capability"])),
                service_mode=cast(str, service["mode"]),
                service_identifier=cast(str, service["identifier"]),
            )
        except (TypeError, ValueError):
            raise BootstrapError("bootstrap schema is invalid") from None
    if record.schema != BOOTSTRAP_SCHEMA:
        raise BootstrapError("bootstrap schema version is invalid")
    if not isinstance(record.installation_id, str) or not _INSTALLATION_RE.fullmatch(
        record.installation_id
    ):
        raise BootstrapError("bootstrap installation ID is invalid")
    for path in (record.endpoint_record, record.agent_capability):
        if not isinstance(path, Path) or not path.is_absolute():
            raise BootstrapError("bootstrap paths must be absolute")
    if record.endpoint_record == record.agent_capability:
        raise BootstrapError("bootstrap paths must be distinct")
    if record.service_mode not in SERVICE_MODES:
        raise BootstrapError("bootstrap service mode is invalid")
    if not isinstance(record.service_identifier, str) or not _SERVICE_ID_RE.fullmatch(
        record.service_identifier
    ):
        raise BootstrapError("bootstrap service identifier is invalid")
    return record


def write_bootstrap(
    path: str | Path, value: BootstrapRecord | Mapping[str, object]
) -> BootstrapRecord:
    """Atomically write a validated owner-only bootstrap record."""
    destination = Path(path)
    record = validate_bootstrap(value)
    _assert_safe_target(destination, allow_missing=True)
    destination.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    parent = destination.parent.lstat()
    if stat.S_ISLNK(parent.st_mode) or not stat.S_ISDIR(parent.st_mode):
        raise BootstrapError("bootstrap directory is unsafe")
    if os.name != "nt":
        destination.parent.chmod(0o700)
    temporary = destination.parent / f".{destination.name}.{secrets.token_hex(8)}"
    descriptor = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    try:
        payload = (
            json.dumps(record.document(), sort_keys=True, separators=(",", ":")) + "\n"
        ).encode("utf-8")
        os.write(descriptor, payload)
        os.fsync(descriptor)
    finally:
        os.close(descriptor)
    try:
        os.replace(temporary, destination)
        if os.name != "nt":
            destination.chmod(0o600)
        _fsync_parent(destination)
    finally:
        temporary.unlink(missing_ok=True)
    return record


def load_bootstrap(path: str | Path) -> BootstrapRecord:
    """Load an owner-only, regular bootstrap record through its closed schema."""
    source = Path(path)
    _assert_safe_target(source, allow_missing=False)
    try:
        value = json.loads(source.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError):
        raise BootstrapError("bootstrap record is invalid") from None
    if not isinstance(value, dict):
        raise BootstrapError("bootstrap schema is invalid")
    return validate_bootstrap(cast(Mapping[str, object], value))


def _assert_safe_target(path: Path, *, allow_missing: bool) -> None:
    try:
        metadata = path.lstat()
    except FileNotFoundError:
        if allow_missing:
            return
        raise BootstrapError("bootstrap record is missing") from None
    if (
        stat.S_ISLNK(metadata.st_mode)
        or not stat.S_ISREG(metadata.st_mode)
        or metadata.st_nlink != 1
    ):
        raise BootstrapError("bootstrap target is unsafe")
    if os.name != "nt" and metadata.st_mode & 0o077:
        raise BootstrapError("bootstrap permissions are too broad")


def _fsync_parent(path: Path) -> None:
    descriptor = os.open(path.parent, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


__all__ = [
    "BOOTSTRAP_SCHEMA",
    "SERVICE_MODES",
    "BootstrapError",
    "BootstrapRecord",
    "load_bootstrap",
    "validate_bootstrap",
    "write_bootstrap",
]
