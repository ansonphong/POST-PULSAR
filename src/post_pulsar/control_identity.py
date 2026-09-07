"""Closed, non-secret installation and process identity for local discovery."""

from __future__ import annotations

import re
from collections.abc import Mapping
from dataclasses import asdict, dataclass
from pathlib import Path


@dataclass(frozen=True, slots=True)
class DiscoveryPaths:
    """Canonical configured locations, never commands or credential values."""

    bootstrap_record: str
    endpoint_record: str
    agent_capability_file: str

    def __post_init__(self) -> None:
        paths = (
            self.bootstrap_record,
            self.endpoint_record,
            self.agent_capability_file,
        )
        for value in paths:
            if (
                not isinstance(value, str)
                or not Path(value).is_absolute()
                or str(Path(value).resolve()) != value
            ):
                raise ValueError("discovery paths must be canonical and absolute")
        if len(set(paths)) != 3:
            raise ValueError("discovery paths must be distinct")

    @classmethod
    def from_paths(
        cls, bootstrap: str | Path, endpoint: str | Path, capability: str | Path
    ) -> DiscoveryPaths:
        return cls(
            *(str(Path(path).resolve()) for path in (bootstrap, endpoint, capability))
        )

    @classmethod
    def read(cls, value: object) -> DiscoveryPaths:
        if not isinstance(value, Mapping) or set(value) != {
            "bootstrap_record",
            "endpoint_record",
            "agent_capability_file",
        }:
            raise ValueError("discovery schema is invalid")
        return cls(**value)


@dataclass(frozen=True, slots=True)
class DaemonIdentity:
    """The same immutable identity appears in endpoint and authenticated handshake."""

    installation_id: str
    pid: int
    process_started_at: int
    startup_nonce: str
    discovery: DiscoveryPaths

    def __post_init__(self) -> None:
        if not isinstance(self.installation_id, str) or not re.fullmatch(
            r"[0-9a-f]{32}", self.installation_id
        ):
            raise ValueError("daemon installation ID is invalid")
        if any(
            type(value) is not int or value <= 0
            for value in (self.pid, self.process_started_at)
        ):
            raise ValueError("daemon process identity is invalid")
        if not isinstance(self.startup_nonce, str) or not re.fullmatch(
            r"[0-9a-f]{64}", self.startup_nonce
        ):
            raise ValueError("daemon startup nonce is invalid")
        if not isinstance(self.discovery, DiscoveryPaths):
            raise ValueError("daemon discovery identity is invalid")

    def document(self) -> dict[str, object]:
        return asdict(self)
