"""Foreground daemon and local credential boundary tests."""

from __future__ import annotations

import io
import json
import os
from pathlib import Path

import pytest

from post_pulsar.control import (
    ControlSecurityError,
    initialize_operator_secret,
    load_agent_capability,
    rotate_agent_capability,
    verify_operator_secret,
)
from post_pulsar.daemon import EndpointRecord, ForegroundDaemon
from post_pulsar.locking import LockContentionError


class _TTY(io.StringIO):
    def isatty(self) -> bool:
        return True


class _Server:
    server_address = ("127.0.0.1", 43210)

    def __init__(self, *_args: object) -> None:
        self.stopped = False

    def serve_forever(self) -> None:
        while not self.stopped:
            import time

            time.sleep(0.001)

    def shutdown(self) -> None:
        self.stopped = True

    def server_close(self) -> None:
        self.stopped = True


def test_capability_rotation_is_owner_only_symlink_safe_and_revokes_old(
    tmp_path: Path,
) -> None:
    path = tmp_path / "control/agent"
    first = rotate_agent_capability(path)
    assert len(bytes.fromhex(first)) == 32
    assert os.stat(path).st_mode & 0o777 == 0o600
    second = rotate_agent_capability(path)
    assert second != first and load_agent_capability(path) == second
    path.unlink()
    path.symlink_to(tmp_path / "elsewhere")
    with pytest.raises(ControlSecurityError):
        rotate_agent_capability(path)


def test_operator_secret_requires_real_tty_and_has_bounded_lockout(
    tmp_path: Path,
) -> None:
    path = tmp_path / "control/operator.json"
    with pytest.raises(ControlSecurityError, match="TTY"):
        initialize_operator_secret(path, input_stream=io.StringIO("secret\n"))
    initialize_operator_secret(path, input_stream=_TTY("correct horse\n"))
    assert verify_operator_secret(path, "correct horse", now=100.0)
    for _ in range(5):
        assert not verify_operator_secret(path, "wrong", now=100.0)
    assert not verify_operator_secret(path, "correct horse", now=101.0)
    assert verify_operator_secret(path, "correct horse", now=401.0)


def test_daemon_holds_instance_lease_and_cleans_only_its_endpoint(
    tmp_path: Path,
) -> None:
    endpoint = tmp_path / "control/endpoint.json"
    daemon = ForegroundDaemon(
        tmp_path / "state",
        endpoint,
        "127.0.0.1",
        0,
        server_factory=lambda *args: _Server(*args),
    )
    daemon.start()
    record = EndpointRecord.read(endpoint)
    assert record.pid == os.getpid() and record.address.startswith("127.0.0.1:")
    second = ForegroundDaemon(
        tmp_path / "state",
        tmp_path / "other.json",
        "127.0.0.1",
        0,
        server_factory=lambda *args: _Server(*args),
    )
    with pytest.raises(LockContentionError):
        second.start()
    endpoint.write_text(json.dumps({"startup_nonce": "not-owner"}), encoding="utf-8")
    daemon.stop()
    assert endpoint.exists()
