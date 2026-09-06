"""Foreground daemon and local credential boundary tests."""

from __future__ import annotations

import io
import json
import os
import threading
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


def test_endpoint_record_rejects_hardlinks(tmp_path: Path) -> None:
    endpoint = tmp_path / "endpoint.json"
    record = EndpointRecord(
        "post-pulsar.control/v1", "127.0.0.1:1", os.getpid(), 1, "n", {}
    )
    record.write(endpoint)
    alias = tmp_path / "alias.json"
    os.link(endpoint, alias)
    with pytest.raises(RuntimeError, match="unsafe"):
        EndpointRecord.read(endpoint)
    with pytest.raises(RuntimeError, match="unsafe"):
        record.write(endpoint)


def test_daemon_stops_worker_admission_before_http_shutdown(tmp_path: Path) -> None:
    order: list[str] = []
    reevaluated = threading.Event()

    def schedule() -> None:
        order.append("schedule")
        if order.count("schedule") >= 3:
            reevaluated.set()

    class Server(_Server):
        def shutdown(self) -> None:
            order.append("http")
            super().shutdown()

    daemon = ForegroundDaemon(
        tmp_path / "state",
        tmp_path / "endpoint.json",
        "127.0.0.1",
        0,
        recovery=lambda: order.append("recover"),
        schedule_admission=schedule,
        server_factory=lambda *args: Server(*args),
        poll_seconds=0.01,
    )
    daemon.start()
    assert reevaluated.wait(0.5)
    daemon.stop()
    assert order[:2] == ["recover", "schedule"]
    assert daemon._stop.is_set()
    assert order.index("http") >= 2


def test_callback_failure_is_durable_and_worker_continues(tmp_path: Path) -> None:
    from post_pulsar.state import StateRepository

    database = tmp_path / "state/post_pulsar.sqlite3"
    with StateRepository(database) as repository:
        assert repository.schema_version == 1
    continued = threading.Event()
    calls = 0

    def schedule() -> None:
        nonlocal calls
        calls += 1
        if calls <= 2:
            raise ValueError("secret-value must never reach durable diagnostics")
        continued.set()

    daemon = ForegroundDaemon(
        tmp_path / "state",
        tmp_path / "endpoint.json",
        "127.0.0.1",
        0,
        recovery=lambda: (_ for _ in ()).throw(ValueError("secret-value")),
        schedule_admission=schedule,
        poll_seconds=0.01,
        server_factory=lambda *args: _Server(*args),
    )
    try:
        daemon.start()
        assert continued.wait(1)
        assert daemon._worker_thread.is_alive()
        with StateRepository.open_existing(database) as repository:
            events = repository.control_events(limit=100)
            assert "callback_failed" in json.dumps(events)
            assert "secret-value" not in json.dumps(events)
    finally:
        daemon.stop()


def test_each_schedule_boundary_respects_shutdown(tmp_path: Path) -> None:
    daemon = ForegroundDaemon(
        tmp_path / "state", tmp_path / "endpoint.json", "127.0.0.1", 0
    )
    dispatched: list[int] = []
    assert daemon.dispatch_if_running(lambda: dispatched.append(1))
    with daemon._claim_gate:
        daemon._stop.set()
    assert not daemon.dispatch_if_running(lambda: dispatched.append(2))
    assert dispatched == [1]
