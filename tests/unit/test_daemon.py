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


def test_endpoint_recovery_never_probes_with_signals(tmp_path: Path, monkeypatch):
    from post_pulsar import daemon as module

    endpoint = tmp_path / "endpoint.json"
    record = EndpointRecord(
        "post-pulsar.control/v1", "127.0.0.1:1", os.getpid(), 1, "n", {}
    )
    record.write(endpoint)

    def forbidden(*args):
        pytest.fail("endpoint recovery must never signal or probe another process")

    monkeypatch.setattr(module.os, "kill", forbidden)
    daemon = ForegroundDaemon(
        tmp_path / "state",
        endpoint,
        "127.0.0.1",
        0,
        server_factory=lambda *args: _Server(*args),
    )
    try:
        daemon.start()
        assert EndpointRecord.read(endpoint).startup_nonce == daemon._nonce
    finally:
        daemon.stop()


def test_endpoint_with_unverifiable_process_identity_is_preserved(
    tmp_path: Path, monkeypatch
):
    import importlib

    identity = importlib.import_module("post_pulsar.process_identity")
    endpoint = tmp_path / "endpoint.json"
    record = EndpointRecord(
        "post-pulsar.control/v1", "127.0.0.1:1", 987, 1, "existing", {}
    )
    record.write(endpoint)
    daemon = ForegroundDaemon(
        tmp_path / "state",
        endpoint,
        "127.0.0.1",
        0,
        server_factory=lambda *args: _Server(*args),
    )

    def unverifiable(pid):
        raise identity.ProcessIdentityError("process identity is unavailable")

    monkeypatch.setattr(identity, "process_start_identity", unverifiable)
    with pytest.raises(identity.ProcessIdentityError):
        daemon.start()
    assert EndpointRecord.read(endpoint) == record
    assert daemon._lease is None


def test_signals_are_installed_before_recovery_and_stop_skips_startup_work(
    tmp_path: Path, monkeypatch
):
    from post_pulsar import daemon as module

    installed = {}
    history = []
    originals = {module.signal.SIGINT: object(), module.signal.SIGTERM: object()}

    def install(signum, handler):
        previous = installed.get(signum, originals[signum])
        installed[signum] = handler
        history.append((signum, handler))
        return previous

    monkeypatch.setattr(module.signal, "signal", install)
    during_recovery = []
    scheduled = []
    servers = []

    def recover():
        during_recovery.extend(installed)
        if module.signal.SIGTERM in installed:
            installed[module.signal.SIGTERM](module.signal.SIGTERM, None)
        else:
            daemon._stop.set()
        assert daemon._lease is not None

    def server_factory(*args):
        servers.append(args)
        return _Server(*args)

    daemon = ForegroundDaemon(
        tmp_path / "state",
        tmp_path / "endpoint.json",
        "127.0.0.1",
        0,
        recovery=recover,
        schedule_admission=lambda: scheduled.append(True),
        server_factory=server_factory,
    )
    daemon.run_forever()
    assert set(during_recovery) == set(originals)
    assert installed == originals
    assert len(history) == 4
    assert scheduled == [] and servers == []
    assert daemon._lease is None


@pytest.mark.parametrize("failure_phase", ["start", "stop"])
def test_signal_handlers_restore_across_entire_lifecycle(
    tmp_path: Path, monkeypatch, failure_phase
):
    from post_pulsar import daemon as module

    originals = {module.signal.SIGINT: object(), module.signal.SIGTERM: object()}
    installed = dict(originals)
    history = []

    def install(signum, handler):
        previous = installed[signum]
        installed[signum] = handler
        history.append((signum, handler))
        return previous

    daemon = ForegroundDaemon(
        tmp_path / "state", tmp_path / "endpoint.json", "127.0.0.1", 0
    )

    def fail():
        raise RuntimeError("injected lifecycle failure")

    monkeypatch.setattr(module.signal, "signal", install)
    monkeypatch.setattr(
        daemon, "start", fail if failure_phase == "start" else daemon._stop.set
    )
    monkeypatch.setattr(
        daemon, "stop", fail if failure_phase == "stop" else lambda: None
    )
    with pytest.raises(RuntimeError, match="injected lifecycle failure"):
        daemon.run_forever()
    assert len(history) == 4
    assert installed == originals


def test_concurrent_stop_drains_startup_callback_before_releasing_lease(tmp_path: Path):
    entered = threading.Event()
    release = threading.Event()
    stop_requested = threading.Event()
    stop_finished = threading.Event()
    startup_finished = threading.Event()
    failures = []
    servers = []

    def recover():
        entered.set()
        if not release.wait(2):
            raise RuntimeError("test did not release startup callback")

    def factory(*args):
        servers.append(args)
        return _Server(*args)

    daemon = ForegroundDaemon(
        tmp_path / "state",
        tmp_path / "endpoint.json",
        "127.0.0.1",
        0,
        recovery=recover,
        server_factory=factory,
    )

    def start():
        try:
            daemon.start()
        except BaseException as exc:
            failures.append(exc)
        finally:
            startup_finished.set()

    def stop():
        stop_requested.set()
        daemon.stop()
        stop_finished.set()

    starter = threading.Thread(target=start)
    stopper = threading.Thread(target=stop)
    starter.start()
    try:
        assert entered.wait(1)
        stopper.start()
        assert stop_requested.wait(1)
        assert not stop_finished.wait(0.1)
        assert daemon._lease is not None
        assert not startup_finished.is_set()
    finally:
        release.set()
        starter.join(2)
        if stopper.ident is not None:
            stopper.join(2)
        daemon.stop()
    assert not starter.is_alive() and not stopper.is_alive()
    assert failures == []
    assert servers == []
    assert daemon._lease is None


def test_accepted_pause_drains_current_request_and_blocks_older_queue_and_due_work(
    tmp_path,
):
    from dataclasses import asdict
    from datetime import UTC, datetime, timedelta
    from post_pulsar.state import (
        BundleFileSnapshot,
        ProfileTargetSnapshot,
        StateRepository,
        TargetSnapshot,
    )

    database = tmp_path / "state/post_pulsar.sqlite3"
    with StateRepository(database) as repository:
        repository.register_profile(
            "fixture",
            tmp_path / "account",
            (
                ProfileTargetSnapshot(
                    "x", "fixture-user", "fixture", "FIXTURE_TOKEN_REFERENCE", {}
                ),
            ),
            config_hash="a" * 64,
        )
        key = repository.add_bundle(
            profile_id="fixture",
            bundle_id="post",
            fingerprint="b" * 64,
            source_bucket="QUEUE",
            files=(
                BundleFileSnapshot(
                    "post.jpg", "image", None, "image", "image/jpeg", 1, "a" * 64
                ),
            ),
            targets=(
                TargetSnapshot(
                    "x",
                    "fixture-user",
                    "fixture",
                    "FIXTURE_TOKEN_REFERENCE",
                    "2",
                    1,
                    {},
                ),
            ),
        )
        for trigger in ("fixture-current", "fixture-queued"):
            arguments = {
                "bundle_id": "post",
                "bucket": "QUEUE",
                "fingerprint": "b" * 64,
                "trigger_id": trigger,
            }
            intent = repository.create_confirmation_intent(
                action="run_now",
                arguments=arguments,
                profile_id="fixture",
                resource_revision=1,
                fingerprint="b" * 64,
                consequence="Fixture-only publication callback.",
                expires_at=datetime.now(UTC) + timedelta(minutes=5),
                bundle_key=key,
            )
            repository.approve_confirmation_intent(
                intent.intent_id, expected_revision=1
            )
            repository.consume_intent_with_request(
                intent_id=intent.intent_id,
                action="run_now",
                arguments=arguments,
                profile_id="fixture",
                resource_revision=1,
                fingerprint="b" * 64,
                idempotency_key=trigger,
                bundle_key=key,
            )
    entered, release, paused, due = (threading.Event() for _ in range(4))
    publications, schedules = [], []

    def schedule():
        if due.is_set():
            with StateRepository.open_existing(database) as repository:
                if not repository.get_pause_state().admission_blocked:
                    schedules.append("fixture-new-due")

    def execute(request):
        if request.action == "pause":
            with StateRepository.open_existing(database) as repository:
                result = repository.execute_local_run_request(
                    request,
                    lambda: asdict(repository.set_paused(True, expected_revision=1)),
                )
            paused.set()
            return result
        publications.append(request.arguments["trigger_id"])
        entered.set()
        assert release.wait(2)
        return {"status": "fixture-completed"}

    daemon = ForegroundDaemon(
        tmp_path / "state",
        tmp_path / "endpoint.json",
        "127.0.0.1",
        0,
        request_executor=execute,
        schedule_admission=schedule,
        server_factory=lambda *args: _Server(*args),
    )
    try:
        daemon.start()
        assert entered.wait(1)
        with StateRepository.open_existing(database) as repository:
            repository.create_run_request(
                profile_id="fixture",
                action="pause",
                arguments={},
                idempotency_key="fixture-pause",
                expected_revision=1,
            )
        due.set()
        release.set()
        assert paused.wait(1)
    finally:
        release.set()
        daemon.stop()
    assert publications == ["fixture-current"] and schedules == []
    with StateRepository.open_existing(database) as repository:
        assert repository.get_run_request(2).status == "queued"
        assert repository.get_pause_state().paused


@pytest.mark.parametrize(
    "header",
    [
        "Authorization",
        "X-Post-Pulsar-Principal",
        "X-Post-Pulsar-Version",
        "X-Post-Pulsar-Control-Version",
        "If-Match",
        "Idempotency-Key",
        "X-Post-Pulsar-Startup-Nonce",
        "X-Post-Pulsar-Operator-Secret",
        "Content-Length",
        "Content-Type",
        "Transfer-Encoding",
        "Host",
    ],
)
@pytest.mark.parametrize("second", ["fixture", "different-fixture"])
def test_http_adapter_rejects_duplicate_singletons_before_dispatch(header, second):
    from email.message import Message
    from io import BytesIO
    from types import SimpleNamespace
    from post_pulsar.daemon import _handler_for

    dispatched, statuses = [], []
    application = SimpleNamespace(handle=lambda request: dispatched.append(request))
    handler = object.__new__(_handler_for(application))
    handler.headers = Message()
    handler.headers[header] = "fixture"
    handler.headers[header.lower()] = second
    handler.command, handler.path = "POST", "/control/v1/shutdown"
    handler.rfile, handler.wfile = BytesIO(), BytesIO()
    handler.send_response = lambda status, *args: statuses.append(status)
    handler.send_header = lambda *args: None
    handler.end_headers = lambda: None
    handler.send_error = lambda status, *args: statuses.append(status)
    handler._serve()
    assert statuses == [400]
    assert dispatched == []
