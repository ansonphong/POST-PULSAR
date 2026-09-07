# mypy: disable-error-code=import-untyped
"""Hermetic fake-adapter launcher for the real POST PULSAR daemon core."""

from __future__ import annotations

import http.client
import ipaddress
import json
import socket
import threading
from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Protocol, cast

from post_pulsar import cli
from post_pulsar.config import SecretValue, load_local_settings
from post_pulsar.control import ControlApplication, load_agent_capability
from post_pulsar.daemon import EndpointRecord, ForegroundDaemon
from post_pulsar.platforms.base import (
    CheckpointWriter,
    PlatformAdapter,
    PreparedPublication,
    PublicationRequest,
    PublicationSnapshot,
    PublishResult,
    ValidationIssue,
)
from post_pulsar.state import DeliveryRecord, RunRequestRecord


class InjectedCrash(BaseException):
    """Represent abrupt process death at a durable orchestration boundary."""


class ClassifiedFailure(RuntimeError):
    """A sanitized fake remote failure with a production retry classification."""

    def __init__(self, classification: str) -> None:
        super().__init__("injected fake-adapter failure")
        self.retry_classification = classification


@dataclass(slots=True)
class FakeClock:
    """Controllable wall and monotonic clocks whose sleeper never blocks."""

    wall: datetime = datetime(2026, 9, 5, 12, tzinfo=UTC)
    monotonic: float = 0.0
    sleeps: list[float] = field(default_factory=list)

    def now(self) -> datetime:
        return self.wall

    def monotonic_now(self) -> float:
        return self.monotonic

    def sleep(self, seconds: float) -> None:
        self.sleeps.append(seconds)
        self.advance(timedelta(seconds=seconds))

    def advance(self, delta: timedelta) -> None:
        self.wall += delta
        self.monotonic += delta.total_seconds()


@dataclass(slots=True)
class AdapterPlan:
    """Per-target behavior programmed by an integration scenario."""

    issues: tuple[ValidationIssue, ...] = ()
    result: PublishResult | None = None
    prepare_failure: str | None = None


@dataclass(slots=True)
class AdapterTrace:
    """Observable fake remote calls without retaining credential values."""

    profile_id: str
    platform: str
    client_id: int
    preflights: int = 0
    prepares: int = 0
    commits: int = 0
    closed: bool = False


class FakeOfficialAdapter:
    """Protocol-faithful adapter that never performs network I/O."""

    def __init__(
        self,
        snapshot: PublicationSnapshot,
        trace: AdapterTrace,
        plan: AdapterPlan,
    ) -> None:
        self.snapshot = snapshot
        self.trace = trace
        self.plan = plan

    def verify_identity(self) -> None:
        return

    def preflight(self, publication: PublicationRequest) -> tuple[ValidationIssue, ...]:
        assert publication.snapshot == self.snapshot
        self.trace.preflights += 1
        return self.plan.issues

    def prepare(
        self,
        publication: PublicationRequest,
        *,
        prior: PreparedPublication | None,
        checkpoints: CheckpointWriter,
    ) -> PreparedPublication:
        del prior
        self.trace.prepares += 1
        if self.plan.prepare_failure is not None:
            raise ClassifiedFailure(self.plan.prepare_failure)
        delivery = checkpoints.advance_phase("ready")
        return PreparedPublication(publication, delivery.attempt_count, ())

    def commit(
        self, prepared: PreparedPublication, *, delivery: DeliveryRecord
    ) -> PublishResult:
        del prepared, delivery
        self.trace.commits += 1
        return self.plan.result or PublishResult.published(
            f"fake-{self.trace.profile_id}-{self.trace.platform}-{self.trace.commits}"
        )

    def close(self) -> None:
        self.trace.closed = True


class FakeAdapterRegistry:
    """Create isolated fake clients and retain their sanitized call traces."""

    def __init__(self) -> None:
        self.plans: dict[tuple[str, str], AdapterPlan] = {}
        self.traces: list[AdapterTrace] = []

    def set_plan(self, profile_id: str, platform: str, plan: AdapterPlan) -> None:
        self.plans[(profile_id, platform)] = plan

    def factory(
        self, snapshot: PublicationSnapshot, token: SecretValue, private: Path
    ) -> PlatformAdapter:
        del token
        assert private.name == "private"
        trace = AdapterTrace(
            snapshot.profile_id,
            snapshot.target.platform,
            len(self.traces) + 1,
        )
        self.traces.append(trace)
        return cast(
            PlatformAdapter,
            FakeOfficialAdapter(
                snapshot,
                trace,
                self.plans.get(
                    (snapshot.profile_id, snapshot.target.platform), AdapterPlan()
                ),
            ),
        )


class _MonkeyPatch(Protocol):
    def setattr(self, target: object, name: str, value: object) -> None: ...


@dataclass(slots=True)
class LoopbackSocketGuard:
    """Record and reject every attempted non-loopback IP bind or connection."""

    denied: list[tuple[str, object]] = field(default_factory=list)

    def install(self, monkeypatch: _MonkeyPatch) -> None:
        original_bind = socket.socket.bind
        original_connect = socket.socket.connect

        def checked_bind(instance: socket.socket, address: object) -> object:
            self._assert_loopback("bind", address)
            return original_bind(instance, address)  # type: ignore[arg-type]

        def checked_connect(instance: socket.socket, address: object) -> object:
            self._assert_loopback("connect", address)
            return original_connect(instance, address)  # type: ignore[arg-type]

        monkeypatch.setattr(socket.socket, "bind", checked_bind)
        monkeypatch.setattr(socket.socket, "connect", checked_connect)

    def _assert_loopback(self, operation: str, address: object) -> None:
        if not isinstance(address, tuple) or not address:
            self.denied.append((operation, address))
            raise OSError("integration socket policy denied non-IP socket")
        host = address[0]
        try:
            allowed = isinstance(host, str) and ipaddress.ip_address(host).is_loopback
        except ValueError:
            allowed = False
        if not allowed:
            self.denied.append((operation, address))
            raise OSError("integration socket policy denied non-loopback socket")


@dataclass(slots=True)
class RunningFakeDaemon:
    """A real daemon/control runtime wired only to fake request execution."""

    daemon: ForegroundDaemon
    control: ControlApplication
    capability: str
    executed: list[int]
    completed: threading.Event

    def request(
        self,
        method: str,
        target: str,
        *,
        body: Mapping[str, object] | None = None,
        headers: Mapping[str, str] | None = None,
        authenticated: bool = True,
    ) -> tuple[int, Mapping[str, object]]:
        endpoint = EndpointRecord.read(self.daemon._endpoint)
        host, port_text = endpoint.address.rsplit(":", 1)
        payload = b"" if body is None else json.dumps(body).encode("utf-8")
        request_headers = {"Content-Type": "application/json", **dict(headers or {})}
        if authenticated:
            request_headers["Authorization"] = f"Bearer {self.capability}"
        connection = http.client.HTTPConnection(host, int(port_text), timeout=2)
        try:
            connection.request(method, target, body=payload, headers=request_headers)
            response = connection.getresponse()
            document = json.loads(response.read())
            return response.status, cast(Mapping[str, object], document)
        finally:
            connection.close()

    def stop(self) -> None:
        self.daemon.stop()


def launch_fake_daemon(
    *,
    config_path: Path,
    registry: FakeAdapterRegistry,
    environ: Mapping[str, str],
    clock: FakeClock,
) -> RunningFakeDaemon:
    """Start real daemon/control handlers on an ephemeral IPv4 loopback port."""

    settings = load_local_settings(config_path)
    capability = load_agent_capability(settings.app.agent_capability_file)
    executed: list[int] = []
    completed = threading.Event()
    holder: dict[str, ForegroundDaemon] = {}

    def execute(request: RunRequestRecord) -> Mapping[str, object]:
        executed.append(request.request_id)
        try:
            return cast(
                Mapping[str, object],
                cli._execute_daemon_request(
                    settings,
                    request,
                    daemon=holder["daemon"],
                    environ=environ,
                    adapter_factory=registry.factory,
                    identity_verifier=lambda *_args: None,
                    clock=clock.now,
                ),
            )
        finally:
            completed.set()

    control = ControlApplication(
        settings.app.state_directory / "post_pulsar.sqlite3",
        settings.app.agent_capability_file,
        operator_verifier_file=settings.app.operator_verifier_file,
        allow_agent_publish=settings.app.allow_agent_publish,
        max_body_bytes=settings.app.control_max_body_bytes,
        max_results=settings.app.control_max_results,
        confirmation_ttl_seconds=settings.app.confirmation_ttl_seconds,
    )
    daemon = ForegroundDaemon(
        settings.app.state_directory,
        settings.app.endpoint_record_file,
        "127.0.0.1",
        0,
        control_application=control,
        agent_capability_file=settings.app.agent_capability_file,
        request_executor=execute,
        poll_seconds=0.01,
    )
    holder["daemon"] = daemon
    daemon.start()
    return RunningFakeDaemon(daemon, control, capability, executed, completed)


__all__ = [
    "AdapterPlan",
    "AdapterTrace",
    "ClassifiedFailure",
    "FakeAdapterRegistry",
    "FakeClock",
    "InjectedCrash",
    "LoopbackSocketGuard",
    "RunningFakeDaemon",
    "launch_fake_daemon",
]
