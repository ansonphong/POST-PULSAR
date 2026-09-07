"""Installation and discovery identity must bind to one daemon incarnation."""

from __future__ import annotations

import json
from dataclasses import asdict, replace
from pathlib import Path

import pytest

from post_pulsar.control import (
    ControlApplication,
    ControlRequest,
    ControlSecurityError,
    rotate_agent_capability,
)
from post_pulsar.control_identity import DaemonIdentity, DiscoveryPaths
from post_pulsar.daemon import EndpointRecord, ForegroundDaemon
from post_pulsar.state import StateRepository


def test_daemon_binds_endpoint_and_authenticated_capabilities(tmp_path: Path) -> None:
    from tests.unit.test_daemon import _Server

    database = tmp_path / "state/post_pulsar.sqlite3"
    StateRepository(database).close()
    capability = tmp_path / "control/agent"
    token = rotate_agent_capability(capability)
    app = ControlApplication(database, capability)
    daemon = ForegroundDaemon(
        database.parent,
        tmp_path / "control/endpoint.json",
        "127.0.0.1",
        0,
        installation_id="a" * 32,
        bootstrap_record_file=tmp_path / "control/bootstrap.json",
        agent_capability_file=capability,
        control_application=app,
        server_factory=lambda *args: _Server(*args),
    )
    daemon.start()
    try:
        endpoint = EndpointRecord.read(tmp_path / "control/endpoint.json")
        request = ControlRequest("GET", "/control/v1/capabilities", {})
        assert app.handle(request).status == 401
        response = app.handle(
            replace(request, headers={"Authorization": f"Bearer {token}"})
        )
        assert response.status == 200
        body = json.loads(response.body)["data"]
        identity_fields = (
            "installation_id",
            "pid",
            "process_started_at",
            "startup_nonce",
            "discovery",
        )
        assert {key: body[key] for key in identity_fields} == {
            key: asdict(endpoint)[key] for key in identity_fields
        }
        assert body["installation_id"] == "a" * 32
        assert body["discovery"] == {
            "bootstrap_record": str((tmp_path / "control/bootstrap.json").resolve()),
            "endpoint_record": str((tmp_path / "control/endpoint.json").resolve()),
            "agent_capability_file": str(capability.resolve()),
        }
        assert token.encode() not in response.body
        assert str(database).encode() not in response.body
        original = asdict(endpoint)
        for key in identity_fields:
            invalid = {**original}
            del invalid[key]
            (tmp_path / "control/endpoint.json").write_text(json.dumps(invalid))
            with pytest.raises(RuntimeError, match="schema"):
                EndpointRecord.read(tmp_path / "control/endpoint.json")
        endpoint.write(tmp_path / "control/endpoint.json")
    finally:
        daemon.stop()


@pytest.mark.parametrize("identity", [None, True, "", "A" * 32, "a" * 31, "a" * 33])
def test_daemon_rejects_invalid_installation_id(
    tmp_path: Path, identity: object
) -> None:
    with pytest.raises(ValueError, match="installation"):
        ForegroundDaemon(
            tmp_path / "state",
            tmp_path / "endpoint",
            "127.0.0.1",
            0,
            installation_id=identity,
            bootstrap_record_file=tmp_path / "bootstrap",
            agent_capability_file=tmp_path / "agent",
        )


def test_unbound_control_cannot_claim_an_incarnation(tmp_path: Path) -> None:
    StateRepository(tmp_path / "state.sqlite3").close()
    token = rotate_agent_capability(tmp_path / "agent")
    app = ControlApplication(tmp_path / "state.sqlite3", tmp_path / "agent")
    response = app.handle(
        ControlRequest(
            "GET", "/control/v1/capabilities", {"Authorization": f"Bearer {token}"}
        )
    )
    assert response.status == 409
    assert json.loads(response.body)["ok"] is False


@pytest.mark.parametrize(
    "field,bad",
    [
        ("installation_id", None),
        ("installation_id", "A" * 32),
        ("pid", True),
        ("process_started_at", 0),
        ("startup_nonce", "b" * 63),
        ("discovery", {}),
        ("capabilities", {}),
        ("capabilities", {"control_api_major": True}),
        ("capabilities", {"control_api_major": 1, "extra": "bad"}),
    ],
)
def test_endpoint_reader_rejects_invalid_identity_and_nested_fields(
    tmp_path: Path, field: str, bad: object
) -> None:
    record = EndpointRecord(
        "post-pulsar.control/v1",
        "127.0.0.1:8765",
        1234,
        123456,
        "b" * 64,
        {"control_api_major": 1},
        "a" * 32,
        DiscoveryPaths.from_paths(
            tmp_path / "bootstrap", tmp_path / "endpoint", tmp_path / "agent"
        ),
    )
    record.write(tmp_path / "endpoint")
    document = {**asdict(record), field: bad}
    (tmp_path / "endpoint").write_text(json.dumps(document))
    with pytest.raises(RuntimeError, match="schema"):
        EndpointRecord.read(tmp_path / "endpoint")


def test_control_binding_is_single_use_and_capability_path_exact(
    tmp_path: Path,
) -> None:
    app = ControlApplication(tmp_path / "database", tmp_path / "agent")
    discovery = DiscoveryPaths.from_paths(
        tmp_path / "bootstrap", tmp_path / "endpoint", tmp_path / "other-agent"
    )
    identity = DaemonIdentity("a" * 32, 1234, 123456, "b" * 64, discovery)
    with pytest.raises(ControlSecurityError, match="capability"):
        app.bind_daemon(lambda: None, identity)
    identity = replace(
        identity,
        discovery=replace(discovery, agent_capability_file=str(tmp_path / "agent")),
    )
    app.bind_daemon(lambda: None, identity)
    with pytest.raises(ControlSecurityError, match="incarnation"):
        app.bind_daemon(lambda: None, replace(identity, startup_nonce="c" * 64))


@pytest.mark.parametrize("invalid", ["relative", "../agent", "agent/../agent"])
def test_discovery_paths_never_rebase_untrusted_locations(
    tmp_path: Path, invalid: str
) -> None:
    with pytest.raises(ValueError, match="canonical"):
        DiscoveryPaths(str(tmp_path / "bootstrap"), str(tmp_path / "endpoint"), invalid)
    paths = DiscoveryPaths.from_paths(
        tmp_path / "bootstrap", tmp_path / "endpoint", tmp_path / "agent"
    )
    with pytest.raises(ValueError, match="schema"):
        DiscoveryPaths.read({**asdict(paths), "extra": "bad"})
