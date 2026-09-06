"""Control-plane CLI and secretless bootstrap contracts."""

from __future__ import annotations

import json
import os
from datetime import UTC, datetime, timedelta
from io import StringIO
from pathlib import Path

import pytest

from post_pulsar.bootstrap import BootstrapError, BootstrapRecord, write_bootstrap
from post_pulsar.cli import (
    EXIT_AUTH,
    EXIT_CONFLICT,
    EXIT_OK,
    EXIT_USAGE,
    main,
)
from post_pulsar.control import ControlApplication, ControlRequest, ControlResponse
from post_pulsar.daemon import EndpointRecord
from post_pulsar.state import ProfileTargetSnapshot, StateRepository


def _write_config(root: Path, *, allow_agent_publish: bool = False) -> Path:
    path = root / "post-pulsar.toml"
    path.write_text(
        f"""
[app]
state_directory = "state"
log_file = "state/post_pulsar.log"
allow_agent_publish = {str(allow_agent_publish).lower()}
agent_capability_file = "state/control/agent-capability"
operator_verifier_file = "state/control/operator-verifier"
bootstrap_file = "state/control/bootstrap.json"
endpoint_record_file = "state/control/endpoint.json"

[[profiles]]
profile_id = "operator"
account_root = "accounts/operator"
timezone = "UTC"

[profiles.x]
enabled = false
expected_remote_user_id = "10001"
expected_username = "operator"
token_env_var = "POST_PULSAR_X_OPERATOR_USER_ACCESS_TOKEN"
request_timeout_seconds = 30
processing_timeout_seconds = 300
chunk_size_bytes = 4194304
""".strip()
        + "\n",
        encoding="utf-8",
    )
    return path


def _setup(root: Path) -> tuple[Path, Path]:
    config = _write_config(root)
    account = root / "accounts/operator"
    account.mkdir(parents=True)
    database = root / "state/post_pulsar.sqlite3"
    with StateRepository(database) as repository:
        repository.register_profile(
            "operator",
            account,
            (
                ProfileTargetSnapshot(
                    "x",
                    "10001",
                    "operator",
                    "POST_PULSAR_X_OPERATOR_USER_ACCESS_TOKEN",
                    {
                        "request_timeout_seconds": 30.0,
                        "processing_timeout_seconds": 300.0,
                        "chunk_size_bytes": 4194304,
                    },
                ),
            ),
            config_hash="1" * 64,
        )
    return config, database


def _live_control(root: Path, database: Path, *, allow_agent_publish: bool = False):
    capability = root / "state/control/agent-capability"
    capability.parent.mkdir(parents=True, exist_ok=True)
    capability.write_text("c" * 64 + "\n", encoding="ascii")
    capability.chmod(0o600)
    EndpointRecord(
        protocol="post-pulsar.control/v1",
        address="127.0.0.1:8765",
        pid=os.getpid(),
        process_started_at=1,
        startup_nonce="d" * 64,
        capabilities={"control_api_major": 1},
    ).write(root / "state/control/endpoint.json")
    application = ControlApplication(
        database, capability, allow_agent_publish=allow_agent_publish
    )

    def transport(
        _endpoint: EndpointRecord, request: ControlRequest
    ) -> ControlResponse:
        return application.handle(request)

    return transport


def _invoke(arguments: list[str], **kwargs: object) -> tuple[int, str, str]:
    output = StringIO()
    errors = StringIO()
    code = main(
        arguments,
        stdout=output,
        stderr=errors,
        environ={},
        **kwargs,  # type: ignore[arg-type]
    )
    return code, output.getvalue(), errors.getvalue()


class _TTY(StringIO):
    def isatty(self) -> bool:
        return True


def test_agent_capability_initialize_rotate_revoke_is_redacted_and_bootstraps(
    tmp_path: Path,
) -> None:
    config = _write_config(tmp_path)
    common = [
        "control",
        "agent-capability",
        "initialize",
        "--config",
        str(config),
        "--installation-id",
        "a" * 32,
        "--service-mode",
        "manual",
        "--service-identifier",
        "post-pulsar",
        "--json",
    ]

    code, output, error = _invoke(common)

    capability = tmp_path / "state/control/agent-capability"
    bootstrap = tmp_path / "state/control/bootstrap.json"
    secret = capability.read_text(encoding="ascii").strip()
    assert code == EXIT_OK and error == ""
    assert len(secret) == 64 and secret not in output
    assert capability.stat().st_mode & 0o077 == 0
    assert bootstrap.stat().st_mode & 0o077 == 0
    document = json.loads(bootstrap.read_text(encoding="utf-8"))
    assert document == {
        "schema": "post-pulsar.bootstrap/v1",
        "installation_id": "a" * 32,
        "endpoint_record": str(tmp_path / "state/control/endpoint.json"),
        "agent_capability": str(capability),
        "service": {"mode": "manual", "identifier": "post-pulsar"},
    }
    old = secret
    code, output, error = _invoke(
        [
            "control",
            "agent-capability",
            "rotate",
            "--config",
            str(config),
            "--json",
        ]
    )
    assert code == EXIT_OK and error == "" and old not in output
    assert capability.read_text(encoding="ascii").strip() != old
    code, output, error = _invoke(
        [
            "control",
            "agent-capability",
            "revoke",
            "--config",
            str(config),
            "--confirm",
            "REVOKE",
            "--json",
        ]
    )
    assert code == EXIT_OK and error == "" and not capability.exists()
    assert "secret" not in output.casefold()


def test_operator_secret_requires_tty_and_stores_only_memory_hard_verifier(
    tmp_path: Path,
) -> None:
    config = _write_config(tmp_path)
    arguments = [
        "control",
        "operator-secret",
        "initialize",
        "--config",
        str(config),
        "--json",
    ]
    code, _output, error = _invoke(arguments, stdin=StringIO("not-accepted\n"))
    assert code == EXIT_AUTH and "TTY" in error

    supplied = "correct horse battery staple"
    code, output, error = _invoke(arguments, stdin=_TTY(supplied + "\n"))
    verifier = tmp_path / "state/control/operator-verifier"
    stored = verifier.read_text(encoding="utf-8")
    assert code == EXIT_OK and error == ""
    assert supplied not in output and supplied not in stored
    assert json.loads(stored)["kdf"] == "scrypt"
    assert verifier.stat().st_mode & 0o077 == 0


def test_bootstrap_rejects_arbitrary_service_commands_and_symlink_targets(
    tmp_path: Path,
) -> None:
    path = tmp_path / "bootstrap.json"
    with pytest.raises(BootstrapError, match="service"):
        write_bootstrap(
            path,
            {
                "schema": "post-pulsar.bootstrap/v1",
                "installation_id": "b" * 32,
                "endpoint_record": str(tmp_path / "endpoint.json"),
                "agent_capability": str(tmp_path / "capability"),
                "service": {
                    "mode": "manual",
                    "identifier": "post-pulsar",
                    "arguments": ["--unsafe"],
                },
            },
        )
    target = tmp_path / "real.json"
    target.write_text("{}", encoding="utf-8")
    path.symlink_to(target)
    with pytest.raises(BootstrapError, match="unsafe"):
        write_bootstrap(
            path,
            BootstrapRecord(
                installation_id="b" * 32,
                endpoint_record=tmp_path / "endpoint.json",
                agent_capability=tmp_path / "capability",
                service_mode="manual",
                service_identifier="post-pulsar",
            ),
        )


def test_schedule_listing_is_profile_explicit_bounded_and_read_only(
    tmp_path: Path,
) -> None:
    config, database = _setup(tmp_path)
    with StateRepository.open_existing(database) as repository:
        repository.create_schedule(
            profile_id="operator",
            schedule_id="morning",
            bucket="QUEUE",
            timezone="UTC",
            weekdays=(0, 2, 4),
            local_time="09:30",
            misfire_grace_seconds=300,
            enabled=False,
        )
    before = database.stat()

    code, output, error = _invoke(
        [
            "schedules",
            "list",
            "--config",
            str(config),
            "--profile",
            "operator",
            "--limit",
            "1",
            "--json",
        ]
    )

    result = json.loads(output)["result"]
    after = database.stat()
    assert code == EXIT_OK and error == ""
    assert result["items"][0]["schedule_id"] == "morning"
    assert result["items"][0]["profile_id"] == "operator"
    assert (after.st_size, after.st_mtime_ns) == (before.st_size, before.st_mtime_ns)


def test_pause_falls_back_to_a_locked_local_durable_request(tmp_path: Path) -> None:
    config, database = _setup(tmp_path)

    code, output, error = _invoke(
        [
            "pause",
            "--config",
            str(config),
            "--profile",
            "operator",
            "--expected-revision",
            "1",
            "--idempotency-key",
            "pause-cli-1",
            "--json",
        ]
    )

    request = json.loads(output)["result"]["request"]
    assert code == EXIT_OK and error == ""
    assert request["action"] == "pause" and request["status"] == "queued"
    with StateRepository.open_existing(database) as repository:
        stored = repository.get_run_request(request["request_id"])
        assert stored.profile_id == "operator"


@pytest.mark.parametrize(
    ("status", "expected"),
    [(401, EXIT_AUTH), (409, EXIT_CONFLICT)],
)
def test_live_control_outcomes_map_to_stable_exit_codes(
    tmp_path: Path, status: int, expected: int
) -> None:
    config, _database = _setup(tmp_path)
    capability = tmp_path / "state/control/agent-capability"
    capability.parent.mkdir(parents=True, exist_ok=True)
    capability.write_text("c" * 64 + "\n", encoding="ascii")
    capability.chmod(0o600)
    endpoint = EndpointRecord(
        protocol="post-pulsar.control/v1",
        address="127.0.0.1:8765",
        pid=os.getpid(),
        process_started_at=1,
        startup_nonce="d" * 64,
        capabilities={"control_api_major": 1},
    )
    endpoint.write(tmp_path / "state/control/endpoint.json")

    def transport(
        _endpoint: EndpointRecord, request: ControlRequest
    ) -> ControlResponse:
        assert request.target == "/control/v1/pause"
        return ControlResponse(
            status,
            {"Content-Type": "application/json"},
            json.dumps(
                {
                    "schema": "post-pulsar.control/v1",
                    "ok": False,
                    "error": {"code": "remote_error", "message": "remote error"},
                    "request_id": "e" * 16,
                }
            ).encode(),
        )

    code, output, error = _invoke(
        [
            "pause",
            "--config",
            str(config),
            "--profile",
            "operator",
            "--expected-revision",
            "1",
            "--idempotency-key",
            "pause-cli-2",
            "--json",
        ],
        control_transport=transport,
    )

    assert code == expected and output == ""
    assert json.loads(error)["error"]["code"] == "remote_error"


def test_parser_rejects_unbounded_or_implicit_profile_arguments(tmp_path: Path) -> None:
    config = _write_config(tmp_path)
    code, _output, _error = _invoke(
        ["schedules", "list", "--config", str(config), "--limit", "501"]
    )
    assert code == EXIT_USAGE


def test_live_daemon_accepts_declared_keyed_schedule_patch(tmp_path: Path) -> None:
    """Dependency contract: T4.5 must route its declared PATCH operation."""
    config, database = _setup(tmp_path)
    with StateRepository.open_existing(database) as repository:
        schedule = repository.create_schedule(
            profile_id="operator",
            schedule_id="morning",
            bucket="QUEUE",
            timezone="UTC",
            weekdays=(0,),
            local_time="09:30",
            misfire_grace_seconds=300,
            enabled=False,
        )
    code, output, error = _invoke(
        [
            "schedules",
            "update",
            "--config",
            str(config),
            "--profile",
            "operator",
            "--schedule-key",
            str(schedule.schedule_key),
            "--schedule-id",
            "morning",
            "--bucket",
            "QUEUE",
            "--timezone",
            "UTC",
            "--weekdays",
            "0,2",
            "--local-time",
            "09:45",
            "--misfire-grace-seconds",
            "300",
            "--disabled",
            "--expected-revision",
            str(schedule.revision),
            "--idempotency-key",
            "schedule-update-1",
            "--json",
        ],
        control_transport=_live_control(tmp_path, database),
    )
    assert code == EXIT_OK, error
    assert json.loads(output)["result"]["request"]["action"] == "schedule_update"


def test_operator_approved_intent_consumes_when_agent_publish_is_disabled(
    tmp_path: Path,
) -> None:
    """Dependency contract: operator authorization is distinct from agent opt-in."""
    config, database = _setup(tmp_path)
    with StateRepository.open_existing(database) as repository:
        schedule = repository.create_schedule(
            profile_id="operator",
            schedule_id="morning",
            bucket="QUEUE",
            timezone="UTC",
            weekdays=(0,),
            local_time="09:30",
            misfire_grace_seconds=300,
            enabled=False,
        )
        intent = repository.create_confirmation_intent(
            action="schedule_enable",
            arguments={},
            profile_id="operator",
            resource_revision=schedule.revision,
            fingerprint=schedule.config_hash,
            consequence="Enable this exact schedule.",
            expires_at=datetime.now(UTC) + timedelta(minutes=5),
            schedule_key=schedule.schedule_key,
        )
        repository.approve_confirmation_intent(
            intent.intent_id, expected_revision=intent.revision
        )
    code, output, error = _invoke(
        [
            "schedules",
            "enable",
            "--config",
            str(config),
            "--profile",
            "operator",
            "--schedule-key",
            str(schedule.schedule_key),
            "--expected-revision",
            str(schedule.revision),
            "--idempotency-key",
            "schedule-enable-1",
            "--intent-id",
            intent.intent_id,
            "--json",
        ],
        control_transport=_live_control(tmp_path, database, allow_agent_publish=False),
    )
    assert code == EXIT_OK, error
    assert json.loads(output)["result"]["request"]["intent_id"] == intent.intent_id
