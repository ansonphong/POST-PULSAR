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
    EXIT_DAEMON_UNAVAILABLE,
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


@pytest.mark.parametrize("matching", [True, False])
def test_shutdown_uses_authenticated_http_and_never_signals(
    tmp_path: Path, monkeypatch, matching: bool
):
    from post_pulsar import cli

    config, database = _setup(tmp_path)
    _live_control(tmp_path, database)
    monkeypatch.setattr(cli, "_endpoint_process_is_live", lambda endpoint: matching)

    def forbidden(*args):
        pytest.fail("shutdown must never send a signal or process probe")

    monkeypatch.setattr(cli.os, "kill", forbidden)
    requests = []

    def transport(endpoint, request):
        requests.append(request)
        assert request.method == "POST"
        assert request.target == "/control/v1/shutdown"
        assert request.headers["Authorization"] == "Bearer " + "c" * 64
        assert json.loads(request.body) == {"startup_nonce": endpoint.startup_nonce}
        return ControlResponse(
            202,
            {},
            json.dumps(
                {
                    "schema": "post-pulsar.control/v1",
                    "ok": True,
                    "data": {"status": "stopping"},
                }
            ).encode(),
        )

    code, _, _ = _invoke(
        [
            "shutdown",
            "--profile",
            "operator",
            "--confirm",
            "SHUTDOWN",
            "--config",
            str(config),
            "--json",
        ],
        control_transport=transport,
    )
    assert code == (EXIT_OK if matching else EXIT_DAEMON_UNAVAILABLE)
    assert len(requests) == int(matching)


class _TTY(StringIO):
    def isatty(self) -> bool:
        return True


def test_daemon_reconcile_drives_exact_bundle_after_unlock(
    tmp_path: Path, monkeypatch
) -> None:
    from post_pulsar import cli
    from post_pulsar.config import load_local_settings
    from post_pulsar.daemon import ForegroundDaemon
    from post_pulsar.state import BundleFileSnapshot, RunRequestRecord

    config, database = _setup(tmp_path)
    settings = load_local_settings(config)
    with StateRepository.open_existing(database) as repository:
        key = repository.add_bundle(
            profile_id="operator",
            bundle_id="post",
            fingerprint="b" * 64,
            source_bucket="QUEUE",
            files=(
                BundleFileSnapshot(
                    "post.jpg", "image", None, "image", "image/jpeg", 1, "a" * 64
                ),
            ),
            targets=(
                cli._configured_target_snapshot(settings.profile("operator"), "x"),
            ),
        )
        delivery = repository.claim_delivery(key, "x", "unknown")
        repository.advance_delivery_phase(
            key,
            "x",
            "final_dispatch_started",
            claim_token="unknown",
            attempt_count=delivery.attempt_count,
        )
        repository.mark_delivery_ambiguous(
            key,
            "x",
            error_code="unknown",
            error_message="Unknown.",
            claim_token="unknown",
            attempt_count=delivery.attempt_count,
        )
        revision = repository.get_bundle(key).revision
    request = RunRequestRecord(
        1,
        "operator",
        "reconcile",
        {
            "bundle_id": "post",
            "fingerprint": "b" * 64,
            "platform": "x",
            "published_remote_id": "remote",
        },
        "reconcile",
        revision,
        key,
        None,
        None,
        "claimed",
        None,
        1,
    )
    daemon = ForegroundDaemon(
        settings.app.state_directory, settings.app.endpoint_record_file, "127.0.0.1", 0
    )
    calls = []

    class Application:
        def __init__(self, *args, **kwargs):
            self.kwargs = kwargs

        def run_once(self, exact):
            with daemon._locks.acquire_profiles(daemon._lease, ("operator",)):
                calls.append(exact)
            return cli.RunOutcome("archived", bundle_key=key, bundle_id="post")

    monkeypatch.setattr(cli, "OneRunApplication", Application)
    with daemon._locks.acquire_instance() as lease:
        daemon._lease = lease
        cli._execute_daemon_request(
            settings,
            request,
            daemon=daemon,
            environ={},
            adapter_factory=None,
            identity_verifier=lambda *a: None,
            clock=lambda: datetime.now(UTC),
        )
    assert len(calls) == 1
    assert (
        calls[0].expected_bundle_key == key
        and calls[0].expected_fingerprint == "b" * 64
    )


def test_draft_edit_failure_keeps_original_bytes(tmp_path: Path, monkeypatch) -> None:
    from PIL import Image
    from post_pulsar import cli
    from post_pulsar.config import load_local_settings
    from post_pulsar.content import scan_inbox
    from post_pulsar.state import RunRequestRecord

    config, _ = _setup(tmp_path)
    drafts = tmp_path / "accounts/operator/DRAFTS"
    drafts.mkdir()
    Image.new("RGB", (8, 8)).save(drafts / "post.jpg")
    caption = drafts / "post.txt"
    caption.write_text("original")
    fingerprint = scan_inbox(drafts).bundles[0].fingerprint
    request = RunRequestRecord(
        1,
        "operator",
        "edit_caption",
        {
            "bundle_id": "post",
            "bucket": "DRAFTS",
            "fingerprint": fingerprint,
            "text": "replacement",
        },
        "edit",
        1,
        None,
        None,
        None,
        "claimed",
        None,
        1,
    )

    def fail_replace(*args):
        raise OSError("injected atomic install failure")

    monkeypatch.setattr(cli.os, "replace", fail_replace)
    with pytest.raises(OSError, match="atomic install"):
        cli._edit_draft_text(load_local_settings(config), request)
    assert caption.read_text() == "original"


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


def test_hardened_lifecycle_threads_discovery_policy_and_keeps_operator_private(
    tmp_path, monkeypatch
):
    import grp
    import pwd
    from types import SimpleNamespace
    from post_pulsar import secure_files
    from post_pulsar.bootstrap import load_bootstrap
    from post_pulsar.config import load_local_settings
    from post_pulsar.control import load_agent_capability, verify_operator_secret

    core, agent, group = os.getuid(), os.getuid() + 12345, os.getgid()
    accounts = [
        SimpleNamespace(pw_uid=uid, pw_name=name, pw_gid=group)
        for uid, name in ((core, "fixture-core"), (agent, "fixture-agent"))
    ]
    monkeypatch.setattr(pwd, "getpwall", lambda: accounts)
    monkeypatch.setattr(
        pwd, "getpwuid", lambda uid: next(a for a in accounts if a.pw_uid == uid)
    )
    monkeypatch.setattr(
        grp,
        "getgrgid",
        lambda gid: SimpleNamespace(gr_gid=gid, gr_mem=["fixture-agent"]),
    )
    monkeypatch.setattr(os, "getgrouplist", lambda name, gid: [group])
    monkeypatch.setattr(os, "getgroups", lambda: [group])
    monkeypatch.setattr(os, "getegid", lambda: group)
    monkeypatch.setattr(secure_files, "_validate_ancestors", lambda path, policy: None)
    config, database = _setup(tmp_path)
    content = config.read_text().replace(
        "[app]",
        f'[app]\ndeployment_mode = "hardened"\nhardened_core_principal = "{core}"\nhardened_agent_principal = "{agent}"\nhardened_agent_group = {group}',
    )
    for filename in ("agent-capability", "bootstrap.json", "endpoint.json"):
        content = content.replace("state/control/" + filename, "discovery/" + filename)
    config.write_text(content)
    policy = load_local_settings(config).app.record_policy
    args = [
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
    code, output, errors = _invoke(args)
    assert code == EXIT_OK and not errors
    bootstrap = load_bootstrap(tmp_path / "discovery/bootstrap.json", policy=policy)
    first = load_agent_capability(bootstrap.agent_capability, policy=policy)
    assert first not in output
    code, output, errors = _invoke(
        ["control", "agent-capability", "rotate", "--config", str(config), "--json"]
    )
    second = load_agent_capability(bootstrap.agent_capability, policy=policy)
    assert code == EXIT_OK and not errors and second != first and second not in output
    code, output, errors = _invoke(
        ["control", "operator-secret", "initialize", "--config", str(config), "--json"],
        stdin=_TTY("fixture operator phrase\n"),
    )
    assert code == EXIT_OK and not errors and "fixture operator phrase" not in output
    verifier = tmp_path / "state/control/operator-verifier"
    assert verify_operator_secret(verifier, "fixture operator phrase", policy=policy)
    assert verifier.stat().st_mode & 0o7777 == 0o600
    assert verifier.parent.stat().st_mode & 0o7777 == 0o700
    endpoint = EndpointRecord(
        "post-pulsar.control/v1", "127.0.0.1:8765", os.getpid(), 1, "fixture-nonce", {}
    )
    endpoint.write(bootstrap.endpoint_record, policy=policy)
    assert EndpointRecord.read(bootstrap.endpoint_record, policy=policy) == endpoint
    for path in (
        tmp_path / "discovery/bootstrap.json",
        bootstrap.agent_capability,
        bootstrap.endpoint_record,
    ):
        assert path.stat().st_mode & 0o7777 == 0o640
    application = ControlApplication(
        database, bootstrap.agent_capability, record_policy=policy
    )
    assert (
        application.handle(
            ControlRequest(
                "GET", "/control/v1/health", {"Authorization": "Bearer " + second}
            )
        ).status
        == 200
    )
    monkeypatch.setattr(os, "getuid", lambda: agent)
    monkeypatch.setattr(os, "geteuid", lambda: agent)
    assert (
        load_bootstrap(tmp_path / "discovery/bootstrap.json", policy=policy)
        == bootstrap
    )
    assert load_agent_capability(bootstrap.agent_capability, policy=policy) == second
    assert EndpointRecord.read(bootstrap.endpoint_record, policy=policy) == endpoint
    with pytest.raises(secure_files.SecureFileError):
        secure_files.assert_owner_file(verifier, policy=policy, agent_read=False)


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


def test_foreground_daemon_receives_recovery_and_periodic_schedule_callbacks(
    tmp_path: Path,
) -> None:
    config, _database = _setup(tmp_path)
    capability = tmp_path / "state/control/agent-capability"
    capability.parent.mkdir(parents=True, exist_ok=True)
    capability.write_text("c" * 64 + "\n", encoding="ascii")
    capability.chmod(0o600)
    captured: dict[str, object] = {}

    class Daemon:
        def __init__(self, *args: object, **kwargs: object) -> None:
            del args
            captured.update(kwargs)

        def run_forever(self) -> None:
            return None

    code, _output, error = _invoke(
        ["daemon", "foreground", "--config", str(config), "--json"],
        daemon_factory=Daemon,
    )
    assert code == EXIT_OK, error
    assert callable(captured["recovery"])
    assert callable(captured["schedule_admission"])
    assert callable(captured["request_executor"])
