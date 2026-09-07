"""Deterministic local command interface for POST PULSAR."""

from __future__ import annotations

import argparse
import getpass as tty_prompt
import hashlib
import ipaddress
import json
import logging
import os
import re
import stat
import sys
import tempfile
import urllib.error
import urllib.request
from collections.abc import Callable, Mapping, Sequence
from contextlib import redirect_stderr, redirect_stdout, suppress
from dataclasses import asdict
from datetime import UTC, datetime
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import IO, TYPE_CHECKING, Final, Literal, Never, Protocol, cast

from post_pulsar.app import (
    AdapterFactory,
    OneRunApplication,
    RunOnceRequest,
    RunOutcome,
    _configured_target_snapshot,
    _default_adapter_factory,
)
from post_pulsar.bootstrap import (
    SERVICE_MODES,
    BootstrapError,
    BootstrapRecord,
    write_bootstrap,
)
from post_pulsar.config import (
    ConfigurationError,
    LocalSettings,
    SecretValue,
    load_local_settings,
    validate_publishing_credentials,
)
from post_pulsar.locking import LockContentionError, LockError, LockManager
from post_pulsar.migration import MigrationError, migrate_legacy_layout
from post_pulsar.platforms.base import (
    AdapterContractError,
    PlatformAdapter,
    PublicationSnapshot,
)
from post_pulsar.state import (
    BundleRecord,
    ConflictError,
    DeliveryRecord,
    Platform,
    RunRequestRecord,
    ScheduleRecord,
    StateError,
    StateRepository,
    TargetSnapshot,
    TransitionError,
)

if TYPE_CHECKING:
    from post_pulsar.control import ControlRequest, ControlResponse
    from post_pulsar.daemon import EndpointRecord, ForegroundDaemon

CONTROL_API_MAJOR: Final = 1
CONTROL_SCHEMA: Final = "post-pulsar.control/v1"
EXIT_OK: Final = 0
EXIT_USAGE: Final = 2
EXIT_NO_WORK: Final = 3
EXIT_INVALID: Final = 4
EXIT_BLOCKED: Final = 5
EXIT_CONTENTION: Final = 6
EXIT_RETRYABLE: Final = 7
EXIT_DAEMON_UNAVAILABLE: Final = 8
EXIT_VERSION: Final = 9
EXIT_AUTH: Final = 10
EXIT_CONFLICT: Final = 11
EXIT_APPROVAL_REQUIRED: Final = 12
_COMMANDS: Final = frozenset(
    {
        "run",
        "status",
        "retry",
        "reconcile",
        "profiles",
        "schedules",
        "requests",
        "run-now",
        "cancel-pending",
        "delete-pending",
        "pause",
        "resume",
        "shutdown",
        "daemon",
        "migrate",
        "control",
        "confirmations",
    }
)
_VERSION: Final = "0.1.0"
_IDENTIFIER_RE: Final = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.:-]{0,127}\Z")

IdentityVerifier = Callable[[PublicationSnapshot, SecretValue, Path], None]


class ControlTransport(Protocol):
    def __call__(
        self, endpoint: EndpointRecord, request: ControlRequest
    ) -> ControlResponse: ...


class DaemonFactory(Protocol):
    def __call__(self, *args: object, **kwargs: object) -> ForegroundDaemon: ...


class _UsageError(ValueError):
    """Argument parsing failed without allowing argparse to exit the process."""


class _RemoteControlError(RuntimeError):
    def __init__(self, status: int, code: str, message: str) -> None:
        super().__init__(message)
        self.status = status
        self.code = code


class _Parser(argparse.ArgumentParser):
    def error(self, message: str) -> Never:
        raise _UsageError(message)


def build_parser() -> argparse.ArgumentParser:
    """Build the stable public argument grammar."""
    parser = _Parser(prog="post-pulsar", description="Safe social publishing")
    parser.add_argument("--config", default="post-pulsar.toml", metavar="PATH")
    parser.add_argument("--json", action="store_true", dest="json_output")
    parser.add_argument("--version", action="version", version=f"%(prog)s {_VERSION}")
    commands = parser.add_subparsers(dest="command")

    run = commands.add_parser("run", help="publish or resume one bundle")
    run.add_argument("--profile", required=True)
    run.add_argument("--bucket", required=True, choices=("QUEUE", "RANDOM", "REELS"))
    run.add_argument("--trigger-id", required=True)

    status = commands.add_parser("status", help="read durable local state")
    status.add_argument("--profile", required=True, type=_profile_id)
    status.add_argument("--bundle-key", type=_positive_integer)

    retry = commands.add_parser("retry", help="enqueue one exact approved retry")
    _add_confirmed_delivery_target(retry)

    reconcile = commands.add_parser(
        "reconcile", help="resolve one ambiguous delivery from operator evidence"
    )
    _add_confirmed_delivery_target(reconcile)
    resolution = reconcile.add_mutually_exclusive_group(required=True)
    resolution.add_argument("--published", metavar="REMOTE_ID")
    resolution.add_argument("--not-published", action="store_true")

    profiles = commands.add_parser("profiles", help="list configured profile IDs")
    profile_commands = profiles.add_subparsers(dest="profiles_command", required=True)
    profiles_list = profile_commands.add_parser("list", help="list profiles")
    profiles_list.add_argument("--limit", type=_page_limit, default=100)

    schedules = commands.add_parser("schedules", help="inspect or mutate schedules")
    schedule_commands = schedules.add_subparsers(
        dest="schedules_command", required=True
    )
    schedules_list = schedule_commands.add_parser("list", help="list schedules")
    schedules_list.add_argument("--profile", required=True, type=_profile_id)
    schedules_list.add_argument("--limit", type=_page_limit, default=100)
    schedules_list.add_argument("--cursor", type=_nonnegative_integer, default=0)
    schedules_create = schedule_commands.add_parser(
        "create", help="enqueue a schedule creation"
    )
    _add_schedule_definition(schedules_create, include_schedule_key=False)
    schedules_update = schedule_commands.add_parser(
        "update", help="enqueue an exact schedule update"
    )
    _add_schedule_definition(schedules_update, include_schedule_key=True)
    for action in ("enable", "disable"):
        schedule_toggle = schedule_commands.add_parser(
            action, help=f"enqueue an exact schedule {action}"
        )
        _add_write_identity(schedule_toggle)
        schedule_toggle.add_argument(
            "--schedule-key", required=True, type=_positive_integer
        )
        schedule_toggle.add_argument("--intent-id", type=_intent_id)

    requests = commands.add_parser("requests", help="inspect durable requests")
    request_commands = requests.add_subparsers(dest="requests_command", required=True)
    requests_list = request_commands.add_parser("list", help="list profile requests")
    requests_list.add_argument("--profile", required=True, type=_profile_id)
    requests_list.add_argument("--limit", type=_page_limit, default=100)
    requests_list.add_argument("--cursor", type=_nonnegative_integer, default=0)
    request_status = request_commands.add_parser("status", help="inspect one request")
    request_status.add_argument("--profile", required=True, type=_profile_id)
    request_status.add_argument("--request-id", required=True, type=_positive_integer)

    run_now = commands.add_parser("run-now", help="enqueue one exact approved run")
    _add_write_identity(run_now)
    run_now.add_argument(
        "--bucket", required=True, choices=("QUEUE", "RANDOM", "REELS")
    )
    run_now.add_argument("--bundle-key", required=True, type=_positive_integer)
    run_now.add_argument("--bundle-id", required=True, type=_bundle_id)
    run_now.add_argument("--fingerprint", required=True, type=_sha256)
    run_now.add_argument("--intent-id", required=True, type=_intent_id)

    for command_name, action_label in (
        ("cancel-pending", "cancellation"),
        ("delete-pending", "deletion"),
    ):
        terminal = commands.add_parser(
            command_name,
            help=f"enqueue {action_label} of one exact pristine pending bundle",
        )
        _add_write_identity(terminal)
        terminal.add_argument("--bundle-key", required=True, type=_positive_integer)
        terminal.add_argument("--bundle-id", required=True, type=_bundle_id)
        terminal.add_argument("--fingerprint", required=True, type=_sha256)
        terminal.add_argument("--intent-id", required=True, type=_intent_id)

    pause = commands.add_parser("pause", help="durably pause new publication")
    _add_write_identity(pause)
    resume = commands.add_parser("resume", help="durably resume publication")
    _add_write_identity(resume)
    resume.add_argument("--intent-id", type=_intent_id)

    shutdown = commands.add_parser("shutdown", help="stop the foreground daemon")
    shutdown.add_argument("--profile", required=True, type=_profile_id)
    shutdown.add_argument("--confirm", required=True, choices=("SHUTDOWN",))

    daemon = commands.add_parser("daemon", help="run the local daemon")
    daemon_commands = daemon.add_subparsers(dest="daemon_command", required=True)
    daemon_commands.add_parser("foreground", help="run until SIGINT or SIGTERM")

    migrate = commands.add_parser("migrate", help="migrate one legacy account")
    migration_commands = migrate.add_subparsers(dest="migration_command", required=True)
    for mode in ("dry-run", "apply"):
        migration = migration_commands.add_parser(mode)
        migration.add_argument("--profile", required=True, type=_profile_id)
        migration.add_argument(
            "--legacy-posts", required=True, type=Path, metavar="DIRECTORY"
        )
        if mode == "apply":
            migration.add_argument("--confirm", required=True, choices=("MIGRATE",))

    control = commands.add_parser("control", help="manage local control credentials")
    control_commands = control.add_subparsers(dest="control_command", required=True)
    capability = control_commands.add_parser(
        "agent-capability", help="manage the agent bearer capability"
    )
    capability_commands = capability.add_subparsers(
        dest="capability_command", required=True
    )
    capability_init = capability_commands.add_parser("initialize")
    _add_bootstrap_identity(capability_init)
    capability_commands.add_parser("rotate")
    capability_revoke = capability_commands.add_parser("revoke")
    capability_revoke.add_argument("--confirm", required=True, choices=("REVOKE",))
    operator = control_commands.add_parser(
        "operator-secret", help="manage the operator approval verifier"
    )
    operator_commands = operator.add_subparsers(dest="operator_command", required=True)
    operator_commands.add_parser("initialize")
    operator_commands.add_parser("rotate")

    confirmations = commands.add_parser(
        "confirmations", help="approve an exact confirmation intent"
    )
    confirmation_commands = confirmations.add_subparsers(
        dest="confirmations_command", required=True
    )
    approve = confirmation_commands.add_parser("approve")
    approve.add_argument("--profile", required=True, type=_profile_id)
    approve.add_argument("--intent-id", required=True, type=_intent_id)
    approve.add_argument(
        "--expected-revision", required=True, type=_nonnegative_integer
    )
    approve.add_argument("--idempotency-key", required=True, type=_identifier)
    return parser


def main(
    argv: Sequence[str] | None = None,
    *,
    stdout: IO[str] | None = None,
    stderr: IO[str] | None = None,
    stdin: IO[str] | None = None,
    environ: Mapping[str, str] | None = None,
    adapter_factory: AdapterFactory | None = None,
    identity_verifier: IdentityVerifier | None = None,
    clock: Callable[[], datetime] | None = None,
    control_transport: ControlTransport | None = None,
    daemon_factory: DaemonFactory | None = None,
) -> int:
    """Execute one CLI request and return a stable process exit code."""
    from post_pulsar.control import ControlSecurityError

    output = stdout or sys.stdout
    errors = stderr or sys.stderr
    parser = build_parser()
    normalized = _normalize_argv(tuple(sys.argv[1:] if argv is None else argv))
    try:
        with redirect_stdout(output), redirect_stderr(errors):
            arguments = parser.parse_args(normalized)
    except _UsageError as exc:
        _write_error(errors, "--json" in normalized, "usage_error", str(exc))
        return EXIT_USAGE
    except SystemExit as exc:
        return int(exc.code or 0)

    json_output = bool(arguments.json_output)
    command = cast(str, arguments.command)
    now = clock or (lambda: datetime.now(UTC))
    settings: LocalSettings | None = None
    try:
        settings = load_local_settings(arguments.config)
        logger = None if command == "status" else _configure_logging(settings)
        result, exit_code = _dispatch(
            arguments,
            settings,
            environ=environ,
            adapter_factory=adapter_factory,
            identity_verifier=identity_verifier or _verify_identity,
            clock=now,
            input_stream=stdin or sys.stdin,
            control_transport=control_transport,
            daemon_factory=daemon_factory,
        )
        _write_result(output, json_output, command, result)
        if logger is not None:
            logger.info(
                "command=%s profile=%s exit_code=%d",
                command,
                getattr(arguments, "profile", ""),
                exit_code,
            )
        return exit_code
    except LockContentionError:
        _write_error(
            errors,
            json_output,
            "lock_contended",
            "another POST PULSAR process is active",
        )
        return EXIT_CONTENTION
    except _RemoteControlError as exc:
        _write_error(errors, json_output, exc.code, str(exc))
        if exc.status in {401, 403}:
            return EXIT_AUTH
        if exc.status == 409:
            if exc.code == "incompatible_control_version":
                return EXIT_VERSION
            return EXIT_CONFLICT
        if exc.status == 503:
            return EXIT_DAEMON_UNAVAILABLE
        if exc.status == 428:
            return EXIT_APPROVAL_REQUIRED
        return EXIT_INVALID
    except ControlSecurityError as exc:
        _write_error(errors, json_output, "authentication_required", str(exc))
        return EXIT_AUTH
    except ConflictError as exc:
        _write_error(errors, json_output, "conflict", str(exc))
        return EXIT_CONFLICT
    except TransitionError as exc:
        _write_error(errors, json_output, "approval_required", str(exc))
        return EXIT_APPROVAL_REQUIRED
    except (
        BootstrapError,
        ConfigurationError,
        MigrationError,
        StateError,
        LockError,
        AdapterContractError,
    ) as exc:
        _write_error(
            errors,
            json_output,
            "request_invalid",
            _redact_message(str(exc), settings, environ),
        )
        return EXIT_INVALID
    except OSError:
        _write_error(
            errors, json_output, "local_io_error", "local I/O operation failed"
        )
        return EXIT_INVALID
    except Exception:
        _write_error(
            errors,
            json_output,
            "internal_error",
            "POST PULSAR could not complete the request",
        )
        return EXIT_INVALID


def _dispatch(
    arguments: argparse.Namespace,
    settings: LocalSettings,
    *,
    environ: Mapping[str, str] | None,
    adapter_factory: AdapterFactory | None,
    identity_verifier: IdentityVerifier,
    clock: Callable[[], datetime],
    input_stream: IO[str],
    control_transport: ControlTransport | None,
    daemon_factory: DaemonFactory | None,
) -> tuple[Mapping[str, object], int]:
    command = cast(str, arguments.command)
    if command == "status":
        target = f"/control/v1/status?profile_id={arguments.profile}"
        if arguments.bundle_key is not None:
            target += f"&bundle_key={arguments.bundle_key}"
        remote = _control_call(settings, "GET", target, transport=control_transport)
        if remote is not None:
            return remote, EXIT_OK
        return _status(settings, arguments.profile, arguments.bundle_key), EXIT_OK
    if command == "run":
        outcome = _run(
            settings,
            arguments.profile,
            arguments.bucket,
            arguments.trigger_id,
            environ=environ,
            adapter_factory=adapter_factory,
            clock=clock,
        )
        return _run_result(outcome), _run_exit_code(outcome)
    if command == "retry":
        return _delivery_mutation(
            settings, arguments, transport=control_transport
        ), EXIT_OK
    if command == "reconcile":
        return _delivery_mutation(
            settings, arguments, transport=control_transport
        ), EXIT_OK
    if command == "profiles":
        remote = _control_call(
            settings,
            "GET",
            f"/control/v1/profiles?limit={arguments.limit}",
            transport=control_transport,
        )
        if remote is not None:
            return remote, EXIT_OK
        return _local_profiles(settings, arguments.limit), EXIT_OK
    if command == "schedules":
        if arguments.schedules_command == "list":
            remote = _control_call(
                settings,
                "GET",
                f"/control/v1/schedules?profile_id={arguments.profile}"
                f"&cursor={arguments.cursor}&limit={arguments.limit}",
                transport=control_transport,
            )
            if remote is not None:
                items = [
                    item
                    for item in cast(Sequence[Mapping[str, object]], remote["items"])
                    if item.get("profile_id") == arguments.profile
                    and int(cast(int, item.get("schedule_key", 0))) > arguments.cursor
                ][: arguments.limit]
                return {
                    "items": items,
                    "next_cursor": _next_key(items, "schedule_key"),
                }, EXIT_OK
            return _local_schedules(
                settings, arguments.profile, arguments.cursor, arguments.limit
            ), EXIT_OK
        return _schedule_mutation(
            settings, arguments, transport=control_transport
        ), EXIT_OK
    if command == "requests":
        if arguments.requests_command == "list":
            remote = _control_call(
                settings,
                "GET",
                f"/control/v1/requests?profile_id={arguments.profile}"
                f"&cursor={arguments.cursor}&limit={arguments.limit}",
                transport=control_transport,
            )
            if remote is not None:
                items = [
                    item
                    for item in cast(Sequence[Mapping[str, object]], remote["items"])
                    if item.get("profile_id") == arguments.profile
                    and int(cast(int, item.get("request_id", 0))) > arguments.cursor
                ][: arguments.limit]
                return {
                    "items": items,
                    "next_cursor": _next_key(items, "request_id"),
                }, EXIT_OK
            return _local_requests(
                settings, arguments.profile, arguments.cursor, arguments.limit
            ), EXIT_OK
        return _request_status(
            settings,
            arguments.profile,
            arguments.request_id,
            transport=control_transport,
        ), EXIT_OK
    if command in {"pause", "resume"}:
        return _pause_resume(settings, arguments, transport=control_transport), EXIT_OK
    if command == "run-now":
        return _run_now(settings, arguments, transport=control_transport), EXIT_OK
    if command in {"cancel-pending", "delete-pending"}:
        return _cancel_pending(
            settings, arguments, transport=control_transport
        ), EXIT_OK
    if command == "shutdown":
        return _shutdown(
            settings, arguments.profile, transport=control_transport
        ), EXIT_OK
    if command == "daemon":
        return _daemon_foreground(
            settings,
            environ=environ,
            adapter_factory=adapter_factory,
            identity_verifier=identity_verifier,
            clock=clock,
            daemon_factory=daemon_factory,
        ), EXIT_OK
    if command == "migrate":
        return _migrate(settings, arguments), EXIT_OK
    if command == "control":
        return _control_lifecycle(settings, arguments, input_stream), EXIT_OK
    if command == "confirmations":
        return _approve_confirmation(
            settings,
            arguments,
            input_stream=input_stream,
            transport=control_transport,
        ), EXIT_OK
    raise _UsageError("a command is required")


def _control_call(
    settings: LocalSettings,
    method: str,
    target: str,
    *,
    body: Mapping[str, object] | None = None,
    headers: Mapping[str, str] | None = None,
    transport: ControlTransport | None,
) -> Mapping[str, object] | None:
    from post_pulsar.control import ControlRequest, load_agent_capability
    from post_pulsar.daemon import EndpointRecord

    endpoint_path = settings.app.endpoint_record_file
    if not endpoint_path.exists():
        return None
    try:
        endpoint = EndpointRecord.read(endpoint_path, policy=settings.app.record_policy)
    except (OSError, RuntimeError, TypeError, ValueError, json.JSONDecodeError) as exc:
        raise StateError("daemon endpoint record is invalid") from exc
    if endpoint.protocol != CONTROL_SCHEMA:
        raise _RemoteControlError(
            409,
            "incompatible_control_version",
            "daemon control version is incompatible",
        )
    major = endpoint.capabilities.get("control_api_major")
    if major != CONTROL_API_MAJOR:
        raise _RemoteControlError(
            409,
            "incompatible_control_version",
            "daemon control version is incompatible",
        )
    capability = load_agent_capability(
        settings.app.agent_capability_file, policy=settings.app.record_policy
    )
    request_headers = {
        "Authorization": f"Bearer {capability}",
        "X-Post-Pulsar-Control-Version": str(CONTROL_API_MAJOR),
        "Accept": "application/json",
    }
    request_headers.update(headers or {})
    payload = (
        b""
        if body is None
        else (json.dumps(body, sort_keys=True, separators=(",", ":")) + "\n").encode(
            "utf-8"
        )
    )
    request = ControlRequest(method, target, request_headers, payload)
    try:
        response = (
            transport(endpoint, request)
            if transport is not None
            else _urllib_transport(endpoint, request)
        )
    except (ConnectionError, TimeoutError, urllib.error.URLError):
        return None
    try:
        document = json.loads(response.body.decode("utf-8"))
    except (UnicodeError, json.JSONDecodeError):
        raise StateError("daemon returned an invalid control envelope") from None
    if not isinstance(document, dict) or document.get("schema") != CONTROL_SCHEMA:
        raise StateError("daemon returned an invalid control envelope")
    if response.status >= 400 or document.get("ok") is not True:
        error = document.get("error", {})
        code = error.get("code") if isinstance(error, dict) else None
        message = error.get("message") if isinstance(error, dict) else None
        safe_code = code if isinstance(code, str) else "daemon_error"
        safe_message = message if isinstance(message, str) else "daemon request failed"
        raise _RemoteControlError(response.status, safe_code, safe_message)
    data = document.get("data")
    if not isinstance(data, dict):
        raise StateError("daemon returned an invalid control result")
    return cast(Mapping[str, object], data)


def _urllib_transport(
    endpoint: EndpointRecord, request: ControlRequest
) -> ControlResponse:
    from post_pulsar.control import ControlResponse

    host, separator, port_text = endpoint.address.rpartition(":")
    if not separator:
        raise ConnectionError("daemon endpoint is unavailable")
    try:
        address = ipaddress.ip_address(host.strip("[]"))
        port = int(port_text)
    except ValueError:
        raise ConnectionError("daemon endpoint is unavailable") from None
    if not address.is_loopback or not 1 <= port <= 65535:
        raise ConnectionError("daemon endpoint is unavailable")
    rendered_host = f"[{address}]" if address.version == 6 else str(address)
    url = f"http://{rendered_host}:{port}{request.target}"
    network_request = urllib.request.Request(
        url,
        data=request.body if request.body else None,
        headers=dict(request.headers),
        method=request.method,
    )
    try:
        with urllib.request.urlopen(  # noqa: S310 - URL is built from parsed loopback IP
            network_request, timeout=2.0
        ) as response:
            return ControlResponse(
                response.status, dict(response.headers.items()), response.read(1048577)
            )
    except urllib.error.HTTPError as exc:
        return ControlResponse(exc.code, dict(exc.headers.items()), exc.read(1048577))


def _control_headers(idempotency_key: str, expected_revision: int) -> dict[str, str]:
    return {
        "Idempotency-Key": idempotency_key,
        "If-Match": str(expected_revision),
    }


def _local_profiles(settings: LocalSettings, limit: int) -> Mapping[str, object]:
    database = settings.app.state_directory / "post_pulsar.sqlite3"
    with StateRepository.open_read_only(database) as repository:
        items = [
            {"profile_id": item.profile_id, "revision": item.revision}
            for item in repository.list_profiles(limit=limit)
        ]
    return {"items": items, "next_cursor": _next_key(items, "profile_id")}


def _local_schedules(
    settings: LocalSettings, profile_id: str, cursor: int, limit: int
) -> Mapping[str, object]:
    settings.profile(profile_id)
    database = settings.app.state_directory / "post_pulsar.sqlite3"
    with StateRepository.open_read_only(database) as repository:
        repository.get_profile(profile_id)
        schedules = repository.list_schedules(
            profile_id=profile_id, after_schedule_key=cursor, limit=limit
        )
        items = [_schedule_document(item) for item in schedules]
    return {"items": items, "next_cursor": _next_key(items, "schedule_key")}


def _local_requests(
    settings: LocalSettings, profile_id: str, cursor: int, limit: int
) -> Mapping[str, object]:
    settings.profile(profile_id)
    database = settings.app.state_directory / "post_pulsar.sqlite3"
    with StateRepository.open_read_only(database) as repository:
        repository.get_profile(profile_id)
        requests = repository.list_run_requests(
            profile_id=profile_id, after_request_id=cursor, limit=limit
        )
        items = [_request_document(item) for item in requests]
    return {"items": items, "next_cursor": _next_key(items, "request_id")}


def _request_status(
    settings: LocalSettings,
    profile_id: str,
    request_id: int,
    *,
    transport: ControlTransport | None,
) -> Mapping[str, object]:
    settings.profile(profile_id)
    remote = _control_call(
        settings,
        "GET",
        f"/control/v1/requests/{request_id}",
        transport=transport,
    )
    if remote is not None:
        if remote.get("profile_id") != profile_id:
            raise StateError("request does not belong to the requested profile")
        return {"request": remote}
    database = settings.app.state_directory / "post_pulsar.sqlite3"
    with StateRepository.open_read_only(database) as repository:
        request = repository.get_run_request(request_id)
        if request.profile_id != profile_id:
            raise StateError("request does not belong to the requested profile")
    return {"request": _request_document(request)}


def _schedule_mutation(
    settings: LocalSettings,
    arguments: argparse.Namespace,
    *,
    transport: ControlTransport | None,
) -> Mapping[str, object]:
    command = cast(str, arguments.schedules_command)
    profile_id = cast(str, arguments.profile)
    settings.profile(profile_id)
    if command in {"create", "update"}:
        action = "schedule_create" if command == "create" else "schedule_update"
        request_arguments: dict[str, object] = {
            "schedule_id": arguments.schedule_id,
            "bucket": arguments.bucket,
            "timezone": arguments.timezone,
            "weekdays": arguments.weekdays,
            "local_time": arguments.local_time,
            "misfire_grace_seconds": arguments.misfire_grace_seconds,
            "enabled": arguments.enabled,
        }
        schedule_key = getattr(arguments, "schedule_key", None)
        schedule_fingerprint = None
        if schedule_key is not None:
            schedule_fingerprint = _assert_schedule_identity(
                settings, profile_id, schedule_key, arguments.schedule_id
            ).config_hash
        intent_id = arguments.intent_id
        direct_target = (
            "/control/v1/schedules"
            if command == "create"
            else f"/control/v1/schedules/{schedule_key}"
        )
        direct_method = "POST" if command == "create" else "PATCH"
    else:
        action = f"schedule_{command}"
        request_arguments = {}
        schedule_key = arguments.schedule_key
        schedule_fingerprint = _assert_schedule_identity(
            settings, profile_id, schedule_key
        ).config_hash
        intent_id = arguments.intent_id
        direct_target = f"/control/v1/schedules/{schedule_key}/disable"
        direct_method = "POST"
        if command == "enable" and intent_id is None:
            raise TransitionError("schedule enable requires an approved intent")
    body: dict[str, object] = {
        "profile_id": profile_id,
        "arguments": request_arguments,
    }
    if schedule_key is not None:
        body["schedule_key"] = schedule_key
    if intent_id is not None:
        body.update(
            {
                "action": action,
                "resource_revision": arguments.expected_revision,
            }
        )
        if schedule_fingerprint is not None:
            body["fingerprint"] = schedule_fingerprint
        remote = _control_call(
            settings,
            "POST",
            f"/control/v1/confirmations/{intent_id}/consume",
            body=body,
            headers=_control_headers(
                arguments.idempotency_key, arguments.expected_revision
            ),
            transport=transport,
        )
    else:
        remote = _control_call(
            settings,
            direct_method,
            direct_target,
            body=body,
            headers=_control_headers(
                arguments.idempotency_key, arguments.expected_revision
            ),
            transport=transport,
        )
    if remote is not None:
        return {"request": remote}
    request = _local_durable_request(
        settings,
        profile_id=profile_id,
        action=action,
        request_arguments=request_arguments,
        idempotency_key=arguments.idempotency_key,
        expected_revision=arguments.expected_revision,
        schedule_key=schedule_key,
        intent_id=intent_id,
    )
    return {"request": _request_document(request)}


def _assert_schedule_identity(
    settings: LocalSettings,
    profile_id: str,
    schedule_key: int,
    schedule_id: str | None = None,
) -> ScheduleRecord:
    database = settings.app.state_directory / "post_pulsar.sqlite3"
    with StateRepository.open_read_only(database) as repository:
        schedule = repository.get_schedule(schedule_key)
        if schedule.profile_id != profile_id or (
            schedule_id is not None and schedule.schedule_id != schedule_id
        ):
            raise ConflictError("schedule exact identity has changed")
        return schedule


def _pause_resume(
    settings: LocalSettings,
    arguments: argparse.Namespace,
    *,
    transport: ControlTransport | None,
) -> Mapping[str, object]:
    action = cast(str, arguments.command)
    profile_id = cast(str, arguments.profile)
    settings.profile(profile_id)
    intent_id = getattr(arguments, "intent_id", None)
    body: dict[str, object] = {"profile_id": profile_id, "arguments": {}}
    target = f"/control/v1/{action}"
    if intent_id is not None:
        body.update(
            {"action": action, "resource_revision": arguments.expected_revision}
        )
        target = f"/control/v1/confirmations/{intent_id}/consume"
    remote = _control_call(
        settings,
        "POST",
        target,
        body=body,
        headers=_control_headers(
            arguments.idempotency_key, arguments.expected_revision
        ),
        transport=transport,
    )
    if remote is not None:
        return {"request": remote}
    request = _local_durable_request(
        settings,
        profile_id=profile_id,
        action=action,
        request_arguments={},
        idempotency_key=arguments.idempotency_key,
        expected_revision=arguments.expected_revision,
        intent_id=intent_id,
    )
    return {"request": _request_document(request)}


def _run_now(
    settings: LocalSettings,
    arguments: argparse.Namespace,
    *,
    transport: ControlTransport | None,
) -> Mapping[str, object]:
    profile_id = cast(str, arguments.profile)
    settings.profile(profile_id)
    database = settings.app.state_directory / "post_pulsar.sqlite3"
    with StateRepository.open_read_only(database) as repository:
        bundle = repository.get_bundle(arguments.bundle_key)
        if (
            bundle.profile_id != profile_id
            or bundle.bundle_id != arguments.bundle_id
            or bundle.fingerprint != arguments.fingerprint
            or bundle.source_bucket != arguments.bucket
        ):
            raise ConflictError("run-now exact bundle identity has changed")
    request_arguments = {
        "bucket": arguments.bucket,
        "bundle_id": arguments.bundle_id,
        "fingerprint": arguments.fingerprint,
        "trigger_id": arguments.idempotency_key,
    }
    body = {
        "action": "run_now",
        "profile_id": profile_id,
        "bundle_key": arguments.bundle_key,
        "resource_revision": arguments.expected_revision,
        "fingerprint": arguments.fingerprint,
        "arguments": request_arguments,
    }
    remote = _control_call(
        settings,
        "POST",
        f"/control/v1/confirmations/{arguments.intent_id}/consume",
        body=body,
        headers=_control_headers(
            arguments.idempotency_key, arguments.expected_revision
        ),
        transport=transport,
    )
    if remote is not None:
        return {"request": remote}
    request = _local_durable_request(
        settings,
        profile_id=profile_id,
        action="run_now",
        request_arguments=request_arguments,
        idempotency_key=arguments.idempotency_key,
        expected_revision=arguments.expected_revision,
        bundle_key=arguments.bundle_key,
        intent_id=arguments.intent_id,
    )
    return {"request": _request_document(request)}


def _cancel_pending(
    settings: LocalSettings,
    arguments: argparse.Namespace,
    *,
    transport: ControlTransport | None,
) -> Mapping[str, object]:
    action = "cancel" if arguments.command == "cancel-pending" else "delete"
    profile_id = cast(str, arguments.profile)
    settings.profile(profile_id)
    database = settings.app.state_directory / "post_pulsar.sqlite3"
    with StateRepository.open_read_only(database) as repository:
        bundle = repository.get_bundle(arguments.bundle_key)
        if (
            bundle.profile_id != profile_id
            or bundle.bundle_id != arguments.bundle_id
            or bundle.fingerprint != arguments.fingerprint
        ):
            raise ConflictError("pending bundle exact identity has changed")
        deliveries = repository.list_bundle_deliveries(arguments.bundle_key)
        if bundle.status != "active" or any(
            delivery.status != "pending"
            or delivery.phase is not None
            or delivery.attempt_count != 0
            for delivery in deliveries
        ):
            raise TransitionError("bundle is not pristine pending work")
        if any(
            repository.list_delivery_artifacts(arguments.bundle_key, delivery.platform)
            for delivery in deliveries
        ):
            raise TransitionError("bundle has durable delivery artifacts")
    request_arguments = {
        "bundle_id": arguments.bundle_id,
        "fingerprint": arguments.fingerprint,
    }
    body = {
        "action": action,
        "profile_id": profile_id,
        "bundle_key": arguments.bundle_key,
        "resource_revision": arguments.expected_revision,
        "fingerprint": arguments.fingerprint,
        "arguments": request_arguments,
    }
    remote = _control_call(
        settings,
        "POST",
        f"/control/v1/confirmations/{arguments.intent_id}/consume",
        body=body,
        headers=_control_headers(
            arguments.idempotency_key, arguments.expected_revision
        ),
        transport=transport,
    )
    if remote is not None:
        return {"request": remote}
    request = _local_durable_request(
        settings,
        profile_id=profile_id,
        action=action,
        request_arguments=request_arguments,
        idempotency_key=arguments.idempotency_key,
        expected_revision=arguments.expected_revision,
        bundle_key=arguments.bundle_key,
        intent_id=arguments.intent_id,
    )
    return {"request": _request_document(request)}


def _delivery_mutation(
    settings: LocalSettings,
    arguments: argparse.Namespace,
    *,
    transport: ControlTransport | None,
) -> Mapping[str, object]:
    action = cast(str, arguments.command)
    profile_id = cast(str, arguments.profile)
    settings.profile(profile_id)
    database = settings.app.state_directory / "post_pulsar.sqlite3"
    with StateRepository.open_read_only(database) as repository:
        bundle = repository.get_bundle(arguments.bundle_key)
        if (
            bundle.profile_id != profile_id
            or bundle.bundle_id != arguments.bundle_id
            or bundle.fingerprint != arguments.fingerprint
        ):
            raise ConflictError(f"{action} exact bundle identity has changed")
        delivery = repository.get_delivery(arguments.bundle_key, arguments.platform)
        expected_status = "failed" if action == "retry" else "ambiguous"
        if bundle.status != "blocked" or delivery.status != expected_status:
            raise TransitionError(f"{action} target is not an exact blocked delivery")
    request_arguments: dict[str, object] = {
        "bundle_id": arguments.bundle_id,
        "fingerprint": arguments.fingerprint,
        "platform": arguments.platform,
    }
    if action == "reconcile":
        request_arguments["published_remote_id"] = arguments.published
    body = {
        "action": action,
        "profile_id": profile_id,
        "bundle_key": arguments.bundle_key,
        "resource_revision": arguments.expected_revision,
        "fingerprint": arguments.fingerprint,
        "arguments": request_arguments,
    }
    remote = _control_call(
        settings,
        "POST",
        f"/control/v1/confirmations/{arguments.intent_id}/consume",
        body=body,
        headers=_control_headers(
            arguments.idempotency_key, arguments.expected_revision
        ),
        transport=transport,
    )
    if remote is not None:
        return {"request": remote}
    request = _local_durable_request(
        settings,
        profile_id=profile_id,
        action=action,
        request_arguments=request_arguments,
        idempotency_key=arguments.idempotency_key,
        expected_revision=arguments.expected_revision,
        bundle_key=arguments.bundle_key,
        intent_id=arguments.intent_id,
    )
    return {"request": _request_document(request)}


def _local_durable_request(
    settings: LocalSettings,
    *,
    profile_id: str,
    action: str,
    request_arguments: Mapping[str, object],
    idempotency_key: str,
    expected_revision: int,
    bundle_key: int | None = None,
    schedule_key: int | None = None,
    intent_id: str | None = None,
) -> RunRequestRecord:
    locks = LockManager(settings.app.state_directory)
    database = settings.app.state_directory / "post_pulsar.sqlite3"
    with (
        locks.acquire_instance() as instance,
        locks.acquire_profiles(instance, (profile_id,)),
        StateRepository.open_existing(database) as repository,
    ):
        if intent_id is not None:
            return repository.consume_intent_with_request(
                intent_id=intent_id,
                action=action,
                arguments=request_arguments,
                profile_id=profile_id,
                resource_revision=expected_revision,
                fingerprint=(
                    repository.get_bundle(bundle_key).fingerprint
                    if bundle_key is not None
                    else (
                        repository.get_schedule(schedule_key).config_hash
                        if schedule_key is not None
                        else None
                    )
                ),
                idempotency_key=idempotency_key,
                bundle_key=bundle_key,
                schedule_key=schedule_key,
            )
        return repository.create_run_request(
            profile_id=profile_id,
            action=action,
            arguments=request_arguments,
            idempotency_key=idempotency_key,
            expected_revision=expected_revision,
            bundle_key=bundle_key,
            schedule_key=schedule_key,
        )


def _shutdown(
    settings: LocalSettings,
    profile_id: str,
    *,
    transport: ControlTransport | None,
) -> Mapping[str, object]:
    from post_pulsar.daemon import EndpointRecord
    from post_pulsar.process_identity import ProcessIdentityError

    settings.profile(profile_id)
    try:
        endpoint = EndpointRecord.read(
            settings.app.endpoint_record_file, policy=settings.app.record_policy
        )
    except FileNotFoundError:
        raise _RemoteControlError(
            503, "daemon_unavailable", "foreground daemon is not running"
        ) from None
    if (
        endpoint.protocol != CONTROL_SCHEMA
        or endpoint.capabilities.get("control_api_major") != CONTROL_API_MAJOR
    ):
        raise _RemoteControlError(
            409,
            "incompatible_control_version",
            "daemon control version is incompatible",
        )
    try:
        matching = _endpoint_process_is_live(endpoint)
    except ProcessIdentityError:
        matching = False
    if not matching:
        raise _RemoteControlError(
            503, "daemon_unavailable", "foreground daemon endpoint is stale"
        )
    remote = _control_call(
        settings,
        "POST",
        "/control/v1/shutdown",
        body={"startup_nonce": endpoint.startup_nonce},
        transport=transport,
    )
    if remote is None:
        raise _RemoteControlError(
            503, "daemon_unavailable", "foreground daemon is not running"
        )
    return {"status": "shutdown_requested", "profile_id": profile_id}


def _endpoint_process_is_live(endpoint: EndpointRecord) -> bool:
    from post_pulsar.process_identity import process_identity_matches

    return process_identity_matches(endpoint.pid, endpoint.process_started_at)


def _daemon_foreground(
    settings: LocalSettings,
    *,
    environ: Mapping[str, str] | None,
    adapter_factory: AdapterFactory | None,
    identity_verifier: IdentityVerifier,
    clock: Callable[[], datetime],
    daemon_factory: DaemonFactory | None,
) -> Mapping[str, object]:
    from post_pulsar.control import ControlApplication, load_agent_capability
    from post_pulsar.daemon import ForegroundDaemon

    load_agent_capability(
        settings.app.agent_capability_file, policy=settings.app.record_policy
    )
    database = settings.app.state_directory / "post_pulsar.sqlite3"
    control = ControlApplication(
        database,
        settings.app.agent_capability_file,
        operator_verifier_file=settings.app.operator_verifier_file,
        allow_agent_publish=settings.app.allow_agent_publish,
        max_body_bytes=settings.app.control_max_body_bytes,
        max_results=settings.app.control_max_results,
        confirmation_ttl_seconds=settings.app.confirmation_ttl_seconds,
        record_policy=settings.app.record_policy,
    )
    holder: dict[str, ForegroundDaemon] = {}

    def recover() -> None:
        from post_pulsar.admission import DraftAdmissionService

        protected: list[BundleRecord] = []
        with StateRepository.open_existing(database, clock=clock) as repository:
            repository.recover_claimed_run_requests(holder["daemon"]._worker_token)
            for profile in settings.profiles:
                try:
                    DraftAdmissionService(repository, profile.account_root).recover()
                except Exception:
                    repository.record_callback_failure(
                        phase="admission", profile_id=profile.profile_id
                    )
                protected.extend(repository.list_protected_bundles(profile.profile_id))
        daemon = holder["daemon"]
        lease = daemon._lease
        if lease is None:
            raise StateError("daemon instance lease is unavailable")
        for bundle in protected:

            def recover_bundle() -> None:
                try:
                    OneRunApplication(
                        settings.config_path,
                        locks=daemon._locks,
                        instance_lease=lease,
                        environ=environ,
                        clock=clock,
                        adapter_factory=adapter_factory,
                    ).run_once(
                        RunOnceRequest(
                            bundle.profile_id,
                            bundle.source_bucket,
                            f"daemon-recovery-{bundle.bundle_key}",
                            expected_bundle_key=bundle.bundle_key,
                            expected_fingerprint=bundle.fingerprint,
                        )
                    )
                except Exception:
                    with StateRepository.open_existing(
                        database, clock=clock
                    ) as repository:
                        repository.record_callback_failure(
                            phase="recovery", bundle_key=bundle.bundle_key
                        )

            if not daemon.dispatch_if_running(recover_bundle):
                break

    def admit_schedules() -> None:
        from post_pulsar.scheduler import DeterministicScheduler

        daemon = holder["daemon"]
        with StateRepository.open_existing(database, clock=clock) as repository:
            work = DeterministicScheduler(repository, wall_clock=clock).tick(
                dispatch_gate=daemon.dispatch_if_running,
                failure_handler=lambda schedule: repository.record_callback_failure(
                    phase="schedule", profile_id=schedule.profile_id
                ),
            )
        lease = daemon._lease
        if lease is None:
            raise StateError("daemon instance lease is unavailable")
        for item in work:

            def dispatch_occurrence() -> None:
                try:
                    OneRunApplication(
                        settings.config_path,
                        locks=daemon._locks,
                        instance_lease=lease,
                        environ=environ,
                        clock=clock,
                        adapter_factory=adapter_factory,
                    ).run_once(
                        RunOnceRequest(
                            item.profile_id,
                            item.bucket,
                            f"schedule-{item.run_id}",
                            schedule_run_id=item.run_id,
                        )
                    )
                except Exception:
                    with StateRepository.open_existing(
                        database, clock=clock
                    ) as repository:
                        repository.record_callback_failure(
                            phase="schedule",
                            run_id=item.run_id,
                            profile_id=item.profile_id,
                        )

            if not daemon.dispatch_if_running(dispatch_occurrence):
                break

    def execute(request: RunRequestRecord) -> Mapping[str, object]:
        return _execute_daemon_request(
            settings,
            request,
            daemon=holder["daemon"],
            environ=environ,
            adapter_factory=adapter_factory,
            identity_verifier=identity_verifier,
            clock=clock,
        )

    factory = daemon_factory or ForegroundDaemon
    daemon = factory(
        settings.app.state_directory,
        settings.app.endpoint_record_file,
        settings.app.control_host,
        settings.app.control_port,
        control_application=control,
        agent_capability_file=settings.app.agent_capability_file,
        record_policy=settings.app.record_policy,
        recovery=recover,
        schedule_admission=admit_schedules,
        request_executor=execute,
    )
    holder["daemon"] = daemon
    daemon.run_forever()
    return {"status": "stopped"}


def _execute_local_mutation(
    repository: StateRepository, request: RunRequestRecord
) -> Mapping[str, object]:
    """Apply the fixed local action inside its durable completion transaction."""
    if request.action in {"pause", "resume"}:
        pause_record = repository.set_paused(
            request.action == "pause", expected_revision=request.expected_revision
        )
        return {"paused": pause_record.paused, "revision": pause_record.revision}
    if request.action == "schedule_create":
        schedule_record = repository.create_schedule(
            profile_id=request.profile_id,
            schedule_id=cast(str, request.arguments["schedule_id"]),
            bucket=cast(
                "Literal['QUEUE', 'RANDOM', 'REELS']",
                request.arguments["bucket"],
            ),
            timezone=cast(str, request.arguments["timezone"]),
            weekdays=cast(Sequence[int], request.arguments["weekdays"]),
            local_time=cast(str, request.arguments["local_time"]),
            misfire_grace_seconds=cast(int, request.arguments["misfire_grace_seconds"]),
            enabled=cast(bool, request.arguments["enabled"]),
            expected_revision=request.expected_revision,
        )
        return _schedule_document(schedule_record)
    if request.action == "schedule_update":
        if request.schedule_key is None:
            raise StateError("schedule update lost its exact resource")
        schedule_record = repository.update_schedule(
            request.schedule_key,
            profile_id=request.profile_id,
            schedule_id=cast(str, request.arguments["schedule_id"]),
            bucket=cast(
                "Literal['QUEUE', 'RANDOM', 'REELS']",
                request.arguments["bucket"],
            ),
            timezone=cast(str, request.arguments["timezone"]),
            weekdays=cast(Sequence[int], request.arguments["weekdays"]),
            local_time=cast(str, request.arguments["local_time"]),
            misfire_grace_seconds=cast(int, request.arguments["misfire_grace_seconds"]),
            enabled=cast(bool, request.arguments["enabled"]),
            expected_revision=request.expected_revision,
        )
        return _schedule_document(schedule_record)
    if request.action in {"schedule_enable", "schedule_disable"}:
        if request.schedule_key is None:
            raise StateError("schedule toggle lost its exact resource")
        schedule_record = repository.set_schedule_enabled(
            request.schedule_key,
            request.action == "schedule_enable",
            expected_revision=request.expected_revision,
        )
        return _schedule_document(schedule_record)
    if request.action in {"cancel", "delete"}:
        if request.bundle_key is None:
            raise StateError("pending terminal request lost its exact resource")
        bundle = repository.get_bundle(request.bundle_key)
        if bundle.bundle_id != request.arguments.get("bundle_id"):
            raise ConflictError("pending bundle identity changed before execution")
        tombstone = repository.terminalize_pending_bundle(
            profile_id=request.profile_id,
            bundle_key=request.bundle_key,
            expected_revision=request.expected_revision,
            fingerprint=cast(str, request.arguments["fingerprint"]),
            action=request.action,
        )
        return _bundle_document(repository, tombstone)
    raise StateError("unsupported local mutation")


def _execute_daemon_request(
    settings: LocalSettings,
    request: RunRequestRecord,
    *,
    daemon: ForegroundDaemon,
    environ: Mapping[str, str] | None,
    adapter_factory: AdapterFactory | None,
    identity_verifier: IdentityVerifier,
    clock: Callable[[], datetime],
) -> Mapping[str, object]:
    database = settings.app.state_directory / "post_pulsar.sqlite3"
    if request.action in {"enqueue", "run_now", "publish_now"}:
        lease = daemon._lease
        if lease is None:
            raise StateError("daemon instance lease is unavailable")
        outcome = OneRunApplication(
            settings.config_path,
            locks=daemon._locks,
            instance_lease=lease,
            environ=environ,
            clock=clock,
            adapter_factory=adapter_factory,
        ).run_once(
            RunOnceRequest(
                request.profile_id,
                cast(
                    "Literal['QUEUE', 'RANDOM', 'REELS']",
                    request.arguments["bucket"],
                ),
                cast(str, request.arguments["trigger_id"]),
                expected_bundle_key=request.bundle_key,
                expected_fingerprint=cast(str, request.arguments["fingerprint"]),
            )
        )
        return _run_result(outcome)
    lease = daemon._lease
    if lease is None:
        raise StateError("daemon instance lease is unavailable")
    with (
        daemon._locks.acquire_profiles(lease, (request.profile_id,)),
        StateRepository.open_existing(database, clock=clock) as repository,
    ):
        pause_blocked = request.action in {
            "admit_draft",
            "enqueue",
            "publish_now",
            "retry",
            "schedule_enable",
        } or (
            request.action in {"schedule_create", "schedule_update"}
            and request.arguments.get("enabled") is True
        )
        if pause_blocked and repository.get_pause_state().admission_blocked:
            raise TransitionError("new publication admission is paused")
        if request.action == "admit_draft":
            from post_pulsar.admission import DraftAdmissionService

            if request.intent_id is None:
                raise StateError("draft admission lost its confirmation intent")
            intent = repository.get_confirmation_intent(request.intent_id)
            record = DraftAdmissionService(
                repository, settings.profile(request.profile_id).account_root
            ).admit(
                profile_id=request.profile_id,
                bucket=cast(
                    "Literal['QUEUE', 'RANDOM', 'REELS']",
                    request.arguments["bucket"],
                ),
                bundle_id=cast(str, request.arguments["bundle_id"]),
                expected_fingerprint=cast(str, intent.fingerprint),
                intent_id=request.intent_id,
            )
            return asdict(record)
        if request.action in {"edit_caption", "edit_alt"}:
            return _edit_draft_text(settings, request)
        if request.action in {
            "pause",
            "resume",
            "schedule_create",
            "schedule_update",
            "schedule_enable",
            "schedule_disable",
            "cancel",
            "delete",
        }:
            return repository.execute_local_run_request(
                request, lambda: _execute_local_mutation(repository, request)
            )
        if request.action == "retry":
            if request.bundle_key is None:
                raise StateError("retry request lost its exact resource")
            bundle = _assert_request_bundle_identity(repository, request)
            platform = _request_platform(request)
            profile = settings.profile(request.profile_id)
            snapshot = _target_snapshot(repository, request.bundle_key, platform)
            current = _configured_target_snapshot(profile, platform)
            if current != snapshot:
                raise ConflictError(
                    "current target configuration differs from snapshot"
                )
            credentials = validate_publishing_credentials(
                settings, request.profile_id, (platform,), environ
            )
            identity_verifier(
                PublicationSnapshot(
                    request.profile_id,
                    bundle.bundle_key,
                    bundle.bundle_id,
                    bundle.fingerprint,
                    bundle.source_bucket,
                    snapshot,
                ),
                credentials.for_target(platform),
                settings.app.state_directory / "staging" / "private",
            )
            delivery = repository.operator_retry(
                request.bundle_key,
                platform,
                expected_bundle_revision=request.expected_revision,
                validated_snapshot=snapshot,
            )
        if request.action == "reconcile":
            if request.bundle_key is None:
                raise StateError("reconcile request lost its exact resource")
            bundle = _assert_request_bundle_identity(repository, request)
            platform = _request_platform(request)
            remote_id = request.arguments.get("published_remote_id")
            if remote_id is not None and not isinstance(remote_id, str):
                raise StateError("reconcile request has invalid operator evidence")
            delivery = repository.operator_reconcile(
                request.bundle_key,
                platform,
                published_remote_id=remote_id,
                expected_bundle_revision=request.expected_revision,
            )
            if remote_id is None:
                return _delivery_document(delivery)
    if request.action in {"retry", "reconcile"}:
        outcome = OneRunApplication(
            settings.config_path,
            locks=daemon._locks,
            instance_lease=lease,
            environ=environ,
            clock=clock,
            adapter_factory=adapter_factory,
        ).run_once(
            RunOnceRequest(
                request.profile_id,
                bundle.source_bucket,
                f"operator-{request.request_id}",
                expected_bundle_key=bundle.bundle_key,
                expected_fingerprint=bundle.fingerprint,
                only_platform=platform if request.action == "retry" else None,
                archive_only=request.action == "reconcile",
            )
        )
        return _run_result(outcome)
    raise StateError("daemon request action is unsupported")


def _edit_draft_text(
    settings: LocalSettings, request: RunRequestRecord
) -> Mapping[str, object]:
    from post_pulsar.content import scan_inbox

    profile = settings.profile(request.profile_id)
    drafts = profile.account_root / "DRAFTS"
    scan = scan_inbox(drafts)
    matches = [
        item
        for item in scan.bundles
        if item.bundle_id == request.arguments.get("bundle_id")
    ]
    if len(matches) != 1 or matches[0].fingerprint != request.arguments.get(
        "fingerprint"
    ):
        raise ConflictError("draft identity changed before edit")
    suffix = ".txt" if request.action == "edit_caption" else "-alt.txt"
    destination = drafts / f"{matches[0].bundle_id}{suffix}"
    payload = cast(str, request.arguments["text"]).encode("utf-8")
    try:
        original = destination.lstat()
    except FileNotFoundError:
        original = None
    if original is not None and (
        not stat.S_ISREG(original.st_mode) or original.st_nlink != 1
    ):
        raise StateError("draft text member is unsafe")
    descriptor, temporary_name = tempfile.mkstemp(prefix=".draft-edit-", dir=drafts)
    temporary = Path(temporary_name)
    try:
        if os.name == "nt":
            from post_pulsar.secure_files import _windows_secure, _windows_validate

            _windows_secure(temporary, False)
            _windows_validate(temporary)
        else:
            os.fchmod(descriptor, 0o600)
        remaining = memoryview(payload)
        while remaining:
            written = os.write(descriptor, remaining)
            if written <= 0:
                raise StateError("draft text edit could not make progress")
            remaining = remaining[written:]
        os.fsync(descriptor)
        os.close(descriptor)
        descriptor = -1
        current = [
            item
            for item in scan_inbox(drafts).bundles
            if item.bundle_id == matches[0].bundle_id
        ]
        if len(current) != 1 or current[0].fingerprint != matches[0].fingerprint:
            raise ConflictError("draft identity changed during edit")
        try:
            latest = destination.lstat()
        except FileNotFoundError:
            latest = None
        if original != latest:
            raise ConflictError("draft text identity changed during edit")
        if latest is not None:
            source_fd = os.open(destination, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0))
            try:
                if os.fstat(source_fd) != original:
                    raise ConflictError(
                        "draft text identity changed before replacement"
                    )
            finally:
                os.close(source_fd)
        os.replace(temporary, destination)
        from post_pulsar.secure_files import fsync_directory

        fsync_directory(drafts)
    finally:
        if descriptor != -1:
            os.close(descriptor)
        temporary.unlink(missing_ok=True)
    edited = [
        item
        for item in scan_inbox(drafts).bundles
        if item.bundle_id == matches[0].bundle_id
    ]
    if len(edited) != 1:
        raise StateError("draft edit produced invalid content")
    return {
        "profile_id": request.profile_id,
        "bundle_id": edited[0].bundle_id,
        "fingerprint": edited[0].fingerprint,
    }


def _assert_request_bundle_identity(
    repository: StateRepository, request: RunRequestRecord
) -> BundleRecord:
    if request.bundle_key is None:
        raise StateError("durable request lost its exact bundle resource")
    bundle = repository.get_bundle(request.bundle_key)
    if (
        bundle.profile_id != request.profile_id
        or bundle.bundle_id != request.arguments.get("bundle_id")
        or bundle.fingerprint != request.arguments.get("fingerprint")
    ):
        raise ConflictError("durable request bundle identity changed")
    return bundle


def _request_platform(request: RunRequestRecord) -> Platform:
    value = request.arguments.get("platform")
    if value not in {"x", "instagram"}:
        raise StateError("durable request platform is invalid")
    return cast(Platform, value)


def _migrate(
    settings: LocalSettings, arguments: argparse.Namespace
) -> Mapping[str, object]:
    profile_id = cast(str, arguments.profile)
    profile = settings.profile(profile_id)
    mode = cast(str, arguments.migration_command)
    config_hash = hashlib.sha256(settings.config_path.read_bytes()).hexdigest()
    if mode == "dry-run":
        locks = LockManager(settings.app.state_directory)
        with locks.acquire_instance():
            report = migrate_legacy_layout(
                selected_profile_id=profile_id,
                profile=profile,
                legacy_posts_directory=arguments.legacy_posts,
                state_directory=settings.app.state_directory,
                config_hash=config_hash,
                mode=mode,
            )
    else:
        report = migrate_legacy_layout(
            selected_profile_id=profile_id,
            profile=profile,
            legacy_posts_directory=arguments.legacy_posts,
            state_directory=settings.app.state_directory,
            config_hash=config_hash,
            mode=mode,
        )
    return {"migration": asdict(report)}


def _control_lifecycle(
    settings: LocalSettings, arguments: argparse.Namespace, input_stream: IO[str]
) -> Mapping[str, object]:
    from post_pulsar.control import (
        ControlSecurityError,
        initialize_operator_secret,
        load_agent_capability,
        rotate_agent_capability,
    )

    if arguments.control_command == "agent-capability":
        path = settings.app.agent_capability_file
        action = cast(str, arguments.capability_command)
        if action == "initialize":
            if path.exists():
                load_agent_capability(path, policy=settings.app.record_policy)
                raise ControlSecurityError("agent capability is already initialized")
            rotate_agent_capability(path, policy=settings.app.record_policy)
            record = write_bootstrap(
                settings.app.bootstrap_file,
                BootstrapRecord(
                    installation_id=arguments.installation_id,
                    endpoint_record=settings.app.endpoint_record_file,
                    agent_capability=settings.app.agent_capability_file,
                    service_mode=arguments.service_mode,
                    service_identifier=arguments.service_identifier,
                ),
                policy=settings.app.record_policy,
            )
            return {
                "status": "initialized",
                "bootstrap": str(settings.app.bootstrap_file),
                "installation_id": record.installation_id,
            }
        if action == "rotate":
            load_agent_capability(path, policy=settings.app.record_policy)
            rotate_agent_capability(path, policy=settings.app.record_policy)
            return {"status": "rotated"}
        load_agent_capability(path, policy=settings.app.record_policy)
        if settings.app.record_policy is not None:
            from post_pulsar.secure_files import SecureFileError

            try:
                settings.app.record_policy.validate(write=True)
            except SecureFileError:
                raise ControlSecurityError(
                    "capability revocation requires the core principal"
                ) from None
        path.unlink()
        _fsync_directory(path.parent)
        return {"status": "revoked"}
    path = settings.app.operator_verifier_file
    action = cast(str, arguments.operator_command)
    if action == "initialize" and path.exists():
        _assert_owner_file(path, "operator verifier", policy=settings.app.record_policy)
        raise ControlSecurityError("operator secret is already initialized")
    if action == "rotate":
        _assert_owner_file(path, "operator verifier", policy=settings.app.record_policy)
    initialize_operator_secret(
        path,
        input_stream=_secret_input(input_stream),
        max_failures=settings.app.operator_max_failures,
        lockout_seconds=settings.app.operator_lockout_seconds,
        policy=settings.app.record_policy,
    )
    return {"status": "initialized" if action == "initialize" else "rotated"}


def _approve_confirmation(
    settings: LocalSettings,
    arguments: argparse.Namespace,
    *,
    input_stream: IO[str],
    transport: ControlTransport | None,
) -> Mapping[str, object]:
    from post_pulsar.control import ControlSecurityError, verify_operator_secret

    profile_id = cast(str, arguments.profile)
    settings.profile(profile_id)
    secret_stream = _secret_input(input_stream)
    if not secret_stream.isatty():
        raise ControlSecurityError("operator approval requires a real TTY")
    supplied = secret_stream.readline().rstrip("\r\n")
    if not supplied:
        raise ControlSecurityError("operator approval secret is invalid")
    remote = _control_call(
        settings,
        "POST",
        f"/control/v1/operator/confirmations/{arguments.intent_id}/approve",
        headers={
            **_control_headers(arguments.idempotency_key, arguments.expected_revision),
            "X-Post-Pulsar-Principal": "operator",
            "X-Post-Pulsar-Operator-Secret": supplied,
        },
        transport=transport,
    )
    if remote is not None:
        if remote.get("profile_id") != profile_id:
            raise StateError("confirmation does not belong to the requested profile")
        return {"confirmation": remote}
    if not verify_operator_secret(
        settings.app.operator_verifier_file, supplied, policy=settings.app.record_policy
    ):
        raise ControlSecurityError("operator authentication failed")
    locks = LockManager(settings.app.state_directory)
    database = settings.app.state_directory / "post_pulsar.sqlite3"
    with (
        locks.acquire_instance() as instance,
        locks.acquire_profiles(instance, (profile_id,)),
        StateRepository.open_existing(database) as repository,
    ):
        intent = repository.get_confirmation_intent(arguments.intent_id)
        if intent.profile_id != profile_id:
            raise ConflictError("confirmation does not belong to the requested profile")
        approved = repository.approve_confirmation_intent(
            arguments.intent_id,
            expected_revision=arguments.expected_revision,
        )
    return {"confirmation": _json_safe(asdict(approved))}


class _GetpassInput:
    def __init__(self, stream: IO[str]) -> None:
        self._stream = stream

    def isatty(self) -> bool:
        return self._stream.isatty()

    def readline(self) -> str:
        return (
            tty_prompt.getpass("Operator approval secret: ", stream=sys.stderr) + "\n"
        )


def _secret_input(stream: IO[str]) -> IO[str] | _GetpassInput:
    return _GetpassInput(stream) if stream is sys.stdin else stream


def _assert_owner_file(path: Path, label: str, *, policy=None) -> None:
    from post_pulsar.control import ControlSecurityError
    from post_pulsar.secure_files import SecureFileError, assert_owner_file

    try:
        assert_owner_file(path, policy=policy, agent_read=False)
    except (OSError, SecureFileError):
        raise ControlSecurityError(f"{label} file is unsafe or missing") from None


def _fsync_directory(path: Path) -> None:
    from post_pulsar.secure_files import fsync_directory

    fsync_directory(path)


def _run(
    settings: LocalSettings,
    profile_id: str,
    bucket: str,
    trigger_id: str,
    *,
    environ: Mapping[str, str] | None,
    adapter_factory: AdapterFactory | None,
    clock: Callable[[], datetime],
) -> RunOutcome:
    settings.profile(profile_id)
    locks = LockManager(settings.app.state_directory)
    with locks.acquire_instance() as instance:
        application = OneRunApplication(
            settings.config_path,
            locks=locks,
            instance_lease=instance,
            environ=environ,
            clock=clock,
            adapter_factory=adapter_factory,
        )
        return application.run_once(
            RunOnceRequest(
                profile_id,
                cast("Literal['QUEUE', 'RANDOM', 'REELS']", bucket),
                trigger_id,
            )
        )


def _status(
    settings: LocalSettings, profile_id: str, bundle_key: int | None
) -> Mapping[str, object]:
    settings.profile(profile_id)
    database = settings.app.state_directory / "post_pulsar.sqlite3"
    with (
        StateRepository.open_read_only(database) as repository,
        repository.read_snapshot(),
    ):
        repository.get_profile(profile_id)
        if bundle_key is None:
            bundles = repository.list_protected_bundles(profile_id)
        else:
            bundle = repository.get_bundle(bundle_key)
            if bundle.profile_id != profile_id:
                raise StateError("bundle does not belong to the requested profile")
            bundles = (bundle,)
        documents = tuple(_bundle_document(repository, item) for item in bundles)
    return {"profile_id": profile_id, "bundles": documents}


def _verify_identity(
    snapshot: PublicationSnapshot, token: SecretValue, private_root: Path
) -> None:
    adapter: PlatformAdapter = _default_adapter_factory(snapshot, token, private_root)
    try:
        adapter.verify_identity()
    finally:
        adapter.close()


def _target_snapshot(
    repository: StateRepository, bundle_key: int, platform: Platform
) -> TargetSnapshot:
    for item in repository.list_target_snapshots(bundle_key):
        if item.platform == platform:
            return item
    raise StateError(f"bundle has no {platform} target")


def _bundle_document(
    repository: StateRepository, bundle: BundleRecord
) -> Mapping[str, object]:
    warnings = sorted(
        {
            cast(str, event["event_code"])
            for event in repository.list_events(bundle.bundle_key)
            if event["event_type"] == "warning"
        }
    )
    return {
        "bundle_key": bundle.bundle_key,
        "bundle_id": bundle.bundle_id,
        "bucket": bundle.source_bucket,
        "status": bundle.status,
        "revision": bundle.revision,
        "archive_path": bundle.archive_path,
        "warnings": warnings,
        "deliveries": [
            _delivery_document(item)
            for item in repository.list_bundle_deliveries(bundle.bundle_key)
        ],
    }


def _delivery_document(delivery: DeliveryRecord) -> Mapping[str, object]:
    return {
        "platform": delivery.platform,
        "status": delivery.status,
        "phase": delivery.phase,
        "attempt_count": delivery.attempt_count,
        "consecutive_failures": delivery.consecutive_failures,
        "safe_to_retry": delivery.safe_to_retry,
        "next_attempt_at": _iso(delivery.next_attempt_at),
        "remote_id": delivery.remote_id,
        "error_code": delivery.error_code,
        "revision": delivery.revision,
    }


def _schedule_document(schedule: ScheduleRecord) -> Mapping[str, object]:
    return {
        "schedule_key": schedule.schedule_key,
        "profile_id": schedule.profile_id,
        "schedule_id": schedule.schedule_id,
        "bucket": schedule.bucket,
        "timezone": schedule.timezone,
        "weekdays": list(schedule.weekdays),
        "local_time": schedule.local_time,
        "misfire_grace_seconds": schedule.misfire_grace_seconds,
        "enabled": schedule.enabled,
        "config_hash": schedule.config_hash,
        "revision": schedule.revision,
    }


def _request_document(request: RunRequestRecord) -> Mapping[str, object]:
    return cast(Mapping[str, object], _json_safe(asdict(request)))


def _json_safe(value: object) -> object:
    if isinstance(value, datetime):
        return _iso(value)
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, dict):
        return {str(key): _json_safe(child) for key, child in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_safe(child) for child in value]
    return value


def _next_key(items: Sequence[Mapping[str, object]], key: str) -> object | None:
    return items[-1].get(key) if items else None


def _run_result(outcome: RunOutcome) -> Mapping[str, object]:
    return {
        "status": outcome.status,
        "bundle_key": outcome.bundle_key,
        "bundle_id": outcome.bundle_id,
        "code": outcome.code,
    }


def _run_exit_code(outcome: RunOutcome) -> int:
    if outcome.status == "archived":
        return EXIT_OK
    if outcome.status in {"empty", "deferred"}:
        return EXIT_NO_WORK
    if outcome.status == "invalid":
        return EXIT_INVALID
    if outcome.status == "blocked":
        return EXIT_BLOCKED
    return EXIT_RETRYABLE


def _write_result(
    stream: IO[str], json_output: bool, command: str, result: Mapping[str, object]
) -> None:
    if json_output:
        document = {
            "schema": CONTROL_SCHEMA,
            "ok": True,
            "command": command,
            "result": result,
        }
        stream.write(json.dumps(document, sort_keys=True, separators=(",", ":")) + "\n")
        return
    if command == "status":
        if "daemon" in result:
            daemon = cast(Mapping[str, object], result["daemon"])
            stream.write(
                f"profile {result['profile_id']}: daemon "
                f"{'paused' if daemon.get('paused') else 'running'}\n"
            )
            return
        bundles = cast(Sequence[Mapping[str, object]], result.get("bundles", ()))
        stream.write(
            f"profile {result['profile_id']}: {len(bundles)} tracked bundle(s)\n"
        )
        for bundle in bundles:
            stream.write(
                f"{bundle['bundle_key']} {bundle['bundle_id']} "
                f"{bundle['bucket']} {bundle['status']}\n"
            )
        return
    if "delivery" in result:
        delivery = cast(Mapping[str, object], result["delivery"])
        stream.write(f"{delivery['platform']}: {delivery['status']}\n")
        return
    if "request" in result:
        request = cast(Mapping[str, object], result["request"])
        stream.write(f"request {request['request_id']}: {request['status']}\n")
        return
    if "confirmation" in result:
        confirmation = cast(Mapping[str, object], result["confirmation"])
        stream.write(
            f"confirmation {confirmation['intent_id']}: {confirmation['state']}\n"
        )
        return
    if "items" in result:
        stream.write(f"{len(cast(Sequence[object], result['items']))} item(s)\n")
        return
    if "migration" in result:
        migration = cast(Mapping[str, object], result["migration"])
        stream.write(f"migration {migration['mode']}: {migration['phase']}\n")
        return
    stream.write(f"{result['status']}\n")


def _write_error(stream: IO[str], json_output: bool, code: str, message: str) -> None:
    if json_output:
        document = {
            "schema": CONTROL_SCHEMA,
            "ok": False,
            "error": {"code": code, "message": message},
        }
        stream.write(json.dumps(document, sort_keys=True, separators=(",", ":")) + "\n")
    else:
        stream.write(f"error: {message}\n")


def _configure_logging(settings: LocalSettings) -> logging.Logger:
    path = settings.app.log_file
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    with suppress(OSError):
        path.parent.chmod(0o700)
    logger = logging.getLogger("post_pulsar.cli")
    logger.setLevel(logging.INFO)
    logger.propagate = False
    for handler in tuple(logger.handlers):
        handler.close()
        logger.removeHandler(handler)
    handler = RotatingFileHandler(
        path,
        maxBytes=settings.app.log_max_bytes,
        backupCount=settings.app.log_backups,
        encoding="utf-8",
    )
    handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
    logger.addHandler(handler)
    with suppress(OSError):
        path.chmod(0o600)
    return logger


def _normalize_argv(argv: tuple[str, ...]) -> tuple[str, ...]:
    global_arguments: list[str] = []
    command_arguments: list[str] = []
    index = 0
    while index < len(argv):
        value = argv[index]
        if value == "--json":
            global_arguments.append(value)
        elif value == "--config":
            global_arguments.append(value)
            index += 1
            if index < len(argv):
                global_arguments.append(argv[index])
        elif value.startswith("--config="):
            global_arguments.append(value)
        else:
            command_arguments.append(value)
        index += 1
    if not command_arguments or (
        command_arguments[0] not in _COMMANDS
        and command_arguments[0] not in {"-h", "--help", "--version"}
    ):
        command_arguments.insert(0, "run")
    return tuple(global_arguments + command_arguments)


def _add_confirmed_delivery_target(parser: argparse.ArgumentParser) -> None:
    _add_write_identity(parser)
    parser.add_argument("--bundle-key", required=True, type=_positive_integer)
    parser.add_argument("--bundle-id", required=True, type=_bundle_id)
    parser.add_argument("--fingerprint", required=True, type=_sha256)
    parser.add_argument("--platform", required=True, choices=("x", "instagram"))
    parser.add_argument("--intent-id", required=True, type=_intent_id)


def _add_write_identity(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--profile", required=True, type=_profile_id)
    parser.add_argument("--expected-revision", required=True, type=_nonnegative_integer)
    parser.add_argument("--idempotency-key", required=True, type=_identifier)


def _add_schedule_definition(
    parser: argparse.ArgumentParser, *, include_schedule_key: bool
) -> None:
    _add_write_identity(parser)
    if include_schedule_key:
        parser.add_argument("--schedule-key", required=True, type=_positive_integer)
    parser.add_argument("--schedule-id", required=True, type=_schedule_id)
    parser.add_argument("--bucket", required=True, choices=("QUEUE", "RANDOM", "REELS"))
    parser.add_argument("--timezone", required=True)
    parser.add_argument("--weekdays", required=True, type=_weekdays)
    parser.add_argument("--local-time", required=True)
    parser.add_argument("--misfire-grace-seconds", required=True, type=_misfire_grace)
    enabled = parser.add_mutually_exclusive_group(required=True)
    enabled.add_argument("--enabled", action="store_true", dest="enabled")
    enabled.add_argument("--disabled", action="store_false", dest="enabled")
    parser.add_argument("--intent-id", type=_intent_id)


def _add_bootstrap_identity(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--installation-id", required=True, type=_installation_id)
    parser.add_argument(
        "--service-mode", required=True, choices=tuple(sorted(SERVICE_MODES))
    )
    parser.add_argument("--service-identifier", required=True, type=_service_id)


def _positive_integer(value: str) -> int:
    try:
        parsed = int(value)
    except ValueError:
        raise argparse.ArgumentTypeError("must be a positive integer") from None
    if parsed <= 0:
        raise argparse.ArgumentTypeError("must be a positive integer")
    return parsed


def _nonnegative_integer(value: str) -> int:
    try:
        parsed = int(value)
    except ValueError:
        raise argparse.ArgumentTypeError("must be a non-negative integer") from None
    if parsed < 0:
        raise argparse.ArgumentTypeError("must be a non-negative integer")
    return parsed


def _page_limit(value: str) -> int:
    parsed = _positive_integer(value)
    if parsed > 500:
        raise argparse.ArgumentTypeError("must not exceed 500")
    return parsed


def _profile_id(value: str) -> str:
    if not re.fullmatch(r"[a-z0-9][a-z0-9-]{0,31}", value):
        raise argparse.ArgumentTypeError("profile ID is invalid")
    return value


def _bundle_id(value: str) -> str:
    if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_-]{0,63}", value):
        raise argparse.ArgumentTypeError("bundle ID is invalid")
    return value


def _schedule_id(value: str) -> str:
    if not re.fullmatch(r"[a-z0-9][a-z0-9-]{0,63}", value):
        raise argparse.ArgumentTypeError("schedule ID is invalid")
    return value


def _identifier(value: str) -> str:
    if not _IDENTIFIER_RE.fullmatch(value):
        raise argparse.ArgumentTypeError("identifier is invalid")
    return value


def _intent_id(value: str) -> str:
    if not re.fullmatch(r"[0-9a-f]{32}", value):
        raise argparse.ArgumentTypeError("intent ID is invalid")
    return value


def _sha256(value: str) -> str:
    if not re.fullmatch(r"[0-9a-f]{64}", value):
        raise argparse.ArgumentTypeError("fingerprint is invalid")
    return value


def _installation_id(value: str) -> str:
    if not re.fullmatch(r"[0-9a-f]{32}", value):
        raise argparse.ArgumentTypeError("installation ID is invalid")
    return value


def _service_id(value: str) -> str:
    if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.@:-]{0,127}", value):
        raise argparse.ArgumentTypeError("service identifier is invalid")
    return value


def _weekdays(value: str) -> tuple[int, ...]:
    try:
        parsed = tuple(sorted({int(item) for item in value.split(",")}))
    except ValueError:
        raise argparse.ArgumentTypeError(
            "weekdays must be comma-separated 0..6"
        ) from None
    if not parsed or any(day < 0 or day > 6 for day in parsed):
        raise argparse.ArgumentTypeError("weekdays must be comma-separated 0..6")
    return parsed


def _misfire_grace(value: str) -> int:
    parsed = _nonnegative_integer(value)
    if parsed > 86400:
        raise argparse.ArgumentTypeError("misfire grace must not exceed 86400")
    return parsed


def _iso(value: datetime | None) -> str | None:
    if value is None:
        return None
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")


def _redact_message(
    message: str,
    settings: LocalSettings | None,
    environ: Mapping[str, str] | None,
) -> str:
    if settings is None:
        return message
    environment = os.environ if environ is None else environ
    redacted = message
    for profile in settings.profiles:
        for target in (profile.x, profile.instagram):
            if target is None:
                continue
            value = environment.get(target.token_env_var, "").strip()
            if value:
                redacted = redacted.replace(value, "<redacted>")
    return redacted


__all__ = [
    "CONTROL_SCHEMA",
    "EXIT_APPROVAL_REQUIRED",
    "EXIT_AUTH",
    "EXIT_BLOCKED",
    "EXIT_CONFLICT",
    "EXIT_CONTENTION",
    "EXIT_DAEMON_UNAVAILABLE",
    "EXIT_INVALID",
    "EXIT_NO_WORK",
    "EXIT_OK",
    "EXIT_RETRYABLE",
    "EXIT_USAGE",
    "EXIT_VERSION",
    "build_parser",
    "main",
]
