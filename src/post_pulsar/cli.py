"""Deterministic local command interface for POST PULSAR."""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from collections.abc import Callable, Mapping, Sequence
from contextlib import redirect_stderr, redirect_stdout
from datetime import UTC, datetime
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import IO, Final, Literal, Never, cast

from post_pulsar.app import (
    AdapterFactory,
    OneRunApplication,
    RunOnceRequest,
    RunOutcome,
    _configured_target_snapshot,
    _default_adapter_factory,
)
from post_pulsar.config import (
    ConfigurationError,
    LocalSettings,
    SecretValue,
    load_local_settings,
    validate_publishing_credentials,
)
from post_pulsar.locking import LockContentionError, LockError, LockManager
from post_pulsar.platforms.base import (
    AdapterContractError,
    PlatformAdapter,
    PublicationSnapshot,
)
from post_pulsar.state import (
    BundleRecord,
    DeliveryRecord,
    Platform,
    StateError,
    StateRepository,
    TargetSnapshot,
)

CONTROL_SCHEMA: Final = "post-pulsar.control/v1"
EXIT_OK: Final = 0
EXIT_USAGE: Final = 2
EXIT_NO_WORK: Final = 3
EXIT_INVALID: Final = 4
EXIT_BLOCKED: Final = 5
EXIT_CONTENTION: Final = 6
EXIT_RETRYABLE: Final = 7
_COMMANDS: Final = frozenset({"run", "status", "retry", "reconcile"})
_VERSION: Final = "0.1.0"

IdentityVerifier = Callable[[PublicationSnapshot, SecretValue, Path], None]


class _UsageError(ValueError):
    pass


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
    status.add_argument("--profile", required=True)
    status.add_argument("--bundle-key", type=_positive_integer)

    retry = commands.add_parser("retry", help="enable one blocked failed delivery")
    _add_delivery_target(retry)
    retry.add_argument("--confirm", required=True, choices=("RETRY",))

    reconcile = commands.add_parser(
        "reconcile", help="resolve one ambiguous delivery from operator evidence"
    )
    _add_delivery_target(reconcile)
    resolution = reconcile.add_mutually_exclusive_group(required=True)
    resolution.add_argument("--published", metavar="REMOTE_ID")
    resolution.add_argument("--not-published", action="store_true")
    reconcile.add_argument("--confirm", required=True, choices=("RECONCILE",))
    return parser


def main(
    argv: Sequence[str] | None = None,
    *,
    stdout: IO[str] | None = None,
    stderr: IO[str] | None = None,
    environ: Mapping[str, str] | None = None,
    adapter_factory: AdapterFactory | None = None,
    identity_verifier: IdentityVerifier | None = None,
    clock: Callable[[], datetime] | None = None,
) -> int:
    """Execute one CLI request and return a stable process exit code."""
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
    except (ConfigurationError, StateError, LockError, AdapterContractError) as exc:
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
) -> tuple[Mapping[str, object], int]:
    command = cast(str, arguments.command)
    if command == "status":
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
        delivery = _retry(
            settings,
            arguments.profile,
            arguments.bundle_key,
            arguments.platform,
            environ=environ,
            identity_verifier=identity_verifier,
        )
        return {"delivery": _delivery_document(delivery)}, EXIT_OK
    if command == "reconcile":
        delivery = _reconcile(
            settings,
            arguments.profile,
            arguments.bundle_key,
            arguments.platform,
            published_remote_id=arguments.published,
        )
        return {"delivery": _delivery_document(delivery)}, EXIT_OK
    raise _UsageError("a command is required")


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
    with StateRepository.open_read_only(database) as repository:
        with repository.read_snapshot():
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


def _retry(
    settings: LocalSettings,
    profile_id: str,
    bundle_key: int,
    platform: Platform,
    *,
    environ: Mapping[str, str] | None,
    identity_verifier: IdentityVerifier,
) -> DeliveryRecord:
    profile = settings.profile(profile_id)
    locks = LockManager(settings.app.state_directory)
    database = settings.app.state_directory / "post_pulsar.sqlite3"
    with locks.acquire_instance() as instance:
        with locks.acquire_profiles(instance, (profile_id,)):
            with StateRepository.open_existing(database) as repository:
                bundle = repository.get_bundle(bundle_key)
                if bundle.profile_id != profile_id:
                    raise StateError("bundle does not belong to the requested profile")
                snapshot = _target_snapshot(repository, bundle_key, platform)
                current = _configured_target_snapshot(profile, platform)
                if current != snapshot:
                    raise StateError(
                        "current target configuration differs from snapshot"
                    )
                credentials = validate_publishing_credentials(
                    settings, profile_id, (platform,), environ
                )
                publication = PublicationSnapshot(
                    profile_id,
                    bundle.bundle_key,
                    bundle.bundle_id,
                    bundle.fingerprint,
                    bundle.source_bucket,
                    snapshot,
                )
                identity_verifier(
                    publication,
                    credentials.for_target(platform),
                    settings.app.state_directory / "staging" / "private",
                )
                return repository.operator_retry(
                    bundle_key,
                    platform,
                    expected_bundle_revision=bundle.revision,
                    validated_snapshot=snapshot,
                )


def _reconcile(
    settings: LocalSettings,
    profile_id: str,
    bundle_key: int,
    platform: Platform,
    *,
    published_remote_id: str | None,
) -> DeliveryRecord:
    settings.profile(profile_id)
    locks = LockManager(settings.app.state_directory)
    database = settings.app.state_directory / "post_pulsar.sqlite3"
    with locks.acquire_instance() as instance:
        with locks.acquire_profiles(instance, (profile_id,)):
            with StateRepository.open_existing(database) as repository:
                bundle = repository.get_bundle(bundle_key)
                if bundle.profile_id != profile_id:
                    raise StateError("bundle does not belong to the requested profile")
                _target_snapshot(repository, bundle_key, platform)
                return repository.operator_reconcile(
                    bundle_key,
                    platform,
                    published_remote_id=published_remote_id,
                    expected_bundle_revision=bundle.revision,
                )


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
        bundles = cast(Sequence[Mapping[str, object]], result["bundles"])
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
    try:
        path.parent.chmod(0o700)
    except OSError:
        pass
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
    try:
        path.chmod(0o600)
    except OSError:
        pass
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


def _add_delivery_target(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--profile", required=True)
    parser.add_argument("--bundle-key", required=True, type=_positive_integer)
    parser.add_argument("--platform", required=True, choices=("x", "instagram"))


def _positive_integer(value: str) -> int:
    try:
        parsed = int(value)
    except ValueError:
        raise argparse.ArgumentTypeError("must be a positive integer") from None
    if parsed <= 0:
        raise argparse.ArgumentTypeError("must be a positive integer")
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
    "EXIT_BLOCKED",
    "EXIT_CONTENTION",
    "EXIT_INVALID",
    "EXIT_NO_WORK",
    "EXIT_OK",
    "EXIT_RETRYABLE",
    "EXIT_USAGE",
    "build_parser",
    "main",
]
