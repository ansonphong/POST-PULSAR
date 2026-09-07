"""Authenticated, bounded local control application for ``post-pulsar.control/v1``."""

from __future__ import annotations

import hashlib
import hmac
import json
import os
import re
import secrets
import stat
import time
from collections.abc import Callable, Mapping
from dataclasses import asdict, dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Final, Protocol, cast
from urllib.parse import parse_qs, unquote, urlsplit

from post_pulsar import __version__
from post_pulsar.content import scan_inbox
from post_pulsar.secure_files import RecordPolicy
from post_pulsar.state import (
    SCHEMA_VERSION,
    BundleRecord,
    ConflictError,
    ProfileRecord,
    StateError,
    StateRepository,
    StateValidationError,
    TransitionError,
    validate_action_arguments,
)

CONTROL_SCHEMA: Final = "post-pulsar.control/v1"
CONTROL_API_MAJOR: Final = 1
DEFAULT_MAX_BODY: Final = 65536
DEFAULT_MAX_RESULTS: Final = 100
_TOKEN_RE: Final = re.compile(r"[0-9a-f]{64}\Z")
_ID_RE: Final = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.:-]{0,127}\Z")
_PUBLISH_ACTIONS: Final = frozenset(
    {
        "admit_draft",
        "enqueue",
        "run_now",
        "publish_now",
        "resume",
        "schedule_create",
        "schedule_update",
        "schedule_enable",
        "retry",
        "reconcile",
    }
)
_FORBIDDEN_INPUT_KEYS: Final = frozenset(
    {
        "path",
        "raw_path",
        "sql",
        "shell",
        "command",
        "token",
        "platform_token",
        "platform_response",
        "platform_response_body",
    }
)
SUPPORTED_OPERATIONS: Final = (
    "health",
    "capabilities",
    "shutdown",
    "status",
    "dashboard",
    "profiles",
    "buckets",
    "inspectBundle",
    "previewBundle",
    "editCaption",
    "editAlt",
    "readyBundle",
    "enqueue",
    "schedules",
    "createSchedule",
    "modifySchedule",
    "disableSchedule",
    "requests",
    "requestStatus",
    "pause",
    "resume",
    "runNow",
    "cancelPending",
    "deletePending",
    "retry",
    "reconcile",
    "createConfirmation",
    "confirmationStatus",
    "consumeConfirmation",
    "operatorApprove",
    "operatorCreateConfirmation",
)


class ControlSecurityError(RuntimeError):
    """A local credential or request failed closed."""


class _TTYInput(Protocol):
    def isatty(self) -> bool: ...
    def readline(self) -> str: ...


@dataclass(frozen=True, slots=True)
class ControlRequest:
    method: str
    target: str
    headers: Mapping[str, str]
    body: bytes = b""


@dataclass(frozen=True, slots=True)
class ControlResponse:
    status: int
    headers: Mapping[str, str]
    body: bytes


def rotate_agent_capability(
    path: str | Path, *, policy: RecordPolicy | None = None
) -> str:
    """Generate and atomically install a fresh 256-bit bearer capability."""
    destination = Path(path)
    _assert_secret_target(destination, allow_missing=True, policy=policy)
    token = secrets.token_hex(32)
    _atomic_owner_write(destination, (token + "\n").encode("ascii"), policy=policy)
    return token


def load_agent_capability(
    path: str | Path, *, policy: RecordPolicy | None = None
) -> str:
    """Load a capability only from an owner-only regular non-symlink file."""
    source = Path(path)
    _assert_secret_target(source, allow_missing=False, policy=policy)
    token = source.read_text(encoding="ascii").strip()
    if not _TOKEN_RE.fullmatch(token):
        raise ControlSecurityError("agent capability file is invalid")
    return token


def initialize_operator_secret(
    path: str | Path,
    *,
    input_stream: _TTYInput,
    max_failures: int = 5,
    lockout_seconds: int = 300,
    policy: RecordPolicy | None = None,
) -> None:
    """Read a secret once from a real terminal and persist only a scrypt verifier."""
    if not input_stream.isatty():
        raise ControlSecurityError("operator secret initialization requires a real TTY")
    if not 3 <= max_failures <= 10 or not 30 <= lockout_seconds <= 3600:
        raise ControlSecurityError("operator lockout policy is invalid")
    secret = input_stream.readline().rstrip("\r\n")
    if not 12 <= len(secret) <= 1024:
        raise ControlSecurityError("operator secret length is invalid")
    salt = secrets.token_bytes(16)
    verifier = hashlib.scrypt(
        secret.encode("utf-8"), salt=salt, n=1 << 14, r=8, p=1, dklen=32
    )
    document = {
        "version": 1,
        "kdf": "scrypt",
        "n": 1 << 14,
        "r": 8,
        "p": 1,
        "salt": salt.hex(),
        "verifier": verifier.hex(),
        "failed_attempts": 0,
        "locked_until": 0.0,
        "max_failures": max_failures,
        "lockout_seconds": lockout_seconds,
    }
    _atomic_owner_write(
        Path(path), _canonical_json(document), policy=policy, agent_read=False
    )


def verify_operator_secret(
    path: str | Path,
    supplied: str,
    *,
    now: float | None = None,
    policy: RecordPolicy | None = None,
) -> bool:
    """Verify and atomically checkpoint a bounded failed-attempt lockout."""
    moment = time.time() if now is None else now
    source = Path(path)
    _assert_secret_target(source, allow_missing=False, policy=policy, agent_read=False)
    try:
        document = json.loads(source.read_text(encoding="utf-8"))
        if not isinstance(document, dict) or document.get("kdf") != "scrypt":
            raise ValueError
        locked_until = float(document["locked_until"])
        failures = int(document["failed_attempts"])
        maximum = int(document["max_failures"])
        lockout = int(document["lockout_seconds"])
        salt = bytes.fromhex(str(document["salt"]))
        expected = bytes.fromhex(str(document["verifier"]))
        actual = hashlib.scrypt(
            supplied.encode("utf-8"),
            salt=salt,
            n=int(document["n"]),
            r=int(document["r"]),
            p=int(document["p"]),
            dklen=len(expected),
        )
    except (KeyError, TypeError, ValueError, OSError):
        raise ControlSecurityError("operator verifier file is invalid") from None
    if moment < locked_until:
        hmac.compare_digest(actual, expected)
        return False
    accepted = hmac.compare_digest(actual, expected)
    if accepted:
        document["failed_attempts"] = 0
        document["locked_until"] = 0.0
    else:
        failures += 1
        if failures >= maximum:
            failures = 0
            document["locked_until"] = moment + lockout
        document["failed_attempts"] = failures
    _atomic_owner_write(
        source, _canonical_json(document), policy=policy, agent_read=False
    )
    return accepted


class ControlApplication:
    """Socket-independent request dispatcher; each call opens its own SQLite handle."""

    def __init__(
        self,
        database_path: str | Path,
        capability_file: str | Path,
        *,
        operator_verifier_file: str | Path | None = None,
        allow_agent_publish: bool = False,
        max_body_bytes: int = DEFAULT_MAX_BODY,
        max_results: int = DEFAULT_MAX_RESULTS,
        confirmation_ttl_seconds: int = 300,
        record_policy: RecordPolicy | None = None,
    ) -> None:
        self._database = Path(database_path)
        self._record_policy = record_policy
        self._capability_file = Path(capability_file)
        self._operator_verifier_file = (
            None if operator_verifier_file is None else Path(operator_verifier_file)
        )
        self._allow_agent_publish = allow_agent_publish
        if not 1024 <= max_body_bytes <= 1048576 or not 1 <= max_results <= 500:
            raise ControlSecurityError("control limits are invalid")
        self._max_body = max_body_bytes
        self._max_results = max_results
        self._intent_ttl = confirmation_ttl_seconds
        self._shutdown: Callable[[], None] | None = None
        self._startup_nonce: str | None = None

    def bind_shutdown(self, callback: Callable[[], None], startup_nonce: str) -> None:
        """Bind graceful shutdown to this authenticated daemon incarnation."""
        if not startup_nonce or self._shutdown is not None:
            raise ControlSecurityError("shutdown incarnation binding is invalid")
        self._shutdown = callback
        self._startup_nonce = startup_nonce

    def handle(self, request: ControlRequest) -> ControlResponse:
        """Validate authentication and route one bounded local request."""
        request_id = secrets.token_hex(8)
        headers = _normalized_headers(request.headers)
        try:
            if len(request.body) > self._max_body:
                return self._error(413, "body_too_large", request_id)
            self._authenticate(headers)
            method = request.method.upper()
            path, query = _safe_target(request.target)
            version = headers.get("x-post-pulsar-control-version")
            if method not in {"GET", "HEAD"} and version not in {None, "1"}:
                return self._error(409, "incompatible_control_version", request_id)
            body = _parse_body(request.body)
            if method not in {"GET", "HEAD"}:
                _reject_forbidden_input(body)
            with StateRepository.open_existing(self._database) as repository:
                result, status = self._dispatch(
                    repository, method, path, query, headers, body
                )
            return self._response(status, result, request_id)
        except ControlSecurityError:
            return self._error(401, "authentication_required", request_id)
        except _PreconditionError:
            return self._error(428, "precondition_required", request_id)
        except ConflictError:
            return self._error(409, "revision_or_idempotency_conflict", request_id)
        except TransitionError:
            return self._error(409, "invalid_state_transition", request_id)
        except StateValidationError:
            return self._error(422, "invalid_request", request_id)
        except (StateError, OSError, ValueError, json.JSONDecodeError):
            return self._error(400, "invalid_request", request_id)
        except Exception:
            return self._error(500, "internal_error", request_id)

    def _dispatch(
        self,
        repository: StateRepository,
        method: str,
        path: str,
        query: Mapping[str, list[str]],
        headers: Mapping[str, str],
        body: Mapping[str, object],
    ) -> tuple[object, int]:
        segments = tuple(item for item in path.split("/") if item)
        if segments[:2] != ("control", "v1"):
            raise StateValidationError("unknown control route")
        route = segments[2:]
        if method == "POST" and route == ("shutdown",):
            _exact_body(body, {"startup_nonce"}, {"startup_nonce"})
            nonce = _text(body, "startup_nonce")
            if self._shutdown is None or self._startup_nonce is None:
                raise TransitionError("daemon shutdown is unavailable")
            if not hmac.compare_digest(nonce, self._startup_nonce):
                raise ConflictError("daemon incarnation changed")
            self._shutdown()
            return {"status": "stopping"}, 202
        if method == "GET" and route == ("health",):
            failures = repository.control_events(limit=10)
            return {
                "status": "degraded" if failures else "ok",
                "callback_failures": failures,
            }, 200
        if method == "GET" and route == ("capabilities",):
            return {
                "core_semver": __version__,
                "control_api_major": CONTROL_API_MAJOR,
                "state_schema": SCHEMA_VERSION,
                "operations": list(SUPPORTED_OPERATIONS),
                "allow_agent_publish": self._allow_agent_publish,
            }, 200
        if method == "GET" and route in {("status",), ("dashboard",)}:
            pause = repository.get_pause_state()
            profile_filter = _one(query, "profile_id")
            bundle_filter = _one(query, "bundle_key")
            if profile_filter is not None:
                profile = repository.get_profile(profile_filter)
                if bundle_filter is None:
                    bundles = repository.list_protected_bundles(profile.profile_id)
                else:
                    bundle = repository.get_bundle(_positive(bundle_filter))
                    if bundle.profile_id != profile.profile_id:
                        raise StateValidationError(
                            "bundle does not belong to requested profile"
                        )
                    bundles = (bundle,)
                return {
                    "profile_id": profile.profile_id,
                    "bundles": [_bundle(repository, item) for item in bundles],
                }, 200
            if bundle_filter is not None:
                raise StateValidationError("bundle filter requires exact profile")
            return {
                "paused": pause.paused,
                "pause_requested": pause.pause_requested,
                "revision": pause.revision,
                "due_work": repository.has_due_work(),
                "profiles": len(repository.list_profiles(limit=self._max_results)),
            }, 200
        if method == "GET" and route == ("profiles",):
            limit = _page_limit(query, self._max_results)
            values = repository.list_profiles(
                after_profile_id=_one(query, "cursor"), limit=limit
            )
            return _page(
                [_profile(item) for item in values],
                values[-1].profile_id if values else None,
            ), 200
        if method == "GET" and len(route) == 2 and route[0] == "profiles":
            return _profile(repository.get_profile(route[1])), 200
        if (
            method == "GET"
            and len(route) == 3
            and route[0] == "profiles"
            and route[2] == "buckets"
        ):
            repository.get_profile(route[1])
            return {"items": ["DRAFTS", "QUEUE", "RANDOM", "REELS"]}, 200
        if (
            method == "GET"
            and len(route) in {6, 7}
            and route[0] == "profiles"
            and route[2] == "buckets"
            and route[4] == "bundles"
        ):
            profile = repository.get_profile(route[1])
            bucket = route[3]
            if bucket not in {"DRAFTS", "QUEUE", "RANDOM", "REELS"}:
                raise StateValidationError("bucket is invalid")
            directory = profile.account_root / bucket
            scan = (
                scan_inbox(directory)
                if bucket == "DRAFTS"
                else scan_inbox(directory / route[5])
            )
            matches = [item for item in scan.bundles if item.bundle_id == route[5]]
            if len(matches) != 1:
                raise StateValidationError("bundle is missing or ambiguous")
            bundle = matches[0]
            return {
                "profile_id": route[1],
                "bucket": bucket,
                "bundle_id": bundle.bundle_id,
                "fingerprint": bundle.fingerprint,
                "members": [
                    {
                        "name": item.relative_name,
                        "role": item.role,
                        "ordinal": item.ordinal,
                        "media_kind": item.media_kind,
                        "mime_type": item.mime_type,
                        "size_bytes": item.size_bytes,
                        "sha256": item.sha256,
                    }
                    for item in bundle.member_snapshots
                ],
                "preview": len(route) == 7 and route[6] == "preview",
            }, 200
        if method == "GET" and route == ("schedules",):
            profile_id = _required_profile_query(query)
            values = repository.list_schedules(
                profile_id=profile_id,
                after_schedule_key=_query_cursor(query),
                limit=_page_limit(query, self._max_results),
            )
            return _page(
                [_schedule(item) for item in values],
                values[-1].schedule_key if values else None,
            ), 200
        if method == "GET" and route == ("requests",):
            profile_id = _required_profile_query(query)
            values = repository.list_run_requests(
                profile_id=profile_id,
                after_request_id=_query_cursor(query),
                limit=_page_limit(query, self._max_results),
            )
            return _page(
                [_request(item) for item in values],
                values[-1].request_id if values else None,
            ), 200
        if method == "GET" and len(route) == 2 and route[0] == "requests":
            return _request(repository.get_run_request(_positive(route[1]))), 200
        if method == "GET" and len(route) == 2 and route[0] == "confirmations":
            return _intent(repository.get_confirmation_intent(route[1])), 200
        if method == "POST" and route == ("confirmations",):
            _exact_body(
                body,
                {
                    "action",
                    "profile_id",
                    "resource_revision",
                    "consequence",
                    "arguments",
                    "bundle_key",
                    "schedule_key",
                    "fingerprint",
                },
                {
                    "action",
                    "profile_id",
                    "resource_revision",
                    "consequence",
                    "arguments",
                },
            )
            action = _text(body, "action")
            validate_action_arguments(action, _object(body, "arguments", default={}))
            if action in _PUBLISH_ACTIONS and not self._allow_agent_publish:
                return {"code": "agent_publish_disabled"}, 403
            key, revision = _write_headers(headers)
            profile_id = _text(body, "profile_id")
            resource_revision = _integer(body, "resource_revision")
            if revision != resource_revision:
                raise ConflictError("intent revision header drift")
            intent = repository.create_confirmation_intent(
                action=action,
                arguments=_object(body, "arguments", default={}),
                profile_id=profile_id,
                resource_revision=resource_revision,
                fingerprint=_optional_text(body, "fingerprint"),
                consequence=_text(body, "consequence", default=f"Authorize {action}"),
                expires_at=datetime.now(UTC) + timedelta(seconds=self._intent_ttl),
                bundle_key=_optional_integer(body, "bundle_key"),
                schedule_key=_optional_integer(body, "schedule_key"),
                idempotency_key=key,
                origin="agent",
            )
            return _intent(intent), 201
        if method == "POST" and route == ("operator", "confirmations"):
            _exact_body(
                body,
                {
                    "action",
                    "profile_id",
                    "resource_revision",
                    "consequence",
                    "arguments",
                    "bundle_key",
                    "schedule_key",
                    "fingerprint",
                },
                {
                    "action",
                    "profile_id",
                    "resource_revision",
                    "consequence",
                    "arguments",
                },
            )
            if headers.get("x-post-pulsar-principal") != "operator":
                raise ControlSecurityError("operator principal is required")
            self._operator_auth(headers)
            action = _text(body, "action")
            key, revision = _write_headers(headers)
            resource_revision = _integer(body, "resource_revision")
            if revision != resource_revision:
                raise ConflictError("intent revision header drift")
            intent = repository.create_confirmation_intent(
                action=action,
                arguments=_object(body, "arguments", default={}),
                profile_id=_text(body, "profile_id"),
                resource_revision=resource_revision,
                fingerprint=_optional_text(body, "fingerprint"),
                consequence=_text(body, "consequence"),
                expires_at=datetime.now(UTC) + timedelta(seconds=self._intent_ttl),
                bundle_key=_optional_integer(body, "bundle_key"),
                schedule_key=_optional_integer(body, "schedule_key"),
                idempotency_key=key,
                origin="operator",
            )
            return _intent(intent), 201
        if (
            method == "POST"
            and len(route) == 3
            and route[0] == "operator"
            and route[1] == "confirmations"
            and route[2].endswith(":approve")
        ):
            raise StateValidationError("operator route identity is invalid")
        if (
            method == "POST"
            and len(route) == 4
            and route[:2] == ("operator", "confirmations")
            and route[3] == "approve"
        ):
            if headers.get("x-post-pulsar-principal") != "operator":
                raise ControlSecurityError("operator principal is required")
            self._operator_auth(headers)
            _key, revision = _write_headers(headers)
            return _intent(
                repository.approve_confirmation_intent(
                    route[2], expected_revision=revision
                )
            ), 200
        if (
            method == "POST"
            and len(route) == 3
            and route[0] == "confirmations"
            and route[2] == "consume"
        ):
            key, revision = _write_headers(headers)
            _exact_body(
                body,
                {
                    "action",
                    "profile_id",
                    "resource_revision",
                    "arguments",
                    "bundle_key",
                    "schedule_key",
                    "fingerprint",
                },
                {"action", "profile_id", "resource_revision", "arguments"},
            )
            if _integer(body, "resource_revision") != revision:
                raise ConflictError("intent revision header drift")
            action = _text(body, "action")
            intent = repository.get_confirmation_intent(route[1])
            if (
                action in _PUBLISH_ACTIONS
                and intent.origin == "agent"
                and not self._allow_agent_publish
            ):
                return {"code": "agent_publish_disabled"}, 403
            result = repository.consume_intent_with_request(
                intent_id=route[1],
                action=action,
                arguments=_object(body, "arguments", default={}),
                profile_id=_text(body, "profile_id"),
                resource_revision=revision,
                fingerprint=_optional_text(body, "fingerprint"),
                idempotency_key=key,
                bundle_key=_optional_integer(body, "bundle_key"),
                schedule_key=_optional_integer(body, "schedule_key"),
            )
            return _request(result), 202
        action = _action_for_route(method, route, body)
        if action is not None:
            key, revision = _write_headers(headers)
            if action in {"edit_caption", "edit_alt"}:
                _exact_body(body, {"text"}, {"text"})
            elif action == "admit_draft":
                _exact_body(body, {"arguments"}, {"arguments"})
            else:
                _exact_body(
                    body,
                    {"profile_id", "arguments", "bundle_key", "schedule_key"},
                    {"profile_id"},
                )
            profile_id = (
                route[1]
                if len(route) >= 6 and route[0] == "profiles"
                else _text(body, "profile_id")
            )
            arguments = dict(_object(body, "arguments", default={}))
            if len(route) >= 6 and route[0] == "profiles":
                if action in {"edit_caption", "edit_alt"}:
                    arguments.update({"bucket": route[3], "bundle_id": route[5]})
                elif arguments.get("bundle_id") != route[5]:
                    raise StateValidationError("draft path and body identity disagree")
                if "text" in body:
                    text_value = body["text"]
                    if not isinstance(text_value, str) or len(text_value) > 10000:
                        raise StateValidationError("text is invalid")
                    arguments["text"] = text_value
                if action in {"edit_caption", "edit_alt"}:
                    draft_scan = scan_inbox(
                        repository.get_profile(profile_id).account_root / "DRAFTS"
                    )
                    matches = [
                        item
                        for item in draft_scan.bundles
                        if item.bundle_id == route[5]
                    ]
                    if len(matches) != 1:
                        raise StateValidationError(
                            "draft bundle is missing or ambiguous"
                        )
                    arguments["fingerprint"] = matches[0].fingerprint
            schedule_key = _optional_integer(body, "schedule_key")
            route_schedule_key = _route_schedule_key(route)
            if route_schedule_key is not None:
                if schedule_key != route_schedule_key:
                    raise StateValidationError("schedule path and body keys disagree")
                schedule_key = route_schedule_key
            validate_action_arguments(action, arguments)
            enabling = _publication_enabling(
                repository, action, arguments, schedule_key
            )
            if enabling:
                if not self._allow_agent_publish:
                    return {"code": "agent_publish_disabled"}, 403
                raise TransitionError("approved confirmation intent must be consumed")
            result = repository.create_run_request(
                profile_id=profile_id,
                action=action,
                arguments=arguments,
                idempotency_key=key,
                expected_revision=revision,
                bundle_key=_optional_integer(body, "bundle_key"),
                schedule_key=schedule_key,
            )
            return _request(result), 202
        raise StateValidationError("unknown control route")

    def _authenticate(self, headers: Mapping[str, str]) -> None:
        authorization = headers.get("authorization", "")
        supplied = authorization[7:] if authorization.startswith("Bearer ") else ""
        expected = load_agent_capability(
            self._capability_file, policy=self._record_policy
        )
        candidate = supplied if _TOKEN_RE.fullmatch(supplied) else "0" * 64
        if not hmac.compare_digest(candidate.encode("ascii"), expected.encode("ascii")):
            raise ControlSecurityError("authentication failed")

    def _operator_auth(self, headers: Mapping[str, str]) -> None:
        if self._operator_verifier_file is None:
            raise ControlSecurityError("operator approval is unavailable")
        supplied = headers.get("x-post-pulsar-operator-secret", "")
        if not verify_operator_secret(
            self._operator_verifier_file, supplied, policy=self._record_policy
        ):
            raise ControlSecurityError("operator authentication failed")

    @staticmethod
    def _response(status: int, data: object, request_id: str) -> ControlResponse:
        return ControlResponse(
            status,
            _response_headers(request_id),
            _canonical_json(
                {
                    "schema": CONTROL_SCHEMA,
                    "ok": True,
                    "data": data,
                    "request_id": request_id,
                }
            ),
        )

    @staticmethod
    def _error(status: int, code: str, request_id: str) -> ControlResponse:
        return ControlResponse(
            status,
            _response_headers(request_id),
            _canonical_json(
                {
                    "schema": CONTROL_SCHEMA,
                    "ok": False,
                    "error": {"code": code, "message": code.replace("_", " ")},
                    "request_id": request_id,
                }
            ),
        )


def _action_for_route(
    method: str, route: tuple[str, ...], body: Mapping[str, object]
) -> str | None:
    if (
        method == "PATCH"
        and len(route) == 7
        and route[0] == "profiles"
        and route[2] == "buckets"
        and route[3] == "DRAFTS"
        and route[4] == "bundles"
        and route[6] in {"caption", "alt"}
    ):
        return "edit_caption" if route[6] == "caption" else "edit_alt"
    if (
        method == "POST"
        and len(route) == 7
        and route[0] == "profiles"
        and route[2] == "buckets"
        and route[3] == "DRAFTS"
        and route[4] == "bundles"
        and route[6] == "ready"
    ):
        return "admit_draft"
    if method == "PATCH" and len(route) == 2 and route[0] == "schedules":
        _positive(route[1])
        return "schedule_update"
    if (
        method == "POST"
        and len(route) == 3
        and route[0] == "schedules"
        and route[2] == "disable"
    ):
        _positive(route[1])
        return "schedule_disable"
    if method != "POST":
        return None
    fixed = {
        ("pause",): "pause",
        ("resume",): "resume",
        ("run-now",): "run_now",
        ("enqueue",): "enqueue",
        ("cancel-pending",): "cancel",
        ("delete-pending",): "delete",
        ("retry",): "retry",
        ("reconcile",): "reconcile",
    }
    if route in fixed:
        return fixed[route]
    if route == ("schedules",):
        return "schedule_create"
    return None


def _route_schedule_key(route: tuple[str, ...]) -> int | None:
    if len(route) >= 2 and route[0] == "schedules" and route[1] != "disable":
        return _positive(route[1])
    return None


def _publication_enabling(
    repository: StateRepository,
    action: str,
    arguments: Mapping[str, object],
    schedule_key: int | None,
) -> bool:
    if action == "resume":
        return repository.has_due_work()
    if action == "schedule_create":
        return arguments.get("enabled") is True
    if action == "schedule_update":
        if arguments.get("enabled") is True:
            return True
        if schedule_key is None:
            return True
        return repository.get_schedule(schedule_key).enabled
    return action in _PUBLISH_ACTIONS


def _write_headers(headers: Mapping[str, str]) -> tuple[str, int]:
    key = headers.get("idempotency-key", "")
    if not _ID_RE.fullmatch(key):
        raise _PreconditionError
    match = re.fullmatch(r'"?([0-9]+)"?', headers.get("if-match", ""))
    if match is None:
        raise _PreconditionError
    return key, int(match.group(1))


class _PreconditionError(StateValidationError):
    """A required optimistic-concurrency header was absent or malformed."""


def _safe_target(target: str) -> tuple[str, Mapping[str, list[str]]]:
    parsed = urlsplit(target)
    if (
        parsed.scheme
        or parsed.netloc
        or parsed.fragment
        or "%2f" in target.casefold()
        or "%5c" in target.casefold()
    ):
        raise StateValidationError("request target is invalid")
    path = unquote(parsed.path)
    if ".." in path.split("/") or "\\" in path or len(path) > 2048:
        raise StateValidationError("request target is invalid")
    query = parse_qs(parsed.query, keep_blank_values=True, max_num_fields=8)
    if any(len(values) != 1 for values in query.values()):
        raise StateValidationError("query field is ambiguous")
    return path.rstrip("/") or "/", query


def _parse_body(body: bytes) -> Mapping[str, object]:
    if not body:
        return dict()
    value = json.loads(body.decode("utf-8"))
    if not isinstance(value, dict):
        raise StateValidationError("request body must be an object")
    return cast(Mapping[str, object], value)


def _reject_forbidden_input(value: object) -> None:
    if isinstance(value, dict):
        for key, child in value.items():
            if str(key).casefold() in _FORBIDDEN_INPUT_KEYS:
                raise StateValidationError("request contains a forbidden field")
            _reject_forbidden_input(child)
    elif isinstance(value, list):
        for child in value:
            _reject_forbidden_input(child)


def _normalized_headers(headers: Mapping[str, str]) -> dict[str, str]:
    result: dict[str, str] = {}
    for key, value in headers.items():
        normalized = key.casefold()
        if normalized in result or "\r" in value or "\n" in value:
            raise ControlSecurityError("request headers are invalid")
        result[normalized] = value
    return result


def _response_headers(request_id: str) -> Mapping[str, str]:
    return {
        "Content-Type": "application/json",
        "Cache-Control": "no-store",
        "X-Content-Type-Options": "nosniff",
        "X-Post-Pulsar-Principal": "core",
        "X-Request-ID": request_id,
    }


def _canonical_json(value: object) -> bytes:
    return (
        json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True)
        + "\n"
    ).encode("utf-8")


def _assert_secret_target(
    path: Path,
    *,
    allow_missing: bool,
    policy: RecordPolicy | None = None,
    agent_read: bool = True,
) -> None:
    from post_pulsar.secure_files import SecureFileError, assert_owner_file

    try:
        assert_owner_file(
            path, allow_missing=allow_missing, policy=policy, agent_read=agent_read
        )
    except (OSError, SecureFileError):
        raise ControlSecurityError("credential file is unsafe or unavailable") from None
    try:
        metadata = path.lstat()
    except FileNotFoundError:
        if allow_missing:
            return
        raise ControlSecurityError("credential file is missing") from None
    if (
        stat.S_ISLNK(metadata.st_mode)
        or not stat.S_ISREG(metadata.st_mode)
        or metadata.st_nlink != 1
    ):
        raise ControlSecurityError("credential file is unsafe")
    if policy is None and os.name != "nt" and metadata.st_mode & 0o077:
        raise ControlSecurityError("credential file permissions are too broad")


def _atomic_owner_write(
    path: Path,
    payload: bytes,
    *,
    policy: RecordPolicy | None = None,
    agent_read: bool = True,
) -> None:
    from post_pulsar.secure_files import SecureFileError, atomic_owner_write

    _assert_secret_target(
        path, allow_missing=True, policy=policy, agent_read=agent_read
    )
    try:
        atomic_owner_write(path, payload, policy=policy, agent_read=agent_read)
    except (OSError, SecureFileError):
        raise ControlSecurityError("credential file write is unsafe") from None


def _one(query: Mapping[str, list[str]], key: str) -> str | None:
    values = query.get(key)
    return None if values is None else values[0]


def _page_limit(query: Mapping[str, list[str]], maximum: int) -> int:
    raw = _one(query, "limit")
    value = maximum if raw is None else int(raw)
    if not 1 <= value <= maximum:
        raise StateValidationError("page limit is invalid")
    return value


def _query_cursor(query: Mapping[str, list[str]]) -> int:
    raw = _one(query, "cursor")
    value = 0 if raw is None else int(raw)
    if value < 0:
        raise StateValidationError("page cursor is invalid")
    return value


def _required_profile_query(query: Mapping[str, list[str]]) -> str:
    value = _one(query, "profile_id")
    if value is None:
        raise StateValidationError("explicit profile query is required")
    return value


def _page(items: list[object], cursor: object | None) -> Mapping[str, object]:
    return {"items": items, "next_cursor": cursor}


def _text(body: Mapping[str, object], key: str, default: str | None = None) -> str:
    value = body.get(key, default)
    if not isinstance(value, str) or not value or len(value) > 512:
        raise StateValidationError(f"{key} is invalid")
    return value


def _exact_body(
    body: Mapping[str, object], allowed: set[str], required: set[str]
) -> None:
    if set(body) - allowed or required - set(body):
        raise StateValidationError("request fields do not match the exact contract")


def _optional_text(body: Mapping[str, object], key: str) -> str | None:
    value = body.get(key)
    return None if value is None else _text(body, key)


def _integer(body: Mapping[str, object], key: str) -> int:
    value = body.get(key)
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise StateValidationError(f"{key} is invalid")
    return value


def _optional_integer(body: Mapping[str, object], key: str) -> int | None:
    return None if body.get(key) is None else _integer(body, key)


def _positive(value: str) -> int:
    result = int(value)
    if result <= 0:
        raise StateValidationError("identifier is invalid")
    return result


def _object(
    body: Mapping[str, object], key: str, *, default: Mapping[str, object]
) -> Mapping[str, object]:
    value = body.get(key, default)
    if not isinstance(value, dict):
        raise StateValidationError(f"{key} must be an object")
    return cast(Mapping[str, object], value)


def _profile(item: ProfileRecord) -> Mapping[str, object]:
    return {
        "profile_id": item.profile_id,
        "revision": item.revision,
    }


def _schedule(value: object) -> Mapping[str, object]:
    return asdict(value)  # type: ignore[arg-type]


def _request(value: object) -> Mapping[str, object]:
    return asdict(value)  # type: ignore[arg-type]


def _bundle(repository: StateRepository, item: BundleRecord) -> Mapping[str, object]:
    return {
        "bundle_key": item.bundle_key,
        "bundle_id": item.bundle_id,
        "bucket": item.source_bucket,
        "status": item.status,
        "revision": item.revision,
        "archive_path": item.archive_path,
        "warnings": sorted(
            {
                event["event_code"]
                for event in repository.list_events(item.bundle_key)
                if event["event_type"] == "warning"
            }
        ),
        "deliveries": [
            {
                **asdict(delivery),
                "next_attempt_at": (
                    None
                    if delivery.next_attempt_at is None
                    else delivery.next_attempt_at.isoformat()
                ),
            }
            for delivery in repository.list_bundle_deliveries(item.bundle_key)
        ],
    }


def _intent(value: object) -> Mapping[str, object]:
    result = asdict(value)  # type: ignore[arg-type]
    result["expires_at"] = cast(datetime, result["expires_at"]).isoformat()
    return result


__all__ = [
    "CONTROL_API_MAJOR",
    "CONTROL_SCHEMA",
    "ControlApplication",
    "ControlRequest",
    "ControlResponse",
    "ControlSecurityError",
    "initialize_operator_secret",
    "load_agent_capability",
    "rotate_agent_capability",
    "verify_operator_secret",
]
