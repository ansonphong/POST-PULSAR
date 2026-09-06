"""Bounded, redacted, single-target HTTP transport for official APIs."""

from __future__ import annotations

import json
import math
import re
import time
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from datetime import UTC, datetime
from email.utils import parsedate_to_datetime
from typing import Final, Literal, TypeAlias
from urllib.parse import unquote, urlsplit

import httpx

from post_pulsar.config import ConfigurationError, PublishingCredentials, SecretValue
from post_pulsar.platforms.base import (
    AdapterContractError,
    PublicationSnapshot,
    RetryClassification,
)

RequestValue: TypeAlias = str | int | float | bool
RequestStage: TypeAlias = Literal["read_only", "pre_final", "final"]
FileValue: TypeAlias = tuple[str, bytes, str]

_RETRYABLE_STATUS: Final = frozenset({408, 425, 429, 500, 502, 503, 504})
_ALLOWED_METHODS: Final = frozenset({"GET", "HEAD", "POST", "PUT", "DELETE"})
_ERROR_CODE_RE: Final = re.compile(r"[a-z][a-z0-9_]{0,63}\Z")


class PlatformHTTPError(AdapterContractError):
    """A response-free transport failure with stable retry semantics."""

    def __init__(
        self,
        code: str,
        message: str,
        retry_classification: RetryClassification,
        *,
        status_code: int | None = None,
    ) -> None:
        if not _ERROR_CODE_RE.fullmatch(code):
            raise AdapterContractError("HTTP diagnostic code is invalid")
        if (
            not message
            or len(message) > 512
            or any(character in "\r\n\x00" for character in message)
        ):
            raise AdapterContractError("HTTP diagnostic message is invalid")
        if retry_classification not in {
            "safe_pre_final",
            "permanent",
            "ambiguous",
        }:
            raise AdapterContractError("HTTP retry classification is invalid")
        super().__init__(message)
        self.code = code
        self.retry_classification = retry_classification
        self.status_code = status_code


@dataclass(frozen=True, slots=True)
class HTTPPolicy:
    """Finite transport limits; no caller-controlled request can disable them."""

    connect_timeout_seconds: float = 10.0
    read_timeout_seconds: float = 30.0
    write_timeout_seconds: float = 30.0
    pool_timeout_seconds: float = 10.0
    max_response_bytes: int = 1024 * 1024
    max_pre_final_attempts: int = 3
    max_retry_after_seconds: float = 60.0
    retry_backoff_seconds: float = 0.25

    def __post_init__(self) -> None:
        for value in (
            self.connect_timeout_seconds,
            self.read_timeout_seconds,
            self.write_timeout_seconds,
            self.pool_timeout_seconds,
        ):
            if (
                isinstance(value, bool)
                or not math.isfinite(value)
                or not 0 < value <= 120
            ):
                raise AdapterContractError("HTTP timeout policy is invalid")
        if isinstance(self.max_response_bytes, bool) or not (
            0 < self.max_response_bytes <= 16 * 1024 * 1024
        ):
            raise AdapterContractError("HTTP response bound is invalid")
        if isinstance(self.max_pre_final_attempts, bool) or not (
            1 <= self.max_pre_final_attempts <= 5
        ):
            raise AdapterContractError("HTTP retry attempt bound is invalid")
        if (
            isinstance(self.max_retry_after_seconds, bool)
            or not math.isfinite(self.max_retry_after_seconds)
            or not 0 <= self.max_retry_after_seconds <= 300
        ):
            raise AdapterContractError("HTTP Retry-After bound is invalid")
        if (
            isinstance(self.retry_backoff_seconds, bool)
            or not math.isfinite(self.retry_backoff_seconds)
            or not 0 <= self.retry_backoff_seconds <= 60
        ):
            raise AdapterContractError("HTTP retry backoff is invalid")


@dataclass(frozen=True, slots=True)
class SafeHTTPResponse:
    """A bounded response exposing parsed JSON but no raw headers or repr body."""

    status_code: int
    _body: bytes = field(repr=False)
    _retry_classification: RetryClassification = field(repr=False)

    @property
    def size_bytes(self) -> int:
        return len(self._body)

    def json(self) -> object:
        try:
            return json.loads(self._body)
        except (json.JSONDecodeError, UnicodeDecodeError):
            raise PlatformHTTPError(
                "invalid_json_response",
                "platform response was not valid JSON",
                self._retry_classification,
                status_code=self.status_code,
            ) from None


class PlatformHTTPClient:
    """One profile-target HTTP client with redaction and one-shot final dispatch."""

    def __init__(
        self,
        snapshot: PublicationSnapshot,
        token: SecretValue,
        *,
        base_url: str,
        policy: HTTPPolicy | None = None,
        sleeper: Callable[[float], None] = time.sleep,
        clock: Callable[[], datetime] | None = None,
        transport: httpx.BaseTransport | None = None,
    ) -> None:
        self.snapshot = snapshot
        self.policy = policy or HTTPPolicy()
        self._token = token.reveal()
        if (
            not self._token
            or len(self._token) > 8192
            or not self._token.isascii()
            or any(not 0x21 <= ord(character) <= 0x7E for character in self._token)
        ):
            raise AdapterContractError("publishing credential is invalid")
        if _contains_secret(snapshot.target.request_settings, self._token):
            self._token = ""
            raise AdapterContractError("publishing credential appeared in target state")
        normalized_base = _validated_base_url(base_url)
        self._sleeper = sleeper
        self._clock = clock or (lambda: datetime.now(UTC))
        self._closed = False
        self._final_attempted = False
        timeout = httpx.Timeout(
            connect=self.policy.connect_timeout_seconds,
            read=self.policy.read_timeout_seconds,
            write=self.policy.write_timeout_seconds,
            pool=self.policy.pool_timeout_seconds,
        )
        self._client = httpx.Client(
            base_url=normalized_base,
            headers={"Authorization": f"Bearer {self._token}"},
            timeout=timeout,
            follow_redirects=False,
            trust_env=False,
            transport=transport,
        )

    def __enter__(self) -> PlatformHTTPClient:
        self._assert_open()
        return self

    def __exit__(self, *_args: object) -> None:
        self.close()

    def __repr__(self) -> str:
        return (
            "PlatformHTTPClient("
            f"profile_id={self.snapshot.profile_id!r}, "
            f"platform={self.snapshot.target.platform!r}, credential=<redacted>)"
        )

    @property
    def final_attempted(self) -> bool:
        """Whether this target client has crossed its one-shot final boundary."""
        return self._final_attempted

    def close(self) -> None:
        if not self._closed:
            self._closed = True
            self._client.close()
            self._token = ""

    def read_only_request(
        self,
        method: str,
        path: str,
        *,
        params: Mapping[str, RequestValue] | None = None,
        retry_budget_seconds: Callable[[], float] | None = None,
    ) -> SafeHTTPResponse:
        if method.upper() not in {"GET", "HEAD"}:
            raise AdapterContractError("read-only request method is invalid")
        return self._request(
            method,
            path,
            stage="read_only",
            params=params,
            retry_budget_seconds=retry_budget_seconds,
        )

    def pre_final_request(
        self,
        method: str,
        path: str,
        *,
        params: Mapping[str, RequestValue] | None = None,
        json_body: object | None = None,
        form: Mapping[str, str] | None = None,
        content: bytes | None = None,
        files: Mapping[str, FileValue] | None = None,
    ) -> SafeHTTPResponse:
        if self._final_attempted:
            raise AdapterContractError(
                "pre-final mutation cannot follow final dispatch"
            )
        return self._request(
            method,
            path,
            stage="pre_final",
            params=params,
            json_body=json_body,
            form=form,
            content=content,
            files=files,
        )

    def final_request(
        self,
        method: str,
        path: str,
        *,
        params: Mapping[str, RequestValue] | None = None,
        json_body: object | None = None,
        form: Mapping[str, str] | None = None,
    ) -> SafeHTTPResponse:
        self._assert_open()
        if method.upper() != "POST":
            raise AdapterContractError("final dispatch must use POST")
        if self._final_attempted:
            raise AdapterContractError("final dispatch was already attempted")
        self._final_attempted = True
        return self._request(
            method,
            path,
            stage="final",
            params=params,
            json_body=json_body,
            form=form,
        )

    def _request(
        self,
        method: str,
        path: str,
        *,
        stage: RequestStage,
        params: Mapping[str, RequestValue] | None = None,
        json_body: object | None = None,
        form: Mapping[str, str] | None = None,
        content: bytes | None = None,
        files: Mapping[str, FileValue] | None = None,
        retry_budget_seconds: Callable[[], float] | None = None,
    ) -> SafeHTTPResponse:
        self._assert_open()
        normalized_method = method.upper()
        if normalized_method not in _ALLOWED_METHODS:
            raise AdapterContractError("HTTP request method is invalid")
        normalized_path = _validated_relative_path(path)
        if content is not None and (json_body is not None or form is not None or files):
            raise AdapterContractError("HTTP request body is ambiguous")
        self._reject_credential_in_payload(
            normalized_path, params, json_body, form, content, files
        )
        attempts = 1 if stage == "final" else self.policy.max_pre_final_attempts
        for attempt in range(1, attempts + 1):
            try:
                with self._client.stream(
                    normalized_method,
                    normalized_path,
                    params=params,
                    json=json_body,
                    data=form,
                    content=content,
                    files=files,
                ) as response:
                    status = response.status_code
                    if status in _RETRYABLE_STATUS and attempt < attempts:
                        self._sleep_before_retry(
                            self._retry_delay(response, attempt), retry_budget_seconds
                        )
                        continue
                    if not 200 <= status < 300:
                        raise self._status_error(status, stage)
                    body = self._read_bounded(response, stage)
                    return SafeHTTPResponse(
                        status,
                        body,
                        _classification_for_stage(stage),
                    )
            except PlatformHTTPError:
                raise
            except httpx.TimeoutException:
                if attempt < attempts:
                    self._sleep_before_retry(
                        self._fallback_delay(attempt), retry_budget_seconds
                    )
                    continue
                raise PlatformHTTPError(
                    "request_timeout",
                    "platform request timed out",
                    _classification_for_stage(stage),
                ) from None
            except httpx.RequestError:
                if attempt < attempts:
                    self._sleep_before_retry(
                        self._fallback_delay(attempt), retry_budget_seconds
                    )
                    continue
                code = (
                    "final_dispatch_uncertain" if stage == "final" else "network_error"
                )
                message = (
                    "final dispatch outcome is uncertain"
                    if stage == "final"
                    else "platform network request failed"
                )
                raise PlatformHTTPError(
                    code,
                    message,
                    _classification_for_stage(stage),
                ) from None
        raise AdapterContractError("HTTP retry loop ended unexpectedly")

    def _read_bounded(self, response: httpx.Response, stage: RequestStage) -> bytes:
        chunks: list[bytes] = []
        size = 0
        for chunk in response.iter_bytes():
            size += len(chunk)
            if size > self.policy.max_response_bytes:
                raise PlatformHTTPError(
                    "response_too_large",
                    "platform response exceeded the configured size bound",
                    _classification_for_stage(stage),
                    status_code=response.status_code,
                )
            chunks.append(chunk)
        return b"".join(chunks)

    def _status_error(self, status: int, stage: RequestStage) -> PlatformHTTPError:
        if stage == "final" and (status in _RETRYABLE_STATUS or 300 <= status < 400):
            return PlatformHTTPError(
                "final_dispatch_uncertain",
                "final dispatch outcome is uncertain",
                "ambiguous",
                status_code=status,
            )
        if status == 429:
            code = "rate_limited"
        elif status >= 500:
            code = "platform_unavailable"
        elif 300 <= status < 400:
            code = "unexpected_redirect"
        else:
            code = "platform_rejected_request"
        return PlatformHTTPError(
            code,
            "platform rejected the request",
            "safe_pre_final" if status in _RETRYABLE_STATUS else "permanent",
            status_code=status,
        )

    def _retry_delay(self, response: httpx.Response, attempt: int) -> float:
        value = response.headers.get("Retry-After")
        if value is None or len(value) > 128:
            return self._fallback_delay(attempt)
        delay = _parse_retry_after(value, self._clock())
        if delay is None:
            return self._fallback_delay(attempt)
        return min(delay, self.policy.max_retry_after_seconds)

    def _fallback_delay(self, attempt: int) -> float:
        return float(
            min(
                self.policy.retry_backoff_seconds * (2 ** (attempt - 1)),
                self.policy.max_retry_after_seconds,
            )
        )

    def _sleep_before_retry(
        self,
        delay: float,
        retry_budget_seconds: Callable[[], float] | None,
    ) -> None:
        if retry_budget_seconds is None:
            self._sleeper(delay)
            return
        remaining = retry_budget_seconds()
        if (
            isinstance(remaining, bool)
            or not isinstance(remaining, (int, float))
            or not math.isfinite(float(remaining))
            or remaining < 0
        ):
            raise AdapterContractError("HTTP retry budget is invalid")
        bounded = min(delay, float(remaining))
        if bounded > 0:
            self._sleeper(bounded)
        if bounded >= float(remaining):
            raise PlatformHTTPError(
                "retry_budget_exhausted",
                "request retry budget was exhausted",
                "safe_pre_final",
            )

    def _reject_credential_in_payload(self, *values: object) -> None:
        token = self._token
        if token and any(_contains_secret(value, token) for value in values):
            raise AdapterContractError(
                "publishing credential is only permitted in Authorization"
            )

    def _assert_open(self) -> None:
        if self._closed:
            raise AdapterContractError("HTTP client is closed")


def create_target_http_client(
    snapshot: PublicationSnapshot,
    credentials: PublishingCredentials,
    *,
    base_url: str,
    policy: HTTPPolicy | None = None,
    sleeper: Callable[[float], None] = time.sleep,
    clock: Callable[[], datetime] | None = None,
    transport: httpx.BaseTransport | None = None,
) -> PlatformHTTPClient:
    """Resolve one active job's credential into one isolated target client."""
    if credentials.profile_id != snapshot.profile_id:
        raise AdapterContractError("publishing credentials belong to another profile")
    try:
        token = credentials.for_target(snapshot.target.platform)
    except ConfigurationError:
        raise AdapterContractError("publishing credential is unavailable") from None
    return PlatformHTTPClient(
        snapshot,
        token,
        base_url=base_url,
        policy=policy,
        sleeper=sleeper,
        clock=clock,
        transport=transport,
    )


def _validated_base_url(value: str) -> str:
    try:
        parsed = urlsplit(value)
        _ = parsed.port
    except ValueError:
        raise AdapterContractError("platform API base URL is invalid") from None
    if (
        parsed.scheme != "https"
        or not parsed.hostname
        or parsed.username is not None
        or parsed.password is not None
        or parsed.query
        or parsed.fragment
    ):
        raise AdapterContractError("platform API base URL is invalid")
    return value.rstrip("/") + "/"


def _validated_relative_path(value: str) -> str:
    if not value or "\\" in value or "\x00" in value:
        raise AdapterContractError("platform API path is invalid")
    parsed = urlsplit(value)
    if parsed.scheme or parsed.netloc or parsed.query or parsed.fragment:
        raise AdapterContractError("platform API path is invalid")
    decoded_path = parsed.path
    for _ in range(3):
        next_path = unquote(decoded_path)
        if next_path == decoded_path:
            break
        decoded_path = next_path
    if (
        "\\" in decoded_path
        or any(part in {".", ".."} for part in decoded_path.split("/"))
        or any(ord(character) < 32 for character in decoded_path)
    ):
        raise AdapterContractError("platform API path is invalid")
    return parsed.path.lstrip("/")


def _classification_for_stage(stage: RequestStage) -> RetryClassification:
    return "ambiguous" if stage == "final" else "safe_pre_final"


def _parse_retry_after(value: str, now: datetime) -> float | None:
    stripped = value.strip()
    if stripped.isascii() and stripped.isdigit():
        return float(stripped)
    try:
        parsed = parsedate_to_datetime(stripped)
    except (TypeError, ValueError, OverflowError):
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return max(
        0.0,
        (parsed.astimezone(UTC) - now.astimezone(UTC)).total_seconds(),
    )


def _contains_secret(value: object, token: str) -> bool:
    if value is None:
        return False
    if isinstance(value, str):
        return token in value
    if isinstance(value, bytes):
        return token.encode() in value
    if isinstance(value, Mapping):
        return any(
            _contains_secret(key, token) or _contains_secret(item, token)
            for key, item in value.items()
        )
    if isinstance(value, (tuple, list)):
        return any(_contains_secret(item, token) for item in value)
    return False


__all__ = [
    "HTTPPolicy",
    "PlatformHTTPClient",
    "PlatformHTTPError",
    "SafeHTTPResponse",
    "create_target_http_client",
]
