"""Immutable, profile-bound contracts for official platform adapters."""

from __future__ import annotations

import math
import re
from abc import ABC, abstractmethod
from collections.abc import Mapping
from dataclasses import dataclass, replace
from datetime import datetime
from types import MappingProxyType
from typing import Final, Literal, Protocol, cast

from post_pulsar.media import PreparedMedia
from post_pulsar.state import (
    ArtifactRecord,
    DeliveryRecord,
    Platform,
    SourceBucket,
    TargetSnapshot,
)

type RetryClassification = Literal["safe_pre_final", "permanent", "ambiguous"]
type PublishOutcome = Literal["published", "failed", "ambiguous"]
type PreFinalPhase = Literal["processing", "ready"]
type IssueSeverity = Literal["error", "warning"]

_PROFILE_RE: Final = re.compile(r"[a-z0-9][a-z0-9-]{0,31}\Z")
_BUNDLE_RE: Final = re.compile(r"[A-Za-z0-9][A-Za-z0-9_-]{0,63}\Z")
_CODE_RE: Final = re.compile(r"[a-z][a-z0-9_]{0,63}\Z")
_SHA256_RE: Final = re.compile(r"[0-9a-f]{64}\Z")
_REMOTE_ID_RE: Final = re.compile(r"[0-9]{1,32}\Z")
_USERNAME_RE: Final[dict[str, re.Pattern[str]]] = {
    "x": re.compile(r"[A-Za-z0-9_]{1,15}\Z"),
    "instagram": re.compile(r"[A-Za-z0-9._]{1,30}\Z"),
}
_TOKEN_ENV_RE: Final[dict[str, re.Pattern[str]]] = {
    "x": re.compile(r"POST_PULSAR_X_[A-Z0-9]+(?:_[A-Z0-9]+)*_USER_ACCESS_TOKEN\Z"),
    "instagram": re.compile(
        r"POST_PULSAR_INSTAGRAM_[A-Z0-9]+(?:_[A-Z0-9]+)*_ACCESS_TOKEN\Z"
    ),
}


class AdapterContractError(RuntimeError):
    """A stable adapter-boundary error containing no platform response data."""


class _BoundCloseable(Protocol):
    @property
    def snapshot(self) -> PublicationSnapshot: ...

    @property
    def final_attempted(self) -> bool: ...

    def close(self) -> None: ...


@dataclass(frozen=True, slots=True)
class ValidationIssue:
    """A bounded validation result safe for durable operator diagnostics."""

    platform: Platform
    severity: IssueSeverity
    code: str
    message: str
    field: str | None = None

    def __post_init__(self) -> None:
        if self.platform not in {"x", "instagram"} or self.severity not in {
            "error",
            "warning",
        }:
            raise AdapterContractError("validation issue classification is invalid")
        _validate_code(self.code)
        _validate_message(self.message)
        if self.field is not None and (
            not self.field or len(self.field) > 64 or not self.field.isascii()
        ):
            raise AdapterContractError("validation issue field is invalid")


@dataclass(frozen=True, slots=True)
class RemoteIdentity:
    """The minimal read-only identity returned by an official API."""

    user_id: str
    username: str

    def __post_init__(self) -> None:
        if not self.user_id or len(self.user_id) > 128:
            raise AdapterContractError("remote identity is invalid")
        if not self.username or len(self.username) > 128:
            raise AdapterContractError("remote identity is invalid")


@dataclass(frozen=True, slots=True)
class PublicationSnapshot:
    """A deeply frozen profile, bundle, and target binding for one job."""

    profile_id: str
    bundle_key: int
    bundle_id: str
    bundle_fingerprint: str
    source_bucket: SourceBucket
    target: TargetSnapshot

    def __post_init__(self) -> None:
        if not _PROFILE_RE.fullmatch(self.profile_id):
            raise AdapterContractError("publication profile identity is invalid")
        if self.bundle_key <= 0 or not _BUNDLE_RE.fullmatch(self.bundle_id):
            raise AdapterContractError("publication bundle identity is invalid")
        if not _SHA256_RE.fullmatch(self.bundle_fingerprint):
            raise AdapterContractError("publication fingerprint is invalid")
        if self.source_bucket not in {"QUEUE", "RANDOM", "REELS"}:
            raise AdapterContractError("publication source bucket is invalid")
        target = self.target
        if target.platform not in _USERNAME_RE:
            raise AdapterContractError("publication target is invalid")
        if not _REMOTE_ID_RE.fullmatch(target.expected_remote_user_id):
            raise AdapterContractError("publication remote identity is invalid")
        if not _USERNAME_RE[target.platform].fullmatch(target.expected_username):
            raise AdapterContractError("publication username is invalid")
        if not _TOKEN_ENV_RE[target.platform].fullmatch(target.token_env_var):
            raise AdapterContractError("publication token reference is invalid")
        if not target.api_version or target.adapter_version <= 0:
            raise AdapterContractError("publication adapter version is invalid")
        frozen_settings = _freeze_mapping(self.target.request_settings)
        object.__setattr__(
            self,
            "target",
            replace(self.target, request_settings=frozen_settings),
        )


@dataclass(frozen=True, slots=True)
class PublicationRequest:
    """Validated local content and immutable remote semantics for one target."""

    snapshot: PublicationSnapshot
    media: PreparedMedia
    text: str | None
    alt_text: str | None

    def __post_init__(self) -> None:
        expected = self.snapshot
        actual = self.media
        if (
            actual.profile_id != expected.profile_id
            or actual.source_bucket != expected.source_bucket
            or actual.bundle_id != expected.bundle_id
            or actual.bundle_fingerprint != expected.bundle_fingerprint
        ):
            raise AdapterContractError(
                "prepared media belongs to a different publication"
            )
        if expected.target.platform not in actual.targets:
            raise AdapterContractError("prepared media excludes the publication target")
        for value in (self.text, self.alt_text):
            if value is not None and "\x00" in value:
                raise AdapterContractError("publication text contains invalid data")


@dataclass(frozen=True, slots=True)
class ArtifactCheckpoint:
    """One exact artifact that must enter durable state before reuse."""

    kind: str
    ordinal: int
    external_id: str | None = None
    relative_path: str | None = None
    sha256: str | None = None
    expires_at: datetime | None = None
    processing_metadata: Mapping[str, object] = MappingProxyType({})

    def __post_init__(self) -> None:
        _validate_code(self.kind)
        if self.ordinal < 0:
            raise AdapterContractError("artifact checkpoint ordinal is invalid")
        object.__setattr__(
            self,
            "processing_metadata",
            _freeze_mapping(self.processing_metadata),
        )


class CheckpointWriter(Protocol):
    """State-owned seam that binds checkpoints to a claimed delivery attempt."""

    def advance_phase(self, phase: PreFinalPhase) -> DeliveryRecord: ...

    def checkpoint_artifact(self, checkpoint: ArtifactCheckpoint) -> ArtifactRecord: ...


@dataclass(frozen=True, slots=True)
class PreparedPublication:
    """Resumable prepared state containing only durable artifact records."""

    publication: PublicationRequest
    attempt_count: int
    artifacts: tuple[ArtifactRecord, ...]

    def __post_init__(self) -> None:
        snapshot = self.publication.snapshot
        if self.attempt_count <= 0:
            raise AdapterContractError("prepared publication attempt is invalid")
        keys: set[tuple[str, int]] = set()
        previous: tuple[str, int] | None = None
        frozen_artifacts: list[ArtifactRecord] = []
        for artifact in self.artifacts:
            if (
                artifact.bundle_key != snapshot.bundle_key
                or artifact.platform != snapshot.target.platform
                or artifact.attempt_count != self.attempt_count
            ):
                raise AdapterContractError("prepared artifact belongs to another job")
            key = (artifact.kind, artifact.ordinal)
            if key in keys or (previous is not None and key < previous):
                raise AdapterContractError("prepared artifacts are not canonical")
            keys.add(key)
            previous = key
            frozen_artifacts.append(
                replace(
                    artifact,
                    processing_metadata=_freeze_mapping(artifact.processing_metadata),
                )
            )
        object.__setattr__(self, "artifacts", tuple(frozen_artifacts))


@dataclass(frozen=True, slots=True)
class PublishResult:
    """A sanitized terminal result from exactly one public-create attempt."""

    outcome: PublishOutcome
    remote_id: str | None
    error_code: str | None
    message: str | None
    retry_classification: RetryClassification | None

    def __post_init__(self) -> None:
        if self.outcome not in {"published", "failed", "ambiguous"}:
            raise AdapterContractError("publication outcome is invalid")
        if self.outcome == "published":
            if (
                not self.remote_id
                or len(self.remote_id) > 128
                or not self.remote_id.isascii()
                or any(character in "\r\n\x00" for character in self.remote_id)
                or self.error_code is not None
                or self.message is not None
                or self.retry_classification is not None
            ):
                raise AdapterContractError("published result is structurally invalid")
            return
        if (
            self.remote_id is not None
            or self.error_code is None
            or self.message is None
        ):
            raise AdapterContractError(
                "failed publication result is structurally invalid"
            )
        _validate_code(self.error_code)
        _validate_message(self.message)
        allowed = (
            {"safe_pre_final", "permanent"}
            if self.outcome == "failed"
            else {"ambiguous"}
        )
        if self.retry_classification not in allowed:
            raise AdapterContractError("publication retry classification is invalid")

    @classmethod
    def published(cls, remote_id: str) -> PublishResult:
        return cls("published", remote_id, None, None, None)

    @classmethod
    def failed(
        cls,
        code: str,
        message: str,
        *,
        retry_classification: Literal["safe_pre_final", "permanent"],
    ) -> PublishResult:
        return cls("failed", None, code, message, retry_classification)

    @classmethod
    def ambiguous(cls, code: str, message: str) -> PublishResult:
        return cls("ambiguous", None, code, message, "ambiguous")


class PlatformAdapter(Protocol):
    """Fixed official-platform lifecycle consumed by orchestration."""

    @property
    def snapshot(self) -> PublicationSnapshot: ...

    def verify_identity(self) -> None: ...

    def preflight(
        self, publication: PublicationRequest
    ) -> tuple[ValidationIssue, ...]: ...

    def prepare(
        self,
        publication: PublicationRequest,
        *,
        prior: PreparedPublication | None,
        checkpoints: CheckpointWriter,
    ) -> PreparedPublication: ...

    def commit(
        self, prepared: PreparedPublication, *, delivery: DeliveryRecord
    ) -> PublishResult: ...

    def close(self) -> None: ...


class BasePlatformAdapter(ABC):
    """Enforce identity, ownership, lifecycle, and one-shot commit invariants."""

    def __init__(self, snapshot: PublicationSnapshot, closer: _BoundCloseable) -> None:
        if closer.snapshot != snapshot:
            closer.close()
            raise AdapterContractError("HTTP client belongs to a different target")
        self._snapshot = snapshot
        self._closer = closer
        self._closed = False
        self._commit_started = False

    @property
    def snapshot(self) -> PublicationSnapshot:
        return self._snapshot

    def __enter__(self) -> BasePlatformAdapter:
        self._assert_open()
        return self

    def __exit__(self, *_args: object) -> None:
        self.close()

    def close(self) -> None:
        if not self._closed:
            self._closed = True
            self._closer.close()

    def verify_identity(self) -> None:
        """Validate the configured remote account through a read-only request."""
        self._assert_open()
        self._verify_expected_identity()

    def preflight(self, publication: PublicationRequest) -> tuple[ValidationIssue, ...]:
        self._assert_publication(publication)
        self._verify_expected_identity()
        issues = self._preflight(publication)
        if not isinstance(issues, tuple) or any(
            not isinstance(issue, ValidationIssue)
            or issue.platform != self.snapshot.target.platform
            for issue in issues
        ):
            raise AdapterContractError("adapter returned invalid validation issues")
        return issues

    def prepare(
        self,
        publication: PublicationRequest,
        *,
        prior: PreparedPublication | None,
        checkpoints: CheckpointWriter,
    ) -> PreparedPublication:
        self._assert_publication(publication)
        if prior is not None:
            self._assert_prepared(prior)
        self._verify_expected_identity()
        prepared = self._prepare(publication, prior, checkpoints)
        if not isinstance(prepared, PreparedPublication):
            raise AdapterContractError("adapter returned invalid prepared state")
        self._assert_prepared(prepared)
        return prepared

    def commit(
        self, prepared: PreparedPublication, *, delivery: DeliveryRecord
    ) -> PublishResult:
        self._assert_prepared(prepared)
        if (
            delivery.bundle_key != self.snapshot.bundle_key
            or delivery.platform != self.snapshot.target.platform
            or delivery.attempt_count != prepared.attempt_count
        ):
            raise AdapterContractError("delivery belongs to a different publication")
        if delivery.status != "in_flight" or delivery.phase != "final_dispatch_started":
            raise AdapterContractError(
                "commit requires durable final-dispatch-started state"
            )
        if self._commit_started:
            raise AdapterContractError("final dispatch was already attempted")
        self._verify_expected_identity()
        self._commit_started = True
        result = self._commit_once(prepared)
        if not isinstance(result, PublishResult):
            raise AdapterContractError("adapter returned invalid publication result")
        if not self._closer.final_attempted:
            raise AdapterContractError("commit did not perform a final request")
        if result.retry_classification == "safe_pre_final":
            raise AdapterContractError(
                "post-final publication result cannot be safely retried"
            )
        return result

    def _assert_open(self) -> None:
        if self._closed:
            raise AdapterContractError("adapter is closed")

    def _assert_publication(self, publication: PublicationRequest) -> None:
        self._assert_open()
        if publication.snapshot != self.snapshot:
            raise AdapterContractError(
                "publication belongs to a different profile or target"
            )

    def _assert_prepared(self, prepared: PreparedPublication) -> None:
        self._assert_publication(prepared.publication)

    def _verify_expected_identity(self) -> None:
        identity = self._verify_remote_identity()
        if not isinstance(identity, RemoteIdentity):
            raise AdapterContractError("adapter returned invalid remote identity")
        expected = self.snapshot.target
        if (
            identity.user_id != expected.expected_remote_user_id
            or identity.username.casefold() != expected.expected_username.casefold()
        ):
            raise AdapterContractError("remote identity does not match target snapshot")

    @abstractmethod
    def _verify_remote_identity(self) -> RemoteIdentity:
        """Perform one read-only remote identity request."""

    @abstractmethod
    def _preflight(
        self, publication: PublicationRequest
    ) -> tuple[ValidationIssue, ...]:
        """Perform platform-specific read-only validation."""

    @abstractmethod
    def _prepare(
        self,
        publication: PublicationRequest,
        prior: PreparedPublication | None,
        checkpoints: CheckpointWriter,
    ) -> PreparedPublication:
        """Perform resumable mutation and checkpoint before proceeding."""

    @abstractmethod
    def _commit_once(self, prepared: PreparedPublication) -> PublishResult:
        """Perform one and only one final public-create request."""


def _freeze_mapping(value: Mapping[str, object]) -> Mapping[str, object]:
    frozen: dict[str, object] = {}
    for key, item in value.items():
        if not isinstance(key, str):
            raise AdapterContractError("snapshot setting key is invalid")
        frozen[key] = _freeze_value(item)
    return MappingProxyType(frozen)


def _freeze_value(value: object) -> object:
    if value is None or isinstance(value, (str, int, bool)):
        return value
    if isinstance(value, float):
        if not math.isfinite(value):
            raise AdapterContractError("snapshot setting value is invalid")
        return value
    if isinstance(value, Mapping):
        return _freeze_mapping(cast(Mapping[str, object], value))
    if isinstance(value, (list, tuple)):
        return tuple(_freeze_value(item) for item in value)
    raise AdapterContractError("snapshot setting value is invalid")


def _validate_code(value: str) -> None:
    if not _CODE_RE.fullmatch(value):
        raise AdapterContractError("diagnostic code is invalid")


def _validate_message(value: str) -> None:
    if (
        not value
        or len(value) > 512
        or any(character in "\r\n\x00" for character in value)
    ):
        raise AdapterContractError("diagnostic message is invalid")


__all__ = [
    "AdapterContractError",
    "ArtifactCheckpoint",
    "BasePlatformAdapter",
    "CheckpointWriter",
    "PlatformAdapter",
    "PreFinalPhase",
    "PreparedPublication",
    "PublicationRequest",
    "PublicationSnapshot",
    "PublishResult",
    "RemoteIdentity",
    "RetryClassification",
    "ValidationIssue",
]
