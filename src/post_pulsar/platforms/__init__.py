"""Official, profile-isolated platform adapter contracts."""

from post_pulsar.platforms.base import (
    AdapterContractError,
    ArtifactCheckpoint,
    BasePlatformAdapter,
    CheckpointWriter,
    PlatformAdapter,
    PreFinalPhase,
    PreparedPublication,
    PublicationRequest,
    PublicationSnapshot,
    PublishResult,
    RemoteIdentity,
    RetryClassification,
    ValidationIssue,
)
from post_pulsar.platforms.http import (
    HTTPPolicy,
    PlatformHTTPClient,
    PlatformHTTPError,
    SafeHTTPResponse,
    create_target_http_client,
)

__all__ = [
    "AdapterContractError",
    "ArtifactCheckpoint",
    "BasePlatformAdapter",
    "CheckpointWriter",
    "HTTPPolicy",
    "PlatformAdapter",
    "PlatformHTTPClient",
    "PlatformHTTPError",
    "PreFinalPhase",
    "PreparedPublication",
    "PublicationRequest",
    "PublicationSnapshot",
    "PublishResult",
    "RemoteIdentity",
    "RetryClassification",
    "SafeHTTPResponse",
    "ValidationIssue",
    "create_target_http_client",
]
