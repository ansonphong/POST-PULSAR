"""Official Instagram Login publishing adapter for Graph API v26.0."""

from __future__ import annotations

import re
import time
import unicodedata
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Final, Literal, Protocol, TypeAlias, cast
from urllib.parse import quote, urlsplit

from post_pulsar.media import (
    MediaSafetyError,
    PublicURLVerification,
    StagedMedia,
    cleanup_staged_media,
    verify_public_media_url,
)
from post_pulsar.platforms.base import (
    AdapterContractError,
    ArtifactCheckpoint,
    BasePlatformAdapter,
    CheckpointWriter,
    PreparedPublication,
    PublicationRequest,
    PublicationSnapshot,
    PublishResult,
    RemoteIdentity,
    RetryClassification,
    ValidationIssue,
)
from post_pulsar.platforms.http import PlatformHTTPClient, PlatformHTTPError
from post_pulsar.state import ArtifactRecord

GRAPH_API_VERSION: Final = "v26.0"
REQUIRED_SCOPES: Final = (
    "instagram_business_basic",
    "instagram_business_content_publish",
)
GRAPH_BASE_URL: Final = "https://graph.instagram.com"
_CONTAINER_LIFETIME: Final = timedelta(hours=23, minutes=45)
_HASHTAG_RE: Final = re.compile(r"(?<!\w)#[\w]+", re.UNICODE)
_MENTION_RE: Final = re.compile(r"(?<!\w)@[A-Za-z0-9._]+")
_READY_STATUS: Final = "FINISHED"
_REMOTE_ID_RE: Final = re.compile(r"[0-9]{1,32}\Z")


class InstagramAdapterError(AdapterContractError):
    """A sanitized preparation failure with explicit retry semantics."""

    def __init__(
        self, code: str, message: str, retry_classification: RetryClassification
    ) -> None:
        super().__init__(message)
        self.code = code
        self.retry_classification = retry_classification


PublicMediaVerifier: TypeAlias = Callable[[StagedMedia], PublicURLVerification]
CleanupOutcome: TypeAlias = Literal["published", "failed", "ambiguous"]
PublicMediaCleaner: TypeAlias = Callable[[StagedMedia, CleanupOutcome], bool]


class WarningCheckpointWriter(Protocol):
    """Optional state-owned warning persistence extension."""

    def record_warning(self, code: str, details: Mapping[str, object]) -> None: ...


class InstagramProcessingCheckpointWriter(Protocol):
    """State-owned CAS seam for durable container processing transitions."""

    def transition_artifact_processing(
        self, current: ArtifactRecord, checkpoint: ArtifactCheckpoint
    ) -> ArtifactRecord: ...


@dataclass(frozen=True, slots=True)
class PublishingQuota:
    """The protected-edge quota values returned by Meta for this account."""

    usage: int
    total: int
    duration_seconds: int


class InstagramAdapter(BasePlatformAdapter):
    """Publish one immutable professional-account target through Instagram Login."""

    def __init__(
        self,
        snapshot: PublicationSnapshot,
        client: PlatformHTTPClient,
        *,
        public_verifier: PublicMediaVerifier | None = None,
        public_cleaner: PublicMediaCleaner | None = None,
        clock: Callable[[], datetime] | None = None,
        monotonic: Callable[[], float] = time.monotonic,
        sleeper: Callable[[float], None] = time.sleep,
        max_status_polls: int | None = None,
        status_poll_seconds: float = 60.0,
    ) -> None:
        if snapshot.target.platform != "instagram":
            client.close()
            raise AdapterContractError("Instagram adapter requires an Instagram target")
        if snapshot.target.api_version != GRAPH_API_VERSION:
            client.close()
            raise AdapterContractError("Instagram Graph API version is unsupported")
        media_base_url = snapshot.target.request_settings.get("media_base_url")
        if not isinstance(media_base_url, str) or not media_base_url:
            client.close()
            raise AdapterContractError("Instagram media base URL is missing")
        media_directory = snapshot.target.request_settings.get("media_directory")
        if (
            not isinstance(media_directory, str)
            or not media_directory
            or not Path(media_directory).is_absolute()
        ):
            client.close()
            raise AdapterContractError("Instagram media directory is missing")
        try:
            processing_timeout = _number_setting(
                snapshot.target.request_settings,
                "processing_timeout_seconds",
                minimum=1.0,
                maximum=86_400.0,
            )
        except AdapterContractError:
            client.close()
            raise
        if isinstance(status_poll_seconds, bool) or not 1 <= status_poll_seconds <= 60:
            client.close()
            raise AdapterContractError("Instagram status poll interval is invalid")
        derived_poll_bound = int(processing_timeout // status_poll_seconds) + 2
        if max_status_polls is None:
            max_status_polls = derived_poll_bound
        if (
            isinstance(max_status_polls, bool)
            or not 1 <= max_status_polls <= derived_poll_bound
        ):
            client.close()
            raise AdapterContractError("Instagram status poll bound is invalid")
        super().__init__(snapshot, client)
        self._client = client
        self._media_base_url = media_base_url
        self._clock = clock or (lambda: datetime.now(UTC))
        self._monotonic = monotonic
        self._sleeper = sleeper
        self._processing_timeout = processing_timeout
        self._max_status_polls = max_status_polls
        self._status_poll_seconds = status_poll_seconds
        self._preflight_verified: set[StagedMedia] = set()
        self._public_verifier = public_verifier or (
            lambda staged: verify_public_media_url(
                staged, media_base_url=media_base_url
            )
        )
        staging_root = Path(media_directory)
        self._public_cleaner = public_cleaner or (
            lambda staged, outcome: cleanup_staged_media(
                staged, staging_root, outcome=outcome
            )
        )

    def _verify_remote_identity(self) -> RemoteIdentity:
        payload = self._client.read_only_request(
            "GET", f"/{GRAPH_API_VERSION}/me", params={"fields": "user_id,username"}
        ).json()
        body = _mapping(payload, "Instagram identity response is invalid")
        user_id = body.get("user_id")
        username = body.get("username")
        if not isinstance(user_id, str) or not isinstance(username, str):
            raise InstagramAdapterError(
                "instagram_identity_invalid",
                "Instagram identity response is invalid",
                "permanent",
            )
        expected = self.snapshot.target
        if (
            user_id != expected.expected_remote_user_id
            or username != expected.expected_username
        ):
            raise InstagramAdapterError(
                "instagram_identity_mismatch",
                "Instagram identity does not match the configured target",
                "permanent",
            )
        return RemoteIdentity(user_id, username)

    def _preflight(
        self, publication: PublicationRequest
    ) -> tuple[ValidationIssue, ...]:
        self._preflight_verified.clear()
        issues = list(_local_validation_issues(publication))
        if any(issue.severity == "error" for issue in issues):
            return tuple(issues)
        try:
            staged_items = self._owned_staging(publication)
        except AdapterContractError:
            issues.append(
                ValidationIssue(
                    "instagram",
                    "error",
                    "instagram_public_media_identity_invalid",
                    "Instagram public media belongs to another publication.",
                    "media",
                )
            )
            return tuple(issues)
        for staged in staged_items:
            try:
                self._verified_public_url(staged)
            except InstagramAdapterError as exc:
                issues.append(
                    ValidationIssue(
                        "instagram", "error", exc.code, exc.args[0], "media"
                    )
                )
            else:
                self._preflight_verified.add(staged)
        quota = self._read_publishing_quota()
        if quota.usage >= quota.total:
            issues.append(
                ValidationIssue(
                    "instagram",
                    "error",
                    "instagram_quota_exhausted",
                    "Instagram content publishing quota is exhausted.",
                )
            )
        return tuple(issues)

    def _prepare(
        self,
        publication: PublicationRequest,
        prior: PreparedPublication | None,
        checkpoints: CheckpointWriter,
    ) -> PreparedPublication:
        issues = _local_validation_issues(publication)
        blocking = next((issue for issue in issues if issue.severity == "error"), None)
        if blocking is not None:
            raise InstagramAdapterError(blocking.code, blocking.message, "permanent")
        staged_items = self._owned_staging(publication)
        artifacts: dict[tuple[str, int], ArtifactRecord] = {
            (item.kind, item.ordinal): item
            for item in (() if prior is None else prior.artifacts)
        }
        container_mutation_started = any(
            kind in {"instagram_child_container", "instagram_parent_container"}
            for kind, _ordinal in artifacts
        )
        try:
            # A direct prepare call cannot bypass the read-only validation barrier.
            if any(staged not in self._preflight_verified for staged in staged_items):
                for staged in staged_items:
                    self._verified_public_url(staged)
            quota = self._read_publishing_quota()
            if quota.usage >= quota.total:
                raise InstagramAdapterError(
                    "instagram_quota_exhausted",
                    "Instagram content publishing quota is exhausted.",
                    "safe_pre_final",
                )

            delivery = checkpoints.advance_phase("processing")
            if prior is not None and prior.attempt_count != delivery.attempt_count:
                raise AdapterContractError(
                    "prior Instagram preparation attempt is stale"
                )
            self._checkpoint_warnings(publication, checkpoints)

            for ordinal, staged in enumerate(staged_items):
                key = ("staged_public", ordinal)
                existing = artifacts.get(key)
                if existing is None:
                    existing = checkpoints.checkpoint_artifact(
                        ArtifactCheckpoint(
                            kind="staged_public",
                            ordinal=ordinal,
                            relative_path=staged.relative_path.as_posix(),
                            sha256=staged.sha256,
                        )
                    )
                    artifacts[key] = existing
                _assert_staging_checkpoint(existing, staged)

            caption = _normalized_caption(publication.text)
            metadata_kinds = tuple(
                item.metadata.kind for item in publication.media.items
            )
            container_mutation_started = True
            if len(staged_items) > 1:
                children = [
                    self._ensure_child_container(
                        ordinal,
                        staged,
                        prior=artifacts.get(("instagram_child_container", ordinal)),
                        checkpoints=checkpoints,
                        artifacts=artifacts,
                    )
                    for ordinal, staged in enumerate(staged_items)
                ]
                child_ids = [
                    _unexpired_external_id(child, self._clock()) for child in children
                ]
                parent = self._ensure_parent_container(
                    {
                        "media_type": "CAROUSEL",
                        "children": ",".join(child_ids),
                        **_caption_form(caption),
                    },
                    prior=artifacts.get(("instagram_parent_container", 0)),
                    checkpoints=checkpoints,
                    artifacts=artifacts,
                )
            elif metadata_kinds == ("video",):
                parent = self._ensure_parent_container(
                    {
                        "media_type": "REELS",
                        "video_url": self._verified_public_url(staged_items[0]),
                        "share_to_feed": "true",
                        **_caption_form(caption),
                    },
                    prior=artifacts.get(("instagram_parent_container", 0)),
                    checkpoints=checkpoints,
                    artifacts=artifacts,
                )
            else:
                parent = self._ensure_parent_container(
                    {
                        "image_url": self._verified_public_url(staged_items[0]),
                        **_caption_form(caption),
                    },
                    prior=artifacts.get(("instagram_parent_container", 0)),
                    checkpoints=checkpoints,
                    artifacts=artifacts,
                )
            parent = self._wait_until_ready(parent, checkpoints)
            artifacts[(parent.kind, parent.ordinal)] = parent
            checkpoints.advance_phase("ready")
            canonical = tuple(artifacts[key] for key in sorted(artifacts))
            return PreparedPublication(publication, delivery.attempt_count, canonical)
        except InstagramAdapterError as exc:
            if self._prepare_failure_allows_cleanup(
                exc, artifacts, container_mutation_started=container_mutation_started
            ):
                self._cleanup_staging(staged_items, "failed")
            raise
        except PlatformHTTPError as exc:
            if self._prepare_failure_allows_cleanup(
                exc, artifacts, container_mutation_started=container_mutation_started
            ):
                self._cleanup_staging(staged_items, "failed")
            raise

    def _commit_once(self, prepared: PreparedPublication) -> PublishResult:
        parent = _artifact(prepared, "instagram_parent_container", 0)
        staged_items = self._owned_staging(prepared.publication)
        try:
            creation_id = _unexpired_external_id(parent, self._clock())
        except InstagramAdapterError:
            self._cleanup_staging(staged_items, "failed")
            raise
        try:
            response = self._client.final_request(
                "POST",
                f"/{GRAPH_API_VERSION}/{self.snapshot.target.expected_remote_user_id}/media_publish",
                form={"creation_id": creation_id},
            )
            result = PublishResult.published(
                _response_id(response.json(), classification="ambiguous")
            )
        except PlatformHTTPError as exc:
            if exc.retry_classification == "permanent":
                result = PublishResult.failed(
                    "instagram_publish_rejected",
                    "Instagram rejected the publication request.",
                    retry_classification="permanent",
                )
            else:
                result = self._resolve_uncertain_publish(creation_id)
        if result.outcome != "ambiguous":
            self._cleanup_staging(staged_items, cast(CleanupOutcome, result.outcome))
        return result

    def _read_publishing_quota(self) -> PublishingQuota:
        user_id = self.snapshot.target.expected_remote_user_id
        payload = self._client.read_only_request(
            "GET",
            f"/{GRAPH_API_VERSION}/{user_id}/content_publishing_limit",
            params={"fields": "quota_usage,config"},
        ).json()
        body = _mapping(payload, "Instagram publishing quota response is invalid")
        data = body.get("data")
        if not isinstance(data, list) or len(data) != 1:
            raise InstagramAdapterError(
                "instagram_quota_invalid",
                "Instagram publishing quota response is invalid",
                "permanent",
            )
        item = _mapping(data[0], "Instagram publishing quota response is invalid")
        config = _mapping(
            item.get("config"), "Instagram publishing quota response is invalid"
        )
        usage = _bounded_integer(item.get("quota_usage"), minimum=0)
        total = _bounded_integer(config.get("quota_total"), minimum=1)
        duration = _bounded_integer(config.get("quota_duration"), minimum=1)
        if usage is None or total is None or duration is None:
            raise InstagramAdapterError(
                "instagram_quota_invalid",
                "Instagram publishing quota response is invalid",
                "permanent",
            )
        return PublishingQuota(usage, total, duration)

    def _ensure_child_container(
        self,
        ordinal: int,
        staged: StagedMedia,
        *,
        prior: ArtifactRecord | None,
        checkpoints: CheckpointWriter,
        artifacts: dict[tuple[str, int], ArtifactRecord],
    ) -> ArtifactRecord:
        if prior is not None:
            artifact = prior
        else:
            public_url = self._verified_public_url(staged)
            container_id = self._create_container(
                {"image_url": public_url, "is_carousel_item": "true"}
            )
            artifact = checkpoints.checkpoint_artifact(
                ArtifactCheckpoint(
                    kind="instagram_child_container",
                    ordinal=ordinal,
                    external_id=container_id,
                    expires_at=self._container_expiry(),
                    processing_metadata={"state": "created"},
                )
            )
            artifacts[("instagram_child_container", ordinal)] = artifact
        artifact = self._wait_until_ready(artifact, checkpoints)
        artifacts[(artifact.kind, artifact.ordinal)] = artifact
        return artifact

    def _ensure_parent_container(
        self,
        form: Mapping[str, str],
        *,
        prior: ArtifactRecord | None,
        checkpoints: CheckpointWriter,
        artifacts: dict[tuple[str, int], ArtifactRecord],
    ) -> ArtifactRecord:
        if prior is not None:
            _unexpired_external_id(prior, self._clock())
            return prior
        container_id = self._create_container(form)
        artifact = checkpoints.checkpoint_artifact(
            ArtifactCheckpoint(
                kind="instagram_parent_container",
                ordinal=0,
                external_id=container_id,
                expires_at=self._container_expiry(),
                processing_metadata={"state": "created"},
            )
        )
        artifacts[("instagram_parent_container", 0)] = artifact
        return artifact

    def _create_container(self, form: Mapping[str, str]) -> str:
        user_id = self.snapshot.target.expected_remote_user_id
        payload = self._client.pre_final_request(
            "POST", f"/{GRAPH_API_VERSION}/{user_id}/media", form=form
        ).json()
        return _response_id(payload, classification="safe_pre_final")

    def _wait_until_ready(
        self, artifact: ArtifactRecord, checkpoints: CheckpointWriter
    ) -> ArtifactRecord:
        container_id = _unexpired_external_id(artifact, self._clock())
        deadline = self._container_processing_deadline(artifact)
        prior_state = artifact.processing_metadata.get("state")
        if prior_state == _READY_STATUS:
            return artifact
        if prior_state in {"ERROR", "EXPIRED"}:
            raise self._terminal_container_error(prior_state)
        if prior_state == "PUBLISHED":
            raise InstagramAdapterError(
                "instagram_container_already_published",
                "Instagram media container publication requires reconciliation",
                "ambiguous",
            )
        if prior_state not in {"created", "IN_PROGRESS"}:
            raise AdapterContractError(
                "prior Instagram container processing state is invalid"
            )
        durable = self._processing_writer(checkpoints)
        for attempt in range(self._max_status_polls):
            if deadline - self._monotonic() <= 0:
                break
            try:
                status = self._read_container_status(
                    container_id,
                    retry_budget_seconds=lambda: max(0.0, deadline - self._monotonic()),
                )
            except PlatformHTTPError as exc:
                if exc.code == "retry_budget_exhausted":
                    break
                raise
            if status not in {
                _READY_STATUS,
                "IN_PROGRESS",
                "ERROR",
                "EXPIRED",
                "PUBLISHED",
            }:
                raise InstagramAdapterError(
                    "instagram_container_status_invalid",
                    "Instagram media container status is invalid",
                    "safe_pre_final",
                )
            if deadline - self._monotonic() <= 0:
                break
            artifact = self._checkpoint_container_state(durable, artifact, status)
            if status == _READY_STATUS:
                return artifact
            if status == "IN_PROGRESS":
                remaining = deadline - self._monotonic()
                if remaining <= 0 or attempt + 1 >= self._max_status_polls:
                    break
                self._sleeper(min(self._status_poll_seconds, remaining))
                continue
            if status in {"ERROR", "EXPIRED"}:
                raise self._terminal_container_error(status)
            if status == "PUBLISHED":
                raise InstagramAdapterError(
                    "instagram_container_already_published",
                    "Instagram media container publication requires reconciliation",
                    "ambiguous",
                )
            raise AdapterContractError("Instagram container status was not handled")
        raise InstagramAdapterError(
            "instagram_container_timeout",
            "Instagram media container did not become ready in time",
            "safe_pre_final",
        )

    def _read_container_status(
        self,
        container_id: str,
        *,
        retry_budget_seconds: Callable[[], float] | None = None,
    ) -> str:
        if not _REMOTE_ID_RE.fullmatch(container_id):
            raise AdapterContractError("Instagram container identity is invalid")
        payload = self._client.read_only_request(
            "GET",
            f"/{GRAPH_API_VERSION}/{container_id}",
            params={"fields": "status_code,status"},
            retry_budget_seconds=retry_budget_seconds,
        ).json()
        body = _mapping(payload, "Instagram container status response is invalid")
        status = body.get("status_code")
        if not isinstance(status, str):
            raise InstagramAdapterError(
                "instagram_container_status_invalid",
                "Instagram media container status is invalid",
                "safe_pre_final",
            )
        return status

    def _resolve_uncertain_publish(self, container_id: str) -> PublishResult:
        deadline = self._monotonic() + self._processing_timeout
        for attempt in range(self._max_status_polls):
            if deadline - self._monotonic() <= 0:
                break
            try:
                status = self._read_container_status(
                    container_id,
                    retry_budget_seconds=lambda: max(0.0, deadline - self._monotonic()),
                )
            except PlatformHTTPError as exc:
                code = (
                    "instagram_publish_status_timeout"
                    if exc.code == "retry_budget_exhausted"
                    else "instagram_publish_status_unavailable"
                )
                return PublishResult.ambiguous(
                    code,
                    "Instagram publication status could not be confirmed.",
                )
            except InstagramAdapterError:
                return PublishResult.ambiguous(
                    "instagram_publish_status_unavailable",
                    "Instagram publication status could not be confirmed.",
                )
            if deadline - self._monotonic() <= 0:
                break
            if status == "IN_PROGRESS":
                remaining = deadline - self._monotonic()
                if remaining <= 0 or attempt + 1 >= self._max_status_polls:
                    break
                self._sleeper(min(self._status_poll_seconds, remaining))
                continue
            codes = {
                "PUBLISHED": "instagram_publish_status_published",
                "FINISHED": "instagram_publish_status_finished",
                "ERROR": "instagram_publish_status_error",
                "EXPIRED": "instagram_publish_status_expired",
            }
            return PublishResult.ambiguous(
                codes.get(status, "instagram_publish_status_unknown"),
                "Instagram publication outcome requires explicit reconciliation.",
            )
        return PublishResult.ambiguous(
            "instagram_publish_status_timeout",
            "Instagram publication status remained uncertain.",
        )

    @staticmethod
    def _processing_writer(
        checkpoints: CheckpointWriter,
    ) -> InstagramProcessingCheckpointWriter:
        if not callable(getattr(checkpoints, "transition_artifact_processing", None)):
            raise AdapterContractError(
                "Instagram processing checkpoint writer is missing"
            )
        return cast(InstagramProcessingCheckpointWriter, checkpoints)

    def _checkpoint_container_state(
        self,
        checkpoints: InstagramProcessingCheckpointWriter,
        current: ArtifactRecord,
        state: str,
    ) -> ArtifactRecord:
        if current.processing_metadata.get("state") == state:
            return current
        checkpoint = ArtifactCheckpoint(
            kind=current.kind,
            ordinal=current.ordinal,
            external_id=current.external_id,
            relative_path=current.relative_path,
            sha256=current.sha256,
            expires_at=current.expires_at,
            processing_metadata={"state": state},
        )
        updated = checkpoints.transition_artifact_processing(current, checkpoint)
        if (
            updated.bundle_key != current.bundle_key
            or updated.platform != current.platform
            or updated.attempt_count != current.attempt_count
            or updated.kind != current.kind
            or updated.ordinal != current.ordinal
            or updated.external_id != current.external_id
            or updated.relative_path != current.relative_path
            or updated.sha256 != current.sha256
            or updated.expires_at != current.expires_at
            or dict(updated.processing_metadata) != {"state": state}
        ):
            raise AdapterContractError(
                "Instagram processing checkpoint changed artifact identity"
            )
        return updated

    @staticmethod
    def _terminal_container_error(status: str) -> InstagramAdapterError:
        return InstagramAdapterError(
            f"instagram_container_{status.casefold()}",
            "Instagram media container cannot be published",
            "safe_pre_final",
        )

    @staticmethod
    def _prepare_failure_allows_cleanup(
        error: InstagramAdapterError | PlatformHTTPError,
        artifacts: Mapping[tuple[str, int], ArtifactRecord],
        *,
        container_mutation_started: bool,
    ) -> bool:
        if isinstance(error, InstagramAdapterError):
            if error.code in {
                "instagram_container_error",
                "instagram_container_expired",
            }:
                return True
            if error.code.startswith("instagram_container_"):
                return False
            return not container_mutation_started
        has_remote_container = any(
            kind in {"instagram_child_container", "instagram_parent_container"}
            for kind, _ordinal in artifacts
        )
        if has_remote_container:
            return False
        return error.retry_classification == "permanent"

    def _owned_staging(
        self, publication: PublicationRequest
    ) -> tuple[StagedMedia, ...]:
        staged_items = tuple(
            _owned_public_staging(publication, item.public)
            for item in publication.media.items
        )
        if any(
            warning.profile_id != self.snapshot.profile_id
            or warning.source_bucket != self.snapshot.source_bucket
            or warning.bundle_id != self.snapshot.bundle_id
            for warning in publication.media.warnings
        ):
            raise AdapterContractError("Instagram warning belongs to another profile")
        for staged in staged_items:
            _assert_public_url_identity(
                staged, cast(str, staged.public_url), self._media_base_url
            )
        return staged_items

    def _verified_public_url(self, staged: StagedMedia) -> str:
        try:
            proof = self._public_verifier(staged)
        except MediaSafetyError:
            raise InstagramAdapterError(
                "instagram_public_media_unavailable",
                "Instagram public media verification failed",
                "safe_pre_final",
            ) from None
        _assert_public_proof(staged, proof)
        _assert_public_url_identity(staged, proof.final_url, self._media_base_url)
        return proof.final_url

    def _cleanup_staging(
        self, staged_items: tuple[StagedMedia, ...], outcome: CleanupOutcome
    ) -> None:
        for staged in staged_items:
            try:
                self._public_cleaner(staged, outcome)
            except MediaSafetyError:
                # Cleanup is best-effort and must never erase or mask publish evidence.
                continue

    def _container_processing_deadline(self, artifact: ArtifactRecord) -> float:
        if artifact.expires_at is None:
            raise AdapterContractError("Instagram container expiry is missing")
        created_at = artifact.expires_at - _CONTAINER_LIFETIME
        wall_deadline = created_at + timedelta(seconds=self._processing_timeout)
        remaining = max(0.0, (wall_deadline - self._clock()).total_seconds())
        return self._monotonic() + remaining

    def _checkpoint_warnings(
        self, publication: PublicationRequest, checkpoints: CheckpointWriter
    ) -> None:
        if not publication.media.warnings:
            return
        callback = getattr(checkpoints, "record_warning", None)
        if not callable(callback):
            raise AdapterContractError("Instagram warning checkpoint writer is missing")
        writer = cast(WarningCheckpointWriter, checkpoints)
        for warning in publication.media.warnings:
            if warning.profile_id != self.snapshot.profile_id:
                raise AdapterContractError(
                    "Instagram warning belongs to another profile"
                )
            writer.record_warning(
                warning.code,
                {
                    "source_name": warning.source_name or "bundle",
                    "message": warning.message,
                },
            )

    def _container_expiry(self) -> datetime:
        now = self._clock()
        if now.tzinfo is None or now.utcoffset() is None:
            raise AdapterContractError("Instagram adapter clock must be timezone-aware")
        return now.astimezone(UTC) + _CONTAINER_LIFETIME


def _local_validation_issues(
    publication: PublicationRequest,
) -> tuple[ValidationIssue, ...]:
    caption = _normalized_caption(publication.text)
    issues: list[ValidationIssue] = []
    if caption is not None and len(caption) > 2_200:
        issues.append(
            ValidationIssue(
                "instagram",
                "error",
                "instagram_caption_too_long",
                "Instagram caption exceeds 2,200 Unicode code points.",
                "caption",
            )
        )
    if caption is not None and len(_HASHTAG_RE.findall(caption)) > 30:
        issues.append(
            ValidationIssue(
                "instagram",
                "error",
                "instagram_too_many_hashtags",
                "Instagram caption exceeds 30 hashtags.",
                "caption",
            )
        )
    if caption is not None and len(_MENTION_RE.findall(caption)) > 20:
        issues.append(
            ValidationIssue(
                "instagram",
                "error",
                "instagram_too_many_mentions",
                "Instagram caption exceeds 20 mentions.",
                "caption",
            )
        )
    items = publication.media.items
    kinds = tuple(item.metadata.kind for item in items)
    if not 1 <= len(items) <= 10 or not (
        all(kind == "image" for kind in kinds) or kinds == ("video",)
    ):
        issues.append(
            ValidationIssue(
                "instagram",
                "error",
                "instagram_media_shape_invalid",
                "Instagram requires one Reel or one to ten images.",
                "media",
            )
        )
    for warning in publication.media.warnings:
        issues.append(
            ValidationIssue(
                "instagram", "warning", warning.code, warning.message, "media"
            )
        )
    return tuple(issues)


def _normalized_caption(value: str | None) -> str | None:
    return None if value is None else unicodedata.normalize("NFC", value)


def _caption_form(caption: str | None) -> dict[str, str]:
    return {} if caption is None else {"caption": caption}


def _owned_public_staging(
    publication: PublicationRequest, staged: StagedMedia | None
) -> StagedMedia:
    snapshot = publication.snapshot
    if (
        staged is None
        or staged.staging_kind != "instagram_public"
        or staged.public_url is None
        or staged.profile_id != snapshot.profile_id
        or staged.source_bucket != snapshot.source_bucket
        or staged.bundle_id != snapshot.bundle_id
        or staged.bundle_fingerprint != snapshot.bundle_fingerprint
    ):
        raise AdapterContractError(
            "Instagram public staging belongs to another publication"
        )
    return staged


def _assert_public_proof(staged: StagedMedia, proof: PublicURLVerification) -> None:
    if (
        proof.sha256 != staged.sha256
        or proof.mime_type.casefold() != staged.mime_type.casefold()
        or proof.size_bytes != staged.size_bytes
        or not proof.final_url
    ):
        raise InstagramAdapterError(
            "instagram_public_media_mismatch",
            "Instagram public media verification did not match staging",
            "safe_pre_final",
        )


def _assert_public_url_identity(
    staged: StagedMedia, candidate: str, media_base_url: str
) -> None:
    try:
        base = urlsplit(media_base_url)
        parsed = urlsplit(candidate)
        base_port = base.port or 443
        candidate_port = parsed.port or 443
    except ValueError:
        raise InstagramAdapterError(
            "instagram_public_media_identity_invalid",
            "Instagram public media URL does not match its publication identity",
            "safe_pre_final",
        ) from None
    expected_path = f"{base.path}{quote(staged.relative_path.name, safe='')}"
    if (
        base.scheme.casefold() != "https"
        or base.hostname is None
        or base.username is not None
        or base.password is not None
        or base.query
        or base.fragment
        or not base.path.endswith("/")
        or parsed.scheme.casefold() != base.scheme.casefold()
        or parsed.hostname is None
        or parsed.hostname.rstrip(".").casefold()
        != base.hostname.rstrip(".").casefold()
        or candidate_port != base_port
        or parsed.username is not None
        or parsed.password is not None
        or parsed.query
        or parsed.fragment
        or parsed.path != expected_path
    ):
        raise InstagramAdapterError(
            "instagram_public_media_identity_invalid",
            "Instagram public media URL does not match its publication identity",
            "safe_pre_final",
        )


def _assert_staging_checkpoint(artifact: ArtifactRecord, staged: StagedMedia) -> None:
    if (
        artifact.kind != "staged_public"
        or artifact.relative_path != staged.relative_path.as_posix()
        or artifact.sha256 != staged.sha256
    ):
        raise AdapterContractError("Instagram staged-media checkpoint conflicts")


def _artifact(prepared: PreparedPublication, kind: str, ordinal: int) -> ArtifactRecord:
    for item in prepared.artifacts:
        if item.kind == kind and item.ordinal == ordinal:
            return item
    raise AdapterContractError("Instagram prepared state has no publish container")


def _unexpired_external_id(artifact: ArtifactRecord, now: datetime) -> str:
    if now.tzinfo is None or now.utcoffset() is None:
        raise AdapterContractError("Instagram adapter clock must be timezone-aware")
    if (
        artifact.external_id is None
        or not _REMOTE_ID_RE.fullmatch(artifact.external_id)
        or artifact.expires_at is None
        or artifact.expires_at <= now.astimezone(UTC)
    ):
        raise InstagramAdapterError(
            "instagram_container_expired",
            "Instagram media container is unavailable or expired",
            "safe_pre_final",
        )
    return artifact.external_id


def _response_id(payload: object, *, classification: RetryClassification) -> str:
    if not isinstance(payload, dict):
        raise PlatformHTTPError(
            "invalid_json_response",
            "platform response was not valid JSON",
            classification,
        )
    body = cast(Mapping[str, object], payload)
    remote_id = body.get("id")
    if not isinstance(remote_id, str) or not _REMOTE_ID_RE.fullmatch(remote_id):
        raise PlatformHTTPError(
            "invalid_json_response",
            "platform response was not valid JSON",
            classification,
        )
    return remote_id


def _mapping(value: object, message: str) -> Mapping[str, object]:
    if not isinstance(value, dict):
        raise InstagramAdapterError(
            "instagram_response_invalid", message, "safe_pre_final"
        )
    return cast(Mapping[str, object], value)


def _bounded_integer(value: object, *, minimum: int) -> int | None:
    if isinstance(value, bool) or not isinstance(value, int):
        return None
    if not minimum <= value <= 2**63 - 1:
        return None
    return value


def _number_setting(
    settings: Mapping[str, object], key: str, *, minimum: float, maximum: float
) -> float:
    value = settings.get(key)
    if (
        isinstance(value, bool)
        or not isinstance(value, (int, float))
        or not minimum <= float(value) <= maximum
    ):
        raise AdapterContractError(f"Instagram {key} is invalid")
    return float(value)


__all__ = [
    "GRAPH_API_VERSION",
    "GRAPH_BASE_URL",
    "REQUIRED_SCOPES",
    "InstagramAdapter",
    "InstagramAdapterError",
    "PublicMediaCleaner",
    "PublishingQuota",
    "PublicMediaVerifier",
]
