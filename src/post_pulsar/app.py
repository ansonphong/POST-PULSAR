# ruff: noqa: BLE001, C901, CPY001, PLR0911, PLR0912, PLR0913, PLR0915, PLR0917
"""Safe one-run orchestration for one immutable profile publication."""

from __future__ import annotations

import hashlib
import secrets
from collections.abc import Callable, Mapping
from contextlib import ExitStack
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import TYPE_CHECKING, Literal, Protocol, cast

from post_pulsar.archive import ArchiveManager
from post_pulsar.config import (
    InstagramSettings,
    ProfileSettings,
    SecretValue,
    TargetName,
    XSettings,
    load_local_settings,
    validate_publishing_credentials,
)
from post_pulsar.content import PublishableBundle, SourceBucket, scan_account_root
from post_pulsar.media import (
    PreparedMedia,
    cleanup_checkpointed_staging,
    cleanup_staged_media,
    prepare_bundle_media,
)
from post_pulsar.platforms.base import (
    AdapterContractError,
    ArtifactCheckpoint,
    CheckpointWriter,
    PlatformAdapter,
    PreparedPublication,
    PublicationRequest,
    PublicationSnapshot,
    PublishResult,
    ValidationIssue,
)
from post_pulsar.platforms.http import HTTPPolicy, PlatformHTTPClient, PlatformHTTPError
from post_pulsar.platforms.instagram import (
    GRAPH_API_VERSION,
    GRAPH_BASE_URL,
    InstagramAdapter,
)
from post_pulsar.platforms.x import XAdapter
from post_pulsar.state import (
    ArtifactRecord,
    BundleAdmissionCandidate,
    BundleFileSnapshot,
    BundleRecord,
    ConflictError,
    DeliveryPhase,
    DeliveryRecord,
    StateRepository,
    StateValidationError,
    TargetSnapshot,
)

if TYPE_CHECKING:
    from post_pulsar.locking import LockLease, LockManager

RunStatus = Literal[
    "empty",
    "archived",
    "published",
    "deferred",
    "blocked",
    "invalid",
    "failed",
]


@dataclass(frozen=True, slots=True)
class RunOnceRequest:
    """Immutable one-run selection and idempotency request."""

    profile_id: str
    bucket: SourceBucket
    trigger_id: str
    schedule_run_id: int | None = None
    expected_bundle_key: int | None = None
    expected_fingerprint: str | None = None


@dataclass(frozen=True, slots=True)
class RunOutcome:
    """Stable local outcome consumed by the command layer."""

    status: RunStatus
    bundle_key: int | None = None
    bundle_id: str | None = None
    code: str | None = None


class OrchestrationFaultInjector(Protocol):
    """Inject a deterministic process boundary failure in tests."""

    def __call__(self, boundary: str) -> None:
        """Raise at one named durable boundary."""
        ...


AdapterFactory = Callable[[PublicationSnapshot, SecretValue, Path], PlatformAdapter]
MediaPreparer = Callable[..., PreparedMedia]
RepositoryOpener = Callable[[Path, Callable[[], datetime]], StateRepository]


class OneRunApplication:
    """Coordinate one profile while borrowing the caller's instance lease."""

    def __init__(
        self,
        config_path: str | Path,
        *,
        locks: LockManager,
        instance_lease: LockLease,
        environ: Mapping[str, str] | None = None,
        clock: Callable[[], datetime] | None = None,
        adapter_factory: AdapterFactory | None = None,
        media_preparer: MediaPreparer = prepare_bundle_media,
        repository_opener: RepositoryOpener | None = None,
        fault_injector: OrchestrationFaultInjector | None = None,
        retry_delay: timedelta = timedelta(minutes=5),
    ) -> None:
        """Bind runtime resources while retaining caller ownership of the lease."""
        self._config_path = Path(config_path)
        self._locks = locks
        self._instance_lease = instance_lease
        self._environ = environ
        self._clock = clock or (lambda: datetime.now(UTC))
        self._adapter_factory = adapter_factory or _default_adapter_factory
        self._media_preparer = media_preparer
        self._repository_opener = repository_opener or (
            lambda path, clock: StateRepository.open_existing(path, clock=clock)
        )
        self._fault = fault_injector
        self._retry_delay = retry_delay

    def run_once(self, request: RunOnceRequest) -> RunOutcome:
        """Recover or publish one exact bundle for the requested profile."""
        settings = load_local_settings(self._config_path)
        profile = settings.profile(request.profile_id)
        self._locks.require_instance(self._instance_lease)
        claim_token = _claim_token(request)
        private_root = settings.app.state_directory / "staging" / "private"
        database = settings.app.state_directory / "post_pulsar.sqlite3"

        with ExitStack() as resources:
            profile_lease = resources.enter_context(
                self._locks.acquire_profiles(
                    self._instance_lease,
                    (request.profile_id,),
                ),
            )
            self._inject("after_profile_lease")
            repository = resources.enter_context(
                self._repository_opener(database, self._clock),
            )
            self._inject("after_state_open")
            schedule_run = None
            if request.schedule_run_id is not None:
                schedule_run = repository.get_schedule_run(request.schedule_run_id)
                schedule = repository.get_schedule(schedule_run.schedule_key)
                if (
                    schedule.profile_id != request.profile_id
                    or schedule.bucket != request.bucket
                ):
                    return RunOutcome("invalid", code="schedule_run_conflict")
                if schedule_run.state == "ambiguous":
                    return RunOutcome("blocked", code="ambiguous_delivery")
                if schedule_run.state not in {"due", "dispatching", "failed"}:
                    return RunOutcome("invalid", code="schedule_run_not_dispatchable")
                resources.callback(
                    repository.synchronize_schedule_run,
                    request.schedule_run_id,
                )
            stored_profile = repository.get_profile(request.profile_id)
            if stored_profile.account_root != profile.account_root.resolve(
                strict=False,
            ):
                msg = "profile root drift detected"
                raise ConflictError(msg)

            repository.recover_stale(
                self._clock(),
                profile_id=request.profile_id,
                reclaim_token=claim_token,
            )
            self._inject("after_stale_recovery")
            try:
                triggered = repository.get_triggered_bundle(
                    trigger_type=("run_once" if schedule_run is None else "schedule"),
                    trigger_id=(
                        request.trigger_id
                        if schedule_run is None
                        else str(schedule_run.run_id)
                    ),
                    profile_id=request.profile_id,
                    source_bucket=request.bucket,
                )
            except ConflictError:
                return RunOutcome("invalid", code="trigger_conflict")
            if triggered is not None and triggered.status == "archived":
                return _outcome("archived", triggered)
            protected = repository.list_protected_bundles(request.profile_id)
            exact_requested = (
                request.expected_bundle_key is not None
                or request.expected_fingerprint is not None
            )
            if exact_requested and (
                request.expected_bundle_key is None
                or request.expected_fingerprint is None
                or schedule_run is not None
            ):
                return RunOutcome("invalid", code="exact_bundle_conflict")
            if exact_requested:
                try:
                    exact = repository.get_bundle(
                        cast(int, request.expected_bundle_key)
                    )
                except StateValidationError:
                    return RunOutcome("invalid", code="exact_bundle_conflict")
                if (
                    exact.profile_id != request.profile_id
                    or exact.source_bucket != request.bucket
                    or exact.fingerprint != request.expected_fingerprint
                    or exact not in protected
                ):
                    return RunOutcome("invalid", code="exact_bundle_conflict")
                bundle = exact
            elif schedule_run is not None and schedule_run.bundle_key is not None:
                bundle = repository.get_bundle(schedule_run.bundle_key)
            elif schedule_run is not None:
                bundle = None
                if protected:
                    return RunOutcome("deferred", code="profile_has_recoverable_work")
            else:
                bundle = protected[0] if protected else None
            selected: PublishableBundle | None = None
            media: PreparedMedia | None = None

            if repository.get_pause_state().paused:
                crossed_safe_boundary = False
                if bundle is not None:
                    paused_deliveries = repository.list_bundle_deliveries(
                        bundle.bundle_key
                    )
                    crossed_safe_boundary = (
                        bundle.status == "archiving"
                        or all(item.status == "published" for item in paused_deliveries)
                        or any(
                            item.phase == "final_dispatch_started"
                            for item in paused_deliveries
                        )
                    )
                if not crossed_safe_boundary:
                    return RunOutcome("blocked", code="publication_paused")

            if bundle is not None and bundle.status == "archiving":
                return self._archive(repository, bundle, profile_lease=profile_lease)
            if bundle is not None and bundle.status == "blocked":
                blocked_deliveries = repository.list_bundle_deliveries(
                    bundle.bundle_key,
                )
                code = (
                    "ambiguous_delivery"
                    if any(item.status == "ambiguous" for item in blocked_deliveries)
                    else "operator_action_required"
                )
                return _outcome("blocked", bundle, code)

            if bundle is None:
                scan = scan_account_root(
                    profile.account_root,
                    buckets=(request.bucket,),
                )
                if scan.issues:
                    return RunOutcome("invalid", code=scan.issues[0].code)
                candidates = scan.for_bucket(request.bucket)
                if not candidates:
                    if schedule_run is not None:
                        repository.transition_schedule_run(
                            schedule_run.run_id,
                            "no_content",
                            expected_revision=schedule_run.revision,
                        )
                    return RunOutcome("empty")
                global_scan = scan_account_root(profile.account_root)
                candidate_ids = {item.bundle_id.casefold() for item in candidates}
                collision = next(
                    (
                        issue
                        for issue in global_scan.issues
                        if issue.code == "duplicate_bundle_across_buckets"
                        and issue.bundle_id is not None
                        and issue.bundle_id.casefold() in candidate_ids
                    ),
                    None,
                )
                if collision is not None:
                    return RunOutcome("invalid", code=collision.code)
                global_structure = next(
                    (
                        issue
                        for issue in global_scan.issues
                        if issue.code
                        in {"account_root_changed", "misplaced_ready_marker"}
                    ),
                    None,
                )
                if global_structure is not None:
                    return RunOutcome("invalid", code=global_structure.code)
                enabled = profile.enabled_targets
                if not enabled:
                    return RunOutcome("invalid", code="no_enabled_targets")
                target_snapshots = tuple(
                    _configured_target_snapshot(profile, target) for target in enabled
                )
                admission_candidates = tuple(
                    _admission_candidate(item) for item in candidates
                )
                preview = repository.preview_selected_bundle(
                    profile_id=request.profile_id,
                    source_bucket=request.bucket,
                    candidates=admission_candidates,
                )
                selected = next(
                    item
                    for item in candidates
                    if item.bundle_id == preview.bundle_id
                    and item.fingerprint == preview.fingerprint
                )
                instagram_settings = _instagram_from_snapshot(target_snapshots)
                try:
                    media = self._media_preparer(
                        selected,
                        profile_id=request.profile_id,
                        targets=enabled,
                        private_staging_directory=private_root,
                        instagram=instagram_settings,
                    )
                except Exception:
                    return RunOutcome("invalid", code="media_validation_failed")
                try:
                    if schedule_run is None:
                        admitted = repository.admit_selected_bundle(
                            profile_id=request.profile_id,
                            source_bucket=request.bucket,
                            candidates=admission_candidates,
                            targets=target_snapshots,
                            trigger_id=request.trigger_id,
                            expected_bundle_id=preview.bundle_id,
                            expected_fingerprint=preview.fingerprint,
                        )
                    else:
                        admitted = repository.admit_scheduled_bundle(
                            schedule_run.run_id,
                            candidates=admission_candidates,
                            targets=target_snapshots,
                            expected_revision=schedule_run.revision,
                            expected_bundle_id=preview.bundle_id,
                            expected_fingerprint=preview.fingerprint,
                        )
                except (ConflictError, StateValidationError):
                    _cleanup_media(
                        media,
                        private_root,
                        instagram_settings,
                        outcome="failed",
                    )
                    return RunOutcome("invalid", code="admission_conflict")
                except Exception:
                    _cleanup_media(
                        media,
                        private_root,
                        instagram_settings,
                        outcome="failed",
                    )
                    raise
                bundle = admitted
                resources.callback(
                    self._cleanup_owned_media_on_exit,
                    repository,
                    bundle.bundle_key,
                    media,
                    private_root,
                    instagram_settings,
                )
                self._inject("after_bundle_admission")

            deliveries = repository.list_bundle_deliveries(bundle.bundle_key)
            if any(item.status == "ambiguous" for item in deliveries):
                return _outcome("blocked", bundle, "ambiguous_delivery")
            if all(item.status == "published" for item in deliveries):
                self._cleanup_checkpointed_staging(
                    repository,
                    bundle,
                    repository.list_target_snapshots(bundle.bundle_key),
                    private_root=private_root,
                    outcome="published",
                )
                return self._archive(repository, bundle, profile_lease=profile_lease)

            selected = _find_stored_source(profile, bundle)
            if selected is None:
                repository.block_bundle(
                    bundle.bundle_key,
                    "fingerprint_drift",
                    expected_revision=bundle.revision,
                )
                return _outcome("blocked", bundle, "fingerprint_drift")
            repository.assert_bundle_source(
                bundle.bundle_key,
                fingerprint=selected.fingerprint,
                profile_root=profile.account_root,
            )

            snapshots = repository.list_target_snapshots(bundle.bundle_key)
            current_by_platform = {
                snapshot.platform: _configured_target_snapshot(
                    profile,
                    snapshot.platform,
                )
                for snapshot in snapshots
            }
            for snapshot in snapshots:
                if current_by_platform[snapshot.platform] != snapshot:
                    repository.block_bundle(
                        bundle.bundle_key,
                        "target_snapshot_drift",
                        expected_revision=repository.get_bundle(
                            bundle.bundle_key,
                        ).revision,
                    )
                    return _outcome("blocked", bundle, "target_snapshot_drift")

            required = tuple(
                delivery
                for delivery in deliveries
                if _delivery_is_due(delivery, self._clock())
            )
            if not required:
                return _outcome("deferred", bundle, "retry_not_due")
            required_targets = tuple(item.platform for item in required)
            credentials = validate_publishing_credentials(
                settings,
                request.profile_id,
                required_targets,
                self._environ,
            )
            instagram_settings = _instagram_from_snapshot(snapshots)
            if media is None:
                media = self._media_preparer(
                    selected,
                    profile_id=request.profile_id,
                    targets=required_targets,
                    private_staging_directory=private_root,
                    instagram=instagram_settings,
                )
                resources.callback(
                    self._cleanup_owned_media_on_exit,
                    repository,
                    bundle.bundle_key,
                    media,
                    private_root,
                    instagram_settings,
                )

            publications: dict[str, PublicationRequest] = {}
            adapters: dict[str, PlatformAdapter] = {}
            for delivery in required:
                target = next(
                    item for item in snapshots if item.platform == delivery.platform
                )
                publication_snapshot = PublicationSnapshot(
                    profile_id=bundle.profile_id,
                    bundle_key=bundle.bundle_key,
                    bundle_id=bundle.bundle_id,
                    bundle_fingerprint=bundle.fingerprint,
                    source_bucket=bundle.source_bucket,
                    target=target,
                )
                publication = PublicationRequest(
                    publication_snapshot,
                    media,
                    selected.content.caption,
                    selected.content.alt_text,
                )
                publications[delivery.platform] = publication
                adapter = self._adapter_factory(
                    publication_snapshot,
                    credentials.for_target(delivery.platform),
                    private_root,
                )
                resources.callback(adapter.close)
                adapters[delivery.platform] = adapter

            claimed: dict[str, DeliveryRecord] = {}
            for delivery in required:
                active = (
                    delivery
                    if delivery.status == "in_flight"
                    else repository.claim_delivery(
                        bundle.bundle_key,
                        delivery.platform,
                        claim_token,
                    )
                )
                claimed[delivery.platform] = active
                _checkpoint_staging(
                    _StateCheckpointWriter(
                        repository,
                        bundle.bundle_key,
                        active.platform,
                        claim_token,
                        active.attempt_count,
                    ),
                    media,
                    active.platform,
                )
            _record_media_warnings(repository, bundle.bundle_key, media)
            self._inject("after_media_staging")

            preflight_failure = self._preflight_all(
                repository,
                bundle,
                claimed,
                publications,
                adapters,
                claim_token,
            )
            if preflight_failure is not None:
                outcome, failed_platform = preflight_failure
                remote_evidence = any(
                    _has_remote_artifacts(repository, item) for item in claimed.values()
                )
                if not remote_evidence:
                    _cleanup_media(
                        media,
                        private_root,
                        instagram_settings,
                        outcome="failed",
                    )
                for original in required:
                    if (
                        original.platform != failed_platform
                        and original.status != "in_flight"
                    ):
                        repository.release_unmutated_delivery_claim(
                            claimed[original.platform],
                            original,
                            claim_token=claim_token,
                        )
                return outcome
            self._inject("after_preflight_barrier")

            for platform in sorted(adapters):
                delivery = claimed[platform]
                publication = publications[platform]
                writer = _StateCheckpointWriter(
                    repository,
                    bundle.bundle_key,
                    delivery.platform,
                    claim_token,
                    delivery.attempt_count,
                )
                _checkpoint_staging(writer, media, delivery.platform)
                artifacts = repository.list_delivery_artifacts(
                    bundle.bundle_key,
                    cast("TargetName", platform),
                    attempt_count=delivery.attempt_count,
                )
                prior = (
                    PreparedPublication(publication, delivery.attempt_count, artifacts)
                    if artifacts
                    else None
                )
                try:
                    prepared = adapters[platform].prepare(
                        publication,
                        prior=prior,
                        checkpoints=writer,
                    )
                except Exception as exc:
                    return self._record_pre_final_exception(
                        repository,
                        bundle,
                        platform,
                        exc,
                        claim_token,
                        media,
                        private_root,
                        instagram_settings,
                    )
                final_delivery = writer.advance_phase("final_dispatch_started")
                self._inject(f"after_final_dispatch_started:{platform}")
                try:
                    result = adapters[platform].commit(
                        prepared,
                        delivery=final_delivery,
                    )
                    self._inject(f"after_remote_commit:{platform}")
                except Exception:
                    repository.mark_delivery_ambiguous(
                        bundle.bundle_key,
                        cast("TargetName", platform),
                        error_code="final_dispatch_uncertain",
                        error_message="Final dispatch outcome is uncertain.",
                        claim_token=claim_token,
                        attempt_count=delivery.attempt_count,
                    )
                    return _outcome("blocked", bundle, "final_dispatch_uncertain")
                self._persist_result(
                    repository,
                    bundle,
                    platform,
                    delivery,
                    result,
                    claim_token,
                )
                self._inject(f"after_result_persisted:{platform}")
                if result.outcome != "published":
                    if result.outcome == "failed":
                        _cleanup_media(
                            media,
                            private_root,
                            instagram_settings,
                            outcome="failed",
                        )
                    return _outcome(
                        "blocked",
                        bundle,
                        result.error_code or "delivery_failed",
                    )

            final_deliveries = repository.list_bundle_deliveries(bundle.bundle_key)
            if all(item.status == "published" for item in final_deliveries):
                _cleanup_media(
                    media,
                    private_root,
                    instagram_settings,
                    outcome="published",
                )
                return self._archive(
                    repository,
                    repository.get_bundle(bundle.bundle_key),
                    profile_lease=profile_lease,
                )
            if any(item.status == "ambiguous" for item in final_deliveries):
                return _outcome("blocked", bundle, "ambiguous_delivery")
            _cleanup_media(
                media,
                private_root,
                instagram_settings,
                outcome="failed",
            )
            return _outcome("failed", bundle, "delivery_failed")

    def _preflight_all(
        self,
        repository: StateRepository,
        bundle: BundleRecord,
        deliveries: Mapping[str, DeliveryRecord],
        publications: Mapping[str, PublicationRequest],
        adapters: Mapping[str, PlatformAdapter],
        claim_token: str,
    ) -> tuple[RunOutcome, str] | None:
        results: dict[str, tuple[ValidationIssue, ...] | Exception] = {}
        for platform in sorted(adapters):
            try:
                results[platform] = adapters[platform].preflight(publications[platform])
            except Exception as exc:
                results[platform] = exc

        for platform in sorted(adapters):
            result = results[platform]
            if isinstance(result, tuple):
                for warning in (
                    issue for issue in result if issue.severity == "warning"
                ):
                    repository.record_warning(
                        bundle.bundle_key,
                        warning.platform,
                        warning.code,
                        {"field": warning.field or "", "message": warning.message},
                    )
                errors = tuple(issue for issue in result if issue.severity == "error")
                if not errors:
                    continue
                issue = errors[0]
                delivery = deliveries[platform]
                self._fail_claim(
                    repository,
                    delivery,
                    "preflight_rejected",
                    "Platform preflight rejected this publication.",
                    permanent=True,
                    claim_token=claim_token,
                )
                return _outcome("blocked", bundle, issue.code), platform
            if isinstance(result, Exception):
                stored_error = result
                classification = _retry_classification(stored_error)
                delivery = deliveries[platform]
                if classification == "ambiguous":
                    repository.mark_delivery_ambiguous(
                        bundle.bundle_key,
                        cast("TargetName", platform),
                        error_code="preflight_ambiguous",
                        error_message="Remote preflight outcome is uncertain.",
                        claim_token=claim_token,
                        attempt_count=delivery.attempt_count,
                    )
                else:
                    failed = self._fail_claim(
                        repository,
                        delivery,
                        "preflight_failed",
                        "Remote preflight failed safely.",
                        permanent=classification == "permanent",
                        claim_token=claim_token,
                    )
                status: RunStatus = (
                    "failed"
                    if classification == "safe_pre_final" and failed.safe_to_retry
                    else "blocked"
                )
                return _outcome(status, bundle, "preflight_failed"), platform
        return None

    def _record_pre_final_exception(
        self,
        repository: StateRepository,
        bundle: BundleRecord,
        platform: str,
        exc: Exception,
        claim_token: str,
        media: PreparedMedia,
        private_root: Path,
        instagram_settings: InstagramSettings | None,
    ) -> RunOutcome:
        classification = _retry_classification(exc)
        delivery = repository.get_delivery(
            bundle.bundle_key,
            cast("TargetName", platform),
        )
        if classification == "ambiguous":
            repository.mark_delivery_ambiguous(
                bundle.bundle_key,
                cast("TargetName", platform),
                error_code="preparation_ambiguous",
                error_message="Remote preparation outcome is uncertain.",
                claim_token=claim_token,
                attempt_count=delivery.attempt_count,
            )
            return _outcome("blocked", bundle, "preparation_ambiguous")
        failed = self._fail_claim(
            repository,
            delivery,
            "preparation_failed",
            "Publication preparation failed safely.",
            permanent=classification == "permanent",
            claim_token=claim_token,
        )
        if classification == "permanent" or not _has_remote_artifacts(
            repository, delivery
        ):
            _cleanup_media(media, private_root, instagram_settings, outcome="failed")
        return _outcome(
            "failed"
            if classification == "safe_pre_final" and failed.safe_to_retry
            else "blocked",
            bundle,
            "preparation_failed",
        )

    def _fail_claim(
        self,
        repository: StateRepository,
        delivery: DeliveryRecord,
        code: str,
        message: str,
        *,
        permanent: bool,
        claim_token: str,
    ) -> DeliveryRecord:
        if not permanent and _has_remote_artifacts(repository, delivery):
            return repository.defer_resumable_delivery(
                delivery.bundle_key,
                delivery.platform,
                error_code=code,
                error_message=message,
                retry_at=self._clock() + self._retry_delay,
                claim_token=claim_token,
                attempt_count=delivery.attempt_count,
            )
        return repository.fail_delivery(
            delivery.bundle_key,
            delivery.platform,
            error_code=code,
            error_message=message,
            retry_at=None if permanent else self._clock() + self._retry_delay,
            permanent=permanent,
            claim_token=claim_token,
            attempt_count=delivery.attempt_count,
        )

    def _persist_result(
        self,
        repository: StateRepository,
        bundle: BundleRecord,
        platform: str,
        delivery: DeliveryRecord,
        result: PublishResult,
        claim_token: str,
    ) -> None:
        target = cast("TargetName", platform)
        arguments = {
            "claim_token": claim_token,
            "attempt_count": delivery.attempt_count,
        }
        if result.outcome == "published":
            repository.publish_delivery(
                bundle.bundle_key,
                target,
                remote_id=cast("str", result.remote_id),
                **arguments,  # type: ignore[arg-type]
            )
        elif result.outcome == "ambiguous":
            repository.mark_delivery_ambiguous(
                bundle.bundle_key,
                target,
                error_code=cast("str", result.error_code),
                error_message=cast("str", result.message),
                **arguments,  # type: ignore[arg-type]
            )
        else:
            repository.reject_final_delivery(
                bundle.bundle_key,
                target,
                error_code=cast("str", result.error_code),
                error_message=cast("str", result.message),
                **arguments,  # type: ignore[arg-type]
            )

    def _archive(
        self,
        repository: StateRepository,
        bundle: BundleRecord,
        *,
        profile_lease: LockLease,
    ) -> RunOutcome:
        archived = ArchiveManager(repository, self._locks).archive_bundle(
            bundle.bundle_key,
            instance_lease=self._instance_lease,
            profile_lease=profile_lease,
        )
        return _outcome("archived", archived)

    def _cleanup_checkpointed_staging(
        self,
        repository: StateRepository,
        bundle: BundleRecord,
        snapshots: tuple[TargetSnapshot, ...],
        *,
        private_root: Path,
        outcome: Literal["published", "failed"],
    ) -> None:
        """Clean exact durable staging descriptors without credentials or adapters."""

        instagram = _instagram_from_snapshot(snapshots)
        seen: set[tuple[str, str, str]] = set()
        for delivery in repository.list_bundle_deliveries(bundle.bundle_key):
            for artifact in repository.list_delivery_artifacts(
                bundle.bundle_key, delivery.platform
            ):
                if artifact.kind not in {"staged_private", "staged_public"}:
                    continue
                key = (
                    artifact.kind,
                    cast("str", artifact.relative_path),
                    cast("str", artifact.sha256),
                )
                if key in seen:
                    continue
                seen.add(key)
                root = private_root
                if artifact.kind == "staged_public":
                    if instagram is None:
                        raise AdapterContractError(
                            "public staging cleanup settings are unavailable"
                        )
                    root = instagram.media_directory
                cleanup_checkpointed_staging(
                    cast("str", artifact.relative_path),
                    cast("str", artifact.sha256),
                    root,
                    outcome=outcome,
                )

    def _cleanup_owned_media_on_exit(
        self,
        repository: StateRepository,
        bundle_key: int,
        media: PreparedMedia,
        private_root: Path,
        instagram: InstagramSettings | None,
    ) -> None:
        """Release staging unless durable state requires ambiguity-safe retention."""

        deliveries = repository.list_bundle_deliveries(bundle_key)
        for delivery in deliveries:
            if delivery.status == "ambiguous" or (
                delivery.status == "in_flight"
                and delivery.phase == "final_dispatch_started"
            ):
                return
            if delivery.status in {"in_flight", "failed"} and _has_remote_artifacts(
                repository, delivery
            ):
                return
        outcome: Literal["published", "failed"] = (
            "published"
            if deliveries and all(item.status == "published" for item in deliveries)
            else "failed"
        )
        _cleanup_media(media, private_root, instagram, outcome=outcome)

    def _inject(self, boundary: str) -> None:
        if self._fault is not None:
            self._fault(boundary)


class _StateCheckpointWriter(CheckpointWriter):
    def __init__(
        self,
        repository: StateRepository,
        bundle_key: int,
        platform: TargetName,
        claim_token: str,
        attempt_count: int,
    ) -> None:
        self._repository = repository
        self._bundle_key = bundle_key
        self._platform = platform
        self._claim_token = claim_token
        self._attempt_count = attempt_count

    def advance_phase(self, phase: DeliveryPhase) -> DeliveryRecord:
        return self._repository.advance_delivery_phase(
            self._bundle_key,
            self._platform,
            phase,
            claim_token=self._claim_token,
            attempt_count=self._attempt_count,
        )

    def checkpoint_artifact(self, checkpoint: ArtifactCheckpoint) -> ArtifactRecord:
        return self._repository.checkpoint_artifact(
            self._bundle_key,
            self._platform,
            kind=checkpoint.kind,
            ordinal=checkpoint.ordinal,
            external_id=checkpoint.external_id,
            relative_path=checkpoint.relative_path,
            sha256=checkpoint.sha256,
            expires_at=checkpoint.expires_at,
            processing_metadata=checkpoint.processing_metadata,
            claim_token=self._claim_token,
            attempt_count=self._attempt_count,
        )

    def transition_artifact_processing(
        self,
        current: ArtifactRecord,
        checkpoint: ArtifactCheckpoint,
    ) -> ArtifactRecord:
        return self._repository.transition_artifact_processing(
            self._bundle_key,
            self._platform,
            kind=current.kind,
            ordinal=current.ordinal,
            external_id=cast("str", current.external_id),
            expected_processing_metadata=current.processing_metadata,
            processing_metadata=checkpoint.processing_metadata,
            claim_token=self._claim_token,
            attempt_count=self._attempt_count,
            expected_expires_at=current.expires_at,
            expires_at=checkpoint.expires_at,
        )

    def replace_expired_artifact(
        self,
        current: ArtifactRecord,
        checkpoint: ArtifactCheckpoint,
    ) -> ArtifactRecord:
        return self._repository.replace_expired_artifact(
            self._bundle_key,
            self._platform,
            kind=current.kind,
            ordinal=current.ordinal,
            expected_external_id=cast("str", current.external_id),
            external_id=cast("str", checkpoint.external_id),
            expires_at=cast("datetime", checkpoint.expires_at),
            processing_metadata=checkpoint.processing_metadata,
            claim_token=self._claim_token,
            attempt_count=self._attempt_count,
        )

    def record_warning(self, code: str, details: Mapping[str, object]) -> None:
        self._repository.record_warning(self._bundle_key, self._platform, code, details)


def _configured_target_snapshot(
    profile: ProfileSettings,
    platform: TargetName,
) -> TargetSnapshot:
    target = profile.target(platform)
    if platform == "x":
        x = cast("XSettings", target)
        settings: Mapping[str, object] = {
            "request_timeout_seconds": x.request_timeout_seconds,
            "processing_timeout_seconds": x.processing_timeout_seconds,
            "chunk_size_bytes": x.chunk_size_bytes,
        }
        api_version = "2"
    else:
        instagram = cast("InstagramSettings", target)
        settings = {
            "media_directory": str(instagram.media_directory),
            "media_base_url": instagram.media_base_url,
            "request_timeout_seconds": instagram.request_timeout_seconds,
            "processing_timeout_seconds": instagram.processing_timeout_seconds,
        }
        api_version = GRAPH_API_VERSION
    return TargetSnapshot(
        platform,
        target.expected_remote_user_id,
        target.expected_username,
        target.token_env_var,
        api_version,
        1,
        settings,
    )


def _admission_candidate(bundle: PublishableBundle) -> BundleAdmissionCandidate:
    return BundleAdmissionCandidate(
        bundle.bundle_id,
        bundle.fingerprint,
        tuple(
            BundleFileSnapshot(
                item.relative_name,
                item.role,
                item.ordinal,
                item.media_kind,
                item.mime_type,
                item.size_bytes,
                item.sha256,
            )
            for item in bundle.content.member_snapshots
        ),
    )


def _find_stored_source(
    profile: ProfileSettings,
    bundle: BundleRecord,
) -> PublishableBundle | None:
    scan = scan_account_root(profile.account_root, buckets=(bundle.source_bucket,))
    if scan.issues:
        return None
    return next(
        (
            item
            for item in scan.for_bucket(bundle.source_bucket)
            if item.bundle_id == bundle.bundle_id
            and item.fingerprint == bundle.fingerprint
        ),
        None,
    )


def _delivery_is_due(delivery: DeliveryRecord, now: datetime) -> bool:
    if delivery.status in {"pending", "in_flight"}:
        return True
    return (
        delivery.status == "failed"
        and delivery.safe_to_retry
        and (delivery.next_attempt_at is None or delivery.next_attempt_at <= now)
    )


def _checkpoint_staging(
    writer: _StateCheckpointWriter,
    media: PreparedMedia,
    platform: str,
) -> None:
    for ordinal, item in enumerate(media.items):
        writer.checkpoint_artifact(
            ArtifactCheckpoint(
                "staged_private",
                ordinal,
                relative_path=item.private.relative_path.as_posix(),
                sha256=item.private.sha256,
            ),
        )
        if platform == "instagram" and item.public is not None:
            writer.checkpoint_artifact(
                ArtifactCheckpoint(
                    "staged_public",
                    ordinal,
                    relative_path=item.public.relative_path.as_posix(),
                    sha256=item.public.sha256,
                ),
            )


def _record_media_warnings(
    repository: StateRepository,
    bundle_key: int,
    media: PreparedMedia,
) -> None:
    for warning in media.warnings:
        platform: TargetName | None = (
            "instagram"
            if warning.code.startswith("instagram_")
            or warning.code == "public_url_verification_limited"
            else None
        )
        repository.record_warning(
            bundle_key,
            platform,
            warning.code,
            {
                "source_name": warning.source_name or "",
                "message": warning.message,
            },
        )


def _has_remote_artifacts(
    repository: StateRepository,
    delivery: DeliveryRecord,
) -> bool:
    return any(
        item.kind not in {"staged_private", "staged_public"}
        for item in repository.list_delivery_artifacts(
            delivery.bundle_key,
            delivery.platform,
            attempt_count=delivery.attempt_count,
        )
    )


def _instagram_from_snapshot(
    snapshots: tuple[TargetSnapshot, ...],
) -> InstagramSettings | None:
    snapshot = next((item for item in snapshots if item.platform == "instagram"), None)
    if snapshot is None:
        return None
    values = snapshot.request_settings
    return InstagramSettings(
        enabled=True,
        expected_remote_user_id=snapshot.expected_remote_user_id,
        expected_username=snapshot.expected_username,
        token_env_var=snapshot.token_env_var,
        media_directory=Path(cast("str", values["media_directory"])),
        media_base_url=cast("str", values["media_base_url"]),
        request_timeout_seconds=_stored_number(values, "request_timeout_seconds"),
        processing_timeout_seconds=_stored_number(values, "processing_timeout_seconds"),
    )


def _default_adapter_factory(
    snapshot: PublicationSnapshot,
    token: SecretValue,
    private_root: Path,
) -> PlatformAdapter:
    timeout = _stored_number(
        snapshot.target.request_settings,
        "request_timeout_seconds",
    )
    policy = HTTPPolicy(
        connect_timeout_seconds=timeout,
        read_timeout_seconds=timeout,
        write_timeout_seconds=timeout,
        pool_timeout_seconds=min(timeout, 10.0),
    )
    base_url = (
        "https://api.x.com" if snapshot.target.platform == "x" else GRAPH_BASE_URL
    )
    client = PlatformHTTPClient(snapshot, token, base_url=base_url, policy=policy)
    if snapshot.target.platform == "x":
        return XAdapter(snapshot, client, private_staging_directory=private_root)
    return InstagramAdapter(snapshot, client)


def _retry_classification(exc: Exception) -> str:
    value = getattr(exc, "retry_classification", None)
    if value in {"safe_pre_final", "permanent", "ambiguous"}:
        return cast("str", value)
    if isinstance(exc, PlatformHTTPError):
        return exc.retry_classification
    if isinstance(exc, AdapterContractError):
        return "permanent"
    return "permanent"


def _stored_number(values: Mapping[str, object], key: str) -> float:
    value = values.get(key)
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        msg = "stored target request setting is invalid"
        raise AdapterContractError(msg)
    return float(value)


def _cleanup_media(
    media: PreparedMedia,
    private_root: Path,
    instagram: InstagramSettings | None,
    *,
    outcome: Literal["published", "failed"],
) -> None:
    for item in media.items:
        cleanup_staged_media(item.private, private_root, outcome=outcome)
        if item.public is not None:
            if instagram is None:
                msg = "public staging cleanup settings are unavailable"
                raise AdapterContractError(
                    msg,
                )
            cleanup_staged_media(
                item.public,
                instagram.media_directory,
                outcome=outcome,
            )


def _claim_token(request: RunOnceRequest) -> str:
    trigger = hashlib.sha256(request.trigger_id.encode("utf-8")).hexdigest()[:24]
    return f"run-{trigger}-{secrets.token_hex(8)}"


def _outcome(
    status: RunStatus,
    bundle: BundleRecord,
    code: str | None = None,
) -> RunOutcome:
    return RunOutcome(status, bundle.bundle_key, bundle.bundle_id, code)


__all__ = [
    "OneRunApplication",
    "OrchestrationFaultInjector",
    "RunOnceRequest",
    "RunOutcome",
]
