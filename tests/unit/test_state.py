"""Transactional SQLite state-machine tests."""

from __future__ import annotations

import hashlib
import sqlite3
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from post_pulsar.state import (
    BundleAdmissionCandidate,
    BundleFileSnapshot,
    ConflictError,
    MigrationRequiredError,
    ProfileTargetSnapshot,
    StateRepository,
    StateValidationError,
    TargetSnapshot,
    TransitionError,
)


class FakeClock:
    """Controllable UTC clock for durable timestamp assertions."""

    def __init__(self) -> None:
        self.value = datetime(2026, 9, 5, 12, tzinfo=UTC)

    def __call__(self) -> datetime:
        return self.value

    def advance(self, **kwargs: float) -> None:
        self.value += timedelta(**kwargs)


def _profile_target(
    remote_id: str = "10001",
    env_var: str = "POST_PULSAR_X_ANSONPHONG_USER_ACCESS_TOKEN",
) -> ProfileTargetSnapshot:
    return ProfileTargetSnapshot(
        platform="x",
        expected_remote_user_id=remote_id,
        expected_username="ansonphong",
        token_env_var=env_var,
        request_settings={"timeout": 30, "chunk_size": 4194304},
    )


def _target(remote_id: str = "10001") -> TargetSnapshot:
    return TargetSnapshot(
        platform="x",
        expected_remote_user_id=remote_id,
        expected_username="ansonphong",
        token_env_var="POST_PULSAR_X_ANSONPHONG_USER_ACCESS_TOKEN",
        api_version="2",
        adapter_version=1,
        request_settings={"timeout": 30, "chunk_size": 4194304},
    )


def _instagram_profile_target() -> ProfileTargetSnapshot:
    return ProfileTargetSnapshot(
        platform="instagram",
        expected_remote_user_id="20001",
        expected_username="anson.phong",
        token_env_var="POST_PULSAR_INSTAGRAM_ANSONPHONG_ACCESS_TOKEN",
        request_settings={"timeout": 30, "media_base_url": "https://media.example/"},
    )


def _instagram_target() -> TargetSnapshot:
    return TargetSnapshot(
        platform="instagram",
        expected_remote_user_id="20001",
        expected_username="anson.phong",
        token_env_var="POST_PULSAR_INSTAGRAM_ANSONPHONG_ACCESS_TOKEN",
        api_version="v26.0",
        adapter_version=1,
        request_settings={"timeout": 30, "media_base_url": "https://media.example/"},
    )


def _file(name: str = "post.jpg") -> BundleFileSnapshot:
    return BundleFileSnapshot(
        relative_name=name,
        role="image",
        ordinal=None,
        media_kind="image",
        mime_type="image/jpeg",
        size_bytes=5,
        sha256="a" * 64,
    )


def _repository(tmp_path: Path, clock: FakeClock) -> StateRepository:
    repository = StateRepository(
        tmp_path / "state/post_pulsar.sqlite3",
        clock=clock,
        secret_values=("never-store-this-token",),
    )
    repository.register_profile(
        "ansonphong",
        tmp_path / "accounts/ansonphong",
        (_profile_target(), _instagram_profile_target()),
        config_hash="1" * 64,
    )
    return repository


def _bundle(repository: StateRepository, bundle_id: str = "post") -> int:
    return repository.add_bundle(
        profile_id="ansonphong",
        bundle_id=bundle_id,
        fingerprint="b" * 64,
        source_bucket="QUEUE",
        files=(_file(f"{bundle_id}.jpg"),),
        targets=(_target(),),
    )


def _claim(
    repository: StateRepository,
    bundle_key: int,
    platform: str,
    token: str,
) -> dict[str, object]:
    delivery = repository.claim_delivery(bundle_key, platform, token)  # type: ignore[arg-type]
    return {"claim_token": token, "attempt_count": delivery.attempt_count}


def test_schema_initialization_reopen_and_pragmas_are_idempotent(
    tmp_path: Path,
) -> None:
    clock = FakeClock()
    path = tmp_path / "state.sqlite3"

    with StateRepository(path, clock=clock) as repository:
        assert repository.schema_version == 1
        assert repository.foreign_keys_enabled
        assert repository.busy_timeout_ms >= 1000
        assert repository.journal_mode in {"wal", "delete", "memory"}
    with StateRepository(path, clock=clock) as repository:
        assert repository.schema_version == 1


def test_open_existing_never_initializes_missing_state(tmp_path: Path) -> None:
    path = tmp_path / "missing" / "post_pulsar.sqlite3"

    with pytest.raises(MigrationRequiredError, match="does not exist"):
        StateRepository.open_existing(path)

    assert not path.exists()


def test_public_readers_reconstruct_profile_work_after_reopen(tmp_path: Path) -> None:
    clock = FakeClock()
    path = tmp_path / "reconstruct.sqlite3"
    repository = StateRepository(path, clock=clock)
    repository.register_profile(
        "ansonphong",
        tmp_path / "accounts/ansonphong",
        (_profile_target(), _instagram_profile_target()),
        config_hash="1" * 64,
    )
    bundle_key = repository.add_bundle(
        profile_id="ansonphong",
        bundle_id="reconstruct",
        fingerprint="b" * 64,
        source_bucket="QUEUE",
        files=(_file("z-last.jpg"), _file("A-first.jpg")),
        targets=(_instagram_target(), _target()),
    )
    claim = _claim(repository, bundle_key, "x", "reconstruct-claim")
    artifact = repository.checkpoint_artifact(
        bundle_key,
        "x",
        kind="x_media_id",
        ordinal=1,
        external_id="media-reconstruct",
        expires_at=clock() + timedelta(hours=1),
        **claim,  # type: ignore[arg-type]
    )
    first_artifact = repository.checkpoint_artifact(
        bundle_key,
        "x",
        kind="x_media_id",
        ordinal=0,
        external_id="media-reconstruct-first",
        expires_at=clock() + timedelta(hours=1),
        **claim,  # type: ignore[arg-type]
    )
    failed = repository.fail_delivery(
        bundle_key,
        "x",
        error_code="transient",
        error_message="Retry later.",
        retry_at=clock() + timedelta(minutes=5),
        **claim,  # type: ignore[arg-type]
    )
    repository.close()

    with StateRepository(path, clock=clock) as reopened:
        assert reopened.list_active_bundles("ansonphong") == (
            reopened.get_bundle(bundle_key),
        )
        assert reopened.list_failed_deliveries("ansonphong") == (failed,)
        assert reopened.list_protected_bundles("ansonphong") == (
            reopened.get_bundle(bundle_key),
        )
        assert [
            item.relative_name for item in reopened.list_bundle_files(bundle_key)
        ] == ["A-first.jpg", "z-last.jpg"]
        assert [
            item.platform for item in reopened.list_target_snapshots(bundle_key)
        ] == ["instagram", "x"]
        assert [
            item.platform for item in reopened.list_bundle_deliveries(bundle_key)
        ] == ["instagram", "x"]
        assert reopened.list_delivery_artifacts(bundle_key, "x") == (
            first_artifact,
            artifact,
        )


@pytest.mark.parametrize("version", [0, 2, 999])
def test_unknown_or_future_schema_requires_explicit_migration(
    tmp_path: Path, version: int
) -> None:
    path = tmp_path / f"legacy-{version}.sqlite3"
    connection = sqlite3.connect(path)
    connection.execute("CREATE TABLE legacy_state(value TEXT)")
    connection.execute(f"PRAGMA user_version = {version}")
    connection.close()

    with pytest.raises(MigrationRequiredError, match="migration required"):
        StateRepository(path)


def test_profile_identity_and_active_root_snapshot_are_immutable(
    tmp_path: Path,
) -> None:
    clock = FakeClock()
    repository = _repository(tmp_path, clock)
    bundle_key = _bundle(repository)

    with pytest.raises(ConflictError, match="remote identity"):
        repository.register_profile(
            "second",
            tmp_path / "accounts/second",
            (
                ProfileTargetSnapshot(
                    platform="x",
                    expected_remote_user_id="10001",
                    expected_username="second",
                    token_env_var="POST_PULSAR_X_SECOND_USER_ACCESS_TOKEN",
                    request_settings={},
                ),
            ),
            config_hash="2" * 64,
        )
    with pytest.raises(ConflictError, match="profile root drift"):
        repository.register_profile(
            "ansonphong",
            tmp_path / "accounts/moved",
            (_profile_target(),),
            config_hash="1" * 64,
        )

    claim = _claim(repository, bundle_key, "x", "archive-claim")
    repository.advance_delivery_phase(
        bundle_key,
        "x",
        "final_dispatch_started",
        **claim,  # type: ignore[arg-type]
    )
    repository.publish_delivery(
        bundle_key,
        "x",
        remote_id="tweet-archive",
        **claim,  # type: ignore[arg-type]
    )
    repository.begin_archiving(bundle_key, expected_revision=1)
    repository.checkpoint_archive_member(bundle_key, "post.jpg", sha256="a" * 64)
    repository.checkpoint_archive_member(
        bundle_key, ".ready", sha256=hashlib.sha256(b"").hexdigest()
    )
    assert repository.list_archive_checkpoints(bundle_key) == ("post.jpg", ".ready")
    repository.mark_archived(bundle_key, "POSTED/QUEUE/post", expected_revision=2)
    updated = repository.register_profile(
        "ansonphong",
        tmp_path / "accounts/moved",
        (_profile_target(),),
        config_hash="3" * 64,
        expected_revision=1,
    )
    assert updated.revision == 2


def test_archive_conflict_can_transition_archiving_bundle_to_blocked(
    tmp_path: Path,
) -> None:
    repository = _repository(tmp_path, FakeClock())
    bundle_key = _bundle(repository)
    claim = _claim(repository, bundle_key, "x", "archive-block-claim")
    repository.advance_delivery_phase(
        bundle_key,
        "x",
        "final_dispatch_started",
        **claim,  # type: ignore[arg-type]
    )
    repository.publish_delivery(
        bundle_key,
        "x",
        remote_id="tweet-archive-block",
        **claim,  # type: ignore[arg-type]
    )
    archiving = repository.begin_archiving(bundle_key, expected_revision=1)

    blocked = repository.block_bundle(
        bundle_key, "archive_conflict", expected_revision=archiving.revision
    )

    assert blocked.status == "blocked"


def test_bundle_admission_is_single_use_exact_and_profile_isolated(
    tmp_path: Path,
) -> None:
    clock = FakeClock()
    repository = _repository(tmp_path, clock)
    first = _bundle(repository, "Post")

    with pytest.raises(ConflictError, match="already been used"):
        _bundle(repository, "post")
    with pytest.raises(ConflictError, match="one active bundle"):
        _bundle(repository, "other")
    repository.block_bundle(first, "operator_hold", expected_revision=1)
    with pytest.raises(ConflictError, match="one active bundle"):
        _bundle(repository, "other")

    repository.register_profile(
        "second",
        tmp_path / "accounts/second",
        (
            ProfileTargetSnapshot(
                platform="x",
                expected_remote_user_id="10002",
                expected_username="second",
                token_env_var="POST_PULSAR_X_SECOND_USER_ACCESS_TOKEN",
                request_settings={},
            ),
        ),
        config_hash="2" * 64,
    )
    second = repository.add_bundle(
        profile_id="second",
        bundle_id="post",
        fingerprint="c" * 64,
        source_bucket="RANDOM",
        files=(_file(),),
        targets=(
            TargetSnapshot(
                platform="x",
                expected_remote_user_id="10002",
                expected_username="second",
                token_env_var="POST_PULSAR_X_SECOND_USER_ACCESS_TOKEN",
                api_version="2",
                adapter_version=1,
                request_settings={},
            ),
        ),
    )
    assert second != first


def test_empty_snapshots_fingerprint_drift_and_snapshot_mutation_fail(
    tmp_path: Path,
) -> None:
    repository = _repository(tmp_path, FakeClock())
    with pytest.raises(StateValidationError, match="target snapshot"):
        repository.add_bundle(
            profile_id="ansonphong",
            bundle_id="empty",
            fingerprint="b" * 64,
            source_bucket="QUEUE",
            files=(_file(),),
            targets=(),
        )
    with pytest.raises(ConflictError, match="target snapshot"):
        repository.add_bundle(
            profile_id="ansonphong",
            bundle_id="wrong-target",
            fingerprint="b" * 64,
            source_bucket="QUEUE",
            files=(_file(),),
            targets=(_target("99999"),),
        )
    bundle_key = _bundle(repository)
    with pytest.raises(ConflictError, match="fingerprint drift"):
        repository.assert_bundle_source(
            bundle_key,
            fingerprint="c" * 64,
            profile_root=tmp_path / "accounts/ansonphong",
        )
    assert repository.get_bundle(bundle_key).status == "blocked"
    with pytest.raises(ConflictError, match="immutable target snapshot"):
        repository.assert_target_snapshot(
            bundle_key,
            TargetSnapshot(
                platform="x",
                expected_remote_user_id="different",
                expected_username="ansonphong",
                token_env_var="POST_PULSAR_X_ANSONPHONG_USER_ACCESS_TOKEN",
                api_version="2",
                adapter_version=1,
                request_settings={"timeout": 30},
            ),
        )


def test_delivery_checkpoints_attempt_and_failure_counters_block_on_fifth(
    tmp_path: Path,
) -> None:
    clock = FakeClock()
    repository = _repository(tmp_path, clock)
    bundle_key = _bundle(repository)

    for attempt in range(1, 6):
        delivery = repository.claim_delivery(bundle_key, "x", f"claim-{attempt}")
        assert delivery.attempt_count == attempt
        repository.checkpoint_artifact(
            bundle_key,
            "x",
            kind="x_media_id",
            ordinal=0,
            external_id=f"media-{attempt}",
            expires_at=clock() + timedelta(hours=1),
            processing_metadata={"state": "pending", "progress_percent": 25},
            claim_token=f"claim-{attempt}",
            attempt_count=delivery.attempt_count,
        )
        repository.advance_delivery_phase(
            bundle_key,
            "x",
            "processing",
            claim_token=f"claim-{attempt}",
            attempt_count=delivery.attempt_count,
        )
        repository.advance_delivery_phase(
            bundle_key,
            "x",
            "ready",
            claim_token=f"claim-{attempt}",
            attempt_count=delivery.attempt_count,
        )
        failed = repository.fail_delivery(
            bundle_key,
            "x",
            error_code="platform_busy",
            error_message="Platform asked to retry later.",
            retry_at=clock(),
            claim_token=f"claim-{attempt}",
            attempt_count=delivery.attempt_count,
        )
        assert failed.consecutive_failures == attempt
        if attempt < 5:
            assert failed.safe_to_retry
            assert repository.get_bundle(bundle_key).status == "active"
        else:
            assert not failed.safe_to_retry
            assert repository.get_bundle(bundle_key).status == "blocked"


def test_artifact_checkpoint_is_idempotent_but_not_mutable(tmp_path: Path) -> None:
    clock = FakeClock()
    repository = _repository(tmp_path, clock)
    bundle_key = _bundle(repository)
    claim = _claim(repository, bundle_key, "x", "claim")
    values = {
        "kind": "staged_private",
        "ordinal": 0,
        "relative_path": f"ansonphong/QUEUE/{'b' * 64}/media.jpg",
        "sha256": "d" * 64,
        "expires_at": clock() + timedelta(hours=1),
        "processing_metadata": {"state": "ready", "check_after_seconds": 2},
        **claim,
    }
    first = repository.checkpoint_artifact(bundle_key, "x", **values)
    replay = repository.checkpoint_artifact(bundle_key, "x", **values)
    assert replay == first

    with pytest.raises(ConflictError, match="checkpoint"):
        repository.checkpoint_artifact(
            bundle_key,
            "x",
            **{**values, "sha256": "e" * 64},
        )


def test_remote_artifact_processing_transitions_and_expired_replacement_are_guarded(
    tmp_path: Path,
) -> None:
    clock = FakeClock()
    repository = _repository(tmp_path, clock)
    bundle_key = _bundle(repository, "x-resume")
    claim = _claim(repository, bundle_key, "x", "x-resume-claim")
    source_hash = "c" * 64
    initialized = repository.checkpoint_artifact(
        bundle_key,
        "x",
        kind="x_media_id",
        ordinal=0,
        external_id="media-old",
        expires_at=clock() + timedelta(minutes=5),
        processing_metadata={
            "state": "initialized",
            "next_segment_index": 0,
            "source_sha256": source_hash,
            "media_type": "video/mp4",
        },
        **claim,  # type: ignore[arg-type]
    )
    appending = repository.transition_artifact_processing(
        bundle_key,
        "x",
        kind="x_media_id",
        ordinal=0,
        external_id="media-old",
        expected_processing_metadata=initialized.processing_metadata,
        processing_metadata={
            "state": "appending",
            "next_segment_index": 1,
            "source_sha256": source_hash,
            "media_type": "video/mp4",
            "processing_deadline": "2026-09-05T12:02:00.000000Z",
        },
        expected_expires_at=initialized.expires_at,
        expires_at=clock() + timedelta(minutes=2),
        **claim,  # type: ignore[arg-type]
    )
    assert appending.processing_metadata["next_segment_index"] == 1
    assert appending.expires_at == clock() + timedelta(minutes=2)
    with pytest.raises(ConflictError, match="expiry"):
        repository.transition_artifact_processing(
            bundle_key,
            "x",
            kind="x_media_id",
            ordinal=0,
            external_id="media-old",
            expected_processing_metadata=appending.processing_metadata,
            processing_metadata=appending.processing_metadata,
            expected_expires_at=initialized.expires_at,
            expires_at=clock() + timedelta(minutes=3),
            **claim,  # type: ignore[arg-type]
        )
    with pytest.raises(ConflictError, match="processing metadata"):
        repository.transition_artifact_processing(
            bundle_key,
            "x",
            kind="x_media_id",
            ordinal=0,
            external_id="media-old",
            expected_processing_metadata=initialized.processing_metadata,
            processing_metadata={"state": "succeeded"},
            **claim,  # type: ignore[arg-type]
        )

    with pytest.raises(TransitionError, match="not expired"):
        repository.replace_expired_artifact(
            bundle_key,
            "x",
            kind="x_media_id",
            ordinal=0,
            expected_external_id="media-old",
            external_id="media-new",
            expires_at=clock() + timedelta(hours=1),
            processing_metadata={"state": "succeeded"},
            **claim,  # type: ignore[arg-type]
        )
    clock.advance(minutes=6)
    replacement = repository.replace_expired_artifact(
        bundle_key,
        "x",
        kind="x_media_id",
        ordinal=0,
        expected_external_id="media-old",
        external_id="media-new",
        expires_at=clock() + timedelta(hours=1),
        processing_metadata={
            "state": "succeeded",
            "source_sha256": source_hash,
            "media_type": "video/mp4",
        },
        **claim,  # type: ignore[arg-type]
    )
    assert replacement.external_id == "media-new"
    assert repository.list_delivery_artifacts(bundle_key, "x") == (replacement,)

    repository.advance_delivery_phase(
        bundle_key,
        "x",
        "final_dispatch_started",
        **claim,  # type: ignore[arg-type]
    )
    clock.advance(hours=2)
    with pytest.raises(TransitionError, match="final dispatch"):
        repository.replace_expired_artifact(
            bundle_key,
            "x",
            kind="x_media_id",
            ordinal=0,
            expected_external_id="media-new",
            external_id="media-never",
            expires_at=clock() + timedelta(hours=1),
            processing_metadata={"state": "succeeded"},
            **claim,  # type: ignore[arg-type]
        )


@pytest.mark.parametrize(
    "metadata",
    [
        {"source_sha256": "not-a-hash"},
        {"media_type": "not a mime"},
        {"next_segment_index": -1},
        {"next_segment_index": 1001},
        {"check_after_seconds": 3601},
    ],
)
def test_remote_processing_metadata_is_bounded(
    tmp_path: Path, metadata: dict[str, object]
) -> None:
    clock = FakeClock()
    repository = _repository(tmp_path, clock)
    bundle_key = _bundle(repository, "x-metadata")
    claim = _claim(repository, bundle_key, "x", "x-metadata-claim")
    with pytest.raises(StateValidationError, match="processing"):
        repository.checkpoint_artifact(
            bundle_key,
            "x",
            kind="x_media_id",
            ordinal=0,
            external_id="media-invalid",
            expires_at=clock() + timedelta(hours=1),
            processing_metadata=metadata,
            **claim,  # type: ignore[arg-type]
        )


def test_instagram_container_and_public_staging_checkpoints_are_durable(
    tmp_path: Path,
) -> None:
    clock = FakeClock()
    repository = _repository(tmp_path, clock)
    bundle_key = repository.add_bundle(
        profile_id="ansonphong",
        bundle_id="instagram-post",
        fingerprint="b" * 64,
        source_bucket="QUEUE",
        files=(_file("instagram-post.jpg"),),
        targets=(_instagram_target(),),
    )
    claim = _claim(repository, bundle_key, "instagram", "claim")
    expiry = clock() + timedelta(hours=24)

    child = repository.checkpoint_artifact(
        bundle_key,
        "instagram",
        kind="instagram_child_container",
        ordinal=0,
        external_id="child-1",
        expires_at=expiry,
        processing_metadata={"state": "IN_PROGRESS"},
        **claim,  # type: ignore[arg-type]
    )
    with pytest.raises(StateValidationError, match="ordinal"):
        repository.checkpoint_artifact(
            bundle_key,
            "instagram",
            kind="instagram_parent_container",
            ordinal=1,
            external_id="parent-wrong-ordinal",
            expires_at=expiry,
            **claim,  # type: ignore[arg-type]
        )
    parent = repository.checkpoint_artifact(
        bundle_key,
        "instagram",
        kind="instagram_parent_container",
        ordinal=0,
        external_id="parent-1",
        expires_at=expiry,
        processing_metadata={"state": "FINISHED"},
        **claim,  # type: ignore[arg-type]
    )
    staged = repository.checkpoint_artifact(
        bundle_key,
        "instagram",
        kind="staged_public",
        ordinal=0,
        relative_path=f"ansonphong-QUEUE-{'b' * 64}-01-{'e' * 64}.jpg",
        sha256="e" * 64,
        expires_at=expiry,
        **claim,  # type: ignore[arg-type]
    )

    assert child.external_id == "child-1"
    assert parent.external_id == "parent-1"
    assert staged.relative_path == f"ansonphong-QUEUE-{'b' * 64}-01-{'e' * 64}.jpg"
    assert staged.expires_at == expiry


def test_published_resets_consecutive_failures_and_preserves_lifetime_attempts(
    tmp_path: Path,
) -> None:
    clock = FakeClock()
    repository = _repository(tmp_path, clock)
    bundle_key = _bundle(repository)
    first_claim = _claim(repository, bundle_key, "x", "one")
    repository.fail_delivery(
        bundle_key,
        "x",
        error_code="retryable",
        error_message="Try later.",
        retry_at=clock(),
        **first_claim,  # type: ignore[arg-type]
    )
    second_claim = _claim(repository, bundle_key, "x", "two")
    repository.advance_delivery_phase(
        bundle_key,
        "x",
        "final_dispatch_started",
        **second_claim,  # type: ignore[arg-type]
    )
    delivery = repository.publish_delivery(
        bundle_key,
        "x",
        remote_id="tweet-1",
        **second_claim,  # type: ignore[arg-type]
    )

    assert delivery.status == "published"
    assert delivery.attempt_count == 2
    assert delivery.consecutive_failures == 0


def test_stale_final_dispatch_is_ambiguous_and_never_auto_retried(
    tmp_path: Path,
) -> None:
    clock = FakeClock()
    repository = _repository(tmp_path, clock)
    bundle_key = _bundle(repository)
    claim = _claim(repository, bundle_key, "x", "claim")
    repository.advance_delivery_phase(
        bundle_key,
        "x",
        "final_dispatch_started",
        **claim,  # type: ignore[arg-type]
    )
    clock.advance(hours=1)

    recovered = repository.recover_stale(
        clock() - timedelta(minutes=30), profile_id="ansonphong"
    )

    assert recovered == ((bundle_key, "x", "ambiguous"),)
    assert repository.get_delivery(bundle_key, "x").status == "ambiguous"
    assert repository.get_bundle(bundle_key).status == "blocked"
    with pytest.raises(TransitionError, match="ambiguous"):
        repository.claim_delivery(bundle_key, "x", "again")


def test_uncertain_final_result_can_be_marked_ambiguous_transactionally(
    tmp_path: Path,
) -> None:
    clock = FakeClock()
    repository = _repository(tmp_path, clock)
    bundle_key = _bundle(repository)
    claim = _claim(repository, bundle_key, "x", "claim")
    repository.advance_delivery_phase(
        bundle_key,
        "x",
        "final_dispatch_started",
        **claim,  # type: ignore[arg-type]
    )

    delivery = repository.mark_delivery_ambiguous(
        bundle_key,
        "x",
        error_code="dispatch_timeout",
        error_message="Final request timed out; outcome is unknown.",
        **claim,  # type: ignore[arg-type]
    )

    assert delivery.status == "ambiguous"
    assert not delivery.safe_to_retry
    assert repository.get_bundle(bundle_key).status == "blocked"


@pytest.mark.parametrize("phase", ["preparing", "processing", "ready"])
def test_stale_pre_final_attempt_becomes_safely_retryable_failure(
    tmp_path: Path, phase: str
) -> None:
    clock = FakeClock()
    repository = _repository(tmp_path, clock)
    bundle_key = _bundle(repository)
    claim = _claim(repository, bundle_key, "x", "claim")
    if phase != "preparing":
        repository.advance_delivery_phase(
            bundle_key,
            "x",
            phase,
            **claim,  # type: ignore[arg-type]
        )
    clock.advance(hours=1)

    recovered = repository.recover_stale(
        clock() - timedelta(minutes=30), profile_id="ansonphong"
    )

    assert recovered == ((bundle_key, "x", "failed"),)
    delivery = repository.get_delivery(bundle_key, "x")
    assert delivery.safe_to_retry
    assert delivery.consecutive_failures == 1


def test_profile_scoped_stale_recovery_reclaims_same_attempt_and_artifacts(
    tmp_path: Path,
) -> None:
    clock = FakeClock()
    repository = _repository(tmp_path, clock)
    first = _bundle(repository)
    claim = _claim(repository, first, "x", "old-claim")
    artifact = repository.checkpoint_artifact(
        first,
        "x",
        kind="x_media_id",
        ordinal=0,
        external_id="resume-media",
        expires_at=clock() + timedelta(hours=2),
        **claim,  # type: ignore[arg-type]
    )
    repository.register_profile(
        "second",
        tmp_path / "accounts/second",
        (
            ProfileTargetSnapshot(
                "x", "30001", "second", "POST_PULSAR_X_SECOND_USER_ACCESS_TOKEN", {}
            ),
        ),
        config_hash="9" * 64,
    )
    second = repository.add_bundle(
        profile_id="second",
        bundle_id="other",
        fingerprint="d" * 64,
        source_bucket="QUEUE",
        files=(_file("other.jpg"),),
        targets=(
            TargetSnapshot(
                "x",
                "30001",
                "second",
                "POST_PULSAR_X_SECOND_USER_ACCESS_TOKEN",
                "2",
                1,
                {},
            ),
        ),
    )
    second_claim = _claim(repository, second, "x", "other-claim")
    clock.advance(hours=1)

    recovered = repository.recover_stale(
        clock() - timedelta(minutes=30),
        profile_id="ansonphong",
        reclaim_token="new-claim",
    )

    assert recovered == ((first, "x", "resumed"),)
    resumed = repository.get_delivery(first, "x")
    assert resumed.attempt_count == claim["attempt_count"]
    assert repository.list_delivery_artifacts(first, "x") == (artifact,)
    assert (
        repository.get_delivery(second, "x").attempt_count
        == second_claim["attempt_count"]
    )


def test_definite_final_rejection_is_failed_and_guarded_not_ambiguous(
    tmp_path: Path,
) -> None:
    repository = _repository(tmp_path, FakeClock())
    bundle_key = _bundle(repository)
    claim = _claim(repository, bundle_key, "x", "final-reject")
    repository.advance_delivery_phase(
        bundle_key,
        "x",
        "final_dispatch_started",
        **claim,  # type: ignore[arg-type]
    )

    rejected = repository.reject_final_delivery(
        bundle_key,
        "x",
        error_code="permission_denied",
        error_message="The platform rejected publication.",
        **claim,  # type: ignore[arg-type]
    )

    assert rejected.status == "failed"
    assert not rejected.safe_to_retry
    assert repository.get_bundle(bundle_key).status == "blocked"


def test_operator_retry_is_guarded_and_resets_only_consecutive_failures(
    tmp_path: Path,
) -> None:
    clock = FakeClock()
    repository = _repository(tmp_path, clock)
    bundle_key = _bundle(repository)
    claim = _claim(repository, bundle_key, "x", "claim")
    repository.fail_delivery(
        bundle_key,
        "x",
        error_code="permanent",
        error_message="Permission denied.",
        retry_at=None,
        permanent=True,
        **claim,  # type: ignore[arg-type]
    )
    before = repository.get_delivery(bundle_key, "x")

    after = repository.operator_retry(
        bundle_key,
        "x",
        expected_bundle_revision=2,
        validated_snapshot=_target(),
    )

    assert after.attempt_count == before.attempt_count == 1
    assert after.consecutive_failures == 0
    assert after.status == "failed"
    assert after.safe_to_retry
    assert repository.get_bundle(bundle_key).status == "active"


def test_warning_events_are_allowlisted_deduplicated_and_secret_safe(
    tmp_path: Path,
) -> None:
    repository = _repository(tmp_path, FakeClock())
    bundle_key = _bundle(repository)
    repository.record_warning(
        bundle_key,
        "instagram",
        "instagram_alt_text_unsupported",
        {"media_count": 1},
    )
    repository.record_warning(
        bundle_key,
        "instagram",
        "instagram_alt_text_unsupported",
        {"media_count": 1},
    )
    assert len(repository.list_events(bundle_key)) == 1

    with pytest.raises(StateValidationError, match="allow-listed"):
        repository.record_warning(bundle_key, "x", "arbitrary", {})
    with pytest.raises(StateValidationError, match="secret"):
        repository.record_warning(
            bundle_key,
            "instagram",
            "instagram_image_normalized",
            {"reason": "never-store-this-token"},
        )


def test_schedule_occurrence_content_claim_and_random_counter_are_atomic(
    tmp_path: Path,
) -> None:
    repository = _repository(tmp_path, FakeClock())
    bundle_key = _bundle(repository)
    schedule = repository.create_schedule(
        profile_id="ansonphong",
        schedule_id="morning",
        bucket="QUEUE",
        timezone="America/Vancouver",
        weekdays=(0, 2, 4),
        local_time="09:30",
        misfire_grace_seconds=600,
        enabled=True,
    )
    run = repository.claim_schedule_occurrence(
        schedule.schedule_key,
        local_date="2026-09-07",
        scheduled_at=datetime(2026, 9, 7, 16, 30, tzinfo=UTC),
        utc_offset_minutes=-420,
        schedule_hash=schedule.config_hash,
        bundle_key=bundle_key,
    )
    replay = repository.claim_schedule_occurrence(
        schedule.schedule_key,
        local_date="2026-09-07",
        scheduled_at=datetime(2026, 9, 7, 16, 30, tzinfo=UTC),
        utc_offset_minutes=-420,
        schedule_hash=schedule.config_hash,
        bundle_key=bundle_key,
    )
    assert replay == run
    assert run.state == "dispatching"
    completed = repository.transition_schedule_run(
        run.run_id, "completed", expected_revision=run.revision
    )
    assert completed.state == "completed"
    with pytest.raises(TransitionError, match="schedule run"):
        repository.transition_schedule_run(
            run.run_id, "failed", expected_revision=completed.revision
        )
    assert repository.next_selection_counter("ansonphong") == 0
    assert repository.next_selection_counter("ansonphong") == 1


def test_transactional_random_admission_selects_fixed_sha256_score_and_links_trigger(
    tmp_path: Path,
) -> None:
    repository = _repository(tmp_path, FakeClock())
    candidates = (
        BundleAdmissionCandidate("Zulu", "b" * 64, (_file("Zulu.jpg"),)),
        BundleAdmissionCandidate("alpha", "c" * 64, (_file("alpha.jpg"),)),
    )

    admitted = repository.admit_selected_bundle(
        profile_id="ansonphong",
        source_bucket="RANDOM",
        candidates=candidates,
        targets=(_target(),),
        trigger_id="manual-42",
    )

    def score(candidate: BundleAdmissionCandidate) -> bytes:
        digest = hashlib.sha256()
        digest.update(b"POST-PULSAR-RANDOM-SELECTION\x00V1\x00")
        for value in (
            "ansonphong".encode(),
            b"0",
            candidate.bundle_id.encode(),
            candidate.fingerprint.encode(),
        ):
            digest.update(len(value).to_bytes(8, "big"))
            digest.update(value)
        return digest.digest()

    expected = min(
        candidates,
        key=lambda item: (score(item), item.bundle_id.casefold(), item.bundle_id),
    )
    assert admitted.bundle_id == expected.bundle_id
    assert admitted.claimed_by_type == "run_once"
    assert admitted.claimed_by_id == "manual-42"
    assert repository.selection_counter("ansonphong") == 1
    replay = repository.admit_selected_bundle(
        profile_id="ansonphong",
        source_bucket="RANDOM",
        candidates=candidates,
        targets=(_target(),),
        trigger_id="manual-42",
    )
    assert replay == admitted
    assert repository.selection_counter("ansonphong") == 1


def test_transactional_admission_rolls_back_selection_counter_on_snapshot_conflict(
    tmp_path: Path,
) -> None:
    repository = _repository(tmp_path, FakeClock())
    candidate = BundleAdmissionCandidate("post", "b" * 64, (_file("post.jpg"),))
    mismatched = TargetSnapshot(
        platform="x",
        expected_remote_user_id="99999",
        expected_username="ansonphong",
        token_env_var="POST_PULSAR_X_ANSONPHONG_USER_ACCESS_TOKEN",
        api_version="2",
        adapter_version=1,
        request_settings={"timeout": 30, "chunk_size": 4194304},
    )

    with pytest.raises(ConflictError, match="target snapshot"):
        repository.admit_selected_bundle(
            profile_id="ansonphong",
            source_bucket="RANDOM",
            candidates=(candidate,),
            targets=(mismatched,),
            trigger_id="manual-42",
        )

    assert repository.selection_counter("ansonphong") == 0
    assert repository.list_active_bundles("ansonphong") == ()


@pytest.mark.parametrize("bucket", ["QUEUE", "REELS"])
def test_transactional_ordered_admission_uses_casefolded_id_without_consuming_random_counter(
    tmp_path: Path, bucket: str
) -> None:
    repository = _repository(tmp_path, FakeClock())
    admitted = repository.admit_selected_bundle(
        profile_id="ansonphong",
        source_bucket=bucket,  # type: ignore[arg-type]
        candidates=(
            BundleAdmissionCandidate("Zulu", "b" * 64, (_file("Zulu.jpg"),)),
            BundleAdmissionCandidate("alpha", "c" * 64, (_file("alpha.jpg"),)),
        ),
        targets=(_target(),),
        trigger_id="scheduler-7",
    )

    assert admitted.bundle_id == "alpha"
    assert admitted.claimed_by_id == "scheduler-7"
    assert repository.selection_counter("ansonphong") == 0


def test_schedule_and_pause_revisions_reject_stale_writes(tmp_path: Path) -> None:
    repository = _repository(tmp_path, FakeClock())
    schedule = repository.create_schedule(
        profile_id="ansonphong",
        schedule_id="morning",
        bucket="RANDOM",
        timezone="UTC",
        weekdays=(1,),
        local_time="10:00",
        misfire_grace_seconds=60,
        enabled=False,
    )
    updated = repository.set_schedule_enabled(
        schedule.schedule_key, True, expected_revision=1
    )
    assert updated.revision == 2
    with pytest.raises(ConflictError, match="revision"):
        repository.set_schedule_enabled(
            schedule.schedule_key, False, expected_revision=1
        )
    pause = repository.set_paused(True, expected_revision=1)
    assert pause.paused and pause.revision == 2
    with pytest.raises(ConflictError, match="revision"):
        repository.set_paused(False, expected_revision=1)


def test_run_requests_are_claimed_once_and_retain_canonical_results(
    tmp_path: Path,
) -> None:
    repository = _repository(tmp_path, FakeClock())
    bundle_key = _bundle(repository)
    request = repository.create_run_request(
        profile_id="ansonphong",
        action="pause",
        arguments={},
        idempotency_key="request-1",
        expected_revision=1,
    )
    assert (
        repository.create_run_request(
            profile_id="ansonphong",
            action="pause",
            arguments={},
            idempotency_key="request-1",
            expected_revision=1,
        )
        == request
    )
    with pytest.raises(ConflictError, match="idempotency"):
        repository.create_run_request(
            profile_id="ansonphong",
            action="pause",
            arguments={"unexpected": True},
            idempotency_key="request-1",
            expected_revision=1,
        )
    claimed = repository.claim_next_run_request("worker-1")
    assert claimed is not None and claimed.status == "claimed"
    completed = repository.complete_run_request(
        claimed.request_id,
        worker_token="worker-1",
        result={"outcome": "accepted", "bundle_key": bundle_key},
    )
    replay = repository.create_run_request(
        profile_id="ansonphong",
        action="pause",
        arguments={},
        idempotency_key="request-1",
        expected_revision=1,
    )
    assert replay.status == "completed"
    assert replay.result == completed.result


def test_resume_always_requires_confirmation_even_when_work_becomes_due(
    tmp_path: Path,
) -> None:
    clock = FakeClock()
    repository = _repository(tmp_path, clock)
    pause = repository.get_pause_state()

    with pytest.raises(TransitionError, match="confirmation"):
        repository.create_run_request(
            profile_id="ansonphong",
            action="resume",
            arguments={},
            idempotency_key="resume-without-work",
            expected_revision=pause.revision,
        )
    assert repository.claim_next_run_request("no-resume-worker") is None

    schedule = repository.create_schedule(
        profile_id="ansonphong",
        schedule_id="resume-due",
        bucket="QUEUE",
        timezone="UTC",
        weekdays=(0,),
        local_time="09:00",
        misfire_grace_seconds=60,
        enabled=True,
    )
    run = repository.create_schedule_occurrence(
        schedule.schedule_key,
        local_date="2026-09-07",
        scheduled_at=datetime(2026, 9, 7, 9, tzinfo=UTC),
        utc_offset_minutes=0,
        schedule_hash=schedule.config_hash,
    )
    repository.transition_schedule_run(
        run.run_id, "due", expected_revision=run.revision
    )

    with pytest.raises(TransitionError, match="confirmation"):
        repository.create_run_request(
            profile_id="ansonphong",
            action="resume",
            arguments={},
            idempotency_key="resume-without-work",
            expected_revision=pause.revision,
        )
    assert repository.claim_next_run_request("still-no-resume-worker") is None

    intent = repository.create_confirmation_intent(
        action="resume",
        arguments={},
        profile_id="ansonphong",
        resource_revision=pause.revision,
        fingerprint=None,
        consequence="Resume queued publishing work.",
        expires_at=clock() + timedelta(minutes=5),
    )
    repository.approve_confirmation_intent(intent.intent_id, expected_revision=1)
    confirmed = repository.consume_intent_with_request(
        intent_id=intent.intent_id,
        action="resume",
        arguments={},
        profile_id="ansonphong",
        resource_revision=pause.revision,
        fingerprint=None,
        idempotency_key="confirmed-resume",
    )
    assert confirmed.status == "queued"


def test_nonconfirmed_request_revision_is_atomic_and_replay_is_canonical(
    tmp_path: Path,
) -> None:
    repository = _repository(tmp_path, FakeClock())
    schedule = repository.create_schedule(
        profile_id="ansonphong",
        schedule_id="disable-request",
        bucket="RANDOM",
        timezone="UTC",
        weekdays=(1,),
        local_time="10:00",
        misfire_grace_seconds=60,
        enabled=False,
    )
    request = repository.create_run_request(
        profile_id="ansonphong",
        action="schedule_disable",
        arguments={"enabled": False},
        idempotency_key="disable-revision-1",
        expected_revision=schedule.revision,
        schedule_key=schedule.schedule_key,
    )
    repository.set_schedule_enabled(
        schedule.schedule_key, True, expected_revision=schedule.revision
    )

    assert (
        repository.create_run_request(
            profile_id="ansonphong",
            action="schedule_disable",
            arguments={"enabled": False},
            idempotency_key="disable-revision-1",
            expected_revision=schedule.revision,
            schedule_key=schedule.schedule_key,
        )
        == request
    )
    with pytest.raises(ConflictError, match="revision"):
        repository.create_run_request(
            profile_id="ansonphong",
            action="schedule_disable",
            arguments={"enabled": False},
            idempotency_key="disable-stale-revision",
            expected_revision=schedule.revision,
            schedule_key=schedule.schedule_key,
        )


def test_approved_intent_is_consumed_once_with_idempotent_request(
    tmp_path: Path,
) -> None:
    clock = FakeClock()
    repository = _repository(tmp_path, clock)
    bundle_key = _bundle(repository)
    intent = repository.create_confirmation_intent(
        action="run_now",
        arguments={"bundle_key": bundle_key},
        profile_id="ansonphong",
        resource_revision=1,
        fingerprint="b" * 64,
        consequence="Publish bundle post to X.",
        expires_at=clock() + timedelta(minutes=5),
        bundle_key=bundle_key,
    )
    approved = repository.approve_confirmation_intent(
        intent.intent_id, expected_revision=1
    )
    assert approved.state == "approved"
    request = repository.consume_intent_with_request(
        intent_id=intent.intent_id,
        action="run_now",
        arguments={"bundle_key": bundle_key},
        profile_id="ansonphong",
        resource_revision=1,
        fingerprint="b" * 64,
        idempotency_key="intent-request-1",
        bundle_key=bundle_key,
    )
    replay = repository.consume_intent_with_request(
        intent_id=intent.intent_id,
        action="run_now",
        arguments={"bundle_key": bundle_key},
        profile_id="ansonphong",
        resource_revision=1,
        fingerprint="b" * 64,
        idempotency_key="intent-request-1",
        bundle_key=bundle_key,
    )
    assert replay == request
    assert repository.get_confirmation_intent(intent.intent_id).state == "consumed"


def test_expired_or_drifted_intent_cannot_create_request(tmp_path: Path) -> None:
    clock = FakeClock()
    repository = _repository(tmp_path, clock)
    bundle_key = _bundle(repository)
    intent = repository.create_confirmation_intent(
        action="retry",
        arguments={"bundle_key": bundle_key},
        profile_id="ansonphong",
        resource_revision=1,
        fingerprint="b" * 64,
        consequence="Retry a blocked delivery.",
        expires_at=clock() + timedelta(seconds=1),
        bundle_key=bundle_key,
    )
    repository.approve_confirmation_intent(intent.intent_id, expected_revision=1)
    clock.advance(seconds=2)
    with pytest.raises(TransitionError, match="expired"):
        repository.consume_intent_with_request(
            intent_id=intent.intent_id,
            action="retry",
            arguments={"bundle_key": bundle_key},
            profile_id="ansonphong",
            resource_revision=1,
            fingerprint="b" * 64,
            idempotency_key="expired",
            bundle_key=bundle_key,
        )
    assert repository.get_confirmation_intent(intent.intent_id).state == "expired"


def test_admission_journal_and_members_checkpoint_idempotently(tmp_path: Path) -> None:
    repository = _repository(tmp_path, FakeClock())
    journal = repository.start_admission(
        profile_id="ansonphong",
        bucket="QUEUE",
        bundle_id="draft-one",
        fingerprint="f" * 64,
        source_path="DRAFTS/QUEUE/draft-one",
        destination_path="QUEUE/draft-one",
        intent_id=None,
    )
    for phase in ("planned", "copied", "verified"):
        repository.checkpoint_admission_member(
            journal.journal_id,
            relative_name="draft-one.jpg",
            sha256="a" * 64,
            size_bytes=5,
            phase=phase,
        )
    revision = repository.get_admission(journal.journal_id).revision
    ready = repository.advance_admission(
        journal.journal_id, "ready_installed", expected_revision=revision
    )
    installed = repository.advance_admission(
        journal.journal_id, "installed", expected_revision=ready.revision
    )

    assert ready.phase == "ready_installed"
    assert installed.phase == "installed"
    assert repository.list_admission_members(journal.journal_id)[0].phase == "verified"


def test_final_dispatch_failure_is_always_ambiguous_and_nonretryable(
    tmp_path: Path,
) -> None:
    clock = FakeClock()
    repository = _repository(tmp_path, clock)
    bundle_key = _bundle(repository)
    claim = _claim(repository, bundle_key, "x", "final-failure")
    repository.advance_delivery_phase(
        bundle_key,
        "x",
        "final_dispatch_started",
        **claim,  # type: ignore[arg-type]
    )

    delivery = repository.fail_delivery(
        bundle_key,
        "x",
        error_code="transport_timeout",
        error_message="The final response was not received.",
        retry_at=clock() + timedelta(minutes=1),
        **claim,  # type: ignore[arg-type]
    )

    assert delivery.status == "ambiguous"
    assert not delivery.safe_to_retry
    assert delivery.next_attempt_at is None
    assert repository.get_bundle(bundle_key).status == "blocked"


def test_stale_delivery_worker_cannot_mutate_a_new_attempt(tmp_path: Path) -> None:
    clock = FakeClock()
    repository = _repository(tmp_path, clock)
    bundle_key = _bundle(repository)
    stale = _claim(repository, bundle_key, "x", "worker-old")
    repository.fail_delivery(
        bundle_key,
        "x",
        error_code="retry",
        error_message="Retry safely.",
        retry_at=clock(),
        **stale,  # type: ignore[arg-type]
    )
    current = _claim(repository, bundle_key, "x", "worker-new")

    with pytest.raises(ConflictError, match="claim"):
        repository.advance_delivery_phase(
            bundle_key,
            "x",
            "processing",
            **stale,  # type: ignore[arg-type]
        )
    with pytest.raises(ConflictError, match="claim"):
        repository.checkpoint_artifact(
            bundle_key,
            "x",
            kind="x_media_id",
            ordinal=0,
            external_id="stale-media",
            expires_at=clock() + timedelta(hours=1),
            **stale,  # type: ignore[arg-type]
        )
    with pytest.raises(ConflictError, match="claim"):
        repository.fail_delivery(
            bundle_key,
            "x",
            error_code="stale",
            error_message="Stale worker.",
            retry_at=clock(),
            **stale,  # type: ignore[arg-type]
        )

    repository.advance_delivery_phase(
        bundle_key,
        "x",
        "final_dispatch_started",
        **current,  # type: ignore[arg-type]
    )
    with pytest.raises(ConflictError, match="claim"):
        repository.publish_delivery(
            bundle_key,
            "x",
            remote_id="stale-post",
            **stale,  # type: ignore[arg-type]
        )
    with pytest.raises(ConflictError, match="claim"):
        repository.mark_delivery_ambiguous(
            bundle_key,
            "x",
            error_code="stale",
            error_message="Stale worker.",
            **stale,  # type: ignore[arg-type]
        )


def test_run_request_matrix_and_intent_resource_drift_fail_closed(
    tmp_path: Path,
) -> None:
    clock = FakeClock()
    repository = _repository(tmp_path, clock)
    bundle_key = _bundle(repository)
    with pytest.raises(TransitionError, match="confirmation"):
        repository.create_run_request(
            profile_id="ansonphong",
            action="run_now",
            arguments={"bundle_key": bundle_key},
            idempotency_key="unconfirmed-run",
            expected_revision=1,
            bundle_key=bundle_key,
        )
    with pytest.raises(StateValidationError, match="bundle resource"):
        repository.create_run_request(
            profile_id="ansonphong",
            action="retry",
            arguments={},
            idempotency_key="missing-bundle",
            expected_revision=1,
        )
    assert (
        repository.create_run_request(
            profile_id="ansonphong",
            action="pause",
            arguments={},
            idempotency_key="safe-pause",
            expected_revision=1,
        ).status
        == "queued"
    )

    intent = repository.create_confirmation_intent(
        action="run_now",
        arguments={"bundle_key": bundle_key},
        profile_id="ansonphong",
        resource_revision=1,
        fingerprint="b" * 64,
        consequence="Publish the exact bundle.",
        expires_at=clock() + timedelta(minutes=5),
        bundle_key=bundle_key,
    )
    repository.approve_confirmation_intent(intent.intent_id, expected_revision=1)
    repository.block_bundle(bundle_key, "operator_hold", expected_revision=1)
    with pytest.raises(ConflictError, match="revision"):
        repository.consume_intent_with_request(
            intent_id=intent.intent_id,
            action="run_now",
            arguments={"bundle_key": bundle_key},
            profile_id="ansonphong",
            resource_revision=1,
            fingerprint="b" * 64,
            idempotency_key="drifted-intent",
            bundle_key=bundle_key,
        )
    assert repository.get_confirmation_intent(intent.intent_id).state == "approved"

    schedule = repository.create_schedule(
        profile_id="ansonphong",
        schedule_id="intent-schedule",
        bucket="QUEUE",
        timezone="UTC",
        weekdays=(0,),
        local_time="10:00",
        misfire_grace_seconds=60,
        enabled=False,
    )
    schedule_intent = repository.create_confirmation_intent(
        action="schedule_enable",
        arguments={"enabled": True},
        profile_id="ansonphong",
        resource_revision=schedule.revision,
        fingerprint=schedule.config_hash,
        consequence="Enable this exact schedule.",
        expires_at=clock() + timedelta(minutes=5),
        schedule_key=schedule.schedule_key,
    )
    repository.approve_confirmation_intent(
        schedule_intent.intent_id, expected_revision=schedule_intent.revision
    )
    schedule_request = repository.consume_intent_with_request(
        intent_id=schedule_intent.intent_id,
        action="schedule_enable",
        arguments={"enabled": True},
        profile_id="ansonphong",
        resource_revision=schedule.revision,
        fingerprint=schedule.config_hash,
        idempotency_key="enable-schedule",
        schedule_key=schedule.schedule_key,
    )
    assert schedule_request.schedule_key == schedule.schedule_key


def test_unfinished_snapshot_prevents_target_removal_or_reassignment(
    tmp_path: Path,
) -> None:
    repository = _repository(tmp_path, FakeClock())
    _bundle(repository)

    with pytest.raises(ConflictError, match="targets cannot change"):
        repository.register_profile(
            "ansonphong",
            tmp_path / "accounts/ansonphong",
            (_instagram_profile_target(),),
            config_hash="2" * 64,
            expected_revision=1,
        )
    with pytest.raises(ConflictError, match="remote identity"):
        repository.register_profile(
            "second",
            tmp_path / "accounts/second",
            (
                ProfileTargetSnapshot(
                    platform="x",
                    expected_remote_user_id="10001",
                    expected_username="second",
                    token_env_var="POST_PULSAR_X_SECOND_USER_ACCESS_TOKEN",
                    request_settings={},
                ),
            ),
            config_hash="3" * 64,
        )


def test_admission_members_are_immutable_monotonic_and_verified_before_ready(
    tmp_path: Path,
) -> None:
    repository = _repository(tmp_path, FakeClock())
    journal = repository.start_admission(
        profile_id="ansonphong",
        bucket="QUEUE",
        bundle_id="strict-draft",
        fingerprint="f" * 64,
        source_path="DRAFTS/QUEUE/strict-draft",
        destination_path="QUEUE/strict-draft",
        intent_id=None,
    )
    checkpoint = {
        "relative_name": "strict-draft.jpg",
        "sha256": "a" * 64,
        "size_bytes": 5,
    }
    with pytest.raises(TransitionError, match="planned"):
        repository.checkpoint_admission_member(
            journal.journal_id, phase="copied", **checkpoint
        )
    repository.checkpoint_admission_member(
        journal.journal_id, phase="planned", **checkpoint
    )
    with pytest.raises(ConflictError, match="identity"):
        repository.checkpoint_admission_member(
            journal.journal_id,
            phase="copied",
            **{**checkpoint, "sha256": "c" * 64},
        )
    with pytest.raises(TransitionError, match="one checkpoint"):
        repository.checkpoint_admission_member(
            journal.journal_id, phase="verified", **checkpoint
        )
    with pytest.raises(TransitionError, match="verified"):
        repository.advance_admission(
            journal.journal_id,
            "ready_installed",
            expected_revision=repository.get_admission(journal.journal_id).revision,
        )
    repository.checkpoint_admission_member(
        journal.journal_id, phase="copied", **checkpoint
    )
    repository.checkpoint_admission_member(
        journal.journal_id, phase="verified", **checkpoint
    )
    ready = repository.advance_admission(
        journal.journal_id,
        "ready_installed",
        expected_revision=repository.get_admission(journal.journal_id).revision,
    )
    assert (
        repository.advance_admission(
            journal.journal_id, "installed", expected_revision=ready.revision
        ).phase
        == "installed"
    )


def test_schedule_runs_expose_every_recoverable_graph_state(tmp_path: Path) -> None:
    repository = _repository(tmp_path, FakeClock())
    bundle_key = _bundle(repository)
    schedule = repository.create_schedule(
        profile_id="ansonphong",
        schedule_id="graph",
        bucket="QUEUE",
        timezone="UTC",
        weekdays=(0,),
        local_time="09:00",
        misfire_grace_seconds=60,
        enabled=True,
    )
    first = repository.create_schedule_occurrence(
        schedule.schedule_key,
        local_date="2026-09-07",
        scheduled_at=datetime(2026, 9, 7, 9, tzinfo=UTC),
        utc_offset_minutes=0,
        schedule_hash=schedule.config_hash,
    )
    assert first.state == "queued" and first.bundle_key is None
    due = repository.transition_schedule_run(
        first.run_id, "due", expected_revision=first.revision
    )
    no_content = repository.transition_schedule_run(
        due.run_id, "no_content", expected_revision=due.revision
    )
    assert no_content.state == "no_content"

    second = repository.create_schedule_occurrence(
        schedule.schedule_key,
        local_date="2026-09-14",
        scheduled_at=datetime(2026, 9, 14, 9, tzinfo=UTC),
        utc_offset_minutes=0,
        schedule_hash=schedule.config_hash,
    )
    assert (
        repository.transition_schedule_run(
            second.run_id, "missed", expected_revision=second.revision
        ).state
        == "missed"
    )

    third = repository.create_schedule_occurrence(
        schedule.schedule_key,
        local_date="2026-09-21",
        scheduled_at=datetime(2026, 9, 21, 9, tzinfo=UTC),
        utc_offset_minutes=0,
        schedule_hash=schedule.config_hash,
    )
    third_due = repository.transition_schedule_run(
        third.run_id, "due", expected_revision=third.revision
    )
    dispatching = repository.claim_schedule_content(
        third.run_id, bundle_key, expected_revision=third_due.revision
    )
    with pytest.raises(TransitionError, match="illegal schedule run transition"):
        repository.transition_schedule_run(
            third.run_id, "no_content", expected_revision=dispatching.revision
        )
    still_dispatching = repository.get_schedule_run(third.run_id)
    assert still_dispatching.state == "dispatching"
    assert still_dispatching.bundle_key == bundle_key
    assert (
        repository.transition_schedule_run(
            third.run_id, "failed", expected_revision=still_dispatching.revision
        ).state
        == "failed"
    )


@pytest.mark.parametrize(
    "tamper",
    [
        "DROP INDEX delivery_remote_identity",
        "DROP TRIGGER bundle_files_no_update",
        "ALTER TABLE profiles ADD COLUMN injected TEXT",
    ],
)
def test_current_schema_manifest_rejects_structural_drift(
    tmp_path: Path, tamper: str
) -> None:
    path = tmp_path / "manifest.sqlite3"
    StateRepository(path).close()
    connection = sqlite3.connect(path)
    connection.execute(tamper)
    connection.close()

    with pytest.raises(MigrationRequiredError, match="migration required"):
        StateRepository(path)


def test_conflicting_application_identity_is_rejected_safely(tmp_path: Path) -> None:
    path = tmp_path / "foreign.sqlite3"
    connection = sqlite3.connect(path)
    connection.execute("PRAGMA application_id = 12345")
    connection.close()

    with pytest.raises(MigrationRequiredError, match="application identity"):
        StateRepository(path)


def test_artifact_kind_path_expiry_and_global_external_identity_invariants(
    tmp_path: Path,
) -> None:
    clock = FakeClock()
    repository = _repository(tmp_path, clock)
    first_bundle = _bundle(repository)
    first_claim = _claim(repository, first_bundle, "x", "first-artifacts")

    with pytest.raises(StateValidationError, match="expiry"):
        repository.checkpoint_artifact(
            first_bundle,
            "x",
            kind="x_media_id",
            ordinal=0,
            external_id="shared-media",
            **first_claim,  # type: ignore[arg-type]
        )
    with pytest.raises(StateValidationError, match="bundle identity"):
        repository.checkpoint_artifact(
            first_bundle,
            "x",
            kind="staged_private",
            ordinal=0,
            relative_path=f"other/QUEUE/{'b' * 64}/media.jpg",
            sha256="d" * 64,
            **first_claim,  # type: ignore[arg-type]
        )
    with pytest.raises(StateValidationError, match="bundle identity"):
        repository.checkpoint_artifact(
            first_bundle,
            "x",
            kind="staged_private",
            ordinal=0,
            relative_path=f"ansonphong/RANDOM/{'b' * 64}/media.jpg",
            sha256="d" * 64,
            **first_claim,  # type: ignore[arg-type]
        )
    repository.checkpoint_artifact(
        first_bundle,
        "x",
        kind="x_media_id",
        ordinal=0,
        external_id="shared-media",
        expires_at=clock() + timedelta(hours=1),
        **first_claim,  # type: ignore[arg-type]
    )

    repository.register_profile(
        "second",
        tmp_path / "accounts/second",
        (
            ProfileTargetSnapshot(
                platform="x",
                expected_remote_user_id="10002",
                expected_username="second",
                token_env_var="POST_PULSAR_X_SECOND_USER_ACCESS_TOKEN",
                request_settings={},
            ),
        ),
        config_hash="2" * 64,
    )
    second_bundle = repository.add_bundle(
        profile_id="second",
        bundle_id="other",
        fingerprint="c" * 64,
        source_bucket="QUEUE",
        files=(_file("other.jpg"),),
        targets=(
            TargetSnapshot(
                platform="x",
                expected_remote_user_id="10002",
                expected_username="second",
                token_env_var="POST_PULSAR_X_SECOND_USER_ACCESS_TOKEN",
                api_version="2",
                adapter_version=1,
                request_settings={},
            ),
        ),
    )
    second_claim = _claim(repository, second_bundle, "x", "second-artifacts")
    with pytest.raises(ConflictError, match="artifact identity"):
        repository.checkpoint_artifact(
            second_bundle,
            "x",
            kind="x_media_id",
            ordinal=0,
            external_id="shared-media",
            expires_at=clock() + timedelta(hours=1),
            **second_claim,  # type: ignore[arg-type]
        )
