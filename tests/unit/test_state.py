"""Transactional SQLite state-machine tests."""

from __future__ import annotations

import sqlite3
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from post_pulsar.state import (
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
        (_profile_target(),),
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

    repository.claim_delivery(bundle_key, "x", "archive-claim")
    repository.advance_delivery_phase(bundle_key, "x", "final_dispatch_started")
    repository.publish_delivery(bundle_key, "x", remote_id="tweet-archive")
    repository.begin_archiving(bundle_key, expected_revision=1)
    repository.mark_archived(bundle_key, "POSTED/QUEUE/post", expected_revision=2)
    updated = repository.register_profile(
        "ansonphong",
        tmp_path / "accounts/moved",
        (_profile_target(),),
        config_hash="3" * 64,
        expected_revision=1,
    )
    assert updated.revision == 2


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
            ordinal=attempt,
            external_id=f"media-{attempt}",
            expires_at=clock() + timedelta(hours=1),
            processing_metadata={"state": "pending", "progress_percent": 25},
        )
        repository.advance_delivery_phase(bundle_key, "x", "processing")
        repository.advance_delivery_phase(bundle_key, "x", "ready")
        failed = repository.fail_delivery(
            bundle_key,
            "x",
            error_code="platform_busy",
            error_message="Platform asked to retry later.",
            retry_at=clock(),
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
    repository.claim_delivery(bundle_key, "x", "claim")
    values = {
        "kind": "staged_private",
        "ordinal": 0,
        "relative_path": "ansonphong/abc/media.jpg",
        "sha256": "d" * 64,
        "expires_at": clock() + timedelta(hours=1),
        "processing_metadata": {"state": "ready", "check_after_seconds": 2},
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


def test_published_resets_consecutive_failures_and_preserves_lifetime_attempts(
    tmp_path: Path,
) -> None:
    clock = FakeClock()
    repository = _repository(tmp_path, clock)
    bundle_key = _bundle(repository)
    repository.claim_delivery(bundle_key, "x", "one")
    repository.fail_delivery(
        bundle_key,
        "x",
        error_code="retryable",
        error_message="Try later.",
        retry_at=clock(),
    )
    repository.claim_delivery(bundle_key, "x", "two")
    repository.advance_delivery_phase(bundle_key, "x", "final_dispatch_started")
    delivery = repository.publish_delivery(bundle_key, "x", remote_id="tweet-1")

    assert delivery.status == "published"
    assert delivery.attempt_count == 2
    assert delivery.consecutive_failures == 0


def test_stale_final_dispatch_is_ambiguous_and_never_auto_retried(
    tmp_path: Path,
) -> None:
    clock = FakeClock()
    repository = _repository(tmp_path, clock)
    bundle_key = _bundle(repository)
    repository.claim_delivery(bundle_key, "x", "claim")
    repository.advance_delivery_phase(bundle_key, "x", "final_dispatch_started")
    clock.advance(hours=1)

    recovered = repository.recover_stale(clock() - timedelta(minutes=30))

    assert recovered == ((bundle_key, "x", "ambiguous"),)
    assert repository.get_delivery(bundle_key, "x").status == "ambiguous"
    assert repository.get_bundle(bundle_key).status == "blocked"
    with pytest.raises(TransitionError, match="ambiguous"):
        repository.claim_delivery(bundle_key, "x", "again")


def test_stale_pre_final_attempt_becomes_safely_retryable_failure(
    tmp_path: Path,
) -> None:
    clock = FakeClock()
    repository = _repository(tmp_path, clock)
    bundle_key = _bundle(repository)
    repository.claim_delivery(bundle_key, "x", "claim")
    repository.advance_delivery_phase(bundle_key, "x", "processing")
    clock.advance(hours=1)

    recovered = repository.recover_stale(clock() - timedelta(minutes=30))

    assert recovered == ((bundle_key, "x", "failed"),)
    delivery = repository.get_delivery(bundle_key, "x")
    assert delivery.safe_to_retry
    assert delivery.consecutive_failures == 1


def test_operator_retry_is_guarded_and_resets_only_consecutive_failures(
    tmp_path: Path,
) -> None:
    clock = FakeClock()
    repository = _repository(tmp_path, clock)
    bundle_key = _bundle(repository)
    repository.claim_delivery(bundle_key, "x", "claim")
    repository.fail_delivery(
        bundle_key,
        "x",
        error_code="permanent",
        error_message="Permission denied.",
        retry_at=None,
        permanent=True,
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
    assert repository.next_selection_counter("ansonphong") == 0
    assert repository.next_selection_counter("ansonphong") == 1


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
        action="run_now",
        arguments={"bucket": "QUEUE"},
        idempotency_key="request-1",
        expected_revision=1,
        bundle_key=bundle_key,
    )
    assert repository.create_run_request(
        profile_id="ansonphong",
        action="run_now",
        arguments={"bucket": "QUEUE"},
        idempotency_key="request-1",
        expected_revision=1,
        bundle_key=bundle_key,
    ) == request
    with pytest.raises(ConflictError, match="idempotency"):
        repository.create_run_request(
            profile_id="ansonphong",
            action="run_now",
            arguments={"bucket": "RANDOM"},
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
        action="run_now",
        arguments={"bucket": "QUEUE"},
        idempotency_key="request-1",
        expected_revision=1,
        bundle_key=bundle_key,
    )
    assert replay.status == "completed"
    assert replay.result == completed.result


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
    intent = repository.create_confirmation_intent(
        action="retry",
        arguments={"bundle": "post"},
        profile_id="ansonphong",
        resource_revision=4,
        fingerprint="b" * 64,
        consequence="Retry a blocked delivery.",
        expires_at=clock() + timedelta(seconds=1),
    )
    repository.approve_confirmation_intent(intent.intent_id, expected_revision=1)
    clock.advance(seconds=2)
    with pytest.raises(TransitionError, match="expired"):
        repository.consume_intent_with_request(
            intent_id=intent.intent_id,
            action="retry",
            arguments={"bundle": "post"},
            profile_id="ansonphong",
            resource_revision=4,
            fingerprint="b" * 64,
            idempotency_key="expired",
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
    repository.checkpoint_admission_member(
        journal.journal_id,
        relative_name="draft-one.jpg",
        sha256="a" * 64,
        size_bytes=5,
        phase="copied",
    )
    repository.checkpoint_admission_member(
        journal.journal_id,
        relative_name="draft-one.jpg",
        sha256="a" * 64,
        size_bytes=5,
        phase="copied",
    )
    ready = repository.advance_admission(
        journal.journal_id, "ready_installed", expected_revision=2
    )
    installed = repository.advance_admission(
        journal.journal_id, "installed", expected_revision=3
    )

    assert ready.phase == "ready_installed"
    assert installed.phase == "installed"
    assert repository.list_admission_members(journal.journal_id)[0].phase == "copied"
