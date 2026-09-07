# mypy: disable-error-code=import-untyped
# ruff: noqa: E402
"""Offline end-to-end evidence for publication, recovery, and local control."""

from __future__ import annotations

import io
import json
import socket
import sys
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import TYPE_CHECKING, cast

import pytest
from PIL import Image

TESTS_ROOT = str(Path(__file__).parents[1])
if TESTS_ROOT not in sys.path:
    sys.path.insert(0, TESTS_ROOT)

if TYPE_CHECKING:
    from tests.support.fake_daemon import (
        AdapterPlan,
        FakeAdapterRegistry,
        FakeClock,
        InjectedCrash,
        LoopbackSocketGuard,
        launch_fake_daemon,
    )
else:
    from support.fake_daemon import (
        AdapterPlan,
        FakeAdapterRegistry,
        FakeClock,
        InjectedCrash,
        LoopbackSocketGuard,
        launch_fake_daemon,
    )

from post_pulsar import cli
from post_pulsar.app import (
    OneRunApplication,
    OrchestrationFaultInjector,
    RunOnceRequest,
    RunOutcome,
)
from post_pulsar.archive import ArchiveFaultInjector, ArchiveManager
from post_pulsar.config import LocalSettings, load_local_settings
from post_pulsar.content import PublishableBundle, scan_account_root
from post_pulsar.control import initialize_operator_secret, rotate_agent_capability
from post_pulsar.daemon import EndpointRecord, ForegroundDaemon
from post_pulsar.locking import LockContentionError, LockLease, LockManager
from post_pulsar.platforms.base import PublishResult, ValidationIssue
from post_pulsar.scheduler import DeterministicScheduler, evaluate_schedule
from post_pulsar.state import (
    BundleFileSnapshot,
    ConflictError,
    Platform,
    ProfileTargetSnapshot,
    SourceBucket,
    StateRepository,
    TransitionError,
)

NOW = datetime(2026, 9, 5, 12, tzinfo=UTC)
PROFILES = ("ansonphong", "360hextile")
FAKE_ENV = {
    "POST_PULSAR_X_ANSONPHONG_USER_ACCESS_TOKEN": "fake-only-anson-x-token",
    "POST_PULSAR_INSTAGRAM_ANSONPHONG_ACCESS_TOKEN": "fake-only-anson-ig-token",
    "POST_PULSAR_X_360HEXTILE_USER_ACCESS_TOKEN": "fake-only-hextile-x-token",
    "POST_PULSAR_INSTAGRAM_360HEXTILE_ACCESS_TOKEN": "fake-only-hextile-ig-token",
}


class _TTY(io.StringIO):
    def isatty(self) -> bool:
        return True


def _write_config(
    root: Path,
    *,
    enabled: dict[str, tuple[str, ...]] | None = None,
    allow_agent_publish: bool = False,
    anson_x_id: str = "10001",
) -> Path:
    root.mkdir(parents=True, exist_ok=True)
    selected = enabled or {
        "ansonphong": ("x", "instagram"),
        "360hextile": ("x",),
    }
    identities = {
        "ansonphong": (anson_x_id, "20001"),
        "360hextile": ("30001", "40001"),
    }
    sections: list[str] = []
    for profile_id in PROFILES:
        x_id, instagram_id = identities[profile_id]
        env_stem = "ANSONPHONG" if profile_id == "ansonphong" else "360HEXTILE"
        sections.append(
            f"""
[[profiles]]
profile_id = "{profile_id}"
account_root = "profiles/{profile_id}"
timezone = "UTC"

[profiles.x]
enabled = {str("x" in selected.get(profile_id, ())).lower()}
expected_remote_user_id = "{x_id}"
expected_username = "{profile_id}"
token_env_var = "POST_PULSAR_X_{env_stem}_USER_ACCESS_TOKEN"
request_timeout_seconds = 1
processing_timeout_seconds = 5
chunk_size_bytes = 1048576

[profiles.instagram]
enabled = {str("instagram" in selected.get(profile_id, ())).lower()}
expected_remote_user_id = "{instagram_id}"
expected_username = "{profile_id}"
token_env_var = "POST_PULSAR_INSTAGRAM_{env_stem}_ACCESS_TOKEN"
media_directory = "media/{profile_id}"
media_base_url = "https://media.invalid/{profile_id}/"
request_timeout_seconds = 1
processing_timeout_seconds = 5
""".strip()
        )
    config = root / "post-pulsar.toml"
    profiles = "\n\n".join(sections)
    config.write_text(
        f"""
[app]
state_directory = "state"
log_file = "state/post_pulsar.log"
agent_capability_file = "state/control/agent-capability"
operator_verifier_file = "state/control/operator-verifier"
bootstrap_file = "state/control/bootstrap.json"
endpoint_record_file = "state/control/endpoint.json"
control_host = "127.0.0.1"
control_port = 8765
allow_agent_publish = {str(allow_agent_publish).lower()}
control_max_body_bytes = 65536
control_max_results = 25
confirmation_ttl_seconds = 300

{profiles}
""".strip()
        + "\n",
        encoding="utf-8",
    )
    return config


def _profile_target_snapshots(
    settings: LocalSettings, profile_id: str
) -> tuple[ProfileTargetSnapshot, ...]:
    profile = settings.profile(profile_id)
    targets: list[ProfileTargetSnapshot] = []
    if profile.x is not None:
        targets.append(
            ProfileTargetSnapshot(
                "x",
                profile.x.expected_remote_user_id,
                profile.x.expected_username,
                profile.x.token_env_var,
                {
                    "request_timeout_seconds": profile.x.request_timeout_seconds,
                    "processing_timeout_seconds": profile.x.processing_timeout_seconds,
                    "chunk_size_bytes": profile.x.chunk_size_bytes,
                },
            )
        )
    if profile.instagram is not None:
        targets.append(
            ProfileTargetSnapshot(
                "instagram",
                profile.instagram.expected_remote_user_id,
                profile.instagram.expected_username,
                profile.instagram.token_env_var,
                {
                    "media_directory": str(profile.instagram.media_directory),
                    "media_base_url": profile.instagram.media_base_url,
                    "request_timeout_seconds": (
                        profile.instagram.request_timeout_seconds
                    ),
                    "processing_timeout_seconds": (
                        profile.instagram.processing_timeout_seconds
                    ),
                },
            )
        )
    return tuple(targets)


def _initialize(
    root: Path,
    clock: FakeClock,
    *,
    enabled: dict[str, tuple[str, ...]] | None = None,
    allow_agent_publish: bool = False,
) -> tuple[Path, LocalSettings, LockManager, LockLease]:
    config = _write_config(
        root, enabled=enabled, allow_agent_publish=allow_agent_publish
    )
    settings = load_local_settings(config)
    for profile in settings.profiles:
        profile.account_root.mkdir(parents=True, exist_ok=True)
    database = settings.app.state_directory / "post_pulsar.sqlite3"
    with StateRepository(database, clock=clock.now) as repository:
        for profile in settings.profiles:
            repository.register_profile(
                profile.profile_id,
                profile.account_root,
                _profile_target_snapshots(settings, profile.profile_id),
                config_hash=("a" if profile.profile_id == "ansonphong" else "b") * 64,
            )
    locks = LockManager(settings.app.state_directory)
    return config, settings, locks, locks.acquire_instance()


def _bundle(
    settings: LocalSettings,
    profile_id: str,
    bucket: str,
    bundle_id: str,
    *,
    ready: bool = True,
    media_suffix: str = ".jpg",
) -> Path:
    account_root = cast(Path, settings.profile(profile_id).account_root)
    directory = account_root / bucket / bundle_id
    directory.mkdir(parents=True)
    media = directory / f"{bundle_id}{media_suffix}"
    if media_suffix == ".jpg":
        Image.new("RGB", (4, 4), "purple").save(media)
    else:
        media.write_bytes(b"fake-container-bytes")
    (directory / f"{bundle_id}.txt").write_text(
        f"offline caption for {bundle_id}\n", encoding="utf-8"
    )
    if ready:
        (directory / ".ready").write_bytes(b"")
    return directory


def _run(
    config: Path,
    locks: LockManager,
    instance: LockLease,
    clock: FakeClock,
    registry: FakeAdapterRegistry,
    profile_id: str,
    bucket: SourceBucket,
    trigger: str,
    *,
    fault: OrchestrationFaultInjector | None = None,
) -> RunOutcome:
    return OneRunApplication(
        config,
        locks=locks,
        instance_lease=instance,
        environ=FAKE_ENV,
        clock=clock.now,
        adapter_factory=registry.factory,
        fault_injector=fault,
    ).run_once(RunOnceRequest(profile_id, bucket, trigger))


def _candidate_files(bundle: PublishableBundle) -> tuple[BundleFileSnapshot, ...]:
    return tuple(
        BundleFileSnapshot(
            member.relative_name,
            member.role,
            member.ordinal,
            member.media_kind,
            member.mime_type,
            member.size_bytes,
            member.sha256,
        )
        for member in bundle.content.member_snapshots
    )


def _admit_existing(
    settings: LocalSettings,
    clock: FakeClock,
    profile_id: str,
    bundle_id: str,
    *,
    platforms: tuple[Platform, ...],
) -> tuple[int, str]:
    profile = settings.profile(profile_id)
    selected = next(
        item
        for item in scan_account_root(profile.account_root).bundles
        if item.bundle_id == bundle_id
    )
    snapshots = tuple(
        cli._configured_target_snapshot(profile, platform) for platform in platforms
    )
    with StateRepository.open_existing(
        settings.app.state_directory / "post_pulsar.sqlite3", clock=clock.now
    ) as repository:
        key = repository.add_bundle(
            profile_id=profile_id,
            bundle_id=bundle_id,
            fingerprint=selected.fingerprint,
            source_bucket="QUEUE",
            files=_candidate_files(selected),
            targets=snapshots,
        )
    return key, selected.fingerprint


def test_selection_validation_preflight_archive_and_profile_isolation(
    tmp_path: Path,
) -> None:
    empty_root = tmp_path / "empty"
    clock = FakeClock(NOW)
    config, settings, locks, instance = _initialize(empty_root, clock)
    registry = FakeAdapterRegistry()
    try:
        empty = _run(
            config, locks, instance, clock, registry, "ansonphong", "QUEUE", "empty"
        )
        assert empty.status == "empty"
        invalid = _bundle(settings, "ansonphong", "QUEUE", "invalid", ready=False)
        rejected = _run(
            config,
            locks,
            instance,
            clock,
            registry,
            "ansonphong",
            "QUEUE",
            "invalid",
        )
        assert rejected.status == "invalid"
        assert invalid.is_dir() and registry.traces == []
    finally:
        instance.release()

    zero_root = tmp_path / "zero"
    zero_clock = FakeClock(NOW)
    zero_config, zero_settings, zero_locks, zero_instance = _initialize(
        zero_root,
        zero_clock,
        enabled={"ansonphong": (), "360hextile": ()},
    )
    _bundle(zero_settings, "ansonphong", "QUEUE", "zero-target")
    try:
        zero = _run(
            zero_config,
            zero_locks,
            zero_instance,
            zero_clock,
            FakeAdapterRegistry(),
            "ansonphong",
            "QUEUE",
            "zero",
        )
        assert (zero.status, zero.code) == ("invalid", "no_enabled_targets")
    finally:
        zero_instance.release()

    barrier_root = tmp_path / "barrier"
    barrier_clock = FakeClock(NOW)
    barrier_config, barrier_settings, barrier_locks, barrier_instance = _initialize(
        barrier_root, barrier_clock
    )
    source = _bundle(barrier_settings, "ansonphong", "QUEUE", "barrier")
    barrier_registry = FakeAdapterRegistry()
    barrier_registry.set_plan(
        "ansonphong",
        "instagram",
        AdapterPlan(
            issues=(
                ValidationIssue(
                    "instagram",
                    "error",
                    "fake_preflight_rejected",
                    "Fake preflight rejected the caption.",
                    "caption",
                ),
            )
        ),
    )
    try:
        blocked = _run(
            barrier_config,
            barrier_locks,
            barrier_instance,
            barrier_clock,
            barrier_registry,
            "ansonphong",
            "QUEUE",
            "barrier",
        )
        assert blocked.status == "blocked"
        assert len(barrier_registry.traces) == 2
        assert all(trace.preflights == 1 for trace in barrier_registry.traces)
        assert all(
            trace.prepares == trace.commits == 0 and trace.closed
            for trace in barrier_registry.traces
        )
        assert source.is_dir()
        assert not any((barrier_root / "state/staging").rglob("*.jpg"))
    finally:
        barrier_instance.release()

    success_root = tmp_path / "success"
    success_clock = FakeClock(NOW)
    success_config, success_settings, success_locks, success_instance = _initialize(
        success_root, success_clock
    )
    anson_source = _bundle(success_settings, "ansonphong", "QUEUE", "two-target")
    hextile_source = _bundle(success_settings, "360hextile", "QUEUE", "isolated")
    success_registry = FakeAdapterRegistry()
    success_registry.set_plan(
        "360hextile", "x", AdapterPlan(prepare_failure="safe_pre_final")
    )
    try:
        archived = _run(
            success_config,
            success_locks,
            success_instance,
            success_clock,
            success_registry,
            "ansonphong",
            "QUEUE",
            "two-target",
        )
        failed = _run(
            success_config,
            success_locks,
            success_instance,
            success_clock,
            success_registry,
            "360hextile",
            "QUEUE",
            "isolated",
        )
        assert archived.status == "archived" and failed.status == "failed"
        assert not anson_source.exists() and hextile_source.is_dir()
        assert (
            success_settings.profile("ansonphong").account_root
            / "POSTED/QUEUE/two-target"
        ).is_dir()
        anson_traces = [
            trace
            for trace in success_registry.traces
            if trace.profile_id == "ansonphong"
        ]
        hextile_traces = [
            trace
            for trace in success_registry.traces
            if trace.profile_id == "360hextile"
        ]
        assert {trace.platform for trace in anson_traces} == {"x", "instagram"}
        assert len({trace.client_id for trace in success_registry.traces}) == 3
        assert len(hextile_traces) == 1 and hextile_traces[0].commits == 0
        with StateRepository.open_existing(
            success_settings.app.state_directory / "post_pulsar.sqlite3"
        ) as repository:
            assert repository.list_protected_bundles("ansonphong") == ()
            assert len(repository.list_protected_bundles("360hextile")) == 1
    finally:
        success_instance.release()


def test_ready_buckets_and_random_selection_are_restart_deterministic(
    tmp_path: Path,
) -> None:
    selected: list[str] = []
    for suffix in ("first", "restart"):
        root = tmp_path / suffix
        clock = FakeClock(NOW)
        config, settings, locks, instance = _initialize(
            root,
            clock,
            enabled={"ansonphong": ("x",), "360hextile": ("x",)},
        )
        _bundle(settings, "ansonphong", "QUEUE", "queue-ready")
        _bundle(
            settings,
            "ansonphong",
            "REELS",
            "reel-ready",
            media_suffix=".mp4",
        )
        _bundle(settings, "360hextile", "RANDOM", "alpha")
        _bundle(settings, "360hextile", "RANDOM", "Zulu")
        scan = scan_account_root(settings.profile("ansonphong").account_root)
        assert {bundle.bucket for bundle in scan.bundles} == {"QUEUE", "REELS"}
        assert all(bundle.ready_marker.name == ".ready" for bundle in scan.bundles)
        try:
            outcome = _run(
                config,
                locks,
                instance,
                clock,
                FakeAdapterRegistry(),
                "360hextile",
                "RANDOM",
                f"random-{suffix}",
            )
            assert outcome.status == "archived"
            selected.append(cast(str, outcome.bundle_id))
        finally:
            instance.release()
    assert selected[0] == selected[1]


def test_partial_retry_waits_for_exact_due_time_and_skips_published_target(
    tmp_path: Path,
) -> None:
    clock = FakeClock(NOW)
    config, settings, locks, instance = _initialize(tmp_path, clock)
    source = _bundle(settings, "ansonphong", "QUEUE", "partial")
    registry = FakeAdapterRegistry()
    registry.set_plan("ansonphong", "x", AdapterPlan(prepare_failure="safe_pre_final"))
    try:
        first = _run(
            config,
            locks,
            instance,
            clock,
            registry,
            "ansonphong",
            "QUEUE",
            "partial-first",
        )
        assert first.status == "failed"
        bundle_key = cast(int, first.bundle_key)
        with StateRepository.open_existing(
            settings.app.state_directory / "post_pulsar.sqlite3", clock=clock.now
        ) as repository:
            deliveries = {
                item.platform: item
                for item in repository.list_bundle_deliveries(bundle_key)
            }
            assert deliveries["instagram"].status == "published"
            due = cast(datetime, deliveries["x"].next_attempt_at)
        commits_before = sum(trace.commits for trace in registry.traces)
        clock.advance(due - clock.now() - timedelta(microseconds=1))
        early = _run(
            config,
            locks,
            instance,
            clock,
            registry,
            "ansonphong",
            "QUEUE",
            "partial-early",
        )
        assert (early.status, early.code) == ("deferred", "retry_not_due")
        assert sum(trace.commits for trace in registry.traces) == commits_before
        clock.advance(timedelta(microseconds=1))
        registry.set_plan("ansonphong", "x", AdapterPlan())
        resumed = _run(
            config,
            locks,
            instance,
            clock,
            registry,
            "ansonphong",
            "QUEUE",
            "partial-due",
        )
        assert resumed.status == "archived"
        retry_traces = registry.traces[2:]
        assert [trace.platform for trace in retry_traces] == ["x"]
        assert not source.exists()
        assert not any((tmp_path / "state/staging").rglob("*.jpg"))
        assert not any((tmp_path / "media").rglob("*.jpg"))
    finally:
        instance.release()


def test_five_failures_require_guarded_retry_and_snapshot_revalidation(
    tmp_path: Path,
) -> None:
    clock = FakeClock(NOW)
    enabled = {"ansonphong": ("x",), "360hextile": ()}
    config, settings, locks, instance = _initialize(tmp_path, clock, enabled=enabled)
    source = _bundle(settings, "ansonphong", "QUEUE", "exhaust")
    registry = FakeAdapterRegistry()
    registry.set_plan("ansonphong", "x", AdapterPlan(prepare_failure="safe_pre_final"))
    try:
        result: RunOutcome | None = None
        for attempt in range(5):
            result = _run(
                config,
                locks,
                instance,
                clock,
                registry,
                "ansonphong",
                "QUEUE",
                f"failure-{attempt}",
            )
            if attempt < 4:
                assert result.status == "failed"
                with StateRepository.open_existing(
                    settings.app.state_directory / "post_pulsar.sqlite3",
                    clock=clock.now,
                ) as repository:
                    delivery = repository.get_delivery(
                        cast(int, result.bundle_key), "x"
                    )
                    retry_at = cast(datetime, delivery.next_attempt_at)
                clock.advance(retry_at - clock.now())
        assert result is not None and result.status == "blocked"
        bundle_key = cast(int, result.bundle_key)
        database = settings.app.state_directory / "post_pulsar.sqlite3"
        stored_snapshot = cli._configured_target_snapshot(
            settings.profile("ansonphong"), "x"
        )
        with StateRepository.open_existing(database, clock=clock.now) as repository:
            before_bundle = repository.get_bundle(bundle_key)
            before_delivery = repository.get_delivery(bundle_key, "x")
            assert before_delivery.consecutive_failures == 5
            assert not before_delivery.safe_to_retry
            bad_snapshot = replace(stored_snapshot, expected_remote_user_id="99999")
            with pytest.raises(ConflictError, match="snapshot"):
                repository.operator_retry(
                    bundle_key,
                    "x",
                    expected_bundle_revision=before_bundle.revision,
                    validated_snapshot=bad_snapshot,
                )
            assert repository.get_bundle(bundle_key) == before_bundle
            assert repository.get_delivery(bundle_key, "x") == before_delivery
            repository.operator_retry(
                bundle_key,
                "x",
                expected_bundle_revision=before_bundle.revision,
                validated_snapshot=stored_snapshot,
            )

        _write_config(tmp_path, enabled=enabled, anson_x_id="99999")
        trace_count = len(registry.traces)
        drifted = _run(
            config,
            locks,
            instance,
            clock,
            registry,
            "ansonphong",
            "QUEUE",
            "drifted",
        )
        assert (drifted.status, drifted.code) == (
            "blocked",
            "target_snapshot_drift",
        )
        assert len(registry.traces) == trace_count and source.is_dir()

        _write_config(tmp_path, enabled=enabled)
        settings = load_local_settings(config)
        with StateRepository.open_existing(database, clock=clock.now) as repository:
            blocked = repository.get_bundle(bundle_key)
            assert blocked.status == "blocked"
            repository.operator_retry(
                bundle_key,
                "x",
                expected_bundle_revision=blocked.revision,
                validated_snapshot=stored_snapshot,
            )
        registry.set_plan("ansonphong", "x", AdapterPlan())
        corrected = _run(
            config,
            locks,
            instance,
            clock,
            registry,
            "ansonphong",
            "QUEUE",
            "corrected",
        )
        assert corrected.status == "archived"
    finally:
        instance.release()


def test_post_create_crash_reconcile_archive_recovery_and_restart_idempotence(
    tmp_path: Path,
) -> None:
    clock = FakeClock(NOW)
    config, settings, locks, instance = _initialize(tmp_path, clock)
    source = _bundle(settings, "ansonphong", "QUEUE", "crash-window")
    registry = FakeAdapterRegistry()

    def crash(boundary: str) -> None:
        if boundary == "after_remote_commit:x":
            raise InjectedCrash

    try:
        with pytest.raises(InjectedCrash):
            _run(
                config,
                locks,
                instance,
                clock,
                registry,
                "ansonphong",
                "QUEUE",
                "crash-create",
                fault=crash,
            )
        commits = sum(trace.commits for trace in registry.traces)
        assert commits == 2 and source.is_dir()
        assert any((tmp_path / "state/staging/private").rglob("*.jpg"))

        restarted = OneRunApplication(
            config,
            locks=locks,
            instance_lease=instance,
            environ={},
            clock=clock.now,
            adapter_factory=lambda *_args: pytest.fail(
                "stale final dispatch attempted to publish again"
            ),
        ).run_once(RunOnceRequest("ansonphong", "QUEUE", "crash-restart"))
        assert (restarted.status, restarted.code) == (
            "blocked",
            "ambiguous_delivery",
        )
        bundle_key = cast(int, restarted.bundle_key)
        database = settings.app.state_directory / "post_pulsar.sqlite3"
        with StateRepository.open_existing(database, clock=clock.now) as repository:
            deliveries = {
                item.platform: item
                for item in repository.list_bundle_deliveries(bundle_key)
            }
            assert deliveries["instagram"].status == "published"
            assert deliveries["x"].status == "ambiguous"
            blocked = repository.get_bundle(bundle_key)
            repository.operator_reconcile(
                bundle_key,
                "x",
                published_remote_id="fake-confirmed-x-post",
                expected_bundle_revision=blocked.revision,
            )

        fired = False

        def archive_crash(boundary: str) -> None:
            nonlocal fired
            if boundary == "after_member_move:crash-window.txt" and not fired:
                fired = True
                raise InjectedCrash

        with (
            locks.acquire_profiles(instance, ("ansonphong",)) as profile_lease,
            StateRepository.open_existing(database, clock=clock.now) as repository,
            pytest.raises(InjectedCrash),
        ):
            ArchiveManager(
                repository,
                locks,
                fault_injector=cast(ArchiveFaultInjector, archive_crash),
            ).archive_bundle(
                bundle_key,
                instance_lease=instance,
                profile_lease=profile_lease,
            )
        with (
            locks.acquire_profiles(instance, ("ansonphong",)) as profile_lease,
            StateRepository.open_existing(database, clock=clock.now) as repository,
        ):
            archived = ArchiveManager(repository, locks).archive_bundle(
                bundle_key,
                instance_lease=instance,
                profile_lease=profile_lease,
            )
            assert archived.status == "archived"
        final = (
            settings.profile("ansonphong").account_root / "POSTED/QUEUE/crash-window"
        )
        assert not source.exists()
        assert {path.name for path in final.iterdir()} == {
            ".ready",
            "crash-window.jpg",
            "crash-window.txt",
        }
        replay = OneRunApplication(
            config,
            locks=locks,
            instance_lease=instance,
            environ={},
            clock=clock.now,
            adapter_factory=lambda *_args: pytest.fail("archived replay republished"),
        ).run_once(RunOnceRequest("ansonphong", "QUEUE", "crash-create"))
        assert replay.status == "archived" and replay.bundle_key == bundle_key
    finally:
        instance.release()


def test_ambiguous_outcome_not_published_changes_one_target_then_requires_retry(
    tmp_path: Path,
) -> None:
    clock = FakeClock(NOW)
    config, settings, locks, instance = _initialize(tmp_path, clock)
    source = _bundle(settings, "ansonphong", "QUEUE", "operator-evidence")
    registry = FakeAdapterRegistry()
    registry.set_plan(
        "ansonphong",
        "x",
        AdapterPlan(
            result=PublishResult.ambiguous(
                "fake_final_unknown", "Fake final result is unknown."
            )
        ),
    )
    try:
        outcome = _run(
            config,
            locks,
            instance,
            clock,
            registry,
            "ansonphong",
            "QUEUE",
            "ambiguous-result",
        )
        assert outcome.status == "blocked"
        key = cast(int, outcome.bundle_key)
        database = settings.app.state_directory / "post_pulsar.sqlite3"
        with StateRepository.open_existing(database, clock=clock.now) as repository:
            instagram_before = repository.get_delivery(key, "instagram")
            blocked = repository.get_bundle(key)
            reconciled = repository.operator_reconcile(
                key,
                "x",
                published_remote_id=None,
                expected_bundle_revision=blocked.revision,
            )
            assert reconciled.error_code == "reconciled_not_published"
            assert not reconciled.safe_to_retry and reconciled.next_attempt_at is None
            assert repository.get_delivery(key, "instagram") == instagram_before
            with pytest.raises(TransitionError):
                repository.claim_delivery(key, "x", "unguarded")
        blocked_run = _run(
            config,
            locks,
            instance,
            clock,
            registry,
            "ansonphong",
            "QUEUE",
            "still-blocked",
        )
        assert blocked_run.code == "operator_action_required"
        with StateRepository.open_existing(database, clock=clock.now) as repository:
            blocked = repository.get_bundle(key)
            repository.operator_retry(
                key,
                "x",
                expected_bundle_revision=blocked.revision,
                validated_snapshot=cli._configured_target_snapshot(
                    settings.profile("ansonphong"), "x"
                ),
            )
        registry.set_plan("ansonphong", "x", AdapterPlan())
        resumed = _run(
            config,
            locks,
            instance,
            clock,
            registry,
            "ansonphong",
            "QUEUE",
            "guarded-retry",
        )
        assert resumed.status == "archived" and not source.exists()
    finally:
        instance.release()


def test_scheduler_dedup_restart_dst_misfire_and_fake_wait(tmp_path: Path) -> None:
    clock = FakeClock(datetime(2026, 9, 5, 12, tzinfo=UTC))
    _config, settings, _locks, instance = _initialize(tmp_path, clock)
    instance.release()
    database = settings.app.state_directory / "post_pulsar.sqlite3"
    with StateRepository.open_existing(database, clock=clock.now) as repository:
        due = repository.create_schedule(
            profile_id="ansonphong",
            schedule_id="due",
            bucket="QUEUE",
            timezone="UTC",
            weekdays=(5,),
            local_time="12:00",
            misfire_grace_seconds=60,
            enabled=True,
        )
        future = repository.create_schedule(
            profile_id="360hextile",
            schedule_id="future",
            bucket="RANDOM",
            timezone="UTC",
            weekdays=(5,),
            local_time="12:01",
            misfire_grace_seconds=60,
            enabled=True,
        )
        missed = repository.create_schedule(
            profile_id="360hextile",
            schedule_id="missed",
            bucket="REELS",
            timezone="UTC",
            weekdays=(5,),
            local_time="11:00",
            misfire_grace_seconds=10,
            enabled=True,
        )
        scheduler = DeterministicScheduler(
            repository,
            wall_clock=clock.now,
            monotonic_clock=clock.monotonic_now,
            sleeper=clock.sleep,
        )
        first = scheduler.tick()
        second = scheduler.tick()
        assert [item.schedule_id for item in first] == ["due"]
        assert [(item.run_id, item.schedule_id) for item in second] == [
            (item.run_id, item.schedule_id) for item in first
        ]
        assert repository.latest_schedule_local_date(due.schedule_key) is not None
        assert repository.latest_schedule_local_date(missed.schedule_key) is not None
        after_wait = scheduler.wake_after(60)
        assert clock.sleeps == [60]
        assert {item.schedule_id for item in after_wait} == {"due", "future"}
        durable_ids = {item.schedule_id: item.run_id for item in after_wait}

    with StateRepository.open_existing(database, clock=clock.now) as reopened:
        restarted = DeterministicScheduler(
            reopened,
            wall_clock=clock.now,
            monotonic_clock=clock.monotonic_now,
            sleeper=clock.sleep,
        ).tick()
        assert {item.schedule_id: item.run_id for item in restarted} == durable_ids
        assert all(not item.retry for item in restarted)
        fold = reopened.create_schedule(
            profile_id="ansonphong",
            schedule_id="fold",
            bucket="QUEUE",
            timezone="America/Vancouver",
            weekdays=(6,),
            local_time="01:30",
            misfire_grace_seconds=3600,
            enabled=False,
        )
        folded = evaluate_schedule(fold, datetime(2026, 11, 1, 9, 15, tzinfo=UTC))
        assert folded is not None
        assert folded.occurrence.scheduled_at == datetime(
            2026, 11, 1, 8, 30, tzinfo=UTC
        )
        gap = replace(
            fold,
            schedule_id="gap",
            local_time="02:30",
            misfire_grace_seconds=1799,
        )
        skipped = evaluate_schedule(gap, datetime(2026, 3, 8, 10, tzinfo=UTC))
        assert skipped is not None and skipped.state == "missed"
        assert skipped.occurrence.gap_delay_seconds == 1800
        assert future.profile_id != due.profile_id


def test_real_loopback_daemon_control_confirmation_and_socket_sentinel(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    socket_enabled: None,
) -> None:
    del socket_enabled
    clock = FakeClock(datetime(2030, 1, 7, 12, tzinfo=UTC))
    config, settings, _locks, instance = _initialize(
        tmp_path,
        clock,
        enabled={"ansonphong": ("x",), "360hextile": ()},
        allow_agent_publish=False,
    )
    source = _bundle(settings, "ansonphong", "QUEUE", "confirmed")
    key, fingerprint = _admit_existing(
        settings, clock, "ansonphong", "confirmed", platforms=("x",)
    )
    instance.release()
    capability = rotate_agent_capability(settings.app.agent_capability_file)
    operator_phrase = "fake operator phrase"
    initialize_operator_secret(
        settings.app.operator_verifier_file,
        input_stream=_TTY(operator_phrase + "\n"),
    )
    guard = LoopbackSocketGuard()
    guard.install(monkeypatch)
    registry = FakeAdapterRegistry()
    running = launch_fake_daemon(
        config_path=config,
        registry=registry,
        environ=FAKE_ENV,
        clock=clock,
    )
    try:
        endpoint = EndpointRecord.read(settings.app.endpoint_record_file)
        assert endpoint.address.startswith("127.0.0.1:")
        assert running.capability == capability
        unauthenticated, _ = running.request(
            "GET", "/control/v1/health", authenticated=False
        )
        assert unauthenticated == 401
        status, bounded = running.request(
            "POST", "/control/v1/pause", body={"oversized": "x" * 70000}
        )
        assert status == 413 and bounded["ok"] is False
        status, forbidden = running.request(
            "POST",
            "/control/v1/pause",
            body={"profile_id": "ansonphong", "command": "publish anything"},
            headers={"Idempotency-Key": "forbidden", "If-Match": "1"},
        )
        assert status == 422 and forbidden["ok"] is False
        assert running.request("POST", "/control/v1/arbitrary", body={})[0] == 422

        with pytest.raises(LockContentionError):
            ForegroundDaemon(
                settings.app.state_directory,
                tmp_path / "other-endpoint.json",
                "127.0.0.1",
                0,
            ).start()

        arguments = {
            "bucket": "QUEUE",
            "bundle_id": "confirmed",
            "fingerprint": fingerprint,
            "trigger_id": "confirmed-agent-request",
        }
        disabled_status, _ = running.request(
            "POST",
            "/control/v1/confirmations",
            body={
                "action": "run_now",
                "profile_id": "ansonphong",
                "resource_revision": 1,
                "bundle_key": key,
                "fingerprint": fingerprint,
                "arguments": arguments,
                "consequence": "Publish this exact fake bundle.",
            },
            headers={"Idempotency-Key": "agent-disabled", "If-Match": "1"},
        )
        assert disabled_status == 403 and running.executed == []

        database = settings.app.state_directory / "post_pulsar.sqlite3"
        with StateRepository.open_existing(database, clock=clock.now) as repository:
            bundle = repository.get_bundle(key)
            intent = repository.create_confirmation_intent(
                action="run_now",
                arguments=arguments,
                profile_id="ansonphong",
                resource_revision=bundle.revision,
                fingerprint=fingerprint,
                consequence="Publish this exact fake bundle.",
                expires_at=clock.now() + timedelta(minutes=5),
                bundle_key=key,
                idempotency_key="trusted-cli-intent",
                origin="operator",
            )
        consume_body = {
            "action": "run_now",
            "profile_id": "ansonphong",
            "resource_revision": intent.resource_revision,
            "bundle_key": key,
            "fingerprint": fingerprint,
            "arguments": arguments,
        }
        unconfirmed, _ = running.request(
            "POST",
            f"/control/v1/confirmations/{intent.intent_id}/consume",
            body=consume_body,
            headers={
                "Idempotency-Key": "consume-confirmed",
                "If-Match": str(intent.resource_revision),
            },
        )
        assert unconfirmed == 409 and running.executed == []

        output, error = io.StringIO(), io.StringIO()
        exit_code = cli.main(
            [
                "confirmations",
                "approve",
                "--config",
                str(config),
                "--profile",
                "ansonphong",
                "--intent-id",
                intent.intent_id,
                "--expected-revision",
                str(intent.revision),
                "--idempotency-key",
                "trusted-cli-approval",
                "--json",
            ],
            stdin=_TTY(operator_phrase + "\n"),
            stdout=output,
            stderr=error,
            clock=clock.now,
        )
        assert exit_code == 0, error.getvalue()
        approval = json.loads(output.getvalue())["result"]["confirmation"]
        assert approval["state"] == "approved"
        accepted, first_response = running.request(
            "POST",
            f"/control/v1/confirmations/{intent.intent_id}/consume",
            body=consume_body,
            headers={
                "Idempotency-Key": "consume-confirmed",
                "If-Match": str(intent.resource_revision),
            },
        )
        replayed, second_response = running.request(
            "POST",
            f"/control/v1/confirmations/{intent.intent_id}/consume",
            body=consume_body,
            headers={
                "Idempotency-Key": "consume-confirmed",
                "If-Match": str(intent.resource_revision),
            },
        )
        assert accepted == replayed == 202
        assert first_response["data"] == second_response["data"]
        assert running.completed.wait(2)
        response_data = cast(dict[str, object], first_response["data"])
        assert running.executed == [cast(int, response_data["request_id"])]
        assert not source.exists() and len(registry.traces) == 1

        with pytest.raises(OSError, match="non-loopback"):
            socket.create_connection(("203.0.113.1", 443), timeout=0.01)
        assert guard.denied == [("connect", ("203.0.113.1", 443))]

        stopped, _ = running.request(
            "POST",
            "/control/v1/shutdown",
            body={"startup_nonce": endpoint.startup_nonce},
        )
        assert stopped == 202
    finally:
        running.stop()
    assert not settings.app.endpoint_record_file.exists()
