"""Safe one-run orchestration tests with real state, locks, media, and archive."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import cast

import httpx
import pytest
from PIL import Image

from post_pulsar.app import (
    OneRunApplication,
    OrchestrationFaultInjector,
    RunOnceRequest,
)
from post_pulsar.config import SecretValue
from post_pulsar.content import scan_account_root
from post_pulsar.locking import LockContentionError, LockLease, LockManager
from post_pulsar.platforms.base import (
    ArtifactCheckpoint,
    CheckpointWriter,
    PlatformAdapter,
    PreparedPublication,
    PublicationRequest,
    PublicationSnapshot,
    PublishResult,
    ValidationIssue,
)
from post_pulsar.platforms.http import HTTPPolicy, PlatformHTTPClient
from post_pulsar.platforms.x import XAdapter
from post_pulsar.media import MediaWarning, prepare_bundle_media
from post_pulsar.state import (
    BundleFileSnapshot,
    DeliveryRecord,
    ProfileTargetSnapshot,
    StateRepository,
    TargetSnapshot,
)

NOW = datetime(2026, 9, 5, 12, tzinfo=UTC)


class HardCrash(BaseException):
    """Model process death that ordinary orchestration cannot catch."""


class ClassifiedFailure(RuntimeError):
    def __init__(self, classification: str) -> None:
        super().__init__("classified test failure")
        self.retry_classification = classification


class FakeAdapter:
    def __init__(
        self,
        snapshot: PublicationSnapshot,
        *,
        issues: tuple[ValidationIssue, ...] = (),
        result: PublishResult | None = None,
        prepare_hook: Callable[[CheckpointWriter], None] | None = None,
    ) -> None:
        self.snapshot = snapshot
        self.issues = issues
        self.result = result or PublishResult.published(
            f"remote-{snapshot.target.platform}"
        )
        self.prepare_hook = prepare_hook
        self.preflights = 0
        self.prepares = 0
        self.commits = 0
        self.closed = False
        self.prior: PreparedPublication | None = None

    def preflight(self, publication: PublicationRequest) -> tuple[ValidationIssue, ...]:
        assert publication.snapshot == self.snapshot
        self.preflights += 1
        return self.issues

    def prepare(
        self,
        publication: PublicationRequest,
        *,
        prior: PreparedPublication | None,
        checkpoints: CheckpointWriter,
    ) -> PreparedPublication:
        self.prepares += 1
        self.prior = prior
        if self.prepare_hook is not None:
            self.prepare_hook(checkpoints)
        delivery = checkpoints.advance_phase("ready")
        return PreparedPublication(publication, delivery.attempt_count, ())

    def commit(
        self, prepared: PreparedPublication, *, delivery: DeliveryRecord
    ) -> PublishResult:
        del prepared, delivery
        self.commits += 1
        return self.result

    def close(self) -> None:
        self.closed = True


class ResumableRemoteAdapter(FakeAdapter):
    """Checkpoint one remote object, then prove the next run reuses it."""

    def __init__(
        self,
        snapshot: PublicationSnapshot,
        calls: list[PreparedPublication | None],
    ) -> None:
        super().__init__(snapshot)
        self._calls = calls

    def prepare(
        self,
        publication: PublicationRequest,
        *,
        prior: PreparedPublication | None,
        checkpoints: CheckpointWriter,
    ) -> PreparedPublication:
        self.prepares += 1
        self.prior = prior
        self._calls.append(prior)
        delivery = checkpoints.advance_phase("processing")
        if len(self._calls) == 1:
            checkpoints.checkpoint_artifact(
                ArtifactCheckpoint(
                    "x_media_id",
                    0,
                    external_id="90001",
                    expires_at=NOW.replace(hour=13),
                    processing_metadata={"state": "pending"},
                )
            )
            raise ClassifiedFailure("safe_pre_final")
        assert prior is not None
        assert any(
            artifact.kind == "x_media_id" and artifact.external_id == "90001"
            for artifact in prior.artifacts
        )
        ready = checkpoints.advance_phase("ready")
        return PreparedPublication(publication, ready.attempt_count, prior.artifacts)


def _write_config(
    root: Path,
    *,
    x_enabled: bool = True,
    instagram_enabled: bool = False,
) -> Path:
    config = root / "post-pulsar.toml"
    config.write_text(
        f"""
[app]
state_directory = "state"
log_file = "state/post_pulsar.log"
agent_capability_file = "state/control/agent-capability"
operator_verifier_file = "state/control/operator-verifier"
bootstrap_file = "state/control/bootstrap.json"
endpoint_record_file = "state/control/endpoint.json"

[[profiles]]
profile_id = "operator"
account_root = "accounts/operator"
timezone = "UTC"

[profiles.x]
enabled = {str(x_enabled).lower()}
expected_remote_user_id = "10001"
expected_username = "operator"
token_env_var = "POST_PULSAR_X_OPERATOR_USER_ACCESS_TOKEN"
request_timeout_seconds = 30
processing_timeout_seconds = 300
chunk_size_bytes = 4194304

[profiles.instagram]
enabled = {str(instagram_enabled).lower()}
expected_remote_user_id = "20001"
expected_username = "operator"
token_env_var = "POST_PULSAR_INSTAGRAM_OPERATOR_ACCESS_TOKEN"
media_directory = "public"
media_base_url = "https://media.example.test/post-pulsar/"
request_timeout_seconds = 30
processing_timeout_seconds = 300
""".strip()
        + "\n",
        encoding="utf-8",
    )
    return config


def _bundle(root: Path, bundle_id: str, bucket: str = "QUEUE") -> Path:
    directory = root / "accounts/operator" / bucket / bundle_id
    directory.mkdir(parents=True)
    Image.new("RGB", (2, 2), "red").save(directory / f"{bundle_id}.jpg")
    (directory / f"{bundle_id}.txt").write_text(
        f"caption {bundle_id}\n", encoding="utf-8"
    )
    (directory / ".ready").write_bytes(b"")
    return directory


def _profile_targets() -> tuple[ProfileTargetSnapshot, ...]:
    return (
        ProfileTargetSnapshot(
            "x",
            "10001",
            "operator",
            "POST_PULSAR_X_OPERATOR_USER_ACCESS_TOKEN",
            {
                "request_timeout_seconds": 30.0,
                "processing_timeout_seconds": 300.0,
                "chunk_size_bytes": 4194304,
            },
        ),
        ProfileTargetSnapshot(
            "instagram",
            "20001",
            "operator",
            "POST_PULSAR_INSTAGRAM_OPERATOR_ACCESS_TOKEN",
            {
                "media_directory": "",
                "media_base_url": "https://media.example.test/post-pulsar/",
                "request_timeout_seconds": 30.0,
                "processing_timeout_seconds": 300.0,
            },
        ),
    )


def _target(platform: str, root: Path) -> TargetSnapshot:
    if platform == "x":
        return TargetSnapshot(
            "x",
            "10001",
            "operator",
            "POST_PULSAR_X_OPERATOR_USER_ACCESS_TOKEN",
            "2",
            1,
            {
                "request_timeout_seconds": 30.0,
                "processing_timeout_seconds": 300.0,
                "chunk_size_bytes": 4194304,
            },
        )
    return TargetSnapshot(
        "instagram",
        "20001",
        "operator",
        "POST_PULSAR_INSTAGRAM_OPERATOR_ACCESS_TOKEN",
        "v26.0",
        1,
        {
            "media_directory": str((root / "public").resolve()),
            "media_base_url": "https://media.example.test/post-pulsar/",
            "request_timeout_seconds": 30.0,
            "processing_timeout_seconds": 300.0,
        },
    )


def _initialize(root: Path) -> tuple[Path, LockManager, LockLease]:
    config = _write_config(root)
    account = (root / "accounts/operator").resolve()
    account.mkdir(parents=True, exist_ok=True)
    state = root / "state"
    with StateRepository(state / "post_pulsar.sqlite3", clock=lambda: NOW) as repo:
        repo.register_profile(
            "operator",
            account,
            (
                _profile_targets()[0],
                ProfileTargetSnapshot(
                    "instagram",
                    "20001",
                    "operator",
                    "POST_PULSAR_INSTAGRAM_OPERATOR_ACCESS_TOKEN",
                    {
                        "media_directory": str((root / "public").resolve()),
                        "media_base_url": "https://media.example.test/post-pulsar/",
                        "request_timeout_seconds": 30.0,
                        "processing_timeout_seconds": 300.0,
                    },
                ),
            ),
            config_hash="1" * 64,
        )
    locks = LockManager(state)
    instance = locks.acquire_instance()
    return config, locks, instance


def _factory(
    adapters: list[FakeAdapter],
    *,
    issues: dict[str, tuple[ValidationIssue, ...]] | None = None,
    results: dict[str, PublishResult] | None = None,
    prepare_hook: Callable[[CheckpointWriter], None] | None = None,
) -> Callable[[PublicationSnapshot, SecretValue, Path], PlatformAdapter]:
    def build(
        snapshot: PublicationSnapshot, token: SecretValue, private: Path
    ) -> PlatformAdapter:
        assert token.reveal() == f"token-{snapshot.target.platform}"
        assert private.name == "private"
        adapter = FakeAdapter(
            snapshot,
            issues=() if issues is None else issues.get(snapshot.target.platform, ()),
            result=None if results is None else results.get(snapshot.target.platform),
            prepare_hook=prepare_hook,
        )
        adapters.append(adapter)
        return cast(PlatformAdapter, adapter)

    return build


def test_run_once_preflights_every_target_before_any_prepare_or_commit(
    tmp_path: Path,
) -> None:
    config, locks, instance = _initialize(tmp_path)
    _write_config(tmp_path, instagram_enabled=True)
    _bundle(tmp_path, "post")
    adapters: list[FakeAdapter] = []
    issue = ValidationIssue(
        "instagram", "error", "ig_caption_invalid", "Caption rejected", "caption"
    )
    app = OneRunApplication(
        config,
        locks=locks,
        instance_lease=instance,
        environ={
            "POST_PULSAR_X_OPERATOR_USER_ACCESS_TOKEN": "token-x",
            "POST_PULSAR_INSTAGRAM_OPERATOR_ACCESS_TOKEN": "token-instagram",
        },
        clock=lambda: NOW,
        adapter_factory=_factory(adapters, issues={"instagram": (issue,)}),
    )

    outcome = app.run_once(RunOnceRequest("operator", "QUEUE", "manual-1"))

    assert outcome.status == "blocked"
    assert len(adapters) == 2
    assert all(adapter.preflights == 1 for adapter in adapters)
    assert all(adapter.prepares == 0 and adapter.commits == 0 for adapter in adapters)
    assert all(adapter.closed for adapter in adapters)
    assert not any((tmp_path / "public").rglob("*.jpg"))
    with StateRepository.open_existing(tmp_path / "state/post_pulsar.sqlite3") as repo:
        attempts = {
            item.platform: item.attempt_count
            for item in repo.list_bundle_deliveries(cast(int, outcome.bundle_key))
        }
        assert attempts == {"instagram": 1, "x": 0}
    assert instance.active  # caller-owned lease was borrowed, not released
    instance.release()


def test_peer_preflight_failure_restores_resumed_remote_attempt(
    tmp_path: Path,
) -> None:
    config, locks, instance = _initialize(tmp_path)
    _write_config(tmp_path, x_enabled=True, instagram_enabled=True)
    _bundle(tmp_path, "post")
    admitted = scan_account_root(tmp_path / "accounts/operator").bundles[0]
    with StateRepository.open_existing(
        tmp_path / "state/post_pulsar.sqlite3", clock=lambda: NOW
    ) as repo:
        bundle_key = repo.add_bundle(
            profile_id="operator",
            bundle_id="post",
            fingerprint=admitted.fingerprint,
            source_bucket="QUEUE",
            files=tuple(
                BundleFileSnapshot(
                    item.relative_name,
                    item.role,
                    item.ordinal,
                    item.media_kind,
                    item.mime_type,
                    item.size_bytes,
                    item.sha256,
                )
                for item in admitted.content.member_snapshots
            ),
            targets=(_target("x", tmp_path), _target("instagram", tmp_path)),
        )
        claim = repo.claim_delivery(bundle_key, "x", "seed-x")
        repo.advance_delivery_phase(
            bundle_key,
            "x",
            "processing",
            claim_token="seed-x",
            attempt_count=claim.attempt_count,
        )
        artifact = repo.checkpoint_artifact(
            bundle_key,
            "x",
            kind="x_media_id",
            ordinal=0,
            external_id="91001",
            expires_at=NOW + timedelta(hours=1),
            processing_metadata={"state": "pending"},
            claim_token="seed-x",
            attempt_count=claim.attempt_count,
        )
        previous = repo.defer_resumable_delivery(
            bundle_key,
            "x",
            error_code="processing_timeout",
            error_message="Remote processing is not finished.",
            retry_at=NOW,
            claim_token="seed-x",
            attempt_count=claim.attempt_count,
        )

    issue = ValidationIssue(
        "instagram", "error", "ig_caption_invalid", "Caption rejected", "caption"
    )
    outcome = OneRunApplication(
        config,
        locks=locks,
        instance_lease=instance,
        environ={
            "POST_PULSAR_X_OPERATOR_USER_ACCESS_TOKEN": "token-x",
            "POST_PULSAR_INSTAGRAM_OPERATOR_ACCESS_TOKEN": "token-instagram",
        },
        clock=lambda: NOW,
        adapter_factory=_factory([], issues={"instagram": (issue,)}),
    ).run_once(RunOnceRequest("operator", "QUEUE", "peer-failure"))

    assert outcome.status == "blocked"
    with StateRepository.open_existing(tmp_path / "state/post_pulsar.sqlite3") as repo:
        restored = repo.get_delivery(bundle_key, "x")
        assert replace(restored, revision=previous.revision) == previous
        assert artifact in repo.list_delivery_artifacts(bundle_key, "x")
    instance.release()


def test_success_archives_exact_bundle_and_releases_owned_resources(
    tmp_path: Path,
) -> None:
    config, locks, instance = _initialize(tmp_path)
    source = _bundle(tmp_path, "Zulu")
    _bundle(tmp_path, "alpha")
    adapters: list[FakeAdapter] = []
    app = OneRunApplication(
        config,
        locks=locks,
        instance_lease=instance,
        environ={"POST_PULSAR_X_OPERATOR_USER_ACCESS_TOKEN": "token-x"},
        clock=lambda: NOW,
        adapter_factory=_factory(adapters),
    )

    outcome = app.run_once(RunOnceRequest("operator", "QUEUE", "manual-2"))

    assert outcome.status == "archived"
    assert outcome.bundle_id == "alpha"
    assert not (tmp_path / "accounts/operator/QUEUE/alpha").exists()
    assert (tmp_path / "accounts/operator/POSTED/QUEUE/alpha").is_dir()
    assert source.is_dir()
    assert adapters[0].commits == 1 and adapters[0].closed
    competing = LockManager(tmp_path / "state")
    with pytest.raises(LockContentionError):
        competing.acquire_instance()
    instance.release()
    with competing.acquire_instance() as competing_instance:
        assert competing_instance.active


def test_real_x_adapter_accepts_orchestrator_staging_checkpoint(
    tmp_path: Path,
) -> None:
    config, locks, instance = _initialize(tmp_path)
    _bundle(tmp_path, "post")
    calls: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(request.url.path)
        if request.url.path == "/2/users/me":
            return httpx.Response(
                200, json={"data": {"id": "10001", "username": "operator"}}
            )
        if request.url.path == "/2/media/upload":
            return httpx.Response(
                201, json={"data": {"id": "8001", "expires_after_secs": 3600}}
            )
        if request.url.path == "/2/tweets":
            return httpx.Response(201, json={"data": {"id": "9001"}})
        raise AssertionError(request.url)

    def factory(
        snapshot: PublicationSnapshot, token: SecretValue, private: Path
    ) -> PlatformAdapter:
        client = PlatformHTTPClient(
            snapshot,
            token,
            base_url="https://api.x.com",
            policy=HTTPPolicy(max_pre_final_attempts=1),
            clock=lambda: NOW,
            transport=httpx.MockTransport(handler),
        )
        return XAdapter(
            snapshot,
            client,
            private_staging_directory=private,
            clock=lambda: NOW,
        )

    outcome = OneRunApplication(
        config,
        locks=locks,
        instance_lease=instance,
        environ={"POST_PULSAR_X_OPERATOR_USER_ACCESS_TOKEN": "token-x"},
        clock=lambda: NOW,
        adapter_factory=factory,
    ).run_once(RunOnceRequest("operator", "QUEUE", "real-x-adapter"))

    assert outcome.status == "archived"
    assert calls.count("/2/users/me") >= 1
    assert [path for path in calls if path != "/2/users/me"] == [
        "/2/media/upload",
        "/2/tweets",
    ]
    instance.release()


@pytest.mark.parametrize(
    ("boundary", "expected_commits"),
    [("after_final_dispatch_started:x", 0), ("after_remote_commit:x", 1)],
)
def test_final_dispatch_boundary_crash_recovers_ambiguous_without_repeat(
    tmp_path: Path, boundary: str, expected_commits: int
) -> None:
    config, locks, instance = _initialize(tmp_path)
    _bundle(tmp_path, "post")
    adapters: list[FakeAdapter] = []

    def crash(boundary: str) -> None:
        if boundary == expected_boundary:
            raise HardCrash

    expected_boundary = boundary

    first = OneRunApplication(
        config,
        locks=locks,
        instance_lease=instance,
        environ={"POST_PULSAR_X_OPERATOR_USER_ACCESS_TOKEN": "token-x"},
        clock=lambda: NOW,
        adapter_factory=_factory(adapters),
        fault_injector=crash,
    )
    with pytest.raises(HardCrash):
        first.run_once(RunOnceRequest("operator", "QUEUE", "crash-1"))
    assert adapters[0].commits == expected_commits and adapters[0].closed
    assert any((tmp_path / "state/staging/private").rglob("*.jpg"))

    second_adapters: list[FakeAdapter] = []
    second = OneRunApplication(
        config,
        locks=locks,
        instance_lease=instance,
        environ={},
        clock=lambda: NOW,
        adapter_factory=_factory(second_adapters),
    )
    outcome = second.run_once(RunOnceRequest("operator", "QUEUE", "crash-2"))

    assert outcome.status == "blocked"
    assert outcome.code == "ambiguous_delivery"
    assert second_adapters == []
    with StateRepository.open_existing(tmp_path / "state/post_pulsar.sqlite3") as repo:
        delivery = repo.list_bundle_deliveries(cast(int, outcome.bundle_key))[0]
        assert delivery.status == "ambiguous"
    assert any((tmp_path / "state/staging/private").rglob("*.jpg"))
    instance.release()


def test_all_published_recovery_archives_with_disabled_targets_and_no_tokens(
    tmp_path: Path,
) -> None:
    config, locks, instance = _initialize(tmp_path)
    _write_config(tmp_path, x_enabled=False)
    directory = _bundle(tmp_path, "post")
    admitted = scan_account_root(tmp_path / "accounts/operator").bundles[0]
    with StateRepository.open_existing(tmp_path / "state/post_pulsar.sqlite3") as repo:
        bundle_key = repo.add_bundle(
            profile_id="operator",
            bundle_id="post",
            fingerprint=admitted.fingerprint,
            source_bucket="QUEUE",
            files=tuple(
                BundleFileSnapshot(
                    item.relative_name,
                    item.role,
                    item.ordinal,
                    item.media_kind,
                    item.mime_type,
                    item.size_bytes,
                    item.sha256,
                )
                for item in admitted.content.member_snapshots
            ),
            targets=(_target("x", tmp_path),),
        )
        claim = repo.claim_delivery(bundle_key, "x", "seed")
        repo.advance_delivery_phase(
            bundle_key,
            "x",
            "final_dispatch_started",
            claim_token="seed",
            attempt_count=claim.attempt_count,
        )
        repo.publish_delivery(
            bundle_key,
            "x",
            remote_id="published-before-crash",
            claim_token="seed",
            attempt_count=claim.attempt_count,
        )
    app = OneRunApplication(
        config,
        locks=locks,
        instance_lease=instance,
        environ={},
        clock=lambda: NOW,
        adapter_factory=lambda *_args: pytest.fail("archive recovery built an adapter"),
    )

    outcome = app.run_once(RunOnceRequest("operator", "QUEUE", "archive-1"))

    assert outcome.status == "archived"
    assert not directory.exists()
    assert (tmp_path / "accounts/operator/POSTED/QUEUE/post").is_dir()
    instance.release()


def test_stale_pre_final_reuses_attempt_and_durable_artifact(tmp_path: Path) -> None:
    config, locks, instance = _initialize(tmp_path)
    _bundle(tmp_path, "post")
    first_adapters: list[FakeAdapter] = []

    def checkpoint_then_crash(writer: CheckpointWriter) -> None:
        writer.checkpoint_artifact(
            ArtifactCheckpoint(
                "x_media_id",
                0,
                external_id="media-resume",
                expires_at=datetime(2026, 9, 5, 13, tzinfo=UTC),
            )
        )
        raise HardCrash

    first = OneRunApplication(
        config,
        locks=locks,
        instance_lease=instance,
        environ={"POST_PULSAR_X_OPERATOR_USER_ACCESS_TOKEN": "token-x"},
        clock=lambda: NOW,
        adapter_factory=_factory(first_adapters, prepare_hook=checkpoint_then_crash),
    )
    with pytest.raises(HardCrash):
        first.run_once(RunOnceRequest("operator", "QUEUE", "resume-1"))

    second_adapters: list[FakeAdapter] = []
    second = OneRunApplication(
        config,
        locks=locks,
        instance_lease=instance,
        environ={"POST_PULSAR_X_OPERATOR_USER_ACCESS_TOKEN": "token-x"},
        clock=lambda: NOW,
        adapter_factory=_factory(second_adapters),
    )
    outcome = second.run_once(RunOnceRequest("operator", "QUEUE", "resume-2"))

    assert outcome.status == "archived"
    assert second_adapters[0].prior is not None
    assert any(
        item.external_id == "media-resume"
        for item in second_adapters[0].prior.artifacts
    )
    with StateRepository.open_existing(tmp_path / "state/post_pulsar.sqlite3") as repo:
        delivery = repo.list_bundle_deliveries(cast(int, outcome.bundle_key))[0]
        assert delivery.attempt_count == 1
    instance.release()


def test_zero_targets_rejects_only_new_admission_without_consuming_bundle(
    tmp_path: Path,
) -> None:
    config, locks, instance = _initialize(tmp_path)
    _write_config(tmp_path, x_enabled=False)
    _bundle(tmp_path, "post")
    app = OneRunApplication(
        config,
        locks=locks,
        instance_lease=instance,
        environ={},
        clock=lambda: NOW,
        adapter_factory=lambda *_args: pytest.fail("zero-target run built an adapter"),
    )

    outcome = app.run_once(RunOnceRequest("operator", "QUEUE", "zero-1"))

    assert outcome.status == "invalid"
    assert outcome.code == "no_enabled_targets"
    with StateRepository.open_existing(tmp_path / "state/post_pulsar.sqlite3") as repo:
        assert repo.list_active_bundles("operator") == ()
        assert repo.selection_counter("operator") == 0
    instance.release()


def test_partial_success_resumes_only_unpublished_target_when_other_is_disabled(
    tmp_path: Path,
) -> None:
    config, locks, instance = _initialize(tmp_path)
    _write_config(tmp_path, x_enabled=True, instagram_enabled=False)
    _bundle(tmp_path, "post")
    admitted = scan_account_root(tmp_path / "accounts/operator").bundles[0]
    files = tuple(
        BundleFileSnapshot(
            item.relative_name,
            item.role,
            item.ordinal,
            item.media_kind,
            item.mime_type,
            item.size_bytes,
            item.sha256,
        )
        for item in admitted.content.member_snapshots
    )
    with StateRepository.open_existing(tmp_path / "state/post_pulsar.sqlite3") as repo:
        bundle_key = repo.add_bundle(
            profile_id="operator",
            bundle_id="post",
            fingerprint=admitted.fingerprint,
            source_bucket="QUEUE",
            files=files,
            targets=(_target("x", tmp_path), _target("instagram", tmp_path)),
        )
        claim = repo.claim_delivery(bundle_key, "instagram", "seed-instagram")
        repo.advance_delivery_phase(
            bundle_key,
            "instagram",
            "final_dispatch_started",
            claim_token="seed-instagram",
            attempt_count=claim.attempt_count,
        )
        repo.publish_delivery(
            bundle_key,
            "instagram",
            remote_id="ig-already-published",
            claim_token="seed-instagram",
            attempt_count=claim.attempt_count,
        )
    adapters: list[FakeAdapter] = []
    seen_targets: list[tuple[str, ...]] = []

    from post_pulsar.media import prepare_bundle_media

    def prepare(*args: object, **kwargs: object) -> object:
        seen_targets.append(cast(tuple[str, ...], kwargs["targets"]))
        return prepare_bundle_media(*args, **kwargs)  # type: ignore[arg-type]

    app = OneRunApplication(
        config,
        locks=locks,
        instance_lease=instance,
        environ={"POST_PULSAR_X_OPERATOR_USER_ACCESS_TOKEN": "token-x"},
        clock=lambda: NOW,
        adapter_factory=_factory(adapters),
        media_preparer=prepare,  # type: ignore[arg-type]
    )

    outcome = app.run_once(RunOnceRequest("operator", "QUEUE", "partial-1"))

    assert outcome.status == "archived"
    assert seen_targets == [("x",)]
    assert [item.snapshot.target.platform for item in adapters] == ["x"]
    instance.release()


def test_warning_only_preflight_is_deduplicated_and_does_not_block(
    tmp_path: Path,
) -> None:
    config, locks, instance = _initialize(tmp_path)
    _bundle(tmp_path, "post")
    warning = ValidationIssue(
        "x",
        "warning",
        "media_cleanup_deferred",
        "Cleanup will be retried safely.",
        "media",
    )
    adapters: list[FakeAdapter] = []
    app = OneRunApplication(
        config,
        locks=locks,
        instance_lease=instance,
        environ={"POST_PULSAR_X_OPERATOR_USER_ACCESS_TOKEN": "token-x"},
        clock=lambda: NOW,
        adapter_factory=_factory(adapters, issues={"x": (warning, warning)}),
    )

    outcome = app.run_once(RunOnceRequest("operator", "QUEUE", "warning-1"))

    assert outcome.status == "archived"
    with StateRepository.open_existing(tmp_path / "state/post_pulsar.sqlite3") as repo:
        events = repo.list_events(cast(int, outcome.bundle_key))
        assert [event["event_code"] for event in events].count(
            "media_cleanup_deferred"
        ) == 1
    instance.release()


def test_definite_final_rejection_never_becomes_ambiguous(tmp_path: Path) -> None:
    config, locks, instance = _initialize(tmp_path)
    _bundle(tmp_path, "post")
    adapters: list[FakeAdapter] = []
    app = OneRunApplication(
        config,
        locks=locks,
        instance_lease=instance,
        environ={"POST_PULSAR_X_OPERATOR_USER_ACCESS_TOKEN": "token-x"},
        clock=lambda: NOW,
        adapter_factory=_factory(
            adapters,
            results={
                "x": PublishResult.failed(
                    "permission_denied",
                    "The platform rejected publication.",
                    retry_classification="permanent",
                )
            },
        ),
    )

    outcome = app.run_once(RunOnceRequest("operator", "QUEUE", "reject-1"))

    assert outcome.status == "blocked"
    with StateRepository.open_existing(tmp_path / "state/post_pulsar.sqlite3") as repo:
        delivery = repo.list_bundle_deliveries(cast(int, outcome.bundle_key))[0]
        assert delivery.status == "failed"
        assert not delivery.safe_to_retry
    assert adapters[0].commits == 1
    instance.release()


def test_ready_marker_drift_after_admission_blocks_before_http(tmp_path: Path) -> None:
    config, locks, instance = _initialize(tmp_path)
    directory = _bundle(tmp_path, "post")
    adapters: list[FakeAdapter] = []

    def mutate_ready(boundary: str) -> None:
        if boundary == "after_bundle_admission":
            (directory / ".ready").write_bytes(b"changed")

    app = OneRunApplication(
        config,
        locks=locks,
        instance_lease=instance,
        environ={"POST_PULSAR_X_OPERATOR_USER_ACCESS_TOKEN": "token-x"},
        clock=lambda: NOW,
        adapter_factory=_factory(adapters),
        fault_injector=mutate_ready,
    )

    outcome = app.run_once(RunOnceRequest("operator", "QUEUE", "drift-1"))

    assert outcome.status == "blocked"
    assert outcome.code == "fingerprint_drift"
    assert adapters == []
    instance.release()


def test_unrelated_invalid_bucket_does_not_block_requested_queue(
    tmp_path: Path,
) -> None:
    config, locks, instance = _initialize(tmp_path)
    _bundle(tmp_path, "post", "QUEUE")
    invalid = _bundle(tmp_path, "bad-reel", "REELS")
    (invalid / ".ready").unlink()
    adapters: list[FakeAdapter] = []
    app = OneRunApplication(
        config,
        locks=locks,
        instance_lease=instance,
        environ={"POST_PULSAR_X_OPERATOR_USER_ACCESS_TOKEN": "token-x"},
        clock=lambda: NOW,
        adapter_factory=_factory(adapters),
    )

    outcome = app.run_once(RunOnceRequest("operator", "QUEUE", "scoped-1"))

    assert outcome.status == "archived"
    assert outcome.bundle_id == "post"
    assert invalid.is_dir()
    instance.release()


@pytest.mark.parametrize(
    "boundary",
    [
        "after_state_open",
        "after_bundle_admission",
        "after_media_staging",
        "after_preflight_barrier",
    ],
)
def test_injected_boundary_releases_owned_resources_but_not_instance(
    tmp_path: Path, boundary: str
) -> None:
    config, locks, instance = _initialize(tmp_path)
    _bundle(tmp_path, "post")
    adapters: list[FakeAdapter] = []

    def fail(candidate: str) -> None:
        if candidate == boundary:
            raise RuntimeError("injected boundary")

    app = OneRunApplication(
        config,
        locks=locks,
        instance_lease=instance,
        environ={"POST_PULSAR_X_OPERATOR_USER_ACCESS_TOKEN": "token-x"},
        clock=lambda: NOW,
        adapter_factory=_factory(adapters),
        fault_injector=cast(OrchestrationFaultInjector, fail),
    )

    with pytest.raises(RuntimeError, match="injected boundary"):
        app.run_once(RunOnceRequest("operator", "QUEUE", f"resource-{boundary}"))

    assert instance.active
    assert all(adapter.closed for adapter in adapters)
    assert not any((tmp_path / "state/staging/private").rglob("*.jpg"))
    with locks.acquire_profiles(instance, ("operator",)):
        with StateRepository.open_existing(
            tmp_path / "state/post_pulsar.sqlite3"
        ) as repository:
            assert repository.schema_version == 1
    instance.release()


def test_fifth_consecutive_safe_failure_blocks_for_guarded_retry(
    tmp_path: Path,
) -> None:
    config, locks, instance = _initialize(tmp_path)
    _bundle(tmp_path, "post")
    admitted = scan_account_root(tmp_path / "accounts/operator").bundles[0]
    with StateRepository.open_existing(
        tmp_path / "state/post_pulsar.sqlite3", clock=lambda: NOW
    ) as repo:
        bundle_key = repo.add_bundle(
            profile_id="operator",
            bundle_id="post",
            fingerprint=admitted.fingerprint,
            source_bucket="QUEUE",
            files=tuple(
                BundleFileSnapshot(
                    item.relative_name,
                    item.role,
                    item.ordinal,
                    item.media_kind,
                    item.mime_type,
                    item.size_bytes,
                    item.sha256,
                )
                for item in admitted.content.member_snapshots
            ),
            targets=(_target("x", tmp_path),),
        )
        for attempt in range(4):
            claim = repo.claim_delivery(bundle_key, "x", f"seed-{attempt}")
            repo.fail_delivery(
                bundle_key,
                "x",
                error_code="transient",
                error_message="Retry safely.",
                retry_at=NOW,
                claim_token=f"seed-{attempt}",
                attempt_count=claim.attempt_count,
            )

    def fail_prepare(_writer: CheckpointWriter) -> None:
        raise ClassifiedFailure("safe_pre_final")

    adapters: list[FakeAdapter] = []
    app = OneRunApplication(
        config,
        locks=locks,
        instance_lease=instance,
        environ={"POST_PULSAR_X_OPERATOR_USER_ACCESS_TOKEN": "token-x"},
        clock=lambda: NOW,
        adapter_factory=_factory(adapters, prepare_hook=fail_prepare),
    )

    outcome = app.run_once(RunOnceRequest("operator", "QUEUE", "fifth-1"))

    assert outcome.status == "blocked"
    with StateRepository.open_existing(tmp_path / "state/post_pulsar.sqlite3") as repo:
        delivery = repo.get_delivery(bundle_key, "x")
        assert delivery.consecutive_failures == 5
        assert not delivery.safe_to_retry
    instance.release()


def test_safe_remote_preparation_retry_reuses_attempt_and_cleans_on_success(
    tmp_path: Path,
) -> None:
    config, locks, instance = _initialize(tmp_path)
    _bundle(tmp_path, "post")
    calls: list[PreparedPublication | None] = []
    adapters: list[ResumableRemoteAdapter] = []

    def factory(
        snapshot: PublicationSnapshot, token: SecretValue, private: Path
    ) -> PlatformAdapter:
        assert token.reveal() == "token-x"
        assert private.name == "private"
        adapter = ResumableRemoteAdapter(snapshot, calls)
        adapters.append(adapter)
        return cast(PlatformAdapter, adapter)

    first = OneRunApplication(
        config,
        locks=locks,
        instance_lease=instance,
        environ={"POST_PULSAR_X_OPERATOR_USER_ACCESS_TOKEN": "token-x"},
        clock=lambda: NOW,
        adapter_factory=factory,
        retry_delay=timedelta(0),
    ).run_once(RunOnceRequest("operator", "QUEUE", "resume-remote-1"))

    assert first.status == "failed"
    assert any((tmp_path / "state/staging/private").rglob("*.jpg"))
    with StateRepository.open_existing(tmp_path / "state/post_pulsar.sqlite3") as repo:
        first_delivery = repo.get_delivery(cast(int, first.bundle_key), "x")
        assert first_delivery.attempt_count == 1
        assert first_delivery.phase == "processing"

    second = OneRunApplication(
        config,
        locks=locks,
        instance_lease=instance,
        environ={"POST_PULSAR_X_OPERATOR_USER_ACCESS_TOKEN": "token-x"},
        clock=lambda: NOW,
        adapter_factory=factory,
    ).run_once(RunOnceRequest("operator", "QUEUE", "resume-remote-2"))

    assert second.status == "archived"
    assert len(calls) == 2 and calls[0] is not None and calls[1] is not None
    assert not any((tmp_path / "state/staging/private").rglob("*.jpg"))
    with StateRepository.open_existing(tmp_path / "state/post_pulsar.sqlite3") as repo:
        final = repo.get_delivery(cast(int, second.bundle_key), "x")
        assert final.attempt_count == 1
        assert final.status == "published"
    assert all(adapter.closed for adapter in adapters)
    instance.release()


def test_future_retry_is_deferred_without_credentials_or_adapter(
    tmp_path: Path,
) -> None:
    config, locks, instance = _initialize(tmp_path)
    _bundle(tmp_path, "post")
    admitted = scan_account_root(tmp_path / "accounts/operator").bundles[0]
    with StateRepository.open_existing(
        tmp_path / "state/post_pulsar.sqlite3", clock=lambda: NOW
    ) as repo:
        bundle_key = repo.add_bundle(
            profile_id="operator",
            bundle_id="post",
            fingerprint=admitted.fingerprint,
            source_bucket="QUEUE",
            files=tuple(
                BundleFileSnapshot(
                    item.relative_name,
                    item.role,
                    item.ordinal,
                    item.media_kind,
                    item.mime_type,
                    item.size_bytes,
                    item.sha256,
                )
                for item in admitted.content.member_snapshots
            ),
            targets=(_target("x", tmp_path),),
        )
        claim = repo.claim_delivery(bundle_key, "x", "future")
        repo.fail_delivery(
            bundle_key,
            "x",
            error_code="transient",
            error_message="Retry later.",
            retry_at=datetime(2026, 9, 5, 13, tzinfo=UTC),
            claim_token="future",
            attempt_count=claim.attempt_count,
        )
    app = OneRunApplication(
        config,
        locks=locks,
        instance_lease=instance,
        environ={},
        clock=lambda: NOW,
        adapter_factory=lambda *_args: pytest.fail("deferred retry built an adapter"),
    )

    outcome = app.run_once(RunOnceRequest("operator", "QUEUE", "future-1"))

    assert outcome.status == "deferred"
    assert outcome.code == "retry_not_due"
    instance.release()


def test_explicit_pre_final_ambiguity_blocks_and_retains_staging(
    tmp_path: Path,
) -> None:
    config, locks, instance = _initialize(tmp_path)
    _bundle(tmp_path, "post")

    def ambiguous_prepare(_writer: CheckpointWriter) -> None:
        raise ClassifiedFailure("ambiguous")

    adapters: list[FakeAdapter] = []
    app = OneRunApplication(
        config,
        locks=locks,
        instance_lease=instance,
        environ={"POST_PULSAR_X_OPERATOR_USER_ACCESS_TOKEN": "token-x"},
        clock=lambda: NOW,
        adapter_factory=_factory(adapters, prepare_hook=ambiguous_prepare),
    )

    outcome = app.run_once(RunOnceRequest("operator", "QUEUE", "ambiguous-1"))

    assert outcome.status == "blocked"
    assert outcome.code == "preparation_ambiguous"
    with StateRepository.open_existing(tmp_path / "state/post_pulsar.sqlite3") as repo:
        delivery = repo.get_delivery(cast(int, outcome.bundle_key), "x")
        assert delivery.status == "ambiguous"
        artifacts = repo.list_delivery_artifacts(cast(int, outcome.bundle_key), "x")
        staged = next(item for item in artifacts if item.kind == "staged_private")
        assert (
            tmp_path / "state/staging/private" / cast(str, staged.relative_path)
        ).is_file()
    assert adapters[0].commits == 0
    instance.release()


def test_invalid_random_media_is_fully_nonmutating(tmp_path: Path) -> None:
    config, locks, instance = _initialize(tmp_path)
    directory = tmp_path / "accounts/operator/RANDOM/broken"
    directory.mkdir(parents=True)
    (directory / "broken.jpg").write_bytes(b"not-an-image")
    (directory / "broken.txt").write_text("caption\n", encoding="utf-8")
    (directory / ".ready").write_bytes(b"")
    app = OneRunApplication(
        config,
        locks=locks,
        instance_lease=instance,
        environ={"POST_PULSAR_X_OPERATOR_USER_ACCESS_TOKEN": "token-x"},
        clock=lambda: NOW,
        adapter_factory=lambda *_args: pytest.fail("invalid media built an adapter"),
    )

    outcome = app.run_once(RunOnceRequest("operator", "RANDOM", "invalid-media"))

    assert outcome.status == "invalid" and outcome.code == "media_validation_failed"
    with StateRepository.open_existing(tmp_path / "state/post_pulsar.sqlite3") as repo:
        assert repo.selection_counter("operator") == 0
        assert repo.list_active_bundles("operator") == ()
    assert not any((tmp_path / "state/staging/private").rglob("*.jpg"))
    instance.release()


def test_cross_bucket_duplicate_is_invalid_without_admission(tmp_path: Path) -> None:
    config, locks, instance = _initialize(tmp_path)
    _bundle(tmp_path, "same", "QUEUE")
    _bundle(tmp_path, "same", "RANDOM")
    app = OneRunApplication(
        config,
        locks=locks,
        instance_lease=instance,
        environ={"POST_PULSAR_X_OPERATOR_USER_ACCESS_TOKEN": "token-x"},
        clock=lambda: NOW,
        adapter_factory=lambda *_args: pytest.fail("duplicate built an adapter"),
    )

    outcome = app.run_once(RunOnceRequest("operator", "QUEUE", "duplicate-1"))

    assert outcome.status == "invalid"
    assert outcome.code == "duplicate_bundle_across_buckets"
    with StateRepository.open_existing(tmp_path / "state/post_pulsar.sqlite3") as repo:
        assert repo.list_active_bundles("operator") == ()
    instance.release()


@pytest.mark.parametrize("boundary", ["after_media_staging", "after_preflight_barrier"])
def test_pre_final_staging_crash_is_resumed_and_cleaned(
    tmp_path: Path, boundary: str
) -> None:
    config, locks, instance = _initialize(tmp_path)
    _bundle(tmp_path, "post")

    def crash(candidate: str) -> None:
        if candidate == boundary:
            raise HardCrash

    first = OneRunApplication(
        config,
        locks=locks,
        instance_lease=instance,
        environ={"POST_PULSAR_X_OPERATOR_USER_ACCESS_TOKEN": "token-x"},
        clock=lambda: NOW,
        adapter_factory=_factory([]),
        fault_injector=crash,
    )
    with pytest.raises(HardCrash):
        first.run_once(RunOnceRequest("operator", "QUEUE", f"crash-{boundary}"))
    assert not any((tmp_path / "state/staging/private").rglob("*.jpg"))

    second = OneRunApplication(
        config,
        locks=locks,
        instance_lease=instance,
        environ={"POST_PULSAR_X_OPERATOR_USER_ACCESS_TOKEN": "token-x"},
        clock=lambda: NOW,
        adapter_factory=_factory([]),
    )
    outcome = second.run_once(
        RunOnceRequest("operator", "QUEUE", f"resume-{boundary}")
    )

    assert outcome.status == "archived"
    assert not any((tmp_path / "state/staging/private").rglob("*.jpg"))
    instance.release()


def test_result_persisted_crash_cleans_private_and_public_without_tokens(
    tmp_path: Path,
) -> None:
    config, locks, instance = _initialize(tmp_path)
    _write_config(tmp_path, x_enabled=False, instagram_enabled=True)
    _bundle(tmp_path, "post")

    def crash(boundary: str) -> None:
        if boundary == "after_result_persisted:instagram":
            raise HardCrash

    first = OneRunApplication(
        config,
        locks=locks,
        instance_lease=instance,
        environ={"POST_PULSAR_INSTAGRAM_OPERATOR_ACCESS_TOKEN": "token-instagram"},
        clock=lambda: NOW,
        adapter_factory=_factory([]),
        fault_injector=crash,
    )
    with pytest.raises(HardCrash):
        first.run_once(RunOnceRequest("operator", "QUEUE", "persisted-1"))
    assert not any((tmp_path / "state/staging/private").rglob("*.jpg"))
    assert not any((tmp_path / "public").rglob("*.jpg"))

    _write_config(tmp_path, x_enabled=False, instagram_enabled=False)
    second = OneRunApplication(
        config,
        locks=locks,
        instance_lease=instance,
        environ={},
        clock=lambda: NOW,
        adapter_factory=lambda *_args: pytest.fail("recovery built an adapter"),
    )
    outcome = second.run_once(RunOnceRequest("operator", "QUEUE", "persisted-2"))

    assert outcome.status == "archived"
    assert not any((tmp_path / "state/staging/private").rglob("*.jpg"))
    assert not any((tmp_path / "public").rglob("*.jpg"))
    instance.release()


def test_archived_trigger_replay_never_selects_or_publishes_new_bundle(
    tmp_path: Path,
) -> None:
    config, locks, instance = _initialize(tmp_path)
    _bundle(tmp_path, "first")
    first_adapters: list[FakeAdapter] = []
    app = OneRunApplication(
        config,
        locks=locks,
        instance_lease=instance,
        environ={"POST_PULSAR_X_OPERATOR_USER_ACCESS_TOKEN": "token-x"},
        clock=lambda: NOW,
        adapter_factory=_factory(first_adapters),
    )
    first = app.run_once(RunOnceRequest("operator", "QUEUE", "durable-trigger"))
    _bundle(tmp_path, "second")
    second_adapters: list[FakeAdapter] = []
    replay = OneRunApplication(
        config,
        locks=locks,
        instance_lease=instance,
        environ={},
        clock=lambda: NOW,
        adapter_factory=_factory(second_adapters),
    ).run_once(RunOnceRequest("operator", "QUEUE", "durable-trigger"))

    assert first.status == replay.status == "archived"
    assert replay.bundle_key == first.bundle_key
    assert second_adapters == []
    assert (tmp_path / "accounts/operator/QUEUE/second").is_dir()
    instance.release()


def test_prepared_media_warnings_are_persisted_once(tmp_path: Path) -> None:
    config, locks, instance = _initialize(tmp_path)
    _bundle(tmp_path, "post")

    def prepare_with_warning(*args: object, **kwargs: object) -> object:
        prepared = prepare_bundle_media(*args, **kwargs)  # type: ignore[arg-type]
        warning = MediaWarning(
            "operator",
            "QUEUE",
            "post",
            "post.jpg",
            "media_cleanup_deferred",
            "Cleanup will be retried safely.",
        )
        return replace(prepared, warnings=(warning, warning))

    outcome = OneRunApplication(
        config,
        locks=locks,
        instance_lease=instance,
        environ={"POST_PULSAR_X_OPERATOR_USER_ACCESS_TOKEN": "token-x"},
        clock=lambda: NOW,
        adapter_factory=_factory([]),
        media_preparer=prepare_with_warning,  # type: ignore[arg-type]
    ).run_once(RunOnceRequest("operator", "QUEUE", "media-warning"))

    with StateRepository.open_existing(tmp_path / "state/post_pulsar.sqlite3") as repo:
        assert [
            item["event_code"] for item in repo.list_events(cast(int, outcome.bundle_key))
        ].count("media_cleanup_deferred") == 1
    instance.release()
