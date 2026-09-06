"""CLI contracts for local, agent-safe POST PULSAR control."""

from __future__ import annotations

import json
import runpy
from datetime import UTC, datetime, timedelta
from io import StringIO
from pathlib import Path
from types import SimpleNamespace
from typing import cast

import pytest
from PIL import Image

import post_pulsar
from post_pulsar import main as package_main
from post_pulsar.cli import (
    CONTROL_SCHEMA,
    EXIT_CONTENTION,
    EXIT_OK,
    EXIT_USAGE,
    _execute_daemon_request,
    main,
)
from post_pulsar.config import SecretValue, load_local_settings
from post_pulsar.content import scan_inbox
from post_pulsar.daemon import ForegroundDaemon
from post_pulsar.locking import LockManager
from post_pulsar.platforms.base import (
    CheckpointWriter,
    PreparedPublication,
    PublicationRequest,
    PublicationSnapshot,
    PublishResult,
    ValidationIssue,
)
from post_pulsar.state import (
    BundleFileSnapshot,
    DeliveryPhase,
    DeliveryRecord,
    ProfileTargetSnapshot,
    StateRepository,
    TargetSnapshot,
)


class CLIAdapter:
    def __init__(self, snapshot: PublicationSnapshot) -> None:
        self.snapshot = snapshot
        self.closed = False

    def verify_identity(self) -> None:
        return None

    def preflight(self, publication: PublicationRequest) -> tuple[ValidationIssue, ...]:
        assert publication.snapshot == self.snapshot
        return ()

    def prepare(
        self,
        publication: PublicationRequest,
        *,
        prior: PreparedPublication | None,
        checkpoints: CheckpointWriter,
    ) -> PreparedPublication:
        del prior
        delivery = checkpoints.advance_phase(cast(DeliveryPhase, "ready"))
        return PreparedPublication(publication, delivery.attempt_count, ())

    def commit(
        self, prepared: PreparedPublication, *, delivery: DeliveryRecord
    ) -> PublishResult:
        del prepared, delivery
        return PublishResult.published("95001")

    def close(self) -> None:
        self.closed = True


def _write_config(root: Path, *, enabled: bool = True) -> Path:
    path = root / "post-pulsar.toml"
    path.write_text(
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
enabled = {str(enabled).lower()}
expected_remote_user_id = "10001"
expected_username = "operator"
token_env_var = "POST_PULSAR_X_OPERATOR_USER_ACCESS_TOKEN"
request_timeout_seconds = 30
processing_timeout_seconds = 300
chunk_size_bytes = 4194304
""".strip()
        + "\n",
        encoding="utf-8",
    )
    return path


def _profile_target() -> ProfileTargetSnapshot:
    return ProfileTargetSnapshot(
        "x",
        "10001",
        "operator",
        "POST_PULSAR_X_OPERATOR_USER_ACCESS_TOKEN",
        {
            "request_timeout_seconds": 30.0,
            "processing_timeout_seconds": 300.0,
            "chunk_size_bytes": 4194304,
        },
    )


def _target() -> TargetSnapshot:
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


def _setup(root: Path, *, enabled: bool = True) -> tuple[Path, Path]:
    config = _write_config(root, enabled=enabled)
    account = root / "accounts/operator"
    account.mkdir(parents=True)
    database = root / "state/post_pulsar.sqlite3"
    with StateRepository(database) as repository:
        repository.register_profile(
            "operator", account, (_profile_target(),), config_hash="1" * 64
        )
    return config, database


def _seed_bundle(database: Path, *, state: str) -> int:
    with StateRepository.open_existing(database) as repository:
        bundle_key = repository.add_bundle(
            profile_id="operator",
            bundle_id=f"post-{state}",
            fingerprint="b" * 64,
            source_bucket="QUEUE",
            files=(
                BundleFileSnapshot(
                    f"post-{state}.jpg",
                    "image",
                    0,
                    "image",
                    "image/jpeg",
                    3,
                    "a" * 64,
                ),
            ),
            targets=(_target(),),
        )
        if state == "pending":
            return bundle_key
        claim = repository.claim_delivery(bundle_key, "x", f"seed-{state}")
        if state == "failed":
            repository.fail_delivery(
                bundle_key,
                "x",
                error_code="permission_denied",
                error_message="Permission denied.",
                retry_at=None,
                permanent=True,
                claim_token=f"seed-{state}",
                attempt_count=claim.attempt_count,
            )
        elif state == "ambiguous":
            repository.advance_delivery_phase(
                bundle_key,
                "x",
                "final_dispatch_started",
                claim_token=f"seed-{state}",
                attempt_count=claim.attempt_count,
            )
            repository.mark_delivery_ambiguous(
                bundle_key,
                "x",
                error_code="dispatch_uncertain",
                error_message="Final result is unknown.",
                claim_token=f"seed-{state}",
                attempt_count=claim.attempt_count,
            )
        return bundle_key


def _approved_delivery_intent(
    database: Path,
    *,
    action: str,
    bundle_key: int,
    published_remote_id: str | None = None,
) -> tuple[str, int]:
    with StateRepository.open_existing(database) as repository:
        bundle = repository.get_bundle(bundle_key)
        arguments: dict[str, object] = {
            "bundle_id": bundle.bundle_id,
            "fingerprint": bundle.fingerprint,
            "platform": "x",
        }
        if action == "reconcile":
            arguments["published_remote_id"] = published_remote_id
        intent = repository.create_confirmation_intent(
            action=action,
            arguments=arguments,
            profile_id="operator",
            resource_revision=bundle.revision,
            fingerprint=bundle.fingerprint,
            consequence=f"Authorize exact {action}.",
            expires_at=datetime.now(UTC) + timedelta(minutes=5),
            bundle_key=bundle_key,
        )
        repository.approve_confirmation_intent(
            intent.intent_id, expected_revision=intent.revision
        )
        return intent.intent_id, bundle.revision


def _invoke(
    arguments: list[str],
    *,
    environ: dict[str, str] | None = None,
    **kwargs: object,
) -> tuple[int, str, str]:
    stdout = StringIO()
    stderr = StringIO()
    code = main(
        arguments,
        stdout=stdout,
        stderr=stderr,
        environ={} if environ is None else environ,
        **kwargs,  # type: ignore[arg-type]
    )
    return code, stdout.getvalue(), stderr.getvalue()


def test_default_run_emits_versioned_json_and_archives(tmp_path: Path) -> None:
    config, _database = _setup(tmp_path)
    bundle = tmp_path / "accounts/operator/QUEUE/post"
    bundle.mkdir(parents=True)
    Image.new("RGB", (2, 2), "blue").save(bundle / "post.jpg")
    (bundle / "post.txt").write_text("hello\n", encoding="utf-8")
    (bundle / ".ready").write_bytes(b"")
    adapters: list[CLIAdapter] = []

    def factory(
        snapshot: PublicationSnapshot, token: SecretValue, private: Path
    ) -> CLIAdapter:
        assert token.reveal() == "token-x"
        assert private.name == "private"
        adapter = CLIAdapter(snapshot)
        adapters.append(adapter)
        return adapter

    code, output, error = _invoke(
        [
            "--config",
            str(config),
            "--json",
            "--profile",
            "operator",
            "--bucket",
            "QUEUE",
            "--trigger-id",
            "cli-run-1",
        ],
        environ={"POST_PULSAR_X_OPERATOR_USER_ACCESS_TOKEN": "token-x"},
        adapter_factory=factory,
    )

    document = json.loads(output)
    assert code == EXIT_OK and error == ""
    assert document["schema"] == CONTROL_SCHEMA
    assert document["command"] == "run"
    assert document["result"]["status"] == "archived"
    assert adapters and all(adapter.closed for adapter in adapters)


def test_status_is_credentialless_read_only_and_renders_warnings(
    tmp_path: Path,
) -> None:
    config, database = _setup(tmp_path, enabled=False)
    bundle_key = _seed_bundle(database, state="pending")
    with StateRepository.open_existing(database) as repository:
        repository.record_warning(
            bundle_key,
            "x",
            "media_cleanup_deferred",
            {"message": "Cleanup will be retried safely."},
        )
    before = database.stat()

    code, output, error = _invoke(
        ["status", "--config", str(config), "--profile", "operator", "--json"],
        identity_verifier=lambda *_args: pytest.fail("status used the network"),
    )

    document = json.loads(output)
    after = database.stat()
    assert code == EXIT_OK and error == ""
    assert document["result"]["bundles"][0]["warnings"] == ["media_cleanup_deferred"]
    assert (after.st_size, after.st_mtime_ns) == (before.st_size, before.st_mtime_ns)
    assert not (tmp_path / "state/post_pulsar.log").exists()


def test_retry_requires_approved_intent_and_enqueues_without_network(
    tmp_path: Path,
) -> None:
    config, database = _setup(tmp_path, enabled=False)
    bundle_key = _seed_bundle(database, state="failed")
    intent_id, revision = _approved_delivery_intent(
        database, action="retry", bundle_key=bundle_key
    )
    common = [
        "retry",
        "--config",
        str(config),
        "--profile",
        "operator",
        "--bundle-key",
        str(bundle_key),
        "--bundle-id",
        "post-failed",
        "--fingerprint",
        "b" * 64,
        "--platform",
        "x",
        "--expected-revision",
        str(revision),
        "--idempotency-key",
        "retry-request",
    ]
    before = database.read_bytes()
    code, _output, _error = _invoke(common)
    assert code == EXIT_USAGE
    assert database.read_bytes() == before
    code, output, error = _invoke(
        [*common, "--intent-id", intent_id, "--json"],
        identity_verifier=lambda *_args: pytest.fail("retry executed during enqueue"),
    )

    assert code == EXIT_OK and error == ""
    assert json.loads(output)["result"]["request"]["status"] == "queued"
    with StateRepository.open_existing(database) as repository:
        assert repository.get_bundle(bundle_key).status == "blocked"


def test_retry_lock_contention_is_nonmutating(tmp_path: Path) -> None:
    config, database = _setup(tmp_path)
    bundle_key = _seed_bundle(database, state="failed")
    intent_id, revision = _approved_delivery_intent(
        database, action="retry", bundle_key=bundle_key
    )
    manager = LockManager(tmp_path / "state")
    with manager.acquire_instance():
        before = database.read_bytes()
        code, _output, error = _invoke(
            [
                "retry",
                "--config",
                str(config),
                "--profile",
                "operator",
                "--bundle-key",
                str(bundle_key),
                "--bundle-id",
                "post-failed",
                "--fingerprint",
                "b" * 64,
                "--platform",
                "x",
                "--expected-revision",
                str(revision),
                "--idempotency-key",
                "retry-contended",
                "--intent-id",
                intent_id,
            ],
            environ={"POST_PULSAR_X_OPERATOR_USER_ACCESS_TOKEN": "token-x"},
            identity_verifier=lambda *_args: pytest.fail("contended retry verified"),
        )
        assert code == EXIT_CONTENTION
        assert "active" in error
        assert database.read_bytes() == before


def test_reconcile_lock_contention_is_nonmutating(tmp_path: Path) -> None:
    config, database = _setup(tmp_path)
    bundle_key = _seed_bundle(database, state="ambiguous")
    intent_id, revision = _approved_delivery_intent(
        database, action="reconcile", bundle_key=bundle_key
    )
    manager = LockManager(tmp_path / "state")
    with manager.acquire_instance():
        before = database.read_bytes()
        code, _output, _error = _invoke(
            [
                "reconcile",
                "--config",
                str(config),
                "--profile",
                "operator",
                "--bundle-key",
                str(bundle_key),
                "--bundle-id",
                "post-ambiguous",
                "--fingerprint",
                "b" * 64,
                "--platform",
                "x",
                "--not-published",
                "--expected-revision",
                str(revision),
                "--idempotency-key",
                "reconcile-contended",
                "--intent-id",
                intent_id,
            ],
            identity_verifier=lambda *_args: pytest.fail("reconcile used the network"),
        )
        assert code == EXIT_CONTENTION
        assert database.read_bytes() == before


@pytest.mark.parametrize("published", [True, False])
def test_reconcile_is_offline_and_enqueues_exact_operator_evidence(
    tmp_path: Path, published: bool
) -> None:
    config, database = _setup(tmp_path, enabled=False)
    bundle_key = _seed_bundle(database, state="ambiguous")
    remote_id = "96001" if published else None
    intent_id, revision = _approved_delivery_intent(
        database,
        action="reconcile",
        bundle_key=bundle_key,
        published_remote_id=remote_id,
    )
    resolution = ["--published", "96001"] if published else ["--not-published"]

    code, output, error = _invoke(
        [
            "reconcile",
            "--config",
            str(config),
            "--profile",
            "operator",
            "--bundle-key",
            str(bundle_key),
            "--bundle-id",
            "post-ambiguous",
            "--fingerprint",
            "b" * 64,
            "--platform",
            "x",
            *resolution,
            "--expected-revision",
            str(revision),
            "--idempotency-key",
            f"reconcile-{published}",
            "--intent-id",
            intent_id,
            "--json",
        ],
        identity_verifier=lambda *_args: pytest.fail("reconcile used the network"),
    )

    request = json.loads(output)["result"]["request"]
    assert code == EXIT_OK and error == ""
    assert request["status"] == "queued"
    assert request["arguments"]["published_remote_id"] == remote_id


def test_durable_retry_does_not_render_credentials_and_package_exports_main(
    tmp_path: Path,
) -> None:
    config, database = _setup(tmp_path)
    bundle_key = _seed_bundle(database, state="failed")
    intent_id, revision = _approved_delivery_intent(
        database, action="retry", bundle_key=bundle_key
    )
    secret = "never-render-this-token"

    code, output, error = _invoke(
        [
            "retry",
            "--config",
            str(config),
            "--profile",
            "operator",
            "--bundle-key",
            str(bundle_key),
            "--bundle-id",
            "post-failed",
            "--fingerprint",
            "b" * 64,
            "--platform",
            "x",
            "--expected-revision",
            str(revision),
            "--idempotency-key",
            "retry-secret-safe",
            "--intent-id",
            intent_id,
            "--json",
        ],
        environ={"POST_PULSAR_X_OPERATOR_USER_ACCESS_TOKEN": secret},
        identity_verifier=lambda *_args: pytest.fail("retry executed during enqueue"),
    )

    assert code == EXIT_OK and error == ""
    assert secret not in output
    log = (tmp_path / "state/post_pulsar.log").read_text(encoding="utf-8")
    assert secret not in log
    assert (tmp_path / "state/post_pulsar.log").stat().st_mode & 0o077 == 0
    assert package_main is main


def test_daemon_retry_executor_revalidates_remote_identity(tmp_path: Path) -> None:
    config, database = _setup(tmp_path)
    bundle_key = _seed_bundle(database, state="failed")
    intent_id, revision = _approved_delivery_intent(
        database, action="retry", bundle_key=bundle_key
    )
    with StateRepository.open_existing(database) as repository:
        bundle = repository.get_bundle(bundle_key)
        request = repository.consume_intent_with_request(
            intent_id=intent_id,
            action="retry",
            arguments={
                "bundle_id": bundle.bundle_id,
                "fingerprint": bundle.fingerprint,
                "platform": "x",
            },
            profile_id="operator",
            resource_revision=revision,
            fingerprint=bundle.fingerprint,
            idempotency_key="retry-executor",
            bundle_key=bundle_key,
        )
    verified: list[str] = []

    def verify(
        snapshot: PublicationSnapshot, token: SecretValue, private: Path
    ) -> None:
        verified.append(snapshot.target.expected_remote_user_id)
        assert token.reveal() == "token-x"
        assert private.name == "private"

    locks = LockManager(tmp_path / "state")
    with locks.acquire_instance() as lease:
        daemon = cast(ForegroundDaemon, SimpleNamespace(_locks=locks, _lease=lease))
        result = _execute_daemon_request(
            load_local_settings(config),
            request,
            daemon=daemon,
            environ={"POST_PULSAR_X_OPERATOR_USER_ACCESS_TOKEN": "token-x"},
            adapter_factory=None,
            identity_verifier=verify,
            clock=lambda: datetime.now(UTC),
        )

    assert verified == ["10001"]
    assert result["safe_to_retry"] is True
    with StateRepository.open_existing(database) as repository:
        assert repository.get_bundle(bundle_key).status == "active"


def test_daemon_reconcile_executor_is_offline(tmp_path: Path) -> None:
    config, database = _setup(tmp_path)
    bundle_key = _seed_bundle(database, state="ambiguous")
    intent_id, revision = _approved_delivery_intent(
        database,
        action="reconcile",
        bundle_key=bundle_key,
        published_remote_id="96001",
    )
    with StateRepository.open_existing(database) as repository:
        bundle = repository.get_bundle(bundle_key)
        request = repository.consume_intent_with_request(
            intent_id=intent_id,
            action="reconcile",
            arguments={
                "bundle_id": bundle.bundle_id,
                "fingerprint": bundle.fingerprint,
                "platform": "x",
                "published_remote_id": "96001",
            },
            profile_id="operator",
            resource_revision=revision,
            fingerprint=bundle.fingerprint,
            idempotency_key="reconcile-executor",
            bundle_key=bundle_key,
        )
    locks = LockManager(tmp_path / "state")
    with locks.acquire_instance() as lease:
        daemon = cast(ForegroundDaemon, SimpleNamespace(_locks=locks, _lease=lease))
        result = _execute_daemon_request(
            load_local_settings(config),
            request,
            daemon=daemon,
            environ={},
            adapter_factory=None,
            identity_verifier=lambda *_args: pytest.fail("reconcile used the network"),
            clock=lambda: datetime.now(UTC),
        )

    assert result["status"] == "published"
    assert result["remote_id"] == "96001"


@pytest.mark.parametrize("action", ["cancel", "delete"])
def test_daemon_pending_terminal_executor_preserves_source_media(
    tmp_path: Path, action: str
) -> None:
    config, database = _setup(tmp_path)
    bundle_key = _seed_bundle(database, state="pending")
    source = tmp_path / "accounts/operator/QUEUE/post-pending/post-pending.jpg"
    source.parent.mkdir(parents=True)
    source.write_bytes(b"source-media")
    with StateRepository.open_existing(database) as repository:
        bundle = repository.get_bundle(bundle_key)
        arguments = {
            "bundle_id": bundle.bundle_id,
            "fingerprint": bundle.fingerprint,
        }
        intent = repository.create_confirmation_intent(
            action=action,
            arguments=arguments,
            profile_id="operator",
            resource_revision=bundle.revision,
            fingerprint=bundle.fingerprint,
            consequence=f"Authorize exact {action} tombstone.",
            expires_at=datetime.now(UTC) + timedelta(minutes=5),
            bundle_key=bundle_key,
        )
        repository.approve_confirmation_intent(
            intent.intent_id, expected_revision=intent.revision
        )
        request = repository.consume_intent_with_request(
            intent_id=intent.intent_id,
            action=action,
            arguments=arguments,
            profile_id="operator",
            resource_revision=bundle.revision,
            fingerprint=bundle.fingerprint,
            idempotency_key=f"{action}-executor",
            bundle_key=bundle_key,
        )
    locks = LockManager(tmp_path / "state")
    with locks.acquire_instance() as lease:
        daemon = cast(ForegroundDaemon, SimpleNamespace(_locks=locks, _lease=lease))
        result = _execute_daemon_request(
            load_local_settings(config),
            request,
            daemon=daemon,
            environ={},
            adapter_factory=None,
            identity_verifier=lambda *_args: pytest.fail(
                "terminal action used network"
            ),
            clock=lambda: datetime.now(UTC),
        )

    assert result["status"] == ("cancelled" if action == "cancel" else "deleted")
    assert source.read_bytes() == b"source-media"


def test_daemon_draft_admission_executor_reaches_journaled_service(
    tmp_path: Path,
) -> None:
    config, database = _setup(tmp_path)
    drafts = tmp_path / "accounts/operator/DRAFTS"
    drafts.mkdir(parents=True)
    (drafts / "draft.txt").write_text("caption", encoding="utf-8")
    (drafts / "draft.jpg").write_bytes(b"image")
    fingerprint = scan_inbox(drafts).bundles[0].fingerprint
    arguments = {"bucket": "QUEUE", "bundle_id": "draft"}
    with StateRepository.open_existing(database) as repository:
        intent = repository.create_confirmation_intent(
            action="admit_draft",
            arguments=arguments,
            profile_id="operator",
            resource_revision=1,
            fingerprint=fingerprint,
            consequence="Promote exact draft.",
            expires_at=datetime.now(UTC) + timedelta(minutes=5),
        )
        repository.approve_confirmation_intent(
            intent.intent_id, expected_revision=intent.revision
        )
        request = repository.consume_intent_with_request(
            intent_id=intent.intent_id,
            action="admit_draft",
            arguments=arguments,
            profile_id="operator",
            resource_revision=1,
            fingerprint=fingerprint,
            idempotency_key="admit-executor",
        )
    locks = LockManager(tmp_path / "state")
    with locks.acquire_instance() as lease:
        daemon = cast(ForegroundDaemon, SimpleNamespace(_locks=locks, _lease=lease))
        result = _execute_daemon_request(
            load_local_settings(config),
            request,
            daemon=daemon,
            environ={},
            adapter_factory=None,
            identity_verifier=lambda *_args: pytest.fail("admission used network"),
            clock=lambda: datetime.now(UTC),
        )
    assert result["phase"] == "installed"
    assert (tmp_path / "accounts/operator/QUEUE/draft/.ready").exists()


def test_daemon_draft_edit_executor_requires_the_exact_fingerprint(
    tmp_path: Path,
) -> None:
    config, database = _setup(tmp_path)
    drafts = tmp_path / "accounts/operator/DRAFTS"
    drafts.mkdir(parents=True)
    caption = drafts / "draft.txt"
    caption.write_text("old", encoding="utf-8")
    (drafts / "draft.jpg").write_bytes(b"image")
    fingerprint = scan_inbox(drafts).bundles[0].fingerprint
    with StateRepository.open_existing(database) as repository:
        request = repository.create_run_request(
            profile_id="operator",
            action="edit_caption",
            arguments={
                "bucket": "DRAFTS",
                "bundle_id": "draft",
                "fingerprint": fingerprint,
                "text": "new caption",
            },
            idempotency_key="edit-executor",
            expected_revision=1,
        )
    locks = LockManager(tmp_path / "state")
    with locks.acquire_instance() as lease:
        daemon = cast(ForegroundDaemon, SimpleNamespace(_locks=locks, _lease=lease))
        result = _execute_daemon_request(
            load_local_settings(config),
            request,
            daemon=daemon,
            environ={},
            adapter_factory=None,
            identity_verifier=lambda *_args: pytest.fail("edit used network"),
            clock=lambda: datetime.now(UTC),
        )
    assert caption.read_text(encoding="utf-8") == "new caption"
    assert result["fingerprint"] != fingerprint


def test_module_entrypoint_returns_the_package_main_exit_code(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(post_pulsar, "main", lambda: 23)

    with pytest.raises(SystemExit) as stopped:
        runpy.run_module("post_pulsar.__main__", run_name="__main__")

    assert stopped.value.code == 23
