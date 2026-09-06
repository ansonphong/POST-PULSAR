"""CLI contracts for local, agent-safe POST PULSAR control."""

from __future__ import annotations

import json
import runpy
from io import StringIO
from pathlib import Path
from typing import cast

import pytest
from PIL import Image

from post_pulsar import main as package_main
import post_pulsar
from post_pulsar.cli import (
    CONTROL_SCHEMA,
    EXIT_CONTENTION,
    EXIT_OK,
    EXIT_USAGE,
    main,
)
from post_pulsar.config import SecretValue
from post_pulsar.locking import LockManager
from post_pulsar.platforms.base import (
    AdapterContractError,
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
        pass

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


def test_retry_requires_confirmation_then_revalidates_identity_under_lock(
    tmp_path: Path,
) -> None:
    config, database = _setup(tmp_path, enabled=False)
    bundle_key = _seed_bundle(database, state="failed")
    common = [
        "retry",
        "--config",
        str(config),
        "--profile",
        "operator",
        "--bundle-key",
        str(bundle_key),
        "--platform",
        "x",
    ]
    before = database.read_bytes()
    code, _output, _error = _invoke(common)
    assert code == EXIT_USAGE
    assert database.read_bytes() == before
    verified: list[str] = []

    def verify(
        snapshot: PublicationSnapshot, token: SecretValue, private: Path
    ) -> None:
        verified.append(snapshot.target.expected_username)
        assert token.reveal() == "token-x"
        assert private.name == "private"

    code, output, error = _invoke(
        common + ["--confirm", "RETRY", "--json"],
        environ={"POST_PULSAR_X_OPERATOR_USER_ACCESS_TOKEN": "token-x"},
        identity_verifier=verify,
    )

    assert code == EXIT_OK and error == "" and verified == ["operator"]
    assert json.loads(output)["result"]["delivery"]["safe_to_retry"] is True
    with StateRepository.open_existing(database) as repository:
        assert repository.get_bundle(bundle_key).status == "active"


def test_retry_lock_contention_is_nonmutating(tmp_path: Path) -> None:
    config, database = _setup(tmp_path)
    bundle_key = _seed_bundle(database, state="failed")
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
                "--platform",
                "x",
                "--confirm",
                "RETRY",
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
                "--platform",
                "x",
                "--not-published",
                "--confirm",
                "RECONCILE",
            ],
            identity_verifier=lambda *_args: pytest.fail("reconcile used the network"),
        )
        assert code == EXIT_CONTENTION
        assert database.read_bytes() == before


@pytest.mark.parametrize("published", [True, False])
def test_reconcile_is_offline_and_requires_exact_operator_evidence(
    tmp_path: Path, published: bool
) -> None:
    config, database = _setup(tmp_path, enabled=False)
    bundle_key = _seed_bundle(database, state="ambiguous")
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
            "--platform",
            "x",
            *resolution,
            "--confirm",
            "RECONCILE",
            "--json",
        ],
        identity_verifier=lambda *_args: pytest.fail("reconcile used the network"),
    )

    delivery = json.loads(output)["result"]["delivery"]
    assert code == EXIT_OK and error == ""
    assert delivery["status"] == ("published" if published else "failed")
    assert delivery["remote_id"] == ("96001" if published else None)


def test_errors_redact_configured_secret_and_package_exports_same_main(
    tmp_path: Path,
) -> None:
    config, database = _setup(tmp_path)
    bundle_key = _seed_bundle(database, state="failed")
    secret = "never-render-this-token"

    def unsafe_failure(
        _snapshot: PublicationSnapshot, token: SecretValue, _private: Path
    ) -> None:
        raise AdapterContractError(f"remote rejected {token.reveal()}")

    code, output, error = _invoke(
        [
            "retry",
            "--config",
            str(config),
            "--profile",
            "operator",
            "--bundle-key",
            str(bundle_key),
            "--platform",
            "x",
            "--confirm",
            "RETRY",
            "--json",
        ],
        environ={"POST_PULSAR_X_OPERATOR_USER_ACCESS_TOKEN": secret},
        identity_verifier=unsafe_failure,
    )

    assert code != EXIT_OK and output == ""
    assert secret not in error
    assert "<redacted>" in error
    log = (tmp_path / "state/post_pulsar.log").read_text(encoding="utf-8")
    assert secret not in log
    assert (tmp_path / "state/post_pulsar.log").stat().st_mode & 0o077 == 0
    assert package_main is main


def test_module_entrypoint_returns_the_package_main_exit_code(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(post_pulsar, "main", lambda: 23)

    with pytest.raises(SystemExit) as stopped:
        runpy.run_module("post_pulsar.__main__", run_name="__main__")

    assert stopped.value.code == 23
