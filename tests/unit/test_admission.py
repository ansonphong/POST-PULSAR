"""Crash-safe DRAFTS admission tests."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from post_pulsar.admission import AdmissionError, DraftAdmissionService
from post_pulsar.content import scan_inbox
from post_pulsar.state import ConflictError, ProfileTargetSnapshot, StateRepository


def _repository(tmp_path: Path) -> StateRepository:
    repository = StateRepository(tmp_path / "state/post_pulsar.sqlite3")
    repository.register_profile(
        "profile",
        tmp_path / "account",
        (
            ProfileTargetSnapshot(
                "x", "1", "profile", "POST_PULSAR_X_PROFILE_USER_ACCESS_TOKEN", {}
            ),
        ),
        config_hash="a" * 64,
    )
    return repository


def _draft(tmp_path: Path) -> tuple[Path, str]:
    root = tmp_path / "account"
    drafts = root / "DRAFTS"
    drafts.mkdir(parents=True)
    (drafts / "hello.txt").write_text("caption", encoding="utf-8")
    (drafts / "hello.jpg").write_bytes(b"image")
    bundle = scan_inbox(drafts).bundles[0]
    return root, bundle.fingerprint


def _intent(repository: StateRepository, fingerprint: str) -> str:
    arguments = {"bucket": "QUEUE", "bundle_id": "hello"}
    intent = repository.create_confirmation_intent(
        action="admit_draft",
        arguments=arguments,
        profile_id="profile",
        resource_revision=1,
        fingerprint=fingerprint,
        consequence="Promote this exact draft",
        expires_at=datetime.now(UTC) + timedelta(minutes=5),
    )
    intent = repository.approve_confirmation_intent(
        intent.intent_id, expected_revision=intent.revision
    )
    repository.consume_intent_with_request(
        intent_id=intent.intent_id,
        action="admit_draft",
        arguments=arguments,
        profile_id="profile",
        resource_revision=1,
        fingerprint=fingerprint,
        idempotency_key="admit-hello",
    )
    return intent.intent_id


def test_admission_copies_exact_members_installs_ready_last_and_retains_sources(
    tmp_path: Path,
) -> None:
    root, fingerprint = _draft(tmp_path)
    repository = _repository(tmp_path)
    service = DraftAdmissionService(repository, root)
    intent_id = _intent(repository, fingerprint)

    admitted = service.admit(
        profile_id="profile",
        bucket="QUEUE",
        bundle_id="hello",
        expected_fingerprint=fingerprint,
        intent_id=intent_id,
    )

    assert admitted.phase == "installed"
    assert (root / "QUEUE/hello/.ready").read_bytes() == b""
    assert (root / "QUEUE/hello/hello.txt").read_text(encoding="utf-8") == "caption"
    assert (root / "DRAFTS/hello.jpg").read_bytes() == b"image"
    assert scan_inbox(root / "QUEUE/hello").bundles[0].fingerprint == fingerprint


def test_admission_rejects_symlinks_drift_duplicates_and_destination_conflicts(
    tmp_path: Path,
) -> None:
    root, fingerprint = _draft(tmp_path)
    repository = _repository(tmp_path)
    service = DraftAdmissionService(repository, root)
    (root / "DRAFTS/hello.jpg").unlink()
    (root / "DRAFTS/hello.jpg").symlink_to(root / "DRAFTS/hello.txt")
    with pytest.raises(AdmissionError):
        service.admit(
            profile_id="profile",
            bucket="QUEUE",
            bundle_id="hello",
            expected_fingerprint=fingerprint,
            intent_id="intent",
        )


@pytest.mark.parametrize(
    ("boundary", "installed"),
    [
        ("after_journal", False),
        ("after_member_planned:hello.jpg", False),
        ("after_member_copied:hello.jpg", False),
        ("after_member_verified:hello.jpg", False),
        ("before_ready_install", False),
        ("after_ready_install", True),
        ("after_atomic_rename", True),
    ],
)
def test_restart_recovers_every_admission_boundary(
    tmp_path: Path, boundary: str, installed: bool
) -> None:
    root, fingerprint = _draft(tmp_path)
    repository = _repository(tmp_path)
    intent_id = _intent(repository, fingerprint)

    def fail(selected: str) -> None:
        if selected == boundary:
            raise RuntimeError("simulated crash")

    service = DraftAdmissionService(repository, root, fault_injector=fail)
    with pytest.raises(RuntimeError, match="simulated crash"):
        service.admit(
            profile_id="profile",
            bucket="QUEUE",
            bundle_id="hello",
            expected_fingerprint=fingerprint,
            intent_id=intent_id,
        )

    recovered = DraftAdmissionService(repository, root).recover()[0]
    assert (recovered.phase == "installed") is installed
    assert (root / "QUEUE/hello/.ready").exists() is installed
    assert (root / "DRAFTS/hello.jpg").read_bytes() == b"image"


def test_atomic_install_never_replaces_a_concurrent_destination(tmp_path: Path) -> None:
    root, fingerprint = _draft(tmp_path)
    repository = _repository(tmp_path)
    intent_id = _intent(repository, fingerprint)

    def race(boundary: str) -> None:
        if boundary == "before_atomic_rename":
            destination = root / "QUEUE/hello"
            destination.mkdir()
            (destination / "competitor").write_text("keep", encoding="utf-8")

    with pytest.raises(ConflictError, match="destination"):
        DraftAdmissionService(repository, root, fault_injector=race).admit(
            profile_id="profile",
            bucket="QUEUE",
            bundle_id="hello",
            expected_fingerprint=fingerprint,
            intent_id=intent_id,
        )
    assert (root / "QUEUE/hello/competitor").read_text(encoding="utf-8") == "keep"
