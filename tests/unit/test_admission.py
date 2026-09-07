"""Crash-safe DRAFTS admission tests."""

from __future__ import annotations


def test_darwin_atomic_exclusive_rename(monkeypatch, tmp_path):
    import post_pulsar.admission as admission

    calls = []

    class Rename:
        def __call__(self, *args):
            calls.append(args)
            return 0

    class Libc:
        renamex_np = Rename()

    monkeypatch.setattr(admission.sys, "platform", "darwin")
    monkeypatch.setattr(admission.ctypes, "CDLL", lambda *a, **k: Libc())
    admission._rename_no_replace(tmp_path / "source", tmp_path / "target")
    assert calls == [(bytes(tmp_path / "source"), bytes(tmp_path / "target"), 4)]


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


def _intent(
    repository: StateRepository,
    fingerprint: str,
    *,
    idempotency_key: str = "admit-hello",
) -> str:
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
        idempotency_key=idempotency_key,
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


def test_recovery_never_rolls_back_another_profiles_journal(tmp_path: Path) -> None:
    root, fingerprint = _draft(tmp_path)
    repository = _repository(tmp_path)
    intent_id = _intent(repository, fingerprint)

    def stop_after_journal(boundary):
        if boundary == "after_journal":
            raise RuntimeError("crash before copy")

    with pytest.raises(RuntimeError, match="before copy"):
        DraftAdmissionService(
            repository, root, fault_injector=stop_after_journal
        ).admit(
            profile_id="profile",
            bucket="QUEUE",
            bundle_id="hello",
            expected_fingerprint=fingerprint,
            intent_id=intent_id,
        )
    journal = repository.list_recoverable_admissions()[0]
    other_root = tmp_path / "other-account"
    other_root.mkdir()
    assert DraftAdmissionService(repository, other_root).recover() == ()
    assert repository.get_admission(journal.journal_id).phase == "started"
    assert (root / "DRAFTS/hello.jpg").read_bytes() == b"image"


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


_PRE_READY_BOUNDARIES = (
    "after_journal",
    "after_member_planned:hello.jpg",
    "after_member_copied:hello.jpg",
    "after_member_verified:hello.jpg",
    "after_member_planned:hello.txt",
    "after_member_copied:hello.txt",
    "after_member_verified:hello.txt",
    "before_ready_install",
)


def _crash_admission(
    repository: StateRepository,
    root: Path,
    fingerprint: str,
    intent_id: str,
    boundary: str,
) -> None:
    def fail(selected: str) -> None:
        if selected == boundary:
            raise RuntimeError("simulated admission crash")

    with pytest.raises(RuntimeError, match="simulated admission crash"):
        DraftAdmissionService(repository, root, fault_injector=fail).admit(
            profile_id="profile",
            bucket="QUEUE",
            bundle_id="hello",
            expected_fingerprint=fingerprint,
            intent_id=intent_id,
        )


@pytest.mark.parametrize("boundary", _PRE_READY_BOUNDARIES)
def test_daemon_replays_same_request_after_each_pre_ready_admission_crash(
    tmp_path: Path,
    boundary: str,
) -> None:
    import threading
    from types import SimpleNamespace

    from post_pulsar.cli import _execute_daemon_request
    from post_pulsar.daemon import ForegroundDaemon

    root, fingerprint = _draft(tmp_path)
    repository = _repository(tmp_path)
    intent_id = _intent(repository, fingerprint)
    request = repository.claim_next_run_request("dead-daemon")
    assert request is not None
    _crash_admission(repository, root, fingerprint, intent_id, boundary)
    prior_journal = repository.list_recoverable_admissions()[0]
    database = repository.path
    repository.close()
    attempted = threading.Event()
    recovered_phases = []

    class Server:
        server_address = ("127.0.0.1", 43210)

        def __init__(self) -> None:
            self.closed = threading.Event()

        def serve_forever(self) -> None:
            self.closed.wait()

        def shutdown(self) -> None:
            self.closed.set()

        def server_close(self) -> None:
            self.closed.set()

    def recover() -> None:
        with StateRepository.open_existing(database) as fresh:
            restored = fresh.recover_claimed_run_requests(daemon._worker_token)
            assert [item.request_id for item in restored] == [request.request_id]
            recovered_phases.extend(
                item.phase for item in DraftAdmissionService(fresh, root).recover()
            )

    settings = SimpleNamespace(
        app=SimpleNamespace(state_directory=tmp_path / "state"),
        profile=lambda _profile_id: SimpleNamespace(account_root=root),
    )

    def execute(replayed):
        assert replayed.request_id == request.request_id
        assert replayed.intent_id == intent_id
        try:
            return _execute_daemon_request(
                settings,
                replayed,
                daemon=daemon,
                environ={},
                adapter_factory=None,
                identity_verifier=lambda *_args: pytest.fail("admission used network"),
                clock=lambda: datetime.now(UTC),
            )
        finally:
            attempted.set()

    daemon = ForegroundDaemon(
        tmp_path / "state",
        tmp_path / "control/endpoint.json",
        "127.0.0.1",
        0,
        recovery=recover,
        request_executor=execute,
        poll_seconds=0.001,
        server_factory=lambda *_args: Server(),
    )
    try:
        daemon.start()
        assert attempted.wait(5), "recovered admission request was not executed"
    finally:
        daemon.stop()

    with StateRepository.open_existing(database) as fresh:
        completed = fresh.get_run_request(request.request_id)
        assert recovered_phases == ["rolled_back"]
        assert completed.status == "completed"
        assert completed.result["phase"] == "installed"
        assert completed.result["journal_id"] != prior_journal.journal_id
        assert fresh.get_confirmation_intent(intent_id).state == "consumed"
        assert len(fresh.list_run_requests()) == 1
    assert (root / "QUEUE/hello/.ready").read_bytes() == b""
    assert scan_inbox(root / "QUEUE/hello").bundles[0].fingerprint == fingerprint
    assert (root / "DRAFTS/hello.jpg").read_bytes() == b"image"
    assert (root / "DRAFTS/hello.txt").read_text(encoding="utf-8") == "caption"
    assert not tuple((root / "QUEUE").glob(".admitting-*"))


def test_rolled_back_admission_restarts_with_fresh_checkpoints_and_staging_identity(
    tmp_path: Path,
) -> None:
    root, fingerprint = _draft(tmp_path)
    repository = _repository(tmp_path)
    intent_id = _intent(repository, fingerprint)
    _crash_admission(
        repository, root, fingerprint, intent_id, "after_member_verified:hello.jpg"
    )
    old_temporary = tuple((root / "QUEUE").glob(".admitting-*"))
    assert len(old_temporary) == 1
    rolled_back = DraftAdmissionService(repository, root).recover()[0]
    assert rolled_back.phase == "rolled_back"
    _crash_admission(
        repository, root, fingerprint, intent_id, "after_member_planned:hello.jpg"
    )
    restarted = repository.list_recoverable_admissions()[0]
    assert restarted.journal_id != rolled_back.journal_id
    assert [
        item.phase for item in repository.list_admission_members(restarted.journal_id)
    ] == ["planned"]
    new_temporary = tuple((root / "QUEUE").glob(".admitting-*"))
    assert len(new_temporary) == 1 and new_temporary != old_temporary
    assert not (new_temporary[0] / ".ready").exists()
    # Reopening also proves the transactional reset restored the canonical schema.
    with StateRepository.open_existing(repository.path) as fresh:
        assert fresh.get_admission(restarted.journal_id).phase == "copying"


def test_fresh_approval_can_retry_failed_rolled_back_unchanged_draft(
    tmp_path: Path,
) -> None:
    root, fingerprint = _draft(tmp_path)
    repository = _repository(tmp_path)
    original_intent = _intent(repository, fingerprint)
    original_request = repository.claim_next_run_request("failed-daemon")
    assert original_request is not None
    _crash_admission(
        repository, root, fingerprint, original_intent, "before_ready_install"
    )
    DraftAdmissionService(repository, root).recover()
    repository.complete_run_request(
        original_request.request_id,
        worker_token="failed-daemon",
        result={"code": "request_failed"},
        failed=True,
    )
    new_intent = _intent(repository, fingerprint, idempotency_key="admit-hello-again")
    admitted = DraftAdmissionService(repository, root).admit(
        profile_id="profile",
        bucket="QUEUE",
        bundle_id="hello",
        expected_fingerprint=fingerprint,
        intent_id=new_intent,
    )
    assert admitted.phase == "installed" and admitted.intent_id == new_intent
    assert repository.get_run_request(original_request.request_id).status == "failed"
    assert repository.get_confirmation_intent(original_intent).state == "consumed"
    assert (root / "DRAFTS/hello.jpg").read_bytes() == b"image"


@pytest.mark.parametrize("claimed", [False, True])
def test_fresh_approval_cannot_supersede_a_live_admission_request(
    tmp_path: Path,
    claimed: bool,
) -> None:
    root, fingerprint = _draft(tmp_path)
    repository = _repository(tmp_path)
    original_intent = _intent(repository, fingerprint)
    if claimed:
        assert repository.claim_next_run_request("active-daemon") is not None
    _crash_admission(repository, root, fingerprint, original_intent, "after_journal")
    rolled_back = DraftAdmissionService(repository, root).recover()[0]
    new_intent = _intent(repository, fingerprint, idempotency_key="admit-hello-other")
    with pytest.raises(ConflictError):
        DraftAdmissionService(repository, root).admit(
            profile_id="profile",
            bucket="QUEUE",
            bundle_id="hello",
            expected_fingerprint=fingerprint,
            intent_id=new_intent,
        )
    assert repository.get_admission(rolled_back.journal_id).phase == "rolled_back"
    assert not (root / "QUEUE/hello").exists()


@pytest.mark.parametrize(
    ("statement", "value"),
    [
        ("UPDATE confirmation_intents SET binding_sha256 = ?", "0" * 64),
        ("UPDATE run_requests SET request_sha256 = ?", "0" * 64),
        ("UPDATE run_requests SET action = ?", "pause"),
        ("UPDATE run_requests SET expected_revision = ?", 2),
        ("UPDATE run_requests SET intent_id = ?", None),
    ],
)
def test_rolled_back_retry_revalidates_consumed_intent_and_exact_request_binding(
    tmp_path: Path,
    statement: str,
    value: object,
) -> None:
    root, fingerprint = _draft(tmp_path)
    repository = _repository(tmp_path)
    intent_id = _intent(repository, fingerprint)
    _crash_admission(repository, root, fingerprint, intent_id, "before_ready_install")
    rolled_back = DraftAdmissionService(repository, root).recover()[0]
    members = repository.list_admission_members(rolled_back.journal_id)
    repository._connection.execute(statement, (value,))
    with pytest.raises((AdmissionError, ConflictError)):
        repository.start_admission(
            profile_id="profile",
            bucket="QUEUE",
            bundle_id="hello",
            fingerprint=fingerprint,
            source_path="DRAFTS",
            destination_path="QUEUE/hello",
            intent_id=intent_id,
        )
    assert repository.get_admission(rolled_back.journal_id).phase == "rolled_back"
    assert repository.list_admission_members(rolled_back.journal_id) == members
    assert (root / "DRAFTS/hello.jpg").read_bytes() == b"image"


def test_admission_retry_reset_is_atomic_and_restores_member_delete_protection(
    tmp_path: Path,
) -> None:
    import sqlite3

    root, fingerprint = _draft(tmp_path)
    repository = _repository(tmp_path)
    intent_id = _intent(repository, fingerprint)
    _crash_admission(repository, root, fingerprint, intent_id, "before_ready_install")
    rolled_back = DraftAdmissionService(repository, root).recover()[0]
    members = repository.list_admission_members(rolled_back.journal_id)
    repository._connection.execute(
        "CREATE TEMP TRIGGER fail_admission_retry BEFORE INSERT ON admission_journals "
        "BEGIN SELECT RAISE(ABORT, 'simulated reset interruption'); END"
    )
    with pytest.raises(ConflictError):
        DraftAdmissionService(repository, root).admit(
            profile_id="profile",
            bucket="QUEUE",
            bundle_id="hello",
            expected_fingerprint=fingerprint,
            intent_id=intent_id,
        )
    assert repository.get_admission(rolled_back.journal_id) == rolled_back
    assert repository.list_admission_members(rolled_back.journal_id) == members
    with pytest.raises(sqlite3.IntegrityError, match="immutable admission member"):
        repository._connection.execute(
            "DELETE FROM admission_members WHERE journal_id = ?",
            (rolled_back.journal_id,),
        )
    with StateRepository.open_existing(repository.path) as fresh:
        assert fresh.get_admission(rolled_back.journal_id) == rolled_back
    assert (root / "DRAFTS/hello.jpg").read_bytes() == b"image"


@pytest.mark.parametrize(
    "changed",
    [
        {"profile_id": "other"},
        {"bucket": "RANDOM"},
        {"bundle_id": "other"},
        {"fingerprint": "b" * 64},
        {"source_path": "DRAFTS/other"},
        {"destination_path": "QUEUE/other"},
        {"intent_id": None},
    ],
)
def test_admission_retry_cannot_change_approved_source_or_destination_binding(
    tmp_path: Path,
    changed: dict[str, object],
) -> None:
    root, fingerprint = _draft(tmp_path)
    repository = _repository(tmp_path)
    repository.register_profile(
        "other", tmp_path / "other-account", (), config_hash="a" * 64
    )
    intent_id = _intent(repository, fingerprint)
    _crash_admission(repository, root, fingerprint, intent_id, "before_ready_install")
    rolled_back = DraftAdmissionService(repository, root).recover()[0]
    members = repository.list_admission_members(rolled_back.journal_id)
    arguments = {
        "profile_id": "profile",
        "bucket": "QUEUE",
        "bundle_id": "hello",
        "fingerprint": fingerprint,
        "source_path": "DRAFTS",
        "destination_path": "QUEUE/hello",
        "intent_id": intent_id,
    }
    with pytest.raises(ConflictError):
        repository.start_admission(**{**arguments, **changed})
    assert repository.get_admission(rolled_back.journal_id) == rolled_back
    assert repository.list_admission_members(rolled_back.journal_id) == members
