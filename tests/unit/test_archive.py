"""Crash-safe exact archival and ordered lease tests."""

from __future__ import annotations

import errno
import hashlib
import os
import shutil
from collections.abc import Callable
from pathlib import Path
from typing import Any, cast

import pytest
from filelock import FileLock

import post_pulsar.archive as archive_module
from post_pulsar.archive import (
    ArchiveFaultInjector,
    ArchiveManager,
    ArchiveSafetyError,
)
from post_pulsar.content import scan_account_root
from post_pulsar.locking import (
    LockLease,
    LockContentionError,
    LockManager,
    LockOrderError,
)
from post_pulsar.state import (
    BundleFileSnapshot,
    BundleRecord,
    ProfileTargetSnapshot,
    StateRepository,
    TargetSnapshot,
    TransitionError,
)


class InjectedCrash(RuntimeError):
    """Simulate abrupt process death at one named durable boundary."""


def _profile_target() -> ProfileTargetSnapshot:
    return ProfileTargetSnapshot(
        platform="x",
        expected_remote_user_id="10001",
        expected_username="operator",
        token_env_var="POST_PULSAR_X_OPERATOR_USER_ACCESS_TOKEN",
        request_settings={"timeout": 30, "chunk_size": 4_194_304},
    )


def _target() -> TargetSnapshot:
    return TargetSnapshot(
        platform="x",
        expected_remote_user_id="10001",
        expected_username="operator",
        token_env_var="POST_PULSAR_X_OPERATOR_USER_ACCESS_TOKEN",
        api_version="2",
        adapter_version=1,
        request_settings={"timeout": 30, "chunk_size": 4_194_304},
    )


def _sha(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _make_repository(
    root: Path, *, bundle_id: str = "post", publish: bool = True
) -> tuple[StateRepository, int, Path]:
    account = root / "accounts" / "operator"
    source = account / "QUEUE" / bundle_id
    source.mkdir(parents=True)
    caption = f"caption for {bundle_id}\n".encode()
    image = b"not-decoded-by-content-discovery"
    (source / f"{bundle_id}.txt").write_bytes(caption)
    (source / f"{bundle_id}.jpg").write_bytes(image)
    (source / ".ready").write_bytes(b"")
    scan = scan_account_root(account)
    assert not scan.issues
    admitted = scan.bundles[0]

    repository = StateRepository(root / "state" / "post_pulsar.sqlite3")
    repository.register_profile(
        "operator", account, (_profile_target(),), config_hash="1" * 64
    )
    bundle_key = repository.add_bundle(
        profile_id="operator",
        bundle_id=bundle_id,
        fingerprint=admitted.fingerprint,
        source_bucket="QUEUE",
        files=(
            BundleFileSnapshot(
                relative_name=f"{bundle_id}.txt",
                role="caption",
                ordinal=None,
                media_kind=None,
                mime_type="text/plain",
                size_bytes=len(caption),
                sha256=_sha(caption),
            ),
            BundleFileSnapshot(
                relative_name=f"{bundle_id}.jpg",
                role="image",
                ordinal=None,
                media_kind="image",
                mime_type="image/jpeg",
                size_bytes=len(image),
                sha256=_sha(image),
            ),
        ),
        targets=(_target(),),
    )
    if publish:
        claim = repository.claim_delivery(bundle_key, "x", "archive-test-claim")
        repository.advance_delivery_phase(
            bundle_key,
            "x",
            "final_dispatch_started",
            claim_token="archive-test-claim",
            attempt_count=claim.attempt_count,
        )
        repository.publish_delivery(
            bundle_key,
            "x",
            remote_id=f"remote-{bundle_id}",
            claim_token="archive-test-claim",
            attempt_count=claim.attempt_count,
        )
    return repository, bundle_key, source


def _archive(
    repository: StateRepository,
    bundle_key: int,
    root: Path,
    *,
    fault: ArchiveFaultInjector | None = None,
    replace: Callable[[Path, Path], None] = os.replace,
) -> BundleRecord:
    locks = LockManager(root / "state")
    with locks.acquire_instance() as instance:
        with locks.acquire_profiles(instance, ["operator"]) as profiles:
            return ArchiveManager(
                repository, locks, fault_injector=fault, replace=replace
            ).archive_bundle(
                bundle_key,
                instance_lease=instance,
                profile_lease=profiles,
            )


def _final(root: Path, bundle_id: str = "post") -> Path:
    return root / "accounts" / "operator" / "POSTED" / "QUEUE" / bundle_id


def test_archives_only_exact_container_after_all_targets_publish(
    tmp_path: Path,
) -> None:
    repository, bundle_key, source = _make_repository(tmp_path)
    unrelated = source.parent / "unrelated"
    unrelated.mkdir()
    (unrelated / "keep.txt").write_text("keep", encoding="utf-8")

    archived = _archive(repository, bundle_key, tmp_path)

    final = _final(tmp_path)
    assert archived.status == "archived"
    assert archived.archive_path == "POSTED/QUEUE/post"
    assert not source.exists()
    assert {entry.name for entry in final.iterdir()} == {
        "post.txt",
        "post.jpg",
        ".ready",
    }
    assert (unrelated / "keep.txt").read_text(encoding="utf-8") == "keep"
    assert set(repository.list_archive_checkpoints(bundle_key)) == {
        "post.txt",
        "post.jpg",
        ".ready",
    }


def test_refuses_to_mutate_filesystem_until_all_targets_are_published(
    tmp_path: Path,
) -> None:
    repository, bundle_key, source = _make_repository(tmp_path, publish=False)

    with pytest.raises(TransitionError, match="all target deliveries"):
        _archive(repository, bundle_key, tmp_path)

    assert source.exists()
    assert not _final(tmp_path).exists()
    assert repository.get_bundle(bundle_key).status == "active"


def test_ordered_leases_are_passed_already_held_and_released_lifo(
    tmp_path: Path,
) -> None:
    locks = LockManager(tmp_path / "state")
    instance = locks.acquire_instance()
    maintenance = locks.acquire_maintenance(instance)
    profiles = locks.acquire_profiles(
        instance, ["z-profile", "a-profile", "a-profile"], maintenance=maintenance
    )
    assert profiles.resources == ("a-profile", "z-profile")

    with pytest.raises(LockOrderError, match="reverse order"):
        instance.release()
    profiles.release()
    maintenance.release()
    instance.release()
    with pytest.raises(LockOrderError, match="released"):
        instance.__enter__()


def test_forged_and_mutated_leases_are_rejected(tmp_path: Path) -> None:
    locks = LockManager(tmp_path / "state")
    forged_instance = LockLease(locks, "instance", ("instance",), ())
    forged_profiles = LockLease(locks, "profiles", ("operator",), ())

    with pytest.raises(LockOrderError, match="already-held"):
        locks.require_instance(forged_instance)
    with pytest.raises(LockOrderError, match="already-held"):
        locks.require_profile(forged_profiles, "operator")

    with locks.acquire_instance() as instance:
        with locks.acquire_profiles(instance, ["operator"]) as profiles:
            object.__setattr__(profiles, "resources", ("operator", "other"))
            with pytest.raises(LockOrderError, match="mutated"):
                locks.require_profile(profiles, "operator")
            object.__setattr__(profiles, "resources", ("operator",))
            object.__setattr__(profiles, "active", False)
            with pytest.raises(LockOrderError, match="mutated"):
                profiles.release()
            object.__setattr__(profiles, "active", True)

    with pytest.raises(LockOrderError, match="already-held"):
        locks.require_instance(instance)


def test_lock_contention_fails_cleanly_before_archive_work(tmp_path: Path) -> None:
    repository, bundle_key, source = _make_repository(tmp_path)
    lock_path = tmp_path / "state" / "locks" / "instance.lock"
    blocker = FileLock(lock_path)
    blocker.acquire(timeout=0)
    try:
        with pytest.raises(LockContentionError, match="another POST PULSAR"):
            LockManager(tmp_path / "state").acquire_instance()
    finally:
        blocker.release()

    assert source.exists()
    assert repository.get_bundle(bundle_key).status == "active"


@pytest.mark.parametrize(
    "boundary",
    [
        "after_begin_archiving",
        "after_staging_directory",
        "after_member_move:post.txt",
        "after_member_checkpoint:post.txt",
        "after_member_move:post.jpg",
        "after_member_checkpoint:post.jpg",
        "after_member_move:.ready",
        "after_member_checkpoint:.ready",
        "after_directory_quarantine:post",
        "before_final_rename",
        "after_final_rename",
        "after_mark_archived",
    ],
)
def test_every_durable_boundary_recovers_after_process_restart(
    tmp_path: Path, boundary: str
) -> None:
    case_root = tmp_path / boundary.replace(":", "-")
    repository, bundle_key, _source = _make_repository(case_root)
    fired = False

    def crash(candidate: str) -> None:
        nonlocal fired
        if candidate == boundary and not fired:
            fired = True
            raise InjectedCrash(boundary)

    with pytest.raises(InjectedCrash, match=boundary):
        _archive(
            repository,
            bundle_key,
            case_root,
            fault=cast(ArchiveFaultInjector, crash),
        )
    repository.close()

    with StateRepository(case_root / "state" / "post_pulsar.sqlite3") as reopened:
        archived = _archive(reopened, bundle_key, case_root)
        assert archived.status == "archived"
        assert {item.name for item in _final(case_root).iterdir()} == {
            "post.txt",
            "post.jpg",
            ".ready",
        }


def test_recovers_staged_only_container(tmp_path: Path) -> None:
    repository, bundle_key, source = _make_repository(tmp_path)
    bundle = repository.get_bundle(bundle_key)
    repository.begin_archiving(bundle_key, expected_revision=bundle.revision)
    staging = (
        source.parents[1]
        / "POSTED"
        / "QUEUE"
        / (f".{bundle.bundle_id}.{bundle.fingerprint}.archiving")
    )
    staging.parent.mkdir(parents=True)
    source.rename(staging)

    assert _archive(repository, bundle_key, tmp_path).status == "archived"
    assert not staging.exists()
    assert _final(tmp_path).exists()


def test_crash_after_source_quarantine_recovers_without_data_loss(
    tmp_path: Path,
) -> None:
    repository, bundle_key, source = _make_repository(tmp_path)

    def crash(boundary: str) -> None:
        if boundary == "after_member_reservation:post.txt":
            raise InjectedCrash(boundary)

    with pytest.raises(InjectedCrash, match="after_member_reservation"):
        _archive(
            repository,
            bundle_key,
            tmp_path,
            fault=crash,
        )
    assert _archive(repository, bundle_key, tmp_path).status == "archived"
    assert repository.get_bundle(bundle_key).status == "archived"
    assert (_final(tmp_path) / "post.txt").exists()


@pytest.mark.parametrize("retain_source", [False, True])
def test_recovers_verified_final_only_or_both_identical(
    tmp_path: Path, retain_source: bool
) -> None:
    repository, bundle_key, source = _make_repository(tmp_path)
    bundle = repository.get_bundle(bundle_key)
    repository.begin_archiving(bundle_key, expected_revision=bundle.revision)
    final = _final(tmp_path)
    final.parent.mkdir(parents=True)
    shutil.copytree(source, final)
    if not retain_source:
        shutil.rmtree(source)

    archived = _archive(repository, bundle_key, tmp_path)

    assert archived.status == "archived"
    assert final.exists()
    assert not source.exists()


@pytest.mark.parametrize("destination_kind", ["different", "empty"])
def test_conflicting_destination_blocks_without_deleting_source(
    tmp_path: Path, destination_kind: str
) -> None:
    repository, bundle_key, source = _make_repository(tmp_path)
    bundle = repository.get_bundle(bundle_key)
    repository.begin_archiving(bundle_key, expected_revision=bundle.revision)
    final = _final(tmp_path)
    final.parent.mkdir(parents=True)
    if destination_kind == "different":
        shutil.copytree(source, final)
        (final / "post.txt").write_text("different", encoding="utf-8")
    else:
        final.mkdir()

    with pytest.raises(ArchiveSafetyError):
        _archive(repository, bundle_key, tmp_path)

    assert source.exists()
    assert final.exists()
    assert repository.get_bundle(bundle_key).status == "blocked"


def test_raced_empty_final_destination_is_never_overwritten(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    repository, bundle_key, source = _make_repository(tmp_path)
    real_install = archive_module._atomic_noreplace

    def race_install(
        source_parent: Any,
        source_name: str,
        destination_parent: Any,
        destination_name: str,
    ) -> None:
        if destination_name == "post":
            _final(tmp_path).mkdir()
        real_install(source_parent, source_name, destination_parent, destination_name)

    monkeypatch.setattr(archive_module, "_atomic_noreplace", race_install)

    with pytest.raises(ArchiveSafetyError):
        _archive(repository, bundle_key, tmp_path)

    assert _final(tmp_path).is_dir()
    assert tuple(_final(tmp_path).iterdir()) == ()
    assert source.exists() or any(
        path.name.endswith(".archiving") for path in _final(tmp_path).parent.iterdir()
    )
    assert repository.get_bundle(bundle_key).status == "blocked"


def test_exdev_copy_is_hash_verified_and_source_removed_last(tmp_path: Path) -> None:
    repository, bundle_key, source = _make_repository(tmp_path)

    def cross_device(_source: Path, _destination: Path) -> None:
        raise OSError(errno.EXDEV, "cross-device")

    archived = _archive(repository, bundle_key, tmp_path, replace=cross_device)

    assert archived.status == "archived"
    assert not source.exists()
    assert (_final(tmp_path) / "post.txt").read_text(encoding="utf-8")


@pytest.mark.parametrize(
    "boundary",
    [
        "after_exdev_temp_created:post.txt",
        "after_exdev_temp_fsync:post.txt",
        "after_exdev_temp_verify:post.txt",
        "after_exdev_install:post.txt",
        "after_exdev_install_fsync:post.txt",
        "after_exdev_staged_verify:post.txt",
        "after_exdev_source_remove:post.txt",
        "after_exdev_source_fsync:post.txt",
        "after_exdev_temp_remove:post.txt",
    ],
)
def test_exdev_boundary_crashes_converge_to_archived_or_blocked(
    tmp_path: Path, boundary: str
) -> None:
    repository, bundle_key, source = _make_repository(tmp_path)
    fired = False

    def cross_device(_source: Path, _destination: Path) -> None:
        raise OSError(errno.EXDEV, "cross-device")

    def crash(candidate: str) -> None:
        nonlocal fired
        if candidate == boundary and not fired:
            fired = True
            raise InjectedCrash(boundary)

    with pytest.raises(InjectedCrash, match=boundary):
        _archive(
            repository,
            bundle_key,
            tmp_path,
            fault=cast(ArchiveFaultInjector, crash),
            replace=cross_device,
        )

    try:
        recovered = _archive(repository, bundle_key, tmp_path, replace=cross_device)
    except ArchiveSafetyError:
        assert boundary == "after_exdev_temp_created:post.txt"
        assert repository.get_bundle(bundle_key).status == "blocked"
        assert (source / "post.txt").exists()
    else:
        assert recovered.status == "archived"


def test_exdev_verification_failure_retains_source(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    repository, bundle_key, source = _make_repository(tmp_path)
    real_link = os.link

    def cross_device(_source: Path, _destination: Path) -> None:
        raise OSError(errno.EXDEV, "cross-device")

    def corrupt_installed_copy(
        source_path: Path,
        destination_path: Path,
        *,
        follow_symlinks: bool = True,
    ) -> None:
        real_link(source_path, destination_path, follow_symlinks=follow_symlinks)
        destination_path.write_bytes(b"corrupt")

    monkeypatch.setattr(os, "link", corrupt_installed_copy)
    with pytest.raises(ArchiveSafetyError):
        _archive(repository, bundle_key, tmp_path, replace=cross_device)

    assert (source / "post.txt").exists()
    assert repository.get_bundle(bundle_key).status == "blocked"


def test_exdev_source_swap_after_copy_is_preserved_and_blocks(
    tmp_path: Path,
) -> None:
    repository, bundle_key, source = _make_repository(tmp_path)

    def cross_device(_source: Path, _destination: Path) -> None:
        raise OSError(errno.EXDEV, "cross-device")

    def swap(boundary: str) -> None:
        if boundary != "after_exdev_staged_verify:post.txt":
            return
        candidates = tuple(source.glob(".archive-source-*.quarantine"))
        assert len(candidates) == 1
        candidates[0].unlink()
        candidates[0].write_bytes(b"unrelated replacement")

    with pytest.raises(ArchiveSafetyError):
        _archive(repository, bundle_key, tmp_path, fault=swap, replace=cross_device)

    candidates = tuple(source.glob(".archive-source-*.quarantine"))
    assert len(candidates) == 1
    assert candidates[0].read_bytes() == b"unrelated replacement"
    assert repository.get_bundle(bundle_key).status == "blocked"


def test_symlink_member_blocks_and_never_moves_target(tmp_path: Path) -> None:
    repository, bundle_key, source = _make_repository(tmp_path)
    outside = tmp_path / "outside.txt"
    outside.write_text("outside", encoding="utf-8")
    (source / "post.txt").unlink()
    (source / "post.txt").symlink_to(outside)

    with pytest.raises(ArchiveSafetyError):
        _archive(repository, bundle_key, tmp_path)

    assert outside.read_text(encoding="utf-8") == "outside"
    assert (source / "post.txt").is_symlink()
    assert repository.get_bundle(bundle_key).status == "blocked"


def test_source_member_swap_is_quarantined_and_never_deleted(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    repository, bundle_key, source = _make_repository(tmp_path)
    real_move = archive_module._atomic_noreplace
    swapped = False

    def swap_before_quarantine(
        source_parent: Any,
        source_name: str,
        destination_parent: Any,
        destination_name: str,
    ) -> None:
        nonlocal swapped
        if source_name == "post.txt" and not swapped:
            (source_parent.path / source_name).unlink()
            (source_parent.path / source_name).write_bytes(b"unrelated replacement")
            swapped = True
        real_move(source_parent, source_name, destination_parent, destination_name)

    monkeypatch.setattr(archive_module, "_atomic_noreplace", swap_before_quarantine)

    with pytest.raises(ArchiveSafetyError):
        _archive(repository, bundle_key, tmp_path)

    quarantined = tuple(source.glob(".archive-source-*.quarantine"))
    assert len(quarantined) == 1
    assert quarantined[0].read_bytes() == b"unrelated replacement"
    assert repository.get_bundle(bundle_key).status == "blocked"


def test_symlinked_posted_parent_is_rejected_before_external_mutation(
    tmp_path: Path,
) -> None:
    repository, bundle_key, source = _make_repository(tmp_path)
    account = source.parents[1]
    outside = tmp_path / "outside-posted"
    outside.mkdir()
    (account / "POSTED").symlink_to(outside, target_is_directory=True)

    with pytest.raises(ArchiveSafetyError, match="unsafe directory"):
        _archive(repository, bundle_key, tmp_path)

    assert tuple(outside.iterdir()) == ()
    assert source.exists()


@pytest.mark.parametrize("swap_level", ["root", "bucket"])
def test_directory_replacement_after_guard_snapshot_blocks_without_escape(
    tmp_path: Path,
    swap_level: str,
) -> None:
    repository, bundle_key, source = _make_repository(tmp_path)
    account = source.parents[1]
    bucket = source.parent
    outside = tmp_path / f"outside-{swap_level}"
    outside.mkdir()
    (outside / "keep.txt").write_text("keep", encoding="utf-8")

    def swap(boundary: str) -> None:
        if boundary != "after_archive_guards":
            return
        if swap_level == "root":
            moved = tmp_path / "moved-account"
            account.rename(moved)
            account.symlink_to(outside, target_is_directory=True)
        else:
            moved = account / "moved-queue"
            bucket.rename(moved)
            bucket.symlink_to(outside, target_is_directory=True)

    with pytest.raises(ArchiveSafetyError):
        _archive(repository, bundle_key, tmp_path, fault=swap)

    assert (outside / "keep.txt").read_text(encoding="utf-8") == "keep"
    assert repository.get_bundle(bundle_key).status == "blocked"


def test_conflicting_partial_staging_never_deletes_source(tmp_path: Path) -> None:
    repository, bundle_key, source = _make_repository(tmp_path)
    bundle = repository.get_bundle(bundle_key)
    repository.begin_archiving(bundle_key, expected_revision=bundle.revision)
    staging = (
        source.parents[1]
        / "POSTED"
        / "QUEUE"
        / (f".{bundle.bundle_id}.{bundle.fingerprint}.archiving")
    )
    staging.mkdir(parents=True)
    (staging / "post.txt").write_text("conflict", encoding="utf-8")

    with pytest.raises(ArchiveSafetyError):
        _archive(repository, bundle_key, tmp_path)

    assert (source / "post.txt").exists()
    assert (staging / "post.txt").exists()


def test_redundant_source_swap_is_preserved_in_delete_quarantine(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    repository, bundle_key, source = _make_repository(tmp_path)
    bundle = repository.get_bundle(bundle_key)
    repository.begin_archiving(bundle_key, expected_revision=bundle.revision)
    final = _final(tmp_path)
    final.parent.mkdir(parents=True)
    shutil.copytree(source, final)
    real_move = archive_module._atomic_noreplace
    swapped = False

    def swap_before_delete(
        source_parent: Any,
        source_name: str,
        destination_parent: Any,
        destination_name: str,
    ) -> None:
        nonlocal swapped
        if (
            source_parent.path == source
            and source_name == "post.txt"
            and destination_name.startswith(".archive-delete-")
            and not swapped
        ):
            (source_parent.path / source_name).unlink()
            (source_parent.path / source_name).write_bytes(b"unrelated replacement")
            swapped = True
        real_move(source_parent, source_name, destination_parent, destination_name)

    monkeypatch.setattr(archive_module, "_atomic_noreplace", swap_before_delete)

    with pytest.raises(ArchiveSafetyError):
        _archive(repository, bundle_key, tmp_path)

    quarantined = tuple(source.glob(".archive-delete-*.quarantine"))
    assert len(quarantined) == 1
    assert quarantined[0].read_bytes() == b"unrelated replacement"
    assert repository.get_bundle(bundle_key).status == "blocked"


def test_unexpected_member_blocks_without_using_prefix_or_glob_moves(
    tmp_path: Path,
) -> None:
    repository, bundle_key, source = _make_repository(tmp_path)
    (source / "catalog.txt").write_text("not admitted", encoding="utf-8")

    with pytest.raises(ArchiveSafetyError, match="unexpected members"):
        _archive(repository, bundle_key, tmp_path)

    assert (source / "catalog.txt").exists()
    assert not _final(tmp_path).exists()


def test_wrong_profile_lease_is_rejected_before_filesystem_work(tmp_path: Path) -> None:
    repository, bundle_key, source = _make_repository(tmp_path)
    locks = LockManager(tmp_path / "state")
    with locks.acquire_instance() as instance:
        with locks.acquire_profiles(instance, ["other-profile"]) as profiles:
            with pytest.raises(LockOrderError, match="does not own"):
                ArchiveManager(repository, locks).archive_bundle(
                    bundle_key,
                    instance_lease=instance,
                    profile_lease=profiles,
                )
    assert source.exists()
