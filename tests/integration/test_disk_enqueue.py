"""Exact approved DRAFTS-to-enqueue workflow without pre-existing bundle rows."""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from PIL import Image
from tests.unit.test_app import NOW, _bundle, _factory, _initialize

from post_pulsar import cli
from post_pulsar.config import load_local_settings
from post_pulsar.content import scan_account_root, scan_inbox
from post_pulsar.control import (
    ControlApplication,
    ControlRequest,
    rotate_agent_capability,
)
from post_pulsar.daemon import ForegroundDaemon
from post_pulsar.state import StateRepository


@pytest.mark.parametrize("change", [None, "bytes", "missing", "collision", "profile"])
@pytest.mark.parametrize("bucket", ["QUEUE", "RANDOM"])
def test_approved_draft_to_exact_enqueue_and_replay(
    tmp_path: Path, change: str | None, bucket: str
):
    config, locks, instance = _initialize(tmp_path)
    settings = load_local_settings(config)
    database = tmp_path / "state/post_pulsar.sqlite3"
    root = settings.profile("operator").account_root
    drafts = root / "DRAFTS"
    drafts.mkdir()
    Image.new("RGB", (4, 4)).save(drafts / "zebra.jpg")
    (drafts / "zebra.txt").write_text("approved caption")
    fingerprint = scan_inbox(drafts).bundles[0].fingerprint
    capability = rotate_agent_capability(settings.app.agent_capability_file)
    control = ControlApplication(
        database, settings.app.agent_capability_file, allow_agent_publish=True
    )
    adapters = []
    daemon = ForegroundDaemon(
        settings.app.state_directory,
        settings.app.endpoint_record_file,
        "127.0.0.1",
        0,
        installation_id="a" * 32,
        bootstrap_record_file=settings.app.bootstrap_file,
        agent_capability_file=settings.app.agent_capability_file,
    )
    # Exercise the real daemon executor with its real leases, without a server/thread.
    daemon._locks, daemon._lease = locks, instance

    def call(route, body, key):
        return control.handle(
            ControlRequest(
                "POST",
                "/control/v1/" + route,
                {
                    "Authorization": "Bearer " + capability,
                    "Idempotency-Key": key,
                    "If-Match": "1",
                },
                json.dumps(body).encode(),
            )
        )

    def approve_and_consume(action, arguments, key):
        body = dict(
            action=action,
            profile_id="operator",
            resource_revision=1,
            fingerprint=fingerprint,
            arguments=arguments,
        )
        created = call("confirmations", {**body, "consequence": "Exact fixture"}, key)
        assert created.status == 201, created.body
        intent_id = json.loads(created.body)["data"]["intent_id"]
        assert (
            call(f"confirmations/{intent_id}/consume", body, key + "-run").status == 409
        )
        with StateRepository.open_existing(database) as repository:
            repository.approve_confirmation_intent(intent_id, expected_revision=1)
        consumed = call(f"confirmations/{intent_id}/consume", body, key + "-run")
        assert consumed.status == 202, consumed.body
        return intent_id, body, json.loads(consumed.body)["data"]["request_id"]

    def execute():
        with StateRepository.open_existing(database, clock=lambda: NOW) as repository:
            request = repository.claim_next_run_request("fixture-worker")
        assert request is not None
        outcome = cli._execute_daemon_request(
            settings,
            request,
            daemon=daemon,
            environ={"POST_PULSAR_X_OPERATOR_USER_ACCESS_TOKEN": "token-x"},
            adapter_factory=_factory(adapters),
            identity_verifier=lambda *_: None,
            clock=lambda: NOW,
        )
        with StateRepository.open_existing(database, clock=lambda: NOW) as repository:
            repository.complete_run_request(
                request.request_id, worker_token="fixture-worker", result=outcome
            )
        return outcome

    try:
        approve_and_consume(
            "admit_draft", {"bucket": bucket, "bundle_id": "zebra"}, "admit"
        )
        execute()
        assert (root / bucket / "zebra/.ready").is_file()
        with StateRepository.open_existing(database) as repository:
            assert repository.list_protected_bundles("operator") == ()
        _bundle(tmp_path, "aardvark", bucket)
        args = dict(
            bucket=bucket,
            bundle_id="zebra",
            fingerprint=fingerprint,
            trigger_id="enqueue-zebra",
        )
        intent_id, body, request_id = approve_and_consume("enqueue", args, "enqueue")
        if change == "bytes":
            (root / bucket / "zebra/zebra.txt").write_text("drift")
        elif change == "missing":
            (root / bucket / "zebra/.ready").unlink()
        elif change == "collision":
            _bundle(tmp_path, "zebra", "RANDOM" if bucket == "QUEUE" else "QUEUE")
        elif change == "profile":
            with StateRepository.open_existing(database) as repository:
                repository._connection.execute(
                    "UPDATE profiles SET revision = revision + 1"
                )
        outcome = execute()
        if change is None:
            assert outcome["status"] == "archived", outcome
            assert outcome["bundle_id"] == "zebra"
            assert sum(adapter.commits for adapter in adapters) == 1
            assert [item.bundle_id for item in scan_account_root(root).bundles] == [
                "aardvark"
            ]
        else:
            assert outcome["status"] == "invalid", outcome
            assert adapters == []
        replay = call(f"confirmations/{intent_id}/consume", body, "enqueue-run")
        assert replay.status == 202, replay.body
        replay_data = json.loads(replay.body)["data"]
        assert replay_data["request_id"] == request_id
        assert replay_data["result"] == outcome
        with StateRepository.open_existing(database) as repository:
            assert repository.claim_next_run_request("fixture-worker") is None
    finally:
        instance.release()
