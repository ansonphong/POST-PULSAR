"""Socket-free contract tests for the loopback control boundary."""

from __future__ import annotations

import json
from pathlib import Path

from post_pulsar.control import (
    ControlApplication,
    ControlRequest,
    rotate_agent_capability,
)
from post_pulsar.state import ProfileTargetSnapshot, StateRepository


def _application(tmp_path: Path, *, allow_publish: bool = False) -> ControlApplication:
    database = tmp_path / "state.sqlite3"
    repository = StateRepository(database)
    repository.register_profile(
        "profile",
        tmp_path / "account",
        (ProfileTargetSnapshot("x", "1", "profile", "TOKEN_REFERENCE", {}),),
        config_hash="a" * 64,
    )
    repository.close()
    capability_file = tmp_path / "agent"
    rotate_agent_capability(capability_file)
    return ControlApplication(
        database, capability_file, allow_agent_publish=allow_publish
    )


def _call(
    app: ControlApplication,
    method: str,
    path: str,
    *,
    token: str | None = None,
    body: object | None = None,
    headers: dict[str, str] | None = None,
):
    request_headers = dict(headers or {})
    if token is not None:
        request_headers["Authorization"] = f"Bearer {token}"
    payload = b"" if body is None else json.dumps(body).encode()
    return app.handle(ControlRequest(method, path, request_headers, payload))


def test_auth_cors_limits_version_and_capabilities(tmp_path: Path) -> None:
    app = _application(tmp_path)
    token = (tmp_path / "agent").read_text(encoding="ascii").strip()
    assert _call(app, "GET", "/control/v1/health").status == 401
    bad = _call(app, "GET", "/control/v1/health", token="0" * 64)
    assert bad.status == 401 and "Access-Control-Allow-Origin" not in bad.headers
    good = _call(app, "GET", "/control/v1/capabilities", token=token)
    document = json.loads(good.body)
    assert document["ok"] is True
    assert document["data"]["control_api_major"] == 1
    assert document["data"]["state_schema"] == 1
    assert (
        _call(
            app, "POST", "/control/v1/pause", token=token, body={"x": "y" * 70000}
        ).status
        == 413
    )


def test_writes_need_revision_idempotency_and_publish_permission(
    tmp_path: Path,
) -> None:
    app = _application(tmp_path)
    token = (tmp_path / "agent").read_text(encoding="ascii").strip()
    missing = _call(app, "POST", "/control/v1/pause", token=token, body={})
    assert missing.status == 428
    headers = {"Idempotency-Key": "pause-1", "If-Match": '"1"'}
    accepted = _call(
        app,
        "POST",
        "/control/v1/pause",
        token=token,
        body={"profile_id": "profile"},
        headers=headers,
    )
    assert accepted.status == 202
    replay = _call(
        app,
        "POST",
        "/control/v1/pause",
        token=token,
        body={"profile_id": "profile"},
        headers=headers,
    )
    assert (
        json.loads(replay.body)["data"]["request_id"]
        == json.loads(accepted.body)["data"]["request_id"]
    )
    blocked = _call(
        app,
        "POST",
        "/control/v1/confirmations",
        token=token,
        body={
            "action": "run_now",
            "profile_id": "profile",
            "resource_revision": 1,
            "arguments": {},
        },
        headers={"Idempotency-Key": "intent-1", "If-Match": '"1"'},
    )
    assert blocked.status == 403


def test_keyed_schedule_routes_execute_and_reject_path_body_drift(
    tmp_path: Path,
) -> None:
    app = _application(tmp_path)
    token = (tmp_path / "agent").read_text(encoding="ascii").strip()
    with StateRepository.open_existing(tmp_path / "state.sqlite3") as repository:
        schedule = repository.create_schedule(
            profile_id="profile",
            schedule_id="morning",
            bucket="QUEUE",
            timezone="UTC",
            weekdays=(0,),
            local_time="09:30",
            misfire_grace_seconds=60,
            enabled=False,
        )
    headers = {"Idempotency-Key": "schedule-patch", "If-Match": '"1"'}
    body = {
        "profile_id": "profile",
        "schedule_key": schedule.schedule_key,
        "arguments": {
            "schedule_id": "morning",
            "bucket": "QUEUE",
            "timezone": "UTC",
            "weekdays": [0, 2],
            "local_time": "09:45",
            "misfire_grace_seconds": 60,
            "enabled": False,
        },
    }

    accepted = _call(
        app,
        "PATCH",
        f"/control/v1/schedules/{schedule.schedule_key}",
        token=token,
        body=body,
        headers=headers,
    )
    drifted = _call(
        app,
        "POST",
        f"/control/v1/schedules/{schedule.schedule_key}/disable",
        token=token,
        body={"profile_id": "profile", "schedule_key": schedule.schedule_key + 1},
        headers={"Idempotency-Key": "schedule-disable", "If-Match": '"1"'},
    )

    assert accepted.status == 202
    assert json.loads(accepted.body)["data"]["action"] == "schedule_update"
    assert drifted.status == 422


def test_openapi_is_authoritative_and_has_every_operation() -> None:
    path = Path(__file__).parents[2] / "api/control-v1.openapi.json"
    document = json.loads(path.read_text(encoding="utf-8"))
    assert document["openapi"] == "3.1.0"
    operation_ids = {
        operation["operationId"]
        for path_item in document["paths"].values()
        for operation in path_item.values()
        if isinstance(operation, dict) and "operationId" in operation
    }
    assert {
        "health",
        "capabilities",
        "status",
        "dashboard",
        "profiles",
        "buckets",
        "inspectBundle",
        "previewBundle",
        "editCaption",
        "editAlt",
        "readyBundle",
        "enqueue",
        "schedules",
        "requests",
        "pause",
        "resume",
        "runNow",
        "cancelPending",
        "deletePending",
        "retry",
        "reconcile",
        "createConfirmation",
        "confirmationStatus",
        "consumeConfirmation",
        "operatorApprove",
    } <= operation_ids
