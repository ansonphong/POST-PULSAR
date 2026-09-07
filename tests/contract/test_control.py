"""Socket-free contract tests for the loopback control boundary."""

from __future__ import annotations

import io
import json
from pathlib import Path

import pytest
from jsonschema import Draft202012Validator
from openapi_spec_validator import validate

from post_pulsar.control import (
    ControlApplication,
    ControlRequest,
    initialize_operator_secret,
    rotate_agent_capability,
)
from post_pulsar.control_identity import DaemonIdentity, DiscoveryPaths
from post_pulsar.state import ProfileTargetSnapshot, StateRepository


@pytest.mark.parametrize(
    "change", [None, "disabled", "bytes", "missing", "collision", "symlink"]
)
def test_disk_enqueue_confirmation_is_exact_and_opted_in(tmp_path: Path, change):
    from PIL import Image

    from post_pulsar.content import scan_account_root

    app = _application(tmp_path, allow_publish=change != "disabled")
    token = (tmp_path / "agent").read_text().strip()
    directory = tmp_path / "account/QUEUE/post"
    directory.mkdir(parents=True)
    Image.new("RGB", (4, 4)).save(directory / "post.jpg")
    (directory / ".ready").write_bytes(b"")
    fingerprint = scan_account_root(tmp_path / "account").bundles[0].fingerprint
    body = dict(
        action="enqueue",
        profile_id="profile",
        resource_revision=1,
        consequence="Exact fixture",
        fingerprint=fingerprint,
        arguments=dict(
            bucket="QUEUE", bundle_id="post", fingerprint=fingerprint, trigger_id="disk"
        ),
    )
    document = json.loads(
        (Path(__file__).parents[2] / "api/control-v1.openapi.json").read_text()
    )
    schema = document["components"]["schemas"]["Confirmation"]
    Draft202012Validator(schema).validate(body)
    assert not Draft202012Validator(schema).is_valid({**body, "action": "run_now"})
    if change == "bytes":
        (directory / "post.txt").write_text("changed")
    elif change == "missing":
        (directory / ".ready").unlink()
    elif change == "collision":
        other = tmp_path / "account/RANDOM/post"
        other.mkdir(parents=True)
        Image.new("RGB", (4, 4)).save(other / "post.jpg")
        (other / ".ready").write_bytes(b"")
    elif change == "symlink":
        (directory / ".ready").unlink()
        (directory / ".ready").symlink_to(directory / "post.jpg")
    response = _call(
        app,
        "POST",
        "/control/v1/confirmations",
        token=token,
        body=body,
        headers={"Idempotency-Key": "disk", "If-Match": "1"},
    )
    assert response.status == (
        201 if change is None else 403 if change == "disabled" else 409
    ), response.body
    with StateRepository.open_existing(tmp_path / "state.sqlite3") as repository:
        assert repository.claim_next_run_request("worker") is None


@pytest.mark.parametrize(
    "path,arguments",
    [
        ("pause", {"unexpected": True}),
        ("schedules", {}),
        (
            "schedules",
            {
                "schedule_id": "bad",
                "bucket": "QUEUE",
                "timezone": "UTC",
                "weekdays": [True],
                "local_time": "12:00",
                "misfire_grace_seconds": 60,
                "enabled": False,
            },
        ),
    ],
)
def test_malformed_action_rejected_before_durable_admission(
    tmp_path: Path, path: str, arguments: dict
) -> None:
    app = _application(tmp_path)
    token = (tmp_path / "agent").read_text().strip()
    response = _call(
        app,
        "POST",
        "/control/v1/" + path,
        token=token,
        body={"profile_id": "profile", "arguments": arguments},
        headers={"Idempotency-Key": "malformed", "If-Match": "1"},
    )
    assert response.status == 422
    with StateRepository.open_existing(tmp_path / "state.sqlite3") as repository:
        assert repository.claim_next_run_request("worker") is None


@pytest.mark.parametrize(
    "action",
    [
        "admit_draft",
        "enqueue",
        "run_now",
        "publish_now",
        "pause",
        "resume",
        "schedule_create",
        "schedule_update",
        "schedule_enable",
        "schedule_disable",
        "cancel",
        "delete",
        "retry",
        "reconcile",
        "edit_caption",
        "edit_alt",
    ],
)
def test_every_action_has_matching_schema_and_durable_admission(
    tmp_path: Path, action: str
) -> None:
    from PIL import Image

    from post_pulsar.state import (
        REQUEST_ARGUMENT_SCHEMAS,
        BundleFileSnapshot,
        TargetSnapshot,
    )

    app = _application(tmp_path, allow_publish=True)
    token = (tmp_path / "agent").read_text().strip()
    values = {
        "bucket": "QUEUE",
        "bundle_id": "post",
        "fingerprint": "b" * 64,
        "trigger_id": "exact",
        "platform": "x",
        "published_remote_id": "remote",
        "schedule_id": "morning",
        "timezone": "UTC",
        "weekdays": [0, 2],
        "local_time": "12:00",
        "misfire_grace_seconds": 60,
        "enabled": False,
        "text": "caption",
    }
    arguments = {
        field: values[field] for field in REQUEST_ARGUMENT_SCHEMAS[action]["required"]
    }
    body = {"profile_id": "profile", "arguments": arguments}
    with StateRepository.open_existing(tmp_path / "state.sqlite3") as repository:
        if action in {
            "enqueue",
            "run_now",
            "publish_now",
            "cancel",
            "delete",
            "retry",
            "reconcile",
        }:
            key = repository.add_bundle(
                profile_id="profile",
                bundle_id="post",
                fingerprint="b" * 64,
                source_bucket="QUEUE",
                files=(
                    BundleFileSnapshot(
                        "post.jpg", "image", None, "image", "image/jpeg", 1, "a" * 64
                    ),
                ),
                targets=(
                    TargetSnapshot("x", "1", "profile", "TOKEN_REFERENCE", "2", 1, {}),
                ),
            )
            body.update(bundle_key=key, fingerprint="b" * 64)
        if action in {"schedule_update", "schedule_enable", "schedule_disable"}:
            schedule = repository.create_schedule(
                profile_id="profile",
                schedule_id="morning",
                bucket="QUEUE",
                timezone="UTC",
                weekdays=[0],
                local_time="12:00",
                misfire_grace_seconds=60,
                enabled=False,
            )
            body.update(schedule_key=schedule.schedule_key)
            if action == "schedule_enable":
                body["fingerprint"] = schedule.config_hash
    direct = {
        "pause": ("POST", "/pause"),
        "resume": ("POST", "/resume"),
        "schedule_create": ("POST", "/schedules"),
        "schedule_update": ("PATCH", "/schedules/1"),
        "schedule_disable": ("POST", "/schedules/1/disable"),
    }
    if action in {"edit_caption", "edit_alt"}:
        drafts = tmp_path / "account/DRAFTS"
        drafts.mkdir(parents=True)
        Image.new("RGB", (8, 8)).save(drafts / "post.jpg")
        (drafts / "post.txt").write_text("original")
        body = {"text": "caption"}
        method, route = (
            "PATCH",
            "/profiles/profile/buckets/DRAFTS/bundles/post/"
            + ("caption" if action == "edit_caption" else "alt"),
        )
    elif action in direct:
        method, route = direct[action]
    else:
        method, route = "POST", "/confirmations"
        body.update(
            action=action, resource_revision=1, consequence="Authorize exact action."
        )
        if action == "admit_draft":
            body["fingerprint"] = "b" * 64
        document = json.loads(
            (Path(__file__).parents[2] / "api/control-v1.openapi.json").read_text()
        )
        Draft202012Validator(
            document["components"]["schemas"]["Confirmation"]
        ).validate(body)
    malformed = json.loads(json.dumps(body))
    if "arguments" in malformed:
        malformed["arguments"]["unexpected"] = True
    else:
        malformed["text"] = 1
    rejected = _call(
        app,
        method,
        "/control/v1" + route,
        token=token,
        body=malformed,
        headers={"Idempotency-Key": "invalid", "If-Match": "1"},
    )
    assert rejected.status == 422
    response = _call(
        app,
        method,
        "/control/v1" + route,
        token=token,
        body=body,
        headers={"Idempotency-Key": "valid", "If-Match": "1"},
    )
    assert response.status == (201 if route == "/confirmations" else 202), response.body
    if route == "/confirmations":
        intent = json.loads(response.body)["data"]
        with StateRepository.open_existing(tmp_path / "state.sqlite3") as repository:
            repository.approve_confirmation_intent(
                intent["intent_id"], expected_revision=1
            )
        del body["consequence"]
        response = _call(
            app,
            "POST",
            f"/control/v1/confirmations/{intent['intent_id']}/consume",
            token=token,
            body=body,
            headers={"Idempotency-Key": "consume", "If-Match": "1"},
        )
        assert response.status == 202, response.body
    assert json.loads(response.body)["data"]["action"] == action


def _application(
    tmp_path: Path, *, allow_publish: bool = False, stopped: list[bool] | None = None
) -> ControlApplication:
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
    app = ControlApplication(
        database, capability_file, allow_agent_publish=allow_publish
    )
    destination = [] if stopped is None else stopped
    app.bind_daemon(
        lambda: destination.append(True),
        DaemonIdentity(
            "a" * 32,
            1234,
            123456,
            "b" * 64,
            DiscoveryPaths.from_paths(
                tmp_path / "bootstrap", tmp_path / "endpoint", capability_file
            ),
        ),
    )
    return app


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


def test_pause_acceptance_persists_gate_before_response(tmp_path: Path) -> None:
    app = _application(tmp_path)
    fixture_capability = (tmp_path / "agent").read_text().strip()
    response = _call(
        app,
        "POST",
        "/control/v1/pause",
        token=fixture_capability,
        body={"profile_id": "profile", "arguments": {}},
        headers={"Idempotency-Key": "fixture-pause", "If-Match": "1"},
    )
    assert response.status == 202
    with StateRepository.open_existing(tmp_path / "state.sqlite3") as repository:
        assert repository.get_pause_state().pause_requested
        assert not repository.get_pause_state().paused


def test_shutdown_requires_authentication_and_exact_incarnation(tmp_path: Path) -> None:
    stopped: list[bool] = []
    app = _application(tmp_path, stopped=stopped)
    fixture_capability = (tmp_path / "agent").read_text().strip()
    assert (
        _call(
            app,
            "POST",
            "/control/v1/shutdown",
            body={"startup_nonce": "b" * 64},
        ).status
        == 401
    )
    assert (
        _call(
            app,
            "POST",
            "/control/v1/shutdown",
            token=fixture_capability,
            body={"startup_nonce": "fixture-stale-nonce"},
        ).status
        == 409
    )
    assert stopped == []
    assert (
        _call(
            app,
            "POST",
            "/control/v1/shutdown",
            token=fixture_capability,
            body={"startup_nonce": "b" * 64},
        ).status
        == 202
    )
    assert stopped == [True]


def test_shutdown_contract_and_capability_route_parity(tmp_path: Path) -> None:
    from post_pulsar.control import SUPPORTED_OPERATIONS

    document = json.loads(
        (Path(__file__).parents[2] / "api/control-v1.openapi.json").read_text()
    )
    operations = set()
    for item in document["paths"].values():
        if "$ref" in item:
            item = document["components"]["pathItems"][item["$ref"].rsplit("/", 1)[1]]
        operations.update(
            operation["operationId"]
            for operation in item.values()
            if isinstance(operation, dict) and "operationId" in operation
        )
    assert set(SUPPORTED_OPERATIONS) == operations
    stopped: list[bool] = []
    app = _application(tmp_path, stopped=stopped)
    token = (tmp_path / "agent").read_text().strip()
    capabilities = json.loads(
        _call(app, "GET", "/control/v1/capabilities", token=token).body
    )["data"]
    assert set(capabilities["operations"]) == operations
    operation = document["paths"]["/shutdown"]["post"]
    request_schema = operation["requestBody"]["content"]["application/json"]["schema"]
    response_schema = operation["responses"]["202"]["content"]["application/json"][
        "schema"
    ]
    body = {"startup_nonce": "b" * 64}
    Draft202012Validator(request_schema).validate(body)
    for invalid in (
        {},
        {"startup_nonce": ""},
        {"startup_nonce": 1},
        {**body, "extra": True},
    ):
        assert not Draft202012Validator(request_schema).is_valid(invalid)
    response = _call(app, "POST", "/control/v1/shutdown", token=token, body=body)
    assert response.status == 202 and stopped == [True]
    Draft202012Validator(response_schema).validate(json.loads(response.body))


def test_capabilities_and_endpoint_share_closed_identity_schemas(
    tmp_path: Path,
) -> None:
    from dataclasses import asdict

    from post_pulsar.daemon import EndpointRecord

    document = json.loads(
        (Path(__file__).parents[2] / "api/control-v1.openapi.json").read_text()
    )
    app = _application(tmp_path)
    token = (tmp_path / "agent").read_text().strip()
    response = json.loads(
        _call(app, "GET", "/control/v1/capabilities", token=token).body
    )
    schema = document["paths"]["/capabilities"]["get"]["responses"]["200"]["content"][
        "application/json"
    ]["schema"]
    validator = Draft202012Validator({**schema, "components": document["components"]})
    validator.validate(response)
    data = response["data"]
    endpoint = EndpointRecord(
        "post-pulsar.control/v1",
        "127.0.0.1:8765",
        data["pid"],
        data["process_started_at"],
        data["startup_nonce"],
        {"control_api_major": 1},
        data["installation_id"],
        DiscoveryPaths.read(data["discovery"]),
    )
    endpoint_validator = Draft202012Validator(
        {
            "$ref": "#/components/schemas/EndpointRecord",
            "components": document["components"],
        }
    )
    endpoint_validator.validate(asdict(endpoint))
    for name, value in (
        ("DaemonCapabilities", data),
        ("EndpointRecord", asdict(endpoint)),
    ):
        exact = Draft202012Validator(
            {
                "$ref": "#/components/schemas/" + name,
                "components": document["components"],
            }
        )
        for key in value:
            assert not exact.is_valid(
                {field: content for field, content in value.items() if field != key}
            )
        assert not exact.is_valid({**value, "token": "forbidden-fixture-value"})
        for key, wrong in (
            ("installation_id", "A" * 32),
            ("pid", True),
            ("process_started_at", 0),
            ("startup_nonce", "b" * 63),
            ("discovery", {**data["discovery"], "extra": "forbidden"}),
        ):
            assert not exact.is_valid({**value, key: wrong})
    for name in ("DiscoveryPaths", "DaemonCapabilities"):
        example_validator = Draft202012Validator(
            {
                "$ref": "#/components/schemas/" + name,
                "components": document["components"],
            }
        )
        for example in document["components"]["schemas"][name]["examples"]:
            example_validator.validate(example)


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
            "arguments": {
                "bucket": "QUEUE",
                "bundle_id": "post",
                "fingerprint": "a" * 64,
                "trigger_id": "exact",
            },
            "consequence": "Publish exact content.",
        },
        headers={"Idempotency-Key": "intent-1", "If-Match": '"1"'},
    )
    assert blocked.status == 403


def test_operator_can_originate_and_consume_intent_when_agent_publish_is_off(
    tmp_path: Path,
) -> None:
    class TTY(io.StringIO):
        def isatty(self) -> bool:
            return True

    app = _application(tmp_path)
    verifier = tmp_path / "operator"
    initialize_operator_secret(verifier, input_stream=TTY("operator secret phrase\n"))
    app = ControlApplication(
        tmp_path / "state.sqlite3",
        tmp_path / "agent",
        operator_verifier_file=verifier,
        allow_agent_publish=False,
    )
    token = (tmp_path / "agent").read_text(encoding="ascii").strip()
    headers = {
        "Idempotency-Key": "operator-intent",
        "If-Match": '"1"',
        "X-Post-Pulsar-Principal": "operator",
        "X-Post-Pulsar-Operator-Secret": "operator secret phrase",
    }
    created = _call(
        app,
        "POST",
        "/control/v1/operator/confirmations",
        token=token,
        headers=headers,
        body={
            "action": "admit_draft",
            "profile_id": "profile",
            "resource_revision": 1,
            "fingerprint": "a" * 64,
            "consequence": "Publish exact content.",
            "arguments": {"bucket": "QUEUE", "bundle_id": "draft"},
        },
    )
    assert created.status == 201
    document = json.loads(created.body)["data"]
    assert document["origin"] == "operator"
    approved = _call(
        app,
        "POST",
        f"/control/v1/operator/confirmations/{document['intent_id']}/approve",
        token=token,
        headers={**headers, "Idempotency-Key": "operator-approve"},
    )
    assert approved.status == 200
    consumed = _call(
        app,
        "POST",
        f"/control/v1/confirmations/{document['intent_id']}/consume",
        token=token,
        headers={"Idempotency-Key": "operator-consume", "If-Match": '"1"'},
        body={
            "action": "admit_draft",
            "profile_id": "profile",
            "resource_revision": 1,
            "fingerprint": "a" * 64,
            "arguments": {"bucket": "QUEUE", "bundle_id": "draft"},
        },
    )
    assert consumed.status == 202


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


def test_status_accepts_exact_profile_and_bundle_filters(tmp_path: Path) -> None:
    app = _application(tmp_path)
    token = (tmp_path / "agent").read_text(encoding="ascii").strip()
    response = _call(app, "GET", "/control/v1/status?profile_id=profile", token=token)
    assert response.status == 200
    assert json.loads(response.body)["data"] == {"profile_id": "profile", "bundles": []}
    invalid = _call(app, "GET", "/control/v1/status?bundle_key=1", token=token)
    assert invalid.status == 422


def test_profile_cursor_schema_matches_page_two_behavior(tmp_path: Path) -> None:
    path = Path(__file__).parents[2] / "api/control-v1.openapi.json"
    document = json.loads(path.read_text(encoding="utf-8"))
    parameters = document["components"]["parameters"]
    profile_cursor = parameters["ProfileCursor"]["schema"]
    assert profile_cursor == {
        "type": "string",
        "pattern": "^[a-z0-9][a-z0-9-]{0,31}$",
    }
    assert document["paths"]["/profiles"]["get"]["parameters"][0] == {
        "$ref": "#/components/parameters/ProfileCursor"
    }
    assert parameters["Cursor"]["schema"] == {"type": "integer", "minimum": 0}
    for route in ("/schedules", "/requests"):
        refs = {item["$ref"] for item in document["paths"][route]["get"]["parameters"]}
        assert "#/components/parameters/Cursor" in refs
        assert "#/components/parameters/ProfileCursor" not in refs

    app = _application(tmp_path)
    token = (tmp_path / "agent").read_text(encoding="ascii").strip()
    with StateRepository.open_existing(tmp_path / "state.sqlite3") as repository:
        repository.register_profile(
            "second-profile",
            tmp_path / "second-account",
            (
                ProfileTargetSnapshot(
                    "x", "2", "second-profile", "SECOND_TOKEN_REFERENCE", {}
                ),
            ),
            config_hash="b" * 64,
        )

    first = _call(app, "GET", "/control/v1/profiles?limit=1", token=token)
    first_page = json.loads(first.body)["data"]
    assert first.status == 200
    assert [item["profile_id"] for item in first_page["items"]] == ["profile"]
    assert first_page["next_cursor"] == "profile"

    second = _call(
        app,
        "GET",
        "/control/v1/profiles?cursor=profile&limit=1",
        token=token,
    )
    second_page = json.loads(second.body)["data"]
    assert second.status == 200
    assert [item["profile_id"] for item in second_page["items"]] == ["second-profile"]
    assert second_page["next_cursor"] == "second-profile"


def test_openapi_is_authoritative_and_has_every_operation() -> None:
    path = Path(__file__).parents[2] / "api/control-v1.openapi.json"
    document = json.loads(path.read_text(encoding="utf-8"))
    assert document["openapi"] == "3.1.0"
    path_items = []
    for path_item in document["paths"].values():
        if "$ref" in path_item:
            path_item = document["components"]["pathItems"][
                path_item["$ref"].rsplit("/", 1)[1]
            ]
        path_items.append(path_item)
    operation_ids = {
        operation["operationId"]
        for path_item in path_items
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
    validate(document)
    schema = document["components"]["schemas"]["Confirmation"]
    Draft202012Validator(schema).validate(
        {
            "action": "run_now",
            "profile_id": "profile",
            "resource_revision": 1,
            "bundle_key": 1,
            "fingerprint": "a" * 64,
            "arguments": {
                "bucket": "QUEUE",
                "bundle_id": "post",
                "fingerprint": "a" * 64,
                "trigger_id": "exact",
            },
            "consequence": "Publish the exact bundle.",
        }
    )
