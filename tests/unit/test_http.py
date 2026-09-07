"""Sanitized platform adapter and HTTP transport contract tests."""

from __future__ import annotations

import httpx
import pytest
import respx
from pytest_socket import SocketBlockedError

from post_pulsar.config import PublishingCredentials, SecretValue
from post_pulsar.media import PreparedMedia
from post_pulsar.platforms.base import (
    AdapterContractError,
    ArtifactCheckpoint,
    BasePlatformAdapter,
    CheckpointWriter,
    PreFinalPhase,
    PreparedPublication,
    PublicationRequest,
    PublicationSnapshot,
    PublishResult,
    RemoteIdentity,
    ValidationIssue,
)
from post_pulsar.platforms.http import (
    HTTPPolicy,
    PlatformHTTPClient,
    PlatformHTTPError,
    create_target_http_client,
)
from post_pulsar.state import ArtifactRecord, DeliveryRecord, TargetSnapshot


def _target(remote_id: str = "10001", username: str = "ansonphong") -> TargetSnapshot:
    return TargetSnapshot(
        platform="x",
        expected_remote_user_id=remote_id,
        expected_username=username,
        token_env_var=f"POST_PULSAR_X_{username.upper()}_USER_ACCESS_TOKEN",
        api_version="2",
        adapter_version=1,
        request_settings={"timeout": 30},
    )


def _snapshot(
    profile_id: str = "ansonphong",
    remote_id: str = "10001",
) -> PublicationSnapshot:
    return PublicationSnapshot(
        profile_id=profile_id,
        bundle_key=7,
        bundle_id="post",
        bundle_fingerprint="b" * 64,
        source_bucket="QUEUE",
        target=_target(remote_id, profile_id),
    )


def _request(snapshot: PublicationSnapshot | None = None) -> PublicationRequest:
    bound = snapshot or _snapshot()
    media = PreparedMedia(
        profile_id=bound.profile_id,
        source_bucket=bound.source_bucket,
        bundle_id=bound.bundle_id,
        bundle_fingerprint=bound.bundle_fingerprint,
        targets=(bound.target.platform,),
        items=(),
        warnings=(),
    )
    return PublicationRequest(snapshot=bound, media=media, text="hello", alt_text=None)


def _delivery(phase: str) -> DeliveryRecord:
    return DeliveryRecord(
        bundle_key=7,
        platform="x",
        status="in_flight",
        phase=phase,  # type: ignore[arg-type]
        attempt_count=1,
        consecutive_failures=0,
        safe_to_retry=True,
        next_attempt_at=None,
        remote_id=None,
        error_code=None,
        error_message=None,
        revision=2,
    )


class _Writer:
    def __init__(self) -> None:
        self.phases: list[str] = []
        self.artifacts: list[ArtifactRecord] = []

    def advance_phase(self, phase: PreFinalPhase) -> DeliveryRecord:
        self.phases.append(phase)
        return _delivery(phase)

    def checkpoint_artifact(self, checkpoint: ArtifactCheckpoint) -> ArtifactRecord:
        artifact = ArtifactRecord(
            bundle_key=7,
            platform="x",
            attempt_count=1,
            kind=checkpoint.kind,
            ordinal=checkpoint.ordinal,
            external_id=checkpoint.external_id,
            relative_path=checkpoint.relative_path,
            sha256=checkpoint.sha256,
            expires_at=checkpoint.expires_at,
            processing_metadata=checkpoint.processing_metadata,
        )
        self.artifacts.append(artifact)
        return artifact


class _Adapter(BasePlatformAdapter):
    def __init__(
        self,
        snapshot: PublicationSnapshot,
        client: PlatformHTTPClient,
        identity: RemoteIdentity,
    ) -> None:
        super().__init__(snapshot, client)
        self.client = client
        self.identity = identity
        self.mutations = 0

    def _verify_remote_identity(self) -> RemoteIdentity:
        return self.identity

    def _preflight(
        self, publication: PublicationRequest
    ) -> tuple[ValidationIssue, ...]:
        return ()

    def _prepare(
        self,
        publication: PublicationRequest,
        prior: PreparedPublication | None,
        checkpoints: CheckpointWriter,
    ) -> PreparedPublication:
        self.mutations += 1
        checkpoints.advance_phase("processing")
        artifact = checkpoints.checkpoint_artifact(
            ArtifactCheckpoint(kind="x_media_id", ordinal=0, external_id="media-1")
        )
        checkpoints.advance_phase("ready")
        return PreparedPublication(publication, 1, (artifact,))

    def _commit_once(self, prepared: PreparedPublication) -> PublishResult:
        response = self.client.final_request(
            "POST", "/publish", json_body={"media_id": "media-1"}
        )
        payload = response.json()
        assert isinstance(payload, dict)
        return PublishResult.published(str(payload["id"]))


class _RetryableAfterFinalAdapter(_Adapter):
    """Deliberately violate the final-result classification contract."""

    def _commit_once(self, prepared: PreparedPublication) -> PublishResult:
        del prepared
        self.client.final_request("POST", "/publish", json_body={"safe": True})
        return PublishResult.failed(
            "incorrect_retry",
            "Adapter incorrectly classified a final request as retryable.",
            retry_classification="safe_pre_final",
        )


def test_pre_final_retry_after_is_bounded_and_token_is_header_only() -> None:
    sleeps: list[float] = []
    token = "token-one-never-render"
    policy = HTTPPolicy(max_pre_final_attempts=2, max_retry_after_seconds=5)
    snapshot = _snapshot()
    with respx.mock(assert_all_called=True) as router:
        route = router.post("https://api.example.test/upload").mock(
            side_effect=[
                httpx.Response(429, headers={"Retry-After": "2"}),
                httpx.Response(200, json={"id": "media-1"}),
            ]
        )
        with PlatformHTTPClient(
            snapshot,
            SecretValue(token),
            base_url="https://api.example.test",
            policy=policy,
            sleeper=sleeps.append,
        ) as client:
            with pytest.raises(AdapterContractError, match="Authorization"):
                client.pre_final_request("POST", "/upload", json_body={"leak": token})
            response = client.pre_final_request(
                "POST", "/upload", json_body={"name": "safe"}
            )

    assert response.json() == {"id": "media-1"}
    assert sleeps == [2.0]
    assert route.call_count == 2
    for call in route.calls:
        request = call.request
        assert request.headers["Authorization"] == f"Bearer {token}"
        assert token not in str(request.url)
        assert token.encode() not in request.content


def test_read_only_retry_budget_caps_retry_after_and_aborts_before_repeat() -> None:
    remaining = 3.0
    sleeps: list[float] = []

    def sleep(seconds: float) -> None:
        nonlocal remaining
        sleeps.append(seconds)
        remaining -= seconds

    with respx.mock(assert_all_called=True) as router:
        route = router.get("https://api.example.test/status").mock(
            return_value=httpx.Response(429, headers={"Retry-After": "60"})
        )
        with (
            PlatformHTTPClient(
                _snapshot(),
                SecretValue("budget-token"),
                base_url="https://api.example.test",
                policy=HTTPPolicy(max_pre_final_attempts=3),
                sleeper=sleep,
            ) as client,
            pytest.raises(PlatformHTTPError, match="retry budget"),
        ):
            client.read_only_request(
                "GET", "/status", retry_budget_seconds=lambda: remaining
            )

    assert sleeps == [3.0]
    assert route.call_count == 1
    request_timeouts = route.calls[0].request.extensions["timeout"]
    assert isinstance(request_timeouts, dict)
    assert all(float(value) <= 3.0 for value in request_timeouts.values())


def test_final_dispatch_is_one_shot_and_uncertain_errors_are_sanitized() -> None:
    token = "final-token-never-render"
    with respx.mock(assert_all_called=True) as router:
        route = router.post("https://api.example.test/publish").mock(
            return_value=httpx.Response(
                503,
                headers={"X-Debug": token},
                content=f"raw body {token}".encode(),
            )
        )
        client = PlatformHTTPClient(
            _snapshot(), SecretValue(token), base_url="https://api.example.test"
        )
        with pytest.raises(PlatformHTTPError) as caught:
            client.final_request("POST", "/publish", json_body={"safe": True})
        with pytest.raises(AdapterContractError, match="already attempted"):
            client.final_request("POST", "/publish", json_body={"safe": True})
        client.close()

    assert route.call_count == 1
    assert caught.value.retry_classification == "ambiguous"
    assert caught.value.code == "final_dispatch_uncertain"
    assert token not in str(caught.value)
    assert token not in repr(caught.value)


def test_response_size_and_invalid_json_fail_with_fixed_diagnostics() -> None:
    policy = HTTPPolicy(max_response_bytes=8, max_pre_final_attempts=1)
    with respx.mock(assert_all_called=True) as router:
        router.get("https://api.example.test/large").mock(
            return_value=httpx.Response(200, content=b"private-response-body")
        )
        router.get("https://api.example.test/invalid").mock(
            return_value=httpx.Response(200, content=b"not-json")
        )
        with PlatformHTTPClient(
            _snapshot(),
            SecretValue("transport-token"),
            base_url="https://api.example.test",
            policy=policy,
        ) as client:
            with pytest.raises(PlatformHTTPError, match="response exceeded") as large:
                client.read_only_request("GET", "/large")
            response = client.read_only_request("GET", "/invalid")
            with pytest.raises(PlatformHTTPError, match="valid JSON") as invalid:
                response.json()

    assert large.value.code == "response_too_large"
    assert invalid.value.code == "invalid_json_response"
    assert "private-response-body" not in repr(large.value)
    assert "not-json" not in repr(invalid.value)


@pytest.mark.parametrize(
    "kwargs",
    [
        {"connect_timeout_seconds": 0},
        {"read_timeout_seconds": float("inf")},
        {"max_response_bytes": 0},
        {"max_pre_final_attempts": 0},
        {"max_retry_after_seconds": 301},
    ],
)
def test_http_policy_rejects_unbounded_values(kwargs: dict[str, object]) -> None:
    with pytest.raises(AdapterContractError):
        HTTPPolicy(**kwargs)  # type: ignore[arg-type]


def test_publish_results_allow_only_sanitized_terminal_states() -> None:
    assert (
        PublishResult.failed(
            "validation_failed",
            "Platform validation failed.",
            retry_classification="permanent",
        ).outcome
        == "failed"
    )
    assert (
        PublishResult.ambiguous(
            "dispatch_uncertain", "Publication outcome is uncertain."
        ).outcome
        == "ambiguous"
    )
    with pytest.raises(AdapterContractError):
        PublishResult(
            "unknown",  # type: ignore[arg-type]
            None,
            "bad",
            "Bad outcome.",
            "ambiguous",
        )
    with pytest.raises(AdapterContractError):
        PublishResult.published("unsafe\nremote-id")


def test_client_factory_rejects_cross_profile_credentials_and_isolates_headers() -> (
    None
):
    first = _snapshot("ansonphong", "10001")
    second = _snapshot("360hextile", "10002")
    first_credentials = PublishingCredentials(
        profile_id="ansonphong", x_user_access_token=SecretValue("first-token")
    )
    second_credentials = PublishingCredentials(
        profile_id="360hextile", x_user_access_token=SecretValue("second-token")
    )
    with pytest.raises(AdapterContractError, match="profile"):
        create_target_http_client(
            first,
            second_credentials,
            base_url="https://api.example.test",
        )

    with respx.mock(assert_all_called=True) as router:
        route = router.get("https://api.example.test/me").mock(
            side_effect=[httpx.Response(200, json={}), httpx.Response(200, json={})]
        )
        with create_target_http_client(
            first, first_credentials, base_url="https://api.example.test"
        ) as first_client:
            first_client.read_only_request("GET", "/me")
        with create_target_http_client(
            second, second_credentials, base_url="https://api.example.test"
        ) as second_client:
            second_client.read_only_request("GET", "/me")

    assert route.calls[0].request.headers["Authorization"] == "Bearer first-token"
    assert route.calls[1].request.headers["Authorization"] == "Bearer second-token"
    assert "first-token" not in repr(first_client)
    assert "second-token" not in repr(second_client)


def test_snapshot_and_adapter_client_binding_are_immutable_and_profile_exact() -> None:
    snapshot = _snapshot()
    with pytest.raises(TypeError):
        snapshot.target.request_settings["timeout"] = 99  # type: ignore[index]

    wrong_client = PlatformHTTPClient(
        _snapshot("360hextile", "10002"),
        SecretValue("wrong-client-token"),
        base_url="https://api.example.test",
        transport=httpx.MockTransport(lambda _request: httpx.Response(200, json={})),
    )
    with pytest.raises(AdapterContractError, match="different target"):
        _Adapter(snapshot, wrong_client, RemoteIdentity("10001", "ansonphong"))
    with pytest.raises(AdapterContractError, match="closed"):
        wrong_client.read_only_request("GET", "/me")


def test_adapter_lifecycle_is_identity_bound_checkpointed_and_commit_gated() -> None:
    snapshot = _snapshot()
    publication = _request(snapshot)
    writer = _Writer()
    with respx.mock(assert_all_called=True) as router:
        route = router.post("https://api.example.test/publish").mock(
            return_value=httpx.Response(200, json={"id": "post-1"})
        )
        client = PlatformHTTPClient(
            snapshot,
            SecretValue("adapter-token"),
            base_url="https://api.example.test",
        )
        adapter = _Adapter(snapshot, client, RemoteIdentity("10001", "ansonphong"))
        assert adapter.preflight(publication) == ()
        prepared = adapter.prepare(publication, prior=None, checkpoints=writer)
        assert writer.phases == ["processing", "ready"]
        assert [item.external_id for item in writer.artifacts] == ["media-1"]
        with pytest.raises(AdapterContractError, match="final-dispatch"):
            adapter.commit(prepared, delivery=_delivery("ready"))
        assert adapter.commit(
            prepared, delivery=_delivery("final_dispatch_started")
        ) == PublishResult.published("post-1")
        with pytest.raises(AdapterContractError, match="already attempted"):
            adapter.commit(prepared, delivery=_delivery("final_dispatch_started"))
        adapter.close()

    assert route.call_count == 1


def test_adapter_cannot_return_safe_retry_after_final_request() -> None:
    snapshot = _snapshot()
    prepared = PreparedPublication(_request(snapshot), 1, ())
    with respx.mock(assert_all_called=True) as router:
        route = router.post("https://api.example.test/publish").mock(
            return_value=httpx.Response(200, json={"id": "post-1"})
        )
        client = PlatformHTTPClient(
            snapshot,
            SecretValue("adapter-token"),
            base_url="https://api.example.test",
        )
        adapter = _RetryableAfterFinalAdapter(
            snapshot, client, RemoteIdentity("10001", "ansonphong")
        )

        with pytest.raises(AdapterContractError, match="cannot be safely retried"):
            adapter.commit(prepared, delivery=_delivery("final_dispatch_started"))
        with pytest.raises(AdapterContractError, match="already attempted"):
            adapter.commit(prepared, delivery=_delivery("final_dispatch_started"))
        adapter.close()

    assert route.call_count == 1


def test_wrong_remote_or_publication_identity_blocks_before_mutation() -> None:
    snapshot = _snapshot()
    client = PlatformHTTPClient(
        snapshot,
        SecretValue("identity-token"),
        base_url="https://api.example.test",
        transport=httpx.MockTransport(lambda _request: httpx.Response(200, json={})),
    )
    adapter = _Adapter(snapshot, client, RemoteIdentity("99999", "attacker"))
    with pytest.raises(AdapterContractError, match="identity"):
        adapter.prepare(_request(snapshot), prior=None, checkpoints=_Writer())
    assert adapter.mutations == 0

    adapter.identity = RemoteIdentity("10001", "ansonphong")
    with pytest.raises(AdapterContractError, match="different profile"):
        adapter.prepare(
            _request(_snapshot("360hextile", "10002")),
            prior=None,
            checkpoints=_Writer(),
        )
    assert adapter.mutations == 0
    adapter.close()


def test_unmocked_socket_attempt_fails_closed_with_sanitized_error() -> None:
    with (
        PlatformHTTPClient(
            _snapshot(),
            SecretValue("socket-token"),
            base_url="https://unmocked.invalid",
            policy=HTTPPolicy(max_pre_final_attempts=1),
        ) as client,
        pytest.raises(SocketBlockedError),
    ):
        client.read_only_request("GET", "/me")
