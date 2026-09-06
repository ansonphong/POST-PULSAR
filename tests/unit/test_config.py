"""Tests for POST PULSAR's non-secret configuration boundary."""

from __future__ import annotations

import dataclasses
import json
from pathlib import Path

import pytest

from post_pulsar.config import (
    ConfigurationError,
    SecretValue,
    load_local_settings,
    validate_publishing_credentials,
)


def _profile(
    *,
    profile_id: str = "ansonphong",
    account_root: str = "accounts/ansonphong",
    timezone: str = "America/Vancouver",
    x_enabled: bool = True,
    x_id: str = "10001",
    x_username: str = "ansonphong",
    x_env: str = "POST_PULSAR_X_ANSONPHONG_USER_ACCESS_TOKEN",
    instagram: bool = True,
    instagram_enabled: bool = True,
    instagram_id: str = "20001",
    instagram_username: str = "anson.phong",
    instagram_env: str = "POST_PULSAR_INSTAGRAM_ANSONPHONG_ACCESS_TOKEN",
    media_directory: str = "public-media/ansonphong",
    media_base_url: str = "https://media.example.com/ansonphong/",
) -> str:
    instagram_table = ""
    if instagram:
        instagram_table = f"""
[profiles.instagram]
enabled = {str(instagram_enabled).lower()}
expected_remote_user_id = "{instagram_id}"
expected_username = "{instagram_username}"
token_env_var = "{instagram_env}"
media_directory = "{media_directory}"
media_base_url = "{media_base_url}"
request_timeout_seconds = 30
processing_timeout_seconds = 300
"""
    return f"""
[[profiles]]
profile_id = "{profile_id}"
account_root = "{account_root}"
timezone = "{timezone}"

[profiles.x]
enabled = {str(x_enabled).lower()}
expected_remote_user_id = "{x_id}"
expected_username = "{x_username}"
token_env_var = "{x_env}"
request_timeout_seconds = 30
processing_timeout_seconds = 300
chunk_size_bytes = 4194304
{instagram_table}
"""


def _write_config(tmp_path: Path, profiles: str | None = None, extra: str = "") -> Path:
    path = tmp_path / "post-pulsar.toml"
    path.write_text(
        f"""
[app]
state_directory = ".post-pulsar"
log_file = ".post-pulsar/logs/post_pulsar.log"
log_max_bytes = 5242880
log_backups = 3
deployment_mode = "simple"
allow_agent_publish = false
control_host = "127.0.0.1"
control_port = 8765
agent_capability_file = ".post-pulsar/control/agent-capability"
operator_verifier_file = ".post-pulsar/control/operator-verifier"
bootstrap_file = ".post-pulsar/control/bootstrap.json"
endpoint_record_file = ".post-pulsar/control/endpoint.json"
{extra}
{profiles if profiles is not None else _profile()}
""".strip()
        + "\n",
        encoding="utf-8",
    )
    return path


def test_load_valid_profile_config_resolves_paths_and_is_frozen(
    tmp_path: Path,
) -> None:
    config_path = _write_config(tmp_path)
    settings = load_local_settings(config_path)

    assert settings.config_path == config_path.resolve()
    assert settings.app.state_directory == (tmp_path / ".post-pulsar").resolve()
    assert (
        settings.app.endpoint_record_file
        == (tmp_path / ".post-pulsar/control/endpoint.json").resolve()
    )
    profile = settings.profile("ansonphong")
    assert profile.account_root == (tmp_path / "accounts/ansonphong").resolve()
    assert profile.enabled_targets == ("x", "instagram")
    assert profile.x is not None
    assert profile.x.expected_username == "ansonphong"
    with pytest.raises(dataclasses.FrozenInstanceError):
        profile.profile_id = "changed"  # type: ignore[misc]


def test_local_config_allows_zero_enabled_targets(tmp_path: Path) -> None:
    settings = load_local_settings(
        _write_config(
            tmp_path,
            _profile(
                x_enabled=False,
                instagram_enabled=False,
                media_base_url="",
            ),
        )
    )
    assert settings.profile("ansonphong").enabled_targets == ()


def test_unknown_fields_and_legacy_top_level_targets_are_rejected(
    tmp_path: Path,
) -> None:
    with pytest.raises(ConfigurationError, match="app.mystery"):
        load_local_settings(_write_config(tmp_path, extra="mystery = 1"))

    path = _write_config(tmp_path)
    path.write_text(path.read_text(encoding="utf-8") + "\n[x]\nenabled = false\n")
    with pytest.raises(ConfigurationError, match="unknown configuration field.*x"):
        load_local_settings(path)


@pytest.mark.parametrize(
    ("field", "replacement"),
    [
        ('state_directory = ".post-pulsar"', 'state_directory = "../state"'),
        (
            'agent_capability_file = ".post-pulsar/control/agent-capability"',
            'agent_capability_file = "/tmp/capability"',
        ),
        ('account_root = "accounts/ansonphong"', 'account_root = "../accounts"'),
        (
            'media_directory = "public-media/ansonphong"',
            'media_directory = "../public-media"',
        ),
    ],
)
def test_paths_cannot_escape_config_directory(
    tmp_path: Path, field: str, replacement: str
) -> None:
    path = _write_config(tmp_path)
    path.write_text(
        path.read_text(encoding="utf-8").replace(field, replacement),
        encoding="utf-8",
    )
    with pytest.raises(ConfigurationError, match="remain inside"):
        load_local_settings(path)


def test_control_files_must_be_distinct_and_inside_state_directory(
    tmp_path: Path,
) -> None:
    path = _write_config(tmp_path)
    path.write_text(
        path.read_text(encoding="utf-8").replace(
            'endpoint_record_file = ".post-pulsar/control/endpoint.json"',
            'endpoint_record_file = "endpoint.json"',
        ),
        encoding="utf-8",
    )
    with pytest.raises(ConfigurationError, match="endpoint_record_file"):
        load_local_settings(path)

    path = _write_config(tmp_path)
    path.write_text(
        path.read_text(encoding="utf-8").replace(
            'endpoint_record_file = ".post-pulsar/control/endpoint.json"',
            'endpoint_record_file = ".post-pulsar/control/bootstrap.json"',
        ),
        encoding="utf-8",
    )
    with pytest.raises(ConfigurationError, match="control files must be distinct"):
        load_local_settings(path)


@pytest.mark.parametrize(
    ("field", "replacement"),
    [
        (
            'agent_capability_file = ".post-pulsar/control/agent-capability"',
            'agent_capability_file = ".post-pulsar/control"',
        ),
        (
            'bootstrap_file = ".post-pulsar/control/bootstrap.json"',
            'bootstrap_file = ".post-pulsar/control/endpoint.json/child"',
        ),
        (
            'log_file = ".post-pulsar/logs/post_pulsar.log"',
            'log_file = ".post-pulsar/control"',
        ),
        (
            'log_file = ".post-pulsar/logs/post_pulsar.log"',
            'log_file = ".post-pulsar/control/bootstrap.json/child"',
        ),
    ],
)
def test_control_and_log_paths_reject_ancestor_descendant_collisions(
    tmp_path: Path, field: str, replacement: str
) -> None:
    path = _write_config(tmp_path)
    path.write_text(
        path.read_text(encoding="utf-8").replace(field, replacement),
        encoding="utf-8",
    )

    with pytest.raises(ConfigurationError, match="overlap"):
        load_local_settings(path)


def test_component_prefixes_are_not_path_collisions(tmp_path: Path) -> None:
    path = _write_config(tmp_path)
    text = path.read_text(encoding="utf-8")
    text = text.replace(
        'log_file = ".post-pulsar/logs/post_pulsar.log"',
        'log_file = ".post-pulsar/control/agent-capability.log"',
    ).replace(
        'bootstrap_file = ".post-pulsar/control/bootstrap.json"',
        'bootstrap_file = ".post-pulsar/control/endpoint.json.backup"',
    )
    path.write_text(text, encoding="utf-8")

    settings = load_local_settings(path)

    assert settings.app.log_file.name == "agent-capability.log"
    assert settings.app.bootstrap_file.name == "endpoint.json.backup"


@pytest.mark.parametrize("deployment_mode", ["shared", "HARDENED", ""])
def test_deployment_mode_is_strict(tmp_path: Path, deployment_mode: str) -> None:
    path = _write_config(tmp_path)
    path.write_text(
        path.read_text(encoding="utf-8").replace(
            'deployment_mode = "simple"', f'deployment_mode = "{deployment_mode}"'
        ),
        encoding="utf-8",
    )
    with pytest.raises(ConfigurationError, match="deployment_mode"):
        load_local_settings(path)


@pytest.mark.parametrize("control_host", ["localhost", "0.0.0.0", "127.0.0.2"])
def test_control_host_is_a_literal_supported_loopback(
    tmp_path: Path, control_host: str
) -> None:
    path = _write_config(tmp_path)
    path.write_text(
        path.read_text(encoding="utf-8").replace("127.0.0.1", control_host),
        encoding="utf-8",
    )
    with pytest.raises(ConfigurationError, match="control_host"):
        load_local_settings(path)


@pytest.mark.parametrize("invalid", ["nan", "inf", "-inf"])
def test_timeout_settings_must_be_finite(tmp_path: Path, invalid: str) -> None:
    path = _write_config(tmp_path)
    path.write_text(
        path.read_text(encoding="utf-8").replace(
            "request_timeout_seconds = 30",
            f"request_timeout_seconds = {invalid}",
            1,
        ),
        encoding="utf-8",
    )
    with pytest.raises(ConfigurationError, match="request_timeout_seconds"):
        load_local_settings(path)


@pytest.mark.parametrize(
    "unsafe_url",
    [
        "http://media.example.com/post-pulsar/",
        "https://user@media.example.com/post-pulsar/",
        "https://media.example.com/post-pulsar/?key=value",
        "https://media.example.com/post-pulsar/?",
        "https://media.example.com/post-pulsar/#fragment",
        "https://media.example.com/post-pulsar/#",
        "https://127.0.0.1/post-pulsar/",
        "https://[::1]/post-pulsar/",
        "https://2130706433/post-pulsar/",
        "https://0x7f000001/post-pulsar/",
        "https://127.1/post-pulsar/",
        "https://%31%32%37.0.0.1/post-pulsar/",
        "https://127%2e0%2e0%2e1/post-pulsar/",
        "https://１２７。０。０。１/post-pulsar/",
        "https://１2７．０.0｡１/post-pulsar/",
        "https://２１３０７０６４３３/post-pulsar/",
        f"https://{'é' * 64}.example/post-pulsar/",
        "https://media.example.com/../private/",
        "https://media.example.com/%2e%2e/private/",
        "https://media.example.com/post-pulsar",
    ],
)
def test_unsafe_instagram_media_base_urls_are_rejected(
    tmp_path: Path, unsafe_url: str
) -> None:
    with pytest.raises(ConfigurationError, match="media_base_url"):
        load_local_settings(
            _write_config(tmp_path, _profile(media_base_url=unsafe_url))
        )


def test_credentials_use_only_selected_profiles_allowlisted_reference(
    tmp_path: Path,
) -> None:
    settings = load_local_settings(_write_config(tmp_path))
    token = "x-secret-value"
    credentials = validate_publishing_credentials(
        settings,
        "ansonphong",
        ("x",),
        {"POST_PULSAR_X_ANSONPHONG_USER_ACCESS_TOKEN": token},
    )
    assert credentials.profile_id == "ansonphong"
    assert credentials.for_target("x").reveal() == token
    assert credentials.instagram_access_token is None


@pytest.mark.parametrize("invalid", ["threads", "", "X"])
def test_dynamic_target_accessors_reject_unknown_names(
    tmp_path: Path, invalid: str
) -> None:
    settings = load_local_settings(_write_config(tmp_path))
    profile = settings.profile("ansonphong")
    credentials = validate_publishing_credentials(
        settings,
        "ansonphong",
        ("x",),
        {"POST_PULSAR_X_ANSONPHONG_USER_ACCESS_TOKEN": "secret"},
    )
    assert profile.target("x") is profile.x
    assert profile.target("instagram") is profile.instagram

    with pytest.raises(ConfigurationError, match="unsupported publishing target"):
        profile.target(invalid)  # type: ignore[arg-type]
    with pytest.raises(ConfigurationError, match="unsupported publishing target"):
        credentials.for_target(invalid)  # type: ignore[arg-type]


def test_required_disabled_snapshot_target_still_loads_credential(
    tmp_path: Path,
) -> None:
    settings = load_local_settings(
        _write_config(tmp_path, _profile(x_enabled=False, instagram=False))
    )
    credentials = validate_publishing_credentials(
        settings,
        "ansonphong",
        ("x",),
        {"POST_PULSAR_X_ANSONPHONG_USER_ACCESS_TOKEN": "secret"},
    )
    assert credentials.for_target("x")


def test_missing_credential_names_variable_without_rendering_other_values(
    tmp_path: Path,
) -> None:
    settings = load_local_settings(_write_config(tmp_path))
    unrelated_secret = "do-not-disclose"
    with pytest.raises(ConfigurationError) as caught:
        validate_publishing_credentials(
            settings,
            "ansonphong",
            ("instagram",),
            {"UNRELATED_SECRET": unrelated_secret},
        )
    message = str(caught.value)
    assert "POST_PULSAR_INSTAGRAM_ANSONPHONG_ACCESS_TOKEN" in message
    assert unrelated_secret not in message


def test_status_style_local_load_never_reads_environment(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.delenv("POST_PULSAR_X_ANSONPHONG_USER_ACCESS_TOKEN", raising=False)
    monkeypatch.delenv("POST_PULSAR_INSTAGRAM_ANSONPHONG_ACCESS_TOKEN", raising=False)
    settings = load_local_settings(_write_config(tmp_path))
    assert settings.profile("ansonphong").enabled_targets == ("x", "instagram")


def test_secret_values_are_redacted_and_not_json_serializable(tmp_path: Path) -> None:
    settings = load_local_settings(_write_config(tmp_path))
    x_token = "x-super-secret"
    instagram_token = "instagram-super-secret"
    credentials = validate_publishing_credentials(
        settings,
        "ansonphong",
        ("x", "instagram"),
        {
            "POST_PULSAR_X_ANSONPHONG_USER_ACCESS_TOKEN": x_token,
            "POST_PULSAR_INSTAGRAM_ANSONPHONG_ACCESS_TOKEN": instagram_token,
        },
    )
    rendered = f"{credentials!r} {credentials} {credentials.x_user_access_token!r}"
    assert x_token not in rendered
    assert instagram_token not in rendered
    assert "<redacted>" in rendered
    assert credentials.for_target("instagram").reveal() == instagram_token
    with pytest.raises(TypeError):
        json.dumps(credentials)
    assert isinstance(credentials.x_user_access_token, SecretValue)


def test_boolean_is_not_accepted_as_integer_setting(tmp_path: Path) -> None:
    path = _write_config(tmp_path)
    path.write_text(
        path.read_text(encoding="utf-8").replace(
            "log_backups = 3", "log_backups = true"
        ),
        encoding="utf-8",
    )
    with pytest.raises(ConfigurationError, match="app.log_backups"):
        load_local_settings(path)


@pytest.mark.parametrize(
    ("table", "key", "original", "invalid"),
    [
        ("profiles.x", "request_timeout_seconds", "30", "nan"),
        ("profiles.x", "processing_timeout_seconds", "300", "inf"),
        ("profiles.instagram", "request_timeout_seconds", "30", "-inf"),
        ("profiles.instagram", "processing_timeout_seconds", "300", "nan"),
    ],
)
def test_all_timeout_settings_must_be_finite(
    tmp_path: Path, table: str, key: str, original: str, invalid: str
) -> None:
    path = _write_config(tmp_path)
    text = path.read_text(encoding="utf-8")
    prefix, section_text = text.split(f"[{table}]", maxsplit=1)
    section_text = section_text.replace(f"{key} = {original}", f"{key} = {invalid}", 1)
    path.write_text(f"{prefix}[{table}]{section_text}", encoding="utf-8")

    with pytest.raises(ConfigurationError, match=key):
        load_local_settings(path)


def test_oversized_timeout_has_sanitized_error(tmp_path: Path) -> None:
    path = _write_config(tmp_path)
    oversized = "1" + ("0" * 400)
    path.write_text(
        path.read_text(encoding="utf-8").replace(
            "request_timeout_seconds = 30",
            f"request_timeout_seconds = {oversized}",
            1,
        ),
        encoding="utf-8",
    )

    with pytest.raises(ConfigurationError) as caught:
        load_local_settings(path)

    message = str(caught.value)
    assert message.endswith("request_timeout_seconds must be a finite positive number")
    assert oversized not in message


@pytest.mark.parametrize(
    ("state_directory", "account_root", "media_directory"),
    [
        ("accounts/ansonphong", "accounts/ansonphong", "public-media/one"),
        ("accounts/ansonphong/state", "accounts/ansonphong", "public-media/one"),
        ("ACCOUNTS/ANSONPHONG/state", "accounts/ansonphong", "public-media/one"),
        (".post-pulsar", "accounts/ansonphong", "accounts/ansonphong/media"),
        (".post-pulsar", "accounts/ansonphong", "Accounts/AnsonPhong/media"),
        ("runtime", "accounts/ansonphong", "runtime"),
        ("runtime/state", "accounts/ansonphong", "runtime"),
        ("Runtime/State", "accounts/ansonphong", "runtime"),
    ],
)
def test_runtime_directory_overlap_combinations_are_rejected(
    tmp_path: Path,
    state_directory: str,
    account_root: str,
    media_directory: str,
) -> None:
    profile = _profile(
        account_root=account_root,
        media_directory=media_directory,
    )
    path = _write_config(tmp_path, profile)
    path.write_text(
        path.read_text(encoding="utf-8").replace(
            'state_directory = ".post-pulsar"',
            f'state_directory = "{state_directory}"',
        ),
        encoding="utf-8",
    )

    with pytest.raises(ConfigurationError, match="overlap"):
        load_local_settings(path)


def test_runtime_path_comparison_is_component_aware(tmp_path: Path) -> None:
    settings = load_local_settings(
        _write_config(
            tmp_path,
            _profile(account_root=".post-pulsar-archive"),
        )
    )

    assert (
        settings.profile("ansonphong").account_root
        == (tmp_path / ".post-pulsar-archive").resolve()
    )


def test_log_file_may_be_inside_state_but_not_an_account_root(
    tmp_path: Path,
) -> None:
    settings = load_local_settings(_write_config(tmp_path))
    assert (
        settings.app.log_file
        == (tmp_path / ".post-pulsar/logs/post_pulsar.log").resolve()
    )

    path = _write_config(tmp_path)
    path.write_text(
        path.read_text(encoding="utf-8").replace(
            'log_file = ".post-pulsar/logs/post_pulsar.log"',
            'log_file = "accounts/ansonphong/post_pulsar.log"',
        ),
        encoding="utf-8",
    )
    with pytest.raises(ConfigurationError, match="app.log_file"):
        load_local_settings(path)
