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


def _write_config(
    tmp_path: Path,
    *,
    x_enabled: bool = True,
    x_user_id: str = "123",
    instagram_enabled: bool = True,
    instagram_user_id: str = "456",
    media_directory: str = "public-media",
    media_base_url: str = "https://media.example.com/post-pulsar/",
    extra: str = "",
) -> Path:
    path = tmp_path / "post-pulsar.toml"
    path.write_text(
        f"""
[app]
posts_directory = "posts"
state_directory = ".post-pulsar"
log_file = "post_pulsar.log"
log_max_bytes = 5242880
log_backups = 3

[x]
enabled = {str(x_enabled).lower()}
user_id = "{x_user_id}"
request_timeout_seconds = 30
processing_timeout_seconds = 300
chunk_size_bytes = 4194304

[instagram]
enabled = {str(instagram_enabled).lower()}
user_id = "{instagram_user_id}"
media_directory = "{media_directory}"
media_base_url = "{media_base_url}"
request_timeout_seconds = 30
processing_timeout_seconds = 300
{extra}
""".strip()
        + "\n",
        encoding="utf-8",
    )
    return path


def test_load_valid_config_resolves_paths_and_is_frozen(tmp_path: Path) -> None:
    config_path = _write_config(tmp_path)

    settings = load_local_settings(config_path)

    assert settings.config_path == config_path.resolve()
    assert settings.app.posts_directory == (tmp_path / "posts").resolve()
    assert settings.app.state_directory == (tmp_path / ".post-pulsar").resolve()
    assert settings.app.log_file == (tmp_path / "post_pulsar.log").resolve()
    assert settings.instagram.media_directory == (
        tmp_path / "public-media"
    ).resolve()
    assert settings.enabled_targets == ("x", "instagram")
    with pytest.raises(dataclasses.FrozenInstanceError):
        settings.x.enabled = False  # type: ignore[misc]


def test_local_config_allows_zero_enabled_targets(tmp_path: Path) -> None:
    settings = load_local_settings(
        _write_config(
            tmp_path,
            x_enabled=False,
            x_user_id="",
            instagram_enabled=False,
            instagram_user_id="",
            media_base_url="",
        )
    )

    assert settings.enabled_targets == ()


@pytest.mark.parametrize(
    ("extra", "expected"),
    [
        ("mystery = 1", "instagram.mystery"),
        ("\n[unknown]\nvalue = 1", "unknown"),
    ],
)
def test_unknown_fields_are_rejected(
    tmp_path: Path, extra: str, expected: str
) -> None:
    with pytest.raises(ConfigurationError, match=expected):
        load_local_settings(_write_config(tmp_path, extra=extra))


@pytest.mark.parametrize(
    "field_line",
    [
        'posts_directory = "../posts"',
        'state_directory = "../state"',
        'log_file = "../post_pulsar.log"',
        'posts_directory = "/tmp/posts"',
    ],
)
def test_paths_cannot_escape_config_directory(
    tmp_path: Path, field_line: str
) -> None:
    path = _write_config(tmp_path)
    text = path.read_text(encoding="utf-8")
    key = field_line.split(" =", maxsplit=1)[0]
    text = text.replace(
        next(line for line in text.splitlines() if line.startswith(f"{key} =")),
        field_line,
    )
    path.write_text(text, encoding="utf-8")

    with pytest.raises(ConfigurationError, match="remain inside"):
        load_local_settings(path)


@pytest.mark.parametrize(
    ("state_directory", "media_directory"),
    [
        ("posts", "public-media"),
        ("posts/state", "public-media"),
        (".post-pulsar", "posts/media"),
        ("runtime", "runtime"),
        ("runtime/state", "runtime"),
    ],
)
def test_runtime_directories_cannot_collide(
    tmp_path: Path, state_directory: str, media_directory: str
) -> None:
    path = _write_config(tmp_path, media_directory=media_directory)
    text = path.read_text(encoding="utf-8").replace(
        'state_directory = ".post-pulsar"',
        f'state_directory = "{state_directory}"',
    )
    path.write_text(text, encoding="utf-8")

    with pytest.raises(ConfigurationError, match="overlap"):
        load_local_settings(path)


@pytest.mark.parametrize("target", ["x", "instagram"])
def test_enabled_targets_require_numeric_expected_account_ids(
    tmp_path: Path, target: str
) -> None:
    kwargs = {f"{target}_user_id": ""}

    with pytest.raises(ConfigurationError, match=f"{target}.user_id"):
        load_local_settings(_write_config(tmp_path, **kwargs))  # type: ignore[arg-type]


@pytest.mark.parametrize(
    "unsafe_url",
    [
        "http://media.example.com/post-pulsar/",
        "https://user@media.example.com/post-pulsar/",
        "https://media.example.com/post-pulsar/?key=value",
        "https://media.example.com/post-pulsar/#fragment",
        "https://127.0.0.1/post-pulsar/",
        "https://[::1]/post-pulsar/",
        "https://media.example.com/../private/",
        "https://media.example.com/%2e%2e/private/",
        "https://media.example.com/post-pulsar",
    ],
)
def test_unsafe_instagram_media_base_urls_are_rejected(
    tmp_path: Path, unsafe_url: str
) -> None:
    with pytest.raises(ConfigurationError, match="instagram.media_base_url"):
        load_local_settings(_write_config(tmp_path, media_base_url=unsafe_url))


def test_unknown_required_target_is_rejected_without_environment_values(
    tmp_path: Path,
) -> None:
    settings = load_local_settings(_write_config(tmp_path))

    with pytest.raises(ConfigurationError, match="unsupported publishing target"):
        validate_publishing_credentials(settings, ("threads",), {})


def test_credentials_validate_only_required_snapshot_targets(tmp_path: Path) -> None:
    settings = load_local_settings(
        _write_config(
            tmp_path,
            x_enabled=False,
            instagram_enabled=False,
            media_base_url="",
        )
    )
    token = "x-secret-value"

    credentials = validate_publishing_credentials(
        settings,
        ("x",),
        {"POST_PULSAR_X_USER_ACCESS_TOKEN": token},
    )

    assert credentials.for_target("x").reveal() == token
    assert credentials.instagram_access_token is None


def test_missing_required_token_names_variable_but_not_other_values(
    tmp_path: Path,
) -> None:
    settings = load_local_settings(_write_config(tmp_path))
    unrelated_secret = "do-not-disclose"

    with pytest.raises(ConfigurationError) as caught:
        validate_publishing_credentials(
            settings,
            ("instagram",),
            {"UNRELATED_SECRET": unrelated_secret},
        )

    message = str(caught.value)
    assert "POST_PULSAR_INSTAGRAM_ACCESS_TOKEN" in message
    assert unrelated_secret not in message


def test_status_style_local_load_never_reads_environment(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config_path = _write_config(tmp_path)
    monkeypatch.delenv("POST_PULSAR_X_USER_ACCESS_TOKEN", raising=False)
    monkeypatch.delenv("POST_PULSAR_INSTAGRAM_ACCESS_TOKEN", raising=False)

    settings = load_local_settings(config_path)

    assert settings.x.enabled is True
    assert settings.instagram.enabled is True


def test_secret_values_are_redacted_and_not_json_serializable(tmp_path: Path) -> None:
    settings = load_local_settings(_write_config(tmp_path))
    x_token = "x-super-secret"
    instagram_token = "instagram-super-secret"

    credentials = validate_publishing_credentials(
        settings,
        settings.enabled_targets,
        {
            "POST_PULSAR_X_USER_ACCESS_TOKEN": x_token,
            "POST_PULSAR_INSTAGRAM_ACCESS_TOKEN": instagram_token,
        },
    )

    rendered = f"{credentials!r} {credentials} {credentials.x_user_access_token!r}"
    assert x_token not in rendered
    assert instagram_token not in rendered
    assert "<redacted>" in rendered
    with pytest.raises(TypeError):
        json.dumps(credentials)
    assert isinstance(credentials.x_user_access_token, SecretValue)


def test_required_disabled_target_still_requires_usable_account_config(
    tmp_path: Path,
) -> None:
    settings = load_local_settings(
        _write_config(
            tmp_path,
            x_enabled=False,
            x_user_id="",
            instagram_enabled=False,
            instagram_user_id="",
            media_base_url="",
        )
    )

    with pytest.raises(ConfigurationError, match="x.user_id"):
        validate_publishing_credentials(
            settings,
            ("x",),
            {"POST_PULSAR_X_USER_ACCESS_TOKEN": "secret"},
        )


def test_boolean_is_not_accepted_as_integer_setting(tmp_path: Path) -> None:
    path = _write_config(tmp_path)
    text = path.read_text(encoding="utf-8").replace(
        "log_backups = 3", "log_backups = true"
    )
    path.write_text(text, encoding="utf-8")

    with pytest.raises(ConfigurationError, match="app.log_backups"):
        load_local_settings(path)
