"""Strict, non-secret configuration loading for POST PULSAR.

Local settings and publishing credentials intentionally have separate loading
paths.  Commands that only inspect or reconcile durable state must not need to
read publishing tokens.
"""

from __future__ import annotations

import ipaddress
import math
import os
import tomllib
import unicodedata
from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field
from pathlib import Path, PureWindowsPath
from typing import Final, Literal, TypeAlias, cast
from urllib.parse import unquote, urlsplit

TargetName: TypeAlias = Literal["x", "instagram"]

X_TOKEN_ENV: Final = "POST_PULSAR_X_USER_ACCESS_TOKEN"
INSTAGRAM_TOKEN_ENV: Final = "POST_PULSAR_INSTAGRAM_ACCESS_TOKEN"
_TARGETS: Final[frozenset[str]] = frozenset({"x", "instagram"})


class ConfigurationError(ValueError):
    """A safe, operator-facing configuration error."""


class SecretValue:
    """An explicitly revealable secret whose normal rendering is redacted."""

    __slots__ = ("__value",)

    def __init__(self, value: str) -> None:
        self.__value = value

    def reveal(self) -> str:
        """Return the value for construction of an authenticated request."""
        return self.__value

    def __bool__(self) -> bool:
        return bool(self.__value)

    def __repr__(self) -> str:
        return "<redacted>"

    def __str__(self) -> str:
        return "<redacted>"


@dataclass(frozen=True, slots=True)
class AppSettings:
    """Filesystem and logging settings shared by all commands."""

    posts_directory: Path
    state_directory: Path
    log_file: Path
    log_max_bytes: int
    log_backups: int


@dataclass(frozen=True, slots=True)
class XSettings:
    """Non-secret X settings."""

    enabled: bool
    user_id: str
    request_timeout_seconds: float
    processing_timeout_seconds: float
    chunk_size_bytes: int


@dataclass(frozen=True, slots=True)
class InstagramSettings:
    """Non-secret Instagram settings."""

    enabled: bool
    user_id: str
    media_directory: Path
    media_base_url: str
    request_timeout_seconds: float
    processing_timeout_seconds: float


@dataclass(frozen=True, slots=True)
class LocalSettings:
    """Validated local configuration without credentials."""

    config_path: Path
    app: AppSettings
    x: XSettings
    instagram: InstagramSettings

    @property
    def enabled_targets(self) -> tuple[TargetName, ...]:
        """Return enabled targets in the application's stable order."""
        enabled: list[TargetName] = []
        if self.x.enabled:
            enabled.append("x")
        if self.instagram.enabled:
            enabled.append("instagram")
        return tuple(enabled)


@dataclass(frozen=True, slots=True)
class PublishingCredentials:
    """Credentials for only the targets required by the caller."""

    x_user_access_token: SecretValue | None = field(default=None, repr=False)
    instagram_access_token: SecretValue | None = field(default=None, repr=False)

    def for_target(self, target: TargetName) -> SecretValue:
        """Return a required target's credential without rendering it."""
        if target == "x":
            value = self.x_user_access_token
        elif target == "instagram":
            value = self.instagram_access_token
        else:  # pragma: no cover - the type checker excludes this branch
            raise ConfigurationError(f"unsupported publishing target: {target}")
        if value is None:
            raise ConfigurationError(f"publishing credential unavailable for {target}")
        return value

    def __repr__(self) -> str:
        return self._redacted_rendering()

    def __str__(self) -> str:
        return self._redacted_rendering()

    def _redacted_rendering(self) -> str:
        x_value = "<redacted>" if self.x_user_access_token else "None"
        instagram_value = "<redacted>" if self.instagram_access_token else "None"
        return (
            "PublishingCredentials("
            f"x_user_access_token={x_value}, "
            f"instagram_access_token={instagram_value})"
        )


def load_local_settings(
    config_path: str | os.PathLike[str] = "post-pulsar.toml",
) -> LocalSettings:
    """Load strict local settings without consulting the environment."""
    path = Path(config_path).expanduser().resolve()
    document = _read_toml(path)
    _reject_unknown(document, {"app", "x", "instagram"}, "configuration")

    app_data = _table(document, "app")
    x_data = _table(document, "x")
    instagram_data = _table(document, "instagram")

    _reject_unknown(
        app_data,
        {
            "posts_directory",
            "state_directory",
            "log_file",
            "log_max_bytes",
            "log_backups",
        },
        "app",
    )
    _reject_unknown(
        x_data,
        {
            "enabled",
            "user_id",
            "request_timeout_seconds",
            "processing_timeout_seconds",
            "chunk_size_bytes",
        },
        "x",
    )
    _reject_unknown(
        instagram_data,
        {
            "enabled",
            "user_id",
            "media_directory",
            "media_base_url",
            "request_timeout_seconds",
            "processing_timeout_seconds",
        },
        "instagram",
    )

    base = path.parent
    app = AppSettings(
        posts_directory=_relative_path(
            base, _string(app_data, "posts_directory", "posts"), "app.posts_directory"
        ),
        state_directory=_relative_path(
            base,
            _string(app_data, "state_directory", ".post-pulsar"),
            "app.state_directory",
        ),
        log_file=_relative_path(
            base, _string(app_data, "log_file", "post_pulsar.log"), "app.log_file"
        ),
        log_max_bytes=_positive_int(
            app_data, "log_max_bytes", 5 * 1024 * 1024, "app"
        ),
        log_backups=_nonnegative_int(app_data, "log_backups", 3, "app"),
    )
    x_settings = XSettings(
        enabled=_boolean(x_data, "enabled", False, "x"),
        user_id=_account_id(x_data, "user_id", "x"),
        request_timeout_seconds=_positive_number(
            x_data, "request_timeout_seconds", 30.0, "x"
        ),
        processing_timeout_seconds=_positive_number(
            x_data, "processing_timeout_seconds", 300.0, "x"
        ),
        chunk_size_bytes=_positive_int(
            x_data, "chunk_size_bytes", 4 * 1024 * 1024, "x"
        ),
    )
    media_base_url = _string(instagram_data, "media_base_url", "")
    instagram_settings = InstagramSettings(
        enabled=_boolean(instagram_data, "enabled", False, "instagram"),
        user_id=_account_id(instagram_data, "user_id", "instagram"),
        media_directory=_relative_path(
            base,
            _string(instagram_data, "media_directory", "public-media"),
            "instagram.media_directory",
        ),
        media_base_url=_validated_media_base_url(media_base_url),
        request_timeout_seconds=_positive_number(
            instagram_data, "request_timeout_seconds", 30.0, "instagram"
        ),
        processing_timeout_seconds=_positive_number(
            instagram_data, "processing_timeout_seconds", 300.0, "instagram"
        ),
    )

    _validate_path_separation(app, instagram_settings)
    if x_settings.enabled:
        _require_account_id("x", x_settings.user_id)
    if instagram_settings.enabled:
        _require_instagram_ready(instagram_settings)

    return LocalSettings(
        config_path=path,
        app=app,
        x=x_settings,
        instagram=instagram_settings,
    )


def validate_publishing_credentials(
    settings: LocalSettings,
    required_targets: Iterable[str],
    environ: Mapping[str, str] | None = None,
) -> PublishingCredentials:
    """Validate target settings and load tokens for explicitly required targets.

    ``required_targets`` comes from either the enabled-target set for new work or
    an immutable target snapshot for resumed work.  It deliberately does not
    depend on the current ``enabled`` flags.
    """
    environment = os.environ if environ is None else environ
    normalized: list[TargetName] = []
    for candidate in required_targets:
        if candidate not in _TARGETS:
            raise ConfigurationError(f"unsupported publishing target: {candidate}")
        target = cast(TargetName, candidate)
        if target not in normalized:
            normalized.append(target)

    missing: list[str] = []
    x_token: SecretValue | None = None
    instagram_token: SecretValue | None = None
    if "x" in normalized:
        _require_account_id("x", settings.x.user_id)
        raw_x_token = environment.get(X_TOKEN_ENV, "")
        if not raw_x_token.strip():
            missing.append(X_TOKEN_ENV)
        else:
            x_token = SecretValue(raw_x_token.strip())
    if "instagram" in normalized:
        _require_instagram_ready(settings.instagram)
        raw_instagram_token = environment.get(INSTAGRAM_TOKEN_ENV, "")
        if not raw_instagram_token.strip():
            missing.append(INSTAGRAM_TOKEN_ENV)
        else:
            instagram_token = SecretValue(raw_instagram_token.strip())

    if missing:
        raise ConfigurationError(
            "missing required publishing environment variable(s): "
            + ", ".join(missing)
        )
    return PublishingCredentials(
        x_user_access_token=x_token,
        instagram_access_token=instagram_token,
    )


def _read_toml(path: Path) -> dict[str, object]:
    try:
        with path.open("rb") as handle:
            parsed = tomllib.load(handle)
    except FileNotFoundError as exc:
        raise ConfigurationError(f"configuration file not found: {path}") from exc
    except (OSError, tomllib.TOMLDecodeError):
        raise ConfigurationError(
            f"configuration file is not readable TOML: {path}"
        ) from None
    return cast(dict[str, object], parsed)


def _table(document: Mapping[str, object], name: str) -> dict[str, object]:
    value = document.get(name, {})
    if not isinstance(value, dict):
        raise ConfigurationError(f"{name} must be a TOML table")
    return cast(dict[str, object], value)


def _reject_unknown(
    values: Mapping[str, object], allowed: set[str], section: str
) -> None:
    unknown = sorted(set(values) - allowed)
    if unknown:
        rendered = ", ".join(
            name if section == "configuration" else f"{section}.{name}"
            for name in unknown
        )
        raise ConfigurationError(f"unknown configuration field(s): {rendered}")


def _string(
    values: Mapping[str, object], key: str, default: str, section: str | None = None
) -> str:
    value = values.get(key, default)
    label = f"{section}.{key}" if section else key
    if not isinstance(value, str):
        raise ConfigurationError(f"{label} must be a string")
    return value


def _boolean(
    values: Mapping[str, object], key: str, default: bool, section: str
) -> bool:
    value = values.get(key, default)
    if type(value) is not bool:
        raise ConfigurationError(f"{section}.{key} must be a boolean")
    return cast(bool, value)


def _positive_number(
    values: Mapping[str, object], key: str, default: float, section: str
) -> float:
    value = values.get(key, default)
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ConfigurationError(f"{section}.{key} must be a finite positive number")
    try:
        number = float(value)
    except OverflowError:
        raise ConfigurationError(
            f"{section}.{key} must be a finite positive number"
        ) from None
    if not math.isfinite(number) or number <= 0:
        raise ConfigurationError(f"{section}.{key} must be a finite positive number")
    return number


def _positive_int(
    values: Mapping[str, object], key: str, default: int, section: str
) -> int:
    value = values.get(key, default)
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise ConfigurationError(f"{section}.{key} must be a positive integer")
    return value


def _nonnegative_int(
    values: Mapping[str, object], key: str, default: int, section: str
) -> int:
    value = values.get(key, default)
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ConfigurationError(f"{section}.{key} must be a non-negative integer")
    return value


def _account_id(values: Mapping[str, object], key: str, section: str) -> str:
    value = _string(values, key, "", section)
    if value and (value != value.strip() or not value.isascii() or not value.isdigit()):
        raise ConfigurationError(f"{section}.{key} must be an ASCII numeric ID")
    return value


def _relative_path(base: Path, raw: str, label: str) -> Path:
    if not raw or "\x00" in raw or "\\" in raw:
        raise ConfigurationError(
            f"{label} must be a nonempty relative path using forward slashes"
        )
    candidate_input = Path(raw)
    windows_input = PureWindowsPath(raw)
    if candidate_input.is_absolute() or windows_input.is_absolute() or windows_input.drive:
        raise ConfigurationError(f"{label} must remain inside the configuration directory")
    candidate = (base / candidate_input).resolve(strict=False)
    if not candidate.is_relative_to(base):
        raise ConfigurationError(f"{label} must remain inside the configuration directory")
    return candidate


def _validated_media_base_url(value: str) -> str:
    if not value:
        return value
    label = "instagram.media_base_url"
    if any(character.isspace() or ord(character) < 32 for character in value):
        raise ConfigurationError(f"{label} must be a safe public HTTPS base URL")
    if "\\" in value or "?" in value or "#" in value:
        raise ConfigurationError(f"{label} must be a safe public HTTPS base URL")
    try:
        parsed = urlsplit(value)
        _ = parsed.port  # Validate port syntax and range.
    except ValueError as exc:
        raise ConfigurationError(f"{label} must be a safe public HTTPS base URL") from exc
    if (
        parsed.scheme.lower() != "https"
        or not parsed.netloc
        or not parsed.hostname
        or parsed.username is not None
        or parsed.query
        or parsed.fragment
        or not parsed.path.endswith("/")
    ):
        raise ConfigurationError(f"{label} must be a safe public HTTPS base URL")
    hostname = parsed.hostname
    if hostname is None:  # Guard the Optional property after URL parsing.
        raise ConfigurationError(f"{label} must be a safe public HTTPS base URL")
    hostname = _ascii_idna_hostname(hostname, label)
    if _looks_like_noncanonical_ipv4(hostname):
        raise ConfigurationError(f"{label} must not contain an IP literal")
    try:
        ipaddress.ip_address(hostname)
    except ValueError:
        normalized_hostname = hostname.casefold()
        if normalized_hostname == "localhost" or normalized_hostname.endswith(
            ".localhost"
        ):
            raise ConfigurationError(f"{label} must use a public hostname")
    else:
        raise ConfigurationError(f"{label} must not contain an IP literal")

    decoded_path = parsed.path
    for _ in range(3):
        next_path = unquote(decoded_path)
        if next_path == decoded_path:
            break
        decoded_path = next_path
    if "\\" in decoded_path or any(
        segment in {".", ".."} for segment in decoded_path.split("/")
    ):
        raise ConfigurationError(f"{label} must not contain traversal")
    return value


def _ascii_idna_hostname(hostname: str, label: str) -> str:
    """Return one strict ASCII hostname representation without resolving it."""
    try:
        ascii_hostname = hostname.encode("idna", errors="strict").decode(
            "ascii", errors="strict"
        )
    except UnicodeError:
        raise ConfigurationError(
            f"{label} must be a safe public HTTPS base URL"
        ) from None
    normalized = ascii_hostname.rstrip(".").casefold()
    if not normalized or "%" in normalized:
        raise ConfigurationError(f"{label} must be a safe public HTTPS base URL")
    return normalized


def _looks_like_noncanonical_ipv4(hostname: str) -> bool:
    """Recognize inet_aton-style numeric hosts without resolving them."""
    components = hostname.split(".")
    if not components or any(not component for component in components):
        return False
    return all(_looks_like_ipv4_number(component) for component in components)


def _looks_like_ipv4_number(component: str) -> bool:
    lowered = component.lower()
    if lowered.startswith("0x"):
        digits = lowered[2:]
        return bool(digits) and all(
            character in "0123456789abcdef" for character in digits
        )
    return component.isascii() and component.isdigit()


def _validate_path_separation(
    app: AppSettings, instagram: InstagramSettings
) -> None:
    directories = {
        "app.posts_directory": app.posts_directory,
        "app.state_directory": app.state_directory,
        "instagram.media_directory": instagram.media_directory,
    }
    items = tuple(directories.items())
    for index, (left_name, left_path) in enumerate(items):
        for right_name, right_path in items[index + 1 :]:
            if _paths_overlap(left_path, right_path):
                raise ConfigurationError(
                    f"runtime directories must not overlap: {left_name}, {right_name}"
                )
    for directory_name, directory in items:
        if _paths_equal(app.log_file, directory) or _path_is_relative_to(
            directory, app.log_file
        ):
            raise ConfigurationError(
                f"app.log_file must not equal or contain {directory_name}"
            )

    for directory_name in ("app.posts_directory", "instagram.media_directory"):
        directory = directories[directory_name]
        if _path_is_relative_to(app.log_file, directory):
            raise ConfigurationError(
                f"app.log_file must not overlap {directory_name}"
            )


def _paths_overlap(left: Path, right: Path) -> bool:
    return (
        _paths_equal(left, right)
        or _path_is_relative_to(left, right)
        or _path_is_relative_to(right, left)
    )


def _paths_equal(left: Path, right: Path) -> bool:
    return _portable_path_parts(left) == _portable_path_parts(right)


def _path_is_relative_to(path: Path, parent: Path) -> bool:
    path_parts = _portable_path_parts(path)
    parent_parts = _portable_path_parts(parent)
    return (
        len(path_parts) >= len(parent_parts)
        and path_parts[: len(parent_parts)] == parent_parts
    )


def _portable_path_parts(path: Path) -> tuple[str, ...]:
    """Normalize components for supported case-insensitive filesystems."""
    return tuple(unicodedata.normalize("NFD", part).lower() for part in path.parts)


def _require_account_id(target: TargetName, user_id: str) -> None:
    if not user_id:
        raise ConfigurationError(f"{target}.user_id is required for publishing")


def _require_instagram_ready(settings: InstagramSettings) -> None:
    _require_account_id("instagram", settings.user_id)
    if not settings.media_base_url:
        raise ConfigurationError(
            "instagram.media_base_url is required for publishing"
        )
