"""Strict multi-profile configuration and environment-only credentials."""

from __future__ import annotations

import ipaddress
import math
import os
import re
import stat
import tomllib
import unicodedata
from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field
from pathlib import Path, PureWindowsPath
from typing import Final, Literal, TypeAlias, cast
from urllib.parse import unquote, urlsplit
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

TargetName: TypeAlias = Literal["x", "instagram"]
DeploymentMode: TypeAlias = Literal["simple", "hardened"]

_TARGETS: Final = frozenset({"x", "instagram"})
_PROFILE_ID_RE: Final = re.compile(r"[a-z0-9][a-z0-9-]{0,31}\Z")
_USERNAME_PATTERNS: Final[dict[TargetName, re.Pattern[str]]] = {
    "x": re.compile(r"[A-Za-z0-9_]{1,15}\Z"),
    "instagram": re.compile(r"[A-Za-z0-9._]{1,30}\Z"),
}
_TIMEZONE_RE: Final = re.compile(r"[A-Za-z0-9._+-]+(?:/[A-Za-z0-9._+-]+)*\Z")
_TOKEN_ENV_PATTERNS: Final[dict[TargetName, re.Pattern[str]]] = {
    "x": re.compile(r"POST_PULSAR_X_[A-Z0-9]+(?:_[A-Z0-9]+)*_USER_ACCESS_TOKEN\Z"),
    "instagram": re.compile(
        r"POST_PULSAR_INSTAGRAM_[A-Z0-9]+(?:_[A-Z0-9]+)*_ACCESS_TOKEN\Z"
    ),
}


class ConfigurationError(ValueError):
    """A safe, operator-facing configuration error."""


class SecretValue:
    """An explicitly revealable secret whose normal rendering is redacted."""

    __slots__ = ("__value",)

    def __init__(self, value: str) -> None:
        self.__value = value

    def reveal(self) -> str:
        """Return the value only for construction of an authenticated request."""
        return self.__value

    def __bool__(self) -> bool:
        return bool(self.__value)

    def __repr__(self) -> str:
        return "<redacted>"

    def __str__(self) -> str:
        return "<redacted>"


@dataclass(frozen=True, slots=True)
class AppSettings:
    """Installation-wide local paths and daemon/control settings."""

    state_directory: Path
    log_file: Path
    log_max_bytes: int
    log_backups: int
    deployment_mode: DeploymentMode
    allow_agent_publish: bool
    control_host: str
    control_port: int
    agent_capability_file: Path
    operator_verifier_file: Path
    bootstrap_file: Path
    endpoint_record_file: Path


@dataclass(frozen=True, slots=True)
class XSettings:
    """One profile's non-secret X target settings."""

    enabled: bool
    expected_remote_user_id: str
    expected_username: str
    token_env_var: str
    request_timeout_seconds: float
    processing_timeout_seconds: float
    chunk_size_bytes: int


@dataclass(frozen=True, slots=True)
class InstagramSettings:
    """One profile's non-secret Instagram target settings."""

    enabled: bool
    expected_remote_user_id: str
    expected_username: str
    token_env_var: str
    media_directory: Path
    media_base_url: str
    request_timeout_seconds: float
    processing_timeout_seconds: float


@dataclass(frozen=True, slots=True)
class ProfileSettings:
    """A stable publishing identity with an isolated content root."""

    profile_id: str
    account_root: Path
    timezone: str
    x: XSettings | None
    instagram: InstagramSettings | None

    @property
    def enabled_targets(self) -> tuple[TargetName, ...]:
        """Return enabled targets in stable platform order."""
        enabled: list[TargetName] = []
        if self.x is not None and self.x.enabled:
            enabled.append("x")
        if self.instagram is not None and self.instagram.enabled:
            enabled.append("instagram")
        return tuple(enabled)

    def target(self, name: TargetName) -> XSettings | InstagramSettings:
        """Return a configured target, including a currently disabled target."""
        target = self.x if name == "x" else self.instagram
        if target is None:
            raise ConfigurationError(
                f"profile {self.profile_id!r} has no configured {name} target"
            )
        return target


@dataclass(frozen=True, slots=True)
class LocalSettings:
    """Validated local configuration without credential values."""

    config_path: Path
    app: AppSettings
    profiles: tuple[ProfileSettings, ...]

    def profile(self, profile_id: str) -> ProfileSettings:
        """Resolve an exact stable profile ID."""
        for profile in self.profiles:
            if profile.profile_id == profile_id:
                return profile
        raise ConfigurationError(f"unknown profile_id: {profile_id}")


@dataclass(frozen=True, slots=True)
class PublishingCredentials:
    """Credentials for one explicitly selected profile and target snapshot."""

    profile_id: str
    x_user_access_token: SecretValue | None = field(default=None, repr=False)
    instagram_access_token: SecretValue | None = field(default=None, repr=False)

    def for_target(self, target: TargetName) -> SecretValue:
        """Return a required target's credential without rendering it."""
        value = (
            self.x_user_access_token if target == "x" else self.instagram_access_token
        )
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
            f"PublishingCredentials(profile_id={self.profile_id!r}, "
            f"x_user_access_token={x_value}, "
            f"instagram_access_token={instagram_value})"
        )


def load_local_settings(
    config_path: str | os.PathLike[str] = "post-pulsar.toml",
) -> LocalSettings:
    """Load strict local settings without consulting the environment."""
    path = Path(config_path).expanduser().resolve()
    document = _read_toml(path)
    _reject_unknown(document, {"app", "profiles"}, "configuration")
    app_data = _table(document, "app")
    _reject_unknown(
        app_data,
        {
            "state_directory",
            "log_file",
            "log_max_bytes",
            "log_backups",
            "deployment_mode",
            "allow_agent_publish",
            "control_host",
            "control_port",
            "agent_capability_file",
            "operator_verifier_file",
            "bootstrap_file",
            "endpoint_record_file",
        },
        "app",
    )

    base = path.parent
    state_directory = _relative_path(
        base,
        _string(app_data, "state_directory", ".post-pulsar", "app"),
        "app.state_directory",
    )
    app = AppSettings(
        state_directory=state_directory,
        log_file=_relative_path(
            base,
            _string(app_data, "log_file", ".post-pulsar/post_pulsar.log", "app"),
            "app.log_file",
        ),
        log_max_bytes=_positive_int(app_data, "log_max_bytes", 5 * 1024 * 1024, "app"),
        log_backups=_nonnegative_int(app_data, "log_backups", 3, "app"),
        deployment_mode=_deployment_mode(app_data),
        allow_agent_publish=_boolean(app_data, "allow_agent_publish", False, "app"),
        control_host=_control_host(app_data),
        control_port=_port(app_data, "control_port", 8765, "app"),
        agent_capability_file=_relative_path(
            base,
            _string(
                app_data,
                "agent_capability_file",
                ".post-pulsar/control/agent-capability",
                "app",
            ),
            "app.agent_capability_file",
        ),
        operator_verifier_file=_relative_path(
            base,
            _string(
                app_data,
                "operator_verifier_file",
                ".post-pulsar/control/operator-verifier",
                "app",
            ),
            "app.operator_verifier_file",
        ),
        bootstrap_file=_relative_path(
            base,
            _string(
                app_data,
                "bootstrap_file",
                ".post-pulsar/control/bootstrap.json",
                "app",
            ),
            "app.bootstrap_file",
        ),
        endpoint_record_file=_relative_path(
            base,
            _string(
                app_data,
                "endpoint_record_file",
                ".post-pulsar/control/endpoint.json",
                "app",
            ),
            "app.endpoint_record_file",
        ),
    )

    profiles_data = document.get("profiles")
    if not isinstance(profiles_data, list) or not profiles_data:
        raise ConfigurationError(
            "configuration must define at least one [[profiles]] table"
        )
    profiles = tuple(
        _parse_profile(base, value, index) for index, value in enumerate(profiles_data)
    )
    _validate_profile_uniqueness(profiles)
    _validate_path_separation(app, profiles)
    return LocalSettings(config_path=path, app=app, profiles=profiles)


def validate_publishing_credentials(
    settings: LocalSettings,
    profile_id: str,
    required_targets: Iterable[str],
    environ: Mapping[str, str] | None = None,
) -> PublishingCredentials:
    """Load only one profile's explicitly required current or snapshot targets."""
    profile = settings.profile(profile_id)
    environment = os.environ if environ is None else environ
    normalized: list[TargetName] = []
    for candidate in required_targets:
        if candidate not in _TARGETS:
            raise ConfigurationError(f"unsupported publishing target: {candidate}")
        target_name = cast(TargetName, candidate)
        if target_name not in normalized:
            normalized.append(target_name)

    tokens: dict[TargetName, SecretValue] = {}
    missing: list[str] = []
    for target_name in normalized:
        target = profile.target(target_name)
        if (
            target_name == "instagram"
            and not cast(InstagramSettings, target).media_base_url
        ):
            raise ConfigurationError(
                f"profile {profile_id!r} Instagram media_base_url is required "
                "for publishing"
            )
        raw_value = environment.get(target.token_env_var, "")
        if not raw_value.strip():
            missing.append(target.token_env_var)
        else:
            tokens[target_name] = SecretValue(raw_value.strip())
    if missing:
        raise ConfigurationError(
            "missing required publishing environment variable(s): " + ", ".join(missing)
        )
    return PublishingCredentials(
        profile_id=profile_id,
        x_user_access_token=tokens.get("x"),
        instagram_access_token=tokens.get("instagram"),
    )


def _parse_profile(base: Path, value: object, index: int) -> ProfileSettings:
    section = f"profiles[{index}]"
    if not isinstance(value, dict):
        raise ConfigurationError(f"{section} must be a TOML table")
    data = cast(dict[str, object], value)
    _reject_unknown(
        data, {"profile_id", "account_root", "timezone", "x", "instagram"}, section
    )
    profile_id = _string(data, "profile_id", "", section)
    if not _PROFILE_ID_RE.fullmatch(profile_id):
        raise ConfigurationError(
            f"{section}.profile_id must match [a-z0-9][a-z0-9-]{{0,31}}"
        )
    timezone = _timezone(data, section)
    return ProfileSettings(
        profile_id=profile_id,
        account_root=_relative_path(
            base,
            _string(data, "account_root", "", section),
            f"{section}.account_root",
        ),
        timezone=timezone,
        x=_parse_x_target(data.get("x"), section),
        instagram=_parse_instagram_target(base, data.get("instagram"), section),
    )


def _parse_x_target(value: object, profile_section: str) -> XSettings | None:
    if value is None:
        return None
    section = f"{profile_section}.x"
    data = _target_table(value, section)
    _reject_unknown(
        data,
        {
            "enabled",
            "expected_remote_user_id",
            "expected_username",
            "token_env_var",
            "request_timeout_seconds",
            "processing_timeout_seconds",
            "chunk_size_bytes",
        },
        section,
    )
    return XSettings(
        enabled=_boolean(data, "enabled", False, section),
        expected_remote_user_id=_remote_id(data, section),
        expected_username=_username(data, section, "x"),
        token_env_var=_token_env_var(data, section, "x"),
        request_timeout_seconds=_positive_number(
            data, "request_timeout_seconds", 30.0, section
        ),
        processing_timeout_seconds=_positive_number(
            data, "processing_timeout_seconds", 300.0, section
        ),
        chunk_size_bytes=_positive_int(
            data, "chunk_size_bytes", 4 * 1024 * 1024, section
        ),
    )


def _parse_instagram_target(
    base: Path, value: object, profile_section: str
) -> InstagramSettings | None:
    if value is None:
        return None
    section = f"{profile_section}.instagram"
    data = _target_table(value, section)
    _reject_unknown(
        data,
        {
            "enabled",
            "expected_remote_user_id",
            "expected_username",
            "token_env_var",
            "media_directory",
            "media_base_url",
            "request_timeout_seconds",
            "processing_timeout_seconds",
        },
        section,
    )
    enabled = _boolean(data, "enabled", False, section)
    media_base_url = _validated_media_base_url(
        _string(data, "media_base_url", "", section), section
    )
    if enabled and not media_base_url:
        raise ConfigurationError(f"{section}.media_base_url is required when enabled")
    return InstagramSettings(
        enabled=enabled,
        expected_remote_user_id=_remote_id(data, section),
        expected_username=_username(data, section, "instagram"),
        token_env_var=_token_env_var(data, section, "instagram"),
        media_directory=_relative_path(
            base,
            _string(data, "media_directory", "", section),
            f"{section}.media_directory",
        ),
        media_base_url=media_base_url,
        request_timeout_seconds=_positive_number(
            data, "request_timeout_seconds", 30.0, section
        ),
        processing_timeout_seconds=_positive_number(
            data, "processing_timeout_seconds", 300.0, section
        ),
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


def _target_table(value: object, section: str) -> dict[str, object]:
    if not isinstance(value, dict):
        raise ConfigurationError(f"{section} must be a TOML table")
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


def _string(values: Mapping[str, object], key: str, default: str, section: str) -> str:
    value = values.get(key, default)
    if not isinstance(value, str):
        raise ConfigurationError(f"{section}.{key} must be a string")
    return value


def _boolean(
    values: Mapping[str, object], key: str, default: bool, section: str
) -> bool:
    value = values.get(key, default)
    if type(value) is not bool:
        raise ConfigurationError(f"{section}.{key} must be a boolean")
    return value


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


def _port(values: Mapping[str, object], key: str, default: int, section: str) -> int:
    value = values.get(key, default)
    if isinstance(value, bool) or not isinstance(value, int) or not 1 <= value <= 65535:
        raise ConfigurationError(f"{section}.{key} must be an integer from 1 to 65535")
    return value


def _deployment_mode(values: Mapping[str, object]) -> DeploymentMode:
    value = _string(values, "deployment_mode", "simple", "app")
    if value not in {"simple", "hardened"}:
        raise ConfigurationError("app.deployment_mode must be simple or hardened")
    return cast(DeploymentMode, value)


def _control_host(values: Mapping[str, object]) -> str:
    value = _string(values, "control_host", "127.0.0.1", "app")
    if value not in {"127.0.0.1", "::1"}:
        raise ConfigurationError("app.control_host must be 127.0.0.1 or ::1")
    return value


def _remote_id(values: Mapping[str, object], section: str) -> str:
    value = _string(values, "expected_remote_user_id", "", section)
    if (
        not value
        or len(value) > 32
        or value != value.strip()
        or not value.isascii()
        or not value.isdigit()
    ):
        raise ConfigurationError(
            f"{section}.expected_remote_user_id must be an ASCII numeric ID"
        )
    return value


def _username(values: Mapping[str, object], section: str, platform: TargetName) -> str:
    value = _string(values, "expected_username", "", section)
    if not _USERNAME_PATTERNS[platform].fullmatch(value):
        raise ConfigurationError(
            f"{section}.expected_username must be a valid {platform} username without @"
        )
    return value


def _token_env_var(
    values: Mapping[str, object], section: str, platform: TargetName
) -> str:
    value = _string(values, "token_env_var", "", section)
    if not _TOKEN_ENV_PATTERNS[platform].fullmatch(value):
        raise ConfigurationError(
            f"{section}.token_env_var is not an allowlisted {platform} token variable"
        )
    return value


def _timezone(values: Mapping[str, object], section: str) -> str:
    value = _string(values, "timezone", "UTC", section)
    if not _TIMEZONE_RE.fullmatch(value):
        raise ConfigurationError(f"{section}.timezone must be an IANA timezone")
    try:
        ZoneInfo(value)
    except (ZoneInfoNotFoundError, ValueError):
        raise ConfigurationError(
            f"{section}.timezone must be an IANA timezone"
        ) from None
    return value


def _relative_path(base: Path, raw: str, label: str) -> Path:
    if not raw or "\x00" in raw or "\\" in raw:
        raise ConfigurationError(
            f"{label} must be a nonempty relative path using forward slashes"
        )
    candidate_input = Path(raw)
    windows_input = PureWindowsPath(raw)
    if (
        candidate_input.is_absolute()
        or windows_input.is_absolute()
        or windows_input.drive
    ):
        raise ConfigurationError(
            f"{label} must remain inside the configuration directory"
        )
    _reject_symlink_components(base, candidate_input, label)
    candidate = (base / candidate_input).resolve(strict=False)
    _reject_symlink_components(base, candidate_input, label)
    if not candidate.is_relative_to(base):
        raise ConfigurationError(
            f"{label} must remain inside the configuration directory"
        )
    return candidate


def _reject_symlink_components(base: Path, relative: Path, label: str) -> None:
    current = base
    for component in relative.parts:
        current /= component
        try:
            mode = os.lstat(current).st_mode
        except FileNotFoundError:
            continue
        except OSError:
            raise ConfigurationError(
                f"{label} metadata could not be validated"
            ) from None
        if stat.S_ISLNK(mode):
            raise ConfigurationError(f"{label} must not contain a symbolic link")


def _validated_media_base_url(value: str, section: str) -> str:
    label = f"{section}.media_base_url"
    if not value:
        return value
    if any(character.isspace() or ord(character) < 32 for character in value):
        raise ConfigurationError(f"{label} must be a safe public HTTPS base URL")
    if "\\" in value or "?" in value or "#" in value:
        raise ConfigurationError(f"{label} must be a safe public HTTPS base URL")
    try:
        parsed = urlsplit(value)
        _ = parsed.port
    except ValueError as exc:
        raise ConfigurationError(
            f"{label} must be a safe public HTTPS base URL"
        ) from exc
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
    hostname = _ascii_idna_hostname(parsed.hostname, label)
    if _looks_like_noncanonical_ipv4(hostname):
        raise ConfigurationError(f"{label} must not contain an IP literal")
    try:
        ipaddress.ip_address(hostname)
    except ValueError:
        if hostname == "localhost" or hostname.endswith(".localhost"):
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


def _validate_profile_uniqueness(profiles: tuple[ProfileSettings, ...]) -> None:
    seen_profiles: set[str] = set()
    remote_owners: dict[tuple[TargetName, str], str] = {}
    env_owners: dict[str, tuple[str, TargetName]] = {}
    for profile in profiles:
        if profile.profile_id in seen_profiles:
            raise ConfigurationError(f"duplicate profile_id: {profile.profile_id}")
        seen_profiles.add(profile.profile_id)
        for platform in cast(tuple[TargetName, ...], ("x", "instagram")):
            target = profile.x if platform == "x" else profile.instagram
            if target is None:
                continue
            identity = (platform, target.expected_remote_user_id)
            if identity in remote_owners:
                raise ConfigurationError(
                    "remote identity is assigned to multiple profiles: "
                    f"{platform} {target.expected_remote_user_id}"
                )
            remote_owners[identity] = profile.profile_id
            if target.token_env_var in env_owners:
                raise ConfigurationError(
                    "token environment variable is assigned to multiple targets: "
                    f"{target.token_env_var}"
                )
            env_owners[target.token_env_var] = (profile.profile_id, platform)


def _validate_path_separation(
    app: AppSettings, profiles: tuple[ProfileSettings, ...]
) -> None:
    directories: dict[str, Path] = {"app.state_directory": app.state_directory}
    for profile in profiles:
        prefix = f"profiles[{profile.profile_id}]"
        directories[f"{prefix}.account_root"] = profile.account_root
        if profile.instagram is not None:
            directories[f"{prefix}.instagram.media_directory"] = (
                profile.instagram.media_directory
            )
    items = tuple(directories.items())
    for index, (left_name, left_path) in enumerate(items):
        for right_name, right_path in items[index + 1 :]:
            if _paths_overlap(left_path, right_path):
                raise ConfigurationError(
                    f"runtime directories must not overlap: {left_name}, {right_name}"
                )

    for name, directory in items:
        if _paths_equal(app.log_file, directory) or _path_is_relative_to(
            directory, app.log_file
        ):
            raise ConfigurationError(f"app.log_file must not equal or contain {name}")
        if name != "app.state_directory" and _path_is_relative_to(
            app.log_file, directory
        ):
            raise ConfigurationError(f"app.log_file must not overlap {name}")

    control_files = {
        "app.agent_capability_file": app.agent_capability_file,
        "app.operator_verifier_file": app.operator_verifier_file,
        "app.bootstrap_file": app.bootstrap_file,
        "app.endpoint_record_file": app.endpoint_record_file,
    }
    for label, path in control_files.items():
        if not _path_is_relative_to(path, app.state_directory) or _paths_equal(
            path, app.state_directory
        ):
            raise ConfigurationError(f"{label} must be inside app.state_directory")
    normalized = [_portable_path_parts(path) for path in control_files.values()]
    if len(normalized) != len(set(normalized)):
        raise ConfigurationError("control files must be distinct")
    if any(_paths_equal(app.log_file, path) for path in control_files.values()):
        raise ConfigurationError("app.log_file must not equal a control file")


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
    return tuple(unicodedata.normalize("NFD", part).casefold() for part in path.parts)


__all__ = [
    "AppSettings",
    "ConfigurationError",
    "InstagramSettings",
    "LocalSettings",
    "ProfileSettings",
    "PublishingCredentials",
    "SecretValue",
    "XSettings",
    "load_local_settings",
    "validate_publishing_credentials",
]
