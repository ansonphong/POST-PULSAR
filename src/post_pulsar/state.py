"""Transactional SQLite persistence for POST PULSAR delivery state."""

from __future__ import annotations

import hashlib
import json
import re
import secrets
import sqlite3
from collections.abc import Callable, Iterable, Iterator, Mapping, Sequence
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import UTC, date, datetime
from pathlib import Path, PurePosixPath
from typing import Final, Literal, TypeAlias, cast
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

Platform: TypeAlias = Literal["x", "instagram"]
SourceBucket: TypeAlias = Literal["QUEUE", "RANDOM", "REELS"]
BundleStatus: TypeAlias = Literal["active", "blocked", "archiving", "archived"]
DeliveryStatus: TypeAlias = Literal[
    "pending", "in_flight", "published", "failed", "ambiguous"
]
DeliveryPhase: TypeAlias = Literal[
    "preparing", "processing", "ready", "final_dispatch_started"
]
ScheduleRunState: TypeAlias = Literal[
    "queued",
    "due",
    "missed",
    "dispatching",
    "no_content",
    "completed",
    "failed",
    "ambiguous",
]

SCHEMA_VERSION: Final = 1
_APPLICATION_ID: Final = 0x50505352
_PLATFORMS: Final = frozenset({"x", "instagram"})
_BUCKETS: Final = frozenset({"QUEUE", "RANDOM", "REELS"})
_BUNDLE_STATES: Final = frozenset({"active", "blocked", "archiving", "archived"})
_PHASE_ORDER: Final = {
    "preparing": 0,
    "processing": 1,
    "ready": 2,
    "final_dispatch_started": 3,
}
_SCHEDULE_RUN_TRANSITIONS: Final = {
    "queued": frozenset({"due", "missed"}),
    "due": frozenset({"dispatching", "no_content"}),
    "dispatching": frozenset({"completed", "failed", "ambiguous"}),
    "missed": frozenset(),
    "no_content": frozenset(),
    "completed": frozenset(),
    "failed": frozenset(),
    "ambiguous": frozenset(),
}
_ARTIFACT_KINDS: Final = frozenset(
    {
        "x_media_id",
        "instagram_child_container",
        "instagram_parent_container",
        "staged_private",
        "staged_public",
    }
)
_PROCESSING_METADATA_KEYS: Final = frozenset(
    {
        "state",
        "progress_percent",
        "check_after_seconds",
        "error_code",
        "next_segment_index",
        "source_sha256",
        "media_type",
    }
)
_WARNING_CODES: Final = frozenset(
    {
        "instagram_alt_text_unsupported",
        "instagram_image_normalized",
        "media_cleanup_deferred",
        "public_url_verification_limited",
    }
)
_EVENT_TYPES: Final = frozenset(
    {
        "operator_retry_enabled",
        "delivery_failed",
        "delivery_ambiguous",
        "fingerprint_drift",
        "profile_root_drift",
        "bundle_blocked",
        "bundle_archiving",
        "bundle_archived",
        "warning",
    }
)
_REQUEST_ACTIONS: Final = frozenset(
    {
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
    }
)
_ADMISSION_PHASES: Final = (
    "started",
    "copying",
    "ready_installed",
    "installed",
    "rolled_back",
)
_ADMISSION_MEMBER_PHASES: Final = frozenset({"planned", "copied", "verified"})
_ADMISSION_MEMBER_PHASE_ORDER: Final = {
    "planned": 0,
    "copied": 1,
    "verified": 2,
}
_BUNDLE_REQUEST_ACTIONS: Final = frozenset(
    {"enqueue", "publish_now", "cancel", "delete", "retry", "reconcile"}
)
_SCHEDULE_REQUEST_ACTIONS: Final = frozenset(
    {"schedule_update", "schedule_enable", "schedule_disable"}
)
_PROFILE_REQUEST_ACTIONS: Final = frozenset(
    {"admit_draft", "pause", "resume", "schedule_create"}
)
_ALWAYS_CONFIRMED_ACTIONS: Final = frozenset(
    {
        "admit_draft",
        "enqueue",
        "run_now",
        "publish_now",
        "resume",
        "schedule_enable",
        "cancel",
        "delete",
        "retry",
        "reconcile",
    }
)
_IDENTIFIER_RE: Final = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.:-]{0,127}\Z")
_PROFILE_RE: Final = re.compile(r"[a-z0-9][a-z0-9-]{0,31}\Z")
_BUNDLE_RE: Final = re.compile(r"[A-Za-z0-9][A-Za-z0-9_-]{0,63}\Z")
_SCHEDULE_RE: Final = re.compile(r"[a-z0-9][a-z0-9-]{0,63}\Z")
_SHA256_RE: Final = re.compile(r"[0-9a-f]{64}\Z")


class StateError(RuntimeError):
    """Base class for safe persistent-state errors."""


class MigrationRequiredError(StateError):
    """The database is not the current supported schema."""


class StateValidationError(StateError):
    """An input cannot safely enter durable state."""


class ConflictError(StateError):
    """Stored immutable or revisioned state conflicts with a request."""


class TransitionError(StateError):
    """A requested state transition is illegal."""


@dataclass(frozen=True, slots=True)
class ProfileTargetSnapshot:
    """Current non-secret target identity for one profile."""

    platform: Platform
    expected_remote_user_id: str
    expected_username: str
    token_env_var: str
    request_settings: Mapping[str, object]


@dataclass(frozen=True, slots=True)
class TargetSnapshot:
    """Immutable request semantics captured when a bundle is admitted."""

    platform: Platform
    expected_remote_user_id: str
    expected_username: str
    token_env_var: str
    api_version: str
    adapter_version: int
    request_settings: Mapping[str, object]


@dataclass(frozen=True, slots=True)
class BundleFileSnapshot:
    """One exact semantic member of an admitted bundle."""

    relative_name: str
    role: str
    ordinal: int | None
    media_kind: str | None
    mime_type: str | None
    size_bytes: int
    sha256: str
    archived: bool = False


@dataclass(frozen=True, slots=True)
class ProfileRecord:
    profile_id: str
    account_root: Path
    config_hash: str
    revision: int


@dataclass(frozen=True, slots=True)
class BundleRecord:
    bundle_key: int
    profile_id: str
    bundle_id: str
    fingerprint: str
    source_bucket: SourceBucket
    profile_root_snapshot: Path
    status: BundleStatus
    revision: int
    archive_path: str | None
    ready_marker_name: str
    ready_marker_archived: bool


@dataclass(frozen=True, slots=True)
class DeliveryRecord:
    bundle_key: int
    platform: Platform
    status: DeliveryStatus
    phase: DeliveryPhase | None
    attempt_count: int
    consecutive_failures: int
    safe_to_retry: bool
    next_attempt_at: datetime | None
    remote_id: str | None
    error_code: str | None
    error_message: str | None
    revision: int


@dataclass(frozen=True, slots=True)
class ArtifactRecord:
    bundle_key: int
    platform: Platform
    attempt_count: int
    kind: str
    ordinal: int
    external_id: str | None
    relative_path: str | None
    sha256: str | None
    expires_at: datetime | None
    processing_metadata: Mapping[str, object]


@dataclass(frozen=True, slots=True)
class ScheduleRecord:
    schedule_key: int
    profile_id: str
    schedule_id: str
    bucket: SourceBucket
    timezone: str
    weekdays: tuple[int, ...]
    local_time: str
    misfire_grace_seconds: int
    enabled: bool
    config_hash: str
    revision: int


@dataclass(frozen=True, slots=True)
class ScheduleRunRecord:
    run_id: int
    schedule_key: int
    bundle_key: int | None
    local_date: str
    scheduled_at: datetime
    utc_offset_minutes: int
    schedule_hash: str
    state: ScheduleRunState
    revision: int


@dataclass(frozen=True, slots=True)
class PauseRecord:
    paused: bool
    revision: int


@dataclass(frozen=True, slots=True)
class RunRequestRecord:
    request_id: int
    profile_id: str
    action: str
    arguments: Mapping[str, object]
    idempotency_key: str
    expected_revision: int
    bundle_key: int | None
    schedule_key: int | None
    intent_id: str | None
    status: str
    result: Mapping[str, object] | None
    revision: int


@dataclass(frozen=True, slots=True)
class ConfirmationIntentRecord:
    intent_id: str
    action: str
    arguments: Mapping[str, object]
    profile_id: str
    bundle_key: int | None
    schedule_key: int | None
    resource_revision: int
    fingerprint: str | None
    consequence: str
    expires_at: datetime
    state: str
    revision: int


@dataclass(frozen=True, slots=True)
class AdmissionRecord:
    journal_id: int
    profile_id: str
    bucket: SourceBucket
    bundle_id: str
    fingerprint: str
    source_path: str
    destination_path: str
    intent_id: str | None
    phase: str
    revision: int


@dataclass(frozen=True, slots=True)
class AdmissionMemberRecord:
    journal_id: int
    relative_name: str
    sha256: str
    size_bytes: int
    phase: str


_SCHEMA = """
CREATE TABLE IF NOT EXISTS profiles (
    profile_id TEXT PRIMARY KEY COLLATE NOCASE,
    account_root TEXT NOT NULL,
    config_hash TEXT NOT NULL,
    revision INTEGER NOT NULL DEFAULT 1 CHECK (revision > 0),
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS profile_targets (
    profile_id TEXT NOT NULL REFERENCES profiles(profile_id),
    platform TEXT NOT NULL CHECK (platform IN ('x', 'instagram')),
    expected_remote_user_id TEXT NOT NULL,
    expected_username TEXT NOT NULL,
    token_env_var TEXT NOT NULL,
    request_settings_json TEXT NOT NULL,
    request_settings_sha256 TEXT NOT NULL,
    PRIMARY KEY (profile_id, platform),
    UNIQUE (platform, expected_remote_user_id),
    UNIQUE (token_env_var)
);
CREATE TABLE IF NOT EXISTS bundles (
    bundle_key INTEGER PRIMARY KEY AUTOINCREMENT,
    profile_id TEXT NOT NULL REFERENCES profiles(profile_id),
    bundle_id TEXT NOT NULL COLLATE NOCASE,
    fingerprint TEXT NOT NULL,
    source_bucket TEXT NOT NULL CHECK (source_bucket IN ('QUEUE', 'RANDOM', 'REELS')),
    profile_root_snapshot TEXT NOT NULL,
    ready_marker_name TEXT NOT NULL DEFAULT '.ready',
    ready_marker_archived INTEGER NOT NULL DEFAULT 0 CHECK (ready_marker_archived IN (0, 1)),
    status TEXT NOT NULL CHECK (status IN ('active', 'blocked', 'archiving', 'archived')),
    revision INTEGER NOT NULL DEFAULT 1 CHECK (revision > 0),
    archive_path TEXT,
    block_reason TEXT,
    claimed_by_type TEXT,
    claimed_by_id TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    UNIQUE (profile_id, bundle_id)
);
CREATE UNIQUE INDEX IF NOT EXISTS one_unfinished_bundle_per_profile
ON bundles(profile_id) WHERE status IN ('active', 'blocked', 'archiving');
CREATE TABLE IF NOT EXISTS target_snapshots (
    bundle_key INTEGER NOT NULL REFERENCES bundles(bundle_key),
    profile_id TEXT NOT NULL,
    profile_root TEXT NOT NULL,
    platform TEXT NOT NULL CHECK (platform IN ('x', 'instagram')),
    expected_remote_user_id TEXT NOT NULL,
    expected_username TEXT NOT NULL,
    token_env_var TEXT NOT NULL,
    api_version TEXT NOT NULL,
    adapter_version INTEGER NOT NULL CHECK (adapter_version > 0),
    request_settings_json TEXT NOT NULL,
    request_settings_sha256 TEXT NOT NULL,
    snapshot_sha256 TEXT NOT NULL,
    PRIMARY KEY (bundle_key, platform)
);
CREATE TABLE IF NOT EXISTS bundle_files (
    bundle_key INTEGER NOT NULL REFERENCES bundles(bundle_key),
    relative_name TEXT NOT NULL COLLATE NOCASE,
    role TEXT NOT NULL,
    ordinal INTEGER,
    media_kind TEXT,
    mime_type TEXT,
    size_bytes INTEGER NOT NULL CHECK (size_bytes >= 0),
    sha256 TEXT NOT NULL,
    archived INTEGER NOT NULL DEFAULT 0 CHECK (archived IN (0, 1)),
    PRIMARY KEY (bundle_key, relative_name)
);
CREATE TABLE IF NOT EXISTS deliveries (
    bundle_key INTEGER NOT NULL,
    platform TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('pending', 'in_flight', 'published', 'failed', 'ambiguous')),
    phase TEXT CHECK (phase IS NULL OR phase IN ('preparing', 'processing', 'ready', 'final_dispatch_started')),
    attempt_count INTEGER NOT NULL DEFAULT 0 CHECK (attempt_count >= 0),
    consecutive_failures INTEGER NOT NULL DEFAULT 0 CHECK (consecutive_failures >= 0),
    safe_to_retry INTEGER NOT NULL DEFAULT 1 CHECK (safe_to_retry IN (0, 1)),
    next_attempt_at TEXT,
    claim_token TEXT,
    remote_id TEXT,
    error_code TEXT,
    error_message TEXT,
    revision INTEGER NOT NULL DEFAULT 1 CHECK (revision > 0),
    updated_at TEXT NOT NULL,
    PRIMARY KEY (bundle_key, platform),
    FOREIGN KEY (bundle_key, platform) REFERENCES target_snapshots(bundle_key, platform)
);
CREATE TABLE IF NOT EXISTS delivery_artifacts (
    bundle_key INTEGER NOT NULL,
    platform TEXT NOT NULL,
    attempt_count INTEGER NOT NULL CHECK (attempt_count > 0),
    kind TEXT NOT NULL CHECK (kind IN ('x_media_id', 'instagram_child_container', 'instagram_parent_container', 'staged_private', 'staged_public')),
    ordinal INTEGER NOT NULL CHECK (ordinal >= 0),
    external_id TEXT,
    relative_path TEXT,
    sha256 TEXT,
    expires_at TEXT,
    processing_metadata_json TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    CHECK (
        (kind = 'x_media_id' AND platform = 'x' AND external_id IS NOT NULL AND relative_path IS NULL AND sha256 IS NULL AND expires_at IS NOT NULL)
        OR (kind IN ('instagram_child_container', 'instagram_parent_container') AND platform = 'instagram' AND external_id IS NOT NULL AND relative_path IS NULL AND sha256 IS NULL AND expires_at IS NOT NULL)
        OR (kind = 'staged_private' AND external_id IS NULL AND relative_path IS NOT NULL AND sha256 IS NOT NULL)
        OR (kind = 'staged_public' AND platform = 'instagram' AND external_id IS NULL AND relative_path IS NOT NULL AND sha256 IS NOT NULL)
    ),
    PRIMARY KEY (bundle_key, platform, attempt_count, kind, ordinal),
    FOREIGN KEY (bundle_key, platform) REFERENCES deliveries(bundle_key, platform)
);
CREATE UNIQUE INDEX IF NOT EXISTS delivery_artifact_external_identity
ON delivery_artifacts(platform, external_id) WHERE external_id IS NOT NULL;
CREATE UNIQUE INDEX IF NOT EXISTS delivery_remote_identity
ON deliveries(platform, remote_id) WHERE remote_id IS NOT NULL;
CREATE TABLE IF NOT EXISTS events (
    event_id INTEGER PRIMARY KEY AUTOINCREMENT,
    occurred_at TEXT NOT NULL,
    bundle_key INTEGER REFERENCES bundles(bundle_key),
    platform TEXT,
    event_type TEXT NOT NULL,
    event_code TEXT NOT NULL,
    safe_detail_json TEXT NOT NULL,
    dedupe_key TEXT NOT NULL UNIQUE
);
CREATE TABLE IF NOT EXISTS schedules (
    schedule_key INTEGER PRIMARY KEY AUTOINCREMENT,
    profile_id TEXT NOT NULL REFERENCES profiles(profile_id),
    schedule_id TEXT NOT NULL COLLATE NOCASE,
    bucket TEXT NOT NULL CHECK (bucket IN ('QUEUE', 'RANDOM', 'REELS')),
    timezone TEXT NOT NULL,
    weekdays_json TEXT NOT NULL,
    local_time TEXT NOT NULL,
    misfire_grace_seconds INTEGER NOT NULL CHECK (misfire_grace_seconds >= 0),
    enabled INTEGER NOT NULL CHECK (enabled IN (0, 1)),
    config_hash TEXT NOT NULL,
    revision INTEGER NOT NULL DEFAULT 1 CHECK (revision > 0),
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    UNIQUE (profile_id, schedule_id)
);
CREATE TABLE IF NOT EXISTS schedule_runs (
    run_id INTEGER PRIMARY KEY AUTOINCREMENT,
    schedule_key INTEGER NOT NULL REFERENCES schedules(schedule_key),
    bundle_key INTEGER REFERENCES bundles(bundle_key),
    local_date TEXT NOT NULL,
    scheduled_at TEXT NOT NULL,
    utc_offset_minutes INTEGER NOT NULL,
    schedule_hash TEXT NOT NULL,
    state TEXT NOT NULL CHECK (state IN ('queued', 'due', 'missed', 'dispatching', 'no_content', 'completed', 'failed', 'ambiguous')),
    revision INTEGER NOT NULL DEFAULT 1 CHECK (revision > 0),
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    UNIQUE (schedule_key, local_date)
);
CREATE TABLE IF NOT EXISTS run_requests (
    request_id INTEGER PRIMARY KEY AUTOINCREMENT,
    profile_id TEXT NOT NULL REFERENCES profiles(profile_id),
    action TEXT NOT NULL,
    arguments_json TEXT NOT NULL,
    request_sha256 TEXT NOT NULL,
    idempotency_key TEXT NOT NULL UNIQUE,
    expected_revision INTEGER NOT NULL,
    bundle_key INTEGER REFERENCES bundles(bundle_key),
    schedule_key INTEGER REFERENCES schedules(schedule_key),
    intent_id TEXT,
    status TEXT NOT NULL CHECK (status IN ('queued', 'claimed', 'completed', 'failed')),
    worker_token TEXT,
    result_json TEXT,
    revision INTEGER NOT NULL DEFAULT 1 CHECK (revision > 0),
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS pause_state (
    singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
    paused INTEGER NOT NULL CHECK (paused IN (0, 1)),
    revision INTEGER NOT NULL CHECK (revision > 0),
    updated_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS confirmation_intents (
    intent_id TEXT PRIMARY KEY,
    action TEXT NOT NULL,
    arguments_json TEXT NOT NULL,
    binding_sha256 TEXT NOT NULL,
    profile_id TEXT NOT NULL REFERENCES profiles(profile_id),
    bundle_key INTEGER REFERENCES bundles(bundle_key),
    schedule_key INTEGER REFERENCES schedules(schedule_key),
    resource_revision INTEGER NOT NULL,
    fingerprint TEXT,
    consequence TEXT NOT NULL,
    nonce TEXT NOT NULL UNIQUE,
    expires_at TEXT NOT NULL,
    state TEXT NOT NULL CHECK (state IN ('pending', 'approved', 'consumed', 'expired')),
    revision INTEGER NOT NULL DEFAULT 1 CHECK (revision > 0),
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS admission_journals (
    journal_id INTEGER PRIMARY KEY AUTOINCREMENT,
    profile_id TEXT NOT NULL REFERENCES profiles(profile_id),
    bucket TEXT NOT NULL CHECK (bucket IN ('QUEUE', 'RANDOM', 'REELS')),
    bundle_id TEXT NOT NULL COLLATE NOCASE,
    fingerprint TEXT NOT NULL,
    source_path TEXT NOT NULL,
    destination_path TEXT NOT NULL,
    intent_id TEXT REFERENCES confirmation_intents(intent_id),
    phase TEXT NOT NULL CHECK (phase IN ('started', 'copying', 'ready_installed', 'installed', 'rolled_back')),
    revision INTEGER NOT NULL DEFAULT 1 CHECK (revision > 0),
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    UNIQUE (profile_id, bucket, bundle_id),
    UNIQUE (profile_id, fingerprint)
);
CREATE TABLE IF NOT EXISTS admission_members (
    journal_id INTEGER NOT NULL REFERENCES admission_journals(journal_id),
    relative_name TEXT NOT NULL COLLATE NOCASE,
    sha256 TEXT NOT NULL,
    size_bytes INTEGER NOT NULL CHECK (size_bytes >= 0),
    phase TEXT NOT NULL CHECK (phase IN ('planned', 'copied', 'verified')),
    updated_at TEXT NOT NULL,
    PRIMARY KEY (journal_id, relative_name)
);
CREATE TABLE IF NOT EXISTS selection_counters (
    profile_id TEXT PRIMARY KEY REFERENCES profiles(profile_id),
    lifetime_counter INTEGER NOT NULL CHECK (lifetime_counter >= 0),
    updated_at TEXT NOT NULL
);
CREATE TRIGGER IF NOT EXISTS target_snapshots_no_update
BEFORE UPDATE ON target_snapshots BEGIN SELECT RAISE(ABORT, 'immutable target snapshot'); END;
CREATE TRIGGER IF NOT EXISTS target_snapshots_no_delete
BEFORE DELETE ON target_snapshots BEGIN SELECT RAISE(ABORT, 'immutable target snapshot'); END;
DROP TRIGGER IF EXISTS bundle_files_no_update;
CREATE TRIGGER bundle_files_no_update
BEFORE UPDATE ON bundle_files
WHEN NOT (
    NEW.bundle_key IS OLD.bundle_key
    AND NEW.relative_name IS OLD.relative_name
    AND NEW.role IS OLD.role
    AND NEW.ordinal IS OLD.ordinal
    AND NEW.media_kind IS OLD.media_kind
    AND NEW.mime_type IS OLD.mime_type
    AND NEW.size_bytes IS OLD.size_bytes
    AND NEW.sha256 IS OLD.sha256
    AND NEW.archived >= OLD.archived
)
BEGIN SELECT RAISE(ABORT, 'immutable bundle file'); END;
CREATE TRIGGER IF NOT EXISTS bundle_files_no_delete
BEFORE DELETE ON bundle_files BEGIN SELECT RAISE(ABORT, 'immutable bundle file'); END;
CREATE TRIGGER IF NOT EXISTS admission_members_identity_no_update
BEFORE UPDATE ON admission_members
WHEN NEW.journal_id != OLD.journal_id
 OR NEW.relative_name != OLD.relative_name
 OR NEW.sha256 != OLD.sha256
 OR NEW.size_bytes != OLD.size_bytes
BEGIN SELECT RAISE(ABORT, 'immutable admission member identity'); END;
CREATE TRIGGER IF NOT EXISTS admission_members_phase_monotonic
BEFORE UPDATE OF phase ON admission_members
WHEN NOT (
    (OLD.phase = 'planned' AND NEW.phase = 'copied')
 OR (OLD.phase = 'copied' AND NEW.phase = 'verified')
)
BEGIN SELECT RAISE(ABORT, 'illegal admission member transition'); END;
CREATE TRIGGER IF NOT EXISTS admission_members_no_delete
BEFORE DELETE ON admission_members
BEGIN SELECT RAISE(ABORT, 'immutable admission member'); END;
"""

_REQUIRED_TABLES: Final = frozenset(
    {
        "profiles",
        "profile_targets",
        "bundles",
        "target_snapshots",
        "bundle_files",
        "deliveries",
        "delivery_artifacts",
        "events",
        "schedules",
        "schedule_runs",
        "run_requests",
        "pause_state",
        "confirmation_intents",
        "admission_journals",
        "admission_members",
        "selection_counters",
    }
)


def _schema_manifest_digest(connection: sqlite3.Connection) -> str:
    """Return a canonical digest covering tables, constraints, indexes, and triggers."""

    rows = connection.execute(
        "SELECT type, name, tbl_name, sql FROM sqlite_master "
        "WHERE type IN ('table', 'index', 'trigger') "
        "AND name NOT LIKE 'sqlite_%' ORDER BY type, name"
    )
    manifest = [
        (
            str(row[0]),
            str(row[1]),
            str(row[2]),
            " ".join(str(row[3] or "").split()),
        )
        for row in rows
    ]
    return hashlib.sha256(
        json.dumps(manifest, ensure_ascii=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()


def _expected_schema_manifest_digest() -> str:
    connection = sqlite3.connect(":memory:")
    try:
        connection.executescript(_SCHEMA)
        return _schema_manifest_digest(connection)
    finally:
        connection.close()


class StateRepository:
    """The only SQL-owning repository for durable application state."""

    def __init__(
        self,
        database_path: str | Path,
        *,
        clock: Callable[[], datetime] | None = None,
        secret_values: Iterable[str] = (),
        busy_timeout_ms: int = 5000,
    ) -> None:
        if busy_timeout_ms < 1000:
            raise StateValidationError("busy timeout must be at least 1000 milliseconds")
        self.path = Path(database_path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._clock = clock or (lambda: datetime.now(UTC))
        self._secrets = tuple(value for value in secret_values if value)
        self._connection = sqlite3.connect(
            self.path,
            isolation_level=None,
            timeout=busy_timeout_ms / 1000,
        )
        self._connection.row_factory = sqlite3.Row
        try:
            self._connection.execute("PRAGMA foreign_keys = ON")
            self._connection.execute(f"PRAGMA busy_timeout = {busy_timeout_ms}")
            self._initialize_schema()
            try:
                self._connection.execute("PRAGMA journal_mode = WAL").fetchone()
            except sqlite3.OperationalError:
                return
        except BaseException:
            self._connection.close()
            raise

    def __enter__(self) -> StateRepository:
        return self

    def __exit__(self, *_args: object) -> None:
        self.close()

    def close(self) -> None:
        self._connection.close()

    @property
    def schema_version(self) -> int:
        return int(self._connection.execute("PRAGMA user_version").fetchone()[0])

    @property
    def foreign_keys_enabled(self) -> bool:
        return bool(self._connection.execute("PRAGMA foreign_keys").fetchone()[0])

    @property
    def busy_timeout_ms(self) -> int:
        return int(self._connection.execute("PRAGMA busy_timeout").fetchone()[0])

    @property
    def journal_mode(self) -> str:
        return str(self._connection.execute("PRAGMA journal_mode").fetchone()[0]).lower()

    def _initialize_schema(self) -> None:
        try:
            version = self.schema_version
            application_id = int(
                self._connection.execute("PRAGMA application_id").fetchone()[0]
            )
            tables = {
                str(row[0])
                for row in self._connection.execute(
                    "SELECT name FROM sqlite_master WHERE type = 'table' "
                    "AND name NOT LIKE 'sqlite_%'"
                )
            }
            if version == 0 and tables:
                raise MigrationRequiredError(
                    "migration required: existing unversioned database is unsupported"
                )
            if version not in {0, SCHEMA_VERSION}:
                raise MigrationRequiredError(
                    f"migration required: unsupported database schema version {version}"
                )
            if application_id not in {0, _APPLICATION_ID}:
                raise MigrationRequiredError(
                    "migration required: database application identity is unsupported"
                )
            if version == SCHEMA_VERSION:
                if application_id != _APPLICATION_ID:
                    raise MigrationRequiredError(
                        "migration required: database application identity is unsupported"
                    )
                if not _REQUIRED_TABLES.issubset(tables):
                    raise MigrationRequiredError(
                        "migration required: current-version database schema is incomplete"
                    )
                if _schema_manifest_digest(
                    self._connection
                ) != _expected_schema_manifest_digest():
                    raise MigrationRequiredError(
                        "migration required: current-version database schema is not canonical"
                    )
                return

            self._connection.executescript("BEGIN IMMEDIATE;\n" + _SCHEMA)
            now = self._now_text()
            self._connection.execute(
                "INSERT INTO pause_state(singleton, paused, revision, updated_at) "
                "VALUES (1, 0, 1, ?)",
                (now,),
            )
            self._connection.execute(f"PRAGMA application_id = {_APPLICATION_ID}")
            self._connection.execute(f"PRAGMA user_version = {SCHEMA_VERSION}")
            self._connection.execute("COMMIT")
            if _schema_manifest_digest(
                self._connection
            ) != _expected_schema_manifest_digest():
                raise StateError("new database schema failed canonical validation")
        except (MigrationRequiredError, StateError):
            raise
        except sqlite3.DatabaseError:
            raise MigrationRequiredError(
                "migration required: database schema could not be validated safely"
            ) from None

    @contextmanager
    def _transaction(self) -> Iterator[None]:
        self._connection.execute("BEGIN IMMEDIATE")
        try:
            yield
        except BaseException:
            self._connection.execute("ROLLBACK")
            raise
        else:
            self._connection.execute("COMMIT")

    def register_profile(
        self,
        profile_id: str,
        account_root: str | Path,
        targets: Sequence[ProfileTargetSnapshot],
        *,
        config_hash: str,
        expected_revision: int | None = None,
    ) -> ProfileRecord:
        _validate_profile_id(profile_id)
        root = Path(account_root).resolve(strict=False)
        if not root.is_absolute():
            raise StateValidationError("profile account root must be absolute")
        _validate_sha256(config_hash, "profile config hash")
        normalized_targets = self._normalize_profile_targets(targets)
        now = self._now_text()
        try:
            with self._transaction():
                existing = self._connection.execute(
                    "SELECT * FROM profiles WHERE profile_id = ?", (profile_id,)
                ).fetchone()
                if existing is None:
                    self._assert_target_ownership_available(
                        profile_id, normalized_targets
                    )
                    self._connection.execute(
                        "INSERT INTO profiles(profile_id, account_root, config_hash, "
                        "created_at, updated_at) VALUES (?, ?, ?, ?, ?)",
                        (profile_id, str(root), config_hash, now, now),
                    )
                    self._replace_profile_targets(profile_id, normalized_targets)
                else:
                    current_root = str(existing["account_root"])
                    stored_targets = self._stored_profile_targets(profile_id)
                    targets_changed = stored_targets != normalized_targets
                    changed = (
                        current_root != str(root)
                        or str(existing["config_hash"]) != config_hash
                        or targets_changed
                    )
                    if not changed:
                        return self._profile_from_row(existing)
                    protected_work = self._profile_has_protected_work(profile_id)
                    if current_root != str(root) and protected_work:
                        raise ConflictError(
                            "profile root drift is blocked while durable work references it"
                        )
                    if targets_changed and protected_work:
                        raise ConflictError(
                            "profile targets cannot change while durable work references them"
                        )
                    if expected_revision != int(existing["revision"]):
                        raise ConflictError("profile revision conflict")
                    self._assert_target_ownership_available(
                        profile_id, normalized_targets
                    )
                    self._connection.execute(
                        "UPDATE profiles SET account_root = ?, config_hash = ?, "
                        "revision = revision + 1, updated_at = ? WHERE profile_id = ?",
                        (str(root), config_hash, now, profile_id),
                    )
                    self._connection.execute(
                        "DELETE FROM profile_targets WHERE profile_id = ?", (profile_id,)
                    )
                    self._replace_profile_targets(profile_id, normalized_targets)
        except sqlite3.IntegrityError as exc:
            message = str(exc).lower()
            if "expected_remote_user_id" in message:
                raise ConflictError(
                    "remote identity is assigned to multiple profiles"
                ) from None
            if "token_env_var" in message:
                raise ConflictError(
                    "token environment variable is assigned to multiple targets"
                ) from None
            raise ConflictError("profile identity conflicts with durable state") from None
        return self.get_profile(profile_id)

    def get_profile(self, profile_id: str) -> ProfileRecord:
        row = self._connection.execute(
            "SELECT * FROM profiles WHERE profile_id = ?", (profile_id,)
        ).fetchone()
        if row is None:
            raise StateValidationError("unknown profile")
        return self._profile_from_row(row)

    def _normalize_profile_targets(
        self, targets: Sequence[ProfileTargetSnapshot]
    ) -> tuple[tuple[str, str, str, str, str, str], ...]:
        normalized: list[tuple[str, str, str, str, str, str]] = []
        seen: set[str] = set()
        for target in targets:
            platform = _validate_platform(target.platform)
            if platform in seen:
                raise StateValidationError("profile has duplicate platform targets")
            seen.add(platform)
            _validate_identifier(target.expected_remote_user_id, "remote identity")
            _validate_identifier(target.expected_username, "expected username")
            _validate_identifier(target.token_env_var, "token environment reference")
            settings_json = self._safe_json(target.request_settings)
            normalized.append(
                (
                    platform,
                    target.expected_remote_user_id,
                    target.expected_username,
                    target.token_env_var,
                    settings_json,
                    _sha256_text(settings_json),
                )
            )
        return tuple(sorted(normalized))

    def _replace_profile_targets(
        self,
        profile_id: str,
        targets: tuple[tuple[str, str, str, str, str, str], ...],
    ) -> None:
        self._connection.executemany(
            "INSERT INTO profile_targets(profile_id, platform, "
            "expected_remote_user_id, expected_username, token_env_var, "
            "request_settings_json, request_settings_sha256) "
            "VALUES (?, ?, ?, ?, ?, ?, ?)",
            ((profile_id, *target) for target in targets),
        )

    def _stored_profile_targets(
        self, profile_id: str
    ) -> tuple[tuple[str, str, str, str, str, str], ...]:
        rows = self._connection.execute(
            "SELECT platform, expected_remote_user_id, expected_username, "
            "token_env_var, request_settings_json, request_settings_sha256 "
            "FROM profile_targets WHERE profile_id = ? ORDER BY platform",
            (profile_id,),
        )
        return tuple(
            (
                str(row[0]),
                str(row[1]),
                str(row[2]),
                str(row[3]),
                str(row[4]),
                str(row[5]),
            )
            for row in rows
        )

    def _profile_has_protected_work(self, profile_id: str) -> bool:
        row = self._connection.execute(
            "SELECT 1 FROM bundles b LEFT JOIN deliveries d ON d.bundle_key = b.bundle_key "
            "WHERE b.profile_id = ? AND (b.status IN ('active', 'blocked', 'archiving') "
            "OR d.status = 'ambiguous') LIMIT 1",
            (profile_id,),
        ).fetchone()
        return row is not None

    def _assert_target_ownership_available(
        self,
        profile_id: str,
        targets: tuple[tuple[str, str, str, str, str, str], ...],
    ) -> None:
        for platform, remote_id, *_rest in targets:
            protected = self._connection.execute(
                "SELECT 1 FROM target_snapshots ts "
                "JOIN bundles b ON b.bundle_key = ts.bundle_key "
                "LEFT JOIN deliveries d ON d.bundle_key = ts.bundle_key "
                "AND d.platform = ts.platform "
                "WHERE ts.platform = ? AND ts.expected_remote_user_id = ? "
                "AND ts.profile_id != ? "
                "AND (b.status IN ('active', 'blocked', 'archiving') "
                "OR d.status = 'ambiguous') LIMIT 1",
                (platform, remote_id, profile_id),
            ).fetchone()
            if protected is not None:
                raise ConflictError(
                    "remote identity is retained by unfinished durable work"
                )

    def add_bundle(
        self,
        *,
        profile_id: str,
        bundle_id: str,
        fingerprint: str,
        source_bucket: SourceBucket,
        files: Sequence[BundleFileSnapshot],
        targets: Sequence[TargetSnapshot],
    ) -> int:
        _validate_bundle_id(bundle_id)
        _validate_sha256(fingerprint, "bundle fingerprint")
        bucket = _validate_bucket(source_bucket)
        if not targets:
            raise StateValidationError("at least one target snapshot is required")
        if not files:
            raise StateValidationError("at least one exact bundle file is required")
        normalized_files = self._normalize_bundle_files(files)
        normalized_targets = self._normalize_target_snapshots(targets)
        now = self._now_text()
        with self._transaction():
            profile = self._connection.execute(
                "SELECT * FROM profiles WHERE profile_id = ?", (profile_id,)
            ).fetchone()
            if profile is None:
                raise StateValidationError("unknown profile")
            duplicate = self._connection.execute(
                "SELECT 1 FROM bundles WHERE profile_id = ? AND bundle_id = ?",
                (profile_id, bundle_id),
            ).fetchone()
            if duplicate is not None:
                raise ConflictError("bundle ID has already been used for this profile")
            active = self._connection.execute(
                "SELECT 1 FROM bundles WHERE profile_id = ? "
                "AND status IN ('active', 'blocked', 'archiving')",
                (profile_id,),
            ).fetchone()
            if active is not None:
                raise ConflictError("profile already has one active bundle")
            configured = {
                target[0]: target for target in self._stored_profile_targets(profile_id)
            }
            for target in normalized_targets:
                current = configured.get(str(target[0]))
                if current is None:
                    raise StateValidationError(
                        "target snapshot is not configured for profile"
                    )
                target_binding = (
                    str(target[1]),
                    str(target[2]),
                    str(target[3]),
                    str(target[7]),
                )
                current_binding = (current[1], current[2], current[3], current[5])
                if target_binding != current_binding:
                    raise ConflictError(
                        "target snapshot does not match current profile target"
                    )
            cursor = self._connection.execute(
                "INSERT INTO bundles(profile_id, bundle_id, fingerprint, source_bucket, "
                "profile_root_snapshot, status, created_at, updated_at) "
                "VALUES (?, ?, ?, ?, ?, 'active', ?, ?)",
                (
                    profile_id,
                    bundle_id,
                    fingerprint,
                    bucket,
                    str(profile["account_root"]),
                    now,
                    now,
                ),
            )
            bundle_key = _lastrowid(cursor)
            self._connection.executemany(
                "INSERT INTO bundle_files(bundle_key, relative_name, role, ordinal, "
                "media_kind, mime_type, size_bytes, sha256) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                ((bundle_key, *item) for item in normalized_files),
            )
            for target in normalized_targets:
                (
                    platform,
                    remote_id,
                    username,
                    token_env,
                    api_version,
                    adapter_version,
                    settings_json,
                    settings_hash,
                    snapshot_hash,
                ) = target
                self._connection.execute(
                    "INSERT INTO target_snapshots(bundle_key, profile_id, profile_root, "
                    "platform, expected_remote_user_id, expected_username, token_env_var, "
                    "api_version, adapter_version, request_settings_json, "
                    "request_settings_sha256, snapshot_sha256) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    (
                        bundle_key,
                        profile_id,
                        str(profile["account_root"]),
                        platform,
                        remote_id,
                        username,
                        token_env,
                        api_version,
                        adapter_version,
                        settings_json,
                        settings_hash,
                        snapshot_hash,
                    ),
                )
                self._connection.execute(
                    "INSERT INTO deliveries(bundle_key, platform, status, updated_at) "
                    "VALUES (?, ?, 'pending', ?)",
                    (bundle_key, platform, now),
                )
        return bundle_key

    def _normalize_bundle_files(
        self, files: Sequence[BundleFileSnapshot]
    ) -> tuple[tuple[object, ...], ...]:
        normalized: list[tuple[object, ...]] = []
        names: set[str] = set()
        for item in files:
            name = _relative_member_name(item.relative_name)
            if name.casefold() in names:
                raise StateValidationError("bundle file names collide case-insensitively")
            names.add(name.casefold())
            if not item.role or len(item.role) > 32:
                raise StateValidationError("bundle file role is invalid")
            if item.ordinal is not None and item.ordinal < 0:
                raise StateValidationError("bundle file ordinal is invalid")
            if item.size_bytes < 0:
                raise StateValidationError("bundle file size is invalid")
            _validate_sha256(item.sha256, "bundle file hash")
            normalized.append(
                (
                    name,
                    item.role,
                    item.ordinal,
                    item.media_kind,
                    item.mime_type,
                    item.size_bytes,
                    item.sha256,
                )
            )
        return tuple(normalized)

    def _normalize_target_snapshots(
        self, targets: Sequence[TargetSnapshot]
    ) -> tuple[tuple[object, ...], ...]:
        normalized: list[tuple[object, ...]] = []
        seen: set[str] = set()
        for target in targets:
            platform = _validate_platform(target.platform)
            if platform in seen:
                raise StateValidationError("duplicate target snapshot")
            seen.add(platform)
            _validate_identifier(target.expected_remote_user_id, "remote identity")
            _validate_identifier(target.expected_username, "expected username")
            _validate_identifier(target.token_env_var, "token environment reference")
            _validate_identifier(target.api_version, "API version")
            if target.adapter_version <= 0:
                raise StateValidationError("adapter version must be positive")
            settings_json = self._safe_json(target.request_settings)
            settings_hash = _sha256_text(settings_json)
            binding = self._safe_json(
                {
                    "platform": platform,
                    "expected_remote_user_id": target.expected_remote_user_id,
                    "expected_username": target.expected_username,
                    "token_env_var": target.token_env_var,
                    "api_version": target.api_version,
                    "adapter_version": target.adapter_version,
                    "request_settings": json.loads(settings_json),
                }
            )
            normalized.append(
                (
                    platform,
                    target.expected_remote_user_id,
                    target.expected_username,
                    target.token_env_var,
                    target.api_version,
                    target.adapter_version,
                    settings_json,
                    settings_hash,
                    _sha256_text(binding),
                )
            )
        return tuple(sorted(normalized))

    def get_bundle(self, bundle_key: int) -> BundleRecord:
        row = self._connection.execute(
            "SELECT * FROM bundles WHERE bundle_key = ?", (bundle_key,)
        ).fetchone()
        if row is None:
            raise StateValidationError("unknown bundle")
        return self._bundle_from_row(row)

    def get_delivery(self, bundle_key: int, platform: Platform) -> DeliveryRecord:
        row = self._delivery_row(bundle_key, _validate_platform(platform))
        return self._delivery_from_row(row)

    def list_active_bundles(self, profile_id: str) -> tuple[BundleRecord, ...]:
        """Return active work for one profile in canonical bundle order."""

        self._require_profile(profile_id)
        rows = self._connection.execute(
            "SELECT * FROM bundles WHERE profile_id = ? AND status = 'active' "
            "ORDER BY bundle_id COLLATE NOCASE, bundle_id, bundle_key",
            (profile_id,),
        )
        return tuple(self._bundle_from_row(row) for row in rows)

    def list_failed_deliveries(
        self, profile_id: str
    ) -> tuple[DeliveryRecord, ...]:
        """Return every failed delivery owned by one profile deterministically."""

        self._require_profile(profile_id)
        rows = self._connection.execute(
            "SELECT d.* FROM deliveries d JOIN bundles b "
            "ON b.bundle_key = d.bundle_key WHERE b.profile_id = ? "
            "AND d.status = 'failed' ORDER BY b.bundle_id COLLATE NOCASE, "
            "b.bundle_id, d.platform, d.bundle_key",
            (profile_id,),
        )
        return tuple(self._delivery_from_row(row) for row in rows)

    def list_protected_bundles(self, profile_id: str) -> tuple[BundleRecord, ...]:
        """Return bundles that retain profile root and target ownership."""

        self._require_profile(profile_id)
        rows = self._connection.execute(
            "SELECT DISTINCT b.* FROM bundles b LEFT JOIN deliveries d "
            "ON d.bundle_key = b.bundle_key WHERE b.profile_id = ? "
            "AND (b.status IN ('active', 'blocked', 'archiving') "
            "OR d.status = 'ambiguous') ORDER BY b.bundle_id COLLATE NOCASE, "
            "b.bundle_id, b.bundle_key",
            (profile_id,),
        )
        return tuple(self._bundle_from_row(row) for row in rows)

    def list_bundle_files(self, bundle_key: int) -> tuple[BundleFileSnapshot, ...]:
        """Load the immutable exact-member manifest for an admitted bundle."""

        self._bundle_row(bundle_key)
        rows = self._connection.execute(
            "SELECT * FROM bundle_files WHERE bundle_key = ? "
            "ORDER BY relative_name COLLATE NOCASE, relative_name",
            (bundle_key,),
        )
        return tuple(self._bundle_file_from_row(row) for row in rows)

    def list_target_snapshots(self, bundle_key: int) -> tuple[TargetSnapshot, ...]:
        """Load immutable target semantics in platform order."""

        self._bundle_row(bundle_key)
        rows = self._connection.execute(
            "SELECT * FROM target_snapshots WHERE bundle_key = ? ORDER BY platform",
            (bundle_key,),
        )
        return tuple(self._target_snapshot_from_row(row) for row in rows)

    def list_bundle_deliveries(self, bundle_key: int) -> tuple[DeliveryRecord, ...]:
        """Load all per-target delivery state in platform order."""

        self._bundle_row(bundle_key)
        rows = self._connection.execute(
            "SELECT * FROM deliveries WHERE bundle_key = ? ORDER BY platform",
            (bundle_key,),
        )
        return tuple(self._delivery_from_row(row) for row in rows)

    def list_delivery_artifacts(
        self,
        bundle_key: int,
        platform: Platform,
        *,
        attempt_count: int | None = None,
    ) -> tuple[ArtifactRecord, ...]:
        """Load durable checkpoints for one delivery and optional exact attempt."""

        platform = _validate_platform(platform)
        self._delivery_row(bundle_key, platform)
        if attempt_count is not None and attempt_count <= 0:
            raise StateValidationError("artifact attempt count must be positive")
        query = (
            "SELECT * FROM delivery_artifacts WHERE bundle_key = ? AND platform = ?"
        )
        parameters: tuple[object, ...] = (bundle_key, platform)
        if attempt_count is not None:
            query += " AND attempt_count = ?"
            parameters += (attempt_count,)
        query += " ORDER BY attempt_count, kind, ordinal"
        rows = self._connection.execute(query, parameters)
        return tuple(self._artifact_from_row(row) for row in rows)

    def assert_bundle_source(
        self,
        bundle_key: int,
        *,
        fingerprint: str,
        profile_root: str | Path,
    ) -> BundleRecord:
        _validate_sha256(fingerprint, "bundle fingerprint")
        current_root = str(Path(profile_root).resolve(strict=False))
        drift_message: str | None = None
        with self._transaction():
            row = self._bundle_row(bundle_key)
            if str(row["fingerprint"]) != fingerprint:
                drift_message = "bundle fingerprint drift detected"
                code = "fingerprint_drift"
            elif str(row["profile_root_snapshot"]) != current_root:
                drift_message = "profile root drift detected"
                code = "profile_root_drift"
            else:
                return self._bundle_from_row(row)
            if str(row["status"]) != "archived":
                self._block_bundle_locked(bundle_key, code)
                self._insert_event_locked(bundle_key, None, code, code, {})
        if drift_message is None:  # Defensive exhaustiveness for static analysis.
            raise StateError("bundle source validation failed without a drift reason")
        raise ConflictError(drift_message)

    def assert_target_snapshot(
        self, bundle_key: int, snapshot: TargetSnapshot
    ) -> None:
        normalized = self._normalize_target_snapshots((snapshot,))[0]
        platform = cast(str, normalized[0])
        expected_hash = cast(str, normalized[-1])
        row = self._connection.execute(
            "SELECT snapshot_sha256 FROM target_snapshots "
            "WHERE bundle_key = ? AND platform = ?",
            (bundle_key, platform),
        ).fetchone()
        if row is None or str(row[0]) != expected_hash:
            raise ConflictError("immutable target snapshot does not match")

    def block_bundle(
        self, bundle_key: int, reason: str, *, expected_revision: int
    ) -> BundleRecord:
        reason = self._safe_text(reason, "block reason")
        with self._transaction():
            row = self._bundle_row(bundle_key)
            self._require_revision(row, expected_revision, "bundle")
            if str(row["status"]) not in {"active", "blocked", "archiving"}:
                raise TransitionError("bundle cannot transition to blocked")
            self._block_bundle_locked(bundle_key, reason)
            self._insert_event_locked(
                bundle_key, None, "bundle_blocked", reason, {"reason": reason}
            )
        return self.get_bundle(bundle_key)

    def begin_archiving(
        self, bundle_key: int, *, expected_revision: int
    ) -> BundleRecord:
        with self._transaction():
            row = self._bundle_row(bundle_key)
            self._require_revision(row, expected_revision, "bundle")
            if str(row["status"]) != "active":
                raise TransitionError("only an active bundle can begin archiving")
            unpublished = self._connection.execute(
                "SELECT 1 FROM deliveries WHERE bundle_key = ? AND status != 'published'",
                (bundle_key,),
            ).fetchone()
            if unpublished is not None:
                raise TransitionError("all target deliveries must be published before archiving")
            now = self._now_text()
            self._connection.execute(
                "UPDATE bundles SET status = 'archiving', revision = revision + 1, "
                "updated_at = ? WHERE bundle_key = ?",
                (now, bundle_key),
            )
            self._insert_event_locked(
                bundle_key, None, "bundle_archiving", "bundle_archiving", {}
            )
        return self.get_bundle(bundle_key)

    def checkpoint_archive_member(
        self,
        bundle_key: int,
        relative_name: str,
        *,
        sha256: str,
    ) -> None:
        """Durably mark one exact hash-verified content member or ready sentinel."""

        relative_name = _relative_path_text(relative_name, "archive member path")
        _validate_sha256(sha256, "archive member hash")
        with self._transaction():
            bundle = self._bundle_row(bundle_key)
            if str(bundle["status"]) != "archiving":
                raise TransitionError(
                    "archive members can checkpoint only while archiving"
                )
            if relative_name == str(bundle["ready_marker_name"]):
                if sha256 != hashlib.sha256(b"").hexdigest():
                    raise ConflictError("ready marker archive hash does not match")
                self._connection.execute(
                    "UPDATE bundles SET ready_marker_archived = 1, updated_at = ? "
                    "WHERE bundle_key = ?",
                    (self._now_text(), bundle_key),
                )
                return
            member = self._connection.execute(
                "SELECT sha256 FROM bundle_files WHERE bundle_key = ? "
                "AND relative_name = ?",
                (bundle_key, relative_name),
            ).fetchone()
            if member is None:
                raise StateValidationError("archive member is not in the exact manifest")
            if str(member["sha256"]) != sha256:
                raise ConflictError("archive member hash does not match snapshot")
            self._connection.execute(
                "UPDATE bundle_files SET archived = 1 WHERE bundle_key = ? "
                "AND relative_name = ?",
                (bundle_key, relative_name),
            )

    def list_archive_checkpoints(self, bundle_key: int) -> tuple[str, ...]:
        """Return exact archive-member checkpoints, including the ready sentinel."""

        bundle = self._bundle_row(bundle_key)
        names = [
            str(row[0])
            for row in self._connection.execute(
                "SELECT relative_name FROM bundle_files WHERE bundle_key = ? "
                "AND archived = 1 ORDER BY relative_name COLLATE NOCASE, relative_name",
                (bundle_key,),
            )
        ]
        if bool(bundle["ready_marker_archived"]):
            names.append(str(bundle["ready_marker_name"]))
        return tuple(names)

    def mark_archived(
        self, bundle_key: int, archive_path: str, *, expected_revision: int
    ) -> BundleRecord:
        archive_path = _relative_path_text(archive_path, "archive path")
        with self._transaction():
            row = self._bundle_row(bundle_key)
            self._require_revision(row, expected_revision, "bundle")
            if str(row["status"]) != "archiving":
                raise TransitionError("only an archiving bundle can become archived")
            incomplete = self._connection.execute(
                "SELECT 1 FROM bundle_files WHERE bundle_key = ? AND archived = 0",
                (bundle_key,),
            ).fetchone()
            if incomplete is not None or not bool(row["ready_marker_archived"]):
                raise TransitionError(
                    "every exact archive member must be checkpointed before completion"
                )
            now = self._now_text()
            self._connection.execute(
                "UPDATE bundles SET status = 'archived', archive_path = ?, "
                "claimed_by_type = NULL, claimed_by_id = NULL, "
                "revision = revision + 1, updated_at = ? WHERE bundle_key = ?",
                (archive_path, now, bundle_key),
            )
            self._insert_event_locked(
                bundle_key,
                None,
                "bundle_archived",
                "bundle_archived",
                {"archive_path": archive_path},
            )
        return self.get_bundle(bundle_key)

    def claim_delivery(
        self, bundle_key: int, platform: Platform, claim_token: str
    ) -> DeliveryRecord:
        platform = _validate_platform(platform)
        _validate_identifier(claim_token, "claim token")
        now = self._now_text()
        with self._transaction():
            bundle = self._bundle_row(bundle_key)
            delivery = self._delivery_row(bundle_key, platform)
            status = str(delivery["status"])
            if status == "ambiguous":
                raise TransitionError("ambiguous delivery cannot be retried automatically")
            if str(bundle["status"]) != "active":
                raise TransitionError("delivery cannot be claimed while bundle is not active")
            if status != "pending":
                if status == "failed" and bool(delivery["safe_to_retry"]):
                    due = _optional_datetime(delivery["next_attempt_at"])
                    if due is not None and due > self._now():
                        raise TransitionError("failed delivery retry is not due")
                else:
                    raise TransitionError("delivery is not claimable")
            self._connection.execute(
                "UPDATE deliveries SET status = 'in_flight', phase = 'preparing', "
                "attempt_count = attempt_count + 1, claim_token = ?, "
                "next_attempt_at = NULL, error_code = NULL, error_message = NULL, "
                "revision = revision + 1, updated_at = ? "
                "WHERE bundle_key = ? AND platform = ?",
                (claim_token, now, bundle_key, platform),
            )
        return self.get_delivery(bundle_key, platform)

    def advance_delivery_phase(
        self,
        bundle_key: int,
        platform: Platform,
        phase: DeliveryPhase,
        *,
        claim_token: str,
        attempt_count: int,
    ) -> DeliveryRecord:
        platform = _validate_platform(platform)
        if phase not in _PHASE_ORDER:
            raise StateValidationError("unsupported delivery phase")
        with self._transaction():
            row = self._delivery_row(bundle_key, platform)
            self._require_delivery_claim(row, claim_token, attempt_count)
            current = cast(str, row["phase"])
            if _PHASE_ORDER[phase] < _PHASE_ORDER[current]:
                raise TransitionError("delivery phase cannot move backward")
            if phase != current:
                updated = self._connection.execute(
                    "UPDATE deliveries SET phase = ?, revision = revision + 1, "
                    "updated_at = ? WHERE bundle_key = ? AND platform = ? "
                    "AND status = 'in_flight' AND claim_token = ? AND attempt_count = ?",
                    (
                        phase,
                        self._now_text(),
                        bundle_key,
                        platform,
                        claim_token,
                        attempt_count,
                    ),
                )
                if updated.rowcount != 1:
                    raise ConflictError("delivery claim became stale")
        return self.get_delivery(bundle_key, platform)

    def checkpoint_artifact(
        self,
        bundle_key: int,
        platform: Platform,
        *,
        kind: str,
        ordinal: int,
        external_id: str | None = None,
        relative_path: str | None = None,
        sha256: str | None = None,
        expires_at: datetime | None = None,
        processing_metadata: Mapping[str, object] | None = None,
        claim_token: str,
        attempt_count: int,
    ) -> ArtifactRecord:
        platform = _validate_platform(platform)
        if kind not in _ARTIFACT_KINDS:
            raise StateValidationError("unsupported artifact checkpoint kind")
        if ordinal < 0:
            raise StateValidationError("artifact ordinal must be non-negative")
        if kind == "instagram_parent_container" and ordinal != 0:
            raise StateValidationError(
                "Instagram parent container ordinal must be zero"
            )
        if kind.startswith("x_") and platform != "x":
            raise StateValidationError("artifact checkpoint platform mismatch")
        if kind.startswith("instagram_") and platform != "instagram":
            raise StateValidationError("artifact checkpoint platform mismatch")
        remote_kinds = {
            "x_media_id",
            "instagram_child_container",
            "instagram_parent_container",
        }
        if kind in remote_kinds:
            if external_id is None:
                raise StateValidationError("remote artifact checkpoint requires an ID")
            _validate_identifier(external_id, "remote artifact ID")
            if relative_path is not None or sha256 is not None:
                raise StateValidationError("remote artifact cannot contain a staging path")
            if expires_at is None:
                raise StateValidationError("remote artifact checkpoint requires an expiry")
        if kind in {"staged_private", "staged_public"}:
            if relative_path is None or sha256 is None:
                raise StateValidationError("staged checkpoint requires path and hash")
            _validate_sha256(sha256, "staged artifact hash")
            if external_id is not None:
                raise StateValidationError("staged artifact cannot contain a remote ID")
        if kind == "staged_public" and platform != "instagram":
            raise StateValidationError("public staging is only supported for Instagram")
        expiry = _timestamp(expires_at) if expires_at is not None else None
        if expires_at is not None and expires_at.astimezone(UTC) <= self._now():
            raise StateValidationError("artifact expiry must be in the future")
        metadata_json = self._processing_metadata_json(processing_metadata or {})
        now = self._now_text()
        values = (
            external_id,
            relative_path,
            sha256,
            expiry,
            metadata_json,
        )
        with self._transaction():
            delivery = self._delivery_row(bundle_key, platform)
            self._require_delivery_claim(delivery, claim_token, attempt_count)
            bundle = self._bundle_row(bundle_key)
            if kind in {"staged_private", "staged_public"}:
                relative_path = self._validated_staged_artifact_path(
                    cast(str, relative_path), kind, bundle
                )
                values = (
                    external_id,
                    relative_path,
                    sha256,
                    expiry,
                    metadata_json,
                )
            existing = self._connection.execute(
                "SELECT * FROM delivery_artifacts WHERE bundle_key = ? AND platform = ? "
                "AND attempt_count = ? AND kind = ? AND ordinal = ?",
                (bundle_key, platform, attempt_count, kind, ordinal),
            ).fetchone()
            if existing is not None:
                stored = tuple(
                    existing[name]
                    for name in (
                        "external_id",
                        "relative_path",
                        "sha256",
                        "expires_at",
                        "processing_metadata_json",
                    )
                )
                if stored != values:
                    raise ConflictError("artifact checkpoint is immutable")
            else:
                try:
                    self._connection.execute(
                        "INSERT INTO delivery_artifacts(bundle_key, platform, attempt_count, "
                        "kind, ordinal, "
                        "external_id, relative_path, sha256, expires_at, "
                        "processing_metadata_json, created_at, updated_at) "
                        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                        (
                            bundle_key,
                            platform,
                            attempt_count,
                            kind,
                            ordinal,
                            *values,
                            now,
                            now,
                        ),
                    )
                except sqlite3.IntegrityError:
                    raise ConflictError(
                        "artifact identity conflicts with durable state"
                    ) from None
                updated = self._connection.execute(
                    "UPDATE deliveries SET revision = revision + 1, updated_at = ? "
                    "WHERE bundle_key = ? AND platform = ? AND status = 'in_flight' "
                    "AND claim_token = ? AND attempt_count = ?",
                    (now, bundle_key, platform, claim_token, attempt_count),
                )
                if updated.rowcount != 1:
                    raise ConflictError("delivery claim became stale")
        row = self._connection.execute(
            "SELECT * FROM delivery_artifacts WHERE bundle_key = ? AND platform = ? "
            "AND attempt_count = ? AND kind = ? AND ordinal = ?",
            (bundle_key, platform, attempt_count, kind, ordinal),
        ).fetchone()
        return self._artifact_from_row(cast(sqlite3.Row, row))

    def transition_artifact_processing(
        self,
        bundle_key: int,
        platform: Platform,
        *,
        kind: str,
        ordinal: int,
        external_id: str,
        expected_processing_metadata: Mapping[str, object],
        processing_metadata: Mapping[str, object],
        claim_token: str,
        attempt_count: int,
    ) -> ArtifactRecord:
        """CAS-update processing state without weakening artifact identity."""

        platform = _validate_platform(platform)
        if kind not in {
            "x_media_id",
            "instagram_child_container",
            "instagram_parent_container",
        }:
            raise StateValidationError(
                "only remote artifacts have processing transitions"
            )
        if ordinal < 0:
            raise StateValidationError("artifact ordinal must be non-negative")
        _validate_identifier(external_id, "remote artifact ID")
        expected_json = self._processing_metadata_json(expected_processing_metadata)
        metadata_json = self._processing_metadata_json(processing_metadata)
        with self._transaction():
            delivery = self._delivery_row(bundle_key, platform)
            self._require_delivery_claim(delivery, claim_token, attempt_count)
            if str(delivery["phase"]) == "final_dispatch_started":
                raise TransitionError(
                    "artifact processing cannot change after final dispatch"
                )
            artifact = self._connection.execute(
                "SELECT * FROM delivery_artifacts WHERE bundle_key = ? "
                "AND platform = ? AND attempt_count = ? AND kind = ? AND ordinal = ?",
                (bundle_key, platform, attempt_count, kind, ordinal),
            ).fetchone()
            if artifact is None:
                raise ConflictError("artifact processing checkpoint is missing")
            if str(artifact["external_id"]) != external_id:
                raise ConflictError("artifact processing identity changed")
            if str(artifact["processing_metadata_json"]) != expected_json:
                raise ConflictError("artifact processing metadata changed")
            if metadata_json != expected_json:
                now = self._now_text()
                updated = self._connection.execute(
                    "UPDATE delivery_artifacts SET processing_metadata_json = ?, "
                    "updated_at = ? WHERE bundle_key = ? AND platform = ? "
                    "AND attempt_count = ? AND kind = ? AND ordinal = ? "
                    "AND external_id = ? AND processing_metadata_json = ?",
                    (
                        metadata_json,
                        now,
                        bundle_key,
                        platform,
                        attempt_count,
                        kind,
                        ordinal,
                        external_id,
                        expected_json,
                    ),
                )
                if updated.rowcount != 1:
                    raise ConflictError("artifact processing checkpoint became stale")
                self._connection.execute(
                    "UPDATE deliveries SET revision = revision + 1, updated_at = ? "
                    "WHERE bundle_key = ? AND platform = ? AND status = 'in_flight' "
                    "AND claim_token = ? AND attempt_count = ?",
                    (now, bundle_key, platform, claim_token, attempt_count),
                )
        row = self._connection.execute(
            "SELECT * FROM delivery_artifacts WHERE bundle_key = ? AND platform = ? "
            "AND attempt_count = ? AND kind = ? AND ordinal = ?",
            (bundle_key, platform, attempt_count, kind, ordinal),
        ).fetchone()
        return self._artifact_from_row(cast(sqlite3.Row, row))

    def replace_expired_artifact(
        self,
        bundle_key: int,
        platform: Platform,
        *,
        kind: str,
        ordinal: int,
        expected_external_id: str,
        external_id: str,
        expires_at: datetime,
        processing_metadata: Mapping[str, object],
        claim_token: str,
        attempt_count: int,
    ) -> ArtifactRecord:
        """Atomically replace one expired remote artifact before final dispatch."""

        platform = _validate_platform(platform)
        if kind not in {
            "x_media_id",
            "instagram_child_container",
            "instagram_parent_container",
        }:
            raise StateValidationError("only remote artifacts can be replaced")
        if ordinal < 0:
            raise StateValidationError("artifact ordinal must be non-negative")
        _validate_identifier(expected_external_id, "expected remote artifact ID")
        _validate_identifier(external_id, "remote artifact ID")
        expiry = _timestamp(expires_at)
        if expires_at.astimezone(UTC) <= self._now():
            raise StateValidationError("replacement artifact expiry must be in the future")
        metadata_json = self._processing_metadata_json(processing_metadata)
        with self._transaction():
            delivery = self._delivery_row(bundle_key, platform)
            self._require_delivery_claim(delivery, claim_token, attempt_count)
            if str(delivery["phase"]) == "final_dispatch_started":
                raise TransitionError(
                    "expired artifact cannot be replaced after final dispatch"
                )
            artifact = self._connection.execute(
                "SELECT * FROM delivery_artifacts WHERE bundle_key = ? "
                "AND platform = ? AND attempt_count = ? AND kind = ? AND ordinal = ?",
                (bundle_key, platform, attempt_count, kind, ordinal),
            ).fetchone()
            if artifact is None or str(artifact["external_id"]) != expected_external_id:
                raise ConflictError("expired artifact identity changed")
            current_expiry = _optional_datetime(artifact["expires_at"])
            if current_expiry is None or current_expiry > self._now():
                raise TransitionError("remote artifact is not expired")
            now = self._now_text()
            try:
                updated = self._connection.execute(
                    "UPDATE delivery_artifacts SET external_id = ?, expires_at = ?, "
                    "processing_metadata_json = ?, updated_at = ? "
                    "WHERE bundle_key = ? AND platform = ? AND attempt_count = ? "
                    "AND kind = ? AND ordinal = ? AND external_id = ?",
                    (
                        external_id,
                        expiry,
                        metadata_json,
                        now,
                        bundle_key,
                        platform,
                        attempt_count,
                        kind,
                        ordinal,
                        expected_external_id,
                    ),
                )
            except sqlite3.IntegrityError:
                raise ConflictError(
                    "replacement artifact identity conflicts with durable state"
                ) from None
            if updated.rowcount != 1:
                raise ConflictError("expired artifact replacement became stale")
            self._connection.execute(
                "UPDATE deliveries SET revision = revision + 1, updated_at = ? "
                "WHERE bundle_key = ? AND platform = ? AND status = 'in_flight' "
                "AND claim_token = ? AND attempt_count = ?",
                (now, bundle_key, platform, claim_token, attempt_count),
            )
        row = self._connection.execute(
            "SELECT * FROM delivery_artifacts WHERE bundle_key = ? AND platform = ? "
            "AND attempt_count = ? AND kind = ? AND ordinal = ?",
            (bundle_key, platform, attempt_count, kind, ordinal),
        ).fetchone()
        return self._artifact_from_row(cast(sqlite3.Row, row))

    def fail_delivery(
        self,
        bundle_key: int,
        platform: Platform,
        *,
        error_code: str,
        error_message: str,
        retry_at: datetime | None,
        permanent: bool = False,
        claim_token: str,
        attempt_count: int,
    ) -> DeliveryRecord:
        platform = _validate_platform(platform)
        error_code = self._safe_text(error_code, "delivery error code", maximum=64)
        error_message = self._safe_text(error_message, "delivery error message")
        retry_timestamp = _timestamp(retry_at) if retry_at is not None else None
        with self._transaction():
            row = self._delivery_row(bundle_key, platform)
            self._require_delivery_claim(row, claim_token, attempt_count)
            if str(row["phase"]) == "final_dispatch_started":
                self._mark_ambiguous_locked(
                    bundle_key,
                    platform,
                    error_code,
                    error_message,
                    claim_token=claim_token,
                    attempt_count=attempt_count,
                )
                return self.get_delivery(bundle_key, platform)
            if not permanent and retry_at is None:
                raise StateValidationError(
                    "retryable failure requires an absolute retry time"
                )
            failures = int(row["consecutive_failures"]) + 1
            retryable = not permanent and failures < 5
            now = self._now_text()
            updated = self._connection.execute(
                "UPDATE deliveries SET status = 'failed', phase = NULL, "
                "consecutive_failures = ?, safe_to_retry = ?, next_attempt_at = ?, "
                "claim_token = NULL, error_code = ?, error_message = ?, "
                "revision = revision + 1, updated_at = ? "
                "WHERE bundle_key = ? AND platform = ? AND status = 'in_flight' "
                "AND claim_token = ? AND attempt_count = ?",
                (
                    failures,
                    int(retryable),
                    retry_timestamp if retryable else None,
                    error_code,
                    error_message,
                    now,
                    bundle_key,
                    platform,
                    claim_token,
                    attempt_count,
                ),
            )
            if updated.rowcount != 1:
                raise ConflictError("delivery claim became stale")
            if not retryable:
                self._block_bundle_locked(bundle_key, error_code)
            self._insert_event_locked(
                bundle_key,
                platform,
                "delivery_failed",
                f"delivery_failed:{int(row['attempt_count'])}",
                {"error_code": error_code, "retryable": retryable},
            )
        return self.get_delivery(bundle_key, platform)

    def publish_delivery(
        self,
        bundle_key: int,
        platform: Platform,
        *,
        remote_id: str,
        claim_token: str,
        attempt_count: int,
    ) -> DeliveryRecord:
        platform = _validate_platform(platform)
        _validate_identifier(remote_id, "remote post ID")
        with self._transaction():
            row = self._delivery_row(bundle_key, platform)
            self._require_delivery_claim(row, claim_token, attempt_count)
            if str(row["status"]) != "in_flight" or str(row["phase"]) != "final_dispatch_started":
                raise TransitionError(
                    "publication result requires a durable final-dispatch phase"
                )
            try:
                updated = self._connection.execute(
                    "UPDATE deliveries SET status = 'published', phase = NULL, "
                    "consecutive_failures = 0, safe_to_retry = 0, next_attempt_at = NULL, "
                    "claim_token = NULL, remote_id = ?, error_code = NULL, "
                    "error_message = NULL, revision = revision + 1, updated_at = ? "
                    "WHERE bundle_key = ? AND platform = ? AND status = 'in_flight' "
                    "AND claim_token = ? AND attempt_count = ?",
                    (
                        remote_id,
                        self._now_text(),
                        bundle_key,
                        platform,
                        claim_token,
                        attempt_count,
                    ),
                )
            except sqlite3.IntegrityError:
                raise ConflictError(
                    "remote publication identity conflicts with durable state"
                ) from None
            if updated.rowcount != 1:
                raise ConflictError("delivery claim became stale")
        return self.get_delivery(bundle_key, platform)

    def mark_delivery_ambiguous(
        self,
        bundle_key: int,
        platform: Platform,
        *,
        error_code: str,
        error_message: str,
        claim_token: str,
        attempt_count: int,
    ) -> DeliveryRecord:
        """Persist an uncertain final-create result without permitting a retry."""

        platform = _validate_platform(platform)
        error_code = self._safe_text(error_code, "delivery error code", maximum=64)
        error_message = self._safe_text(error_message, "delivery error message")
        with self._transaction():
            row = self._delivery_row(bundle_key, platform)
            self._require_delivery_claim(row, claim_token, attempt_count)
            if (
                str(row["status"]) != "in_flight"
                or str(row["phase"]) != "final_dispatch_started"
            ):
                raise TransitionError(
                    "only an uncertain final-dispatch result can become ambiguous"
                )
            self._mark_ambiguous_locked(
                bundle_key,
                platform,
                error_code,
                error_message,
                claim_token=claim_token,
                attempt_count=attempt_count,
            )
        return self.get_delivery(bundle_key, platform)

    def recover_stale(
        self, stale_before: datetime
    ) -> tuple[tuple[int, Platform, str], ...]:
        cutoff = _timestamp(stale_before)
        recovered: list[tuple[int, Platform, str]] = []
        with self._transaction():
            rows = tuple(
                self._connection.execute(
                    "SELECT * FROM deliveries WHERE status = 'in_flight' "
                    "AND updated_at <= ? ORDER BY bundle_key, platform",
                    (cutoff,),
                )
            )
            for row in rows:
                bundle_key = int(row["bundle_key"])
                platform = cast(Platform, str(row["platform"]))
                if str(row["phase"]) == "final_dispatch_started":
                    self._mark_ambiguous_locked(
                        bundle_key,
                        platform,
                        "stale_final_dispatch",
                        "Final dispatch outcome is unknown.",
                        claim_token=str(row["claim_token"]),
                        attempt_count=int(row["attempt_count"]),
                    )
                    recovered.append((bundle_key, platform, "ambiguous"))
                    continue
                failures = int(row["consecutive_failures"]) + 1
                retryable = failures < 5
                now = self._now_text()
                updated = self._connection.execute(
                    "UPDATE deliveries SET status = 'failed', phase = NULL, "
                    "consecutive_failures = ?, safe_to_retry = ?, next_attempt_at = ?, "
                    "claim_token = NULL, error_code = 'stale_pre_final', "
                    "error_message = 'Interrupted before final dispatch; safe to retry.', "
                    "revision = revision + 1, updated_at = ? "
                    "WHERE bundle_key = ? AND platform = ? AND status = 'in_flight' "
                    "AND claim_token = ? AND attempt_count = ?",
                    (
                        failures,
                        int(retryable),
                        now if retryable else None,
                        now,
                        bundle_key,
                        platform,
                        str(row["claim_token"]),
                        int(row["attempt_count"]),
                    ),
                )
                if updated.rowcount != 1:
                    raise ConflictError("stale delivery recovery lost its claim")
                if not retryable:
                    self._block_bundle_locked(bundle_key, "stale_pre_final")
                recovered.append((bundle_key, platform, "failed"))
        return tuple(recovered)

    def operator_retry(
        self,
        bundle_key: int,
        platform: Platform,
        *,
        expected_bundle_revision: int,
        validated_snapshot: TargetSnapshot,
    ) -> DeliveryRecord:
        platform = _validate_platform(platform)
        self.assert_target_snapshot(bundle_key, validated_snapshot)
        with self._transaction():
            bundle = self._bundle_row(bundle_key)
            self._require_revision(bundle, expected_bundle_revision, "bundle")
            delivery = self._delivery_row(bundle_key, platform)
            if str(bundle["status"]) != "blocked" or str(delivery["status"]) != "failed":
                raise TransitionError("operator retry requires a blocked failed delivery")
            now = self._now_text()
            self._connection.execute(
                "UPDATE deliveries SET phase = NULL, consecutive_failures = 0, "
                "safe_to_retry = 1, next_attempt_at = ?, error_code = NULL, "
                "error_message = NULL, revision = revision + 1, updated_at = ? "
                "WHERE bundle_key = ? AND platform = ?",
                (now, now, bundle_key, platform),
            )
            other_block = self._connection.execute(
                "SELECT 1 FROM deliveries WHERE bundle_key = ? AND platform != ? "
                "AND (status = 'ambiguous' OR (status = 'failed' AND safe_to_retry = 0))",
                (bundle_key, platform),
            ).fetchone()
            if other_block is None:
                self._connection.execute(
                    "UPDATE bundles SET status = 'active', block_reason = NULL, "
                    "revision = revision + 1, updated_at = ? WHERE bundle_key = ?",
                    (now, bundle_key),
                )
            self._insert_event_locked(
                bundle_key,
                platform,
                "operator_retry_enabled",
                f"operator_retry:{int(delivery['attempt_count'])}",
                {},
            )
        return self.get_delivery(bundle_key, platform)

    def record_warning(
        self,
        bundle_key: int,
        platform: Platform | None,
        code: str,
        details: Mapping[str, object],
    ) -> None:
        if code not in _WARNING_CODES:
            raise StateValidationError("warning code is not allow-listed")
        if platform is not None:
            platform = _validate_platform(platform)
        detail_json = self._safe_json(details)
        dedupe = _sha256_text(
            self._safe_json(
                {
                    "bundle_key": bundle_key,
                    "platform": platform,
                    "code": code,
                    "details": json.loads(detail_json),
                }
            )
        )
        with self._transaction():
            self._bundle_row(bundle_key)
            self._insert_event_locked(
                bundle_key,
                platform,
                "warning",
                code,
                details,
                dedupe_key=dedupe,
            )

    def list_events(self, bundle_key: int) -> tuple[Mapping[str, object], ...]:
        rows = self._connection.execute(
            "SELECT occurred_at, platform, event_type, event_code, safe_detail_json "
            "FROM events WHERE bundle_key = ? ORDER BY event_id",
            (bundle_key,),
        )
        return tuple(
            {
                "occurred_at": row["occurred_at"],
                "platform": row["platform"],
                "event_type": row["event_type"],
                "event_code": row["event_code"],
                "details": json.loads(str(row["safe_detail_json"])),
            }
            for row in rows
        )

    def create_schedule(
        self,
        *,
        profile_id: str,
        schedule_id: str,
        bucket: SourceBucket,
        timezone: str,
        weekdays: Sequence[int],
        local_time: str,
        misfire_grace_seconds: int,
        enabled: bool,
        expected_revision: int | None = None,
    ) -> ScheduleRecord:
        if not _SCHEDULE_RE.fullmatch(schedule_id):
            raise StateValidationError("schedule ID is invalid")
        bucket = _validate_bucket(bucket)
        weekdays_tuple = tuple(sorted(set(weekdays)))
        if not weekdays_tuple or any(day < 0 or day > 6 for day in weekdays_tuple):
            raise StateValidationError("schedule weekdays must be integers from 0 through 6")
        _validate_timezone(timezone)
        _validate_local_time(local_time)
        if not 0 <= misfire_grace_seconds <= 86400:
            raise StateValidationError("schedule misfire grace is out of range")
        settings = {
            "bucket": bucket,
            "timezone": timezone,
            "weekdays": weekdays_tuple,
            "local_time": local_time,
            "misfire_grace_seconds": misfire_grace_seconds,
            "enabled": enabled,
        }
        config_hash = _sha256_text(self._safe_json(settings))
        weekdays_json = self._safe_json(weekdays_tuple)
        now = self._now_text()
        with self._transaction():
            self._require_profile(profile_id)
            existing = self._connection.execute(
                "SELECT * FROM schedules WHERE profile_id = ? AND schedule_id = ?",
                (profile_id, schedule_id),
            ).fetchone()
            if existing is None:
                cursor = self._connection.execute(
                    "INSERT INTO schedules(profile_id, schedule_id, bucket, timezone, "
                    "weekdays_json, local_time, misfire_grace_seconds, enabled, "
                    "config_hash, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    (
                        profile_id,
                        schedule_id,
                        bucket,
                        timezone,
                        weekdays_json,
                        local_time,
                        misfire_grace_seconds,
                        int(enabled),
                        config_hash,
                        now,
                        now,
                    ),
                )
                schedule_key = _lastrowid(cursor)
            else:
                schedule_key = int(existing["schedule_key"])
                if str(existing["config_hash"]) == config_hash:
                    return self._schedule_from_row(existing)
                self._require_revision(existing, expected_revision, "schedule")
                self._connection.execute(
                    "UPDATE schedules SET bucket = ?, timezone = ?, weekdays_json = ?, "
                    "local_time = ?, misfire_grace_seconds = ?, enabled = ?, "
                    "config_hash = ?, revision = revision + 1, updated_at = ? "
                    "WHERE schedule_key = ?",
                    (
                        bucket,
                        timezone,
                        weekdays_json,
                        local_time,
                        misfire_grace_seconds,
                        int(enabled),
                        config_hash,
                        now,
                        schedule_key,
                    ),
                )
        return self.get_schedule(schedule_key)

    def get_schedule(self, schedule_key: int) -> ScheduleRecord:
        row = self._connection.execute(
            "SELECT * FROM schedules WHERE schedule_key = ?", (schedule_key,)
        ).fetchone()
        if row is None:
            raise StateValidationError("unknown schedule")
        return self._schedule_from_row(row)

    def set_schedule_enabled(
        self, schedule_key: int, enabled: bool, *, expected_revision: int
    ) -> ScheduleRecord:
        with self._transaction():
            row = self._schedule_row(schedule_key)
            self._require_revision(row, expected_revision, "schedule")
            settings = {
                "bucket": row["bucket"],
                "timezone": row["timezone"],
                "weekdays": json.loads(str(row["weekdays_json"])),
                "local_time": row["local_time"],
                "misfire_grace_seconds": row["misfire_grace_seconds"],
                "enabled": enabled,
            }
            self._connection.execute(
                "UPDATE schedules SET enabled = ?, config_hash = ?, "
                "revision = revision + 1, updated_at = ? WHERE schedule_key = ?",
                (
                    int(enabled),
                    _sha256_text(self._safe_json(settings)),
                    self._now_text(),
                    schedule_key,
                ),
            )
        return self.get_schedule(schedule_key)

    def create_schedule_occurrence(
        self,
        schedule_key: int,
        *,
        local_date: str,
        scheduled_at: datetime,
        utc_offset_minutes: int,
        schedule_hash: str,
    ) -> ScheduleRunRecord:
        """Persist a recoverable queued occurrence before it becomes due."""

        try:
            date.fromisoformat(local_date)
        except ValueError:
            raise StateValidationError("schedule local date is invalid") from None
        scheduled_text = _timestamp(scheduled_at)
        if not -1440 <= utc_offset_minutes <= 1440:
            raise StateValidationError("schedule UTC offset is invalid")
        _validate_sha256(schedule_hash, "schedule hash")
        with self._transaction():
            existing = self._connection.execute(
                "SELECT * FROM schedule_runs WHERE schedule_key = ? AND local_date = ?",
                (schedule_key, local_date),
            ).fetchone()
            if existing is not None:
                expected = (
                    scheduled_text,
                    utc_offset_minutes,
                    schedule_hash,
                )
                stored = tuple(
                    existing[name]
                    for name in (
                        "scheduled_at",
                        "utc_offset_minutes",
                        "schedule_hash",
                    )
                )
                if stored != expected:
                    raise ConflictError("schedule occurrence already has a different claim")
                return self._schedule_run_from_row(existing)
            schedule = self._schedule_row(schedule_key)
            if not bool(schedule["enabled"]):
                raise TransitionError("disabled schedule cannot claim an occurrence")
            if str(schedule["config_hash"]) != schedule_hash:
                raise ConflictError("schedule hash drift")
            now = self._now_text()
            cursor = self._connection.execute(
                "INSERT INTO schedule_runs(schedule_key, local_date, "
                "scheduled_at, utc_offset_minutes, schedule_hash, state, created_at, updated_at) "
                "VALUES (?, ?, ?, ?, ?, 'queued', ?, ?)",
                (
                    schedule_key,
                    local_date,
                    scheduled_text,
                    utc_offset_minutes,
                    schedule_hash,
                    now,
                    now,
                ),
            )
            run_id = _lastrowid(cursor)
        return self.get_schedule_run(run_id)

    def claim_schedule_content(
        self,
        run_id: int,
        bundle_key: int,
        *,
        expected_revision: int,
    ) -> ScheduleRunRecord:
        """Atomically bind due work to exact profile content and start dispatch."""

        with self._transaction():
            run = self._schedule_run_row(run_id)
            self._require_revision(run, expected_revision, "schedule run")
            if str(run["state"]) != "due" or run["bundle_key"] is not None:
                raise TransitionError("only an unclaimed due schedule run can dispatch")
            schedule = self._schedule_row(int(run["schedule_key"]))
            bundle = self._bundle_row(bundle_key)
            if (
                str(bundle["profile_id"]) != str(schedule["profile_id"])
                or str(bundle["source_bucket"]) != str(schedule["bucket"])
                or str(bundle["status"]) != "active"
            ):
                raise ConflictError(
                    "schedule content claim does not match profile and bucket"
                )
            if bundle["claimed_by_id"] is not None:
                raise ConflictError("bundle is already transactionally claimed")
            now = self._now_text()
            updated_run = self._connection.execute(
                "UPDATE schedule_runs SET bundle_key = ?, state = 'dispatching', "
                "revision = revision + 1, updated_at = ? WHERE run_id = ? "
                "AND state = 'due' AND bundle_key IS NULL AND revision = ?",
                (bundle_key, now, run_id, expected_revision),
            )
            if updated_run.rowcount != 1:
                raise ConflictError("schedule run claim became stale")
            updated_bundle = self._connection.execute(
                "UPDATE bundles SET claimed_by_type = 'schedule', claimed_by_id = ?, "
                "updated_at = ? WHERE bundle_key = ? AND claimed_by_id IS NULL",
                (str(run_id), now, bundle_key),
            )
            if updated_bundle.rowcount != 1:
                raise ConflictError("bundle schedule claim became stale")
        return self.get_schedule_run(run_id)

    def claim_schedule_occurrence(
        self,
        schedule_key: int,
        *,
        local_date: str,
        scheduled_at: datetime,
        utc_offset_minutes: int,
        schedule_hash: str,
        bundle_key: int,
    ) -> ScheduleRunRecord:
        """Resume the queued→due→dispatching graph while claiming exact content."""

        run = self.create_schedule_occurrence(
            schedule_key,
            local_date=local_date,
            scheduled_at=scheduled_at,
            utc_offset_minutes=utc_offset_minutes,
            schedule_hash=schedule_hash,
        )
        if run.state == "queued":
            run = self.transition_schedule_run(
                run.run_id, "due", expected_revision=run.revision
            )
        if run.state == "due":
            return self.claim_schedule_content(
                run.run_id, bundle_key, expected_revision=run.revision
            )
        if run.state == "dispatching" and run.bundle_key == bundle_key:
            return run
        raise ConflictError("schedule occurrence cannot claim different content")

    def get_schedule_run(self, run_id: int) -> ScheduleRunRecord:
        return self._schedule_run_from_row(self._schedule_run_row(run_id))

    def transition_schedule_run(
        self,
        run_id: int,
        state: ScheduleRunState,
        *,
        expected_revision: int,
    ) -> ScheduleRunRecord:
        """Apply one legal, revision-guarded schedule-run transition."""

        if state not in _SCHEDULE_RUN_TRANSITIONS:
            raise StateValidationError("unknown schedule run state")
        with self._transaction():
            row = self._schedule_run_row(run_id)
            self._require_revision(row, expected_revision, "schedule run")
            current = str(row["state"])
            if state not in _SCHEDULE_RUN_TRANSITIONS[current]:
                raise TransitionError("illegal schedule run transition")
            if state == "no_content" and row["bundle_key"] is not None:
                raise TransitionError(
                    "schedule run with claimed content cannot become no-content"
                )
            updated = self._connection.execute(
                "UPDATE schedule_runs SET state = ?, revision = revision + 1, "
                "updated_at = ? WHERE run_id = ? AND state = ? AND revision = ?",
                (state, self._now_text(), run_id, current, expected_revision),
            )
            if updated.rowcount != 1:
                raise ConflictError("schedule run transition became stale")
        return self.get_schedule_run(run_id)

    def next_selection_counter(self, profile_id: str) -> int:
        with self._transaction():
            self._require_profile(profile_id)
            row = self._connection.execute(
                "SELECT lifetime_counter FROM selection_counters WHERE profile_id = ?",
                (profile_id,),
            ).fetchone()
            current = 0 if row is None else int(row[0])
            if row is None:
                self._connection.execute(
                    "INSERT INTO selection_counters(profile_id, lifetime_counter, updated_at) "
                    "VALUES (?, 1, ?)",
                    (profile_id, self._now_text()),
                )
            else:
                self._connection.execute(
                    "UPDATE selection_counters SET lifetime_counter = lifetime_counter + 1, "
                    "updated_at = ? WHERE profile_id = ?",
                    (self._now_text(), profile_id),
                )
        return current

    def set_paused(self, paused: bool, *, expected_revision: int) -> PauseRecord:
        with self._transaction():
            row = self._connection.execute(
                "SELECT * FROM pause_state WHERE singleton = 1"
            ).fetchone()
            self._require_revision(cast(sqlite3.Row, row), expected_revision, "pause state")
            self._connection.execute(
                "UPDATE pause_state SET paused = ?, revision = revision + 1, updated_at = ? "
                "WHERE singleton = 1",
                (int(paused), self._now_text()),
            )
        return self.get_pause_state()

    def get_pause_state(self) -> PauseRecord:
        row = self._connection.execute(
            "SELECT paused, revision FROM pause_state WHERE singleton = 1"
        ).fetchone()
        return PauseRecord(paused=bool(row["paused"]), revision=int(row["revision"]))

    def create_run_request(
        self,
        *,
        profile_id: str,
        action: str,
        arguments: Mapping[str, object],
        idempotency_key: str,
        expected_revision: int,
        bundle_key: int | None = None,
        schedule_key: int | None = None,
        intent_id: str | None = None,
    ) -> RunRequestRecord:
        with self._transaction():
            return self._create_run_request_locked(
                profile_id=profile_id,
                action=action,
                arguments=arguments,
                idempotency_key=idempotency_key,
                expected_revision=expected_revision,
                bundle_key=bundle_key,
                schedule_key=schedule_key,
                intent_id=intent_id,
                approved_intent_id=None,
            )

    def _create_run_request_locked(
        self,
        *,
        profile_id: str,
        action: str,
        arguments: Mapping[str, object],
        idempotency_key: str,
        expected_revision: int,
        bundle_key: int | None,
        schedule_key: int | None,
        intent_id: str | None,
        approved_intent_id: str | None,
    ) -> RunRequestRecord:
        if action not in _REQUEST_ACTIONS:
            raise StateValidationError("run request action is not allow-listed")
        _validate_identifier(idempotency_key, "idempotency key")
        if expected_revision < 0:
            raise StateValidationError("expected revision must be non-negative")
        self._require_profile(profile_id)
        arguments_json = self._safe_json(arguments)
        confirmation_required = self._validate_request_action_locked(
            action,
            json.loads(arguments_json),
            profile_id,
            bundle_key,
            schedule_key,
        )
        binding = self._safe_json(
            {
                "profile_id": profile_id,
                "action": action,
                "arguments": json.loads(arguments_json),
                "expected_revision": expected_revision,
                "bundle_key": bundle_key,
                "schedule_key": schedule_key,
                "intent_id": intent_id,
            }
        )
        request_hash = _sha256_text(binding)
        existing = self._connection.execute(
            "SELECT * FROM run_requests WHERE idempotency_key = ?", (idempotency_key,)
        ).fetchone()
        if existing is not None:
            if str(existing["request_sha256"]) != request_hash:
                raise ConflictError("idempotency key was used for a different request")
            return self._request_from_row(existing)
        if confirmation_required and (
            approved_intent_id is None or approved_intent_id != intent_id
        ):
            raise TransitionError("run request requires an approved confirmation intent")
        if not confirmation_required and intent_id is not None:
            raise StateValidationError(
                "run request action does not accept a confirmation intent"
            )
        self._assert_request_resource_revision_locked(
            action=action,
            profile_id=profile_id,
            bundle_key=bundle_key,
            schedule_key=schedule_key,
            expected_revision=expected_revision,
        )
        now = self._now_text()
        cursor = self._connection.execute(
            "INSERT INTO run_requests(profile_id, action, arguments_json, request_sha256, "
            "idempotency_key, expected_revision, bundle_key, schedule_key, intent_id, "
            "status, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'queued', ?, ?)",
            (
                profile_id,
                action,
                arguments_json,
                request_hash,
                idempotency_key,
                expected_revision,
                bundle_key,
                schedule_key,
                intent_id,
                now,
                now,
            ),
        )
        return self._request_from_row(
            cast(
                sqlite3.Row,
                self._connection.execute(
                    "SELECT * FROM run_requests WHERE request_id = ?",
                    (_lastrowid(cursor),),
                ).fetchone(),
            )
        )

    def claim_next_run_request(self, worker_token: str) -> RunRequestRecord | None:
        _validate_identifier(worker_token, "worker token")
        with self._transaction():
            row = self._connection.execute(
                "SELECT * FROM run_requests WHERE status = 'queued' "
                "ORDER BY request_id LIMIT 1"
            ).fetchone()
            if row is None:
                return None
            self._connection.execute(
                "UPDATE run_requests SET status = 'claimed', worker_token = ?, "
                "revision = revision + 1, updated_at = ? WHERE request_id = ?",
                (worker_token, self._now_text(), int(row["request_id"])),
            )
            claimed = self._connection.execute(
                "SELECT * FROM run_requests WHERE request_id = ?",
                (int(row["request_id"]),),
            ).fetchone()
            return self._request_from_row(cast(sqlite3.Row, claimed))

    def complete_run_request(
        self,
        request_id: int,
        *,
        worker_token: str,
        result: Mapping[str, object],
        failed: bool = False,
    ) -> RunRequestRecord:
        result_json = self._safe_json(result)
        with self._transaction():
            row = self._request_row(request_id)
            if str(row["status"]) != "claimed" or str(row["worker_token"]) != worker_token:
                raise TransitionError("run request is not claimed by this worker")
            self._connection.execute(
                "UPDATE run_requests SET status = ?, result_json = ?, "
                "revision = revision + 1, updated_at = ? WHERE request_id = ?",
                (
                    "failed" if failed else "completed",
                    result_json,
                    self._now_text(),
                    request_id,
                ),
            )
        return self.get_run_request(request_id)

    def get_run_request(self, request_id: int) -> RunRequestRecord:
        return self._request_from_row(self._request_row(request_id))

    def create_confirmation_intent(
        self,
        *,
        action: str,
        arguments: Mapping[str, object],
        profile_id: str,
        resource_revision: int,
        fingerprint: str | None,
        consequence: str,
        expires_at: datetime,
        bundle_key: int | None = None,
        schedule_key: int | None = None,
    ) -> ConfirmationIntentRecord:
        if action not in _REQUEST_ACTIONS:
            raise StateValidationError("confirmation action is not allow-listed")
        if resource_revision < 0:
            raise StateValidationError("resource revision must be non-negative")
        if fingerprint is not None:
            _validate_sha256(fingerprint, "confirmation fingerprint")
        consequence = self._safe_text(consequence, "confirmation consequence", maximum=512)
        expires_text = _timestamp(expires_at)
        if _parse_timestamp(expires_text) <= self._now():
            raise StateValidationError("confirmation intent expiry must be in the future")
        arguments_json = self._safe_json(arguments)
        intent_id = secrets.token_hex(16)
        nonce = secrets.token_hex(32)
        now = self._now_text()
        with self._transaction():
            self._require_profile(profile_id)
            confirmation_required = self._validate_request_action_locked(
                action,
                json.loads(arguments_json),
                profile_id,
                bundle_key,
                schedule_key,
            )
            if not confirmation_required:
                raise StateValidationError(
                    "confirmation intent is not valid for this action"
                )
            self._assert_confirmation_resource_locked(
                action=action,
                profile_id=profile_id,
                bundle_key=bundle_key,
                schedule_key=schedule_key,
                resource_revision=resource_revision,
                fingerprint=fingerprint,
            )
            binding = self._intent_binding(
                action,
                arguments_json,
                profile_id,
                bundle_key,
                schedule_key,
                resource_revision,
                fingerprint,
            )
            self._connection.execute(
                "INSERT INTO confirmation_intents(intent_id, action, arguments_json, "
                "binding_sha256, profile_id, bundle_key, schedule_key, resource_revision, "
                "fingerprint, consequence, nonce, expires_at, state, created_at, updated_at) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'pending', ?, ?)",
                (
                    intent_id,
                    action,
                    arguments_json,
                    binding,
                    profile_id,
                    bundle_key,
                    schedule_key,
                    resource_revision,
                    fingerprint,
                    consequence,
                    nonce,
                    expires_text,
                    now,
                    now,
                ),
            )
        return self.get_confirmation_intent(intent_id)

    def approve_confirmation_intent(
        self, intent_id: str, *, expected_revision: int
    ) -> ConfirmationIntentRecord:
        expired = False
        with self._transaction():
            row = self._intent_row(intent_id)
            self._require_revision(row, expected_revision, "confirmation intent")
            if str(row["state"]) != "pending":
                raise TransitionError("only a pending confirmation intent can be approved")
            if _parse_timestamp(str(row["expires_at"])) <= self._now():
                self._expire_intent_locked(intent_id)
                expired = True
            else:
                self._connection.execute(
                    "UPDATE confirmation_intents SET state = 'approved', "
                    "revision = revision + 1, updated_at = ? WHERE intent_id = ?",
                    (self._now_text(), intent_id),
                )
        if expired:
            raise TransitionError("confirmation intent expired")
        return self.get_confirmation_intent(intent_id)

    def consume_intent_with_request(
        self,
        *,
        intent_id: str,
        action: str,
        arguments: Mapping[str, object],
        profile_id: str,
        resource_revision: int,
        fingerprint: str | None,
        idempotency_key: str,
        bundle_key: int | None = None,
        schedule_key: int | None = None,
    ) -> RunRequestRecord:
        expired = False
        result: RunRequestRecord | None = None
        with self._transaction():
            arguments_json = self._safe_json(arguments)
            binding = self._intent_binding(
                action,
                arguments_json,
                profile_id,
                bundle_key,
                schedule_key,
                resource_revision,
                fingerprint,
            )
            existing_request = self._connection.execute(
                "SELECT * FROM run_requests WHERE idempotency_key = ?",
                (idempotency_key,),
            ).fetchone()
            if existing_request is not None:
                result = self._create_run_request_locked(
                    profile_id=profile_id,
                    action=action,
                    arguments=arguments,
                    idempotency_key=idempotency_key,
                    expected_revision=resource_revision,
                    bundle_key=bundle_key,
                    schedule_key=schedule_key,
                    intent_id=intent_id,
                    approved_intent_id=intent_id,
                )
            else:
                row = self._intent_row(intent_id)
                if _parse_timestamp(str(row["expires_at"])) <= self._now():
                    self._expire_intent_locked(intent_id)
                    expired = True
                elif str(row["state"]) != "approved":
                    raise TransitionError("confirmation intent is not approved")
                elif str(row["binding_sha256"]) != binding:
                    raise ConflictError("confirmation intent binding drift")
                elif (
                    cast(int | None, row["bundle_key"]) != bundle_key
                    or cast(int | None, row["schedule_key"]) != schedule_key
                ):
                    raise ConflictError("confirmation intent resource drift")
                else:
                    self._assert_confirmation_resource_locked(
                        action=action,
                        profile_id=profile_id,
                        bundle_key=bundle_key,
                        schedule_key=schedule_key,
                        resource_revision=resource_revision,
                        fingerprint=fingerprint,
                    )
                    result = self._create_run_request_locked(
                        profile_id=profile_id,
                        action=action,
                        arguments=arguments,
                        idempotency_key=idempotency_key,
                        expected_revision=resource_revision,
                        bundle_key=bundle_key,
                        schedule_key=schedule_key,
                        intent_id=intent_id,
                        approved_intent_id=intent_id,
                    )
                    consumed = self._connection.execute(
                        "UPDATE confirmation_intents SET state = 'consumed', "
                        "revision = revision + 1, updated_at = ? WHERE intent_id = ? "
                        "AND state = 'approved' AND binding_sha256 = ? AND revision = ?",
                        (
                            self._now_text(),
                            intent_id,
                            binding,
                            int(row["revision"]),
                        ),
                    )
                    if consumed.rowcount != 1:
                        raise ConflictError("confirmation intent consumption became stale")
        if expired:
            raise TransitionError("confirmation intent expired")
        return cast(RunRequestRecord, result)

    def get_confirmation_intent(self, intent_id: str) -> ConfirmationIntentRecord:
        return self._intent_from_row(self._intent_row(intent_id))

    def start_admission(
        self,
        *,
        profile_id: str,
        bucket: SourceBucket,
        bundle_id: str,
        fingerprint: str,
        source_path: str,
        destination_path: str,
        intent_id: str | None,
    ) -> AdmissionRecord:
        bucket = _validate_bucket(bucket)
        _validate_bundle_id(bundle_id)
        _validate_sha256(fingerprint, "admission fingerprint")
        source_path = _relative_path_text(source_path, "admission source path")
        destination_path = _relative_path_text(
            destination_path, "admission destination path"
        )
        with self._transaction():
            self._require_profile(profile_id)
            existing = self._connection.execute(
                "SELECT * FROM admission_journals WHERE profile_id = ? "
                "AND bucket = ? AND bundle_id = ?",
                (profile_id, bucket, bundle_id),
            ).fetchone()
            if existing is not None:
                expected = (
                    fingerprint,
                    source_path,
                    destination_path,
                    intent_id,
                )
                stored = tuple(
                    existing[name]
                    for name in (
                        "fingerprint",
                        "source_path",
                        "destination_path",
                        "intent_id",
                    )
                )
                if stored != expected:
                    raise ConflictError("admission journal conflicts with existing bundle")
                return self._admission_from_row(existing)
            now = self._now_text()
            try:
                cursor = self._connection.execute(
                    "INSERT INTO admission_journals(profile_id, bucket, bundle_id, "
                    "fingerprint, source_path, destination_path, intent_id, phase, "
                    "created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, 'started', ?, ?)",
                    (
                        profile_id,
                        bucket,
                        bundle_id,
                        fingerprint,
                        source_path,
                        destination_path,
                        intent_id,
                        now,
                        now,
                    ),
                )
            except sqlite3.IntegrityError:
                raise ConflictError(
                    "admission fingerprint or destination was already used"
                ) from None
            journal_id = _lastrowid(cursor)
        return self.get_admission(journal_id)

    def checkpoint_admission_member(
        self,
        journal_id: int,
        *,
        relative_name: str,
        sha256: str,
        size_bytes: int,
        phase: str,
    ) -> AdmissionMemberRecord:
        relative_name = _relative_member_name(relative_name)
        _validate_sha256(sha256, "admission member hash")
        if size_bytes < 0:
            raise StateValidationError("admission member size is invalid")
        if phase not in _ADMISSION_MEMBER_PHASES:
            raise StateValidationError("admission member phase is invalid")
        with self._transaction():
            journal = self._admission_row(journal_id)
            if str(journal["phase"]) in {
                "ready_installed",
                "installed",
                "rolled_back",
            }:
                raise TransitionError(
                    "sealed admission journal cannot checkpoint members"
                )
            existing = self._connection.execute(
                "SELECT * FROM admission_members WHERE journal_id = ? AND relative_name = ?",
                (journal_id, relative_name),
            ).fetchone()
            if existing is not None:
                if (str(existing["sha256"]), int(existing["size_bytes"])) != (
                    sha256,
                    size_bytes,
                ):
                    raise ConflictError("admission member identity is immutable")
                current_phase = str(existing["phase"])
                if phase == current_phase:
                    return self._admission_member_from_row(existing)
                if (
                    _ADMISSION_MEMBER_PHASE_ORDER[phase]
                    != _ADMISSION_MEMBER_PHASE_ORDER[current_phase] + 1
                ):
                    raise TransitionError(
                        "admission member phase must advance one checkpoint at a time"
                    )
                now = self._now_text()
                self._connection.execute(
                    "UPDATE admission_members SET phase = ?, updated_at = ? "
                    "WHERE journal_id = ? AND relative_name = ? AND phase = ?",
                    (phase, now, journal_id, relative_name, current_phase),
                )
                self._connection.execute(
                    "UPDATE admission_journals SET phase = 'copying', "
                    "revision = revision + 1, updated_at = ? WHERE journal_id = ?",
                    (now, journal_id),
                )
            else:
                if phase != "planned":
                    raise TransitionError(
                        "admission member must begin with a planned checkpoint"
                    )
                now = self._now_text()
                self._connection.execute(
                    "INSERT INTO admission_members(journal_id, relative_name, sha256, "
                    "size_bytes, phase, updated_at) VALUES (?, ?, ?, ?, ?, ?)",
                    (journal_id, relative_name, sha256, size_bytes, phase, now),
                )
                self._connection.execute(
                    "UPDATE admission_journals SET phase = 'copying', "
                    "revision = revision + 1, updated_at = ? WHERE journal_id = ?",
                    (now, journal_id),
                )
        row = self._connection.execute(
            "SELECT * FROM admission_members WHERE journal_id = ? AND relative_name = ?",
            (journal_id, relative_name),
        ).fetchone()
        return self._admission_member_from_row(cast(sqlite3.Row, row))

    def advance_admission(
        self, journal_id: int, phase: str, *, expected_revision: int
    ) -> AdmissionRecord:
        if phase not in _ADMISSION_PHASES:
            raise StateValidationError("admission phase is invalid")
        with self._transaction():
            row = self._admission_row(journal_id)
            self._require_revision(row, expected_revision, "admission journal")
            current = str(row["phase"])
            if phase == current:
                return self._admission_from_row(row)
            if current in {"installed", "rolled_back"} or phase == "started":
                raise TransitionError("illegal admission journal transition")
            if phase == "rolled_back":
                allowed = True
            else:
                allowed = (
                    (current, phase)
                    in {("copying", "ready_installed"), ("ready_installed", "installed")}
                )
            if not allowed:
                raise TransitionError("admission phase cannot move backward")
            if phase == "ready_installed":
                incomplete = self._connection.execute(
                    "SELECT 1 FROM admission_members WHERE journal_id = ? "
                    "AND phase != 'verified' LIMIT 1",
                    (journal_id,),
                ).fetchone()
                member_count = int(
                    self._connection.execute(
                        "SELECT COUNT(*) FROM admission_members WHERE journal_id = ?",
                        (journal_id,),
                    ).fetchone()[0]
                )
                if member_count == 0 or incomplete is not None:
                    raise TransitionError(
                        "all admission members must be verified before ready install"
                    )
            if phase != current:
                self._connection.execute(
                    "UPDATE admission_journals SET phase = ?, revision = revision + 1, "
                    "updated_at = ? WHERE journal_id = ?",
                    (phase, self._now_text(), journal_id),
                )
        return self.get_admission(journal_id)

    def get_admission(self, journal_id: int) -> AdmissionRecord:
        return self._admission_from_row(self._admission_row(journal_id))

    def list_admission_members(
        self, journal_id: int
    ) -> tuple[AdmissionMemberRecord, ...]:
        rows = self._connection.execute(
            "SELECT * FROM admission_members WHERE journal_id = ? "
            "ORDER BY relative_name COLLATE NOCASE, relative_name",
            (journal_id,),
        )
        return tuple(self._admission_member_from_row(row) for row in rows)

    def _mark_ambiguous_locked(
        self,
        bundle_key: int,
        platform: Platform,
        error_code: str,
        error_message: str,
        *,
        claim_token: str,
        attempt_count: int,
    ) -> None:
        now = self._now_text()
        updated = self._connection.execute(
            "UPDATE deliveries SET status = 'ambiguous', phase = NULL, "
            "safe_to_retry = 0, next_attempt_at = NULL, claim_token = NULL, "
            "error_code = ?, error_message = ?, "
            "revision = revision + 1, updated_at = ? "
            "WHERE bundle_key = ? AND platform = ? AND status = 'in_flight' "
            "AND claim_token = ? AND attempt_count = ?",
            (
                error_code,
                error_message,
                now,
                bundle_key,
                platform,
                claim_token,
                attempt_count,
            ),
        )
        if updated.rowcount != 1:
            raise ConflictError("delivery claim became stale")
        self._block_bundle_locked(bundle_key, error_code)
        self._insert_event_locked(
            bundle_key,
            platform,
            "delivery_ambiguous",
            f"delivery_ambiguous:{error_code}",
            {"error_code": error_code},
        )

    def _block_bundle_locked(self, bundle_key: int, reason: str) -> None:
        self._connection.execute(
            "UPDATE bundles SET status = 'blocked', block_reason = ?, "
            "revision = revision + 1, updated_at = ? WHERE bundle_key = ? "
            "AND status != 'blocked'",
            (reason, self._now_text(), bundle_key),
        )

    def _insert_event_locked(
        self,
        bundle_key: int | None,
        platform: Platform | None,
        event_type: str,
        event_code: str,
        details: Mapping[str, object],
        *,
        dedupe_key: str | None = None,
    ) -> None:
        if event_type not in _EVENT_TYPES:
            raise StateValidationError("event type is not allow-listed")
        event_code = self._safe_text(event_code, "event code", maximum=128)
        detail_json = self._safe_json(details)
        if dedupe_key is None:
            dedupe_key = _sha256_text(
                f"{bundle_key}:{platform}:{event_type}:{event_code}"
            )
        self._connection.execute(
            "INSERT OR IGNORE INTO events(occurred_at, bundle_key, platform, "
            "event_type, event_code, safe_detail_json, dedupe_key) "
            "VALUES (?, ?, ?, ?, ?, ?, ?)",
            (
                self._now_text(),
                bundle_key,
                platform,
                event_type,
                event_code,
                detail_json,
                dedupe_key,
            ),
        )

    def _processing_metadata_json(self, metadata: Mapping[str, object]) -> str:
        unknown = set(metadata) - _PROCESSING_METADATA_KEYS
        if unknown:
            raise StateValidationError("processing metadata contains unsupported fields")
        for key, value in metadata.items():
            if not isinstance(value, (str, int, float, bool)) or isinstance(value, complex):
                raise StateValidationError("processing metadata values must be scalar")
            if isinstance(value, str) and len(value) > 256:
                raise StateValidationError("processing metadata value is too long")
            if key == "progress_percent" and (
                isinstance(value, bool)
                or not isinstance(value, (int, float))
                or not 0 <= value <= 100
            ):
                raise StateValidationError("processing progress is out of range")
        return self._safe_json(metadata)

    def _safe_json(self, value: object) -> str:
        try:
            rendered = json.dumps(
                value,
                sort_keys=True,
                separators=(",", ":"),
                ensure_ascii=True,
                allow_nan=False,
            )
        except (TypeError, ValueError):
            raise StateValidationError("durable metadata must be bounded JSON") from None
        if len(rendered.encode("utf-8")) > 65536:
            raise StateValidationError("durable metadata exceeds the size limit")
        self._reject_secret(rendered)
        return rendered

    def _safe_text(self, value: str, label: str, *, maximum: int = 1024) -> str:
        if (
            not value
            or len(value) > maximum
            or any(ord(character) < 32 and character not in "\t\n" for character in value)
        ):
            raise StateValidationError(f"{label} is invalid")
        self._reject_secret(value)
        return value

    def _reject_secret(self, rendered: str) -> None:
        if any(secret in rendered for secret in self._secrets):
            raise StateValidationError("durable operator context contains a configured secret")

    def _intent_binding(
        self,
        action: str,
        arguments_json: str,
        profile_id: str,
        bundle_key: int | None,
        schedule_key: int | None,
        resource_revision: int,
        fingerprint: str | None,
    ) -> str:
        return _sha256_text(
            self._safe_json(
                {
                    "action": action,
                    "arguments": json.loads(arguments_json),
                    "profile_id": profile_id,
                    "bundle_key": bundle_key,
                    "schedule_key": schedule_key,
                    "resource_revision": resource_revision,
                    "fingerprint": fingerprint,
                }
            )
        )

    def _validate_request_action_locked(
        self,
        action: str,
        arguments: object,
        profile_id: str,
        bundle_key: int | None,
        schedule_key: int | None,
    ) -> bool:
        if not isinstance(arguments, dict):
            raise StateValidationError("run request arguments must be an object")
        if action in _BUNDLE_REQUEST_ACTIONS:
            if bundle_key is None or schedule_key is not None:
                raise StateValidationError(
                    "run request action requires exactly one bundle resource"
                )
        elif action in _SCHEDULE_REQUEST_ACTIONS:
            if schedule_key is None or bundle_key is not None:
                raise StateValidationError(
                    "run request action requires exactly one schedule resource"
                )
        elif action in _PROFILE_REQUEST_ACTIONS:
            if bundle_key is not None or schedule_key is not None:
                raise StateValidationError(
                    "run request action is bound only to its profile"
                )
        elif action == "run_now":
            if schedule_key is not None:
                raise StateValidationError("run-now cannot bind a schedule resource")
        else:
            raise StateValidationError("run request action is not allow-listed")

        if bundle_key is not None and str(
            self._bundle_row(bundle_key)["profile_id"]
        ) != profile_id:
            raise ConflictError("run request bundle belongs to another profile")
        if schedule_key is not None and str(
            self._schedule_row(schedule_key)["profile_id"]
        ) != profile_id:
            raise ConflictError("run request schedule belongs to another profile")

        if action in _ALWAYS_CONFIRMED_ACTIONS:
            return True
        if action == "schedule_create":
            return arguments.get("enabled") is True
        if action == "schedule_update":
            schedule = self._schedule_row(cast(int, schedule_key))
            due = self._connection.execute(
                "SELECT 1 FROM schedule_runs WHERE schedule_key = ? "
                "AND state IN ('queued', 'due', 'dispatching') LIMIT 1",
                (schedule_key,),
            ).fetchone()
            return bool(schedule["enabled"]) or due is not None or arguments.get(
                "enabled"
            ) is True
        return False

    def _assert_request_resource_revision_locked(
        self,
        *,
        action: str,
        profile_id: str,
        bundle_key: int | None,
        schedule_key: int | None,
        expected_revision: int,
    ) -> None:
        """Bind a new request to the current resource inside its insert transaction."""

        if bundle_key is not None:
            self._require_revision(
                self._bundle_row(bundle_key), expected_revision, "run request bundle"
            )
            return
        if schedule_key is not None:
            self._require_revision(
                self._schedule_row(schedule_key),
                expected_revision,
                "run request schedule",
            )
            return
        if action in {"pause", "resume"}:
            pause = cast(
                sqlite3.Row,
                self._connection.execute(
                    "SELECT * FROM pause_state WHERE singleton = 1"
                ).fetchone(),
            )
            self._require_revision(pause, expected_revision, "run request pause state")
            return
        profile = self._connection.execute(
            "SELECT * FROM profiles WHERE profile_id = ?", (profile_id,)
        ).fetchone()
        self._require_revision(
            cast(sqlite3.Row, profile), expected_revision, "run request profile"
        )

    def _assert_confirmation_resource_locked(
        self,
        *,
        action: str,
        profile_id: str,
        bundle_key: int | None,
        schedule_key: int | None,
        resource_revision: int,
        fingerprint: str | None,
    ) -> None:
        if bundle_key is not None:
            bundle = self._bundle_row(bundle_key)
            if str(bundle["profile_id"]) != profile_id:
                raise ConflictError("confirmation bundle belongs to another profile")
            self._require_revision(bundle, resource_revision, "confirmation bundle")
            if fingerprint is None or str(bundle["fingerprint"]) != fingerprint:
                raise ConflictError("confirmation bundle fingerprint drift")
            return
        if schedule_key is not None:
            schedule = self._schedule_row(schedule_key)
            if str(schedule["profile_id"]) != profile_id:
                raise ConflictError("confirmation schedule belongs to another profile")
            self._require_revision(
                schedule, resource_revision, "confirmation schedule"
            )
            if fingerprint is None or str(schedule["config_hash"]) != fingerprint:
                raise ConflictError("confirmation schedule fingerprint drift")
            return
        if action in {"pause", "resume"}:
            pause = cast(
                sqlite3.Row,
                self._connection.execute(
                    "SELECT * FROM pause_state WHERE singleton = 1"
                ).fetchone(),
            )
            self._require_revision(pause, resource_revision, "confirmation pause state")
        else:
            profile = self._connection.execute(
                "SELECT * FROM profiles WHERE profile_id = ?", (profile_id,)
            ).fetchone()
            self._require_revision(
                cast(sqlite3.Row, profile),
                resource_revision,
                "confirmation profile",
            )
        if action == "admit_draft":
            if fingerprint is None:
                raise StateValidationError(
                    "draft admission confirmation requires a fingerprint"
                )
        elif fingerprint is not None:
            raise StateValidationError(
                "profile confirmation cannot bind a bundle fingerprint"
            )

    def _expire_intent_locked(self, intent_id: str) -> None:
        self._connection.execute(
            "UPDATE confirmation_intents SET state = 'expired', "
            "revision = revision + 1, updated_at = ? WHERE intent_id = ?",
            (self._now_text(), intent_id),
        )

    def _bundle_row(self, bundle_key: int) -> sqlite3.Row:
        row = self._connection.execute(
            "SELECT * FROM bundles WHERE bundle_key = ?", (bundle_key,)
        ).fetchone()
        if row is None:
            raise StateValidationError("unknown bundle")
        return cast(sqlite3.Row, row)

    def _delivery_row(self, bundle_key: int, platform: Platform) -> sqlite3.Row:
        row = self._connection.execute(
            "SELECT * FROM deliveries WHERE bundle_key = ? AND platform = ?",
            (bundle_key, platform),
        ).fetchone()
        if row is None:
            raise StateValidationError("unknown delivery")
        return cast(sqlite3.Row, row)

    def _schedule_row(self, schedule_key: int) -> sqlite3.Row:
        row = self._connection.execute(
            "SELECT * FROM schedules WHERE schedule_key = ?", (schedule_key,)
        ).fetchone()
        if row is None:
            raise StateValidationError("unknown schedule")
        return cast(sqlite3.Row, row)

    def _schedule_run_row(self, run_id: int) -> sqlite3.Row:
        row = self._connection.execute(
            "SELECT * FROM schedule_runs WHERE run_id = ?", (run_id,)
        ).fetchone()
        if row is None:
            raise StateValidationError("unknown schedule run")
        return cast(sqlite3.Row, row)

    def _request_row(self, request_id: int) -> sqlite3.Row:
        row = self._connection.execute(
            "SELECT * FROM run_requests WHERE request_id = ?", (request_id,)
        ).fetchone()
        if row is None:
            raise StateValidationError("unknown run request")
        return cast(sqlite3.Row, row)

    def _intent_row(self, intent_id: str) -> sqlite3.Row:
        row = self._connection.execute(
            "SELECT * FROM confirmation_intents WHERE intent_id = ?", (intent_id,)
        ).fetchone()
        if row is None:
            raise StateValidationError("unknown confirmation intent")
        return cast(sqlite3.Row, row)

    def _admission_row(self, journal_id: int) -> sqlite3.Row:
        row = self._connection.execute(
            "SELECT * FROM admission_journals WHERE journal_id = ?", (journal_id,)
        ).fetchone()
        if row is None:
            raise StateValidationError("unknown admission journal")
        return cast(sqlite3.Row, row)

    def _require_profile(self, profile_id: str) -> None:
        if self._connection.execute(
            "SELECT 1 FROM profiles WHERE profile_id = ?", (profile_id,)
        ).fetchone() is None:
            raise StateValidationError("unknown profile")

    @staticmethod
    def _require_delivery_claim(
        row: sqlite3.Row, claim_token: str, attempt_count: int
    ) -> None:
        _validate_identifier(claim_token, "claim token")
        if attempt_count <= 0:
            raise StateValidationError("delivery attempt identity is invalid")
        if (
            str(row["status"]) != "in_flight"
            or str(row["claim_token"]) != claim_token
            or int(row["attempt_count"]) != attempt_count
        ):
            raise ConflictError("delivery claim is stale")

    @staticmethod
    def _validated_staged_artifact_path(
        relative_path: str, kind: str, bundle: sqlite3.Row
    ) -> str:
        normalized = _relative_path_text(relative_path, "staged artifact path")
        path = PurePosixPath(normalized)
        profile_id = str(bundle["profile_id"])
        bucket = str(bundle["source_bucket"])
        fingerprint = str(bundle["fingerprint"])
        if kind == "staged_private":
            if len(path.parts) != 4 or path.parts[:3] != (
                profile_id,
                bucket,
                fingerprint,
            ):
                raise StateValidationError(
                    "private staged artifact path does not match bundle identity"
                )
        else:
            prefix = f"{profile_id}-{bucket}-{fingerprint}-"
            if len(path.parts) != 1 or not path.name.startswith(prefix):
                raise StateValidationError(
                    "public staged artifact path does not match bundle identity"
                )
        return normalized

    @staticmethod
    def _require_revision(
        row: sqlite3.Row, expected_revision: int | None, label: str
    ) -> None:
        if expected_revision is None or int(row["revision"]) != expected_revision:
            raise ConflictError(f"{label} revision conflict")

    def _profile_from_row(self, row: sqlite3.Row) -> ProfileRecord:
        return ProfileRecord(
            profile_id=str(row["profile_id"]),
            account_root=Path(str(row["account_root"])),
            config_hash=str(row["config_hash"]),
            revision=int(row["revision"]),
        )

    def _bundle_from_row(self, row: sqlite3.Row) -> BundleRecord:
        return BundleRecord(
            bundle_key=int(row["bundle_key"]),
            profile_id=str(row["profile_id"]),
            bundle_id=str(row["bundle_id"]),
            fingerprint=str(row["fingerprint"]),
            source_bucket=cast(SourceBucket, str(row["source_bucket"])),
            profile_root_snapshot=Path(str(row["profile_root_snapshot"])),
            status=cast(BundleStatus, str(row["status"])),
            revision=int(row["revision"]),
            archive_path=cast(str | None, row["archive_path"]),
            ready_marker_name=str(row["ready_marker_name"]),
            ready_marker_archived=bool(row["ready_marker_archived"]),
        )

    def _bundle_file_from_row(self, row: sqlite3.Row) -> BundleFileSnapshot:
        return BundleFileSnapshot(
            relative_name=str(row["relative_name"]),
            role=str(row["role"]),
            ordinal=cast(int | None, row["ordinal"]),
            media_kind=cast(str | None, row["media_kind"]),
            mime_type=cast(str | None, row["mime_type"]),
            size_bytes=int(row["size_bytes"]),
            sha256=str(row["sha256"]),
            archived=bool(row["archived"]),
        )

    def _target_snapshot_from_row(self, row: sqlite3.Row) -> TargetSnapshot:
        return TargetSnapshot(
            platform=cast(Platform, str(row["platform"])),
            expected_remote_user_id=str(row["expected_remote_user_id"]),
            expected_username=str(row["expected_username"]),
            token_env_var=str(row["token_env_var"]),
            api_version=str(row["api_version"]),
            adapter_version=int(row["adapter_version"]),
            request_settings=cast(
                Mapping[str, object], json.loads(str(row["request_settings_json"]))
            ),
        )

    def _delivery_from_row(self, row: sqlite3.Row) -> DeliveryRecord:
        return DeliveryRecord(
            bundle_key=int(row["bundle_key"]),
            platform=cast(Platform, str(row["platform"])),
            status=cast(DeliveryStatus, str(row["status"])),
            phase=cast(DeliveryPhase | None, row["phase"]),
            attempt_count=int(row["attempt_count"]),
            consecutive_failures=int(row["consecutive_failures"]),
            safe_to_retry=bool(row["safe_to_retry"]),
            next_attempt_at=_optional_datetime(row["next_attempt_at"]),
            remote_id=cast(str | None, row["remote_id"]),
            error_code=cast(str | None, row["error_code"]),
            error_message=cast(str | None, row["error_message"]),
            revision=int(row["revision"]),
        )

    def _artifact_from_row(self, row: sqlite3.Row) -> ArtifactRecord:
        return ArtifactRecord(
            bundle_key=int(row["bundle_key"]),
            platform=cast(Platform, str(row["platform"])),
            attempt_count=int(row["attempt_count"]),
            kind=str(row["kind"]),
            ordinal=int(row["ordinal"]),
            external_id=cast(str | None, row["external_id"]),
            relative_path=cast(str | None, row["relative_path"]),
            sha256=cast(str | None, row["sha256"]),
            expires_at=_optional_datetime(row["expires_at"]),
            processing_metadata=cast(
                Mapping[str, object], json.loads(str(row["processing_metadata_json"]))
            ),
        )

    def _schedule_from_row(self, row: sqlite3.Row) -> ScheduleRecord:
        return ScheduleRecord(
            schedule_key=int(row["schedule_key"]),
            profile_id=str(row["profile_id"]),
            schedule_id=str(row["schedule_id"]),
            bucket=cast(SourceBucket, str(row["bucket"])),
            timezone=str(row["timezone"]),
            weekdays=tuple(int(day) for day in json.loads(str(row["weekdays_json"]))),
            local_time=str(row["local_time"]),
            misfire_grace_seconds=int(row["misfire_grace_seconds"]),
            enabled=bool(row["enabled"]),
            config_hash=str(row["config_hash"]),
            revision=int(row["revision"]),
        )

    def _schedule_run_from_row(self, row: sqlite3.Row) -> ScheduleRunRecord:
        return ScheduleRunRecord(
            run_id=int(row["run_id"]),
            schedule_key=int(row["schedule_key"]),
            bundle_key=cast(int | None, row["bundle_key"]),
            local_date=str(row["local_date"]),
            scheduled_at=_parse_timestamp(str(row["scheduled_at"])),
            utc_offset_minutes=int(row["utc_offset_minutes"]),
            schedule_hash=str(row["schedule_hash"]),
            state=cast(ScheduleRunState, str(row["state"])),
            revision=int(row["revision"]),
        )

    def _request_from_row(self, row: sqlite3.Row) -> RunRequestRecord:
        result = row["result_json"]
        return RunRequestRecord(
            request_id=int(row["request_id"]),
            profile_id=str(row["profile_id"]),
            action=str(row["action"]),
            arguments=cast(Mapping[str, object], json.loads(str(row["arguments_json"]))),
            idempotency_key=str(row["idempotency_key"]),
            expected_revision=int(row["expected_revision"]),
            bundle_key=cast(int | None, row["bundle_key"]),
            schedule_key=cast(int | None, row["schedule_key"]),
            intent_id=cast(str | None, row["intent_id"]),
            status=str(row["status"]),
            result=(
                None
                if result is None
                else cast(Mapping[str, object], json.loads(str(result)))
            ),
            revision=int(row["revision"]),
        )

    def _intent_from_row(self, row: sqlite3.Row) -> ConfirmationIntentRecord:
        return ConfirmationIntentRecord(
            intent_id=str(row["intent_id"]),
            action=str(row["action"]),
            arguments=cast(Mapping[str, object], json.loads(str(row["arguments_json"]))),
            profile_id=str(row["profile_id"]),
            bundle_key=cast(int | None, row["bundle_key"]),
            schedule_key=cast(int | None, row["schedule_key"]),
            resource_revision=int(row["resource_revision"]),
            fingerprint=cast(str | None, row["fingerprint"]),
            consequence=str(row["consequence"]),
            expires_at=_parse_timestamp(str(row["expires_at"])),
            state=str(row["state"]),
            revision=int(row["revision"]),
        )

    def _admission_from_row(self, row: sqlite3.Row) -> AdmissionRecord:
        return AdmissionRecord(
            journal_id=int(row["journal_id"]),
            profile_id=str(row["profile_id"]),
            bucket=cast(SourceBucket, str(row["bucket"])),
            bundle_id=str(row["bundle_id"]),
            fingerprint=str(row["fingerprint"]),
            source_path=str(row["source_path"]),
            destination_path=str(row["destination_path"]),
            intent_id=cast(str | None, row["intent_id"]),
            phase=str(row["phase"]),
            revision=int(row["revision"]),
        )

    def _admission_member_from_row(self, row: sqlite3.Row) -> AdmissionMemberRecord:
        return AdmissionMemberRecord(
            journal_id=int(row["journal_id"]),
            relative_name=str(row["relative_name"]),
            sha256=str(row["sha256"]),
            size_bytes=int(row["size_bytes"]),
            phase=str(row["phase"]),
        )

    def _now(self) -> datetime:
        value = self._clock()
        _timestamp(value)
        return value.astimezone(UTC)

    def _now_text(self) -> str:
        return _timestamp(self._now())


def _validate_platform(value: str) -> Platform:
    if value not in _PLATFORMS:
        raise StateValidationError("unsupported platform")
    return cast(Platform, value)


def _validate_bucket(value: str) -> SourceBucket:
    if value not in _BUCKETS:
        raise StateValidationError("unsupported source bucket")
    return cast(SourceBucket, value)


def _validate_profile_id(value: str) -> None:
    if not _PROFILE_RE.fullmatch(value):
        raise StateValidationError("profile ID is invalid")


def _validate_bundle_id(value: str) -> None:
    if not _BUNDLE_RE.fullmatch(value):
        raise StateValidationError("bundle ID is invalid")


def _validate_identifier(value: str, label: str) -> None:
    if not _IDENTIFIER_RE.fullmatch(value):
        raise StateValidationError(f"{label} is invalid")


def _validate_sha256(value: str, label: str) -> None:
    if not _SHA256_RE.fullmatch(value):
        raise StateValidationError(f"{label} must be a lowercase SHA-256 digest")


def _relative_member_name(value: str) -> str:
    path = PurePosixPath(value)
    if (
        not value
        or "\\" in value
        or path.is_absolute()
        or len(path.parts) != 1
        or path.parts[0] in {".", ".."}
    ):
        raise StateValidationError("bundle member name must be one relative component")
    return value


def _relative_path_text(value: str, label: str) -> str:
    path = PurePosixPath(value)
    if (
        not value
        or "\\" in value
        or path.is_absolute()
        or any(part in {".", ".."} for part in path.parts)
    ):
        raise StateValidationError(f"{label} must be a safe relative path")
    return path.as_posix()


def _validate_timezone(value: str) -> None:
    try:
        ZoneInfo(value)
    except (ZoneInfoNotFoundError, ValueError):
        raise StateValidationError("schedule timezone is invalid") from None


def _validate_local_time(value: str) -> None:
    try:
        parsed = datetime.strptime(value, "%H:%M")
    except ValueError:
        raise StateValidationError("schedule local time must be HH:MM") from None
    if parsed.strftime("%H:%M") != value:
        raise StateValidationError("schedule local time must be HH:MM")


def _timestamp(value: datetime | None) -> str:
    if value is None or value.tzinfo is None or value.utcoffset() is None:
        raise StateValidationError("timestamp must be timezone-aware")
    return value.astimezone(UTC).isoformat(timespec="microseconds").replace("+00:00", "Z")


def _parse_timestamp(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(UTC)


def _optional_datetime(value: object) -> datetime | None:
    return None if value is None else _parse_timestamp(str(value))


def _lastrowid(cursor: sqlite3.Cursor) -> int:
    value = cursor.lastrowid
    if value is None:
        raise StateError("database insert did not return an identifier")
    return value


def _sha256_text(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


__all__ = [
    "AdmissionMemberRecord",
    "AdmissionRecord",
    "ArtifactRecord",
    "BundleFileSnapshot",
    "BundleRecord",
    "ConfirmationIntentRecord",
    "ConflictError",
    "DeliveryRecord",
    "MigrationRequiredError",
    "PauseRecord",
    "ProfileRecord",
    "ProfileTargetSnapshot",
    "RunRequestRecord",
    "SCHEMA_VERSION",
    "ScheduleRecord",
    "ScheduleRunRecord",
    "StateError",
    "StateRepository",
    "StateValidationError",
    "TargetSnapshot",
    "TransitionError",
]
