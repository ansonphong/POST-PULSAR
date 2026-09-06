# Stage 2 — Technical design

Date: 2026-09-05

Status: candidate for design-quality gate

## 1. Goal and non-goals

POST PULSAR is a one-shot, local-first social publisher. Each invocation finds
one valid content bundle, snapshots the enabled X and Instagram targets,
preflights the bundle for every target, publishes sequentially, records each
remote outcome, and archives the exact source members only after every target
is known published.

This design modernizes identity, correctness, APIs, security, packaging,
scripts, tests, and documentation. It does not add another platform, post type,
UI, internal scheduler, cloud service, analytics, notification path, or live
test mode.

## 2. Naming contract

| Surface | Value |
|---|---|
| Human/product | POST PULSAR / Post Pulsar |
| Repository and directory | `POST-PULSAR` |
| Python distribution and CLI | `post-pulsar` |
| Python package | `post_pulsar` |
| Environment prefix | `POST_PULSAR_` |
| Windows task | `PostPulsar` |
| Default log | `post_pulsar.log` |

Anson Phong remains the author/copyright holder. Current files and documentation
must not retain PHONG-BOT as a product identity. Git history is not rewritten.

## 3. Component architecture

```text
src/post_pulsar/
  __init__.py          package version
  __main__.py          `python -m post_pulsar`
  cli.py               argument parsing, logging bootstrap, exit mapping
  config.py            TOML + environment loading and typed validation
  content.py           exact filename parser, bundle model, fingerprinting
  media.py             Pillow/ffprobe inspection and preparation
  state.py             SQLite schema, claims, delivery transitions
  locking.py           one-process lock
  archive.py           exact-member archive and recovery
  app.py               one-run orchestration
  platforms/
    base.py            adapter protocol and result/error types
    x.py               official X v2 HTTP workflow
    instagram.py       official Meta Instagram HTTP workflow
tests/
  unit pure parser/config/state/archive/media tests
  contract mocked HTTP platform tests
```

The platform boundary is explicit but fixed to the two existing targets. It is
not a dynamic plugin framework.

## 4. Core domain contracts

### 4.1 Content bundle

`ContentBundle` is an immutable dataclass:

```text
bundle_id: str
caption: str | None
alt_text: str | None
images: tuple[Path, ...]
video: Path | None
members: tuple[Path, ...]
fingerprint: str
```

The fingerprint is SHA-256 over a versioned canonical stream containing each
member's normalized role, filename, byte length, and bytes. It detects content
changes after a partial remote delivery. Paths are resolved but never followed
through symlinks.

### 4.2 Adapter contract

Each adapter exposes:

```text
name: Literal["x", "instagram"]
preflight(bundle) -> tuple[ValidationIssue, ...]
publish(bundle) -> PublishResult
```

`ValidationIssue` contains a severity (`error` or `warning`), stable code, safe
message, and optional target/member. Only errors block; warnings such as
Instagram's unsupported publish-time alt text remain visible without changing
the payload silently.

`PublishResult` contains platform, one terminal result (`published`, `failed`,
or `ambiguous`), remote ID when known, and a sanitized operator-facing error
code/message. It never contains credentials, headers, tokens, or raw request
bodies.

An explicit 4xx platform rejection is `failed`. A timeout, connection loss, or
5xx after dispatch of the final create/publish request is `ambiguous`, because
the platform may have committed the post. Upload/container failures before the
final create can be `failed` and retried later. The application never retries a
final create automatically within the same or a future run when its outcome is
ambiguous.

## 5. Configuration and secrets

Tracked `post-pulsar.toml.example` documents non-secret settings. The operator
copies it to ignored `post-pulsar.toml`, which is the default config discovered
beside the installed/project launcher unless `--config` is supplied:

```toml
[app]
posts_directory = "posts"
state_directory = ".post-pulsar"
log_file = "post_pulsar.log"
log_max_bytes = 5242880
log_backups = 3

[x]
enabled = false
user_id = ""
request_timeout_seconds = 30
processing_timeout_seconds = 300
chunk_size_bytes = 4194304

[instagram]
enabled = false
user_id = ""
media_directory = "public-media"
media_base_url = "https://media.example.com/post-pulsar/"
request_timeout_seconds = 30
processing_timeout_seconds = 300
```

Paths are resolved relative to the configuration file, not process cwd.
Unknown fields and invalid combinations fail before network access. Instagram
requires an HTTPS base URL, nonempty user ID, and a media directory when
enabled. X likewise requires its expected user ID so a valid token for the wrong
account cannot resume or publish a bundle.

Secrets are accepted only from the environment:

- `POST_PULSAR_X_USER_ACCESS_TOKEN`
- `POST_PULSAR_INSTAGRAM_ACCESS_TOKEN`

X uses a current OAuth 2.0 user-context access token with `tweet.read`,
`tweet.write`, `users.read`, and `media.write` scopes. Token acquisition,
refresh, persistence, consent UI, and revocation remain the operator's secret-
management responsibility; each scheduled run must inject a valid token. An
app-only bearer token is not accepted for publishing.

The program does not implicitly load `.env`. `.env.example` contains names and
comments, never values, for operators who explicitly source it or map it through
a secret manager. Configuration and secret values are never included in model
representations. Tokens are placed in `Authorization` headers, never logs. Meta
calls do not put access tokens in query strings.

## 6. Exact content grammar

Bundle IDs match `[A-Za-z0-9](?:[A-Za-z0-9_-]{0,62}[A-Za-z0-9])?` and are
case-insensitively unique. An ID may not end in `-alt` or `-<digits>` and may
not equal a Windows device name (`CON`, `PRN`, `AUX`, `NUL`, `COM1`–`COM9`, or
`LPT1`–`LPT9`) under case-folding. Dots, trailing spaces/dots, path separators,
Unicode lookalikes, and names longer than 64 characters are not portable and
are rejected.

| Role | Exact filename |
|---|---|
| Caption | `ID.txt` |
| Shared alt text | `ID-alt.txt` |
| Single image | `ID.jpg`, `ID.jpeg`, `ID.png`, or `ID.gif` |
| Image sequence | `ID-1.ext` through `ID-N.ext` |
| Single video | `ID.mp4` or `ID.mov` |

Rules:

- A numbered image sequence is contiguous from 1 and sorted numerically.
- Numbered and unnumbered media do not mix.
- Images and video do not mix; numbered video is invalid.
- Duplicate caption/alt/media roles, case collisions, symlinks, empty text,
  unsupported extensions, and ambiguous names are validation errors.
- An unsupported file whose stem exactly aliases a recognized bundle ID or its
  `-N`/`-alt` role invalidates that bundle so intended media cannot be silently
  omitted. Other unsupported visible files are inbox warnings, are ignored, and
  never become empty candidates.
- The parser produces one immutable `members` tuple. Every later operation uses
  that tuple; no prefix glob is allowed.
- Deterministic local validation completes for every target first. Read-only
  remote preflight then verifies target identity, credentials/permissions, and
  Instagram quota/public media reachability. Both phases pass for every target
  before the first mutating upload or container request. This avoids publishing
  to X before discovering a permanent Instagram incompatibility.

## 7. Media inspection and preparation

Extensions are hints, not trust boundaries.

- Pillow opens and verifies every image, checks decoded format/dimensions, and
  closes it before network use.
- X accepts up to four supported images or one video; each image uses the v2
  media endpoint and the shared alt text is applied to every image.
- Instagram accepts one JPEG or a JPEG carousel of up to ten items, or one
  supported MP4/MOV Reel. GIF is rejected for Instagram rather than flattened.
- PNG/JPEG inputs for Instagram are normalized into unique prepared JPEG files
  inside the configured public-media directory. Alpha is composited onto white,
  EXIF orientation is applied, metadata is stripped, and dimensions/quality are
  bounded without overwriting input.
- `ffprobe` is invoked as an argument vector with a timeout and JSON output.
  Video checks cover real container, H.264/HEVC video, AAC audio when present,
  frame rate, dimensions, pixel format, aspect ratio, duration, and byte size
  according to each platform's documented limits.
- Prepared public media names include the bundle fingerprint and cannot collide.
  Their URLs are formed by quoting one filename segment beneath the validated
  HTTPS base URL. Files remain until Meta reports the container ready and the
  publish request completes, then are removed with input-path exclusion checks.
- Instagram video is copied or transcoded into the same fingerprinted public
  staging directory and follows the same URL, lifetime, crash cleanup, and
  input-exclusion rules. Prepared files for failed pre-publication work expire
  through reconciliation; files for ambiguous publication remain until the
  operator resolves the delivery.

Platform constants are code-owned and linked to official documentation. User
configuration may lower operational limits but cannot raise official maxima.

## 8. SQLite delivery model

SQLite is stored at `<state_directory>/post_pulsar.sqlite3`. Connections enable
foreign keys, a busy timeout, and WAL where the filesystem supports it. Schema
versioning uses `PRAGMA user_version` and forward-only idempotent migrations.

```sql
bundles(
  bundle_id TEXT PRIMARY KEY COLLATE NOCASE,
  fingerprint TEXT NOT NULL,
  status TEXT NOT NULL,
  archive_path TEXT,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL
)

target_snapshots(
  bundle_id TEXT NOT NULL,
  platform TEXT NOT NULL,
  expected_account_id TEXT NOT NULL,
  api_version TEXT NOT NULL,
  adapter_version INTEGER NOT NULL,
  nonsecret_config_json TEXT NOT NULL,
  nonsecret_config_sha256 TEXT NOT NULL,
  PRIMARY KEY(bundle_id, platform),
  FOREIGN KEY(bundle_id) REFERENCES bundles(bundle_id)
)

bundle_files(
  bundle_id TEXT NOT NULL,
  relative_name TEXT NOT NULL,
  role TEXT NOT NULL,
  ordinal INTEGER,
  media_kind TEXT,
  mime_type TEXT,
  size_bytes INTEGER NOT NULL,
  sha256 TEXT NOT NULL,
  archived INTEGER NOT NULL DEFAULT 0,
  PRIMARY KEY(bundle_id, relative_name),
  FOREIGN KEY(bundle_id) REFERENCES bundles(bundle_id)
)

deliveries(
  bundle_id TEXT NOT NULL,
  platform TEXT NOT NULL,
  status TEXT NOT NULL,
  phase TEXT,
  attempts INTEGER NOT NULL DEFAULT 0,
  safe_to_retry INTEGER NOT NULL DEFAULT 1,
  next_attempt_at TEXT,
  prepared_json TEXT,
  remote_id TEXT,
  error_code TEXT,
  error_message TEXT,
  updated_at TEXT NOT NULL,
  PRIMARY KEY(bundle_id, platform),
  FOREIGN KEY(bundle_id, platform)
    REFERENCES target_snapshots(bundle_id, platform)
)

events(
  event_id INTEGER PRIMARY KEY AUTOINCREMENT,
  occurred_at TEXT NOT NULL,
  bundle_id TEXT,
  platform TEXT,
  event_type TEXT NOT NULL,
  safe_detail_json TEXT NOT NULL
)
```

`target_snapshots` freezes the platform name, expected remote account identity,
API/adapter version, public-media mapping, and every non-secret setting that can
alter a request. Resumption compares the current snapshot hash and verified
remote identity; a mismatch blocks rather than posting to a different account
or with different semantics.

Valid delivery states are `pending`, `in_flight`, `published`, `failed`, and
`ambiguous`. Valid bundle states are `active`, `archiving`, `archived`, and
`blocked`. `prepared_json` contains only non-secret upload/container IDs and
expiry/checkpoint data.

State transitions happen in immediate transactions:

```text
pending/failed -> in_flight -> published
                           -> failed      (known safe or terminal rejection)
                           -> ambiguous   (final create may have committed)
```

`failed` is retryable only when `safe_to_retry=1` and `next_attempt_at` has
passed. This covers confirmed throttling/transient failures before public
creation. Backoff is bounded exponential with jitter; after five consecutive
safe failures the delivery becomes `safe_to_retry=0` and the bundle `blocked`
for operator action. Deterministic validation, auth/permission failures, and
other permanent 4xx responses immediately set `safe_to_retry=0`/`blocked`.
Timeout, disconnect, malformed success, or 5xx after a final create begins is
always `ambiguous`, never `failed`. Selection considers only pending or
retryable-due failures and never skips a blocked/ambiguous bundle silently.

After the operator fixes a permanent condition, `post-pulsar retry BUNDLE
TARGET` is the only blocked-delivery recovery door. It accepts only a blocked
`failed` delivery, reloads current configuration/secrets, repeats all local and
read-only remote preflight, verifies the frozen target identity/config semantics
remain compatible, then transactionally clears the block, resets the consecutive
failure counter, and sets the delivery to retryable `failed` due now. It records
an allow-listed `operator_retry_enabled` event. Failure to revalidate changes no
state. A target cannot be abandoned because that would violate the immutable
delivery snapshot; changing required targets needs a new bundle ID.

On startup, preparation/processing work with a stored artifact may resume or
become a safe `failed`; stale `in_flight` final-create attempts become
`ambiguous` and are not silently retried. A stored target snapshot is
authoritative even if config later disables a platform. A changed fingerprint
for an active/partial bundle sets it `blocked`. Published targets are never
invoked again.

A bundle ID is permanently single-use. An ID already recorded as archived is
not eligible if files with that ID later reappear; the operator must choose a
new ID. This preserves the legacy duplicate-prevention intent and avoids archive
directory reuse.

## 9. Single-run orchestration

1. Parse CLI without network side effects.
2. Load and validate TOML plus secrets; configure redacted rotating logging.
3. Acquire one non-blocking `FileLock` under the state directory. Contention is
   an actionable nonzero exit and never waits through a scheduler overlap.
4. Open/migrate state and reconcile any `archiving` bundle.
5. Discover and parse the inbox once.
6. Prefer an existing incomplete bundle; otherwise choose one new valid bundle
   randomly and atomically snapshot current enabled targets.
7. Refuse `ambiguous`, changed-fingerprint, or invalid bundles with guidance.
8. Instantiate only required adapters and preflight all remaining targets.
9. For each remaining target in stable order, mark `in_flight`, publish once,
   and durably record its result immediately.
10. When and only when all snapshotted deliveries are `published`, archive exact
    members and mark the bundle `archived`.
11. Return a stable process exit code and close resources.

Primary execution is `post-pulsar run [--config PATH]`; omitting `run` keeps
simple scheduler invocation equivalent. Two safety-only commands support the
new durable state: `post-pulsar status` reports incomplete/ambiguous bundles,
and `post-pulsar reconcile BUNDLE TARGET --published REMOTE_ID` or
`--not-published` applies an explicit operator-confirmed resolution. They do not
query by caption similarity or create content.

`status` prints a fixed-column human table by default and a versioned JSON
object with `--json`; both expose only bundle ID, fingerprint prefix, target,
state, safe error code, attempt count, and remote ID. It exits 0 when no action
is needed and 5 when blocked/ambiguous work requires attention. `reconcile`
accepts only an existing ambiguous delivery, requires exactly one mutually
exclusive outcome flag, validates a published remote ID as a nonempty digit
string, records an audit event, and exits 0 only after the transition commits.
`--published` transitions directly to `published` with that ID;
`--not-published` transitions to retryable `failed`, resets the consecutive
failure counter, and schedules it immediately. It never displays secrets or
prepared public URLs. `post-pulsar retry BUNDLE TARGET` provides the separately
guarded recovery path for a blocked known failure after its cause is fixed.

Exit codes:

| Code | Meaning |
|---:|---|
| 0 | Published and archived, or healthy empty inbox |
| 2 | Configuration or deterministic content/preflight error |
| 3 | Another POST PULSAR process owns the lock |
| 4 | Known platform failure; safe to retry on a later run |
| 5 | Any blocked, ambiguous, changed, or otherwise operator-action-required delivery |
| 6 | Local state/archive/infrastructure failure |

## 10. Archive protocol and recovery

The archive path is `<posts_directory>/posted/ID/`. Before moving anything, the application
creates the destination, checks every existing destination by hash, and records
bundle status `archiving`. Each exact member is moved atomically when source and
destination share a filesystem. A matching destination is idempotently accepted;
a conflicting destination blocks the bundle. On restart, reconciliation checks
every declared member in source or destination, finishes safe moves, and only
then marks `archived`. Unknown files are untouched.

Legacy flat files already under `posts/posted/` remain archived and are not
re-imported. Documentation explains the new per-bundle archive layout.

## 11. HTTP and platform contracts

### 11.1 Shared transport behavior

- `requests.Session` with explicit connect/read timeout and TLS verification.
- Bounded retry with jitter only for idempotent GET/status requests and upload
  steps documented as safe. No library-level retry on final post creation.
- Parse structured error JSON but sanitize it through an allowlist of safe
  fields. Never log headers, URL query tokens, full bodies, or response dumps.
- Dependency injection accepts a session and sleeper/clock for offline tests.

### 11.2 X v2

Base URL: `https://api.x.com`. A user-context OAuth 2.0 Bearer token authorizes
every request; `GET /2/users/me` verifies the target identity before mutation.

- Images: send multipart raw bytes to `POST /2/media/upload` with
  `media_category=tweet_image`; require `data.id`.
- Shared alt text: `POST /2/media/metadata` with
  `{id, metadata: {alt_text: {text}}}` for every image ID.
- Video: `POST /2/media/upload/initialize`, append multipart binary chunks in
  increasing `segment_index` to `/2/media/upload/{id}/append`, then
  `POST /2/media/upload/{id}/finalize`.
- Poll `GET /2/media/upload?media_id=...` using server-provided
  `check_after_secs`, clamped to safe bounds, until `succeeded`, `failed`, or the
  configured monotonic deadline.
- Create: exactly one `POST /2/tweets` with optional `text` and
  `media.media_ids`; require `data.id` and persist it immediately.
- Weighted text validation uses `twitter-text-parser` conformance behavior and
  the standard 280 weighted-character ceiling. The old configurable 25,000
  assumption is removed.

### 11.3 Instagram official API

Base URL: `https://graph.instagram.com/v26.0`, fixed to the current documented
version at implementation time rather than floating or operator-selected.
Authentication is a Bearer header. Read-only preflight verifies the configured
user ID/account type and available publishing quota.

- Single image: prepare public JPEG, then `POST /{user_id}/media` with
  `image_url` and caption.
- Carousel: create one `is_carousel_item=true` child container for each public
  JPEG, wait as required, then create a `media_type=CAROUSEL` parent with ordered
  child IDs and caption.
- Video: prepare/publicly expose the validated input and create a
  `media_type=REELS` container with `video_url` and caption.
- Poll `GET /{container_id}?fields=status_code,status` to `FINISHED`; map
  `ERROR`/`EXPIRED` and deadlines to safe failures before publish.
- Finalize exactly once via `POST /{user_id}/media_publish` with `creation_id`;
  require/persist the returned media ID.

The official Instagram publishing contract does not currently expose a publish-
time alt-text field. POST PULSAR preserves the shared alt file, applies it to X,
and emits one stable `instagram_alt_text_unsupported` preflight warning when
Instagram is targeted. It does not claim the text was sent or silently omit the
fact from status/log output.

The implementation assumes Business Login for Instagram, a Professional
account, and `instagram_business_basic` plus
`instagram_business_content_publish`. It does not implement browser OAuth,
token refresh service, or media hosting. Live parity is conditional on the
operator supplying those prerequisites.

## 12. Logging and observability

Logging is configured once in the CLI with console and rotating-file handlers.
Child loggers use `post_pulsar.<component>`. Each outcome includes a generated
run ID, bundle ID, platform, stable event name, and safe result code. Formatter
filters redact known secret values and token-like keys. Normal logs do not print
full captions, alt text, local directory listings, or response bodies. Repeated
application construction does not duplicate handlers.

## 13. Packaging, dependency, and automation policy

- Python requirement: `>=3.12`; CI covers 3.12, 3.13, and 3.14.
- PEP 621 metadata and console script live in `pyproject.toml`.
- Runtime dependencies are current direct packages only: Requests, Pillow,
  filelock, and twitter-text-parser. Standard-library `tomllib`, dataclasses,
  and SQLite cover configuration, models, and state. Exact transitive versions
  are generated into a dated lock.
- Development dependencies include current pytest/coverage, Ruff, mypy/type
  stubs, build, pip-tools, and pip-audit.
- Ruff formatting/lint, strict-enough mypy, pytest with branch coverage, wheel
  build, and `pip-audit` are required verification gates.
- `.gitattributes` makes source/docs LF and `.bat` CRLF. Generated caches,
  databases, locks, logs, secrets, public preparation files, and environments
  are ignored.
- CI performs no network calls beyond dependency installation and never receives
  platform credentials.

Unix and Windows setup scripts create/use `.venv` in the project and install the
locked set. They do not upgrade the OS, assume root, mutate cron automatically,
pause, or depend on activation. Run scripts resolve their own directory and
`exec`/return the package's exit status. Task setup is explicit and idempotently
uses the renamed `PostPulsar` task.

## 14. Migration contract

1. Confirm the already renamed directory and requested Git remote.
2. Normalize line endings under an explicit `.gitattributes` policy.
3. Replace old source names rather than retaining PHONG-BOT compatibility shims.
4. Provide `post-pulsar.toml.example` and `.env.example`; never read or print old
   credentials during automated migration.
5. Lock down ignored legacy `config.json` and `instagram_session.json` to owner
   access where possible, but do not copy their secrets. The user rotates X
   credentials, revokes the Instagram private session, provisions official
   tokens, and removes old secret files after verifying migration.
6. Preserve existing `posts/` and legacy `posts/posted/` content.
7. No live authentication or post is attempted by tests or migration commands.

## 15. Acceptance and falsification

The design succeeds when identity search is clean, the requested remote is
active, fresh locked installation/build/audit pass, tests prove exact grouping
and every state/recovery branch, mocked contracts prove current API request
shapes and polling, scripts preserve exit status, and docs match behavior.

It is falsified if it claims live Instagram readiness without a Professional
account/public HTTPS media mapping, claims exactly-once delivery after a lost
remote response, silently converts unsupported media, retains private password
automation, archives after partial success, retries an ambiguous create, or
adds report-only features to implementation.
