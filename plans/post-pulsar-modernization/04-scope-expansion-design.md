# Stage 5 scope expansion — multi-account scheduling and MCP control

Date: 2026-09-05

Status: approved superseding design

The operator expanded the product during execution. This document supersedes
the earlier exclusions for multiple accounts, built-in scheduling, a daemon,
and agent control. The already-completed package, secure local-configuration,
and exact-bundle foundations remain valid after their Phase 1 review fixes.

## Product boundary

The ecosystem has two repositories:

- **POST-PULSAR** owns all publishing authority, credentials, media validation,
  durable state, scheduling, recovery, and the local control protocol.
- **POST-PULSAR-PLUGINS** is a thin, secretless MCP/client package for Codex,
  Claude Code, and other MCP hosts. It never imports core internals, reads the
  core database, accepts shell fragments, or talks to social APIs.

There is still no GUI, cloud service, distributed worker, browser OAuth flow,
analytics product, or additional social platform in this delivery.

## GRUG architecture

One installation runs one foreground daemon, one SQLite database, one
publishing worker, and a small authenticated loopback HTTP control API. The OS
service manager owns background startup and restart. In simple same-user mode,
when the daemon is absent, the MCP broker may request only a preconfigured
allow-listed user-service start (systemd user unit, launchd label, or Windows
scheduled task) with no shell. Hardened separate-identity mode disables broker
auto-start and requires an operator-enabled system service/LaunchDaemon/Windows
service to be running; absence returns exact operator guidance. The service
manager—not the agent process—provides the daemon's publishing environment. The
broker never starts Python directly, installs a service, inherits social
credentials, or kills a process.

Stable `profile_id` values match `[a-z0-9][a-z0-9-]{0,31}`. Each profile owns
one content root, timezone, schedules, and at most one target per platform. A
remote `(platform, expected_remote_user_id)` belongs to only one profile.
Every admitted bundle snapshots its profile, target identity, non-secret
request configuration, source bucket, and fingerprint.

The tracked example defines X profiles `ansonphong` and `360hextile` using only
environment-variable *names*. Tokens and remote numeric IDs remain external.
Authenticated HTTP clients are created per profile/target and never shared
across accounts.

## Content roots and buckets

```text
accounts/<profile>/
├── DRAFTS/{QUEUE,RANDOM,REELS}/<bundle-id>/
├── QUEUE/<bundle-id>/
├── RANDOM/<bundle-id>/
├── REELS/<bundle-id>/
└── POSTED/<bucket>/<bundle-id>/
```

- `QUEUE` selects the smallest case-folded bundle ID.
- `RANDOM` uses a persisted selection counter and a SHA-256 score over profile,
  counter, bundle ID, and fingerprint; the selection and counter increment are
  one transaction.
- `REELS` is an editorial queue that accepts exactly one video and is selected
  deterministically. Platform media validation remains authoritative.

Bucket scanning enumerates only immediate `<bundle-id>` directories, and each
bundle directory is scanned non-recursively with the exact filename grammar.
Agent/host file tools may place media only under mirrored unpublished DRAFTS. A
core preview may inspect drafts, but the daemon never selects them. After trusted
CLI approval, core copies the prospective exact content into a daemon-owned
same-filesystem hidden admission directory, hashing the same opened streams and
checking metadata/race invariants. It verifies the preview fingerprint bound to
the intent, writes/fsyncs a canonical `.ready` sentinel last, fsyncs the parent,
and atomically renames the complete bundle directory into its destination.
Fault recovery journals every phase; a conflict never overwrites, and the
original draft is retained with a durable admitted-fingerprint record to prevent
duplicate admission. The sentinel is an operational member but is excluded from
the versioned semantic content fingerprint. This prevents half-copied bundles
without changing the approved fingerprint. Roots may not overlap,
escape, contain symlinks, collide by case, or move while work is active.

## Scheduling

Schedules intentionally use a small schema: stable ID, bucket, IANA timezone,
weekdays, local `HH:MM`, enabled flag, and bounded same-day misfire grace. No
cron or arbitrary RRULE is accepted.

Each `(profile_id, schedule_id, local_date)` fires at most once. The daemon
normalizes the occurrence to UTC, persists the original local date/offset and
schedule hash, and orders simultaneous work by UTC time, profile, then schedule
ID. A nonexistent DST wall time runs on the first tick after the gap within the
grace; a repeated time runs once. A late tick beyond grace becomes `missed` and
requires explicit rescheduling. Clock time is recalculated after wake; monotonic
time is used only for sleeps/timeouts.

The legal state path is `queued → due|missed`,
`due → dispatching|no_content`, and
`dispatching → completed|failed|ambiguous`.
Existing retryable delivery work takes precedence over admitting new content.
Blocked or ambiguous work stops only its profile.

## Daemon and migration

The daemon holds `instance.lock` for life. Schema/layout migration first owns
that same instance gate and then `maintenance.lock`. Publication uses sorted
profile locks; all threads have separate SQLite connections. Shutdown stops new
admission and waits through the current safe publication boundary.

SQLite uses surrogate bundle keys plus profile-scoped uniqueness and adds
profiles, profile targets, schedules, schedule runs, durable run requests, and
pause state. Migration requires an explicit profile, dry-run/apply modes, a
backup of current-schema state, a durable journal, count/hash/foreign-key
validation, hash-verified filesystem moves, atomic phase commits, and idempotent
crash recovery. It never duplicates imported content into two profiles.

The only supported migration input is the legacy PHONG-BOT flat inbox with no
delivery database; no pre-profile POST PULSAR schema was released. Any existing
unknown/non-current SQLite `user_version` fails closed. Migration creates fresh
state and moves only exact valid bundles. Active legacy content is
placed in the chosen profile's `RANDOM` bucket and receives a ready marker only
after its exact members are hash-verified at the destination. Unknown schemas,
partial bundles, and unrelated files are reported and left untouched.

The global lock order is instance → maintenance when applicable → sorted
profile IDs → SQLite write transaction. Daemon mode holds the instance gate for
life; migration takes it before checking state and then takes maintenance, so a
daemon cannot race the check. The lifetime gate is not reacquired by application
services; its single worker acquires one profile lock before publication/archive
work. One-shot mode owns the instance gate briefly and then the selected profile
lock. Control threads enqueue durable requests in short transactions and never
acquire profile locks.

## Control protocol

The versioned JSON protocol is `post-pulsar.control/v1`. The core exposes
capabilities/version/status plus bounded endpoints for status, dashboard,
profiles, buckets, bundles, requests, schedules, pause/resume, enqueue, and
run-now. It binds only to the literal configured loopback address, requires a
scoped 256-bit agent capability with constant-time comparison, and verifies the
separate interactive operator approval secret only on approval routes. It has
no CORS and bounds every
body/list/string, and returns only redacted structured errors.

Mutations require an idempotency key and expected revision. Draft admission/
ready, enqueue, run-now/publish-now, resume with due work, enabling or modifying
an enabled/due publishing schedule, cancel/delete, retry, and reconcile use a
one-time confirmation intent bound to the exact action,
arguments, fingerprint, revision, expiry, and human consequence. The agent may
create an intent but cannot approve it. Approval occurs only through the trusted
interactive core CLI, outside MCP, and atomically changes the core-owned intent
to approved; MCP can then consume only that exact approved intent once.
Publishing-capable MCP tools are absent unless core config sets
`allow_agent_publish = true`; this is installation-level eligibility, not
approval and not a model prompt. No tool deletes source media or remote posts.

Social tokens remain environment-only. The MCP uses a scoped agent capability
generated and rotated explicitly and stored in a symlink-rejected owner-only
file or Windows user ACL. Approval requires a separate operator secret whose
memory-hard verifier is stored by core; the secret is initialized and entered
only through a real TTY/console prompt, is never printed, piped, placed in an
environment variable, config, endpoint record, or agent prompt, and has bounded
failed-attempt lockout. The operator endpoint verifies it before approving an
exact intent; the agent capability cannot call that endpoint.

Hardened deployment runs the daemon/state/publishable buckets as a dedicated OS
service identity and grants the agent identity access only to DRAFTS plus its
agent capability. Single-user mode remains supported for GRUG-simple local use,
but documentation explicitly says it is not a security boundary against an
agent also granted arbitrary same-user shell/filesystem access. All claims that
prompt/tool calls cannot publish refer to the MCP capability surface and the
hardened deployment boundary, not to a hostile same-user process.

Core owns a checked-in OpenAPI 3.1/JSON Schema contract for the complete
`post-pulsar.control/v1` protocol: routes/methods, authentication and version
headers, common envelope/errors, exact inputs/outputs and bounds, pagination,
revisions, idempotency, confirmation state, and compatibility behavior. Core
handlers validate it. The plugin vendors an exact hash-addressed copy and both
repositories reject contract drift in tests.

## Plugin repository

`POST-PULSAR-PLUGINS/plugins/post-pulsar` is one self-contained installable
plugin containing its Python 3.12 stdio MCP server, frozen lock, provider-neutral
skills, required Codex `.codex-plugin/plugin.json`, Claude
`.claude-plugin/plugin.json`, host-specific MCP launch configs, strict Pydantic
schemas, compatibility metadata, tests, CI, and documentation. The repository
root owns only marketplace/catalog and repository-level documentation files.

The broker discovers the permission/ACL-validated endpoint record and validates
core protocol and version capabilities. In simple mode it safely contends on an
auto-start lock, requests only the configured allow-listed user service, and
waits for a bounded handshake. In hardened mode it never auto-starts and returns
operator guidance until the separate-identity service is online. Incompatible
versions permit degraded read-only health and disable all mutation.

Read tools/resources cover status, dashboard, profiles, buckets, queue,
schedules, core-discovered bundle/media inspection, and publish previews.
Mutating tools may edit bounded caption/alt text for an unready bundle and ask
core to ready/enqueue existing bundle IDs, manage schedules, pause/resume,
run-now, cancel/delete, retry, and reconcile under the CLI-approval and opt-in
rules. Media is placed in configured profile folders by the operator/host file
tools; MCP accepts no raw path or media-byte upload.

Schedule occurrence claiming and content claiming are one transaction. For a
DST fold, the earlier UTC instant is canonical; for a gap, the first valid tick
within grace is canonical. Simultaneous schedules for one profile are ordered
and later occurrences record `no_content` when the first consumes the only
eligible bundle. `no_content` is distinct from `missed`. An occurrence becomes
`completed` only after all selected targets publish and archive; retryable
delivery leaves it `failed` with the durable bundle linked, ambiguous delivery
sets it `ambiguous`, and no occurrence may admit a second bundle while linked
work is recoverable.

## Verification floor

Tests deny social-network access. Required coverage includes two-profile
credential/header isolation, remote-account uniqueness, root traversal and
symlink swaps, deterministic bucket selection, ready-marker admission, schedule
DST/misfire/rollback/duplicate ticks, daemon singleton/restart/shutdown, schema
migration faults, ambiguous final dispatch, MCP schema/annotation/confirmation
safety, stdio protocol purity, auto-start contention, version mismatch, and a
hermetic real-core/fake-platform cross-repository journey.

The official OpenAI plugin architecture supports combining skills and MCP
servers without a UI. The local stdio plugin is for Codex and Claude Code; a
public hosted plugin would require a later remote transport and is out of scope.
