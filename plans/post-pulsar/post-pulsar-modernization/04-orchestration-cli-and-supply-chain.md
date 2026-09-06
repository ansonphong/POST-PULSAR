# Phase 4: Orchestration, CLI, and Supply Chain

## Codebase Anchors

Connect the safe components into profile-aware one-shot and daemon workflows,
then freeze the verified dependency graph.

### Task 4.1: Build safe one-run orchestration

Build the new application service alongside the runnable legacy entry point. It
loads local config, acquires the profile lock, validates the already-migrated
schema/layout, recovers stale phases, and
resumes the oldest incomplete or archiving bundle first. Its immutable target
snapshot governs resumed work even when current config disables every target;
compatible settings/secrets are required only if remote work remains, while an
all-published bundle can finish archival with no token. Only when no stored work
exists does it reject zero currently enabled targets before claiming state and
otherwise select one new valid bundle using the bucket's defined deterministic
ordering/transactional selector through injected clock and selection state. Create
immutable target snapshots, verify content drift, inspect and reversibly stage
local media, then run every exact public-URL and read-only remote preflight
before remote mutation. For required unpublished targets, call resumable
`prepare`, checkpoint every artifact, persist `final_dispatch_started`, call
one-shot `commit`, and durably record its result. A crash after the remote final
create side effect but before the result transaction therefore restarts as
`ambiguous` and cannot call the adapter again. Persist deduplicated warning
events. Block ambiguous or exhausted/permanent failures and archive only after
all snapshot targets are known published. Only sanitized transient-safe
failures auto-resume after `next_attempt_at`; permanent or five-times-exhausted
failures require guarded retry, ambiguous outcomes require reconcile, and a
published target is never republished. Borrow the caller-owned instance lease
without registering or releasing it; own only the per-job profile lease, SQLite
connection, HTTP sessions, staging handles, and adapters with
context managers/`ExitStack`; injected exceptions after each boundary must
release resources in reverse order while retaining staging only for ambiguous
evidence. Tests inject clock/selection-state/sleeper and assert the fixed
SHA-256 RANDOM score; no pluggable nondeterministic chooser exists.

Expose `run_once(profile_id, bucket, trigger_id)` (or an equivalent immutable
request). Recover that profile's due transient-safe work first, then select from
the requested bucket: `QUEUE`/`REELS` use the lowest case-folded ID and `RANDOM`
transactionally scores using the persisted profile selection counter. Selection,
counter increment, bundle insertion, target snapshot, and trigger linkage commit
together. Require the exact ready marker, freeze profile,
bucket, target ownership and token-reference names in the snapshot, and create
fresh target clients. A blocked or ambiguous profile must not prevent another
profile's run.

**Test:** yes

**Dependencies:**
- T1.4
- T2.2
- T2.3
- T2.4
- T3.2
- T3.3

**Files:**
- `src/post_pulsar/app.py`
- `tests/unit/test_app.py`

**Acceptance:**
- No mutating request occurs until every enabled target passes deterministic and read-only remote preflight.
- Partial success resumes only unpublished safe targets, while ambiguous state blocks unattended retry and archive.
- All-target success archives exact members once; zero candidates and invalid candidates are deterministic nonmutating outcomes.
- Zero enabled targets never admits/claims a new bundle, but cannot prevent stored recovery or all-published archival; post-create/pre-result crashes become ambiguous, never a duplicate create.
- Disabling targets after partial delivery cannot hide the immutable snapshot, and disabling after all targets published cannot prevent archive recovery.
- Injected failures prove the process lock can be reacquired, SQLite reopened, sessions closed, and staged-file retention follows persisted delivery state.

**Verify-After:**
- `.venv/bin/python -m pytest tests/unit/test_app.py -q` (focused)

### Task 4.2: Expose safe run, status, retry, and reconcile commands

Implement the `post-pulsar` CLI with `run` as the default one-shot operation
plus read-only `status` and guarded `retry`/`reconcile` recovery commands.
Finalize `__init__.py` and `__main__.py` so both entry paths call `cli.main()`
and return identical exit codes. Every local mutating command enters through the
instance gate and applicable ordered profile/maintenance lock; `status`
is strictly read-only/WAL-safe. `status` and operator-confirmed `reconcile`
load only local settings and need no token/network even when the current config
disabled a snapshotted platform; `run` and `retry` validate secrets plus remote
identity for only the required snapshot targets. Add human/versioned JSON
status including deduplicated warning codes, explicit bundle/platform targeting,
rotating secret-safe logs, stable exit codes, and concise errors. `retry`
revalidates under lock before changing state. `reconcile` may record an
operator-supplied remote ID or confirm non-publication but never makes an
unverified final call.
All bundle and recovery commands require explicit profile identity and
understand the standard bucket names. Machine output uses the shared
`post-pulsar.control/v1` envelope consumed later by the daemon and MCP bridge.

**Test:** yes

**Dependencies:**
- T4.1
- T2.4

**Files:**
- `src/post_pulsar/cli.py`
- `src/post_pulsar/__init__.py`
- `src/post_pulsar/__main__.py`
- `tests/unit/test_cli.py`

**Acceptance:**
- run, status, retry, and reconcile parse deterministically and map outcomes to documented exit codes.
- Recovery commands require exact bundle/platform selection and cannot silently reset ambiguous or published deliveries.
- JSON output is machine-readable and logs/exceptions redact configured token values.
- Lock-contention tests prove `retry` and `reconcile` leave state unchanged while `run` owns the lock; status/reconcile work without credentials or network and render persisted warnings.

**Verify-After:**
- `.venv/bin/python -m pytest tests/unit/test_cli.py -q` (focused)
- `.venv/bin/python -m post_pulsar --help` (scoped_check)

### Task 4.3: Lock dependencies and enforce CI security gates

Resolve declared direct dependencies through the pinned `.venv/bin/uv` to an
exact lock with Python 3.12 support, ensuring legacy Tweepy/instagrapi baggage is
absent. Add CI that installs from the frozen lock and runs formatting/lint,
strict typing, socket-denied unit/contract/integration tests with coverage,
package build, lock consistency, and `pip-audit`. CI permits network only for
dependency installation and vulnerability metadata; the test process itself
has no live platform or general outbound network access and receives no
credentials. Add a focused CI contract test that parses the workflow and
validates executable steps, the 3.12/3.13/3.14 matrix, frozen install, lock
check, coverage, build, audit, and absence of platform secrets. The committed
lock is the durable artifact; project metadata, Git history, and the final
report record its 2026-09-05 refresh provenance.

**Test:** yes

**Dependencies:**
- T1.1
- T3.2
- T3.3

**Files:**
- `pyproject.toml`
- `uv.lock`
- `.github/workflows/ci.yml`
- `tests/unit/test_ci.py`

**Acceptance:**
- uv.lock is generated from pyproject.toml, exact, current as of 2026-09-05, and valid for the supported Python runtime.
- CI uses the frozen lock and executable quality, typing, socket-denied tests/coverage, build, and vulnerability audit gates without secrets.
- The resolved dependency graph contains neither tweepy nor instagrapi and a lock-scoped vulnerability audit reports no known unacknowledged vulnerability.

**Verify-After:**
- `.venv/bin/uv lock --check` (focused)
- `python3 -c "import tomllib; d=tomllib.load(open('uv.lock','rb')); names={p['name'].lower().replace('_','-') for p in d['package']}; assert not {'tweepy','instagrapi'} & names"` (scoped_check)
- `.venv/bin/uv run --frozen pip-audit` (scoped_check)
- `.venv/bin/python -m pytest tests/unit/test_ci.py -q` (focused)

### Task 4.4: Implement deterministic profile scheduling

Implement a pure scheduler for stable profile schedule IDs, allow-listed source
buckets, IANA timezones, weekdays, local `HH:MM`, and bounded same-day misfire
grace. Persist each unique `(profile,schedule,local_date)` occurrence with its
original offset, UTC instant, and schedule hash. Repeated DST times fire once;
nonexistent times run at the first valid tick within grace; late ticks beyond
grace become `missed`; previous dates are never replayed. Recalculate wall time
after each wake and use monotonic time only for sleeping/timeouts. Order due
work by UTC instant, profile ID, then schedule ID. Retries for existing safe
work take precedence over new admission.

**Test:** yes

**Dependencies:**
- T2.2
- T4.1

**Files:**
- `src/post_pulsar/scheduler.py`
- `src/post_pulsar/state.py`
- `src/post_pulsar/app.py`
- `tests/unit/test_scheduler.py`
- `tests/unit/test_state.py`
- `tests/unit/test_app.py`

**Acceptance:**
- Duplicate ticks, restart, DST fold/gap, rollback, simultaneous slots, and
  bounded misfires have deterministic non-duplicating outcomes under fake time.
- Schedule state follows
  `queued|due|dispatching|completed|failed|ambiguous|missed|no_content`;
  `no_content` is terminal for that local-date occurrence and the next scheduled
  local date remains normally eligible; ambiguous work is never automatically
  dispatched again.
- Legal transitions are `queued → due|missed`, `due → dispatching|no_content`,
  and `dispatching → completed|failed|ambiguous`. The earlier UTC fold is
  canonical; DST-gap grace is measured from nominal wall time to the first
  valid instant. Schedule/content claim is atomic, simultaneous same-profile
  occurrences are ordered, and only the winner can consume a bundle.
- A linked transient-safe failure remains `failed` while its bundle is due for
  automatic retry; permanent/exhausted failure requires guarded retry,
  ambiguity requires reconcile, `no_content` is distinct from `missed`, and
  completion requires all-target publication plus archive.
- A linked retry keeps the occurrence `failed` on another safe failure, changes
  it to `ambiguous` on uncertainty, and reaches `completed` only after publish
  plus archive. Reconcile-as-published continues to archive/completed;
  reconcile-as-not-published leaves it failed until an explicit safe retry.
- Cron/RRULE and unbounded catch-up are rejected.

**Verify-After:**
- `.venv/bin/python -m pytest tests/unit/test_state.py -q` (focused)
- `.venv/bin/python -m pytest tests/unit/test_app.py -q` (focused)
- `.venv/bin/python -m pytest tests/unit/test_scheduler.py -q` (focused)

### Task 4.5: Add the foreground daemon and authenticated control API

Run one foreground daemon with a lifetime `instance.lock`, a single sequential
publishing worker, and one standard-library loopback HTTP thread using its own
SQLite connection. Startup recovers stored work before schedule admission;
shutdown stops new admission and waits through the current safe publication
boundary. Publish a checked-in authoritative OpenAPI 3.1/JSON Schema contract
for every `post-pulsar.control/v1` operation: health, capabilities, status,
dashboard, profiles, buckets, bundle/media inspect and preview, caption/alt edit
for unready bundles, ready/enqueue, schedules, requests, pause/resume, run-now,
cancel pending, retry, reconcile, confirmation create/status/consume, and
operator approval. It defines exact methods/routes, schemas/bounds, version and
principal/authentication headers, common error/status envelope, pagination, revisions,
idempotency, and ambiguity results; core handlers validate it. Bind only the configured literal loopback
address, require the 256-bit agent bearer capability with constant-time comparison,
disable CORS, limit bodies/results, redact headers/errors, and accept no raw
paths, SQL, shell, platform tokens, or platform response bodies. Writes require
idempotency keys and expected revisions and enqueue durable requests rather
than publishing in the HTTP thread. Write an atomic owner-only endpoint record
containing only protocol, address, process identity, startup nonce, and
capabilities, and remove it only when still owned by the shutting-down daemon.
Atomic bootstrap/endpoint replacement preserves the configured hardened-mode
read-only agent ACL while denying agent writes; stale or widened ACLs fail
closed.
Generate the agent control capability through an explicit setup/rotation
command, store it in a symlink-rejected owner-only file (or Windows user ACL
equivalent), and publish only its configured location in the endpoint record.
Initialize a separate operator approval secret from a real TTY/console with a
memory-hard verifier stored by core; refuse piped/environment/config input,
never echo or log it, and enforce bounded failed-attempt lockout. The server
verifies that secret only on operator approval routes. Social tokens remain
environment-only and never enter these files.
Persist expiring single-use confirmation intents for publish, retry, reconcile,
and destructive pending-work mutation. Bind each intent to the exact action,
arguments, profile/resource, revision, fingerprint, consequence text, expiry,
and nonce. The agent may create/status an intent but only the trusted interactive
core CLI using the operator approval secret can approve it; consume it transactionally
with its idempotent request. Agent-origin publishing intents cannot be created
or consumed unless installation config already sets
`allow_agent_publish = true`; separately authenticated operator-origin intents remain available under
normal operator policy.

The OpenAPI operation matrix requires trusted approval plus an approved intent
for draft admission/ready, enqueue, run-now/publish-now, resume when due work
exists, enable or modify an enabled/due schedule, cancel/delete pending work,
retry, and reconcile. Pause, schedule disable, read/preview, and editing an
unready DRAFTS caption/alt do not publish and need no intent. All publication-
enabling agent operations also require `allow_agent_publish`; paused-due and
already-due cases are tested explicitly.

Draft admission opens exact members without following symlinks, copies and
hashes the same streams into a daemon-owned same-filesystem hidden directory,
re-stats sources, persists every member/phase checkpoint, verifies the preview
semantic fingerprint bound to the intent, writes/fsyncs `.ready` last, fsyncs
parents, and atomically renames the complete directory. Crash recovery resumes
or safely rolls back every boundary; conflicts never overwrite. Original DRAFTS
remain unchanged and an admitted-fingerprint record prevents duplicate
promotion. The operational sentinel is excluded from the semantic fingerprint.

**Test:** yes

**Dependencies:**
- T4.1
- T4.4

**Files:**
- `src/post_pulsar/daemon.py`
- `src/post_pulsar/control.py`
- `src/post_pulsar/admission.py`
- `src/post_pulsar/config.py`
- `src/post_pulsar/state.py`
- `post-pulsar.toml.example`
- `api/control-v1.openapi.json`
- `tests/unit/test_daemon.py`
- `tests/unit/test_admission.py`
- `tests/unit/test_config.py`
- `tests/unit/test_state.py`
- `tests/contract/test_control.py`

**Acceptance:**
- A second daemon fails cleanly; work remains sequential and profile-isolated;
  restart/shutdown cannot interrupt or duplicate final dispatch.
- Authentication, loopback binding, limits, revisions, idempotency, pagination,
  redaction, pause state, confirmation replay/expiry/drift, and durable requests
  are socket-denied contract tests.
- Endpoint/credential tests cover 0600-or-user-ACL enforcement, symlink defense,
  agent-capability generation/rotation/revocation, TTY-only operator-secret
  initialization/verification/lockout, stale PID/start-time/nonce records, and
  safe cleanup without exposing credentials in status, errors, or logs.
- Agent principal calls cannot approve intents or use operator-only routes;
  unready-bundle edits address only a core-discovered profile/bucket/bundle ID
  and never transfer media bytes or accept a raw filesystem path.
- Admission fault tests cover concurrent replacement, every member checkpoint,
  before/after sentinel installation and atomic rename, restart recovery,
  source retention, duplicate intent consumption, and destination conflict.
- Capabilities report core semver, control API major, state schema, and supported
  operations; incompatible clients remain read-only.

**Verify-After:**
- `.venv/bin/python -m pytest tests/unit/test_config.py tests/unit/test_state.py tests/unit/test_admission.py tests/unit/test_daemon.py tests/contract/test_control.py -q` (focused)

### Task 4.6: Extend the CLI for profiles, scheduling, daemon control, and migration

Extend the CLI with explicit profile/bucket selection, schedule listing and
mutation, durable run-now, pause/resume/shutdown, daemon foreground mode, and
guarded multi-account migration dry-run/apply. Add explicit agent control-
capability initialize/rotate/revoke commands and TTY-only operator approval-
secret initialize/rotate commands; output never prints either secret. Control
initialization also writes the owner-only non-secret bootstrap record consumed
through `POST_PULSAR_MCP_BOOTSTRAP`, with installation ID, endpoint-record path,
agent-capability path, and an enum-validated installed service mode/identifier.
Local status falls back to
read-only SQLite if the daemon is absent. Mutations use the authenticated
control API when available or the same profile/maintenance locks locally; they
never alter in-flight work. Add JSON protocol envelopes and stable exit codes.
Publish, retry, reconcile, and pending-work deletion require exact IDs and the
same preview/confirmation-intent rules as control clients; interactive CLI
approval uses the operator principal outside MCP, then the exact approved intent
may be consumed once. Operator approval remains available when agent publication
eligibility is disabled, but the agent still cannot create publishing intents.

**Test:** yes

**Dependencies:**
- T2.4
- T4.2
- T4.5

**Files:**
- `src/post_pulsar/cli.py`
- `src/post_pulsar/__main__.py`
- `src/post_pulsar/bootstrap.py`
- `api/bootstrap.schema.json`
- `tests/unit/test_cli_control.py`

**Acceptance:**
- Commands are profile-explicit, bounded, deterministic, secret-safe, and map
  daemon-unavailable/version/auth/conflict outcomes to documented codes.
- Migration cannot run beside the daemon; recovery cannot mutate a different or
  in-flight profile; status remains credential-free.
- Agent-capability initialization/rotation/revocation is permission-checked,
  redacted, invalidates stale sessions, and cannot be called through the agent
  API; operator-secret lifecycle refuses noninteractive input and stores only
  its memory-hard verifier.
- Bootstrap creation is atomic, owner-only, schema-validated, contains no
  credential values, and rejects arbitrary service executables/arguments.

**Verify-After:**
- `.venv/bin/python -m pytest tests/unit/test_cli.py tests/unit/test_cli_control.py -q` (focused)
- `.venv/bin/python -m post_pulsar --help` (scoped_check)
