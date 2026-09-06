# Phase 4: Orchestration, CLI, and Supply Chain

## Codebase Anchors

Connect the safe components into the existing one-shot workflow and freeze its verified dependency graph.

### Task 4.1: Build safe one-run orchestration

Build the new application service alongside the runnable legacy entry point. It
loads local config, acquires the lock, migrates/recovers stale phases, and
resumes the oldest incomplete or archiving bundle first. Its immutable target
snapshot governs resumed work even when current config disables every target;
compatible settings/secrets are required only if remote work remains, while an
all-published bundle can finish archival with no token. Only when no stored work
exists does it reject zero currently enabled targets before claiming state and
otherwise select one new valid bundle randomly through an injected chooser/
clock. Create
immutable target snapshots, verify content drift, inspect and reversibly stage
local media, then run every exact public-URL and read-only remote preflight
before remote mutation. For required unpublished targets, call resumable
`prepare`, checkpoint every artifact, persist `final_dispatch_started`, call
one-shot `commit`, and durably record its result. A crash after the remote final
create side effect but before the result transaction therefore restarts as
`ambiguous` and cannot call the adapter again. Persist deduplicated warning
events. Block ambiguous or exhausted/permanent failures and archive only after
all snapshot targets are known published. Failed terminal deliveries require
explicit retry and a published target is never republished. Own the lock,
SQLite connection, HTTP sessions, staging handles, and adapters with
context managers/`ExitStack`; injected exceptions after each boundary must
release resources in reverse order while retaining staging only for ambiguous
evidence. Tests inject deterministic chooser/clock/sleeper.

**Test:** yes

**Dependencies:**
- T2.2
- T2.3
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
Finalize `__main__.py` so both entry paths call `cli.main()`. Every mutating
command and schema migration acquires the same state-directory lock; `status`
is strictly read-only/WAL-safe. `status` and operator-confirmed `reconcile`
load only local settings and need no token/network even when the current config
disabled a snapshotted platform; `run` and `retry` validate secrets plus remote
identity for only the required snapshot targets. Add human/versioned JSON
status including deduplicated warning codes, explicit bundle/platform targeting,
rotating secret-safe logs, stable exit codes, and concise errors. `retry`
revalidates under lock before changing state. `reconcile` may record an
operator-supplied remote ID or confirm non-publication but never makes an
unverified final call.

**Test:** yes

**Dependencies:**
- T4.1

**Files:**
- `src/post_pulsar/cli.py`
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
- T4.2

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
