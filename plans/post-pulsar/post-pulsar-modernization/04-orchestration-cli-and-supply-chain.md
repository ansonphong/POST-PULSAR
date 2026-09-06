# Phase 4: Orchestration, CLI, and Supply Chain

## Codebase Anchors

Connect the safe components into the existing one-shot workflow and freeze its verified dependency graph.

### Task 4.1: Build safe one-run orchestration

Replace PhongBot with an application service that acquires the lock, resumes the oldest incomplete bundle first and otherwise selects one new valid bundle randomly through an injected chooser, creates immutable target snapshots, verifies content drift, runs all local then all read-only remote preflight before mutation, publishes required pending/failed targets sequentially, persists every transition, blocks ambiguous outcomes, and archives only after all snapshot targets are known published. Failed terminal deliveries require explicit retry and a previously published target is never republished; tests inject a deterministic chooser.

**Test:** yes

**Dependencies:**
- T2.2
- T2.3
- T3.2
- T3.3

**Files:**
- `src/post_pulsar/app.py`
- `tests/unit/test_app.py`
- `phong-bot.py`

**Acceptance:**
- No mutating request occurs until every enabled target passes deterministic and read-only remote preflight.
- Partial success resumes only unpublished safe targets, while ambiguous state blocks unattended retry and archive.
- All-target success archives exact members once; zero candidates and invalid candidates are deterministic nonmutating outcomes.

**Verify-After:**
- `.venv/bin/python -m pytest tests/unit/test_app.py -q` (focused)

### Task 4.2: Expose safe run, status, retry, and reconcile commands

Implement the post-pulsar CLI with run as the default one-shot operation plus read-only status and guarded retry/reconcile recovery commands required by the state design. Add human and JSON status output, explicit bundle/platform targeting for recovery, confirmation bypass only through an intentional flag, rotating secret-safe logs, stable exit codes, and concise errors. Reconcile may record operator-supplied remote IDs or confirm failure but must never make an unverified final publish call.

**Test:** yes

**Dependencies:**
- T4.1

**Files:**
- `src/post_pulsar/cli.py`
- `tests/unit/test_cli.py`

**Acceptance:**
- run, status, retry, and reconcile parse deterministically and map outcomes to documented exit codes.
- Recovery commands require exact bundle/platform selection and cannot silently reset ambiguous or published deliveries.
- JSON output is machine-readable and logs/exceptions redact configured token values.

**Verify-After:**
- `.venv/bin/python -m pytest tests/unit/test_cli.py -q` (focused)
- `.venv/bin/python -m post_pulsar --help` (scoped_check)

### Task 4.3: Lock dependencies and enforce CI security gates

Resolve the declared direct dependencies to an exact uv lock with Python 3.12 support, ensuring all legacy Tweepy/instagrapi/transitive-freeze baggage is absent. Add CI that installs from the frozen lock and runs formatting/lint, strict typing, unit/contract/integration tests with coverage, package build, lock consistency, and pip-audit. Keep platform contract tests offline and never inject production credentials into CI. The committed lock is the durable artifact; pyproject metadata, Git history, and the final report record its 2026-09-05 refresh provenance.

**Test:** no

**Dependencies:**
- T1.1
- T3.2
- T3.3
- T4.2

**Files:**
- `pyproject.toml`
- `uv.lock`
- `.github/workflows/ci.yml`

**Acceptance:**
- uv.lock is generated from pyproject.toml, exact, current as of 2026-09-05, and valid for the supported Python runtime.
- CI uses the frozen lock and contains quality, typing, tests/coverage, build, and vulnerability audit gates without secrets.
- The resolved dependency graph contains neither tweepy nor instagrapi and a lock-scoped vulnerability audit reports no known unacknowledged vulnerability.

**Verify-After:**
- `uv lock --check` (focused)
- `if uv tree | rg -qi "tweepy|instagrapi"; then exit 1; fi` (scoped_check)
- `uv run --frozen pip-audit` (scoped_check)
- `python3 -c "from pathlib import Path; s=Path('.github/workflows/ci.yml').read_text(); assert all(x in s for x in ('uv lock --check','ruff','mypy','pytest','pip-audit','build'))"` (scoped_check)

