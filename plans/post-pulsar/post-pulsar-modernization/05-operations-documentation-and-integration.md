# Phase 5: Operations, Documentation, and Integration

## Codebase Anchors

Replace unsafe launchers, finish the operator migration, and prove the critical workflow offline.

### Task 5.1: Replace setup, launcher, and daemon service scripts

Perform one atomic operational cutover. Rename every launcher/setup/task script
to POST PULSAR, make it resolve its own repository path, create/use a local
`.venv`, install via the frozen lock without OS upgrades, propagate exit codes,
and avoid duplicate service/task entries. Unix scripts are non-root and shell-safe;
Windows scripts quote paths and explicitly create/update `PostPulsar`. The
launchers expose one-shot commands and the foreground `daemon`; the example
user service and Windows scheduled task run the daemon and rely on its durable
scheduler instead of invoking one publication per OS tick. Installing or
removing background startup is always an explicit operator action. Scripts
support a simple same-user installation and print guarded, explicit
steps for the recommended hardened service-user installation. Hardened mode
keeps state/publishable buckets under the daemon identity and grants the agent
identity write access only to DRAFTS plus read-only access to bootstrap,
endpoint, and its scoped capability records; scripts validate but
do not silently create users, widen ACLs, or move existing content. Remove the
legacy `venv` helper, flat Python entry/modules, requirements freeze,
secret-bearing JSON sample, and Threads-era updater only in this same task, so
every prior commit retains the old runnable path and this commit switches fully
to the already-tested package. Update README's minimum install/run references
in the same cutover; `T5.2` completes the full documentation rewrite. Add
static script/identity/import contract tests and exercise Windows parsing/dry-
run behavior through `cmd.exe` without changing Task Scheduler.
Each setup detects a foreign-layout `.venv` (`Scripts` versus `bin`) and fails
with an actionable guarded recreate instruction rather than deleting or using
the wrong interpreter; isolated tests construct both mismatched layouts without
touching the developer environment.

**Test:** yes

**Dependencies:**
- T4.3
- T4.5
- T4.6

**Files:**
- `setup.sh`
- `setup-post-pulsar.sh`
- `setup.bat`
- `setup-post-pulsar.bat`
- `run-bot.sh`
- `run-post-pulsar.sh`
- `run-bot.bat`
- `run-post-pulsar.bat`
- `task-setup.bat`
- `task-setup-post-pulsar.bat`
- `service/post-pulsar.service`
- `service/com.post-pulsar.daemon.plist`
- `venv.bat`
- `phong-bot.py`
- `post_base.py`
- `post_x.py`
- `post_instagram.py`
- `requirements.txt`
- `config-sample.json`
- `update_config.py`
- `README.md`
- `tests/unit/test_scripts.py`

**Acceptance:**
- Only POST PULSAR-named scripts remain and each safely handles a repository path containing spaces.
- Setup is project-local, lock-frozen, and repeatable, never upgrades the operating system, and launchers return the application exit code.
- Unix syntax and Windows batch quoting/idempotent task command construction pass automated checks; the Windows task path is exercised only in dry-run mode during verification.
- Built-in schedules run only through the singleton foreground daemon; OS integration starts/restarts that daemon and never creates a second scheduling authority.
- systemd-user, launchd-user, and Windows task installation is explicit,
  repeatable, user-scoped, and does not start, stop, or replace an unrelated
  process; the MCP bridge requests only the selected installed service.
- Hardened separate-identity mode uses an operator-enabled system service and
  disables MCP auto-start; daemon absence returns setup guidance rather than
  crossing the identity boundary.
- Hardened ACL tests require read-only agent access to atomically replaced
  bootstrap/endpoint/capability records while denying state, config,
  publishable-bucket, ready-marker, and operator-verifier writes.
- Simple same-user mode is labeled a convenience rather than an agent sandbox;
  hardened service-user permission checks prove the agent cannot write ready
  markers, publishable buckets, state, config, or operator-secret verifier.
- The cutover commit leaves the new package runnable/importable and contains no active legacy entry/module/config/dependency path.
- POSIX and Windows setup paths reject the other OS's `.venv` layout deterministically and never delete it automatically.

**Verify-After:**
- `bash -n setup-post-pulsar.sh run-post-pulsar.sh` (focused)
- `.venv/bin/python -m pytest tests/unit/test_scripts.py -q` (focused)
- `cmd.exe /d /c "set POST_PULSAR_DRY_RUN=1&& task-setup-post-pulsar.bat"` (scoped_check)
- `! rg -n -i "phong[-_ ]?bot|/root/|run-bot" setup-post-pulsar.sh setup-post-pulsar.bat run-post-pulsar.sh run-post-pulsar.bat task-setup-post-pulsar.bat` (scoped_check)
- `.venv/bin/python -m compileall -q src/post_pulsar && .venv/bin/python -m post_pulsar --help` (scoped_check)

### Task 5.2: Rewrite operator, migration, and security documentation

Rewrite README for POST PULSAR's actual X and Instagram support, official API
prerequisites, `ffprobe`/FFmpeg operator provisioning, configuration, one-shot
workflow, profile/account isolation, `QUEUE`/`RANDOM`/`REELS` buckets, ready
markers, built-in scheduling, daemon/service operation, authenticated local
control, exact media grammar/limits, normalization and alt
warnings, status/recovery, locked installation, and tests. Add a migration guide
covering filename/config changes, required OAuth/Professional-account/public-
hosting setup, plaintext credential/session revocation, and state expectations.
Add security policy/secret hygiene, the MCP/plugin trust boundary and safe
installation directions for the sibling repository, and update licensing identity without
changing GPL terms/authorship. Create `MODERNIZATION_REPORT.md` recording
completed changes and actual evidence available through `T5.2`, operator-only
gates, lock/API refresh date, and clearly separated easy future platform/API
opportunities and original recommendations; it must state none of those future
items were implemented and must not guess or contain pending-evidence claims.
Add a focused documentation contract test for required headings, implemented
commands/environment variables, truthful negated/migration references, and
prohibited active stale claims/install instructions.

**Test:** yes

**Dependencies:**
- T4.6
- T5.1

**Files:**
- `README.md`
- `MIGRATION.md`
- `SECURITY.md`
- `MODERNIZATION_REPORT.md`
- `LICENSE.md`
- `tests/unit/test_docs.py`

**Acceptance:**
- Documentation matches implemented commands, profiles, buckets, schedules, daemon/control operation, locked installation, file grammar, supported media, recovery states, and official APIs.
- Security documentation distinguishes simple and hardened identity/ACL modes,
  explains DRAFTS admission and TTY approval, and does not claim protection from
  arbitrary same-user shell/file access.
- Migration explicitly tells operators to rotate legacy X credentials, revoke the private Instagram session, and provision environment tokens without printing values.
- No unsupported platform or feature is advertised and licensing remains GPL-3.0 with Anson Phong attribution.
- The modernization report fulfills the requested end-state/future-opportunity deliverable without presenting unimplemented ideas as current capability.

**Verify-After:**
- `.venv/bin/python -m pytest tests/unit/test_docs.py -q` (focused)
- `rg -n "POST PULSAR|post-pulsar|POST_PULSAR_|official|rotate|revoke|ambiguous" README.md MIGRATION.md SECURITY.md` (scoped_check)

### Task 5.3: Prove the offline end-to-end workflow

Add integration tests using temporary profile roots/config/state directories, fake
official adapters, and injected chooser/clock/sleeper. Exercise zero-target and
no-candidate paths, invalid bundles, all-preflight-before-mutation, successful
two-target archive, one-target failure with `next_attempt_at` skipped until the
fake clock advances exactly past due, published-target skip, fingerprint drift,
ambiguous final outcome, reconcile, interrupted archive recovery, and restart
idempotence. Inject a crash after the fake remote final-create side effect but
before DB result commit; restart must mark stale `final_dispatch_started`
ambiguous, call no publish method, retain source, and require explicit
reconciliation. Cover five-failure exhaustion with controlled time, operator
correction plus guarded retry, failed retry revalidation leaving state unchanged,
immutable snapshot mismatch remaining blocked, exact `reconcile --not-published`,
and resource/staging cleanup. Cover two profiles with isolated clients and
failures, ready-marker admission, every standard bucket, deterministic random
selection, schedule deduplication/restart/DST/misfire behavior, daemon singleton
and graceful shutdown, authenticated bounded control requests, idempotent agent
requests, and the trusted-CLI-approved confirmation-intent publication path.
The test harness permits only ephemeral loopback sockets for the control server
and retains a sentinel proving every non-loopback/platform socket is denied.
Keep hooks focused; full project gates run in
Stage 6/CI.

**Test:** yes

**Dependencies:**
- T4.6
- T5.2

**Files:**
- `tests/integration/test_workflow.py`
- `tests/support/fake_daemon.py`
- `MODERNIZATION_REPORT.md`

**Acceptance:**
- Integration coverage demonstrates that partial, ambiguous, drifted, and interrupted states cannot duplicate posts or lose source files.
- Blocked known failures require explicit guarded retry, failed revalidation is nonmutating, snapshot mismatch remains blocked, and reconcile --not-published changes only the selected target.
- Backoff/exhaustion assertions use an injected clock with no real sleep; the post-created/pre-commit crash window is explicitly proven ambiguous and non-republishing.
- The test suite performs no live login, upload, publish, or external HTTP request.
- The two named example profiles never share credentials, clients, roots, target identities, failures, archives, or schedule runs.
- Daemon and control tests bind only ephemeral loopback ports, deny arbitrary commands/paths, and prove that unconfirmed or installation-disabled agent publication cannot dispatch.
- The test-only fake-adapter daemon launcher exercises the real core daemon/control handlers, is excluded from built runtime entry points, and is available to the sibling repository's hermetic compatibility test.
- The modernization report is finalized with actual Stage 5 focused integration evidence and no pending/guessed gate claims; broad Stage 6 evidence is delivered in the final review response.

**Verify-After:**
- `.venv/bin/uv run --frozen pytest tests/integration/test_workflow.py -q` (focused)
- `.venv/bin/uv run --frozen ruff check tests/integration/test_workflow.py` (scoped_check)
- `.venv/bin/uv run --frozen mypy tests/integration/test_workflow.py` (scoped_check)
- `git diff --exit-code -- uv.lock` (scoped_check)
