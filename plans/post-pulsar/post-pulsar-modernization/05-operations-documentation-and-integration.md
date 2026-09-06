# Phase 5: Operations, Documentation, and Integration

## Codebase Anchors

Replace unsafe launchers, finish the operator migration, and prove the critical workflow offline.

### Task 5.1: Replace setup, launcher, and scheduled-task scripts

Rename all operational scripts to POST PULSAR and make them resolve their own repository path, create/use a project-local .venv, install the frozen project without system package upgrades, propagate exit codes, and avoid duplicate cron/task entries. The Unix scripts must be non-root and shell-safe; Windows scripts must quote paths and create/update the PostPulsar scheduled task explicitly. Remove the legacy venv helper and all hard-coded /root/phong-bot paths. Add static script contract tests and exercise Windows parsing/dry-run behavior through cmd.exe without changing Task Scheduler.

**Test:** yes

**Dependencies:**
- T4.2
- T4.3

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
- `venv.bat`
- `tests/unit/test_scripts.py`

**Acceptance:**
- Only POST PULSAR-named scripts remain and each safely handles a repository path containing spaces.
- Setup is project-local, lock-frozen, and repeatable, never upgrades the operating system, and launchers return the application exit code.
- Unix syntax and Windows batch quoting/idempotent task command construction pass automated checks; the Windows task path is exercised only in dry-run mode during verification.
- Scheduling remains external and one-shot; no new scheduler product feature is introduced.

**Verify-After:**
- `bash -n setup-post-pulsar.sh run-post-pulsar.sh` (focused)
- `.venv/bin/python -m pytest tests/unit/test_scripts.py -q` (focused)
- `cmd.exe /d /c "set POST_PULSAR_DRY_RUN=1&& task-setup-post-pulsar.bat"` (scoped_check)
- `! rg -n -i "phong[-_ ]?bot|/root/|run-bot" setup-post-pulsar.sh setup-post-pulsar.bat run-post-pulsar.sh run-post-pulsar.bat task-setup-post-pulsar.bat` (scoped_check)

### Task 5.2: Rewrite operator, migration, and security documentation

Rewrite README for POST PULSAR's actual X and Instagram support, official API prerequisites, configuration, one-shot workflow, external scheduling, media grammar, limitations, status/recovery, locked installation, and testing. Add a migration guide covering filename/config changes, required OAuth/Professional-account/public-hosting setup, plaintext credential and Instagram session revocation, and state reset expectations. Add security policy/secret hygiene and update licensing identity without changing GPL terms or authorship. Add a focused documentation contract test for required headings, implemented commands/environment variables, and prohibited stale claims.

**Test:** yes

**Dependencies:**
- T4.3
- T5.1

**Files:**
- `README.md`
- `MIGRATION.md`
- `SECURITY.md`
- `LICENSE.md`
- `tests/unit/test_docs.py`

**Acceptance:**
- Documentation matches implemented commands, config, locked installation, file grammar, supported media, recovery states, and official APIs.
- Migration explicitly tells operators to rotate legacy X credentials, revoke the private Instagram session, and provision environment tokens without printing values.
- No unsupported platform or feature is advertised and licensing remains GPL-3.0 with Anson Phong attribution.

**Verify-After:**
- `.venv/bin/python -m pytest tests/unit/test_docs.py -q` (focused)
- `rg -n "POST PULSAR|post-pulsar|POST_PULSAR_|official|rotate|revoke|ambiguous" README.md MIGRATION.md SECURITY.md` (scoped_check)
- `! rg -n -i "Threads API unavailable|instagrapi|tweepy|username/password login" README.md SECURITY.md` (scoped_check)

### Task 5.3: Prove the offline end-to-end workflow

Add integration tests that assemble real temporary inbox/config/state directories with fake official adapters and exercise no-candidate, invalid-bundle, all-preflight-before-mutation, successful two-target archive, one-target failure then safe retry, published-target skip, fingerprint drift, ambiguous final outcome, reconcile, interrupted archive recovery, and restart idempotence. Cover permanent or attempt-exhausted failure followed by operator correction and guarded retry, failed retry revalidation leaving state unchanged, immutable snapshot mismatch remaining blocked, and exact reconcile --not-published transition. Keep this task's hooks focused; complete project gates run in Stage 6/CI rather than inside task execution.

**Test:** yes

**Dependencies:**
- T4.2
- T5.2

**Files:**
- `tests/integration/test_workflow.py`
- `tests/conftest.py`

**Acceptance:**
- Integration coverage demonstrates that partial, ambiguous, drifted, and interrupted states cannot duplicate posts or lose source files.
- Blocked known failures require explicit guarded retry, failed revalidation is nonmutating, snapshot mismatch remains blocked, and reconcile --not-published changes only the selected target.
- The test suite performs no live login, upload, publish, or external HTTP request.

**Verify-After:**
- `uv run pytest tests/integration/test_workflow.py -q` (focused)
- `uv run ruff check tests/integration/test_workflow.py tests/conftest.py` (scoped_check)
- `uv run mypy tests/integration/test_workflow.py` (scoped_check)

