---
stage: 5
target: standard
repo: post-pulsar
context: ["plans/post-pulsar-modernization/01-brainstorm.md", "plans/post-pulsar-modernization/02-design.md", "plans/post-pulsar-modernization/02-design-evaluation.md", "plans/post-pulsar-modernization/03-hardening-amendments.md"]
docs: ["README.md", "https://docs.x.com/x-api/media/initialize-media-upload", "https://docs.x.com/x-api/media/upload-media", "https://docs.x.com/x-api/posts/create-or-edit-post", "https://www.postman.com/meta/instagram/documentation/6yqw8pt/instagram-api"]
depends: none
blocks: none
why: "The relocated repository still contains the PHONG-BOT identity, fragile filename globbing, obsolete/private platform integrations, plaintext credential loading, broken X video upload, and no durable per-platform delivery state. This plan modernizes the existing two-platform publishing product without adding product features."
stage_state: active
---

# POST PULSAR Modernization — Master Plan

## Task Checklist

### Phase 1: Identity, Configuration, and Content ([`01-identity-configuration-and-content.md`](01-identity-configuration-and-content.md))

- [x] #0195 `T1.1` **Task T1.1:** Establish package and identity baseline
- [ ] #ae4a `T1.2` **Task T1.2:** Replace configuration and secret loading
- [ ] #9b81 `T1.3` **Task T1.3:** Implement exact content bundle parsing

### Phase 2: Media and Durable Delivery Safety ([`02-media-and-durable-delivery-safety.md`](02-media-and-durable-delivery-safety.md))

- [ ] #6fd4 `T2.1` **Task T2.1:** Inspect and prepare media safely
- [ ] #5262 `T2.2` **Task T2.2:** Implement the SQLite delivery state machine
- [ ] #7951 `T2.3` **Task T2.3:** Add single-instance locking and exact archival recovery

### Phase 3: Official Platform Integrations ([`03-official-platform-integrations.md`](03-official-platform-integrations.md))

- [ ] #a30f `T3.1` **Task T3.1:** Define sanitized adapter and HTTP contracts
- [ ] #bb50 `T3.2` **Task T3.2:** Implement the official X API v2 adapter
- [ ] #25de `T3.3` **Task T3.3:** Implement the official Instagram adapter

### Phase 4: Orchestration, CLI, and Supply Chain ([`04-orchestration-cli-and-supply-chain.md`](04-orchestration-cli-and-supply-chain.md))

- [ ] #aa9e `T4.1` **Task T4.1:** Build safe one-run orchestration
- [ ] #1efa `T4.2` **Task T4.2:** Expose safe run, status, retry, and reconcile commands
- [ ] #8ae2 `T4.3` **Task T4.3:** Lock dependencies and enforce CI security gates

### Phase 5: Operations, Documentation, and Integration ([`05-operations-documentation-and-integration.md`](05-operations-documentation-and-integration.md))

- [ ] #c0d4 `T5.1` **Task T5.1:** Replace setup, launcher, and scheduled-task scripts
- [ ] #30ed `T5.2` **Task T5.2:** Rewrite operator, migration, and security documentation
- [ ] #97cd `T5.3` **Task T5.3:** Prove the offline end-to-end workflow

## File Structure

| File | Action | Purpose |
| --- | --- | --- |
| `.gitattributes` | create | Make line endings and text normalization deterministic across Windows and Linux. |
| `.gitignore` | modify | Ignore POST PULSAR secrets, runtime state, logs, environments, and generated media. |
| `pyproject.toml` | create | Define the Python 3.12 package, CLI, dependencies, and tool policy. |
| `src/post_pulsar` | create | Replace the flat legacy scripts with the POST PULSAR application package. |
| `tests` | create | Protect parser, state, archive, API, CLI, and orchestration contracts. |
| `post-pulsar.toml.example` | create | Document non-secret configuration. |
| `.env.example` | create | Document secret environment variable names without values. |
| `.github/workflows/ci.yml` | create | Run supported-version quality and security checks. |
| `uv.lock` | create | Lock exact transitive dependency versions reproducibly. |
| `README.md` | modify | Document the current product, APIs, setup, operation, and limitations. |
| `MIGRATION.md` | create | Guide existing PHONG-BOT operators through the breaking security/API migration. |
| `SECURITY.md` | create | Document secret handling, rotation, reporting, and supported practices. |
| `MODERNIZATION_REPORT.md` | create | Record completed work, verification evidence, operator gates, and clearly out-of-scope future opportunities. |
| `LICENSE.md` | modify | Retain licensing while applying the durable POST PULSAR identity. |
| `phong-bot.py` | delete | Remove the obsolete unsafe orchestrator. |
| `post_base.py` | delete | Remove the obsolete platform base implementation. |
| `post_x.py` | delete | Remove the legacy Tweepy integration. |
| `post_instagram.py` | delete | Remove the private Instagram username/password integration. |
| `update_config.py` | delete | Remove the stale Threads-era secret duplication utility. |
| `config-sample.json` | delete | Replace secret-bearing JSON configuration with TOML plus environment secrets. |
| `requirements.txt` | delete | Replace the stale freeze with declared direct dependencies and a generated lock. |
| `setup.sh` | move | Rename and make the Unix setup script local, repeatable, and safe. |
| `setup.bat` | move | Rename and modernize the Windows setup script. |
| `run-bot.sh` | move | Rename and modernize the Unix launcher. |
| `run-bot.bat` | move | Rename and modernize the Windows launcher. |
| `task-setup.bat` | move | Rename and modernize Windows Task Scheduler setup. |
| `venv.bat` | delete | Remove the ambiguous legacy interactive environment helper. |

## Acceptance

- Every current product, package, command, configuration, script, task, log, and documentation surface uses POST PULSAR naming; no tracked PHONG-BOT identity remains except migration/history context.
- The Git origin is git@github.com:ansonphong/POST-PULSAR.git and the repository root remains POST-PULSAR.
- Only official X API v2 and official Meta Instagram API publishing paths remain; Tweepy, instagrapi, username/password login, and persisted Instagram sessions are absent.
- Secrets are environment-only, never written or logged, and invalid/missing credentials fail before mutation.
- Exact content parsing prevents prefix collisions and symlink/path confusion; all enabled targets pass local and read-only remote preflight before publishing begins.
- SQLite records immutable target snapshots and per-platform delivery outcomes; partial or ambiguous delivery cannot be silently retried or archived.
- Only exact validated bundle members are archived, and only after all required targets are confirmed published.
- Supported media is inspected by content, constrained per platform, and temporary public Instagram media is safely staged and reconciled.
- Focused unit, contract, and integration tests pass; lint, typing, packaging, lock verification, and dependency audit pass.
- README and migration/security documents explain setup, credential provisioning, scheduling, limitations, recovery, and operator-required rotation steps without advertising new features.
- A final modernization report records completed changes and evidence, then separately lists easy future API/platform opportunities and original recommendations while stating they were not implemented.

## Execution Rules

- This master file is the sole checkbox ledger.
- Use planctl to mutate task state; never hand-edit checkbox marks.
- Phase files contain task detail and Verify-After hooks, but no checkboxes.
- Stage 6 is report-only: it runs broad quality/security gates and delivers their actual evidence in the final review response; `T5.3` owns the last tracked modernization-report update.
