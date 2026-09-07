---
stage: 5
target: standard
repo: post-pulsar
context: ["plans/post-pulsar-modernization/01-brainstorm.md", "plans/post-pulsar-modernization/02-design.md", "plans/post-pulsar-modernization/02-design-evaluation.md", "plans/post-pulsar-modernization/03-hardening-amendments.md", "plans/post-pulsar-modernization/04-scope-expansion-design.md"]
docs: ["README.md", "https://docs.x.com/x-api/media/initialize-media-upload", "https://docs.x.com/x-api/media/upload-media", "https://docs.x.com/x-api/posts/create-or-edit-post", "https://www.postman.com/meta/instagram/documentation/6yqw8pt/instagram-api", "https://developers.openai.com/plugins/concepts/plugins", "https://developers.openai.com/plugins/build/mcp-server", "https://developers.openai.com/plugins/build/plugins"]
depends: none
blocks: none
why: "Modernize the former PHONG-BOT into POST PULSAR, then deliver the operator-approved expansion: safe multi-account scheduling plus a separate secretless MCP/plugin repository for agent control without a GUI."
stage_state: active
---

# POST PULSAR Modernization — Master Plan

## Task Checklist

### Phase 1: Identity, Configuration, and Content ([`01-identity-configuration-and-content.md`](01-identity-configuration-and-content.md))

- [x] #0195 `T1.1` **Task T1.1:** Establish package and identity baseline
- [x] #ae4a `T1.2` **Task T1.2:** Replace configuration and secret loading
- [x] #9b81 `T1.3` **Task T1.3:** Implement exact content bundle parsing
- [x] #a497 `T1.4` **Task T1.4:** Add profiles, account roots, standard buckets, and ready markers

### Phase 2: Media and Durable Delivery Safety ([`02-media-and-durable-delivery-safety.md`](02-media-and-durable-delivery-safety.md))

- [x] #6fd4 `T2.1` **Task T2.1:** Inspect and prepare media safely
- [x] #5262 `T2.2` **Task T2.2:** Implement the SQLite delivery state machine
- [x] #7951 `T2.3` **Task T2.3:** Add single-instance locking and exact archival recovery
- [x] #bd92 `T2.4` **Task T2.4:** Add journaled single-account state and layout migration

### Phase 3: Official Platform Integrations ([`03-official-platform-integrations.md`](03-official-platform-integrations.md))

- [x] #a30f `T3.1` **Task T3.1:** Define sanitized adapter and HTTP contracts
- [x] #bb50 `T3.2` **Task T3.2:** Implement the official X API v2 adapter
- [x] #25de `T3.3` **Task T3.3:** Implement the official Instagram adapter

### Phase 4: Orchestration, CLI, and Supply Chain ([`04-orchestration-cli-and-supply-chain.md`](04-orchestration-cli-and-supply-chain.md))

- [x] #aa9e `T4.1` **Task T4.1:** Build safe one-run orchestration
- [x] #1efa `T4.2` **Task T4.2:** Expose safe run, status, retry, and reconcile commands
- [x] #8ae2 `T4.3` **Task T4.3:** Lock dependencies and enforce CI security gates
- [x] #0ef7 `T4.4` **Task T4.4:** Implement deterministic profile scheduling
- [x] #371e `T4.5` **Task T4.5:** Add the foreground daemon and authenticated control API
- [x] #1a24 `T4.6` **Task T4.6:** Extend the CLI for profiles, scheduling, daemon control, and migration

### Phase 5: Operations, Documentation, and Integration ([`05-operations-documentation-and-integration.md`](05-operations-documentation-and-integration.md))

- [x] #c0d4 `T5.1` **Task T5.1:** Replace setup, launcher, and scheduled-task scripts
- [x] #30ed `T5.2` **Task T5.2:** Rewrite operator, migration, and security documentation
- [x] #97cd `T5.3` **Task T5.3:** Prove the offline end-to-end workflow

### Phase 6: MCP and Agent Plugins ([`06-mcp-and-agent-plugins.md`](06-mcp-and-agent-plugins.md))

- [x] #ba2e `T6.1` **Task T6.1:** Create the POST-PULSAR-PLUGINS repository and package baseline
- [ ] #f421 `T6.2` **Task T6.2:** Implement the versioned core bridge and safe daemon auto-start
- [ ] #232c `T6.3` **Task T6.3:** Expose read-only MCP tools and resources
- [ ] #6067 `T6.4` **Task T6.4:** Add confirmed scheduling, queue, publishing, and recovery tools
- [ ] #609f `T6.5` **Task T6.5:** Package provider-neutral skills for Codex and Claude Code
- [ ] #6af4 `T6.6` **Task T6.6:** Prove cross-repository compatibility and document installation

### Phase 7: Ecosystem Report ([`07-ecosystem-report.md`](07-ecosystem-report.md))

- [ ] #ce83 `T7.1` **Task T7.1:** Record verified plugin compatibility in the core report

## File Structure

| File | Action | Purpose |
| --- | --- | --- |
| `.gitattributes` | create | Make line endings and text normalization deterministic across Windows and Linux. |
| `.gitignore` | modify | Ignore POST PULSAR secrets, runtime state, logs, environments, and generated media. |
| `pyproject.toml` | create | Define the Python 3.12 package, CLI, dependencies, and tool policy. |
| `src/post_pulsar` | create | Replace the flat legacy scripts with the POST PULSAR application package. |
| `accounts/<profile>/{DRAFTS/{QUEUE,RANDOM,REELS},QUEUE,RANDOM,REELS}/<bundle-id>` | runtime | Separate agent-writable unpublished bundle directories from profile-isolated publishable bundle directories. |
| `accounts/<profile>/POSTED/<bucket>/<bundle-id>` | runtime | Archive each exact published bundle under its original profile and editorial bucket. |
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
| `../POST-PULSAR-PLUGINS` | create | Separate secretless stdio MCP server and Codex/Claude plugin package. |

## Acceptance

- Every current product, package, command, configuration, script, task, log, and documentation surface uses POST PULSAR naming; no tracked PHONG-BOT identity remains except migration/history context.
- The Git origin is git@github.com:ansonphong/POST-PULSAR.git and the repository root remains POST-PULSAR.
- Only official X API v2 and official Meta Instagram API publishing paths remain; Tweepy, instagrapi, username/password login, and persisted Instagram sessions are absent.
- Social publishing secrets are environment-only and never written or logged;
  the local agent capability and operator approval verifier are separately
  initialized, scoped, owner-only, rotatable/revocable, and never exposed in output. Invalid/missing
  credentials fail before mutation.
- Exact content parsing prevents prefix collisions and symlink/path confusion; all enabled targets pass local and read-only remote preflight before publishing begins.
- SQLite records immutable target snapshots and per-platform delivery outcomes; partial or ambiguous delivery cannot be silently retried or archived.
- Only exact validated bundle members are archived, and only after all required targets are confirmed published.
- Supported media is inspected by content, constrained per platform, and temporary public Instagram media is safely staged and reconciled.
- Focused unit, contract, and integration tests pass; lint, typing, packaging, lock verification, and dependency audit pass.
- README and migration/security documents explain setup, credential provisioning, scheduling, limitations, recovery, and operator-required rotation steps without advertising new features.
- A final modernization report records completed changes and evidence, then separately lists easy future API/platform opportunities and original recommendations while stating they were not implemented.
- Stable profiles isolate account roots, token references, authenticated clients, remote identities, delivery failures, archives, and schedules; the tracked examples name X profiles `ansonphong` and `360hextile` without secrets.
- Agent-writable DRAFTS are never publishable; trusted core approval atomically
  admits exact bundles into `QUEUE`, `RANDOM`, or `REELS`, where deterministic
  selection and bounded timezone-aware schedules cannot double-fire across
  restarts or DST transitions.
- One foreground daemon and SQLite database own sequential publishing. Its loopback control protocol is authenticated, versioned, bounded, redacted, idempotent, and incapable of arbitrary command/path execution.
- `POST-PULSAR-PLUGINS` provides validated Codex and Claude Code packaging plus a stdio MCP interface. It never receives social tokens or bypasses core publication, ambiguity, retry, or archival rules.
- Irreversible MCP operations require exact-resource revisions, idempotency,
  installation-level eligibility, and expiring one-time intents approved through
  a trusted interactive core CLI outside MCP. Single-user mode does not claim to
  sandbox an agent that is separately granted arbitrary same-user shell access.

## Execution Rules

- This master file is the sole checkbox ledger.
- Use planctl to mutate task state; never hand-edit checkbox marks.
- Phase files contain task detail and Verify-After hooks, but no checkboxes.
- Stage 6 is report-only: it runs broad quality/security gates across both repositories and delivers their actual evidence in the final review response; `T7.1` owns the last tracked ecosystem-report update.
