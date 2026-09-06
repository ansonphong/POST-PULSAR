# Phase 6: MCP and Agent Plugins

## Codebase Anchors

Create a separate, provider-neutral agent surface that controls POST PULSAR
through its public local protocol. The sibling repository never owns social
credentials, publishing state, or platform behavior.

Unprefixed paths in this phase are relative to the self-contained installable
plugin root `../POST-PULSAR-PLUGINS/plugins/post-pulsar`. Paths beginning with
`../../` are relative to the sibling repository root. Every task implementation
commit belongs only to the sibling repository; the core ledger update is its
separate conductor commit.

### Task 6.1: Create the POST-PULSAR-PLUGINS repository and package baseline

Use the supported Codex plugin scaffold to create `plugins/post-pulsar`, its
required `.codex-plugin/plugin.json`, skill and MCP directories, and a
repository-local Codex marketplace at `.agents/plugins/marketplace.json`.
Initialize the sibling Git repository, configure the intended origin as
`git@github.com:ansonphong/POST-PULSAR-PLUGINS.git`, and add a Python 3.12
package, frozen dependency lock, deterministic line endings, ignore policy,
license, compatibility metadata, a vendored hash-addressed copy of the core
control contract, and empty test structure. Bootstrap a local `.venv` and pinned
`uv` before any verifier command. Add the Claude plugin manifest/catalog
location without claiming compatibility until later tasks validate it.
Define exact bootstrap discovery through `POST_PULSAR_MCP_BOOTSTRAP`, pointing
to an owner-only setup-generated JSON record whose schema contains the endpoint-
record path, allow-listed service-manager enum/identifier, agent-capability path,
and installation ID. The record contains no credential value and exists before
the daemon; no circular endpoint discovery is allowed.

The GitHub repository may not exist yet. Local initialization and commits are
required; a missing remote is an operator gate and must not weaken or block the
core repository.

**Test:** yes

**Dependencies:**
- T4.6

**Files:**
- `../../.gitignore`
- `../../.gitattributes`
- `../../LICENSE.md`
- `pyproject.toml`
- `uv.lock`
- `compatibility.toml`
- `contracts/control-v1.openapi.json`
- `contracts/control-v1.sha256`
- `schemas/bootstrap.schema.json`
- `post-pulsar-mcp.toml.example`
- `../../.agents/plugins/marketplace.json`
- `../../.claude-plugin/marketplace.json`
- `.codex-plugin/plugin.json`
- `.claude-plugin/plugin.json`
- `.mcp.json`
- `claude.mcp.json`
- `src/post_pulsar_mcp/__init__.py`
- `tests/conftest.py`
- `tests/unit/test_manifest.py`

**Acceptance:**
- The plugin-creator scaffold and validator pass without hand-invented Codex manifest fields.
- The sibling repository has an independent history and the intended origin URL, while no source file contains a social or control token.
- Python, build, and lock metadata use supported current versions and deterministic installs.
- `compatibility.toml`, package metadata, both manifests, and the vendored core-contract hash agree on versions and supported core/control/state ranges.

**Verify-After:**
- `.venv/bin/uv lock --check && .venv/bin/uv run --frozen pytest tests/unit/test_manifest.py -q` (focused)
- `test "$(git -C ../.. remote get-url origin)" = git@github.com:ansonphong/POST-PULSAR-PLUGINS.git` (scoped_check)
- `! git -C ../.. grep -nEi '(bearer|access[_-]?token|client[_-]?secret)\s*[=:]\s*[^$<{ ]+' -- . ':!plugins/post-pulsar/tests/**'` (scoped_check)

### Task 6.2: Implement the versioned core bridge and safe daemon auto-start

Implement a typed client for `post-pulsar.control/v1`. Discover only an
owner-readable bootstrap, endpoint, and agent-capability records at explicitly configured locations, validate
its permissions, literal loopback endpoint, protocol, process identity, and
capabilities, then authenticate every request. Never read the core database or
import its private Python package.

When the core is absent in simple mode, contend on an owner-only auto-start lock
and request only an enum-selected, setup-generated user service (systemd user
unit, launchd label, or Windows scheduled task) through a fixed absolute
executable/argument template with no shell. Hardened mode disables auto-start
and returns exact operator guidance until its separate-identity system service
is online. The service manager owns the daemon environment; the plugin never
inherits social credentials. Use `stdin=DEVNULL`, redirect
stdout/stderr to an owner-only log, close inherited descriptors, and detach as
appropriate for the OS. Bound startup,
handshake, request, and shutdown waits. Losing contenders wait for the same
verified endpoint; they do not request another start. Never install a service, kill a
process, accept command fragments, or forward arbitrary URLs/paths. A protocol
or capability mismatch exposes degraded read-only health and disables mutation.

**Test:** yes

**Dependencies:**
- T6.1

**Files:**
- `src/post_pulsar_mcp/config.py`
- `src/post_pulsar_mcp/models.py`
- `src/post_pulsar_mcp/client.py`
- `src/post_pulsar_mcp/autostart.py`
- `schemas/bootstrap.schema.json`
- `contracts/control-v1.openapi.json`
- `compatibility.toml`
- `tests/unit/test_config.py`
- `tests/unit/test_client.py`
- `tests/unit/test_autostart.py`

**Acceptance:**
- Discovery, authentication, redaction, bounds, timeout, and version negotiation are typed and tested.
- Missing/stale/moved bootstrap and endpoint records, installation-ID mismatch,
  unsafe permissions/ACLs, and unsupported service modes fail closed on every
  supported OS abstraction.
- Hardened mode requires the expected read-only agent ACL on bootstrap,
  endpoint, and agent-capability records and never attempts auto-start.
- Concurrent absent-daemon requests produce at most one safe allow-listed service-start request and all successful callers validate the same handshake.
- Incompatible cores cannot receive mutation, and no bridge input becomes a shell command, arbitrary URL, or unrestricted filesystem path.
- A noisy service manager/daemon cannot contaminate MCP stdout; the bridge never stops a process and validates endpoint PID/start-time/nonce before use.

**Verify-After:**
- `.venv/bin/uv run --frozen pytest tests/unit/test_config.py tests/unit/test_client.py tests/unit/test_autostart.py -q` (focused)
- `.venv/bin/uv run --frozen mypy src/post_pulsar_mcp/config.py src/post_pulsar_mcp/models.py src/post_pulsar_mcp/client.py src/post_pulsar_mcp/autostart.py` (scoped_check)
- `! rg -n 'shell\s*=\s*True|os\.system|subprocess\..*shell|sqlite3|requests\.(get|post)\(' src/post_pulsar_mcp` (scoped_check)

### Task 6.3: Expose read-only MCP tools and resources

Build one stdio MCP server with strict Pydantic inputs and JSON-only stdout.
Expose concise tools/resources for capabilities, health, dashboard, profiles,
buckets, bundles/queue, schedules, requests, media inspection, and publication
preview. Every operation delegates to the bridge and returns bounded, redacted,
stable schemas with pagination. Mark all read operations with correct MCP
read-only/idempotent annotations and keep logs on stderr.
Check in a normative registry naming every tool/resource and its exact input,
output, identifier, revision, pagination bound, required core operation, and
MCP `readOnlyHint`, `destructiveHint`, `idempotentHint`, and `openWorldHint`.
Generate/validate server registration from this registry rather than duplicating
freehand schemas.

**Test:** yes

**Dependencies:**
- T6.2

**Files:**
- `src/post_pulsar_mcp/server.py`
- `src/post_pulsar_mcp/read_tools.py`
- `src/post_pulsar_mcp/resources.py`
- `contracts/mcp-registry.json`
- `tests/unit/test_read_tools.py`
- `tests/contract/test_stdio.py`

**Acceptance:**
- Hosts can discover and call the documented read tools/resources without receiving credentials, raw headers, database paths, or unbounded results.
- Stdout contains only valid MCP frames; diagnostics use stderr and secrets are redacted.
- Tool schemas reject extra fields, traversal, oversize strings/lists, and unsupported protocol capabilities before core mutation.

**Verify-After:**
- `.venv/bin/uv run --frozen pytest tests/unit/test_read_tools.py tests/contract/test_stdio.py -q` (focused)
- `.venv/bin/uv run --frozen ruff check src/post_pulsar_mcp/server.py src/post_pulsar_mcp/read_tools.py src/post_pulsar_mcp/resources.py tests/unit/test_read_tools.py tests/contract/test_stdio.py` (scoped_check)

### Task 6.4: Add confirmed scheduling, queue, publishing, and recovery tools

Add tools for bounded caption/alt edits on an existing unready
core-discovered bundle, validation/readiness, enqueue, schedule
create/update/enable/disable, pause/resume, run-now, cancel/delete pending work,
retry, and reconcile. All mutations require an idempotency key and expected
revision. Consequential operations ask the core to create an expiring
confirmation intent and return its exact summary. The agent can poll intent
status, but only the trusted interactive core CLI/operator principal can approve
it outside MCP; after approval, a separate tool asks core to consume it once.
The plugin holds no approval authority or signing secret. Direct
publication is absent from tool discovery unless the core reports the operator
setting `allow_agent_publish = true`. No tool deletes source media or remote
posts, accepts credentials, edits arbitrary config/database fields, or bypasses
core validation and state transitions. MCP accepts no raw filesystem path or
media bytes; operators/host file tools place media only in the configured
DRAFTS tree, then MCP addresses it by profile, draft bucket, and bundle ID.

**Test:** yes

**Dependencies:**
- T6.3

**Files:**
- `src/post_pulsar_mcp/write_tools.py`
- `src/post_pulsar_mcp/intents.py`
- `src/post_pulsar_mcp/server.py`
- `contracts/mcp-registry.json`
- `tests/unit/test_write_tools.py`
- `tests/unit/test_intents.py`
- `tests/contract/test_stdio.py`

**Acceptance:**
- Replayed idempotency keys return the same outcome; stale revisions and changed fingerprints fail closed.
- Core confirmation binds action, arguments, resource identity, revision, fingerprint, consequence, expiry, and one-time nonce; the plugin can neither approve nor forge it, and replay, mutation, expiry, or cross-profile use is rejected.
- Through the MCP capability surface, agent publishing cannot be enabled by a
  prompt/tool call and cannot dispatch when installation opt-in or trusted CLI
  approval is absent; docs explicitly exclude separately granted arbitrary
  same-user shell/file authority from that guarantee.
- The registry's exact matrix covers draft admission/ready, enqueue, run-now,
  resume-due, enabled/due schedule mutation, cancel/delete, retry, and reconcile;
  annotations/discovery match core opt-in and approval requirements.

**Verify-After:**
- `.venv/bin/uv run --frozen pytest tests/unit/test_write_tools.py tests/unit/test_intents.py tests/contract/test_stdio.py -q` (focused)
- `.venv/bin/uv run --frozen mypy src/post_pulsar_mcp/write_tools.py src/post_pulsar_mcp/intents.py src/post_pulsar_mcp/server.py` (scoped_check)

### Task 6.5: Package provider-neutral skills for Codex, Claude Code, and MCP hosts

Write small provider-neutral skills that teach agents to inspect status,
prepare/preview/queue content, manage constrained schedules, and recover safely.
Keep trusted-CLI approval as a visible user-facing step. Wire the same stdio
server into validated Codex and Claude Code packages and document a
generic MCP-client recipe for Grok Build or other hosts that support local MCP;
do not claim a native integration that was not exercised. Skills never embed
tokens, hard-coded account paths, hidden prompt authority, or duplicate core
business rules.

The tracked standard-library launcher requires Python 3.12 and the exact
bootstrap `uv` version recorded in compatibility metadata. From a cached plugin
copy with no `.venv`, it verifies lock/metadata hashes, takes a file lock, and
uses `uv sync --frozen` to provision an environment in the host's persistent
per-plugin data directory keyed by plugin version plus lock hash; it never writes
the plugin cache. First launch may download only locked dependencies, subsequent
launches reuse the verified environment, and runtime forwards only an explicit
environment allowlist. Both host configs resolve the installed plugin root and
invoke this launcher without assuming the current working directory.

**Test:** yes

**Dependencies:**
- T6.4

**Files:**
- `skills/post-pulsar/SKILL.md`
- `skills/post-pulsar/references/workflows.md`
- `.codex-plugin/plugin.json`
- `.claude-plugin/plugin.json`
- `.mcp.json`
- `claude.mcp.json`
- `scripts/launch-mcp.py`
- `../../.agents/plugins/marketplace.json`
- `../../.claude-plugin/marketplace.json`
- `docs/CODEX.md`
- `docs/CLAUDE-CODE.md`
- `docs/GROK-BUILD.md`
- `tests/contract/test_plugin_packages.py`

**Acceptance:**
- Codex's host validator passes when available and repository-owned contract
  tests enforce the manifest/schema subset in CI; the local marketplace
  identifies the actual plugin path/version.
- Codex `plugin.json` references its `.mcp.json`; Claude packaging references
  its distinct top-level-`mcpServers` configuration. Both use exact
  command/arguments, an allow-listed environment, and a working-directory-
  independent frozen launcher without secrets.
- A test copies only the declared plugin source to a cache path containing
  spaces, starts with no development `.venv`, provisions through a local test
  wheelhouse, and launches/discovers the MCP server successfully through both
  host configurations on first and subsequent runs.
- Skills are concise, capability-aware, and preserve preview/confirmation/recovery safety for every host.

**Verify-After:**
- `.venv/bin/uv run --frozen pytest tests/contract/test_plugin_packages.py -q` (focused)

### Task 6.6: Prove cross-repository compatibility and document installation

Add CI plus fake-client unit coverage and a hermetic real-core integration suite
covering the complete agent journey: auto-start contention, handshake,
two-profile dashboard, media preview, ready
and enqueue, schedule creation, opt-in/confirmation publication, ambiguous
recovery, restart, pagination, version mismatch, and redaction. Deny external
network access. The test builds/installs the declared compatible core revision,
starts its actual daemon/control handlers through the core's test-only fake-
adapter launcher with temporary roots, drives the MCP server over stdio, and
validates the same vendored protocol artifact and SHA on both sides. It uses no
private core imports or shared database. Document installation/uninstallation, core compatibility,
permissions, threat boundaries, Codex/Claude/generic MCP configuration, and the
operator gate for creating/pushing the GitHub repository. This task commits only
to the sibling repository; the later core-report task records its verified SHA.

**Test:** yes

**Dependencies:**
- T5.3
- T6.5

**Files:**
- `../../.github/workflows/ci.yml`
- `../../README.md`
- `README.md`
- `SECURITY.md`
- `COMPATIBILITY.md`
- `integration-core.lock`
- `tests/unit/test_ci.py`
- `tests/contract/test_docs.py`
- `tests/integration/test_real_core_journey.py`

**Acceptance:**
- CI defines frozen full-suite, lint, typing, build, dependency audit,
  repository-owned plugin validation, contract-hash, and secret-scan gates; the
  broad gates run only at phase review/CI.
- Cross-repository tests against the real daemon/control layer prove protocol compatibility without private imports, shared databases, social credentials, social HTTP, real sleeps, or live publication.
- `integration-core.lock` records the exact core Git URL, immutable commit SHA,
  package version, and control-contract hash; CI checks out that SHA and verifies
  all four before launch.
- Documentation distinguishes local stdio use from any future hosted/public plugin and states the actual remote/push status truthfully.

**Verify-After:**
- `.venv/bin/uv run --frozen pytest tests/integration/test_real_core_journey.py tests/unit/test_ci.py tests/contract/test_docs.py -q` (focused)
- `.venv/bin/uv run --frozen ruff check tests/integration/test_real_core_journey.py tests/unit/test_ci.py tests/contract/test_docs.py` (scoped_check)
- `.venv/bin/uv run --frozen mypy tests/integration/test_real_core_journey.py` (scoped_check)
- `git diff --exit-code -- uv.lock` (scoped_check)
