# Stage 5 scope-expansion hardening

Date: 2026-09-05

Verdict: **VERIFIED — no open high/medium gaps**

The approved multi-account, scheduler/daemon, and MCP/plugin expansion reopened
planning and hardening before execution continued. Three independent reviewers
examined the full seven-phase plan over successive correction waves:

- system/state: profiles, SQLite ownership, retries, locks, migration,
  scheduling/DST, admission, archive, task dependencies, and focused tests;
- security/execution quality: same-user trust limits, DRAFTS separation,
  operator approval, consequential actions, credentials, atomicity, and
  per-repository commits;
- MCP/platform packaging: authoritative control/tool contracts, daemon
  discovery/startup, self-contained cached runtime, Codex/Claude manifests,
  compatibility pinning, and real-core integration.

## Hardened decisions

- `BUCKET/<bundle-id>/` is the only publishable content shape. DRAFTS are never
  scheduled; core promotes an approved semantic fingerprint through a journaled
  private directory and atomically renames it with `.ready` installed last.
- One instance gate excludes daemon, one-shot, and migration races. Daemon work
  borrows the caller-owned lifetime lease and takes ordered profile locks.
- Schedules have explicit DST, misfire, `no_content`, retry, reconcile, and
  simultaneous-claim semantics with durable idempotency/revision state.
- The checked-in `post-pulsar.control/v1` OpenAPI contract is the authority.
  The plugin vendors its hash and exposes a checked-in exact MCP registry.
- MCP uses an agent bearer capability. Consequential actions need an exact
  intent approved through a TTY-only operator secret; hardened mode separates
  daemon and agent identities, while simple mode openly does not sandbox an
  agent granted arbitrary same-user shell access.
- Auto-start is same-user mode only. Hardened mode relies on an externally
  managed service and returns operator guidance when absent.
- `plugins/post-pulsar` is self-contained and provisions a frozen persistent
  runtime from a cached install with no development virtualenv assumption.
- Plugin integration pins an immutable core revision/version/contract hash and
  drives the real daemon/control layer with fake platform adapters and social
  sockets denied.

Mechanical validation: `0 errors, 0 warnings`. Independent final verdicts:
system/state `VERIFIED`, security/quality `VERIFIED`, MCP/platform `VERIFIED`.
