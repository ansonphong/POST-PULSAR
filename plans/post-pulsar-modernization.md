---
stage: 5
repo: POST-PULSAR
stage_state: active
---

# POST PULSAR modernization

Modernize the former PHONG-BOT repository as POST PULSAR, then implement the
operator-approved expansion for multiple account profiles, simple built-in
scheduling, and a separate MCP/plugin repository for agent operation without a
GUI.

## Waterfall record

- Stage 1 — Brainstorm: complete; direction converged.
- Stage 2 — Design: complete; quality gate B+ (8.55/10).
- Stage 3 — Plan: complete; expanded 27-task, seven-phase graph passes the
  mechanical validator with zero errors or warnings.
- Stage 4 — Harden: complete; the original 15-task graph passed 40/40 gaps and
  the expanded graph passed three independent final verification reviews with
  no open high/medium gaps.
- Stage 5 — Execute: active; T1.1-T1.3 implemented and independently repaired.
- Stage 6 — Review: pending.

## Scope contract

In scope: complete POST PULSAR identity migration, official current publishing
APIs, secure configuration, exact content grouping, durable per-platform
delivery state, safe archival, current packaging/dependencies, platform-aware
validation, reliable scripts, tests, CI, documentation, isolated multi-account
profiles, standard content buckets, deterministic scheduling, one local daemon,
an authenticated loopback control protocol, and a separate secretless local
MCP/plugin repository for Codex, Claude Code, and generic MCP hosts.

Out of scope: new social networks, GUI, cloud/hosted MCP service, distributed
workers, analytics, notifications, browser OAuth flows, remote-post deletion,
and live posting during automated verification. Those opportunities belong only
in the final report.

## Definition of done

- The directory and Git remote are POST PULSAR/POST-PULSAR and tracked product
  identity contains no PHONG-BOT remnants.
- Existing X and Instagram text/image/carousel/video intent is implemented
  through supported official APIs, subject to documented platform constraints.
- Secrets are not stored in tracked or non-secret configuration and are never
  logged.
- Partial delivery cannot silently archive a bundle or repost a successful
  target; ambiguous remote outcomes stop for reconciliation.
- Exact parsed bundle membership is used for validation, posting, and archive.
- Fresh installation, lint, type checks, offline tests, build, and dependency
  audit pass on the declared Python baseline.
- README and migration guidance describe prerequisites and limitations honestly.
- Stable profiles and owner-scoped roots isolate `ansonphong` and `360hextile`
  examples without storing credentials; `QUEUE`, `RANDOM`, and `REELS` use
  exact ready markers and deterministic selection.
- A singleton foreground daemon provides bounded timezone-aware scheduling and
  an authenticated versioned loopback API without arbitrary shell/path access.
- `POST-PULSAR-PLUGINS` exposes validated stdio MCP and Codex/Claude packaging;
  sensitive actions require revisions, idempotency, operator opt-in, and
  expiring one-time confirmation.
