---
stage: 3
repo: POST-PULSAR
stage_state: active
---

# POST PULSAR modernization

Modernize the former PHONG-BOT repository without expanding its product scope.
The finished application remains a one-shot local content publisher for X and
Instagram, suitable for manual use or an external OS scheduler.

## Waterfall record

- Stage 1 — Brainstorm: complete; direction converged.
- Stage 2 — Design: complete; quality gate B+ (8.55/10).
- Stage 3 — Plan: complete; 15-task execution graph validated with zero
  mechanical warnings and no execution blockers.
- Stage 4 — Harden: pending.
- Stage 5 — Execute: pending.
- Stage 6 — Review: pending.

## Scope contract

In scope: complete POST PULSAR identity migration, official current publishing
APIs, secure configuration, exact content grouping, durable per-platform
delivery state, safe archival, current packaging/dependencies, platform-aware
validation, reliable scripts, tests, CI, and documentation.

Out of scope: new social networks, new post types or controls, an internal
scheduler, UI, cloud hosting, analytics, notifications, and live posting during
automated verification. Those opportunities belong only in the final report.

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
