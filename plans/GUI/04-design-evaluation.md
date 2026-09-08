---
title: POST PULSAR Studio design evaluation
kind: design-evaluation
stage: 2
stage_state: done
created: 2026-09-07
design_ref: 02-design.md
verdict: PASS
confidence: 0.99
---

# POST PULSAR Studio — design evaluation

## Verdict

**PASS — ready for Stage 3 implementation planning.**

This evaluates specification readiness, not a running GUI. No frontend,
gateway, native helper, or new daemon contract has been implemented, and none
of the future test gates in the design has been executed. The design is judged
complete enough that a fresh planning agent should not need to invent material
product, authority, state, packaging, or cross-platform behavior.

## Weighted score

| Dimension | Weight | Score | Weighted result |
| --- | ---: | ---: | ---: |
| Visual specification | 30% | 94 | 28.20 |
| Interaction and state fidelity | 25% | 97 | 24.25 |
| Component and system consistency | 25% | 96 | 24.00 |
| Accessibility and testability | 20% | 97 | 19.40 |
| **Overall** | **100%** |  | **95.85 / 100 (A)** |

## Evidence

### Visual specification — 94/100

The design defines a restrained “quiet orbital control” direction, semantic
tokens, typography, density, layout, motion, responsive rules, dark/light/
forced-color behavior, and a controlled Phosphor icon vocabulary. The six
[visual acceptance plates](03-visual-acceptance-plates.md) cover overview,
content, editing/authorization, schedule pulses, ambiguous recovery, and
degraded system state. Anti-template tests explicitly reject generic dashboard
cards, glass/gradient decoration, fake charts, pill overload, color-only state,
and drag-and-drop semantics that the core does not support.

The exact brand font and final palette remain intentionally gated on license
and contrast proof. That is a bounded visual decision, not an architecture or
interaction gap.

### Interaction and state fidelity — 97/100

The [design](02-design.md) maps the real DRAFTS, QUEUE, RANDOM, REELS, and POSTED
semantics instead of inventing reservations or arbitrary ordering. It separates
executor state, action result, bundle state, and per-platform delivery state;
defines exact legal actions for every resource condition; preserves edits on
conflict; and treats partial or ambiguous publication honestly.

The default authority path is executable at design level: Studio stages an
inert daemon-owned proposal, an independent trusted terminal CLI re-fetches and
renders the canonical consequence, and only that CLI creates/approves/consumes
the operator intent. Proposal claiming, expiry, cancellation, lost responses,
safe tombstones, durable idempotency correlation, and restart lookup are
specified. The agent-enabled branch remains separate and explicit.

### Component and system consistency — 96/100

One typed domain model serves all five product screens. Svelte components use
owned shadcn-svelte/Bits UI primitives rather than treating a component kit as
the product aesthetic. A static SvelteKit application talks only to a narrow
same-origin FastAPI BFF; the daemon remains the sole scheduler, publisher,
filesystem and SQLite authority. MCP remains a peer client of that same daemon
contract, with coordinated core/OpenAPI/plugin compatibility gates.

The design gives browser sessions, daemon sessions, request queues, polling,
inventory scans, preview jobs, caches, and terminal proposals exact bounds and
lifecycle rules. The future Tauri seam is an interface boundary—not a second
product architecture—and the open-source source build retains feature parity
with any paid convenience package.

### Accessibility and testability — 97/100

WCAG 2.2 AA, WAI-ARIA interaction patterns, keyboard behavior, focus return,
screen-reader landmarks, 200% zoom, forced colors, reduced motion, touch
targets, and non-color state communication are release gates. Schedule and
recovery visualizations have explicit accessible equivalents.

The verification strategy covers typed contracts, CSP in the packaged build,
real-browser journeys, two-profile isolation, stale edits, DST, no-content,
ambiguous delivery, authority boundaries, session capacity, hostile localhost
peers, preview resource abuse, proposal contention, tombstone expiry, restart
recovery, and native smoke tests on Linux, Windows, and macOS.

## Hardened review record

Three independent review lanes reached PASS with no remaining high- or
medium-severity findings:

| Review lane | Final verdict | Confidence | Material influence |
| --- | --- | ---: | --- |
| Architecture and security (Astra) | PASS | 99% | Pinned core-only TLS identity, independent operator principal, revocable capability generations, inert proposal rendezvous |
| Product and UX contracts (Sol) | PASS | 99% | Exact default/agent-enabled flows, truthful platform counters and DST example, complete visible states |
| Operations and testing (Sol) | PASS | 99% | Bounded sandbox workers, session reaping/capacity, serialized daemon access, terminal tombstones and restart correlation |

The review rounds closed these earlier blockers:

- browser cookies replaced by one-use launch tickets and memory-only bearers;
- unproven localhost bearer disclosure replaced by pinned hardened TLS plus
  capability-bound sessions with immediate generation revocation;
- agent-principal terminal spawning replaced by an independent trusted CLI;
- undefined authorization handoff replaced by a closed, inert, daemon-owned
  proposal resource;
- terminal-result deletion replaced by bounded safe tombstones and durable
  proposal-to-request correlation;
- cross-platform preview claims replaced by numeric limits, native sandbox
  requirements, self-tests, and fail-closed metadata-only degradation;
- misleading caption and DST examples corrected in the acceptance plates.

## Stage 2 acceptance

- [x] Screens map to current or explicitly required daemon contracts.
- [x] Browser and BFF cannot read SQLite, provider secrets, roots, or raw
      control payloads.
- [x] Bootstrap and daemon authentication have replay, origin, lifetime,
      revocation, hostile-port, and recovery falsifiers.
- [x] Operator approval preserves the hardened principal boundary.
- [x] Mutations define preconditions, idempotency, conflicts, durable tracking,
      terminal states, and unknown-outcome recovery.
- [x] Bucket and scheduling views describe current domain semantics truthfully.
- [x] Preview and inventory work has numeric isolation and latency budgets.
- [x] GUI and MCP compatibility has a coordinated cross-repository contract.
- [x] Visual, responsive, accessibility, empty, unsafe, and degraded states are
      specified and testable.
- [x] Static packaging preserves a future Tauri host without runtime Node.
- [x] Paid packaging is convenience only; public source retains full function.
- [x] Every high risk has a falsifier and no reviewed high/medium design blocker
      remains.

## Remaining gates, not defects

- Refresh and lock the latest compatible dependency graph when implementation
  starts; the observed versions are a dated research baseline, not floating
  requirements.
- Prove the three native preview sandbox helpers and browser smoke matrix before
  enabling derived media previews on each OS.
- Choose the final palette/font only after contrast and license checks.
- Complete a current legal/store review before promising an app-store channel;
  signed direct installers remain the fallback.
- Write the Stage 3 task plan before changing production code or contracts.

These gates are explicit work for planning and release. They do not require a
new product or architecture decision.
