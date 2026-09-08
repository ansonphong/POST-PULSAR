---
title: POST PULSAR Studio GUI brainstorm
kind: brainstorm
stage: 1
stage_state: done
created: 2026-09-07
core_ref: 2c775d13171775aa0e8bff00424744f9a6bac0a8
---

# Stage 1 — POST PULSAR Studio brainstorm

Verdict: **CONVERGED**

## Product thesis

POST PULSAR Studio should be a beautiful local publishing cockpit for one
operator managing several social identities. It makes the daemon's durable
state visible and approachable without becoming a second scheduler, a second
publisher, a hosted service, or a generic social-media SaaS.

The product promise is deliberately small:

> See what will publish, decide when it may publish, prepare the exact content,
> and understand anything that needs attention.

The primary operator is Anson today. The product remains useful to any person
who clones or forks the open-source repository and configures their own local
profiles. The interface optimizes for a single trusted operator and multiple
accounts, not multiple human users, teams, RBAC, billing, or remote access.

## Requested shape

- Python remains the backend and the existing daemon remains authoritative.
- Use the current stable Svelte generation for the GUI.
- Use SvelteKit as a statically built client, Tailwind CSS, shadcn-svelte-style
  open-code components, Bits UI primitives, and Phosphor icons.
- Make it minimalist, powerful, responsive, keyboard-friendly, and visually
  distinctive rather than a generic admin template.
- Make multi-profile scheduling, editorial buckets, projected sequencing,
  RANDOM pulses, REELS, publication visibility, and recovery first-class.
- Preserve and improve the MCP integration. Agents and the GUI operate on the
  same daemon state, revisions, confirmation intents, and request history.
- Run as a local web application now. Preserve a clean seam for a future Tauri
  desktop shell on Windows, macOS, and Linux.
- Keep the complete application open source and locally operable.
- A possible later roughly USD $20 desktop edition sells signed, packaged,
  store-delivered convenience, not exclusive features or a subscription.

## Jobs to be done

1. **Orient:** tell at a glance whether POST PULSAR is healthy, paused, busy,
   degraded, or waiting for operator intervention.
2. **Choose identity:** move safely between `ansonphong`, `360hextile`, and
   future profiles without confusing their content or schedules.
3. **Prepare content:** inspect media, edit caption and shared alt text, see
   validation results, and admit an exact draft to a publishing bucket.
4. **Program pulses:** create and understand recurring bucket schedules in the
   profile timezone, including DST and missed-run behavior.
5. **Understand selection:** see the deterministic next candidate for QUEUE and
   REELS and a server-computed projection for RANDOM without pretending a
   future occurrence reserves that bundle.
6. **Publish deliberately:** request publish-now and other consequential work
   through the same fingerprint, revision, idempotency, and approval rules as
   the CLI and MCP server.
7. **Recover safely:** distinguish retryable, blocked, partial, and ambiguous
   outcomes and guide the operator without risking a duplicate post.
8. **Stay out of the terminal most of the time:** routine observation and
   editorial work happens visually; high-authority approval remains explicit.

## Personas and authority

These are modes of one operator, not separate application accounts:

| Mode | Needs | Authority boundary |
| --- | --- | --- |
| Content operator | Drafts, previews, captions, alt text, buckets | Safe editorial mutations with exact fingerprint checks |
| Publishing operator | Consequence review and exact approval | Existing operator secret and real-TTY boundary remain authoritative |
| Recovery operator | Delivery evidence, retry and reconciliation | Never retries an ambiguous final create |
| Observer | Health, calendar, activity, diagnostics | Read-only browser session |
| Deployment administrator | Credentials, services, ACLs, upgrades | Remains outside ordinary GUI workflows |

## Current foundation and hard gaps

The core already provides the difficult safety foundation: one daemon, one
sequential publisher, SQLite delivery state, multi-profile isolation, durable
schedules and requests, deterministic bucket selection, authenticated
loopback control, confirmation intents, recovery, and official platform
adapters.

The current control API is not yet a complete visual-studio API:

| Desired surface | Current truth | Design consequence |
| --- | --- | --- |
| Overview | `/dashboard` has pause flags, due-work Boolean, and profile count | Add bounded server-computed aggregates and freshness |
| Profiles | List exposes ID and revision | Add safe timezone, target identity, and readiness projection |
| Bucket board | Endpoint lists bucket names only | Add bounded paginated inventory and scan issues |
| Bundle preview | Returns hashes/member metadata, not visual media or text | Add exact detail, validation, and derived preview contracts |
| POSTED history | Not exposed by bucket inspection | Add immutable archive inventory without filesystem paths |
| Calendar | Schedule rules exist; occurrences are not exposed | Core computes authoritative occurrence forecasts and DST annotations |
| Activity | Requests lack a unified descending event timeline | Add bounded chronological activity projection |
| Draft edits | The server fingerprints at ingress, not from the user's viewed copy | Require the caller-viewed fingerprint and reject stale writes |
| Imports | JSON control forbids media bytes and arbitrary paths | Keep folder workflow for v1; separately design bounded staging later |
| Generated types | Success envelope data is generic | Define exact per-operation response schemas before UI generation |

The design must not describe the daemon as API-frozen. Its ownership and safety
invariants remain unchanged, while narrow read projections and concurrency-safe
editorial contracts must be added.

## Product direction alternatives

### A. Browser calls the current control API directly

Rejected. It would expose a daemon capability to JavaScript, encourage generic
proxy behavior, lack browser-safe sessions and media contracts, and make it
easy to leak discovery paths or future privileged operations.

### B. Static SvelteKit client plus a separate FastAPI gateway

Selected. FastAPI serves the compiled app and a narrow `/api/ui/v1` browser
contract. It authenticates the browser, validates daemon discovery, translates
only allowlisted view models and operations, and never becomes a publisher.
The daemon survives GUI crashes and browser closure.

### C. Embed FastAPI and the UI inside the publishing daemon

Rejected. This is one process fewer but couples UI dependency failures, slow
browsers, thumbnail work, and traffic spikes to the scheduler and publisher.

### D. Flask gateway

Viable but not selected. The application benefits more from explicit Pydantic
DTOs, generated OpenAPI, ASGI lifespan, bounded streaming, and one shared typed
contract than from Flask's smaller hello-world surface.

### E. Tauri first

Deferred. A native shell improves file pickers, OS notifications, approval,
signing, and distribution, but adds Rust, WebView behavior, sandboxing,
sidecars, service integration, and three-platform release work before the
workflow has been proven in a browser.

### F. Direct SQLite or account-folder access from the gateway

Rejected. It creates a second authority, breaks hardened split-principal
deployments, bypasses durable requests, and makes GUI and MCP behavior diverge.

## Information architecture alternatives

### Calendar-first

Strong for campaigns, weak for intervention and content readiness. It also
invites the false idea that a recurring bucket pulse reserves a specific post.

### Kanban-first

Familiar but semantically wrong. POST PULSAR buckets are filesystem-backed
selection policies, not arbitrary workflow columns. Drag-to-reorder is not an
existing domain operation.

### Intervention-first cockpit

Selected. Overview leads with anything unsafe or blocked. Content exposes the
five bucket views. Schedules uses a weekly pulse calendar. Activity provides
the complete durable narrative. The next selection is visible but labelled a
projection.

Primary navigation:

1. Overview
2. Content
3. Schedules
4. Activity
5. System

The profile selector is global, but `All profiles` is allowed only where the
domain supports it. Content and schedule edits always bind one exact profile
in the URL.

## Selected visual direction

The working name is **POST PULSAR Studio**. The visual idea is **quiet orbital
control**: dense enough to be powerful, calm enough to leave running all day.

- Near-black graphite and warm off-white surfaces, with one crisp pulsar-blue
  accent and reserved amber/red operational colors.
- A signature `Pulse Rail` presents upcoming schedule occurrences across
  profiles as useful temporal information, not decoration.
- Thin orbital rings and radial ticks appear only in identity, empty states,
  and schedule visualization. Avoid decorative gradient fog, glassmorphism,
  giant metrics, and generic AI-dashboard styling.
- Typography is compact and editorial. Bundle captions receive visual space;
  technical hashes and revisions use a restrained mono face.
- Phosphor regular icons are the default. Duotone is reserved for large empty
  states and the product mark. Icons never replace labels for consequential
  actions.
- Motion communicates causality in 120–180 ms. No perpetual animation, no
  bouncing cards, and full `prefers-reduced-motion` behavior.

## GRUG constraints

Every layer must justify itself:

- One authoritative daemon and SQLite store.
- One separate local UI process; one Uvicorn worker.
- One same-origin browser API; no CORS in production.
- Static frontend; Node is build-time only.
- Plain typed fetch wrapper; no global state framework until measured need.
- Polling first; no WebSocket/SSE protocol until a durable event cursor exists.
- No GUI database, background sync engine, service worker, offline mutation
  queue, analytics pipeline, plugin framework, or cloud control plane.
- No optimistic success for durable mutations. HTTP 202 means queued.
- No drag-only interactions and no invented queue semantics.
- Prefer open-code shadcn-svelte components over a giant opaque UI package.

## Open-source and paid-convenience seam

The complete feature set remains buildable and usable from the public source.
No cloud login, telemetry, activation server, subscription, or proprietary
feature flag is required.

A future paid desktop package may provide:

- signed/notarized installers;
- bundled Python/backend and static frontend;
- safe daemon/service installation;
- native file picker, folder reveal, notifications, and tray affordances;
- store-managed or signed updates;
- polished uninstall and migration behavior.

It must not provide exclusive publishing capabilities. A buyer pays for a
trusted build and easy delivery and retains the freedoms of the applicable
open-source licenses.

GPL-3.0 permits charging for distribution, but recipients retain source and
redistribution rights. Apple store terms can impose additional restrictions,
so Mac App Store feasibility is an explicit legal/release gate rather than a
promise. Direct signed macOS distribution is the fallback. Before accepting
material outside contributions, the maintainer should obtain qualified advice
on GPL/store compatibility, contribution terms, and whether a narrowly scoped
distribution exception or dual-licensing policy is needed. This design does
not change the repository license.

## MVP and deferrals

### Designed GUI release

- Secure local launcher/session and daemon handshake.
- Responsive, accessible application shell and profile switching.
- Intervention-first overview and safe system diagnostics.
- DRAFTS, QUEUE, RANDOM, REELS, and POSTED inventory.
- Bounded image thumbnails and video poster/metadata placeholders.
- Exact bundle detail, caption/shared-alt editing, validation, admission, and
  publish-now workflow.
- Weekly recurring bucket-pulse calendar, occurrence forecast, enable/disable,
  and schedule editing.
- Durable activity, pause/resume, request tracking, retry, and reconciliation.
- Real-TTY operator approval broker or exact CLI handoff.
- Coordinated MCP contract compatibility.

### Deliberately later

- Browser multipart media upload and full video streaming.
- One-off post reservations or campaign planner.
- Arbitrary queue reordering and drag-and-drop bucket semantics.
- Per-image alt text unless the content grammar changes.
- OAuth/configuration editing and credential storage.
- Provider analytics, remote post deletion, additional platforms, teams/RBAC,
  remote access, hosted service, mobile client, or offline mode.
- Tauri shell, native approval, store submission, auto-update, and payment.

The existing folder-first workflow remains a feature: prepare content in
`DRAFTS`, then use Studio to inspect and admit it. Folder reveal is deferred
from the web release because it conflicts with the gateway's no-root boundary;
a later Tauri host bridge may offer the fixed profile-ID action after native
authority review. The browser never supplies or receives an arbitrary path.

## Converged decisions

1. FastAPI gateway plus statically built SvelteKit is the target topology.
2. Use Svelte 5, SvelteKit 2, Tailwind CSS 4, shadcn-svelte/Bits UI open-code
   primitives, and `phosphor-svelte`, all exact-locked at implementation time.
3. Optimize for a single local operator with multiple isolated profiles.
4. Preserve current bucket semantics and show projected selection honestly.
5. Treat the daemon and its durable request model as the sole mutation path.
6. Preserve MCP as a peer client and coordinate contract releases.
7. Use polling with bounded refresh policies before streaming protocols.
8. Keep browser, provider, and operator credentials separate.
9. Keep complete functionality open source; paid packaging is convenience.
10. Design a static, same-origin frontend and host bridge that can later run
    inside Tauri without rewriting product views or domain operations.

## Research baseline

Verified 2026-09-07 registry versions are evidence for design compatibility,
not pins for a future implementation:

| Package | Observed stable version |
| --- | ---: |
| `svelte` | 5.57.0 |
| `@sveltejs/kit` | 2.70.3 |
| `@sveltejs/adapter-static` | 3.0.10 |
| `vite` | 8.2.2 |
| `tailwindcss` / `@tailwindcss/vite` | 4.3.3 |
| `shadcn-svelte` | 1.6.1 |
| `bits-ui` | 2.19.0 |
| `phosphor-svelte` | 3.1.0 |
| `fastapi` | 0.141.1 |
| `uvicorn` | 0.52.4 |

Implementation must refresh official releases, browser support, licenses, and
security advisories, then freeze exact compatible versions. “Latest” means the
latest stable compatible graph proven by tests, not blind floating ranges.

Primary references:

- [Svelte](https://svelte.dev/)
- [SvelteKit static adapter](https://svelte.dev/docs/kit/adapter-static)
- [Tailwind CSS installation](https://tailwindcss.com/docs/installation)
- [shadcn-svelte](https://www.shadcn-svelte.com/docs)
- [Phosphor Icons](https://phosphoricons.com/)
- [FastAPI features](https://fastapi.tiangolo.com/features/)
- [Tauri 2 overview](https://v2.tauri.app/start/)
- [GNU GPL FAQ](https://www.gnu.org/licenses/gpl-faq.html)
- [Apple Standard EULA](https://www.apple.com/legal/internet-services/itunes/dev/stdeula/)
- [WCAG 2.2](https://www.w3.org/TR/WCAG22/)

## Stage 1 exit

Direction is converged. The design must resolve the remaining contracts rather
than defer them: browser bootstrap, browser-to-gateway authentication,
gateway/daemon authority, typed view models, exact stale-edit behavior,
bounded preview work, truthful schedule projection, operator approval,
cross-repository MCP compatibility, lifecycle, packaging, accessibility, and
the future Tauri/open-source distribution seam.
