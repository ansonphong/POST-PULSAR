---
title: POST PULSAR Studio local GUI design
kind: design
stage: 2
stage_state: done
created: 2026-09-07
core_ref: 2c775d13171775aa0e8bff00424744f9a6bac0a8
design_version: 2
---

# POST PULSAR Studio — local GUI design

## 1. Decision summary

Build POST PULSAR Studio as a statically compiled SvelteKit application served
by a separate local FastAPI gateway. The existing POST PULSAR daemon remains
the only scheduler, durable command processor, publishing engine, filesystem
authority, and SQLite authority.

The first release is a local web app for one operator with multiple profiles.
It is optimized for Windows, macOS, and Linux desktop browsers and deliberately
prepared for a future Tauri 2 shell. The full application remains open source.
A future paid desktop build may charge approximately USD $20 for signed,
packaged, store-delivered convenience without withholding functionality.

Routine observation, editing, and forecasting are visual. Publication-enabling
actions still receive exact approval in a trusted terminal; an enabled schedule
then runs unattended under that approved rule. F5 retains the browser session
and resumes observation of pending requests.

```text
Browser tab
  SvelteKit static SPA
        │ exact-origin /api/ui/v1 + ephemeral browser bearer
        ▼
POST PULSAR Studio gateway
  FastAPI · one Uvicorn worker · foreground lifecycle
        │ allowlisted authenticated loopback operations
        ▼
POST PULSAR daemon
  control contract · schedules · durable requests · SQLite · content roots
        │
        ├──────── official X / Instagram APIs
        │
        └──────── exact archive and recovery

Codex / Claude Code / generic MCP client
        │
        ▼
POST-PULSAR-PLUGINS MCP server ─────► the same daemon/control contract
```

Closing Studio never stops scheduled publishing. Restarting Studio never
changes daemon state. Browser traffic cannot call platform adapters directly.

## 2. Goals

- Provide a calm, high-signal operational overview across all configured
  profiles.
- Make `DRAFTS`, `QUEUE`, `RANDOM`, `REELS`, and `POSTED` understandable and
  visually useful without changing their safety semantics.
- Support caption/shared-alt editing, draft admission, publish-now, recurring
  bucket schedules, pause/resume, retry, and reconciliation through durable
  daemon operations.
- Show exactly which profile, remote identities, fingerprint, revision,
  bucket, targets, and consequence a mutation binds.
- Present current and projected schedule behavior using core timezone/DST
  rules.
- Keep GUI and MCP behavior consistent and protect both with coordinated
  contract tests.
- Work naturally in current desktop browsers on Windows, macOS, and Linux.
- Preserve a static-frontend and host-bridge seam suitable for Tauri later.
- Meet WCAG 2.2 AA and remain usable without drag, hover, animation, or color.
- Remain self-hosted, forkable, inspectable, telemetry-free, and functional
  without a vendor account.

## 3. Non-goals

- A hosted service, remote/LAN access, cloud synchronization, or mobile app.
- Multiple human users, RBAC, teams, approval delegation, or billing.
- A second scheduler, direct GUI database, or direct GUI platform client.
- Analytics, engagement reporting, provider inboxes, or remote-post deletion.
- New social networks or publication types.
- One-off campaign reservations. Current schedules are recurring bucket pulses.
- Arbitrary queue ordering, drag-to-reorder, or free-form Kanban semantics.
- Browser credential/OAuth provisioning.
- Offline mutations, service workers, or background browser synchronization.
- Browser media uploads or full video streaming in the first release.
- Tauri implementation, app-store submission, licensing changes, or payment in
  this phase.

## 4. System invariants

1. The daemon is the sole durable state and publishing authority.
2. The gateway never opens canonical SQLite state, provider credentials,
   account roots, or platform adapters.
3. Every domain mutation becomes the same durable request or confirmation flow
   available to other clients. Ephemeral session, preview-job, and inert-proposal
   operations do not mutate publishing state or grant operator authority.
4. HTTP `202 Accepted` means queued, never completed.
5. A timed-out mutation has an unknown local transport outcome; the client
   resolves the original idempotency key/request instead of creating another.
6. A stale profile revision, schedule revision, bundle revision, fingerprint,
   daemon incarnation, or confirmation intent blocks mutation.
7. Ambiguous final provider outcomes never offer automatic retry.
8. A schedule targets a bucket. A calendar occurrence does not reserve a
   particular bundle.
9. RANDOM remains deterministic and recoverable; a displayed next item is a
   projection until admission.
10. GUI closure, browser reload, gateway failure, and gateway upgrade cannot
    stop the daemon or invalidate durable work.
11. MCP remains independently usable if the GUI is absent.
12. No daemon/operator/provider secret, absolute filesystem path, raw provider
    response, or discovery record is sent to browser JavaScript. The dedicated
    ephemeral browser credential is the explicitly bounded exception in §7.2;
    safe member basenames and public identities are presentation data.

## 5. Deployment topology and lifecycle

### 5.1 Processes

`post-pulsar daemon foreground` remains the independently supervised long-lived
process. `post-pulsar web` is a separate foreground Studio process. It runs one
Uvicorn worker because its state includes ephemeral browser sessions, one
validated daemon incarnation, and ordered upstream writes.

Studio startup:

1. Acquire a Studio-only installation lock. Never acquire or replace the
   daemon lease. If a validated Studio incarnation already owns the lock, a
   second `post-pulsar web --open` delegates only a fresh-launch request over
   the owner-authenticated launcher channel described below and exits.
2. Bind a pre-created port-0 socket on canonical `127.0.0.1`; retain the exact
   socket to eliminate port-selection races.
3. Load compiled assets through `importlib.resources`, never from the working
   directory.
4. Validate the daemon bootstrap/endpoint record, PID creation identity,
   installation ID, startup nonce, API major, supported operations, and
   authenticated capabilities.
5. If the daemon is absent in simple mode, optionally request startup through
   one fixed, already-installed service identifier and wait at most 30 seconds.
6. In hardened/manual mode, do not cross identities or spawn Python. Render
   exact operator instructions.
7. Publish Studio readiness, mint a one-time launch ticket, and open the
   browser only after the server can complete the session exchange.

Studio shutdown revokes its browser sessions, abandons its polling and preview
receipts, closes its HTTP client, and removes only its own endpoint record. It
does not own or cancel daemon-side derivative jobs; those finish within their
hard bound or expire. It does not send daemon shutdown. Browser closure is not
treated as a lifecycle event.

Studio writes an owner-only launcher record and a separate 256-bit launcher
capability beside its lock. The record binds installation ID, Studio PID and
process creation identity, exact numeric endpoint, and Studio incarnation.
The launcher capability is newly generated for every Studio incarnation,
stored only owner-readable, and invalidated/deleted with all outstanding launch
tickets on shutdown, stale-record recovery, or explicit launcher rotation.
Only another local CLI process with file access can call the private
`/internal/launch-ticket` operation. That operation is not part of the browser
OpenAPI, rejects the browser bearer, never returns its launcher capability, and
only creates a bounded one-use launch artifact. A second `web --open` validates
the record and process identity, uses the launcher capability once, requests a
new artifact, opens it, and exits. Startup removes stale records/artifacts only
after proving the recorded process is gone.

Ctrl+C stops Studio only. The daemon's systemd-user, launchd, Windows Task
Scheduler, or administrator-provisioned service lifecycle remains independent.

### 5.2 Daemon startup convenience

Commands are closed and rendered from validated installed service metadata:

| Service mode | Studio Start control |
| --- | --- |
| `systemd-user` | Simple mode only: fixed installed `systemctl --user start post-pulsar.service` |
| `launchd-agent` | Simple mode only: `launchctl kickstart` with the exact installed user-domain label |
| `windows-task` | Simple mode only: `schtasks.exe /run` with the exact installed task name |
| `windows-service` | Instructions only; no SCM/elevation call |
| `manual` | Instructions only |
| Any service mode under hardened deployment | Instructions only; no Start button or start request |

Unknown/unvalidated service metadata is instructions-only, never a guessed
command. System always explains: `Closing this tab does not stop scheduled
publishing.`

Studio may start but never install, enable, stop, remove, or rewrite a service.
Contention uses one bounded startup lock and rechecks authenticated discovery.
If browser launch fails and a trusted TTY is attached, Studio prints the
non-secret base URL plus a short-lived one-time human code directly to that TTY
(not redirected stdout/stderr); the operator opens the URL and enters the code.
The code has the same one-use/60-second bounds as a launch ticket. Without a
TTY, Studio does not mint or print a secret and exits with safe instructions to
rerun `post-pulsar web --open` interactively. WSL uses this explicit fallback.

### 5.3 Development topology

Vite may proxy `/api/ui/v1` to FastAPI only in development, with an explicit
allowlist for its exact origin. Production is always one origin and has no CORS
middleware. A development supervisor may run Vite, FastAPI, and a fake daemon;
it is never packaged as the production launcher.

## 6. Technology and repository structure

### 6.1 Selected stack

| Concern | Decision | Reason |
| --- | --- | --- |
| Browser framework | Svelte 5 + SvelteKit 2, TypeScript strict | Concise reactive views, routing/layout conventions, static output |
| Static build | `@sveltejs/adapter-static` | No Node production runtime; future Tauri-compatible assets |
| Build | Vite | Supported Svelte path and Tailwind first-party integration |
| Styling | Tailwind CSS 4 with CSS-first tokens | Fast composition with a small, inspectable visual vocabulary |
| Components | shadcn-svelte open code over Bits UI | Accessible primitives with locally owned component source |
| Icons | `phosphor-svelte` | Requested visual family; tree-shaken component imports |
| Python web | FastAPI + one Uvicorn worker | Exact DTOs/OpenAPI, lifespan, bounded local streaming |
| Daemon client | One lifespan-owned `httpx.AsyncClient` | Existing ecosystem, strict transport controls |
| Browser state | Route state + local Svelte state + typed fetch cache | Avoid a global state framework until demonstrated need |
| Refresh | Bounded polling | No durable event cursor exists; easier recovery semantics |
| Tests | pytest, Vitest, Testing Library, Playwright, axe | Contract through real-browser coverage |

At execution time, refresh stable versions and license/security metadata, then
freeze compatible exact versions. The 2026-09-07 research baseline appears in
the brainstorm document. Vite 8 requires Node `^20.19` or `>=22.12`; select one
maintained Node LTS line and pin the package manager through `packageManager`.

FastAPI/Uvicorn belong to a `web` optional dependency group so headless daemon
installations do not inherit browser-server dependencies.

Normal source setup remains headless unless the operator passes a documented
`--with-web` option to the POSIX or Windows setup script; that option executes
the frozen equivalent of `uv sync --extra web`. Paid desktop bundles always
include the web extra. FastAPI/Uvicorn imports are lazy, so every headless CLI
and daemon command works without them. `post-pulsar web` without the extra exits
with code 2 and the exact local installation commands; it never partially
starts.

### 6.2 Repository layout

```text
POST-PULSAR/
├── api/
│   ├── control-v1.openapi.json
│   └── ui-v1.openapi.json
├── src/post_pulsar/
│   ├── web/
│   │   ├── app.py
│   │   ├── auth.py
│   │   ├── daemon_client.py
│   │   ├── dto.py
│   │   ├── lifecycle.py
│   │   ├── redaction.py
│   │   └── routes/
│   └── web_dist/                 # deterministic release assets
├── web/
│   ├── src/
│   │   ├── lib/api/              # generated types + handwritten client
│   │   ├── lib/components/ui/    # owned shadcn-svelte component source
│   │   ├── lib/components/studio/
│   │   ├── lib/host/
│   │   └── routes/
│   ├── package.json
│   ├── pnpm-lock.yaml
│   ├── svelte.config.js
│   └── vite.config.ts
└── tests/
    ├── web/
    └── integration/
```

Release assets are generated, deterministic, and included in wheel and sdist.
CI rebuilds them from the exact JavaScript lock and fails if committed output
differs. The source package remains buildable with documented Node tooling;
installed runtime operation needs no Node.

Compiled `web_dist` is tracked release input and included recursively through
setuptools package-data plus the sdist manifest. Building a wheel from the sdist
must not require Node. CI proves both isolated headless installation and
isolated `[web]` installation, loads nested hashed assets through
`importlib.resources`, and serves a deep link from each built artifact.

No SvelteKit server routes, SSR dependency, remote font, CDN, analytics script,
or service worker is allowed. Client routing uses a static fallback and is
tested on deep links served by FastAPI.

## 7. Trust boundaries and browser security

### 7.1 Threat model

Protect against:

- a malicious webpage attempting CSRF against loopback;
- another local HTTP service on a different port;
- DNS rebinding and hostile `Host`/`Origin` values;
- XSS through captions, filenames, provider errors, or media metadata;
- stale daemon endpoint records and malicious port reuse;
- accidental capability, path, or operator-secret disclosure;
- oversized/slow requests and malformed media;
- browser history, referrer, process-list, or log leakage;
- simple-mode accidents and hardened-mode cross-principal escalation.

The project does not claim protection from a malicious process already running
as the same OS principal in simple mode; that principal can read the same local
files. Hardened mode preserves distinct service/agent principals and ACLs.

Studio stores its own lock, incarnation/launcher record, owner-only launch
artifacts, and web log beneath platformdirs-derived directories for the current
Studio principal, keyed by installation ID. Disposable preview metadata/cache
lives in the matching cache location; server-side browser-session and ticket
hashes remain in memory. These locations are validated not to overlap daemon
state, content roots, discovery, operator storage, logs, or each other. Creation
and cleanup reuse symlink/hardlink-safe owner-only atomic file policy. Hardened
mode never assumes write access to daemon-owned state.

### 7.2 Session bootstrap

Ordinary cookies are not the authentication primitive. Cookies are shared
across ports, while browser origins include the port; RFC 6265 documents this
weak confidentiality boundary.

1. Studio generates a 256-bit one-use ticket with a 60-second expiry.
2. It writes an owner-only temporary launch HTML file containing the ticket in
   a fragment redirect. Only the temporary file path appears in browser process
   arguments; the ticket does not.
3. The static Svelte bootstrap reads the fragment, immediately calls
   `history.replaceState`, and exchanges it at the exact Studio origin using a
   JSON request and custom header.
4. Studio consumes the ticket once and returns a separate random browser
   bearer that has no meaning to the daemon. A session has a 30-minute idle
   expiry and eight-hour absolute expiry. Authenticated traffic slides the idle
   deadline but never the absolute deadline; the bearer stays unchanged until
   expiry, Lock, or gateway restart. There is no session-rotate operation.
5. The Svelte client holds the bearer in same-origin, tab-scoped
   `sessionStorage` and sends it in `Authorization` for every protected read,
   mutation, and preview fetch. This survives F5; another loopback port has a
   different origin and cannot read it. Server expiry/revocation remains final.
6. The launch file is removed after successful exchange or expiry. The
   browser's storage normally disappears on tab closure, but restored browser
   sessions may restore it. Security relies on server expiry/revocation, not a
   guarantee of browser deletion. Server state disappears on gateway restart,
   expiry, or explicit lock.

At most four unconsumed launch tickets and eight browser sessions exist per
Studio incarnation. Minting a fifth ticket revokes the oldest unconsumed
ticket. A ninth session exchange is rejected with `session_capacity` and
instructions to lock another tab or wait for expiry; Studio never silently
evicts an active session. At most one ticket is emitted by a single CLI
gesture. Ticket/session records contain only hashes. Expired records are reaped
before every mint, exchange, and authenticated request and by a
bounded 60-second maintenance task. Tab closure is not relied on to notify the
server; its server record expires normally. Explicit Lock revokes the session
and clears its tab storage; gateway shutdown revokes every server session.

No browser bearer is placed in a cookie, URL, query string, localStorage,
IndexedDB, log, error report, or static asset. Restore the session after refresh
only by presenting its bearer to the server; stale storage is not proof of
access. On expiry/restart, clear it and offer the exact `post-pulsar web --open`
or trusted-terminal one-time-code path. If sessionStorage is blocked, announce
memory-only fallback and its refresh limitation. Open external links with
`noopener,noreferrer`; never copy a session into a new window intentionally.
Same-origin XSS can access either memory or sessionStorage, so strict CSP and
text-only rendering remain the relevant boundary.

Static shell assets may be fetched without a session; all product data and
preview bytes require the browser bearer. No route returns a credential that
can mint another session.

### 7.3 HTTP policy

- Bind only numeric `127.0.0.1`; no configurable wildcard/LAN host.
- Require the exact numeric `Host` including the active port.
- Require exact `Origin` and Fetch Metadata on mutations and session exchange;
  validate them on protected reads when supplied.
- No production CORS headers and no wildcard origin.
- Require correct JSON or bounded multipart content types.
- Reject duplicate security, length, host, origin, and authorization headers.
- Apply explicit header, body, response, idle, upstream, and total timeouts.
- Externalize the Svelte bootstrap. Generate build-time hashes for any
  unavoidable generated script block. `script-src` never receives
  `unsafe-inline` or `unsafe-eval`; `connect-src 'self'`, `img-src 'self'
  blob:`, `frame-ancestors 'none'`, and `object-src 'none'` are mandatory.
- Prefer CSS classes/variables for component positioning. If audited Bits UI
  primitives require runtime `style` attributes, scope `style-src-attr` to the
  minimum browser-compatible policy while keeping scripts strict. The compiled
  production assets and every shipped primitive must pass CSP browser tests;
  implementation may not silently relax the header.
- Set `Referrer-Policy: no-referrer`, `X-Content-Type-Options: nosniff`, a
  restrictive Permissions Policy, frame denial, and `Cache-Control: no-store`
  for all protected responses.
- Render all operator/provider text as text nodes. No raw HTML or arbitrary
  remote media URL enters the DOM.
- Disable FastAPI production docs, Uvicorn access logs, and server banners.

### 7.4 Daemon client

The gateway uses one `httpx.AsyncClient` with `trust_env=False`, redirects
disabled, strict loopback URL construction, response-size bounds, and explicit
timeouts. Because the current daemon listener is single-threaded, every daemon
call passes through one bounded priority dispatcher with exactly one in-flight
request and queue depth 64. Writes, health, active intent/request observation,
foreground reads, background refresh, and cached preview fetch are descending
priority classes. Identical reads coalesce across browser sessions. Expired
background work returns a stale/degraded DTO rather than growing the queue. It
revalidates discovery before starting the following daemon-authentication
handshake.

Use standard pinned TLS and the existing per-request capability bearer, with
no custom HMAC challenge, derived daemon-session bearer, or session-generation
cache. The client first validates the protected discovery record and TLS peer
using §7.5, then sends `Authorization: Bearer <capability>`. The core re-reads
and compares the current capability on every request, including keepalive
connections. Rotation/revocation therefore has no derived session to invalidate.
Proposal records bind the current capability digest internally and are reaped
on a capability change; that digest is never a credential or a browser field.

Both simple and hardened new-match releases use this one transport. Simple
mode provisions its identity automatically as the owner, without claiming
protection from a compromised same-user process. Hardened mode uses the same
wire protocol with pre-provisioned core/agent ACL separation. Keeping TLS in
simple mode avoids sending credentials to a recycled port and avoids shipping
two authentication implementations. The previously considered HMAC protocol
is removed, not deferred for a planner to invent. Old matching HTTP core/plugin
pairs remain supported as old installations; a new Studio never falls back to
their transport.

Safe GETs may retry once only after renewed discovery and TLS validation. Writes are never blindly
retried. A daemon incarnation, installation, version, operation, or capability
mismatch immediately disables mutations and clears dependent cached data.

The BFF maps explicit Pydantic DTOs. It never relays daemon dictionaries,
headers, error bodies, paths, nonces, environment variable names, platform
responses, or future operations generically.

### 7.5 Pinned TLS control contract

This is a required **new** core/plugin contract, not a claim about today's
`control-v1.openapi.json`. Preserve existing application operation semantics;
update discovery, server transport, CLI, MCP client, and contract pins together.

| Item | Closed design choice |
| --- | --- |
| Transport | TLS 1.3 only, HTTP/1.1, literal loopback IP, no redirects/proxy/system trust, no TLS early data |
| Key/certificate | Per-installation ECDSA P-256 key; self-signed X.509 v3 leaf; SHA-256 signature; critical CA=false and digitalSignature; EKU serverAuth; IP SANs `127.0.0.1` and `::1`; random 128-bit positive serial |
| Certificate lifetime | Starts five minutes before creation, ends 365 days after; System warns at 30 days remaining. Expiry fails closed and gives the fixed renewal command. |
| Discovery addition | Closed `control_transport` object: `scheme: "https"`, `auth: "capability-bearer-v1"`, `certificate_pem` (one certificate, ≤8 KiB), `certificate_sha256` (64 lower-case hex SHA-256 of complete DER), `identity_generation` (32 lower-case random hex) |
| Existing identity | Installation ID, PID creation identity, startup nonce, exact endpoint and API major stay required; no file path is added to public transport metadata |
| Trust setup | Validate file ACL/identity and PEM fingerprint locally; use a fresh `ssl.PROTOCOL_TLS_CLIENT` context trusting only that certificate, hostname/IP verification and `CERT_REQUIRED`; compare the peer's complete DER SHA-256 immediately after TLS negotiation and before any HTTP bytes; never load system roots, certifi, environment overrides, or TLS key logging |
| Authentication | Existing `Authorization: Bearer`; operator routes additionally require existing operator-secret authentication. Only after successful TLS verification may HTTP headers/body be sent. |
| Identity change | Close all pooled connections; discard cached authority and outstanding proposals; validate discovery and connect afresh. No opportunistic downgrade or trust-on-first-use. |

The certificate is an exact leaf pin, replacing the earlier SPKI-pin wording.
Python `ssl` and HTTPX's explicit SSL-context support provide transport and
certificate validation; use a maintained certificate library for provisioning,
never handwritten TLS or certificate parsing. Its exact compatible dependency
belongs in the refreshed lock. The browser never sees this certificate,
capability, key, or control endpoint; its Studio origin remains loopback HTTP.

`post-pulsar control tls initialize` is an owner/core-only local command with no
network input. Simple first setup runs it automatically. Hardened setup runs it
explicitly after ACL provisioning. `post-pulsar control tls rotate` requires
the daemon stopped at a verified safe boundary. It writes/fsyncs a fresh
immutable key/certificate generation in core storage, then atomically replaces
one core-owned active-generation manifest. Startup derives the public
certificate and fingerprint from that committed generation before publishing
the endpoint. A crash before the manifest replacement uses the old generation;
after replacement it uses the new one. Retain at most the previous generation
for explicit stopped-daemon rollback, never simultaneous old/new trust. Neither
command accepts a browser-supplied path or adjusts hardened ACLs.

Tests use fixed non-production certificate fixtures: exact pin success, wrong
certificate even with the same public key, wrong installation/IP, expired/not-
yet-valid certificate, TLS 1.2, changed discovery while pooled, rotation crash
before/after manifest switch, capability revocation on a live connection, and
zero HTTP credential bytes sent to a rejected peer. These are required future
contract tests, not pre-existing vectors or executed evidence.

Bound socket work in the daemon, not only the BFF: TLS handshake/header/body
deadlines of 2 seconds each, 16 KiB headers, 64 KiB request bodies, 2 MiB JSON
responses, and 2-second response writes. At most eight socket-reader slots
feed a bounded queue of 32 complete requests; timeout/overflow closes or returns
`busy`. Parse and verify complete requests before entering the single domain
dispatcher. No slow socket holds the publishing lock or an open database
transaction. The daemon retains one serialized mutation authority and one
publisher; the socket readers add no scheduler or database writer.

## 8. Authority and approval model

The browser session proves access to this Studio instance. It is neither the
daemon agent capability nor operator authority.

Studio runs with the existing agent-side discovery/capability access. In
hardened deployment it does not run as the daemon/core principal and cannot
open private state. `allow_agent_publish=false` is represented honestly and is
never changed by the GUI.

Consequential flows:

```text
allow_agent_publish=true
  operator gesture → Studio creates exact agent intent
  → independent CLI refetches, renders, and approves
  → Studio refetches bindings → operator chooses Continue
  → Studio consumes → durable request → observe terminal result

allow_agent_publish=false (default)
  operator gesture → Studio stages inert core-owned proposal
  → independent CLI claims, refetches, renders, and prompts
  → CLI creates + approves + consumes exact operator intent
  → durable request → Studio observes proposal/activity result
```

The operator secret is never typed into Svelte, submitted to the BFF, stored in
browser state, or retained by the web process. Studio may show and copy only a
fixed command containing an opaque proposal ID, such as `post-pulsar authorize
pprop_7K…`; no action fields, secret, or shell syntax enter that command.

The current CLI can approve an existing intent but cannot create an operator-
origin intent when `allow_agent_publish=false`. The GUI release therefore adds
a daemon-owned, explicitly non-authorizing authorization-proposal rendezvous.
Studio submits the exact proposal through its authenticated agent session; this
operation can stage data but cannot create/approve an intent, consume one, or
enqueue work. The daemon validates and stores it in core-owned memory and
returns an opaque 128-bit base32 proposal ID restricted to the literal grammar
`pprop_[A-Z2-7]{26}`. A ninth active proposal is rejected with
`proposal_capacity`; nothing is silently evicted.

Proposals are single-use and expire after five minutes. Their action-bearing
bodies are deleted on terminal completion, cancellation, expiry, daemon
restart, relevant resource/profile change, or originating agent-capability
rotation/revocation. A bounded reaper runs before proposal
operations and every 60 seconds. The closed record is no larger than 32 KiB and
contains:

- schema version, opaque 128-bit proposal ID, creation/expiry, and the stable
  idempotency binding `authorization-proposal:<proposal-id>`;
- exact action enum plus closed action arguments;
- profile ID/revision, resource kind/ID/revision, bucket and immutable schedule
  ID where applicable;
- full content fingerprint, ordered target account/platform IDs, and any
  delivery/request preconditions;
- core-stamped `installation_id` and `startup_nonce`, checked against validated
  discovery by the trusted CLI, and a SHA-256 hash of the complete canonical
  proposal fields. The BFF's opaque browser `daemon_session` is never part of
  the canonical proposal or accepted as core identity.

Canonical hashing is fixed: exclude only the `canonical_sha256` field; encode
the closed record with sorted object keys, ASCII-escaped JSON strings, no
insignificant whitespace, literal JSON booleans/null, integers only (no floats
or NaN), explicit null for absent optional fields, and arrays in domain order.
Do not normalize Unicode content. Hash UTF-8 bytes of the literal prefix
`post-pulsar.authorization-proposal/v1` followed by one NUL byte and that JSON.
The core and trusted Python CLI share this pure encoding function; browser
JSON serialization is not an authority input. Contract fixtures must cover
non-ASCII text, nulls, reordered keys, changed target order, and one-field drift.

The browser receives only proposal ID/state/expiry and the fixed command; it
never receives the record body. The proposal ID is non-secret and doubles as
the safe durable-correlation input, but the browser cannot submit the resulting
privileged request. In a separate real terminal,
the operator launches the installed CLI directly. In hardened mode that CLI
runs as the trusted operator/core principal, not as a child of the agent-side
Studio. It validates the exact proposal-ID grammar, connects to the daemon over
the pinned server-authenticated channel below, and retrieves the core-owned
record by ID. The ID authorizes only reading/declining that inert proposal.

The CLI rejects unknown/duplicate fields, oversize input, wrong schema, expiry,
or canonical hash mismatch. It independently re-fetches every named resource,
recomputes the canonical action binding and consequence, and compares profile/
resource revisions, target set, fingerprint, arguments, and policy. Any
mismatch atomically marks the proposal stale and performs no mutation. Only
after rendering that recomputed consequence in the trusted terminal does it
read the operator secret with `getpass`, create, approve, and consume the
operator-origin intent. It reuses the proposal idempotency key exactly once.
The daemon atomically records safe terminal state plus opaque intent/request IDs
on the proposal; Studio may poll only that safe projection and must re-fetch
authoritative activity before claiming success. A lost CLI response is an
unknown outcome resolved by the same proposal ID/idempotency binding, never by
replay.

At intent consumption/enqueue, the daemon commits the existing durable
idempotency record under `authorization-proposal:<proposal-id>` in the same
transaction as the durable request. It then replaces the proposal body with a
safe tombstone containing only proposal ID, terminal state, opaque intent/
request IDs, and terminal time. Tombstones are at most 1 KiB, retained for 24
hours, capped at the 128 newest entries, and excluded from the eight-active-
proposal limit. The originating Studio incarnation may acknowledge and delete
a tombstone sooner. After restart or tombstone eviction, proposal status
derives the same safe correlation from the durable idempotency/request lookup;
an unconsumed in-memory proposal becomes `expired`. Thus recovery never
requires persisting or reconstructing the action-bearing proposal body.

Proposal state is `staged → claimed → authorized | declined | stale | expired |
unknown`. Claim is an atomic daemon operation, and only one proposal may be
`claimed` installation-wide; concurrent terminals receive
`authorization_busy`. The claim inherits the proposal's five-minute absolute
deadline and cannot be renewed. After terminal review and immediately before
intent creation, the daemon rechecks expiry, daemon incarnation, current
capability digest, policy, profile/resource revisions, targets, fingerprint, and
exact arguments under the same serialization boundary. Terminal Ctrl+C
declines an unmutated claim. Browser Cancel may delete only `staged`; once
claimed, Cancel, Lock, tab close, or Studio loss merely abandons observation
and cannot claim that terminal work stopped. Claim expiry releases the mutex;
if transport is lost after mutation may have begun, state becomes `unknown` and
same-key activity lookup is the only recovery.

When `allow_agent_publish=true`, Studio may create the exact agent-origin intent
and the trusted CLI may review/approve that existing intent before Studio
explicitly consumes it. When false, the new `authorize` command performs the
whole trusted operator path. Existing `confirmations approve` must also be
hardened to fetch and render the canonical intent before reading/submitting the
secret and to validate the returned exact identity before claiming success.

This separation is enforced in the daemon, not just in Studio's route allowlist.
Every consume entry point checks the stored immutable intent origin before
consumption or mutation replay: operator-origin intents require operator
authentication over the pinned channel; a capability/session alone receives
`operator_required`, even with the correct ID, binding, and idempotency key.
The existing generic consume route must enforce this check too. Agent-origin
consumption remains available only under its existing enabled policy and exact
approved binding. Safe request-status lookup grants no consumption authority.
Tests include a second agent racing the trusted CLI between approval and
consumption, and replaying a consumed operator intent under an agent session.

The new authorize CLI and existing approval command use the same §7.5 pinned
TLS transport as Studio and MCP; the capability proves client access, not server
identity. In hardened mode discovery is readable but not writable by agent/
Studio. The trusted approval CLI runs as the existing core principal and has
that principal's file rights. This does not invent a third operator identity
or claim process isolation from trusted core tools. No operator-authenticated
request is sent until certificate, installation, incarnation, and ACL checks
pass. Simple mode uses the same transport with its documented same-user limit.

A future Tauri native approval helper requires a separate authority design and
OS user-presence review.

Simple editorial operations use the narrow direct durable-request path only
when the daemon already permits them. A confirmation dialog explains a
consequence but never masquerades as cryptographic/operator approval.

Global pause is labelled installation-wide. Resume approval summarizes every
profile with due work that could be released, not only the selected profile.

## 9. Browser API contract

### 9.1 Contract shape

The browser uses only `/api/ui/v1`. Every operation has an exact Pydantic
request/response model and exact OpenAPI schema; `dict[str, Any]` success data
and generic passthrough envelopes are forbidden.

Successful JSON responses use:

```json
{
  "schema": "post-pulsar.ui/v1",
  "request_id": "ui_opaque_id",
  "observed_at": "2026-09-07T18:00:00Z",
  "daemon_session": "opaque_non_secret_epoch",
  "data": {}
}
```

`daemon_session` is a gateway-generated opaque epoch. It lets the UI discard
out-of-order data after a daemon restart without exposing the daemon nonce or
process identity.

Errors use a closed vocabulary:

```json
{
  "schema": "post-pulsar.ui/v1",
  "request_id": "ui_opaque_id",
  "error": {
    "code": "stale_resource",
    "message": "This draft changed after it was opened.",
    "retryable": false,
    "refetch_required": true,
    "field_errors": []
  }
}
```

Messages are safe UI copy selected by the gateway, not raw exception or
provider text. Unknown server codes render a generic failure plus request ID.

### 9.2 Candidate browser operations

Names are design-level and may be normalized during planning, but their
authority and data requirements are fixed.

| Method and route | Purpose | Authority |
| --- | --- | --- |
| `POST /session/exchange` | Consume one launch ticket | One-use bootstrap |
| `POST /session/lock` | Revoke current browser session | Browser bearer |
| `GET /health` | Studio/daemon compatibility projection | Browser bearer |
| `GET /overview` | Bounded intervention-first aggregate | Browser bearer |
| `GET /profiles` | Safe profile descriptors | Browser bearer |
| `GET /profiles/{id}/content` | Paginated bucket inventory | Browser bearer |
| `GET /profiles/{id}/content/{bucket}/{bundle}` | Exact bundle detail | Browser bearer |
| `GET .../preview/{member}` | Bounded derived preview blob | Browser bearer |
| `PATCH .../caption` / `.../alt` | Fingerprint-bound editorial request | Browser bearer + revision + idempotency |
| `POST .../admit` | Default: stage inert admission proposal; enabled agent policy: exact agent-intent flow | Existing confirmation policy; never browser consumption of an operator intent |
| `POST .../publish` | Default: stage inert publish proposal; enabled agent policy: exact agent-intent flow | Existing confirmation policy; never browser consumption of an operator intent |
| `POST /authorization-proposals` | Stage inert exact proposal under default policy | Browser bearer + revision; no authority granted |
| `GET/DELETE /authorization-proposals/{id}` | Poll safe state or cancel only an unclaimed proposal | Browser bearer + proposal ownership |
| `GET /schedules` | Rules plus bounded forecast | Browser bearer |
| `POST /schedule-preview` | Bounded read-only forecast for one unsaved exact rule | Browser bearer; no schedule or request is created |
| `POST/PATCH /schedules` | Durable schedule mutations | Existing confirmation policy |
| `GET /activity` | Descending unified history | Browser bearer |
| `GET /requests/{id}` | Durable request progress | Browser bearer |
| `POST /pause` / `POST /resume` | Global admission control | Existing policy |
| `POST /deliveries/{id}/retry` | Exact safe recovery request | Approved intent |
| `POST /deliveries/{id}/reconcile` | Exact ambiguity resolution | Approved intent |

There is no arbitrary URL, filesystem path, SQL, command, environment name,
header map, control route, or provider payload parameter.

Opening a DRAFTS folder is deferred from the web release. Resolving or opening
an account root would contradict the gateway's no-root boundary. The UI shows a
safe CLI instruction; a later Tauri `HostBridge` may implement a fixed
profile-ID action after its separate native authority review. No browser route
accepts or returns the folder path.

### 9.3 Required daemon projections

The existing control surface needs narrow additions before Studio can claim
the full experience:

1. Exact success schemas, enums, timestamps, nullable fields, error codes, and
   descending cursor pagination.
2. Safe profile projection: timezone, enabled platforms, expected public
   usernames, configuration readiness, and revision—never token variable names
   or roots.
3. Bounded content inventory for all five views with scan generation,
   freshness, eligibility, scoped issues, fingerprint, media summary, caption/
   alt presence, target summary, and server-projected selection.
4. Exact detail with bounded caption/shared-alt text, member metadata,
   normalized validation issues, targets, immutable delivery information, and
   no absolute archive path.
5. Asynchronous bounded preview-derivative request and authenticated result.
6. Dashboard aggregates grouped by profile and state, including oldest/next
   actionable items and partial-failure metadata.
7. POSTED archive inventory and exact immutable detail.
8. Core-computed next occurrences and occurrence history with DST facts,
   `missed`, `no_content`, and execution state.
9. Unified activity with timestamps, source kind, state transition, safe
   summary, filters, and descending pagination.
10. Exact caption/alt `change` union in §10.1; whitespace-only text, null and
    ambiguous change bodies are rejected.
11. Caller-viewed fingerprint precondition for editorial writes.
12. Stable request `Location`, request ID, idempotency outcome, and safe timeout
    lookup guidance.
13. Cross-profile due-work projection before global resume.
14. Feature/capability flags so compatible versions degrade intentionally.
15. A trusted `post-pulsar authorize` terminal path for operator-origin intent
    create/review/approve/consume under the default `allow_agent_publish=false`
    policy, plus hardened canonical review for existing intent approval.
16. Draft admission provenance: current/admitted fingerprint, destination
    bucket/bundle, intent/request identity, timestamp, and whether the draft
    changed after admission.
17. Core-owned inert authorization proposals with the Section 8 schema, limits,
    state machine, one-claim mutex, safe agent status projection, and exact
    operator CLI retrieval/authorization operations.
18. Standard pinned TLS server identity per §7.5, protected discovery and key,
    stopped-daemon rotation, and per-request capability authentication; no
    custom challenge protocol or derived control-session cache.
19. Operator-origin consumption enforcement across new and existing routes,
    including replay paths; proposal possession never substitutes for operator
    authentication.

These additions preserve the current operation semantics. Any control schema
change triggers a coordinated core/plugin contract release. The old matching
core plus old plugin remains valid; the new core is not shipped until the new
plugin's pinned contract/hash and real-core journey pass.

### 9.4 Inventory and scan budgets

Filesystem scanning hashes media and must not become a five-second poll.

- Inventory refresh occurs on navigation, explicit refresh, or after a relevant
  completed mutation—not on the dashboard heartbeat.
- One content scan per profile may run at a time; at most two read-only scan/
  derivative subprocesses run installation-wide.
- The daemon returns cached summaries with `observed_at`, `scan_generation`,
  and `stale` fields or returns `202` for an in-progress refresh.
- Disposable inventory/preview caches live outside canonical SQLite schema and
  can be deleted/rebuilt. They never authorize publication.
- Default page size is 50, maximum 100. One inventory job accepts at most 3,000
  visible directory entries, 1,000 recognized bundles, 16 members per bundle,
  4 GiB of cumulatively hashed source bytes, and 10 seconds of wall time.
- The shared background queue holds at most 32 jobs. Overflow returns
  `preview_busy`/`scan_busy` with retry-after guidance and never blocks control.
- Exceeding an inventory limit returns the safely discovered page with
  `incomplete=true` and an exact limit code, suppresses aggregate counts and
  selection projection, and renders `Scan blocked/incomplete`, never empty.
- Presentation summaries expire after 5 minutes or relevant mutation;
  derivatives expire after 24 hours. Cache LRU size is 256 MiB per installation.
- Image preview input is capped at 64 MiB compressed and 40 megapixels decoded;
  output remains 1024 px/512 KiB. Video poster jobs have an eight-second hard
  deadline and inherit existing admitted media size/type constraints.
- An error anywhere that causes authoritative selection to fail closed renders
  `Scan blocked`, never `Empty bucket`.

### 9.5 Preview plane

The browser never receives a path or direct file URL.

1. Request names exact profile, bucket, bundle ID, fingerprint, and member ID.
2. The daemon validates the identity and returns an existing derivative or a
   JSON job receipt. Expensive scan/decode/`ffprobe` work never runs on the
   synchronous control request thread or publishing worker.
3. A daemon supervisor verifies the exact source identity/hash and streams it
   through a bounded broker into an owner-only per-job scratch area; the worker
   receives only that staged input and a pre-created output handle, never a
   content-root path, provider credential, inherited environment secret, or
   unrestricted destination. Image input remains at most 64 MiB; staged video
   input may not exceed the existing admitted platform limit or 512 MiB,
   whichever is lower. The parent rejects size changes while copying, verifies
   the staged hash, and removes scratch data on every exit path.
4. Derived previews are a capability, independently gated per platform. The
   first usable shell may render metadata/placeholders while decoder adapters
   are developed; that milestone does not complete the designed Content/media
   experience. The full GUI design retains working image previews on supported
   reference systems, with video posters optional. A source build has the same
   capability code/build instructions as a convenience package. Preview work
   does not gate the safe observation/edit/scheduling surfaces. When enabled,
   each platform uses a small self-tested sandbox launcher:
   Linux uses user/mount/network namespaces plus a seccomp syscall allowlist;
   macOS uses a signed App Sandbox helper with no network or broad file
   entitlements; Windows uses an AppContainer/restricted token with no network
   capability and a Job Object. Each permits only the staged input, output
   handle, runtime libraries, and required decoder executable. No fallback
   runs malformed-media decoding unsandboxed: if the helper or its startup
   self-test is unavailable, derivatives are disabled and Studio shows bounded
   metadata plus a safe placeholder. Raw source installs may explicitly report
   `preview_sandbox_unavailable`; this degrades previews, never publishing.
5. Each job is one process with no descendants, at most 6 CPU seconds, 8 wall
   seconds, 512 MiB RSS/address space, 32 POSIX descriptors or 64 Windows
   handles, one output file, and 512 KiB output. The supervisor independently
   monitors wall time/RSS/output, terminates on the first breach, waits one
   second, then kills the entire process group/Job Object. `ffprobe` is launched
   directly as that one worker with an exact argument array, file/pipe protocol
   allowlist, bounded probe/analyze duration, and no playlist/network protocol;
   it cannot launch another process. The image helper strips metadata and
   enforces decoded pixels/dimensions before allocating the final derivative.
6. The supervisor validates type, dimensions, byte count, and hash, then
   atomically installs the output. Sandbox setup failure, forbidden network/
   source/child access, runaway CPU/RSS, handle/output overflow, malformed
   media, or timeout all fail closed to a normalized preview error.
7. Cache identity is immutable over profile, bucket, bundle ID, bundle
   fingerprint, member SHA-256, transformation version, dimensions, and output
   format. A stale/changed source can never be returned under the earlier key.
8. Job state is `queued | running | ready | failed | expired`; receipts expire
   after ten minutes. Studio abandonment does not cancel the daemon job.
9. A new authenticated daemon binary route serves only a ready cached
   derivative on the existing literal-loopback listener. It requires the
   current capability bearer over pinned TLS plus opaque job/result ID, allows
   GET only, disallows Range/redirect/chunked responses, requires exact
   `Content-Type` and `Content-Length`, sets `no-store`/`nosniff`, and sends at
   most 512 KiB. It never accepts a path or performs generation.
10. All daemon calls, including this cached result fetch, pass through the BFF's
   single upstream priority dispatcher. The BFF reads the full bounded result
   under a two-second deadline before responding, so a slow browser cannot hold
   the daemon listener. Health/writes outrank derivative fetches.
11. The BFF verifies type/length/hash, buffers at most 512 KiB, and returns it
   through an authenticated browser route.
12. Svelte fetches it with its authorization header, creates an object URL, and
   revokes that URL when the component unmounts or the fingerprint changes.

Initial target: WebP/JPEG/PNG derivative no larger than 1024 px on its longest
edge and 512 KiB. Unsupported/corrupt images and unavailable video poster
generation produce a safe placeholder plus metadata. Original video streaming
is out of scope.

With no verified decoder capability, show only safe member role, size, claimed
MIME, and hash from the bounded scan. Dimensions, duration, codec, and visual
preview are `not inspected`, unless exact-fingerprint cached inspection
evidence exists. Do not run Pillow or ffprobe as an unadvertised fallback to
populate metadata. The existing publish preflight is unchanged. Native sandbox
availability is a per-platform capability/release gate, not a claim that every
source checkout already has signed helper binaries. Planning must preserve
this intended preview capability rather than declaring the whole media feature
finished at a metadata-only milestone.

Health requests must remain below 100 ms p95 and durable write acceptance below
250 ms p95 on the reference local test host while the preview queue is full and
both subprocesses are saturated or forcibly timed out. Publishing/scheduler
progress may not miss its next poll interval because of presentation work. A
failure of these falsifiers requires a separately discovered preview listener
or stronger isolation before release; it may not be waived as cosmetic.

## 10. Mutation, concurrency, and request state

### 10.1 Editorial lost-update protection

Current caption/alt PATCH behavior rescans at ingress but does not know which
fingerprint the operator viewed. Studio requires a new closed request that
carries:

- exact profile, bucket `DRAFTS`, and bundle ID;
- exact fingerprint displayed to the operator;
- exact resource/profile revision where applicable;
- the exact replacement/unset `change` union below;
- one idempotency key for that user gesture.

For both caption and alt, the UI/core PATCH body has exactly
`expected_fingerprint` (64 lower-case hex) and `change`. `change` is exactly one
of `{"text":"non-empty text"}` or `{"unset":true}`. Reject null, empty or
whitespace-only text, false unset, both fields, extra fields, or text over
10,000 Unicode code points / the 64 KiB request bound. Preserve accepted text
verbatim; do not trim or normalize it silently. `If-Match` is the profile
revision (configuration changes), not a fictitious draft counter; the viewed
fingerprint detects content edits. `Idempotency-Key` binds the whole request.
The daemon's closed durable edit arguments carry bucket, bundle ID, fingerprint,
and the same `change` union. This is a coordinated schema change, including MCP.

Unset removes the exact caption/alt member under the same safe-file and
mutation-lock policy as Save. Absent member is a completed `unchanged` result;
never write an empty file. If unsetting would remove the draft's last semantic
member, reject `last_draft_member` and preserve it; this operation is not bundle
deletion. Successful edits return the resulting fingerprint and field presence.

The daemon compares the viewed fingerprint at ingress and immediately before
atomic replacement. Any mismatch returns a conflict. Studio preserves unsaved
text, fetches the current version, and shows `Your version`, `Current version`,
and a deliberate retry action. It never silently rebases.

The guaranteed lost-update protection covers daemon-mediated GUI, CLI, and MCP
edits: one per-bundle mutation serialization boundary spans final validation,
replacement, and result observation. Admission uses that same boundary for
daemon-mediated edits. A directly editing filesystem process need not obey it;
two scans and `os.replace` cannot provide atomic compare-and-swap against such
a writer. Folder-first workflows remain supported, but the editor explains
that an external editor must be closed while saving through Studio. Detected
external changes preserve the local form and return a conflict, never an
automatic replay. Post-write identity drift is shown as unverified, not Saved.
This is not a claim to isolate arbitrary DRAFTS writers: publication still
copies and verifies every exact fingerprint-bound member independently.

### 10.2 Resource/action matrix

Studio does not implement a composite `Admit and publish` action.

| Resource shown | Visible primary action | Daemon action/binding | Result interpretation |
| --- | --- | --- | --- |
| DRAFTS, current fingerprint never admitted | `Admit to…` | Approved `admit_draft`; profile revision + full fingerprint + exact destination bucket | Track admission request; show linked ready copy only after complete |
| DRAFTS, same fingerprint already admitted | `View admitted copy` | Read-only navigation using admission provenance | Repeat admission disabled; editing creates a new fingerprint |
| DRAFTS changed after prior admission | `Review admission destinations…` | New approved `admit_draft` only to an unused eligible destination | A changed fingerprint does not free an existing destination identity |
| Ready on-disk QUEUE/RANDOM/REELS bundle not yet durable | `Publish now` | Approved `enqueue`; profile + bucket + ID + full fingerprint + revision | Success only after admitted bundle/deliveries/archive agree |
| Eligible durable pending/recoverable bundle | `Run now` | Approved `run_now`; bundle key + revision + fingerprint | Follow exact bundle/delivery result, never request status alone |
| Known blocked failed delivery | `Review recovery` then `Retry` | Approved `retry`; exact bundle/platform/revision/fingerprint | Only after current target validation; never for ambiguous |
| Ambiguous delivery | `Verify on platform` then `Reconcile` | Approved `reconcile`; exact remote ID or explicit not-published attestation | Retry remains hidden until not-published reconciliation completes |
| Active/in-flight bundle | `View progress` | Read-only request/bundle lookup | No second publish action |
| Archiving bundle | `View progress` | Read-only bundle lookup | No publish/retry action while archive converges |
| Archived/published evidence | `View history` | Read-only POSTED detail | No republish action |
| Imported legacy archive without delivery evidence | `View archive` | Read-only POSTED detail | Label publication status unknown |
| Cancelled/deleted tombstone | `View history` | Read-only detail | No publish action |
| Invalid/scan-blocked source | `Review issues` | Read-only inventory/detail | All consequential actions disabled with exact issue |

Every disabled action has textual reasoning. Presence in POSTED is not itself
proof of publication: migration may import legacy archives. Detail distinguishes
`published and archived`, `archived with partial evidence`, and `imported
archive — publication unverified` from durable provenance and delivery state.

Draft inventory includes `admission_state`, admitted fingerprint, destination
identity, intent/request identity, admitted time, and `changed_since_admission`.
The original DRAFT and ready copy link to one another without being conflated.

Admission journals permanently reserve `(profile, destination bucket, bundle
ID)` and also prevent reuse of a fingerprint within a profile. Archiving or
removing the ready directory does not release that reservation. Destination
eligibility therefore consults journals and filesystem state together. An
occupied destination shows `Bundle ID already admitted here` and a link to its
history. A changed draft may use a different eligible unused bucket; otherwise
the operator prepares a new semantic bundle ID in DRAFTS with the existing
folder workflow. Studio v1 never renames files, invents a version suffix,
overwrites a ready copy, or promises replacement admission. The final admission
check repeats these constraints after approval, so another client can make the
displayed destination stale without causing replacement.

### 10.3 Idempotency

- Generate one cryptographically random idempotency key per explicit gesture.
- Retain it with the exact canonical request while the outcome is unknown.
- Reuse it only for a byte-equivalent transport retry or lookup.
- Changing any field creates a new gesture and key.
- Never auto-replay publish, retry, reconcile, admission, enable, or resume
  after conflict or unknown outcome.
- Out-of-order responses with an older resource revision or daemon session
  cannot replace newer client state.

Within a living document, unknown outcomes use the in-memory canonical request
and original key. To survive F5, tab sessionStorage may additionally retain at
most 32 safe pending receipts (≤1 KiB each, eight-hour expiry): operation ID,
profile/resource IDs, idempotency key, request digest, observed revision/epoch,
and known request/proposal ID. No caption, alt text, action body, provider data,
or automatic replay instruction is stored. A 33rd unresolved mutation is not
dispatched until a receipt settles or the operator deliberately abandons its
observation; never silently evict a live receipt. This exception is
observation-only.
After refresh, restore auth then resolve these receipts through exact key/ID
lookup before enabling another mutation on the affected resource. `not_found`
while an earlier dispatch may still be live means unknown, not permission to
replay. The BFF retains bounded in-flight dispatch state across a browser
disconnect; it does not cancel a forwarded write because fetch was aborted.
Only a known terminal result clears the pending lock automatically. An operator
may deliberately abandon observation after reviewing fresh authoritative state;
the UI never reports that as cancellation of the original work.

After gateway loss, connection ambiguity or lost receipts, Studio shows durable
requests/activity and asks the operator to identify the result; it does not
auto-replay. A lost unsaved form is not reconstructed from storage. Dirty-form
navigation/refresh uses the browser's supported leave warning, and queued Save
is visibly distinct from Saved. No offline mutation queue or persistent BFF
journal is introduced.

### 10.4 Separate state axes

```text
local workflow: idle → validating → awaiting approval → queued → observing
durable intent:  pending → approved → consumed
                              └────→ expired
request executor: queued → claimed → completed | failed
action result:    action-specific closed result
bundle:           active | blocked | archiving | archived | cancelled | deleted
delivery:         pending | in_flight | published | failed | ambiguous
```

Policy denial, authentication failure, validation error, operator cancellation,
and a local `rejected` label are API/UI outcomes—not invented durable intent
states.

A completed executor request is not publication success. Its action result may
still be `empty`, `deferred`, `blocked`, `invalid`, or `failed`. Publication
success copy is allowed only when the authoritative action result is `archived`
and bundle, all snapshotted deliveries, and archive checkpoints agree. Any
other result renders its exact outcome. The interface never collapses these
axes into one generic badge.

| Request state | Action result | Resource evidence | UI copy |
| --- | --- | --- | --- |
| queued/claimed | any/not final | current progress | `Queued` / `Running` |
| failed | safe normalized error | unchanged/inspect | `Request failed` plus legal next step |
| completed | archived | bundle archived and all deliveries published | `Published and archived` |
| completed | empty | no candidate | `No eligible content` |
| completed | deferred | retry/due facts | `Deferred` with timing/reason |
| completed | blocked/invalid/failed | exact bundle/delivery facts | Exact blocked/invalid/failed state—not success |
| completed or transport lost | ambiguous delivery | platform outcome unknown | `Verification required`; Retry hidden |

### 10.5 Approval subflows

| Policy/path | Durable flow | Studio behavior |
| --- | --- | --- |
| `allow_agent_publish=true` | GUI creates agent intent → trusted CLI renders/approves → GUI refetches exact intent → explicit Consume | Continue only if every binding, origin, revision and expiry matches |
| `allow_agent_publish=false` | Studio stages inert core-owned proposal → independent trusted `authorize <id>` CLI claims/refetches → renders canonical consequence → prompts → creates/approves/consumes operator intent → durable request | Studio polls safe proposal state and authoritative activity; it cannot create the privileged intent |
| No independent trusted TTY | No approval mutation | Read/edit-safe operation remains; consequential UI shows the exact CLI requirement |

The terminal CLI is launched independently by the operator in a real TTY from
the fixed safe-ID command in Section 8—never by Studio and never as a shell
string assembled from action data. Safe proposal/intent/request IDs and state
return through the daemon rendezvous, not through the web process. Studio
re-fetches and compares action, origin, profile, targets, fingerprint, resource
revision, arguments, consequence, and expiry before enabling any remaining
Continue step.

### 10.6 Recovery rules

- `safe_to_retry=true` plus `next_attempt_at` shows automatic retry timing and
  does not promote a manual button.
- Blocked known failure may expose approved Retry after refreshed identity and
  fingerprint checks.
- Ambiguous delivery never exposes Retry. The operator follows a platform
  verification checklist and reconciles `published` with exact remote ID or
  `not published` with explicit attestation.
- `not published` reconciliation and a later Retry are separate approved
  gestures.
- Partial delivery lists each platform state without marking the bundle done.
- Normal UI exposes Cancel for eligible pristine work. Delete remains advanced
  until it has meaning visibly distinct from the existing tombstone behavior.

## 11. Domain presentation model

### 11.1 Profiles

Profile identity is persistent in the URL: `/p/{profile_id}/...`. `All
profiles` exists for Overview and Activity. Content and schedule editing
require one exact profile. Switching with unsaved work prompts before discard,
aborts old reads, clears prior pagination, and never momentarily relabels old
data under the new account.

Profile cards show safe expected usernames, platform icons, timezone,
configuration readiness, enabled/disabled target state, and intervention
counts. Credential presence is not provider-health proof.

### 11.2 Buckets

| Bucket | Meaning in Studio | Selection language |
| --- | --- | --- |
| DRAFTS | Flat editable files grouped virtually by parsed bundle ID | Not publishable |
| QUEUE | Ready immutable bundle directories | `Projected next` by canonical ID order |
| RANDOM | Ready immutable bundle directories | `Projected next` from current durable score |
| REELS | Ready video bundle directories | `Projected next` by canonical ID order |
| POSTED | Immutable archive grouped by original bucket | Archive/history; publication evidence shown separately |

The visual treatment may resemble a board, but columns are not arbitrary state.
There is no drag-to-move or drag-to-reorder in v1. The UI explains selection
and provides keyboard-accessible admission actions from DRAFTS. If explicit
ordering is later added, it must be a daemon-owned domain contract exposed to
MCP as well—not a client-only array.

### 11.3 Schedules and pulse calendar

A schedule is a recurring pulse targeting one profile and bucket. The calendar
renders authoritative core forecasts, not JavaScript timezone calculations.

- Weekdays use Monday `0` through Sunday `6` in the daemon but human labels in
  the UI.
- Time is `HH:MM` in the explicit IANA schedule timezone.
- Show schedule-local time and the operator's browser-reported current IANA
  timezone equivalent. If unavailable, show UTC with that label; never guess.
  Browser conversion formats a daemon-supplied UTC instant only and never
  calculates a schedule occurrence.
- A repeated DST time fires once at the earlier instant.
- A nonexistent time moves to the first valid minute; the gap counts against
  misfire grace.
- An evaluated current-date occurrence beyond grace is recorded as `missed`;
  past dates are not backfilled.
- Simultaneous schedules can yield `no_content` after another occurrence uses
  the only eligible bundle.
- A projected bundle may change before the pulse; no card says `reserved`.

New schedules begin disabled by default. Enabling, changing an enabled rule,
or modifying one with due work uses the confirmation flow. Disable uses the
existing safe operation but still waits for its durable request to complete.

Forecasts and execution evidence are different records. History shows only
persisted `schedule_runs`, retaining their captured scheduled instant and rule
hash. If the daemon was offline for whole dates, those dates may have no record;
show `No execution record`, never infer `missed`, `no_content`, or successful
execution from today's schedule. Editing a rule does not rewrite past history.
Forecast responses identify the current rule revision and observation time;
disabled rules are marked hypothetical and cannot appear as enabled pulses.

Forecast algorithm and bounds are shared by every view:

1. The daemon walks successive matching local dates and calls the existing
   `resolve_occurrence`; never step through UTC hours or simulate publication.
2. Pulse Rail uses `(observed_now, observed_now + 24 hours]`, shows at most 12
   occurrences, and links overflow to Schedules. A selected calendar week is
   an explicit half-open UTC window corresponding to seven browser-local dates;
   its maximum span is 169 hours across DST. Each rule is evaluated in its own
   stored timezone and then clipped to that shared window.
3. Editor preview returns the next five instants for one exact candidate rule,
   searching at most 35 successive local dates. Unsaved/disabled previews are
   explicitly hypothetical; no durable occurrence is created. A date with no
   valid instant yields a bounded `unresolvable_local_date` issue, not a crash
   or invented instant.
4. One forecast call examines at most 128 rules and returns at most 500
   occurrences; exceeding either bound yields `truncated=true` with a scope-
   narrowing instruction. Only a complete bounded result may show an exact
   overflow count; otherwise say `More occurrences`.
5. Order by UTC instant, profile ID, schedule ID. Each item includes rule
   revision/hash, nominal local date/time, resolved UTC instant, offset,
   gap-delay seconds, and fold/gap annotation. Past durable history stays a
   separate projection.
6. No consumption simulation, future RANDOM-counter increment, reservation,
   or predicted `no_content` execution state is performed. A bucket's current
   projected-next candidate is a separate observed fact, using core
   `preview_selected_bundle` semantics without changing the counter.

## 12. Information architecture and screens

### 12.1 Application shell

Persistent desktop shell:

```text
┌──────────────────────────────────────────────────────────────────────┐
│ POST PULSAR  [All profiles ▾]       Healthy · Running     ⌘K   ●  │
├───────────────┬──────────────────────────────────────────────────────┤
│ Overview      │ page title · context actions                         │
│ Content       │                                                      │
│ Schedules     │                 active page                          │
│ Activity   2  │                                                      │
│ System        │                                                      │
│               │                                                      │
│ ansonphong    │                                                      │
│ 360hextile    │                                                      │
└───────────────┴──────────────────────────────────────────────────────┘
```

The header always shows daemon connection/incarnation state, global pause
state, exact profile scope, and outstanding intervention count. Global actions
are not visually nested under a profile.

The header selector is the sole profile-scope control. Sidebar entries remain
product navigation; any profile summaries there are passive status shortcuts
that open the same selector rather than implementing a second selection model.

Primary routes:

- `/overview`
- `/p/{profile_id}/content/{bucket}`
- `/p/{profile_id}/content/{bucket}/{bundle_id}`
- `/p/{profile_id}/schedules`
- `/activity` and `/p/{profile_id}/activity`
- `/system`

Deep links restore view context after a secure session is established. They do
not contain fingerprints, secrets, raw captions, paths, or operation tokens.

### 12.2 Overview

Overview is intervention-first:

1. Ambiguous deliveries needing platform verification.
2. Blocked failures and degraded callback/recovery state.
3. Global pause or pause-requested state.
4. Active and archiving work.
5. Safe automatic retries and next attempt.
6. Ready-content counts by profile/bucket.
7. Enabled schedules and next canonical occurrences.
8. Recent durable activity.

The signature **Pulse Rail** spans the main content width and shows at most
12 occurrences within the next 24 hours across profiles. Each mark contains profile,
bucket, schedule-local time, operator-local time, and forecast state. It is
fully duplicated by an accessible list.

Avoid vanity analytics. Counts are operational and server-computed. Every card
shows `Observed <time>` and a stale/degraded indication when relevant.

### 12.3 Content

Content uses bucket tabs rather than a draggable Kanban. Desktop view offers a
responsive card grid and compact list toggle. Cards show:

- thumbnail or bounded placeholder;
- bundle ID, media kind/count, and source bucket;
- caption excerpt and caption/alt presence;
- target platform badges and material validation warnings;
- fingerprint short form with full copy action;
- eligibility and projected-next status;
- schedule badges phrased `eligible for` rather than `scheduled at`;
- current durable/archived state where applicable.

Filters are URL-backed: media kind, validation state, target, caption/alt
presence, and search by bundle ID. There is no infinite scroll; cursor pages
have explicit `Load more` and retain focus correctly.

Empty views distinguish `No content`, `Scan in progress`, `Scan blocked`,
`Unable to refresh`, and `No matching filters`.

### 12.4 Bundle detail/editor

Desktop uses a two-pane layout: media stage left, editorial/validation panel
right. Narrow layouts stack media, fields, validation, and actions.

Required content:

- exact profile and expected platform usernames;
- media order, dimensions/duration, MIME, size, and safe poster/thumbnail;
- full caption and shared alt text;
- X weighted count and Instagram caption constraints;
- explicit Instagram shared-alt unsupported warning;
- platform-specific preflight results and normalization warnings;
- bucket, bundle ID, current fingerprint, and relevant revision;
- delivery/artifact history for admitted or archived bundles;
- separate actions for Save, Unset, Admit to bucket, and Publish now.

Save is asynchronous: button becomes `Saving…`, then `Queued`, and only becomes
`Saved` after the durable request completes. A stale conflict opens a three-way
comparison without discarding local text.

Admission review names the destination bucket, exact files, fingerprint,
platforms, and consequence. It explains that the draft remains while a ready
immutable copy is created.

### 12.5 Schedules

Schedules combines:

- a compact weekly pulse strip as the default visual;
- an accessible chronological list/table;
- rule cards grouped by profile and bucket;
- a right-side editor sheet on desktop and full page on narrow screens.

The first release does not use a month calendar: rules are weekly recurring and
a month grid adds noise and suggests one-off reservations. A rolling 7-day and
`Next occurrences` view communicates the actual model more accurately.

Editor fields:

- stable lowercase schedule ID;
- bucket;
- weekday checkbox fieldset;
- local `HH:MM`;
- IANA timezone defaulted from profile;
- misfire grace with plain-language example;
- enabled state;
- authoritative next five occurrences with DST notes.

Creating a schedule starts disabled. Any publication-enabling change gets a
clear consequence review and terminal-approval state.

Schedule ID is editable only during creation and immutable thereafter; changing
identity means creating a new schedule. Pulse Rail ordering is canonical UTC
occurrence time, then profile ID, then schedule ID, matching daemon tie-break
expectations rather than browser locale sorting.

### 12.6 Activity and recovery

Activity merges requests, bundle/delivery transitions, schedule occurrences,
and sanitized daemon failures into a descending timeline. Filters include
profile, platform, kind, state, and intervention required.

Each row has a stable textual summary, timestamp with timezone, status icon and
label, request/resource identity, and expandable safe detail. No raw provider
response appears.

Recovery views are purpose-built, not generic forms:

- Retry view proves known failure and refreshed target identity.
- Ambiguity view presents a platform-verification checklist and exact published
  remote-ID versus not-published paths.
- Partial view separates successful and uncertain platforms.
- Expired approval preserves the consequence but requires a newly generated
  intent; it never silently recreates one.

### 12.7 System

System displays safe operational facts:

- Studio/core version and build identifiers;
- compatible/incompatible state;
- daemon health and current global admission state;
- configured profiles, safe remote usernames, timezones, enabled targets;
- credential/configuration readiness without values or environment names;
- required `ffprobe` video-inspection capability, optional `ffmpeg` derivative
  capability, image derivative capability, and video-poster capability as four
  separate facts; missing poster support never marks a valid Reel invalid;
- service mode and safe start instructions;
- core control contract version/hash, the compatible plugin release range,
  setup documentation, and the explicit statement `MCP client presence is not
  observable`;
- open-source license, source link, privacy statement, and diagnostics export.

Diagnostics export is explicit, redacted, bounded, and previewed before save.
No button edits credentials, configuration files, ACLs, or services in v1.

## 13. Visual system

### 13.1 Design character

**Quiet orbital control** combines shadcn-style restraint with one recognizable
POST PULSAR motif. It should feel like a precise creative tool, not an
enterprise dashboard or a sci-fi game HUD.

The required hierarchy and anti-template compositions are defined in
[03-visual-acceptance-plates.md](03-visual-acceptance-plates.md). High-fidelity
implementation references must be approved from those plates before screenshot
baselines become authoritative.

Principles:

- Content and intervention outrank chrome.
- Dense information uses hierarchy and whitespace, not boxes around every row.
- Color means state only after typography and iconography establish meaning.
- One signature temporal visualization is enough.
- Rounded corners are restrained; not every label is a pill.
- Shadows are subtle and elevation is rare.
- No decorative glass blur, neon gradient wash, fake charts, or animated
  particles.

### 13.2 Tokens

Token names are semantic and live in Tailwind 4 CSS-first `@theme` variables.
Provisional dark palette:

| Token | Direction |
| --- | --- |
| Canvas | `#090B10` deep graphite |
| Surface | `#10141B` |
| Raised surface | `#171C25` |
| Border | `#29313D` |
| Primary text | `#F3F6FA` |
| Muted text | `#9BA7B5` |
| Pulsar accent | `#57D3E6` |
| Success | `#69D39E` |
| Warning | `#F0B45A` |
| Danger | `#F06D7A` |

The paired light palette is canvas `#F7F8FA`, surface `#FFFFFF`, raised surface
`#EEF1F5`, border `#778395`, primary text `#121923`, muted text `#4D5B6D`, accent
`#086779`, success `#17653F`, warning `#805100`, and danger `#A62A3D`. Accent-fill
buttons use `#071317` text in dark mode and white in light mode. Dark input and
focus boundaries that must identify a control use `#63758B`; the darker panel
border is decorative, not its only interaction boundary. Hover/selected states
use these opaque tokens, never opacity that reduces required contrast. State
text always includes the icon/label defined in the closed state model.

A complete accessible light theme is designed from the same semantic tokens,
not produced by inversion. Final colors require automated contrast checks in
default, hover, focus, selected, disabled, and high-contrast states.

- Spacing uses a 4 px base with a deliberately small scale.
- Radius: 6 px controls, 10 px panels, 14 px only for signature surfaces.
- Default control height: 36 px desktop, with at least WCAG 2.2 target-size
  spacing; primary touch targets are 40–44 px.
- Sidebar: 224 px expanded and 64 px collapsed.
- Header: 56 px. Main content max: 1600 px.
- Data-heavy text: 13–14 px; body: 15–16 px; page title: 24–28 px.

Use the system sans stack `ui-sans-serif, system-ui, -apple-system,
BlinkMacSystemFont, "Segoe UI", sans-serif` with line-height 1.45, and mono
`ui-monospace, "SFMono-Regular", Consolas, "Liberation Mono", monospace` for
technical identities. This is the v1 font decision, with native OS rendering
checked in the browser matrix. A later bundled brand font is optional and
requires its own license/contrast review; never fetch fonts remotely.

### 13.3 Phosphor usage

Use tree-shaken component imports from `phosphor-svelte`.

- Regular weight for navigation and ordinary actions.
- Bold for tiny state emphasis only.
- Duotone for product mark and empty states only.
- `aria-hidden=true` when an adjacent text label exists.
- Icon-only controls require an accessible name and tooltip, but consequential
  actions retain visible labels.
- Platform brand marks are distinguished from operational icons.

shadcn-svelte is treated as copied, owned component source—not an aesthetic
shortcut. Use Bits UI behavior primitives, replace its default icon assumptions
with Phosphor consistently, and keep one local component inventory.

### 13.4 Components

Initial owned primitives:

- Button, IconButton, LinkButton
- Badge and StatusLabel
- Input, Textarea, Select, CheckboxGroup, TimeField
- Dialog, AlertDialog, Sheet, Popover, Tooltip
- Tabs, SegmentedControl, Breadcrumb
- Table, Pagination, Skeleton, EmptyState
- Toast and persistent InlineNotice
- CommandPalette
- ProfileSwitcher
- BundleCard, MediaStage, ValidationList
- PulseRail, OccurrenceList, ScheduleEditor
- RequestProgress, DeliveryState, RecoveryPanel

One component renders each domain status axis from a closed enum. Pages do not
invent colors or labels ad hoc.

CommandPalette opens with Cmd+K on macOS and Ctrl+K elsewhere, with the matching
visible accelerator. Its closed command set is navigation to the five screens,
profile switch, focus bundle-ID search, and Lock session. No shell, publication,
approval, hidden admin command, or arbitrary route runs through it. It follows
the labelled combobox/listbox keyboard pattern, supports Escape, and returns
focus to its invoker.

TimeField is a labelled native `input type="time"` with minute precision, a
visible 24-hour HH:MM example and separate timezone label. It emits a local
time string, never a Date/UTC timestamp; invalid/incomplete input cannot submit.
PulseRail has decorative marks paired with the same ordered semantic
OccurrenceList. Every available mark action has a labelled keyboard-operable
list action; neither surface requires dragging or implements an ARIA grid.

### 13.5 Motion

- 120 ms hover/focus, 160–180 ms sheet/dialog transitions.
- Animate opacity/transform only; avoid layout shifts.
- Active publishing may use one subtle finite pulse when state advances, not a
  perpetual glow.
- Polling never steals focus or animates the whole list.
- `prefers-reduced-motion` removes nonessential motion and smooth scrolling.

## 14. Responsive and cross-browser behavior

Desktop-first does not mean desktop-only:

| Width | Behavior |
| --- | --- |
| `>=1280` | Expanded navigation, two-pane detail, full Pulse Rail |
| `1024–1279` | Collapsible navigation, two-pane detail where viable |
| `768–1023` | Drawer navigation, stacked detail, condensed weekly strip |
| `<768` | Single column, list-first content, full-page editors, no dense drag interactions |

All functionality remains reachable at 320 CSS px and 200% zoom, though desktop
is the primary workflow. Test current Chromium/Edge, Firefox, and Safari/WebKit.
Do not rely on a Chromium-only file API for core behavior.

The future Tauri window targets a comfortable minimum near 1024×700 but must
still render narrow accessibility/zoom layouts correctly.

## 15. Accessibility contract

Target WCAG 2.2 AA.

- Semantic landmarks, headings, lists, tables, fieldsets, labels, and native
  controls precede ARIA.
- Set document `lang="en"`; provide a first-focusable Skip to main content
  link, one main landmark, and route-change focus on the new page heading.
- Full keyboard operation; no drag-only, hover-only, or pointer-only action.
- Follow WAI-ARIA Authoring Practices for dialogs, grids, menus, tabs, and focus
  restoration.
- Visible two-color focus indicators remain clear on every surface.
- State uses icon plus text and never color alone.
- Dialogs trap focus, restore focus, close on Escape when safe, and retain a
  visible Cancel/Close action.
- Async completion/failure uses a polite live region; do not announce countdown
  changes every second.
- Errors have a page/form summary, field association, and recovery instruction.
- Time always includes timezone in consequential contexts.
- Weekday selection is one labelled fieldset.
- Pulse Rail, charts, and media carousels have equivalent lists and controls.
- Captions and IDs wrap without horizontal loss at 200% zoom.
- Reduced motion, forced colors, high contrast, screen reader, keyboard-only,
  and touch target behavior are automated/manual release gates.

## 16. Client state, polling, and freshness

Keep client state small:

- URL owns profile, route, bucket, filters, and pagination cursor.
- Component state owns forms and unsaved edits.
- One typed fetch/cache layer owns server resources by exact profile/resource
  identity, revision, fingerprint, and daemon session.
- No offline store and no queued browser mutations.

Polling policy:

| Resource | Active interval |
| --- | ---: |
| Health/overview | 5 seconds with jitter |
| Active request/intent | 1 second until terminal |
| Activity first page | 10 seconds while visible |
| Inactive tab | 30 seconds or suspended |
| Bucket inventory | Navigation, explicit refresh, relevant mutation only |

Coalesce identical in-flight reads, cancel superseded/profile-old requests,
and exponentially back off failures to 30 seconds. Each response carries
observation time and daemon session. A late response cannot replace a newer
revision or another incarnation.

SSE is reconsidered only after the daemon has a durable monotonic event cursor.
WebSockets are not justified for this one-way operational dashboard.

## 17. Empty, degraded, and unsafe states

The UI has distinct designed states for:

- Studio session missing/expired;
- daemon offline, starting, restarting, stopping, or incompatible;
- capability revoked/authentication failed;
- callback/recovery degradation;
- admission running, pause requested, and fully paused;
- empty bucket versus blocked/failed scan;
- unsupported, corrupt, queued, or unavailable preview;
- stale profile, schedule, bundle revision, or fingerprint;
- intent draft, awaiting approval, approved, consumed, expired, or rejected;
- request queued, claimed, completed, failed, or unknown after transport loss;
- automatic retry scheduled versus blocked known failure;
- ambiguous final provider outcome;
- partial multi-platform publication;
- schedule `missed` and `no_content`;
- configuration present but provider status unverified.

When offline or incompatible, preserve readable cached layout only if clearly
stamped stale, disable all writes, and never queue an action for later.

## 18. MCP coexistence and cross-repository contract

Studio and MCP are peers, not wrappers around one another:

```text
Svelte → FastAPI BFF ─┐
                     ├─→ daemon commands/queries → durable state
Agent → MCP server ──┘
```

- The GUI never calls the MCP server.
- The MCP server never screen-scrapes or calls the GUI.
- Both resolve the same daemon incarnation and use the same profile IDs,
  revisions, fingerprints, confirmation intents, idempotency rules, and durable
  request IDs.
- A GUI edit by Anson must become visible to an agent on the next read; a
  concurrent agent edit must cause a visible GUI conflict rather than overwrite.
- Browser-only DTOs remain allowlisted and do not expand MCP authority.
- New domain mutations useful to agents are added to the control contract and
  MCP surface deliberately; purely visual derivatives need not become agent
  tools.
- Control changes require a coordinated `POST-PULSAR-PLUGINS` revision, updated
  contract/hash/core pin, redaction review, and real-core integration journey.
- Compatibility tests run old-match and new-match pairs. Mismatched versions
  fail closed with upgrade instructions; they never fall back to raw calls.

Studio's System screen displays only the core control contract version/hash,
compatible plugin release range, and setup documentation. It says `MCP client
presence is not observable`. Version metadata does not prove installation or a
live connection. A future presence signal requires an explicit
privacy-preserving handshake contract; Studio does not enumerate clients,
inspect their configs, or expose their capability.

## 19. Tauri-ready host seam

No Tauri dependency ships in the first web release. Preparation consists of
interfaces and static-build constraints, not speculative Rust code.

### 19.1 Frontend seams

Define small TypeScript interfaces:

```ts
interface StudioTransport {
  request<T>(operation: UiOperation<T>): Promise<T>;
  fetchPreview(ref: PreviewRef): Promise<Blob>;
}

interface HostBridge {
  platform(): Promise<"windows" | "macos" | "linux" | "web">;
  openDraftsFolder(profileId: string): Promise<void>;
  notify(notification: SafeNotification): Promise<void>;
  chooseImportFiles?(): Promise<OpaqueFileSelection>;
}
```

The web implementation uses same-origin fetch; native-only host methods return
an explicit unsupported result. A later Tauri implementation may use `invoke`
for folder reveal, file selection, and notifications while retaining the same
Svelte routes, DTOs, and daemon request semantics.

Views never import Tauri APIs directly, assume a filesystem path, depend on
Node server routes, or construct backend origins themselves.

### 19.2 Future desktop topology

Preferred future topology:

- Tauri 2 shell owns the WebView window, tray, deep links, notifications,
  native dialogs, and signed update integration.
- The statically built Svelte application is unchanged apart from selecting the
  Tauri `HostBridge`.
- A bundled POST PULSAR Python executable/gateway runs as a version-matched
  sidecar or installed background service.
- The daemon remains independently recoverable and is not killed when the
  window closes.
- Native approval, keychain use, file imports, and background service install
  each receive separate threat/design review.

Do not promise a universal app-store package. Windows Store, Mac App Store,
signed direct macOS distribution, AppImage/deb/rpm, Flatpak, and other Linux
channels have different sandbox, background-service, filesystem, updater,
licensing, and signing constraints. Direct signed installers are the baseline;
store acceptance is a gated distribution project.

### 19.3 Cross-platform web requirements now

- Use platformdirs and existing service metadata; no hard-coded slash, drive,
  home, shell, or executable assumptions.
- Fixed host actions use argument arrays, never shell strings.
- File/folder reveal resolves configured profile ID on the trusted side and is
  disabled across hardened identity boundaries.
- Browser open failure prints a safe recovery path.
- CI covers Linux, Windows, and macOS path/process/service rendering; manual
  browser smoke covers the native browser/WebView family before release.

## 20. Open-source and paid distribution contract

### 20.1 Open-source promise

- The public repository contains the complete core, gateway, Svelte source,
  components, schemas, tests, build scripts, and packaging instructions.
- GitHub builds and forks have the same publishing features as paid binaries.
- No telemetry, advertising, activation, mandatory account, remote license
  check, proprietary API, or hosted dependency.
- Every release has exact lockfiles, source tag, checksums, SBOM, dependency
  notices, reproducible-build evidence where practical, and security policy.
- The app may show an unobtrusive source/support link; it does not nag or
  degrade functionality.

### 20.2 Roughly USD $20 convenience edition

Value is delivery:

- code-signed/notarized executable;
- bundled runtimes and static assets;
- installer, service setup, shortcuts, tray, clean uninstall;
- store purchase/update handling or signed direct updater;
- native file picker, notifications, and OS polish;
- tested upgrade/migration path.

There is no subscription and no functional paywall. Because GPL recipients can
redistribute binaries, price enforcement cannot be the product moat; trust,
convenience, current signatures, and support are.

### 20.3 Licensing gate

The current project is GPL-3.0-only. Charging for copies is compatible with GPL,
but distributed recipients retain source and redistribution rights. Some store
terms may add restrictions inconsistent with those rights. Before outside
contributors or store submission:

1. Inventory ownership and licenses of core, frontend, fonts, icons, copied
   shadcn-svelte source, Rust/Tauri dependencies, and bundled runtimes.
2. Obtain qualified legal review for each store's current agreement.
3. Decide contribution policy while relicensing remains practically possible.
4. If needed, adopt an explicit compatible exception/dual-license approach
   through a separate authorized decision—not through this design document.
5. Always publish corresponding source and notices for the exact paid build.

This design changes no license and makes no legal guarantee.

## 21. Observability, privacy, and diagnostics

Studio has a separate owner-only bounded log. It never shares the daemon's
rotating file because multiprocess rollover is unsafe.

Allowed structured events:

- startup/readiness/shutdown;
- safe daemon handshake and incarnation change;
- service-start request outcome;
- UI and daemon request IDs;
- normalized safe error code/status;
- polling/cache/preview timing buckets;
- session created, locked, expired—without credential material.

Never log launch URLs/fragments, temporary HTML content, browser bearer,
daemon capability, cookies, operator/provider secrets, request/response bodies,
captions, alt text, filenames, paths, platform responses, or headers.

`GET /api/ui/v1/health` returns Studio version/build hash, safe daemon
compatibility/health, and mutation availability. Diagnostics export is opt-in,
redacted, bounded, locally saved, and previewed before the operator confirms.
There is no telemetry endpoint.

## 22. Performance and resource budgets

Initial design budgets:

| Budget | Target |
| --- | ---: |
| Studio ready-to-health with online daemon | under 3 seconds |
| Initial compressed JavaScript | at most 250 KiB, reviewed exceptions only |
| Default / maximum page | 50 / 100 items |
| Ordinary JSON request body | at most 64 KiB |
| Preview derivative | at most 512 KiB and 1024 px longest edge |
| Concurrent browser requests | 16 maximum |
| Concurrent upstream daemon calls | 1; priority queue depth 64 |
| Concurrent inventory/preview workers | 2 installation-wide |
| Preview worker CPU / wall limit | 6 seconds / 8 seconds |
| Preview worker memory | 512 MiB RSS/address-space hard ceiling |
| Preview worker process / descriptor limit | 1 process; 32 POSIX FDs or 64 Windows handles |
| Preview worker staged input / output | admitted limit or 512 MiB maximum input; one 512 KiB output |
| Active request poll | 1 Hz, stopped at terminal state |
| Idle Studio process | under 150 MiB RSS and 1% CPU on reference host |
| Disposable cache | 256 MiB hard LRU ceiling |
| Health while presentation workers saturated | under 100 ms p95 |
| Durable write acceptance while saturated | under 250 ms p95 |

Performance cannot be bought by bypassing exact scans for publication. Caches
improve presentation only and always carry freshness. Slow/poisoned preview
work cannot starve health, scheduling, publishing, or control operations.

## 23. Verification strategy

### 23.1 Python and contracts

- Frozen dependency lock, Ruff format/check, strict mypy, build, dependency
  audit, and at least the existing 85% coverage gate.
- FastAPI ASGI tests for every DTO, code, limit, header, and lifecycle path.
- Validate UI and control OpenAPI; every handler maps one exact operation ID and
  schema.
- Regenerate TypeScript types and fail on diff.
- Prohibit generic success data and raw control passthrough.

### 23.2 Frontend

- Exact package-manager lock install, formatting, lint, Svelte checks,
  TypeScript strict, Vitest, Testing Library, and dependency/license audit.
- Deterministic static rebuild and wheel/sdist asset verification.
- Component tests for every closed state enum and form validation path.
- Real packaged deep-link execution under FastAPI's build-derived CSP hash
  manifest; any browser CSP console violation fails the test.
- Visual regression at representative desktop/narrow widths, dark/light,
  forced colors, reduced motion, empty/loading/degraded/error states.

### 23.3 Security/adversarial

- Wrong Host/Origin/Fetch Metadata; no CORS; DNS rebinding attempt.
- Malicious server on another localhost port cannot obtain/use a Studio
  credential.
- Ticket replay, expiry, duplicate exchange, log/process-list/history leakage,
  and temporary-file permissions/cleanup.
- Duplicate headers, wrong content type, oversized/slow body, response limit,
  redirect/proxy bypass, traversal and arbitrary route/path injection.
- Caption/filename/error XSS, CSP enforcement, frame denial, no remote assets.
- Daemon capability rotation, endpoint replacement, PID reuse, port collision,
  and incompatible API.
- Every control request rechecks the current capability, including a pooled
  connection after rotation/revocation. Discovery/certificate/incarnation
  changes discard connections and proposals; no derived authority survives.
- Ninth browser-session rejection, TTL reaping, ticket/session capacity
  recovery, and tab-close expiry without a fictitious close notification.
- Browser never receives daemon/operator/provider secrets or absolute paths.

### 23.4 Integration and real browser

Hermetic integration launches the real daemon, real FastAPI process, compiled
Svelte app, Playwright browser, fake providers, fake clock, ephemeral loopback,
and external-socket denial.

Required journeys:

1. Secure launch and session exchange; replay fails.
2. Two profiles never bleed content or schedule state.
3. Dashboard and five content views distinguish empty from blocked scan.
4. Concurrent GUI/agent caption edit produces a preserved-work conflict.
5. Admission and enabled schedule wait for exact terminal approval.
6. Lost `202` response resolves the same idempotent request without duplicate.
7. Daemon restart disables writes, re-handshakes, and recovers durable request.
8. DST fold/gap, `missed`, simultaneous `no_content`, and projection changes.
9. Partial and ambiguous provider outcomes expose only legal recovery actions.
10. Preview bomb, malformed media, timeout, worker saturation, and cache expiry
    preserve control responsiveness. Each native OS job also proves that
    network access, source traversal, child creation, descriptor/handle growth,
    CPU/RSS overflow, and excess output fail closed; a missing sandbox produces
    `preview_sandbox_unavailable` and never an unsandboxed decode.
11. Keyboard, screen reader landmarks, focus restoration, 200% zoom, axe,
    reduced motion, and touch target checks.
12. Both approval policies: agent intent approval/consume when enabled, and the
    trusted operator-origin authorize helper when agent publishing is disabled.
13. A completed executor request with `empty`, `deferred`, `blocked`, `invalid`,
    or `failed` never renders publication success.
14. Invalid/tampered proposal ID, stale revision/fingerprint/targets, expiry,
    duplicate claim/use, two-tab contention, browser Cancel/Lock after claim,
    CLI crash, daemon restart, hostile-port TLS identity, and lost CLI response
    never broaden authority or duplicate a durable request; the same proposal/
    idempotency binding remains observable in activity. Assertions cover safe
    tombstone acknowledgement/TTL reaping, the later `proposal_not_found`
    response, and restart lookup of an already committed durable request.

No live social post is required for CI or GUI release verification.

### 23.5 OS matrix

- Linux: current supported distribution, Chromium and Firefox, systemd-user.
- Windows: native Python/service path and Edge/Chromium; WSL is a documented
  fallback, not the primary Windows deployment.
- macOS: current supported release, Safari/WebKit and launchd.

This browser/service matrix covers simple same-user deployments on all three
OSes. Hardened split-principal deployment additionally requires the current
native policy validation: Linux POSIX policy and Windows protected ACLs.
macOS hardened mode is currently unsupported because the core's native ACL
validator fails closed there; requesting it must show `hardened_unsupported`,
never silently start simple mode. macOS simple mode remains a full web-app
target. Adding hardened macOS later requires its own native ACL/principal
design and tests; a sandboxed preview helper does not establish that support.

Mocked service rendering runs in CI; one approved native browser smoke per OS is
a release gate. Future Tauri adds WebView2, WKWebView, WebKitGTK, code signing,
notarization, updater, sandbox, install/uninstall, and background-service tests.

## 24. Rollout, compatibility, and rollback

1. Land secure read-only gateway, session, handshake, health, and typed UI shell
   against current control endpoints.
2. In one non-shippable compatibility phase, add pinned hardened TLS identity,
   per-request capability auth, exact read projections, preview workers/routes,
   fingerprint-safe edits, inert proposal rendezvous, and trusted authorization;
   update OpenAPI, sibling plugin contract copy/hash/core pin, and old-match/
   new-match integration pairs. None ships independently.
3. Add content views and schedule forecasts.
4. Add Studio editorial and approval experiences over the proven contracts.
5. Add activity/recovery workflows.
6. Package deterministic static assets and cross-platform launcher.
7. Run full hermetic browser and native smoke gates.

Prefer no authoritative SQLite schema change for the first GUI release.
Presentation caches are disposable and separately versioned. If a later domain
change requires schema migration, back up canonical state, fail closed on
unknown versions, test forward recovery from copies, and never promise that
downgrading the wheel reverses the database.

For the no-authoritative-schema-change v1 release, rollback stops Studio,
requests graceful daemon shutdown and waits for its safe boundary, installs the
previous matching core/assets/plugin pair, restarts the daemon, verifies state
schema plus authenticated handshake, and only then reopens Studio. A wheel is
never replaced beneath a running daemon. Any later schema-changing release must
define and test its own migration recovery; this v1 procedure does not promise
backward database migration. Studio and daemon version mismatch renders
diagnostics only; it does not attempt compatibility by guessing.

## 25. Risk register and falsifiers

| Risk | Severity | Design mitigation | Falsifying evidence |
| --- | --- | --- | --- |
| Browser gains daemon/operator authority | High | Separate bearer, allowlisted DTO/actions, terminal approval | Browser can invoke raw route/header or read capability |
| Local cross-port credential theft | High | No auth cookie; one-use launch and origin-scoped tab bearer | Other port obtains a credential accepted by Studio |
| Stale daemon-mediated draft overwrite | High | Viewed fingerprint plus shared mutation serialization; external-writer limit explicit | Second GUI/MCP edit overwrites a changed draft using the old fingerprint |
| Wrong-account approval | High | Exact profile/targets/fingerprint/revision/consequence | Any bound field changes and intent still consumes |
| Unknown response duplicates work | High | Stable idempotency and lookup-before-retry | Dropped response creates a second request/post |
| Preview harms publisher | High | Background bounded derivative, small cached result | Poisoned media raises control/scheduler latency beyond budget |
| Scan error looks empty | High | Explicit blocked issue/freshness model | Failed authoritative scan renders zero items without warning |
| UI offers retry for ambiguity | High | Closed recovery action matrix | Ambiguous state presents or automatically issues Retry |
| Daemon replacement accepted | High | Protected exact certificate pin, TLS, incarnation/installation validation | Stale/reused endpoint enables writes |
| GUI/MCP semantics diverge | High | Same durable operations and coordinated contract journey | Same inputs yield different binding/state outcomes |
| Upgrade damages state | High | Avoid schema change; backups/fail-closed migrations | Older/newer binary mutates unknown schema |
| GUI exit stops schedules | High | Independent daemon/service lifecycle | Closing tab/gateway stops daemon |
| Store promise conflicts with GPL/sandbox | Medium | Legal/store feasibility gate and direct-download fallback | Store terms prevent compliant distribution/service model |
| Visual polish hides state | Medium | Text+icon states, intervention priority, accessibility tests | Operator cannot distinguish pause/queued/ambiguous states |

## 26. Final design decisions and deferred gates

Resolved:

- FastAPI separate gateway; no Flask and no daemon embedding.
- Static SvelteKit/Svelte 5 frontend; Node is build-time only.
- Tailwind 4, owned shadcn-svelte/Bits UI components, Phosphor icons.
- Intervention-first product and weekly pulse calendar.
- Existing bucket semantics; projections rather than invented reservations.
- One-use launch plus origin-scoped sessionStorage bearer; no auth cookie.
- Independent real-TTY approval over core-pinned TLS; no operator secret in
  browser/BFF or agent-principal child.
- Polling first, bounded previews, disposable GUI caches.
- Open-source feature parity and later paid packaging convenience.
- Explicit Tauri `StudioTransport`/`HostBridge` seam with no Rust now.

Deferred operator/product gates:

- Validate the specified dark/light tokens and system typography in rendered
  components before accepting visual baselines; a later brand font is optional.
- Native browser smoke sessions on each OS require scheduled human approval.
- Browser upload/import needs its own staging/ACL/crash-recovery design.
- Native Tauri approval, file import, service installer, updater, and store
  distribution need separate designs.
- App-store licensing/sandbox feasibility requires current agreements and
  qualified review.

## 27. Design acceptance criteria

The design is ready for Stage 3 planning only when a reviewer confirms:

These are specification-completeness checks. The test suites in §23 remain
future implementation/release gates; this design does not claim they ran.

- [x] Every screen maps to current or explicitly required daemon contracts.
- [x] No GUI component reads SQLite, provider secrets, roots, or raw control
      payloads.
- [x] Browser bootstrap specifies localhost, replay, log, history, argv, origin,
      lifetime, and recovery falsifiers with unambiguous expected outcomes.
- [x] Authority flows preserve real-TTY operator approval and hardened
      principal separation.
- [x] Every mutation specifies revision, fingerprint, idempotency, 202 tracking,
      conflict, unknown-outcome, and terminal behavior.
- [x] Content views reflect exact DRAFTS/QUEUE/RANDOM/REELS/POSTED semantics.
- [x] Schedule/calendar copy reflects bucket pulses, DST, misfire, missed, and
      no-content rules without claiming reservation.
- [x] Preview and inventory work is bounded and cannot block publishing.
- [x] GUI/MCP compatibility has a cross-repository release and test contract.
- [x] Visual tokens, components, responsive behavior, motion, and all unsafe/
      empty/degraded states are specified.
- [x] WCAG 2.2 AA and native browser/OS gates are explicit.
- [x] Static packaging preserves a future Tauri shell without runtime Node.
- [x] The public source build has complete functional parity with any paid
      convenience package.
- [x] All high risks have specified executable falsifiers and no material
      contract remains an unstated implementation judgment.

## 28. Primary references

- [Current POST PULSAR control contract](../../api/control-v1.openapi.json)
- [Current core security model](../../SECURITY.md)
- [Svelte](https://svelte.dev/)
- [SvelteKit static adapter](https://svelte.dev/docs/kit/adapter-static)
- [Tailwind CSS v4 installation](https://tailwindcss.com/docs/installation)
- [shadcn-svelte introduction](https://www.shadcn-svelte.com/docs)
- [Phosphor Icons](https://phosphoricons.com/)
- [FastAPI features](https://fastapi.tiangolo.com/features/)
- [Python SSL contexts and certificate validation](https://docs.python.org/3/library/ssl.html)
- [HTTPX explicit SSL context](https://www.python-httpx.org/advanced/ssl/)
- [Browser sessionStorage lifetime and origin scope](https://developer.mozilla.org/en-US/docs/Web/API/Window/sessionStorage)
- [Apple App Sandbox and helper constraints](https://developer.apple.com/documentation/security/protecting-user-data-with-app-sandbox)
- [FastAPI strict content-type/CSRF note](https://fastapi.tiangolo.com/advanced/strict-content-type/)
- [RFC 6265 cookie weak confidentiality](https://www.rfc-editor.org/rfc/rfc6265#section-8.5)
- [Tauri 2 overview](https://v2.tauri.app/start/)
- [Tauri platform prerequisites](https://v2.tauri.app/start/prerequisites/)
- [WCAG 2.2](https://www.w3.org/TR/WCAG22/)
- [WAI-ARIA Authoring Practices](https://www.w3.org/WAI/ARIA/apg/)
- [GNU GPL FAQ](https://www.gnu.org/licenses/gpl-faq.html)
- [Apple Standard EULA](https://www.apple.com/legal/internet-services/itunes/dev/stdeula/)
