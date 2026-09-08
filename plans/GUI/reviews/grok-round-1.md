<!-- Grok Build review evidence; model grok-4.6; reasoning xhigh; read-only. -->
<!-- Review base: 932ef71; conductor revisions began during the review. -->

I'll run a read-only Stage 2 design review: inspect the four GUI design docs and core source, farm independent review slices, then synthesize a structured verdict.Farming independent review slices and reading the four design docs plus core evidence in parallel.Subagent spawn is blocked, so I'll review the four design docs and core source myself.Continuing through the rest of the design spec and the current core contracts.Next I’ll check the live control contract, scheduler/DST behavior, and confirmation/admission paths against the design claims.Independent Stage 2 review is complete. Prior PASS/99% is not treated as evidence. Subagent farm was blocked by policy, so this is one full-document + tracked-source pass.# FULL REVIEW — POST PULSAR Studio (Stage 2, read-only)

**Verdict: `CONDITIONAL_PASS`**
**Endorse product shape for Stage 3: yes.**
**Endorse the technical bundle as written for Stage 3: no, not until G1–G6 are written back into `02-design.md`.**

This is specification-readiness, not runtime fidelity. There is no GUI implementation. Prior `PASS` / `0.99` / `95.85` in `plans/GUI/04-design-evaluation.md` is a claim, not evidence. Independent `spawn_subagent` slices were requested and **denied by policy**; this review is a single pass over the four GUI docs plus tracked core, `SECURITY.md`, `pyproject.toml`, and `api/control-v1.openapi.json`. Credentials, `.env`, `config.json`, sessions, logs, auth caches, media, and `CLAUDE.md` were not opened.

---

## Coverage

| Domain | Docs | Source evidence | Result |
| --- | --- | --- | --- |
| Product thesis / GRUG / OSS vs $20 | `01` §thesis, GRUG, paid seam; `02` §§1–3, 20 | `LICENSE.md` GPL-3.0-only; `pyproject.toml` `license = "GPL-3.0-only"`; no web extra yet | Product shape is right. Paid convenience is not a feature gate. |
| IA / five buckets / profiles | `01` jobs 1–8; `02` §11–12; plates A–B | `README.md` `ansonphong` / `360hextile`; `content.py` QUEUE/RANDOM/REELS; `control.py:405` buckets omit POSTED | Semantics match core. Inventory APIs are missing (already listed as required additions). |
| Sequencing / RANDOM / QUEUE | `02` §4 inv. 8–9, §11.2; plate B | `state.py:1455–1537` QUEUE = min ID; RANDOM = `_random_selection_score(profile, counter)` without increment on preview | Design is truthful **if** projection stays daemon-side. |
| Calendar / DST / misfire | `02` §11.3; plate D | `scheduler.py:81–128`; `tests/unit/test_scheduler.py:50–72` (Vancouver fold 2026-11-01, gap 2026-03-08) | DST **rules** match. Forecast **API/algorithm** does not exist and is underspecified (G2). |
| Principal / browser / daemon identity | `02` §§5, 7–8; `01` authority table | `control.py:720–728` Bearer file compare; `daemon.py:476–485` cleartext `HTTPServer`; no HMAC/TLS; `SECURITY.md:54–72` simple-mode same-user bound | New identity protocol is a core rewrite, and the wire contract is not actually specified (G1). |
| Confirmation / idempotency / races | `02` §§8, 10; plate C, E | `allow_agent_publish` default false (`config.py:275`); `_PUBLISH_ACTIONS` (`control.py:45–57`); CLI approve sends secret over HTTP (`cli.py:1791–1817`); no `authorize` / proposal resource | Proposal state machine is detailed. Canonical hash/TLS still invented at plan time (G1). Default admit still needs a TTY (intentional, must stay explicit). |
| Edit / admission conflicts | `02` §10.1–10.2 | Caption fingerprint stamped from **current disk** at ingress (`control.py:656–669`), not viewed copy; `_edit_draft_text` always writes bytes (`cli.py:1580–1649`); `TextEdit.text` required string (`openapi.json:2065–2076`) | Viewed-fingerprint conflict is a real current hole; design names it. Unset wire shape is not chosen (G6). |
| Preview sandbox / resources | `02` §9.4–9.5 | `media.py` already runs in-process Pillow + `ffprobe` on the publish path; `previewBundle` returns `"preview": bool` (`control.py:445`) | Three-OS sandbox as a web-release ship requirement is out of scale for v1 (G4). |
| Start/stop / packaging / MCP | `02` §§5, 6, 18, 19, 24; plate F | `cli.py:88–107` no `web`; bootstrap modes include `windows-service` (`bootstrap.py:17–18`); systemd/launchd templates exist; MCP is a sibling peer (`SECURITY.md:147–187`) | Independent daemon lifecycle is sound. Start-command matrix is incomplete (G5). |
| Visual / a11y / components | `02` §§13–15; plates; `04` scores | No frontend; plates are ASCII hierarchy, not high-fidelity | Spec is directional, not closed (rubric below). |
| `04` PASS record | `04` frontmatter + §Hardened review record | Those Astra/Sol reviews are not in this repo | **Not evidence.** |

---

## What is sound (keep)

The product thesis is the right size: a local cockpit over one daemon, not a second publisher, not SaaS, not Kanban.

Concrete matches to tracked core:

- Closing Studio must not stop publishing (`02` §1, §4.10) vs daemon instance lock + independent systemd/launchd/task (`daemon.py`, `service/*`).
- QUEUE next = canonical ID order; RANDOM next = durable score from selection counter; displayed next is a projection (`state.py:1455–1537`). Do not implement this in the browser.
- DST: repeated local time → earlier instant; missing time → first valid minute; gap seconds count against grace (`scheduler.py:91–127`). Plate D’s Vancouver 02:00→03:00 / 3600s fixture is consistent with that function (the unit test uses 02:30 / 1800s; same rule).
- Missed is recorded, not backfilled (`scheduler.py` only admits **today’s** local date).
- Default `allow_agent_publish = false` matches `config.py` and `SECURITY.md`. Browser never gets the operator secret.
- MCP as a peer of the same control contract, not a GUI wrapper (`02` §18; `SECURITY.md` MCP section).
- Tauri is an interface seam (`StudioTransport` / `HostBridge`), not a v1 product.
- OSS full features vs ~$20 signed convenience is consistent with GPL-3.0-only (`02` §20; `LICENSE.md`).
- BFF must not proxy `/shutdown` (agent bearer may shut the daemon today: `openapi.json:24–27`). That omission in `/api/ui/v1` is correct.
- Recovery matrix (no retry on ambiguous; reconcile ≠ retry) matches `app.py` blocked/ambiguous handling.

The previous evaluation’s **product IA** is mostly right. Its **completeness and scoring** are not.

---

## Prior PASS / 99% / 95.85

`plans/GUI/04-design-evaluation.md` frontmatter (`verdict: PASS`, `confidence: 0.99`) and the 94/97/96/97 table are **withdrawn as evidence**.

Reasons:

1. `02-design.md` is still `stage_state: active` (line 5) while `04` is `stage_state: done`. The design is not self-declared complete.
2. `04` asserts HMAC/TLS “test vectors” and “no remaining high/medium findings.” The HMAC/TLS **wire format is not in the design or in the checked-in control contract**.
3. Visual 94/100 is scored against ASCII plates and a provisional dark palette whose contrast is explicitly unproven.
4. The “three independent review lanes (Astra/Sol)” are not artifacts in this repository.

Treat `04` as an earlier opinion. This review replaces it.

---

## Findings

Severity: **blocker** = Stage 3 would have to invent a security/protocol or would ship the wrong v1; **major** = a planner would invent user-visible behavior; **minor** = should be pinned, reasonable default exists; **nit** = polish.

Classification: **material** = fix in the design before planning; **gate** = already named as implementation/release work; **optional** = nice, not required.

### G1 — Control identity protocol is v1-blocking and not actually specified
**Severity:** blocker · **Class:** material

**Claim:** Studio v1 depends on a new daemon handshake: HMAC challenge/session, capability generations, and TLS 1.3 with a core-owned SPKI pin (`02` §7.4 lines 379–413; §8 lines 538–551). `04` treats this as closed.

**Current truth:** `_authenticate` is a loopback `Authorization: Bearer` compare against the capability file (`control.py:720–728`). The listener is cleartext `HTTPServer` (`daemon.py:476–485`). Discovery (`control_identity.py`, `EndpointRecord`) has no pin, cert, generation, or session. OpenAPI `servers[0].url` is `http://127.0.0.1:{port}/control/v1`.

**Failure sequence:** A Stage 3 planner writes “implement HMAC+TLS” and must invent MAC algorithm, canonical encoding, nonce size, header names, challenge TTL, replay cache, TLS key type, SAN (`127.0.0.1` only?), validity, which discovery file holds the SPKI pin, rotation atomicity, and test vectors. `02` line 399 says those “are part of the checked-in control contract.” They are not checked in. Line 549’s “same protocol where available” in simple mode is undefined.

**Why this is also the wrong v1 scope:** `SECURITY.md:54–72` already says simple same-user mode does not defend against another process as that user. Studio in simple mode is the same principal talking to itself. The **load-bearing** new boundary is: browser never sees the daemon capability; Origin/Host checks; allowlisted `/api/ui/v1`; operator secret stays on a real TTY. HMAC sessions + loopback TLS are load-bearing for **hardened split-principal** operator CLI identity, not for a single-owner local web app.

**Smallest correction:** Split v1 by `deployment_mode`.

- **simple (default, Anson’s box):** keep today’s per-request bearer; BFF holds it; browser never sees it; no TLS/HMAC/generation requirement.
- **hardened:** either (a) a real contract appendix (HMAC-SHA256, encodings, headers, TTLs, cert profile, pin field, rotation, vectors) **before** Stage 3, or (b) hardened Studio is read/edit-only in v1 and approval stays `post-pulsar confirmations approve` as today.
- Keep the **authorization-proposal** rendezvous in both modes — that is the actual missing product piece so the browser can stage admit/publish without `allow_agent_publish=true`.

Do not start Stage 3 while GUI v1 is defined as “also rewrite control authentication.”

---

### G2 — Pulse forecast bounds and consumption simulation are three different specs
**Severity:** major · **Class:** material

**Conflict:**

| Place | Bound / behavior |
| --- | --- |
| `02` §12.2:1026 | Pulse Rail = **next 24 hours or next 12 occurrences** |
| `02` §12.5:1101 | Editor = **next five** occurrences |
| `02` §9.3 item 8 | “Core-computed next occurrences and occurrence history” — no horizon |
| Plate D:154–156 | “no content possible if prior pulse consumes it” (simulated consumption) |

**Current truth:** `evaluate_schedule` only evaluates **the local date currently visible** (`scheduler.py:111–128`). There is no forecast API, no history API, no `no_content` projection. `no_content` is an execution transition after a due pulse finds an empty candidate set (`app.py:326–333`).

**Failure sequence:** Implementer A walks 24h of instants; B emits 12 marks; C simulates QUEUE consumption across the week (including RANDOM counter increments). Pulse Rail, editor, and Overview disagree. A “reserved-looking” empty warning appears for a pulse that would still have content because an in-flight publish failed.

**Smallest correction:** Pin one algorithm in `02` §11.3:

1. Reuse `resolve_occurrence` over successive matching local dates.
2. Pulse Rail: occurrences with `scheduled_at` in `(now, now+24h]`, cap **12**, overflow “N more” → Schedules. Tie-break already stated: UTC time, profile ID, schedule ID (`02:1107–1109`; matches `_work_order` in `scheduler.py:274–275`).
3. Editor: next **5** instants for that rule only.
4. **Do not simulate bundle consumption in v1.** Replace Plate D’s consumption sentence with the already-correct “projection may change.” Optional coarse copy: if enabled pulses for that profile+bucket in the window **exceed current eligible inventory count**, show “later pulses may find no content” — a count, not a named bundle.

---

### G3 — In-memory browser session makes F5 a terminal round-trip
**Severity:** major · **Class:** material (UX vs stated job)

**Claim:** Job 8: “Stay out of the terminal most of the time” (`01:66–67`). Session: bearer only in JS memory; hard refresh requires `post-pulsar web --open` (`02:304–335`). Cookies rejected with a real reason (RFC 6265 port non-isolation; `02:300–302`).

**Failure sequence:**

1. Operator is in DRAFTS, caption dirty or Save in flight (`202` queued).
2. F5, crash-restore, or “reopen last tabs.”
3. Bearer is gone. Session-loss screen sends them to the terminal.
4. In-memory idempotency key is also gone (`02:849–854` already forbids transparent replay after gateway/browser loss).
5. They Save again with a new key. If the first request completed, viewed-fingerprint conflict (good). If it is still queued, two edit requests exist.

For a local cockpit you leave open, this is worse than the CLI for **observation**. XSS is already assumed mitigated by text nodes + strict CSP (`02:354–364`); an in-memory bearer is equally stealable from an XSS’d page while it is open. `sessionStorage` does not leak to other localhost ports.

**Smallest correction:** Allow **`sessionStorage` only** for the Studio bearer (still no cookies, `localStorage`, URL, or logs). Tab close still drops it. Keep the 30-minute idle / 8-hour absolute server expiry. Document: same-origin, CSP-gated. Hard refresh must not require a TTY.

---

### G4 — Three native preview sandboxes are specified as a web-release ship, not a fail-closed gate
**Severity:** major · **Class:** material (scope vs GRUG)

`02` §9.5:737–744: “The web release **ships and self-tests**” Linux namespaces+seccomp, a signed macOS App Sandbox helper, and Windows AppContainer+Job Object. Fail-closed to metadata is already specified.

Publishing already decodes operator media in-process (`media.py` Pillow + `ffprobe`). GUI preview is not a new threat class for the daemon’s publish worker; it is extra presentation work.

**Failure sequence:** Stage 3 becomes three OS-native sandbox products before Content grid thumbnails exist. Or someone “temporarily” decodes in the BFF/daemon HTTP thread and misses the 100 ms health / 250 ms write falsifiers (`02:783–788`) on the single-threaded listener (`daemon.py` non-threaded `HTTPServer`).

**Smallest correction:** v1 derived previews are **optional**. Default is MIME / size / hash / role, with a static placeholder. Native sandbox helpers are a **later release gate**. Never unsandboxed decode for GUI. Publishing stays on existing `media.py`. This matches the design’s own fail-closed path; delete the sentence that the web release ships the helpers.

Related: Plate C shows `1440×1080 · JPEG · 1.8 MiB`. Current inspect returns size/MIME/hash, not dimensions (`control.py:428–446`). **Dimensions/duration are preview-derived.** When the sandbox is down, do not invent them.

---

### G5 — Daemon start matrix contradicts hardened/manual and misses `windows-service`
**Severity:** major · **Class:** material

- Plate F: systemd-user **and** `[Start service]` (`03:207–211`).
- `02` §5.1.6: hardened/manual → **do not** spawn; render instructions.
- `02` §5.2: allowed starts are `systemctl --user start`, `launchctl kickstart`, `schtasks.exe /run` only.
- `api/bootstrap.schema.json:19` and `bootstrap.py:17–18` also have **`windows-service`** and **`manual`**.

**Failure sequence:** Hardened install shows Start service; Studio, running as agent, tries `systemctl --user` or equivalent and either fails closed (confusing) or crosses principals (the thing `SECURITY.md:74–108` exists to prevent). On Windows SCM (`windows-service`), there is no designed start command; a planner will guess `sc start` or misuse `schtasks`.

**Smallest correction:** One closed table in `02` §5.2:

| `service.mode` | Studio Start control |
| --- | --- |
| `systemd-user` / `launchd-agent` / `windows-task` | Button only if `deployment_mode=simple` |
| `windows-service` / `manual` / `hardened` any mode | Instructions only; no button |

Update Plate F to a simple-mode plate plus a hardened plate without Start.

Also state in System copy: **closing this tab does not stop scheduled publishing.** That is invariant 10; it is not on a plate.

---

### G6 — Caption/alt unset is required but has no wire shape
**Severity:** major · **Class:** material (small, but a planner will invent it)

`02` §9.3 item 10 and §10.1:818: “replacement text or explicit null/unset”; whitespace-only rejected.

Current: `TextEdit` **requires** `text: string` (`openapi.json:2065–2076`); executor always `os.replace`s bytes (`cli.py:1580–1649`). Empty string would create/keep an empty file, not unset.

**Smallest correction:** Closed body is exactly one of `{"text": "<non-empty, non-whitespace>"}` or `{"unset": true}`. Reject `text: null`, `""`, and whitespace. Unset deletes the caption/alt member if present and is fingerprint-preconditioned like Save.

(The viewed-fingerprint check itself is already a named daemon addition — **gate**, not a new finding.)

---

### G7 — Command palette is chrome without a command set; accelerator is Mac-only
**Severity:** minor · **Class:** material

Plate A and `02:979` show `⌘K`. Component list includes `CommandPalette` (`02:1236`). Windows/Linux are first-class (`02` §14, §23.5). No command inventory.

**Smallest correction:** Accelerator is Ctrl+K / Cmd+K. Closed v1 commands: go Overview/Content/Schedules/Activity/System; switch profile; focus bundle-ID search; Lock session. No arbitrary actions, no shell.

---

### G8 — Operator-local timezone source is unnamed
**Severity:** minor · **Class:** material

`02` §11.3:958: show schedule-local and “the operator’s current local equivalent.” Not stated whether that is the browser TZ, a Studio setting, or a profile.

**Smallest correction:** Display conversion uses the **browser’s current IANA timezone**. Scheduling and DST always use the schedule’s stored timezone via the daemon. Never compute occurrences in JavaScript.

---

## Already-defined implementation / release gates (not defects)

These are real work, but `02` already names them. Do not re-litigate as architecture holes:

| Gate | Evidence now | Design already says |
| --- | --- | --- |
| Typed success schemas (today `Envelope.data` is unconstrained) | `openapi.json:887–901` `"data": {}` | `02` §9.3 item 1; `01:104` |
| Profile projection (timezone, usernames, readiness) | `_profile` is `{profile_id, revision}` (`control.py:1055–1058`); timezone lives in TOML (`config.py:129–134`) | §9.3 item 2 |
| Paginated five-bucket inventory + POSTED | `control.py:405` names only DRAFTS/QUEUE/RANDOM/REELS; inspect needs `bundle_id` | §9.3 items 3, 7 |
| Caption text / validation in detail | inspect is hashes/members only (`control.py:428–446`) | §9.3 item 4 |
| Viewed-fingerprint editorial writes | stamped at ingress (`control.py:656–669`) | §10.1 |
| `post-pulsar authorize` + harden existing approve (today: secret first, no canonical re-render) | `cli.py:1791–1817`; `_COMMANDS` has no `web`/`authorize` | §8, §9.3 items 15–17 |
| Inert proposals + tombstones | not in source | §8 |
| Preview bytes route | `"preview": true` boolean | §9.5 |
| `web` extra, `ui-v1.openapi.json`, `web_dist` | `pyproject.toml` has no `web` extra; package-data is `py.typed` only | §6 |
| Palette/font contrast and license | provisional hex (`02:1178–1191`) | §26, `04` remaining gates |
| Coordinated MCP plugin hash/release | `SECURITY.md:184–187`; Phase 6 is a sibling plan | §18, §24.2 |
| Native browser smoke per OS | none | §23.5 |
| Store/legal before promising an app store | GPL-3.0-only | §20.3 |

`_bundle` currently includes `archive_path` (`control.py:1077`). The BFF must never pass that through (`02` §4.12, §7.4 last paragraph). That is a gate, not a new decision.

---

## Cross-document contradictions

| ID | A | B | Resolution |
| --- | --- | --- | --- |
| X1 | `02` `stage_state: active` | `04` `stage_state: done` + PASS | Keep `02` active until G1–G6 are amended; do not treat `04` as closed. |
| X2 | Plate F always `[Start service]` | `02` §5.1.6 hardened/manual: no start | G5 |
| X3 | Pulse Rail 24h **or** 12 vs editor next 5 vs Plate D consumption | one calendar product | G2 |
| X4 | `04`: identity “specified” with test vectors | `02`: vectors “will be” in a contract that does not exist | G1 |
| X5 | Plates `⌘K` | Windows/Linux primary (`02` §14) | G7 |
| X6 | Job 8 “out of the terminal most of the time” | default policy: every Admit/Publish/Enable/Retry/Resume needs a TTY (`02` §8, plate C) | Not a spec bug if the design **summary** says: v1 GUI is inspect/edit/forecast; consequential mutation is TTY-approved. Do not let a planner “one-click Admit.” |
| X7 | `02` §9.2 `POST .../admit` “Create/consume exact admission intent” | §8 default path stages an **inert proposal**, CLI creates the intent | Fix the table: browser never creates/consumes operator intents when `allow_agent_publish=false`. |

DST plate vs `scheduler.py` is **not** a contradiction (see coverage).

---

## GUI feature → current control API

| Studio surface | Current `control/v1` | Gap type |
| --- | --- | --- |
| Session / launch ticket | none | new (Studio-only) |
| Overview / Pulse Rail | `/dashboard`: pause flags, `due_work`, profile **count** | missing aggregates + forecast |
| Profiles | id + revision | missing timezone/usernames/readiness |
| Content tabs | bucket **names** only; no POSTED | missing inventory |
| Bundle detail | hashes/members; no caption body, no dimensions | missing |
| Preview image | boolean `preview` | missing |
| Save caption/alt | PATCH exists; fingerprint is disk-now; no unset | partial |
| Admit / publish / retry / reconcile | confirmation + TTY; no proposal | missing rendezvous |
| Schedules list/edit | rules CRUD; no occurrences | missing forecast |
| Activity timeline | per-profile `/requests` | missing unified descending feed |
| Pause/resume | exists; resume confirmation-conditional on due work | map through proposal in default policy |
| Health compatibility | `/health` = callback failures | different meaning; don’t conflate |
| Shutdown | exists for any bearer | **must not** appear in `/api/ui/v1` |
| MCP | same daemon contract | keep peer; coordinate plugin when control changes |

---

## Questions a planner would otherwise invent

Answer these in `02` (short bullets), then plan:

1. **Simple vs hardened Studio v1:** G1 split — yes or no?
2. **Forecast:** instants only, G2 bounds — yes or no?
3. **sessionStorage:** G3 — yes or no?
4. **v1 thumbnails:** metadata-only unless sandbox exists — yes or no?
5. **Start button:** G5 table — yes or no?
6. **Unset body:** G6 XOR schema — yes or no?
7. **Operator TZ:** browser IANA — yes or no?
8. **⌘K command list:** G7 — yes or no?
9. **Does `POST /api/ui/v1/.../admit` ever create an agent intent, or only a proposal when `allow_agent_publish=false`?** (Must be the latter.)
10. **DRAFTS lost-update token:** viewed fingerprint is authoritative; `If-Match` is profile revision only to catch config changes (profile revision does not move on caption save). Confirm.
11. **RANDOM projection:** must call daemon `preview_selected_bundle` semantics **without** incrementing the counter. Never hash IDs in the BFF.
12. **MCP v1:** plugin ships in the same compatibility phase as any new control operations (proposals, forecasts, inventory). No GUI-only mutation.
13. **`windows-service`:** out of Studio start allowlist unless separately designed.
14. **Light canvas hex** can stay a release gate; do not block Stage 3 on final brand color.

---

## Spec-readiness rubric (not runtime fidelity)

| Dimension | Weight | Score /10 | Why |
| --- | --- | --- | --- |
| Visual specification | 30% | **6.5** | Hierarchy plates A–F, anti-template, dark tokens, density, forced-colors table. Light theme has **no hex**. Pulse Rail is ASCII. Font gated. Command palette empty. |
| Interaction / state | 25% | **7.0** | Resource/action matrix, 202≠success, recovery, pause, bucket truth, DST rules are strong. Session death, forecast simulation, identity handshake, unset wire are holes. |
| Components / system | 25% | **7.0** | Owned shadcn/Bits inventory, one enum-driven status, BFF topology, MCP peer, Tauri seam. `PulseRail` / `TimeField` / `CommandPalette` have names without widget contracts. |
| Accessibility | 20% | **7.5** | WCAG 2.2 AA, keyboard, focus return, landmarks, reduced motion, accessible occurrence list, non-color state. Contrast unproven; Mac-only ⌘K; no skip-link/lang pin. |
| **Weighted** | | **6.95 / 10** | |

Previous 95.85 / A is not a spec-readiness score.

---

## Product vs terminal friction (explicit)

Under default `allow_agent_publish=false` (current TOML default):

| Job | Visual in v1? |
| --- | --- |
| Orient, switch `ansonphong` / `360hextile`, inspect media, edit caption/alt, see QUEUE/RANDOM/REELS/POSTED, see pulses, see ambiguous recovery | **Yes**, if G1–G6 are closed and the listed **gates** are implemented |
| Admit, publish-now, enable schedule, resume-with-due-work, retry, reconcile | **TTY** `post-pulsar authorize pprop_…` (or today’s approve path until proposals exist) |

That is a coherent single-owner security choice, not a GUI that replaces the terminal. It **does** remove the current need to mentally parse CLI status/schedules/requests and to open folders to see captions. It does **not** make Admit a primary one-click button (plate C is correct).

If the operator wanted zero TTY for Admit, that would be `allow_agent_publish=true` plus existing `confirmations approve` — still a TTY, just a shorter one. Do not weaken that in Stage 3.

---

## Simplicity vs extra security layers

**Keep (load-bearing, GRUG-justified):** separate FastAPI process; daemon remains publisher; no GUI SQLite; no cookies on localhost; Origin/Host; allowlisted DTOs; no optimistic success; TTY for publication-enabling actions; MCP peer; polling; static Svelte.

**Do not put on the v1 critical path (theater or unspecified):** loopback TLS+HMAC for simple mode; capability-generation session cache (current code already re-reads the file every request); three-OS preview sandboxes; file:// launch HTML as the only happy path (TTY human code must be first-class, not a WSL footnote).

---

## Verdict

| Question | Answer |
| --- | --- |
| Product shape (intervention cockpit, five true buckets, weekly pulses, BFF + static Svelte 5, daemon authority, MCP peer, Tauri later, OSS full features, ~$20 convenience only) | **Endorse** |
| Technical shape as currently written (GUI v1 = new TLS/HMAC control plane + native sandboxes + memory-only session + underspecified forecasts) | **Do not endorse** |
| Ready for Stage 3 implementation planning | **Not until G1–G6 are amended in `02-design.md`** (G7–G8 can ride in the same patch) |
| Residual after that | Named **gates** (inventory, typed schemas, proposals, authorize CLI, POSTED, fingerprint writes). Those are implementation, not missing product decisions. |
| `04` PASS | **Rejected** |

**Stage 3 entry bar:** `02` `stage_state` stays `active` until G1–G6 are in the document with closed tables/algorithms; then a new evaluation may say PASS. Do not plan tasks that invent crypto, forecast consumption, or OS sandboxes.

[stopReason=end_turn — run ended without a clean EndTurn]
