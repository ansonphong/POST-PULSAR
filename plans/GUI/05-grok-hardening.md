---
title: POST PULSAR Studio Grok design hardening
kind: design-review
stage: 2
created: 2026-09-08
design_ref: 02-design.md
review_base: 932ef71
verdict: PASS
stage_state: done
---

# POST PULSAR Studio — independent Grok hardening

## Scope and evidence

The user requested a full Grok/Codex review and design repair until both endorse
the product and technical shape for implementation planning. This remains
Stage 2. Production code, runtime configuration, database schema, plugin
implementation, and implementation task plans are outside this pass.

The review covers the brainstorm, design, visual acceptance plates, evaluation,
and relevant tracked core and sibling MCP client source. Existing numerical
scores are historical judgments, not proof of correctness or executed tests.

Grok round 1 uses Grok Build `grok-4.6`, extra-high reasoning, high review budget,
and enforced read-only mode. The conductor independently checks source behavior
and will disposition every finding before requesting a confirming Grok review.

## Conductor findings — all closed at design level

| ID | Severity | Evidence and concrete issue | Required design disposition |
| --- | --- | --- | --- |
| C1 | Medium | `state.py:start_admission` permanently binds profile/bucket/bundle ID and rejects a changed fingerprint at that destination. Design §10.2 currently offers an updated-version admission without this constraint. | Show occupied destination conflicts; require an eligible unused destination or a new externally prepared semantic bundle ID. Never imply replacement or automatic version creation. |
| C2 | Medium | `scheduler.py:evaluate_schedule` evaluates only the current local date. A daemon offline for whole days has no recorded occurrences for them. | Separate observed durable `missed` events from absent history; never synthesize proof of missed execution from today's recurrence rule. |
| C3 | High | `control.py` currently allows capability-authenticated consumption of operator-origin intents. The proposed trusted CLI flow says Studio cannot consume them but does not require the corresponding core denial. | Require operator authentication for operator-origin consumption, including all existing consume routes and replay paths; agent-origin consume remains its separate policy. |
| C4 | Medium | `secure_files.py:_reject_posix_acl` rejects non-Linux POSIX hardened mode. | Specify macOS simple-mode support separately from hardened Linux/Windows; no automatic downgrade of a requested hardened installation. |
| C5 | Medium | Two fingerprint scans plus filesystem replace are not atomic compare-and-swap against an uncooperative external DRAFTS writer. | Define guaranteed concurrency for daemon-mediated edits, external-editor coordination/limits, preserved local text, and admission's independent exact hash checks. |
| C6 | Medium | §8 runs the approval CLI as core but also claims the operator cannot write core-owned discovery. Existing hardened mode has two principals, not three. | Retain trusted core/operator file rights and deny discovery writes to agent/Studio; do not invent a third principal or process isolation from trusted core tools. |

Corrections for C1–C6 are in design version 2 and closed by Grok's confirming pass.
Small consistency corrections also distinguish browser credentials from the
secrets forbidden in DTOs, distinguish ephemeral UI state from durable domain
mutations, remove the superseded terminal-broker alternative from the
brainstorm, and move an implementation DST fixture out of the visible UI plate.

## Requirement coverage for final acceptance

| Requirement | Authoritative design evidence | Independent code/release evidence checked |
| --- | --- | --- |
| One local operator, multiple accounts, simple structure | §§1–6, 11–12; brainstorm product thesis and GRUG constraints | `config.py` profile/target model; daemon ownership |
| Python backend, current Svelte/Tailwind/shadcn-svelte/Phosphor | §§6, 13; dated stable-version baseline | `pyproject.toml`; official Svelte static-adapter/CSP documentation |
| Buckets, random selection, sequencing, scheduling/calendar | §§9–12; plates A–D | `content.py`, `scheduler.py`, selection/admission/schedule records in `state.py` |
| Effective MCP integration and concurrent editing | §§10, 18 | Core OpenAPI; sibling MCP client at `78c7ebe131a8c18a24e3f0084085bad1c479c742` |
| Authority, conflicts, duplicate prevention, safe recovery | §§7–10, 17, 21, 23, 25 | `control.py`, `cli.py`, `state.py`, `SECURITY.md` |
| Minimalist functional visual design and accessibility | §§12–17; all six visual plates | Specification review; actual browser/visual/a11y gates remain unexecuted |
| Web app on Windows/macOS/Linux and future Tauri | §§5–6, 14, 19, 23–24 | Existing native identity/ACL constraints; official Apple helper-sandbox guidance |
| Full open source, possible one-time paid convenience | §20; brainstorm distribution contract | Current GPL-3.0-only metadata; future store/legal and signing gates remain explicit |
| Ready for implementation planning, still design only | Final disposition and exact reviewed-file evidence below | Scoped diff must contain design documents only |

## Grok round 1 and dispositions

The [complete Grok review](reviews/grok-round-1.md) returned
`CONDITIONAL_PASS`: it endorsed the product shape but required technical changes.
Grok Build returned `is_error=false`, exit 0, session
`01a08283-e2e0-7f10-ad21-f866d7b1a9b9`, in 720,622 ms. Its internal delegation
attempt was denied by the worker's policy, so this was one Grok review, not a
swarm. The runner labelled lower-case `end_turn` as non-clean in a note despite
the successful result; the complete substantive review was available and read.

| ID | Disposition in revised design | Rationale |
| --- | --- | --- |
| G1 | Replace custom HMAC/derived sessions with standard TLS 1.3 and current per-request capability checks; exact §7.5 certificate/discovery/provisioning/rotation/bounds/tests; exact proposal hash encoding in §8 | Accept the missing protocol contract and excess complexity. Retain one pinned transport in both modes to avoid credential disclosure on recycled ports; do not defer hardened GUI functionality or leave a planner to invent crypto. Grok explicitly accepted this alternative in round 2 and withdrew its suggested cleartext split. |
| G2 | Exact forecast algorithm/windows/caps/overflow in §11.3; no consumption simulation; plate D corrected | Rail, week, and editor use one core primitive with explicit presentation bounds. |
| G3 | Same-origin tab sessionStorage auth, bounded observation-only pending receipts, leave warning, restore via server verification | F5 no longer needs a terminal or resubmits work. Restoration/expiry limits are explicit; no caption/action payload or offline queue is stored. |
| G4 | Preview capability gated independently per OS; metadata-only shell is an early milestone, not completion of the designed media experience; no invented dimensions or unsandboxed fallback | Accept per-platform availability and honest metadata. Do not remove working image previews from the user's final intended GUI merely to simplify delivery. Grok explicitly accepted this scope-preserving alternative in round 2. |
| G5 | Closed service-mode/Start table; Windows-service/manual/hardened instructions only; corrected plate F and closure copy | No guessed service command or principal crossing. |
| G6 | Exact fingerprint plus XOR `change` body; explicit unset/no-op/last-member behavior in §10.1 | No null/empty-file ambiguity or invented draft revision. |
| G7 | Cmd+K/Ctrl+K and closed commands; CommandPalette, TimeField and PulseRail behavior in §13.4 | Functional keyboard contract, not unexplained chrome. |
| G8 | Browser-reported IANA display zone, UTC fallback; daemon computes occurrences | Display conversion never changes scheduling. |

Grok's X1–X7 contradictions are covered by the superseded evaluation, corrected
service/forecast/accelerator tables, explicit approval-policy route mapping, and
summary of terminal-approved consequential actions. C1–C6 were independently
confirmed against current source and integrated alongside those findings.

The confirming review accepted G1/G4 on their merits, checked the repaired
contracts together, and returned PASS. This is design consensus, not a claim
that the proposed implementation exists.

## Focused documentation verification

- `git diff --check` passes for the current design edits. The changed/untracked
  task artifacts are confined to `plans/GUI`; no production code or plugin
  implementation was edited.
- Relative Markdown file links resolve. A scoped scan of the new design/review
  documents found no private-key blocks or common GitHub/xAI/OpenAI/AWS token
  patterns; this is not a full historical repository secret audit.
- Service-mode enum was compared directly with `bootstrap.py` and
  `api/bootstrap.schema.json`; all five modes are accounted for.
- Primary references for the new transport/storage decisions were inspected:
  [Python ssl](https://docs.python.org/3/library/ssl.html),
  [HTTPX SSL contexts](https://www.python-httpx.org/advanced/ssl/), and
  [sessionStorage](https://developer.mozilla.org/en-US/docs/Web/API/Window/sessionStorage).
  The particular local protocol is our design choice, not an endorsement by
  those projects.
- Relative-luminance calculations for the specified token pairs gave these
  ratios: dark primary 17.03:1, dark muted 6.99:1, dark accent-button text
  10.65:1, dark control boundary 3.91:1; light primary 17.66:1, light muted
  6.11:1, light accent-button text 6.51:1, light control boundary 3.84:1.
  These eight static pairs pass their text (4.5:1) or control (3:1) threshold;
  they do not prove every rendered state or WCAG conformance.
- No runtime test suite was run for this documentation-only change. All
  protocol, filesystem race, provider, preview, browser, and native verifiers
  in the design remain future implementation/release gates.

## Final agreement and Stage 2 exit

The [confirming Grok review](reviews/grok-round-2.md) returned **PASS** and
explicitly endorsed both the revised product and technical shape for Stage 3.
Grok reported no known material design gaps and marked every G1–G8 and C1–C6
closed. The conductor independently agrees based on the current documents and
the source evidence in this record.

Round 2 provenance: Grok Build `grok-4.6`, extra-high reasoning, read-only,
exit 0, `is_error=false`, session
`01a08299-7a05-7610-b22d-7b8f177b7192`, duration 482,751 ms. Its internal
delegation attempt was again denied; both reviews were single Grok passes.
The original raw reports are preserved unchanged. The runner's lower-case
`end_turn` note does not override their successful, complete result bodies.

Two nonblocking consistency corrections follow the exact options Grok approved:

1. §8 canonical proposals bind core-stamped installation ID/startup nonce;
   the BFF's opaque browser epoch is not canonical core identity.
2. §7.2 authenticated traffic slides idle expiry only; the bearer is stable until
   expiry/Lock/restart. No new rotation endpoint is needed.

The reviewed main design before these two corrections had SHA-256
`9044f437df6ab61af4caaf53e27e9cbcd8f93020499062d048ab649553f02521`.
The only subsequent substantive design changes are those approved corrections;
evaluation/report and stage metadata were then finalized. The confirming
review's suggestion to leave Stage 2 active until planning starts is a workflow
opinion, not a product finding: planctl records this completed design stage,
while Stage 3 remains unstarted.

| Reviewer | Product shape | Technical shape | Planning readiness |
| --- | --- | --- | --- |
| Grok | Endorsed | Endorsed, including G1/G4 alternatives | PASS |
| Conductor | Endorsed | Endorsed after source checks and both nit corrections | PASS |

The conductor rubric is 8.35/10; Grok's dimension scores yield 8.34/10
(the report's printed 8.4 total is an arithmetic/rounding slip). Both meet the
Stage 2 B threshold. These are qualitative specification assessments, not
runtime evidence or probabilities.

All requirement rows above have design/source or review evidence. No requested
GUI capability was removed merely to obtain a passing review. Implementing
the proposed protocols, previews, UI, MCP changes and their tests remains
future work; no Stage 3 task plan or production implementation was created.
