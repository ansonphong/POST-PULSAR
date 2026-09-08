<!-- Grok Build confirming review; model grok-4.6; reasoning xhigh; read-only. -->

I'll review the revised POST PULSAR Studio design against the first Grok findings and the current contracts, read-only. Starting with the hardening notes, round-1 review, and the changed design docs.Round-1 review lives under `plans/GUI/reviews`. Next I’ll load it with the current design contracts and farm independent checks.Farming independent slices for TLS, preview/forecast, session/source, and visuals while I read the current design contracts.Subagent spawn is blocked, so I’ll continue the review directly: remaining design sections and the source contracts they claim to close.Continuing through authority, preview, mutation, forecast, and the source claims they depend on.Design contracts are in. Next I’ll check the live core paths those G/C closures depend on so leftover holes are source-backed, not assumed.# FULL REVIEW — POST PULSAR Studio (Stage 2 confirming pass)

**Verdict: `PASS`**
**Endorse the revised product shape: yes.**
**Endorse the revised technical shape: yes.**
**Ready for Stage 3 implementation planning: yes.**

This is specification-readiness, not runtime fidelity. There is still no GUI, gateway, TLS listener, proposal resource, or preview helper in tracked source. `spawn_subagent` was denied again; this is one full pass over current `01`–`05`, plates, round 1, `git diff 932ef71 -- plans/GUI`, and the cited core (`control.py`, `daemon.py`, `state.py`, `scheduler.py`, `secure_files.py`, `cli.py`, `SECURITY.md`, OpenAPI/bootstrap). Credentials, `.env`, ignored configs, sessions, logs, media, auth caches, and `CLAUDE.md` were not opened. Prior `PASS` / `95.85` remains withdrawn; `04` is correctly `UNDER_REVIEW`.

---

## G1 / G4 decisions

**G1 — accept the conductor’s alternative.** One pinned TLS 1.3 + existing per-request capability bearer in **both** simple and hardened, with no HMAC/challenge/derived control session, is specific enough for Stage 3. §7.5 closes algorithm, leaf profile, SANs, lifetime, discovery object, pin-before-HTTP-bytes, stopped rotation, socket bounds, and the required test list. §8 closes proposal canonical hash encoding. Simple first setup auto-runs `control tls initialize`; hardened runs it after ACL provisioning. Recycled-port protection of the operator secret and a single CLI/MCP/Studio implementation are load-bearing; they are not theater. I withdraw the round-1 simple-mode cleartext split. Do not defer the hardened full GUI. Remaining TLS work is implementation of that table, not crypto invention.

**G4 — accept the conductor’s alternative.** It is a coherent, scope-preserving response. Metadata-only is an early usable shell, not completion of the designed media experience. Image previews stay in the intended GUI on supported reference hosts; video posters stay optional; source builds share capability code and fail closed when the helper/self-test is missing. Native containment is a per-platform **release proof gate**, not a claim that signed helpers exist in every checkout, and not a requirement that three OS sandboxes ship before observation/edit/scheduling. Do not narrow the requested visual/media product to metadata-only for planning convenience.

---

## Finding verdicts

| ID | Verdict | Evidence |
| --- | --- | --- |
| **G1** | **Closed** | §§7.4–7.5, 9.3.18, 23.3, 24. HMAC language is removal, not deferral. Current source is still cleartext `HTTPServer` + bearer compare (`daemon.py`, `control.py:720–728`); that is the named new contract, not a missing decision. |
| **G2** | **Closed** | §11.3 steps 1–6; plates D; §§12.2, 12.5. `resolve_occurrence` over matching local dates; rail `(now, now+24h]` cap 12; week = seven browser-local dates clipped in UTC (max 169h); editor next 5 / 35 dates; no consumption/RANDOM-counter simulation. Matches `scheduler.py:81–128` and `_work_order`. |
| **G3** | **Closed** | §§7.2, 10.3; plates B/C. `sessionStorage` bearer; ≤32 observation-only receipts; no caption/action body/offline queue; F5 restores via server verification; restore/expiry/gateway-restart caveats are explicit. |
| **G4** | **Closed** (alternative accepted) | §9.5 items 4, 12; §23.4.10; §23.5; plate C dimensions note. |
| **G5** | **Closed** | §5.2 table covers every `bootstrap.schema.json` mode plus hardened overlay and unknown metadata; plate F is simple `systemd-user` only; invariant 10 is on the plate. |
| **G6** | **Closed** | §10.1 XOR `{"text":…}` / `{"unset":true}`; `If-Match` = profile revision; viewed fingerprint = content; last-member reject. Current `TextEdit` `{text}` (`openapi.json`) is the coordinated schema gate. |
| **G7** | **Closed** | §13.4; plate A. Cmd+K / Ctrl+K; closed navigation/search/profile/Lock set; TimeField and PulseRail widget contracts. |
| **G8** | **Closed** | §11.3. Browser IANA, UTC fallback labelled; daemon computes occurrences; JS only formats supplied UTC instants. |
| **C1** | **Closed** | §10.2; plate C. Journals permanently reserve `(profile, bucket, bundle_id)` and fingerprint-per-profile. Matches `admission_journals` UNIQUE keys and `start_admission` conflict on changed fingerprint (`state.py:744–764, 4426–4528`). No replacement/version invention. |
| **C2** | **Closed** | §11.3; plate D. History = persisted `schedule_runs` only; offline dates show `No execution record`. Matches `evaluate_schedule` current-local-date-only. |
| **C3** | **Closed** as a required core change | §8, §9.3.19. Today `/confirmations/{id}/consume` will consume an operator-origin intent with only the agent bearer (`control.py:585–626`; `test_operator_can_originate_and_consume_intent_when_agent_publish_is_off`). Design now requires operator auth on every consume/replay of operator-origin intents. That is an implementation/contract gate, not an open product choice. |
| **C4** | **Closed** | §23.5. macOS simple is a full web target; hardened is Linux/Windows only; requested hardened on macOS is `hardened_unsupported`, never a silent simple start. Matches `secure_files.py:_reject_posix_acl` non-Linux fail-closed. |
| **C5** | **Closed** | §10.1. Daemon-mediated edits share one per-bundle serialization boundary; two scans + `os.replace` (`cli.py:1580–1649`) are not CAS against an uncooperative folder writer; local text preserved; admission still copies/verifies exact fingerprints. |
| **C6** | **Closed** | §8. Two principals (core/operator vs agent/Studio). Trusted authorize CLI uses core file rights. No third identity. Matches `RecordPolicy` and `SECURITY.md`. |

Round-1 X1–X7 are closed by the same edits (`04` no longer claims PASS; plate F; forecast; TLS specified as new; accelerators; TTY summary; admit = proposal under default policy).

---

## Remaining findings

**No known material design gaps.** A Stage 3 planner does not have to invent crypto, forecast consumption, unset wire shape, start commands, or a third principal.

Two consistency nits, not blockers:

1. **§8 leftover “daemon session epoch Studio observed.”** HMAC sessions are gone. Hash that field as core-stamped `installation_id` + `startup_nonce` from discovery (already rechecked at claim). Do not put the BFF’s opaque `daemon_session` in the canonical proposal.
2. **§7.2 “rotate” vs §9.2.** There is no session-rotate route. Slide idle on authenticated traffic, keep the same bearer until absolute 8h / Lock / restart; or add `POST /session/rotate`. Either is fine; pick one sentence.

---

## Gates, not defects

Unchanged from round 1, and now honestly labelled: typed UI/control schemas; profile/inventory/POSTED/caption projections; viewed-fingerprint writes + `change` union; `authorize` + harden existing approve; inert proposals; §7.5 TLS in daemon/CLI/MCP together; C3 consume enforcement; per-OS preview proof; `web` extra (still absent from `pyproject.toml`); contrast/hi-fi; native browser smoke; MCP coordinated pin; store/legal. `_bundle.archive_path` must still never reach the browser.

Do not force those implementations in this design stage, and do not drop requested GUI functionality to avoid them.

---

## Spec-readiness (specification quality, not executed GUI)

| Dimension | Weight | Score /10 | Why |
| --- | ---: | ---: | --- |
| Visual specification | 30% | **8.0** | Plates A–F + dark/light tokens + anti-template + widget contracts. ASCII plates and unproven contrast remain named gates. |
| Interaction / state | 25% | **8.6** | Authority, idempotency, restart/tombstones, F5 receipts, forecast, edit union, admission occupancy, truthful history. |
| Components / system | 25% | **8.6** | One daemon, BFF, static Svelte, closed TLS, MCP peer, Tauri seam, service matrix, bounded preview capability. |
| Accessibility | 20% | **8.2** | WCAG 2.2 AA, skip/lang, OS accelerators, native TimeField, occurrence list, forced-colors. Contrast still unmeasured. |
| **Weighted** | | **8.4 / 10** | |

---

**Stage 3 may plan against this design.** Keep `02` `stage_state` active until planning starts; do not treat `04`’s old 95.85 as evidence. Implementation must still prove §7.5, C3, preview isolation, and the listed projections — those are release/contract proofs of an already-chosen shape.

[stopReason=end_turn — run ended without a clean EndTurn]
