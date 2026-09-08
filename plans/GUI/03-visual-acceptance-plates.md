---
title: POST PULSAR Studio visual acceptance plates
kind: design-reference
stage: 2
stage_state: done
created: 2026-09-07
---

# POST PULSAR Studio — visual acceptance plates

These low-fidelity compositions are behavioral and hierarchy contracts. They
are not pixel mockups. Stage 3 must preserve the hierarchy; implementation must
produce approved high-fidelity reference screenshots before visual regression
baselines are accepted.

## Plate A — all-profile overview with intervention

```text
┌─────────────────────────────────────────────────────────────────────────────┐
│ ◉ POST PULSAR     [All profiles ▾]        HEALTHY · RUNNING      ⌘K   ● │
├──────────────┬──────────────────────────────────────────────────────────────┤
│ Overview  ●  │ Good evening, Anson                             Observed now │
│ Content      │ Two items need your attention.                               │
│ Schedules    │                                                              │
│ Activity  2  │ ┌─ VERIFY ON PLATFORM ────────────────────────────────────┐ │
│ System       │ │ 360hextile · Instagram · “orbital-study”               │ │
│              │ │ Final response was lost. Do not retry.     [Review →]  │ │
│              │ └─────────────────────────────────────────────────────────┘ │
│              │                                                              │
│              │ NEXT PULSES                                      Next 24 h  │
│              │  now ──●───────○────────●────────────○──────────────►       │
│              │        360     anson     anson        360                   │
│              │        RANDOM  QUEUE     RANDOM       REELS                 │
│              │                                                              │
│              │ CONTENT READY              ACTIVE                           │
│              │  ansonphong       18       1 publishing                    │
│              │  360hextile       11       1 retry scheduled               │
│              │                                                              │
│              │ RECENT ACTIVITY                                  [View all] │
│              │ ✓ X published · ansonphong · atmospheric-03       4m       │
│              │ ↻ Retry at 19:12 · 360hextile · ceramic-loop      8m       │
└──────────────┴──────────────────────────────────────────────────────────────┘
```

Acceptance notes:

- The one ambiguous outcome dominates the page; totals never outrank it.
- Pulse Rail is functional and has an adjacent accessible occurrence list.
- Profile color is never the only identity; every item names the profile.
- Global Running/Paused state stays outside selected-profile chrome.
- No follower/engagement vanity statistics.

Narrow adaptation: drawer navigation; intervention card first; Pulse Rail
becomes a vertical chronological list; content counts become two compact rows.

## Plate B — profile content inventory

```text
┌─────────────────────────────────────────────────────────────────────────────┐
│ CONTENT       [ansonphong ▾]              Search bundles…    Grid ▦  List ☷ │
│ DRAFTS 4   QUEUE 8   RANDOM 6   REELS 2   POSTED 128                    ↻ │
├─────────────────────────────────────────────────────────────────────────────┤
│ QUEUE · canonical ID order              Projected next: atmospheric-03     │
│                                                                             │
│ ┌───────────────────┐ ┌───────────────────┐ ┌───────────────────┐          │
│ │   [image poster]  │ │   [image poster]  │ │  [video poster]   │          │
│ │                   │ │                   │ │                   │          │
│ │ atmospheric-03    │ │ blue-hour-01      │ │ ceramic-loop      │          │
│ │ 3 images · X + IG │ │ image · X         │ │ Reel · IG         │          │
│ │ Caption ✓  Alt ✓  │ │ Caption ✓  Alt —  │ │ Caption ✓         │          │
│ │ ● PROJECTED NEXT  │ │ ○ READY           │ │ ⚠ POSTER MISSING  │          │
│ └───────────────────┘ └───────────────────┘ └───────────────────┘          │
│                                                                             │
│ [Load 50 more]                          Observed 14:03 · scan complete      │
└─────────────────────────────────────────────────────────────────────────────┘
```

Acceptance notes:

- Buckets are tabs, not draggable workflow columns.
- Projected selection is explicit and never called reserved.
- A video-poster failure does not imply an invalid Reel.
- `Scan blocked` replaces the grid with a persistent issue surface; it never
  renders as zero content.
- POSTED differentiates proven publication from imported unknown archive.
- Grid/list preference may persist as non-sensitive presentation state; no
  resource or mutation data is persisted.

Narrow adaptation: one card per row; tabs horizontally scroll with visible
edge affordance; filter opens a sheet; no hover-only controls.

## Plate C — draft editor and approval boundary

```text
┌─────────────────────────────────────────────────────────────────────────────┐
│ ← DRAFTS / orbital-study                     ansonphong · X + Instagram     │
├────────────────────────────────────┬────────────────────────────────────────┤
│                                    │ CAPTION                                  │
│         [large media stage]        │ ┌────────────────────────────────────┐ │
│                                    │ │ Exact editable text…               │ │
│  ◀  1 / 3  ▶                       │ └────────────────────────────────────┘ │
│                                    │ SHARED ALT TEXT                       │
│  1440×1080 · JPEG · 1.8 MiB        │ [Atmospheric ceramic study…        ] │
│                                    │ ⚠ Instagram does not receive this alt │
│                                    │                                        │
│                                    │ VALIDATION                             │
│                                    │ ✓ X: 214 weighted / 280                │
│                                    │ ✓ Instagram: 214 / 2,200               │
│                                    │   3 hashtags · 0 mentions              │
│                                    │                                        │
│                                    │ Fingerprint 8b72…a91c        [Copy]    │
│                                    │ [Save draft]   [Admit to QUEUE…]       │
└────────────────────────────────────┴────────────────────────────────────────┘
```

Admission review:

```text
┌─ Admit exact draft ─────────────────────────────────────────────────────────┐
│ ansonphong → QUEUE                                                         │
│ X @ansonphong · Instagram @ansonphong                                     │
│ orbital-study · 3 images · fingerprint 8b72…a91c [Show full]              │
│                                                                            │
│ This creates an immutable ready copy. The editable DRAFT remains.          │
│                                                                            │
│ Approval is completed in an independent trusted terminal.                 │
│ Only the inert proposal ID is copied; the secret never enters Studio.     │
│                          [Cancel] [Copy terminal command] [Await approval] │
└────────────────────────────────────────────────────────────────────────────┘
```

Acceptance notes:

- Save, Unset, Admit, and Publish are never one ambiguous primary button.
- DRAFTS never shows Publish now.
- Terminal approval renders/recomputes the canonical consequence independently.
- A stale save preserves local content and presents old/current/local values.
- Focus returns to the originating button after dialog close.

## Plate D — weekly pulse calendar

```text
┌─────────────────────────────────────────────────────────────────────────────┐
│ SCHEDULES     [360hextile ▾]       Sep 7–13          [New schedule]        │
├─────────────────────────────────────────────────────────────────────────────┤
│             MON      TUE      WED      THU      FRI      SAT      SUN       │
│ 07:00       ● QUEUE           ● QUEUE           ● QUEUE                    │
│ 12:30                ○ RANDOM          ○ RANDOM          ○ RANDOM           │
│ 18:00                                           ! REELS                     │
│                                                                             │
│ ! REELS · Friday 18:00 America/Vancouver · validation needs attention      │
│                                                                             │
│ NEXT OCCURRENCES                                                           │
│ Sep 8 12:30 PDT · RANDOM · projection may change                           │
│ Sep 9 07:00 PDT · QUEUE · no content possible if prior pulse consumes it   │
│ Sep 10 12:30 PDT · RANDOM · projection may change                          │
│                                                                             │
│ DST TEST FIXTURE — not this displayed week                                 │
│ Mar 8 2026 02:00 America/Vancouver → 03:00; 3600 s counts against grace.   │
└─────────────────────────────────────────────────────────────────────────────┘
```

Acceptance notes:

- Rolling week plus list, not a decorative month calendar.
- Ordering is UTC occurrence, profile ID, schedule ID.
- Schedule-local and operator-local time appear for consequential views.
- DST fold/gap, `missed`, and `no_content` have icon plus full text.
- Existing schedule ID is read-only.
- The visible list is an excerpt; the implemented accessible list represents
  every graphical occurrence in the selected week.

## Plate E — partial/ambiguous recovery

```text
┌─────────────────────────────────────────────────────────────────────────────┐
│ RECOVERY / ceramic-loop                         360hextile · fingerprint…  │
├─────────────────────────────────────────────────────────────────────────────┤
│ X             ✓ PUBLISHED       Remote ID 1938…                 [Open]     │
│ Instagram     ? OUTCOME UNKNOWN Final response was lost                    │
│                                                                             │
│ Do not retry. A retry could publish this Reel twice.                        │
│                                                                             │
│ VERIFY ON INSTAGRAM                                                         │
│ □ Open the exact account @360hextile                                       │
│ □ Search the publication window and compare media/caption                  │
│ □ Record whether the post exists                                           │
│                                                                             │
│ [It is published…]                      [It is not published…]              │
│ Both choices require a new exact terminal authorization.                   │
└─────────────────────────────────────────────────────────────────────────────┘
```

Acceptance notes:

- Retry is absent, not merely de-emphasized.
- Platform success and uncertainty are separate.
- Published reconciliation requires exact remote ID.
- Not-published reconciliation and later Retry are separate approved actions.
- Warning color is supplementary to icon, heading, and explanatory copy.

## Plate F — disconnected/degraded system

```text
┌─────────────────────────────────────────────────────────────────────────────┐
│ SYSTEM                                            STUDIO ONLINE · DAEMON — │
├─────────────────────────────────────────────────────────────────────────────┤
│ ┌─ DAEMON OFFLINE ────────────────────────────────────────────────────────┐ │
│ │ Studio cannot verify the active POST PULSAR daemon. Writes are disabled.│ │
│ │ Service mode: systemd-user                         [Start service]       │ │
│ └─────────────────────────────────────────────────────────────────────────┘ │
│                                                                             │
│ COMPONENT              VERSION / CONTRACT                    STATE          │
│ Studio                0.x · build abc123                    ✓ ready         │
│ Core daemon           expected control hash 7f…             — unavailable   │
│ MCP compatibility     plugin range 0.x                      not observable  │
│ ffprobe               required video inspection             ✓ available     │
│ ffmpeg                optional poster generation            — unavailable   │
│                                                                             │
│ [Copy redacted diagnostics]              No telemetry · GPL-3.0 source ↗   │
└─────────────────────────────────────────────────────────────────────────────┘
```

Acceptance notes:

- Studio availability is not daemon/provider health.
- No cached view appears current; any retained data is stamped stale.
- MCP client presence is explicitly not observable.
- `ffprobe` and poster-generation capability are distinct.
- Incompatible versions offer diagnostics/upgrade guidance only.

## Theme and forced-color reference

| Semantic role | Dark | Light | Forced-colors behavior |
| --- | --- | --- | --- |
| Canvas | graphite | warm near-white | `Canvas` |
| Primary text | near-white | near-black | `CanvasText` |
| Accent/selection | pulsar cyan | deep cyan | `Highlight`/`HighlightText` |
| Success | green + check + text | deep green + check + text | text/icon/border |
| Warning | amber + warning icon + text | ochre + icon + text | text/icon/border |
| Danger | coral + stop icon + text | deep red + icon + text | text/icon/border |
| Focus | two-color 2 px outer/inner ring | same semantic ring | system focus color |

Palette values in the main design are provisional until every state passes
WCAG contrast. Light mode retains the same hierarchy and density. Forced-color
mode removes decorative orbital lines and keeps borders, state text, icons, and
focus visible.

## Anti-template checks

Implementation fails visual acceptance if it:

- looks like an unmodified shadcn dashboard block;
- leads with four oversized number cards;
- uses purple/blue gradient fog, glass panels, or decorative charts;
- represents every label as a pill or every section as an elevated card;
- hides profile identity behind color/avatar alone;
- makes unsafe and successful states differ only by hue;
- uses perpetual pulses/spinners where static state is sufficient;
- treats the bucket view as draggable Kanban;
- sacrifices caption/media workspace to navigation chrome;
- lacks the functional Pulse Rail and intervention hierarchy.
