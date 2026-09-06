# Stage 4 — Hardened design amendments

Date: 2026-09-05

Status: approved superseding decisions

This record preserves the Stage 2 design review while making the execution
contract unambiguous. Where this file differs from `02-design.md`, these
decisions and the generated phase plans are authoritative.

## Publication boundary and checkpoints

The adapter interface is `preflight` (read-only), `prepare` (resumable remote
artifacts), and `commit` (one final public-create request). `prepare` accepts
prior state and a state-owned callback that transactionally stores every X
media ID/expiry and every Instagram child/parent container ID plus staged-file
descriptors. Delivery phases are `preparing`, `processing`, `ready`, and
`final_dispatch_started`; the final phase commits before `/2/tweets` or
`media_publish`. A stale final-dispatch phase is always ambiguous. Earlier
stale phases may resume or safely fail. A final request is never automatically
repeated.

SQLite stores monotonic lifetime `attempt_count` and a separate
`consecutive_failures`. Intermediate progress never resets the failure count;
only terminal publication or a successful guarded operator retry does. Five
consecutive failures block the bundle. Warning events are allow-listed,
redacted, deduplicated, durable, and visible through status.

## Verified media lifecycle

Inspection opens source files without following symlinks and copies/hashes the
same byte stream into fingerprinted private immutable staging. Adapters upload
only those verified bytes. Instagram derivatives additionally enter the
configured public staging root. The lifecycle is inspect → reversible local
staging → bounded exact public-URL verification → remote preparation → durable
final-dispatch marker → one final request → result persistence → state-aware
cleanup. Known terminal results clean hash-matched staging; ambiguous results
retain evidence. Normalization/alpha compositing and Instagram alt-text loss
produce persistent warnings.

Public URL checks permit only a fixed HTTPS base with no userinfo, query,
fragment, IP literal, or traversal; every redirect and DNS result is checked
against loopback, link-local, private, and other nonpublic destinations.
Responses are bounded and must match expected status, MIME, signature, and
size/hash where feasible. This validates operator hosting configuration, not
Meta-region reachability.

Archival first persists `archiving`, moves exact members into a fingerprinted
staging directory with per-file checkpoints/hash verification, then atomically
renames to `<posts_directory>/posted/ID`. Source/staged/final conflicts and
`EXDEV` have explicit fail-closed recovery; no unknown file is overwritten or
deleted.

## Current official platform details

X status polling is `GET /2/media/upload?command=STATUS&media_id=ID`.
Checkpointed media expiry may authorize safe re-upload only before final
dispatch and only from unchanged verified bytes. Image alt text is NFC
normalized, image-only, and at most 1,000 characters. Current conservative
non-entitlement media constraints in the phase plan are code-owned and tested.

Instagram Login preflight uses `GET /v26.0/me?fields=user_id,username` and
`GET /v26.0/{user_id}/content_publishing_limit?fields=quota_usage,config`.
Protected-edge access establishes capability; no undocumented `account_type`
field or hard-coded quota is used. Reels set `share_to_feed=true`. Captions are
NFC normalized and checked before staging/container mutation against 2,200
code points, 30 hashtags, and 20 mentions. Uncertain `media_publish` status
polling never authorizes another publish and never invents a missing media ID.

## Command, recovery, and verification ordering

Local settings load separately from publishing credentials. `status` and
operator-confirmed `reconcile` require no token/network. All state-mutating
commands and migrations take the same process lock. `run` recovers stored work
before evaluating current enabled targets; immutable snapshots govern resumes.
At least one enabled target is required only to admit a new bundle.

Legacy modules/config/dependencies/scripts remain runnable through the new
package build and are removed atomically with the operational script cutover.
Tests deny sockets beginning with the first network-capable media tests. CI
allows network for dependency installation and vulnerability metadata only;
the test process has no live platform or general outbound access. A pinned
`uv` in the verifier environment owns the lock workflow. `T5.3` owns the last
tracked modernization-report update using actual Stage 5 evidence; broad gates
remain a report-only Stage 6/CI responsibility and their actual results appear
in the final review response.
