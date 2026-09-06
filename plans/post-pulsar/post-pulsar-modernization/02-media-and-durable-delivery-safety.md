# Phase 2: Media and Durable Delivery Safety

## Codebase Anchors

Build media inspection, the SQLite state machine, and exact archival/recovery primitives.

### Task 2.1: Inspect and prepare media safely

Use Pillow verification and bounded `ffprobe` argument-vector calls to identify
real media characteristics and enforce explicit code-owned limits. X accepts
1–4 JPEG/PNG/non-animated GIF images at no more than 5 MiB each; one
animated GIF uses `tweet_gif` at no more than 15 MiB, 1280×1080, 350 frames,
and 300M aggregate pixels; one baseline video is MP4/H.264/AAC-LC/YUV420,
0.5–140 seconds, no more than 512 MiB, 32×32–1280×1024, 60 fps, and aspect
1:3–3:1. Instagram JPEG derivatives are at most 8 MiB, width 320–1440, aspect
4:5–1.91:1, with carousel items normalized to a common ratio; Reels use
MP4/MOV source, H.264/HEVC, AAC 48 kHz, 23–60 fps, at most 1920 horizontal
pixels, 25 Mbps video, 128 kbps audio, 3 seconds–15 minutes, and 1 GiB.
Normalize Instagram images to fingerprinted metadata-stripped JPEGs and emit an
`instagram_image_normalized` warning descriptor whenever bytes, dimensions, or
alpha compositing change; X always receives original bytes. During inspection,
open regular source files without following symlinks and copy/hash the same
stream into fingerprinted private immutable staging. Re-stat device/inode/size
where supported; adapters upload only these verified copies, never a newly
reopened mutable source path. Stage reversible Instagram image/video files only
inside the configured public directory and return immutable `StagedMedia`
descriptors containing relative path, hash, MIME, size, and cleanup policy;
durable checkpointing belongs to the state writer used by `T3.1`/`T4.1`. Form
URLs by percent-encoding one generated filename segment. The lifecycle is
inspect → create verified private/public local staging → read-only
verify the exact public URL → create remote artifacts → final publish → persist
result → cleanup. Reject every redirect or DNS result to loopback, link-local,
private, or otherwise nonpublic addresses; cap redirects and streaming response
bytes, require HTTP 200 plus expected MIME/signature and size/hash where
feasible. This validates operator hosting configuration, not Meta-region
reachability. Missing `ffprobe`, timeout, nonzero exit, malformed/oversized JSON,
and sanitized error paths are explicit tests. Instagram alt-text loss remains a
persisted warning rather than silently claiming support.

**Test:** yes

**Dependencies:**
- T1.2
- T1.3

**Files:**
- `src/post_pulsar/media.py`
- `tests/conftest.py`
- `tests/unit/test_media.py`

**Acceptance:**
- Decoded format, dimensions, animation, video streams/codecs/duration/aspect/size, and target limits are validated before mutation.
- Prepared names are collision-resistant and derived from the fingerprint; no input can be overwritten or deleted.
- `ffprobe` uses no shell and has bounded execution/error output; staging URLs remain beneath the validated public HTTPS base through every redirect/DNS check.
- Staged-media and warning descriptors are deterministic; later orchestration checkpoints them. Hash-matched staging is removed on known terminal outcomes, retained for ambiguous outcomes, and never confused with source input.
- Replace and symlink-swap fault tests prove uploaded bytes are the admitted verified fingerprint, not a mutable path reopened after validation.
- The root autouse socket-denial fixture exists before public-URL tests, and a sentinel proves an unmocked outbound request fails with no platform-host exception.

**Verify-After:**
- `.venv/bin/python -m pytest tests/unit/test_media.py -q` (focused)

### Task 2.2: Implement the SQLite delivery state machine

Create forward-only idempotent SQLite schema initialization for bundles,
immutable target snapshots, exact bundle files, deliveries, and allow-listed
audit/warning events. Enable foreign keys, busy timeout, and WAL when supported.
Delivery phases are `preparing`, `processing`, `ready`, and
`final_dispatch_started`; the last phase must commit before `/2/tweets` or
`media_publish` begins. Expose transactional checkpoint methods for every
returned X media ID and Instagram child/parent container ID, staged relative
path/hash, absolute artifact expiry, and sanitized processing metadata.
Enforce pending/in_flight/published/failed/ambiguous delivery transitions,
active/blocked/archiving/archived bundle transitions, permanent single-use
case-insensitive IDs, target identity/config hashes, timestamps, and
transactional claims. Store monotonic lifetime `attempt_count` separately from
resettable `consecutive_failures`; increment lifetime attempts when an attempt
starts and consecutive failures only when that attempt ends failed. Intermediate
prepare/processing/checkpoint progress never resets it. Reset consecutive
failures only on terminal `published` or after guarded operator retry, and base
backoff/five-failure blocking only on consecutive failures. A test in which
every attempt checkpoints progress then fails at the same later phase must still
block on the fifth failure. Persist and
deduplicate preflight warning codes for later status rendering.

**Test:** yes

**Dependencies:**
- T1.2
- T1.3

**Files:**
- `src/post_pulsar/state.py`
- `tests/unit/test_state.py`

**Acceptance:**
- Schema creation and re-open are idempotent and reject unsupported future user_version values.
- Illegal transitions, snapshot mutation, fingerprint drift, duplicate case-folded IDs, empty target snapshots, and automatic ambiguous retries are rejected transactionally.
- Events and delivery errors contain safe operator context but cannot contain configured secret values.
- Crash tests at every artifact/phase checkpoint prove stale pre-final work is resumable/safely failed and only stale `final_dispatch_started` becomes ambiguous; total and consecutive counters retain their distinct semantics.

**Verify-After:**
- `.venv/bin/python -m pytest tests/unit/test_state.py -q` (focused)

### Task 2.3: Add single-instance locking and exact archival recovery

Implement a state-directory process lock and an archive transaction that moves
only the snapshotted exact members into `<posts_directory>/posted/ID`. Persist
`archiving` before filesystem mutation, create a fingerprinted staging
directory, verify fingerprint/membership, checkpoint each exact member after
`os.replace`, hash-verify the complete staged set, then atomically rename to the
final directory. Define recovery for source-only, staged-only, final-only,
both-identical, both-different, an empty/conflicting destination, and `EXDEV`;
cross-filesystem fallback must copy to a temporary file, fsync, verify hash,
atomically install it, and remove source only after durable verification.
Ambiguous cases never overwrite or delete. Reject symlinks/cross-bundle paths
and never use broad globs.

**Test:** yes

**Dependencies:**
- T2.2

**Files:**
- `src/post_pulsar/locking.py`
- `src/post_pulsar/archive.py`
- `tests/unit/test_archive.py`

**Acceptance:**
- Concurrent invocations fail cleanly without publishing or archiving.
- Only exact snapshotted members can move and all-target published is required before archive begins.
- Fault injection after every database/filesystem boundary converges to one known archived or blocked state under the configured posts root; unrelated inbox files remain untouched.

**Verify-After:**
- `.venv/bin/python -m pytest tests/unit/test_archive.py -q` (focused)
