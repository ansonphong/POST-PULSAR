# Phase 2: Media and Durable Delivery Safety

## Codebase Anchors

Build media inspection, the SQLite state machine, and exact archival/recovery primitives.

### Task 2.1: Inspect and prepare media safely

Use Pillow verification and bounded ffprobe argument-vector calls to identify real media characteristics and enforce code-owned X/Instagram limits. Normalize Instagram images to fingerprinted metadata-stripped JPEGs, stage image/video files only inside the configured public directory, form quoted HTTPS URLs, and enforce input-path exclusion and cleanup policies. Keep Instagram alt-text loss as a visible warning rather than silently claiming support.

**Test:** yes

**Dependencies:**
- T1.2
- T1.3

**Files:**
- `src/post_pulsar/media.py`
- `tests/unit/test_media.py`

**Acceptance:**
- Decoded format, dimensions, animation, video streams/codecs/duration/aspect/size, and target limits are validated before mutation.
- Prepared names are collision-resistant and derived from the fingerprint; no input can be overwritten or deleted.
- ffprobe uses no shell and has bounded execution; staging URLs remain beneath the validated HTTPS base.

**Verify-After:**
- `.venv/bin/python -m pytest tests/unit/test_media.py -q` (focused)

### Task 2.2: Implement the SQLite delivery state machine

Create forward-only idempotent SQLite schema initialization for bundles, immutable target snapshots, exact bundle files, deliveries, and audit events. Enable foreign keys, busy timeout, and WAL when supported. Enforce pending/in_flight/published/failed/ambiguous delivery transitions, active/blocked/archiving/archived bundle transitions, permanent single-use case-insensitive IDs, target identity/config hashes, monotonic attempts, timestamps, sanitized errors, and transactional claims.

**Test:** yes

**Dependencies:**
- T1.2
- T1.3

**Files:**
- `src/post_pulsar/state.py`
- `tests/unit/test_state.py`

**Acceptance:**
- Schema creation and re-open are idempotent and reject unsupported future user_version values.
- Illegal transitions, snapshot mutation, fingerprint drift, duplicate case-folded IDs, and automatic ambiguous retries are rejected transactionally.
- Events and delivery errors contain safe operator context but cannot contain configured secret values.

**Verify-After:**
- `.venv/bin/python -m pytest tests/unit/test_state.py -q` (focused)

### Task 2.3: Add single-instance locking and exact archival recovery

Implement a state-directory process lock and an archive transaction that moves only the snapshotted exact members into <posts_directory>/posted/ID. Verify fingerprint and membership immediately before archiving, reject symlinks/destination collisions/cross-bundle paths, record archiving before filesystem mutation, and reconcile restart states without broad globs or destructive guessing.

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
- Interrupted moves reconcile to a known archived or blocked state under the configured posts root and unrelated inbox files remain untouched.

**Verify-After:**
- `.venv/bin/python -m pytest tests/unit/test_archive.py -q` (focused)

