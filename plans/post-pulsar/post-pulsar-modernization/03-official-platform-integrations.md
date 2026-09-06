# Phase 3: Official Platform Integrations

## Codebase Anchors

Replace Tweepy and the private Instagram client with narrow official HTTP adapters and mocked contracts.

### Task 3.1: Define sanitized adapter and HTTP contracts

Define the fixed X/Instagram adapter protocol, `ValidationIssue`,
`PreparedPublication`, `PublishResult`, and safe retry classification. Split the
adapter lifecycle into read-only `preflight`, resumable `prepare`, and one-shot
`commit`. `prepare` accepts prior prepared state plus a state-owned checkpoint
writer and must checkpoint every returned upload/container ID, expiry, staged
file, and processing transition before proceeding. `commit` may be called only
after the orchestrator durably marks `final_dispatch_started` and performs
exactly one final public-create request with no internal retry. Build a
requests-based transport with bounded connect/read timeouts,
Retry-After-aware pre-final retry handling, redacted diagnostics, response-size
bounds, and Authorization-header-only tokens. Inherit the autouse socket-denial
fixture established by `T2.1`; no platform-host exception is allowed. Keep the
legacy BasePoster runnable until the atomic `T5.1`
operational cutover and prevent raw response bodies or headers from becoming
operator messages.

**Test:** yes

**Dependencies:**
- T1.2
- T1.3
- T2.1

**Files:**
- `src/post_pulsar/platforms/__init__.py`
- `src/post_pulsar/platforms/base.py`
- `src/post_pulsar/platforms/http.py`
- `tests/unit/test_http.py`

**Acceptance:**
- Adapters return only published, failed, or ambiguous terminal results with sanitized stable error codes.
- Tokens appear only in Authorization headers and are removed from exceptions, logs, and repr output.
- The transport distinguishes safe pre-final retries from uncertain final create/publish dispatch.
- All contract tests fail closed on any real socket; prepared artifacts and phases have an explicit durable checkpoint interface and `commit` cannot retry internally.

**Verify-After:**
- `.venv/bin/python -m pytest tests/unit/test_http.py -q` (focused)

### Task 3.2: Implement the official X API v2 adapter

Implement an official X API v2 adapter alongside the still-runnable legacy
module, using OAuth 2.0 user-context bearer authentication. Perform read-only
expected-account verification before mutation and weighted text validation.
Before any upload/checkpoint mutation, NFC-normalize image alt text, require at
most 1,000 characters, and reject alt text for video or animated GIF media.
Use simple image upload or chunked media initialize/append/finalize, then poll
exactly `GET /2/media/upload?command=STATUS&media_id={id}`; reject legacy
command-style INIT/APPEND/FINALIZE calls against the simple endpoint. Apply
shared alt metadata per image and make final `POST /2/tweets` only through
`commit`. Checkpoint each media ID plus `expires_after_secs` converted to a
wall-clock expiry with a conservative clock-skew margin. Before
`final_dispatch_started`, unchanged-source expired artifacts are discarded and
safely re-uploaded; after final dispatch, expiry never downgrades ambiguity or
authorizes another post. Bound polling/backoff, honor Retry-After, classify 4xx
failures and post-dispatch network/5xx outcomes correctly, and never repeat an
uncertain final create.

**Test:** yes

**Dependencies:**
- T2.1
- T3.1

**Files:**
- `src/post_pulsar/platforms/x.py`
- `tests/contract/test_x.py`

**Acceptance:**
- Text, images, and existing video support use only documented v2 endpoints and media IDs.
- Chunk numbering, both required status query parameters, finalize/status polling, timeouts, metadata, expiry/reuse boundaries, and final payloads match mocked official contracts.
- Wrong account identity blocks before mutation and ambiguous final-create outcomes are never automatically repeated.
- Alt-text 1,000/1,001 boundaries and image versus video/animated-GIF eligibility are preflighted before any upload.

**Verify-After:**
- `.venv/bin/python -m pytest tests/contract/test_x.py -q` (focused)

### Task 3.3: Implement the official Instagram adapter

Implement the official Meta Instagram API adapter alongside the still-runnable
legacy module, pinned to graph `v26.0`. For Instagram Login, verify identity with
`GET /v26.0/me?fields=user_id,username`, comparing returned `user_id` to the
configured target, then query
`GET /v26.0/{user_id}/content_publishing_limit?fields=quota_usage,config` and
parse `data[0].quota_usage`, `config.quota_total`, and
`config.quota_duration`; successful protected-edge access is the Professional
capability check and no undocumented `account_type` field or hard-coded quota
is used. Verify every staged public URL through the bounded nonpublic-address
and byte checks from `T2.1`, create single image, carousel, or Reel containers,
and poll status. Reel creation must include `media_type=REELS` and
`share_to_feed=true` to preserve legacy feed visibility. Call `media_publish`
once only through `commit`. After an uncertain response, bounded read-only
status polling maps `PUBLISHED` to ambiguous with unresolved remote ID,
`FINISHED` or deadline to ambiguous, `IN_PROGRESS` to continued bounded polling,
and `ERROR`/`EXPIRED` to recorded evidence requiring explicit reconciliation;
none authorizes an automatic second publish. Retain staging/container IDs for
every ambiguous result, clean only hash-matched pre-publication/known-terminal
staging, persist alt/normalization warnings, and classify uncertainty without
automatic retry.
Before staging or container creation, NFC-normalize the caption and enforce at
most 2,200 Unicode code points, 30 hashtags, and 20 `@` mentions, with exact
boundary contract tests and zero staging, HTTP mutation, or delivery-artifact
checkpoint mutation on rejection. The orchestrator may persist the sanitized
terminal validation failure against an already-admitted target snapshot.

**Test:** yes

**Dependencies:**
- T2.1
- T3.1

**Files:**
- `src/post_pulsar/platforms/instagram.py`
- `tests/contract/test_instagram.py`

**Acceptance:**
- The new `post_pulsar.platforms.instagram` path uses only the official Meta endpoint and bearer header and contains no private login/session import or credential behavior; repository-wide legacy removal occurs in `T5.1`.
- Image, carousel, and existing feed-video behavior map to exact official identity/quota/container/status/publish shapes with public media URLs and `share_to_feed=true`.
- Quota, account mismatch, private/redirected/mismatched public bytes, every documented status, container failure, timeout, warning persistence, and ambiguous `media_publish` cases produce safe deterministic results.
- Caption length/hashtag/mention boundaries reject invalid content before staging, container creation, HTTP mutation, or delivery-artifact checkpoint mutation while permitting a sanitized validation-failure state record.

**Verify-After:**
- `.venv/bin/python -m pytest tests/contract/test_instagram.py -q` (focused)
