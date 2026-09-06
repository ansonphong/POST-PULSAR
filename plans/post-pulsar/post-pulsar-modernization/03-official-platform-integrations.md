# Phase 3: Official Platform Integrations

## Codebase Anchors

Replace Tweepy and the private Instagram client with narrow official HTTP adapters and mocked contracts.

### Task 3.1: Define sanitized adapter and HTTP contracts

Define the fixed X/Instagram adapter protocol, ValidationIssue, PublishResult, safe retry classification, and a requests-based transport with bounded connect/read timeouts, Retry-After-aware pre-final retry handling, redacted diagnostics, response size bounds, and Authorization-header-only tokens. Remove the legacy BasePoster abstraction and prevent raw response bodies or headers from becoming operator messages.

**Test:** yes

**Dependencies:**
- T1.2
- T1.3

**Files:**
- `src/post_pulsar/platforms/__init__.py`
- `src/post_pulsar/platforms/base.py`
- `src/post_pulsar/platforms/http.py`
- `tests/unit/test_http.py`
- `post_base.py`

**Acceptance:**
- Adapters return only published, failed, or ambiguous terminal results with sanitized stable error codes.
- Tokens appear only in Authorization headers and are removed from exceptions, logs, and repr output.
- The transport distinguishes safe pre-final retries from uncertain final create/publish dispatch.

**Verify-After:**
- `.venv/bin/python -m pytest tests/unit/test_http.py -q` (focused)

### Task 3.2: Implement the official X API v2 adapter

Replace XPoster/Tweepy with official X API v2 calls using OAuth 2.0 user-context bearer authentication. Perform read-only expected-account verification before mutation, weighted text validation, simple or chunked media initialize/append/finalize/status polling, per-image shared alt metadata, and final POST /2/tweets. Bound polling/backoff, honor Retry-After, classify 4xx failures and post-dispatch network/5xx outcomes correctly, and never repeat an uncertain final create.

**Test:** yes

**Dependencies:**
- T2.1
- T3.1

**Files:**
- `src/post_pulsar/platforms/x.py`
- `tests/contract/test_x.py`
- `post_x.py`

**Acceptance:**
- Text, images, and existing video support use only documented v2 endpoints and media IDs.
- Chunk numbering, finalize/status polling, timeouts, metadata, and final payloads match mocked official contracts.
- Wrong account identity blocks before mutation and ambiguous final-create outcomes are never automatically repeated.

**Verify-After:**
- `.venv/bin/python -m pytest tests/contract/test_x.py -q` (focused)

### Task 3.3: Implement the official Instagram adapter

Replace InstagramPoster/instagrapi username-password sessions with the official Meta Instagram API pinned to a documented graph version. Verify expected professional account identity and publishing limits read-only, create single image, carousel, or Reel containers from staged public HTTPS media, poll container status within bounds, and call media_publish once. Clean pre-publication staging safely, retain ambiguous-publication staging, surface unsupported publish-time alt text, and classify final dispatch uncertainty without automatic retry.

**Test:** yes

**Dependencies:**
- T2.1
- T3.1

**Files:**
- `src/post_pulsar/platforms/instagram.py`
- `tests/contract/test_instagram.py`
- `post_instagram.py`

**Acceptance:**
- Only the official Meta endpoint and bearer header are used; private login/session code and dependencies are absent.
- Image, carousel, and existing feed-video behavior map to official container/status/publish contracts with public media URLs.
- Quota, account mismatch, container failure, timeout, and ambiguous media_publish cases produce safe deterministic results.

**Verify-After:**
- `.venv/bin/python -m pytest tests/contract/test_instagram.py -q` (focused)

