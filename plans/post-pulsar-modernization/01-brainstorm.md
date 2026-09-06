# Stage 1 — Brainstorm synthesis

Date: 2026-09-05

Verdict: **CONVERGED**

## Product boundary

Preserve the existing intended workflow: one invocation selects one local
content bundle and publishes its caption plus images or video to each enabled X
and Instagram target. Manual and OS-scheduled operation, shared alt text,
logging, and post archival remain. This modernization does not add platforms,
post controls, a scheduler, a UI, hosting, or analytics.

## Why a dependency bump is insufficient

The old implementation has broken X video upload, private Instagram
username/password automation, prefix-based bundle collisions, archive-on-any-
success data loss, no durable remote IDs or retry state, misleading scheduling
and logging, plaintext credentials with unsafe local permissions, stale Threads
documentation, a copied 2024 transitive dependency set, and no tests. These are
contract failures that package upgrades alone cannot repair.

## Converged direction

1. Use `POST PULSAR` for the human name, `POST-PULSAR` for the repository,
   `post-pulsar` for the distribution/CLI, `post_pulsar` for Python imports, and
   `POST_PULSAR_` for environment variables. Preserve Anson Phong attribution.
2. Replace the flat script layout with a small `src/post_pulsar` package split
   into configuration, content parsing, state/orchestration, logging/locking,
   and two explicit platform adapters. This is not a general plugin system.
3. Use official X API v2 endpoints for image/video media upload, metadata,
   processing-status polling, and post creation. Use supported user-context
   authentication and retain returned remote IDs.
4. Replace `instagrapi` private login/session behavior with the official Meta
   Instagram API for Professional accounts. Publish containers, poll their
   status, and publish only when ready. Map local media names to an operator-
   supplied public HTTPS base URL because Meta fetches image/carousel media.
5. Keep the legacy filename workflow but parse it exactly. A portable bundle ID
   owns only `ID.txt`, `ID-alt.txt`, `ID.<media>`, or contiguous numbered images
   `ID-1.<media>` through `ID-N.<media>`. Reject ambiguity, symlinks, case
   collisions, mixed numbered/unnumbered media, and image/video mixes.
6. Use SQLite for a target snapshot and per-platform states (`pending`,
   `in_flight`, `published`, `failed`, `ambiguous`). Retry only known-safe
   failures, never automatic ambiguous creates, and archive exact members only
   after every snapshotted target is published.
7. Add a single-instance cross-platform lock, deterministic preflight for every
   target before the first upload, rotating redacted logs, and idempotent archive
   recovery under `posts/posted/ID/`.
8. Put non-secret settings in `post-pulsar.toml`; load credentials only from
   `POST_PULSAR_*` environment variables or the operator's secret manager.
   Remove secret-copy scripts and private Instagram session files.
9. Target Python 3.12+, declare only direct runtime dependencies in
   `pyproject.toml`, generate an exact dated lock, and add current lint, typing,
   test/coverage, build, and dependency-audit tooling plus offline CI.
10. Make setup/runners project-relative and non-root, leave scheduling external,
    and remove the unused in-application schedule configuration.

## Alternatives rejected

- Rename plus dependency bump: retains correctness and security defects.
- Updating Tweepy/instagrapi in place: retains legacy media/private API risk.
- Generic adapters, queue service, web UI, or bundled hosting: expands scope.
- JSON sidecars: weaker crash/concurrency behavior than standard-library SQLite.
- Parallel publication: unnecessary for this low-volume workflow and harder to
  reason about after partial failure.

## Falsifiers and operator gates

- Live Instagram use requires a Professional account, minimally scoped token,
  correct permissions, and public HTTPS media reachable by Meta.
- Live X use requires current paid/API access and write/media entitlements.
- Existing credentials must be rotated and the old Instagram session revoked;
  automated code cannot safely perform that external action.
- Exactly-once remote creation cannot be promised across a lost response without
  platform idempotency. POST PULSAR will stop ambiguous deliveries rather than
  risk duplicates.
- Official Instagram does not preserve every legacy extension identically. The
  application must report platform-specific rejections instead of silently
  degrading content.

## Primary references

- X media introduction: https://docs.x.com/x-api/media/introduction
- X media best practices: https://docs.x.com/x-api/media/quickstart/best-practices
- X create-post endpoint: https://docs.x.com/x-api/posts/create-or-edit-post
- X authentication: https://docs.x.com/x-api/getting-started/getting-access
- Meta official Instagram API collection: https://www.postman.com/meta/instagram/documentation/6yqw8pt/instagram-api
- Meta Instagram Login permissions: https://www.postman.com/meta/instagram/folder/1z5vxzu/instagram-api-with-instagram-login

