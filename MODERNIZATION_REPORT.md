# POST PULSAR modernization report

Report boundary: repository implementation and evidence through task T5.3.
Refresh date: 2026-09-05.

## Implemented through T5.3

The former flat PHONG-BOT scripts have been replaced by the `post_pulsar`
Python 3.12 package and `post-pulsar` CLI while retaining Anson Phong's
authorship and GPL-3.0 terms.

Completed implementation includes:

- Strict non-secret TOML configuration, allowlisted environment credentials,
  exact remote account bindings, and isolated `ansonphong` and `360hextile`
  profile roots.
- Exact content parsing, deterministic bundle fingerprints, `DRAFTS`, ready
  `QUEUE`/`RANDOM`/`REELS` buckets, empty `.ready` admission, and safe
  `POSTED/<bucket>` archival.
- Bounded Pillow/`ffprobe` inspection, private staging, Instagram public-media
  verification, Instagram JPEG normalization, and durable alt/normalization
  warnings.
- Durable SQLite profile, bundle, target, delivery, artifact, schedule,
  request, intent, admission, pause, event, and archive state with ordered
  installation/profile/maintenance locking.
- Official X API v2 identity, media-upload, processing and create-post flows,
  plus official Instagram Login Graph API v26.0 identity, quota, container,
  status and publish flows for professional accounts.
- One-shot orchestration that preflights every target, checkpoints remote side
  effects, resumes known-safe work, stops ambiguous final outcomes, and
  archives only after every snapshotted target is known published.
- Explicit legacy migration with dry-run/apply, immutable journal binding,
  database backup, WAL recovery checks, exact copy/removal checkpoints, and no
  invented delivery history.
- Multi-profile CLI status and mutations, timezone-aware durable schedules,
  one sequential foreground daemon, authenticated loopback control v1,
  revisions/idempotency, one-time confirmation intents, pause/resume, retry,
  reconciliation, and graceful incarnation-bound shutdown.
- Simple same-user and hardened split-principal record policies, DRAFTS-only
  agent write guidance, core-only operator verification, and fail-closed native
  identity/ACL validation.
- Frozen dependency setup, project-relative POSIX/Windows launchers, explicit
  user startup integration, service templates, operator hardened guidance, and
  the README, migration, security, licensing, and documentation contract
  delivered by T5.3.
- A hermetic offline integration harness using temporary profile/state/content
  roots, protocol-faithful fake official adapters, injected time, and a real
  loopback daemon/control server. The harness retains a non-loopback denial
  sentinel and is test-only, so it is absent from built runtime entry points.

The implemented platform set is exactly X and Instagram professional-account
publishing. There is no general platform plugin interface in core.

## Evidence available through T5.3

Evidence is repository-local and does not rely on a live social account:

- `tests/unit/test_config.py`, `test_profiles.py`, and `test_content.py` encode
  strict configuration, profile isolation, bundle grammar, collisions, bucket
  admission, and ready-marker behavior.
- `tests/unit/test_state.py`, `test_archive.py`, `test_admission.py`, and
  `test_migration.py` encode durable transitions, stale recovery, ambiguity,
  exact archival, draft promotion, cutover binding, backup, and crash seams.
- `tests/unit/test_media.py`, `test_http.py`, and contract tests
  `tests/contract/test_x.py` and `test_instagram.py` exercise local media limits,
  pinned public URL verification, bounded HTTP behavior, and official endpoint
  request/response contracts with fake transports.
- `tests/unit/test_app.py`, `test_cli.py`, `test_cli_control.py`,
  `test_scheduler.py`, `test_daemon.py`, `test_secure_files.py`, and
  `test_process_identity.py` encode one-shot, CLI, confirmation, scheduling,
  worker, recovery, identity and filesystem policies.
- `api/control-v1.openapi.json` and `api/bootstrap.schema.json` are checked
  contracts for the local control and secretless discovery records.
- `tests/unit/test_ci.py` and `test_scripts.py` inspect locked CI/setup and
  project-relative launcher/service behavior. `tests/unit/test_docs.py` is the
  focused T5.3 contract for current headings, commands, environment names,
  migration/security negations, licensing, and absence of active legacy claims.
- `tests/integration/test_workflow.py` drives the real application, state,
  scheduler, archive, daemon, control, and trusted-CLI approval paths with fake
  adapters. It covers partial and exhausted retries, immutable drift,
  post-create/pre-commit ambiguity, explicit published/not-published
  reconciliation, interrupted archive recovery, restart idempotence, two
  isolated profiles, every standard bucket, application-level REELS execution
  with deterministic fake `ffprobe` metadata, post-admission exact-member
  fingerprint drift before adapter mutation, schedule restart/DST/misfire, and
  authenticated bounded loopback control. `tests/support/fake_daemon.py`
  exposes the same hermetic fake-adapter daemon launcher and media probe for
  sibling compatibility checks without adding a runtime command or provider
  endpoint.

Observed Stage 5 focused evidence for T5.3: `.venv/bin/uv run --frozen pytest
tests/integration/test_workflow.py -q` exited 0 with `10 passed`; the
scoped Ruff and mypy commands exited 0, and `git diff --exit-code -- uv.lock`
exited 0. No broad suite, build, coverage, packaging, or audit result is claimed
here; those remain Stage 6/CI evidence.

These are automated, hermetic implementation checks. They do not constitute a
provider-issued OAuth grant, a live post, public-host reachability from Meta, or
an OS administrator's ACL attestation.

## Operator-only gates

The following external actions were not performed by repository automation and
must be completed and recorded by the operator for each deployment:

- Rotate every legacy X credential, revoke the private Instagram session and
  obsolete tokens, and provision fresh environment tokens without exposing
  values.
- Confirm current X access tier, write/media entitlements, user-context token,
  scopes, numeric user ID, and username through official X surfaces.
- Configure a Meta app for Instagram Login; confirm the professional Business
  or Creator account, required permissions, numeric user ID, username, token,
  publishing quota, and external public HTTPS media reachability.
- Install and trust FFmpeg/`ffprobe`, run it as the daemon identity, and decide
  public-media retention and web-server isolation.
- Review and run the legacy migration, inspect every untouched/error item,
  retain its journal and backup, and decide when old data can be retired.
- Install/start the selected service explicitly and verify restart behavior,
  logs, clock/timezone, secret environment, and a human-approved live smoke
  plan. Automated contract tests never make a live post.
- For hardened mode, pre-provision distinct principals and native ACLs, perform
  every positive and negative access test using a real agent logon/token, and
  operate the daemon as an administrator-enabled system service.
- Independently review and verify any sibling MCP/plugin revision before
  installation. T5.3 provides a reusable hermetic launcher, but records no
  claim that an unreviewed sibling revision has passed compatibility checks.

## Lock and API refresh

Dependency lock and official API documentation refresh date: **2026-09-05**.

`pyproject.toml` sets `exclude-newer = "2026-09-06T00:00:00Z"`; `uv.lock` records
the matching resolution boundary and exact artifact hashes. Setup pins
`uv==0.12.10` and executes `uv sync --frozen`. This date is the review boundary,
not a promise that a provider will preserve endpoints, permissions, tiers,
quotas, or limits. Operators must compare official X and Meta documentation
again before live enablement.

The API review used these official sources:

- [X Manage Posts](https://docs.x.com/x-api/posts/manage-tweets/introduction)
- [X Create Post](https://docs.x.com/x-api/posts/create-post)
- [X Media introduction](https://docs.x.com/x-api/media/introduction)
- [X Initialize media upload](https://docs.x.com/x-api/media/initialize-media-upload)
- [Meta Instagram API with Instagram Login](https://developers.facebook.com/docs/instagram-platform/instagram-api-with-instagram-login/)
- [Meta Instagram content publishing](https://developers.facebook.com/docs/instagram-platform/instagram-api-with-instagram-login/content-publishing/)

## Easy future platform and API opportunities — NOT IMPLEMENTED

These are bounded opportunities, not current capabilities:

- Re-evaluate X's current OAuth 2.0 refresh/token lifecycle and media endpoint
  changes, then add an operator-owned authorization helper without storing app
  secrets in TOML.
- Re-evaluate newer Meta Graph API versions and add a deliberate version
  migration after contract fixtures and live operator validation.
- Add currently unsupported official Instagram publication types only after
  their media, accessibility, quota, retry, and ambiguous-create contracts are
  designed; examples include Stories or mixed/video carousels.
- Add another official platform adapter only with user-context authentication,
  remote identity pinning, deterministic preflight, durable artifact state, and
  an explicit ambiguous-final-create recovery path.
- Add opt-in provider health/entitlement diagnostics that are read-only,
  redacted, bounded, and never treated as proof a later publish will succeed.

Platform names sometimes suggested for expansion—such as Threads, Facebook or
Bluesky—are not supported by this repository.
No item in this future section was implemented through T5.3.

## Original recommendations and disposition

The modernization's original recommendations were to adopt the POST PULSAR
identity; replace the flat script layout with a typed package; use official X
and Instagram professional-account APIs; parse the legacy filename workflow
exactly; use SQLite delivery/artifact state; stop ambiguous creates; archive
only complete multi-target work; move credentials to environment variables;
lock current Python dependencies; add hermetic tests; and provide safe,
project-relative operator scripts. Those recommendations are represented in
the implementation and evidence sections above.

The original analysis also rejected a generic adapter framework, bundled
hosting, web UI, analytics product, parallel publisher, and blind dependency
bump. They remain outside the implemented product. Later scope added durable
multi-profile scheduling and authenticated local control, which are recorded as
implemented above; it did not turn the core into a hosted service or a social
platform plugin framework.

For clarity, none of the future opportunities listed in the preceding section
was implemented through T5.3. This report contains no forecast presented as
test evidence and no assertion of a sibling repository revision.
