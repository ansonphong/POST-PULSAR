# POST PULSAR

POST PULSAR is a local-first Python 3.12 publisher for two implemented targets:
X and Instagram professional accounts. It supports explicit one-shot runs and a
single foreground daemon that owns durable schedules, recovery, and an
authenticated loopback control API. Anson Phong is the original author and
copyright holder.

## Supported platforms and official prerequisites

### X

POST PULSAR uses the official X API v2: `GET /2/users/me`, the v2 media upload
endpoints, and one final `POST /2/tweets`. Provision an X developer account,
Project and App, then authorize a user access token for the exact account in
`expected_remote_user_id` and `expected_username`. The token must have the
official scopes needed to identify the user and create posts and media. Review
the current X access tier, scope, rate, and media rules before enabling a
profile; API access and commercial terms are controlled by X.

Official references:

- [Manage Posts prerequisites and authentication](https://docs.x.com/x-api/posts/manage-tweets/introduction)
- [Create Post](https://docs.x.com/x-api/posts/create-post)
- [Media upload overview and limits](https://docs.x.com/x-api/media/introduction)
- [Initialize chunked media upload](https://docs.x.com/x-api/media/initialize-media-upload)

The application accepts one environment-provided OAuth user access token per X
profile. It does not accept app-only bearer credentials, consumer secrets, or
OAuth refresh secrets in TOML.

### Instagram

POST PULSAR uses Instagram Login and the official Instagram Graph API v26.0 at
`graph.instagram.com`. The account must be an Instagram professional account
(Business or Creator), the Meta app must be configured for Instagram API access,
and the account token must include `instagram_business_basic` and
`instagram_business_content_publish`. Configure the numeric Instagram user ID
and username that the token identifies.

Meta fetches publishing media from URLs. `media_directory` must therefore be
served at the exact public HTTPS `media_base_url`; every URL must be reachable
without local authentication and return the staged bytes with the exact MIME
type. POST PULSAR rejects IP literals, loopback hosts, unsafe redirects, private
DNS destinations, byte/hash mismatches, and URLs outside the configured base.

Official references:

- [Instagram API with Instagram Login](https://developers.facebook.com/docs/instagram-platform/instagram-api-with-instagram-login/)
- [Instagram content publishing](https://developers.facebook.com/docs/instagram-platform/instagram-api-with-instagram-login/content-publishing/)
- [Instagram Login setup](https://developers.facebook.com/docs/instagram-platform/instagram-api-with-instagram-login/get-started/)

The supported platforms above are exhaustive. No other social platform adapter
is implemented.

## Locked installation

Requirements are Python 3.12 or newer and a separately provisioned `ffprobe`
executable for video or Reel inspection. `ffprobe` is distributed with FFmpeg;
install FFmpeg through a trusted operating-system package source or the
[official FFmpeg download guidance](https://ffmpeg.org/download.html), then run
`ffprobe -version` as the daemon identity. POST PULSAR never installs or updates
FFmpeg or the operating system.

Copy `post-pulsar.toml.example` to the untracked `post-pulsar.toml`, review the
non-secret settings, and provision referenced environment variables outside
version control. The setup scripts create a repository-local `.venv`, install
the pinned `uv==0.12.10`, and run `uv sync --frozen` against `uv.lock`:

```sh
./setup-post-pulsar.sh
```

```batch
setup-post-pulsar.bat
```

The scripts do not update the lock, upgrade system packages, create identities,
or start background services. A `.venv` from the other OS family or an
incomplete environment is reported for manual review and is never deleted.
Use `./run-post-pulsar.sh ...` on POSIX and `run-post-pulsar.bat ...` on Windows,
or invoke the installed `post-pulsar` console command from `.venv`.

Locked installation installs code only. Runtime state must already be a
canonical POST PULSAR database; operators upgrading the legacy application use
the explicit workflow in [MIGRATION.md](MIGRATION.md).

## Configuration and profile isolation

Configuration is strict TOML. Paths are relative to the configuration file,
must remain below its directory, must not traverse symlinks, and state,
account, log, discovery, and Instagram public-media paths must satisfy the
implemented non-overlap rules. Profile IDs are lowercase portable IDs; remote
account IDs, token-variable names, and account roots must be unique.

This credential-free example binds two isolated accounts. Numeric IDs are
examples and must be replaced from the official identity endpoints:

```toml
[app]
state_directory = ".post-pulsar"
log_file = ".post-pulsar/logs/post_pulsar.log"
deployment_mode = "simple"
allow_agent_publish = false
control_host = "127.0.0.1"
control_port = 8765
agent_capability_file = ".post-pulsar/control/agent-capability"
operator_verifier_file = ".post-pulsar/control/operator-verifier"
bootstrap_file = ".post-pulsar/control/bootstrap.json"
endpoint_record_file = ".post-pulsar/control/endpoint.json"

[[profiles]]
profile_id = "ansonphong"
account_root = "accounts/ansonphong"
timezone = "America/Vancouver"

[profiles.x]
enabled = true
expected_remote_user_id = "100000000000000001"
expected_username = "ansonphong"
token_env_var = "POST_PULSAR_X_ANSONPHONG_USER_ACCESS_TOKEN"
request_timeout_seconds = 30
processing_timeout_seconds = 300
chunk_size_bytes = 4194304

[[profiles]]
profile_id = "360hextile"
account_root = "accounts/360hextile"
timezone = "America/Vancouver"

[profiles.x]
enabled = true
expected_remote_user_id = "100000000000000002"
expected_username = "360hextile"
token_env_var = "POST_PULSAR_X_360HEXTILE_USER_ACCESS_TOKEN"
request_timeout_seconds = 30
processing_timeout_seconds = 300
chunk_size_bytes = 4194304

[profiles.instagram]
enabled = true
expected_remote_user_id = "200000000000000002"
expected_username = "360hextile"
token_env_var = "POST_PULSAR_INSTAGRAM_360HEXTILE_ACCESS_TOKEN"
media_directory = "public-media/360hextile"
media_base_url = "https://media.example.com/360hextile/"
request_timeout_seconds = 30
processing_timeout_seconds = 300
```

Set each named environment variable in the operator or service manager's secret
environment without printing its value. A run loads credentials only for the
selected profile's durable target snapshot. Before any mutation, POST PULSAR
calls the official identity endpoint and refuses a token whose remote ID or
username differs from the configured binding.

## Content grammar and media limits

Each profile has its own account root:

```text
accounts/ansonphong/
├── DRAFTS/                    # flat editable bundle members; never publishable
├── QUEUE/<bundle-id>/         # ready bundle directory
├── RANDOM/<bundle-id>/        # ready bundle directory
├── REELS/<bundle-id>/         # ready bundle directory
└── POSTED/<bucket>/<bundle-id>/
```

`DRAFTS` contains flat bundle files without `.ready`. Agent-assisted admission
copies one exact draft into a hidden directory, verifies every byte, installs
`.ready` last, and atomically renames it into the chosen publishable bucket.
Operators may also prepare a publishable directory directly, but it is eligible
only when it contains an empty, regular, non-symlink `.ready` file. A marker at
the account root, a non-empty marker, nested directories, symlinks, ambiguous
names, or any error anywhere in the selected bucket fails the scan closed.

A portable `<bundle-id>` is 1–64 ASCII letters, digits, `_`, or `-`, begins and
ends with an alphanumeric character, is not a Windows device name, and must not
end in case-insensitive `-alt` or `-<digits>`. Names are case-sensitive but
case-fold collisions are rejected.

One bundle uses exactly one of these media forms:

```text
post.jpg | post.jpeg | post.png | post.gif       # one image
post-1.jpg, post-2.png, ...                      # contiguous images from 1
post.mp4 | post.mov                              # one video, never numbered
post.txt                                         # optional UTF-8 caption
post-alt.txt                                     # optional UTF-8 alt text
.ready                                           # empty regular sentinel in publishable bundles
```

Extensions are lowercase and exact. Images and video cannot be mixed;
numbered and unnumbered images cannot be mixed. Caption and alt files are
trimmed and may not be empty. Unsupported visible files are warnings only when
they do not alias a recognized bundle identity; ambiguous aliases are errors.

Enforced publication limits are:

| Target | Accepted media and local preflight |
| --- | --- |
| X images | 1–4 decoded JPEG, PNG, or non-animated GIF images; each at most 5 MiB. |
| X animated GIF | Sole media item; at most 15 MiB, 1280×1080, 350 frames, and 300 million aggregate pixels. |
| X video | Sole `.mp4`; at most 512 MiB; 0.5–140 seconds; H.264, YUV420, optional AAC-LC, 32–1280 wide, 32–1024 high, aspect ratio 1:3–3:1, at most 60 fps. |
| X text | At most 280 weighted characters. Shared alt text is limited to 1,000 characters and is valid only for images. |
| Instagram images | 1–10 JPEG or PNG sources; GIF is rejected. Output is normalized to metadata-free JPEG, 320–1440 pixels wide and aspect ratio 4:5–1.91:1, at most 8 MiB. |
| Instagram Reel | One MP4 or MOV, at most 1 GiB; 3–900 seconds; H.264 or HEVC; 23–60 fps; width at most 1920; video at most 25 Mbps; optional AAC at 48 kHz and at most 128 kbps. |
| Instagram caption | At most 2,200 Unicode code points, 30 hashtags, and 20 mentions. |

`ffprobe` supplies bounded video metadata; malformed, missing, excessive, or
timed-out probe output fails before upload. Instagram image normalization and
alt limitations are persisted as `instagram_image_normalized` and
`instagram_alt_text_unsupported` warnings. Instagram does not receive the
shared alt text. One bundle enabled for both targets must pass both platforms'
limits.

Bucket selection is deterministic. `QUEUE` and `REELS` choose the first bundle
by case-insensitive bundle ID with exact spelling as a tie-breaker. `RANDOM`
chooses the smallest durable SHA-256 selection score derived from profile,
selection counter, bundle ID, and fingerprint. It is randomized in distribution
but reproducible for recovery. A bundle ID can appear in only one publishable
bucket, and each profile can have at most one active/recoverable bundle.

## One-shot workflow

Use a unique stable trigger ID for one logical request. Reusing it resumes or
returns the durable result; binding the same trigger to different work is a
conflict.

```sh
post-pulsar profiles list
post-pulsar run --profile ansonphong --bucket QUEUE --trigger-id manual-20260905-001
post-pulsar status --profile ansonphong
```

The launcher equivalent is:

```sh
./run-post-pulsar.sh run --profile ansonphong --bucket QUEUE --trigger-id manual-20260905-001
```

The run acquires the installation and profile locks, recovers stale work,
selects and snapshots one ready bundle, validates all enabled targets, verifies
remote identities, checkpoints each remote side effect, performs at most one
final create per target, and archives exact source bytes and `.ready` beneath
`POSTED/<bucket>/<bundle-id>`. Exit codes are `0` archived, `2` usage, `3` no
work/deferred, `4` invalid, `5` blocked, `6` lock contention, `7` retryable,
`8` daemon unavailable, `9` protocol version mismatch, `10` authentication,
`11` revision/idempotency conflict, and `12` approval required.

## Built-in scheduling and daemon operation

Schedules are durable and evaluated only by `post-pulsar daemon foreground`.
They use the profile's explicit IANA timezone, weekdays `0..6` (Monday through
Sunday), and local `HH:MM`. A DST fold chooses the earlier instant; a gap moves
to the first valid minute and counts that delay against the misfire grace.
Missed occurrences are recorded rather than silently backfilled. Recoverable
runs are dispatched before new due work, in deterministic order, by one
sequential daemon worker.

Example: create an enabled Monday/Wednesday/Friday QUEUE schedule. The required
revision and idempotency values are explicit concurrency inputs:

```sh
post-pulsar schedules create --profile ansonphong --expected-revision 0 \
  --idempotency-key schedule-anson-mwf-v1 --schedule-id mwf-morning \
  --bucket QUEUE --timezone America/Vancouver --weekdays 0,2,4 \
  --local-time 07:00 --misfire-grace-seconds 900 --enabled
post-pulsar schedules list --profile ansonphong
post-pulsar daemon foreground
```

For a simple same-user convenience deployment, explicitly install—but do not
start—the inspected templates:

```sh
./setup-post-pulsar.sh --install-user-service
systemctl --user start post-pulsar.service       # Linux
```

On macOS use the printed `launchctl bootstrap` command. On Windows run
`task-setup-post-pulsar.bat install`; the scheduled task starts the foreground
daemon at logon. Removal affects future startup only and does not stop a running
process. Hardened deployments must instead use an operator-provisioned system
service under the daemon identity; first inspect
`./setup-post-pulsar.sh --hardened-guide` or
`setup-post-pulsar.bat --hardened-guide`.

## Authenticated local control

The daemon binds only canonical `127.0.0.1` or `::1`, writes an incarnation-bound
endpoint record, and serves `post-pulsar.control/v1`. Every request needs the
256-bit bearer capability stored in `agent_capability_file`; loopback alone is
not authentication. Initialize it once with a non-secret random installation ID
and the already selected service identity:

```sh
post-pulsar control agent-capability initialize \
  --installation-id 0123456789abcdef0123456789abcdef \
  --service-mode systemd-user --service-identifier post-pulsar.service
post-pulsar control operator-secret initialize
```

The second command reads a secret of at least 12 characters from a real TTY and
stores only a scrypt verifier with bounded failed-attempt lockout. Use
`control agent-capability rotate`, `control agent-capability revoke --confirm
REVOKE`, and `control operator-secret rotate` for lifecycle operations.

Read-only CLI commands use the live daemon when available and otherwise open
canonical state read-only. Mutations require exact profile/resource identity,
revision and idempotency data. Consequential agent-origin actions exist only
when `allow_agent_publish = true`, require an expiring exact confirmation
intent, require separate operator approval through `post-pulsar confirmations
approve` on a real TTY, and consume that intent once. See
[SECURITY.md](SECURITY.md) and the authoritative schema in
`api/control-v1.openapi.json`.

## Status, recovery, and warnings

Use JSON for complete state, delivery phases, warnings, revisions, and exact
identities:

```sh
post-pulsar --json status --profile ansonphong
post-pulsar --json schedules list --profile ansonphong
post-pulsar --json requests list --profile ansonphong
post-pulsar --json requests status --profile ansonphong --request-id 7
```

An `active` bundle is being delivered or is recoverable, `archiving` resumes an
exact archive transaction, `archived` is complete, and `blocked` needs operator
attention. A pre-final retryable failure records `safe_to_retry` and
`next_attempt_at`; automatic recovery never retries an ambiguous final outcome.
After checking the platform directly, create and approve an exact intent, then
use the full identity-bound `post-pulsar retry` command for a proven failed
delivery or `post-pulsar reconcile --published REMOTE_ID` / `--not-published`
for an ambiguous delivery. Read the exact flags from `--help`; bundle key, ID,
fingerprint, platform, revision, idempotency key, and intent ID are all required.

`pause` blocks new admission but lets work that crossed the final safe boundary
converge. On daemon start, POST PULSAR recovers stale requests, draft admission
journals, safe delivery phases, scheduled runs, and archive checkpoints. It
does not infer that an uncertain remote create succeeded. Callback failures are
durably reported by control health as degraded. Logs rotate at the configured
size and contain sanitized operational events, not credential values.

## Tests

Install the frozen development group, then run a focused file or the project
suite as appropriate:

```sh
.venv/bin/python -m pytest tests/unit/test_docs.py -q
.venv/bin/python -m pytest -q
```

Contract tests use fake HTTP transports and do not perform live posts. A real
platform smoke test, OAuth provisioning, public hosting, OS service/ACL setup,
and credential rotation remain operator actions.

## License

POST PULSAR remains GPL-3.0. See [LICENSE.md](LICENSE.md).

Copyright (C) 2024 Anson Phong
