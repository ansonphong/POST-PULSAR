# Migrating from PHONG-BOT to POST PULSAR

## Migration scope

This guide covers the one implemented migration: one explicit profile from the
legacy flat PHONG-BOT `posts` directory into POST PULSAR's profile-isolated
content tree and canonical SQLite state. It does not migrate credentials,
private sessions, remote identities, schedules, logs, old posting history, or
delivery outcomes. It never infers that legacy content was published merely
because a file was in `posted/`.

Migration is destructive to successfully migrated source members: after exact
copy, hash verification, durable state, and ready-marker installation, the
legacy source member is removed. Make an offline backup of the legacy posts
directory, configuration, and any existing POST PULSAR state before `apply`.
Do not place that backup inside the configured account or state directories.

## Before migration: revoke, rotate, and provision

Treat all plaintext values from the former `config.json` and any local session
store as exposed:

1. In the X developer console, rotate the legacy X credentials. Revoke old user
   access tokens and other keys that PHONG-BOT could read. Authorize a fresh
   OAuth user access token for the intended account and required official API
   access.
2. In Instagram/Meta account security, revoke the private Instagram session
   formerly used by browser/password automation. Do not reuse an Instagram
   password, cookie, session, or private API token with POST PULSAR.
3. Configure the Meta app and Instagram Login for a professional Business or
   Creator account. Grant `instagram_business_basic` and
   `instagram_business_content_publish`, obtain a new official access token,
   and provision public HTTPS hosting for the exact `media_directory` exposed
   beneath `media_base_url`.
4. Copy `post-pulsar.toml.example` to `post-pulsar.toml`; enter only non-secret
   IDs, usernames, paths, timeouts, and environment-variable names. Provision
   each `POST_PULSAR_...` token in the operator or service manager environment.
   Use a secret manager or masked interactive facility and never print token values.
   Never paste tokens into shell history, logs, TOML, tests, issue
   reports, commits, or screenshots.
5. Verify each numeric remote ID and username through the official identity
   endpoint before enabling its target. Verify `ffprobe -version` as the daemon
   identity and validate the Instagram public URL from an external network.

Follow the official references in [README.md](README.md). Migration does not
perform OAuth, account conversion, public hosting, token rotation, or session
revocation for you.

## Filename and configuration changes

The active product identity is POST PULSAR, repository `POST-PULSAR`, package
`post_pulsar`, and command `post-pulsar`. Historical names remain below only so
operators can identify what must change.

| Legacy PHONG-BOT | POST PULSAR |
| --- | --- |
| `config-sample.json` / `config.json` | `post-pulsar.toml.example` / untracked `post-pulsar.toml` |
| Credentials embedded in JSON | Allowlisted `POST_PULSAR_...` environment variables referenced by name from TOML |
| One `posts/` root | One `account_root` per explicit `[[profiles]]` entry |
| Flat active files in `posts/` | `accounts/<profile>/RANDOM/<bundle-id>/...` with `.ready` |
| Flat files under `posts/posted/` | `accounts/<profile>/POSTED/RANDOM/<bundle-id>/...` with `.ready` |
| `phong-bot.py` | `post-pulsar` or `python -m post_pulsar` |
| `setup.sh`, legacy Windows setup, and `requirements.txt` | `setup-post-pulsar.sh`, `setup-post-pulsar.bat`, and frozen `uv.lock` |
| Legacy run/task launchers | `run-post-pulsar.sh`, `run-post-pulsar.bat`, and `task-setup-post-pulsar.bat` |
| Per-run external scheduler entries | Durable schedules owned by `post-pulsar daemon foreground` |

The legacy parser and the current bundle parser use the same semantic roles:
`id.txt`, `id-alt.txt`, one `id.<media>`, contiguous `id-1.<image>` sequences,
or one unnumbered video. Current publishable content is directory-scoped and
requires an empty `.ready` sentinel. Review the exact grammar and platform
limits in the README before migration; unsafe, partial, ambiguous, colliding,
or changing bundles stop the operation.

Legacy active bundles deliberately migrate to `RANDOM`, preserving the old
random-selection intent. Legacy `posted/` bundles become archived under
`POSTED/RANDOM`. Unsupported unrelated files are reported and left untouched.

## Dry run and apply

Run from the POST PULSAR repository with the locked environment. Select exactly
one configured profile and supply an absolute or reviewed relative legacy posts
directory that does not overlap state or destination paths:

```sh
post-pulsar --json migrate dry-run \
  --profile ansonphong --legacy-posts /srv/phong-bot/posts
```

Inspect every reported item, disposition, destination, issue, untouched path,
required input, and database change. A dry run creates no journal, database,
backup, destination, or `.ready` file. Resolve errors at the legacy source and
repeat the dry run only after reviewing why its exact plan changed.

When the plan is approved and backups and replacement credentials are ready:

```sh
post-pulsar --json migrate apply \
  --profile ansonphong --legacy-posts /srv/phong-bot/posts --confirm MIGRATE
```

Repeat the whole dry-run/apply sequence separately for `360hextile`, with that
profile's own legacy root. Never point two profiles at the same legacy source or
account root. Do not run the daemon, one-shot publisher, or another migration
against the same installation during cutover; the migration also acquires
instance, maintenance, and profile locks and fails on contention.

After apply, inspect `post-pulsar --json status --profile ansonphong`, validate
the account tree, initialize local control records if needed, and only then
start the foreground daemon. Do not enable live targets until official remote
identity verification, public-media hosting, permissions, and a human-reviewed
smoke plan are complete.

## State expectations and recovery

The durable migration journal is
`.post-pulsar/legacy-migration-v1.json` by default. It binds the selected
profile, source root, destination root, state root, configuration hash, exact
bundle/member hashes, and phase. If a current canonical database existed before
cutover, the migration creates the owner-only
`post_pulsar.pre-migration-v1.sqlite3` backup before mutating it. Do not edit,
rename, copy over, or delete the journal, backup, database, WAL, destination, or
source quarantine files during recovery.

An interrupted `migrate apply` is resumed by issuing the same command with the
same profile, source path, configuration identity, and confirmation. Recovery
accepts only state and WAL evidence cryptographically and structurally bound to
that journal; a mismatch fails closed for operator review. A completed apply is
idempotent, and a later dry run reports the already-complete state without
mutation.

Each migrated bundle is registered without a delivery outcome. Active legacy
content is ready in `RANDOM`; archived legacy content is recorded as archived
in `POSTED/RANDOM`; exact source files are removed only after verified durable
installation. Unrelated files listed as `untouched` remain at the legacy root.
The journal remains as the cutover audit and recovery record. Preserve it and
the pre-migration database backup according to your retention policy.

If anything is blocked, stop automation, preserve all artifacts, and record only
sanitized path/phase/error metadata. Never include legacy credentials, token
values, cookies, session contents, captions, media bytes, or private platform
responses in a support report.
