# POST PULSAR security policy

## Supported security boundary

Security fixes apply to the current `master` code and the exact dependencies in
`uv.lock`; no historical release support window is promised. POST PULSAR is a
local publisher with powerful credentials. Its controls reduce accidental
cross-profile publication, unsafe filesystem inputs, duplicate final creates,
and unauthenticated local control. They do not make an untrusted operating
system, administrator, daemon account, or dependency safe.

The core trusts its configured operator, daemon identity, Python runtime,
FFmpeg/`ffprobe`, operating system, filesystem, DNS/TLS stack, official X and
Meta APIs, and locked Python dependency graph. Platform authentication and
authorization remain enforced by X and Meta. Root, an OS administrator, backup
operators, debuggers, and processes able to replace the runtime are outside the
application boundary.

## Secret hygiene

Social platform tokens belong only in allowlisted environment variables such as
`POST_PULSAR_X_ANSONPHONG_USER_ACCESS_TOKEN` and
`POST_PULSAR_INSTAGRAM_360HEXTILE_ACCESS_TOKEN`. TOML stores the variable name,
never the value. Provision service secrets through the operating system's
credential/environment facility, restrict who can inspect the service
definition and process environment, and ensure crash reporting and shell
tracing cannot capture them.

Never commit, log, echo, print, screenshot, attach, or paste social platform
tokens, OAuth codes, client secrets, passwords, cookies, private Instagram
sessions, the agent bearer capability, or operator input. Environment variable
names and clearly marked dummy values are safe; real values are not. Keep
`post-pulsar.toml`, `.post-pulsar/`, account roots, public staging, backups, and
service configuration out of source control. Public Instagram staging contains
publishable media by design; isolate the exact served directory, use
unguessable generated filenames, disable indexes, and apply a short external
retention policy after POST PULSAR's terminal hash-bound cleanup.

On suspected disclosure, stop new admission, rotate the affected X or Instagram
token at the official provider, revoke legacy sessions and tokens, rotate the
local agent capability, and rotate the operator secret if its input may have
been captured. Review status for ambiguous deliveries before retrying anything.
Capability lifecycle commands are:

```sh
post-pulsar control agent-capability rotate
post-pulsar control agent-capability revoke --confirm REVOKE
post-pulsar control operator-secret rotate
```

Revocation removes the local capability file; it does not revoke a platform
token, stop a daemon, or erase copies already held by a same-user process.

## Simple same-user mode

`deployment_mode = "simple"` creates owner-only state directories and files
(POSIX modes or protected current-user Windows ACLs), keeps control on loopback,
and requires the agent bearer capability. This is useful protection against
other unprivileged OS users and accidental broad permissions.

Simple mode does not protect against arbitrary same-user shell or file access.
Any process running as the same user may be able to read environment tokens and
the bearer capability; edit TOML, DRAFTS, publishable buckets, `.ready` markers,
or SQLite state; replace the Python environment; attach to the process; or call
the control API directly. Loopback and confirmation prompts are not a sandbox
against an already compromised same-user account. User systemd/launchd startup
and the Windows logon task are convenience automation with this same boundary.

Keep `allow_agent_publish = false` unless the operator intentionally wants an
agent holding the local capability to request publication-related confirmation
intents. This setting is installation eligibility, not proof that simple mode
isolates the agent from the operator.

## Hardened split-principal mode

`deployment_mode = "hardened"` is the recommended agent deployment. It validates
a pre-provisioned, distinct core/daemon principal and agent principal. The
setup guides report the required layout but do not create accounts, set
ownership, change ACLs, install a system service, or elevate privileges.

The OS administrator must establish and test this policy before first start:

- The daemon identity has core-only write access to configuration, private
  state and SQLite, logs, private staging, `QUEUE`/`RANDOM`/`REELS`, every
  `.ready` marker, `POSTED`, and the core-only operator verifier.
- The agent identity has DRAFTS-only write access below each approved profile.
  It cannot create or replace `.ready`, move content into a publishable bucket,
  alter config/state, or read social platform tokens.
- Bootstrap, endpoint record, and agent capability live together in a
  pre-provisioned discovery directory outside private state. Discovery and the
  capability are read-only to the agent and writable only by core. Atomic
  replacements must retain those permissions.
- On Linux, configure numeric core and agent UIDs plus a dedicated positive GID
  whose only members are agent and optionally core. The agent may have no other
  groups. Required discovery policy is exactly `0750` directories and `0640`
  files, core-owned with the dedicated group, with no extended POSIX ACLs.
- On Windows, configure distinct resolvable unprivileged user SIDs and exact
  protected core/agent ACLs on every relevant ancestor. Validate with `icacls`
  and a real agent logon token. Do not use the same-user scheduled task.

Run `setup-post-pulsar.sh --hardened-guide` or
`setup-post-pulsar.bat --hardened-guide`, substitute reviewed absolute paths and
identities, and perform every positive and negative access check shown. Adapt
the service template to an operator-managed system service under the daemon
identity; store social platform tokens in that service's private environment.
Set hardened identity fields and the three discovery paths in TOML only after
the filesystem policy exists. Initialize the bootstrap with service mode
`manual`; hardened mode deliberately disables MCP auto-start.

Hardened checks fail closed on ambiguous identities, extra groups, set-user-ID
execution, unsafe ancestors, replaceable directories, unexpected modes/owners,
extended Linux ACLs, or unvalidated Windows ACLs. Native enforcement currently
supports the implemented Linux POSIX policy and Windows protected ACL policy;
other POSIX systems fail closed where the required native ACL validation is
unavailable. Containers, network filesystems, virtualization, backup agents,
and administrator overrides require a separate OS threat assessment.

## Draft admission and TTY approval

`DRAFTS` is a non-publishable editing area. The daemon scans one flat semantic
bundle, binds its exact fingerprint into a confirmation intent, copies verified
members into a hidden core-owned directory, writes `.ready` last, and atomically
installs the directory in `QUEUE`, `RANDOM`, or `REELS`. Direct agent writes to
publishable buckets are not part of the hardened policy.

All local control calls require the agent bearer capability. Read-only
discovery, status, capability, profile, bucket, schedule, request, and preview
operations reveal bounded metadata, not platform tokens or media bytes.
Write operations also require revisions and idempotency keys. Filesystem paths,
SQL, shell text, arbitrary commands, tokens, media bytes, and raw platform
responses are rejected from control input.

When `allow_agent_publish = false`, agent-origin publication-enabling intents
are rejected. When it is true, the agent can request an expiring intent but
cannot approve it. A trusted operator must review the exact action, profile,
arguments, revision, bundle identity/fingerprint, consequence, and expiry, then
run `post-pulsar confirmations approve ...` outside MCP. Approval requires a
real TTY and the operator secret; the persisted file contains only a scrypt
verifier with a bounded failed-attempt lockout. The approved intent is
single-use and mutation, replay, expiry, or cross-profile use fails closed.

TTY approval is a human authorization boundary only when the operator terminal
and split identity are actually trusted. It does not rescue simple mode from a
same-user compromise, and it does not validate the truth or safety of drafted
content on the operator's behalf.

## MCP and plugin trust boundary

The core repository contains the authenticated `post-pulsar.control/v1` API and
secretless bootstrap schema, not an in-process plugin framework. A sibling
`POST-PULSAR-PLUGINS` repository may provide a local stdio MCP client. Treat any
plugin package as executable third-party code with the permissions of its host,
even when it is named for POST PULSAR. The bearer capability is authorization
to call the bounded local API; it is not a social platform token and does not
permit direct database, filesystem, or provider access.

Safe installation means:

1. Obtain the sibling repository from its operator-approved origin. Pin and
   review an exact commit; inspect its plugin manifests, MCP launch command,
   frozen dependency lock, install script, tool schemas, and requested host
   permissions before execution. Do not install a similarly named registry
   package by name alone.
2. Verify the sibling's documented compatible core version and exact
   `post-pulsar.control/v1` schema/hash against this checkout. If compatibility
   evidence is absent or differs, do not connect it.
3. Install only with the sibling repository's own reviewed locked/offline-safe
   directions. Do not grant it this repository's `.venv`, TOML, SQLite state,
   social token environment, operator verifier, account roots, or public-media
   write access.
4. Set `POST_PULSAR_MCP_BOOTSTRAP` to the reviewed bootstrap file path only.
   The bootstrap is secretless and points to the endpoint and capability files;
   never put capability contents or social platform tokens in that variable or
   an MCP host configuration.
5. In hardened mode run the MCP host as the agent principal. Grant DRAFTS-only
   write and read-only discovery/capability access described above. Keep the
   daemon and operator approval CLI under the core principal.
6. Start services separately under operator control. A plugin must not start
   Python directly, install a service, cross identities, stop arbitrary
   processes, or fall back to private core imports/state access when discovery
   fails. Remove its host configuration and isolated runtime to uninstall; then
   rotate the agent capability if access is no longer intended.

No sibling revision or cross-repository compatibility is asserted by this
T5.2 document. Such evidence must be recorded only after an exact sibling
revision passes its own packaging, contract, real-core, and secret-scan gates.
A hosted/public plugin or remote MCP transport is not implemented.

## Reporting a vulnerability

Report security issues privately to the repository maintainer before public
disclosure. Include the POST PULSAR commit, operating system, deployment mode,
affected profile/platform names, sanitized command, observed result, and a
minimal reproduction using dummy values. Do not include credentials, bearer
capabilities, operator input, private paths, account content, database files,
logs containing personal data, or raw provider responses.

If private contact is unavailable, open a minimal issue that requests a secure
channel and contains no exploit details or secrets. Preserve relevant local
state without modifying an ambiguous delivery, revoke exposed credentials at
the provider, and state which rotations were completed.
