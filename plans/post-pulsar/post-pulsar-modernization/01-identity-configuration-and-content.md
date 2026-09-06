# Phase 1: Identity, Configuration, and Content

## Codebase Anchors

Establish the POST PULSAR package and secure inputs before any platform or state work.

### Task 1.1: Establish package and identity baseline

Create a Python 3.12 src-layout distribution named post-pulsar with reserved
`post-pulsar` and `python -m post_pulsar` entry surfaces. Until `T4.2` wires the
real CLI, `__main__.py` must be import-safe and report that implementation is
not complete; it must not import a nonexistent module. Declare current direct
runtime/development dependencies, including a pinned `uv` bootstrap and
`pytest-socket`, plus strict Ruff/mypy/pytest policy. Add deterministic
line-ending rules and ignores for POST PULSAR runtime artifacts. Preserve
Anson Phong as author while eliminating active PHONG-BOT package identity.
Create the ignored `.venv` with `python3 -m venv`, install the editable
development extras, and use its pinned `uv` for every later lock command.
Verify the checkout basename and exact fetch/push origin; set a drifted origin
to `git@github.com:ansonphong/POST-PULSAR.git`, but stop for a guarded directory
rename if the checkout is not already `POST-PULSAR`. Keep the legacy
`requirements.txt` until the atomic operational cutover in `T5.1`.

**Test:** no

**Files:**
- `.gitattributes`
- `.gitignore`
- `pyproject.toml`
- `src/post_pulsar/__init__.py`
- `src/post_pulsar/__main__.py`

**Acceptance:**
- Project metadata requires Python 3.12+, reserves the post-pulsar entry point, and names only supported direct dependencies and declared verification tools.
- Tracked text files have deterministic attributes and generated state, local config, tokens, logs, environments, and prepared media are ignored.
- The ignored `.venv` is bootstrapped from declared extras, its pinned `uv` is executable, and importing `post_pulsar` exposes the same version as project metadata.
- Repository basename and fetch/push origin exactly match `POST-PULSAR` and the requested SSH URL; legacy installation remains runnable until `T5.1`.

**Verify-After:**
- `python3 -c "import tomllib; d=tomllib.load(open('pyproject.toml','rb')); assert d['project']['name']=='post-pulsar'; assert d['project']['requires-python']=='>=3.12'"` (focused)
- `.venv/bin/python -c "import importlib.metadata as m, post_pulsar; assert post_pulsar.__version__ == m.version('post-pulsar')"` (focused)
- `.venv/bin/uv --version` (focused)
- `python3 -c "import subprocess; p=subprocess.run(['git','check-attr','text','eol','--','README.md','src/post_pulsar/__init__.py','setup.bat'],text=True,capture_output=True,check=True).stdout; assert 'README.md: text: set' in p and 'README.md: eol: lf' in p and 'setup.bat: eol: crlf' in p"` (scoped_check)
- `python3 -c "import subprocess; paths=['post-pulsar.toml','.env','.post-pulsar/post_pulsar.sqlite3','post_pulsar.log','.venv/bin/python','public-media/file.jpg']; assert all(subprocess.run(['git','check-ignore','-q',p]).returncode==0 for p in paths)"` (scoped_check)
- `test "$(basename "$PWD")" = POST-PULSAR && test "$(git remote get-url origin)" = git@github.com:ansonphong/POST-PULSAR.git && test "$(git remote get-url --push origin)" = git@github.com:ansonphong/POST-PULSAR.git` (scoped_check)

### Task 1.2: Replace configuration and secret loading

Implement frozen typed TOML configuration whose paths resolve relative to the
config file. Split local settings loading from publishing credential
validation: `status` and operator-confirmed `reconcile` require neither tokens
nor network, while `run` and guarded `retry` validate only required
snapshotted targets. Local config accepts zero enabled targets so stored
recovery remains reachable; `T4.1` alone rejects zero targets when it reaches
new-bundle admission. Reject unknown
fields, unsafe/colliding paths, enabled targets without
expected account IDs, and an Instagram base URL containing non-HTTPS scheme,
userinfo, query, fragment, IP literal, or traversal. Read publishing tokens
only from `POST_PULSAR_X_USER_ACCESS_TOKEN` and
`POST_PULSAR_INSTAGRAM_ACCESS_TOKEN`; never load `.env` implicitly or serialize
secrets. Add safe tracked examples alongside the still-runnable legacy files;
`config-sample.json` and `update_config.py` are removed atomically in `T5.1`.
Without reading contents, reduce ignored legacy `config.json` and
`instagram_session.json` permissions to owner-only where supported; deletion
and credential/session rotation remain explicit operator actions.

**Test:** yes

**Dependencies:**
- T1.1

**Files:**
- `src/post_pulsar/config.py`
- `post-pulsar.toml.example`
- `.env.example`
- `tests/unit/test_config.py`

**Acceptance:**
- Valid TOML returns immutable app/X/Instagram local settings with paths anchored to the config location, while publishing credential validation is command-specific.
- Unknown keys, path escape/collision, unsafe HTTPS settings, and missing credentials/account IDs for an actually required remote target fail with sanitized actionable errors; zero-target local settings remain valid for recovery commands.
- `status` and `reconcile` load local state without credentials even when a snapshotted target is now disabled; tracked examples contain variable names and placeholders only.
- If ignored legacy config/session files exist, no group/other permission bits remain where chmod is supported; their contents are never printed or migrated.

**Verify-After:**
- `.venv/bin/python -m pytest tests/unit/test_config.py -q` (focused)
- `! rg -n "(api_secret|password|sessionid|access_token[[:space:]]*[:=][[:space:]]*['\"][^'\"]+)" post-pulsar.toml.example .env.example src/post_pulsar/config.py` (scoped_check)
- `python3 -c "import pathlib,stat; ps=[pathlib.Path(x) for x in ('config.json','instagram_session.json') if pathlib.Path(x).exists()]; assert all(stat.S_IMODE(p.stat().st_mode) & 0o077 == 0 for p in ps)"` (scoped_check)

### Task 1.3: Implement exact content bundle parsing

Replace PhongBot._get_basename_without_number and prefix globbing with the Stage 2 exact filename grammar. Produce immutable ContentBundle values, a versioned SHA-256 fingerprint over exact members, deterministic numeric ordering, and explicit inbox errors/warnings. Reject symlinks, case collisions, reserved/nonportable IDs, gaps, mixed media roles, empty text, duplicate roles, and unsupported files that alias a recognized bundle.

**Test:** yes

**Dependencies:**
- T1.1

**Files:**
- `src/post_pulsar/content.py`
- `tests/unit/test_content.py`

**Acceptance:**
- The parser can never group cat with catalog and returns one exact immutable members tuple used downstream.
- Case-insensitive collisions, role aliases, sequence gaps, mixed media, symlinks, and Windows-unsafe IDs are rejected deterministically.
- Fingerprint changes for any role/name/length/content change and remains stable for the same ordered bundle.

**Verify-After:**
- `.venv/bin/python -m pytest tests/unit/test_content.py -q` (focused)

### Task 1.4: Add profiles, account roots, standard buckets, and ready markers

Extend the typed configuration and exact discovery model with stable
`profile_id` values, globally unique remote platform identities, isolated
account roots, per-target environment-variable references, unpublished
`DRAFTS/{QUEUE,RANDOM,REELS}` roots, and the publishable `QUEUE`, `RANDOM`, and
`REELS` buckets. The tracked example defines
`ansonphong` and `360hextile` X profiles using operator-replaced remote-ID
placeholders and distinct token variable names only. Add non-secret daemon and
control settings, including agent-capability, operator-verifier, bootstrap, and
endpoint-record paths. `T4.5`/`T4.6` own secure secret initialization and
rotation. Enumerate only immediate `<bundle-id>` directories in each bucket and
scan each directory non-recursively with the exact filename grammar. Admit a
publishable bundle only when its canonical `.ready` sentinel is present; the
sentinel is an operational member excluded from the semantic content
fingerprint. Reuse `scan_inbox(bundle_directory)` but require exactly one parsed
bundle whose ID and case match the container directory; nested directories,
extra bundle IDs, and a sentinel anywhere except that directory are errors.
`QUEUE` and `REELS` order by case-folded ID; `RANDOM` returns candidates
for the transactional selector in `T4.1`. `REELS` accepts one video only.
Reject overlapping/nested/case-colliding roots, symlinks, invalid profile IDs,
duplicate `(platform, expected_remote_user_id)` ownership, unsafe environment
variable references. Replace the original top-level `[x]`/`[instagram]` model
with an explicit `[[profiles]]` schema whose target tables define enabled flag,
expected numeric ID, expected username, token environment reference, and
platform request settings. Declare `tzdata` for portable IANA timezone support.
Stored-work root drift is enforced later by `T2.2`/`T4.1`, after durable state
exists.

**Test:** yes

**Dependencies:**
- T1.2
- T1.3

**Files:**
- `src/post_pulsar/config.py`
- `src/post_pulsar/content.py`
- `post-pulsar.toml.example`
- `.env.example`
- `pyproject.toml`
- `tests/unit/test_config.py`
- `tests/unit/test_profiles.py`

**Acceptance:**
- Profiles and account roots are stable, isolated, immutable configuration
  identities; tokens and real remote IDs remain outside tracked examples.
- `QUEUE`, `RANDOM`, and `REELS` discovery uses exact bundle membership and
  requires the matching regular ready marker without following symlinks.
- The only accepted publishable shape is
  `BUCKET/<bundle-id>/{exact content members,.ready}`; container and parsed IDs
  match exactly and scanning never descends another level.
- DRAFTS content is never a publication candidate; later core admission copies
  it through a daemon-owned journaled staging directory, verifies the approved
  semantic fingerprint, installs the sentinel last, and atomically renames the
  complete bundle directory into a publishable bucket while retaining the draft.
- The two requested X usernames have independent secret references, and one
  remote platform identity cannot be assigned to multiple profiles.
- Invalid roots, IDs, bucket content, and ready-marker races fail before state
  or network mutation.

**Verify-After:**
- `.venv/bin/python -m pytest tests/unit/test_config.py tests/unit/test_profiles.py -q` (focused)
- `! rg -n "(access_token|token)[[:space:]]*=[[:space:]]*['\"][^'\"]+" post-pulsar.toml.example .env.example` (scoped_check)
