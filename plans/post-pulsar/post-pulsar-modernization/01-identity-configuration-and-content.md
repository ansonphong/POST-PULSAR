# Phase 1: Identity, Configuration, and Content

## Codebase Anchors

Establish the POST PULSAR package and secure inputs before any platform or state work.

### Task 1.1: Establish package and identity baseline

Create a Python 3.12 src-layout distribution named post-pulsar with the post-pulsar entry point and python -m post_pulsar support. Declare current direct runtime and development dependencies and strict Ruff/mypy/pytest policy in pyproject.toml, add deterministic line-ending rules, update ignores for POST PULSAR runtime artifacts, and remove the stale requirements freeze. Preserve Anson Phong as author while eliminating active PHONG-BOT package identity. Bootstrap the ignored verifier environment with python3 -m venv .venv and install the editable development extras so every downstream focused hook has a declared runner.

**Test:** no

**Files:**
- `.gitattributes`
- `.gitignore`
- `pyproject.toml`
- `src/post_pulsar/__init__.py`
- `src/post_pulsar/__main__.py`
- `requirements.txt`

**Acceptance:**
- Project metadata requires Python 3.12+, exposes post-pulsar, and names only supported direct dependencies.
- Tracked text files have deterministic attributes and generated state, local config, tokens, logs, environments, and prepared media are ignored.
- The legacy requirements.txt is removed; the ignored .venv is bootstrapped from the declared extras and importing post_pulsar exposes the same version as project metadata.

**Verify-After:**
- `python3 -c "import tomllib; d=tomllib.load(open('pyproject.toml','rb')); assert d['project']['name']=='post-pulsar'; assert d['project']['requires-python']=='>=3.12'"` (focused)
- `.venv/bin/python -c "import importlib.metadata as m, post_pulsar; assert post_pulsar.__version__ == m.version('post-pulsar')"` (focused)
- `git check-attr text eol -- .gitattributes README.md src/post_pulsar/__init__.py` (scoped_check)

### Task 1.2: Replace configuration and secret loading

Implement frozen typed TOML configuration whose paths resolve relative to the config file. Reject unknown fields, unsafe paths, enabled targets without expected account IDs, and invalid Instagram HTTPS media settings before network access. Read publishing tokens only from POST_PULSAR_X_USER_ACCESS_TOKEN and POST_PULSAR_INSTAGRAM_ACCESS_TOKEN; never load .env implicitly or serialize secrets. Replace config-sample.json and the broken Threads-era update_config.py with safe tracked examples. Without reading their contents, reduce ignored legacy config.json and instagram_session.json permissions to owner-only where the filesystem supports it; deletion and credential/session rotation remain explicit operator actions documented later.

**Test:** yes

**Dependencies:**
- T1.1

**Files:**
- `src/post_pulsar/config.py`
- `post-pulsar.toml.example`
- `.env.example`
- `tests/unit/test_config.py`
- `config-sample.json`
- `update_config.py`

**Acceptance:**
- Valid TOML returns immutable app/X/Instagram settings with paths anchored to the config location.
- Unknown keys, path escape/collision, invalid HTTPS settings, and missing enabled-target tokens or account IDs fail with sanitized actionable errors.
- Tracked examples contain variable names and placeholders only; legacy JSON and token-copy updater are gone.
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

