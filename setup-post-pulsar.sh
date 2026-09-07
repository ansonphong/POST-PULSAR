#!/usr/bin/env bash
# Copyright (C) 2024 Anson Phong
# SPDX-License-Identifier: GPL-3.0-only

set -euo pipefail

SCRIPT_DIR=$(CDPATH= cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P)
VENV_DIR="$SCRIPT_DIR/.venv"
PYTHON="$VENV_DIR/bin/python"
UV_VERSION="0.12.10"

fail() {
    printf 'POST PULSAR setup: %s\n' "$1" >&2
    exit "${2:-1}"
}

show_hardened_guide() {
    cat <<'GUIDE'
Recommended hardened separate-identity installation (validate and report only)

This script does not create users, change ownership or ACLs, move content, or
install/start a system service. Review every command with the OS administrator.

1. Choose distinct unprivileged daemon and agent identities. Keep config,
   state, operator-verifier, publishable QUEUE/RANDOM/REELS buckets, and every
   .ready marker owned and writable only by the daemon identity.
2. Grant the agent identity write access only below each DRAFTS directory.
   Grant read-only access to the bootstrap, endpoint, and agent-capability
   records. Their parent discovery directory must let daemon-owned atomic
   replacements remain readable but never writable by the agent identity.
3. Validate both identities and paths before enabling anything. Examples:
     id DAEMON_IDENTITY
     id AGENT_IDENTITY
     namei -l /absolute/path/to/post-pulsar.toml
     getfacl /absolute/path/to/{post-pulsar.toml,state,discovery,accounts}
     sudo -u AGENT_IDENTITY test -w /absolute/path/to/accounts/PROFILE/DRAFTS
     sudo -u AGENT_IDENTITY test ! -w /absolute/path/to/accounts/PROFILE/QUEUE
     sudo -u AGENT_IDENTITY test ! -w /absolute/path/to/accounts/PROFILE/RANDOM
     sudo -u AGENT_IDENTITY test ! -w /absolute/path/to/accounts/PROFILE/REELS
     sudo -u AGENT_IDENTITY test ! -w /absolute/path/to/state
     sudo -u AGENT_IDENTITY test ! -w /absolute/path/to/post-pulsar.toml
     sudo -u AGENT_IDENTITY test ! -w /absolute/path/to/operator-verifier
   Also validate that the agent cannot create or replace any .ready marker.
4. Adapt service/post-pulsar.service as an operator-managed system service,
   adding User=DAEMON_IDENTITY and Group=DAEMON_IDENTITY. Provision its secret
   environment outside the agent account, then explicitly enable it as an OS
   administrator only after the checks above succeed.
5. Set deployment_mode = "hardened" and initialize the bootstrap with service
   mode "manual". Hardened mode disables MCP auto-start. If the daemon is
   absent, the agent must receive operator setup guidance; it must not cross
   the identity boundary to start Python or the system service.

On Windows, use distinct resolvable user SIDs, protected inherited ACLs, and an
operator-enabled Windows service. Verify with icacls and a real logon/token for
the agent identity. Do not use the same-user scheduled task for hardened mode.
GUIDE
}

render_template() {
    local source=$1
    local destination=$2
    local escaped
    escaped=${SCRIPT_DIR//\\/\\\\}
    escaped=${escaped//&/\\&}
    escaped=${escaped//|/\\|}
    escaped=${escaped//%/%%}
    sed "s|@POST_PULSAR_ROOT@|$escaped|g" "$source" > "$destination"
}

install_user_service() {
    local os_name destination temporary marker
    os_name=$(uname -s)
    marker="PostPulsar-Repository=$SCRIPT_DIR"
    case "$os_name" in
        Linux)
            command -v systemctl >/dev/null 2>&1 || fail "systemctl is unavailable"
            destination="${XDG_CONFIG_HOME:-$HOME/.config}/systemd/user/post-pulsar.service"
            mkdir -p -- "$(dirname -- "$destination")"
            if [[ -e "$destination" ]] && ! grep -Fq -- "$marker" "$destination"; then
                fail "refusing to replace unrelated unit: $destination"
            fi
            temporary=$(mktemp "${destination}.new.XXXXXX")
            trap 'rm -f -- "${temporary:-}"' RETURN
            render_template "$SCRIPT_DIR/service/post-pulsar.service" "$temporary"
            mv -- "$temporary" "$destination"
            trap - RETURN
            systemctl --user daemon-reload
            systemctl --user enable post-pulsar.service
            printf '%s\n' "Installed and enabled user unit $destination (not started)."
            printf '%s\n' "Start explicitly: systemctl --user start post-pulsar.service"
            printf '%s\n' "Bootstrap service selection: --service-mode systemd-user --service-identifier post-pulsar.service"
            ;;
        Darwin)
            command -v launchctl >/dev/null 2>&1 || fail "launchctl is unavailable"
            destination="$HOME/Library/LaunchAgents/com.post-pulsar.daemon.plist"
            mkdir -p -- "$(dirname -- "$destination")"
            if [[ -e "$destination" ]] && ! grep -Fq -- "$marker" "$destination"; then
                fail "refusing to replace unrelated launch agent: $destination"
            fi
            temporary=$(mktemp "${destination}.new.XXXXXX")
            trap 'rm -f -- "${temporary:-}"' RETURN
            render_template "$SCRIPT_DIR/service/com.post-pulsar.daemon.plist" "$temporary"
            mv -- "$temporary" "$destination"
            trap - RETURN
            printf '%s\n' "Installed user LaunchAgent $destination (not loaded or started)."
            printf '%s\n' "Load explicitly: launchctl bootstrap gui/$UID '$destination'"
            printf '%s\n' "Bootstrap service selection: --service-mode launchd-agent --service-identifier com.post-pulsar.daemon"
            ;;
        *)
            fail "user service installation is unsupported on $os_name"
            ;;
    esac
}

remove_user_service() {
    local os_name destination marker
    os_name=$(uname -s)
    marker="PostPulsar-Repository=$SCRIPT_DIR"
    case "$os_name" in
        Linux)
            destination="${XDG_CONFIG_HOME:-$HOME/.config}/systemd/user/post-pulsar.service"
            [[ -e "$destination" ]] || {
                printf '%s\n' "POST PULSAR user unit is not installed."
                return
            }
            grep -Fq -- "$marker" "$destination" || fail "refusing to remove unrelated unit: $destination"
            systemctl --user disable post-pulsar.service
            rm -- "$destination"
            systemctl --user daemon-reload
            printf '%s\n' "Removed POST PULSAR background startup; no process was stopped."
            ;;
        Darwin)
            destination="$HOME/Library/LaunchAgents/com.post-pulsar.daemon.plist"
            [[ -e "$destination" ]] || {
                printf '%s\n' "POST PULSAR user LaunchAgent is not installed."
                return
            }
            grep -Fq -- "$marker" "$destination" || fail "refusing to remove unrelated launch agent: $destination"
            rm -- "$destination"
            printf '%s\n' "Removed future POST PULSAR background startup; no process was stopped."
            printf '%s\n' "If already loaded, an operator may explicitly run: launchctl bootout gui/$UID/com.post-pulsar.daemon"
            ;;
        *)
            fail "user service removal is unsupported on $os_name"
            ;;
    esac
}

setup_environment() {
    if [[ -e "$VENV_DIR/Scripts" ]]; then
        fail "foreign Windows layout found at '$VENV_DIR'. It is never removed automatically; review it, then remove it manually and rerun setup." 2
    fi
    if [[ -e "$VENV_DIR" && ! -x "$PYTHON" ]]; then
        fail "incomplete POSIX environment at '$VENV_DIR'. It is never removed automatically; review it, then remove it manually and rerun setup." 2
    fi

    command -v python3 >/dev/null 2>&1 || fail "Python 3.12 or newer is required"
    python3 -c 'import sys; raise SystemExit(0 if sys.version_info >= (3, 12) else 1)' || fail "Python 3.12 or newer is required"

    if [[ ! -x "$PYTHON" ]]; then
        python3 -m venv "$VENV_DIR"
    fi
    "$PYTHON" -m pip install --disable-pip-version-check "uv==$UV_VERSION"
    (
        cd -- "$SCRIPT_DIR"
        export VIRTUAL_ENV="$VENV_DIR"
        "$PYTHON" -m uv sync --frozen --active
    )
    printf '%s\n' "POST PULSAR is installed in $VENV_DIR."
    printf '%s\n' "Simple same-user setup is a convenience mode; it is not an agent sandbox."
    printf '%s\n' "Run: '$SCRIPT_DIR/run-post-pulsar.sh' --help"
    printf '%s\n' "Recommended separate-identity instructions: '$0' --hardened-guide"
}

case "${1:-}" in
    --hardened-guide)
        [[ $# -eq 1 ]] || fail "usage: $0 --hardened-guide"
        show_hardened_guide
        ;;
    --install-user-service)
        [[ $# -eq 1 ]] || fail "usage: $0 --install-user-service"
        setup_environment
        install_user_service
        ;;
    --remove-user-service)
        [[ $# -eq 1 ]] || fail "usage: $0 --remove-user-service"
        remove_user_service
        ;;
    "")
        setup_environment
        ;;
    *)
        fail "usage: $0 [--install-user-service|--remove-user-service|--hardened-guide]"
        ;;
esac
