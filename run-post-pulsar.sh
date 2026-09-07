#!/usr/bin/env bash
# Copyright (C) 2024 Anson Phong
# SPDX-License-Identifier: GPL-3.0-only

set -euo pipefail

SCRIPT_DIR=$(CDPATH= cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P)
PYTHON="$SCRIPT_DIR/.venv/bin/python"

if [[ -e "$SCRIPT_DIR/.venv/Scripts" ]]; then
    printf '%s\n' "POST PULSAR: foreign Windows .venv layout; run setup-post-pulsar.sh after reviewing and manually removing .venv." >&2
    exit 2
fi
if [[ ! -x "$PYTHON" ]]; then
    printf '%s\n' "POST PULSAR is not set up. Run '$SCRIPT_DIR/setup-post-pulsar.sh'." >&2
    exit 2
fi

cd -- "$SCRIPT_DIR"
if [[ "${1:-}" == "daemon" && $# -eq 1 ]]; then
    exec "$PYTHON" -m post_pulsar "daemon" "foreground"
fi
exec "$PYTHON" -m post_pulsar "$@"
