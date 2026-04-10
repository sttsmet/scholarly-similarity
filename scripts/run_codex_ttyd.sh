#!/usr/bin/env bash
set -euo pipefail

WORKSPACE_ROOT="${WORKSPACE_ROOT:-$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)}"
TTYD_BIN="${TTYD_BIN:-ttyd}"
TTYD_HOST="${TTYD_HOST:-127.0.0.1}"
TTYD_PORT="${TTYD_PORT:-7681}"
TTYD_CREDENTIALS="${TTYD_CREDENTIALS:-}"
CODEX_BIN="${CODEX_BIN:-codex}"
CODEX_HOME="${CODEX_HOME:-$HOME/.codex}"
SHELL_BIN="${SHELL_BIN:-/bin/bash}"

if ! command -v "$TTYD_BIN" >/dev/null 2>&1; then
  echo "ttyd was not found on PATH. Install ttyd before starting this service." >&2
  exit 1
fi

if ! command -v "$CODEX_BIN" >/dev/null 2>&1; then
  echo "codex was not found on PATH. Install Codex CLI and log in before starting this service." >&2
  exit 1
fi

mkdir -p "$CODEX_HOME"
cd "$WORKSPACE_ROOT"

TTYD_ARGS=(
  -i "$TTYD_HOST"
  -p "$TTYD_PORT"
  -W
)

if [[ -n "$TTYD_CREDENTIALS" ]]; then
  TTYD_ARGS+=(-c "$TTYD_CREDENTIALS")
fi

CODEX_COMMAND="cd \"$WORKSPACE_ROOT\" && export CODEX_HOME=\"$CODEX_HOME\" && exec \"$CODEX_BIN\""

exec "$TTYD_BIN" "${TTYD_ARGS[@]}" "$SHELL_BIN" -lc "$CODEX_COMMAND"
