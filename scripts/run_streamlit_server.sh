#!/usr/bin/env bash
set -euo pipefail

WORKSPACE_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$WORKSPACE_ROOT"

if [[ -x "$WORKSPACE_ROOT/.venv/bin/python" ]]; then
  PYTHON_BIN="$WORKSPACE_ROOT/.venv/bin/python"
else
  PYTHON_BIN="python3"
fi

exec "$PYTHON_BIN" -m streamlit run src/ui/streamlit_app.py
