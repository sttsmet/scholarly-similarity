from __future__ import annotations

import sys
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from src.workspace_smoke import format_workspace_smoke_report, run_workspace_smoke, workspace_smoke_exit_code  # noqa: E402


def main() -> int:
    result = run_workspace_smoke(REPO_ROOT)
    print(format_workspace_smoke_report(result))
    return workspace_smoke_exit_code(result)


if __name__ == "__main__":
    raise SystemExit(main())
