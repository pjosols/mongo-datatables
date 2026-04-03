#!/usr/bin/env bash
# Language-specific scanners from ltc config
set -euo pipefail

REPORTS=".ltc/reports"
mkdir -p "$REPORTS"

_run_scanners() {
    local lang="$1"
    local cmds
    cmds=$(uv run python -c "
from ltc.config import load_scanner_config
cfg = load_scanner_config()
for cmds in cfg.get('$lang', {}).values():
    for cmd in cmds:
        print(cmd)
" 2>/dev/null) || return 0
    while IFS= read -r cmd; do
        [ -z "$cmd" ] && continue
        eval "uv run $cmd" 2>/dev/null || true
    done <<< "$cmds"
}

if [ -f "pyproject.toml" ] || [ -f "setup.py" ]; then
    _run_scanners python
fi

if [ -f "tsconfig.json" ]; then
    _run_scanners typescript
fi

exit 0
