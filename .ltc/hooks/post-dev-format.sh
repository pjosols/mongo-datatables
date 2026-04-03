#!/usr/bin/env bash
# Format and lint
set -euo pipefail

if [ -f "pyproject.toml" ] || [ -f "setup.py" ]; then
    uv run ruff format --quiet . 2>/dev/null || true
    uv run ruff check --fix --quiet . 2>/dev/null || true
fi

exit 0
