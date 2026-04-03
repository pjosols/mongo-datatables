#!/usr/bin/env bash
# Security scanners → .ltc/reports/
set -euo pipefail

REPORTS=".ltc/reports"
mkdir -p "$REPORTS"

command -v semgrep >/dev/null 2>&1 && semgrep --config=auto --json -o "$REPORTS/semgrep.json" . 2>/dev/null || true
command -v trivy >/dev/null 2>&1 && trivy fs --format json -o "$REPORTS/trivy.json" . 2>/dev/null || true

if find . -name "*.tf" -o -name "*.yaml" -o -name "*.yml" -o -name "Dockerfile" 2>/dev/null | grep -q .; then
    command -v checkov >/dev/null 2>&1 && checkov -d . -o json > "$REPORTS/checkov.json" 2>/dev/null || true
fi

exit 0
