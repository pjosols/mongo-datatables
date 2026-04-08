"""Tests for CVE-2026-25645 mitigation: requests predictable temp file vulnerability.

Verifies that the project enforces requests>=2.33.0 (the patched version)
via pyproject.toml constraint-dependencies and uv.lock resolution.
"""

from __future__ import annotations

import re
import tomllib
from pathlib import Path

ROOT = Path(__file__).parent.parent.parent
PYPROJECT = ROOT / "pyproject.toml"
UV_LOCK = ROOT / "uv.lock"

VULNERABLE_VERSION = (2, 32, 4)
FIXED_VERSION = (2, 33, 0)


def _parse_version(v: str) -> tuple[int, ...]:
    return tuple(int(x) for x in v.split(".")[:3])


def test_pyproject_constrains_requests_to_fixed_version() -> None:
    """pyproject.toml must pin requests to >=2.33.0 to exclude CVE-2026-25645."""
    data = tomllib.loads(PYPROJECT.read_text())
    constraints: list[str] = data.get("tool", {}).get("uv", {}).get("constraint-dependencies", [])
    requests_constraints = [c for c in constraints if c.startswith("requests")]
    assert requests_constraints, "No requests constraint found in [tool.uv] constraint-dependencies"
    # Extract the version bound, e.g. "requests>=2.33.0"
    for constraint in requests_constraints:
        match = re.search(r">=(\d+\.\d+\.\d+)", constraint)
        assert match, f"Expected >= constraint in: {constraint}"
        bound = _parse_version(match.group(1))
        assert bound >= FIXED_VERSION, (
            f"requests constraint {constraint!r} allows CVE-2026-25645 "
            f"(must be >=2.33.0, got >={'.'.join(str(x) for x in bound)})"
        )


def test_uv_lock_resolves_requests_to_patched_version() -> None:
    """uv.lock must resolve requests to >=2.33.0, not the vulnerable 2.32.4."""
    lock_text = UV_LOCK.read_text()
    # Find all resolved requests versions in the lock file
    versions = re.findall(r'^name = "requests"\nversion = "([^"]+)"', lock_text, re.MULTILINE)
    assert versions, "requests not found in uv.lock"
    for version_str in versions:
        resolved = _parse_version(version_str)
        assert resolved >= FIXED_VERSION, (
            f"uv.lock resolves requests to {version_str}, "
            f"which is vulnerable to CVE-2026-25645 (fixed in 2.33.0)"
        )
        assert resolved != VULNERABLE_VERSION, (
            f"uv.lock resolves requests to vulnerable version {version_str} (CVE-2026-25645)"
        )
