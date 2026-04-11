"""Verify pyproject.toml dependency constraints follow library conventions."""

import re
from pathlib import Path

import tomllib

PYPROJECT = Path(__file__).parent.parent / "pyproject.toml"
EXACT_PIN = re.compile(r"^[A-Za-z0-9_-]+==[0-9]+\.[0-9]+")
RANGE_PIN = re.compile(r"^[A-Za-z0-9_-]+>=")


def _load() -> dict:
    with PYPROJECT.open("rb") as f:
        return tomllib.load(f)


def test_runtime_deps_use_ranges() -> None:
    """Runtime dependencies must use version ranges, not exact pins (library convention)."""
    data = _load()
    deps = data["project"]["dependencies"]
    exact = [d for d in deps if EXACT_PIN.match(d)]
    assert exact == [], f"Runtime deps must not use exact pins (library): {exact}"


def test_optional_deps_exactly_pinned() -> None:
    """All optional/extra dependencies must use exact version pins (==)."""
    data = _load()
    extras = data["project"].get("optional-dependencies", {})
    not_pinned = []
    for group, deps in extras.items():
        for d in deps:
            if not EXACT_PIN.match(d):
                not_pinned.append(f"[{group}] {d}")
    assert not_pinned == [], f"Optional deps not exactly pinned: {not_pinned}"


def test_pymongo_present() -> None:
    """pymongo must be listed as a runtime dependency."""
    data = _load()
    deps = data["project"]["dependencies"]
    assert any(d.startswith("pymongo") for d in deps), "pymongo not found in dependencies"


def test_urllib3_not_direct_dep() -> None:
    """urllib3 must not be a direct runtime dependency (it is a transitive dep of pymongo)."""
    data = _load()
    deps = data["project"]["dependencies"]
    assert not any(d.startswith("urllib3") for d in deps), "urllib3 should not be a direct dependency"
