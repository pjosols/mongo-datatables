"""Tests that pyproject.toml dependency versions are exactly pinned."""

import re
from pathlib import Path

import tomllib

PYPROJECT = Path(__file__).parent.parent / "pyproject.toml"
EXACT_PIN = re.compile(r"^[A-Za-z0-9_-]+==[0-9]+\.[0-9]+")


def _load() -> dict:
    with PYPROJECT.open("rb") as f:
        return tomllib.load(f)


def test_runtime_deps_exactly_pinned() -> None:
    """All runtime dependencies must use exact version pins (==)."""
    data = _load()
    deps = data["project"]["dependencies"]
    not_pinned = [d for d in deps if not EXACT_PIN.match(d)]
    assert not_pinned == [], f"Runtime deps not exactly pinned: {not_pinned}"


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
    assert any(d.startswith("pymongo==") for d in deps), "pymongo not found in dependencies"


def test_urllib3_present() -> None:
    """urllib3 must be listed as a runtime dependency."""
    data = _load()
    deps = data["project"]["dependencies"]
    assert any(d.startswith("urllib3==") for d in deps), "urllib3 not found in dependencies"
