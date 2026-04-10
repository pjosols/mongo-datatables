"""Tests that pyproject.toml dependency versions are properly pinned."""

import re
from pathlib import Path

import tomllib

PYPROJECT = Path(__file__).parent.parent / "pyproject.toml"

# Regex: specifier must contain an upper bound (<N or <=N)
UPPER_BOUND = re.compile(r"<[=\d]")


def _load() -> dict:
    with PYPROJECT.open("rb") as f:
        return tomllib.load(f)


def test_runtime_deps_have_upper_bounds() -> None:
    """All runtime dependencies must have an upper-bound version specifier."""
    data = _load()
    deps = data["project"]["dependencies"]
    missing = [d for d in deps if not UPPER_BOUND.search(d)]
    assert missing == [], f"Runtime deps missing upper bound: {missing}"


def test_runtime_deps_have_lower_bounds() -> None:
    """All runtime dependencies must have a lower-bound version specifier."""
    data = _load()
    deps = data["project"]["dependencies"]
    missing = [d for d in deps if ">=" not in d and "==" not in d]
    assert missing == [], f"Runtime deps missing lower bound: {missing}"


def test_optional_deps_have_upper_bounds() -> None:
    """All optional/extra dependencies must have an upper-bound version specifier."""
    data = _load()
    extras = data["project"].get("optional-dependencies", {})
    missing = []
    for group, deps in extras.items():
        for d in deps:
            if not UPPER_BOUND.search(d):
                missing.append(f"[{group}] {d}")
    assert missing == [], f"Optional deps missing upper bound: {missing}"


def test_optional_deps_have_lower_bounds() -> None:
    """All optional/extra dependencies must have a lower-bound version specifier."""
    data = _load()
    extras = data["project"].get("optional-dependencies", {})
    missing = []
    for group, deps in extras.items():
        for d in deps:
            if ">=" not in d and "==" not in d:
                missing.append(f"[{group}] {d}")
    assert missing == [], f"Optional deps missing lower bound: {missing}"


def test_pymongo_pinned_below_major_5() -> None:
    """pymongo must be pinned below major version 5 to avoid breaking changes."""
    data = _load()
    deps = data["project"]["dependencies"]
    pymongo = next((d for d in deps if d.startswith("pymongo")), None)
    assert pymongo is not None, "pymongo not found in dependencies"
    assert "<5" in pymongo, f"pymongo not pinned below 5: {pymongo}"


def test_urllib3_pinned_below_major_3() -> None:
    """urllib3 must be pinned below major version 3."""
    data = _load()
    deps = data["project"]["dependencies"]
    urllib3 = next((d for d in deps if d.startswith("urllib3")), None)
    assert urllib3 is not None, "urllib3 not found in dependencies"
    assert "<3" in urllib3, f"urllib3 not pinned below 3: {urllib3}"
