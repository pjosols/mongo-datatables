"""Tests for removal of get_searchpanes_options wrapper from filter.py.

Verifies that:
- filter.py does not expose get_searchpanes_options
- core.py imports get_searchpanes_options directly from mongo_datatables.search_panes
- the canonical get_searchpanes_options lives in mongo_datatables.search_panes
"""
from pathlib import Path

_ROOT = Path(__file__).parent.parent.parent.parent / "mongo_datatables" / "datatables"


def test_filter_module_has_no_get_searchpanes_options():
    """filter.py source must not define or re-export get_searchpanes_options."""
    source = (_ROOT / "filter.py").read_text()
    assert "get_searchpanes_options" not in source


def test_filter_module_source_does_not_contain_alias():
    """filter.py source must not contain the removed _get_searchpanes_options_fn alias."""
    source = (_ROOT / "filter.py").read_text()
    assert "_get_searchpanes_options_fn" not in source


def test_filter_module_source_does_not_import_get_searchpanes_options():
    """filter.py source must not import get_searchpanes_options at all."""
    source = (_ROOT / "filter.py").read_text()
    assert "get_searchpanes_options" not in source


def test_search_panes_exposes_get_searchpanes_options():
    """get_searchpanes_options must be defined in mongo_datatables.search_panes."""
    source = (Path(__file__).parent.parent.parent.parent / "mongo_datatables" / "search_panes.py").read_text()
    assert "def get_searchpanes_options" in source


def test_core_imports_get_searchpanes_options_from_search_panes():
    """core.py must import get_searchpanes_options from mongo_datatables.search_panes."""
    source = (_ROOT / "core.py").read_text()
    assert "from mongo_datatables.search_panes import get_searchpanes_options" in source


def test_core_does_not_import_get_searchpanes_options_from_filter():
    """core.py must not import get_searchpanes_options from datatables.filter."""
    source = (_ROOT / "core.py").read_text()
    # If there's a filter import block, get_searchpanes_options must not be in it
    if "from mongo_datatables.datatables.filter import" in source:
        filter_import_block = source.split("from mongo_datatables.datatables.filter import")[1]
        # Extract until end of import statement (closing paren or next non-continuation line)
        assert "get_searchpanes_options" not in filter_import_block.split(")")[0]


def test_filter_public_api_excludes_get_searchpanes_options():
    """filter.py source must not define get_searchpanes_options as a public name."""
    source = (_ROOT / "filter.py").read_text()
    assert "def get_searchpanes_options" not in source
    assert "build_filter" in source
    assert "build_sort_specification" in source
    assert "build_projection" in source
