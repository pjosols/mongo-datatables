"""Tests for search[caseInsensitive] support."""
import pytest
from unittest.mock import MagicMock, patch
from mongo_datatables.query_builder import MongoQueryBuilder


@pytest.fixture
def qb():
    fm = MagicMock()
    fm.get_field_type.return_value = "string"
    fm.get_db_field.side_effect = lambda x: x
    return MongoQueryBuilder(fm, use_text_index=False, has_text_index=False)


# --- build_global_search ---

def test_global_search_default_case_insensitive(qb):
    result = qb.build_global_search(["hello"], ["name"])
    assert result["$or"][0]["name"]["$options"] == "i"


def test_global_search_explicit_case_insensitive_true(qb):
    result = qb.build_global_search(["hello"], ["name"], case_insensitive=True)
    assert result["$or"][0]["name"]["$options"] == "i"


def test_global_search_case_sensitive(qb):
    result = qb.build_global_search(["hello"], ["name"], case_insensitive=False)
    assert result["$or"][0]["name"]["$options"] == ""


def test_global_search_smart_multi_term_case_sensitive(qb):
    result = qb.build_global_search(["foo", "bar"], ["name"], search_smart=True, case_insensitive=False)
    # $and of $or per term
    assert result["$and"][0]["name"]["$options"] == ""
    assert result["$and"][1]["name"]["$options"] == ""


def test_global_search_quoted_phrase_case_sensitive(qb):
    result = qb.build_global_search(["hello world"], ["name"], original_search='"hello world"', case_insensitive=False)
    assert result["$or"][0]["name"]["$options"] == ""


# --- build_column_search ---

def test_column_search_default_case_insensitive(qb):
    columns = [{"data": "name", "searchable": True, "search": {"value": "Alice", "regex": False}}]
    result = qb.build_column_search(columns)
    assert result["$and"][0]["name"]["$options"] == "i"


def test_column_search_global_case_sensitive(qb):
    columns = [{"data": "name", "searchable": True, "search": {"value": "Alice", "regex": False}}]
    result = qb.build_column_search(columns, case_insensitive=False)
    assert result["$and"][0]["name"]["$options"] == ""


def test_column_search_per_column_override_sensitive(qb):
    """Per-column caseInsensitive=False overrides global True."""
    columns = [{"data": "name", "searchable": True, "search": {"value": "Alice", "regex": False, "caseInsensitive": False}}]
    result = qb.build_column_search(columns, case_insensitive=True)
    assert result["$and"][0]["name"]["$options"] == ""


def test_column_search_per_column_override_insensitive(qb):
    """Per-column caseInsensitive=True overrides global False."""
    columns = [{"data": "name", "searchable": True, "search": {"value": "Alice", "regex": False, "caseInsensitive": True}}]
    result = qb.build_column_search(columns, case_insensitive=False)
    assert result["$and"][0]["name"]["$options"] == "i"


def test_column_search_per_column_string_false(qb):
    """String 'false' coerced to False."""
    columns = [{"data": "name", "searchable": True, "search": {"value": "Alice", "regex": False, "caseInsensitive": "false"}}]
    result = qb.build_column_search(columns, case_insensitive=True)
    assert result["$and"][0]["name"]["$options"] == ""


# --- build_column_specific_search ---

def test_colon_search_default_case_insensitive(qb):
    result = qb.build_column_specific_search(["name:Alice"], ["name"])
    assert result["$and"][0]["name"]["$options"] == "i"


def test_colon_search_case_sensitive(qb):
    result = qb.build_column_specific_search(["name:Alice"], ["name"], case_insensitive=False)
    assert result["$and"][0]["name"]["$options"] == ""


# --- datatables.py integration ---

def _make_dt(search_dict):
    from mongo_datatables import DataTables
    collection = MagicMock()
    collection.aggregate.return_value = iter([])
    request_args = {
        "draw": 1, "start": 0, "length": 10,
        "search": search_dict,
        "order": [],
        "columns": [{"data": "name", "searchable": True, "orderable": True, "search": {"value": "", "regex": False}}],
    }
    return DataTables(MagicMock(), collection, request_args, ["name"])


def test_datatables_global_search_case_insensitive_false():
    """DataTables request with search[caseInsensitive]=false produces case-sensitive query."""
    dt = _make_dt({"value": "Alice", "regex": False, "caseInsensitive": "false"})
    cond = dt.global_search_condition
    assert cond["$or"][0]["name"]["$options"] == ""


def test_datatables_global_search_case_insensitive_default():
    """DataTables request without caseInsensitive defaults to case-insensitive."""
    dt = _make_dt({"value": "Alice", "regex": False})
    cond = dt.global_search_condition
    assert cond["$or"][0]["name"]["$options"] == "i"
