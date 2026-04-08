"""Tests for the datatables/ subpackage consolidation.

Verifies new subpackage import paths and that the public API
remains intact after moving DataTables and internals into datatables/.
"""
import pytest


# ---------------------------------------------------------------------------
# Top-level public API
# ---------------------------------------------------------------------------

def test_top_level_import_datatables():
    from mongo_datatables import DataTables
    assert DataTables is not None


def test_top_level_import_datafield():
    from mongo_datatables import DataField
    assert DataField is not None


def test_datatables_and_datafield_same_object_across_paths():
    from mongo_datatables import DataTables as A, DataField as AF
    from mongo_datatables.datatables import DataTables as B, DataField as BF
    from mongo_datatables.datatables.core import DataTables as C
    assert A is B is C
    assert AF is BF


# ---------------------------------------------------------------------------
# datatables/query/ subpackage
# ---------------------------------------------------------------------------

def test_query_subpackage_exports_builder():
    from mongo_datatables.datatables.query import MongoQueryBuilder
    assert MongoQueryBuilder is not None


def test_query_subpackage_conditions_importable():
    from mongo_datatables.datatables.query.conditions import (
        parse_operator, build_number_condition, build_date_condition,
    )
    assert callable(parse_operator)


def test_query_subpackage_regex_utils_importable():
    from mongo_datatables.datatables.query.regex_utils import validate_regex, safe_regex
    assert callable(validate_regex)
    assert callable(safe_regex)


def test_query_subpackage_column_control_importable():
    from mongo_datatables.datatables.query.column_control import build_column_control_conditions
    assert callable(build_column_control_conditions)


def test_query_subpackage_global_search_importable():
    from mongo_datatables.datatables.query.global_search import build_global_search
    assert callable(build_global_search)


# ---------------------------------------------------------------------------
# datatables/ direct modules
# ---------------------------------------------------------------------------

def test_datatables_formatting_importable():
    from mongo_datatables.datatables.formatting import format_result_values, remap_aliases, process_cursor
    assert callable(format_result_values)


def test_datatables_request_validator_importable():
    from mongo_datatables.datatables.request_validator import validate_request_args
    assert callable(validate_request_args)


# ---------------------------------------------------------------------------
# Functional smoke tests on canonical paths
# ---------------------------------------------------------------------------

def test_parse_operator_canonical():
    from mongo_datatables.datatables.query.conditions import parse_operator
    op, val = parse_operator(">=10")
    assert op == ">="
    assert val == "10"


def test_build_number_condition_canonical():
    from mongo_datatables.datatables.query.conditions import build_number_condition
    result = build_number_condition("age", "25", ">")
    assert result == {"age": {"$gt": 25}}


def test_validate_regex_canonical_valid():
    from mongo_datatables.datatables.query.regex_utils import validate_regex
    assert validate_regex("hello.*world") == "hello.*world"


def test_validate_regex_canonical_invalid():
    from mongo_datatables.datatables.query.regex_utils import validate_regex
    with pytest.raises(ValueError):
        validate_regex("[invalid")


def test_safe_regex_canonical_escapes():
    from mongo_datatables.datatables.query.regex_utils import safe_regex
    result = safe_regex("hello.world", False)
    assert "\\." in result


def test_build_column_control_conditions_canonical():
    from mongo_datatables.datatables.query.column_control import build_column_control_conditions
    cc = {"list": {"0": "active", "1": "inactive"}}
    result = build_column_control_conditions("status", "text", cc)
    assert len(result) == 1
    assert "$in" in result[0]["status"]


def test_build_global_search_canonical_empty():
    from mongo_datatables.datatables.query.global_search import build_global_search
    from mongo_datatables.utils import FieldMapper
    fm = FieldMapper([])
    result = build_global_search(fm, False, False, False, [], ["name"])
    assert result == {}


def test_validate_request_args_canonical():
    from mongo_datatables.datatables.request_validator import validate_request_args
    args = {
        "draw": "1",
        "start": "0",
        "length": "10",
        "search": {"value": "", "regex": "false"},
        "columns": [],
        "order": [],
    }
    result = validate_request_args(args)
    assert result["draw"] == 1
    assert result["start"] == 0
