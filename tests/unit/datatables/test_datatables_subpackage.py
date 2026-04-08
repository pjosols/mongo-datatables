"""Tests for the datatables subpackage refactor.

Verifies public API compatibility, subpackage exports, and core logic
in datatables/filter.py and datatables/results.py.
"""
import pytest
from unittest.mock import MagicMock, patch
from pymongo.errors import PyMongoError


# ---------------------------------------------------------------------------
# Public API compatibility
# ---------------------------------------------------------------------------

def test_top_level_import_datatables():
    from mongo_datatables import DataTables
    assert DataTables is not None


def test_top_level_import_datafield():
    from mongo_datatables import DataField
    assert DataField is not None


def test_subpackage_lazy_import_datatables():
    from mongo_datatables.datatables import DataTables
    assert DataTables is not None


def test_subpackage_lazy_import_datafield():
    from mongo_datatables.datatables import DataField
    assert DataField is not None


def test_top_level_and_subpackage_datatables_same_class():
    from mongo_datatables import DataTables as A
    from mongo_datatables.datatables import DataTables as B
    assert A is B


def test_top_level_and_subpackage_datafield_same_class():
    from mongo_datatables import DataField as A
    from mongo_datatables.datatables import DataField as B
    assert A is B


def test_subpackage_getattr_unknown_raises():
    import mongo_datatables.datatables as pkg
    with pytest.raises(AttributeError):
        _ = pkg.NonExistent


# ---------------------------------------------------------------------------
# Subpackage __init__ re-exports
# ---------------------------------------------------------------------------

def test_subpackage_exports_filter_functions():
    from mongo_datatables.datatables import (
        build_filter,
        build_sort_specification,
        build_projection,
        get_searchpanes_options,
    )
    assert callable(build_filter)
    assert callable(build_sort_specification)
    assert callable(build_projection)
    assert callable(get_searchpanes_options)


def test_subpackage_exports_results_functions():
    from mongo_datatables.datatables import (
        build_pipeline,
        fetch_results,
        get_rowgroup_data,
        count_total,
        count_filtered,
    )
    assert callable(build_pipeline)
    assert callable(fetch_results)
    assert callable(get_rowgroup_data)
    assert callable(count_total)
    assert callable(count_filtered)


# ---------------------------------------------------------------------------
# datatables/filter.py — build_projection
# ---------------------------------------------------------------------------

def test_build_projection_basic():
    from mongo_datatables.datatables.filter import build_projection
    from mongo_datatables.utils import FieldMapper

    fm = FieldMapper([])
    columns = [{"data": "name"}, {"data": "age"}]
    proj = build_projection(columns, fm)
    assert proj["name"] == 1
    assert proj["age"] == 1
    assert proj["_id"] == 1


def test_build_projection_with_row_id():
    from mongo_datatables.datatables.filter import build_projection
    from mongo_datatables.utils import FieldMapper

    fm = FieldMapper([])
    columns = [{"data": "name"}]
    proj = build_projection(columns, fm, row_id="custom_id")
    assert proj["custom_id"] == 1


def test_build_projection_skips_empty_data():
    from mongo_datatables.datatables.filter import build_projection
    from mongo_datatables.utils import FieldMapper

    fm = FieldMapper([])
    columns = [{"data": ""}, {"data": None}, {"data": "title"}]
    proj = build_projection(columns, fm)
    assert "title" in proj
    assert "" not in proj


# ---------------------------------------------------------------------------
# datatables/filter.py — build_sort_specification
# ---------------------------------------------------------------------------

def test_build_sort_specification_asc():
    from mongo_datatables.datatables.filter import build_sort_specification
    from mongo_datatables.utils import FieldMapper

    fm = FieldMapper([])
    columns = [{"data": "name", "orderable": True}]
    request_args = {"order": [{"column": 0, "dir": "asc", "name": ""}]}
    sort = build_sort_specification(request_args, columns, fm)
    assert sort["name"] == 1


def test_build_sort_specification_desc():
    from mongo_datatables.datatables.filter import build_sort_specification
    from mongo_datatables.utils import FieldMapper

    fm = FieldMapper([])
    columns = [{"data": "name", "orderable": True}]
    request_args = {"order": [{"column": 0, "dir": "desc", "name": ""}]}
    sort = build_sort_specification(request_args, columns, fm)
    assert sort["name"] == -1


def test_build_sort_specification_always_includes_id_tiebreak():
    from mongo_datatables.datatables.filter import build_sort_specification
    from mongo_datatables.utils import FieldMapper

    fm = FieldMapper([])
    columns = [{"data": "name", "orderable": True}]
    request_args = {"order": [{"column": 0, "dir": "asc", "name": ""}]}
    sort = build_sort_specification(request_args, columns, fm)
    assert "_id" in sort


def test_build_sort_specification_empty_order():
    from mongo_datatables.datatables.filter import build_sort_specification
    from mongo_datatables.utils import FieldMapper

    fm = FieldMapper([])
    sort = build_sort_specification({"order": []}, [], fm)
    assert sort == {"_id": 1}


# ---------------------------------------------------------------------------
# datatables/results.py — build_pipeline
# ---------------------------------------------------------------------------

def test_build_pipeline_basic_structure():
    from mongo_datatables.datatables.results import build_pipeline

    pipeline = build_pipeline(
        current_filter={"status": "active"},
        pipeline_stages=[],
        sort_specification={"name": 1},
        projection={"name": 1},
        start=0,
        limit=10,
    )
    ops = [list(s.keys())[0] for s in pipeline]
    assert "$match" in ops
    assert "$sort" in ops
    assert "$limit" in ops
    assert "$project" in ops


def test_build_pipeline_no_skip_when_start_zero():
    from mongo_datatables.datatables.results import build_pipeline

    pipeline = build_pipeline({}, [], {"_id": 1}, {"_id": 1}, start=0, limit=10)
    ops = [list(s.keys())[0] for s in pipeline]
    assert "$skip" not in ops


def test_build_pipeline_skip_when_start_nonzero():
    from mongo_datatables.datatables.results import build_pipeline

    pipeline = build_pipeline({}, [], {"_id": 1}, {"_id": 1}, start=5, limit=10)
    ops = [list(s.keys())[0] for s in pipeline]
    assert "$skip" in ops


def test_build_pipeline_no_paginate():
    from mongo_datatables.datatables.results import build_pipeline

    pipeline = build_pipeline({}, [], {"_id": 1}, {"_id": 1}, start=0, limit=10, paginate=False)
    ops = [list(s.keys())[0] for s in pipeline]
    assert "$skip" not in ops
    assert "$limit" not in ops


def test_build_pipeline_text_filter_match_first():
    from mongo_datatables.datatables.results import build_pipeline

    text_filter = {"$text": {"$search": "hello"}}
    pipeline = build_pipeline(text_filter, [{"$lookup": {}}], {"_id": 1}, {"_id": 1}, 0, 10)
    # $text filter must be first stage
    assert list(pipeline[0].keys())[0] == "$match"


def test_build_pipeline_no_match_when_empty_filter():
    from mongo_datatables.datatables.results import build_pipeline

    pipeline = build_pipeline({}, [], {"_id": 1}, {"_id": 1}, 0, 10)
    ops = [list(s.keys())[0] for s in pipeline]
    assert "$match" not in ops


# ---------------------------------------------------------------------------
# datatables/results.py — count_total
# ---------------------------------------------------------------------------

def test_count_total_uses_estimated_for_large_collections():
    from mongo_datatables.datatables.results import count_total

    col = MagicMock()
    col.estimated_document_count.return_value = 200000
    col.count_documents.return_value = 200000
    result = count_total(col, {})
    col.estimated_document_count.assert_called_once()
    # No custom filter and large collection → estimated used directly
    col.count_documents.assert_not_called()
    assert result == 200000


def test_count_total_uses_count_documents_for_small_collections():
    from mongo_datatables.datatables.results import count_total

    col = MagicMock()
    col.estimated_document_count.return_value = 50
    col.count_documents.return_value = 50
    result = count_total(col, {})
    col.count_documents.assert_called_once_with({})
    assert result == 50


def test_count_total_returns_zero_on_error():
    from mongo_datatables.datatables.results import count_total

    col = MagicMock()
    col.estimated_document_count.side_effect = PyMongoError("fail")
    col.count_documents.side_effect = PyMongoError("fail")
    result = count_total(col, {})
    assert result == 0


# ---------------------------------------------------------------------------
# datatables/results.py — count_filtered
# ---------------------------------------------------------------------------

def test_count_filtered_returns_total_when_no_filter():
    from mongo_datatables.datatables.results import count_filtered

    col = MagicMock()
    result = count_filtered(col, {}, [], total_count=42, allow_disk_use=False)
    col.aggregate.assert_not_called()
    assert result == 42


def test_count_filtered_uses_aggregation():
    from mongo_datatables.datatables.results import count_filtered

    col = MagicMock()
    col.aggregate.return_value = [{"total": 7}]
    result = count_filtered(col, {"status": "active"}, [], total_count=100, allow_disk_use=False)
    assert result == 7


def test_count_filtered_returns_zero_on_empty_aggregation():
    from mongo_datatables.datatables.results import count_filtered

    col = MagicMock()
    col.aggregate.return_value = []
    result = count_filtered(col, {"status": "active"}, [], total_count=100, allow_disk_use=False)
    assert result == 0


def test_count_filtered_falls_back_to_count_documents_on_error():
    from mongo_datatables.datatables.results import count_filtered

    col = MagicMock()
    col.aggregate.side_effect = PyMongoError("agg fail")
    col.count_documents.return_value = 3
    result = count_filtered(col, {"x": 1}, [], total_count=10, allow_disk_use=False)
    assert result == 3


# ---------------------------------------------------------------------------
# datatables/results.py — fetch_results
# ---------------------------------------------------------------------------

def test_fetch_results_returns_empty_list_on_pymongo_error():
    from mongo_datatables.datatables.results import fetch_results
    from mongo_datatables.utils import FieldMapper

    col = MagicMock()
    col.aggregate.side_effect = PyMongoError("fail")
    fm = FieldMapper([])
    result = fetch_results(col, [], None, fm, None, None, None, False)
    assert result == []


def test_fetch_results_returns_empty_list_on_value_error():
    from mongo_datatables.datatables.results import fetch_results
    from mongo_datatables.utils import FieldMapper

    col = MagicMock()
    col.aggregate.return_value = iter([])
    fm = FieldMapper([])
    with patch("mongo_datatables.datatables.results.process_cursor", side_effect=ValueError("bad data")):
        result = fetch_results(col, [], None, fm, None, None, None, False)
    assert result == []


# ---------------------------------------------------------------------------
# datatables/results.py — get_rowgroup_data
# ---------------------------------------------------------------------------

def test_get_rowgroup_data_returns_none_when_no_rowgroup():
    from mongo_datatables.datatables.results import get_rowgroup_data
    from mongo_datatables.utils import FieldMapper

    col = MagicMock()
    fm = FieldMapper([])
    result = get_rowgroup_data(col, [], fm, {}, {}, False)
    assert result is None


def test_get_rowgroup_data_returns_none_for_invalid_datasrc():
    from mongo_datatables.datatables.results import get_rowgroup_data
    from mongo_datatables.utils import FieldMapper

    col = MagicMock()
    fm = FieldMapper([])
    request_args = {"rowGroup": {"dataSrc": ["invalid"]}}
    result = get_rowgroup_data(col, [], fm, {}, request_args, False)
    assert result is None


def test_get_rowgroup_data_returns_groups():
    from mongo_datatables.datatables.results import get_rowgroup_data
    from mongo_datatables.utils import FieldMapper

    col = MagicMock()
    col.aggregate.return_value = [{"_id": "A", "count": 3}, {"_id": "B", "count": 1}]
    fm = FieldMapper([])
    request_args = {"rowGroup": {"dataSrc": "status"}}
    result = get_rowgroup_data(col, [], fm, {}, request_args, False)
    assert result is not None
    assert result["dataSrc"] == "status"
    assert result["groups"]["A"]["count"] == 3


# ---------------------------------------------------------------------------
# Orphaned module check — files removed during datatables/ consolidation
# ---------------------------------------------------------------------------

import os
import importlib

_ORPHANED_MODULES = [
    "datatables_core",
    "query_builder",
    "query_conditions",
    "query_global_search",
    "column_control",
    "regex_utils",
    "formatting",
    "request_validator",
]

_PACKAGE_DIR = os.path.join(os.path.dirname(__file__), "..", "..", "mongo_datatables")


@pytest.mark.parametrize("module_name", _ORPHANED_MODULES)
def test_orphaned_file_does_not_exist(module_name: str) -> None:
    path = os.path.join(_PACKAGE_DIR, f"{module_name}.py")
    assert not os.path.exists(path), f"Orphaned file still present: mongo_datatables/{module_name}.py"


@pytest.mark.parametrize("module_name", _ORPHANED_MODULES)
def test_orphaned_module_not_importable(module_name: str) -> None:
    with pytest.raises(ModuleNotFoundError):
        importlib.import_module(f"mongo_datatables.{module_name}")
