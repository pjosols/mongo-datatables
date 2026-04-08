"""Tests for resource exhaustion limits in aggregation pipeline construction."""
import logging
import pytest
from unittest.mock import MagicMock, patch
from pymongo.collection import Collection
from pymongo.database import Database

from mongo_datatables import DataTables, DataField
from mongo_datatables.datatables._limits import MAX_PIPELINE_STAGES, MAX_FACET_BRANCHES, MAX_PANE_OPTIONS
from mongo_datatables.search_panes import get_searchpanes_options
from mongo_datatables.utils import FieldMapper


_BASE_ARGS = {
    "draw": 1, "start": 0, "length": 10,
    "search": {"value": "", "regex": False},
    "order": [{"column": 0, "dir": "asc"}],
    "columns": [{"data": "name", "searchable": True, "orderable": True,
                 "search": {"value": "", "regex": False}}],
}


def _make_dt(extra_args=None, **kwargs):
    col = MagicMock(spec=Collection)
    col.list_indexes.return_value = []
    db = MagicMock(spec=Database)
    db.__getitem__ = MagicMock(return_value=col)
    mongo = MagicMock()
    mongo.db = db
    args = {**_BASE_ARGS, **(extra_args or {})}
    with patch.object(DataTables, "_check_text_index"):
        dt = DataTables(mongo, "test", args, **kwargs)
        dt.collection = col
        dt._has_text_index = False
    return dt, col


class TestPipelineStagesLimit:
    def test_stages_within_limit_accepted(self):
        stages = [{"$match": {"x": i}} for i in range(MAX_PIPELINE_STAGES)]
        dt, _ = _make_dt(pipeline_stages=stages)
        assert len(dt.pipeline_stages) == MAX_PIPELINE_STAGES

    def test_stages_exceeding_limit_truncated(self):
        stages = [{"$match": {"x": i}} for i in range(MAX_PIPELINE_STAGES + 5)]
        dt, _ = _make_dt(pipeline_stages=stages)
        assert len(dt.pipeline_stages) == MAX_PIPELINE_STAGES

    def test_stages_truncation_logs_warning(self, caplog):
        stages = [{"$match": {"x": i}} for i in range(MAX_PIPELINE_STAGES + 1)]
        with caplog.at_level(logging.WARNING, logger="mongo_datatables.datatables.core"):
            _make_dt(pipeline_stages=stages)
        assert any("truncated" in r.message for r in caplog.records)

    def test_empty_pipeline_stages_accepted(self):
        dt, _ = _make_dt(pipeline_stages=[])
        assert dt.pipeline_stages == []

    def test_none_pipeline_stages_defaults_empty(self):
        dt, _ = _make_dt(pipeline_stages=None)
        assert dt.pipeline_stages == []

    def test_exactly_max_stages_not_truncated(self):
        stages = [{"$match": {"x": i}} for i in range(MAX_PIPELINE_STAGES)]
        dt, _ = _make_dt(pipeline_stages=stages)
        assert len(dt.pipeline_stages) == MAX_PIPELINE_STAGES


class TestFacetBranchesLimit:
    def _make_columns(self, n: int):
        return [
            {"data": f"col{i}", "searchable": True, "orderable": True,
             "search": {"value": "", "regex": False}}
            for i in range(n)
        ]

    def _make_field_mapper(self, n: int):
        fields = [DataField(f"col{i}", "string") for i in range(n)]
        return FieldMapper(fields)

    def test_facet_branches_within_limit_all_included(self):
        n = MAX_FACET_BRANCHES
        columns = self._make_columns(n)
        field_mapper = self._make_field_mapper(n)
        col = MagicMock(spec=Collection)
        facet_doc = {f"col{i}": [{"_id": "x", "count": 1}] for i in range(n)}
        col.aggregate.return_value = iter([facet_doc])
        result = get_searchpanes_options(columns, field_mapper, {}, {}, col, False)
        assert len(result) == n

    def test_facet_branches_exceeding_limit_truncated(self):
        n = MAX_FACET_BRANCHES + 5
        columns = self._make_columns(n)
        field_mapper = self._make_field_mapper(n)
        col = MagicMock(spec=Collection)
        facet_doc = {f"col{i}": [{"_id": "x", "count": 1}] for i in range(n)}
        col.aggregate.return_value = iter([facet_doc])
        result = get_searchpanes_options(columns, field_mapper, {}, {}, col, False)
        assert len(result) <= MAX_FACET_BRANCHES

    def test_facet_branches_truncation_logs_warning(self, caplog):
        n = MAX_FACET_BRANCHES + 1
        columns = self._make_columns(n)
        field_mapper = self._make_field_mapper(n)
        col = MagicMock(spec=Collection)
        facet_doc = {f"col{i}": [{"_id": "x", "count": 1}] for i in range(n)}
        col.aggregate.side_effect = [[facet_doc], [facet_doc]]
        with caplog.at_level(logging.WARNING, logger="mongo_datatables.search_panes"):
            get_searchpanes_options(columns, field_mapper, {}, {}, col, False)
        assert any("truncated" in r.message for r in caplog.records)

    def test_single_facet_pipeline_call_regardless_of_column_count(self):
        """Verify only 2 aggregate calls are made (total + count) regardless of columns."""
        n = 10
        columns = self._make_columns(n)
        field_mapper = self._make_field_mapper(n)
        col = MagicMock(spec=Collection)
        facet_doc = {f"col{i}": [] for i in range(n)}
        col.aggregate.side_effect = [[facet_doc], [facet_doc]]
        get_searchpanes_options(columns, field_mapper, {}, {}, col, False)
        assert col.aggregate.call_count == 2


class TestPaneOptionsLimit:
    def test_pane_options_capped_at_max(self):
        n = MAX_PANE_OPTIONS + 10
        columns = [{"data": "status", "searchable": True, "orderable": True,
                    "search": {"value": "", "regex": False}}]
        field_mapper = FieldMapper([DataField("status", "string")])
        col = MagicMock(spec=Collection)
        facet_doc = {"status": [{"_id": f"val{i}", "count": 1} for i in range(n)]}
        col.aggregate.side_effect = [[facet_doc], [facet_doc]]
        result = get_searchpanes_options(columns, field_mapper, {}, {}, col, False)
        assert len(result["status"]) <= MAX_PANE_OPTIONS

    def test_pane_options_within_limit_all_returned(self):
        n = 5
        columns = [{"data": "status", "searchable": True, "orderable": True,
                    "search": {"value": "", "regex": False}}]
        field_mapper = FieldMapper([DataField("status", "string")])
        col = MagicMock(spec=Collection)
        facet_doc = {"status": [{"_id": f"val{i}", "count": n - i} for i in range(n)]}
        col.aggregate.side_effect = [[facet_doc], [facet_doc]]
        result = get_searchpanes_options(columns, field_mapper, {}, {}, col, False)
        assert len(result["status"]) == n


class TestAllowDiskUse:
    def test_allow_disk_use_passed_to_aggregate(self):
        columns = [{"data": "name", "searchable": True, "orderable": True,
                    "search": {"value": "", "regex": False}}]
        field_mapper = FieldMapper([DataField("name", "string")])
        col = MagicMock(spec=Collection)
        facet_doc = {"name": [{"_id": "Alice", "count": 1}]}
        col.aggregate.side_effect = [[facet_doc], [facet_doc]]
        get_searchpanes_options(columns, field_mapper, {}, {}, col, allow_disk_use=True)
        for call_args in col.aggregate.call_args_list:
            assert call_args.kwargs.get("allowDiskUse") is True

    def test_allow_disk_use_false_passed_to_aggregate(self):
        columns = [{"data": "name", "searchable": True, "orderable": True,
                    "search": {"value": "", "regex": False}}]
        field_mapper = FieldMapper([DataField("name", "string")])
        col = MagicMock(spec=Collection)
        facet_doc = {"name": [{"_id": "Alice", "count": 1}]}
        col.aggregate.side_effect = [[facet_doc], [facet_doc]]
        get_searchpanes_options(columns, field_mapper, {}, {}, col, allow_disk_use=False)
        for call_args in col.aggregate.call_args_list:
            assert call_args.kwargs.get("allowDiskUse") is False
