"""Test DataTables pipeline building, count optimization, and pipeline_stages."""
from unittest.mock import MagicMock, Mock, patch
from pymongo.collection import Collection
from pymongo.errors import PyMongoError

from mongo_datatables import DataTables, DataField
from mongo_datatables.datatables.results import build_pipeline
from tests.base_test import BaseDataTablesTest


_BASE_ARGS = {
    "draw": "1",
    "start": "0",
    "length": "10",
    "search": {"value": "", "regex": False},
    "order": [{"column": "0", "dir": "asc", "name": ""}],
    "columns": [{"data": "name", "searchable": "true", "orderable": "true",
                  "search": {"value": "", "regex": False}, "name": ""}],
}

_P2_BASE_ARGS = {
    "draw": 1, "start": 0, "length": 10,
    "search": {"value": "", "regex": False},
    "order": [{"column": 0, "dir": "asc"}],
    "columns": [{"data": "Title", "searchable": True, "orderable": True,
                  "search": {"value": "", "regex": False}}],
}


def _make_dt(args=None, **kwargs):
    mongo = MagicMock()
    col = MagicMock()
    col.list_indexes.return_value = []
    mongo.__getitem__ = MagicMock(return_value=col)
    with patch.object(DataTables, '_check_text_index'):
        dt = DataTables(mongo, 'test', args or _BASE_ARGS, **kwargs)
        dt.collection = col
        dt._has_text_index = False
    return dt


def _make_p2_dt(request_args, data_fields=None, **custom_filter):
    col = MagicMock(spec=Collection)
    col.list_indexes = MagicMock(return_value=[])
    col.aggregate = MagicMock(return_value=iter([]))
    col.count_documents = MagicMock(return_value=0)
    col.estimated_document_count = MagicMock(return_value=0)
    db = {"test": col}
    return DataTables(db, "test", request_args, data_fields or [], **custom_filter), col


class TestBuildPipelineStructure:
    def test_paginate_true_includes_skip_and_limit(self):
        args = {**_BASE_ARGS, "start": "20", "length": "10"}
        dt = _make_dt(args)
        pipeline = build_pipeline(dt.filter, dt.pipeline_stages, dt.sort_specification, dt.projection, dt.start, dt.limit, paginate=True)
        stages = [list(s.keys())[0] for s in pipeline]
        assert "$skip" in stages
        assert "$limit" in stages

    def test_paginate_false_excludes_skip_and_limit(self):
        args = {**_BASE_ARGS, "start": "20", "length": "10"}
        dt = _make_dt(args)
        pipeline = build_pipeline(dt.filter, dt.pipeline_stages, dt.sort_specification, dt.projection, dt.start, dt.limit, paginate=False)
        stages = [list(s.keys())[0] for s in pipeline]
        assert "$skip" not in stages
        assert "$limit" not in stages

    def test_always_ends_with_project(self):
        dt = _make_dt()
        for paginate in (True, False):
            pipeline = build_pipeline(dt.filter, dt.pipeline_stages, dt.sort_specification, dt.projection, dt.start, dt.limit, paginate=paginate)
            assert list(pipeline[-1].keys())[0] == "$project"

    def test_no_match_when_no_filter(self):
        dt = _make_dt()
        pipeline = build_pipeline(dt.filter, dt.pipeline_stages, dt.sort_specification, dt.projection, dt.start, dt.limit)
        stages = [list(s.keys())[0] for s in pipeline]
        assert "$match" not in stages

    def test_skip_omitted_when_start_is_zero(self):
        dt = _make_dt()
        pipeline = build_pipeline(dt.filter, dt.pipeline_stages, dt.sort_specification, dt.projection, dt.start, dt.limit, paginate=True)
        stages = [list(s.keys())[0] for s in pipeline]
        assert "$skip" not in stages

    def test_limit_omitted_when_length_is_negative_one(self):
        args = {**_BASE_ARGS, "length": "-1"}
        dt = _make_dt(args)
        pipeline = build_pipeline(dt.filter, dt.pipeline_stages, dt.sort_specification, dt.projection, dt.start, dt.limit, paginate=True)
        stages = [list(s.keys())[0] for s in pipeline]
        assert "$limit" not in stages

    def test_default_paginate_is_true(self):
        args = {**_BASE_ARGS, "start": "5", "length": "10"}
        dt = _make_dt(args)
        assert build_pipeline(dt.filter, dt.pipeline_stages, dt.sort_specification, dt.projection, dt.start, dt.limit) == build_pipeline(dt.filter, dt.pipeline_stages, dt.sort_specification, dt.projection, dt.start, dt.limit, paginate=True)


class TestBuildPipelineConsistency:
    def test_results_and_export_share_match_stage(self):
        dt = _make_dt()
        paginated = build_pipeline(dt.filter, dt.pipeline_stages, dt.sort_specification, dt.projection, dt.start, dt.limit, paginate=True)
        export = build_pipeline(dt.filter, dt.pipeline_stages, dt.sort_specification, dt.projection, dt.start, dt.limit, paginate=False)
        p_match = next((s for s in paginated if "$match" in s), None)
        e_match = next((s for s in export if "$match" in s), None)
        assert p_match == e_match

    def test_results_and_export_share_sort_stage(self):
        dt = _make_dt()
        paginated = build_pipeline(dt.filter, dt.pipeline_stages, dt.sort_specification, dt.projection, dt.start, dt.limit, paginate=True)
        export = build_pipeline(dt.filter, dt.pipeline_stages, dt.sort_specification, dt.projection, dt.start, dt.limit, paginate=False)
        p_sort = next((s for s in paginated if "$sort" in s), None)
        e_sort = next((s for s in export if "$sort" in s), None)
        assert p_sort == e_sort

    def test_results_and_export_share_project_stage(self):
        dt = _make_dt()
        paginated = build_pipeline(dt.filter, dt.pipeline_stages, dt.sort_specification, dt.projection, dt.start, dt.limit, paginate=True)
        export = build_pipeline(dt.filter, dt.pipeline_stages, dt.sort_specification, dt.projection, dt.start, dt.limit, paginate=False)
        assert paginated[-1] == export[-1]


class TestCountOptimization:
    """Test optimized count operations."""

    def setup_method(self):
        self.mock_collection = Mock()
        self.mock_collection.list_indexes.return_value = []
        self.mock_db = {"test_collection": self.mock_collection}

    def _base_request(self, search_value="", columns=None):
        return {
            "draw": 1, "start": 0, "length": 10,
            "search": {"value": search_value, "regex": False},
            "columns": columns or [],
            "order": [],
        }

    def test_count_total_uses_estimated_for_large_collections(self):
        mock_collection = Mock()
        mock_collection.estimated_document_count.return_value = 500000
        mock_collection.count_documents.return_value = 500000
        mock_collection.list_indexes.return_value = []
        dt = DataTables({"test_collection": mock_collection}, "test_collection",
                        self._base_request())
        result = dt.count_total()
        assert result == 500000
        mock_collection.estimated_document_count.assert_called_once()
        mock_collection.count_documents.assert_not_called()

    def test_count_total_uses_exact_for_small_collections(self):
        mock_collection = Mock()
        mock_collection.estimated_document_count.return_value = 50000
        mock_collection.count_documents.return_value = 50000
        mock_collection.list_indexes.return_value = []
        dt = DataTables({"test_collection": mock_collection}, "test_collection",
                        self._base_request())
        result = dt.count_total()
        assert result == 50000
        mock_collection.estimated_document_count.assert_called_once()
        mock_collection.count_documents.assert_called_once_with({})

    def test_count_filtered_uses_aggregation_pipeline(self):
        mock_collection = Mock()
        mock_collection.aggregate.return_value = [{"total": 25000}]
        mock_collection.list_indexes.return_value = []
        mock_collection.estimated_document_count.return_value = 0
        mock_collection.count_documents.return_value = 0
        columns = [{"data": "name", "searchable": True, "orderable": True,
                    "search": {"value": "", "regex": False}}]
        dt = DataTables({"test_collection": mock_collection}, "test_collection",
                        self._base_request(search_value="test", columns=columns))
        result = dt.count_filtered()
        assert result == 25000
        mock_collection.aggregate.assert_called_once()
        call_args = mock_collection.aggregate.call_args[0][0]
        assert len(call_args) == 2
        assert "$match" in call_args[0]
        assert "$count" in call_args[1]

    def test_count_operations_handle_errors_gracefully(self):
        mock_collection = Mock()
        mock_collection.estimated_document_count.side_effect = PyMongoError("Connection error")
        mock_collection.count_documents.side_effect = PyMongoError("Connection error")
        mock_collection.aggregate.side_effect = PyMongoError("Connection error")
        mock_collection.list_indexes.return_value = []
        dt = DataTables({"test_collection": mock_collection}, "test_collection",
                        self._base_request())
        assert dt.count_total() == 0
        assert dt.count_filtered() == 0

    def test_count_total_with_custom_filter_large_collection(self):
        self.mock_collection.estimated_document_count.return_value = 500_000
        self.mock_collection.count_documents.return_value = 1_200
        columns = [{"data": "name", "searchable": "true", "orderable": "true",
                    "search": {"value": "", "regex": False}}]
        dt = DataTables(
            self.mock_db, "test_collection",
            {"draw": "1", "start": "0", "length": "10",
             "search": {"value": "", "regex": False},
             "columns": columns,
             "order": [{"column": "0", "dir": "asc"}]},
            status="active",
        )
        result = dt.count_total()
        self.mock_collection.count_documents.assert_called_once_with({"status": "active"})
        assert result == 1_200

    def test_count_total_with_custom_filter_small_collection(self):
        self.mock_collection.estimated_document_count.return_value = 50
        self.mock_collection.count_documents.return_value = 30
        columns = [{"data": "name", "searchable": "true", "orderable": "true",
                    "search": {"value": "", "regex": False}}]
        dt = DataTables(
            self.mock_db, "test_collection",
            {"draw": "1", "start": "0", "length": "10",
             "search": {"value": "", "regex": False},
             "columns": columns,
             "order": [{"column": "0", "dir": "asc"}]},
            role="admin",
        )
        result = dt.count_total()
        self.mock_collection.count_documents.assert_called_once_with({"role": "admin"})
        assert result == 30


def test_count_total_no_int_conversion_needed():
    mock_collection = Mock()
    mock_collection.estimated_document_count.return_value = 200000
    mock_collection.list_indexes.return_value = []
    dt = DataTables({"test_collection": mock_collection}, "test_collection",
                    {"draw": 1, "start": 0, "length": 10,
                     "search": {"value": "", "regex": False},
                     "columns": [], "order": []})
    assert dt.count_total() == 200000
    mock_collection.count_documents.assert_not_called()


class TestPipelineStages:
    """Tests for the pipeline_stages parameter."""

    FIELDS = [DataField("name", "string")]

    def _base_args(self, search_value=""):
        return {
            "draw": 1, "start": 0, "length": 10,
            "search": {"value": search_value, "regex": False},
            "order": [{"column": 0, "dir": "asc"}],
            "columns": [{"data": "name", "searchable": True, "orderable": True,
                         "search": {"value": "", "regex": False}}],
        }

    def _make_dt(self, pipeline_stages=None, search_value=""):
        col = MagicMock()
        col.aggregate = MagicMock(return_value=iter([]))
        col.count_documents = MagicMock(return_value=0)
        col.estimated_document_count = MagicMock(return_value=0)
        col.list_indexes = MagicMock(return_value=[])
        db = {"test": col}
        return DataTables(db, "test", self._base_args(search_value), self.FIELDS,
                          pipeline_stages=pipeline_stages), col

    def test_default_none_no_extra_stages(self):
        dt, _ = self._make_dt()
        assert dt.pipeline_stages == []
        pipeline = build_pipeline(dt.filter, dt.pipeline_stages, dt.sort_specification, dt.projection, dt.start, dt.limit)
        assert not any("$addFields" in s for s in pipeline)

    def test_single_stage_prepended(self):
        stage = {"$addFields": {"full_name": {"$concat": ["$first", " ", "$last"]}}}
        dt, _ = self._make_dt(pipeline_stages=[stage])
        pipeline = build_pipeline(dt.filter, dt.pipeline_stages, dt.sort_specification, dt.projection, dt.start, dt.limit)
        assert pipeline[0] == stage

    def test_multiple_stages_order_preserved(self):
        s1 = {"$addFields": {"x": 1}}
        s2 = {"$unwind": "$tags"}
        dt, _ = self._make_dt(pipeline_stages=[s1, s2])
        pipeline = build_pipeline(dt.filter, dt.pipeline_stages, dt.sort_specification, dt.projection, dt.start, dt.limit)
        assert pipeline[0] == s1
        assert pipeline[1] == s2

    def test_stages_before_match(self):
        stage = {"$addFields": {"x": 1}}
        dt, _ = self._make_dt(pipeline_stages=[stage], search_value="hello")
        pipeline = build_pipeline(dt.filter, dt.pipeline_stages, dt.sort_specification, dt.projection, dt.start, dt.limit)
        stage_keys = [list(s.keys())[0] for s in pipeline]
        add_idx = stage_keys.index("$addFields")
        for mi in [i for i, k in enumerate(stage_keys) if k == "$match"]:
            assert add_idx < mi

    def test_stages_not_mutated(self):
        original = [{"$addFields": {"x": 1}}]
        dt, _ = self._make_dt(pipeline_stages=original)
        build_pipeline(dt.filter, dt.pipeline_stages, dt.sort_specification, dt.projection, dt.start, dt.limit)
        build_pipeline(dt.filter, dt.pipeline_stages, dt.sort_specification, dt.projection, dt.start, dt.limit)
        assert original == [{"$addFields": {"x": 1}}]

    def test_count_filtered_includes_stages(self):
        stage = {"$addFields": {"x": 1}}
        dt, col = self._make_dt(pipeline_stages=[stage], search_value="hello")
        col.aggregate.return_value = iter([{"total": 5}])
        dt.count_filtered()
        call_args = col.aggregate.call_args[0][0]
        assert call_args[0] == stage

    def test_empty_list_same_as_none(self):
        dt_none, _ = self._make_dt(pipeline_stages=None)
        dt_empty, _ = self._make_dt(pipeline_stages=[])
        assert dt_none.pipeline_stages == dt_empty.pipeline_stages == []
        p_none = build_pipeline(dt_none.filter, dt_none.pipeline_stages, dt_none.sort_specification, dt_none.projection, dt_none.start, dt_none.limit)
        p_empty = build_pipeline(dt_empty.filter, dt_empty.pipeline_stages, dt_empty.sort_specification, dt_empty.projection, dt_empty.start, dt_empty.limit)
        assert p_none == p_empty


class TestDataTablesCoverageGaps:
    """Tests targeting remaining coverage gaps in datatables.py."""

    def test_orderdata_column_with_no_data_skipped(self):
        args = {
            **_P2_BASE_ARGS,
            "order": [{"column": 0, "dir": "asc"}],
            "columns": [
                {"data": "Title", "searchable": True, "orderable": True,
                 "search": {"value": "", "regex": False}, "orderData": 1},
                {"data": "", "searchable": False, "orderable": True,
                 "search": {"value": "", "regex": False}},
            ],
        }
        dt, _ = _make_p2_dt(args)
        assert "_id" in dt.sort_specification

    def test_orderdata_duplicate_field_skipped(self):
        args = {
            **_P2_BASE_ARGS,
            "order": [{"column": 0, "dir": "asc"}, {"column": 1, "dir": "desc"}],
            "columns": [
                {"data": "Title", "searchable": True, "orderable": True,
                 "search": {"value": "", "regex": False}, "orderData": [0, 1]},
                {"data": "Title", "searchable": True, "orderable": True,
                 "search": {"value": "", "regex": False}, "orderData": [0]},
            ],
        }
        dt, _ = _make_p2_dt(args)
        assert list(dt.sort_specification.keys()).count("Title") == 1

    def test_projection_skips_column_without_data(self):
        args = {
            **_P2_BASE_ARGS,
            "columns": [
                {"data": "Title", "searchable": True, "orderable": True,
                 "search": {"value": "", "regex": False}},
                {"searchable": False, "orderable": False,
                 "search": {"value": "", "regex": False}},
            ],
        }
        dt, _ = _make_p2_dt(args)
        assert "Title" in dt.projection
        assert "_id" in dt.projection

    def test_filter_has_text_nested_in_list(self):
        f = {"$and": [{"$text": {"$search": "hello"}}, {"status": "active"}]}
        assert DataTables._filter_has_text(f) is True

    def test_filter_has_text_not_nested(self):
        f = {"$and": [{"status": "active"}]}
        assert DataTables._filter_has_text(f) is False

    def test_results_cached_on_second_call(self):
        dt, col = _make_p2_dt(_P2_BASE_ARGS)
        col.aggregate.return_value = iter([])
        dt.results()
        dt.results()
        assert col.aggregate.call_count == 1

    def test_count_filtered_cached_on_second_call(self):
        dt, col = _make_p2_dt({**_P2_BASE_ARGS, "search": {"value": "x", "regex": False}})
        col.aggregate.return_value = iter([{"total": 5}])
        first = dt.count_filtered()
        col.aggregate.return_value = iter([{"total": 99}])
        second = dt.count_filtered()
        assert first == second

    def test_get_export_data_pymongo_error_returns_empty(self):
        dt, col = _make_p2_dt(_P2_BASE_ARGS)
        col.aggregate.side_effect = PyMongoError("db error")
        assert dt.get_export_data() == []

    def test_get_export_data_unexpected_exception_returns_empty(self):
        dt, col = _make_p2_dt(_P2_BASE_ARGS)
        col.aggregate.side_effect = RuntimeError("unexpected")
        assert dt.get_export_data() == []


class TestAllowDiskUse(BaseDataTablesTest):
    """Tests for allow_disk_use parameter."""

    def test_default_is_false(self):
        dt = DataTables(self.mongo, 'users', self.request_args)
        self.assertFalse(dt.allow_disk_use)

    def test_allow_disk_use_true_stored(self):
        dt = DataTables(self.mongo, 'users', self.request_args, allow_disk_use=True)
        self.assertTrue(dt.allow_disk_use)

    def test_results_passes_allow_disk_use_false(self):
        self.collection.aggregate.return_value = iter([])
        dt = DataTables(self.mongo, 'users', self.request_args)
        dt.results()
        _, kwargs = self.collection.aggregate.call_args
        self.assertEqual(kwargs.get('allowDiskUse'), False)

    def test_results_passes_allow_disk_use_true(self):
        self.collection.aggregate.return_value = iter([])
        dt = DataTables(self.mongo, 'users', self.request_args, allow_disk_use=True)
        dt.results()
        _, kwargs = self.collection.aggregate.call_args
        self.assertEqual(kwargs.get('allowDiskUse'), True)

    def test_count_filtered_passes_allow_disk_use(self):
        self.collection.aggregate.return_value = iter([{'total': 5}])
        dt = DataTables(self.mongo, 'users', self.request_args, allow_disk_use=True, status='active')
        dt.count_filtered()
        _, kwargs = self.collection.aggregate.call_args
        self.assertEqual(kwargs.get('allowDiskUse'), True)

    def test_get_export_data_passes_allow_disk_use(self):
        self.collection.aggregate.return_value = iter([])
        dt = DataTables(self.mongo, 'users', self.request_args, allow_disk_use=True)
        dt.get_export_data()
        _, kwargs = self.collection.aggregate.call_args
        self.assertEqual(kwargs.get('allowDiskUse'), True)

    def test_backward_compatible_no_allow_disk_use_arg(self):
        self.collection.aggregate.return_value = iter([])
        dt = DataTables(self.mongo, 'users', self.request_args)
        dt.results()
        self.collection.aggregate.assert_called_once()



