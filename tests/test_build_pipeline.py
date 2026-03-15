"""Tests for DataTables._build_pipeline() shared pipeline builder."""
import pytest
from unittest.mock import MagicMock, patch
from mongo_datatables import DataTables


BASE_ARGS = {
    "draw": "1",
    "start": "0",
    "length": "10",
    "search": {"value": "", "regex": False},
    "order": [{"column": "0", "dir": "asc", "name": ""}],
    "columns": [{"data": "name", "searchable": "true", "orderable": "true",
                  "search": {"value": "", "regex": False}, "name": ""}],
}


def make_dt(args=None, **kwargs):
    mongo = MagicMock()
    col = MagicMock()
    col.list_indexes.return_value = []
    mongo.__getitem__ = MagicMock(return_value=col)
    with patch.object(DataTables, '_check_text_index'):
        dt = DataTables(mongo, 'test', args or BASE_ARGS, **kwargs)
        dt.collection = col
        dt._has_text_index = False
    return dt


class TestBuildPipelineStructure:
    def test_paginate_true_includes_skip_and_limit(self):
        args = {**BASE_ARGS, "start": "20", "length": "10"}
        dt = make_dt(args)
        pipeline = dt._build_pipeline(paginate=True)
        stages = [list(s.keys())[0] for s in pipeline]
        assert "$skip" in stages
        assert "$limit" in stages

    def test_paginate_false_excludes_skip_and_limit(self):
        args = {**BASE_ARGS, "start": "20", "length": "10"}
        dt = make_dt(args)
        pipeline = dt._build_pipeline(paginate=False)
        stages = [list(s.keys())[0] for s in pipeline]
        assert "$skip" not in stages
        assert "$limit" not in stages

    def test_always_ends_with_project(self):
        dt = make_dt()
        for paginate in (True, False):
            pipeline = dt._build_pipeline(paginate=paginate)
            assert list(pipeline[-1].keys())[0] == "$project"

    def test_no_match_when_no_filter(self):
        dt = make_dt()
        pipeline = dt._build_pipeline()
        stages = [list(s.keys())[0] for s in pipeline]
        assert "$match" not in stages

    def test_skip_omitted_when_start_is_zero(self):
        dt = make_dt()  # start=0
        pipeline = dt._build_pipeline(paginate=True)
        stages = [list(s.keys())[0] for s in pipeline]
        assert "$skip" not in stages

    def test_limit_omitted_when_length_is_negative_one(self):
        args = {**BASE_ARGS, "length": "-1"}
        dt = make_dt(args)
        pipeline = dt._build_pipeline(paginate=True)
        stages = [list(s.keys())[0] for s in pipeline]
        assert "$limit" not in stages

    def test_default_paginate_is_true(self):
        args = {**BASE_ARGS, "start": "5", "length": "10"}
        dt = make_dt(args)
        with_default = dt._build_pipeline()
        with_explicit = dt._build_pipeline(paginate=True)
        assert with_default == with_explicit


class TestBuildPipelineConsistency:
    def test_results_and_export_share_match_stage(self):
        """results() and get_export_data() must apply the same filter."""
        dt = make_dt()
        paginated = dt._build_pipeline(paginate=True)
        export = dt._build_pipeline(paginate=False)
        # Both should have the same $match (or both omit it)
        p_match = next((s for s in paginated if "$match" in s), None)
        e_match = next((s for s in export if "$match" in s), None)
        assert p_match == e_match

    def test_results_and_export_share_sort_stage(self):
        dt = make_dt()
        paginated = dt._build_pipeline(paginate=True)
        export = dt._build_pipeline(paginate=False)
        p_sort = next((s for s in paginated if "$sort" in s), None)
        e_sort = next((s for s in export if "$sort" in s), None)
        assert p_sort == e_sort

    def test_results_and_export_share_project_stage(self):
        dt = make_dt()
        paginated = dt._build_pipeline(paginate=True)
        export = dt._build_pipeline(paginate=False)
        assert paginated[-1] == export[-1]
