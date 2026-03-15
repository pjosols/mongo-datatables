"""Tests for SearchPanes total/count dual-count support (server-side protocol)."""
import pytest
from unittest.mock import MagicMock, call
from pymongo.collection import Collection
from pymongo.database import Database
from mongo_datatables import DataTables, DataField


def _make_dt(request_args, data_fields=None, custom_filter=None):
    mongo = MagicMock()
    mongo.db = MagicMock(spec=Database)
    collection = MagicMock(spec=Collection)
    mongo.db.__getitem__.return_value = collection
    collection.list_indexes.return_value = []
    collection.estimated_document_count.return_value = 0
    kwargs = {}
    if custom_filter:
        kwargs.update(custom_filter)
    return DataTables(
        mongo, "test_collection", request_args,
        data_fields=data_fields or [], **kwargs
    )


BASE_ARGS = {
    "draw": 1, "start": 0, "length": 10,
    "search": {"value": "", "regex": False},
    "order": [],
    "columns": [
        {"data": "status", "searchable": True, "orderable": True,
         "search": {"value": "", "regex": False}},
    ],
    "searchPanes": True,
}

# $facet returns a single document: {col_name: [{_id, count}, ...]}
def _facet_result(col_name, rows):
    return [{col_name: rows}]


class TestSearchPanesTotalCount:

    def test_options_include_total_and_count_keys(self):
        """Each option must have both 'total' and 'count' keys."""
        dt = _make_dt(BASE_ARGS, [DataField("status", "string")])
        dt.collection.aggregate.return_value = _facet_result("status", [
            {"_id": "Active", "count": 5},
            {"_id": "Inactive", "count": 3},
        ])
        options = dt.get_searchpanes_options()
        assert "status" in options
        for opt in options["status"]:
            assert "total" in opt, "Missing 'total' key"
            assert "count" in opt, "Missing 'count' key"

    def test_total_equals_base_count_no_filter(self):
        """When no search filter is active, total and count should be equal."""
        dt = _make_dt(BASE_ARGS, [DataField("status", "string")])
        dt.collection.aggregate.return_value = _facet_result("status", [
            {"_id": "Active", "count": 7},
        ])
        options = dt.get_searchpanes_options()
        opt = options["status"][0]
        assert opt["total"] == 7
        assert opt["count"] == 7

    def test_count_zero_when_filtered_out(self):
        """An option filtered out by current search should have count=0."""
        args = dict(BASE_ARGS)
        args["search"] = {"value": "Active", "regex": False}
        dt = _make_dt(args, [DataField("status", "string")])

        # First call (total pipeline): both values
        # Second call (count pipeline, with filter): only Active
        dt.collection.aggregate.side_effect = [
            _facet_result("status", [{"_id": "Active", "count": 5}, {"_id": "Inactive", "count": 3}]),
            _facet_result("status", [{"_id": "Active", "count": 5}]),
        ]
        options = dt.get_searchpanes_options()
        status_opts = {o["value"]: o for o in options["status"]}
        assert status_opts["Active"]["total"] == 5
        assert status_opts["Active"]["count"] == 5
        assert status_opts["Inactive"]["total"] == 3
        assert status_opts["Inactive"]["count"] == 0

    def test_two_aggregations_called_per_column(self):
        """get_searchpanes_options must call aggregate exactly twice (total + count)."""
        dt = _make_dt(BASE_ARGS, [DataField("status", "string")])
        dt.collection.aggregate.return_value = _facet_result("status", [{"_id": "Active", "count": 2}])
        dt.get_searchpanes_options()
        assert dt.collection.aggregate.call_count == 2

    def test_two_aggregations_total_regardless_of_column_count(self):
        """$facet approach must call aggregate exactly 2 times even with multiple columns."""
        args = dict(BASE_ARGS)
        args["columns"] = [
            {"data": "status", "searchable": True, "orderable": True,
             "search": {"value": "", "regex": False}},
            {"data": "category", "searchable": True, "orderable": True,
             "search": {"value": "", "regex": False}},
            {"data": "region", "searchable": True, "orderable": True,
             "search": {"value": "", "regex": False}},
        ]
        dt = _make_dt(args, [
            DataField("status", "string"),
            DataField("category", "string"),
            DataField("region", "string"),
        ])
        dt.collection.aggregate.return_value = [{"status": [], "category": [], "region": []}]
        dt.get_searchpanes_options()
        assert dt.collection.aggregate.call_count == 2

    def test_total_pipeline_uses_custom_filter_only(self):
        """The 'total' aggregation must use only custom_filter, not search filters."""
        args = dict(BASE_ARGS)
        args["search"] = {"value": "something", "regex": False}
        dt = _make_dt(args, [DataField("status", "string")], custom_filter={"tenant": "acme"})
        dt.collection.aggregate.return_value = [{"status": []}]
        dt.get_searchpanes_options()

        calls = dt.collection.aggregate.call_args_list
        assert len(calls) == 2
        total_pipeline = calls[0][0][0]
        # First stage of total pipeline must be the custom_filter match
        assert total_pipeline[0] == {"$match": {"tenant": "acme"}}
        # Must NOT contain a global search condition
        for stage in total_pipeline:
            if "$match" in stage:
                assert "status" not in stage["$match"] or stage["$match"] == {"tenant": "acme"}

    def test_count_pipeline_uses_full_filter(self):
        """The 'count' aggregation must use the full filter (custom + search)."""
        args = dict(BASE_ARGS)
        args["search"] = {"value": "Active", "regex": False}
        dt = _make_dt(args, [DataField("status", "string")])
        dt.collection.aggregate.return_value = [{"status": []}]
        dt.get_searchpanes_options()

        calls = dt.collection.aggregate.call_args_list
        count_pipeline = calls[1][0][0]
        # Full filter should be present in the count pipeline
        assert count_pipeline[0]["$match"] == dt.filter

    def test_options_sorted_by_total_descending(self):
        """Options should be sorted by total count descending."""
        dt = _make_dt(BASE_ARGS, [DataField("status", "string")])
        dt.collection.aggregate.return_value = _facet_result("status", [
            {"_id": "Rare", "count": 1},
            {"_id": "Common", "count": 10},
            {"_id": "Medium", "count": 5},
        ])
        options = dt.get_searchpanes_options()
        totals = [o["total"] for o in options["status"]]
        assert totals == sorted(totals, reverse=True)

    def test_no_searchpanes_no_options_in_response(self):
        """get_rows() must not include searchPanes key when not requested."""
        args = dict(BASE_ARGS)
        args.pop("searchPanes")
        dt = _make_dt(args, [DataField("status", "string")])
        dt.collection.aggregate.return_value = iter([])
        dt.collection.count_documents.return_value = 0
        response = dt.get_rows()
        assert "searchPanes" not in response

    def test_get_rows_includes_total_in_options(self):
        """get_rows() response searchPanes.options must include 'total' per option."""
        dt = _make_dt(BASE_ARGS, [DataField("status", "string")])
        dt.collection.aggregate.side_effect = [
            # results() call
            iter([]),
            # get_searchpanes_options total call
            _facet_result("status", [{"_id": "Active", "count": 4}]),
            # get_searchpanes_options count call
            _facet_result("status", [{"_id": "Active", "count": 4}]),
        ]
        dt.collection.count_documents.return_value = 4
        response = dt.get_rows()
        assert "searchPanes" in response
        opts = response["searchPanes"]["options"]["status"]
        assert opts[0]["total"] == 4
        assert opts[0]["count"] == 4
