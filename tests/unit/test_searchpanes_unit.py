"""Consolidated SearchPanes tests."""
import pytest
from unittest.mock import MagicMock, patch, call
from datetime import datetime
from bson import ObjectId
from pymongo.collection import Collection
from pymongo.database import Database
from mongo_datatables import DataTables, DataField
from mongo_datatables.datatables import DataTables as DT
from mongo_datatables.utils import FieldMapper
from mongo_datatables.query_builder import MongoQueryBuilder


class TestSearchPanes:
    def setup_method(self):
        self.mongo = MagicMock()
        self.mongo.db = MagicMock(spec=Database)
        self.collection = MagicMock(spec=Collection)
        self.mongo.db.__getitem__.return_value = self.collection
        self.collection.list_indexes.return_value = []
        self.collection.estimated_document_count.return_value = 0
        self.data_fields = [
            DataField("name", "string"),
            DataField("age", "number"),
            DataField("status", "string"),
        ]

    def test_searchpanes_options_generation(self):
        request_args = {
            "draw": 1, "start": 0, "length": 10,
            "columns": [
                {"data": "name", "searchable": True},
                {"data": "age", "searchable": True},
                {"data": "status", "searchable": True},
            ],
            "searchPanes": True
        }
        facet_doc = {
            "name": [{"_id": "Alice", "count": 8}],
            "age": [{"_id": 30, "count": 4}],
            "status": [{"_id": "Active", "count": 5}, {"_id": "Inactive", "count": 3}],
        }
        self.collection.aggregate.side_effect = [[facet_doc], [facet_doc]]
        dt = DataTables(self.mongo, "test_collection", request_args, data_fields=self.data_fields)
        options = dt.get_searchpanes_options()
        assert "status" in options
        assert len(options["status"]) == 2
        assert options["status"][0]["label"] == "Active"
        assert options["status"][0]["count"] == 5

    def test_searchpanes_filtering(self):
        request_args = {
            "draw": 1, "start": 0, "length": 10,
            "columns": [{"data": "status", "searchable": True}],
            "searchPanes": {"status": ["Active", "Pending"]}
        }
        dt = DataTables(self.mongo, "test_collection", request_args, data_fields=self.data_fields)
        filter_condition = dt._parse_searchpanes_filters()
        assert filter_condition == {"$and": [{"status": {"$in": ["Active", "Pending"]}}]}

    def test_searchpanes_in_response(self):
        request_args = {
            "draw": 1, "start": 0, "length": 10,
            "columns": [{"data": "status", "searchable": True}],
            "searchPanes": True
        }
        self.collection.count_documents.return_value = 10
        self.collection.aggregate.return_value = [{"_id": "Active", "count": 5}]
        dt = DataTables(self.mongo, "test_collection", request_args, data_fields=self.data_fields)
        response = dt.get_rows()
        assert "searchPanes" in response
        assert "options" in response["searchPanes"]

    def test_searchpanes_number_conversion(self):
        request_args = {
            "draw": 1, "start": 0, "length": 10,
            "columns": [{"data": "age", "searchable": True}],
            "searchPanes": {"age": ["25", "30"]}
        }
        dt = DataTables(self.mongo, "test_collection", request_args, data_fields=self.data_fields)
        filter_condition = dt._parse_searchpanes_filters()
        assert filter_condition == {"$and": [{"age": {"$in": [25, 30]}}]}


def _make_dt_new(data_fields, request_args):
    """Factory that bypasses DataTables.__init__ for direct attribute injection."""
    col = MagicMock()
    col.list_indexes.return_value = []
    col.estimated_document_count.return_value = 0
    with patch.object(DT, '_get_collection', return_value=col), \
         patch.object(DT, '_check_text_index'):
        dt = DT.__new__(DT)
        dt.collection = col
        dt.request_args = request_args
        dt.data_fields = data_fields
        dt.field_mapper = FieldMapper(data_fields)
        dt.use_text_index = False
        dt.allow_disk_use = False
        dt.row_class = dt.row_data = dt.row_attr = dt.row_id = None
        dt.custom_filter = {}
        dt._results = dt._recordsTotal = dt._recordsFiltered = dt._filter_cache = None
        dt._has_text_index = False
        dt.query_builder = MongoQueryBuilder(dt.field_mapper, False, False)
        return dt


class TestSearchPanesDateFilter:
    def test_date_iso_date_string_converted_to_datetime(self):
        dt = _make_dt_new([DataField('created_at', 'date')], {"searchPanes": {"created_at": ["2024-03-15"]}})
        result = dt._parse_searchpanes_filters()
        assert result == {"$and": [{"created_at": {"$in": [datetime(2024, 3, 15)]}}]}

    def test_date_iso_datetime_string_converted_to_datetime(self):
        dt = _make_dt_new([DataField('created_at', 'date')], {"searchPanes": {"created_at": ["2024-03-15T00:00:00.000Z"]}})
        result = dt._parse_searchpanes_filters()
        assert result == {"$and": [{"created_at": {"$in": [datetime(2024, 3, 15)]}}]}

    def test_date_invalid_falls_back_to_string(self):
        dt = _make_dt_new([DataField('created_at', 'date')], {"searchPanes": {"created_at": ["not-a-date"]}})
        result = dt._parse_searchpanes_filters()
        assert result == {"$and": [{"created_at": {"$in": ["not-a-date"]}}]}

    def test_date_multiple_values(self):
        dt = _make_dt_new([DataField('created_at', 'date')], {"searchPanes": {"created_at": ["2024-01-01", "2024-06-15T12:00:00Z"]}})
        result = dt._parse_searchpanes_filters()
        assert result == {"$and": [{"created_at": {"$in": [datetime(2024, 1, 1), datetime(2024, 6, 15)]}}]}


class TestSearchPanesExceptionNarrowing:
    def test_invalid_objectid_falls_back_to_raw_string(self):
        dt = _make_dt_new([DataField('ref', 'objectid')], {"searchPanes": {"ref": ["not-an-objectid"]}})
        result = dt._parse_searchpanes_filters()
        assert result == {"$and": [{"ref": {"$in": ["not-an-objectid"]}}]}

    def test_valid_objectid_is_converted(self):
        oid = ObjectId()
        dt = _make_dt_new([DataField('ref', 'objectid')], {"searchPanes": {"ref": [str(oid)]}})
        result = dt._parse_searchpanes_filters()
        assert result == {"$and": [{"ref": {"$in": [oid]}}]}

    def test_invalid_date_falls_back_to_raw_string(self):
        dt = _make_dt_new([DataField('created_at', 'date')], {"searchPanes": {"created_at": ["not-a-date"]}})
        result = dt._parse_searchpanes_filters()
        assert result == {"$and": [{"created_at": {"$in": ["not-a-date"]}}]}

    def test_valid_date_is_converted_to_datetime(self):
        dt = _make_dt_new([DataField('created_at', 'date')], {"searchPanes": {"created_at": ["2024-06-01"]}})
        result = dt._parse_searchpanes_filters()
        assert result == {"$and": [{"created_at": {"$in": [datetime(2024, 6, 1)]}}]}


BASE_ARGS = {
    "draw": 1, "start": 0, "length": 10,
    "search": {"value": "", "regex": False},
    "order": [],
    "columns": [
        {"data": "status", "searchable": True, "orderable": True, "search": {"value": "", "regex": False}},
    ],
    "searchPanes": True,
}


def _make_dt_full(request_args, data_fields=None, custom_filter=None):
    mongo = MagicMock()
    mongo.db = MagicMock(spec=Database)
    collection = MagicMock(spec=Collection)
    mongo.db.__getitem__.return_value = collection
    collection.list_indexes.return_value = []
    collection.estimated_document_count.return_value = 0
    kwargs = {}
    if custom_filter:
        kwargs.update(custom_filter)
    return DataTables(mongo, "test_collection", request_args, data_fields=data_fields or [], **kwargs)


def _facet_result(col_name, rows):
    return [{col_name: rows}]


class TestSearchPanesTotalCount:
    def test_options_include_total_and_count_keys(self):
        dt = _make_dt_full(BASE_ARGS, [DataField("status", "string")])
        dt.collection.aggregate.return_value = _facet_result("status", [
            {"_id": "Active", "count": 5}, {"_id": "Inactive", "count": 3},
        ])
        options = dt.get_searchpanes_options()
        assert "status" in options
        for opt in options["status"]:
            assert "total" in opt
            assert "count" in opt

    def test_total_equals_base_count_no_filter(self):
        dt = _make_dt_full(BASE_ARGS, [DataField("status", "string")])
        dt.collection.aggregate.return_value = _facet_result("status", [{"_id": "Active", "count": 7}])
        options = dt.get_searchpanes_options()
        opt = options["status"][0]
        assert opt["total"] == 7
        assert opt["count"] == 7

    def test_count_zero_when_filtered_out(self):
        args = dict(BASE_ARGS)
        args["search"] = {"value": "Active", "regex": False}
        dt = _make_dt_full(args, [DataField("status", "string")])
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
        dt = _make_dt_full(BASE_ARGS, [DataField("status", "string")])
        dt.collection.aggregate.return_value = _facet_result("status", [{"_id": "Active", "count": 2}])
        dt.get_searchpanes_options()
        assert dt.collection.aggregate.call_count == 2

    def test_two_aggregations_total_regardless_of_column_count(self):
        args = dict(BASE_ARGS)
        args["columns"] = [
            {"data": "status", "searchable": True, "orderable": True, "search": {"value": "", "regex": False}},
            {"data": "category", "searchable": True, "orderable": True, "search": {"value": "", "regex": False}},
            {"data": "region", "searchable": True, "orderable": True, "search": {"value": "", "regex": False}},
        ]
        dt = _make_dt_full(args, [DataField("status", "string"), DataField("category", "string"), DataField("region", "string")])
        dt.collection.aggregate.return_value = [{"status": [], "category": [], "region": []}]
        dt.get_searchpanes_options()
        assert dt.collection.aggregate.call_count == 2

    def test_total_pipeline_uses_custom_filter_only(self):
        args = dict(BASE_ARGS)
        args["search"] = {"value": "something", "regex": False}
        dt = _make_dt_full(args, [DataField("status", "string")], custom_filter={"tenant": "acme"})
        dt.collection.aggregate.return_value = [{"status": []}]
        dt.get_searchpanes_options()
        calls = dt.collection.aggregate.call_args_list
        assert len(calls) == 2
        total_pipeline = calls[0][0][0]
        assert total_pipeline[0] == {"$match": {"tenant": "acme"}}
        for stage in total_pipeline:
            if "$match" in stage:
                assert "status" not in stage["$match"] or stage["$match"] == {"tenant": "acme"}

    def test_count_pipeline_uses_full_filter(self):
        args = dict(BASE_ARGS)
        args["search"] = {"value": "Active", "regex": False}
        dt = _make_dt_full(args, [DataField("status", "string")])
        dt.collection.aggregate.return_value = [{"status": []}]
        dt.get_searchpanes_options()
        calls = dt.collection.aggregate.call_args_list
        count_pipeline = calls[1][0][0]
        assert count_pipeline[0]["$match"] == dt.filter

    def test_options_sorted_by_total_descending(self):
        dt = _make_dt_full(BASE_ARGS, [DataField("status", "string")])
        dt.collection.aggregate.return_value = _facet_result("status", [
            {"_id": "Rare", "count": 1}, {"_id": "Common", "count": 10}, {"_id": "Medium", "count": 5},
        ])
        options = dt.get_searchpanes_options()
        totals = [o["total"] for o in options["status"]]
        assert totals == sorted(totals, reverse=True)

    def test_no_searchpanes_no_options_in_response(self):
        args = dict(BASE_ARGS)
        args.pop("searchPanes")
        dt = _make_dt_full(args, [DataField("status", "string")])
        dt.collection.aggregate.return_value = iter([])
        dt.collection.count_documents.return_value = 0
        response = dt.get_rows()
        assert "searchPanes" not in response

    def test_get_rows_includes_total_in_options(self):
        dt = _make_dt_full(BASE_ARGS, [DataField("status", "string")])
        dt.collection.aggregate.side_effect = [
            iter([]),
            _facet_result("status", [{"_id": "Active", "count": 4}]),
            _facet_result("status", [{"_id": "Active", "count": 4}]),
        ]
        dt.collection.count_documents.return_value = 4
        response = dt.get_rows()
        assert "searchPanes" in response
        opts = response["searchPanes"]["options"]["status"]
        assert opts[0]["total"] == 4
        assert opts[0]["count"] == 4


class TestSearchPanesCountMapFix:
    """Tests for the count_map _hashable key fix in get_searchpanes_options."""

    def _make_dt(self, request_args, collection, data_fields=None):
        from mongo_datatables import DataTables
        from unittest.mock import MagicMock
        from pymongo.database import Database
        mongo = MagicMock()
        mongo.db = MagicMock(spec=Database)
        mongo.db.__getitem__.return_value = collection
        return DataTables(mongo, "test", request_args, data_fields or [])

    def test_count_map_uses_hashable_key_for_decimal128(self):
        """count_map lookup must use _hashable key so Decimal128 values match."""
        from bson import Decimal128
        from unittest.mock import MagicMock, patch

        request_args = {
            "draw": 1, "start": 0, "length": 10,
            "columns": [{"data": "price", "searchable": "true"}],
            "search": {"value": ""},
            "searchPanes": True,
        }
        collection = MagicMock()
        collection.list_indexes.return_value = []

        d128 = Decimal128("9.99")
        # total facet: price=9.99 appears 5 times
        total_facet = [{"price": [{"_id": d128, "count": 5}]}]
        # count facet: price=9.99 appears 3 times (filtered)
        count_facet = [{"price": [{"_id": d128, "count": 3}]}]
        collection.aggregate.side_effect = [total_facet, count_facet]

        dt = self._make_dt(request_args, collection)
        options = dt.get_searchpanes_options()

        assert "price" in options
        assert len(options["price"]) == 1
        opt = options["price"][0]
        assert opt["total"] == 5
        # Before the fix this was 0; after the fix it must be 3
        assert opt["count"] == 3, f"count_map lookup failed: got {opt['count']}, expected 3"

    def test_count_map_non_decimal128_values_unaffected(self):
        """String values (already hashable) must still resolve correctly."""
        from unittest.mock import MagicMock

        request_args = {
            "draw": 1, "start": 0, "length": 10,
            "columns": [{"data": "status", "searchable": "true"}],
            "search": {"value": ""},
            "searchPanes": True,
        }
        collection = MagicMock()
        collection.list_indexes.return_value = []

        total_facet = [{"status": [{"_id": "active", "count": 10}]}]
        count_facet = [{"status": [{"_id": "active", "count": 7}]}]
        collection.aggregate.side_effect = [total_facet, count_facet]

        dt = self._make_dt(request_args, collection)
        options = dt.get_searchpanes_options()

        assert options["status"][0]["count"] == 7
