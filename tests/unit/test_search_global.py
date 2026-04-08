"""Global search condition tests: search terms, text index, search_fixed."""
import unittest
from unittest.mock import MagicMock, patch

import pytest
from pymongo.collection import Collection
from pymongo.database import Database

from mongo_datatables import DataTables, DataField
from mongo_datatables.datatables.query import MongoQueryBuilder
from mongo_datatables.search_fixed import parse_column_search_fixed, parse_search_fixed
from mongo_datatables.utils import FieldMapper
from tests.base_test import BaseDataTablesTest


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------

def _base_request(search_value="", columns=None, extra=None):
    cols = columns or [
        {"data": "name", "name": "", "searchable": True, "orderable": True,
         "search": {"value": "", "regex": False}},
        {"data": "email", "name": "", "searchable": True, "orderable": True,
         "search": {"value": "", "regex": False}},
        {"data": "status", "name": "", "searchable": True, "orderable": True,
         "search": {"value": "", "regex": False}},
    ]
    args = {
        "draw": "1", "start": 0, "length": 10,
        "search": {"value": search_value, "regex": False},
        "order": [{"column": 0, "dir": "asc"}],
        "columns": cols,
    }
    if extra:
        args.update(extra)
    return args


def _mock_mongo():
    mongo = MagicMock()
    mongo.db = MagicMock(spec=Database)
    col = MagicMock(spec=Collection)
    col.estimated_document_count.return_value = 0
    mongo.db.__getitem__.return_value = col
    return mongo, col


# ---------------------------------------------------------------------------
# Search terms and searchable columns
# ---------------------------------------------------------------------------

class TestSearchTerms(unittest.TestCase):
    def setUp(self):
        self.mongo, self.col = _mock_mongo()

    def _dt(self, search_value=""):
        args = _base_request(search_value)
        return DataTables(self.mongo, "users", args)

    def test_search_terms_empty(self):
        self.assertEqual(self._dt("").search_terms, [])

    def test_search_terms_split(self):
        self.assertEqual(self._dt("John active").search_terms, ["John", "active"])

    def test_search_terms_without_colon(self):
        self.assertEqual(self._dt("John status:active email:example.com").search_terms_without_a_colon, ["John"])

    def test_search_terms_with_colon(self):
        self.assertEqual(
            set(self._dt("John status:active email:example.com").search_terms_with_a_colon),
            {"status:active", "email:example.com"},
        )

    def test_searchable_columns(self):
        self.assertEqual(self._dt().searchable_columns, ["name", "email", "status"])

    def test_column_search_conditions(self):
        args = _base_request()
        args["columns"][0]["search"]["value"] = "John"
        args["columns"][0]["search"]["regex"] = True
        dt = DataTables(self.mongo, "users", args)
        result = dt.column_search_conditions
        self.assertIn("$and", result)
        self.assertTrue(any("name" in cond for cond in result["$and"]))

    def test_column_specific_search_condition(self):
        result = self._dt("status:active").column_specific_search_condition
        self.assertIn("$and", result)
        self.assertTrue(any("status" in cond for cond in result["$and"]))

    def test_global_search_condition_empty(self):
        self.assertEqual(self._dt("").global_search_condition, {})

    def test_global_search_with_text_index(self):
        with patch.object(DataTables, "has_text_index", return_value=True):
            dt = DataTables(self.mongo, "users", _base_request("John"), use_text_index=True)
            result = dt.global_search_condition
        self.assertIn("$text", result)
        self.assertEqual(result["$text"]["$search"], '"John"')

    def test_global_search_without_text_index(self):
        with patch.object(DataTables, "has_text_index", return_value=False):
            dt = DataTables(self.mongo, "users", _base_request("John"), use_text_index=False)
            result = dt.global_search_condition
        self.assertIn("$or", result)

    def test_number_field_gt_operator(self):
        args = _base_request("number_field:>10", columns=[
            {"data": "number_field", "name": "number_field", "searchable": True,
             "search": {"value": "", "regex": False}},
        ])
        dt = DataTables(self.mongo, "test", args,
                        data_fields=[DataField("number_field", "number")],
                        use_text_index=False)
        result = dt.column_specific_search_condition
        self.assertIn("$and", result)
        cond = next((c.get("number_field") for c in result["$and"] if "number_field" in c), None)
        self.assertIsNotNone(cond)
        self.assertIn("$gt", cond)

    def test_number_field_gte_operator(self):
        args = _base_request("number_field:>=10", columns=[
            {"data": "number_field", "name": "number_field", "searchable": True,
             "search": {"value": "", "regex": False}},
        ])
        dt = DataTables(self.mongo, "test", args,
                        data_fields=[DataField("number_field", "number")],
                        use_text_index=False)
        result = dt.column_specific_search_condition
        cond = next((c.get("number_field") for c in result["$and"] if "number_field" in c), None)
        self.assertIn("$gte", cond)


# ---------------------------------------------------------------------------
# search_fixed — legacy dict format
# ---------------------------------------------------------------------------

class TestSearchFixed(BaseDataTablesTest):

    def _make_dt(self, extra_args):
        args = dict(self.request_args)
        args.update(extra_args)
        with patch.object(DataTables, "has_text_index",
                          new_callable=lambda: property(lambda self: False)):
            return DataTables(self.mongo, "test_collection", args,
                              [DataField("name", "string"),
                               DataField("email", "string"),
                               DataField("status", "string")])

    def test_no_search_fixed_returns_empty(self):
        self.assertEqual(self._make_dt({})._parse_search_fixed(), {})

    def test_empty_dict_returns_empty(self):
        self.assertEqual(self._make_dt({"searchFixed": {}})._parse_search_fixed(), {})

    def test_single_fixed_search_produces_or_across_columns(self):
        result = self._make_dt({"searchFixed": {"role": "admin"}})._parse_search_fixed()
        self.assertIn("$or", result)
        fields = [list(cond.keys())[0] for cond in result["$or"]]
        self.assertIn("name", fields)
        self.assertIn("email", fields)
        self.assertIn("status", fields)

    def test_multiple_fixed_searches_are_anded(self):
        result = self._make_dt({"searchFixed": {"role": "admin", "dept": "eng"}})._parse_search_fixed()
        self.assertIn("$and", result)
        self.assertEqual(len(result["$and"]), 2)

    def test_empty_value_is_skipped(self):
        result = self._make_dt({"searchFixed": {"role": "", "dept": "eng"}})._parse_search_fixed()
        self.assertNotIn("$and", result)
        self.assertIn("$or", result)

    def test_search_fixed_included_in_filter(self):
        self.assertNotEqual(self._make_dt({"searchFixed": {"role": "admin"}}).filter, {})

    def test_search_fixed_combined_with_global_search(self):
        args = {"searchFixed": {"role": "admin"}, "search": {"value": "john", "regex": "false"}}
        self.assertIn("$and", self._make_dt(args).filter)

    def test_non_dict_search_fixed_ignored(self):
        self.assertEqual(self._make_dt({"searchFixed": "invalid"})._parse_search_fixed(), {})


# ---------------------------------------------------------------------------
# search_fixed — wire format (new array + legacy compat)
# ---------------------------------------------------------------------------

def _make_dt_wire(search_extra=None, columns_search_extra=None, search_fixed_legacy=None):
    db = MagicMock()
    collection = MagicMock()
    db.__getitem__ = MagicMock(return_value=collection)
    collection.index_information.return_value = {}

    search = {"value": "", "regex": False}
    if search_extra:
        search.update(search_extra)

    columns = [
        {"data": "name", "searchable": True, "orderable": True,
         "search": {"value": "", "regex": False}},
        {"data": "status", "searchable": True, "orderable": True,
         "search": {"value": "", "regex": False}},
    ]
    if columns_search_extra:
        for i, extra in enumerate(columns_search_extra):
            if extra and i < len(columns):
                columns[i]["search"].update(extra)

    request_args = {
        "draw": "1", "start": "0", "length": "10",
        "search": search,
        "columns": columns,
        "order": [{"column": 0, "dir": "asc"}],
    }
    if search_fixed_legacy is not None:
        request_args["searchFixed"] = search_fixed_legacy

    data_fields = [DataField("name", "string"), DataField("status", "string")]
    dt = DataTables(db, "users", request_args, data_fields=data_fields)
    with patch.object(type(dt), "has_text_index",
                      new_callable=lambda: property(lambda self: False)):
        return dt


def test_global_fixed_array_single_entry():
    dt = _make_dt_wire(search_extra={"fixed": [{"name": "lock", "term": "Alice"}]})
    result = dt._parse_search_fixed()
    assert "$or" in result
    fields = [list(c.keys())[0] for c in result["$or"]]
    assert "name" in fields
    assert "status" in fields


def test_global_fixed_array_multiple_entries_anded():
    dt = _make_dt_wire(search_extra={"fixed": [
        {"name": "a", "term": "Alice"},
        {"name": "b", "term": "active"},
    ]})
    result = dt._parse_search_fixed()
    assert "$and" in result
    assert len(result["$and"]) == 2


def test_global_fixed_array_function_term_skipped():
    dt = _make_dt_wire(search_extra={"fixed": [{"name": "fn", "term": "function"}]})
    assert dt._parse_search_fixed() == {}


def test_global_fixed_array_empty_produces_no_filter():
    dt = _make_dt_wire(search_extra={"fixed": []})
    assert dt._parse_search_fixed() == {}


def test_column_fixed_array_single_entry():
    dt = _make_dt_wire(columns_search_extra=[{"fixed": [{"name": "lock", "term": "Alice"}]}])
    result = dt._parse_column_search_fixed()
    assert "name" in str(result)
    assert "Alice" in str(result)


def test_column_fixed_array_function_term_skipped():
    dt = _make_dt_wire(columns_search_extra=[{"fixed": [{"name": "fn", "term": "function"}]}])
    assert dt._parse_column_search_fixed() == {}


def test_global_and_column_fixed_combined():
    dt = _make_dt_wire(
        search_extra={"fixed": [{"name": "g", "term": "Alice"}]},
        columns_search_extra=[None, {"fixed": [{"name": "s", "term": "active"}]}],
    )
    assert dt._parse_search_fixed() != {}
    c = dt._parse_column_search_fixed()
    assert c != {}
    assert "status" in str(c)


def test_legacy_global_searchFixed_dict():
    dt = _make_dt_wire(search_fixed_legacy={"role": "admin"})
    assert "$or" in dt._parse_search_fixed()


def test_legacy_column_searchFixed_dict():
    db = MagicMock()
    collection = MagicMock()
    db.__getitem__ = MagicMock(return_value=collection)
    collection.index_information.return_value = {}
    request_args = {
        "draw": "1", "start": "0", "length": "10",
        "search": {"value": "", "regex": False},
        "columns": [
            {"data": "name", "searchable": True, "orderable": True,
             "search": {"value": "", "regex": False}, "searchFixed": {"lock": "Alice"}},
            {"data": "status", "searchable": True, "orderable": True,
             "search": {"value": "", "regex": False}},
        ],
        "order": [{"column": 0, "dir": "asc"}],
    }
    dt = DataTables(db, "users", request_args,
                    data_fields=[DataField("name", "string"), DataField("status", "string")])
    result = dt._parse_column_search_fixed()
    assert "name" in str(result)
    assert "Alice" in str(result)


def test_mixed_new_array_and_legacy_dict():
    dt = _make_dt_wire(
        search_extra={"fixed": [{"name": "new", "term": "Alice"}]},
        search_fixed_legacy={"old": "admin"},
    )
    result = dt._parse_search_fixed()
    assert "$and" in result
    assert len(result["$and"]) == 2


# ---------------------------------------------------------------------------
# search_fixed — coverage gaps
# ---------------------------------------------------------------------------

class TestSearchFixedCoverageGaps(unittest.TestCase):

    def _qb(self, data_fields=None):
        fm = FieldMapper(data_fields or [])
        return MongoQueryBuilder(fm, use_text_index=False, has_text_index=False), fm

    def test_parse_search_fixed_non_list_fixed_falls_through_to_legacy(self):
        qb, _ = self._qb([DataField("name", "string")])
        request_args = {
            "search": {"fixed": "not-a-list"},
            "searchFixed": {"key": "alice"},
        }
        result = parse_search_fixed(request_args, qb, ["name"])
        self.assertIn("$or", result)

    def test_parse_search_fixed_empty_cond_skipped(self):
        qb, _ = self._qb([DataField("name", "string")])
        result = parse_search_fixed({"searchFixed": {"key": "alice"}}, qb, [])
        self.assertEqual(result, {})

    def test_parse_column_search_fixed_skips_column_with_no_data(self):
        qb, fm = self._qb([DataField("name", "string")])
        columns = [{"search": {"value": "", "regex": False}, "searchFixed": {"k": "alice"}}]
        self.assertEqual(parse_column_search_fixed(columns, fm, qb), {})

    def test_parse_column_search_fixed_non_list_fixed_falls_through_to_legacy(self):
        qb, fm = self._qb([DataField("name", "string")])
        columns = [{
            "data": "name",
            "searchable": True,
            "search": {"value": "", "regex": False, "fixed": "not-a-list"},
            "searchFixed": {"key": "alice"},
        }]
        result = parse_column_search_fixed(columns, fm, qb)
        self.assertNotEqual(result, {})
        self.assertIn("name", str(result))

    def test_parse_column_search_fixed_empty_cond_skipped(self):
        qb, fm = self._qb([DataField("name", "string")])
        columns = [{
            "data": "name",
            "searchable": False,
            "search": {"value": "", "regex": False},
            "searchFixed": {"key": "alice"},
        }]
        self.assertEqual(parse_column_search_fixed(columns, fm, qb), {})
