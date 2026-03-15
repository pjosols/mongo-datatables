"""Regression tests for v1.30.0 fixes."""
import unittest
from unittest.mock import MagicMock
from datetime import datetime, timedelta

from pymongo.database import Database
from pymongo.collection import Collection

from mongo_datatables import DataTables, DataField
from mongo_datatables.query_builder import MongoQueryBuilder
from mongo_datatables.utils import FieldMapper


def _make_dt(data_fields=None):
    mongo = MagicMock()
    mongo.db = MagicMock(spec=Database)
    collection = MagicMock(spec=Collection)
    mongo.db.__getitem__ = MagicMock(return_value=collection)
    collection.list_indexes.return_value = []
    request_args = {
        "draw": 1, "start": 0, "length": 10,
        "columns": [{"data": "created", "searchable": True, "orderable": True,
                     "search": {"value": "", "regex": False}}],
        "order": [{"column": 0, "dir": "asc"}],
        "search": {"value": "", "regex": False},
    }
    return DataTables(mongo, "col", request_args, data_fields=data_fields or [])


def _make_qb():
    fm = MagicMock(spec=FieldMapper)
    fm.get_field_type.return_value = "text"
    fm.get_db_field.side_effect = lambda x: x
    return MongoQueryBuilder(fm)


class TestBuildColumnSearchNesting(unittest.TestCase):
    """Fix 1: build_column_search inner blocks nested inside outer if."""

    def test_has_cc_only_no_search_value_no_unbound_error(self):
        """has_cc=True, search_value empty: must not raise and must return cc condition."""
        qb = _make_qb()
        columns = [{
            "data": "name",
            "searchable": True,
            "search": {"value": ""},
            "columnControl": {"search": {"value": "foo", "logic": "contains"}},
        }]
        result = qb.build_column_search(columns)
        self.assertIn("$and", result)

    def test_not_searchable_with_cc_no_unbound_error(self):
        """searchable=False, has_cc=True: must not raise and must return cc condition."""
        qb = _make_qb()
        columns = [{
            "data": "status",
            "searchable": False,
            "search": {"value": "active"},
            "columnControl": {"search": {"value": "active", "logic": "equal"}},
        }]
        result = qb.build_column_search(columns)
        self.assertIn("$and", result)

    def test_not_searchable_no_cc_returns_empty(self):
        """searchable=False, no cc, search_value present: returns empty dict."""
        qb = _make_qb()
        columns = [{
            "data": "hidden",
            "searchable": False,
            "search": {"value": "test"},
        }]
        result = qb.build_column_search(columns)
        self.assertEqual(result, {})


class TestHashableOutsideLoop(unittest.TestCase):
    """Fix 2: _hashable defined outside the for loop in get_searchpanes_options."""

    def test_searchpanes_options_multiple_columns(self):
        """_hashable must work correctly across all columns (not just the first)."""
        mongo = MagicMock()
        mongo.db = MagicMock(spec=Database)
        collection = MagicMock(spec=Collection)
        mongo.db.__getitem__ = MagicMock(return_value=collection)
        collection.list_indexes.return_value = []

        request_args = {
            "draw": 1, "start": 0, "length": 10,
            "columns": [
                {"data": "name", "searchable": True, "orderable": True,
                 "search": {"value": "", "regex": False}},
                {"data": "status", "searchable": True, "orderable": True,
                 "search": {"value": "", "regex": False}},
            ],
            "order": [{"column": 0, "dir": "asc"}],
            "search": {"value": "", "regex": False},
        }
        facet_doc = {
            "name": [{"_id": "Alice", "count": 3}, {"_id": "Bob", "count": 2}],
            "status": [{"_id": "active", "count": 4}, {"_id": "inactive", "count": 1}],
        }
        collection.aggregate.side_effect = [[facet_doc], [facet_doc]]

        dt = DataTables(mongo, "col", request_args,
                        data_fields=[DataField("name", "string"), DataField("status", "string")])
        options = dt.get_searchpanes_options()

        # Both columns must be present and correctly populated
        self.assertIn("name", options)
        self.assertIn("status", options)
        self.assertEqual(len(options["name"]), 2)
        self.assertEqual(len(options["status"]), 2)


class TestSbDateBetweenSemantics(unittest.TestCase):
    """Fix 3: _sb_date between/!between use day-inclusive exclusive upper bound."""

    def setUp(self):
        self.dt = _make_dt([DataField("created", "date")])

    def test_between_uses_lt_not_lte(self):
        """between: upper bound must be $lt end+1day, not $lte end."""
        result = self.dt._sb_date("created", "between", "2024-01-01", "2024-01-31")
        cond = result["created"]
        self.assertIn("$lt", cond)
        self.assertNotIn("$lte", cond)
        self.assertEqual(cond["$lt"], datetime(2024, 2, 1))

    def test_between_lower_bound(self):
        """between: lower bound must be $gte start."""
        result = self.dt._sb_date("created", "between", "2024-01-01", "2024-01-31")
        self.assertEqual(result["created"]["$gte"], datetime(2024, 1, 1))

    def test_not_between_upper_uses_gte_not_gt(self):
        """!between: upper complement must be $gte end+1day, not $gt end."""
        result = self.dt._sb_date("created", "!between", "2024-01-01", "2024-01-31")
        upper = result["$or"][1]
        self.assertIn("$gte", upper["created"])
        self.assertNotIn("$gt", upper["created"])
        self.assertEqual(upper["created"]["$gte"], datetime(2024, 2, 1))

    def test_not_between_lower_bound(self):
        """!between: lower complement must be $lt start."""
        result = self.dt._sb_date("created", "!between", "2024-01-01", "2024-01-31")
        lower = result["$or"][0]
        self.assertEqual(lower["created"]["$lt"], datetime(2024, 1, 1))

    def test_between_single_day_range(self):
        """between same start/end: $gte day, $lt day+1 (covers full day)."""
        result = self.dt._sb_date("created", "between", "2024-06-15", "2024-06-15")
        cond = result["created"]
        self.assertEqual(cond["$gte"], datetime(2024, 6, 15))
        self.assertEqual(cond["$lt"], datetime(2024, 6, 16))


if __name__ == "__main__":
    unittest.main()
