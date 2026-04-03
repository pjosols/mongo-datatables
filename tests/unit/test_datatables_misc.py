"""Tests for DataTables miscellaneous features: draw, input validation, regression fixes."""
import unittest
from datetime import datetime
from unittest.mock import MagicMock, patch
from pymongo.collection import Collection
from pymongo.database import Database
from pymongo.errors import PyMongoError

from mongo_datatables import DataTables, DataField, Editor
from mongo_datatables.query_builder import MongoQueryBuilder
from mongo_datatables.search_builder import _sb_group, _sb_date
from mongo_datatables.utils import FieldMapper
import mongo_datatables


class TestDrawProperty(unittest.TestCase):
    def setUp(self):
        self.mongo = MagicMock()
        self.collection = MagicMock()
        self.mongo.db = MagicMock()
        self.mongo.db.__getitem__ = MagicMock(return_value=self.collection)
        self.collection.list_indexes.return_value = iter([])
        self.base_args = {
            "draw": "1", "start": "0", "length": "10",
            "search": {"value": "", "regex": False},
            "order": [{"column": "0", "dir": "asc"}],
            "columns": [{"data": "name", "name": "", "searchable": "true", "orderable": "true",
                         "search": {"value": "", "regex": False}}],
        }

    def _make(self, draw_val):
        return DataTables(self.mongo, "users", {**self.base_args, "draw": draw_val})

    def test_normal_integer_string(self):
        self.assertEqual(self._make("5").draw, 5)

    def test_string_one(self):
        self.assertEqual(self._make("1").draw, 1)

    def test_negative_clamped_to_one(self):
        self.assertEqual(self._make("-3").draw, 1)

    def test_zero_clamped_to_one(self):
        self.assertEqual(self._make("0").draw, 1)

    def test_non_numeric_defaults_to_one(self):
        self.assertEqual(self._make("abc").draw, 1)

    def test_none_defaults_to_one(self):
        self.assertEqual(self._make(None).draw, 1)

    def test_float_string_defaults_to_one(self):
        self.assertEqual(self._make("2.5").draw, 1)

    def test_large_valid_number(self):
        self.assertEqual(self._make("999").draw, 999)


class TestInputValidation(unittest.TestCase):
    def setUp(self):
        self.mongo = MagicMock()
        self.collection = MagicMock()
        self.mongo.db = MagicMock()
        self.mongo.db.__getitem__ = MagicMock(return_value=self.collection)
        self.collection.list_indexes.return_value = iter([])
        self.base_args = {
            "draw": "1", "start": "0", "length": "10",
            "search": {"value": "", "regex": False},
            "order": [{"column": "0", "dir": "asc"}],
            "columns": [{"data": "name", "name": "", "searchable": "true", "orderable": "true",
                         "search": {"value": "", "regex": False}}],
        }

    def _make(self, extra_args):
        return DataTables(self.mongo, "users", {**self.base_args, **extra_args})

    def test_start_valid(self):
        self.assertEqual(self._make({"start": "20"}).start, 20)

    def test_start_invalid_string(self):
        self.assertEqual(self._make({"start": "abc"}).start, 0)

    def test_start_negative(self):
        self.assertEqual(self._make({"start": "-5"}).start, 0)

    def test_start_none(self):
        self.assertEqual(self._make({"start": None}).start, 0)

    def test_start_missing(self):
        args = {k: v for k, v in self.base_args.items() if k != "start"}
        self.assertEqual(DataTables(self.mongo, "users", args).start, 0)

    def test_limit_valid(self):
        self.assertEqual(self._make({"length": "25"}).limit, 25)

    def test_limit_minus_one(self):
        self.assertEqual(self._make({"length": "-1"}).limit, -1)

    def test_limit_invalid_string(self):
        self.assertEqual(self._make({"length": "abc"}).limit, 10)

    def test_limit_none(self):
        self.assertEqual(self._make({"length": None}).limit, 10)

    def test_limit_missing(self):
        args = {k: v for k, v in self.base_args.items() if k != "length"}
        self.assertEqual(DataTables(self.mongo, "users", args).limit, 10)

    def test_draw_valid(self):
        self.collection.aggregate.return_value = iter([{"name": "Alice"}])
        self.collection.estimated_document_count.return_value = 1
        dt = self._make({"draw": "3"})
        with patch.object(dt, "count_total", return_value=1), \
             patch.object(dt, "count_filtered", return_value=1), \
             patch.object(dt, "results", return_value=[{"name": "Alice"}]):
            resp = dt.get_rows()
        self.assertEqual(resp["draw"], 3)

    def test_draw_invalid_string(self):
        dt = self._make({"draw": "xyz"})
        with patch.object(dt, "count_total", return_value=0), \
             patch.object(dt, "count_filtered", return_value=0), \
             patch.object(dt, "results", return_value=[]):
            resp = dt.get_rows()
        self.assertEqual(resp["draw"], 1)

    def test_draw_missing(self):
        dt = DataTables(self.mongo, "users", {**self.base_args, "draw": None})
        with patch.object(dt, "count_total", return_value=0), \
             patch.object(dt, "count_filtered", return_value=0), \
             patch.object(dt, "results", return_value=[]):
            resp = dt.get_rows()
        self.assertEqual(resp["draw"], 1)


class TestInit(unittest.TestCase):
    """Test cases for module initialization."""

    def test_imports(self):
        self.assertTrue(hasattr(mongo_datatables, 'DataTables'))
        self.assertTrue(hasattr(mongo_datatables, 'Editor'))

    def test_version(self):
        self.assertTrue(hasattr(mongo_datatables, '__version__'))
        self.assertIsInstance(mongo_datatables.__version__, str)

    def test_imports_work(self):
        self.assertIsNotNone(DataTables)
        self.assertIsNotNone(Editor)


def _make_regression_dt(data_fields=None):
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
        qb = _make_qb()
        columns = [{
            "data": "name", "searchable": True, "search": {"value": ""},
            "columnControl": {"search": {"value": "foo", "logic": "contains"}},
        }]
        result = qb.build_column_search(columns)
        self.assertIn("$and", result)

    def test_not_searchable_with_cc_no_unbound_error(self):
        qb = _make_qb()
        columns = [{
            "data": "status", "searchable": False, "search": {"value": "active"},
            "columnControl": {"search": {"value": "active", "logic": "equal"}},
        }]
        result = qb.build_column_search(columns)
        self.assertIn("$and", result)

    def test_not_searchable_no_cc_returns_empty(self):
        qb = _make_qb()
        columns = [{"data": "hidden", "searchable": False, "search": {"value": "test"}}]
        result = qb.build_column_search(columns)
        self.assertEqual(result, {})


class TestHashableOutsideLoop(unittest.TestCase):
    """Fix 2: _hashable defined outside the for loop in get_searchpanes_options."""

    def test_searchpanes_options_multiple_columns(self):
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
        self.assertIn("name", options)
        self.assertIn("status", options)
        self.assertEqual(len(options["name"]), 2)
        self.assertEqual(len(options["status"]), 2)


class TestSbDateBetweenSemantics(unittest.TestCase):
    """Fix 3: _sb_date between/!between use day-inclusive exclusive upper bound."""

    def test_between_uses_lt_not_lte(self):
        result = _sb_date("created", "between", "2024-01-01", "2024-01-31")
        cond = result["created"]
        self.assertIn("$lt", cond)
        self.assertNotIn("$lte", cond)
        self.assertEqual(cond["$lt"], datetime(2024, 2, 1))

    def test_between_lower_bound(self):
        result = _sb_date("created", "between", "2024-01-01", "2024-01-31")
        self.assertEqual(result["created"]["$gte"], datetime(2024, 1, 1))

    def test_not_between_upper_uses_gte_not_gt(self):
        result = _sb_date("created", "!between", "2024-01-01", "2024-01-31")
        upper = result["$or"][1]
        self.assertIn("$gte", upper["created"])
        self.assertNotIn("$gt", upper["created"])
        self.assertEqual(upper["created"]["$gte"], datetime(2024, 2, 1))

    def test_not_between_lower_bound(self):
        result = _sb_date("created", "!between", "2024-01-01", "2024-01-31")
        lower = result["$or"][0]
        self.assertEqual(lower["created"]["$lt"], datetime(2024, 1, 1))

    def test_between_single_day_range(self):
        result = _sb_date("created", "between", "2024-06-15", "2024-06-15")
        cond = result["created"]
        self.assertEqual(cond["$gte"], datetime(2024, 6, 15))
        self.assertEqual(cond["$lt"], datetime(2024, 6, 16))


class TestSbGroup(unittest.TestCase):
    def _fm(self):
        return FieldMapper([])

    def test_empty_group_returns_empty(self):
        self.assertEqual(_sb_group({"logic": "AND", "criteria": []}, self._fm()), {})

    def test_single_criterion_not_wrapped(self):
        c = {"condition": "=", "origData": "Title", "type": "string", "value": ["1984"]}
        result = _sb_group({"logic": "AND", "criteria": [c]}, self._fm())
        self.assertNotIn("$and", result)
        self.assertNotEqual(result, {})

    def test_and_logic_wraps_in_and(self):
        c = {"condition": "=", "origData": "Title", "type": "string", "value": ["1984"]}
        result = _sb_group({"logic": "AND", "criteria": [c, c]}, self._fm())
        self.assertIn("$and", result)

    def test_or_logic_wraps_in_or(self):
        c = {"condition": "=", "origData": "Title", "type": "string", "value": ["1984"]}
        result = _sb_group({"logic": "OR", "criteria": [c, c]}, self._fm())
        self.assertIn("$or", result)

    def test_nested_group(self):
        c = {"condition": "=", "origData": "Title", "type": "string", "value": ["1984"]}
        inner = {"logic": "OR", "criteria": [c, c]}
        outer = {"logic": "AND", "criteria": [c, inner]}
        result = _sb_group(outer, self._fm())
        self.assertIn("$and", result)

    def test_invalid_criterion_skipped(self):
        bad = {"condition": "=", "type": "string", "value": ["1984"]}
        result = _sb_group({"logic": "AND", "criteria": [bad]}, self._fm())
        self.assertEqual(result, {})


class TestGetRowgroupData:
    _BASE_ARGS = {
        "draw": 1, "start": 0, "length": 10,
        "search": {"value": "", "regex": False},
        "order": [{"column": 0, "dir": "asc"}],
        "columns": [{"data": "Title", "searchable": True, "orderable": True,
                      "search": {"value": "", "regex": False}}],
    }

    def _make_dt(self, request_args, data_fields=None, **custom_filter):
        col = MagicMock(spec=Collection)
        col.list_indexes = MagicMock(return_value=[])
        col.aggregate = MagicMock(return_value=iter([]))
        col.count_documents = MagicMock(return_value=0)
        col.estimated_document_count = MagicMock(return_value=0)
        db = {"test": col}
        return DataTables(db, "test", request_args, data_fields or [], **custom_filter), col

    def test_no_rowgroup_config_returns_none(self):
        dt, _ = self._make_dt(self._BASE_ARGS)
        assert dt._get_rowgroup_data() is None

    def test_string_datasrc_builds_pipeline(self):
        args = {**self._BASE_ARGS, "rowGroup": {"dataSrc": "Title"}}
        dt, col = self._make_dt(args)
        col.aggregate = MagicMock(return_value=iter([{"_id": "1984", "count": 1}]))
        result = dt._get_rowgroup_data()
        assert result is not None
        assert "dataSrc" in result
        assert "groups" in result

    def test_numeric_datasrc_maps_to_column(self):
        args = {**self._BASE_ARGS, "rowGroup": {"dataSrc": 0}}
        dt, col = self._make_dt(args)
        col.aggregate = MagicMock(return_value=iter([{"_id": "1984", "count": 1}]))
        result = dt._get_rowgroup_data()
        assert result is not None
        assert result["dataSrc"] == 0

    def test_out_of_range_datasrc_returns_none(self):
        args = {**self._BASE_ARGS, "rowGroup": {"dataSrc": 99}}
        dt, _ = self._make_dt(args)
        assert dt._get_rowgroup_data() is None

    def test_pymongo_error_returns_none(self):
        args = {**self._BASE_ARGS, "rowGroup": {"dataSrc": "Title"}}
        dt, col = self._make_dt(args)
        col.aggregate = MagicMock(side_effect=PyMongoError("db error"))
        assert dt._get_rowgroup_data() is None

    def test_empty_field_name_returns_none(self):
        args = {**self._BASE_ARGS, "rowGroup": {"dataSrc": ""}}
        dt, _ = self._make_dt(args)
        assert dt._get_rowgroup_data() is None

    def test_rowgroup_with_active_filter(self):
        args = {**self._BASE_ARGS, "rowGroup": {"dataSrc": "Title"},
                "search": {"value": "foo", "regex": False}}
        dt, col = self._make_dt(args)
        col.aggregate = MagicMock(return_value=iter([{"_id": "1984", "count": 1}]))
        dt._get_rowgroup_data()
        pipeline_arg = col.aggregate.call_args[0][0]
        assert any("$match" in stage for stage in pipeline_arg)


class TestGlobalSearchPerf:
    """Tests for global search query builder performance and correctness."""

    def _make_perf_qb(self, columns):
        fm = FieldMapper(columns)
        return MongoQueryBuilder(fm, use_text_index=False, has_text_index=False)

    def test_field_mapper_called_once_per_column_not_per_term(self):
        from unittest.mock import MagicMock
        fm = MagicMock(spec=FieldMapper)
        fm.get_field_type.return_value = "text"
        fm.get_db_field.side_effect = lambda c: c
        qb = MongoQueryBuilder(fm, use_text_index=False, has_text_index=False)
        columns = ["name", "city", "country"]
        terms = ["alice", "bob", "carol"]
        qb.build_global_search(terms, columns)
        assert fm.get_field_type.call_count == len(columns)
        assert fm.get_db_field.call_count == len(columns)

    def test_global_search_multi_term_produces_correct_or_conditions(self):
        qb = self._make_perf_qb(["name", "city"])
        result = qb.build_global_search(["alice", "bob"], ["name", "city"])
        assert "$and" in result
        assert len(result["$and"]) == 2
        for term_cond in result["$and"]:
            assert "$or" in term_cond
            assert len(term_cond["$or"]) == 2

    def test_global_search_quoted_phrase_word_boundary(self):
        qb = self._make_perf_qb(["name"])
        result = qb.build_global_search(["alice"], ["name"], original_search='"alice"')
        assert "$or" in result
        pattern = result["$or"][0]["name"]["$regex"]
        assert pattern.startswith("\\b") and pattern.endswith("\\b")

    def test_global_search_skips_date_columns(self):
        fields = [DataField("created", "date"), DataField("name", "string")]
        fm = FieldMapper(fields)
        qb = MongoQueryBuilder(fm, use_text_index=False, has_text_index=False)
        result = qb.build_global_search(["alice"], ["created", "name"])
        assert "$or" in result
        fields_searched = [list(cond.keys())[0] for cond in result["$or"]]
        assert "created" not in fields_searched
        assert "name" in fields_searched


if __name__ == '__main__':
    unittest.main()
