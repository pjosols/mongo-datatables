"""Consolidated serialization tests (float, Decimal128, Binary, Regex)."""
import json
import math
import uuid
import unittest
import pytest
from decimal import Decimal
from unittest.mock import MagicMock
from bson import Binary, Decimal128, ObjectId, Regex

from mongo_datatables import DataTables
from tests.base_test import BaseDataTablesTest


def _make_dt_simple():
    col = MagicMock()
    col.list_indexes.return_value = []
    db = MagicMock()
    db.__getitem__ = MagicMock(return_value=col)
    return DataTables(db, "test", {
        "draw": "1", "start": "0", "length": "10",
        "search": {"value": "", "regex": False},
        "order": [], "columns": [],
    }, data_fields=[])


# ---------------------------------------------------------------------------
# test_float_serialization.py — TestFloatSerialization
# ---------------------------------------------------------------------------

class TestFloatSerialization:
    def test_nan_converted_to_none(self):
        dt = _make_dt_simple()
        d = {"score": float("nan")}
        dt._format_result_values(d)
        assert d["score"] is None

    def test_inf_converted_to_none(self):
        dt = _make_dt_simple()
        d = {"score": float("inf")}
        dt._format_result_values(d)
        assert d["score"] is None

    def test_neg_inf_converted_to_none(self):
        dt = _make_dt_simple()
        d = {"score": float("-inf")}
        dt._format_result_values(d)
        assert d["score"] is None

    def test_finite_float_unchanged(self):
        dt = _make_dt_simple()
        d = {"score": 3.14}
        dt._format_result_values(d)
        assert d["score"] == 3.14

    def test_nan_in_nested_dict(self):
        dt = _make_dt_simple()
        d = {"stats": {"avg": float("nan"), "count": 5}}
        dt._format_result_values(d)
        assert d["stats"]["avg"] is None
        assert d["stats"]["count"] == 5

    def test_nan_in_list_converted_to_none(self):
        dt = _make_dt_simple()
        d = {"values": [float("nan"), float("inf"), 1.5]}
        dt._format_result_values(d)
        assert d["values"][0] is None
        assert d["values"][1] is None
        assert d["values"][2] == 1.5


# ---------------------------------------------------------------------------
# test_decimal128_serialization.py — TestDecimal128Serialization, TestSearchPanesDecimal128
# ---------------------------------------------------------------------------

class TestDecimal128Serialization(BaseDataTablesTest):
    def _make_dt(self):
        return DataTables(self.mongo, 'test_collection', self.request_args)

    def test_top_level_decimal128_converted_to_float(self):
        dt = self._make_dt()
        doc = {'price': Decimal128('19.99')}
        dt._format_result_values(doc)
        self.assertIsInstance(doc['price'], float)
        self.assertAlmostEqual(doc['price'], 19.99, places=2)

    def test_nested_decimal128_converted(self):
        dt = self._make_dt()
        doc = {'details': {'amount': Decimal128('1234.56')}}
        dt._format_result_values(doc)
        self.assertIsInstance(doc['details']['amount'], float)
        self.assertAlmostEqual(doc['details']['amount'], 1234.56, places=2)

    def test_decimal128_in_list_converted(self):
        dt = self._make_dt()
        doc = {'prices': [Decimal128('10.00'), Decimal128('20.50')]}
        dt._format_result_values(doc)
        self.assertEqual(doc['prices'], [10.0, 20.5])

    def test_decimal128_zero(self):
        dt = self._make_dt()
        doc = {'balance': Decimal128('0.00')}
        dt._format_result_values(doc)
        self.assertEqual(doc['balance'], 0.0)

    def test_decimal128_negative(self):
        dt = self._make_dt()
        doc = {'delta': Decimal128('-99.99')}
        dt._format_result_values(doc)
        self.assertAlmostEqual(doc['delta'], -99.99, places=2)

    def test_non_decimal128_unaffected(self):
        dt = self._make_dt()
        doc = {'name': 'Alice', 'count': 42, 'active': True}
        dt._format_result_values(doc)
        self.assertEqual(doc, {'name': 'Alice', 'count': 42, 'active': True})

    def test_mixed_list_with_decimal128(self):
        dt = self._make_dt()
        oid = ObjectId()
        doc = {'items': [Decimal128('5.00'), oid, 'text']}
        dt._format_result_values(doc)
        self.assertAlmostEqual(doc['items'][0], 5.0, places=2)
        self.assertEqual(doc['items'][1], str(oid))
        self.assertEqual(doc['items'][2], 'text')


class TestSearchPanesDecimal128(BaseDataTablesTest):
    def test_searchpanes_decimal128_display_value(self):
        from mongo_datatables.datatables import DataField
        request_args = dict(self.request_args)
        request_args['columns'][0]['searchable'] = 'true'
        dt = DataTables(
            self.mongo, 'test_collection', request_args,
            data_fields=[DataField('name', 'number')]
        )
        facet_result = [{'_id': Decimal128('9.99'), 'count': 3}]
        total_agg = [{'name': facet_result}]
        count_agg = [{'name': facet_result}]
        self.collection.aggregate.side_effect = [iter(total_agg), iter(count_agg)]
        options = dt.get_searchpanes_options()
        self.assertIn('name', options)
        labels = [o['label'] for o in options['name']]
        self.assertIn('9.99', labels)


# ---------------------------------------------------------------------------
# test_binary_serialization.py — TestBinarySerializationTopLevel, InList, JsonSerializable
# ---------------------------------------------------------------------------

class TestBinarySerializationTopLevel(BaseDataTablesTest):
    def _make_dt(self):
        return DataTables(self.mongo, 'test_collection', self.request_args)

    def test_uuid_subtype4_serialized_as_uuid_string(self):
        uid = uuid.uuid4()
        doc = {"user_id": Binary(uid.bytes, 4)}
        self._make_dt()._format_result_values(doc)
        self.assertEqual(doc["user_id"], str(uid))

    def test_uuid_subtype3_serialized_as_uuid_string(self):
        uid = uuid.uuid4()
        doc = {"user_id": Binary(uid.bytes, 3)}
        self._make_dt()._format_result_values(doc)
        self.assertEqual(doc["user_id"], str(uid))

    def test_non_uuid_binary_serialized_as_hex(self):
        raw = Binary(b"\x01\x02\x03", 0)
        doc = {"data": raw}
        self._make_dt()._format_result_values(doc)
        self.assertEqual(doc["data"], raw.hex())

    def test_non_binary_fields_unaffected(self):
        doc = {"name": "Alice", "age": 30}
        self._make_dt()._format_result_values(doc)
        self.assertEqual(doc["name"], "Alice")
        self.assertEqual(doc["age"], 30)


class TestBinarySerializationInList(BaseDataTablesTest):
    def _make_dt(self):
        return DataTables(self.mongo, 'test_collection', self.request_args)

    def test_uuid_in_list_serialized(self):
        uid = uuid.uuid4()
        doc = {"ids": [Binary(uid.bytes, 4)]}
        self._make_dt()._format_result_values(doc)
        self.assertEqual(doc["ids"], [str(uid)])

    def test_non_uuid_binary_in_list_serialized_as_hex(self):
        raw = Binary(b"\xde\xad", 0)
        doc = {"data": [raw]}
        self._make_dt()._format_result_values(doc)
        self.assertEqual(doc["data"], [raw.hex()])


class TestBinaryJsonSerializable(BaseDataTablesTest):
    def test_result_is_json_serializable(self):
        uid = uuid.uuid4()
        doc = {"user_id": Binary(uid.bytes, 4), "data": Binary(b"\x01", 0)}
        DataTables(self.mongo, 'test_collection', self.request_args)._format_result_values(doc)
        json.dumps(doc)


# ---------------------------------------------------------------------------
# test_regex_serialization.py — TestRegexSerialization
# ---------------------------------------------------------------------------

class TestRegexSerialization:
    def test_regex_with_flags_serialized(self):
        dt = _make_dt_simple()
        doc = {"pattern": Regex("foo.*bar", "i")}
        dt._format_result_values(doc)
        assert doc["pattern"] == "/foo.*bar/i"

    def test_regex_no_flags_serialized(self):
        dt = _make_dt_simple()
        doc = {"pattern": Regex("simple")}
        dt._format_result_values(doc)
        assert doc["pattern"] == "/simple/"

    def test_regex_multiple_flags(self):
        dt = _make_dt_simple()
        doc = {"pattern": Regex("test", "im")}
        dt._format_result_values(doc)
        result = doc["pattern"]
        assert result.startswith("/test/")
        assert "i" in result
        assert "m" in result

    def test_regex_in_list(self):
        dt = _make_dt_simple()
        doc = {"patterns": [Regex("alpha", "i"), Regex("beta")]}
        dt._format_result_values(doc)
        assert doc["patterns"][0] == "/alpha/i"
        assert doc["patterns"][1] == "/beta/"

    def test_regex_json_serializable(self):
        dt = _make_dt_simple()
        doc = {"pattern": Regex("test", "i")}
        dt._format_result_values(doc)
        result = json.dumps(doc)
        assert "/test/i" in result

    def test_non_regex_fields_unaffected(self):
        dt = _make_dt_simple()
        doc = {"name": "Alice", "age": 30, "pattern": Regex("x")}
        dt._format_result_values(doc)
        assert doc["name"] == "Alice"
        assert doc["age"] == 30
        assert doc["pattern"] == "/x/"

    def test_regex_in_list_json_serializable(self):
        dt = _make_dt_simple()
        doc = {"patterns": [Regex("a", "i"), "plain_string"]}
        dt._format_result_values(doc)
        result = json.dumps(doc)
        assert "/a/i" in result
        assert "plain_string" in result
