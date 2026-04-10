"""Test DataTables serialization: float, Decimal128, Binary, Regex, formatting."""
import json
import unittest
import uuid
from decimal import Decimal
from bson import Binary, Decimal128, ObjectId, Regex

from mongo_datatables import DataTables
from mongo_datatables.datatables import DataField
from mongo_datatables.datatables.formatting import format_result_values, process_cursor, remap_aliases
from mongo_datatables.utils import FieldMapper
from tests.unit.base_test import BaseDataTablesTest


# ---------------------------------------------------------------------------
# test_float_serialization.py — TestFloatSerialization
# ---------------------------------------------------------------------------

class TestFloatSerialization(BaseDataTablesTest):
    """Test NaN, Inf, and finite float serialization."""
        return DataTables(self.mongo, 'test_collection', self.request_args)

    def test_nan_converted_to_none(self):
        d = {"score": float("nan")}
        self._make_dt()._format_result_values(d)
        self.assertIsNone(d["score"])

    def test_inf_converted_to_none(self):
        d = {"score": float("inf")}
        self._make_dt()._format_result_values(d)
        self.assertIsNone(d["score"])

    def test_neg_inf_converted_to_none(self):
        d = {"score": float("-inf")}
        self._make_dt()._format_result_values(d)
        self.assertIsNone(d["score"])

    def test_finite_float_unchanged(self):
        d = {"score": 3.14}
        self._make_dt()._format_result_values(d)
        self.assertEqual(d["score"], 3.14)

    def test_nan_in_nested_dict(self):
        d = {"stats": {"avg": float("nan"), "count": 5}}
        self._make_dt()._format_result_values(d)
        self.assertIsNone(d["stats"]["avg"])
        self.assertEqual(d["stats"]["count"], 5)

    def test_nan_in_list_converted_to_none(self):
        d = {"values": [float("nan"), float("inf"), 1.5]}
        self._make_dt()._format_result_values(d)
        self.assertIsNone(d["values"][0])
        self.assertIsNone(d["values"][1])
        self.assertEqual(d["values"][2], 1.5)


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

class TestRegexSerialization(BaseDataTablesTest):
    def _make_dt(self):
        return DataTables(self.mongo, 'test_collection', self.request_args)

    def test_regex_with_flags_serialized(self):
        doc = {"pattern": Regex("foo.*bar", "i")}
        self._make_dt()._format_result_values(doc)
        self.assertEqual(doc["pattern"], "/foo.*bar/i")

    def test_regex_no_flags_serialized(self):
        doc = {"pattern": Regex("simple")}
        self._make_dt()._format_result_values(doc)
        self.assertEqual(doc["pattern"], "/simple/")

    def test_regex_multiple_flags(self):
        doc = {"pattern": Regex("test", "im")}
        self._make_dt()._format_result_values(doc)
        result = doc["pattern"]
        self.assertTrue(result.startswith("/test/"))
        self.assertIn("i", result)
        self.assertIn("m", result)

    def test_regex_in_list(self):
        doc = {"patterns": [Regex("alpha", "i"), Regex("beta")]}
        self._make_dt()._format_result_values(doc)
        self.assertEqual(doc["patterns"][0], "/alpha/i")
        self.assertEqual(doc["patterns"][1], "/beta/")

    def test_regex_json_serializable(self):
        doc = {"pattern": Regex("test", "i")}
        self._make_dt()._format_result_values(doc)
        result = json.dumps(doc)
        self.assertIn("/test/i", result)

    def test_non_regex_fields_unaffected(self):
        doc = {"name": "Alice", "age": 30, "pattern": Regex("x")}
        self._make_dt()._format_result_values(doc)
        self.assertEqual(doc["name"], "Alice")
        self.assertEqual(doc["age"], 30)
        self.assertEqual(doc["pattern"], "/x/")

    def test_regex_in_list_json_serializable(self):
        doc = {"patterns": [Regex("a", "i"), "plain_string"]}
        self._make_dt()._format_result_values(doc)
        result = json.dumps(doc)
        self.assertIn("/a/i", result)
        self.assertIn("plain_string", result)


class TestFormattingCoverageGaps(unittest.TestCase):
    """Cover the 5 uncovered branches in formatting.py."""

    def test_format_result_values_empty_dict_returns_early(self):
        """L21: empty dict → early return, no error."""
        d = {}
        format_result_values(d)
        self.assertEqual(d, {})

    def test_format_result_values_list_containing_dict(self):
        """L32: list item is a dict → recursive call on it."""
        oid = ObjectId()
        d = {"items": [{"_id": oid, "name": "x"}]}
        format_result_values(d)
        self.assertEqual(d["items"][0]["_id"], str(oid))

    def test_remap_aliases_dotted_top_key_missing_from_doc(self):
        """L88->71: dotted db_field but top-level key absent → no del, loop continues."""
        fm = FieldMapper([DataField("meta.date", "date", alias="pub_date")])
        doc = {"name": "Alice"}  # "meta" key not present
        result = remap_aliases(doc, fm)
        self.assertNotIn("meta", result)

    def test_remap_aliases_simple_rename_field_absent(self):
        """L98->71: simple rename but db_field not in doc → no-op, loop continues."""
        fm = FieldMapper([DataField("full_name", "string", alias="display_name")])
        doc = {"age": 30}  # "full_name" not present
        result = remap_aliases(doc, fm)
        self.assertNotIn("display_name", result)
        self.assertEqual(result["age"], 30)

    def test_process_cursor_row_without_id_fields(self):
        """L117->119: row has neither row_id nor _id → no DT_RowId set."""
        fm = FieldMapper([])
        cursor = [{"name": "Alice", "age": 30}]
        result = process_cursor(cursor, row_id=None, field_mapper=fm)
        self.assertNotIn("DT_RowId", result[0])
        self.assertEqual(result[0]["name"], "Alice")
