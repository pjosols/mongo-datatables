"""Tests for bson.Binary / UUID serialization in _format_result_values."""
import json
import uuid
import unittest
from bson import Binary
from mongo_datatables import DataTables
from tests.base_test import BaseDataTablesTest


class TestBinarySerializationTopLevel(BaseDataTablesTest):
    """Tests that Binary values are serialized correctly at the top level."""

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
    """Tests that Binary values inside lists are serialized correctly."""

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
    """Tests that Binary values produce JSON-serializable output."""

    def test_result_is_json_serializable(self):
        uid = uuid.uuid4()
        doc = {"user_id": Binary(uid.bytes, 4), "data": Binary(b"\x01", 0)}
        DataTables(self.mongo, 'test_collection', self.request_args)._format_result_values(doc)
        # Should not raise
        json.dumps(doc)


if __name__ == '__main__':
    unittest.main()
