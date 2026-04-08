"""Test Editor validation, error handling, and field mapping."""
import unittest
from unittest.mock import MagicMock, patch
from bson.errors import InvalidId
from bson.objectid import ObjectId
from pymongo.collection import Collection
from pymongo.database import Database
from pymongo.errors import PyMongoError

from mongo_datatables import Editor
from mongo_datatables.datatables import DataField
from mongo_datatables.exceptions import InvalidDataError, DatabaseOperationError, FieldMappingError


class TestEditorValidation(unittest.TestCase):
    def setUp(self):
        self.mongo = MagicMock()
        self.mongo.db = MagicMock(spec=Database)
        self.collection = MagicMock(spec=Collection)
        self.mongo.db.__getitem__.return_value = self.collection

    def test_db_property_returns_none_for_plain_object(self):
        collection = self.collection

        class FakeClient:
            def __getitem__(self, name):
                return collection

        editor = Editor(FakeClient(), 'test', {})
        self.assertIsNone(editor.db)

    def test_map_ui_field_to_db_field(self):
        data_fields = [DataField('full_name', 'string', alias='name')]
        editor = Editor(self.mongo, 'test', {}, data_fields=data_fields)
        self.assertEqual(editor.map_ui_field_to_db_field('name'), 'full_name')

    def test_search_skips_none_values(self):
        cursor = MagicMock()
        cursor.limit.return_value = [
            {"_id": ObjectId(), "status": None},
            {"_id": ObjectId(), "status": "active"},
        ]
        self.collection.find.return_value = cursor
        editor = Editor(self.mongo, 'test', {"action": "search", "field": "status", "search": "act"})
        result = editor.search()
        self.assertEqual(len(result["data"]), 1)
        self.assertEqual(result["data"][0]["value"], "active")

    def test_search_with_values_number_field(self):
        data_fields = [DataField('score', 'number')]
        cursor = MagicMock()
        cursor.limit.return_value = [{"_id": ObjectId(), "score": 42}]
        self.collection.find.return_value = cursor
        editor = Editor(self.mongo, 'test',
                        {"action": "search", "field": "score", "values": ["42"]},
                        data_fields=data_fields)
        result = editor.search()
        self.assertIn("data", result)

    def test_create_wraps_pymongo_error(self):
        self.collection.insert_one.side_effect = PyMongoError("db error")
        editor = Editor(self.mongo, 'test', {"action": "create", "data": {"0": {"name": "x"}}})
        with self.assertRaises(DatabaseOperationError):
            editor.create()

    def test_create_wraps_unexpected_exception(self):
        self.collection.insert_one.side_effect = RuntimeError("unexpected")
        editor = Editor(self.mongo, 'test', {"action": "create", "data": {"0": {"name": "x"}}})
        with self.assertRaises(RuntimeError):
            editor.create()

    def test_create_reraises_invalid_data_error(self):
        editor = Editor(self.mongo, 'test', {"action": "create", "data": {"0": {"name": "x"}}})
        with patch('mongo_datatables.editor.crud.preprocess_document', side_effect=InvalidDataError("bad")):
            with self.assertRaises(InvalidDataError):
                editor.create()

    def test_edit_raises_for_invalid_objectid_on_update(self):
        doc_id = str(ObjectId())
        self.collection.update_one.side_effect = InvalidId("bad id")
        self.collection.find_one.return_value = {"_id": ObjectId(doc_id), "name": "x"}
        editor = Editor(self.mongo, 'test',
                        {"action": "edit", "data": {doc_id: {"name": "x"}}},
                        doc_id=doc_id)
        with self.assertRaises(InvalidDataError):
            editor.edit()

    def test_edit_reraises_invalid_data_error(self):
        doc_id = str(ObjectId())
        self.collection.update_one.side_effect = InvalidDataError("bad")
        editor = Editor(self.mongo, 'test',
                        {"action": "edit", "data": {doc_id: {"name": "x"}}},
                        doc_id=doc_id)
        with self.assertRaises(InvalidDataError):
            editor.edit()

    def test_edit_wraps_pymongo_error(self):
        doc_id = str(ObjectId())
        self.collection.update_one.side_effect = PyMongoError("db error")
        editor = Editor(self.mongo, 'test',
                        {"action": "edit", "data": {doc_id: {"name": "x"}}},
                        doc_id=doc_id)
        with self.assertRaises(DatabaseOperationError):
            editor.edit()

    def test_edit_wraps_unexpected_exception(self):
        doc_id = str(ObjectId())
        self.collection.update_one.side_effect = RuntimeError("unexpected")
        editor = Editor(self.mongo, 'test',
                        {"action": "edit", "data": {doc_id: {"name": "x"}}},
                        doc_id=doc_id)
        with self.assertRaises(RuntimeError):
            editor.edit()


class TestCoerceValues(unittest.TestCase):
    def setUp(self):
        self.mongo = MagicMock()
        self.mongo.db = MagicMock(spec=Database)
        self.collection = MagicMock(spec=Collection)
        self.mongo.db.__getitem__.return_value = self.collection

    def _editor(self, field_type: str = "number") -> Editor:
        data_fields = [DataField("score", field_type)]
        return Editor(self.mongo, "test", {}, data_fields=data_fields)

    def test_coerce_values_number(self):
        editor = self._editor()
        result = editor._coerce_values('score', ['42', '3.14'])
        self.assertEqual(result[0], 42)
        self.assertAlmostEqual(result[1], 3.14)

    def test_coerce_values_boolean(self):
        data_fields = [DataField('active', 'boolean')]
        editor = Editor(self.mongo, 'test', {}, data_fields=data_fields)
        result = editor._coerce_values('active', ['true', '1', 'false', True])
        self.assertEqual(result, [True, True, False, True])

    def test_coerce_values_string_type_passthrough(self):
        data_fields = [DataField('tag', 'string')]
        editor = Editor(self.mongo, 'test', {}, data_fields=data_fields)
        values = ['foo', 'bar']
        self.assertEqual(editor._coerce_values('tag', values), values)

    def test_invalid_number_string_falls_back_to_raw(self):
        editor = self._editor()
        result = editor._coerce_values("score", ["not-a-number"])
        self.assertEqual(result, ["not-a-number"])

    def test_field_mapping_error_raised_by_to_number_is_caught(self):
        editor = self._editor()
        with patch(
            "mongo_datatables.editor.core.TypeConverter.to_number",
            side_effect=FieldMappingError("Cannot convert"),
        ):
            result = editor._coerce_values("score", ["bad"])
        self.assertEqual(result, ["bad"])

    def test_mixed_valid_and_invalid_numbers(self):
        editor = self._editor()
        result = editor._coerce_values("score", ["42", "bad", "3.14"])
        self.assertEqual(result[0], 42)
        self.assertEqual(result[1], "bad")
        self.assertAlmostEqual(result[2], 3.14)

    def test_empty_string_falls_back_to_raw(self):
        editor = self._editor()
        result = editor._coerce_values("score", [""])
        self.assertEqual(result, [""])

    def test_none_value_falls_back_to_raw(self):
        editor = self._editor()
        result = editor._coerce_values("score", [None])
        self.assertEqual(result, [None])
