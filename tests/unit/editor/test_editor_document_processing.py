"""Test Editor document preprocessing, nested fields, and type conversions."""
import unittest
import pytest
from unittest.mock import MagicMock
from datetime import datetime
from bson.objectid import ObjectId
from pymongo.collection import Collection
from pymongo.database import Database

from mongo_datatables import Editor
from mongo_datatables.datatables import DataField
from mongo_datatables.editor.document import preprocess_document
from mongo_datatables.exceptions import InvalidDataError
from mongo_datatables.utils import FieldMapper


class TestPreprocessDocument(unittest.TestCase):
    def setUp(self):
        self.mongo = MagicMock()
        self.mongo.db = MagicMock(spec=Database)
        self.collection = MagicMock(spec=Collection)
        self.mongo.db.__getitem__.return_value = self.collection

    def test_preprocess_document_with_json_data(self):
        request_args = {"action": "create", "data": {"0": {
            "name": "Test User",
            "tags": "[\"tag1\", \"tag2\"]",
            "metadata": "{\"key\": \"value\"}",
        }}}
        editor = Editor(self.mongo, 'users', request_args)
        processed_doc, dot_notation = editor._preprocess_document(editor.data["0"])
        self.assertEqual(processed_doc["tags"], ["tag1", "tag2"])
        self.assertEqual(processed_doc["metadata"], {"key": "value"})

    def test_preprocess_document_with_date_fields(self):
        request_args = {"action": "create", "data": {"0": {
            "name": "Test User",
            "created_at": "2023-01-15T14:30:45",
            "update_date": "2023-02-20",
            "metadata.last_login_time": "2023-03-10T09:15:30Z"
        }}}
        editor = Editor(self.mongo, 'users', request_args)
        processed_doc, dot_notation = editor._preprocess_document(editor.data["0"])
        self.assertIsInstance(processed_doc["created_at"], datetime)
        self.assertIsInstance(processed_doc["update_date"], datetime)
        self.assertIsInstance(dot_notation["metadata.last_login_time"], datetime)
        self.assertEqual(processed_doc["created_at"].year, 2023)
        self.assertEqual(processed_doc["update_date"].month, 2)
        self.assertEqual(dot_notation["metadata.last_login_time"].month, 3)

    def test_preprocess_document_with_nested_fields(self):
        request_args = {"action": "create", "data": {"0": {
            "name": "Test User",
            "profile.bio": "Developer",
            "profile.skills": "[\"Python\", \"MongoDB\"]",
            "contact.email": "test@example.com",
            "contact.phone": "123-456-7890"
        }}}
        editor = Editor(self.mongo, 'users', request_args)
        processed_doc, dot_notation = editor._preprocess_document(editor.data["0"])
        self.assertEqual(dot_notation["profile.bio"], "Developer")
        self.assertEqual(dot_notation["profile.skills"], ["Python", "MongoDB"])
        self.assertNotIn("profile.bio", processed_doc)
        self.assertNotIn("contact.email", processed_doc)

    def test_preprocess_document_date_parse_failure_on_dot_key(self):
        editor = Editor(self.mongo, 'test', {})
        doc = {"profile.created_at": "not-a-date"}
        processed, dot = editor._preprocess_document(doc)
        self.assertEqual(dot["profile.created_at"], "not-a-date")

    def test_preprocess_document_non_string_value(self):
        editor = Editor(self.mongo, 'test', {})
        processed, dot = editor._preprocess_document({"count": 42, "name": "Alice"})
        self.assertEqual(processed["count"], 42)

    def test_preprocess_document_date_parse_failure_no_dot(self):
        editor = Editor(self.mongo, 'test', {})
        processed, dot = editor._preprocess_document({"created_at": "not-a-date"})
        self.assertNotIn("created_at", dot)
        self.assertEqual(processed.get("created_at"), "not-a-date")

    def test_format_response_document(self):
        editor = Editor(self.mongo, 'users', {})
        doc = {
            "_id": ObjectId(), "name": "Test User",
            "created_at": datetime(2023, 5, 15, 10, 30, 0),
            "tags": ["tag1", "tag2"], "metadata": {"key": "value"},
            "active": True, "score": 95.5
        }
        response_doc = editor._format_response_document(doc)
        self.assertNotIn("_id", response_doc)
        self.assertIn("DT_RowId", response_doc)
        self.assertEqual(response_doc["DT_RowId"], str(doc["_id"]))
        self.assertEqual(response_doc["name"], "Test User")
        self.assertIsInstance(response_doc["created_at"], str)
        self.assertEqual(response_doc["created_at"], "2023-05-15T10:30:00")

    def test_format_response_document_objectid_in_non_id_field(self):
        oid = ObjectId()
        editor = Editor(self.mongo, 'test', {})
        result = editor._format_response_document({"_id": ObjectId(), "ref": oid, "name": "x"})
        self.assertEqual(result["ref"], str(oid))

    def test_format_response_document_no_id_field(self):
        editor = Editor(self.mongo, 'test', {})
        result = editor._format_response_document({"name": "x", "score": 1})
        self.assertNotIn("DT_RowId", result)
        self.assertEqual(result["name"], "x")


class TestPreprocessDocumentInputValidation:
    """Tests that preprocess_document rejects malicious/malformed payloads."""

    def _call(self, doc, fields=None, data_fields=None):
        fm = FieldMapper(data_fields or [])
        return preprocess_document(doc, fields or {}, data_fields or [], fm)

    def test_valid_doc_processes_normally(self):
        processed, dot = self._call({"name": "Alice"})
        assert processed["name"] == "Alice"

    def test_too_many_keys_raises(self):
        doc = {f"field_{i}": "v" for i in range(201)}
        with pytest.raises(InvalidDataError, match="too many keys"):
            self._call(doc)

    def test_deeply_nested_doc_raises(self):
        doc: dict = {}
        current = doc
        for _ in range(11):
            current["child"] = {}
            current = current["child"]
        with pytest.raises(InvalidDataError, match="nesting exceeds"):
            self._call(doc)

    def test_oversized_string_raises(self):
        doc = {"payload": "x" * 1_000_001}
        with pytest.raises(InvalidDataError, match="exceeds maximum string length"):
            self._call(doc)

    def test_invalid_field_name_raises(self):
        with pytest.raises(InvalidDataError):
            self._call({"$evil": "value"})

    def test_empty_doc_returns_empty(self):
        processed, dot = self._call({})
        assert processed == {}
        assert dot == {}

    def test_json_string_value_is_parsed(self):
        processed, _ = self._call({"tags": '["a", "b"]'})
        assert processed["tags"] == ["a", "b"]

    def test_non_dict_input_raises_or_passes_gracefully(self):
        with pytest.raises((AttributeError, TypeError, InvalidDataError)):
            self._call("not a dict")
