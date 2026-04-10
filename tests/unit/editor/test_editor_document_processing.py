"""Test Editor document preprocessing, nested fields, and type conversions.

Validates JSON parsing, date field handling (CWE-20 fix), nested field separation,
input validation, and response formatting for Editor CRUD operations.
"""
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


class TestPreprocessDocument(unittest.TestCase):
    """Test preprocess_document with JSON, dates, and nested fields."""

    def setUp(self):
        self.mongo = MagicMock()
        self.mongo.db = MagicMock(spec=Database)
        self.collection = MagicMock(spec=Collection)
        self.mongo.db.__getitem__.return_value = self.collection

    def test_preprocess_document_with_json_data(self):
        """Parse JSON strings in document fields."""
        from mongo_datatables.datatables import DataField
        request_args = {"action": "create", "data": {"0": {
            "name": "Test User",
            "tags": "[\"tag1\", \"tag2\"]",
            "metadata": "{\"key\": \"value\"}",
        }}}
        data_fields = [DataField("name", "string"), DataField("tags", "array"), DataField("metadata", "object")]
        editor = Editor(self.mongo, 'users', request_args, data_fields=data_fields)
        processed_doc, dot_notation = editor._preprocess_document(editor.data["0"])
        self.assertEqual(processed_doc["tags"], ["tag1", "tag2"])
        self.assertEqual(processed_doc["metadata"], {"key": "value"})

    def test_preprocess_document_with_date_fields(self):
        """Date fields are only parsed when declared via data_fields (CWE-20 fix)."""
        from mongo_datatables.datatables import DataField
        from mongo_datatables.editor.document import preprocess_document
        data_fields = [
            DataField("created_at", "date"),
            DataField("update_date", "date"),
            DataField("last_login_time", "date"),
        ]
        doc = {
            "name": "Test User",
            "created_at": "2023-01-15T14:30:45",
            "update_date": "2023-02-20T00:00:00",
            "last_login_time": "2023-03-10T09:15:30",
        }
        fields = {f.alias: f for f in data_fields}
        processed_doc, _ = preprocess_document(doc, fields, data_fields)
        self.assertIsInstance(processed_doc["created_at"], datetime)
        self.assertIsInstance(processed_doc["update_date"], datetime)
        self.assertEqual(processed_doc["created_at"].year, 2023)
        self.assertEqual(processed_doc["update_date"].month, 2)

    def test_preprocess_document_with_nested_fields(self):
        from mongo_datatables.datatables import DataField
        request_args = {"action": "create", "data": {"0": {
            "name": "Test User",
            "profile.bio": "Developer",
            "profile.skills": "[\"Python\", \"MongoDB\"]",
            "contact.email": "test@example.com",
            "contact.phone": "123-456-7890"
        }}}
        data_fields = [
            DataField("name", "string"), DataField("profile", "object"),
            DataField("contact", "object"),
        ]
        editor = Editor(self.mongo, 'users', request_args, data_fields=data_fields)
        processed_doc, dot_notation = editor._preprocess_document(editor.data["0"])
        self.assertEqual(dot_notation["profile.bio"], "Developer")
        self.assertEqual(dot_notation["profile.skills"], ["Python", "MongoDB"])
        self.assertNotIn("profile.bio", processed_doc)
        self.assertNotIn("contact.email", processed_doc)

    def test_preprocess_document_date_parse_failure_on_dot_key(self):
        """Store raw value when date parsing fails on dot-notation key."""
        from mongo_datatables.datatables import DataField
        data_fields = [DataField("profile", "object")]
        editor = Editor(self.mongo, 'test', {}, data_fields=data_fields)
        doc = {"profile.created_at": "not-a-date"}
        processed, dot = editor._preprocess_document(doc)
        self.assertEqual(dot["profile.created_at"], "not-a-date")

    def test_preprocess_document_non_string_value(self):
        """Handle non-string values (numbers, booleans) correctly."""
        from mongo_datatables.datatables import DataField
        data_fields = [DataField("count", "number"), DataField("name", "string")]
        editor = Editor(self.mongo, 'test', {}, data_fields=data_fields)
        processed, dot = editor._preprocess_document({"count": 42, "name": "Alice"})
        self.assertEqual(processed["count"], 42)

    def test_preprocess_document_date_parse_failure_no_dot(self):
        """Store raw value when date parsing fails on non-dot key."""
        from mongo_datatables.datatables import DataField
        data_fields = [DataField("created_at", "string")]
        editor = Editor(self.mongo, 'test', {}, data_fields=data_fields)
        processed, dot = editor._preprocess_document({"created_at": "not-a-date"})
        self.assertNotIn("created_at", dot)
        self.assertEqual(processed.get("created_at"), "not-a-date")

    def test_format_response_document(self):
        """Format MongoDB document for Editor response with ObjectId and datetime conversion."""
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
        """Convert ObjectId in non-_id fields to string."""
        oid = ObjectId()
        editor = Editor(self.mongo, 'test', {})
        result = editor._format_response_document({"_id": ObjectId(), "ref": oid, "name": "x"})
        self.assertEqual(result["ref"], str(oid))

    def test_format_response_document_no_id_field(self):
        """Omit DT_RowId when document has no _id field."""
        editor = Editor(self.mongo, 'test', {})
        result = editor._format_response_document({"name": "x", "score": 1})
        self.assertNotIn("DT_RowId", result)
        self.assertEqual(result["name"], "x")


class TestPreprocessDocumentInputValidation:
    """Test preprocess_document rejects malicious/malformed payloads."""

    def _call(self, doc, fields=None, data_fields=None):
        return preprocess_document(doc, fields or {}, data_fields or [])

    def _call_with_wl(self, doc, *field_names):
        df = [DataField(n, "string") for n in field_names]
        return preprocess_document(doc, {f.alias: f for f in df}, df)

    def test_valid_doc_processes_normally(self):
        """Process valid document with whitelist normally."""
        processed, dot = self._call_with_wl({"name": "Alice"}, "name")
        assert processed["name"] == "Alice"

    def test_no_whitelist_raises(self):
        """Raise InvalidDataError when no whitelist is configured."""
        with pytest.raises(InvalidDataError, match="whitelist"):
            self._call({"name": "Alice"})

    def test_too_many_keys_raises(self):
        """Raise InvalidDataError when document has too many keys."""
        doc = {f"field_{i}": "v" for i in range(201)}
        with pytest.raises(InvalidDataError, match="too many keys"):
            self._call(doc)

    def test_deeply_nested_doc_raises(self):
        """Raise InvalidDataError when document nesting exceeds limit."""
        doc: dict = {}
        current = doc
        for _ in range(11):
            current["child"] = {}
            current = current["child"]
        with pytest.raises(InvalidDataError, match="nesting exceeds"):
            self._call(doc)

    def test_oversized_string_raises(self):
        """Raise InvalidDataError when string value exceeds maximum length."""
        doc = {"payload": "x" * 1_000_001}
        with pytest.raises(InvalidDataError, match="exceeds maximum string length"):
            self._call(doc)

    def test_invalid_field_name_raises(self):
        """Raise InvalidDataError when field name contains invalid characters."""
        with pytest.raises(InvalidDataError):
            self._call_with_wl({"$evil": "value"}, "$evil")

    def test_empty_doc_returns_empty(self):
        """Return empty dicts for empty document."""
        processed, dot = self._call_with_wl({}, "name")
        assert processed == {}
        assert dot == {}

    def test_json_string_value_is_parsed(self):
        """Parse JSON string values to objects."""
        processed, _ = self._call_with_wl({"tags": '["a", "b"]'}, "tags")
        assert processed["tags"] == ["a", "b"]

    def test_non_dict_input_raises_or_passes_gracefully(self):
        """Raise or handle gracefully when input is not a dict."""
        with pytest.raises((AttributeError, TypeError, InvalidDataError)):
            self._call("not a dict")
