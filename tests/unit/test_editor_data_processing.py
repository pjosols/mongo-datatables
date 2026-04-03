"""Editor tests — data processing and document transformation."""
import unittest
import pytest
from unittest.mock import MagicMock, patch
from datetime import datetime
from bson.errors import InvalidId
from bson.objectid import ObjectId
from pymongo.collection import Collection
from pymongo.database import Database
from pymongo.results import InsertOneResult, UpdateResult, DeleteResult
from pymongo.errors import PyMongoError

from mongo_datatables import Editor
from mongo_datatables.datatables import DataField
from mongo_datatables.editor import StorageAdapter
from mongo_datatables.exceptions import InvalidDataError, DatabaseOperationError

class TestEditorAdvanced(unittest.TestCase):
    def setUp(self):
        self.mongo = MagicMock()
        self.mongo.db = MagicMock(spec=Database)
        self.collection = MagicMock(spec=Collection)
        self.mongo.db.__getitem__.return_value = self.collection
        self.sample_id = str(ObjectId())
        self.sample_id2 = str(ObjectId())

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

    def test_process_updates_with_nested_data(self):
        data_fields = [
            DataField("profile.joined_date", "date"),
            DataField("stats.visits", "number"),
            DataField("settings.notifications", "boolean"),
            DataField("tags", "array")
        ]
        editor = Editor(self.mongo, 'users', {}, data_fields=data_fields)
        data = {
            "profile": {"name": "John Doe", "joined_date": "2023-05-15"},
            "stats": {"visits": "42", "last_seen": "2023-06-01T10:30:00"},
            "settings": {"notifications": "true", "theme": "dark"},
            "tags": "[\"member\", \"premium\"]"
        }
        updates = {}
        editor._process_updates(data, updates)
        self.assertIsInstance(updates["profile.joined_date"], datetime)
        self.assertEqual(updates["profile.joined_date"].day, 15)
        self.assertEqual(updates["stats.visits"], 42)
        self.assertTrue(updates["settings.notifications"])
        self.assertEqual(updates["tags"], ["member", "premium"])

    def test_process_updates_with_type_conversions(self):
        data_fields = [
            DataField("age", "number"), DataField("active", "boolean"),
            DataField("scores", "array"), DataField("birthday", "date")
        ]
        editor = Editor(self.mongo, 'users', {}, data_fields=data_fields)
        data = {"age": "30", "active": "yes", "scores": "[90, 85, 95]", "birthday": "1993-08-20"}
        updates = {}
        editor._process_updates(data, updates)
        self.assertEqual(updates["age"], 30)
        self.assertTrue(updates["active"])
        self.assertEqual(updates["scores"], [90, 85, 95])
        self.assertIsInstance(updates["birthday"], datetime)

    def test_process_updates_with_invalid_values(self):
        data_fields = [
            DataField("age", "number"), DataField("joined_date", "date"), DataField("tags", "array")
        ]
        editor = Editor(self.mongo, 'users', {}, data_fields=data_fields)
        data = {"age": "not-a-number", "joined_date": "invalid-date", "tags": "not-valid-json"}
        updates = {}
        editor._process_updates(data, updates)
        self.assertEqual(updates["age"], "not-a-number")
        self.assertEqual(updates["joined_date"], "invalid-date")
        self.assertEqual(updates["tags"], ["not-valid-json"])

    def test_edit_with_complex_nested_updates(self):
        doc_id = self.sample_id
        self.collection.find_one.return_value = {
            "_id": ObjectId(doc_id), "name": "Original Name",
            "profile": {"bio": "Original Bio"}, "contact": {"email": "original@example.com"}
        }
        update_result = MagicMock(spec=UpdateResult)
        update_result.modified_count = 1
        self.collection.update_one.return_value = update_result
        request_args = {"action": "edit", "data": {doc_id: {
            "DT_RowId": doc_id, "name": "Updated Name",
            "profile.bio": "Updated Bio", "profile.skills": "[\"Python\", \"MongoDB\"]",
            "contact.email": "updated@example.com", "contact.phone": "987-654-3210"
        }}}
        editor = Editor(self.mongo, 'users', request_args, doc_id=doc_id,
                        data_fields=[DataField('profile.skills', 'array')])
        result = editor.edit()
        self.collection.update_one.assert_called_once()
        args, _ = self.collection.update_one.call_args
        self.assertEqual(args[0]["_id"], ObjectId(doc_id))
        set_updates = args[1]["$set"]
        self.assertEqual(set_updates["name"], "Updated Name")
        self.assertEqual(set_updates["profile.skills"], ["Python", "MongoDB"])
        self.assertIn("data", result)

    def test_create_with_complex_nested_structure(self):
        insert_result = MagicMock(spec=InsertOneResult)
        insert_result.inserted_id = ObjectId()
        self.collection.insert_one.return_value = insert_result
        self.collection.find_one.return_value = {
            "_id": insert_result.inserted_id, "name": "New User",
            "profile": {"bio": "Developer", "skills": ["Python", "MongoDB"]},
            "contact": {"email": "new@example.com", "phone": "123-456-7890"},
            "created_at": datetime(2023, 6, 15, 10, 30, 0)
        }
        request_args = {"action": "create", "data": {"0": {
            "name": "New User", "profile.bio": "Developer",
            "profile.skills": "[\"Python\", \"MongoDB\"]",
            "contact.email": "new@example.com", "contact.phone": "123-456-7890",
            "created_at": "2023-06-15T10:30:00"
        }}}
        editor = Editor(self.mongo, 'users', request_args)
        result = editor.create()
        self.collection.insert_one.assert_called_once()
        args, _ = self.collection.insert_one.call_args
        inserted_doc = args[0]
        self.assertEqual(inserted_doc["name"], "New User")
        self.assertIn("profile", inserted_doc)
        self.assertEqual(inserted_doc["profile"]["bio"], "Developer")
        self.assertIn("data", result)




class TestEditorDataProcessing(unittest.TestCase):
    def setUp(self):
        self.mongo = MagicMock()
        self.mongo.db = MagicMock(spec=Database)
        self.collection = MagicMock(spec=Collection)
        self.mongo.db.__getitem__.return_value = self.collection

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




class TestEditorCoverageGaps(unittest.TestCase):
    def setUp(self):
        self.mongo = MagicMock()
        self.mongo.db = MagicMock(spec=Database)
        self.collection = MagicMock(spec=Collection)
        self.mongo.db.__getitem__.return_value = self.collection

    # Line 203 — db property returns None for unrecognised object
    def test_db_property_returns_none_for_plain_object(self):
        collection = self.collection

        class FakeClient:
            def __getitem__(self, name):
                return collection

        editor = Editor(FakeClient(), 'test', {})
        self.assertIsNone(editor.db)

    # Line 223 — map_ui_field_to_db_field()
    def test_map_ui_field_to_db_field(self):
        data_fields = [DataField('full_name', 'string', alias='name')]
        editor = Editor(self.mongo, 'test', {}, data_fields=data_fields)
        self.assertEqual(editor.map_ui_field_to_db_field('name'), 'full_name')

    # Line 358 — search() skips doc where field value is None
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

    # Lines 376-377 — _coerce_values() number path
    def test_coerce_values_number(self):
        data_fields = [DataField('score', 'number')]
        editor = Editor(self.mongo, 'test', {}, data_fields=data_fields)
        result = editor._coerce_values('score', ['42', '3.14', 'bad'])
        self.assertEqual(result[0], 42)
        self.assertAlmostEqual(result[1], 3.14)
        self.assertEqual(result[2], 'bad')  # fallback

    # Line 381 — _coerce_values() boolean path
    def test_coerce_values_boolean(self):
        data_fields = [DataField('active', 'boolean')]
        editor = Editor(self.mongo, 'test', {}, data_fields=data_fields)
        result = editor._coerce_values('active', ['true', '1', 'false', True])
        self.assertEqual(result, [True, True, False, True])

    # Lines 376-377, 381 via search() values branch (line 348)
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

    # Lines 499-501 — create() wraps PyMongoError
    def test_create_wraps_pymongo_error(self):
        self.collection.insert_one.side_effect = PyMongoError("db error")
        editor = Editor(self.mongo, 'test', {"action": "create", "data": {"0": {"name": "x"}}})
        with self.assertRaises(DatabaseOperationError):
            editor.create()

    # Lines 499-501 — create() wraps unexpected Exception
    def test_create_wraps_unexpected_exception(self):
        self.collection.insert_one.side_effect = RuntimeError("unexpected")
        editor = Editor(self.mongo, 'test', {"action": "create", "data": {"0": {"name": "x"}}})
        with self.assertRaises(DatabaseOperationError):
            editor.create()

    # Line 495 — create() re-raises InvalidDataError
    def test_create_reraises_invalid_data_error(self):
        editor = Editor(self.mongo, 'test', {"action": "create", "data": {"0": {"name": "x"}}})
        with patch('mongo_datatables.editor.crud.preprocess_document', side_effect=InvalidDataError("bad")):
            with self.assertRaises(InvalidDataError):
                editor.create()

    # Lines 545-547 — _preprocess_document date parse fails on dot-key
    def test_preprocess_document_date_parse_failure_on_dot_key(self):
        editor = Editor(self.mongo, 'test', {})
        doc = {"profile.created_at": "not-a-date"}
        processed, dot = editor._preprocess_document(doc)
        # Falls back to raw string in dot_notation_updates
        self.assertEqual(dot["profile.created_at"], "not-a-date")

    # Line 595 — edit() raises InvalidDataError for bad ObjectId during update_one
    def test_edit_raises_for_invalid_objectid_on_update(self):
        doc_id = str(ObjectId())
        self.collection.update_one.side_effect = InvalidId("bad id")
        self.collection.find_one.return_value = {"_id": ObjectId(doc_id), "name": "x"}
        editor = Editor(self.mongo, 'test',
                        {"action": "edit", "data": {doc_id: {"name": "x"}}},
                        doc_id=doc_id)
        with self.assertRaises(InvalidDataError):
            editor.edit()

    # Lines 609, 613-615 — edit() re-raises and wraps exceptions
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
        with self.assertRaises(DatabaseOperationError):
            editor.edit()

    # Line 626 — _process_updates() returns early for non-dict data
    def test_process_updates_non_dict_is_noop(self):
        editor = Editor(self.mongo, 'test', {})
        updates = {}
        editor._process_updates("a string", updates)
        self.assertEqual(updates, {})

    # Line 630 — _process_updates() skips None values
    def test_process_updates_skips_none_values(self):
        editor = Editor(self.mongo, 'test', {})
        updates = {}
        editor._process_updates({"name": None, "age": "30"}, updates)
        self.assertNotIn("name", updates)
        self.assertIn("age", updates)

    # Line 644 — _process_updates() date field value containing 'T'
    def test_process_updates_date_with_T(self):
        data_fields = [DataField('birthday', 'date')]
        editor = Editor(self.mongo, 'test', {}, data_fields=data_fields)
        updates = {}
        editor._process_updates({"birthday": "1993-08-20T00:00:00"}, updates)
        self.assertIsInstance(updates["birthday"], datetime)

    # Line 273 (missed line) + 268->271 (branch) — _format_response_document
    def test_format_response_document_objectid_in_non_id_field(self):
        """Line 273: ObjectId in a non-_id field gets stringified."""
        oid = ObjectId()
        editor = Editor(self.mongo, 'test', {})
        result = editor._format_response_document({"_id": ObjectId(), "ref": oid, "name": "x"})
        self.assertEqual(result["ref"], str(oid))

    def test_format_response_document_no_id_field(self):
        """Branch 268->271: doc with no _id key — skips DT_RowId assignment."""
        editor = Editor(self.mongo, 'test', {})
        result = editor._format_response_document({"name": "x", "score": 1})
        self.assertNotIn("DT_RowId", result)
        self.assertEqual(result["name"], "x")

    # Branch 381 — _coerce_values falls through to return values for non-number/boolean type
    def test_coerce_values_string_type_passthrough(self):
        """Branch 381: field type 'string' — values returned unchanged."""
        data_fields = [DataField('tag', 'string')]
        editor = Editor(self.mongo, 'test', {}, data_fields=data_fields)
        values = ['foo', 'bar']
        self.assertEqual(editor._coerce_values('tag', values), values)

    # Branch 521->532 — _preprocess_document with a non-string value
    def test_preprocess_document_non_string_value(self):
        """Branch 521->532: integer value skips the isinstance(val, str) block."""
        editor = Editor(self.mongo, 'test', {})
        processed, dot = editor._preprocess_document({"count": 42, "name": "Alice"})
        self.assertEqual(processed["count"], 42)

    # Branch 546->520 — FieldMappingError on a date-like key WITHOUT a dot
    def test_preprocess_document_date_parse_failure_no_dot(self):
        """Branch 546->520: FieldMappingError on plain key (not dot-notation) — no fallback write."""
        editor = Editor(self.mongo, 'test', {})
        # 'created_at' ends with 'at' so is_date_field=True; value is unparseable
        processed, dot = editor._preprocess_document({"created_at": "not-a-date"})
        # key has no dot, so the except block does nothing — value stays as-is in processed_doc
        self.assertNotIn("created_at", dot)
        self.assertEqual(processed.get("created_at"), "not-a-date")
