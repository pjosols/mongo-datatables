"""Consolidated editor tests (merged from test_editor*.py files)."""
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


# ---------------------------------------------------------------------------
# test_editor.py — TestEditor
# ---------------------------------------------------------------------------

class TestEditor(unittest.TestCase):
    def setUp(self):
        self.mongo = MagicMock()
        self.mongo.db = MagicMock(spec=Database)
        self.collection = MagicMock(spec=Collection)
        self.mongo.db.__getitem__.return_value = self.collection
        self.sample_id = str(ObjectId())
        self.sample_id2 = str(ObjectId())
        self.create_args = {
            "action": "create",
            "data": {"0": {"name": "John Doe", "email": "john@example.com",
                           "status": "active", "created_at": "2023-01-01T12:00:00"}}
        }
        self.edit_args = {
            "action": "edit",
            "data": {self.sample_id: {"name": "Jane Smith", "status": "inactive"}}
        }
        self.remove_args = {"action": "remove"}
        self.sample_doc = {
            "_id": ObjectId(self.sample_id), "name": "John Doe",
            "email": "john@example.com", "status": "active",
            "created_at": datetime(2023, 1, 1, 12, 0, 0)
        }
        self.updated_doc = {
            "_id": ObjectId(self.sample_id), "name": "Jane Smith",
            "email": "john@example.com", "status": "inactive",
            "created_at": datetime(2023, 1, 1, 12, 0, 0)
        }

    def test_initialization(self):
        editor = Editor(self.mongo, 'users', self.create_args, self.sample_id)
        self.assertEqual(editor.mongo, self.mongo)
        self.assertEqual(editor.collection_name, 'users')
        self.assertEqual(editor.request_args, self.create_args)
        self.assertEqual(editor.doc_id, self.sample_id)

    def test_initialization_with_defaults(self):
        editor = Editor(self.mongo, 'users', None)
        self.assertEqual(editor.request_args, {})
        self.assertEqual(editor.doc_id, "")

    def test_db_property(self):
        editor = Editor(self.mongo, 'users', self.create_args)
        self.assertEqual(editor.db, self.mongo.db)

    def test_collection_property(self):
        editor = Editor(self.mongo, 'users', self.create_args)
        self.assertEqual(editor.collection, self.collection)
        self.mongo.db.__getitem__.assert_called_once_with('users')

    def test_action_property(self):
        editor = Editor(self.mongo, 'users', self.create_args)
        self.assertEqual(editor.action, "create")
        editor = Editor(self.mongo, 'users', {})
        self.assertEqual(editor.action, "")

    def test_data_property(self):
        editor = Editor(self.mongo, 'users', self.create_args)
        self.assertEqual(editor.data, self.create_args["data"])
        editor = Editor(self.mongo, 'users', {})
        self.assertEqual(editor.data, {})

    def test_list_of_ids_property_empty(self):
        editor = Editor(self.mongo, 'users', self.create_args)
        self.assertEqual(editor.list_of_ids, [])

    def test_list_of_ids_property_single(self):
        editor = Editor(self.mongo, 'users', self.create_args, self.sample_id)
        self.assertEqual(editor.list_of_ids, [self.sample_id])

    def test_list_of_ids_property_multiple(self):
        ids = f"{self.sample_id},{self.sample_id2}"
        editor = Editor(self.mongo, 'users', self.create_args, ids)
        self.assertEqual(editor.list_of_ids, [self.sample_id, self.sample_id2])

    def test_preprocess_document(self):
        editor = Editor(self.mongo, 'users', self.create_args)
        doc = {
            "name": "John Doe", "email": "john@example.com",
            "status": None, "tags": '[\"tag1\", \"tag2\"]',
            "created_at": "2023-01-01T12:00:00"
        }
        result = editor._preprocess_document(doc)
        processed = result[0] if isinstance(result, tuple) else result
        self.assertNotIn("status", processed)
        self.assertEqual(processed["tags"], ["tag1", "tag2"])
        self.assertIsInstance(processed["created_at"], datetime)

    def test_format_response_document(self):
        editor = Editor(self.mongo, 'users', self.create_args)
        doc = {
            "_id": ObjectId(self.sample_id), "name": "John Doe",
            "created_at": datetime(2023, 1, 1, 12, 0, 0), "ref_id": ObjectId()
        }
        formatted = editor._format_response_document(doc)
        self.assertIn("DT_RowId", formatted)
        self.assertEqual(formatted["DT_RowId"], self.sample_id)
        self.assertNotIn("_id", formatted)
        self.assertIsInstance(formatted["created_at"], str)
        self.assertIsInstance(formatted["ref_id"], str)

    def test_remove_method_no_id(self):
        editor = Editor(self.mongo, 'users', self.remove_args)
        with self.assertRaises(InvalidDataError):
            editor.remove()

    def test_remove_method_with_id(self):
        editor = Editor(self.mongo, 'users', self.remove_args, self.sample_id)
        delete_result = MagicMock(spec=DeleteResult)
        delete_result.deleted_count = 1
        self.collection.delete_one.return_value = delete_result
        result = editor.remove()
        self.collection.delete_one.assert_called_once_with({"_id": ObjectId(self.sample_id)})
        self.assertEqual(result, {})

    def test_remove_method_with_multiple_ids(self):
        ids = f"{self.sample_id},{self.sample_id2}"
        editor = Editor(self.mongo, 'users', self.remove_args, ids)
        delete_result = MagicMock(spec=DeleteResult)
        delete_result.deleted_count = 1
        self.collection.delete_one.return_value = delete_result
        result = editor.remove()
        expected_calls = [
            unittest.mock.call({"_id": ObjectId(self.sample_id)}),
            unittest.mock.call({"_id": ObjectId(self.sample_id2)})
        ]
        self.collection.delete_one.assert_has_calls(expected_calls)
        self.assertEqual(result, {})

    def test_remove_method_exception(self):
        editor = Editor(self.mongo, 'users', self.remove_args, self.sample_id)
        self.collection.delete_one.side_effect = PyMongoError("Database error")
        with self.assertRaises(DatabaseOperationError) as context:
            editor.remove()
        self.assertIn("Failed to delete documents", str(context.exception))

    def test_create_method_no_data(self):
        editor = Editor(self.mongo, 'users', {"action": "create", "data": {}})
        with self.assertRaises(InvalidDataError):
            editor.create()

    def test_create_method_with_data(self):
        editor = Editor(self.mongo, 'users', self.create_args)
        insert_result = MagicMock(spec=InsertOneResult)
        insert_result.inserted_id = ObjectId(self.sample_id)
        self.collection.insert_one.return_value = insert_result
        self.collection.find_one.return_value = self.sample_doc
        result = editor.create()
        self.collection.insert_one.assert_called_once()
        self.collection.find_one.assert_called_once_with({"_id": ObjectId(self.sample_id)})
        self.assertIn("data", result)
        self.assertEqual(len(result["data"]), 1)
        self.assertEqual(result["data"][0]["DT_RowId"], self.sample_id)

    def test_create_method_exception(self):
        editor = Editor(self.mongo, 'users', self.create_args)
        self.collection.insert_one.side_effect = PyMongoError("Database error")
        with self.assertRaises(DatabaseOperationError) as context:
            editor.create()
        self.assertIn("Failed to create document", str(context.exception))

    def test_edit_method_no_id(self):
        editor = Editor(self.mongo, 'users', self.edit_args)
        with self.assertRaises(InvalidDataError):
            editor.edit()

    def test_edit_method_with_id(self):
        editor = Editor(self.mongo, 'users', self.edit_args, self.sample_id)
        update_result = MagicMock(spec=UpdateResult)
        update_result.modified_count = 1
        self.collection.update_one.return_value = update_result
        self.collection.find_one.return_value = self.updated_doc
        result = editor.edit()
        self.collection.update_one.assert_called_once()
        args, kwargs = self.collection.update_one.call_args
        self.assertEqual(args[0], {"_id": ObjectId(self.sample_id)})
        self.assertIn("$set", args[1])
        self.collection.find_one.assert_called_once_with({"_id": ObjectId(self.sample_id)})
        self.assertIn("data", result)
        self.assertEqual(len(result["data"]), 1)
        self.assertEqual(result["data"][0]["DT_RowId"], self.sample_id)
        self.assertEqual(result["data"][0]["name"], "Jane Smith")

    def test_edit_method_missing_data_for_id(self):
        different_id = str(ObjectId())
        edit_args = {"action": "edit", "data": {different_id: {"name": "Wrong ID"}}}
        editor = Editor(self.mongo, 'users', edit_args, self.sample_id)
        result = editor.edit()
        self.collection.update_one.assert_not_called()
        self.assertEqual(result["data"], [])

    def test_edit_method_exception(self):
        editor = Editor(self.mongo, 'users', self.edit_args, self.sample_id)
        self.collection.update_one.side_effect = PyMongoError("Database error")
        with self.assertRaises(DatabaseOperationError) as context:
            editor.edit()
        self.assertIn("Failed to update documents", str(context.exception))

    def test_process_method_create(self):
        editor = Editor(self.mongo, 'users', self.create_args)
        with patch.object(Editor, 'create', return_value={"data": [{"result": "ok"}]}) as mock_create:
            result = editor.process()
            mock_create.assert_called_once()
            self.assertEqual(result, {"data": [{"result": "ok"}]})

    def test_process_method_edit(self):
        editor = Editor(self.mongo, 'users', self.edit_args, self.sample_id)
        with patch.object(Editor, 'edit', return_value={"data": [{"result": "ok"}]}) as mock_edit:
            result = editor.process()
            mock_edit.assert_called_once()
            self.assertEqual(result, {"data": [{"result": "ok"}]})

    def test_process_method_remove(self):
        editor = Editor(self.mongo, 'users', self.remove_args, self.sample_id)
        with patch.object(Editor, 'remove', return_value={}) as mock_remove:
            result = editor.process()
            mock_remove.assert_called_once()
            self.assertEqual(result, {})

    def test_process_method_unsupported_action(self):
        editor = Editor(self.mongo, 'users', {"action": "invalid"})
        result = editor.process()
        self.assertIn("error", result)
        self.assertIn("Unsupported action", result["error"])

    def test_run_pre_hook_no_hook_returns_true(self):
        editor = Editor(self.mongo, 'users', self.create_args)
        self.assertTrue(editor._run_pre_hook("create", "0", {"name": "x"}))

    def test_run_pre_hook_truthy_proceeds(self):
        hook = MagicMock(return_value=True)
        editor = Editor(self.mongo, 'users', self.create_args, hooks={"pre_create": hook})
        result = editor._run_pre_hook("create", "0", {"name": "x"})
        self.assertTrue(result)
        hook.assert_called_once_with("0", {"name": "x"})

    def test_run_pre_hook_falsy_cancels(self):
        hook = MagicMock(return_value=False)
        editor = Editor(self.mongo, 'users', self.create_args, hooks={"pre_create": hook})
        self.assertFalse(editor._run_pre_hook("create", "0", {"name": "x"}))

    def test_run_pre_hook_none_return_cancels(self):
        hook = MagicMock(return_value=None)
        editor = Editor(self.mongo, 'users', self.create_args, hooks={"pre_create": hook})
        self.assertFalse(editor._run_pre_hook("create", "0", {}))

    def test_create_with_hook_all_proceed(self):
        hook = MagicMock(return_value=True)
        editor = Editor(self.mongo, 'users', self.create_args, hooks={"pre_create": hook})
        insert_result = MagicMock()
        insert_result.inserted_id = ObjectId(self.sample_id)
        self.collection.insert_one.return_value = insert_result
        self.collection.find_one.return_value = self.sample_doc
        result = editor.create()
        self.assertIn("data", result)
        self.assertNotIn("cancelled", result)
        hook.assert_called_once()

    def test_create_with_hook_cancels_row(self):
        hook = MagicMock(return_value=False)
        editor = Editor(self.mongo, 'users', self.create_args, hooks={"pre_create": hook})
        result = editor.create()
        self.collection.insert_one.assert_not_called()
        self.assertIn("cancelled", result)
        self.assertIn("0", result["cancelled"])
        self.assertEqual(result["data"], [])

    def test_create_with_hook_partial_cancel(self):
        multi_create_args = {"action": "create", "data": {"0": {"name": "Alice"}, "1": {"name": "Bob"}}}
        hook = MagicMock(side_effect=lambda row_id, _: row_id == "0")
        editor = Editor(self.mongo, 'users', multi_create_args, hooks={"pre_create": hook})
        insert_result = MagicMock()
        insert_result.inserted_id = ObjectId(self.sample_id)
        self.collection.insert_one.return_value = insert_result
        self.collection.find_one.return_value = self.sample_doc
        result = editor.create()
        self.assertEqual(self.collection.insert_one.call_count, 1)
        self.assertEqual(len(result["data"]), 1)
        self.assertIn("cancelled", result)
        self.assertEqual(result["cancelled"], ["1"])

    def test_edit_with_hook_cancels_row(self):
        hook = MagicMock(return_value=False)
        editor = Editor(self.mongo, 'users', self.edit_args, self.sample_id, hooks={"pre_edit": hook})
        result = editor.edit()
        self.collection.update_one.assert_not_called()
        self.assertIn("cancelled", result)
        self.assertIn(self.sample_id, result["cancelled"])
        self.assertEqual(result["data"], [])

    def test_edit_with_hook_all_proceed(self):
        hook = MagicMock(return_value=True)
        editor = Editor(self.mongo, 'users', self.edit_args, self.sample_id, hooks={"pre_edit": hook})
        update_result = MagicMock()
        update_result.modified_count = 1
        self.collection.update_one.return_value = update_result
        self.collection.find_one.return_value = self.updated_doc
        result = editor.edit()
        self.assertIn("data", result)
        self.assertNotIn("cancelled", result)

    def test_remove_with_hook_cancels_row(self):
        hook = MagicMock(return_value=False)
        editor = Editor(self.mongo, 'users', self.remove_args, self.sample_id, hooks={"pre_remove": hook})
        result = editor.remove()
        self.collection.delete_one.assert_not_called()
        self.assertIn("cancelled", result)
        self.assertIn(self.sample_id, result["cancelled"])

    def test_remove_with_hook_all_proceed(self):
        hook = MagicMock(return_value=True)
        editor = Editor(self.mongo, 'users', self.remove_args, self.sample_id, hooks={"pre_remove": hook})
        delete_result = MagicMock()
        delete_result.deleted_count = 1
        self.collection.delete_one.return_value = delete_result
        result = editor.remove()
        self.collection.delete_one.assert_called_once()
        self.assertEqual(result, {})

    def test_remove_partial_cancel(self):
        ids = f"{self.sample_id},{self.sample_id2}"
        hook = MagicMock(side_effect=lambda doc_id, _: doc_id == self.sample_id)
        editor = Editor(self.mongo, 'users', self.remove_args, ids, hooks={"pre_remove": hook})
        delete_result = MagicMock()
        self.collection.delete_one.return_value = delete_result
        result = editor.remove()
        self.assertEqual(self.collection.delete_one.call_count, 1)
        self.assertIn("cancelled", result)
        self.assertEqual(result["cancelled"], [self.sample_id2])

    def test_hooks_default_empty(self):
        editor = Editor(self.mongo, 'users', self.create_args)
        self.assertEqual(editor.hooks, {})

    def test_hooks_stored_correctly(self):
        hook = MagicMock()
        editor = Editor(self.mongo, 'users', self.create_args, hooks={"pre_create": hook})
        self.assertIs(editor.hooks["pre_create"], hook)


# ---------------------------------------------------------------------------
# test_editor_advanced.py — TestEditorAdvanced
# ---------------------------------------------------------------------------

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


# ---------------------------------------------------------------------------
# test_editor_crud.py — TestEditorCRUD
# ---------------------------------------------------------------------------

class TestEditorCRUD(unittest.TestCase):
    def setUp(self):
        self.mongo = MagicMock()
        self.mongo.db = MagicMock(spec=Database)
        self.collection = MagicMock(spec=Collection)
        self.mongo.db.__getitem__.return_value = self.collection
        self.sample_id = str(ObjectId())
        self.sample_id2 = str(ObjectId())

    def test_create_simple_document(self):
        insert_result = MagicMock(spec=InsertOneResult)
        insert_result.inserted_id = ObjectId()
        self.collection.insert_one.return_value = insert_result
        self.collection.find_one.return_value = {
            "_id": insert_result.inserted_id, "name": "Test User",
            "email": "test@example.com", "age": 30
        }
        editor = Editor(self.mongo, 'users', {
            "action": "create",
            "data": {"0": {"name": "Test User", "email": "test@example.com", "age": "30"}}
        })
        result = editor.create()
        self.collection.insert_one.assert_called_once()
        self.assertIn("data", result)
        self.assertEqual(len(result["data"]), 1)
        self.assertEqual(result["data"][0]["name"], "Test User")

    def test_edit_simple_document(self):
        doc_id = self.sample_id
        self.collection.find_one.return_value = {
            "_id": ObjectId(doc_id), "name": "Original Name",
            "email": "original@example.com", "age": 25
        }
        update_result = MagicMock(spec=UpdateResult)
        update_result.modified_count = 1
        self.collection.update_one.return_value = update_result
        editor = Editor(self.mongo, 'users', {
            "action": "edit",
            "data": {doc_id: {"DT_RowId": doc_id, "name": "Updated Name",
                               "email": "updated@example.com", "age": "35"}}
        }, doc_id=doc_id)
        result = editor.edit()
        self.collection.update_one.assert_called_once()
        args, _ = self.collection.update_one.call_args
        self.assertEqual(args[0]["_id"], ObjectId(doc_id))
        self.assertIn("$set", args[1])
        self.assertEqual(args[1]["$set"]["name"], "Updated Name")

    def test_remove_document(self):
        doc_id = self.sample_id
        delete_result = MagicMock(spec=DeleteResult)
        delete_result.deleted_count = 1
        self.collection.delete_one.return_value = delete_result
        editor = Editor(self.mongo, 'users', {
            "action": "remove",
            "data": {doc_id: {"DT_RowId": doc_id, "id": doc_id}}
        }, doc_id=doc_id)
        result = editor.remove()
        self.collection.delete_one.assert_called_once()
        args, _ = self.collection.delete_one.call_args
        self.assertEqual(args[0]["_id"], ObjectId(doc_id))
        self.assertEqual(result, {})

    def test_batch_edit(self):
        doc_id1, doc_id2 = self.sample_id, self.sample_id2
        doc1 = {"_id": ObjectId(doc_id1), "name": "User 1", "status": "active"}
        doc2 = {"_id": ObjectId(doc_id2), "name": "User 2", "status": "inactive"}

        def mock_find_one(filter_dict):
            if filter_dict["_id"] == ObjectId(doc_id1):
                return doc1
            elif filter_dict["_id"] == ObjectId(doc_id2):
                return doc2
            return None

        self.collection.find_one.side_effect = mock_find_one
        update_result = MagicMock(spec=UpdateResult)
        update_result.modified_count = 1
        self.collection.update_one.return_value = update_result
        editor = Editor(self.mongo, 'users', {
            "action": "edit",
            "data": {
                doc_id1: {"DT_RowId": doc_id1, "status": "approved"},
                doc_id2: {"DT_RowId": doc_id2, "status": "approved"}
            }
        }, doc_id=f"{doc_id1},{doc_id2}")
        result = editor.edit()
        self.assertEqual(self.collection.update_one.call_count, 2)
        self.assertIn("data", result)
        self.assertEqual(len(result["data"]), 2)


# ---------------------------------------------------------------------------
# test_editor_data_processing.py — TestEditorDataProcessing
# ---------------------------------------------------------------------------

class TestEditorDataProcessing(unittest.TestCase):
    def setUp(self):
        self.mongo = MagicMock()
        self.mongo.db = MagicMock(spec=Database)
        self.collection = MagicMock(spec=Collection)
        self.mongo.db.__getitem__.return_value = self.collection

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

    def test_preprocess_document_with_date_fields(self):
        request_args = {"action": "create", "data": {"0": {
            "name": "Test User", "created_at": "2023-01-15T14:30:45",
            "update_date": "2023-02-20", "metadata.last_login_time": "2023-03-10T09:15:30Z"
        }}}
        editor = Editor(self.mongo, 'users', request_args)
        processed_doc, dot_notation = editor._preprocess_document(editor.data["0"])
        self.assertIsInstance(processed_doc["created_at"], datetime)
        self.assertIsInstance(processed_doc["update_date"], datetime)
        self.assertIsInstance(dot_notation["metadata.last_login_time"], datetime)

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


# ---------------------------------------------------------------------------
# test_editor_dependent.py — TestEditorDependent
# ---------------------------------------------------------------------------

def _make_dependent_editor(request_args, dependent_handlers=None):
    mongo = MagicMock()
    mongo.db = MagicMock()
    mongo.db.__getitem__ = MagicMock(return_value=MagicMock())
    return Editor(mongo, "test_collection", request_args, dependent_handlers=dependent_handlers)


class TestEditorDependent(unittest.TestCase):
    def test_dependent_calls_handler_with_field_values_rows(self):
        handler = MagicMock(return_value={"options": {"city": [{"label": "NYC", "value": 1}]}})
        editor = _make_dependent_editor(
            {"action": "dependent", "field": "country", "values": {"country": "US"}, "rows": [{"country": "CA"}]},
            dependent_handlers={"country": handler},
        )
        result = editor.dependent()
        handler.assert_called_once_with("country", {"country": "US"}, [{"country": "CA"}])
        self.assertEqual(result, {"options": {"city": [{"label": "NYC", "value": 1}]}})

    def test_dependent_no_handler_raises_invalid_data_error(self):
        editor = _make_dependent_editor(
            {"action": "dependent", "field": "country", "values": {}, "rows": []},
            dependent_handlers={},
        )
        with self.assertRaises(InvalidDataError):
            editor.dependent()

    def test_dependent_no_handlers_configured_raises(self):
        editor = _make_dependent_editor(
            {"action": "dependent", "field": "country", "values": {}, "rows": []},
        )
        with self.assertRaises(InvalidDataError):
            editor.dependent()

    def test_dependent_via_process_dispatches_correctly(self):
        handler = MagicMock(return_value={"values": {"city": "London"}})
        editor = _make_dependent_editor(
            {"action": "dependent", "field": "country", "values": {"country": "UK"}, "rows": []},
            dependent_handlers={"country": handler},
        )
        result = editor.process()
        self.assertEqual(result, {"values": {"city": "London"}})

    def test_dependent_via_process_unknown_field_returns_error(self):
        editor = _make_dependent_editor(
            {"action": "dependent", "field": "unknown", "values": {}, "rows": []},
            dependent_handlers={"country": MagicMock()},
        )
        result = editor.process()
        self.assertIn("error", result)

    def test_dependent_missing_values_defaults_to_empty_dict(self):
        handler = MagicMock(return_value={})
        editor = _make_dependent_editor(
            {"action": "dependent", "field": "country"},
            dependent_handlers={"country": handler},
        )
        editor.dependent()
        handler.assert_called_once_with("country", {}, [])

    def test_dependent_missing_rows_defaults_to_empty_list(self):
        handler = MagicMock(return_value={})
        editor = _make_dependent_editor(
            {"action": "dependent", "field": "country", "values": {"country": "US"}},
            dependent_handlers={"country": handler},
        )
        editor.dependent()
        handler.assert_called_once_with("country", {"country": "US"}, [])

    def test_dependent_response_can_contain_all_protocol_keys(self):
        full_response = {
            "options": {"city": [{"label": "NYC", "value": 1}]},
            "values": {"zip": "10001"}, "messages": {"city": "Select a city"},
            "errors": {"zip": "Invalid zip"}, "labels": {"city": "City/Town"},
            "show": ["zip"], "hide": "region", "enable": ["city"], "disable": "country",
        }
        handler = MagicMock(return_value=full_response)
        editor = _make_dependent_editor(
            {"action": "dependent", "field": "country", "values": {}, "rows": []},
            dependent_handlers={"country": handler},
        )
        self.assertEqual(editor.dependent(), full_response)

    def test_dependent_multiple_handlers_dispatches_to_correct_one(self):
        handler_a = MagicMock(return_value={"values": {"b": "from_a"}})
        handler_b = MagicMock(return_value={"values": {"a": "from_b"}})
        editor = _make_dependent_editor(
            {"action": "dependent", "field": "field_b", "values": {}, "rows": []},
            dependent_handlers={"field_a": handler_a, "field_b": handler_b},
        )
        result = editor.dependent()
        handler_a.assert_not_called()
        handler_b.assert_called_once()
        self.assertEqual(result, {"values": {"a": "from_b"}})

    def test_dependent_bypasses_validators(self):
        validator_called = []
        def validator(val):
            validator_called.append(True)
            return None
        handler = MagicMock(return_value={})
        editor = _make_dependent_editor(
            {"action": "dependent", "field": "country", "values": {}, "rows": []},
            dependent_handlers={"country": handler},
        )
        editor.validators = {"country": validator}
        editor.process()
        self.assertEqual(validator_called, [])


# ---------------------------------------------------------------------------
# test_editor_gaps.py — TestEditorErrorResponseFormat, TestEditorMultiRowCreate
# ---------------------------------------------------------------------------

class TestEditorErrorResponseFormat(unittest.TestCase):
    def setUp(self):
        self.mongo = MagicMock()
        self.mongo.db = MagicMock(spec=Database)
        self.collection = MagicMock(spec=Collection)
        self.mongo.db.__getitem__.return_value = self.collection

    def test_unsupported_action_returns_error_dict(self):
        editor = Editor(self.mongo, 'users', {'action': 'invalid'})
        result = editor.process()
        self.assertIn('error', result)
        self.assertNotIn('data', result)

    def test_db_error_returns_error_dict(self):
        self.collection.insert_one.side_effect = PyMongoError('db down')
        editor = Editor(self.mongo, 'users', {'action': 'create', 'data': {'0': {'name': 'x'}}})
        result = editor.process()
        self.assertIn('error', result)

    def test_invalid_data_returns_error_dict(self):
        editor = Editor(self.mongo, 'users', {'action': 'create', 'data': {}})
        result = editor.process()
        self.assertIn('error', result)

    def test_invalid_id_on_remove_returns_error_dict(self):
        self.collection.delete_one.side_effect = PyMongoError('fail')
        editor = Editor(self.mongo, 'users', {'action': 'remove'}, doc_id='not-an-objectid')
        result = editor.process()
        self.assertIn('error', result)

    def test_validators_pass_allows_create(self):
        oid = ObjectId()
        insert_result = MagicMock(spec=InsertOneResult)
        insert_result.inserted_id = oid
        self.collection.insert_one.return_value = insert_result
        self.collection.find_one.return_value = {'_id': oid, 'name': 'Alice'}
        editor = Editor(self.mongo, 'users',
                        {'action': 'create', 'data': {'0': {'name': 'Alice'}}},
                        validators={'name': lambda v: None})
        result = editor.process()
        self.assertIn('data', result)
        self.assertNotIn('fieldErrors', result)

    def test_validators_fail_returns_field_errors(self):
        editor = Editor(self.mongo, 'users',
                        {'action': 'create', 'data': {'0': {'name': ''}}},
                        validators={'name': lambda v: 'Name is required' if not v else None})
        result = editor.process()
        self.assertIn('fieldErrors', result)
        self.assertNotIn('data', result)
        self.assertEqual(result['fieldErrors'][0]['name'], 'name')
        self.assertEqual(result['fieldErrors'][0]['status'], 'Name is required')

    def test_validators_multiple_field_errors(self):
        editor = Editor(self.mongo, 'users',
                        {'action': 'create', 'data': {'0': {'name': '', 'email': 'bad'}}},
                        validators={
                            'name': lambda v: 'Required' if not v else None,
                            'email': lambda v: 'Invalid email' if v and '@' not in v else None,
                        })
        result = editor.process()
        self.assertIn('fieldErrors', result)
        self.assertEqual(len(result['fieldErrors']), 2)

    def test_validators_not_run_for_remove(self):
        self.collection.delete_one.return_value = MagicMock()
        doc_id = str(ObjectId())
        editor = Editor(self.mongo, 'users', {'action': 'remove'},
                        doc_id=doc_id, validators={'name': lambda v: 'Required'})
        result = editor.process()
        self.assertNotIn('fieldErrors', result)

    def test_no_validators_no_field_errors(self):
        oid = ObjectId()
        insert_result = MagicMock(spec=InsertOneResult)
        insert_result.inserted_id = oid
        self.collection.insert_one.return_value = insert_result
        self.collection.find_one.return_value = {'_id': oid, 'name': 'Bob'}
        editor = Editor(self.mongo, 'users', {'action': 'create', 'data': {'0': {'name': 'Bob'}}})
        result = editor.process()
        self.assertIn('data', result)
        self.assertNotIn('fieldErrors', result)


class TestEditorMultiRowCreate(unittest.TestCase):
    def setUp(self):
        self.mongo = MagicMock()
        self.mongo.db = MagicMock(spec=Database)
        self.collection = MagicMock(spec=Collection)
        self.mongo.db.__getitem__.return_value = self.collection

    def _make_insert_mock(self, oid):
        m = MagicMock(spec=InsertOneResult)
        m.inserted_id = oid
        return m

    def test_single_row_create_still_works(self):
        oid = ObjectId()
        self.collection.insert_one.return_value = self._make_insert_mock(oid)
        self.collection.find_one.return_value = {'_id': oid, 'name': 'Alice'}
        editor = Editor(self.mongo, 'users', {'action': 'create', 'data': {'0': {'name': 'Alice'}}})
        result = editor.create()
        self.assertEqual(len(result['data']), 1)
        self.collection.insert_one.assert_called_once()

    def test_multi_row_create_inserts_all_rows(self):
        oids = [ObjectId(), ObjectId(), ObjectId()]
        self.collection.insert_one.side_effect = [self._make_insert_mock(o) for o in oids]
        self.collection.find_one.side_effect = [
            {'_id': oids[0], 'name': 'Alice'}, {'_id': oids[1], 'name': 'Bob'},
            {'_id': oids[2], 'name': 'Carol'},
        ]
        editor = Editor(self.mongo, 'users', {
            'action': 'create',
            'data': {'0': {'name': 'Alice'}, '1': {'name': 'Bob'}, '2': {'name': 'Carol'}}
        })
        result = editor.create()
        self.assertEqual(len(result['data']), 3)
        self.assertEqual(self.collection.insert_one.call_count, 3)

    def test_multi_row_create_returns_all_dt_row_ids(self):
        oids = [ObjectId(), ObjectId()]
        self.collection.insert_one.side_effect = [self._make_insert_mock(o) for o in oids]
        self.collection.find_one.side_effect = [
            {'_id': oids[0], 'name': 'Alice'}, {'_id': oids[1], 'name': 'Bob'},
        ]
        editor = Editor(self.mongo, 'users', {
            'action': 'create', 'data': {'0': {'name': 'Alice'}, '1': {'name': 'Bob'}}
        })
        result = editor.create()
        row_ids = [r['DT_RowId'] for r in result['data']]
        self.assertEqual(row_ids, [str(oids[0]), str(oids[1])])

    def test_multi_row_create_sorted_order(self):
        oids = [ObjectId(), ObjectId(), ObjectId()]
        names_inserted = []

        def capture_insert(doc):
            names_inserted.append(doc.get('name'))
            idx = len(names_inserted) - 1
            m = MagicMock(spec=InsertOneResult)
            m.inserted_id = oids[idx]
            return m

        self.collection.insert_one.side_effect = capture_insert
        self.collection.find_one.side_effect = [
            {'_id': oids[0], 'name': 'First'}, {'_id': oids[1], 'name': 'Second'},
            {'_id': oids[2], 'name': 'Third'},
        ]
        editor = Editor(self.mongo, 'users', {
            'action': 'create',
            'data': {'2': {'name': 'Third'}, '0': {'name': 'First'}, '1': {'name': 'Second'}}
        })
        editor.create()
        self.assertEqual(names_inserted, ['First', 'Second', 'Third'])

    def test_empty_data_raises_invalid_data_error(self):
        editor = Editor(self.mongo, 'users', {'action': 'create', 'data': {}})
        with self.assertRaises(InvalidDataError):
            editor.create()

    def test_multi_row_via_process(self):
        oids = [ObjectId(), ObjectId()]
        self.collection.insert_one.side_effect = [self._make_insert_mock(o) for o in oids]
        self.collection.find_one.side_effect = [
            {'_id': oids[0], 'name': 'X'}, {'_id': oids[1], 'name': 'Y'},
        ]
        editor = Editor(self.mongo, 'users', {
            'action': 'create', 'data': {'0': {'name': 'X'}, '1': {'name': 'Y'}}
        })
        result = editor.process()
        self.assertIn('data', result)
        self.assertEqual(len(result['data']), 2)


# ---------------------------------------------------------------------------
# test_editor_nested_data.py — TestEditorNestedData (unique tests only)
# ---------------------------------------------------------------------------

class TestEditorNestedData(unittest.TestCase):
    def setUp(self):
        self.mongo = MagicMock()
        self.mongo.db = MagicMock(spec=Database)
        self.collection = MagicMock(spec=Collection)
        self.mongo.db.__getitem__.return_value = self.collection
        self.sample_id = str(ObjectId())

    def test_preprocess_document_with_nested_fields(self):
        request_args = {"action": "create", "data": {"0": {
            "name": "Test User", "profile.bio": "Developer",
            "profile.skills": "[\"Python\", \"MongoDB\"]",
            "contact.email": "test@example.com", "contact.phone": "123-456-7890"
        }}}
        editor = Editor(self.mongo, 'users', request_args)
        processed_doc, dot_notation = editor._preprocess_document(editor.data["0"])
        self.assertEqual(dot_notation["profile.bio"], "Developer")
        self.assertEqual(dot_notation["profile.skills"], ["Python", "MongoDB"])
        self.assertNotIn("profile.bio", processed_doc)
        self.assertNotIn("contact.email", processed_doc)

    def test_process_updates_with_nested_data(self):
        data_fields = [
            DataField("profile.joined_date", "date"), DataField("stats.visits", "number"),
            DataField("settings.notifications", "boolean"), DataField("tags", "array")
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
        self.assertEqual(updates["stats.visits"], 42)
        self.assertTrue(updates["settings.notifications"])
        self.assertEqual(updates["tags"], ["member", "premium"])

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
        set_updates = args[1]["$set"]
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
        self.assertIn("data", result)


# ---------------------------------------------------------------------------
# test_editor_options.py — TestEditorOptions
# ---------------------------------------------------------------------------

class TestEditorOptions(unittest.TestCase):
    def setUp(self):
        self.mongo = MagicMock()
        self.mongo.db = MagicMock(spec=Database)
        self.collection = MagicMock(spec=Collection)
        self.mongo.db.__getitem__.return_value = self.collection
        oid = ObjectId()
        insert_result = MagicMock(spec=InsertOneResult)
        insert_result.inserted_id = oid
        self.collection.insert_one.return_value = insert_result
        self.collection.find_one.return_value = {'_id': oid, 'name': 'Alice'}
        self.collection.update_one.return_value = MagicMock()
        self.collection.delete_one.return_value = MagicMock()

    def _make_editor(self, request_args, doc_id='', options=None):
        return Editor(self.mongo, 'users', request_args, doc_id=doc_id, options=options)

    def test_no_options_key_absent_by_default(self):
        editor = self._make_editor({'action': 'create', 'data': {'0': {'name': 'Alice'}}})
        result = editor.process()
        self.assertNotIn('options', result)

    def test_options_dict_included_in_create_response(self):
        opts = {'status': [{'label': 'Active', 'value': 1}, {'label': 'Inactive', 'value': 0}]}
        editor = self._make_editor({'action': 'create', 'data': {'0': {'name': 'Alice'}}}, options=opts)
        result = editor.process()
        self.assertEqual(result['options'], opts)

    def test_options_callable_called_and_result_included(self):
        opts = {'city': [{'label': 'Edinburgh', 'value': 1}]}
        editor = self._make_editor({'action': 'create', 'data': {'0': {'name': 'Alice'}}},
                                   options=lambda: opts)
        result = editor.process()
        self.assertEqual(result['options'], opts)

    def test_options_on_edit_action(self):
        oid = str(self.collection.find_one.return_value['_id'])
        opts = {'role': [{'label': 'Admin', 'value': 'admin'}]}
        editor = self._make_editor(
            {'action': 'edit', 'data': {oid: {'name': 'Bob'}}}, doc_id=oid, options=opts)
        result = editor.process()
        self.assertEqual(result['options'], opts)

    def test_options_on_remove_action(self):
        oid = str(ObjectId())
        opts = {'type': [{'label': 'X', 'value': 'x'}]}
        editor = self._make_editor({'action': 'remove'}, doc_id=oid, options=opts)
        result = editor.process()
        self.assertEqual(result['options'], opts)

    def test_options_callable_called_fresh_each_process_call(self):
        call_count = {'n': 0}

        def dynamic_opts():
            call_count['n'] += 1
            return {'field': [{'label': str(call_count['n']), 'value': call_count['n']}]}

        editor = self._make_editor({'action': 'create', 'data': {'0': {'name': 'Alice'}}},
                                   options=dynamic_opts)
        editor.process()
        editor.process()
        self.assertEqual(call_count['n'], 2)


# ---------------------------------------------------------------------------
# test_editor_pymongo_types.py — TestEditorPymongoTypes
# ---------------------------------------------------------------------------

class TestEditorPymongoTypes(unittest.TestCase):
    def _make_collection(self):
        col = MagicMock()
        col.insert_one.return_value = MagicMock(inserted_id="id1")
        col.find_one.return_value = {"_id": "id1", "name": "Test"}
        return col

    def test_flask_pymongo_object(self):
        col = self._make_collection()
        mongo = MagicMock()
        mongo.db.__getitem__ = MagicMock(return_value=col)
        editor = Editor(mongo, "items", {})
        self.assertIs(editor.collection, col)
        mongo.db.__getitem__.assert_called_once_with("items")

    def test_mongo_client_object(self):
        col = self._make_collection()
        client = MagicMock(spec=["get_database"])
        db_mock = MagicMock()
        db_mock.__getitem__ = MagicMock(return_value=col)
        client.get_database.return_value = db_mock
        editor = Editor(client, "items", {})
        self.assertIs(editor.collection, col)
        client.get_database.assert_called_once()

    def test_raw_database_object(self):
        col = self._make_collection()
        db = MagicMock(spec=Database)
        db.__getitem__ = MagicMock(return_value=col)
        editor = Editor(db, "items", {})
        self.assertIs(editor.collection, col)
        db.__getitem__.assert_called_once_with("items")

    def test_dict_style_fallback(self):
        col = self._make_collection()
        obj = {"items": col}
        editor = Editor(obj, "items", {})
        self.assertIs(editor.collection, col)

    def test_db_property_flask_pymongo(self):
        mongo = MagicMock()
        editor = Editor(mongo, "items", {})
        self.assertIs(editor.db, mongo.db)

    def test_db_property_mongo_client(self):
        client = MagicMock(spec=["get_database"])
        client.get_database.return_value = MagicMock(spec=Database)
        editor = Editor(client, "items", {})
        self.assertIs(editor.db, client.get_database())

    def test_db_property_raw_database(self):
        db = MagicMock(spec=Database)
        editor = Editor(db, "items", {})
        self.assertIs(editor.db, db)


# ---------------------------------------------------------------------------
# test_editor_row_metadata.py — pytest-style classes
# ---------------------------------------------------------------------------

def _make_row_metadata_editor(request_args, row_class=None, row_data=None, row_attr=None):
    mongo = MagicMock()
    doc_id = str(ObjectId())
    editor = Editor(mongo, "test", request_args, doc_id=doc_id,
                    row_class=row_class, row_data=row_data, row_attr=row_attr)
    return editor


def _mock_row_metadata_doc(editor):
    oid = ObjectId()
    doc = {"_id": oid, "name": "Alice"}
    editor._collection.find_one.return_value = doc
    editor._collection.insert_one.return_value = MagicMock(inserted_id=oid)
    return oid


class TestEditorRowMetadataAbsent:
    def test_no_row_class_key_absent(self):
        editor = _make_row_metadata_editor({"action": "create", "data": {"0": {"name": "Alice"}}})
        _mock_row_metadata_doc(editor)
        result = editor.create()
        assert "DT_RowClass" not in result["data"][0]

    def test_no_row_data_key_absent(self):
        editor = _make_row_metadata_editor({"action": "create", "data": {"0": {"name": "Alice"}}})
        _mock_row_metadata_doc(editor)
        result = editor.create()
        assert "DT_RowData" not in result["data"][0]

    def test_no_row_attr_key_absent(self):
        editor = _make_row_metadata_editor({"action": "create", "data": {"0": {"name": "Alice"}}})
        _mock_row_metadata_doc(editor)
        result = editor.create()
        assert "DT_RowAttr" not in result["data"][0]


class TestEditorRowClassStatic:
    def test_static_row_class_in_create(self):
        editor = _make_row_metadata_editor(
            {"action": "create", "data": {"0": {"name": "Alice"}}}, row_class="highlight")
        _mock_row_metadata_doc(editor)
        result = editor.create()
        assert result["data"][0]["DT_RowClass"] == "highlight"

    def test_static_row_class_in_edit(self):
        oid = ObjectId()
        doc_id = str(oid)
        mongo = MagicMock()
        editor = Editor(mongo, "test",
                        {"action": "edit", "data": {doc_id: {"name": "Bob"}}},
                        doc_id=doc_id, row_class="active")
        editor._collection.find_one.return_value = {"_id": oid, "name": "Bob"}
        result = editor.edit()
        assert result["data"][0]["DT_RowClass"] == "active"


class TestEditorRowClassCallable:
    def test_callable_row_class_receives_dt_row_id(self):
        received = {}

        def cls_fn(row):
            received.update(row)
            return "computed"

        editor = _make_row_metadata_editor(
            {"action": "create", "data": {"0": {"name": "Alice"}}}, row_class=cls_fn)
        _mock_row_metadata_doc(editor)
        result = editor.create()
        assert "DT_RowId" in received
        assert result["data"][0]["DT_RowClass"] == "computed"

    def test_callable_row_class_return_value_used(self):
        editor = _make_row_metadata_editor(
            {"action": "create", "data": {"0": {"name": "Alice"}}},
            row_class=lambda row: f"row-{row['DT_RowId'][:4]}")
        _mock_row_metadata_doc(editor)
        result = editor.create()
        dt_row_id = result["data"][0]["DT_RowId"]
        assert result["data"][0]["DT_RowClass"] == f"row-{dt_row_id[:4]}"


class TestEditorRowData:
    def test_static_row_data_dict(self):
        editor = _make_row_metadata_editor(
            {"action": "create", "data": {"0": {"name": "Alice"}}},
            row_data={"source": "mongo", "version": 2})
        _mock_row_metadata_doc(editor)
        result = editor.create()
        assert result["data"][0]["DT_RowData"] == {"source": "mongo", "version": 2}

    def test_callable_row_data(self):
        editor = _make_row_metadata_editor(
            {"action": "create", "data": {"0": {"name": "Alice"}}},
            row_data=lambda row: {"id": row["DT_RowId"]})
        _mock_row_metadata_doc(editor)
        result = editor.create()
        dt_row_id = result["data"][0]["DT_RowId"]
        assert result["data"][0]["DT_RowData"] == {"id": dt_row_id}


class TestEditorRowAttr:
    def test_static_row_attr_dict(self):
        editor = _make_row_metadata_editor(
            {"action": "create", "data": {"0": {"name": "Alice"}}},
            row_attr={"data-type": "record", "tabindex": "0"})
        _mock_row_metadata_doc(editor)
        result = editor.create()
        assert result["data"][0]["DT_RowAttr"] == {"data-type": "record", "tabindex": "0"}


class TestEditorRowMetadataAllThree:
    def test_all_three_combined(self):
        editor = _make_row_metadata_editor(
            {"action": "create", "data": {"0": {"name": "Alice"}}},
            row_class="highlight", row_data={"pkey": 1}, row_attr={"data-id": "x"})
        _mock_row_metadata_doc(editor)
        result = editor.create()
        row = result["data"][0]
        assert row["DT_RowClass"] == "highlight"
        assert row["DT_RowData"] == {"pkey": 1}
        assert row["DT_RowAttr"] == {"data-id": "x"}


# ---------------------------------------------------------------------------
# test_editor_search_action.py — TestEditorSearchAction
# ---------------------------------------------------------------------------

def _make_search_editor(request_args):
    mongo = MagicMock()
    mongo.db = MagicMock(spec=Database)
    collection = MagicMock(spec=Collection)
    mongo.db.__getitem__.return_value = collection
    editor = Editor(mongo, "countries", request_args)
    return editor, collection


class TestEditorSearchAction(unittest.TestCase):
    def test_search_by_term_returns_matching_labels(self):
        editor, col = _make_search_editor({"action": "search", "field": "country", "search": "u"})
        col.find.return_value.limit.return_value = [{"country": "Uganda"}, {"country": "Ukraine"}]
        result = editor.search()
        self.assertEqual(result["data"], [{"label": "Uganda", "value": "Uganda"},
                                          {"label": "Ukraine", "value": "Ukraine"}])

    def test_search_by_values_returns_exact_matches(self):
        editor, col = _make_search_editor({"action": "search", "field": "country",
                                           "values": ["Uganda", "Ukraine"]})
        col.find.return_value.limit.return_value = [{"country": "Uganda"}, {"country": "Ukraine"}]
        result = editor.search()
        self.assertEqual(len(result["data"]), 2)
        self.assertEqual(result["data"][0]["value"], "Uganda")

    def test_search_unknown_field_returns_empty(self):
        editor, col = _make_search_editor({"action": "search", "field": "nonexistent", "search": "x"})
        col.find.return_value.limit.return_value = []
        result = editor.search()
        self.assertEqual(result["data"], [])

    def test_search_empty_term_returns_results(self):
        editor, col = _make_search_editor({"action": "search", "field": "country", "search": ""})
        col.find.return_value.limit.return_value = [{"country": "France"}, {"country": "Germany"}]
        result = editor.search()
        self.assertEqual(len(result["data"]), 2)

    def test_search_deduplicates_values(self):
        editor, col = _make_search_editor({"action": "search", "field": "country", "search": "u"})
        col.find.return_value.limit.return_value = [
            {"country": "Uganda"}, {"country": "Uganda"}, {"country": "Ukraine"}]
        result = editor.search()
        values = [d["value"] for d in result["data"]]
        self.assertEqual(values, ["Uganda", "Ukraine"])

    def test_search_no_params_returns_empty(self):
        editor, col = _make_search_editor({"action": "search", "field": "country"})
        result = editor.search()
        self.assertEqual(result, {"data": []})
        col.find.assert_not_called()

    def test_search_registered_in_process(self):
        editor, col = _make_search_editor({"action": "search", "field": "country", "search": "u"})
        with patch.object(Editor, "search", return_value={"data": []}) as mock_search:
            result = editor.process()
            mock_search.assert_called_once()
            self.assertEqual(result, {"data": []})

    def test_search_by_values_coerces_number_type(self):
        """values list with string numbers should be coerced to int for number fields."""

        mongo = MagicMock()
        mongo.db = MagicMock(spec=Database)
        col = MagicMock(spec=Collection)
        mongo.db.__getitem__.return_value = col
        col.find.return_value.limit.return_value = [{"_id": "x", "age": 30}]
        editor = Editor(mongo, "items", {"action": "search", "field": "age", "values": ["30"]},
                        data_fields=[DataField("age", "number")])
        result = editor.search()
        col.find.assert_called_once_with({"age": {"$in": [30]}}, {"age": 1})
        assert result == {"data": [{"label": "30", "value": 30}]}

    def test_search_by_values_coerces_boolean_type(self):
        """values list with string booleans should be coerced to bool for boolean fields."""

        mongo = MagicMock()
        mongo.db = MagicMock(spec=Database)
        col = MagicMock(spec=Collection)
        mongo.db.__getitem__.return_value = col
        col.find.return_value.limit.return_value = [{"_id": "x", "active": True}]
        editor = Editor(mongo, "items", {"action": "search", "field": "active", "values": ["true"]},
                        data_fields=[DataField("active", "boolean")])
        result = editor.search()
        col.find.assert_called_once_with({"active": {"$in": [True]}}, {"active": 1})
        assert result == {"data": [{"label": "True", "value": True}]}

    def test_search_by_values_no_field_type_passes_through(self):
        """values without a declared field type are passed through unchanged."""
        editor, col = _make_search_editor({"action": "search", "field": "name", "values": ["Alice"]})
        col.find.return_value.limit.return_value = [{"_id": "x", "name": "Alice"}]
        result = editor.search()
        col.find.assert_called_once_with({"name": {"$in": ["Alice"]}}, {"name": 1})
        assert result == {"data": [{"label": "Alice", "value": "Alice"}]}

    def test_search_value_preserves_original_type_in_response(self):
        """value in response dict should be the original MongoDB type, not a string."""
        editor, col = _make_search_editor({"action": "search", "field": "score", "search": "42"})
        col.find.return_value.limit.return_value = [{"_id": "x", "score": 42}]
        result = editor.search()
        assert result["data"][0]["value"] == 42
        assert isinstance(result["data"][0]["value"], int)


# ---------------------------------------------------------------------------
# test_editor_upload.py — TestEditorUpload, TestEditorFilesInResponse
# ---------------------------------------------------------------------------

class TestEditorUpload(unittest.TestCase):
    def setUp(self):
        self.mongo = MagicMock()
        self.mongo.db = MagicMock(spec=Database)
        self.collection = MagicMock(spec=Collection)
        self.mongo.db.__getitem__.return_value = self.collection

    def _make_editor(self, request_args, storage=None):
        return Editor(self.mongo, 'files', request_args, storage_adapter=storage)

    def test_storage_adapter_interface(self):
        adapter = StorageAdapter()
        with self.assertRaises(NotImplementedError):
            adapter.store('avatar', 'photo.png', 'image/png', b'data')

    def test_storage_adapter_retrieve_not_implemented(self):
        adapter = StorageAdapter()
        with self.assertRaises(NotImplementedError):
            adapter.retrieve('some-id')

    def test_upload_no_adapter_returns_error(self):
        editor = self._make_editor({'action': 'upload', 'uploadField': 'avatar'}, storage=None)
        result = editor.process()
        self.assertIn('error', result)

    def test_upload_no_field_returns_error(self):
        adapter = MagicMock()
        editor = self._make_editor({'action': 'upload'}, storage=adapter)
        result = editor.process()
        self.assertIn('error', result)

    def test_upload_no_file_returns_error(self):
        adapter = MagicMock()
        editor = self._make_editor({'action': 'upload', 'uploadField': 'avatar', 'upload': None},
                                   storage=adapter)
        result = editor.process()
        self.assertIn('error', result)

    def test_upload_calls_adapter_store(self):
        adapter = MagicMock(spec=StorageAdapter)
        adapter.store.return_value = 'file-id-123'
        file_data = {'filename': 'photo.png', 'content_type': 'image/png', 'data': b'imgdata'}
        editor = self._make_editor({'action': 'upload', 'uploadField': 'avatar', 'upload': file_data},
                                   storage=adapter)
        result = editor.upload()
        adapter.store.assert_called_once_with('avatar', 'photo.png', 'image/png', b'imgdata')
        self.assertEqual(result, {'upload': {'id': 'file-id-123'}, 'files': {}})

    def test_upload_adapter_store_exception_returns_error(self):
        adapter = MagicMock()
        adapter.store.side_effect = Exception('storage failure')
        file_data = {'filename': 'x.pdf', 'content_type': 'application/pdf', 'data': b'pdf'}
        editor = self._make_editor({'action': 'upload', 'uploadField': 'doc', 'upload': file_data},
                                   storage=adapter)
        result = editor.process()
        self.assertIn('error', result)

    def test_upload_via_process_dispatch(self):
        adapter = MagicMock()
        adapter.store.return_value = 'abc'
        file_data = {'filename': 'f.jpg', 'content_type': 'image/jpeg', 'data': b'jpg'}
        editor = self._make_editor({'action': 'upload', 'uploadField': 'img', 'upload': file_data},
                                   storage=adapter)
        result = editor.process()
        self.assertIn('upload', result)
        self.assertEqual(result['upload']['id'], 'abc')

    def test_upload_files_dict_from_adapter(self):
        class RichAdapter:
            def store(self, field, filename, content_type, data): return 'rich-id'
            def retrieve(self, file_id): raise NotImplementedError
            def files_for_field(self, field):
                return {'rich-id': {'filename': 'photo.png', 'web_path': '/uploads/photo.png'}}

        file_data = {'filename': 'photo.png', 'content_type': 'image/png', 'data': b'x'}
        editor = Editor(self.mongo, 'files', {'action': 'upload', 'uploadField': 'avatar',
                                               'upload': file_data}, storage_adapter=RichAdapter())
        result = editor.upload()
        self.assertEqual(result['upload']['id'], 'rich-id')
        self.assertIn('avatar', result['files'])
        self.assertIn('rich-id', result['files']['avatar'])

    def test_existing_actions_unaffected(self):
        oid = ObjectId()
        insert_result = MagicMock(spec=InsertOneResult)
        insert_result.inserted_id = oid
        self.collection.insert_one.return_value = insert_result
        self.collection.find_one.return_value = {'_id': oid, 'name': 'Test'}
        editor = Editor(self.mongo, 'users',
                        {'action': 'create', 'data': {'0': {'name': 'Test'}}},
                        storage_adapter=None)
        result = editor.process()
        self.assertIn('data', result)


@pytest.fixture
def mock_collection_upload():
    return MagicMock()


class TestEditorFilesInResponse:
    def _make_adapter_with_files(self, files_data):
        class Adapter(StorageAdapter):
            def store(self, field, filename, content_type, data): return "1"
            def retrieve(self, file_id): return b""
            def files_for_field(self, field): return files_data.get(field, {})
        return Adapter()

    def test_create_includes_files_when_file_fields_configured(self, mock_collection_upload):
        files_data = {"photo": {"1": {"filename": "a.png", "web_path": "/uploads/a.png"}}}
        adapter = self._make_adapter_with_files(files_data)
        mock_collection_upload.insert_one.return_value = MagicMock(inserted_id=ObjectId())
        mock_collection_upload.find_one.return_value = {"_id": ObjectId(), "name": "Alice", "photo": "1"}
        editor = Editor(MagicMock(), "col",
                        {"action": "create", "data": {"0": {"name": "Alice", "photo": "1"}}},
                        storage_adapter=adapter, file_fields=["photo"])
        editor._collection = mock_collection_upload
        result = editor.create()
        assert "files" in result
        assert result["files"]["photo"] == {"1": {"filename": "a.png", "web_path": "/uploads/a.png"}}

    def test_edit_includes_files_when_file_fields_configured(self, mock_collection_upload):
        doc_id = str(ObjectId())
        files_data = {"photo": {"1": {"filename": "b.png", "web_path": "/uploads/b.png"}}}
        adapter = self._make_adapter_with_files(files_data)
        mock_collection_upload.update_one.return_value = MagicMock()
        mock_collection_upload.find_one.return_value = {"_id": ObjectId(doc_id), "name": "Bob", "photo": "1"}
        editor = Editor(MagicMock(), "col",
                        {"action": "edit", "data": {doc_id: {"name": "Bob", "photo": "1"}}},
                        doc_id=doc_id, storage_adapter=adapter, file_fields=["photo"])
        editor._collection = mock_collection_upload
        result = editor.edit()
        assert "files" in result
        assert result["files"]["photo"] == {"1": {"filename": "b.png", "web_path": "/uploads/b.png"}}

    def test_create_no_files_without_file_fields(self, mock_collection_upload):
        adapter = self._make_adapter_with_files({"photo": {"1": {}}})
        mock_collection_upload.insert_one.return_value = MagicMock(inserted_id=ObjectId())
        mock_collection_upload.find_one.return_value = {"_id": ObjectId(), "name": "Alice"}
        editor = Editor(MagicMock(), "col",
                        {"action": "create", "data": {"0": {"name": "Alice"}}},
                        storage_adapter=adapter)
        editor._collection = mock_collection_upload
        result = editor.create()
        assert "files" not in result

    def test_create_no_files_without_adapter(self, mock_collection_upload):
        mock_collection_upload.insert_one.return_value = MagicMock(inserted_id=ObjectId())
        mock_collection_upload.find_one.return_value = {"_id": ObjectId(), "name": "Alice"}
        editor = Editor(MagicMock(), "col",
                        {"action": "create", "data": {"0": {"name": "Alice"}}},
                        file_fields=["photo"])
        editor._collection = mock_collection_upload
        result = editor.create()
        assert "files" not in result

    def test_create_no_files_when_adapter_lacks_files_for_field(self, mock_collection_upload):
        class BasicAdapter(StorageAdapter):
            def store(self, field, filename, content_type, data): return "1"
            def retrieve(self, file_id): return b""
        mock_collection_upload.insert_one.return_value = MagicMock(inserted_id=ObjectId())
        mock_collection_upload.find_one.return_value = {"_id": ObjectId(), "name": "Alice"}
        editor = Editor(MagicMock(), "col",
                        {"action": "create", "data": {"0": {"name": "Alice"}}},
                        storage_adapter=BasicAdapter(), file_fields=["photo"])
        editor._collection = mock_collection_upload
        result = editor.create()
        assert "files" not in result

    def test_files_empty_when_no_files_stored(self, mock_collection_upload):
        adapter = self._make_adapter_with_files({})
        mock_collection_upload.insert_one.return_value = MagicMock(inserted_id=ObjectId())
        mock_collection_upload.find_one.return_value = {"_id": ObjectId(), "name": "Alice"}
        editor = Editor(MagicMock(), "col",
                        {"action": "create", "data": {"0": {"name": "Alice"}}},
                        storage_adapter=adapter, file_fields=["photo"])
        editor._collection = mock_collection_upload
        result = editor.create()
        assert "files" not in result

    def test_multiple_file_fields(self, mock_collection_upload):
        files_data = {
            "photo": {"1": {"web_path": "/uploads/1.png"}},
            "doc": {"2": {"web_path": "/uploads/2.pdf"}},
        }
        adapter = self._make_adapter_with_files(files_data)
        mock_collection_upload.insert_one.return_value = MagicMock(inserted_id=ObjectId())
        mock_collection_upload.find_one.return_value = {"_id": ObjectId(), "photo": "1", "doc": "2"}
        editor = Editor(MagicMock(), "col",
                        {"action": "create", "data": {"0": {"photo": "1", "doc": "2"}}},
                        storage_adapter=adapter, file_fields=["photo", "doc"])
        editor._collection = mock_collection_upload
        result = editor.create()
        assert result["files"]["photo"] == {"1": {"web_path": "/uploads/1.png"}}
        assert result["files"]["doc"] == {"2": {"web_path": "/uploads/2.pdf"}}


# ---------------------------------------------------------------------------
# Editor protocol fixes (merged from test_editor_protocol_fixes.py)
# ---------------------------------------------------------------------------

class TestListOfIdsWhitespaceStripping:
    """list_of_ids must strip whitespace from comma-separated IDs."""

    def test_strips_spaces_around_ids(self):
        mongo = MagicMock()
        mongo.db = MagicMock(spec=Database)
        oid = str(ObjectId())
        editor = Editor(mongo, 'c', {}, f" {oid} ")
        assert editor.list_of_ids == [oid]

    def test_strips_spaces_in_multi_id_string(self):
        mongo = MagicMock()
        mongo.db = MagicMock(spec=Database)
        oid1, oid2 = str(ObjectId()), str(ObjectId())
        editor = Editor(mongo, 'c', {}, f"{oid1} , {oid2}")
        assert editor.list_of_ids == [oid1, oid2]

    def test_filters_empty_segments(self):
        mongo = MagicMock()
        mongo.db = MagicMock(spec=Database)
        oid = str(ObjectId())
        editor = Editor(mongo, 'c', {}, f"{oid},")
        assert editor.list_of_ids == [oid]

    def test_empty_doc_id_returns_empty_list(self):
        mongo = MagicMock()
        mongo.db = MagicMock(spec=Database)
        assert Editor(mongo, 'c', {}, "").list_of_ids == []

    def test_whitespace_only_doc_id_returns_empty_list(self):
        mongo = MagicMock()
        mongo.db = MagicMock(spec=Database)
        assert Editor(mongo, 'c', {}, "   ").list_of_ids == []

    def test_remove_uses_stripped_ids(self):
        mongo = MagicMock()
        mongo.db = MagicMock(spec=Database)
        col = MagicMock(spec=Collection)
        mongo.db.__getitem__.return_value = col
        oid = str(ObjectId())
        col.delete_one.return_value = MagicMock(spec=DeleteResult)
        editor = Editor(mongo, 'c', {'action': 'remove'}, f" {oid} ")
        result = editor.remove()
        col.delete_one.assert_called_once_with({'_id': ObjectId(oid)})
        assert result == {}


class TestUploadHasattr:
    """upload() must use hasattr so inherited files_for_field methods work."""

    def test_inherited_files_for_field_is_called(self):
        mongo = MagicMock()
        mongo.db = MagicMock(spec=Database)

        class BaseAdapter(StorageAdapter):
            def store(self, field, filename, content_type, data):
                return 'file-1'
            def files_for_field(self, field):
                return {'file-1': {'filename': 'test.png'}}

        class ConcreteAdapter(BaseAdapter):
            pass

        adapter = ConcreteAdapter()
        editor = Editor(mongo, 'c', {
            'action': 'upload',
            'uploadField': 'avatar',
            'upload': {'filename': 'test.png', 'content_type': 'image/png', 'data': b'bytes'}
        }, storage_adapter=adapter)
        result = editor.upload()
        assert result['upload']['id'] == 'file-1'
        assert result['files']['avatar'] == {'file-1': {'filename': 'test.png'}}

    def test_adapter_without_files_for_field_returns_empty_files(self):
        mongo = MagicMock()
        mongo.db = MagicMock(spec=Database)

        class MinimalAdapter(StorageAdapter):
            def store(self, field, filename, content_type, data):
                return 'file-2'

        adapter = MinimalAdapter()
        editor = Editor(mongo, 'c', {
            'action': 'upload',
            'uploadField': 'doc',
            'upload': {'filename': 'doc.pdf', 'content_type': 'application/pdf', 'data': b'pdf'}
        }, storage_adapter=adapter)
        result = editor.upload()
        assert result['upload']['id'] == 'file-2'
        assert result['files'] == {}


class TestEditValidatorScope:
    """process() must only validate rows in list_of_ids for edit actions."""

    def test_only_validates_rows_being_edited(self):
        mongo = MagicMock()
        mongo.db = MagicMock(spec=Database)
        col = MagicMock(spec=Collection)
        mongo.db.__getitem__.return_value = col
        oid1, oid2 = str(ObjectId()), str(ObjectId())
        validated_fields = []

        def name_validator(value):
            validated_fields.append(value)
            return None

        col.update_one.return_value = MagicMock(spec=UpdateResult)
        col.find_one.return_value = {'_id': ObjectId(oid1), 'name': 'Alice'}

        editor = Editor(mongo, 'c', {
            'action': 'edit',
            'data': {oid1: {'name': 'Alice'}, oid2: {'name': 'Bob'}}
        }, doc_id=oid1, validators={'name': name_validator})
        editor.process()
        assert validated_fields == ['Alice']

    def test_edit_validator_error_only_for_edited_rows(self):
        mongo = MagicMock()
        mongo.db = MagicMock(spec=Database)
        col = MagicMock(spec=Collection)
        mongo.db.__getitem__.return_value = col
        oid1, oid2 = str(ObjectId()), str(ObjectId())

        def strict_validator(value):
            return 'Value is invalid' if value == 'invalid' else None

        editor = Editor(mongo, 'c', {
            'action': 'edit',
            'data': {oid1: {'name': 'valid'}, oid2: {'name': 'invalid'}}
        }, doc_id=oid1, validators={'name': strict_validator})
        col.update_one.return_value = MagicMock(spec=UpdateResult)
        col.find_one.return_value = {'_id': ObjectId(oid1), 'name': 'valid'}
        result = editor.process()
        assert 'fieldErrors' not in result
        assert 'data' in result

    def test_create_validates_all_rows(self):
        mongo = MagicMock()
        mongo.db = MagicMock(spec=Database)
        col = MagicMock(spec=Collection)
        mongo.db.__getitem__.return_value = col
        validated_fields = []

        def name_validator(value):
            validated_fields.append(value)
            return None

        insert_result = MagicMock()
        insert_result.inserted_id = ObjectId()
        col.insert_one.return_value = insert_result
        col.find_one.return_value = {'_id': insert_result.inserted_id, 'name': 'Alice'}

        editor = Editor(mongo, 'c', {
            'action': 'create',
            'data': {'0': {'name': 'Alice'}, '1': {'name': 'Bob'}}
        }, validators={'name': name_validator})
        editor.process()
        assert sorted(validated_fields) == ['Alice', 'Bob']


# ---------------------------------------------------------------------------
# Gap 1 — DT_RowId / DT_Row* keys must not leak into MongoDB $set on edit
# ---------------------------------------------------------------------------

class TestEditDTRowFieldsStripped(unittest.TestCase):
    """DT_Row* keys submitted by the Editor client must never reach MongoDB."""

    def setUp(self):
        self.mongo = MagicMock()
        self.mongo.db = MagicMock(spec=Database)
        self.collection = MagicMock(spec=Collection)
        self.mongo.db.__getitem__.return_value = self.collection
        self.doc_id = str(ObjectId())
        self.collection.update_one.return_value = MagicMock(spec=UpdateResult)
        self.collection.find_one.return_value = {
            "_id": ObjectId(self.doc_id), "name": "Alice"
        }

    def _edit(self, row_data):
        editor = Editor(
            self.mongo, "users",
            {"action": "edit", "data": {self.doc_id: row_data}},
            doc_id=self.doc_id,
        )
        return editor.edit()

    def _set_keys(self):
        args, _ = self.collection.update_one.call_args
        return set(args[1]["$set"].keys())

    def test_dt_row_id_not_in_set(self):
        self._edit({"DT_RowId": self.doc_id, "name": "Alice"})
        self.assertNotIn("DT_RowId", self._set_keys())

    def test_dt_row_class_not_in_set(self):
        self._edit({"DT_RowClass": "highlight", "name": "Alice"})
        self.assertNotIn("DT_RowClass", self._set_keys())

    def test_dt_row_data_not_in_set(self):
        self._edit({"DT_RowData": {"pkey": 1}, "name": "Alice"})
        self.assertNotIn("DT_RowData", self._set_keys())

    def test_dt_row_attr_not_in_set(self):
        self._edit({"DT_RowAttr": {"data-id": "x"}, "name": "Alice"})
        self.assertNotIn("DT_RowAttr", self._set_keys())

    def test_normal_fields_still_in_set(self):
        self._edit({"DT_RowId": self.doc_id, "name": "Alice", "status": "active"})
        keys = self._set_keys()
        self.assertIn("name", keys)
        self.assertIn("status", keys)

    def test_all_dt_row_variants_stripped(self):
        self._edit({
            "DT_RowId": self.doc_id,
            "DT_RowClass": "x",
            "DT_RowData": {},
            "DT_RowAttr": {},
            "name": "Alice",
        })
        keys = self._set_keys()
        self.assertFalse(any(k.startswith("DT_Row") for k in keys))
        self.assertIn("name", keys)

    def test_no_update_when_only_dt_row_fields(self):
        """If the client only sends DT_Row* fields, no $set should be issued."""
        self._edit({"DT_RowId": self.doc_id})
        self.collection.update_one.assert_not_called()

    def test_pre_hook_receives_stripped_data(self):
        """pre_edit hook should not see DT_Row* keys."""
        received = {}
        hook = MagicMock(side_effect=lambda row_id, row_data: received.update(row_data) or True)
        editor = Editor(
            self.mongo, "users",
            {"action": "edit", "data": {self.doc_id: {"DT_RowId": self.doc_id, "name": "Alice"}}},
            doc_id=self.doc_id,
            hooks={"pre_edit": hook},
        )
        editor.edit()
        self.assertNotIn("DT_RowId", received)
        self.assertIn("name", received)


# ---------------------------------------------------------------------------
# Coverage gap tests
# ---------------------------------------------------------------------------

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
        with patch.object(editor, '_preprocess_document', side_effect=InvalidDataError("bad")):
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
