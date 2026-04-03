"""Editor tests — core Editor class — init, CRUD, hooks."""
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


