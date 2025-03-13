import unittest
from unittest.mock import MagicMock, patch
import json
from datetime import datetime
from bson.objectid import ObjectId
from pymongo.collection import Collection
from pymongo.database import Database
from pymongo.results import InsertOneResult, UpdateResult, DeleteResult

from mongo_datatables import Editor


class TestEditor(unittest.TestCase):
    """Test cases for the Editor class"""

    def setUp(self):
        """Set up test fixtures before each test method"""
        # Create a mock PyMongo object
        self.mongo = MagicMock()
        self.mongo.db = MagicMock(spec=Database)
        self.collection = MagicMock(spec=Collection)
        self.mongo.db.__getitem__.return_value = self.collection

        # Sample document IDs
        self.sample_id = str(ObjectId())
        self.sample_id2 = str(ObjectId())

        # Sample request args for different operations
        self.create_args = {
            "action": "create",
            "data": {
                "0": {
                    "name": "John Doe",
                    "email": "john@example.com",
                    "status": "active",
                    "created_at": "2023-01-01T12:00:00"
                }
            }
        }

        self.edit_args = {
            "action": "edit",
            "data": {
                self.sample_id: {
                    "name": "Jane Smith",
                    "status": "inactive"
                }
            }
        }

        self.remove_args = {
            "action": "remove"
        }

        # Sample documents for mock responses
        self.sample_doc = {
            "_id": ObjectId(self.sample_id),
            "name": "John Doe",
            "email": "john@example.com",
            "status": "active",
            "created_at": datetime(2023, 1, 1, 12, 0, 0)
        }

        self.updated_doc = {
            "_id": ObjectId(self.sample_id),
            "name": "Jane Smith",
            "email": "john@example.com",
            "status": "inactive",
            "created_at": datetime(2023, 1, 1, 12, 0, 0)
        }

    def test_initialization(self):
        """Test initialization of Editor class"""
        editor = Editor(self.mongo, 'users', self.create_args, self.sample_id)

        self.assertEqual(editor.mongo, self.mongo)
        self.assertEqual(editor.collection_name, 'users')
        self.assertEqual(editor.request_args, self.create_args)
        self.assertEqual(editor.doc_id, self.sample_id)

    def test_initialization_with_defaults(self):
        """Test initialization with default values"""
        editor = Editor(self.mongo, 'users', None)

        self.assertEqual(editor.request_args, {})
        self.assertEqual(editor.doc_id, "")

    def test_db_property(self):
        """Test the db property"""
        editor = Editor(self.mongo, 'users', self.create_args)
        self.assertEqual(editor.db, self.mongo.db)

    def test_collection_property(self):
        """Test the collection property"""
        editor = Editor(self.mongo, 'users', self.create_args)
        self.assertEqual(editor.collection, self.collection)
        self.mongo.db.__getitem__.assert_called_once_with('users')

    def test_action_property(self):
        """Test action property"""
        editor = Editor(self.mongo, 'users', self.create_args)
        self.assertEqual(editor.action, "create")

        # Test with empty request
        editor = Editor(self.mongo, 'users', {})
        self.assertEqual(editor.action, "")

    def test_data_property(self):
        """Test data property"""
        editor = Editor(self.mongo, 'users', self.create_args)
        self.assertEqual(editor.data, self.create_args["data"])

        # Test with empty request
        editor = Editor(self.mongo, 'users', {})
        self.assertEqual(editor.data, {})

    def test_list_of_ids_property_empty(self):
        """Test list_of_ids property with empty doc_id"""
        editor = Editor(self.mongo, 'users', self.create_args)
        self.assertEqual(editor.list_of_ids, [])

    def test_list_of_ids_property_single(self):
        """Test list_of_ids property with a single ID"""
        editor = Editor(self.mongo, 'users', self.create_args, self.sample_id)
        self.assertEqual(editor.list_of_ids, [self.sample_id])

    def test_list_of_ids_property_multiple(self):
        """Test list_of_ids property with multiple IDs"""
        ids = f"{self.sample_id},{self.sample_id2}"
        editor = Editor(self.mongo, 'users', self.create_args, ids)
        self.assertEqual(editor.list_of_ids, [self.sample_id, self.sample_id2])

    def test_preprocess_document(self):
        """Test _preprocess_document method"""
        editor = Editor(self.mongo, 'users', self.create_args)

        # Test with basic data
        doc = {
            "name": "John Doe",
            "email": "john@example.com",
            "status": None,  # Should be removed
            "tags": '[\"tag1\", \"tag2\"]',  # Should be converted to list
            "created_at": "2023-01-01T12:00:00"  # Should be converted to datetime
        }

        # Call the preprocess method and examine the result
        result = editor._preprocess_document(doc)

        # Check if result is a tuple (seems like the method now returns a tuple)
        if isinstance(result, tuple):
            # If it's a tuple, the processed document is likely the first element
            processed = result[0]
        else:
            # Otherwise use the result directly
            processed = result

        # Check empty values are removed
        self.assertNotIn("status", processed)

        # Check JSON is parsed
        self.assertEqual(processed["tags"], ["tag1", "tag2"])

        # Check date parsing
        self.assertIsInstance(processed["created_at"], datetime)

    def test_format_response_document(self):
        """Test _format_response_document method"""
        editor = Editor(self.mongo, 'users', self.create_args)

        # Test with MongoDB document
        doc = {
            "_id": ObjectId(self.sample_id),
            "name": "John Doe",
            "created_at": datetime(2023, 1, 1, 12, 0, 0),
            "ref_id": ObjectId()
        }

        formatted = editor._format_response_document(doc)

        # Check _id is transformed to DT_RowId
        self.assertIn("DT_RowId", formatted)
        self.assertEqual(formatted["DT_RowId"], self.sample_id)
        self.assertNotIn("_id", formatted)

        # Check datetime is formatted
        self.assertIsInstance(formatted["created_at"], str)

        # Check ObjectId is converted to string
        self.assertIsInstance(formatted["ref_id"], str)

    def test_remove_method_no_id(self):
        """Test remove method with no ID"""
        editor = Editor(self.mongo, 'users', self.remove_args)

        with self.assertRaises(ValueError):
            editor.remove()

    def test_remove_method_with_id(self):
        """Test remove method with an ID"""
        editor = Editor(self.mongo, 'users', self.remove_args, self.sample_id)

        # Mock delete_one to return a successful result
        delete_result = MagicMock(spec=DeleteResult)
        delete_result.deleted_count = 1
        self.collection.delete_one.return_value = delete_result

        result = editor.remove()

        # Check delete_one was called with correct ID
        self.collection.delete_one.assert_called_once_with({"_id": ObjectId(self.sample_id)})

        # Check empty result on success
        self.assertEqual(result, {})

    def test_remove_method_with_multiple_ids(self):
        """Test remove method with multiple IDs"""
        ids = f"{self.sample_id},{self.sample_id2}"
        editor = Editor(self.mongo, 'users', self.remove_args, ids)

        # Mock delete_one to return successful results
        delete_result = MagicMock(spec=DeleteResult)
        delete_result.deleted_count = 1
        self.collection.delete_one.return_value = delete_result

        result = editor.remove()

        # Check delete_one was called twice with correct IDs
        expected_calls = [
            unittest.mock.call({"_id": ObjectId(self.sample_id)}),
            unittest.mock.call({"_id": ObjectId(self.sample_id2)})
        ]
        self.collection.delete_one.assert_has_calls(expected_calls)

        # Check empty result on success
        self.assertEqual(result, {})

    def test_remove_method_exception(self):
        """Test remove method handling exceptions"""
        editor = Editor(self.mongo, 'users', self.remove_args, self.sample_id)

        # Make delete_one raise an exception
        self.collection.delete_one.side_effect = Exception("Database error")

        result = editor.remove()

        # Check error is returned
        self.assertIn("error", result)
        self.assertEqual(result["error"], "Database error")

    def test_create_method_no_data(self):
        """Test create method with no data"""
        editor = Editor(self.mongo, 'users', {"action": "create", "data": {}})

        with self.assertRaises(ValueError):
            editor.create()

    def test_create_method_with_data(self):
        """Test create method with data"""
        editor = Editor(self.mongo, 'users', self.create_args)

        # Mock insert_one to return a successful result
        insert_result = MagicMock(spec=InsertOneResult)
        insert_result.inserted_id = ObjectId(self.sample_id)
        self.collection.insert_one.return_value = insert_result

        # Mock find_one to return the inserted document
        self.collection.find_one.return_value = self.sample_doc

        result = editor.create()

        # Check insert_one was called with processed data
        self.collection.insert_one.assert_called_once()

        # Check find_one was called to get the inserted document
        self.collection.find_one.assert_called_once_with({"_id": ObjectId(self.sample_id)})

        # Check result contains the formatted document
        self.assertIn("data", result)
        self.assertEqual(len(result["data"]), 1)
        self.assertEqual(result["data"][0]["DT_RowId"], self.sample_id)

    def test_create_method_exception(self):
        """Test create method handling exceptions"""
        editor = Editor(self.mongo, 'users', self.create_args)

        # Make insert_one raise an exception
        self.collection.insert_one.side_effect = Exception("Database error")

        result = editor.create()

        # Check error is returned
        self.assertIn("error", result)
        self.assertEqual(result["error"], "Database error")

    def test_edit_method_no_id(self):
        """Test edit method with no ID"""
        editor = Editor(self.mongo, 'users', self.edit_args)

        with self.assertRaises(ValueError):
            editor.edit()

    def test_edit_method_with_id(self):
        """Test edit method with an ID"""
        editor = Editor(self.mongo, 'users', self.edit_args, self.sample_id)

        # Mock update_one to return a successful result
        update_result = MagicMock(spec=UpdateResult)
        update_result.modified_count = 1
        self.collection.update_one.return_value = update_result

        # Mock find_one to return the updated document
        self.collection.find_one.return_value = self.updated_doc

        result = editor.edit()

        # Check update_one was called with correct ID and data
        self.collection.update_one.assert_called_once()
        args, kwargs = self.collection.update_one.call_args
        self.assertEqual(args[0], {"_id": ObjectId(self.sample_id)})
        self.assertIn("$set", args[1])

        # Check find_one was called to get the updated document
        self.collection.find_one.assert_called_once_with({"_id": ObjectId(self.sample_id)})

        # Check result contains the formatted document
        self.assertIn("data", result)
        self.assertEqual(len(result["data"]), 1)
        self.assertEqual(result["data"][0]["DT_RowId"], self.sample_id)
        self.assertEqual(result["data"][0]["name"], "Jane Smith")

    def test_edit_method_missing_data_for_id(self):
        """Test edit method when data is missing for the ID"""
        # Create args with data for a different ID
        different_id = str(ObjectId())
        edit_args = {
            "action": "edit",
            "data": {
                different_id: {
                    "name": "Wrong ID"
                }
            }
        }

        editor = Editor(self.mongo, 'users', edit_args, self.sample_id)

        result = editor.edit()

        # Check update_one was not called
        self.collection.update_one.assert_not_called()

        # Check empty data is returned
        self.assertEqual(result["data"], [])

    def test_edit_method_exception(self):
        """Test edit method handling exceptions"""
        editor = Editor(self.mongo, 'users', self.edit_args, self.sample_id)

        # Make update_one raise an exception
        self.collection.update_one.side_effect = Exception("Database error")

        result = editor.edit()

        # Check error is returned
        self.assertIn("error", result)
        self.assertEqual(result["error"], "Database error")

    def test_process_method_create(self):
        """Test process method for create action"""
        editor = Editor(self.mongo, 'users', self.create_args)

        # Mock the create method
        with patch.object(Editor, 'create', return_value={"data": [{"result": "ok"}]}) as mock_create:
            result = editor.process()

            # Check create was called
            mock_create.assert_called_once()

            # Check result is returned
            self.assertEqual(result, {"data": [{"result": "ok"}]})

    def test_process_method_edit(self):
        """Test process method for edit action"""
        editor = Editor(self.mongo, 'users', self.edit_args, self.sample_id)

        # Mock the edit method
        with patch.object(Editor, 'edit', return_value={"data": [{"result": "ok"}]}) as mock_edit:
            result = editor.process()

            # Check edit was called
            mock_edit.assert_called_once()

            # Check result is returned
            self.assertEqual(result, {"data": [{"result": "ok"}]})

    def test_process_method_remove(self):
        """Test process method for remove action"""
        editor = Editor(self.mongo, 'users', self.remove_args, self.sample_id)

        # Mock the remove method
        with patch.object(Editor, 'remove', return_value={}) as mock_remove:
            result = editor.process()

            # Check remove was called
            mock_remove.assert_called_once()

            # Check result is returned
            self.assertEqual(result, {})

    def test_process_method_unsupported_action(self):
        """Test process method with unsupported action"""
        invalid_args = {"action": "invalid"}
        editor = Editor(self.mongo, 'users', invalid_args)

        with self.assertRaises(ValueError) as context:
            editor.process()

        self.assertIn("Unsupported action", str(context.exception))


if __name__ == '__main__':
    # Run tests with increased verbosity
    unittest.main(verbosity=2)
