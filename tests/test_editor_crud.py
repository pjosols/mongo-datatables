import unittest
from unittest.mock import MagicMock, patch
import json
from datetime import datetime
from bson.objectid import ObjectId
from pymongo.collection import Collection
from pymongo.database import Database
from pymongo.results import InsertOneResult, UpdateResult, DeleteResult

from mongo_datatables import Editor


class TestEditorCRUD(unittest.TestCase):
    """Test cases for the CRUD operations in the Editor class"""

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

    def test_create_simple_document(self):
        """Test creating a simple document"""
        # Mock insert_one to return success
        insert_result = MagicMock(spec=InsertOneResult)
        insert_result.inserted_id = ObjectId()
        self.collection.insert_one.return_value = insert_result
        
        # Mock find_one to return our newly created document
        created_doc = {
            "_id": insert_result.inserted_id,
            "name": "Test User",
            "email": "test@example.com",
            "age": 30
        }
        self.collection.find_one.return_value = created_doc
        
        # Create Editor instance with create action
        request_args = {
            "action": "create",
            "data": {
                "0": {
                    "name": "Test User",
                    "email": "test@example.com",
                    "age": "30"
                }
            }
        }
        editor = Editor(self.mongo, 'users', request_args)
        
        # Perform the create
        result = editor.create()
        
        # Verify the insert operation was called
        self.collection.insert_one.assert_called_once()
        
        # Verify the result contains the created document
        self.assertIn("data", result)
        self.assertEqual(len(result["data"]), 1)
        self.assertEqual(result["data"][0]["name"], "Test User")
        self.assertEqual(result["data"][0]["email"], "test@example.com")
        self.assertEqual(result["data"][0]["age"], 30)

    def test_edit_simple_document(self):
        """Test editing a simple document"""
        # Create sample document in the database
        doc_id = self.sample_id
        original_doc = {
            "_id": ObjectId(doc_id),
            "name": "Original Name",
            "email": "original@example.com",
            "age": 25
        }
        
        # Mock find_one to return our sample document
        self.collection.find_one.return_value = original_doc
        
        # Mock update_one to return success
        update_result = MagicMock(spec=UpdateResult)
        update_result.modified_count = 1
        self.collection.update_one.return_value = update_result
        
        # Create Editor instance with edit action
        request_args = {
            "action": "edit",
            "data": {
                doc_id: {
                    "DT_RowId": doc_id,
                    "name": "Updated Name",
                    "email": "updated@example.com",
                    "age": "35"
                }
            }
        }
        # Pass the document ID to the constructor
        editor = Editor(self.mongo, 'users', request_args, doc_id=doc_id)
        
        # Perform the edit
        result = editor.edit()
        
        # Verify the update operation was called with correct parameters
        self.collection.update_one.assert_called_once()
        
        # Get the call arguments
        args, kwargs = self.collection.update_one.call_args
        filter_condition, update_operation = args
        
        # Verify filter condition uses the correct ID
        self.assertEqual(filter_condition["_id"], ObjectId(doc_id))
        
        # Verify $set operation contains all the expected updates
        self.assertIn("$set", update_operation)
        set_updates = update_operation["$set"]
        
        self.assertEqual(set_updates["name"], "Updated Name")
        self.assertEqual(set_updates["email"], "updated@example.com")
        self.assertEqual(set_updates["age"], "35")

    def test_remove_document(self):
        """Test removing a document"""
        # Create sample document ID
        doc_id = self.sample_id
        
        # Mock delete_one to return success
        delete_result = MagicMock(spec=DeleteResult)
        delete_result.deleted_count = 1
        self.collection.delete_one.return_value = delete_result
        
        # Create Editor instance with remove action
        request_args = {
            "action": "remove",
            "data": {
                doc_id: {
                    "DT_RowId": doc_id,
                    "id": doc_id
                }
            }
        }
        editor = Editor(self.mongo, 'users', request_args, doc_id=doc_id)
        
        # Perform the remove
        result = editor.remove()
        
        # Verify the delete operation was called with correct parameters
        self.collection.delete_one.assert_called_once()
        
        # Get the call arguments
        args, kwargs = self.collection.delete_one.call_args
        filter_condition = args[0]
        
        # Verify filter condition uses the correct ID
        self.assertEqual(filter_condition["_id"], ObjectId(doc_id))
        
        # Verify the result is empty (success)
        self.assertEqual(result, {})

    def test_batch_edit(self):
        """Test editing multiple documents in a batch"""
        # Create sample document IDs
        doc_id1 = self.sample_id
        doc_id2 = self.sample_id2
        
        # Mock find_one to return our sample documents
        doc1 = {"_id": ObjectId(doc_id1), "name": "User 1", "status": "active"}
        doc2 = {"_id": ObjectId(doc_id2), "name": "User 2", "status": "inactive"}
        
        def mock_find_one(filter_dict):
            if filter_dict["_id"] == ObjectId(doc_id1):
                return doc1
            elif filter_dict["_id"] == ObjectId(doc_id2):
                return doc2
            return None
        
        self.collection.find_one.side_effect = mock_find_one
        
        # Mock update_one to return success
        update_result = MagicMock(spec=UpdateResult)
        update_result.modified_count = 1
        self.collection.update_one.return_value = update_result
        
        # Create Editor instance with edit action for multiple documents
        request_args = {
            "action": "edit",
            "data": {
                doc_id1: {
                    "DT_RowId": doc_id1,
                    "status": "approved"
                },
                doc_id2: {
                    "DT_RowId": doc_id2,
                    "status": "approved"
                }
            }
        }
        editor = Editor(self.mongo, 'users', request_args, doc_id=f"{doc_id1},{doc_id2}")
        
        # Perform the batch edit
        result = editor.edit()
        
        # Verify the update operation was called twice
        self.assertEqual(self.collection.update_one.call_count, 2)
        
        # Verify the result contains both updated documents
        self.assertIn("data", result)
        self.assertEqual(len(result["data"]), 2)


if __name__ == '__main__':
    unittest.main()
