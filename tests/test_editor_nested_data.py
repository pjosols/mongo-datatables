import unittest
from unittest.mock import MagicMock, patch
import json
from datetime import datetime
from bson.objectid import ObjectId
from mongo_datatables.datatables import DataField
from pymongo.collection import Collection
from pymongo.database import Database
from pymongo.results import InsertOneResult, UpdateResult

from mongo_datatables import Editor


class TestEditorNestedData(unittest.TestCase):
    """Test cases for handling nested data structures in the Editor class"""

    def setUp(self):
        """Set up test fixtures before each test method"""
        # Create a mock PyMongo object
        self.mongo = MagicMock()
        self.mongo.db = MagicMock(spec=Database)
        self.collection = MagicMock(spec=Collection)
        self.mongo.db.__getitem__.return_value = self.collection

        # Sample document IDs
        self.sample_id = str(ObjectId())

    def test_preprocess_document_with_nested_fields(self):
        """Test preprocessing document with nested fields using dot notation"""
        # Create Editor instance with create action
        request_args = {
            "action": "create",
            "data": {
                "0": {
                    "name": "Test User",
                    "profile.bio": "Developer",
                    "profile.skills": "[\"Python\", \"MongoDB\"]",
                    "contact.email": "test@example.com",
                    "contact.phone": "123-456-7890"
                }
            }
        }
        editor = Editor(self.mongo, 'users', request_args)
        
        # Get the data to preprocess
        data = editor.data["0"]
        
        # Call the preprocess method
        processed_doc, dot_notation = editor._preprocess_document(data)
        
        # Verify dot notation fields were extracted
        self.assertEqual(dot_notation["profile.bio"], "Developer")
        self.assertEqual(dot_notation["profile.skills"], ["Python", "MongoDB"])
        self.assertEqual(dot_notation["contact.email"], "test@example.com")
        self.assertEqual(dot_notation["contact.phone"], "123-456-7890")
        
        # Verify main doc doesn't contain dot notation fields
        self.assertNotIn("profile.bio", processed_doc)
        self.assertNotIn("profile.skills", processed_doc)
        self.assertNotIn("contact.email", processed_doc)
        self.assertNotIn("contact.phone", processed_doc)

    def test_process_updates_with_nested_data(self):
        """Test processing updates with nested data structures"""
        # Create Editor instance with data_fields
        data_fields = [
            DataField("profile.joined_date", "date"),
            DataField("stats.visits", "number"),
            DataField("settings.notifications", "boolean"),
            DataField("tags", "array")
        ]
        editor = Editor(self.mongo, 'users', {}, data_fields=data_fields)
        
        # Create nested data structure
        data = {
            "profile": {
                "name": "John Doe",
                "joined_date": "2023-05-15"
            },
            "stats": {
                "visits": "42",
                "last_seen": "2023-06-01T10:30:00"
            },
            "settings": {
                "notifications": "true",
                "theme": "dark"
            },
            "tags": "[\"member\", \"premium\"]"
        }
        
        # Process updates
        updates = {}
        editor._process_updates(data, updates)
        
        # Verify updates were processed correctly
        self.assertIsInstance(updates["profile.joined_date"], datetime)
        self.assertEqual(updates["profile.joined_date"].year, 2023)
        self.assertEqual(updates["profile.joined_date"].month, 5)
        self.assertEqual(updates["profile.joined_date"].day, 15)
        
        self.assertEqual(updates["profile.name"], "John Doe")
        self.assertEqual(updates["stats.visits"], 42)  # Converted to int
        self.assertTrue(updates["settings.notifications"])  # Converted to boolean
        self.assertEqual(updates["settings.theme"], "dark")
        self.assertEqual(updates["tags"], ["member", "premium"])  # Parsed JSON array

    def test_edit_with_complex_nested_updates(self):
        """Test edit operation with complex nested updates"""
        # Create sample document in the database
        doc_id = self.sample_id
        original_doc = {
            "_id": ObjectId(doc_id),
            "name": "Original Name",
            "profile": {
                "bio": "Original Bio",
                "skills": ["Original Skill"]
            },
            "contact": {
                "email": "original@example.com"
            }
        }
        
        # Mock find_one to return our sample document
        self.collection.find_one.return_value = original_doc
        
        # Mock update_one to return success
        update_result = MagicMock(spec=UpdateResult)
        update_result.modified_count = 1
        self.collection.update_one.return_value = update_result
        
        # Create Editor instance with edit action and nested updates
        request_args = {
            "action": "edit",
            "data": {
                doc_id: {
                    "DT_RowId": doc_id,
                    "name": "Updated Name",
                    "profile.bio": "Updated Bio",
                    "profile.skills": "[\"Python\", \"MongoDB\"]",
                    "contact.email": "updated@example.com",
                    "contact.phone": "987-654-3210"  # New field
                }
            }
        }
        # Pass the document ID and data_fields to the constructor
        data_fields = [
            DataField('profile.skills', 'array')  # Specify that profile.skills should be treated as an array
        ]
        editor = Editor(self.mongo, 'users', request_args, doc_id=doc_id, data_fields=data_fields)
        
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
        self.assertEqual(set_updates["profile.bio"], "Updated Bio")
        self.assertEqual(set_updates["profile.skills"], ["Python", "MongoDB"])
        self.assertEqual(set_updates["contact.email"], "updated@example.com")
        self.assertEqual(set_updates["contact.phone"], "987-654-3210")
        
        # Verify the result contains the updated document
        self.assertIn("data", result)
        self.assertEqual(len(result["data"]), 1)

    def test_create_with_complex_nested_structure(self):
        """Test create operation with complex nested structure"""
        # Mock insert_one to return success
        insert_result = MagicMock(spec=InsertOneResult)
        insert_result.inserted_id = ObjectId()
        self.collection.insert_one.return_value = insert_result
        
        # Mock find_one to return our newly created document
        created_doc = {
            "_id": insert_result.inserted_id,
            "name": "New User",
            "profile": {
                "bio": "Developer",
                "skills": ["Python", "MongoDB"]
            },
            "contact": {
                "email": "new@example.com",
                "phone": "123-456-7890"
            },
            "created_at": datetime(2023, 6, 15, 10, 30, 0)
        }
        self.collection.find_one.return_value = created_doc
        
        # Create Editor instance with create action and nested structure
        request_args = {
            "action": "create",
            "data": {
                "0": {
                    "name": "New User",
                    "profile.bio": "Developer",
                    "profile.skills": "[\"Python\", \"MongoDB\"]",
                    "contact.email": "new@example.com",
                    "contact.phone": "123-456-7890",
                    "created_at": "2023-06-15T10:30:00"
                }
            }
        }
        editor = Editor(self.mongo, 'users', request_args)
        
        # Perform the create
        result = editor.create()
        
        # Verify the insert operation was called
        self.collection.insert_one.assert_called_once()
        
        # Get the call arguments
        args, kwargs = self.collection.insert_one.call_args
        inserted_doc = args[0]
        
        # Verify the document structure is correct
        self.assertEqual(inserted_doc["name"], "New User")
        
        # The nested structure should be created properly
        self.assertIn("profile", inserted_doc)
        self.assertIsInstance(inserted_doc["profile"], dict)
        self.assertEqual(inserted_doc["profile"]["bio"], "Developer")
        self.assertEqual(inserted_doc["profile"]["skills"], ["Python", "MongoDB"])
        
        self.assertIn("contact", inserted_doc)
        self.assertIsInstance(inserted_doc["contact"], dict)
        self.assertEqual(inserted_doc["contact"]["email"], "new@example.com")
        self.assertEqual(inserted_doc["contact"]["phone"], "123-456-7890")
        
        # Verify the result contains the created document
        self.assertIn("data", result)
        self.assertEqual(len(result["data"]), 1)


if __name__ == '__main__':
    unittest.main()
