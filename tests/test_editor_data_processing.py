import unittest
from unittest.mock import MagicMock, patch
import json
from datetime import datetime
from bson.objectid import ObjectId
from pymongo.collection import Collection
from pymongo.database import Database

from mongo_datatables import Editor
from mongo_datatables.datatables import DataField


class TestEditorDataProcessing(unittest.TestCase):
    """Test cases for data processing in the Editor class"""

    def setUp(self):
        """Set up test fixtures before each test method"""
        # Create a mock PyMongo object
        self.mongo = MagicMock()
        self.mongo.db = MagicMock(spec=Database)
        self.collection = MagicMock(spec=Collection)
        self.mongo.db.__getitem__.return_value = self.collection

    def test_process_updates_with_type_conversions(self):
        """Test processing updates with various type conversions"""
        # Create Editor instance with data_fields
        data_fields = [
            DataField("age", "number"),
            DataField("active", "boolean"),
            DataField("scores", "array"),
            DataField("birthday", "date")
        ]
        editor = Editor(self.mongo, 'users', {}, data_fields=data_fields)
        
        # Create data with various types
        data = {
            "age": "30",
            "active": "yes",
            "scores": "[90, 85, 95]",
            "birthday": "1993-08-20"
        }
        
        # Process updates
        updates = {}
        editor._process_updates(data, updates)
        
        # Verify type conversions
        self.assertEqual(updates["age"], 30)  # String to int
        self.assertTrue(updates["active"])  # "yes" to True
        self.assertEqual(updates["scores"], [90, 85, 95])  # JSON string to array
        self.assertIsInstance(updates["birthday"], datetime)  # String to datetime

    def test_process_updates_with_invalid_values(self):
        """Test processing updates with invalid values that should be handled gracefully"""
        # Create Editor instance with data_fields
        data_fields = [
            DataField("age", "number"),
            DataField("joined_date", "date"),
            DataField("tags", "array")
        ]
        editor = Editor(self.mongo, 'users', {}, data_fields=data_fields)
        
        # Create data with invalid values
        data = {
            "age": "not-a-number",
            "joined_date": "invalid-date",
            "tags": "not-valid-json"
        }
        
        # Process updates
        updates = {}
        editor._process_updates(data, updates)
        
        # Verify invalid values are handled gracefully
        self.assertEqual(updates["age"], "not-a-number")  # Kept as string
        self.assertEqual(updates["joined_date"], "invalid-date")  # Kept as string
        self.assertEqual(updates["tags"], ["not-valid-json"])  # Treated as single-item array

    def test_preprocess_document_with_date_fields(self):
        """Test preprocessing document with date fields"""
        # Create Editor instance with create action
        request_args = {
            "action": "create",
            "data": {
                "0": {
                    "name": "Test User",
                    "created_at": "2023-01-15T14:30:45",
                    "update_date": "2023-02-20",
                    "metadata.last_login_time": "2023-03-10T09:15:30Z"
                }
            }
        }
        editor = Editor(self.mongo, 'users', request_args)
        
        # Get the data to preprocess
        data = editor.data["0"]
        
        # Call the preprocess method
        processed_doc, dot_notation = editor._preprocess_document(data)
        
        # Verify date fields were converted to datetime objects
        self.assertIsInstance(processed_doc["created_at"], datetime)
        self.assertIsInstance(processed_doc["update_date"], datetime)
        self.assertIsInstance(dot_notation["metadata.last_login_time"], datetime)
        
        # Verify the dates were parsed correctly
        self.assertEqual(processed_doc["created_at"].year, 2023)
        self.assertEqual(processed_doc["created_at"].month, 1)
        self.assertEqual(processed_doc["created_at"].day, 15)
        
        self.assertEqual(processed_doc["update_date"].year, 2023)
        self.assertEqual(processed_doc["update_date"].month, 2)
        self.assertEqual(processed_doc["update_date"].day, 20)
        
        self.assertEqual(dot_notation["metadata.last_login_time"].year, 2023)
        self.assertEqual(dot_notation["metadata.last_login_time"].month, 3)
        self.assertEqual(dot_notation["metadata.last_login_time"].day, 10)

    def test_preprocess_document_with_json_data(self):
        """Test preprocessing document with JSON string data"""
        # Create Editor instance with create action
        request_args = {
            "action": "create",
            "data": {
                "0": {
                    "name": "Test User",
                    "tags": "[\"tag1\", \"tag2\"]",  # JSON string
                    "metadata": "{\"key\": \"value\"}",  # JSON string
                }
            }
        }
        editor = Editor(self.mongo, 'users', request_args)
        
        # Get the data to preprocess
        data = editor.data["0"]
        
        # Call the preprocess method
        processed_doc, dot_notation = editor._preprocess_document(data)
        
        # Verify JSON strings were parsed
        self.assertEqual(processed_doc["tags"], ["tag1", "tag2"])
        self.assertEqual(processed_doc["metadata"], {"key": "value"})

    def test_format_response_document(self):
        """Test formatting document for response"""
        # Create Editor instance
        editor = Editor(self.mongo, 'users', {})
        
        # Create a sample document with various types
        doc = {
            "_id": ObjectId(),
            "name": "Test User",
            "created_at": datetime(2023, 5, 15, 10, 30, 0),
            "tags": ["tag1", "tag2"],
            "metadata": {"key": "value"},
            "active": True,
            "score": 95.5
        }
        
        # Format the document for response
        response_doc = editor._format_response_document(doc)
        
        # Verify ObjectId was converted to string DT_RowId
        self.assertNotIn("_id", response_doc)
        self.assertIn("DT_RowId", response_doc)
        self.assertEqual(response_doc["DT_RowId"], str(doc["_id"]))
        
        # Verify other fields were preserved
        self.assertEqual(response_doc["name"], "Test User")
        self.assertEqual(response_doc["tags"], ["tag1", "tag2"])
        self.assertEqual(response_doc["metadata"], {"key": "value"})
        self.assertTrue(response_doc["active"])
        self.assertEqual(response_doc["score"], 95.5)
        
        # Verify datetime was converted to ISO format string for JSON serialization
        self.assertIsInstance(response_doc["created_at"], str)
        self.assertEqual(response_doc["created_at"], "2023-05-15T10:30:00")


if __name__ == '__main__':
    unittest.main()
