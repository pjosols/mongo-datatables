from unittest.mock import patch, MagicMock
from tests.base_test import BaseDataTablesTest
from pymongo import MongoClient
from pymongo.database import Database
from pymongo.collection import Collection
from mongo_datatables.datatables import DataField
import re
from datetime import datetime
from bson.objectid import ObjectId

from mongo_datatables import DataTables


class TestDataTablesEdgeCases(BaseDataTablesTest):
    """Test cases for edge cases and uncovered code in the DataTables class"""

    def test_initialization_with_mongo_client(self):
        """Test initialization with a PyMongo client"""
        # Create a mock MongoClient
        mongo_client = MagicMock(spec=MongoClient)
        db = MagicMock(spec=Database)
        collection = MagicMock(spec=Collection)
        
        # Setup the mocks
        mongo_client.get_database.return_value = db
        db.__getitem__.return_value = collection
        
        # Initialize DataTables with the client
        datatables = DataTables(mongo_client, 'test_collection', self.request_args)
        
        # Verify the collection was retrieved correctly
        mongo_client.get_database.assert_called_once()
        db.__getitem__.assert_called_once_with('test_collection')
        
        # Verify the collection property returns the correct collection
        self.assertEqual(datatables.collection, collection)

    def test_initialization_with_database(self):
        """Test initialization with a PyMongo database"""
        # Create a mock Database
        db = MagicMock(spec=Database)
        collection = MagicMock(spec=Collection)
        
        # Setup the mocks
        db.__getitem__.return_value = collection
        
        # Initialize DataTables with the database
        datatables = DataTables(db, 'test_collection', self.request_args)
        
        # Verify the collection was retrieved correctly
        db.__getitem__.assert_called_once_with('test_collection')
        
        # Verify the collection property returns the correct collection
        self.assertEqual(datatables.collection, collection)

    def test_initialization_with_dict_like(self):
        """Test initialization with a dict-like object"""
        # Create a dict-like object
        dict_like = {}
        collection = MagicMock(spec=Collection)
        dict_like['test_collection'] = collection
        
        # Initialize DataTables with the dict-like object
        datatables = DataTables(dict_like, 'test_collection', self.request_args)
        
        # Verify the collection property returns the correct collection
        self.assertEqual(datatables.collection, collection)

    def test_exact_phrase_search_without_text_index(self):
        """Test exact phrase search when text index is not available"""
        # Set up a search with a quoted phrase
        self.request_args['search']['value'] = '"exact phrase"'
        
        # Create a DataTables instance with use_text_index=False
        # This is more reliable than mocking has_text_index
        datatables = DataTables(self.mongo, 'test_collection', self.request_args, use_text_index=False)
        
        # Get the global search condition
        condition = datatables.global_search_condition
        
        # Verify the condition uses regex for exact phrase matching
        self.assertIn('$or', condition)
        
        # Check that the regex pattern includes word boundaries
        for subcondition in condition['$or']:
                field_name = list(subcondition.keys())[0]
                if '$regex' in subcondition[field_name]:
                    # The regex pattern will have escaped backslashes and spaces
                    self.assertIn('\\bexact\\ phrase\\b', subcondition[field_name]['$regex'])
                    self.assertEqual(subcondition[field_name]['$options'], 'i')

    def test_numeric_field_search(self):
        """Test search with numeric field types"""
        # Set up a search with a numeric value
        self.request_args['search']['value'] = '42'
        
        # Initialize DataTables with data_fields
        data_fields = [
            DataField('age', 'number'),
            DataField('name', 'string')
        ]
        
        # Add columns to request args to match the data_fields
        self.request_args['columns'] = [
            {'data': 'name', 'searchable': True},
            {'data': 'age', 'searchable': True}
        ]
        
        # Create DataTables instance
        datatables = DataTables(self.mongo, 'test_collection', self.request_args, data_fields=data_fields)
        
        # Get the global search condition
        condition = datatables.global_search_condition
        
        # Verify the condition includes numeric comparison
        self.assertIn('$or', condition)
        
        # Find the numeric condition
        numeric_condition = None
        for subcondition in condition['$or']:
            if 'age' in subcondition:
                numeric_condition = subcondition
                break
        
        # Verify numeric field is queried with exact value, not regex
        self.assertIsNotNone(numeric_condition)
        self.assertEqual(numeric_condition['age'], 42.0)

    def test_numeric_field_search_with_invalid_number(self):
        """Test search with numeric field types but invalid number"""
        # Set up a search with a non-numeric value
        self.request_args['search']['value'] = 'not-a-number'
        
        # Initialize DataTables with data_fields
        data_fields = [
            DataField('age', 'number'),
            DataField('name', 'string')
        ]
        
        # Add columns to request args to match the data_fields
        self.request_args['columns'] = [
            {'data': 'name', 'searchable': True},
            {'data': 'age', 'searchable': True}
        ]
        
        # Create DataTables instance
        datatables = DataTables(self.mongo, 'test_collection', self.request_args, data_fields=data_fields)
        
        # Get the global search condition
        condition = datatables.global_search_condition
        
        # Verify the condition does not include numeric field
        self.assertIn('$or', condition)
        
        # Check that no condition exists for the age field
        age_condition = None
        for subcondition in condition['$or']:
            if 'age' in subcondition:
                age_condition = subcondition
                break
        
        # Verify numeric field is not included in the search
        self.assertIsNone(age_condition)

if __name__ == '__main__':
    unittest.main()
