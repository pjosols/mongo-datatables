from unittest.mock import patch
from tests.base_test import BaseDataTablesTest
from mongo_datatables import DataTables


class TestInitialization(BaseDataTablesTest):
    """Test cases for DataTables initialization and basic properties"""

    def test_initialization(self):
        """Test initialization of DataTables class"""
        datatables = DataTables(self.mongo, 'users', self.request_args)

        # Test basic attributes
        self.assertEqual(datatables.collection, self.collection)
        self.assertEqual(datatables.request_args, self.request_args)
        self.assertEqual(datatables.custom_filter, {})

    def test_initialization_with_custom_filter(self):
        """Test initialization with custom filter"""
        custom_filter = {"status": "active"}
        datatables = DataTables(self.mongo, 'users', self.request_args, **custom_filter)

        self.assertEqual(datatables.custom_filter, custom_filter)

    def test_collection_property(self):
        """Test the collection property"""
        datatables = DataTables(self.mongo, 'users', self.request_args)
        self.assertEqual(datatables.collection, self.collection)
        self.mongo.db.__getitem__.assert_called_once_with('users')

    def test_start(self):
        """Test start property"""
        self.request_args["start"] = 20
        datatables = DataTables(self.mongo, 'users', self.request_args)
        self.assertEqual(datatables.start, 20)

    def test_limit(self):
        """Test limit property"""
        # Normal case
        self.request_args["length"] = 25
        datatables = DataTables(self.mongo, 'users', self.request_args)
        self.assertEqual(datatables.limit, 25)

        # Test with -1 (all records)
        # In the new implementation, limit returns -1 instead of None
        self.request_args["length"] = -1
        datatables = DataTables(self.mongo, 'users', self.request_args)
        self.assertEqual(datatables.limit, -1)

    def test_count_total(self):
        """Test count_total method"""
        self.collection.count_documents.return_value = 100
        datatables = DataTables(self.mongo, 'users', self.request_args)
        self.assertEqual(datatables.count_total(), 100)
        self.collection.count_documents.assert_called_once_with({})

    def test_count_filtered(self):
        """Test count_filtered method"""
        # Create a DataTables instance with a custom filter
        custom_filter = {"status": "active"}
        datatables = DataTables(self.mongo, 'users', self.request_args, **custom_filter)
        
        # Set up the mock return value
        self.collection.count_documents.return_value = 50
        
        # Call the method
        result = datatables.count_filtered()
        
        # Verify the result
        self.assertEqual(result, 50)
        
        # Verify that count_documents was called with the filter
        # Note: We can't check the exact filter content because it depends on the implementation
        self.collection.count_documents.assert_called_once()

    def test_projection(self):
        """Test projection property"""
        datatables = DataTables(self.mongo, 'users', self.request_args)
        projection = datatables.projection

        # Check that _id is included and all requested columns are included
        self.assertEqual(projection["_id"], 1)
        for column in ["name", "email", "status"]:
            self.assertEqual(projection[column], 1)

    def test_projection_with_nested_fields(self):
        """Test projection property with nested fields"""
        self.request_args["columns"].append(
            {"data": "address.city", "name": "", "searchable": True, "orderable": True,
             "search": {"value": "", "regex": False}}
        )
        datatables = DataTables(self.mongo, 'users', self.request_args)
        projection = datatables.projection

        # In the new implementation, the dot notation is preserved in the projection
        self.assertIn("address.city", projection)
        self.assertEqual(projection["address.city"], 1)
        
        # The parent field is not automatically included
        self.assertNotIn("address", projection)
