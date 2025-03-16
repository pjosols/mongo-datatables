import unittest
from unittest.mock import MagicMock, patch
from bson.objectid import ObjectId
from pymongo.collection import Collection
from pymongo.database import Database

from mongo_datatables import DataTables


class BaseDataTablesTest(unittest.TestCase):
    """Base test class for DataTables tests"""

    def setUp(self):
        """Set up test fixtures before each test method"""
        # Create a mock PyMongo object
        self.mongo = MagicMock()
        self.mongo.db = MagicMock(spec=Database)
        self.collection = MagicMock(spec=Collection)
        self.mongo.db.__getitem__.return_value = self.collection

        # Sample DataTables request parameters
        self.request_args = {
            "draw": "1",
            "start": 0,
            "length": 10,
            "search": {"value": "", "regex": False},
            "order": [{"column": 0, "dir": "asc"}],
            "columns": [
                {"data": "name", "name": "", "searchable": True, "orderable": True,
                 "search": {"value": "", "regex": False}},
                {"data": "email", "name": "", "searchable": True, "orderable": True,
                 "search": {"value": "", "regex": False}},
                {"data": "status", "name": "", "searchable": True, "orderable": True,
                 "search": {"value": "", "regex": False}}
            ]
        }

        # Sample documents for mocked responses
        self.sample_docs = [
            {"_id": ObjectId(), "name": "John Doe", "email": "john@example.com", "status": "active"},
            {"_id": ObjectId(), "name": "Jane Smith", "email": "jane@example.com", "status": "inactive"},
            {"_id": ObjectId(), "name": "Bob Johnson", "email": "bob@example.com", "status": "active"}
        ]
