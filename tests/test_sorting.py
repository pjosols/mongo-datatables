from unittest.mock import patch
from tests.base_test import BaseDataTablesTest
from mongo_datatables import DataTables


class TestSorting(BaseDataTablesTest):
    """Test cases for DataTables sorting functionality"""

    def test_orderable_columns(self):
        """Test orderable_columns property"""
        datatables = DataTables(self.mongo, 'users', self.request_args)
        # In the new implementation, we use columns property to get all columns
        # and then filter for orderable ones
        orderable_columns = [col['data'] for col in datatables.columns if col.get('orderable', True)]
        self.assertEqual(set(orderable_columns), set(["name", "email", "status"]))
