import unittest
import sys
import os

# Add the parent directory to the path so we can import the module directly
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# Import the module being tested
import mongo_datatables
from mongo_datatables import DataTables, Editor


class TestInit(unittest.TestCase):
    """Test cases for module initialization"""

    def test_imports(self):
        """Test that import classes are available"""
        self.assertTrue(hasattr(mongo_datatables, 'DataTables'))
        self.assertTrue(hasattr(mongo_datatables, 'Editor'))

    def test_version(self):
        """Test version is defined and accessible"""
        self.assertTrue(hasattr(mongo_datatables, '__version__'))
        self.assertIsInstance(mongo_datatables.__version__, str)

    def test_imports_work(self):
        """Test that imports actually work"""
        self.assertIsNotNone(DataTables)
        self.assertIsNotNone(Editor)


if __name__ == '__main__':
    unittest.main()