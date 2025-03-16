# Import all test modules to make them accessible through the tests package

# Core tests
from tests.test_init import TestInit
from tests.test_editor import TestEditor

# DataTables functionality tests
from tests.test_datatables_initialization import *
from tests.test_datatables_query_building import *
from tests.test_datatables_edge_cases import *
from tests.test_datatables_filtering import *
from tests.test_datatables_pagination import *
from tests.test_datatables_results import *
from tests.test_datatables_search import *
from tests.test_datatables_sorting import *
from tests.test_datatables_text_search import *
from tests.test_datatables_query_stats import *

# Editor functionality tests
from tests.test_editor_advanced import *
from tests.test_editor_crud import *
from tests.test_editor_data_processing import *
from tests.test_editor_nested_data import *

# Define __all__ to control what gets imported with 'from tests import *'
__all__ = [
    # Test classes
    'TestInit',
    'TestEditor',
    # Test discovery function
    'get_all_test_classes',
]

def get_all_test_classes():
    """Return a list of all test classes defined in the tests package"""
    import unittest
    import sys
    
    # Get all classes from this module that are unittest.TestCase subclasses
    test_classes = []
    for name, obj in sys.modules[__name__].__dict__.items():
        if name.startswith('Test') and isinstance(obj, type) and issubclass(obj, unittest.TestCase):
            test_classes.append(obj)
    
    return test_classes