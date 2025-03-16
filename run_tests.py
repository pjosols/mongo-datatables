#!/usr/bin/env python
import unittest
import sys
import os

# Import all test classes from the tests package
from tests import get_all_test_classes

def discover_and_run_tests():
    """Discover and run all tests in the tests directory"""
    test_loader = unittest.TestLoader()
    test_dir = os.path.join(os.path.dirname(__file__), 'tests')
    test_suite = test_loader.discover(test_dir, pattern='test_*.py')
    
    print("\n=== Running Tests with Discovery ===\n")
    result = unittest.TextTestRunner(verbosity=2).run(test_suite)
    return result

def run_tests():
    """Run all test cases manually using the test classes from tests/__init__.py"""
    suite = unittest.TestSuite()
    
    # Add all test classes from the tests package
    for test_class in get_all_test_classes():
        suite.addTest(unittest.makeSuite(test_class))

    print("\n=== Running Tests Manually ===\n")
    result = unittest.TextTestRunner(verbosity=2).run(suite)
    return result

if __name__ == "__main__":
    # Parse command line arguments
    import argparse
    parser = argparse.ArgumentParser(description='Run mongo-datatables tests')
    parser.add_argument('--discover', action='store_true', help='Use test discovery instead of manual test loading')
    parser.add_argument('--manual', action='store_true', help='Use manual test loading instead of discovery')
    args = parser.parse_args()
    
    # By default, use discovery unless manual is specified
    if args.manual:
        result = run_tests()
    else:
        result = discover_and_run_tests()
        
    sys.exit(not result.wasSuccessful())