#!/usr/bin/env python
import unittest
import sys
from tests.test_init import TestInit
from tests.test_datatables import TestDataTables
from tests.test_editor import TestEditor

def run_tests():
    """Run all test cases"""
    suite = unittest.TestSuite()
    suite.addTest(unittest.makeSuite(TestInit))
    suite.addTest(unittest.makeSuite(TestDataTables))
    suite.addTest(unittest.makeSuite(TestEditor))

    print("\n=== Running Tests ===\n")
    result = unittest.TextTestRunner(verbosity=2).run(suite)
    return result

if __name__ == "__main__":
    result = run_tests()
    sys.exit(not result.wasSuccessful())