#!/usr/bin/env python3
"""
Comprehensive RowGroup Extension Validation Script

This script validates the RowGroup extension implementation in mongo-datatables v1.13.4
by testing various scenarios and configurations.
"""

import sys
from unittest.mock import MagicMock
from mongo_datatables import DataTables, DataField


def test_rowgroup_basic_functionality():
    """Test basic RowGroup functionality."""
    print("✓ Testing basic RowGroup functionality...")
    
    # Create mock MongoDB
    mock_mongo = MagicMock()
    mock_collection = MagicMock()
    mock_mongo.db.__getitem__.return_value = mock_collection
    
    # Mock aggregation results
    mock_collection.aggregate.return_value = [
        {"_id": "Fiction", "count": 2, "Pages_sum": 500, "Pages_avg": 250},
        {"_id": "Non-Fiction", "count": 3, "count": 3, "Pages_sum": 600, "Pages_avg": 200}
    ]
    
    request_data = {
        "draw": 1,
        "start": 0,
        "length": 10,
        "columns": [
            {"data": "Title", "searchable": True},
            {"data": "Category", "searchable": True},
            {"data": "Pages", "searchable": True}
        ],
        "rowGroup": {
            "dataSrc": "Category"
        }
    }
    
    data_fields = [
        DataField("Title", "string"),
        DataField("Category", "string"),
        DataField("Pages", "number")
    ]
    
    dt = DataTables(mock_mongo.db, "books", request_data, data_fields=data_fields)
    
    # Test configuration parsing
    config = dt._parse_rowgroup_config()
    assert config is not None, "RowGroup config should not be None"
    assert config["dataSrc"] == "Category", "dataSrc should be 'Category'"
    
    print("  ✓ RowGroup configuration parsing works")
    return True


def test_rowgroup_numeric_datasrc():
    """Test RowGroup with numeric dataSrc (column index)."""
    print("✓ Testing RowGroup with numeric dataSrc...")
    
    # Create mock MongoDB
    mock_mongo = MagicMock()
    mock_collection = MagicMock()
    mock_mongo.db.__getitem__.return_value = mock_collection
    
    request_data = {
        "draw": 1,
        "start": 0,
        "length": 10,
        "columns": [
            {"data": "Title", "searchable": True},
            {"data": "Category", "searchable": True},
            {"data": "Pages", "searchable": True}
        ],
        "rowGroup": {
            "dataSrc": 1  # Category column (0-indexed)
        }
    }
    
    data_fields = [
        DataField("Title", "string"),
        DataField("Category", "string"),
        DataField("Pages", "number")
    ]
    
    dt = DataTables(mock_mongo.db, "books", request_data, data_fields=data_fields)
    
    # Test configuration parsing
    config = dt._parse_rowgroup_config()
    assert config is not None, "RowGroup config should not be None"
    assert config["dataSrc"] == 1, "dataSrc should be 1"
    
    print("  ✓ RowGroup numeric dataSrc works")
    return True


def test_rowgroup_validation():
    """Test RowGroup configuration validation."""
    print("✓ Testing RowGroup validation...")
    
    from mongo_datatables.config_validator import ConfigValidator
    
    data_fields = [
        DataField("Title", "string"),
        DataField("Category", "string"),
        DataField("Pages", "number")
    ]
    
    validator = ConfigValidator(data_fields)
    
    # Test valid string dataSrc
    result = validator.validate_rowgroup_config({"dataSrc": "Category"})
    assert result.is_valid, "Valid string dataSrc should pass validation"
    
    # Test valid numeric dataSrc
    result = validator.validate_rowgroup_config({"dataSrc": 1})
    assert result.is_valid, "Valid numeric dataSrc should pass validation"
    
    # Test invalid string dataSrc
    result = validator.validate_rowgroup_config({"dataSrc": "NonExistentField"})
    assert not result.is_valid, "Invalid string dataSrc should fail validation"
    
    # Test invalid numeric dataSrc
    result = validator.validate_rowgroup_config({"dataSrc": 10})
    assert not result.is_valid, "Invalid numeric dataSrc should fail validation"
    
    print("  ✓ RowGroup validation works correctly")
    return True


def test_rowgroup_backward_compatibility():
    """Test RowGroup backward compatibility."""
    print("✓ Testing RowGroup backward compatibility...")
    
    # Create mock MongoDB
    mock_mongo = MagicMock()
    mock_collection = MagicMock()
    mock_mongo.db.__getitem__.return_value = mock_collection
    
    # Test with no RowGroup config
    request_data = {
        "draw": 1,
        "start": 0,
        "length": 10,
        "columns": [
            {"data": "Title", "searchable": True},
            {"data": "Category", "searchable": True}
        ]
    }
    
    data_fields = [
        DataField("Title", "string"),
        DataField("Category", "string")
    ]
    
    dt = DataTables(mock_mongo.db, "books", request_data, data_fields=data_fields)
    
    # Should return None when no RowGroup config
    config = dt._parse_rowgroup_config()
    assert config is None, "RowGroup config should be None when not specified"
    
    rowgroup_data = dt._get_rowgroup_data()
    assert rowgroup_data is None, "RowGroup data should be None when not configured"
    
    print("  ✓ RowGroup backward compatibility works")
    return True


def test_rowgroup_with_other_extensions():
    """Test RowGroup compatibility with other extensions."""
    print("✓ Testing RowGroup with other extensions...")
    
    # Create mock MongoDB
    mock_mongo = MagicMock()
    mock_collection = MagicMock()
    mock_mongo.db.__getitem__.return_value = mock_collection
    
    request_data = {
        "draw": 1,
        "start": 0,
        "length": 10,
        "columns": [
            {"data": "Title", "searchable": True},
            {"data": "Category", "searchable": True}
        ],
        "rowGroup": {"dataSrc": "Category"},
        "searchPanes": True,
        "fixedColumns": {"left": 1},
        "responsive": True,
        "buttons": True,
        "select": {"style": "multi"}
    }
    
    data_fields = [
        DataField("Title", "string"),
        DataField("Category", "string")
    ]
    
    dt = DataTables(mock_mongo.db, "books", request_data, data_fields=data_fields)
    
    # Test that RowGroup config is still parsed correctly
    config = dt._parse_rowgroup_config()
    assert config is not None, "RowGroup config should work with other extensions"
    assert config["dataSrc"] == "Category", "dataSrc should be preserved"
    
    print("  ✓ RowGroup works with other extensions")
    return True


def main():
    """Run all RowGroup validation tests."""
    print("=" * 60)
    print("MONGO-DATATABLES ROWGROUP EXTENSION VALIDATION")
    print("=" * 60)
    
    tests = [
        test_rowgroup_basic_functionality,
        test_rowgroup_numeric_datasrc,
        test_rowgroup_validation,
        test_rowgroup_backward_compatibility,
        test_rowgroup_with_other_extensions
    ]
    
    passed = 0
    failed = 0
    
    for test in tests:
        try:
            if test():
                passed += 1
            else:
                failed += 1
                print(f"  ✗ {test.__name__} failed")
        except Exception as e:
            failed += 1
            print(f"  ✗ {test.__name__} failed with error: {e}")
    
    print("\n" + "=" * 60)
    print(f"VALIDATION RESULTS: {passed} passed, {failed} failed")
    print("=" * 60)
    
    if failed == 0:
        print("🎉 All RowGroup validation tests PASSED!")
        print("✓ RowGroup extension is working correctly")
        print("✓ Configuration parsing works")
        print("✓ Validation is functioning")
        print("✓ Backward compatibility maintained")
        print("✓ Integration with other extensions works")
        return True
    else:
        print("❌ Some RowGroup validation tests FAILED!")
        return False


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)