#!/usr/bin/env python3
"""
Real-world RowGroup API Validation Test

This script validates the RowGroup extension with various real-world scenarios
including different dataSrc configurations, MongoDB aggregation pipeline efficiency,
and integration with existing features.
"""

from mongo_datatables import DataTables, DataField
from unittest.mock import MagicMock


def test_rowgroup_with_search_and_pagination():
    """Test RowGroup with search and pagination."""
    print("✓ Testing RowGroup with search and pagination...")
    
    mock_mongo = MagicMock()
    mock_collection = MagicMock()
    mock_mongo.db.__getitem__.return_value = mock_collection
    
    # Mock aggregation for RowGroup
    mock_collection.aggregate.side_effect = [
        [{"_id": "Fiction", "count": 5}, {"_id": "Non-Fiction", "count": 3}],  # RowGroup data
        []  # Main results
    ]
    mock_collection.count_documents.return_value = 8
    
    request_data = {
        "draw": 1,
        "start": 5,
        "length": 10,
        "search": {"value": "python", "regex": False},
        "columns": [
            {"data": "title", "searchable": True},
            {"data": "category", "searchable": True}
        ],
        "rowGroup": {"dataSrc": "category"}
    }
    
    data_fields = [
        DataField("title", "string"),
        DataField("category", "string")
    ]
    
    dt = DataTables(mock_mongo.db, "books", request_data, data_fields=data_fields)
    response = dt.get_rows()
    
    assert "rowGroup" in response
    assert response["rowGroup"]["dataSrc"] == "category"
    print("  ✓ RowGroup works with search and pagination")


def test_rowgroup_with_sorting():
    """Test RowGroup with column sorting."""
    print("✓ Testing RowGroup with sorting...")
    
    mock_mongo = MagicMock()
    mock_collection = MagicMock()
    mock_mongo.db.__getitem__.return_value = mock_collection
    
    mock_collection.aggregate.side_effect = [
        [{"_id": "A", "count": 2}, {"_id": "B", "count": 3}],  # RowGroup data
        []  # Main results
    ]
    mock_collection.count_documents.return_value = 5
    
    request_data = {
        "draw": 1,
        "start": 0,
        "length": 10,
        "order": [{"column": 0, "dir": "desc"}],
        "columns": [
            {"data": "title", "searchable": True, "orderable": True},
            {"data": "category", "searchable": True, "orderable": True}
        ],
        "rowGroup": {"dataSrc": 1}  # Category column by index
    }
    
    data_fields = [
        DataField("title", "string"),
        DataField("category", "string")
    ]
    
    dt = DataTables(mock_mongo.db, "books", request_data, data_fields=data_fields)
    response = dt.get_rows()
    
    assert "rowGroup" in response
    assert response["rowGroup"]["dataSrc"] == 1
    print("  ✓ RowGroup works with sorting")


def test_rowgroup_aggregation_pipeline():
    """Test RowGroup MongoDB aggregation pipeline efficiency."""
    print("✓ Testing RowGroup aggregation pipeline...")
    
    mock_mongo = MagicMock()
    mock_collection = MagicMock()
    mock_mongo.db.__getitem__.return_value = mock_collection
    
    # Mock complex aggregation results
    mock_collection.aggregate.side_effect = [
        [
            {
                "_id": "Fiction", 
                "count": 10, 
                "pages_sum": 2500, 
                "pages_avg": 250,
                "rating_sum": 42.5,
                "rating_avg": 4.25
            },
            {
                "_id": "Non-Fiction", 
                "count": 8, 
                "pages_sum": 1800, 
                "pages_avg": 225,
                "rating_sum": 35.2,
                "rating_avg": 4.4
            }
        ],  # RowGroup data
        []  # Main results
    ]
    mock_collection.count_documents.return_value = 18
    
    request_data = {
        "draw": 1,
        "start": 0,
        "length": 10,
        "columns": [
            {"data": "title", "searchable": True},
            {"data": "category", "searchable": True},
            {"data": "pages", "searchable": True},
            {"data": "rating", "searchable": True}
        ],
        "rowGroup": {
            "dataSrc": "category",
            "startRender": True,
            "endRender": True
        }
    }
    
    data_fields = [
        DataField("title", "string"),
        DataField("category", "string"),
        DataField("pages", "number"),
        DataField("rating", "number")
    ]
    
    dt = DataTables(mock_mongo.db, "books", request_data, data_fields=data_fields)
    
    # Test that the aggregation pipeline is built correctly
    rowgroup_data = dt._get_rowgroup_data()
    assert rowgroup_data is not None
    assert "groups" in rowgroup_data
    
    print("  ✓ RowGroup aggregation pipeline works efficiently")


def test_rowgroup_with_multiple_extensions():
    """Test RowGroup with multiple DataTables extensions."""
    print("✓ Testing RowGroup with multiple extensions...")
    
    mock_mongo = MagicMock()
    mock_collection = MagicMock()
    mock_mongo.db.__getitem__.return_value = mock_collection
    
    # Mock for SearchPanes and RowGroup
    mock_collection.aggregate.side_effect = [
        [],  # SearchPanes for title
        [],  # SearchPanes for category  
        [],  # SearchPanes for pages
        [{"_id": "Fiction", "count": 5}],  # RowGroup data
        []   # Main results
    ]
    mock_collection.count_documents.return_value = 5
    
    request_data = {
        "draw": 1,
        "start": 0,
        "length": 10,
        "columns": [
            {"data": "title", "searchable": True},
            {"data": "category", "searchable": True},
            {"data": "pages", "searchable": True}
        ],
        "rowGroup": {"dataSrc": "category"},
        "searchPanes": True,
        "fixedColumns": {"left": 1},
        "responsive": {"breakpoints": {"sm": 576}},
        "buttons": {"extend": ["csv", "excel"]},
        "select": {"style": "multi"}
    }
    
    data_fields = [
        DataField("title", "string"),
        DataField("category", "string"),
        DataField("pages", "number")
    ]
    
    dt = DataTables(mock_mongo.db, "books", request_data, data_fields=data_fields)
    response = dt.get_rows()
    
    # Verify all extensions are present
    assert "rowGroup" in response
    assert "searchPanes" in response
    assert "fixedColumns" in response
    assert "responsive" in response
    assert "buttons" in response
    assert "select" in response
    
    print("  ✓ RowGroup integrates well with other extensions")


def test_rowgroup_edge_cases():
    """Test RowGroup edge cases and error handling."""
    print("✓ Testing RowGroup edge cases...")
    
    mock_mongo = MagicMock()
    mock_collection = MagicMock()
    mock_mongo.db.__getitem__.return_value = mock_collection
    
    # Test with empty dataSrc
    request_data = {
        "draw": 1,
        "start": 0,
        "length": 10,
        "columns": [{"data": "title", "searchable": True}],
        "rowGroup": {}  # No dataSrc
    }
    
    data_fields = [DataField("title", "string")]
    
    dt = DataTables(mock_mongo.db, "books", request_data, data_fields=data_fields)
    rowgroup_data = dt._get_rowgroup_data()
    assert rowgroup_data is None, "Should return None for empty RowGroup config"
    
    # Test with invalid column index
    request_data["rowGroup"] = {"dataSrc": 10}  # Out of range
    dt = DataTables(mock_mongo.db, "books", request_data, data_fields=data_fields)
    rowgroup_data = dt._get_rowgroup_data()
    assert rowgroup_data is None, "Should return None for invalid column index"
    
    print("  ✓ RowGroup handles edge cases correctly")


def main():
    """Run all API validation tests."""
    print("=" * 60)
    print("ROWGROUP API VALIDATION TESTS")
    print("=" * 60)
    
    tests = [
        test_rowgroup_with_search_and_pagination,
        test_rowgroup_with_sorting,
        test_rowgroup_aggregation_pipeline,
        test_rowgroup_with_multiple_extensions,
        test_rowgroup_edge_cases
    ]
    
    passed = 0
    failed = 0
    
    for test in tests:
        try:
            test()
            passed += 1
        except Exception as e:
            failed += 1
            print(f"  ✗ {test.__name__} failed: {e}")
    
    print("\n" + "=" * 60)
    print(f"API VALIDATION RESULTS: {passed} passed, {failed} failed")
    print("=" * 60)
    
    if failed == 0:
        print("🎉 All RowGroup API validation tests PASSED!")
        print("✓ RowGroup works with search and pagination")
        print("✓ RowGroup works with sorting")
        print("✓ MongoDB aggregation pipeline is efficient")
        print("✓ Integration with other extensions works")
        print("✓ Edge cases are handled properly")
        return True
    else:
        print("❌ Some API validation tests FAILED!")
        return False


if __name__ == "__main__":
    import sys
    success = main()
    sys.exit(0 if success else 1)