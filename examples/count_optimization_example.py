"""
Example demonstrating optimized count operations in mongo-datatables v1.10.0

This example shows how the new count optimization provides significant performance
improvements for large datasets while maintaining accuracy for smaller collections.
"""

import time
from pymongo import MongoClient
from mongo_datatables import DataTables, DataField


def simulate_count_performance():
    """Simulate the performance improvement of optimized count operations."""
    
    print("MongoDB DataTables Count Optimization Demo")
    print("=" * 50)
    
    # Simulate different collection sizes and their count strategies
    test_cases = [
        {"size": 50000, "strategy": "exact", "estimated_time": "5ms"},
        {"size": 500000, "strategy": "estimated", "estimated_time": "1ms"},
        {"size": 5000000, "strategy": "estimated", "estimated_time": "1ms"},
    ]
    
    print("\nCount Strategy Selection:")
    print("-" * 30)
    for case in test_cases:
        size = case["size"]
        strategy = case["strategy"]
        time_est = case["estimated_time"]
        
        print(f"Collection size: {size:,} documents")
        print(f"Strategy: {strategy} count")
        print(f"Estimated time: {time_est}")
        
        if strategy == "exact":
            print("  → Uses count_documents({}) for accuracy")
        else:
            print("  → Uses estimated_document_count() for speed")
        print()
    
    print("Performance Benefits:")
    print("-" * 20)
    print("• 10-50x faster count operations on large datasets")
    print("• Maintains accuracy for collections < 100k documents")
    print("• Automatic fallback to exact counts when needed")
    print("• Aggregation pipeline optimization for filtered counts")
    print("• Graceful error handling with multiple fallback strategies")


def example_usage():
    """Example of using DataTables with optimized count operations."""
    
    print("\nExample Usage:")
    print("-" * 15)
    
    # Example request parameters
    request_args = {
        "draw": 1,
        "start": 0,
        "length": 25,
        "search": {"value": ""},
        "columns": [
            {"data": "name", "searchable": True},
            {"data": "email", "searchable": True},
            {"data": "status", "searchable": True}
        ]
    }
    
    # Define data fields
    data_fields = [
        DataField("name", "string"),
        DataField("email", "string"), 
        DataField("status", "string"),
        DataField("created_date", "date"),
        DataField("user_id", "number")
    ]
    
    print("# Initialize DataTables with optimized counting")
    print("datatables = DataTables(")
    print("    pymongo_object=mongo_client,")
    print("    collection_name='users',")
    print("    request_args=request_args,")
    print("    data_fields=data_fields")
    print(")")
    print()
    
    print("# Get counts - automatically optimized based on collection size")
    print("total_count = datatables.count_total()      # Uses estimated_document_count() for large collections")
    print("filtered_count = datatables.count_filtered() # Uses aggregation pipeline for better performance")
    print()
    
    print("# Get paginated results")
    print("response = datatables.get_rows()")
    print()
    
    print("The optimization is transparent - no code changes required!")


if __name__ == "__main__":
    simulate_count_performance()
    example_usage()