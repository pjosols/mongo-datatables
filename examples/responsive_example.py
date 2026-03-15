"""
Example demonstrating Responsive extension support in mongo-datatables.

This example shows how to configure and use the Responsive extension
for DataTables with MongoDB backend.
"""

from pymongo import MongoClient
from mongo_datatables import DataTables, DataField


def responsive_example():
    """Example showing responsive extension configuration."""
    
    # Sample DataTables request with responsive configuration
    request_args = {
        "draw": "1",
        "start": "0",
        "length": "10",
        "search": {"value": "", "regex": False},
        "order": [{"column": 0, "dir": "asc"}],
        "columns": [
            {"data": "name", "searchable": True, "orderable": True},
            {"data": "email", "searchable": True, "orderable": True},
            {"data": "phone", "searchable": True, "orderable": True},
            {"data": "address", "searchable": True, "orderable": True},
            {"data": "status", "searchable": True, "orderable": True}
        ],
        # Responsive extension configuration
        "responsive": {
            "breakpoints": {
                "xs": 0,
                "sm": 576,
                "md": 768,
                "lg": 992,
                "xl": 1200
            },
            "display": {
                "childRow": True,
                "childRowImmediate": False
            },
            "priorities": {
                "0": 1,  # Name column - highest priority
                "1": 2,  # Email column - second priority
                "2": 10000,  # Phone column - lowest priority
                "3": 9999,   # Address column - second lowest
                "4": 3   # Status column - third priority
            }
        }
    }
    
    # Define data fields
    data_fields = [
        DataField('name', 'string'),
        DataField('email', 'string'),
        DataField('phone', 'string'),
        DataField('address', 'string'),
        DataField('status', 'string')
    ]
    
    # Connect to MongoDB (replace with your connection details)
    client = MongoClient('mongodb://localhost:27017/')
    db = client['your_database']
    
    # Create DataTables instance
    dt = DataTables(
        pymongo_object=db,
        collection_name='users',
        request_args=request_args,
        data_fields=data_fields
    )
    
    # Get the response with responsive configuration
    response = dt.get_rows()
    
    print("DataTables Response:")
    print(f"Draw: {response['draw']}")
    print(f"Records Total: {response['recordsTotal']}")
    print(f"Records Filtered: {response['recordsFiltered']}")
    print(f"Data Count: {len(response['data'])}")
    
    # The responsive configuration is included in the response
    if 'responsive' in response:
        print("\nResponsive Configuration:")
        print(f"Breakpoints: {response['responsive']['breakpoints']}")
        print(f"Display: {response['responsive']['display']}")
        print(f"Priorities: {response['responsive']['priorities']}")
    
    return response


if __name__ == "__main__":
    # Note: This example requires a running MongoDB instance
    # and a 'users' collection with appropriate data
    try:
        response = responsive_example()
        print("\nExample completed successfully!")
    except Exception as e:
        print(f"Example failed: {e}")
        print("Make sure MongoDB is running and the collection exists.")