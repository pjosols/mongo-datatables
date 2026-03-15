"""
Example: Using FixedColumns extension with mongo-datatables

This example demonstrates how to use the FixedColumns extension
to fix columns on the left and/or right side of a DataTable.
"""

from mongo_datatables import DataTables, DataField

# Sample request from DataTables with FixedColumns configuration
request_args = {
    "draw": 1,
    "start": 0,
    "length": 10,
    "columns": [
        {"data": "id", "searchable": False, "orderable": True},
        {"data": "name", "searchable": True, "orderable": True},
        {"data": "email", "searchable": True, "orderable": True},
        {"data": "department", "searchable": True, "orderable": True},
        {"data": "salary", "searchable": True, "orderable": True},
        {"data": "hire_date", "searchable": True, "orderable": True},
        {"data": "status", "searchable": True, "orderable": True},
        {"data": "actions", "searchable": False, "orderable": False}
    ],
    # FixedColumns configuration: fix first 2 columns on left, last 1 on right
    "fixedColumns": {
        "left": 2,   # Fix 'id' and 'name' columns on the left
        "right": 1   # Fix 'actions' column on the right
    }
}

# Define data fields
data_fields = [
    DataField("_id", "objectid", "id"),
    DataField("name", "string"),
    DataField("email", "string"),
    DataField("department", "string"),
    DataField("salary", "number"),
    DataField("hire_date", "date"),
    DataField("status", "string"),
    DataField("actions", "string")
]

def handle_datatables_request(mongo_client, request_args):
    """Handle DataTables request with FixedColumns support."""
    
    # Create DataTables instance
    dt = DataTables(
        pymongo_object=mongo_client,
        collection_name="employees",
        request_args=request_args,
        data_fields=data_fields
    )
    
    # Get the response - FixedColumns config will be included automatically
    response = dt.get_rows()
    
    # The response will include:
    # {
    #     "draw": 1,
    #     "recordsTotal": 1000,
    #     "recordsFiltered": 1000,
    #     "data": [...],
    #     "fixedColumns": {
    #         "left": 2,
    #         "right": 1
    #     }
    # }
    
    return response

# Example client-side JavaScript configuration:
"""
$('#example').DataTable({
    processing: true,
    serverSide: true,
    ajax: '/datatables_endpoint',
    columns: [
        { data: 'id', title: 'ID' },
        { data: 'name', title: 'Name' },
        { data: 'email', title: 'Email' },
        { data: 'department', title: 'Department' },
        { data: 'salary', title: 'Salary' },
        { data: 'hire_date', title: 'Hire Date' },
        { data: 'status', title: 'Status' },
        { data: 'actions', title: 'Actions' }
    ],
    fixedColumns: {
        left: 2,    // Fix first 2 columns (ID, Name)
        right: 1    // Fix last column (Actions)
    }
});
"""

# The server will automatically parse the fixedColumns configuration
# from the request and include it in the response for client-side use.