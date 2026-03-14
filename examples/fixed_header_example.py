"""
FixedHeader Extension Example

This example demonstrates how to use the FixedHeader extension with mongo-datatables.
The FixedHeader extension keeps table headers visible during scrolling, which is 
essential for large datasets where users lose context without visible column headers.
"""

from pymongo import MongoClient
from mongo_datatables import DataTables, DataField

# Sample data setup
def setup_sample_data():
    """Create sample data for demonstration."""
    client = MongoClient('mongodb://localhost:27017/')
    db = client['datatables_demo']
    collection = db['employees']
    
    # Clear existing data
    collection.delete_many({})
    
    # Insert sample employee data
    employees = []
    for i in range(100):
        employees.append({
            'employee_id': f'EMP{i:03d}',
            'name': f'Employee {i}',
            'department': ['Engineering', 'Sales', 'Marketing', 'HR'][i % 4],
            'salary': 50000 + (i * 1000),
            'hire_date': f'2020-{(i % 12) + 1:02d}-{(i % 28) + 1:02d}',
            'active': i % 10 != 0  # Every 10th employee is inactive
        })
    
    collection.insert_many(employees)
    return db

def fixed_header_example():
    """Demonstrate FixedHeader extension usage."""
    db = setup_sample_data()
    
    # Define data fields
    data_fields = [
        DataField('employee_id', 'string', 'ID'),
        DataField('name', 'string', 'Name'),
        DataField('department', 'string', 'Department'),
        DataField('salary', 'number', 'Salary'),
        DataField('hire_date', 'date', 'Hire Date'),
        DataField('active', 'boolean', 'Active')
    ]
    
    print("=== FixedHeader Extension Examples ===\n")
    
    # Example 1: Simple boolean configuration
    print("1. Simple FixedHeader (header only):")
    request_args_simple = {
        'draw': 1,
        'start': 0,
        'length': 10,
        'fixedHeader': True,  # Simple boolean - fixes header only
        'columns': [
            {'data': 'employee_id', 'searchable': True, 'orderable': True},
            {'data': 'name', 'searchable': True, 'orderable': True},
            {'data': 'department', 'searchable': True, 'orderable': True},
            {'data': 'salary', 'searchable': True, 'orderable': True},
            {'data': 'hire_date', 'searchable': True, 'orderable': True},
            {'data': 'active', 'searchable': True, 'orderable': True}
        ]
    }
    
    dt_simple = DataTables(db, 'employees', request_args_simple, data_fields)
    response_simple = dt_simple.get_rows()
    
    print(f"Response includes fixedHeader: {response_simple.get('fixedHeader')}")
    print(f"Records found: {len(response_simple['data'])}\n")
    
    # Example 2: Object configuration with header and footer
    print("2. FixedHeader with header and footer:")
    request_args_full = {
        'draw': 1,
        'start': 0,
        'length': 10,
        'fixedHeader': {
            'header': True,
            'footer': True
        },
        'columns': [
            {'data': 'employee_id', 'searchable': True, 'orderable': True},
            {'data': 'name', 'searchable': True, 'orderable': True},
            {'data': 'department', 'searchable': True, 'orderable': True},
            {'data': 'salary', 'searchable': True, 'orderable': True}
        ]
    }
    
    dt_full = DataTables(db, 'employees', request_args_full, data_fields)
    response_full = dt_full.get_rows()
    
    print(f"Response includes fixedHeader: {response_full.get('fixedHeader')}")
    print(f"Records found: {len(response_full['data'])}\n")
    
    # Example 3: FixedHeader with other extensions
    print("3. FixedHeader combined with other extensions:")
    request_args_combined = {
        'draw': 1,
        'start': 0,
        'length': 10,
        'fixedHeader': True,
        'fixedColumns': {'left': 2},  # Fix first 2 columns
        'responsive': True,           # Enable responsive behavior
        'columns': [
            {'data': 'employee_id', 'searchable': True, 'orderable': True},
            {'data': 'name', 'searchable': True, 'orderable': True},
            {'data': 'department', 'searchable': True, 'orderable': True},
            {'data': 'salary', 'searchable': True, 'orderable': True},
            {'data': 'hire_date', 'searchable': True, 'orderable': True}
        ]
    }
    
    dt_combined = DataTables(db, 'employees', request_args_combined, data_fields)
    response_combined = dt_combined.get_rows()
    
    print(f"FixedHeader config: {response_combined.get('fixedHeader')}")
    print(f"FixedColumns config: {response_combined.get('fixedColumns')}")
    print(f"Responsive config: {response_combined.get('responsive')}")
    print(f"Records found: {len(response_combined['data'])}\n")
    
    # Example 4: Disabled FixedHeader
    print("4. FixedHeader disabled:")
    request_args_disabled = {
        'draw': 1,
        'start': 0,
        'length': 10,
        'fixedHeader': False,  # Explicitly disabled
        'columns': [
            {'data': 'name', 'searchable': True, 'orderable': True},
            {'data': 'department', 'searchable': True, 'orderable': True}
        ]
    }
    
    dt_disabled = DataTables(db, 'employees', request_args_disabled, data_fields)
    response_disabled = dt_disabled.get_rows()
    
    print(f"FixedHeader in response: {'fixedHeader' in response_disabled}")
    print(f"Records found: {len(response_disabled['data'])}\n")

def client_side_html_example():
    """Generate HTML example showing client-side usage."""
    html_example = '''
<!DOCTYPE html>
<html>
<head>
    <title>FixedHeader Extension Example</title>
    <link rel="stylesheet" type="text/css" href="https://cdn.datatables.net/1.13.7/css/jquery.dataTables.min.css"/>
    <link rel="stylesheet" type="text/css" href="https://cdn.datatables.net/fixedheader/3.4.0/css/fixedHeader.dataTables.min.css"/>
    <script type="text/javascript" src="https://code.jquery.com/jquery-3.7.0.min.js"></script>
    <script type="text/javascript" src="https://cdn.datatables.net/1.13.7/js/jquery.dataTables.min.js"></script>
    <script type="text/javascript" src="https://cdn.datatables.net/fixedheader/3.4.0/js/dataTables.fixedHeader.min.js"></script>
</head>
<body>
    <h1>FixedHeader Extension Example</h1>
    <table id="example" class="display" style="width:100%">
        <thead>
            <tr>
                <th>ID</th>
                <th>Name</th>
                <th>Department</th>
                <th>Salary</th>
                <th>Hire Date</th>
                <th>Active</th>
            </tr>
        </thead>
        <tfoot>
            <tr>
                <th>ID</th>
                <th>Name</th>
                <th>Department</th>
                <th>Salary</th>
                <th>Hire Date</th>
                <th>Active</th>
            </tr>
        </tfoot>
    </table>

    <script>
    $(document).ready(function() {
        $('#example').DataTable({
            processing: true,
            serverSide: true,
            ajax: {
                url: '/datatables_endpoint',
                type: 'POST',
                data: function(d) {
                    // Add FixedHeader configuration to request
                    d.fixedHeader = {
                        header: true,
                        footer: true
                    };
                    return d;
                }
            },
            columns: [
                { data: 'employee_id' },
                { data: 'name' },
                { data: 'department' },
                { data: 'salary' },
                { data: 'hire_date' },
                { data: 'active' }
            ],
            // FixedHeader will be configured automatically based on server response
            fixedHeader: true,
            scrollY: '400px',  // Enable vertical scrolling to see fixed header effect
            scrollCollapse: true
        });
    });
    </script>
</body>
</html>
    '''
    
    print("=== Client-Side HTML Example ===")
    print("Save the following HTML to see FixedHeader in action:")
    print(html_example)

if __name__ == '__main__':
    try:
        fixed_header_example()
        client_side_html_example()
    except Exception as e:
        print(f"Error running example: {e}")
        print("Make sure MongoDB is running on localhost:27017")