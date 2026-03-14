"""
ColReorder Extension Example for mongo-datatables

This example demonstrates how to use the ColReorder extension with mongo-datatables
to enable drag-and-drop column reordering functionality.
"""

from flask import Flask, request, jsonify, render_template_string
from pymongo import MongoClient
from mongo_datatables import DataTables, DataField

app = Flask(__name__)

# MongoDB connection
client = MongoClient('mongodb://localhost:27017/')
db = client['example_db']

# Sample data fields
data_fields = [
    DataField('name', 'string', 'Name'),
    DataField('position', 'string', 'Position'),
    DataField('office', 'string', 'Office'),
    DataField('age', 'number', 'Age'),
    DataField('start_date', 'date', 'Start Date'),
    DataField('salary', 'number', 'Salary')
]

@app.route('/')
def index():
    """Render the main page with ColReorder example."""
    return render_template_string('''
<!DOCTYPE html>
<html>
<head>
    <title>ColReorder Extension Example</title>
    <link rel="stylesheet" type="text/css" href="https://cdn.datatables.net/1.13.7/css/jquery.dataTables.min.css">
    <link rel="stylesheet" type="text/css" href="https://cdn.datatables.net/colreorder/1.7.0/css/colReorder.dataTables.min.css">
    <script type="text/javascript" src="https://code.jquery.com/jquery-3.7.0.min.js"></script>
    <script type="text/javascript" src="https://cdn.datatables.net/1.13.7/js/jquery.dataTables.min.js"></script>
    <script type="text/javascript" src="https://cdn.datatables.net/colreorder/1.7.0/js/dataTables.colReorder.min.js"></script>
</head>
<body>
    <div style="margin: 20px;">
        <h1>ColReorder Extension Example</h1>
        <p>Drag column headers to reorder columns. The server will track the column configuration.</p>
        
        <table id="example" class="display" style="width:100%">
            <thead>
                <tr>
                    <th>Name</th>
                    <th>Position</th>
                    <th>Office</th>
                    <th>Age</th>
                    <th>Start Date</th>
                    <th>Salary</th>
                </tr>
            </thead>
        </table>
    </div>

    <script>
    $(document).ready(function() {
        $('#example').DataTable({
            processing: true,
            serverSide: true,
            ajax: {
                url: '/data',
                type: 'POST',
                data: function(d) {
                    // Include ColReorder configuration
                    d.colReorder = {
                        realtime: true
                    };
                    return d;
                }
            },
            columns: [
                { data: 'name' },
                { data: 'position' },
                { data: 'office' },
                { data: 'age' },
                { data: 'start_date' },
                { data: 'salary' }
            ],
            colReorder: {
                realtime: true,
                order: [0, 1, 2, 3, 4, 5]  // Initial column order
            },
            pageLength: 25,
            lengthMenu: [[10, 25, 50, -1], [10, 25, 50, "All"]]
        });
    });
    </script>
</body>
</html>
    ''')

@app.route('/data', methods=['POST'])
def data():
    """Handle DataTables server-side processing with ColReorder support."""
    try:
        # Get request parameters
        request_data = request.get_json() or request.form.to_dict()
        
        # Create DataTables instance
        dt = DataTables(
            pymongo_object=db,
            collection_name='employees',
            request_args=request_data,
            data_fields=data_fields
        )
        
        # Get the response with ColReorder support
        response = dt.get_rows()
        
        return jsonify(response)
        
    except Exception as e:
        return jsonify({
            'error': str(e),
            'draw': request_data.get('draw', 1),
            'recordsTotal': 0,
            'recordsFiltered': 0,
            'data': []
        }), 500

@app.route('/setup')
def setup_data():
    """Setup sample data for the example."""
    try:
        # Clear existing data
        db.employees.delete_many({})
        
        # Insert sample data
        sample_employees = [
            {
                'name': 'John Doe',
                'position': 'Software Engineer',
                'office': 'New York',
                'age': 28,
                'start_date': '2022-01-15',
                'salary': 75000
            },
            {
                'name': 'Jane Smith',
                'position': 'Product Manager',
                'office': 'San Francisco',
                'age': 32,
                'start_date': '2021-03-10',
                'salary': 95000
            },
            {
                'name': 'Bob Johnson',
                'position': 'Designer',
                'office': 'Los Angeles',
                'age': 26,
                'start_date': '2022-06-01',
                'salary': 65000
            },
            {
                'name': 'Alice Brown',
                'position': 'Data Scientist',
                'office': 'Chicago',
                'age': 30,
                'start_date': '2021-09-15',
                'salary': 85000
            },
            {
                'name': 'Charlie Wilson',
                'position': 'DevOps Engineer',
                'office': 'Seattle',
                'age': 29,
                'start_date': '2022-02-20',
                'salary': 80000
            }
        ]
        
        db.employees.insert_many(sample_employees)
        
        return jsonify({
            'message': f'Successfully inserted {len(sample_employees)} sample employees',
            'count': len(sample_employees)
        })
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

if __name__ == '__main__':
    print("ColReorder Extension Example")
    print("=" * 40)
    print("1. Start the Flask application")
    print("2. Visit http://localhost:5000/setup to create sample data")
    print("3. Visit http://localhost:5000 to see the ColReorder example")
    print("4. Drag column headers to reorder columns")
    print()
    
    app.run(debug=True, port=5000)