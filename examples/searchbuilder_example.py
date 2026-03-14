"""
SearchBuilder Extension Example

This example demonstrates how to use the SearchBuilder extension with mongo-datatables.
SearchBuilder provides a visual query builder interface that's much more user-friendly
than manual search syntax.

Features demonstrated:
- SearchBuilder options generation for field-specific dropdowns
- Support for various operators (equals, contains, greater than, etc.)
- AND/OR logic groups and nested conditions
- Integration with existing server-side processing
"""

from flask import Flask, request, jsonify, render_template_string
from pymongo import MongoClient
from datetime import datetime
from bson.objectid import ObjectId

from mongo_datatables import DataTables, DataField

app = Flask(__name__)

# MongoDB connection
client = MongoClient('mongodb://localhost:27017/')
db = client['searchbuilder_demo']
collection = db['employees']

# Sample data setup
def setup_sample_data():
    """Insert sample employee data for demonstration."""
    if collection.count_documents({}) == 0:
        sample_employees = [
            {
                "_id": ObjectId(),
                "name": "John Doe",
                "age": 30,
                "salary": 75000,
                "department": "Engineering",
                "position": "Senior Developer",
                "hire_date": datetime(2020, 1, 15),
                "active": True,
                "skills": ["Python", "JavaScript", "MongoDB"]
            },
            {
                "_id": ObjectId(),
                "name": "Jane Smith",
                "age": 28,
                "salary": 68000,
                "department": "Engineering",
                "position": "Frontend Developer",
                "hire_date": datetime(2021, 3, 10),
                "active": True,
                "skills": ["React", "TypeScript", "CSS"]
            },
            {
                "_id": ObjectId(),
                "name": "Bob Johnson",
                "age": 35,
                "salary": 85000,
                "department": "Engineering",
                "position": "Tech Lead",
                "hire_date": datetime(2019, 6, 20),
                "active": True,
                "skills": ["Java", "Spring", "Microservices"]
            },
            {
                "_id": ObjectId(),
                "name": "Alice Brown",
                "age": 32,
                "salary": 72000,
                "department": "Marketing",
                "position": "Marketing Manager",
                "hire_date": datetime(2020, 11, 5),
                "active": True,
                "skills": ["SEO", "Content Marketing", "Analytics"]
            },
            {
                "_id": ObjectId(),
                "name": "Charlie Wilson",
                "age": 29,
                "salary": 65000,
                "department": "Sales",
                "position": "Sales Representative",
                "hire_date": datetime(2021, 8, 12),
                "active": True,
                "skills": ["CRM", "Lead Generation", "Negotiation"]
            },
            {
                "_id": ObjectId(),
                "name": "Diana Davis",
                "age": 26,
                "salary": 58000,
                "department": "HR",
                "position": "HR Specialist",
                "hire_date": datetime(2022, 2, 1),
                "active": True,
                "skills": ["Recruitment", "Employee Relations", "Training"]
            },
            {
                "_id": ObjectId(),
                "name": "Frank Miller",
                "age": 45,
                "salary": 95000,
                "department": "Engineering",
                "position": "Engineering Manager",
                "hire_date": datetime(2018, 4, 8),
                "active": False,  # Left the company
                "skills": ["Leadership", "Architecture", "Strategy"]
            }
        ]
        
        collection.insert_many(sample_employees)
        print("Sample data inserted successfully!")

# Define data fields with proper types for SearchBuilder
data_fields = [
    DataField("name", "string"),
    DataField("age", "number"),
    DataField("salary", "number"),
    DataField("department", "string"),
    DataField("position", "string"),
    DataField("hire_date", "date"),
    DataField("active", "boolean")
]

@app.route('/')
def index():
    """Serve the main page with DataTables and SearchBuilder."""
    html_template = '''
    <!DOCTYPE html>
    <html>
    <head>
        <title>SearchBuilder Extension Demo</title>
        <link rel="stylesheet" type="text/css" href="https://cdn.datatables.net/1.13.7/css/jquery.dataTables.min.css">
        <link rel="stylesheet" type="text/css" href="https://cdn.datatables.net/searchbuilder/1.6.0/css/searchBuilder.dataTables.min.css">
        <link rel="stylesheet" type="text/css" href="https://cdn.datatables.net/datetime/1.5.1/css/dataTables.dateTime.min.css">
        <link rel="stylesheet" type="text/css" href="https://cdn.datatables.net/buttons/2.4.2/css/buttons.dataTables.min.css">
        
        <script type="text/javascript" src="https://code.jquery.com/jquery-3.7.0.min.js"></script>
        <script type="text/javascript" src="https://cdn.datatables.net/1.13.7/js/jquery.dataTables.min.js"></script>
        <script type="text/javascript" src="https://cdn.datatables.net/searchbuilder/1.6.0/js/dataTables.searchBuilder.min.js"></script>
        <script type="text/javascript" src="https://cdn.datatables.net/datetime/1.5.1/js/dataTables.dateTime.min.js"></script>
        <script type="text/javascript" src="https://cdn.datatables.net/buttons/2.4.2/js/dataTables.buttons.min.js"></script>
        
        <style>
            body { font-family: Arial, sans-serif; margin: 20px; }
            .container { max-width: 1200px; margin: 0 auto; }
            h1 { color: #333; }
            .info { background: #f0f8ff; padding: 15px; border-radius: 5px; margin-bottom: 20px; }
        </style>
    </head>
    <body>
        <div class="container">
            <h1>SearchBuilder Extension Demo</h1>
            
            <div class="info">
                <h3>SearchBuilder Features:</h3>
                <ul>
                    <li><strong>Visual Query Builder:</strong> Build complex queries using a user-friendly interface</li>
                    <li><strong>Field-Specific Operators:</strong> Different operators based on field type (text, number, date)</li>
                    <li><strong>AND/OR Logic:</strong> Combine conditions with logical operators</li>
                    <li><strong>Nested Groups:</strong> Create complex nested condition groups</li>
                    <li><strong>Real-time Filtering:</strong> Results update as you build your query</li>
                </ul>
                <p><strong>Try it:</strong> Click the "Search Builder" button above the table to start building queries!</p>
            </div>
            
            <table id="employeeTable" class="display" style="width:100%">
                <thead>
                    <tr>
                        <th>Name</th>
                        <th>Age</th>
                        <th>Salary</th>
                        <th>Department</th>
                        <th>Position</th>
                        <th>Hire Date</th>
                        <th>Active</th>
                    </tr>
                </thead>
            </table>
        </div>

        <script>
        $(document).ready(function() {
            $('#employeeTable').DataTable({
                processing: true,
                serverSide: true,
                ajax: {
                    url: '/data',
                    type: 'POST',
                    data: function(d) {
                        // Include SearchBuilder parameters
                        return d;
                    }
                },
                columns: [
                    { data: 'name', name: 'name' },
                    { data: 'age', name: 'age' },
                    { 
                        data: 'salary', 
                        name: 'salary',
                        render: function(data, type, row) {
                            if (type === 'display') {
                                return '$' + parseInt(data).toLocaleString();
                            }
                            return data;
                        }
                    },
                    { data: 'department', name: 'department' },
                    { data: 'position', name: 'position' },
                    { 
                        data: 'hire_date', 
                        name: 'hire_date',
                        render: function(data, type, row) {
                            if (type === 'display' && data) {
                                return new Date(data).toLocaleDateString();
                            }
                            return data;
                        }
                    },
                    { 
                        data: 'active', 
                        name: 'active',
                        render: function(data, type, row) {
                            if (type === 'display') {
                                return data ? 'Yes' : 'No';
                            }
                            return data;
                        }
                    }
                ],
                dom: 'Qlfrtip', // Q = SearchBuilder
                searchBuilder: {
                    columns: [0, 1, 2, 3, 4, 5, 6] // Enable SearchBuilder for all columns
                },
                pageLength: 25,
                lengthMenu: [[10, 25, 50, -1], [10, 25, 50, "All"]],
                order: [[0, 'asc']]
            });
        });
        </script>
    </body>
    </html>
    '''
    return render_template_string(html_template)

@app.route('/data', methods=['POST'])
def get_data():
    """Handle DataTables server-side processing with SearchBuilder support."""
    try:
        # Get request parameters
        request_args = request.get_json() or request.form.to_dict()
        
        # Convert form data to proper format for nested parameters
        if hasattr(request, 'form'):
            # Handle SearchBuilder parameters from form data
            searchbuilder_data = {}
            for key, value in request.form.items():
                if key.startswith('searchBuilder'):
                    # Parse nested SearchBuilder parameters
                    # This is a simplified parser - in production you might want more robust parsing
                    if 'conditions' in key:
                        if 'conditions' not in searchbuilder_data:
                            searchbuilder_data['conditions'] = []
                        # Add condition parsing logic here
                    elif 'logic' in key:
                        searchbuilder_data['logic'] = value
            
            if searchbuilder_data:
                request_args['searchBuilder'] = searchbuilder_data
        
        # Create DataTables instance
        dt = DataTables(
            db, 
            'employees', 
            request_args, 
            data_fields=data_fields
        )
        
        # Get the response
        response = dt.get_rows()
        
        return jsonify(response)
        
    except Exception as e:
        print(f"Error processing request: {str(e)}")
        return jsonify({
            'draw': int(request_args.get('draw', 1)),
            'recordsTotal': 0,
            'recordsFiltered': 0,
            'data': [],
            'error': str(e)
        })

@app.route('/searchbuilder-options')
def get_searchbuilder_options():
    """Get SearchBuilder options for field dropdowns."""
    try:
        # Mock request for options
        request_args = {
            'draw': 1,
            'start': 0,
            'length': 10,
            'searchBuilder': True,
            'columns': [
                {'data': 'name', 'searchable': True},
                {'data': 'age', 'searchable': True},
                {'data': 'salary', 'searchable': True},
                {'data': 'department', 'searchable': True},
                {'data': 'position', 'searchable': True},
                {'data': 'hire_date', 'searchable': True},
                {'data': 'active', 'searchable': True}
            ]
        }
        
        dt = DataTables(db, 'employees', request_args, data_fields=data_fields)
        options = dt.get_searchbuilder_options()
        
        return jsonify(options)
        
    except Exception as e:
        return jsonify({'error': str(e)})

if __name__ == '__main__':
    # Setup sample data
    setup_sample_data()
    
    print("SearchBuilder Demo Server Starting...")
    print("Visit http://localhost:5000 to see the demo")
    print("\nSearchBuilder Features:")
    print("- Click 'Search Builder' button to open the query builder")
    print("- Add conditions using different operators for each field type")
    print("- Combine conditions with AND/OR logic")
    print("- Create nested condition groups for complex queries")
    print("- Results update in real-time as you build your query")
    
    app.run(debug=True, port=5000)