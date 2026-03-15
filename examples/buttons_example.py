"""
Example demonstrating Buttons extension support in mongo-datatables.

This example shows how to configure and use the DataTables Buttons extension
for export functionality (CSV, Excel, PDF), print, copy, and column visibility.
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
    DataField('name', 'string'),
    DataField('email', 'string'),
    DataField('age', 'number'),
    DataField('department', 'string'),
    DataField('salary', 'number'),
    DataField('hire_date', 'date')
]

@app.route('/')
def index():
    """Render the main page with DataTables and Buttons extension."""
    html_template = """
    <!DOCTYPE html>
    <html>
    <head>
        <title>Buttons Extension Example</title>
        <link rel="stylesheet" href="https://cdn.datatables.net/1.13.6/css/jquery.dataTables.min.css">
        <link rel="stylesheet" href="https://cdn.datatables.net/buttons/2.4.2/css/buttons.dataTables.min.css">
        <script src="https://code.jquery.com/jquery-3.7.0.min.js"></script>
        <script src="https://cdn.datatables.net/1.13.6/js/jquery.dataTables.min.js"></script>
        <script src="https://cdn.datatables.net/buttons/2.4.2/js/dataTables.buttons.min.js"></script>
        <script src="https://cdnjs.cloudflare.com/ajax/libs/jszip/3.10.1/jszip.min.js"></script>
        <script src="https://cdnjs.cloudflare.com/ajax/libs/pdfmake/0.1.53/pdfmake.min.js"></script>
        <script src="https://cdnjs.cloudflare.com/ajax/libs/pdfmake/0.1.53/vfs_fonts.js"></script>
        <script src="https://cdn.datatables.net/buttons/2.4.2/js/buttons.html5.min.js"></script>
        <script src="https://cdn.datatables.net/buttons/2.4.2/js/buttons.print.min.js"></script>
        <script src="https://cdn.datatables.net/buttons/2.4.2/js/buttons.colVis.min.js"></script>
    </head>
    <body>
        <h1>DataTables with Buttons Extension</h1>
        <table id="example" class="display" style="width:100%">
            <thead>
                <tr>
                    <th>Name</th>
                    <th>Email</th>
                    <th>Age</th>
                    <th>Department</th>
                    <th>Salary</th>
                    <th>Hire Date</th>
                </tr>
            </thead>
        </table>

        <script>
        $(document).ready(function() {
            $('#example').DataTable({
                processing: true,
                serverSide: true,
                ajax: {
                    url: '/data',
                    type: 'POST',
                    data: function(d) {
                        // Add buttons configuration to request
                        d.buttons = {
                            export: {
                                csv: true,
                                excel: true,
                                pdf: true
                            },
                            colvis: {
                                enabled: true
                            },
                            print: {
                                enabled: true
                            },
                            copy: {
                                enabled: true
                            }
                        };
                        return d;
                    }
                },
                columns: [
                    { data: 'name' },
                    { data: 'email' },
                    { data: 'age' },
                    { data: 'department' },
                    { data: 'salary' },
                    { data: 'hire_date' }
                ],
                dom: 'Bfrtip',
                buttons: [
                    {
                        extend: 'csv',
                        text: 'Export CSV',
                        action: function(e, dt, button, config) {
                            // Get export data from server
                            $.post('/export', dt.ajax.params(), function(data) {
                                // Convert to CSV and download
                                var csv = convertToCSV(data.data);
                                downloadCSV(csv, 'export.csv');
                            });
                        }
                    },
                    {
                        extend: 'excel',
                        text: 'Export Excel'
                    },
                    {
                        extend: 'pdf',
                        text: 'Export PDF'
                    },
                    {
                        extend: 'print',
                        text: 'Print'
                    },
                    {
                        extend: 'copy',
                        text: 'Copy'
                    },
                    {
                        extend: 'colvis',
                        text: 'Column Visibility'
                    }
                ]
            });
        });

        function convertToCSV(data) {
            if (!data || data.length === 0) return '';
            
            var headers = Object.keys(data[0]);
            var csv = headers.join(',') + '\\n';
            
            data.forEach(function(row) {
                var values = headers.map(function(header) {
                    var value = row[header] || '';
                    return '"' + String(value).replace(/"/g, '""') + '"';
                });
                csv += values.join(',') + '\\n';
            });
            
            return csv;
        }

        function downloadCSV(csv, filename) {
            var blob = new Blob([csv], { type: 'text/csv' });
            var url = window.URL.createObjectURL(blob);
            var a = document.createElement('a');
            a.setAttribute('hidden', '');
            a.setAttribute('href', url);
            a.setAttribute('download', filename);
            document.body.appendChild(a);
            a.click();
            document.body.removeChild(a);
        }
        </script>
    </body>
    </html>
    """
    return render_template_string(html_template)

@app.route('/data', methods=['POST'])
def data():
    """Handle DataTables server-side processing requests."""
    try:
        dt = DataTables(
            db,
            'employees',
            request.form.to_dict(flat=False),
            data_fields
        )
        
        return jsonify(dt.get_rows())
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/export', methods=['POST'])
def export_data():
    """Handle export data requests for full dataset."""
    try:
        dt = DataTables(
            db,
            'employees',
            request.form.to_dict(flat=False),
            data_fields
        )
        
        # Return all data for export (bypassing pagination)
        export_data = dt.get_export_data()
        
        return jsonify({
            'data': export_data,
            'recordsTotal': dt.count_total(),
            'recordsFiltered': dt.count_filtered()
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/seed')
def seed_data():
    """Seed the database with sample data."""
    sample_data = [
        {
            'name': 'John Doe',
            'email': 'john.doe@example.com',
            'age': 30,
            'department': 'Engineering',
            'salary': 75000,
            'hire_date': '2020-01-15'
        },
        {
            'name': 'Jane Smith',
            'email': 'jane.smith@example.com',
            'age': 28,
            'department': 'Marketing',
            'salary': 65000,
            'hire_date': '2021-03-10'
        },
        {
            'name': 'Bob Johnson',
            'email': 'bob.johnson@example.com',
            'age': 35,
            'department': 'Sales',
            'salary': 70000,
            'hire_date': '2019-07-22'
        }
    ]
    
    db.employees.delete_many({})
    db.employees.insert_many(sample_data)
    
    return jsonify({'message': 'Sample data inserted successfully'})

if __name__ == '__main__':
    app.run(debug=True)