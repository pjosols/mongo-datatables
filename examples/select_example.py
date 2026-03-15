"""
Example demonstrating Select extension support with mongo-datatables.

This example shows how to use the Select extension for row selection functionality
with server-side processing. The Select extension works primarily client-side
but requires server-side configuration passing.
"""

from flask import Flask, request, jsonify
from pymongo import MongoClient
from mongo_datatables import DataTables, DataField

app = Flask(__name__)

# MongoDB connection
client = MongoClient('mongodb://localhost:27017/')
db = client['example_db']
collection = db['books']

# Sample data fields
data_fields = [
    DataField('title', 'string', 'Title'),
    DataField('author', 'string', 'Author'),
    DataField('published_date', 'date', 'Published'),
    DataField('pages', 'number', 'Pages'),
    DataField('isbn', 'string', 'ISBN')
]

@app.route('/api/books')
def get_books():
    """API endpoint for DataTables with Select extension support."""
    
    # Get request parameters
    request_args = request.args.to_dict()
    
    # Parse nested parameters (columns, order, search, select, etc.)
    for key in list(request_args.keys()):
        if '[' in key and ']' in key:
            # Handle nested parameters like columns[0][data]
            parts = key.replace(']', '').split('[')
            current = request_args
            
            for i, part in enumerate(parts[:-1]):
                if part not in current:
                    current[part] = {}
                current = current[part]
            
            current[parts[-1]] = request_args[key]
            del request_args[key]
    
    # Handle Select extension parameters
    if 'select' in request_args:
        select_param = request_args['select']
        if select_param == 'true':
            request_args['select'] = True
        elif select_param == 'false':
            request_args['select'] = False
        else:
            # Parse select configuration
            try:
                import json
                request_args['select'] = json.loads(select_param)
            except:
                # Default to simple boolean
                request_args['select'] = True
    
    # Create DataTables instance
    dt = DataTables(
        pymongo_object=db,
        collection_name='books',
        request_args=request_args,
        data_fields=data_fields
    )
    
    # Return JSON response
    return jsonify(dt.get_rows())

@app.route('/')
def index():
    """Serve the HTML page with DataTables and Select extension."""
    return '''
    <!DOCTYPE html>
    <html>
    <head>
        <title>Select Extension Example</title>
        <link rel="stylesheet" href="https://cdn.datatables.net/1.13.7/css/jquery.dataTables.min.css">
        <link rel="stylesheet" href="https://cdn.datatables.net/select/1.7.0/css/select.dataTables.min.css">
        <script src="https://code.jquery.com/jquery-3.7.0.min.js"></script>
        <script src="https://cdn.datatables.net/1.13.7/js/jquery.dataTables.min.js"></script>
        <script src="https://cdn.datatables.net/select/1.7.0/js/dataTables.select.min.js"></script>
    </head>
    <body>
        <h1>Books with Select Extension</h1>
        <p>Click rows to select them. Use Ctrl+click for multi-selection.</p>
        
        <table id="books-table" class="display" style="width:100%">
            <thead>
                <tr>
                    <th>Title</th>
                    <th>Author</th>
                    <th>Published</th>
                    <th>Pages</th>
                    <th>ISBN</th>
                </tr>
            </thead>
        </table>
        
        <div style="margin-top: 20px;">
            <button id="get-selected">Get Selected Rows</button>
            <button id="select-all">Select All</button>
            <button id="deselect-all">Deselect All</button>
        </div>
        
        <div id="selected-info" style="margin-top: 10px;"></div>

        <script>
        $(document).ready(function() {
            var table = $('#books-table').DataTable({
                processing: true,
                serverSide: true,
                ajax: {
                    url: '/api/books',
                    type: 'GET',
                    data: function(d) {
                        // Add Select extension configuration
                        d.select = {
                            style: 'multi'  // Allow multiple row selection
                        };
                        return d;
                    }
                },
                columns: [
                    { data: 'title' },
                    { data: 'author' },
                    { data: 'published_date' },
                    { data: 'pages' },
                    { data: 'isbn' }
                ],
                select: {
                    style: 'multi',
                    selector: 'tr'
                }
            });
            
            // Handle selection events
            table.on('select', function(e, dt, type, indexes) {
                updateSelectedInfo();
            });
            
            table.on('deselect', function(e, dt, type, indexes) {
                updateSelectedInfo();
            });
            
            // Button handlers
            $('#get-selected').click(function() {
                var selectedData = table.rows({ selected: true }).data().toArray();
                console.log('Selected rows:', selectedData);
                alert('Selected ' + selectedData.length + ' rows. Check console for details.');
            });
            
            $('#select-all').click(function() {
                table.rows().select();
            });
            
            $('#deselect-all').click(function() {
                table.rows().deselect();
            });
            
            function updateSelectedInfo() {
                var count = table.rows({ selected: true }).count();
                $('#selected-info').text('Selected rows: ' + count);
            }
        });
        </script>
    </body>
    </html>
    '''

if __name__ == '__main__':
    # Insert sample data if collection is empty
    if collection.count_documents({}) == 0:
        sample_books = [
            {
                'title': 'The Great Gatsby',
                'author': 'F. Scott Fitzgerald',
                'published_date': '1925-04-10',
                'pages': 180,
                'isbn': '978-0-7432-7356-5'
            },
            {
                'title': 'To Kill a Mockingbird',
                'author': 'Harper Lee',
                'published_date': '1960-07-11',
                'pages': 281,
                'isbn': '978-0-06-112008-4'
            },
            {
                'title': '1984',
                'author': 'George Orwell',
                'published_date': '1949-06-08',
                'pages': 328,
                'isbn': '978-0-452-28423-4'
            }
        ]
        collection.insert_many(sample_books)
        print("Sample data inserted.")
    
    app.run(debug=True)