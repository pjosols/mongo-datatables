"""
Example demonstrating RowGroup extension support in mongo-datatables.

This example shows how to configure and use the DataTables RowGroup extension
for visual data organization with grouping headers and footers.
"""

from flask import Flask, render_template_string, request, jsonify
from pymongo import MongoClient
from mongo_datatables import DataTables, DataField

app = Flask(__name__)

# MongoDB setup
client = MongoClient('mongodb://localhost:27017/')
db = client['datatables_demo']
collection = db['products']

# Sample data
sample_data = [
    {"name": "Laptop Pro", "category": "Electronics", "price": 1299.99, "stock": 15},
    {"name": "Wireless Mouse", "category": "Electronics", "price": 29.99, "stock": 50},
    {"name": "Office Chair", "category": "Furniture", "price": 199.99, "stock": 8},
    {"name": "Standing Desk", "category": "Furniture", "price": 399.99, "stock": 12},
    {"name": "Smartphone", "category": "Electronics", "price": 699.99, "stock": 25},
    {"name": "Bookshelf", "category": "Furniture", "price": 149.99, "stock": 6},
    {"name": "Tablet", "category": "Electronics", "price": 329.99, "stock": 18},
    {"name": "Coffee Table", "category": "Furniture", "price": 89.99, "stock": 10}
]

# Initialize sample data
collection.delete_many({})
collection.insert_many(sample_data)

@app.route('/')
def index():
    """Render the main page with DataTables and RowGroup extension."""
    template = """
    <!DOCTYPE html>
    <html>
    <head>
        <title>RowGroup Extension Example</title>
        <link rel="stylesheet" href="https://cdn.datatables.net/1.13.6/css/jquery.dataTables.min.css">
        <link rel="stylesheet" href="https://cdn.datatables.net/rowgroup/1.4.0/css/rowGroup.dataTables.min.css">
        <script src="https://code.jquery.com/jquery-3.6.0.min.js"></script>
        <script src="https://cdn.datatables.net/1.13.6/js/jquery.dataTables.min.js"></script>
        <script src="https://cdn.datatables.net/rowgroup/1.4.0/js/dataTables.rowGroup.min.js"></script>
    </head>
    <body>
        <div style="margin: 20px;">
            <h1>DataTables with RowGroup Extension</h1>
            <p>Products grouped by category with summary calculations.</p>
            
            <table id="example" class="display" style="width:100%">
                <thead>
                    <tr>
                        <th>Name</th>
                        <th>Category</th>
                        <th>Price</th>
                        <th>Stock</th>
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
                    url: '/api/data',
                    type: 'POST'
                },
                columns: [
                    { data: 'name' },
                    { data: 'category' },
                    { data: 'price', render: function(data) { return '$' + parseFloat(data).toFixed(2); } },
                    { data: 'stock' }
                ],
                rowGroup: {
                    dataSrc: 'category',
                    startRender: function(rows, group) {
                        var groupData = rows.data();
                        var totalPrice = 0;
                        var totalStock = 0;
                        
                        for (var i = 0; i < groupData.length; i++) {
                            totalPrice += parseFloat(groupData[i].price);
                            totalStock += parseInt(groupData[i].stock);
                        }
                        
                        return $('<tr/>')
                            .append('<td colspan="4"><strong>' + group + 
                                   ' (' + rows.count() + ' items, Total Value: $' + 
                                   totalPrice.toFixed(2) + ', Total Stock: ' + totalStock + ')</strong></td>');
                    },
                    endRender: function(rows, group) {
                        return $('<tr/>')
                            .append('<td colspan="4" style="text-align: right; font-style: italic;">End of ' + group + ' group</td>');
                    }
                },
                order: [[1, 'asc']]  // Order by category to group properly
            });
        });
        </script>
    </body>
    </html>
    """
    return render_template_string(template)

@app.route('/api/data', methods=['POST'])
def api_data():
    """API endpoint for DataTables with RowGroup extension support."""
    
    # Define data fields
    data_fields = [
        DataField('name', 'string'),
        DataField('category', 'string'),
        DataField('price', 'number'),
        DataField('stock', 'number')
    ]
    
    # Create DataTables instance
    dt = DataTables(
        db,
        'products',
        request.form.to_dict(),
        data_fields=data_fields
    )
    
    # Return JSON response
    return jsonify(dt.get_rows())

if __name__ == '__main__':
    print("Starting RowGroup extension example...")
    print("Visit http://localhost:5000 to see the demo")
    app.run(debug=True)