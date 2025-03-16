========
Examples
========

This page provides various examples of using mongo-datatables in different scenarios.

Basic DataTables Setup
======================

A simple setup with Flask and PyMongo:

.. code-block:: python

    from flask import Flask, render_template, request, jsonify
    from flask_pymongo import PyMongo
    from mongo_datatables import DataTables

    app = Flask(__name__)
    app.config["MONGO_URI"] = "mongodb://localhost:27017/myDatabase"
    mongo = PyMongo(app)

    @app.route('/api/data', methods=['POST'])
    def get_data():
        data = request.get_json()
        results = DataTables(mongo, 'users', data).get_rows()
        return jsonify(results)

Filtering with Custom Criteria
==============================

Add custom MongoDB filtering criteria:

.. code-block:: python

    @app.route('/api/active-users', methods=['POST'])
    def get_active_users():
        data = request.get_json()
        results = DataTables(
            mongo,
            'users',
            data,
            status='active',  # Only return active users
            age={'$gte': 18}  # Only users 18 and older
        ).get_rows()
        return jsonify(results)

Date Range Filtering
====================

Filter documents based on date ranges:

.. code-block:: python

    from datetime import datetime, timedelta

    @app.route('/api/recent-orders', methods=['POST'])
    def get_recent_orders():
        data = request.get_json()

        # Get orders from the last 30 days
        thirty_days_ago = datetime.now() - timedelta(days=30)

        results = DataTables(
            mongo,
            'orders',
            data,
            order_date={'$gte': thirty_days_ago}
        ).get_rows()
        return jsonify(results)

Working with Nested Documents
=============================

MongoDB supports nested documents, and mongo-datatables handles them using dot notation:

.. code-block:: python

    # Example MongoDB document
    {
        "_id": ObjectId("..."),
        "name": "John Doe",
        "contact": {
            "email": "john@example.com",
            "phone": "555-1234"
        },
        "addresses": [
            {
                "type": "home",
                "street": "123 Main St",
                "city": "Anytown"
            },
            {
                "type": "work",
                "street": "456 Business Ave",
                "city": "Commerce City"
            }
        ]
    }

    # In your HTML/JavaScript, define columns with dot notation
    columns: [
        { data: 'name' },
        { data: 'contact.email' },
        { data: 'contact.phone' },
        { data: 'addresses.0.city' }  # First address city
    ]

Advanced Editor Example
=======================

Complete DataTables Editor integration with custom fields:

.. code-block:: python

    @app.route('/api/editor', methods=['POST'])
    def editor_endpoint():
        data = request.get_json()
        doc_id = request.args.get('id', '')
        result = Editor(mongo, 'users', data, doc_id).process()
        return jsonify(result)

.. code-block:: javascript

    $(document).ready(function() {
        var editor = new $.fn.dataTable.Editor({
            ajax: {
                url: '/api/editor',
                type: 'POST',
                contentType: 'application/json',
                data: function(d) {
                    return JSON.stringify(d);
                }
            },
            table: '#example',
            fields: [
                {
                    label: 'Name',
                    name: 'name'
                },
                {
                    label: 'Email',
                    name: 'contact.email',  // Nested field
                    type: 'email'
                },
                {
                    label: 'Status',
                    name: 'status',
                    type: 'select',
                    options: [
                        { label: 'Active', value: 'active' },
                        { label: 'Inactive', value: 'inactive' },
                        { label: 'Pending', value: 'pending' }
                    ]
                },
                {
                    label: 'Notes',
                    name: 'notes',
                    type: 'textarea'
                }
            ]
        });

        // Initialize DataTable with Editor
        $('#example').DataTable({
            dom: 'Bfrtip',
            processing: true,
            serverSide: true,
            ajax: { /* ... */ },
            columns: [ /* ... */ ],
            select: true,
            buttons: [
                { extend: 'create', editor: editor },
                { extend: 'edit', editor: editor },
                { extend: 'remove', editor: editor }
            ]
        });
    });

Using with Flask
================

mongo-datatables works perfectly with Flask and Flask-PyMongo:

.. code-block:: python

    from flask import Flask, render_template, request, jsonify
    from flask_pymongo import PyMongo
    from mongo_datatables import DataTables, Editor

    app = Flask(__name__)
    app.config["MONGO_URI"] = "mongodb://localhost:27017/myDatabase"
    mongo = PyMongo(app)

    @app.route('/')
    def index():
        return render_template('index.html')

    # DataTables server-side processing
    @app.route('/api/books', methods=['POST'])
    def get_books():
        data = request.get_json()
        results = DataTables(mongo, 'books', data).get_rows()
        return jsonify(results)

    # Editor operations (create, edit, delete)
    @app.route('/api/editor/books', methods=['POST'])
    def edit_books():
        data = request.get_json()
        doc_id = request.args.get('id', '')

        field_types = {
            "Title": "text",
            "PublisherInfo.Date": "date",
            "Pages": "number",
            "Rating": "number",
            "Themes": "array"
        }

        result = Editor(mongo, 'books', data, doc_id, field_types=field_types).process()
        return jsonify(result)

    if __name__ == '__main__':
        app.run(debug=True)

Using with Django
=================

mongo-datatables also works with Django and django-pymongo:

.. code-block:: python

    from django.http import JsonResponse
    from django.views.decorators.csrf import csrf_exempt
    import json
    from pymongo import MongoClient
    from mongo_datatables import DataTables

    # Connect to MongoDB
    client = MongoClient('mongodb://localhost:27017/')
    db = client.my_database

    @csrf_exempt
    def get_data(request):
        data = json.loads(request.body)
        results = DataTables(db, 'users', data).get_rows()
        return JsonResponse(results)


Custom Search Fields
====================

You can customize search behavior by adding specific search patterns in your JavaScript:

.. code-block:: javascript

    // Add a custom search input to search by email
    $('#email-search').on('keyup', function() {
        var value = $(this).val();
        table.search('email:' + value).draw();
    });

    // Add status filter buttons
    $('#status-active').on('click', function() {
        table.search('status:active').draw();
    });

    $('#status-inactive').on('click', function() {
        table.search('status:inactive').draw();
    });

Handling Numeric Values
=======================

When working with numeric values in DataTables and MongoDB, there are important considerations regarding how numbers are processed between JavaScript, Python, and MongoDB.

The Issue: Floating Point Numbers with Zero Decimal Places
----------------------------------------------------------

JavaScript doesn't distinguish between integers and floating-point numbers when the decimal part is zero. If you define a value as ``5.0`` in JavaScript, it's treated internally as just ``5`` (an integer).

This can cause inconsistencies when:

1. You want to display numeric values consistently with decimal places (e.g., always showing "5.0" instead of "5")
2. You need to preserve the exact numeric type (float vs. integer) in your database

How DataTables Processes Numeric Values
---------------------------------------

When you define select options in DataTables Editor:

.. code-block:: javascript

    options: [
        { label: "★★½ (2.5)", value: 2.5 },
        { label: "★★★ (3.0)", value: 3.0 },
        { label: "★★★★★ (5.0)", value: 5.0 }
    ]

Here's what happens:

1. For values with non-zero decimal parts (like 2.5), JavaScript maintains them as floating-point
2. For values with zero decimal parts (like 3.0 or 5.0), JavaScript converts them to integers (3 or 5)
3. When sending to the server, they're sent as numeric JSON values, not strings

Solutions for Consistent Numeric Handling
-----------------------------------------

Option 1: Force values to be strings in DataTables (Recommended)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

This approach ensures consistent handling by always sending strings to the server:

.. code-block:: javascript

    options: [
        { label: "★★½ (2.5)", value: "2.5" },
        { label: "★★★ (3.0)", value: "3.0" },
        { label: "★★★★★ (5.0)", value: "5.0" }
    ]

**Advantages:**

- Strings are processed through the string-to-number conversion in the Editor class
- Values with decimal points are properly recognized as floats
- The display format is preserved

Option 2: Format Display Values in the Table
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

If you only care about consistent display but not storage type:

.. code-block:: javascript

    {
        data: 'Rating',
        render: function(data) {
            return parseFloat(data).toFixed(1);  // Always display with one decimal place
        }
    }

When to Use Each Approach
-------------------------

1. **Use Option 1 (string values)** when:

   - You want consistent handling of float vs. integer without modifying your backend
   - You need to preserve the decimal format for storage and display
   - You want the actual stored value to maintain its decimal precision

2. **Use Option 2 (display formatting)** when:

   - You only care about display consistency
   - The actual storage format (int vs. float) doesn't matter
   - You prefer working with native JavaScript numeric values in your code

MongoDB Number Storage Behavior
-------------------------------

MongoDB internally optimizes numeric storage:

- Integers are stored as 32-bit or 64-bit integers
- Decimals with zero fractional parts (``5.0``) are typically stored as integers (``5``)

This is normal behavior and usually doesn't affect functionality, but it can impact how numbers are returned and displayed if you rely on type exactness.

Best Practice Recommendation
----------------------------

For consistent handling of numeric values with decimal places, use **Option 1** and define Editor values as strings. This provides the cleanest solution with the least chance of inconsistency across the JavaScript-Python-MongoDB pipeline.