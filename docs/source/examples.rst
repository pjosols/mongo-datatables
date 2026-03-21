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
    from mongo_datatables import DataTables, Editor, DataField

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

        fields = [
            DataField("Title", "string"),
            DataField("PublisherInfo.Date", "date", alias="Published"),
            DataField("Pages", "number"),
            DataField("Rating", "number"),
            DataField("Themes", "array"),
        ]

        result = Editor(mongo, 'books', data, doc_id, data_fields=fields).process()
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


Custom Search Inputs with Colon Syntax
=======================================

Drive field-specific searches from custom inputs using the colon syntax.
Comparison operators (``>``, ``>=``, ``<``, ``<=``, ``=``) work on ``number``
and ``date`` fields:

.. code-block:: javascript

    // Field-contains search driven by a text input
    $('#artist-search').on('keyup', function() {
        table.search('artist:' + $(this).val()).draw();
    });

    // Exact-match on a categorical field
    $('#status-active').on('click', function() {
        table.search('status:active').draw();
    });

    // Numeric comparison: only rows where year > value
    $('#year-from').on('change', function() {
        table.search('year:>=' + $(this).val()).draw();
    });

    // Date range using two inputs
    function applyDateRange() {
        var from = $('#date-from').val();
        var to   = $('#date-to').val();
        var term = '';
        if (from) term += 'release_date:>=' + from + ' ';
        if (to)   term += 'release_date:<=' + to;
        table.search(term.trim()).draw();
    }
    $('#date-from, #date-to').on('change', applyDateRange);

