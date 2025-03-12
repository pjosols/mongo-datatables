========
Examples
========

This page provides various examples of using mongo-datatables in different scenarios.

Basic DataTables Setup
=====================

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
=============================

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
===================

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
===========================

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
=====================

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

Using with Django
===============

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

Using with FastAPI
================

Example with FastAPI:

.. code-block:: python

    from fastapi import FastAPI, Request
    from motor.motor_asyncio import AsyncIOMotorClient
    from mongo_datatables import DataTables

    app = FastAPI()
    client = AsyncIOMotorClient('mongodb://localhost:27017')
    db = client.my_database

    @app.post('/api/data')
    async def get_data(request: Request):
        data = await request.json()
        results = DataTables(db, 'users', data).get_rows()
        return results

Custom Search Fields
==================

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