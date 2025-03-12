==========
DataTables
==========

Overview
========

The ``DataTables`` class provides server-side processing for MongoDB integration with jQuery DataTables.
It handles all aspects of integrating MongoDB with DataTables, including pagination, sorting, searching,
and filtering with optimizations for large datasets.

Class Documentation
=================

.. py:class:: mongo_datatables.datatables.DataTables(pymongo_object, collection_name, request_args, field_types=None, **custom_filter)

   Server-side processor for MongoDB integration with jQuery DataTables.

   This class handles all aspects of server-side processing including:

   - Pagination
   - Sorting
   - Global search across multiple columns
   - Column-specific search
   - Custom filters
   - Type-aware search operations
   - Performance optimization with MongoDB text indexes

   :param pymongo_object: PyMongo client connection or Flask-PyMongo instance
   :param collection_name: Name of the MongoDB collection
   :param request_args: DataTables request parameters (typically from request.get_json())
   :param field_types: Optional mapping of field names to their types ('date', 'number', 'boolean', 'text')
   :param \**custom_filter: Additional filtering criteria for MongoDB queries

   .. py:method:: get_rows()

      Get the complete formatted response for DataTables.

      :return: Dictionary containing all required DataTables response fields
      :rtype: dict

      Example response format::

          {
              'recordsTotal': 100,
              'recordsFiltered': 15,
              'draw': 1,
              'data': [
                  {
                      'DT_RowId': '507f1f77bcf86cd799439011',
                      'name': 'John Doe',
                      'email': 'john@example.com',
                      'age': 30
                  },
                  ...
              ]
          }

   .. py:method:: results()

      Execute the MongoDB query and return formatted results.

      :return: List of documents formatted for DataTables response
      :rtype: list

   .. py:method:: _process_value_by_type(field, value)

      Process a search value based on field type.

      Converts string inputs to appropriate types based on field_types mapping.
      Supports range and comparison operations for numeric and date fields.

      :param field: Field name
      :param value: Search value as string
      :return: Processed value with appropriate type or query operator

Key Properties
=============

.. py:attribute:: db

   Get the MongoDB database instance.

   :return: The PyMongo database instance

.. py:attribute:: collection

   Get the MongoDB collection.

   :return: The PyMongo collection instance

.. py:attribute:: has_text_index

   Check if the collection has a text index for optimized text search.

   :return: True if a text index exists, False otherwise

.. py:attribute:: search_terms

   Extract search terms from the DataTables request.

   :return: List of search terms split by whitespace

.. py:attribute:: requested_columns

   Get the list of column names requested by DataTables.

   :return: List of column names

.. py:attribute:: filter

   Build the complete MongoDB filter query, combining custom filters, global search, and column-specific search.
   Optimizes query structure based on available indexes and field types.

   :return: Complete MongoDB query filter

.. py:attribute:: sort_specification

   Build the MongoDB sort specification based on DataTables order request.

   :return: Dictionary for MongoDB sort operation

.. py:attribute:: projection

   Build the MongoDB projection to return requested fields. Uses $ifNull to handle missing fields gracefully.

   :return: MongoDB projection specification

Advanced Features
================

Type-Aware Search
----------------

The DataTables processor supports specialized handling for different field types:

- **Date fields**: Supports date comparison operations (>, <) and date range searches
- **Numeric fields**: Supports numeric comparison (>, <, >=, <=) and range searches (e.g., "10-20")
- **Boolean fields**: Intelligently converts "true", "false", "yes", "no", etc. to proper boolean values
- **Text fields**: Uses regex search with case-insensitivity by default

To use type-aware search, provide a field_types mapping during initialization:

.. code-block:: python

    field_types = {
        'created_at': 'date',
        'price': 'number',
        'is_active': 'boolean'
    }

    results = DataTables(mongo, 'products', data, field_types=field_types).get_rows()

Optimized Search Performance
---------------------------

For large datasets, the DataTables processor implements several optimizations:

1. **Text index utilization**: Automatically uses MongoDB text indexes when available for improved search performance
2. **Efficient query structure**: Prioritizes specific column searches over global searches
3. **Optimized aggregation pipeline**: Structures the MongoDB pipeline for best performance
4. **Type-specific filtering**: Only searches relevant fields based on the search term type

Field-specific search syntax (field:value) offers better performance than global search for large collections:

.. code-block:: javascript

    // In the DataTable search box:
    "status:active price:>100"  // Will efficiently search only status and price fields

Example Usage
============

Basic usage with Flask:

.. code-block:: python

    from flask import Flask, render_template, request, jsonify
    from flask_pymongo import PyMongo
    from mongo_datatables import DataTables

    app = Flask(__name__)
    app.config["MONGO_URI"] = "mongodb://localhost:27017/myDatabase"
    mongo = PyMongo(app)

    @app.route('/table')
    def table_view():
        return render_template('table.html')

    @app.route('/api/data', methods=['POST'])
    def get_data():
        data = request.get_json()
        # Basic usage
        results = DataTables(mongo, 'users', data).get_rows()
        return jsonify(results)

With field type specifications:

.. code-block:: python

    @app.route('/api/data', methods=['POST'])
    def get_data():
        data = request.get_json()
        # Define field types for optimized search
        field_types = {
            'created_at': 'date',
            'last_login': 'date',
            'age': 'number',
            'active': 'boolean'
        }
        results = DataTables(mongo, 'users', data, field_types=field_types).get_rows()
        return jsonify(results)

Advanced filtering with date range:

.. code-block:: python

    from datetime import datetime, timedelta

    @app.route('/api/data', methods=['POST'])
    def get_data():
        data = request.get_json()
        today = datetime.now()
        expiry_date = today + timedelta(days=60)

        # Find contracts expiring in the next 60 days
        results = DataTables(
            mongo,
            'contracts',
            data,
            field_types={'ExpiryDate': 'date'},
            ExpiryDate={'$gt': today, '$lt': expiry_date}
        ).get_rows()
        return jsonify(results)

Performance Tips
===============

For large MongoDB collections, consider the following optimizations:

1. **Create text indexes** for fields commonly used in global search:

   .. code-block:: python

       # Create a text index on multiple fields
       db.collection.create_index([
           ('name', 'text'),
           ('description', 'text')
       ])

2. **Use field_types parameter** to enable type-specific optimizations:

   .. code-block:: python

       field_types = {
           'price': 'number',
           'created_at': 'date',
           'active': 'boolean'
       }

3. **Encourage field-specific searching** with field:value syntax for better performance:

   .. code-block:: javascript

       // More efficient than global search
       "status:active category:electronics price:>100"

4. **Create regular indexes** for fields used in sorting and filtering:

   .. code-block:: python

       # Create indexes for commonly sorted/filtered fields
       db.collection.create_index('price')
       db.collection.create_index('created_at')