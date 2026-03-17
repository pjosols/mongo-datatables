==========
DataTables
==========

Overview
========

The ``DataTables`` class provides server-side processing for MongoDB integration with jQuery DataTables.
It handles pagination, sorting, searching, and filtering with optimizations for large datasets.

Class Documentation
===================

.. autoclass:: mongo_datatables.datatables.DataField
   :members:
   :undoc-members:

.. autoclass:: mongo_datatables.datatables.DataTables
   :members:
   :undoc-members:
   :show-inheritance:

Using DataField
===============

The ``DataField`` class provides a powerful way to define and manage your MongoDB fields.
While not strictly required for basic functionality, it offers significant advantages for
complex data structures, type handling, and UI integration.

Basic Usage
-----------

.. code-block:: python

    from mongo_datatables import DataTables, DataField

    fields = [
        DataField("Title", "string"),
        DataField("Author", "string"),
        DataField("PublisherInfo.Date", "date", alias="Published"),
        DataField("Pages", "number")
    ]

    dt = DataTables(mongo, "books", request.get_json(), data_fields=fields)

Key Benefits
------------

1. **Type-Aware Operations**

   DataField ensures each field's data type is properly handled during searching,
   sorting, and filtering (e.g., date ranges vs. text searches).

2. **UI/Database Field Mapping**

   Map user-friendly field names in your UI to actual database field paths:

   .. code-block:: python

       # In the UI, this appears as "Published"
       # In the database, it's stored as "PublisherInfo.Date"
       DataField("PublisherInfo.Date", "date", alias="Published")

3. **Nested Field Support**

   Easily work with nested document structures using dot notation:

   .. code-block:: python

       DataField("Publisher.Name", "string")
       DataField("Publisher.Location.City", "string", alias="City")

4. **Validation**

   DataField validates field types against supported types:
   ``string, number, date, boolean, array, object, objectid, null``

Search Capabilities
===================

1. **Global Search**

   The standard DataTables search box performs a global search across all searchable columns.

2. **Quoted Phrase Search**

   Enclose terms in double quotes to search for exact phrases:

   .. code-block:: javascript

       "Bob Smith"  // Searches for the exact phrase 'Bob Smith'

3. **Field-Specific Search**

   Use ``field:value`` syntax to search within a specific field:

   .. code-block:: javascript

       "name:john status:active"

4. **Comparison Operators**

   For numeric and date fields, use comparison operators in field-specific searches:

   .. code-block:: javascript

       "price:>100"
       "created_at:<2025-01-01"

5. **Combined Terms**

   .. code-block:: javascript

       "John Smith" status:active department:sales

Type-Aware Search
-----------------

- **Date fields**: Supports date comparison operations (>, <, >=, <=) and date range searches
- **Numeric fields**: Supports numeric comparison (>, <, >=, <=) and range searches
- **Text fields**: Uses regex search with case-insensitivity by default

.. code-block:: python

    data_fields = [
        DataField('created_at', 'date'),
        DataField('price', 'number'),
        DataField('is_active', 'boolean')
    ]

    results = DataTables(mongo, 'products', data, data_fields=data_fields).get_rows()

Performance Tips
================

1. **Create a text index** for efficient global search on large collections:

   .. code-block:: python

       db.collection.create_index([
           ('name', 'text'),
           ('description', 'text')
       ])

2. **Create regular indexes** for fields used in sorting and filtering:

   .. code-block:: python

       db.collection.create_index('created_at')
       db.collection.create_index('status')

3. **Use field-specific search** (``field:value``) for better performance over global search
   on large collections.

4. **Index notes**:

   - Text indexes are used for global searches when ``use_text_index=True`` (the default)
   - Field-specific searches use regular indexes, not text indexes
   - MongoDB allows only one text index per collection, but it can span multiple fields
   - For collections over 100,000 documents, text indexes are strongly recommended

   .. code-block:: python

       # Force regex search even if a text index exists
       datatables = DataTables(mongo, 'collection', request_args, use_text_index=False)
