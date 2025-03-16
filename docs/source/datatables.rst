==========
DataTables
==========

Overview
========

The ``DataTables`` class provides server-side processing for MongoDB integration with jQuery DataTables.
It handles all aspects of integrating MongoDB with DataTables, including pagination, sorting, searching,
and filtering with optimizations for large datasets.

Class Documentation
===================

.. py:class:: mongo_datatables.datatables.DataField(name, data_type, alias=None)

   Represents a data field with MongoDB and DataTables column mapping.
   
   This class defines a field name in MongoDB with its full path (including parent objects),
   a column alias mapping for DataTables, and type information for proper data handling
   and optimized searching.

   :param name: The full field path in MongoDB (e.g., 'Title' or 'PublisherInfo.Date')
   :param data_type: The data type of the field (must be a valid MongoDB type)
   :param alias: Optional UI display name (defaults to the field name if not provided)

.. py:class:: mongo_datatables.datatables.DataTables(pymongo_object, collection_name, request_args, data_fields=None, use_text_index=True, debug_mode=False, **custom_filter)

   Server-side processor for MongoDB integration with jQuery DataTables.

   This class handles all aspects of server-side processing including:

   - Pagination
   - Sorting
   - Global search across multiple columns
   - Column-specific search
   - Custom filters
   - Type-aware search operations
   - Performance optimization with MongoDB text indexes
   - UI field name to database field name mapping

   :param pymongo_object: PyMongo client connection or Flask-PyMongo instance
   :param collection_name: Name of the MongoDB collection
   :param request_args: DataTables request parameters (typically from request.get_json())
   :param data_fields: List of DataField objects defining database fields with UI mappings
   :param use_text_index: Whether to use text indexes when available (default: True)
   :param debug_mode: Whether to collect and return debug information (default: False)
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

Key Properties
==============

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
=================

Using DataField
==============

The ``DataField`` class provides a powerful way to define and manage your MongoDB fields when using mongo-datatables. While not strictly required for basic functionality, it offers significant advantages for complex data structures, type handling, and UI integration.

Basic Usage
-----------

.. code-block:: python

    from mongo_datatables import DataTables, DataField

    # Define your data fields
    fields = [
        DataField("Title", "string"),
        DataField("Author", "string"),
        DataField("PublisherInfo.Date", "date", alias="Published"),
        DataField("Pages", "number")
    ]

    # Create DataTables instance with these fields
    dt = DataTables(mongo, "books", request.get_json(), data_fields=fields)

Key Benefits
-----------

1. **Type-Aware Operations**
   
   DataField ensures that each field's data type is properly handled during:
   - Searching (e.g., date ranges vs. text searches)
   - Sorting (e.g., numeric vs. lexicographic ordering)
   - Filtering (e.g., applying appropriate operators)

2. **UI/Database Field Mapping**
   
   Map user-friendly field names in your UI to actual database field paths:

   .. code-block:: python

       # In the UI, this appears as "Published"
       # In the database, it's stored as "PublisherInfo.Date"
       DataField("PublisherInfo.Date", "date", alias="Published")

3. **Nested Field Support**
   
   Easily work with nested document structures:

   .. code-block:: python

       # Access nested fields with dot notation
       DataField("Publisher.Name", "string")
       DataField("Publisher.Location.City", "string", alias="City")

4. **Validation**
   
   DataField validates field types against MongoDB's supported types:
   ``string, number, date, boolean, array, object, objectId, null``

Legacy Support
-------------

For backward compatibility, mongo-datatables still supports the older approach using a simple dictionary:

.. note::

    Always use the DataField approach for defining field types and mappings. This provides better type safety, validation, and explicit field mapping capabilities.

Best Practices
-------------

1. Always define ``DataField`` objects for all queryable fields
2. Use appropriate data types to ensure optimal query performance
3. Provide user-friendly aliases for complex field paths
4. For nested fields, always use the full path with dot notation

Search Capabilities
=================

The DataTables processor provides a rich set of search capabilities that can be used to filter data efficiently:

1. **Global Search**
   
   The standard search box in DataTables performs a global search across all searchable columns:
   
   .. code-block:: javascript
   
       // In the DataTable search box:
       "smith"  // Searches for 'smith' in all searchable columns

2. **Quoted Phrase Search**
   
   Enclose terms in double quotes to search for exact phrases. This is particularly useful for names, addresses, or any multi-word terms:
   
   .. code-block:: javascript
   
       // In the DataTable search box:
       "Bob Smith"  // Searches for the exact phrase 'Bob Smith'
       
   When using quoted phrases, the search will match the exact sequence of words rather than treating them as separate search terms.

3. **Field-Specific Search**
   
   Use the `field:value` syntax to search within specific fields for more targeted and efficient queries:
   
   .. code-block:: javascript
   
       // In the DataTable search box:
       "name:john status:active"  // Searches for 'john' in the name field and 'active' in the status field

4. **Comparison Operators**
   
   For numeric and date fields, you can use comparison operators (>, <, >=, <=, =) in field-specific searches:
   
   .. code-block:: javascript
   
       // In the DataTable search box:
       "price:>100"  // Finds records with price greater than 100
       "created_at:<2025-01-01"  // Finds records created before January 1, 2025

5. **Combined Search Terms**
   
   You can combine multiple search terms, quoted phrases, and field-specific searches in a single query:
   
   .. code-block:: javascript
   
       // In the DataTable search box:
       "John Smith" status:active department:sales  // Searches for the exact phrase 'John Smith' AND status='active' AND department='sales'

Optimization Notes:

* Text indexes are automatically used for global searches and quoted phrase searches when available
* Field-specific searches use regular indexes for optimal performance
* For best performance with large collections, prefer field-specific searches over global searches

.. note::
    Combining field-specific searches with global searches may result in complex queries that don't use indexes optimally

Type-Aware Search
-----------------

The DataTables processor supports specialized handling for different field types:

- **Date fields**: Supports date comparison operations (>, <, >=, <=) and date range searches
- **Numeric fields**: Supports numeric comparison (>, <, >=, <=) and range searches (e.g., "10-20")
- **Text fields**: Uses regex search with case-insensitivity by default

To use type-aware search, provide DataField objects during initialization:

.. code-block:: python

    data_fields = [
        DataField('created_at', 'date'),
        DataField('price', 'number'),
        DataField('is_active', 'boolean')
    ]

    results = DataTables(mongo, 'products', data, data_fields=data_fields).get_rows()

Optimized Search Performance
----------------------------

DataTables processor implements several advanced optimizations for large datasets:

1. **Index-aware search strategy**: Automatically detects and utilizes available MongoDB text indexes
2. **Smart query planning**: Dynamically chooses between text index queries and regex searches based on query complexity and available indexes
3. **Type-aware filtering**: Uses the ``DataField`` type information to apply appropriate search operators
4. **Optimized aggregation pipeline**: Structures the MongoDB pipeline for maximum performance
5. **Efficient date filtering**: Specialized handling for date comparisons and ranges

Performance Comparison:

+----------------------+--------------------+----------------------+
| Collection Size      | Regex Search       | Index-Optimized      |
+======================+====================+======================+
| < 10,000 documents   | Fast               | Fast                 |
+----------------------+--------------------+----------------------+
| 10,000-100,000       | Moderate           | Fast                 |
+----------------------+--------------------+----------------------+
| > 100,000            | Slow               | Fast                 |
+----------------------+--------------------+----------------------+
| > 1,000,000          | Very slow/timeout  | Fast                 |
+----------------------+--------------------+----------------------+

Field-specific search syntax (field:value) offers better performance than global search for large collections:

.. code-block:: javascript

    // In the DataTable search box:
    "status:active price:>100"  // Will efficiently search only status and price fields

Performance Optimization with Indexes
-------------------------------------

When working with large MongoDB collections, creating proper indexes is **critical** for performance. Without appropriate indexes, queries can become extremely slow or timeout entirely, especially when using DataTables with server-side processing.

Text Indexes for Search Performance
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

In version 1.1.0, the ``DataTables`` class has been enhanced to intelligently leverage MongoDB text indexes for fast, efficient search operations. When a text index is available, it will automatically be used for search queries, providing significant performance benefits:

.. code-block:: python

    # Create a text index in MongoDB (do this once in your setup script)
    db.your_collection.create_index([("field1", "text"), ("field2", "text")])

    # DataTables will automatically use the text index when available
    datatables = DataTables(mongo, 'your_collection', request_args, use_text_index=True)

Benefits of text indexes:

* **Dramatically faster search** on large collections (10-100x performance improvement)
* **Better relevance scoring** for search results
* **Language-aware stemming** for more natural search
* **Support for exact phrase queries** using quotes

Index-Optimized vs. Regex Search
-----------------------------

The library now implements a dual-mode search strategy:

1. **Index-Optimized Search** (Fast)
   * Used when text indexes exist on the collection
   * Extremely fast, even on collections with millions of documents
   * Whole-word matching only (no partial word matches)
   * Supports exact phrase matching with quotes
   * Automatically detects available text indexes
   
2. **Regex-Based Search** (Flexible)
   * Used when no text indexes exist or when ``use_text_index=False``
   * Supports partial word matching (e.g., searching for 'prog' will match 'programming')
   * Slower performance on large collections
   * More flexible for complex search patterns

You can control this behavior with the ``use_text_index`` parameter:

.. code-block:: python

    # Force regex search even if text indexes exist
    datatables = DataTables(mongo, 'your_collection', request_args, use_text_index=False)

.. note::
   MongoDB has a limit of one text index per collection, but you can include multiple fields in a single text index.
   
.. warning::
   For collections with more than 100,000 documents, text indexes are strongly recommended.
   Regex searches on unindexed fields with large collections can cause significant performance issues.

Regular Indexes for Sorting and Filtering
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

In addition to text indexes, create regular indexes for fields used in sorting and filtering:

.. code-block:: python

    # Create indexes for commonly sorted/filtered fields
    db.your_collection.create_index("created_at")
    db.your_collection.create_index("status")

Fields that are frequently used in sorting operations (via the ``order`` parameter in DataTables) or filtering conditions should be indexed to improve query performance.

Query Statistics for Debugging
------------------------------

In version 1.1.0, the DataTables processor includes enhanced query statistics tracking to help debug and optimize your MongoDB queries. These statistics provide detailed insights into query execution, including whether text indexes are being utilized.

To enable detailed statistics, use the ``debug_mode`` parameter when creating the DataTables instance:

.. code-block:: python

    # Enable debug mode to collect detailed statistics
    results = DataTables(mongo, 'users', data, debug_mode=True).get_rows()
    query_stats = results['_query_stats']  # Access the query statistics
    
    # Example of logging search performance
    if query_stats['text_index_used']:
        print(f"Using text index for search: {query_stats['search_term']}")
        print(f"Execution time: {query_stats['execution_time_ms']} ms")
    else:
        print("Using regex search - consider adding a text index")
        print(f"Execution time: {query_stats['execution_time_ms']} ms")

Available Statistics
^^^^^^^^^^^^^^^^^^^^

The ``_query_stats`` dictionary includes the following information when ``debug_mode=True``:

* ``text_index_used``: Boolean indicating whether MongoDB text indexes were used for the search
* ``search_term``: The processed search term
* ``execution_time_ms``: Query execution time in milliseconds
* ``total_records``: Total number of records in the collection
* ``filtered_records``: Number of records after applying filters
* ``query_filter``: The MongoDB filter that was generated
* ``aggregation_pipeline``: The complete aggregation pipeline used
* ``search_type``: The type of search being performed (e.g., 'exact_phrase', 'text', 'regex')
* ``date_filters``: Any date-specific filters that were applied
* ``used_standard_index``: Boolean indicating whether standard indexes were used
* ``sorted_fields``: List of fields used for sorting

This information is particularly valuable when optimizing queries for large collections, as it helps you understand whether your queries are efficiently using indexes or falling back to less efficient methods like regex searches.

Advanced Date Filtering
---------------------

In version 1.1.0, mongo-datatables introduces enhanced date filtering capabilities with comparison operators for date fields. This allows for more precise date-based queries directly from the DataTables search interface.

Supported Date Comparison Operators:

* ``>`` - Greater than (after date)
* ``<`` - Less than (before date)
* ``>=`` - Greater than or equal to (on or after date)
* ``<=`` - Less than or equal to (on or before date)
* ``=`` - Equal to (exact date match)

Example Usage in Search Box:

.. code-block:: javascript

    // In the DataTable search box:
    "created_at:>2025-01-01"  // Find records created after January 1, 2025
    "created_at:<2025-03-15"  // Find records created before March 15, 2025
    "created_at:>=2025-01-01 created_at:<=2025-03-15"  // Date range (between Jan 1 and Mar 15)

Date Format Support:

* ISO format: ``YYYY-MM-DD`` (e.g., 2025-03-15)
* Time components are also supported: ``YYYY-MM-DD HH:MM:SS``

Implementation with DataField:

.. code-block:: python

    from mongo_datatables import DataTables, DataField
    
    # Define date fields
    fields = [
        DataField("created_at", "date"),
        DataField("updated_at", "date")
    ]
    
    # DataTables will automatically handle date comparison operators
    dt = DataTables(mongo, "users", request.get_json(), data_fields=fields)

The date filtering functionality works seamlessly with the ``DataField`` class for proper date type handling.

Practical Query Statistics Usage
-----------------------------

The query statistics feature in version 1.1.0 provides valuable insights for performance monitoring, debugging, and optimization. Here are practical examples of how to use this feature in your applications:

1. **Performance Monitoring Dashboard**

   Create a performance dashboard to track query performance over time:

   .. code-block:: python

       @app.route('/api/data', methods=['POST'])
       def get_data():
           start_time = time.time()
           data = request.get_json()
           
           # Enable debug mode to collect statistics
           results = DataTables(mongo, 'users', data, debug_mode=True).get_rows()
           stats = results.pop('_query_stats', {})  # Remove stats before sending to client
           
           # Log statistics for monitoring
           log_entry = {
               'timestamp': datetime.now(),
               'collection': 'users',
               'search_term': stats.get('search_term', ''),
               'execution_time_ms': stats.get('execution_time_ms', 0),
               'total_records': stats.get('total_records', 0),
               'filtered_records': stats.get('filtered_records', 0),
               'text_index_used': stats.get('text_index_used', False),
               'query_complexity': len(str(stats.get('query_filter', {})))
           }
           
           # Store in a separate collection for analysis
           mongo.db.query_stats.insert_one(log_entry)
           
           return jsonify(results)

2. **Automatic Index Recommendation**

   Analyze query patterns to recommend indexes:

   .. code-block:: python

       def analyze_query_performance():
           # Find slow queries (execution time > 100ms)
           slow_queries = mongo.db.query_stats.find({
               'execution_time_ms': {'$gt': 100},
               'text_index_used': False
           })
           
           # Group by search fields to identify candidates for indexing
           index_candidates = {}
           for query in slow_queries:
               search_term = query.get('search_term', '')
               if ':' in search_term:  # Field-specific search
                   field = search_term.split(':', 1)[0]
                   if field not in index_candidates:
                       index_candidates[field] = 0
                   index_candidates[field] += 1
           
           # Recommend indexes for frequently searched fields
           recommendations = []
           for field, count in sorted(index_candidates.items(), key=lambda x: x[1], reverse=True):
               if count >= 10:  # Threshold for recommendation
                   recommendations.append(f"db.users.createIndex({{'{field}': 1}})")
           
           return recommendations

3. **Real-time Performance Alerts**

   Set up alerts for performance degradation:

   .. code-block:: python

       def check_query_performance(stats):
           # Alert on slow queries
           if stats.get('execution_time_ms', 0) > 500:
               # Send alert (email, Slack, etc.)
               send_alert(
                   f"Slow query detected: {stats.get('execution_time_ms')}ms"
               )

4. **Client-Side Performance Indicator**

   Provide users with feedback about query performance:

   .. code-block:: python

       @app.route('/api/data', methods=['POST'])
       def get_data():
           data = request.get_json()
           results = DataTables(mongo, 'users', data, debug_mode=True).get_rows()
           
           # Add performance indicator for client
           execution_time = results.get('_query_stats', {}).get('execution_time_ms', 0)
           results['performance_indicator'] = {
               'execution_time_ms': execution_time,
               'performance_rating': 'good' if execution_time < 100 else 'fair' if execution_time < 500 else 'poor'
           }
           
           # Remove detailed stats
           results.pop('_query_stats', None)
           
           return jsonify(results)

These examples demonstrate how to leverage the query statistics feature for monitoring, optimization, and providing feedback to users about query performance.

Example Usage
^^^^^^^^^^^^^

.. code-block:: python

    @app.route('/api/data', methods=['POST'])
    def get_data():
        data = request.get_json()
        results = DataTables(mongo, 'users', data).get_rows()
        
        # Log query statistics for performance monitoring
        stats = results['_query_stats']
        app.logger.info(f"Search type: {stats['search_type']}")
        app.logger.info(f"Used text index: {stats['used_text_index']}")
        
        # Remove stats before sending to client (optional)
        del results['_query_stats']
        return jsonify(results)

Performance Tips
================

For large MongoDB collections, consider the following optimizations:

1. **Create both text and regular indexes** for fields used in searches:

   .. code-block:: python

       # Text indexes improve global search performance
       db.collection.create_index([
           ('name', 'text'),
           ('description', 'text')
       ])

       # Regular indexes improve field-specific searches and sorting
       # Important: Create regular indexes even for text-indexed fields
       db.collection.create_index('name')
       db.collection.create_index('description')

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

5. **Important note on index types**:
   - Text indexes are only used for global searches with a single term
   - Field-specific searches (either through field:value syntax or column-specific search) use regular indexes, not text indexes
   - For optimal performance, create both types of indexes for frequently searched fields

Enhanced Search Features
========================

The ``DataTables`` class includes advanced search capabilities that optimize query performance and provide detailed debugging information:

- **Text Index Utilization**: Automatically leverages MongoDB text indexes for significantly improved search performance
- **Query Statistics**: Provides detailed statistics for debugging and performance monitoring
- **Optimized Search Strategies**: Intelligently handles both simple and complex search terms
- **Exact Phrase Matching**: Supports quoted phrases for precise matching

These features are built directly into the ``DataTables`` class and require no special configuration to use.

Example Usage:

.. code-block:: python

    from mongo_datatables import DataTables
    
    @app.route('/api/data', methods=['POST'])
    def get_data():
        data = request.get_json()
        results = DataTables(mongo, 'users', data).get_rows()
        return jsonify(results)

The ``DataTables`` class automatically detects when to use text indexes versus regex searches based on the search terms and available indexes. For quoted phrases, it will use text indexes with exact phrase matching when available, falling back to regex searches when necessary.

Query statistics are included in the response to help you understand and optimize your search performance. See the `Query Statistics for Debugging`_ section for details on accessing and using these statistics.