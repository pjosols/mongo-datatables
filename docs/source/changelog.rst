=========
Changelog
=========

1.1.1 (2025-03-16)
------------------

* Added new ``DataField`` class for improved field type management, nested field support, and UI-to-database field mapping
* Implemented index-optimized fast text searches that automatically utilize MongoDB text indexes when available
* Added advanced date filtering with comparison operators (>, <, >=, <=, =) for date fields
* Added support for quoted string searches (preserves phrases as single search terms)
* Enhanced search implementation with exact phrase matching for quoted terms
* Improved handling of complex search queries with multiple terms
* Added comparison operators (>, <, >=, <=, =) for numeric field-specific searches
* Added debug mode with detailed query statistics for performance monitoring and troubleshooting

1.0.1 (2025-03-13)
------------------

* Fixed path collision error when using dotted notation for nested fields in projection

1.0.0 (2025-03-11) [YANKED]
---------------------------

.. warning::
   This release was yanked from PyPI due to a critical bug with nested document field handling that could cause path collision errors. Please use version 1.0.1 or later.

* First stable release of the rewritten package
* Comprehensive docstrings and inline documentation
* Basic support for regex-based searches (not optimized for large collections)
* Added support for nested document field handling
* Improved error handling and validation
* Extensive test coverage
* Documentation on Read the Docs

0.3.0 (2019-07-17)
------------------

* Added native DataTables column search functionality
* Implemented support for column-specific regex or exact matching
* Improved search handling with proper precedence for different search types
* Enhanced documentation with references to DataTables server-side documentation
* Performance and stability improvements

0.2.6 (2018-08-23)
------------------

* Improved type handling with explicit checks for lists, dictionaries, and floats
* Enhanced JSON serialization for specific data types
* Performance optimizations
* Stability improvements

0.2.5 (2017-09-26)
------------------

* Improved error handling in Editor by catching TypeError exceptions
* Enhanced JSON parsing resilience for complex data types
* Bug fixes and stability improvements

0.2.4 (2017-09-23)
------------------

* Documentation updates
* Minor code refinements
* Stability improvements

0.2.3 (2017-09-23)
------------------

* Major performance improvement: Switched to MongoDB aggregation pipeline for results
* Changed `length` property to `limit` with proper handling of None value
* Enhanced projection to use `$ifNull` to handle missing fields gracefully
* Improved data filtering in Editor to ignore empty values
* Editor now filters out keys with empty values before insert/update

0.2.2 (2017-09-18)
------------------

* Documentation improvement for request_args parameter, clarifying use with Flask
* Minor code refinements

0.2.1 (2017-09-17)
------------------

* Improved validation of search terms with colon syntax
* Minor refinements to code structure

0.2.0 (2017-09-17)
------------------

* Added JSON handling for complex data types
* Proper JSON encoding of complex objects in results
* Introduced support for MongoDB ObjectId handling
* Class naming inconsistency between imports and implementations
* Added support for Flask integration

0.1.4 (2017-09-14)
------------------

* Initial implementation with:
* Basic DataTables server-side processing
* Support for MongoDB integration
* Simple filtering and sorting
* Custom filter support
* Basic Editor implementation for CRUD operations
* Basic type conversion for data fields