=========
Changelog
=========

1.0.1 (2025-03-13)
------------------

* Fixed path collision error when using dotted notation for nested fields in projection
* Added recursive processing of nested documents to ensure proper JSON serialization
* Fixed datetime serialization issue to properly handle date objects at any nesting level
* Improved handling of complex MongoDB document structures
* Enhanced Editor handling for nested document fields
* Implemented proper data transformation in Editor for nested fields
* Resolved field mapping issues between Editor and MongoDB documents
* Added support for array field editing in Editor component
* Fixed UI rendering issues with special characters in JavaScript
* Added bidirectional field mapping for Editor to handle both flat and nested data structures

1.0.0 (2025-03-11)
------------------

* First stable release of the rewritten package
* Comprehensive docstrings and inline documentation
* Performance optimizations for large datasets
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