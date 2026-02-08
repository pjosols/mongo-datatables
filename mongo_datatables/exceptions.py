"""Custom exceptions for mongo-datatables library.

This module defines domain-specific exceptions for better error handling
and debugging throughout the mongo-datatables library.
"""


class MongoDataTablesError(Exception):
    """Base exception for all mongo-datatables errors.

    All custom exceptions in this library inherit from this base class,
    allowing callers to catch all library-specific errors with a single
    except clause if desired.
    """
    pass


class InvalidDataError(MongoDataTablesError):
    """Raised when input data is invalid or malformed.

    This exception is raised when:
    - Request parameters are missing required fields
    - Document IDs have invalid format (e.g., invalid ObjectId)
    - Data types cannot be parsed or converted
    - Required data is missing for an operation

    Example:
        >>> raise InvalidDataError("Document ID is required for edit operation")
    """
    pass


class DatabaseOperationError(MongoDataTablesError):
    """Raised when a MongoDB operation fails.

    This exception wraps MongoDB errors and provides context about
    what operation was being attempted when the error occurred.

    Example:
        >>> try:
        >>>     collection.insert_one(doc)
        >>> except PyMongoError as e:
        >>>     raise DatabaseOperationError(f"Failed to insert document") from e
    """
    pass


class FieldMappingError(MongoDataTablesError):
    """Raised when field mapping or type conversion fails.

    This exception is raised when:
    - A field name cannot be mapped from UI to database representation
    - Type conversion fails for a specific field
    - Field validation fails

    Example:
        >>> raise FieldMappingError(f"Cannot convert '{value}' to number for field '{field}'")
    """
    pass


class QueryBuildError(MongoDataTablesError):
    """Raised when query construction fails.

    This exception is raised when there are issues building MongoDB
    queries from DataTables request parameters.

    Example:
        >>> raise QueryBuildError("Invalid comparison operator for text field")
    """
    pass
