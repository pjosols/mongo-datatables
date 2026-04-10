"""Domain-specific exceptions for mongo-datatables."""


class MongoDataTablesError(Exception):
    """Base exception for all mongo-datatables errors."""
    pass


class InvalidDataError(MongoDataTablesError):
    """Raised when input data is invalid or malformed."""
    pass


class DatabaseOperationError(MongoDataTablesError):
    """Raised when a MongoDB operation fails."""
    pass


class FieldMappingError(MongoDataTablesError):
    """Raised when field mapping or type conversion fails."""
    pass


class QueryBuildError(MongoDataTablesError):
    """Raised when query construction fails."""
    pass
