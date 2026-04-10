"""Domain-specific exceptions for mongo-datatables."""


class MongoDataTablesError(Exception):
    """Indicate a mongo-datatables error."""
    pass


class InvalidDataError(MongoDataTablesError):
    """Indicate invalid or malformed input data."""
    pass


class DatabaseOperationError(MongoDataTablesError):
    """Indicate a MongoDB operation failure."""
    pass


class FieldMappingError(MongoDataTablesError):
    """Indicate field mapping or type conversion failure."""
    pass


class QueryBuildError(MongoDataTablesError):
    """Indicate query construction failure."""
    pass
