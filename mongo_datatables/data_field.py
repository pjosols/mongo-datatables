"""DataField class for MongoDB field definitions with DataTables column mapping."""


class DataField:
    """Represents a data field with MongoDB and DataTables column mapping.

    Maps an alias name to a fully nested field name in MongoDB and specifies
    a field type for optimized searching.

    Data type handling:
        - 'keyword': Exact equality match — no regex, works with a regular MongoDB index.
        - 'number': Supports exact matching and comparison operators (>, <, >=, <=, =)
        - 'date': Supports date parsing and comparison operators for ISO format dates
        - 'objectid': Serialised as a string in the response
        - All other types: Treated as text with case-insensitive regex search

    Attributes:
        name: The full field path in the database (e.g., 'PublisherInfo.Date')
        data_type: The type of data stored in this field
        alias: The name to display in the UI (defaults to the last part of the field path)
    """

    VALID_TYPES = frozenset(
        ['string', 'keyword', 'number', 'date', 'boolean', 'array', 'object', 'objectid', 'null']
    )

    def __init__(self, name: str, data_type: str, alias: str = None) -> None:
        """Initialize a DataField.

        name: The full field path in MongoDB (e.g., 'Title' or 'PublisherInfo.Date')
        data_type: The data type of the field. 'number' and 'date' support comparison
                   operators; 'objectid' has special output formatting; all others use regex.
        alias: Optional UI display name (defaults to the field name if not provided).
        Raises ValueError if name is empty or data_type is invalid.
        """
        if not name or not name.strip():
            raise ValueError("DataField name must be a non-empty string")
        self.name = name

        data_type = data_type.lower()
        if data_type not in self.VALID_TYPES:
            raise ValueError(f"Invalid data_type '{data_type}'. Must be one of: {sorted(self.VALID_TYPES)}")
        self.data_type = data_type

        default_alias = name.split('.')[-1] if '.' in name else name
        self.alias = alias or default_alias

    def __repr__(self) -> str:
        alias_str = f", alias='{self.alias}'" if self.alias != self.name.split('.')[-1] else ""
        return f"DataField(name='{self.name}'{alias_str}, data_type='{self.data_type}')"
