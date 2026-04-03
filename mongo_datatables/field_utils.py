"""Field mapping and search term parsing utilities for mongo-datatables."""

import logging
import shlex
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)


class FieldMapper:
    """Manages field name mappings between UI and database representations.

    Handles mapping UI field aliases to database field names, reverse mapping,
    and field type lookup for both UI and database field names.
    """

    def __init__(self, data_fields: List[Any]) -> None:
        """Initialize field mapper with DataField objects.

        data_fields: List of DataField objects defining field mappings.
        """
        from mongo_datatables.data_field import DataField

        self.data_fields = data_fields or []
        self.field_types: Dict[str, str] = {}
        self.ui_to_db: Dict[str, str] = {}
        self.db_to_ui: Dict[str, str] = {}

        for field in self.data_fields:
            if isinstance(field, DataField):
                self.field_types[field.name] = field.data_type
                self.ui_to_db[field.alias] = field.name
                self.db_to_ui[field.name] = field.alias

    def get_db_field(self, ui_field: str) -> str:
        """Map a UI field name to its database field name.

        ui_field: UI field name or alias.
        Returns database field name, or the original name if no mapping exists.
        """
        return self.ui_to_db.get(ui_field, ui_field)

    def get_ui_field(self, db_field: str) -> str:
        """Map a database field name to its UI field name.

        db_field: Database field name.
        Returns UI field name or alias, or the original name if no mapping exists.
        """
        return self.db_to_ui.get(db_field, db_field)

    def get_field_type(self, field_name: str) -> Optional[str]:
        """Get the data type for a field.

        field_name: Field name (can be UI or database field name).
        Returns field type string, or None if not found.
        """
        if field_name in self.field_types:
            return self.field_types[field_name]
        return self.field_types.get(self.get_db_field(field_name))


class SearchTermParser:
    """Utilities for parsing search terms with quoted phrase support.

    Handles search strings that may contain quoted phrases, treating quoted
    text as single search terms.
    """

    @staticmethod
    def parse(search_value: str) -> List[str]:
        """Extract search terms from a search string.

        Handles quoted phrases (both single and double quotes) as single terms.
        For example, 'Author:Robert "Jonathan Kennedy"' parses as
        ['Author:Robert', 'Jonathan Kennedy'].

        search_value: Search string to parse.
        Returns list of search terms with quoted phrases preserved as single terms.
        """
        if not search_value:
            return []
        try:
            return shlex.split(search_value)
        except ValueError as e:
            logger.warning("Malformed search syntax '%s': %s. Using simple split.", search_value, e)
            return search_value.split()
