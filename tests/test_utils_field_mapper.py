"""Unit tests for FieldMapper utility class."""
import unittest
from mongo_datatables.datatables import DataField
from mongo_datatables.utils import FieldMapper


class TestFieldMapper(unittest.TestCase):
    """Test cases for FieldMapper utility class."""

    def setUp(self):
        """Set up test fixtures."""
        # Create sample data fields for testing
        self.data_fields = [
            DataField("title", "string", "Title"),
            DataField("author.name", "string", "Author"),
            DataField("publishDate", "date", "Published"),
            DataField("pageCount", "number", "Pages"),
            DataField("isPublished", "boolean", "Status"),
            DataField("tags", "array", "Tags"),
        ]

    def test_initialization_with_data_fields(self):
        """Test FieldMapper initialization with data fields."""
        mapper = FieldMapper(self.data_fields)

        self.assertEqual(len(mapper.data_fields), 6)
        self.assertIsNotNone(mapper.field_types)
        self.assertIsNotNone(mapper.ui_to_db)
        self.assertIsNotNone(mapper.db_to_ui)

    def test_initialization_with_empty_list(self):
        """Test FieldMapper initialization with empty list."""
        mapper = FieldMapper([])

        self.assertEqual(len(mapper.data_fields), 0)
        self.assertEqual(mapper.field_types, {})
        self.assertEqual(mapper.ui_to_db, {})
        self.assertEqual(mapper.db_to_ui, {})

    def test_initialization_with_none(self):
        """Test FieldMapper initialization with None."""
        mapper = FieldMapper(None)

        self.assertEqual(len(mapper.data_fields), 0)
        self.assertEqual(mapper.field_types, {})

    def test_get_db_field_with_mapping(self):
        """Test getting database field name from UI alias."""
        mapper = FieldMapper(self.data_fields)

        # Test mapped fields
        self.assertEqual(mapper.get_db_field("Title"), "title")
        self.assertEqual(mapper.get_db_field("Author"), "author.name")
        self.assertEqual(mapper.get_db_field("Published"), "publishDate")
        self.assertEqual(mapper.get_db_field("Pages"), "pageCount")

    def test_get_db_field_without_mapping(self):
        """Test getting database field when no mapping exists (returns same)."""
        mapper = FieldMapper(self.data_fields)

        # Non-existent field should return itself
        self.assertEqual(mapper.get_db_field("NonExistent"), "NonExistent")
        self.assertEqual(mapper.get_db_field("random_field"), "random_field")

    def test_get_db_field_case_sensitive(self):
        """Test that field mapping is case-sensitive."""
        mapper = FieldMapper(self.data_fields)

        # Correct case
        self.assertEqual(mapper.get_db_field("Title"), "title")

        # Wrong case should not map
        self.assertEqual(mapper.get_db_field("title"), "title")  # Returns itself
        self.assertEqual(mapper.get_db_field("TITLE"), "TITLE")  # Returns itself

    def test_get_ui_field_with_mapping(self):
        """Test getting UI field name from database field."""
        mapper = FieldMapper(self.data_fields)

        # Test reverse mapping
        self.assertEqual(mapper.get_ui_field("title"), "Title")
        self.assertEqual(mapper.get_ui_field("author.name"), "Author")
        self.assertEqual(mapper.get_ui_field("publishDate"), "Published")
        self.assertEqual(mapper.get_ui_field("pageCount"), "Pages")

    def test_get_ui_field_without_mapping(self):
        """Test getting UI field when no mapping exists."""
        mapper = FieldMapper(self.data_fields)

        # Non-existent field should return itself
        self.assertEqual(mapper.get_ui_field("nonexistent"), "nonexistent")

    def test_get_field_type_by_db_name(self):
        """Test getting field type using database field name."""
        mapper = FieldMapper(self.data_fields)

        self.assertEqual(mapper.get_field_type("title"), "string")
        self.assertEqual(mapper.get_field_type("author.name"), "string")
        self.assertEqual(mapper.get_field_type("publishDate"), "date")
        self.assertEqual(mapper.get_field_type("pageCount"), "number")
        self.assertEqual(mapper.get_field_type("isPublished"), "boolean")
        self.assertEqual(mapper.get_field_type("tags"), "array")

    def test_get_field_type_by_ui_name(self):
        """Test getting field type using UI alias."""
        mapper = FieldMapper(self.data_fields)

        # Should map UI name to DB name and get type
        self.assertEqual(mapper.get_field_type("Title"), "string")
        self.assertEqual(mapper.get_field_type("Author"), "string")
        self.assertEqual(mapper.get_field_type("Published"), "date")
        self.assertEqual(mapper.get_field_type("Pages"), "number")

    def test_get_field_type_nonexistent(self):
        """Test getting field type for non-existent field."""
        mapper = FieldMapper(self.data_fields)

        # Non-existent field should return None
        self.assertIsNone(mapper.get_field_type("nonexistent"))
        self.assertIsNone(mapper.get_field_type(""))

    def test_nested_field_mapping(self):
        """Test mapping for nested fields (dot notation)."""
        mapper = FieldMapper(self.data_fields)

        # author.name should map correctly
        self.assertEqual(mapper.get_db_field("Author"), "author.name")
        self.assertEqual(mapper.get_ui_field("author.name"), "Author")
        self.assertEqual(mapper.get_field_type("author.name"), "string")

    def test_field_with_no_alias(self):
        """Test field where alias is same as name."""
        # Create field with no explicit alias
        fields = [
            DataField("simpleField", "string")  # No alias, should use name
        ]
        mapper = FieldMapper(fields)

        # When no alias specified, it should use the field name
        # So both should work
        self.assertEqual(mapper.get_db_field("simpleField"), "simpleField")
        self.assertEqual(mapper.get_field_type("simpleField"), "string")

    def test_multiple_fields_same_type(self):
        """Test multiple fields with the same type."""
        fields = [
            DataField("field1", "string", "Field1"),
            DataField("field2", "string", "Field2"),
            DataField("field3", "string", "Field3"),
        ]
        mapper = FieldMapper(fields)

        # All should have correct type
        self.assertEqual(mapper.get_field_type("field1"), "string")
        self.assertEqual(mapper.get_field_type("field2"), "string")
        self.assertEqual(mapper.get_field_type("field3"), "string")

        # All should have correct mappings
        self.assertEqual(mapper.get_db_field("Field1"), "field1")
        self.assertEqual(mapper.get_db_field("Field2"), "field2")
        self.assertEqual(mapper.get_db_field("Field3"), "field3")

    def test_field_types_dictionary(self):
        """Test that field_types dictionary is populated correctly."""
        mapper = FieldMapper(self.data_fields)

        expected_types = {
            "title": "string",
            "author.name": "string",
            "publishDate": "date",
            "pageCount": "number",
            "isPublished": "boolean",
            "tags": "array",
        }

        self.assertEqual(mapper.field_types, expected_types)

    def test_ui_to_db_mapping_dictionary(self):
        """Test that ui_to_db dictionary is populated correctly."""
        mapper = FieldMapper(self.data_fields)

        expected_mapping = {
            "Title": "title",
            "Author": "author.name",
            "Published": "publishDate",
            "Pages": "pageCount",
            "Status": "isPublished",
            "Tags": "tags",
        }

        self.assertEqual(mapper.ui_to_db, expected_mapping)

    def test_db_to_ui_mapping_dictionary(self):
        """Test that db_to_ui dictionary is populated correctly."""
        mapper = FieldMapper(self.data_fields)

        expected_mapping = {
            "title": "Title",
            "author.name": "Author",
            "publishDate": "Published",
            "pageCount": "Pages",
            "isPublished": "Status",
            "tags": "Tags",
        }

        self.assertEqual(mapper.db_to_ui, expected_mapping)

    def test_all_valid_mongo_types(self):
        """Test FieldMapper with all valid MongoDB data types."""
        fields = [
            DataField("string_field", "string"),
            DataField("number_field", "number"),
            DataField("date_field", "date"),
            DataField("boolean_field", "boolean"),
            DataField("array_field", "array"),
            DataField("object_field", "object"),
            DataField("objectid_field", "objectid"),
            DataField("null_field", "null"),
        ]
        mapper = FieldMapper(fields)

        # All types should be stored correctly
        self.assertEqual(mapper.get_field_type("string_field"), "string")
        self.assertEqual(mapper.get_field_type("number_field"), "number")
        self.assertEqual(mapper.get_field_type("date_field"), "date")
        self.assertEqual(mapper.get_field_type("boolean_field"), "boolean")
        self.assertEqual(mapper.get_field_type("array_field"), "array")
        self.assertEqual(mapper.get_field_type("object_field"), "object")
        self.assertEqual(mapper.get_field_type("objectid_field"), "objectid")
        self.assertEqual(mapper.get_field_type("null_field"), "null")

    def test_empty_string_field_name(self):
        """Test handling of empty field name."""
        mapper = FieldMapper(self.data_fields)

        # Empty string should return None
        self.assertIsNone(mapper.get_field_type(""))
        self.assertEqual(mapper.get_db_field(""), "")
        self.assertEqual(mapper.get_ui_field(""), "")


if __name__ == '__main__':
    unittest.main()
