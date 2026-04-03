"""Tests for utility classes: FieldMapper, SearchTermParser, TypeConverter, is_truthy."""
import unittest

import pytest

from mongo_datatables import DataField
from mongo_datatables.exceptions import FieldMappingError
from mongo_datatables.utils import FieldMapper, SearchTermParser, TypeConverter, is_truthy


class TestFieldMapper(unittest.TestCase):
    """Test cases for FieldMapper utility class."""

    def setUp(self):
        self.data_fields = [
            DataField("title", "string", "Title"),
            DataField("author.name", "string", "Author"),
            DataField("publishDate", "date", "Published"),
            DataField("pageCount", "number", "Pages"),
            DataField("isPublished", "boolean", "Status"),
            DataField("tags", "array", "Tags"),
        ]

    def test_initialization_with_data_fields(self):
        mapper = FieldMapper(self.data_fields)
        self.assertEqual(len(mapper.data_fields), 6)
        self.assertIsNotNone(mapper.field_types)
        self.assertIsNotNone(mapper.ui_to_db)
        self.assertIsNotNone(mapper.db_to_ui)

    def test_initialization_with_empty_list(self):
        mapper = FieldMapper([])
        self.assertEqual(len(mapper.data_fields), 0)
        self.assertEqual(mapper.field_types, {})
        self.assertEqual(mapper.ui_to_db, {})
        self.assertEqual(mapper.db_to_ui, {})

    def test_initialization_with_none(self):
        mapper = FieldMapper(None)
        self.assertEqual(len(mapper.data_fields), 0)
        self.assertEqual(mapper.field_types, {})

    def test_get_db_field_with_mapping(self):
        mapper = FieldMapper(self.data_fields)
        self.assertEqual(mapper.get_db_field("Title"), "title")
        self.assertEqual(mapper.get_db_field("Author"), "author.name")
        self.assertEqual(mapper.get_db_field("Published"), "publishDate")
        self.assertEqual(mapper.get_db_field("Pages"), "pageCount")

    def test_get_db_field_without_mapping(self):
        mapper = FieldMapper(self.data_fields)
        self.assertEqual(mapper.get_db_field("NonExistent"), "NonExistent")
        self.assertEqual(mapper.get_db_field("random_field"), "random_field")

    def test_get_db_field_case_sensitive(self):
        mapper = FieldMapper(self.data_fields)
        self.assertEqual(mapper.get_db_field("Title"), "title")
        self.assertEqual(mapper.get_db_field("title"), "title")
        self.assertEqual(mapper.get_db_field("TITLE"), "TITLE")

    def test_get_ui_field_with_mapping(self):
        mapper = FieldMapper(self.data_fields)
        self.assertEqual(mapper.get_ui_field("title"), "Title")
        self.assertEqual(mapper.get_ui_field("author.name"), "Author")
        self.assertEqual(mapper.get_ui_field("publishDate"), "Published")
        self.assertEqual(mapper.get_ui_field("pageCount"), "Pages")

    def test_get_ui_field_without_mapping(self):
        mapper = FieldMapper(self.data_fields)
        self.assertEqual(mapper.get_ui_field("nonexistent"), "nonexistent")

    def test_get_field_type_by_db_name(self):
        mapper = FieldMapper(self.data_fields)
        self.assertEqual(mapper.get_field_type("title"), "string")
        self.assertEqual(mapper.get_field_type("author.name"), "string")
        self.assertEqual(mapper.get_field_type("publishDate"), "date")
        self.assertEqual(mapper.get_field_type("pageCount"), "number")
        self.assertEqual(mapper.get_field_type("isPublished"), "boolean")
        self.assertEqual(mapper.get_field_type("tags"), "array")

    def test_get_field_type_by_ui_name(self):
        mapper = FieldMapper(self.data_fields)
        self.assertEqual(mapper.get_field_type("Title"), "string")
        self.assertEqual(mapper.get_field_type("Author"), "string")
        self.assertEqual(mapper.get_field_type("Published"), "date")
        self.assertEqual(mapper.get_field_type("Pages"), "number")

    def test_get_field_type_nonexistent(self):
        mapper = FieldMapper(self.data_fields)
        self.assertIsNone(mapper.get_field_type("nonexistent"))
        self.assertIsNone(mapper.get_field_type(""))

    def test_nested_field_mapping(self):
        mapper = FieldMapper(self.data_fields)
        self.assertEqual(mapper.get_db_field("Author"), "author.name")
        self.assertEqual(mapper.get_ui_field("author.name"), "Author")
        self.assertEqual(mapper.get_field_type("author.name"), "string")

    def test_field_with_no_alias(self):
        fields = [DataField("simpleField", "string")]
        mapper = FieldMapper(fields)
        self.assertEqual(mapper.get_db_field("simpleField"), "simpleField")
        self.assertEqual(mapper.get_field_type("simpleField"), "string")

    def test_multiple_fields_same_type(self):
        fields = [
            DataField("field1", "string", "Field1"),
            DataField("field2", "string", "Field2"),
            DataField("field3", "string", "Field3"),
        ]
        mapper = FieldMapper(fields)
        for i in range(1, 4):
            self.assertEqual(mapper.get_field_type(f"field{i}"), "string")
            self.assertEqual(mapper.get_db_field(f"Field{i}"), f"field{i}")

    def test_field_types_dictionary(self):
        mapper = FieldMapper(self.data_fields)
        expected = {
            "title": "string", "author.name": "string", "publishDate": "date",
            "pageCount": "number", "isPublished": "boolean", "tags": "array",
        }
        self.assertEqual(mapper.field_types, expected)

    def test_ui_to_db_mapping_dictionary(self):
        mapper = FieldMapper(self.data_fields)
        expected = {
            "Title": "title", "Author": "author.name", "Published": "publishDate",
            "Pages": "pageCount", "Status": "isPublished", "Tags": "tags",
        }
        self.assertEqual(mapper.ui_to_db, expected)

    def test_db_to_ui_mapping_dictionary(self):
        mapper = FieldMapper(self.data_fields)
        expected = {
            "title": "Title", "author.name": "Author", "publishDate": "Published",
            "pageCount": "Pages", "isPublished": "Status", "tags": "Tags",
        }
        self.assertEqual(mapper.db_to_ui, expected)

    def test_all_valid_mongo_types(self):
        fields = [
            DataField("string_field", "string"), DataField("number_field", "number"),
            DataField("date_field", "date"), DataField("boolean_field", "boolean"),
            DataField("array_field", "array"), DataField("object_field", "object"),
            DataField("objectid_field", "objectid"), DataField("null_field", "null"),
        ]
        mapper = FieldMapper(fields)
        for f in fields:
            self.assertEqual(mapper.get_field_type(f.name), f.data_type)

    def test_empty_string_field_name(self):
        mapper = FieldMapper(self.data_fields)
        self.assertIsNone(mapper.get_field_type(""))
        self.assertEqual(mapper.get_db_field(""), "")
        self.assertEqual(mapper.get_ui_field(""), "")


class TestSearchTermParser(unittest.TestCase):
    """Test cases for SearchTermParser utility class."""

    def test_parse_empty_string(self):
        self.assertEqual(SearchTermParser.parse(""), [])
        self.assertEqual(SearchTermParser.parse(None), [])

    def test_parse_simple_terms(self):
        self.assertEqual(SearchTermParser.parse("term1 term2 term3"), ["term1", "term2", "term3"])

    def test_parse_single_term(self):
        self.assertEqual(SearchTermParser.parse("singleterm"), ["singleterm"])

    def test_parse_double_quoted_phrase(self):
        self.assertEqual(SearchTermParser.parse('Author:Robert "Jonathan Kennedy"'),
                         ["Author:Robert", "Jonathan Kennedy"])
        self.assertEqual(SearchTermParser.parse('"complete phrase"'), ["complete phrase"])

    def test_parse_single_quoted_phrase(self):
        self.assertEqual(SearchTermParser.parse("Author:Robert 'Jonathan Kennedy'"),
                         ["Author:Robert", "Jonathan Kennedy"])
        self.assertEqual(SearchTermParser.parse("'complete phrase'"), ["complete phrase"])

    def test_parse_mixed_quotes(self):
        self.assertEqual(SearchTermParser.parse('"double quote" and \'single quote\''),
                         ["double quote", "and", "single quote"])

    def test_parse_multiple_quoted_phrases(self):
        self.assertEqual(SearchTermParser.parse('"first phrase" "second phrase" "third phrase"'),
                         ["first phrase", "second phrase", "third phrase"])

    def test_parse_quoted_with_unquoted(self):
        self.assertEqual(
            SearchTermParser.parse('term1 "quoted term" term2 \'another quoted\' term3'),
            ["term1", "quoted term", "term2", "another quoted", "term3"]
        )

    def test_parse_field_specific_search(self):
        self.assertEqual(SearchTermParser.parse("Title:MongoDB Author:Smith Status:active"),
                         ["Title:MongoDB", "Author:Smith", "Status:active"])

    def test_parse_field_specific_with_quoted_value(self):
        self.assertEqual(SearchTermParser.parse('Title:"Advanced MongoDB" Author:Smith'),
                         ["Title:Advanced MongoDB", "Author:Smith"])

    def test_parse_empty_quotes(self):
        self.assertEqual(SearchTermParser.parse('term1 "" term2'), ["term1", "", "term2"])
        self.assertEqual(SearchTermParser.parse("term1 '' term2"), ["term1", "", "term2"])

    def test_parse_quotes_within_quotes(self):
        self.assertEqual(SearchTermParser.parse('\'He said "Hello"\''), ['He said "Hello"'])
        self.assertEqual(SearchTermParser.parse('"It\'s working"'), ["It's working"])

    def test_parse_malformed_quotes_unclosed(self):
        result = SearchTermParser.parse('term1 "unclosed quote term2')
        self.assertIsInstance(result, list)
        self.assertEqual(len(result), 4)

    def test_parse_special_characters(self):
        self.assertEqual(SearchTermParser.parse("user@example.com test-value date:2023-01-01"),
                         ["user@example.com", "test-value", "date:2023-01-01"])

    def test_parse_multiple_spaces(self):
        self.assertEqual(SearchTermParser.parse("term1    term2     term3"),
                         ["term1", "term2", "term3"])

    def test_parse_tabs_and_newlines(self):
        self.assertEqual(SearchTermParser.parse("term1\tterm2\nterm3"),
                         ["term1", "term2", "term3"])

    def test_parse_quoted_empty_string_only(self):
        self.assertEqual(SearchTermParser.parse('""'), [""])

    def test_parse_complex_search_example(self):
        search = 'Title:"MongoDB Guide" Author:Smith Year:>2020 Status:published'
        self.assertEqual(SearchTermParser.parse(search),
                         ["Title:MongoDB Guide", "Author:Smith", "Year:>2020", "Status:published"])

    def test_parse_quoted_colon_syntax(self):
        self.assertEqual(SearchTermParser.parse('Field:"value with spaces"'),
                         ["Field:value with spaces"])

    def test_parse_backslash_escapes(self):
        result = SearchTermParser.parse('term1 "escaped\\ space" term2')
        self.assertIn("escaped\\ space", result)

    def test_parse_unicode_characters(self):
        self.assertEqual(SearchTermParser.parse('emoji:🎉 chinese:你好 term'),
                         ["emoji:🎉", "chinese:你好", "term"])

    def test_parse_numeric_values(self):
        self.assertEqual(SearchTermParser.parse("age:25 price:>100 quantity:<=50"),
                         ["age:25", "price:>100", "quantity:<=50"])

    def test_parse_preserves_operator_symbols(self):
        self.assertEqual(SearchTermParser.parse("field:>=100 another:<50"),
                         ["field:>=100", "another:<50"])

    def test_parse_single_word_no_split(self):
        self.assertEqual(SearchTermParser.parse("MongoDB"), ["MongoDB"])

    def test_parse_whitespace_only(self):
        self.assertEqual(SearchTermParser.parse("   "), [])
        self.assertEqual(SearchTermParser.parse("\t\n"), [])


class TestTypeConverter(unittest.TestCase):
    """Test cases for TypeConverter utility class."""

    def test_to_number_integer(self):
        self.assertEqual(TypeConverter.to_number("42"), 42)
        self.assertEqual(TypeConverter.to_number("0"), 0)
        self.assertEqual(TypeConverter.to_number("-15"), -15)
        self.assertIsInstance(TypeConverter.to_number("42"), int)

    def test_to_number_float(self):
        self.assertEqual(TypeConverter.to_number("3.14"), 3.14)
        self.assertIsInstance(TypeConverter.to_number("3.14"), float)

    def test_to_number_scientific_notation(self):
        self.assertEqual(TypeConverter.to_number("1.5e2"), 150.0)
        self.assertEqual(TypeConverter.to_number("1e-3"), 0.001)

    def test_to_number_invalid(self):
        for val in ("not a number", "12.34.56", "", "12abc"):
            with self.assertRaises(FieldMappingError):
                TypeConverter.to_number(val)

    def test_to_number_whitespace(self):
        self.assertEqual(TypeConverter.to_number("  42  "), 42)
        self.assertEqual(TypeConverter.to_number("\t3.14\n"), 3.14)

    def test_to_boolean_true_values(self):
        for value in ['true', 'True', 'TRUE', 'yes', 'Yes', 'YES', '1', 't', 'T', 'y', 'Y']:
            with self.subTest(value=value):
                self.assertTrue(TypeConverter.to_boolean(value))

    def test_to_boolean_false_values(self):
        for value in ['false', 'False', 'FALSE', 'no', 'No', 'NO', '0', 'f', 'F', 'n', 'N', '']:
            with self.subTest(value=value):
                self.assertFalse(TypeConverter.to_boolean(value))

    def test_to_boolean_edge_cases(self):
        self.assertFalse(TypeConverter.to_boolean("maybe"))
        self.assertFalse(TypeConverter.to_boolean("2"))
        self.assertFalse(TypeConverter.to_boolean("on"))

    def test_to_array_valid_json_array(self):
        self.assertEqual(TypeConverter.to_array('["a", "b", "c"]'), ["a", "b", "c"])
        self.assertEqual(TypeConverter.to_array('[1, 2, 3]'), [1, 2, 3])
        self.assertEqual(TypeConverter.to_array('[]'), [])

    def test_to_array_json_with_mixed_types(self):
        self.assertEqual(TypeConverter.to_array('["string", 123, true, null]'),
                         ["string", 123, True, None])

    def test_to_array_non_json_string(self):
        self.assertEqual(TypeConverter.to_array("simple string"), ["simple string"])
        self.assertEqual(TypeConverter.to_array("not [valid] json"), ["not [valid] json"])

    def test_to_array_json_object_not_array(self):
        self.assertEqual(TypeConverter.to_array('{"key": "value"}'), [{"key": "value"}])

    def test_to_array_json_scalar(self):
        self.assertEqual(TypeConverter.to_array('123'), [123])
        self.assertEqual(TypeConverter.to_array('"string"'), ["string"])

    def test_to_array_empty_string(self):
        self.assertEqual(TypeConverter.to_array(""), [""])

    def test_parse_json_valid_object(self):
        self.assertEqual(TypeConverter.parse_json('{"name": "John", "age": 30}'),
                         {"name": "John", "age": 30})

    def test_parse_json_valid_array(self):
        self.assertEqual(TypeConverter.parse_json('[1, 2, 3]'), [1, 2, 3])

    def test_parse_json_nested_structure(self):
        result = TypeConverter.parse_json('{"users": [{"name": "Alice"}, {"name": "Bob"}]}')
        self.assertEqual(len(result["users"]), 2)

    def test_parse_json_scalars(self):
        self.assertEqual(TypeConverter.parse_json('123'), 123)
        self.assertEqual(TypeConverter.parse_json('"string"'), "string")
        self.assertEqual(TypeConverter.parse_json('true'), True)
        self.assertIsNone(TypeConverter.parse_json('null'))

    def test_parse_json_invalid(self):
        for val in ("not valid json", "{unclosed", "{'single': 'quotes'}"):
            with self.assertRaises(FieldMappingError):
                TypeConverter.parse_json(val)

    def test_parse_json_empty_string(self):
        with self.assertRaises(FieldMappingError):
            TypeConverter.parse_json("")

    def test_parse_json_with_whitespace(self):
        self.assertEqual(TypeConverter.parse_json('  {"key": "value"}  '), {"key": "value"})

    def test_number_conversion_edge_values(self):
        self.assertEqual(TypeConverter.to_number("9999999999999999"), 9999999999999999)
        self.assertAlmostEqual(TypeConverter.to_number("0.0000001"), 0.0000001)
        self.assertEqual(TypeConverter.to_number("-0"), 0)

    def test_array_conversion_with_nested_json(self):
        result = TypeConverter.to_array('[{"id": 1, "data": [1, 2, 3]}, {"id": 2, "data": [4, 5, 6]}]')
        self.assertEqual(len(result), 2)
        self.assertEqual(result[0]["data"], [1, 2, 3])

    def test_parse_json_unicode(self):
        result = TypeConverter.parse_json('{"emoji": "🎉", "chinese": "你好"}')
        self.assertEqual(result["emoji"], "🎉")
        self.assertEqual(result["chinese"], "你好")

    def test_parse_json_escaped_characters(self):
        result = TypeConverter.parse_json('{"quote": "\\"Hello\\"", "newline": "Line1\\nLine2"}')
        self.assertEqual(result["quote"], '"Hello"')
        self.assertEqual(result["newline"], "Line1\nLine2")


@pytest.mark.parametrize("value", [True, "true", "True", 1])
def test_is_truthy_truthy_values(value):
    assert is_truthy(value) is True


@pytest.mark.parametrize("value", [False, "false", "False", 0, None, "", "yes", "1", 2])
def test_is_truthy_falsy_values(value):
    assert is_truthy(value) is False


if __name__ == '__main__':
    unittest.main()
