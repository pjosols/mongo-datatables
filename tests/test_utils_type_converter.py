"""Unit tests for TypeConverter utility class."""
import unittest
import json
from mongo_datatables.utils import TypeConverter
from mongo_datatables.exceptions import FieldMappingError


class TestTypeConverter(unittest.TestCase):
    """Test cases for TypeConverter utility class."""

    # ============ to_number tests ============

    def test_to_number_integer(self):
        """Test converting string to integer."""
        self.assertEqual(TypeConverter.to_number("42"), 42)
        self.assertEqual(TypeConverter.to_number("0"), 0)
        self.assertEqual(TypeConverter.to_number("-15"), -15)
        self.assertIsInstance(TypeConverter.to_number("42"), int)

    def test_to_number_float(self):
        """Test converting string to float."""
        self.assertEqual(TypeConverter.to_number("3.14"), 3.14)
        self.assertEqual(TypeConverter.to_number("0.5"), 0.5)
        self.assertEqual(TypeConverter.to_number("-2.5"), -2.5)
        self.assertIsInstance(TypeConverter.to_number("3.14"), float)

    def test_to_number_scientific_notation(self):
        """Test converting scientific notation."""
        self.assertEqual(TypeConverter.to_number("1.5e2"), 150.0)
        self.assertEqual(TypeConverter.to_number("1e-3"), 0.001)

    def test_to_number_invalid(self):
        """Test invalid number conversions."""
        with self.assertRaises(FieldMappingError):
            TypeConverter.to_number("not a number")

        with self.assertRaises(FieldMappingError):
            TypeConverter.to_number("12.34.56")

        with self.assertRaises(FieldMappingError):
            TypeConverter.to_number("")

        with self.assertRaises(FieldMappingError):
            TypeConverter.to_number("12abc")

    def test_to_number_whitespace(self):
        """Test number conversion with whitespace."""
        # Python's float/int handle leading/trailing whitespace
        self.assertEqual(TypeConverter.to_number("  42  "), 42)
        self.assertEqual(TypeConverter.to_number("\t3.14\n"), 3.14)

    # ============ to_boolean tests ============

    def test_to_boolean_true_values(self):
        """Test converting various strings to True."""
        true_values = ['true', 'True', 'TRUE', 'yes', 'Yes', 'YES', '1', 't', 'T', 'y', 'Y']
        for value in true_values:
            with self.subTest(value=value):
                self.assertTrue(TypeConverter.to_boolean(value))

    def test_to_boolean_false_values(self):
        """Test converting various strings to False."""
        false_values = ['false', 'False', 'FALSE', 'no', 'No', 'NO', '0', 'f', 'F', 'n', 'N', '']
        for value in false_values:
            with self.subTest(value=value):
                self.assertFalse(TypeConverter.to_boolean(value))

    def test_to_boolean_edge_cases(self):
        """Test boolean conversion edge cases."""
        # Any string not in the true list should be false
        self.assertFalse(TypeConverter.to_boolean("maybe"))
        self.assertFalse(TypeConverter.to_boolean("2"))
        self.assertFalse(TypeConverter.to_boolean("on"))
        self.assertFalse(TypeConverter.to_boolean("off"))

    # ============ to_array tests ============

    def test_to_array_valid_json_array(self):
        """Test converting valid JSON array strings."""
        result = TypeConverter.to_array('["a", "b", "c"]')
        self.assertEqual(result, ["a", "b", "c"])
        self.assertIsInstance(result, list)

        result = TypeConverter.to_array('[1, 2, 3]')
        self.assertEqual(result, [1, 2, 3])

        result = TypeConverter.to_array('[]')
        self.assertEqual(result, [])

    def test_to_array_json_with_mixed_types(self):
        """Test converting JSON array with mixed types."""
        result = TypeConverter.to_array('["string", 123, true, null]')
        self.assertEqual(result, ["string", 123, True, None])

    def test_to_array_non_json_string(self):
        """Test converting non-JSON string to single-element array."""
        result = TypeConverter.to_array("simple string")
        self.assertEqual(result, ["simple string"])

        result = TypeConverter.to_array("not [valid] json")
        self.assertEqual(result, ["not [valid] json"])

    def test_to_array_json_object_not_array(self):
        """Test converting JSON object (not array) wraps in array."""
        result = TypeConverter.to_array('{"key": "value"}')
        self.assertEqual(result, [{"key": "value"}])

    def test_to_array_json_scalar(self):
        """Test converting JSON scalar wraps in array."""
        result = TypeConverter.to_array('123')
        self.assertEqual(result, [123])

        result = TypeConverter.to_array('"string"')
        self.assertEqual(result, ["string"])

    def test_to_array_empty_string(self):
        """Test converting empty string."""
        result = TypeConverter.to_array("")
        self.assertEqual(result, [""])

    # ============ parse_json tests ============

    def test_parse_json_valid_object(self):
        """Test parsing valid JSON objects."""
        result = TypeConverter.parse_json('{"name": "John", "age": 30}')
        self.assertEqual(result, {"name": "John", "age": 30})
        self.assertIsInstance(result, dict)

    def test_parse_json_valid_array(self):
        """Test parsing valid JSON arrays."""
        result = TypeConverter.parse_json('[1, 2, 3]')
        self.assertEqual(result, [1, 2, 3])
        self.assertIsInstance(result, list)

    def test_parse_json_nested_structure(self):
        """Test parsing complex nested JSON."""
        json_str = '{"users": [{"name": "Alice", "age": 25}, {"name": "Bob", "age": 30}]}'
        result = TypeConverter.parse_json(json_str)
        self.assertEqual(len(result["users"]), 2)
        self.assertEqual(result["users"][0]["name"], "Alice")

    def test_parse_json_scalars(self):
        """Test parsing JSON scalar values."""
        self.assertEqual(TypeConverter.parse_json('123'), 123)
        self.assertEqual(TypeConverter.parse_json('"string"'), "string")
        self.assertEqual(TypeConverter.parse_json('true'), True)
        self.assertEqual(TypeConverter.parse_json('false'), False)
        self.assertEqual(TypeConverter.parse_json('null'), None)

    def test_parse_json_invalid(self):
        """Test parsing invalid JSON raises exception."""
        with self.assertRaises(FieldMappingError):
            TypeConverter.parse_json("not valid json")

        with self.assertRaises(FieldMappingError):
            TypeConverter.parse_json("{unclosed")

        with self.assertRaises(FieldMappingError):
            TypeConverter.parse_json("{'single': 'quotes'}")  # JSON requires double quotes

    def test_parse_json_empty_string(self):
        """Test parsing empty string raises exception."""
        with self.assertRaises(FieldMappingError):
            TypeConverter.parse_json("")

    def test_parse_json_with_whitespace(self):
        """Test parsing JSON with extra whitespace."""
        result = TypeConverter.parse_json('  {"key": "value"}  ')
        self.assertEqual(result, {"key": "value"})

    # ============ Integration tests ============

    def test_number_conversion_edge_values(self):
        """Test number conversion with edge values."""
        # Very large numbers
        self.assertEqual(TypeConverter.to_number("9999999999999999"), 9999999999999999)

        # Very small decimals
        result = TypeConverter.to_number("0.0000001")
        self.assertAlmostEqual(result, 0.0000001)

        # Negative zero
        self.assertEqual(TypeConverter.to_number("-0"), 0)

    def test_array_conversion_with_nested_json(self):
        """Test array conversion with nested JSON structures."""
        json_str = '[{"id": 1, "data": [1, 2, 3]}, {"id": 2, "data": [4, 5, 6]}]'
        result = TypeConverter.to_array(json_str)
        self.assertEqual(len(result), 2)
        self.assertEqual(result[0]["data"], [1, 2, 3])
        self.assertEqual(result[1]["id"], 2)

    def test_parse_json_unicode(self):
        """Test parsing JSON with unicode characters."""
        result = TypeConverter.parse_json('{"emoji": "🎉", "chinese": "你好"}')
        self.assertEqual(result["emoji"], "🎉")
        self.assertEqual(result["chinese"], "你好")

    def test_parse_json_escaped_characters(self):
        """Test parsing JSON with escaped characters."""
        result = TypeConverter.parse_json('{"quote": "\\"Hello\\"", "newline": "Line1\\nLine2"}')
        self.assertEqual(result["quote"], '"Hello"')
        self.assertEqual(result["newline"], "Line1\nLine2")


if __name__ == '__main__':
    unittest.main()
