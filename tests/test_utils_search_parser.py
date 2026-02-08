"""Unit tests for SearchTermParser utility class."""
import unittest
from mongo_datatables.utils import SearchTermParser


class TestSearchTermParser(unittest.TestCase):
    """Test cases for SearchTermParser utility class."""

    def test_parse_empty_string(self):
        """Test parsing empty search string."""
        result = SearchTermParser.parse("")
        self.assertEqual(result, [])

        result = SearchTermParser.parse(None)
        self.assertEqual(result, [])

    def test_parse_simple_terms(self):
        """Test parsing simple space-separated terms."""
        result = SearchTermParser.parse("term1 term2 term3")
        self.assertEqual(result, ["term1", "term2", "term3"])

    def test_parse_single_term(self):
        """Test parsing single term."""
        result = SearchTermParser.parse("singleterm")
        self.assertEqual(result, ["singleterm"])

    def test_parse_double_quoted_phrase(self):
        """Test parsing double-quoted phrases."""
        result = SearchTermParser.parse('Author:Robert "Jonathan Kennedy"')
        self.assertEqual(result, ["Author:Robert", "Jonathan Kennedy"])

        result = SearchTermParser.parse('"complete phrase"')
        self.assertEqual(result, ["complete phrase"])

    def test_parse_single_quoted_phrase(self):
        """Test parsing single-quoted phrases."""
        result = SearchTermParser.parse("Author:Robert 'Jonathan Kennedy'")
        self.assertEqual(result, ["Author:Robert", "Jonathan Kennedy"])

        result = SearchTermParser.parse("'complete phrase'")
        self.assertEqual(result, ["complete phrase"])

    def test_parse_mixed_quotes(self):
        """Test parsing with both single and double quotes."""
        result = SearchTermParser.parse('"double quote" and \'single quote\'')
        self.assertEqual(result, ["double quote", "and", "single quote"])

    def test_parse_multiple_quoted_phrases(self):
        """Test parsing multiple quoted phrases."""
        result = SearchTermParser.parse('"first phrase" "second phrase" "third phrase"')
        self.assertEqual(result, ["first phrase", "second phrase", "third phrase"])

    def test_parse_quoted_with_unquoted(self):
        """Test parsing mix of quoted and unquoted terms."""
        result = SearchTermParser.parse('term1 "quoted term" term2 \'another quoted\' term3')
        self.assertEqual(result, ["term1", "quoted term", "term2", "another quoted", "term3"])

    def test_parse_field_specific_search(self):
        """Test parsing field:value syntax."""
        result = SearchTermParser.parse("Title:MongoDB Author:Smith Status:active")
        self.assertEqual(result, ["Title:MongoDB", "Author:Smith", "Status:active"])

    def test_parse_field_specific_with_quoted_value(self):
        """Test parsing field:value with quoted value."""
        result = SearchTermParser.parse('Title:"Advanced MongoDB" Author:Smith')
        self.assertEqual(result, ["Title:Advanced MongoDB", "Author:Smith"])

    def test_parse_empty_quotes(self):
        """Test parsing empty quoted strings."""
        result = SearchTermParser.parse('term1 "" term2')
        self.assertEqual(result, ["term1", "", "term2"])

        result = SearchTermParser.parse("term1 '' term2")
        self.assertEqual(result, ["term1", "", "term2"])

    def test_parse_quotes_within_quotes(self):
        """Test parsing quotes within different quote types."""
        # Double quotes inside single quotes
        result = SearchTermParser.parse('\'He said "Hello"\'')
        self.assertEqual(result, ['He said "Hello"'])

        # Single quotes inside double quotes
        result = SearchTermParser.parse('"It\'s working"')
        self.assertEqual(result, ["It's working"])

    def test_parse_malformed_quotes_unclosed(self):
        """Test parsing with unclosed quotes (graceful fallback)."""
        # shlex will raise ValueError for unclosed quotes, which our parser catches
        result = SearchTermParser.parse('term1 "unclosed quote term2')
        # Should fall back to simple split
        self.assertIsInstance(result, list)
        # The fallback behavior splits on whitespace: ['term1', '"unclosed', 'quote', 'term2']
        self.assertEqual(len(result), 4)

    def test_parse_special_characters(self):
        """Test parsing with special characters."""
        result = SearchTermParser.parse("user@example.com test-value date:2023-01-01")
        self.assertEqual(result, ["user@example.com", "test-value", "date:2023-01-01"])

    def test_parse_multiple_spaces(self):
        """Test parsing with multiple spaces between terms."""
        result = SearchTermParser.parse("term1    term2     term3")
        self.assertEqual(result, ["term1", "term2", "term3"])

    def test_parse_tabs_and_newlines(self):
        """Test parsing with tabs and newlines."""
        result = SearchTermParser.parse("term1\tterm2\nterm3")
        self.assertEqual(result, ["term1", "term2", "term3"])

    def test_parse_quoted_empty_string_only(self):
        """Test parsing string with only quotes."""
        result = SearchTermParser.parse('""')
        self.assertEqual(result, [""])

    def test_parse_complex_search_example(self):
        """Test complex real-world search example."""
        search = 'Title:"MongoDB Guide" Author:Smith Year:>2020 Status:published'
        result = SearchTermParser.parse(search)
        self.assertEqual(result, [
            "Title:MongoDB Guide",
            "Author:Smith",
            "Year:>2020",
            "Status:published"
        ])

    def test_parse_quoted_colon_syntax(self):
        """Test quoted phrases with colon syntax."""
        result = SearchTermParser.parse('Field:"value with spaces"')
        self.assertEqual(result, ["Field:value with spaces"])

    def test_parse_backslash_escapes(self):
        """Test parsing with backslash escapes."""
        # shlex handles backslash escapes in POSIX mode
        result = SearchTermParser.parse('term1 "escaped\\ space" term2')
        # In POSIX mode, backslash escapes the next character
        # So "escaped\\ space" becomes "escaped\ space" (backslash preserved)
        self.assertIn("escaped\\ space", result)

    def test_parse_unicode_characters(self):
        """Test parsing with unicode characters."""
        result = SearchTermParser.parse('emoji:🎉 chinese:你好 term')
        self.assertEqual(result, ["emoji:🎉", "chinese:你好", "term"])

    def test_parse_numeric_values(self):
        """Test parsing with numeric values."""
        result = SearchTermParser.parse("age:25 price:>100 quantity:<=50")
        self.assertEqual(result, ["age:25", "price:>100", "quantity:<=50"])

    def test_parse_preserves_operator_symbols(self):
        """Test that comparison operators are preserved."""
        result = SearchTermParser.parse("field:>=100 another:<50")
        self.assertEqual(result, ["field:>=100", "another:<50"])

    def test_parse_single_word_no_split(self):
        """Test single word returns as single element."""
        result = SearchTermParser.parse("MongoDB")
        self.assertEqual(result, ["MongoDB"])

    def test_parse_whitespace_only(self):
        """Test parsing whitespace-only string."""
        result = SearchTermParser.parse("   ")
        self.assertEqual(result, [])

        result = SearchTermParser.parse("\t\n")
        self.assertEqual(result, [])


if __name__ == '__main__':
    unittest.main()
