"""Unit tests for DateHandler utility class.

This test suite specifically validates the critical date arithmetic bug fix
where datetime(..., day+1) would crash on month boundaries.
"""
import unittest
from datetime import datetime, timedelta
from mongo_datatables.utils import DateHandler
from mongo_datatables.exceptions import FieldMappingError


class TestDateHandler(unittest.TestCase):
    """Test cases for DateHandler utility class."""

    def test_parse_iso_date_valid(self):
        """Test parsing valid ISO format dates."""
        # Standard date
        result = DateHandler.parse_iso_date("2023-06-15")
        self.assertEqual(result, datetime(2023, 6, 15))

        # Start of year
        result = DateHandler.parse_iso_date("2023-01-01")
        self.assertEqual(result, datetime(2023, 1, 1))

        # End of year
        result = DateHandler.parse_iso_date("2023-12-31")
        self.assertEqual(result, datetime(2023, 12, 31))

    def test_parse_iso_date_invalid(self):
        """Test parsing invalid date strings."""
        # Wrong format
        with self.assertRaises(FieldMappingError):
            DateHandler.parse_iso_date("06/15/2023")

        # Incomplete date
        with self.assertRaises(FieldMappingError):
            DateHandler.parse_iso_date("2023-06")

        # Invalid date
        with self.assertRaises(FieldMappingError):
            DateHandler.parse_iso_date("2023-13-01")  # Invalid month

        with self.assertRaises(FieldMappingError):
            DateHandler.parse_iso_date("2023-02-30")  # Invalid day

    def test_parse_iso_datetime_with_timezone(self):
        """Test parsing datetime strings with timezone info."""
        # With Z suffix
        result = DateHandler.parse_iso_datetime("2023-06-15T10:30:00Z")
        self.assertEqual(result.year, 2023)
        self.assertEqual(result.month, 6)
        self.assertEqual(result.day, 15)
        self.assertEqual(result.hour, 10)

        # With timezone offset
        result = DateHandler.parse_iso_datetime("2023-06-15T10:30:00+00:00")
        self.assertEqual(result.year, 2023)
        self.assertEqual(result.hour, 10)

    def test_parse_iso_datetime_without_timezone(self):
        """Test parsing datetime strings without timezone."""
        result = DateHandler.parse_iso_datetime("2023-06-15T10:30:00")
        self.assertEqual(result, datetime(2023, 6, 15, 10, 30, 0))

    def test_get_next_day_december_31(self):
        """CRITICAL TEST: Verify Dec 31 doesn't crash (the original bug)."""
        # This is the exact scenario that caused the crash with datetime(..., day+1)
        dec_31 = datetime(2023, 12, 31)
        next_day = DateHandler.get_next_day(dec_31)

        # Should be Jan 1 of next year
        self.assertEqual(next_day, datetime(2024, 1, 1))
        self.assertEqual(next_day.year, 2024)
        self.assertEqual(next_day.month, 1)
        self.assertEqual(next_day.day, 1)

    def test_get_next_day_january_31(self):
        """Test Jan 31 -> Feb 1 transition."""
        jan_31 = datetime(2023, 1, 31)
        next_day = DateHandler.get_next_day(jan_31)

        self.assertEqual(next_day, datetime(2023, 2, 1))
        self.assertEqual(next_day.month, 2)
        self.assertEqual(next_day.day, 1)

    def test_get_next_day_february_28_non_leap(self):
        """Test Feb 28 -> Mar 1 transition (non-leap year)."""
        feb_28 = datetime(2023, 2, 28)  # 2023 is not a leap year
        next_day = DateHandler.get_next_day(feb_28)

        self.assertEqual(next_day, datetime(2023, 3, 1))
        self.assertEqual(next_day.month, 3)
        self.assertEqual(next_day.day, 1)

    def test_get_next_day_february_29_leap(self):
        """Test Feb 29 -> Mar 1 transition (leap year)."""
        feb_29 = datetime(2024, 2, 29)  # 2024 is a leap year
        next_day = DateHandler.get_next_day(feb_29)

        self.assertEqual(next_day, datetime(2024, 3, 1))
        self.assertEqual(next_day.month, 3)
        self.assertEqual(next_day.day, 1)

    def test_get_next_day_all_month_ends(self):
        """Test all month-end transitions for a full year."""
        # Days in each month (non-leap year)
        month_ends = {
            1: 31,   # Jan
            2: 28,   # Feb (non-leap)
            3: 31,   # Mar
            4: 30,   # Apr
            5: 31,   # May
            6: 30,   # Jun
            7: 31,   # Jul
            8: 31,   # Aug
            9: 30,   # Sep
            10: 31,  # Oct
            11: 30,  # Nov
            12: 31   # Dec
        }

        year = 2023
        for month, last_day in month_ends.items():
            with self.subTest(month=month):
                month_end = datetime(year, month, last_day)
                next_day = DateHandler.get_next_day(month_end)

                # Calculate expected next month and year
                expected_month = month + 1 if month < 12 else 1
                expected_year = year if month < 12 else year + 1

                self.assertEqual(next_day.day, 1, f"Failed for {year}-{month}-{last_day}")
                self.assertEqual(next_day.month, expected_month, f"Failed for {year}-{month}-{last_day}")
                self.assertEqual(next_day.year, expected_year, f"Failed for {year}-{month}-{last_day}")

    def test_get_next_day_leap_year_full_test(self):
        """Test all month-end transitions for a leap year."""
        month_ends = {
            1: 31, 2: 29, 3: 31, 4: 30, 5: 31, 6: 30,
            7: 31, 8: 31, 9: 30, 10: 31, 11: 30, 12: 31
        }

        year = 2024  # Leap year
        for month, last_day in month_ends.items():
            with self.subTest(month=month, year=year):
                month_end = datetime(year, month, last_day)
                next_day = DateHandler.get_next_day(month_end)

                expected_month = month + 1 if month < 12 else 1
                expected_year = year if month < 12 else year + 1

                self.assertEqual(next_day.day, 1)
                self.assertEqual(next_day.month, expected_month)
                self.assertEqual(next_day.year, expected_year)

    def test_get_date_range_for_comparison_greater_than(self):
        """Test date range for > operator."""
        result = DateHandler.get_date_range_for_comparison("2023-06-15", ">")

        # Should be >= 2023-06-16 (next day)
        self.assertIn("$gte", result)
        self.assertEqual(result["$gte"], datetime(2023, 6, 16))

    def test_get_date_range_for_comparison_less_than(self):
        """Test date range for < operator."""
        result = DateHandler.get_date_range_for_comparison("2023-06-15", "<")

        # Should be < 2023-06-15
        self.assertIn("$lt", result)
        self.assertEqual(result["$lt"], datetime(2023, 6, 15))

    def test_get_date_range_for_comparison_greater_equal(self):
        """Test date range for >= operator."""
        result = DateHandler.get_date_range_for_comparison("2023-06-15", ">=")

        # Should be >= 2023-06-15
        self.assertIn("$gte", result)
        self.assertEqual(result["$gte"], datetime(2023, 6, 15))

    def test_get_date_range_for_comparison_less_equal(self):
        """Test date range for <= operator."""
        result = DateHandler.get_date_range_for_comparison("2023-06-15", "<=")

        # Should be < 2023-06-16 (whole day included)
        self.assertIn("$lt", result)
        self.assertEqual(result["$lt"], datetime(2023, 6, 16))

    def test_get_date_range_for_comparison_equals(self):
        """Test date range for = operator (exact match for whole day)."""
        result = DateHandler.get_date_range_for_comparison("2023-06-15", "=")

        # Should match the whole day: >= 2023-06-15 AND < 2023-06-16
        self.assertIn("$gte", result)
        self.assertIn("$lt", result)
        self.assertEqual(result["$gte"], datetime(2023, 6, 15))
        self.assertEqual(result["$lt"], datetime(2023, 6, 16))

    def test_get_date_range_for_comparison_no_operator(self):
        """Test date range with no operator (defaults to exact match)."""
        result = DateHandler.get_date_range_for_comparison("2023-06-15", None)

        # Should match the whole day
        self.assertIn("$gte", result)
        self.assertIn("$lt", result)
        self.assertEqual(result["$gte"], datetime(2023, 6, 15))
        self.assertEqual(result["$lt"], datetime(2023, 6, 16))

    def test_get_date_range_december_31_with_operators(self):
        """CRITICAL TEST: Verify Dec 31 works with all operators."""
        # This specifically tests the bug fix

        # Test >
        result = DateHandler.get_date_range_for_comparison("2023-12-31", ">")
        self.assertEqual(result["$gte"], datetime(2024, 1, 1))

        # Test <=
        result = DateHandler.get_date_range_for_comparison("2023-12-31", "<=")
        self.assertEqual(result["$lt"], datetime(2024, 1, 1))

        # Test =
        result = DateHandler.get_date_range_for_comparison("2023-12-31", "=")
        self.assertEqual(result["$gte"], datetime(2023, 12, 31))
        self.assertEqual(result["$lt"], datetime(2024, 1, 1))

    def test_get_date_range_all_month_ends_with_equals(self):
        """Test = operator for all month-end dates."""
        month_ends = [
            "2023-01-31", "2023-02-28", "2023-03-31", "2023-04-30",
            "2023-05-31", "2023-06-30", "2023-07-31", "2023-08-31",
            "2023-09-30", "2023-10-31", "2023-11-30", "2023-12-31"
        ]

        for date_str in month_ends:
            with self.subTest(date=date_str):
                result = DateHandler.get_date_range_for_comparison(date_str, "=")

                # Should have both $gte and $lt
                self.assertIn("$gte", result)
                self.assertIn("$lt", result)

                # The difference should be exactly 1 day
                gte_date = result["$gte"]
                lt_date = result["$lt"]
                self.assertEqual((lt_date - gte_date).days, 1)

    def test_get_date_range_invalid_operator(self):
        """Test invalid operator raises exception."""
        with self.assertRaises(FieldMappingError):
            DateHandler.get_date_range_for_comparison("2023-06-15", "!=")

    def test_get_date_range_invalid_date(self):
        """Test invalid date string raises exception."""
        with self.assertRaises(FieldMappingError):
            DateHandler.get_date_range_for_comparison("invalid-date", ">")


if __name__ == '__main__':
    unittest.main()
