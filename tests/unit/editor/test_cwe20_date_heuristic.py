"""Verify CWE-20 fix: date parsing only for declared fields, not name-suffix heuristics.

Ensures preprocess_document() rejects attacker-controlled field names ending in
'date', 'time', or 'at' and only parses ISO datetimes for fields explicitly
declared with data_type == 'date' in data_fields.
"""
import pytest
from datetime import datetime
from unittest.mock import MagicMock

from mongo_datatables.editor.document import preprocess_document
from mongo_datatables.data_field import DataField


def _call(doc, data_fields=None):
    fields = {f.alias: f for f in (data_fields or [])}
    return preprocess_document(doc, fields, data_fields or [])


# --- Heuristic suffix must NOT trigger date parsing ---


def test_suffix_date_not_parsed_without_declaration():
    """Field ending in 'date' must NOT be auto-parsed as datetime."""
    processed, _ = _call({"update_date": "2024-01-15T00:00:00"})
    assert isinstance(processed["update_date"], str)


def test_suffix_time_not_parsed_without_declaration():
    """Field ending in 'time' must NOT be auto-parsed as datetime."""
    processed, _ = _call({"start_time": "2024-01-15T10:00:00"})
    assert isinstance(processed["start_time"], str)


def test_suffix_at_not_parsed_without_declaration():
    """Field ending in 'at' must NOT be auto-parsed as datetime."""
    processed, _ = _call({"created_at": "2024-01-15T10:00:00"})
    assert isinstance(processed["created_at"], str)


def test_arbitrary_attacker_field_not_parsed():
    """Attacker-supplied field name matching date suffix must stay as string."""
    processed, _ = _call({"evil_date": "2024-01-15T00:00:00"})
    assert isinstance(processed["evil_date"], str)


def test_dot_notation_suffix_not_parsed_without_declaration():
    """Nested field with date suffix must NOT be auto-parsed."""
    _, dot = _call({"profile.created_at": "2024-01-15T10:00:00"})
    assert isinstance(dot["profile.created_at"], str)


# --- Declared date fields MUST be parsed ---


def test_declared_date_field_is_parsed():
    """Field declared with data_type='date' must be parsed to datetime."""
    df = DataField("created_at", "date")
    processed, _ = _call({"created_at": "2024-01-15T10:00:00"}, [df])
    assert isinstance(processed["created_at"], datetime)


def test_declared_date_field_nested_is_parsed():
    """Nested field declared with data_type='date' must be parsed to datetime."""
    df = DataField("profile.created_at", "date")
    _, dot = _call({"profile.created_at": "2024-01-15T10:00:00"}, [df])
    assert isinstance(dot["profile.created_at"], datetime)


def test_non_date_declared_field_not_parsed():
    """Field declared as 'string' must not be parsed as datetime even with date suffix."""
    df = DataField("created_at", "string")
    processed, _ = _call({"created_at": "2024-01-15T10:00:00"}, [df])
    assert isinstance(processed["created_at"], str)


# --- No data_fields whitelist: no date parsing at all ---


def test_no_data_fields_no_date_parsing():
    """Without data_fields, no date parsing should occur regardless of field name."""
    processed, _ = _call({"created_at": "2024-01-15T10:00:00", "end_date": "2024-06-01"})
    assert isinstance(processed["created_at"], str)
    assert isinstance(processed["end_date"], str)


# --- Multiple fields: only declared date fields parsed ---


def test_only_declared_date_fields_parsed_among_many():
    """Only the field declared as date is parsed; undeclared fields are filtered when data_fields set."""
    df = DataField("published_at", "date")
    processed, _ = _call(
        {"published_at": "2024-03-01T00:00:00", "created_at": "2024-01-01T00:00:00"},
        [df],
    )
    assert isinstance(processed["published_at"], datetime)
    # created_at is not in data_fields whitelist, so it is filtered out
    assert "created_at" not in processed
