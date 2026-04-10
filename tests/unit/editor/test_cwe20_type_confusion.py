"""Tests for CWE-20: type confusion injection via unconditional JSON parsing.

Verifies that preprocess_document() only attempts json.loads for fields whose
declared data_type is 'array' or 'object', and rejects parsed values that would
change the type of string/number/boolean fields.
"""
import pytest

from mongo_datatables.datatables import DataField
from mongo_datatables.editor.document import preprocess_document


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make(fields_spec: dict[str, str], doc: dict) -> tuple[dict, dict]:
    """Call preprocess_document with fields built from {name: data_type} spec."""
    df = [DataField(name, dtype) for name, dtype in fields_spec.items()]
    fields = {f.alias: f for f in df}
    return preprocess_document(doc, fields, df)


# ---------------------------------------------------------------------------
# JSON parsing is BLOCKED for non-array/object types
# ---------------------------------------------------------------------------

class TestJsonParsingBlockedForScalarTypes:
    """JSON strings must NOT be parsed for string, number, or boolean fields."""

    def test_string_field_json_array_stays_raw(self):
        """A JSON array string in a 'string' field must remain a string."""
        processed, _ = _make({"name": "string"}, {"name": '["injected", "array"]'})
        assert processed["name"] == '["injected", "array"]'
        assert isinstance(processed["name"], str)

    def test_string_field_json_object_stays_raw(self):
        """A JSON object string in a 'string' field must remain a string."""
        processed, _ = _make({"bio": "string"}, {"bio": '{"$ne": null}'})
        assert processed["bio"] == '{"$ne": null}'
        assert isinstance(processed["bio"], str)

    def test_number_field_json_object_stays_raw(self):
        """A JSON object string in a 'number' field must remain a string."""
        processed, _ = _make({"score": "number"}, {"score": '{"$gt": 0}'})
        assert processed["score"] == '{"$gt": 0}'
        assert isinstance(processed["score"], str)

    def test_boolean_field_json_array_stays_raw(self):
        """A JSON array string in a 'boolean' field must remain a string."""
        processed, _ = _make({"active": "boolean"}, {"active": '[true, false]'})
        assert processed["active"] == '[true, false]'
        assert isinstance(processed["active"], str)

    def test_date_field_json_object_stays_raw(self):
        """A JSON object string in a 'date' field is not parsed as JSON."""
        processed, _ = _make({"created_at": "date"}, {"created_at": '{"$date": "2023-01-01"}'})
        # Should not be a dict — either raw string or a datetime (date parse attempt)
        assert not isinstance(processed.get("created_at"), dict)

    def test_string_field_json_number_stays_raw(self):
        """A bare JSON number string in a 'string' field must remain a string."""
        processed, _ = _make({"label": "string"}, {"label": "42"})
        assert processed["label"] == "42"
        assert isinstance(processed["label"], str)

    def test_string_field_json_bool_stays_raw(self):
        """A JSON boolean string in a 'string' field must remain a string."""
        processed, _ = _make({"flag": "string"}, {"flag": "true"})
        assert processed["flag"] == "true"
        assert isinstance(processed["flag"], str)


# ---------------------------------------------------------------------------
# JSON parsing is ALLOWED for array and object types
# ---------------------------------------------------------------------------

class TestJsonParsingAllowedForContainerTypes:
    """JSON strings MUST be parsed for 'array' and 'object' fields."""

    def test_array_field_json_string_is_parsed(self):
        """A JSON array string in an 'array' field is parsed to a list."""
        processed, _ = _make({"tags": "array"}, {"tags": '["a", "b"]'})
        assert processed["tags"] == ["a", "b"]
        assert isinstance(processed["tags"], list)

    def test_object_field_json_string_is_parsed(self):
        """A JSON object string in an 'object' field is parsed to a dict."""
        processed, _ = _make({"meta": "object"}, {"meta": '{"key": "value"}'})
        assert processed["meta"] == {"key": "value"}
        assert isinstance(processed["meta"], dict)

    def test_array_field_invalid_json_stays_raw(self):
        """An invalid JSON string in an 'array' field stays as a string."""
        processed, _ = _make({"tags": "array"}, {"tags": "not-json"})
        assert processed["tags"] == "not-json"
        assert isinstance(processed["tags"], str)

    def test_object_field_invalid_json_stays_raw(self):
        """An invalid JSON string in an 'object' field stays as a string."""
        processed, _ = _make({"meta": "object"}, {"meta": "{bad json"})
        assert processed["meta"] == "{bad json"
        assert isinstance(processed["meta"], str)


# ---------------------------------------------------------------------------
# Mixed document: only container fields are parsed
# ---------------------------------------------------------------------------

class TestMixedDocumentTypeEnforcement:
    """In a document with mixed field types, only array/object fields are JSON-parsed."""

    def test_only_container_fields_parsed_in_mixed_doc(self):
        """String and number fields stay raw; array and object fields are parsed."""
        doc = {
            "name": '["should", "stay", "raw"]',
            "score": '{"$gt": 0}',
            "tags": '["parsed", "list"]',
            "meta": '{"parsed": true}',
        }
        processed, _ = _make(
            {"name": "string", "score": "number", "tags": "array", "meta": "object"},
            doc,
        )
        assert processed["name"] == '["should", "stay", "raw"]'
        assert processed["score"] == '{"$gt": 0}'
        assert processed["tags"] == ["parsed", "list"]
        assert processed["meta"] == {"parsed": True}

    def test_injection_attempt_on_string_field_blocked(self):
        """Injecting a nested dict via a string field is blocked."""
        doc = {"username": '{"$where": "sleep(1000)"}'}
        processed, _ = _make({"username": "string"}, doc)
        assert isinstance(processed["username"], str)
        assert processed["username"] == '{"$where": "sleep(1000)"}'

    def test_injection_attempt_on_number_field_blocked(self):
        """Injecting a nested dict via a number field is blocked."""
        doc = {"age": '{"$gt": -1}'}
        processed, _ = _make({"age": "number"}, doc)
        assert isinstance(processed["age"], str)

    def test_non_string_values_pass_through_unchanged(self):
        """Non-string values (int, bool, list) are not affected by the JSON gate."""
        doc = {"count": 5, "active": True, "tags": ["x", "y"]}
        processed, _ = _make(
            {"count": "number", "active": "boolean", "tags": "array"},
            doc,
        )
        assert processed["count"] == 5
        assert processed["active"] is True
        assert processed["tags"] == ["x", "y"]
