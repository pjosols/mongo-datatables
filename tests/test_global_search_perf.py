"""Tests for build_global_search field mapper lookup efficiency."""
from unittest.mock import MagicMock, call
from mongo_datatables.utils import FieldMapper
from mongo_datatables.query_builder import MongoQueryBuilder


def _make_qb(columns):
    """Build a MongoQueryBuilder with a real FieldMapper for the given column names."""
    fm = FieldMapper(columns)
    return MongoQueryBuilder(fm, use_text_index=False, has_text_index=False)


def test_field_mapper_called_once_per_column_not_per_term():
    """get_field_type and get_db_field should be called once per column, not once per (term, column)."""
    fm = MagicMock(spec=FieldMapper)
    fm.get_field_type.return_value = "text"
    fm.get_db_field.side_effect = lambda c: c

    qb = MongoQueryBuilder(fm, use_text_index=False, has_text_index=False)
    columns = ["name", "city", "country"]
    terms = ["alice", "bob", "carol"]

    qb.build_global_search(terms, columns)

    # Each column looked up exactly once regardless of term count
    assert fm.get_field_type.call_count == len(columns)
    assert fm.get_db_field.call_count == len(columns)


def test_global_search_multi_term_produces_correct_or_conditions():
    """Multiple terms across multiple columns produce the right $or conditions."""
    qb = _make_qb(["name", "city"])
    result = qb.build_global_search(["alice", "bob"], ["name", "city"])

    assert "$or" in result
    # 2 terms × 2 columns = 4 conditions
    assert len(result["$or"]) == 4


def test_global_search_quoted_phrase_word_boundary():
    """Quoted single term uses word-boundary regex."""
    qb = _make_qb(["name"])
    result = qb.build_global_search(["alice"], ["name"], original_search='"alice"')

    assert "$or" in result
    assert len(result["$or"]) == 1
    pattern = result["$or"][0]["name"]["$regex"]
    assert pattern.startswith("\\b") and pattern.endswith("\\b")


def test_global_search_skips_date_columns():
    """Date-typed columns are excluded from global search results."""
    from mongo_datatables.datatables import DataField
    fields = [DataField("created", "date"), DataField("name", "string")]
    fm = FieldMapper(fields)
    qb = MongoQueryBuilder(fm, use_text_index=False, has_text_index=False)

    result = qb.build_global_search(["alice"], ["created", "name"])

    assert "$or" in result
    # Only 'name' should appear — 'created' is date type
    fields_searched = [list(cond.keys())[0] for cond in result["$or"]]
    assert "created" not in fields_searched
    assert "name" in fields_searched
