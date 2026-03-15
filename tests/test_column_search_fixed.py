import pytest
from unittest.mock import MagicMock, patch
from mongo_datatables import DataTables, DataField


def make_dt(columns_extra=None, search_fixed_global=None):
    """Build a DataTables instance with optional per-column searchFixed."""
    db = MagicMock()
    collection = MagicMock()
    db.__getitem__ = MagicMock(return_value=collection)
    collection.index_information.return_value = {}

    columns = [
        {"data": "name", "search": {"value": "", "regex": False}, "searchable": True, "orderable": True},
        {"data": "status", "search": {"value": "", "regex": False}, "searchable": True, "orderable": True},
        {"data": "age", "search": {"value": "", "regex": False}, "searchable": True, "orderable": True},
    ]
    if columns_extra:
        for i, extra in enumerate(columns_extra):
            if extra and i < len(columns):
                columns[i].update(extra)

    request_args = {
        "draw": "1",
        "start": "0",
        "length": "10",
        "search": {"value": "", "regex": False},
        "columns": columns,
        "order": [{"column": 0, "dir": "asc"}],
    }
    if search_fixed_global:
        request_args["searchFixed"] = search_fixed_global

    data_fields = [
        DataField("name", data_type="string"),
        DataField("status", data_type="string"),
        DataField("age", data_type="number"),
    ]
    dt = DataTables(db, "users", request_args, data_fields=data_fields)
    with patch.object(type(dt), "has_text_index", new_callable=lambda: property(lambda self: False)):
        return dt


class TestColumnSearchFixed:

    def test_single_column_fixed_search(self):
        """Per-column searchFixed applies only to that column's field."""
        dt = make_dt(columns_extra=[{"searchFixed": {"lock": "Alice"}}])
        with patch.object(type(dt), "has_text_index", new_callable=lambda: property(lambda self: False)):
            f = dt.filter
        # Should produce a regex condition on 'name' field only
        assert "name" in str(f)
        assert "status" not in str(f) or "$and" in str(f)

    def test_column_fixed_search_scoped_to_column(self):
        """Per-column searchFixed on status column targets status field."""
        dt = make_dt(columns_extra=[None, {"searchFixed": {"statusLock": "active"}}])
        with patch.object(type(dt), "has_text_index", new_callable=lambda: property(lambda self: False)):
            f = dt.filter
        assert "status" in str(f)

    def test_multiple_column_fixed_searches_anded(self):
        """Multiple per-column searchFixed conditions are ANDed."""
        dt = make_dt(columns_extra=[
            {"searchFixed": {"nameLock": "Alice"}},
            {"searchFixed": {"statusLock": "active"}},
        ])
        with patch.object(type(dt), "has_text_index", new_callable=lambda: property(lambda self: False)):
            f = dt.filter
        assert "$and" in str(f)
        assert "name" in str(f)
        assert "status" in str(f)

    def test_empty_column_fixed_search_ignored(self):
        """Empty searchFixed values are skipped."""
        dt = make_dt(columns_extra=[{"searchFixed": {"lock": ""}}])
        with patch.object(type(dt), "has_text_index", new_callable=lambda: property(lambda self: False)):
            f = dt.filter
        assert f == {}

    def test_non_dict_column_fixed_search_ignored(self):
        """Non-dict searchFixed on a column is ignored."""
        dt = make_dt(columns_extra=[{"searchFixed": "not-a-dict"}])
        with patch.object(type(dt), "has_text_index", new_callable=lambda: property(lambda self: False)):
            f = dt.filter
        assert f == {}

    def test_column_fixed_and_global_fixed_combined(self):
        """Per-column and global searchFixed both apply (ANDed)."""
        dt = make_dt(
            columns_extra=[{"searchFixed": {"nameLock": "Alice"}}],
            search_fixed_global={"tenantFilter": "acme"}
        )
        with patch.object(type(dt), "has_text_index", new_callable=lambda: property(lambda self: False)):
            f = dt.filter
        assert "$and" in str(f)

    def test_no_column_fixed_search_no_effect(self):
        """Columns without searchFixed produce no extra conditions."""
        dt = make_dt()
        with patch.object(type(dt), "has_text_index", new_callable=lambda: property(lambda self: False)):
            f = dt.filter
        assert f == {}

    def test_multiple_named_searches_in_one_column(self):
        """Multiple named searches in one column's searchFixed are all applied."""
        dt = make_dt(columns_extra=[{"searchFixed": {"lock1": "Alice", "lock2": "Bob"}}])
        with patch.object(type(dt), "has_text_index", new_callable=lambda: property(lambda self: False)):
            f = dt.filter
        # Both values should produce conditions on 'name'
        assert "name" in str(f)
