"""Shared helpers for datatables unit tests."""
from unittest.mock import MagicMock, patch

from mongo_datatables import DataTables, DataField


def make_dt_wire(search_extra=None, columns_search_extra=None, search_fixed_legacy=None):
    """Build a DataTables instance backed by mocked MongoDB for wire-format tests.

    search_extra: dict merged into the top-level search object.
    columns_search_extra: list of dicts merged into each column's search object.
    search_fixed_legacy: value placed at request_args["searchFixed"].
    Returns a DataTables instance with has_text_index patched to False.
    """
    db = MagicMock()
    collection = MagicMock()
    db.__getitem__ = MagicMock(return_value=collection)
    collection.index_information.return_value = {}

    search = {"value": "", "regex": False}
    if search_extra:
        search.update(search_extra)

    columns = [
        {"data": "name", "searchable": True, "orderable": True,
         "search": {"value": "", "regex": False}},
        {"data": "status", "searchable": True, "orderable": True,
         "search": {"value": "", "regex": False}},
    ]
    if columns_search_extra:
        for i, extra in enumerate(columns_search_extra):
            if extra and i < len(columns):
                columns[i]["search"].update(extra)

    request_args = {
        "draw": "1", "start": "0", "length": "10",
        "search": search,
        "columns": columns,
        "order": [{"column": 0, "dir": "asc"}],
    }
    if search_fixed_legacy is not None:
        request_args["searchFixed"] = search_fixed_legacy

    data_fields = [DataField("name", "string"), DataField("status", "string")]
    dt = DataTables(db, "users", request_args, data_fields=data_fields)
    with patch.object(type(dt), "has_text_index",
                      new_callable=lambda: property(lambda self: False)):
        return dt
