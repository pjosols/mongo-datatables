"""Tests for the row_id feature (v1.21.0)."""
from bson.objectid import ObjectId
from tests.base_test import BaseDataTablesTest
from mongo_datatables import DataTables


class TestRowId(BaseDataTablesTest):

    def _make_dt(self, row_id=None, extra_columns=None):
        dt = DataTables(self.mongo, "test_collection", self.request_args, row_id=row_id)
        dt.collection = self.collection
        return dt

    # 1. Default (row_id=None): DT_RowId from _id, _id removed from row
    def test_row_id_none_uses_id(self):
        oid = ObjectId()
        rows = self._make_dt()._process_cursor([{"_id": oid, "name": "Alice"}])
        assert rows[0]["DT_RowId"] == str(oid)
        assert "_id" not in rows[0]

    # 2. row_id='employee_id': DT_RowId = str(employee_id value)
    def test_row_id_custom_field_sets_dt_row_id(self):
        rows = self._make_dt(row_id="employee_id")._process_cursor(
            [{"_id": ObjectId(), "employee_id": "EMP-42", "name": "Bob"}]
        )
        assert rows[0]["DT_RowId"] == "EMP-42"

    # 3. When row_id='employee_id', the employee_id key remains in the row
    def test_row_id_custom_field_stays_in_row(self):
        rows = self._make_dt(row_id="employee_id")._process_cursor(
            [{"_id": ObjectId(), "employee_id": "EMP-42", "name": "Bob"}]
        )
        assert "employee_id" in rows[0]
        assert rows[0]["employee_id"] == "EMP-42"

    # 4. When row_id is set, _id is NOT popped (stays in doc)
    def test_row_id_id_not_popped_when_custom_row_id(self):
        oid = ObjectId()
        rows = self._make_dt(row_id="employee_id")._process_cursor(
            [{"_id": oid, "employee_id": "EMP-42", "name": "Bob"}]
        )
        # _id should still be present (not popped) and formatted as string
        assert "_id" in rows[0]
        assert rows[0]["_id"] == str(oid)

    # 5. row_id field missing from doc falls back to _id
    def test_row_id_field_not_in_doc_falls_back_to_id(self):
        oid = ObjectId()
        rows = self._make_dt(row_id="missing_field")._process_cursor(
            [{"_id": oid, "name": "Carol"}]
        )
        assert rows[0]["DT_RowId"] == str(oid)
        assert "_id" not in rows[0]

    # 6. row_id='sku' is included in projection
    def test_row_id_included_in_projection(self):
        dt = self._make_dt(row_id="sku")
        assert dt.projection.get("sku") == 1

    # 7. row_id field not in columns is still projected
    def test_row_id_not_in_columns_still_projected(self):
        # columns only has name/email/status — no 'sku'
        dt = self._make_dt(row_id="sku")
        column_fields = {c["data"] for c in self.request_args["columns"]}
        assert "sku" not in column_fields
        assert dt.projection.get("sku") == 1

    # 8. Backward compatibility: no row_id param works identically
    def test_row_id_backward_compatible(self):
        oid = ObjectId()
        dt = DataTables(self.mongo, "test_collection", self.request_args)
        dt.collection = self.collection
        rows = dt._process_cursor([{"_id": oid, "name": "Dave"}])
        assert rows[0]["DT_RowId"] == str(oid)
        assert "_id" not in rows[0]
        assert "DT_RowClass" not in rows[0]
        assert "DT_RowData" not in rows[0]
        assert "DT_RowAttr" not in rows[0]
