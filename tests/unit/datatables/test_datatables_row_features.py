"""Test DataTables row features: DT_RowId, row_class, row_data, row_attr, alias remapping."""
import unittest
from bson.objectid import ObjectId

from mongo_datatables import DataTables, DataField
from tests.unit.base_test import BaseDataTablesTest


class TestRemapAliases(BaseDataTablesTest):
    def _dt(self, data_fields=None):
        return DataTables(self.mongo, "test_collection", self.request_args,
                          data_fields=data_fields or [])

    def test_no_alias_no_change(self):
        dt = self._dt()
        doc = {"title": "Hello", "DT_RowId": "abc"}
        assert dt._remap_aliases(doc) == {"title": "Hello", "DT_RowId": "abc"}

    def test_simple_rename(self):
        dt = self._dt([DataField("pub_date", "date", alias="Published")])
        doc = {"pub_date": "2001-01-01"}
        result = dt._remap_aliases(doc)
        assert result == {"Published": "2001-01-01"}
        assert "pub_date" not in result

    def test_nested_field_extracted_to_alias(self):
        dt = self._dt([DataField("PublisherInfo.Date", "date", alias="Published")])
        doc = {"PublisherInfo": {"Date": "2001-12-12"}}
        result = dt._remap_aliases(doc)
        assert result["Published"] == "2001-12-12"
        assert "PublisherInfo" not in result

    def test_nested_field_missing_value_unchanged(self):
        dt = self._dt([DataField("PublisherInfo.Date", "date", alias="Published")])
        doc = {"title": "Book"}
        result = dt._remap_aliases(doc)
        assert "Published" not in result
        assert result == {"title": "Book"}

    def test_shared_parent_not_deleted(self):
        dt = self._dt([
            DataField("Info.Date", "date", alias="Published"),
            DataField("Info.Author", "string", alias="Writer"),
        ])
        doc = {"Info": {"Date": "2001-01-01", "Author": "Bob"}}
        result = dt._remap_aliases(doc)
        assert result["Published"] == "2001-01-01"
        assert result["Writer"] == "Bob"

    def test_process_cursor_applies_remapping(self):
        dt = self._dt([DataField("PublisherInfo.Date", "date", alias="Published")])
        cursor = [{"_id": "abc", "PublisherInfo": {"Date": "2001-12-12"}}]
        result = dt._process_cursor(cursor)
        assert len(result) == 1
        assert result[0]["Published"] == "2001-12-12"
        assert result[0]["DT_RowId"] == "abc"
        assert "PublisherInfo" not in result[0]

    def test_alias_same_as_db_field_no_change(self):
        dt = self._dt([DataField("Date", "date")])
        doc = {"Date": "2001-01-01"}
        result = dt._remap_aliases(doc)
        assert result == {"Date": "2001-01-01"}


class TestRowId(BaseDataTablesTest):

    def _make_dt(self, row_id=None):
        dt = DataTables(self.mongo, "test_collection", self.request_args, row_id=row_id)
        dt.collection = self.collection
        return dt

    def test_row_id_none_uses_id(self):
        oid = ObjectId()
        rows = self._make_dt()._process_cursor([{"_id": oid, "name": "Alice"}])
        assert rows[0]["DT_RowId"] == str(oid)
        assert "_id" not in rows[0]

    def test_row_id_custom_field_sets_dt_row_id(self):
        rows = self._make_dt(row_id="employee_id")._process_cursor(
            [{"_id": ObjectId(), "employee_id": "EMP-42", "name": "Bob"}]
        )
        assert rows[0]["DT_RowId"] == "EMP-42"

    def test_row_id_custom_field_stays_in_row(self):
        rows = self._make_dt(row_id="employee_id")._process_cursor(
            [{"_id": ObjectId(), "employee_id": "EMP-42", "name": "Bob"}]
        )
        assert "employee_id" in rows[0]
        assert rows[0]["employee_id"] == "EMP-42"

    def test_row_id_id_not_popped_when_custom_row_id(self):
        oid = ObjectId()
        rows = self._make_dt(row_id="employee_id")._process_cursor(
            [{"_id": oid, "employee_id": "EMP-42", "name": "Bob"}]
        )
        assert "_id" in rows[0]
        assert rows[0]["_id"] == str(oid)

    def test_row_id_field_not_in_doc_falls_back_to_id(self):
        oid = ObjectId()
        rows = self._make_dt(row_id="missing_field")._process_cursor(
            [{"_id": oid, "name": "Carol"}]
        )
        assert rows[0]["DT_RowId"] == str(oid)
        assert "_id" not in rows[0]

    def test_row_id_included_in_projection(self):
        dt = self._make_dt(row_id="sku")
        assert dt.projection.get("sku") == 1

    def test_row_id_not_in_columns_still_projected(self):
        dt = self._make_dt(row_id="sku")
        column_fields = {c["data"] for c in self.request_args["columns"]}
        assert "sku" not in column_fields
        assert dt.projection.get("sku") == 1

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


class TestRowMetadata(BaseDataTablesTest):

    def _make_dt(self, **kwargs):
        dt = DataTables(self.mongo, "test_collection", self.request_args, **kwargs)
        dt.collection = self.collection
        return dt

    def _run_results(self, dt):
        self.collection.aggregate.return_value = iter(self.sample_docs)
        return dt.results()

    def test_row_class_static(self):
        rows = self._run_results(self._make_dt(row_class="highlight"))
        for row in rows:
            assert row["DT_RowClass"] == "highlight"

    def test_row_class_callable(self):
        fn = lambda r: "active" if r.get("status") == "active" else "inactive"
        rows = self._run_results(self._make_dt(row_class=fn))
        for row in rows:
            assert row["DT_RowClass"] == fn(row)

    def test_row_class_absent_by_default(self):
        rows = self._run_results(self._make_dt())
        for row in rows:
            assert "DT_RowClass" not in row

    def test_row_class_callable_receives_dt_row_id(self):
        seen = []
        def capture(r):
            seen.append(r)
            return "ok"
        rows = self._run_results(self._make_dt(row_class=capture))
        assert len(seen) == len(rows)
        for r in seen:
            assert "DT_RowId" in r

    def test_row_data_static(self):
        rows = self._run_results(self._make_dt(row_data={"source": "mongo"}))
        for row in rows:
            assert row["DT_RowData"] == {"source": "mongo"}

    def test_row_data_callable(self):
        rows = self._run_results(self._make_dt(row_data=lambda r: {"id": r.get("DT_RowId")}))
        for row in rows:
            assert row["DT_RowData"] == {"id": row["DT_RowId"]}

    def test_row_data_absent_by_default(self):
        rows = self._run_results(self._make_dt())
        for row in rows:
            assert "DT_RowData" not in row

    def test_row_attr_static(self):
        rows = self._run_results(self._make_dt(row_attr={"data-type": "record"}))
        for row in rows:
            assert row["DT_RowAttr"] == {"data-type": "record"}

    def test_row_attr_callable(self):
        rows = self._run_results(self._make_dt(row_attr=lambda r: {"title": r.get("name", "")}))
        for row in rows:
            assert row["DT_RowAttr"] == {"title": row.get("name", "")}

    def test_row_attr_absent_by_default(self):
        rows = self._run_results(self._make_dt())
        for row in rows:
            assert "DT_RowAttr" not in row

    def test_all_three_combined(self):
        rows = self._run_results(self._make_dt(
            row_class="row", row_data={"x": 1}, row_attr={"tabindex": "0"}
        ))
        for row in rows:
            assert row["DT_RowClass"] == "row"
            assert row["DT_RowData"] == {"x": 1}
            assert row["DT_RowAttr"] == {"tabindex": "0"}

    def test_only_row_class_set(self):
        rows = self._run_results(self._make_dt(row_class="x"))
        for row in rows:
            assert "DT_RowClass" in row
            assert "DT_RowData" not in row
            assert "DT_RowAttr" not in row

    def test_get_rows_includes_row_class(self):
        dt = self._make_dt(row_class="highlight")
        self.collection.aggregate.return_value = iter(self.sample_docs)
        self.collection.count_documents.return_value = 3
        response = dt.get_rows()
        for row in response["data"]:
            assert row["DT_RowClass"] == "highlight"

    def test_backward_compatible_no_row_kwargs(self):
        dt = DataTables(self.mongo, "test_collection", self.request_args)
        dt.collection = self.collection
        self.collection.aggregate.return_value = iter(self.sample_docs)
        rows = dt.results()
        assert isinstance(rows, list)
        for row in rows:
            assert "DT_RowClass" not in row
            assert "DT_RowData" not in row
            assert "DT_RowAttr" not in row


if __name__ == '__main__':
    unittest.main()
