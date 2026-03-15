"""Tests for DT_RowClass, DT_RowData, DT_RowAttr per-row metadata."""
from tests.base_test import BaseDataTablesTest
from mongo_datatables import DataTables


class TestRowMetadata(BaseDataTablesTest):

    def _make_dt(self, **kwargs):
        dt = DataTables(self.mongo, "test_collection", self.request_args, **kwargs)
        dt.collection = self.collection
        return dt

    def _run_results(self, dt):
        self.collection.aggregate.return_value = iter(self.sample_docs)
        return dt.results()

    # DT_RowClass

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

    # DT_RowData

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

    # DT_RowAttr

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

    # Combined

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

    # get_rows integration

    def test_get_rows_includes_row_class(self):
        dt = self._make_dt(row_class="highlight")
        self.collection.aggregate.return_value = iter(self.sample_docs)
        self.collection.count_documents.return_value = 3
        response = dt.get_rows()
        for row in response["data"]:
            assert row["DT_RowClass"] == "highlight"

    # Backward compatibility

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
