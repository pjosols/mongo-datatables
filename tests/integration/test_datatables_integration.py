import pytest
from mongo_datatables import DataTables, DataField
from tests.integration.conftest import make_request


class TestDataTablesIntegration:

    def test_basic_get_rows_no_search(self, mongo_db, books_col):
        req = make_request(draw=1)
        resp = DataTables(mongo_db, "books", req).get_rows()
        assert resp["draw"] == 1
        assert resp["recordsTotal"] == 10
        assert resp["recordsFiltered"] == 10
        assert len(resp["data"]) == 10
        assert all("DT_RowId" in row for row in resp["data"])

    def test_global_regex_search_returns_matching_rows(self, mongo_db, books_col):
        req = make_request(search_value="Orwell")
        resp = DataTables(mongo_db, "books", req, use_text_index=False).get_rows()
        assert resp["recordsFiltered"] == 2
        assert all(row["Author"] == "George Orwell" for row in resp["data"])

    def test_global_text_index_search(self, mongo_db, books_col):
        req = make_request(search_value="Orwell")
        resp = DataTables(mongo_db, "books", req, use_text_index=True).get_rows()
        assert resp["recordsFiltered"] == 2

    def test_column_search_string(self, mongo_db, books_col):
        cols = [
            {"data": "Title", "searchable": "true", "orderable": "true", "search": {"value": ""}},
            {"data": "Author", "searchable": "true", "orderable": "true", "search": {"value": "Bradbury"}},
            {"data": "Pages", "searchable": "true", "orderable": "true", "search": {"value": ""}},
            {"data": "Genre", "searchable": "true", "orderable": "true", "search": {"value": ""}},
        ]
        req = make_request(columns=cols)
        resp = DataTables(mongo_db, "books", req).get_rows()
        assert resp["recordsFiltered"] == 1
        assert resp["data"][0]["Author"] == "Ray Bradbury"

    def test_column_search_number_range(self, mongo_db, books_col):
        # Pages 100|200 means >=100 AND <=200: Fahrenheit(158), Gatsby(180), OfMice(112), AnimalFarm(112) = 4
        cols = [
            {"data": "Title", "searchable": "true", "orderable": "true", "search": {"value": ""}},
            {"data": "Author", "searchable": "true", "orderable": "true", "search": {"value": ""}},
            {"data": "Pages", "searchable": "true", "orderable": "true", "search": {"value": "100|200"}},
            {"data": "Genre", "searchable": "true", "orderable": "true", "search": {"value": ""}},
        ]
        req = make_request(columns=cols)
        data_fields = [DataField("Pages", "number")]
        resp = DataTables(mongo_db, "books", req, data_fields=data_fields).get_rows()
        assert resp["recordsFiltered"] == 4
        assert all(100 <= row["Pages"] <= 200 for row in resp["data"])

    def test_pagination_start_and_length(self, mongo_db, books_col):
        req = make_request(start=0, length=3)
        resp = DataTables(mongo_db, "books", req).get_rows()
        assert len(resp["data"]) == 3
        assert resp["recordsTotal"] == 10

    def test_length_minus_one_returns_all(self, mongo_db, books_col):
        req = make_request(length=-1)
        resp = DataTables(mongo_db, "books", req).get_rows()
        assert len(resp["data"]) == 10

    def test_multi_column_sort(self, mongo_db, books_col):
        cols = [
            {"data": "Title", "searchable": "true", "orderable": "true", "search": {"value": ""}},
            {"data": "Author", "searchable": "true", "orderable": "true", "search": {"value": ""}},
            {"data": "Pages", "searchable": "true", "orderable": "true", "search": {"value": ""}},
            {"data": "Genre", "searchable": "true", "orderable": "true", "search": {"value": ""}},
        ]
        order = [{"column": 3, "dir": "asc"}, {"column": 0, "dir": "asc"}]
        req = make_request(columns=cols, order=order)
        resp = DataTables(mongo_db, "books", req).get_rows()
        genres = [row["Genre"] for row in resp["data"]]
        assert genres == sorted(genres)

    def test_colon_syntax_search(self, mongo_db, books_col):
        req = make_request(search_value="Author:Orwell")
        resp = DataTables(mongo_db, "books", req, use_text_index=False).get_rows()
        assert resp["recordsFiltered"] == 2

    def test_alias_field_remapping(self, mongo_db, books_col):
        data_fields = [DataField("PublisherInfo.Date", "date", alias="Published")]
        cols = [
            {"data": "Title", "searchable": "true", "orderable": "true", "search": {"value": ""}},
            {"data": "Published", "searchable": "false", "orderable": "false", "search": {"value": ""}},
        ]
        req = make_request(columns=cols)
        resp = DataTables(mongo_db, "books", req, data_fields=data_fields).get_rows()
        assert len(resp["data"]) == 10
        assert all("Published" in row for row in resp["data"])
        assert all("PublisherInfo" not in row for row in resp["data"])

    def test_searchpanes_options(self, mongo_db, books_col):
        cols = [
            {"data": "Title", "searchable": "true", "orderable": "true", "search": {"value": ""}},
            {"data": "Genre", "searchable": "true", "orderable": "true", "search": {"value": ""}},
        ]
        req = make_request(columns=cols, searchPanes={"Genre": []})
        resp = DataTables(mongo_db, "books", req).get_rows()
        assert "searchPanes" in resp
        options = resp["searchPanes"]["options"]
        assert "Genre" in options
        genre_labels = {o["label"] for o in options["Genre"]}
        assert "Fiction" in genre_labels
        assert "Dystopia" in genre_labels
        fiction_opt = next(o for o in options["Genre"] if o["label"] == "Fiction")
        assert fiction_opt["total"] == 5

    def test_custom_filter_passthrough(self, mongo_db, books_col):
        req = make_request()
        resp = DataTables(mongo_db, "books", req, Genre="Dystopia").get_rows()
        assert resp["recordsTotal"] == 3
        assert resp["recordsFiltered"] == 3
        assert all(row["Genre"] == "Dystopia" for row in resp["data"])

    def test_searchbuilder_number_criterion(self, mongo_db, books_col):
        sb = {
            "logic": "AND",
            "criteria": [{"condition": ">", "origData": "Pages", "type": "num", "value": ["300"]}]
        }
        req = make_request(searchBuilder=sb)
        resp = DataTables(mongo_db, "books", req).get_rows()
        assert resp["recordsFiltered"] == 2
        assert all(row["Pages"] > 300 for row in resp["data"])

    def test_draw_counter_echoed(self, mongo_db, books_col):
        req = make_request(draw=42)
        resp = DataTables(mongo_db, "books", req).get_rows()
        assert resp["draw"] == 42

    def test_pymongo_database_object(self, mongo_db, books_col):
        # Pass the Database object directly — tests _get_collection Database branch
        req = make_request()
        resp = DataTables(mongo_db, "books", req).get_rows()
        assert resp["recordsTotal"] == 10
        assert "error" not in resp
