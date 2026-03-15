import unittest
from unittest.mock import MagicMock, patch
from pymongo.collection import Collection
from pymongo.database import Database

from mongo_datatables import Editor


def _make_editor(request_args):
    mongo = MagicMock()
    mongo.db = MagicMock(spec=Database)
    collection = MagicMock(spec=Collection)
    mongo.db.__getitem__.return_value = collection
    editor = Editor(mongo, "countries", request_args)
    return editor, collection


class TestEditorSearchAction(unittest.TestCase):

    def test_search_by_term_returns_matching_labels(self):
        editor, col = _make_editor({"action": "search", "field": "country", "search": "u"})
        col.find.return_value.limit.return_value = [{"country": "Uganda"}, {"country": "Ukraine"}]
        result = editor.search()
        self.assertEqual(result["data"], [{"label": "Uganda", "value": "Uganda"},
                                          {"label": "Ukraine", "value": "Ukraine"}])

    def test_search_by_values_returns_exact_matches(self):
        editor, col = _make_editor({"action": "search", "field": "country",
                                    "values": ["Uganda", "Ukraine"]})
        col.find.return_value.limit.return_value = [{"country": "Uganda"}, {"country": "Ukraine"}]
        result = editor.search()
        self.assertEqual(len(result["data"]), 2)
        self.assertEqual(result["data"][0]["value"], "Uganda")

    def test_search_unknown_field_returns_empty(self):
        editor, col = _make_editor({"action": "search", "field": "nonexistent", "search": "x"})
        col.find.return_value.limit.return_value = []
        result = editor.search()
        self.assertEqual(result["data"], [])

    def test_search_empty_term_returns_results(self):
        editor, col = _make_editor({"action": "search", "field": "country", "search": ""})
        col.find.return_value.limit.return_value = [{"country": "France"}, {"country": "Germany"}]
        result = editor.search()
        self.assertEqual(len(result["data"]), 2)

    def test_search_deduplicates_values(self):
        editor, col = _make_editor({"action": "search", "field": "country", "search": "u"})
        col.find.return_value.limit.return_value = [{"country": "Uganda"}, {"country": "Uganda"}, {"country": "Ukraine"}]
        result = editor.search()
        values = [d["value"] for d in result["data"]]
        self.assertEqual(values, ["Uganda", "Ukraine"])

    def test_search_no_params_returns_empty(self):
        editor, col = _make_editor({"action": "search", "field": "country"})
        result = editor.search()
        self.assertEqual(result, {"data": []})
        col.find.assert_not_called()

    def test_search_registered_in_process(self):
        editor, col = _make_editor({"action": "search", "field": "country", "search": "u"})
        with patch.object(Editor, "search", return_value={"data": []}) as mock_search:
            result = editor.process()
            mock_search.assert_called_once()
            self.assertEqual(result, {"data": []})


if __name__ == "__main__":
    unittest.main(verbosity=2)
