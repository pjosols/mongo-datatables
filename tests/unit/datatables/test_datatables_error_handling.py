"""Test DataTables error handling: PyMongo errors, edge cases, safe responses."""
import copy
import unittest
from unittest.mock import MagicMock, patch
from bson.objectid import ObjectId
from datetime import datetime
from pymongo.errors import PyMongoError
import pymongo

from mongo_datatables import DataTables, DataField
from tests.unit.base_test import BaseDataTablesTest


class TestDataTablesErrorHandling(BaseDataTablesTest):
    """Test cases for error handling in DataTables."""

    def setUp(self):
        super().setUp()
        self.data_fields = [
            DataField('title', 'string'),
            DataField('author', 'string'),
            DataField('year', 'number'),
            DataField('rating', 'number'),
        ]

    def test_error_in_results_method(self):
        self.request_args['columns'] = [
            {'data': 'title', 'searchable': True, 'orderable': True, 'search': {'value': '', 'regex': False}},
            {'data': 'author', 'searchable': True, 'orderable': True, 'search': {'value': '', 'regex': False}},
        ]
        datatables = DataTables(self.mongo, 'test_collection', self.request_args,
                                data_fields=self.data_fields)
        with patch.object(datatables.collection, 'aggregate',
                          side_effect=pymongo.errors.OperationFailure('Test error')):
            self.assertEqual(datatables.results(), [])

    def test_error_in_count_total(self):
        datatables = DataTables(self.mongo, 'test_collection', self.request_args,
                                data_fields=self.data_fields)
        with patch.object(datatables.collection, 'count_documents',
                          side_effect=PyMongoError('Test error')):
            datatables._recordsTotal = None
            self.assertEqual(datatables.count_total(), 0)

    def test_error_in_count_filtered(self):
        datatables = DataTables(self.mongo, 'test_collection', self.request_args,
                                data_fields=self.data_fields)
        with patch.object(datatables.collection, 'count_documents',
                          side_effect=PyMongoError('Test error')):
            datatables._recordsFiltered = None
            self.assertEqual(datatables.count_filtered(), 0)

    def test_invalid_sort_specification(self):
        self.request_args['order'] = [{'column': 0, 'dir': 'asc'}]
        self.request_args['columns'] = [
            {'data': '', 'orderable': True, 'searchable': False, 'search': {'value': '', 'regex': False}},
            {'data': 'author', 'orderable': True, 'searchable': True, 'search': {'value': '', 'regex': False}},
        ]
        datatables = DataTables(self.mongo, 'test_collection', self.request_args,
                                data_fields=self.data_fields)
        sort_spec = datatables.sort_specification
        self.assertTrue(isinstance(sort_spec, (dict, list)))

    def _make_complex_result(self) -> dict:
        return {
            '_id': ObjectId('5f50c31e8a91e8c9c8d5c5d5'),
            'title': 'Test Title',
            'published_date': datetime(2020, 1, 1),
            'nested': {'id': ObjectId('5f50c31e8a91e8c9c8d5c5d6'), 'date': datetime(2020, 2, 2)},
            'array_field': [ObjectId('5f50c31e8a91e8c9c8d5c5d7'), datetime(2020, 3, 3)],
        }

    def _formatted_complex_result(self) -> dict:
        dt = DataTables(self.mongo, 'test_collection', self.request_args, data_fields=self.data_fields)
        result = copy.deepcopy(self._make_complex_result())
        dt._format_result_values(result)
        return result

    def test_format_result_values_converts_objectid_to_str(self):
        result = self._formatted_complex_result()
        self.assertIsInstance(result['_id'], str)

    def test_format_result_values_converts_datetime_to_str(self):
        result = self._formatted_complex_result()
        self.assertIsInstance(result['published_date'], str)

    def test_format_result_values_converts_nested_objectid_to_str(self):
        result = self._formatted_complex_result()
        self.assertIsInstance(result['nested']['id'], str)

    def test_format_result_values_converts_nested_datetime_to_str(self):
        result = self._formatted_complex_result()
        self.assertIsInstance(result['nested']['date'], str)

    def test_format_result_values_converts_array_objectid_to_str(self):
        result = self._formatted_complex_result()
        self.assertIsInstance(result['array_field'][0], str)

    def test_format_result_values_converts_array_datetime_to_str(self):
        result = self._formatted_complex_result()
        self.assertIsInstance(result['array_field'][1], str)

    def test_count_filtered_both_aggregate_and_count_documents_fail(self):
        dt = DataTables(self.mongo, 'test_collection', self.request_args, ["name"])
        dt._filter_cache = {"name": "test"}
        self.collection.aggregate.side_effect = PyMongoError("aggregate failed")
        self.collection.count_documents.side_effect = PyMongoError("count_documents failed")
        self.assertEqual(dt.count_filtered(), 0)

    def test_get_rows_returns_error_field_on_exception(self):
        dt = DataTables(self.mongo, "test_collection", self.request_args)
        with patch.object(dt, "results", side_effect=RuntimeError("pipeline failed")):
            response = dt.get_rows()
        self.assertIn("error", response)
        # Generic message — must not echo raw exception text back to client
        self.assertIsInstance(response["error"], str)
        self.assertNotIn("pipeline failed", response["error"])
        self.assertEqual(response["data"], [])
        self.assertEqual(response["recordsTotal"], 0)
        self.assertEqual(response["recordsFiltered"], 0)
        self.assertIn("draw", response)

    def test_get_rows_returns_error_field_on_pymongo_error(self):
        dt = DataTables(self.mongo, "test_collection", self.request_args)
        with patch.object(dt, "count_total", side_effect=PyMongoError("connection refused")):
            response = dt.get_rows()
        self.assertIn("error", response)
        # Generic message — must not leak raw DB error to client
        self.assertNotIn("connection refused", response["error"])
        self.assertEqual(response["data"], [])

    def test_check_text_index_handles_pymongo_error(self):
        self.collection.list_indexes.side_effect = PyMongoError("not connected")
        dt = DataTables(self.mongo, "test_collection", self.request_args)
        self.assertFalse(dt.has_text_index)

    def test_get_rows_success_has_no_error_field(self):
        dt = DataTables(self.mongo, "test_collection", self.request_args)
        response = dt.get_rows()
        self.assertNotIn("error", response)
        self.assertIn("data", response)
        self.assertIn("draw", response)


class TestDataTablesEdgeCases(BaseDataTablesTest):
    """Test cases for edge cases in the DataTables class."""

    def _exact_phrase_condition(self):
        self.request_args['search']['value'] = '"exact phrase"'
        dt = DataTables(self.mongo, 'test_collection', self.request_args, use_text_index=False)
        return dt.global_search_condition

    def test_exact_phrase_search_without_text_index_or_structure(self):
        self.assertIn('$or', self._exact_phrase_condition())

    def _exact_phrase_regex_sub(self):
        condition = self._exact_phrase_condition()
        return next(
            (s for s in condition['$or'] if '$regex' in s.get(list(s.keys())[0], {})),
            None,
        )

    def test_exact_phrase_search_without_text_index_regex_pattern(self):
        regex_sub = self._exact_phrase_regex_sub()
        self.assertIsNotNone(regex_sub)
        field = list(regex_sub.keys())[0]
        self.assertIn('\\bexact\\ phrase\\b', regex_sub[field]['$regex'])

    def test_exact_phrase_search_without_text_index_regex_options(self):
        regex_sub = self._exact_phrase_regex_sub()
        self.assertIsNotNone(regex_sub)
        field = list(regex_sub.keys())[0]
        self.assertEqual(regex_sub[field]['$options'], 'i')

    def test_numeric_field_search(self):
        self.request_args['search']['value'] = '42'
        data_fields = [DataField('age', 'number'), DataField('name', 'string')]
        self.request_args['columns'] = [
            {'data': 'name', 'searchable': True, 'orderable': True, 'search': {'value': '', 'regex': False}},
            {'data': 'age', 'searchable': True, 'orderable': True, 'search': {'value': '', 'regex': False}},
        ]
        datatables = DataTables(self.mongo, 'test_collection', self.request_args,
                                data_fields=data_fields)
        condition = datatables.global_search_condition
        self.assertIn('$or', condition)
        numeric_condition = next((c for c in condition['$or'] if 'age' in c), None)
        self.assertIsNotNone(numeric_condition)
        self.assertEqual(numeric_condition['age'], 42.0)

    def test_numeric_field_search_with_invalid_number(self):
        self.request_args['search']['value'] = 'not-a-number'
        data_fields = [DataField('age', 'number'), DataField('name', 'string')]
        self.request_args['columns'] = [
            {'data': 'name', 'searchable': True, 'orderable': True, 'search': {'value': '', 'regex': False}},
            {'data': 'age', 'searchable': True, 'orderable': True, 'search': {'value': '', 'regex': False}},
        ]
        datatables = DataTables(self.mongo, 'test_collection', self.request_args,
                                data_fields=data_fields)
        condition = datatables.global_search_condition
        self.assertIn('$or', condition)
        age_condition = next((c for c in condition['$or'] if 'age' in c), None)
        self.assertIsNone(age_condition)


if __name__ == '__main__':
    unittest.main()
