"""Tests for DataTables error handling."""
import copy
import unittest
from unittest.mock import MagicMock, patch
from bson.objectid import ObjectId
from datetime import datetime
from pymongo.errors import PyMongoError
import pymongo

from mongo_datatables import DataTables, DataField
from tests.base_test import BaseDataTablesTest


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

    def test_format_result_values_with_complex_data(self):
        datatables = DataTables(self.mongo, 'test_collection', self.request_args,
                                data_fields=self.data_fields)
        result_dict = {
            '_id': ObjectId('5f50c31e8a91e8c9c8d5c5d5'),
            'title': 'Test Title',
            'published_date': datetime(2020, 1, 1),
            'nested': {'id': ObjectId('5f50c31e8a91e8c9c8d5c5d6'), 'date': datetime(2020, 2, 2)},
            'array_field': [ObjectId('5f50c31e8a91e8c9c8d5c5d7'), datetime(2020, 3, 3)],
        }
        result_copy = copy.deepcopy(result_dict)
        datatables._format_result_values(result_copy)
        self.assertIsInstance(result_copy['_id'], str)
        self.assertIsInstance(result_copy['published_date'], str)
        self.assertIsInstance(result_copy['nested']['id'], str)
        self.assertIsInstance(result_copy['nested']['date'], str)
        self.assertIsInstance(result_copy['array_field'][0], str)
        self.assertIsInstance(result_copy['array_field'][1], str)

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
        self.assertEqual(response["error"], "pipeline failed")
        self.assertEqual(response["data"], [])
        self.assertEqual(response["recordsTotal"], 0)
        self.assertEqual(response["recordsFiltered"], 0)
        self.assertIn("draw", response)

    def test_get_rows_returns_error_field_on_pymongo_error(self):
        dt = DataTables(self.mongo, "test_collection", self.request_args)
        with patch.object(dt, "count_total", side_effect=PyMongoError("connection refused")):
            response = dt.get_rows()
        self.assertIn("error", response)
        self.assertIn("connection refused", response["error"])
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

    def test_exact_phrase_search_without_text_index(self):
        self.request_args['search']['value'] = '"exact phrase"'
        datatables = DataTables(self.mongo, 'test_collection', self.request_args,
                                use_text_index=False)
        condition = datatables.global_search_condition
        self.assertIn('$or', condition)
        for subcondition in condition['$or']:
            field_name = list(subcondition.keys())[0]
            if '$regex' in subcondition[field_name]:
                self.assertIn('\\bexact\\ phrase\\b', subcondition[field_name]['$regex'])
                self.assertEqual(subcondition[field_name]['$options'], 'i')

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
