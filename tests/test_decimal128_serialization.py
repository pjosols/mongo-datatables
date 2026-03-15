"""Tests for bson.Decimal128 serialization in _format_result_values and SearchPanes."""
import unittest
from decimal import Decimal
from unittest.mock import MagicMock, patch
from bson import Decimal128, ObjectId
from mongo_datatables import DataTables
from tests.base_test import BaseDataTablesTest


class TestDecimal128Serialization(BaseDataTablesTest):
    """Tests that Decimal128 values are serialized to float in query results."""

    def _make_dt(self):
        return DataTables(self.mongo, 'test_collection', self.request_args)

    def test_top_level_decimal128_converted_to_float(self):
        dt = self._make_dt()
        doc = {'price': Decimal128('19.99')}
        dt._format_result_values(doc)
        self.assertIsInstance(doc['price'], float)
        self.assertAlmostEqual(doc['price'], 19.99, places=2)

    def test_nested_decimal128_converted(self):
        dt = self._make_dt()
        doc = {'details': {'amount': Decimal128('1234.56')}}
        dt._format_result_values(doc)
        self.assertIsInstance(doc['details']['amount'], float)
        self.assertAlmostEqual(doc['details']['amount'], 1234.56, places=2)

    def test_decimal128_in_list_converted(self):
        dt = self._make_dt()
        doc = {'prices': [Decimal128('10.00'), Decimal128('20.50')]}
        dt._format_result_values(doc)
        self.assertEqual(doc['prices'], [10.0, 20.5])

    def test_decimal128_zero(self):
        dt = self._make_dt()
        doc = {'balance': Decimal128('0.00')}
        dt._format_result_values(doc)
        self.assertEqual(doc['balance'], 0.0)

    def test_decimal128_negative(self):
        dt = self._make_dt()
        doc = {'delta': Decimal128('-99.99')}
        dt._format_result_values(doc)
        self.assertAlmostEqual(doc['delta'], -99.99, places=2)

    def test_non_decimal128_unaffected(self):
        dt = self._make_dt()
        doc = {'name': 'Alice', 'count': 42, 'active': True}
        dt._format_result_values(doc)
        self.assertEqual(doc, {'name': 'Alice', 'count': 42, 'active': True})

    def test_mixed_list_with_decimal128(self):
        """List containing Decimal128 alongside other types."""
        dt = self._make_dt()
        oid = ObjectId()
        doc = {'items': [Decimal128('5.00'), oid, 'text']}
        dt._format_result_values(doc)
        self.assertAlmostEqual(doc['items'][0], 5.0, places=2)
        self.assertEqual(doc['items'][1], str(oid))
        self.assertEqual(doc['items'][2], 'text')


class TestSearchPanesDecimal128(BaseDataTablesTest):
    """Tests that Decimal128 values are handled in SearchPanes options."""

    def test_searchpanes_decimal128_display_value(self):
        from mongo_datatables.datatables import DataField
        request_args = dict(self.request_args)
        request_args['columns'][0]['searchable'] = 'true'
        dt = DataTables(
            self.mongo, 'test_collection', request_args,
            data_fields=[DataField('name', 'number')]
        )
        # Simulate facet result with Decimal128 _id
        facet_result = [{'_id': Decimal128('9.99'), 'count': 3}]
        total_agg = [{'name': facet_result}]
        count_agg = [{'name': facet_result}]
        self.collection.aggregate.side_effect = [iter(total_agg), iter(count_agg)]
        options = dt.get_searchpanes_options()
        self.assertIn('name', options)
        labels = [o['label'] for o in options['name']]
        self.assertIn('9.99', labels)


if __name__ == '__main__':
    unittest.main()
