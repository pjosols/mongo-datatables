"""Tests for allow_disk_use parameter support."""
from unittest.mock import call, patch
from tests.base_test import BaseDataTablesTest
from mongo_datatables import DataTables


class TestAllowDiskUse(BaseDataTablesTest):
    """Tests for allow_disk_use parameter."""

    def test_default_is_false(self):
        dt = DataTables(self.mongo, 'users', self.request_args)
        self.assertFalse(dt.allow_disk_use)

    def test_allow_disk_use_true_stored(self):
        dt = DataTables(self.mongo, 'users', self.request_args, allow_disk_use=True)
        self.assertTrue(dt.allow_disk_use)

    def test_results_passes_allow_disk_use_false(self):
        self.collection.aggregate.return_value = iter([])
        dt = DataTables(self.mongo, 'users', self.request_args)
        dt.results()
        args, kwargs = self.collection.aggregate.call_args
        self.assertEqual(kwargs.get('allowDiskUse'), False)

    def test_results_passes_allow_disk_use_true(self):
        self.collection.aggregate.return_value = iter([])
        dt = DataTables(self.mongo, 'users', self.request_args, allow_disk_use=True)
        dt.results()
        args, kwargs = self.collection.aggregate.call_args
        self.assertEqual(kwargs.get('allowDiskUse'), True)

    def test_count_filtered_passes_allow_disk_use(self):
        self.collection.aggregate.return_value = iter([{'total': 5}])
        dt = DataTables(self.mongo, 'users', self.request_args, allow_disk_use=True, status='active')
        dt.count_filtered()
        args, kwargs = self.collection.aggregate.call_args
        self.assertEqual(kwargs.get('allowDiskUse'), True)

    def test_get_export_data_passes_allow_disk_use(self):
        self.collection.aggregate.return_value = iter([])
        dt = DataTables(self.mongo, 'users', self.request_args, allow_disk_use=True)
        dt.get_export_data()
        args, kwargs = self.collection.aggregate.call_args
        self.assertEqual(kwargs.get('allowDiskUse'), True)

    def test_backward_compatible_no_allow_disk_use_arg(self):
        """Existing code that doesn't pass allow_disk_use still works."""
        self.collection.aggregate.return_value = iter([])
        dt = DataTables(self.mongo, 'users', self.request_args)
        dt.results()
        # Should not raise; allowDiskUse=False is passed (harmless)
        self.collection.aggregate.assert_called_once()
