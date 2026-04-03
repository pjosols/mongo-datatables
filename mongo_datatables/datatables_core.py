"""Backward-compatible re-export. Use mongo_datatables.datatables instead."""
from mongo_datatables.datatables import DataTables
from mongo_datatables.data_field import DataField

__all__ = ["DataTables", "DataField"]
