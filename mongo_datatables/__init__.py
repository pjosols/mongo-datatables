"""
MongoDB integration with jQuery DataTables
"""
__version__ = '1.1.2'

from mongo_datatables.datatables import DataTables, DataField
from mongo_datatables.editor import Editor

__all__ = ['DataTables', 'DataField', 'Editor']