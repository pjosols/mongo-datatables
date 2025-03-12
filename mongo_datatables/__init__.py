"""
MongoDB integration with jQuery DataTables
"""
__version__ = '1.0.0'

from mongo_datatables.datatables import DataTables
from mongo_datatables.editor import Editor

__all__ = ['DataTables', 'Editor']