from unittest.mock import MagicMock, patch
from tests.base_test import BaseDataTablesTest
import json
from datetime import datetime
from bson.objectid import ObjectId
from pymongo.collection import Collection
from pymongo.database import Database
from pymongo.cursor import Cursor

from mongo_datatables import DataTables


class TestDataTablesQueryBuilding(BaseDataTablesTest):
    """Test cases for query building and result formatting in the DataTables class"""

    pass
    
    

if __name__ == '__main__':
    unittest.main()
