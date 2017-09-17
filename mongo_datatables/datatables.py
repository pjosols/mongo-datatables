"""Connect DataTables server side to a MongoDB database.

Supports column sorting and filtering by search terms.  Also supports custom filtering in case you want to manipulate
the search server side, for example send a value like days_til_expiration=60 and convert to Mongo search like
{'ExpiryDate': {'$gt': ts, '$lt': ds}} where ts is today's date like 2017-09-12 and ds is today's date plus 60 days.
"""
import json
from bson.objectid import ObjectId


class DataTables(object):
    def __init__(self, pymongo_object, collection, request_args, **custom_filter):
        """

        :param pymongo_object: The PyMongo object representing the connection to a Mongo instance.
        :param collection: The Mongo collection
        :param request_args: The Flask request args, from request.args.to_dict()
        :param custom_filter: kwargs to be used as a custom Mongo filter, like key=value
        """

        self.mongo = pymongo_object
        self.collection = collection
        self.request_args = request_args
        self.custom_filter = custom_filter

    @property
    def db(self):
        return self.mongo.db

    @property
    def search_terms(self):
        return str(self.request_args.get("search")["value"]).split()

    @property
    def search_terms_without_a_colon(self):
        return [term for term in self.search_terms if ":" not in term]

    @property
    def search_terms_with_a_colon(self):
        return [term for term in self.search_terms if ":" in term]

    @property
    def requested_columns(self):
        return [column["data"] for column in self.request_args.get("columns")]

    @property
    def draw(self):
        return self.request_args.get("draw")

    @property
    def start(self):
        return self.request_args.get("start")

    @property
    def length(self):
        _length = self.request_args.get("length")
        if _length == -1:
            return 0
        return _length

    @property
    def cardinality(self):
        return self.db[self.collection].count()

    @property
    def cardinality_filtered(self):
        return self.db[self.collection].find(self.filter).count()

    @property
    def order_dir(self):
        """
        Return '1' for 'asc' or '-1' for 'desc'
        :return:
        """
        _dir = self.request_args.get("order")[0]["dir"]
        _MONGO_ORDER = {'asc': 1, 'desc': -1}
        return _MONGO_ORDER[_dir]

    @property
    def order_column(self):
        """DataTables provides the index of the order column, but Mongo .sort wants its name.

        :return:
        """
        _order_col = self.request_args.get("order")[0]["column"]
        return self.requested_columns[_order_col]

    @property
    def projection(self):
        p = {}
        for key in self.requested_columns:
            p.update({key: 1})
        return p

    def search_specific_key(self):
        """Search specific keys (columns) like 'key:value'.

        :return:
        """
        _col_specific_search = {}
        for term in self.search_terms_with_a_colon:
            col, term = term.split(':')
            _col_specific_search.update({col: {'$regex': term, '$options': 'i'}})

        return _col_specific_search

    def search_query(self):
        """Build the MongoDB query, searching every column for every term (case insensitive regex).

        :return:
        """
        # D3
        _search_query = {}
        if self.search_terms_without_a_colon:
            # L2
            and_filter_on_all_terms = []
            for term in self.search_terms_without_a_colon:
                # D2
                _or_filter = {}
                # L1
                or_filter_on_all_columns = []
                for column in self.requested_columns:
                    # D1
                    column_filter = {
                        column: {'$regex': term, '$options': 'i'}
                    }
                    or_filter_on_all_columns.append(column_filter)
                _or_filter['$or'] = or_filter_on_all_columns
                and_filter_on_all_terms.append(_or_filter)

            _search_query['$and'] = and_filter_on_all_terms

        return _search_query

    @property
    def filter(self):
        _filter = {}
        _filter.update(self.custom_filter)
        _filter.update(self.search_query())
        _filter.update(self.search_specific_key())
        return _filter

    def results(self):
        _results = list(self.db[self.collection]
                        .find(self.filter,
                              self.projection)
                        .skip(self.start)
                        .limit(self.length)
                        .sort(self.order_column, self.order_dir))

        processed_results = []
        for result in _results:
            result = dict(result)
            result["DT_RowId"] = str(result.pop('_id'))  # rename the _id and convert ObjectId to str

            # go through every val in result and try to json.dumps objects and arrays - skip this if strings are okay
            for key, val in result.items():
                if type(val) not in [str, int, ObjectId]:
                    result[key] = json.dumps(val)

            processed_results.append(result)

        return processed_results

    def get_rows(self):
        return {
            'recordsTotal': str(self.cardinality),
            'recordsFiltered': str(self.cardinality_filtered),
            'draw': int(str(self.draw)),  # cast draw as integer to prevent XSS
            'data': self.results()
        }
