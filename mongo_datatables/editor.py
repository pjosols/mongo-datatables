from bson.objectid import ObjectId


class Editor(object):
    def __init__(self, pymongo_object, collection, request_args, doc_id):
        """

        :param pymongo_object:
        :param collection:
        :param request_args:
        :param doc_id:
        """

        self._mongo = pymongo_object
        self._collection = collection
        self._request_args = request_args
        self._doc_id = doc_id

    @property
    def db(self):
        return self._mongo.db

    @property
    def action(self):
        return self._request_args.get("action")

    @property
    def data(self):
        return self._request_args.get("data")

    @property
    def list_of_ids(self):
        return self._doc_id.split(",")

    def remove(self):
        """

        :return: empty {}
        """
        for _id in self.list_of_ids:
            self.db[self._collection].delete_one({"_id": ObjectId(_id)})
        return {}

    def create(self):
        """
        Use PyMongo insert_one to add a document to a collection.  self.data contains the new entry with no _id, like
        {'0': {'val': 'test', 'group': 'test', 'text': 'test'}}

        :return: output like {'data': [{'DT_RowID': 'x', ... }]}
        """

        data_obj = self.data['0']
        for key, val in data_obj.items():

            # Try to save it as an array
            if type(val) == str and ',' in val:
                data_obj[key] = val.split(',')

            else:  # Try to save it as an integer
                try:
                    data_obj[key] = int(val)
                except ValueError:
                    pass
                except SyntaxError:
                    pass
                else:
                    pass

        self.db[self._collection].insert_one(data_obj)

        # After insert, data_obj now includes an _id of type ObjectId, but we need it named DT_RowId and of type str.
        data_obj["DT_RowId"] = str(data_obj.pop("_id", None))
        return {"data": [data_obj]}

    def edit(self):
        """

        :return: output like { 'data': [ {'DT_RowID': 'x', ... }, {'DT_RowID': 'y',... }, ...]}
        """
        data = []

        for _id in self.list_of_ids:
            doc = self.data[_id]

            for key, val in doc.items():

                # Try to save it as an array
                if type(val) == str and ',' in val:
                    doc[key] = val.split(',')

                else:  # Try to save it as an integer
                    try:
                        doc[key] = int(val)
                    except ValueError:
                        pass
                    except SyntaxError:
                        pass
                    else:
                        pass

            self.db[self._collection].update_one({"_id": ObjectId(_id)}, {"$set": doc}, upsert=False)

            # add the _id to the doc object
            doc["DT_RowId"] = _id

            # add each doc object to the data array
            data.append(doc)

        return {"data": data}

    def update_rows(self):
        if self.action == "remove":
            return self.remove()
        elif self.action == "create":
            return self.create()
        elif self.action == "edit":
            return self.edit()
