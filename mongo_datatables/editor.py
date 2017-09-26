from bson.objectid import ObjectId
import json


class Editor(object):
    def __init__(self, pymongo_object, collection, request_args, doc_id):
        """

        :param pymongo_object:
        :param collection:
        :param request_args:
        :param doc_id:
        """

        self.mongo = pymongo_object
        self.collection = collection
        self.request_args = request_args
        self.doc_id = doc_id

    @property
    def db(self):
        return self.mongo.db

    @property
    def action(self):
        return self.request_args.get("action")

    @property
    def data(self):
        return self.request_args.get("data")

    @property
    def list_of_ids(self):
        return self.doc_id.split(",")

    def remove(self):
        """

        :return: empty {}
        """
        for _id in self.list_of_ids:
            self.db[self.collection].delete_one({"_id": ObjectId(_id)})
        return {}

    def create(self):
        """
        Use PyMongo insert_one to add a document to a collection.  self.data contains the new entry with no _id, like
        {'0': {'val': 'test', 'group': 'test', 'text': 'test'}}

        :return: output like {'data': [{'DT_RowID': 'x', ... }]}
        """

        data_obj = {k: v for k, v in self.data['0'].items() if v}  # ignore keys that might not exist

        # try to save an object or array
        for key, val in data_obj.items():
            try:
                data_obj[key] = json.loads(val)
            except (json.decoder.JSONDecodeError, TypeError):
                pass

        self.db[self.collection].insert_one(data_obj)

        # After insert, data_obj now includes an _id of type ObjectId, but we need it named DT_RowId and of type str.
        data_obj["DT_RowId"] = str(data_obj.pop("_id", None))
        return {"data": [data_obj]}

    def edit(self):
        """

        :return: output like { 'data': [ {'DT_RowID': 'x', ... }, {'DT_RowID': 'y',... }, ...]}
        """
        data = []

        for _id in self.list_of_ids:
            doc = {k: v for k, v in self.data[_id].items() if v}  # ignore keys that might not exist

            # try to save an object or array
            for key, val in doc.items():
                try:
                    doc[key] = json.loads(val)
                except (json.decoder.JSONDecodeError, TypeError):
                    pass

            self.db[self.collection].update_one({"_id": ObjectId(_id)}, {"$set": doc}, upsert=False)

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
