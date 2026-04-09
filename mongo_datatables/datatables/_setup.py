"""Collection resolution and text-index detection helpers for DataTables."""

import logging
from typing import Any

from pymongo.collection import Collection
from pymongo.database import Database
from pymongo.errors import PyMongoError

logger = logging.getLogger(__name__)


def get_collection(pymongo_object: Any, collection_name: str) -> Collection:
    """Resolve a MongoDB collection from a PyMongo client or Flask-PyMongo instance.

    pymongo_object: PyMongo client, Flask-PyMongo instance, or Database.
    collection_name: Name of the target collection.
    Returns a pymongo Collection.
    """
    if isinstance(pymongo_object, Database):
        return pymongo_object[collection_name]
    if hasattr(pymongo_object, "db"):
        return pymongo_object.db[collection_name]
    if hasattr(pymongo_object, "get_database"):
        return pymongo_object.get_database()[collection_name]
    return pymongo_object[collection_name]


def check_text_index(collection: Collection, use_text_index: bool) -> bool:
    """Detect whether the collection has a text index.

    collection: The pymongo Collection to inspect.
    use_text_index: If False, skips the check and returns False immediately.
    Returns True if a text index exists, False otherwise.
    """
    if not use_text_index:
        return False
    try:
        return any(
            "textIndexVersion" in idx for idx in collection.list_indexes()
        )
    except PyMongoError:
        logger.warning(
            "Failed to check text index on collection %s",
            collection.name,
            exc_info=True,
        )
        return False
