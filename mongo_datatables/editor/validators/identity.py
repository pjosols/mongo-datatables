"""Validation for MongoDB collection names, document IDs, and field names."""

import re

from bson.objectid import ObjectId
from bson.errors import InvalidId as ObjectIdError

from mongo_datatables.exceptions import InvalidDataError

_FIELD_NAME_RE = re.compile(r"^[A-Za-z0-9_\-\.]+$")
_COLLECTION_NAME_RE = re.compile(r"^[^\x00$]{1,120}$")


def validate_collection_name(name: str) -> None:
    """Validate a MongoDB collection name against MongoDB naming rules.

    Rejects empty strings, names containing null bytes or '$', and names
    exceeding 120 bytes — preventing injection via collection name.

    name: collection name string to validate.
    Raises InvalidDataError if the name is invalid.
    """
    if not isinstance(name, str) or not _COLLECTION_NAME_RE.match(name):
        raise InvalidDataError(
            f"Invalid collection name {name!r}. Must be a non-empty string "
            "without null bytes or '$', and at most 120 characters."
        )


def validate_doc_id(doc_id: str) -> None:
    """Validate that every ID in a comma-separated doc_id string is a valid ObjectId.

    doc_id: comma-separated string of MongoDB ObjectId hex strings.
    Raises InvalidDataError if any ID is malformed.
    """
    if not doc_id:
        return
    for raw in doc_id.split(","):
        candidate = raw.strip()
        if not candidate:
            continue
        try:
            ObjectId(candidate)
        except (ObjectIdError, ValueError, TypeError) as exc:
            raise InvalidDataError(
                f"Invalid document ID format: {candidate!r}"
            ) from exc


def validate_field_name(name: str) -> None:
    """Validate a single field name against the allowed character whitelist.

    Rejects names containing special characters that could be used for injection.

    name: field name string to validate.
    Raises InvalidDataError if the name contains disallowed characters.
    """
    if not _FIELD_NAME_RE.match(name):
        raise InvalidDataError(
            f"Field name {name!r} contains invalid characters. "
            "Only alphanumeric characters, underscores, hyphens, and dots are allowed."
        )
