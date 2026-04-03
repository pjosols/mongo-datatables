"""Pluggable storage backend for Editor file uploads."""


class StorageAdapter:
    """Pluggable storage backend for Editor file uploads.

    Subclass and implement :meth:`store` to persist uploaded files.
    Optionally implement :meth:`retrieve` and ``files_for_field`` to support
    additional features.

    **Optional protocol method** — ``files_for_field(field: str) -> dict``:
    If defined on a subclass, it is called after ``create`` and ``edit``
    operations (when ``file_fields`` is configured on :class:`Editor`) and
    after ``upload`` operations.  The returned dict is included in the Editor
    response as ``files[field]``, letting the client display thumbnails or
    filenames without a separate request.  Expected return shape::

        {"<file_id>": {"filename": "photo.jpg", "web_path": "/uploads/photo.jpg"}}

    If the method is absent, the ``files`` key is omitted from those responses
    (the existing ``hasattr`` check is preserved intentionally).
    """

    def store(self, field: str, filename: str, content_type: str, data: bytes) -> str:
        """Persist an uploaded file and return a unique identifier.

        field: The Editor field name the upload belongs to.
        filename: Original filename as reported by the browser.
        content_type: MIME type of the uploaded file.
        data: Raw file bytes.
        Returns a unique string ID that can later be passed to :meth:`retrieve`.
        Raises NotImplementedError — subclasses must implement this method.
        """
        raise NotImplementedError

    def retrieve(self, file_id: str) -> bytes:
        """Return the raw bytes for a previously stored file.

        file_id: The ID string returned by :meth:`store`.
        Returns raw file bytes.
        Raises NotImplementedError — subclasses must implement this method.
        """
        raise NotImplementedError
