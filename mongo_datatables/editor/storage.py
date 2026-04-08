"""Pluggable storage backend for Editor file uploads."""


class StorageAdapter:
    """Pluggable storage backend for Editor file uploads.

    Subclass and implement :meth:`store` to persist uploaded files.
    Optionally implement :meth:`retrieve`, ``files_for_field``, and
    :meth:`validate_upload` to support additional features.

    **Security requirements for subclasses**:

    - Store files outside the web root or use a content-addressed path so
      stored filenames cannot be guessed or traversed.
    - Never use the client-supplied filename as the storage path; generate a
      UUID or content-hash based name instead.
    - Set restrictive permissions on stored files (e.g. 0o640).
    - Do not serve stored files with executable MIME types.

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

    def validate_upload(
        self,
        field: str,
        filename: str,
        content_type: str,
        data: bytes,
    ) -> None:
        """Optional hook for adapter-level upload validation.

        Called by :func:`handle_upload` before :meth:`store`. Override to add
        adapter-specific checks such as per-user quotas, additional MIME
        restrictions, or integration with an external virus scanner.

        The default implementation is a no-op; subclasses may raise
        :class:`~mongo_datatables.exceptions.InvalidDataError` to reject a file.

        field: Editor field name the upload belongs to.
        filename: Original filename (already sanitised by framework validators).
        content_type: MIME type of the uploaded file.
        data: Raw file bytes.
        """

    def store(self, field: str, filename: str, content_type: str, data: bytes) -> str:
        """Persist an uploaded file and return a unique identifier.

        Implementations must NOT use ``filename`` as the storage path.
        Generate a UUID or content-hash based name to prevent path traversal
        and filename-guessing attacks.

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
