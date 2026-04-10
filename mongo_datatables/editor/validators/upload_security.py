"""Validate file uploads: magic numbers, filename safety, and size limits."""

import re
from typing import Optional, Protocol, runtime_checkable

from mongo_datatables.exceptions import InvalidDataError


@runtime_checkable
class ScannerProtocol(Protocol):
    def scan(self, filename: str, data: bytes) -> bool: ...

# Magic bytes: (offset, bytes) for each allowed MIME type
_MAGIC: dict[str, tuple[int, bytes]] = {
    "image/jpeg": (0, b"\xff\xd8\xff"),
    "image/png": (0, b"\x89PNG\r\n\x1a\n"),
    "image/gif": (0, b"GIF8"),
    "image/webp": (8, b"WEBP"),
    "application/pdf": (0, b"%PDF-"),
    "text/plain": (0, b""),  # no magic — size-only check
    "text/csv": (0, b""),
}

# Per-type size limits in bytes (default 10 MB)
_TYPE_SIZE_LIMITS: dict[str, int] = {
    "image/jpeg": 10 * 1024 * 1024,
    "image/png": 10 * 1024 * 1024,
    "image/gif": 5 * 1024 * 1024,
    "image/webp": 10 * 1024 * 1024,
    "application/pdf": 25 * 1024 * 1024,
    "text/plain": 5 * 1024 * 1024,
    "text/csv": 5 * 1024 * 1024,
}

# Extensions that are always rejected regardless of content-type
_BLOCKED_EXTENSIONS: frozenset[str] = frozenset({
    ".exe", ".bat", ".cmd", ".sh", ".ps1", ".vbs", ".js", ".jar",
    ".php", ".py", ".rb", ".pl", ".cgi", ".asp", ".aspx", ".jsp",
    ".htaccess", ".htpasswd", ".phtml", ".phar",
})

# Filename patterns that indicate malicious intent
_MALICIOUS_FILENAME_RE = re.compile(
    r"(%[0-9a-fA-F]{2})"          # URL-encoded characters
    r"|(\x00)"                     # null bytes
    r"|(\.{2,})"                   # multiple dots (e.g. ../../)
    r"|([<>:\"|?*])"               # Windows-reserved characters
)


def _base_type(content_type: str) -> str:
    """Return the base MIME type, stripping parameters and normalising case.

    content_type: raw Content-Type value (may include parameters).
    Returns the base type string, e.g. 'image/jpeg'.
    """
    return content_type.split(";")[0].strip().lower()


def validate_file_type(filename: str, content_type: str, data: bytes) -> None:
    """Validate file type via whitelist and magic number verification.

    Rejects files whose content-type is not whitelisted, whose magic bytes
    do not match the declared content-type, or whose extension is blocked.

    filename: original filename.
    content_type: MIME type declared by the client.
    data: raw file bytes.
    Raises InvalidDataError if the file type is not allowed or magic bytes mismatch.
    """
    base_type = _base_type(content_type)

    if base_type not in _MAGIC:
        raise InvalidDataError(
            f"File type {base_type!r} is not allowed. "
            f"Allowed types: {', '.join(sorted(_MAGIC))}"
        )

    magic_offset, magic_bytes = _MAGIC[base_type]
    if magic_bytes:
        chunk = data[magic_offset: magic_offset + len(magic_bytes)]
        if chunk != magic_bytes:
            raise InvalidDataError(
                f"File content does not match declared type {base_type!r}"
            )
    elif base_type in ("text/plain", "text/csv"):
        try:
            text = data.decode("utf-8")
        except UnicodeDecodeError as exc:
            raise InvalidDataError(
                f"File content is not valid UTF-8 for declared type {base_type!r}"
            ) from exc
        # Reject null bytes or non-printable control characters outside CR/LF/TAB
        if any(
            ord(ch) < 0x20 and ch not in ("\t", "\n", "\r")
            for ch in text
        ):
            raise InvalidDataError(
                f"File content contains disallowed control characters for type {base_type!r}"
            )


def validate_filename_safety(filename: str) -> None:
    """Detect malicious filename patterns beyond basic path traversal.

    Rejects filenames with URL-encoded characters, null bytes, Windows-reserved
    characters, and blocked executable extensions.

    filename: original filename string.
    Raises InvalidDataError if the filename is unsafe.
    """
    if _MALICIOUS_FILENAME_RE.search(filename):
        raise InvalidDataError(
            f"Filename {filename!r} contains disallowed characters or patterns"
        )

    lower = filename.lower()
    # Check every suffix component (catches "evil.php.jpg" style attacks)
    parts = lower.split(".")
    for part in parts[1:]:  # skip the base name
        ext = f".{part}"
        if ext in _BLOCKED_EXTENSIONS:
            raise InvalidDataError(
                f"Filename {filename!r} contains a blocked extension: {ext!r}"
            )


def validate_file_size_for_type(content_type: str, data: bytes) -> None:
    """Enforce per-file-type size limits.

    content_type: MIME type (may include parameters).
    data: raw file bytes.
    Raises InvalidDataError if the file exceeds the per-type limit.
    """
    base_type = _base_type(content_type)
    limit = _TYPE_SIZE_LIMITS.get(base_type)
    if limit is not None and len(data) > limit:
        raise InvalidDataError(
            f"File of type {base_type!r} exceeds the {limit // (1024 * 1024)} MB size limit"
        )


def run_virus_scan_hook(
    filename: str,
    data: bytes,
    scanner: Optional[ScannerProtocol] = None,
) -> None:
    """Integration point for virus scanning.

    If a scanner object is provided it must implement ``scan(filename, data) -> bool``
    returning True when the file is clean. A return value of False raises
    InvalidDataError. When no scanner is provided the check is skipped.

    filename: original filename (used for logging/reporting only).
    data: raw file bytes.
    scanner: optional scanner implementing ScannerProtocol.
    Raises InvalidDataError if the scanner reports the file as infected.
    """
    if scanner is None:
        return
    if not callable(getattr(scanner, "scan", None)):
        raise InvalidDataError("scanner must implement a callable 'scan' method")
    if not scanner.scan(filename, data):
        raise InvalidDataError(f"File {filename!r} was rejected by the virus scanner")
