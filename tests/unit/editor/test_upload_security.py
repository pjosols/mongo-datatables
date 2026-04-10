"""Tests for upload security validators: upload_security and payload modules."""
import pytest
from unittest.mock import MagicMock

from mongo_datatables.exceptions import InvalidDataError
from mongo_datatables.editor.validators.upload_security import (
    validate_file_type,
    validate_filename_safety,
    validate_file_size_for_type,
    run_virus_scan_hook,
)
from mongo_datatables.editor.validators.payload import validate_upload_data

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_JPEG_MAGIC = b"\xff\xd8\xff" + b"\x00" * 100
_PNG_MAGIC = b"\x89PNG\r\n\x1a\n" + b"\x00" * 100
_GIF_MAGIC = b"GIF8" + b"\x00" * 100
_WEBP_MAGIC = b"\x00" * 8 + b"WEBP" + b"\x00" * 100
_PDF_MAGIC = b"%PDF-" + b"\x00" * 100


# ---------------------------------------------------------------------------
# validate_file_type
# ---------------------------------------------------------------------------

class TestValidateFileType:
    def test_jpeg_valid(self):
        validate_file_type("photo.jpg", "image/jpeg", _JPEG_MAGIC)

    def test_png_valid(self):
        validate_file_type("img.png", "image/png", _PNG_MAGIC)

    def test_gif_valid(self):
        validate_file_type("anim.gif", "image/gif", _GIF_MAGIC)

    def test_webp_valid(self):
        validate_file_type("img.webp", "image/webp", _WEBP_MAGIC)

    def test_pdf_valid(self):
        validate_file_type("doc.pdf", "application/pdf", _PDF_MAGIC)

    def test_text_plain_valid(self):
        validate_file_type("notes.txt", "text/plain", b"hello world")

    def test_csv_valid(self):
        validate_file_type("data.csv", "text/csv", b"a,b,c")

    def test_text_plain_binary_content_raises(self):
        # PHP webshell bytes are not valid UTF-8 — must be rejected
        php_webshell = b"<?php system($_GET['cmd']); ?>\xff\xfe"
        with pytest.raises(InvalidDataError, match="not valid UTF-8"):
            validate_file_type("shell.txt", "text/plain", php_webshell)

    def test_text_csv_binary_content_raises(self):
        # Executable bytes declared as CSV must be rejected
        exe_bytes = b"MZ\x90\x00\x03\x00\x00\x00"  # PE header
        with pytest.raises(InvalidDataError, match="not valid UTF-8"):
            validate_file_type("data.csv", "text/csv", exe_bytes)

    def test_text_plain_arbitrary_binary_raises(self):
        # Any non-UTF-8 binary content must not pass as text/plain
        with pytest.raises(InvalidDataError, match="not valid UTF-8"):
            validate_file_type("evil.txt", "text/plain", b"\x80\x81\x82\x83")

    def test_disallowed_content_type_raises(self):
        with pytest.raises(InvalidDataError, match="not allowed"):
            validate_file_type("evil.exe", "application/octet-stream", b"\x00")

    def test_magic_mismatch_raises(self):
        with pytest.raises(InvalidDataError, match="does not match"):
            validate_file_type("fake.jpg", "image/jpeg", b"NOTJPEG" + b"\x00" * 50)

    def test_content_type_with_charset_param_accepted(self):
        validate_file_type("notes.txt", "text/plain; charset=utf-8", b"hello")

    def test_empty_data_magic_mismatch_raises(self):
        with pytest.raises(InvalidDataError, match="does not match"):
            validate_file_type("empty.jpg", "image/jpeg", b"")

    def test_html_content_type_raises(self):
        with pytest.raises(InvalidDataError, match="not allowed"):
            validate_file_type("page.html", "text/html", b"<html>")


# ---------------------------------------------------------------------------
# validate_filename_safety
# ---------------------------------------------------------------------------

class TestValidateFilenameSafety:
    def test_safe_filename_passes(self):
        validate_filename_safety("photo.jpg")

    def test_null_byte_raises(self):
        with pytest.raises(InvalidDataError, match="disallowed"):
            validate_filename_safety("evil\x00.jpg")

    def test_url_encoded_raises(self):
        with pytest.raises(InvalidDataError, match="disallowed"):
            validate_filename_safety("file%20name.jpg")

    def test_windows_reserved_char_raises(self):
        with pytest.raises(InvalidDataError, match="disallowed"):
            validate_filename_safety("file<name>.jpg")

    def test_multiple_dots_raises(self):
        with pytest.raises(InvalidDataError, match="disallowed"):
            validate_filename_safety("file...jpg")

    def test_blocked_extension_exe_raises(self):
        with pytest.raises(InvalidDataError, match="blocked extension"):
            validate_filename_safety("malware.exe")

    def test_blocked_extension_php_raises(self):
        with pytest.raises(InvalidDataError, match="blocked extension"):
            validate_filename_safety("shell.php")

    def test_double_extension_attack_raises(self):
        with pytest.raises(InvalidDataError, match="blocked extension"):
            validate_filename_safety("evil.php.jpg")

    def test_blocked_extension_sh_raises(self):
        with pytest.raises(InvalidDataError, match="blocked extension"):
            validate_filename_safety("script.sh")

    def test_blocked_extension_js_raises(self):
        with pytest.raises(InvalidDataError, match="blocked extension"):
            validate_filename_safety("payload.js")

    def test_safe_pdf_filename_passes(self):
        validate_filename_safety("report.pdf")

    def test_safe_csv_filename_passes(self):
        validate_filename_safety("data.csv")


# ---------------------------------------------------------------------------
# validate_file_size_for_type
# ---------------------------------------------------------------------------

class TestValidateFileSizeForType:
    def test_within_limit_passes(self):
        validate_file_size_for_type("image/jpeg", b"\xff\xd8\xff" + b"\x00" * 100)

    def test_exceeds_jpeg_limit_raises(self):
        big = b"\x00" * (11 * 1024 * 1024)
        with pytest.raises(InvalidDataError, match="exceeds"):
            validate_file_size_for_type("image/jpeg", big)

    def test_exceeds_gif_limit_raises(self):
        big = b"\x00" * (6 * 1024 * 1024)
        with pytest.raises(InvalidDataError, match="exceeds"):
            validate_file_size_for_type("image/gif", big)

    def test_exceeds_pdf_limit_raises(self):
        big = b"\x00" * (26 * 1024 * 1024)
        with pytest.raises(InvalidDataError, match="exceeds"):
            validate_file_size_for_type("application/pdf", big)

    def test_unknown_type_no_limit_passes(self):
        # unknown types have no limit entry — should not raise
        validate_file_size_for_type("application/octet-stream", b"\x00" * (100 * 1024 * 1024))

    def test_content_type_with_params_respected(self):
        big = b"\x00" * (6 * 1024 * 1024)
        with pytest.raises(InvalidDataError, match="exceeds"):
            validate_file_size_for_type("image/gif; charset=utf-8", big)


# ---------------------------------------------------------------------------
# run_virus_scan_hook
# ---------------------------------------------------------------------------

class TestRunVirusScanHook:
    def test_no_scanner_passes(self):
        run_virus_scan_hook("file.jpg", b"data", scanner=None)

    def test_clean_file_passes(self):
        scanner = MagicMock()
        scanner.scan.return_value = True
        run_virus_scan_hook("file.jpg", b"data", scanner=scanner)
        scanner.scan.assert_called_once_with("file.jpg", b"data")

    def test_infected_file_raises(self):
        scanner = MagicMock()
        scanner.scan.return_value = False
        with pytest.raises(InvalidDataError, match="rejected by the virus scanner"):
            run_virus_scan_hook("evil.jpg", b"data", scanner=scanner)

    def test_scanner_without_scan_method_raises(self):
        with pytest.raises(InvalidDataError, match="callable 'scan' method"):
            run_virus_scan_hook("file.jpg", b"data", scanner=object())

    def test_scanner_scan_not_callable_raises(self):
        scanner = MagicMock()
        scanner.scan = "not_callable"
        with pytest.raises(InvalidDataError, match="callable 'scan' method"):
            run_virus_scan_hook("file.jpg", b"data", scanner=scanner)


# ---------------------------------------------------------------------------
# validate_upload_data (payload.py integration)
# ---------------------------------------------------------------------------

class TestValidateUploadData:
    def _valid_upload(self) -> dict:
        return {
            "filename": "photo.jpg",
            "content_type": "image/jpeg",
            "data": _JPEG_MAGIC,
        }

    def test_valid_upload_passes(self):
        validate_upload_data(self._valid_upload())

    def test_not_dict_raises(self):
        with pytest.raises(InvalidDataError, match="must be a dict"):
            validate_upload_data("not a dict")

    def test_missing_filename_raises(self):
        upload = self._valid_upload()
        del upload["filename"]
        with pytest.raises(InvalidDataError, match="filename"):
            validate_upload_data(upload)

    def test_empty_filename_raises(self):
        upload = self._valid_upload()
        upload["filename"] = "   "
        with pytest.raises(InvalidDataError, match="filename"):
            validate_upload_data(upload)

    def test_filename_too_long_raises(self):
        upload = self._valid_upload()
        upload["filename"] = "a" * 256 + ".jpg"
        with pytest.raises(InvalidDataError, match="filename"):
            validate_upload_data(upload)

    def test_path_traversal_filename_raises(self):
        upload = self._valid_upload()
        upload["filename"] = "../etc/passwd"
        with pytest.raises(InvalidDataError, match="path characters"):
            validate_upload_data(upload)

    def test_missing_content_type_raises(self):
        upload = self._valid_upload()
        del upload["content_type"]
        with pytest.raises(InvalidDataError, match="content_type"):
            validate_upload_data(upload)

    def test_empty_content_type_raises(self):
        upload = self._valid_upload()
        upload["content_type"] = ""
        with pytest.raises(InvalidDataError, match="content_type"):
            validate_upload_data(upload)

    def test_data_not_bytes_raises(self):
        upload = self._valid_upload()
        upload["data"] = "string data"
        with pytest.raises(InvalidDataError, match="must be bytes"):
            validate_upload_data(upload)

    def test_empty_data_raises(self):
        upload = self._valid_upload()
        upload["data"] = b""
        with pytest.raises(InvalidDataError, match="must not be empty"):
            validate_upload_data(upload)

    def test_data_exceeds_global_limit_raises(self):
        upload = self._valid_upload()
        upload["data"] = b"\x00" * (51 * 1024 * 1024)
        with pytest.raises(InvalidDataError, match="exceeds maximum size"):
            validate_upload_data(upload)

    def test_disallowed_content_type_raises(self):
        upload = self._valid_upload()
        upload["content_type"] = "application/x-executable"
        with pytest.raises(InvalidDataError, match="not allowed"):
            validate_upload_data(upload)

    def test_magic_mismatch_raises(self):
        upload = self._valid_upload()
        upload["data"] = b"NOTJPEG" + b"\x00" * 50
        with pytest.raises(InvalidDataError, match="does not match"):
            validate_upload_data(upload)

    def test_blocked_extension_raises(self):
        upload = self._valid_upload()
        upload["filename"] = "shell.php"
        with pytest.raises(InvalidDataError, match="blocked extension"):
            validate_upload_data(upload)

    def test_virus_scanner_called_and_clean_passes(self):
        scanner = MagicMock()
        scanner.scan.return_value = True
        validate_upload_data(self._valid_upload(), scanner=scanner)
        scanner.scan.assert_called_once()

    def test_virus_scanner_rejects_raises(self):
        scanner = MagicMock()
        scanner.scan.return_value = False
        with pytest.raises(InvalidDataError, match="virus scanner"):
            validate_upload_data(self._valid_upload(), scanner=scanner)

    def test_bytearray_data_accepted(self):
        upload = self._valid_upload()
        upload["data"] = bytearray(_JPEG_MAGIC)
        validate_upload_data(upload)
