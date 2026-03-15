# Editor Protocol Gap Analysis

Reference: https://editor.datatables.net/manual/server

## Current State

`editor.py` handles the core CRUD actions (`create`, `edit`, `remove`) with:
- `data` parameter parsing with row IDs
- `DT_RowId` in responses
- Nested/dot-notation field updates
- Date, number, boolean, array type coercion
- Field name mapping (UI ↔ DB)
- Multi-row edit and delete (via comma-separated IDs)
- Multi-row create (all `data[N]` keys processed in order) ✅
- Editor protocol error/fieldErrors JSON from `process()` ✅
- Optional `validators` dict for field-level validation ✅

## Gaps (Priority Order)

### 1. Error/fieldErrors response format (HIGH) ✅ DONE
`process()` now catches all exceptions and returns `{"error": "..."}`.
Optional `validators` dict accepted in `__init__`; field failures return
`{"fieldErrors": [{"name": field, "status": message}, ...]}`.
Tests: `tests/test_editor_gaps.py::TestEditorErrorResponseFormat`

### 2. Multi-row create (HIGH) ✅ DONE
`create()` now loops over all keys in `self.data` in sorted numeric order,
inserting each row and returning all created documents in `{"data": [...]}`.
Tests: `tests/test_editor_gaps.py::TestEditorMultiRowCreate`

### 3. `action=search` — autocomplete/tags lookup (MEDIUM) ✅ DONE
`search()` method added to `Editor`; registered in `process()` (bypasses validators).
Supports `search=<term>` (case-insensitive regex) and `values[]=<val>` (exact `$in` lookup).
Returns `{"data": [{"label": "...", "value": "..."}]}` with deduplication, limit 100.
Tests: `tests/test_editor_search_action.py`

### 4. `action=upload` — file upload (MEDIUM) ✅ DONE
`StorageAdapter` base class added to `editor.py`; subclass and implement `store(field, filename, content_type, data) -> str`.
Optional `files_for_field(field)` method on the adapter populates the `files` dict in the response.
`Editor.__init__` accepts `storage_adapter=` kwarg; `upload()` method dispatches the action and returns
`{"upload": {"id": "..."}, "files": {...}}`. Without an adapter, returns `{"error": "..."}` gracefully.
Tests: `tests/test_editor_upload.py`

### 5. `options` in DataTables read response (LOW)
Editor can populate select/radio/checkbox fields from an `options` dict
returned alongside `data` in the initial DataTables Ajax response.
Add an optional `options` parameter to `DataTables.get_rows()` or a
helper that merges options into the response.

### 6. `cancelled` response + pre-event hooks (LOW)
Protocol supports `cancelled: [row_ids]` for rows the server chose not
to process. The PHP/Node libraries use `preCreate`/`preEdit`/`preRemove`
events that can cancel individual rows.
Add optional pre-operation callbacks; if a callback returns falsy for a
row, skip it and include its ID in `cancelled`.

### 7. `DT_RowClass`/`DT_RowData`/`DT_RowAttr` in Editor responses (LOW)
`datatables.py` supports these row metadata fields, but
`_format_response_document` in `editor.py` only sets `DT_RowId`.
Accept optional `row_class`, `row_data`, `row_attr` params (mirroring
the DataTables class) and apply them in Editor responses too.

### 8. `files` in DataTables read response (LOW)
Depends on upload support (#4). Once uploads exist, include file
metadata in the initial DataTables read response so Editor can resolve
file references on page load.
