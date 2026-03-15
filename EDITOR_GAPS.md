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

### 5. `options` in DataTables read response (LOW) ✅ DONE
Editor can populate select/radio/checkbox fields from an `options` dict
returned alongside `data` in any Editor response. Pass `options=` to
`Editor.__init__` as a plain dict or zero-arg callable; `process()` merges
it into every response when set.

### 6. `cancelled` response + pre-event hooks (LOW) ✅ DONE
`hooks=` kwarg added to `Editor.__init__` (dict of `'pre_create'`/`'pre_edit'`/`'pre_remove'`
callables). Each hook is called as `hook(row_id, row_data) -> bool`; a falsy return skips
that row and adds its ID to a `cancelled` list. The `cancelled` key is included in the
response only when non-empty. `_run_pre_hook()` helper centralises the dispatch.
Tests: `tests/test_editor.py::TestEditor::test_run_pre_hook_*`, `test_create_with_hook_*`,
`test_edit_with_hook_*`, `test_remove_with_hook_*`, `test_remove_partial_cancel`

### 7. `DT_RowClass`/`DT_RowData`/`DT_RowAttr` in Editor responses (LOW) ✅ DONE
`row_class`, `row_data`, `row_attr` kwargs added to `Editor.__init__` (mirroring the DataTables class).
Applied in `_format_response_document` after `DT_RowId` is set, using the same callable-or-static
pattern as the DataTables class. Absent by default — keys only appear in responses when configured.
Tests: `tests/test_editor_row_metadata.py` (11 tests)

### 8. `files` in create/edit responses (LOW) ✅ DONE
Depends on upload support (#4). Pass `file_fields=["fieldName", ...]` to `Editor.__init__`
along with a `storage_adapter` that implements `files_for_field(field) -> dict`.
`create()` and `edit()` will include `{"files": {"fieldName": {"id": {...metadata}}}}` in
their responses so the Editor client can resolve file references immediately after write.
Tests: `tests/test_editor_upload.py::TestEditorFilesInResponse`
