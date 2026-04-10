# Changelog

All notable changes to mongo-datatables are documented here.

## [Unreleased]

### Changed

- `tests/unit/datatables/test_datatables_results.py`: refined class docstrings to Wholeshoot convention; updated helper method docstrings for precision
- `tests/unit/datatables/test_datatables_error_handling.py`: module docstring documents error handling and edge case coverage
- `datatables/query/regex_utils.py`: refined module docstring for conciseness; all function docstrings follow Wholeshoot convention
- Updated module docstrings to be more precise and imperative
- **README**: Regex mode section now documents ReDoS protection and pattern validation limits
- `editor/core.py`: refined `Editor.__init__` docstring; added Wholeshoot docstrings to all methods
- `editor/crud.py`: refined module docstring; added Wholeshoot docstrings to `_fmt()`, `run_create()`, `run_edit()`, `run_remove()`, `resolve_collection()`, `resolve_db()`
- `editor/document.py`: refined module docstring to emphasize CWE-20 and CWE-915 security fixes; added Wholeshoot docstrings to `format_response_document()`, `preprocess_document()`, `build_updates()`
- `editor/validators/upload_security.py`: refined module docstring for conciseness
- `datatables/_limits.py`: refined module docstring to include pagination
- `datatables/compat.py`: added Wholeshoot docstrings to all methods and properties
- `tests/unit/datatables/test_compat.py`: module docstring documents backward-compatible shim verification; added Wholeshoot docstrings to all test classes and methods
- `datatables/core.py`: added Wholeshoot docstrings to all properties and methods
- `datatables/request_validator.py`: refined module docstring for precision; refined function docstrings to specify exception conditions inline
- Test class docstrings updated to Wholeshoot convention: `tests/unit/base_test.py`, `tests/unit/datatables/test_serialization_unit.py`, `tests/unit/datatables/test_sort_unit.py`, `tests/unit/editor/test_editor_crud.py`, `tests/unit/editor/test_editor_upload.py`, `tests/unit/datatables/test_request_validator.py`
- Test helper methods documented: `_make_formatted_doc()`, `_setup_edit()`, `_partial_cancel_result()`
- `tests/unit/datatables/test_datatables_pagination.py`: added class and method docstrings following Wholeshoot convention
- `tests/unit/editor/test_cwe20_date_heuristic.py`: added module docstring; added Wholeshoot docstrings to all test functions
- `tests/unit/editor/test_cwe915_mass_assignment.py`: refined module docstring to Wholeshoot convention (imperative, no filler)
- `tests/unit/editor/test_editor_document_processing.py`: added module docstring; added Wholeshoot docstrings to test classes and methods
- `tests/unit/datatables/test_sort_unit.py`: added Wholeshoot docstrings to all test classes and methods; documented helper methods `_make_dt()`, `_make_columns()`, `_col()`
- `tests/unit/datatables/test_request_validator.py`: refined all test class docstrings to Wholeshoot convention (imperative, no filler)
- `tests/unit/datatables/test_specific_exception_handling.py`: refined module docstring to Wholeshoot convention (imperative, no filler)

### Added

- **README**: File Uploads section documenting security validation (magic bytes, filename safety, size limits, virus scanning)

## [2.1.1] - 2026-04-10

### Changed

- Moved search modules into `datatables/search/` subpackage — internal restructuring, no public API impact
- Tightened input validation in Editor search and upload handlers
- Updated all docstrings to Wholeshoot convention for consistency
- `editor/search.py`: refined module and function docstrings for clarity and precision

### Fixed

- `editor/document.py`: declared date fields were not being parsed due to wrong attribute name (`field_type` vs `data_type`)
- `datatables/formatting.py`: `format_result_values()` docstring now follows Wholeshoot convention
- `exceptions.py`: exception class docstrings now follow Wholeshoot convention (one-sentence, no examples)
- Various type annotation corrections and import cleanup

## [2.1.0] - 2026-04-07

### Added

- **Input validation**: `validate_request_args()` validates all DataTables request parameters at construction — rejects missing required keys, wrong types, and out-of-range values
- **Input validation**: `validate_editor_request_args()` validates Editor request structure and action values at construction
- **Upload security**: `validate_upload_data()` enforces filename safety, content-type format, size limits (50 MB), and magic-byte verification for common file types (JPEG, PNG, GIF, WebP, PDF, plain text, CSV)
- **Regex safety**: `validate_regex()` in `datatables/query/regex_utils.py` rejects patterns exceeding 200 characters or containing known ReDoS constructs before passing to MongoDB `$regex`
- **SearchBuilder depth limit**: `_MAX_SB_DEPTH = 10` prevents unbounded recursion on deeply nested criteria payloads
- **Document payload limits**: `validate_document_payload()` enforces max nesting depth (10), max keys (200), and max string value length (1 MB) to guard against resource exhaustion

### Changed

- Refactored `datatables` module into a subpackage: `core.py`, `filter.py`, `results.py`, `formatting.py`, `request_validator.py`, `response.py`, `compat.py`, and `query/` sub-package — no behaviour changes
- Refactored `editor` module into a subpackage: `core.py`, `crud.py`, `document.py`, `search.py`, `storage.py`, `dispatch.py`, and `validators/` sub-package — no behaviour changes
- Removed internal re-export shim modules (`query_builder.py`, `query_conditions.py`, `query_global_search.py`, `column_control.py`, `datatables_core.py`, `formatting.py`, `regex_utils.py`, `request_validator.py`) — public API unchanged
- Restructured test suite to mirror source subpackage layout (`tests/unit/datatables/`, `tests/unit/editor/`)
- Updated all docstrings to Wholeshoot convention

### Fixed

- All public methods now catch specific exception types rather than bare `Exception` — `get_rows()`, `get_export_data()`, `fetch_results()`, `count_total()`, `count_filtered()`, `get_rowgroup_data()`, `run_create()`, `run_edit()`, `run_remove()`, `Editor.process()`
- `_parse_extension_config()` now handles all extension types generically, not just `dataSrc` — fixes RowGroup and other extensions
- `Editor.__init__` now validates `collection_name` and `doc_id` format before use
- `uploadField` added to allowed Editor request keys — fixes upload action rejection
- Improved exception handling in the upload flow
- `datatables/compat.py` imports `get_searchpanes_options` directly from `search_panes` after dead wrapper removal

## [2.0.0] - 2026-03-21

Major release. Significant new functionality, correctness fixes, and a full
documentation and packaging overhaul since the last published version (1.1.1).

### Added

**Search**
- Smart / AND semantics: `search[smart]=true` (DataTables default) requires each word to independently match at least one searchable column
- Per-column smart AND: `columns[i][search][smart]` mirrors global smart behaviour
- Case sensitivity control: `search[caseInsensitive]` (global) and `columns[i][search][caseInsensitive]` (per-column)
- Regex mode: `search[regex]` and `columns[i][search][regex]` pass raw MongoDB regex patterns
- Colon syntax comparison operators: `field:>N`, `field:>=N`, `field:<N`, `field:<=N`, `field:=N` for `number` and `date` fields
- `keyword` DataField type: exact equality match (`{field: value}`) instead of regex — uses a regular MongoDB index, ideal for categorical/code fields
- Column search inputs now support comparison operators (`>`, `>=`, `<`, `<=`, `=`) for `number` and `date` fields via prefix syntax (e.g. `>=2024-01-01`), consistent with colon syntax; `keyword` column search uses exact equality
- `stemming` parameter on `DataTables` (default `False`): set `True` when using a text index to match morphological variants — "City" also matches "Cities", "run" matches "running"
- Pipe-delimited range syntax for column search: `min|max` on `number` and `date` fields
- Quoted phrase search via word-boundary regex (and `$text` phrase when using text index)
- `columns[i][name]` lookup in column search alongside `columns[i][data]`

**SearchPanes**
- Full server-side SearchPanes support with dual `total` / `count` per value for protocol compliance

**SearchBuilder**
- Full server-side SearchBuilder support: nested AND/OR criteria trees for `string`, `number`, `date`, `html-num`, and `html-num-fmt` column types
- `<=` and `>=` operator support for date criteria

**Named Fixed Searches**
- DataTables 2.x `search.fixed` wire format (array of `{name, term}` objects) with fallback to legacy `searchFixed` top-level dict
- Per-column `columns[i].search.fixed` support

**Sorting**
- Multi-column sort respecting the full `order` array and `orderable` flag
- ColReorder support: `order[i][name]` name-based column ordering
- `columns[i][orderData]` redirect: sorting one column can redirect to another column's index

**DataTables constructor**
- `pipeline_stages`: inject `$lookup`, `$addFields`, `$unwind`, etc. before `$match` in every pipeline
- `allow_disk_use`: pass `allowDiskUse=True` to all aggregation pipelines
- `row_id`: specify a custom field as `DT_RowId` instead of MongoDB `_id`
- `row_class`, `row_data`, `row_attr`: per-row `DT_Row*` metadata (static value or callable)
- `get_export_data()`: return all matching rows without pagination for export endpoints

**Editor**
- `options` parameter: plain dict or zero-arg callable for server-driven select/radio/checkbox options
- `Editor.search()`: `action=search` handler for `autocomplete` and `tags` field types
- `dependent_handlers`: dict of callables for dependent field Ajax requests
- `hooks`: `pre_create`, `pre_edit`, `pre_remove` callables; return falsy to cancel a row
- `validators`: dict mapping field names to callables for field-level validation
- `file_fields` + `StorageAdapter`: pluggable file upload support
- `row_class`, `row_data`, `row_attr` on Editor responses
- Support for all pymongo object types (MongoClient, Database, Flask-PyMongo)

**Serialization**
- `bson.Binary` fields serialized to UUID string (subtypes 3/4) or hex
- NaN/Inf float values serialized as `None` to prevent JSON errors
- `objectid` fields serialized as strings

### Fixed

- **Global text search**: multi-term `$text` search used OR semantics — adding more search terms broadened results instead of narrowing them; now uses AND semantics (quoted terms by default, `+term` prefix when `stemming=True`)
- **Global search**: `keyword` fields were incorrectly included in global search as regex; now excluded — exact-match fields are not appropriate for free-text search
- **SearchBuilder**: `searchBuilder` payload delivered as a JSON string (rather than a decoded object) is now parsed before processing. Occurs when DataTables `submitAs:'json'` interacts with certain extension `preXhr` handler orderings.
- **SearchPanes**: filter selections sent as `{"0": "val"}` (numeric-keyed object) rather than `["val"]` (array) are now correctly normalised before building the `$in` query. Fixes zero results when clicking a pane value.
- **SearchPanes**: array-type fields (e.g. `genre`) now appear correctly in pane options. A `$unwind` stage is prepended to the `$facet` branch for array fields so individual elements are counted as distinct options rather than the whole array being excluded.
- `DataField` aliases now correctly resolved to MongoDB field names in projections, global search, and column search — previously aliased fields silently returned no results or omitted fields from responses
- `search[regex]` string coercion: `bool("false") == True` bug; now uses membership test
- `searchable` and `orderable` string/bool coercion for HTTP form data (`"true"`/`"false"` as strings)
- `$limit: -1` error when DataTables sends `length: -1` ("Show All"); `$limit` stage now omitted
- Date range column search upper bound uses `$lt` (exclusive) to prevent off-by-one inclusion
- SearchBuilder ISO datetime strings (e.g. `2024-01-15T00:00:00.000Z`) now parsed correctly
- `html-num` and `html-num-fmt` SearchBuilder types now route to numeric comparison, not regex
- `re.escape()` applied consistently in all colon-syntax `$regex` paths
- `get_rows()` returns a DataTables-compatible error response on unhandled exceptions instead of propagating
- `$text` `$match` stage placed before `pipeline_stages` (MongoDB requirement)
- Text index bypassed when `regex=true` or `caseInsensitive=false` (MongoDB `$text` does not support these)
- Search terms containing multiple colons (e.g. `url:https://example.com`) no longer silently dropped

### Changed

- `pyproject.toml` replaces `setup.py`; `Documentation` URL updated to readthedocs
- `datatables.py` refactored into focused modules: `search_builder.py`, `search_fixed.py`, `formatting.py`, `search_panes.py` — no behaviour changes
- `count_total()` uses `estimated_document_count()` for collections > 100k documents for performance
- Field mapper lookups in `build_global_search` pre-computed outside the term loop (O(N×M) → O(M))
- Full documentation rewrite: README, readthedocs API reference, and narrative guides

## [1.1.1] - 2025-03-16

- Added new `DataField` class for improved field type management, nested field support, and UI-to-database field mapping
- Implemented index-optimized fast text searches that automatically utilize MongoDB text indexes when available
- Added advanced date filtering with comparison operators (`>`, `<`, `>=`, `<=`, `=`) for date fields
- Added support for quoted string searches (preserves phrases as single search terms)
- Enhanced search implementation with exact phrase matching for quoted terms
- Improved handling of complex search queries with multiple terms
- Added comparison operators (`>`, `<`, `>=`, `<=`, `=`) for numeric field-specific searches

## [1.0.1] - 2025-03-13

- Fixed path collision error when using dotted notation for nested fields in projection

## [1.0.0] - 2025-03-11 [YANKED]

> Yanked from PyPI due to a critical bug with nested document field handling that could cause path collision errors. Use 1.0.1 or later.

- First stable release of the rewritten package
- Basic support for regex-based searches
- Added support for nested document field handling
- Improved error handling and validation

## [0.3.0] - 2019-07-17

- Added native DataTables column search functionality
- Implemented support for column-specific regex or exact matching

## [0.2.6] - 2018-08-23

- Improved type handling with explicit checks for lists, dictionaries, and floats
- Enhanced JSON serialization for specific data types

## [0.2.5] - 2017-09-26

- Improved error handling in Editor by catching TypeError exceptions
- Enhanced JSON parsing resilience for complex data types

## [0.2.4] - 2017-09-23

- Documentation updates and minor code refinements

## [0.2.3] - 2017-09-23

- Major performance improvement: switched to MongoDB aggregation pipeline for results
- Enhanced projection to use `$ifNull` to handle missing fields gracefully
- Editor now filters out keys with empty values before insert/update

## [0.2.2] - 2017-09-18

- Documentation improvement for `request_args` parameter

## [0.2.1] - 2017-09-17

- Improved validation of search terms with colon syntax

## [0.2.0] - 2017-09-17

- Added JSON handling for complex data types and MongoDB ObjectId
- Added support for Flask integration

## [0.1.4] - 2017-09-14

- Initial implementation with basic DataTables server-side processing
- Basic Editor implementation for CRUD operations

Last published version on PyPI.
