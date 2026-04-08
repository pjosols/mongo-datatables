# Changelog

All notable changes to mongo-datatables are documented here.

## [Unreleased]

### Changed

- Restructured test suite to mirror source subpackage layout: `tests/unit/datatables/` for DataTables tests, `tests/unit/datatables/query/` for query builder tests, `tests/unit/editor/` for Editor tests — improves maintainability and discoverability
- Added module docstrings to test package `__init__.py` files following Wholeshoot convention
- Updated module and function docstrings to follow Wholeshoot convention (one sentence, imperative, no filler)
- Enhanced docstrings in `datatables/response.py` to clarify return value structure and purpose
- Enhanced docstrings in `editor/core.py` to clarify protocol-compliant error handling
- Enhanced docstrings in `editor/document.py` to specify type conversions and metadata handling
- Refined `editor/validator.py` module docstring to Wholeshoot convention (one sentence, imperative)
- Updated test module docstrings to follow Wholeshoot convention
- Simplified `DataTables` class docstring to one sentence
- Simplified `get_searchpanes_options()` docstring to Wholeshoot convention with flat parameter list
- Simplified `parse_searchpanes_filters()` docstring to Wholeshoot convention
- Updated `datatables/compat.py` module docstring to clarify backward-compatible re-exports and shim mixin purpose
- Updated `test_compat.py` module docstring to follow Wholeshoot convention
- Updated `datatables/compat.py` module docstring to imperative form
- Updated test module docstrings in `test_datatables_filtering.py`, `test_datatables_misc.py`, `test_datatables_pipeline.py`, `test_extensions_unit.py`, and `test_sort_unit.py` to imperative form
- Updated `DataTables._parse_extension_config()` docstring to imperative form
- Updated `parse_extension_config()` docstring to imperative form
- Added docstring to `Editor._resolve_options()` following Wholeshoot convention
- Updated module docstrings in `datatables/request_validator.py`, `editor/core.py`, `editor/search.py`, `editor/validators/__init__.py`, `editor/validators/payload.py`, `search_builder.py`, and `search_panes.py` to imperative form
- Updated test module docstrings in `test_request_validator.py`, `test_editor_subpackage.py`, and `test_editor_upload.py` to imperative form

### Fixed

- **Import correctness**: `datatables/compat.py` now imports `get_searchpanes_options` from `search_panes` module instead of the removed `filter` wrapper — resolves import error after wrapper removal

### Fixed

- **Error handling**: `DataTables.get_rows()` now catches `PyMongoError`, `ValueError`, `TypeError`, `KeyError`, and `RuntimeError` to return a generic error message instead of propagating exceptions — prevents information disclosure via stack traces
- **Error handling**: `DataTables.get_export_data()` now catches database and data errors to return an empty list instead of propagating exceptions
- **Error handling**: `Editor.process()` now catches `PyMongoError`, `InvalidDataError`, `FieldMappingError`, `DatabaseOperationError`, `KeyError`, `TypeError`, and `ValueError` to return error dicts with generic messages — prevents information disclosure
- **Error handling**: `fetch_results()` now catches `PyMongoError`, `ValueError`, and `TypeError` to return an empty list instead of propagating
- **Error handling**: `count_total()` now catches `PyMongoError` to return 0 instead of propagating
- **Error handling**: `count_filtered()` now catches `PyMongoError`, `ValueError`, and `TypeError` to return 0 instead of propagating
- **Error handling**: `get_rowgroup_data()` now catches `PyMongoError` to return None instead of propagating
- **Error handling**: `run_create()`, `run_edit()`, and `run_remove()` now catch `PyMongoError` to raise `DatabaseOperationError` instead of propagating raw database errors
- **Error handling**: `_check_text_index()` now catches `PyMongoError` to set `_has_text_index = False` instead of propagating

## [2.1.0] - 2026-04-07

### Changed

- Refactored `datatables` module into a subpackage with focused modules: `filter.py` for query building and `results.py` for result fetching and counting — no behaviour changes
- Refactored `editor` module into a subpackage with focused modules: `core.py` for the main Editor class, `crud.py` for create/edit/remove operations, `document.py` for document formatting and preprocessing, `search.py` for search/dependent/upload handlers, `storage.py` for the pluggable StorageAdapter, and `validator.py` for request validation — no behaviour changes
- Removed deprecated internal re-export shim modules (`query_builder.py`, `query_conditions.py`, `query_global_search.py`, `column_control.py`, `datatables_core.py`, `formatting.py`, `regex_utils.py`, `request_validator.py`) — public API via `mongo_datatables` top-level imports is unchanged
- Updated module docstrings to follow Wholeshoot convention (one sentence, imperative)

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
