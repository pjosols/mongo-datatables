# Changelog

All notable changes to mongo-datatables are documented here.

## [2.0.0] - 2026-03-17

Major release. Significant new functionality, correctness fixes, and a full
documentation and packaging overhaul since the last published version (1.1.1).

### Added

**Search**
- Smart / AND semantics: `search[smart]=true` (DataTables default) requires each word to independently match at least one searchable column
- Per-column smart AND: `columns[i][search][smart]` mirrors global smart behaviour
- Case sensitivity control: `search[caseInsensitive]` (global) and `columns[i][search][caseInsensitive]` (per-column)
- Regex mode: `search[regex]` and `columns[i][search][regex]` pass raw MongoDB regex patterns
- Colon syntax comparison operators: `field:>N`, `field:>=N`, `field:<N`, `field:<=N`, `field:=N` for `number` and `date` fields
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

---

## [1.1.1] - prior release

Last published version on PyPI.
