## [1.38.0] - 2026-03-15

### Added
- Per-column `columns[i][search][smart]` AND semantics: when `smart=true` (default) and a column search value contains multiple words, each word must independently match the column field (AND logic). Mirrors the existing global `search[smart]` behavior. When `smart=false` or `regex=true`, the original single-phrase behavior is preserved.

### Fixed
- `_parse_search_fixed`: corrected a missing newline that placed `continue` and the next statement on the same line.

## [1.34.0] - 2026-03-15

### Added
- `Editor.__init__` now accepts `options=` kwarg (plain dict or zero-arg callable). When set, `process()` includes an `options` key in every response, enabling server-driven population of select/radio/checkbox field options per the DataTables Editor protocol.

## [1.32.0] - 2026-03-15

### Added
- `Editor.search()` — implements the DataTables Editor `action=search` protocol for `autocomplete` and `tags` field types. Supports `search=<term>` (case-insensitive regex prefix match) and `values[]=<val>` (exact `$in` lookup). Returns `{"data": [{"label": "...", "value": "..."}]}` with deduplication and a 100-result limit.

## [1.31.1] - 2026-03-15

### Changed
- Narrowed bare `except Exception` to `except (ValueError, TypeError, FieldMappingError)` in `_sb_number` and `_sb_date` — unexpected exceptions now surface instead of being silently swallowed

## [1.31.0] - 2026-03-15
### Added
- `search[caseInsensitive]` support: global search, column search, and colon-syntax search now
  respect the DataTables `caseInsensitive` flag (default `true`). Pass `false` to opt into
  case-sensitive regex matching. Per-column override via `columns[i][search][caseInsensitive]`
  takes precedence over the global setting.

## [1.29.6] - 2026-03-15
### Added
- `bson.Binary` serialization in `_format_result_values`: UUID subtypes (3/4) are converted to canonical UUID string form via `uuid.UUID`; other subtypes are hex-encoded. Fixes `TypeError` at JSON serialization time for documents containing binary fields. Handled in both top-level field and list-items branches.

## [1.29.4] - 2026-03-15
### Fixed
- `_sb_date` now handles full ISO datetime strings (e.g. `2024-01-15T00:00:00.000Z`) from DataTables SearchBuilder by extracting the date portion with `.split('T')[0]` before parsing. Previously, ISO datetime strings caused `parse_iso_date` to raise `FieldMappingError`, silently returning `{}` — the SearchBuilder date filter appeared to work in the UI but had no effect on results. Same fix as v1.28.2 (ColumnControl) and v1.29.2 (SearchPanes).

## [1.29.0] - 2026-03-15

### Changed
- Editor now supports all pymongo object types (MongoClient, raw Database, Flask-PyMongo, dict-style), matching DataTables._get_collection behavior
- `db` property updated to resolve correctly for all supported pymongo object types
- `collection` property now resolved at construction time via `_resolve_collection()` static method

## [1.28.0] - 2026-03-15

### Added
- `search[smart]` AND semantics for multi-word global search. When `search[smart]=true` (DataTables default), each search term must match at least one searchable column (`$and` of per-term `$or`s). When `search[smart]=false`, the previous flat `$or` behavior is preserved. Single-term and quoted-phrase searches are unaffected.

## [1.27.5] - 2026-03-15
### Fixed
- `search_terms_with_a_colon` now uses `":" in term` instead of `term.count(":") == 1`, fixing silent drop of search terms containing multiple colons (e.g. `url:https://example.com`). Such terms were excluded from both field-specific and global search paths, producing zero results with no error. `build_column_specific_search` already uses `split(":", 1)` so multi-colon values are handled correctly.
- `_sb_criterion` now routes `html-num` and `html-num-fmt` SearchBuilder column types to `_sb_number` instead of `_sb_string`, matching the DataTables spec. Previously these types produced `$regex` conditions instead of numeric comparisons.

## [1.27.4] - 2026-03-15
### Added / Fixed
- `get_rows()` now returns a DataTables-compatible error response on unhandled exceptions: `{"draw": ..., "error": str(e), "recordsTotal": 0, "recordsFiltered": 0, "data": []}` instead of propagating the exception to the caller
- `_check_text_index()` now guards `list_indexes()` against `PyMongoError`, falling back to `_has_text_index = False` on connection failure
- Added 4 new tests covering both error paths and the success (no-error-key) path (564 total)

## v1.27.2 — 2026-03-15

### Performance: Pre-compute field mapper lookups in `build_global_search`

- In `query_builder.py`, `build_global_search()` previously called `field_mapper.get_field_type()` and `field_mapper.get_db_field()` inside the inner `for term in search_terms` loop, causing O(N×M) lookups for N terms and M columns.
- Refactored to pre-compute `col_meta` (a list of `(db_field, field_type)` tuples, excluding date columns) once before the term loop, reducing lookups to O(M).
- No API changes. Identical output for all inputs.
- Added 4 new tests in `tests/test_global_search_perf.py` covering: call-count efficiency, multi-term OR conditions, quoted-phrase word-boundary regex, and date-column exclusion.

# Changelog

All notable changes to mongo-datatables are documented here.

## [1.27.1] - 2026-03-15
### Fixed
- Date range column search upper bound now uses `$lt` (exclusive) instead of `$lte` (inclusive) with next-day midnight, preventing off-by-one inclusion of documents at exact day boundaries

## [1.27.0] - 2026-03-15
### Added
- `columns[i][orderData]` support: when a column definition includes `orderData` (int or list of ints), sorting that column redirects to the specified column indices instead. Scalar and list forms both supported. Out-of-range indices and non-orderable targets are silently skipped. Backward compatible — columns without `orderData` behave identically to before.

## [1.22.0] - 2026-03-14

### Added
- `searchFixed` support (DataTables 2.0+ named searches): named fixed searches sent as
  `searchFixed[name]=value` in request args are now applied as additional AND-combined
  global search conditions across all searchable columns.

## [1.21.0] - 2026-03-14
### Added
- `row_id` parameter to `DataTables.__init__()`: specify a custom field to use as `DT_RowId` instead of MongoDB `_id`. The field stays in the row data and is always projected. Default `None` preserves existing behavior.

## [1.20.1] - 2026-03-14
### Fixed
- `columns[i][searchable]` string coercion: DataTables sends `"true"`/`"false"` as strings from HTTP form data; `column.get("searchable", False)` treated `"false"` as truthy. Fixed in `searchable_columns` property, `get_searchpanes_options`, and `MongoQueryBuilder.build_column_search` using the same membership-test pattern already applied to the `regex` flag.

## [1.20.2] - 2026-03-14
### Fixed
- `orderable` string/bool coercion in `get_sort_specification()`: columns with `orderable: False` (bool) were incorrectly included in sort specs. Now uses `not in (False, "false", "False", 0)` consistent with the `searchable` fix (v1.20.1).

## [1.20.2] - 2026-03-14
### Removed
- Dead `except PyMongoError` block in `count_filtered()` that was unreachable (inner `except Exception` caught first); collapsed to flat 3-level fallback
- Unused `startRender`/`endRender` keys from `_parse_rowgroup_config()` (server never used them); method reduced from 20 to 6 lines

## [1.20.0] - 2026-03-14
### Added
- SearchBuilder date `<=` and `>=` operator support in `_sb_date()`

## [1.19.3] - 2026-03-14
### Changed
- Replaced 4 near-identical extension config parsers (`_parse_fixedcolumns_config`, `_parse_searchpanes_config`, `_parse_searchbuilder_config`, `_parse_rowgroup_config`) with single `_parse_extension_config(key)` helper; net -102 lines

## [1.19.2] - 2026-03-14
### Fixed
- `re.escape()` missing in colon-syntax `$regex` paths in `build_column_specific_search` and fallback branches of `_build_number_condition`/`_build_date_condition`

## [1.19.1] - 2026-03-14
### Added
- `DT_RowClass`, `DT_RowData`, `DT_RowAttr` per-row metadata support via optional constructor params (accepts static value or callable)

### Fixed
- `search[regex]` string coercion: `bool("false") == True` bug fixed; now checks membership in `(True, "true", "True", 1)`

## [1.19.0] - 2026-03-14
### Added
- `columns[i][name]` support in column search (alongside existing `columns[i][data]` lookup)

### Fixed
- NaN/Inf float values in query results now serialized as `None` to prevent JSON serialization errors

## [1.18.2] - 2026-03-14
### Fixed
- `$limit` stage now omitted from aggregation pipeline when `length <= 0` (DataTables "Show All" sends `length: -1`), preventing MongoDB error from `$limit: -1`

## [1.18.1] - 2026-03-14
### Fixed
- `count_total()` now passes `custom_filter` to `count_documents()` instead of empty `{}`
- `_sb_string()` negation conditions (`!=`, `!contains`, `!starts`, `!ends`) now use pure-dict `{"$not": {"$regex": ..., "$options": "i"}}` form instead of `re.compile()` objects, ensuring BSON serializability

## [1.18.0] - 2026-03-14
### Added
- SearchPanes `total` + `count` dual-count for full server-side protocol compliance
- `_remap_aliases()` helper to remap DB field names back to UI aliases in query results

## [1.17.4] - 2026-03-14
### Changed
- Removed dead inner `try/except` from `count_total()` that existed only for mock handling

## [1.17.3] - 2026-03-14
### Fixed
- `_sb_string()` negation conditions (`!=`, `!contains`, `!starts`, `!ends`) now use
  `re.compile(..., re.IGNORECASE)` instead of invalid `{"$not": {"$regex": ..., "$options": "i"}}`
  syntax that MongoDB rejects
- `build_global_search()` quoted-phrase and unquoted non-text-index paths now resolve
  column aliases to DB field names via `field_mapper.get_db_field()`, fixing queries
  that silently matched nothing when DataField aliases differed from MongoDB field names

## [1.17.2] - 2026-03-14
### Fixed
- `projection` property now resolves UI column aliases to actual MongoDB field names via
  `field_mapper.get_db_field()`. Previously, aliased DataFields caused MongoDB to silently
  omit those fields from query results.

## [1.17.1] - 2026-03-14
### Added
- ColReorder `order[i][name]` support in `get_sort_specification()` for name-based column ordering

### Changed
- `filter` property now cached via `_filter_cache` to avoid recomputation on repeated access

## [1.17.0] - 2026-03-14
### Added
- SearchBuilder server-side support: full nested AND/OR criteria tree with string, number, and date conditions

## [1.16.1] - 2026-03-14
### Fixed
- `build_column_search()` text branch used UI alias instead of `db_field` as MongoDB regex key

## [1.16.0] - 2026-03-14
### Added
- Range filtering: pipe-delimited `min|max` syntax for number and date column searches

## [1.15.1] - 2026-03-14
### Performance
- Skip `list_indexes()` DB call when `use_text_index=False`

## [1.15.0] - 2026-03-14
### Added
- Multi-column sort: `get_sort_specification()` iterates full `order` array and respects `orderable` flag

## [1.14.1] - 2026-03-14
### Fixed
- Dead `clean_term` variable removed
- `\b` anchor corruption in quoted-phrase regex search fixed

## [1.14.0] - 2026-03-14
### Added
- Regex search flag support: `search[regex]` and `columns[i][search][regex]` now respected; applies `re.escape()` for literal search, raw pattern for regex mode
