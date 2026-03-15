# mongo-datatables Improvement Log

This log tracks iterative improvements made to the mongo-datatables library.

---

**Iteration 15 (v1.19.1) — Feature: DT_RowClass / DT_RowData / DT_RowAttr per-row metadata**

- Added three new optional constructor parameters to `DataTables.__init__`: `row_class`, `row_data`, `row_attr` (all default `None`, placed before `**custom_filter`).
- Each accepts a static value (str/dict) or a callable `(row_dict) -> value` applied after alias remapping in `_process_cursor`.
- When set, injects `DT_RowClass`, `DT_RowData`, or `DT_RowAttr` keys into each result row — consumed by DataTables client to set CSS class, `data-*` attributes, and arbitrary HTML attributes on `<tr>` elements.
- Zero behavior change when parameters are omitted (fully backward compatible).
- 14 tests added in `tests/test_row_metadata.py`. Total: 403 tests passing.

---

**Iteration 7 (v1.19.0 → v1.19.1)** — Bug fix: Global search `search[regex]` string coercion.

- `datatables.py` `global_search_condition`: `search_regex=bool(self.request_args.get("search", {}).get("regex", False))` → `search_regex=self.request_args.get("search", {}).get("regex", False) in (True, "true", "True", 1)`. DataTables sends `search[regex]` as the string `"false"` or `"true"`, not a Python bool. `bool("false") == True`, so global search never escaped regex special characters (`.`, `+`, `*`, etc.) when it should. Mirrors the identical fix applied to column search in v1.19.0.
- Tests added: 3 new tests in `test_datatables_regex_search.py` covering `"false"` string, `"true"` string, and absent key cases for global search. Total: 389 tests passing.

---

**Iteration 6 (v1.18.2 → v1.19.0)** — Feature: `columns[i][name]` support in column search + regex flag string coercion fix.

- `query_builder.py` `build_column_search()`: `column_name = column["data"]` → `column_name = column.get("name") or column["data"]`. When DataTables sends `columns[i][name]` (e.g. via ColReorder), the name is now used for field/type lookup, falling back to `data` when name is absent or empty. Mirrors the same name-or-data resolution already used in `get_sort_specification()` for sort.
- `query_builder.py` `build_column_search()`: `regex_flag = column_search.get("regex", False)` → `regex_flag = column_search.get("regex") in (True, "true", "True", 1)`. DataTables sends `regex` as the string `"false"` or `"true"`, not a Python bool. The old code treated `"false"` as truthy, so `re.escape()` was never applied. Fixed to coerce correctly.
- 11 new tests in `test_colreorder_column_search.py` (6 for name resolution, 5 for regex coercion).
- 386 passed (was 375).

---

**Iteration 4 (v1.18.0 → v1.18.1)** — Quality: Fixed `_sb_string` BSON-serializability. Replaced `re.compile()` objects in `$not` queries for negative string conditions (`!=`, `!contains`, `!starts`, `!ends`) with pure-dict `{"$not": {"$regex": ..., "$options": "i"}}` form. This makes all SearchBuilder query dicts JSON/BSON-serializable and consistent with the rest of the codebase. 8 tests added, 369 total passing.

---

## Iteration 14 — 2026-03-14 (v1.17.4 → v1.18.0)

**Type:** Feature
**Focus:** SearchPanes `total` + `count` dual-count (full server-side protocol compliance)

### Problem
`get_searchpanes_options()` returned only a `count` key per option. The DataTables SearchPanes server-side protocol requires **two** counts:
- `total`: count of each value across the base dataset (custom_filter only, no search/pane filters) — used to show the unfiltered badge and enable "deselect" behaviour
- `count`: count with all current filters applied — used to show how many rows match after filtering

Without `total`, SearchPanes badges showed incorrect numbers and the deselect/reset flow was broken.

### Change
`mongo_datatables/datatables.py` — `get_searchpanes_options()`:
- Replaced single aggregation with two aggregations per column:
  1. **total pipeline**: `custom_filter` only → builds `total_map`
  2. **count pipeline**: full `self.filter` → builds `count_map`
- Merged results: all values from `total_map`, `count` from `count_map` (defaults to 0 if filtered out)
- Removed the now-redundant `$sort` and `$limit` pipeline stages (sorting done in Python via `sorted(..., key=lambda x: -x[1])[:1000]`)
- Each option dict now has `{"label", "value", "total", "count"}`

### Tests Added (9 new)
`tests/test_searchpanes_total_count.py`:
- `test_options_include_total_and_count_keys` — both keys present
- `test_total_equals_base_count_no_filter` — equal when no active filter
- `test_count_zero_when_filtered_out` — filtered-out value gets count=0
- `test_two_aggregations_called_per_column` — exactly 2 aggregate calls per column
- `test_total_pipeline_uses_custom_filter_only` — total pipeline isolation
- `test_count_pipeline_uses_full_filter` — count pipeline uses full filter
- `test_options_sorted_by_total_descending` — sort order
- `test_no_searchpanes_no_options_in_response` — no key when not requested
- `test_get_rows_includes_total_in_options` — end-to-end via get_rows()

### Results
- 359 tests passed (was 350)
- No regressions
- Backward compatible: existing `count` key still present; `total` is additive

---

## Iteration 4 — 2026-03-14

**Type:** Quality  
**Version:** 1.15.0 → 1.15.1  
**Focus:** Performance — eliminate unnecessary DB round-trip on init

### Problem
`_check_text_index()` always called `list(self.collection.list_indexes())` during `__init__`, even when `use_text_index=False`. Every DataTables instantiation incurred an extra MongoDB network call that served no purpose in that code path.

### Change
`mongo_datatables/datatables.py` — `_check_text_index()`:
- Added early return when `use_text_index=False`, setting `_has_text_index = False` directly
- Simplified the detection to `any(...)` (single-pass, no intermediate list)

### Tests Added (4 new)
`tests/test_datatables_initialization.py`:
- `test_use_text_index_false_skips_list_indexes` — verifies `list_indexes` not called when disabled
- `test_use_text_index_true_calls_list_indexes` — verifies `list_indexes` IS called when enabled
- `test_has_text_index_true_when_index_present` — correct detection of text index
- `test_has_text_index_false_when_no_index` — correct detection when absent

### Results
- 272 tests passed (was 268)
- No regressions
- API fully backward compatible (`use_text_index` default remains `True`)

---

## Iteration 1 — 2026-03-14

**Type:** Feature  
**Version:** 1.13.4 → 1.14.0  
**Branch:** feature/regex-search-flag  

### Feature: DataTables Regex Search Flag Support

**Problem:** The DataTables server-side protocol sends `search[regex]` (global) and `columns[i][search][regex]` (per-column) boolean flags to indicate whether the search value should be treated as a raw regex pattern. Both flags were silently ignored — the library always applied `$regex` with the raw (unescaped) value, which was both incorrect (special chars like `.` and `+` were treated as regex metacharacters when they shouldn't be) and incomplete (regex patterns were never honored when the flag was True).

**Fix:**
- `query_builder.py` — `build_column_search`: reads `column["search"]["regex"]`; applies `re.escape()` when False (default), uses raw pattern when True
- `query_builder.py` — `build_global_search`: added `search_regex: bool = False` parameter; same escape/raw logic applied to both quoted-phrase and multi-term OR paths
- `datatables.py` — `global_search_condition`: passes `search_regex=bool(request_args["search"]["regex"])` to `build_global_search`

**Tests added:** `tests/test_datatables_regex_search.py` — 10 new tests

**Test results:** 249 passed, 59 subtests passed, 0 failed

**Backward compatibility:** Fully maintained. Default behavior (`regex=False`) now correctly escapes special characters, which is a correctness fix for the common case. Clients that were relying on unescaped literal strings with special regex chars (e.g., searching for `john.doe` and accidentally matching `johnXdoe`) will now get correct exact-character matching.

**DataTables protocol reference:** https://datatables.net/manual/server-side — `search.regex` and `columns[].search.regex` parameters

---

## Iteration 2 — 2026-03-14

**Type:** Enhancement (Bug Fix / Code Quality)  
**Version:** 1.14.0 → 1.14.1  
**Branch:** quoted-term  

### Fix: Dead variable + \b anchor corruption in quoted-phrase regex search

**Problem:** In `build_global_search` (quoted-phrase branch), two bugs existed:
1. `clean_term` was assigned but never used — dead code.
2. `\b` word-boundary anchors were applied unconditionally around `regex_term`, even when `search_regex=True`. This corrupted user-supplied patterns: e.g. `"^foo"` became `\b^foo\b` — an invalid/broken regex.

**Fix:** `query_builder.py` — quoted-term branch of `build_global_search`:
- Removed dead `clean_term` variable.
- `\b` anchors now only applied when `search_regex=False` (literal path); raw patterns pass through unchanged when `search_regex=True`.

**Tests added:** `tests/test_regex_quoted_phrase.py` — 9 new tests (3 for regex=False, 6 for regex=True)

**Test results:** 258 passed, 59 subtests passed, 0 failed

**Backward compatibility:** Fully maintained. The `search_regex=False` (default) path is unchanged in behavior. The `search_regex=True` path now correctly passes raw patterns without wrapping them in `\b` anchors.

## Iteration 3 — 2026-03-14 (v1.14.1 → v1.15.0)
**Feature: Multi-Column Sort Support**
- `get_sort_specification()` now iterates the full `order` array instead of only `order[0]`
- Respects `columns[i][orderable]` flag — non-orderable columns are skipped
- First occurrence of a field wins when the same column appears multiple times
- `_id` tiebreaker still appended unless `_id` is already in the sort spec
- Added 10 tests in `test_datatables_sort.py`
- Result: 246 passed, 0 failed

## Iteration 5 — 2026-03-14

**Type:** Quality / Code Quality
**Version:** 1.15.0 → 1.15.1
**Focus:** DRY refactor + bare except fixes

### Changes Made

1. **Extracted `_process_cursor()` helper** (`datatables.py`)
   - Eliminated ~15 lines of duplicated result-processing logic shared between `results()` and `get_export_data()`
   - Both methods now delegate cursor-to-list conversion to `_process_cursor()`
   - Zero behavior change; purely structural improvement

2. **Fixed bare `except:` clauses** in `_parse_searchpanes_filters()` (`datatables.py`)
   - Number conversion: `except:` → `except (ValueError, TypeError):`
   - ObjectId conversion: `except:` → `except Exception:`
   - Prevents silently swallowing serious errors (MemoryError, KeyboardInterrupt, etc.)

### Test Results
- All existing tests pass (272 passed, 59 subtests passed)
- No regressions
- API fully backward compatible

### Quality Impact
- Reduced code duplication in result processing path
- Improved exception handling specificity
- Single point of maintenance for cursor→dict conversion logic

Iteration 6 (v1.15.1→1.16.0): Range filtering — pipe-delimited min|max syntax in column search values for number and date type columns. Also fixed date column search to use date-aware parsing instead of regex. 12 tests added.

**Iteration 7 — 2026-03-14** (v1.16.0 → v1.16.1)
Bug Fix: `build_column_search` text branch used `column_name` (UI alias) instead of `db_field` (MongoDB path) as the regex condition key. One-line fix on the `else` branch to match the already-correct number/date branches. 4 tests added. 288 passed.

## Iteration 8 — 2026-03-14

**Type:** Feature  
**Version:** 1.16.1 → 1.17.0  
**Focus:** SearchBuilder server-side support

### Feature: DataTables SearchBuilder Integration

**Problem:** The DataTables SearchBuilder extension sends a nested `searchBuilder` parameter when `serverSide: true` is enabled, containing a typed criteria tree with AND/OR logic. This was silently ignored — users could not use the SearchBuilder UI for server-side filtered tables.

**Implementation:** Added four methods to `DataTables` in `datatables.py`:
- `_parse_search_builder()` — entry point; reads `request_args["searchBuilder"]`
- `_sb_group()` — recursively converts a criteria group to `$and`/`$or`
- `_sb_criterion()` — dispatches a leaf criterion by type (num, date, string)
- `_sb_number()`, `_sb_date()`, `_sb_string()` — type-specific MongoDB condition builders

Integrated into the `filter` property so SearchBuilder conditions compose correctly with custom filters, SearchPanes, global search, and column search.

**Conditions supported:**
- String/html: `=`, `!=`, `contains`, `!contains`, `starts`, `!starts`, `ends`, `!ends`, `null`, `!null`
- Number: `=`, `!=`, `<`, `<=`, `>`, `>=`, `between`, `!between`, `null`, `!null`
- Date: `=`, `!=`, `<`, `>`, `between`, `!between`, `null`, `!null`
- Nested groups with AND/OR logic (recursive)

**Tests added:** `tests/test_search_builder.py` — 35 new tests covering all condition types, logic operators, nesting, and filter integration.

**Test results:** 318 passed, 59 subtests passed, 0 failed

**Backward compatibility:** Fully maintained. The `searchBuilder` key is only read when present; all existing behavior is unchanged.

## Iteration 9 — Performance: Cache `filter` Property (v1.17.0 → v1.17.1)

**Date:** 2026-03-14  
**Type:** Quality / Performance  
**Version:** 1.17.0 → 1.17.1

### Problem
The `filter` property recomputed 6 sub-conditions on every access. With 4 callers each doing `if self.filter: ... self.filter`, a single request triggered up to 8 full recomputations of `_parse_search_builder()`, `_parse_searchpanes_filters()`, `global_search_condition`, `column_search_conditions`, and `column_specific_search_condition`.

### Solution
Added `_filter_cache = None` in `__init__` alongside existing caches (`_results`, `_recordsTotal`, `_recordsFiltered`). Extracted filter computation into `_build_filter()` method. The `filter` property now returns the cached result on repeated access.

### Changes
- `datatables.py`: Added `self._filter_cache = None` in `__init__`, added `_build_filter()` method, `filter` property now caches via `_filter_cache`
- 2 new tests for cache behavior (same-object identity, None-before-access)

### Results
- 330 tests passing (328 existing + 2 new)
- Zero behavior change — pure performance improvement
- Consistent with existing caching pattern (`_results`, `_recordsTotal`, `_recordsFiltered`)

## Iteration 8 (v1.17.0 → v1.17.1) — 2026-03-14
**Type:** Feature
**Feature:** ColReorder `order[i][name]` support in `get_sort_specification`

**Problem:** DataTables ColReorder extension sends `order[i][name]` (column name string) instead of relying solely on `order[i][column]` (integer index) when columns are reordered client-side. The previous implementation only used the integer index, causing incorrect sort behavior after column reordering.

**Solution:** In `get_sort_specification`, when `order[i][name]` is non-empty, first attempt to resolve the column by matching against `column["name"]` or `column["data"]`. Fall back to index-based lookup only when name lookup yields no match or name is absent/empty.

**Files changed:**
- `mongo_datatables/datatables.py` — `get_sort_specification`: added name-based column resolution with index fallback
- `tests/test_colreorder.py` — 10 new tests

**Tests:** 10 added in `test_colreorder.py`. 328 passed (up from 318).

## Iteration 10 — Code Quality Cleanup (v1.17.2)

**Type:** Quality
**Date:** 2026-03-14
**Version:** 1.17.1 → 1.17.2

### Changes
- `_sb_date()`: Replaced `__import__('datetime').timedelta(days=1)` (×2) with `timedelta(days=1)` — module-level import already present; `__import__` inline is an anti-pattern
- `_sb_number()` / `_sb_date()`: Removed redundant local `from mongo_datatables.utils import TypeConverter/DateHandler` — already imported at module level
- `_check_text_index()`: Collapsed `indexes = list(...); any(... for idx in indexes)` into `any(... for idx in self.collection.list_indexes())` — avoids unnecessary list materialization
- `count_filtered()`: Changed `except (PyMongoError, Exception)` → `except Exception` — `PyMongoError` is a subclass of `Exception`; the tuple was redundant

### Tests
330 passed, 59 subtests, 0 failures (0.48s) — no regressions

### Notes
Pure code quality fixes. Zero behavior change. Fully backward compatible.

## Iteration 11 — v1.17.2 (2026-03-14)

**Type:** Bug Fix  
**Focus:** Projection alias resolution

### Problem
The `projection` property used `column["data"]` (UI alias) directly as MongoDB projection keys. When a `DataField` had an alias differing from its db field name, MongoDB silently omitted those fields from results.

### Fix
One-line change in `projection` property: `projection[self.field_mapper.get_db_field(column["data"])] = 1`

### Tests
3 new tests in `test_datatables_query_pipeline.py` covering aliased, non-aliased, and mixed projection scenarios.

### Result
333 tests passing. Backward compatible — no alias means `get_db_field` returns the original name unchanged.

## Iteration 12 — v1.17.3 (2026-03-14)

**Type:** Feature / Bug Fix  
**Focus:** Proper `length=-1` (Show All) handling

### Problem
DataTables sends `length=-1` when the user selects "Show All" from the page length menu. The `results()` method was adding `{"$limit": -1}` to the MongoDB aggregation pipeline. MongoDB rejects negative `$limit` values, causing a runtime error on real databases. The existing test incorrectly asserted `$limit: -1` was present and noted "This is likely to be handled by MongoDB as 'no limit'" — it is not.

### Fix
One-line change in `results()`: `if self.limit:` → `if self.limit and self.limit > 0:`

When `length=-1`, no `$limit` stage is added to the pipeline, returning all matching documents. The `limit` property still returns `-1` for backward compatibility (callers can check `datatables.limit == -1`).

### Tests
- Fixed `test_pagination_with_all_records` in `test_datatables_pagination.py` to assert no `$limit` stage when `length=-1`
- Added `tests/test_length_all.py` with 5 new tests covering: omit limit on -1, limit property value, positive limit included, zero length omits limit, get_rows() works end-to-end

### Result
338 tests passing. Backward compatible.

## Iteration 13 (v1.17.3 → v1.17.4) — 2026-03-14

**Type:** Quality (dead code removal)
**Focus:** Code clarity and test correctness

### Change: Removed dead inner try/except from `count_total()`

**File:** `mongo_datatables/datatables.py`

**Problem:** `count_total()` had an inner `try/except (TypeError, ValueError)` block wrapping `int(estimated_count)`. The comment said "Convert to int in case it's a mock object" — a test concern embedded in production code. `estimated_document_count()` always returns `int` per PyMongo spec, so the except branch was unreachable in production. The block also contained an early `return` that bypassed the outer cache assignment path, making the control flow harder to reason about.

**Fix:** Removed the 9-line dead block entirely. The method now reads linearly: call `estimated_document_count()`, branch on size/custom_filter, assign `_recordsTotal`, return.

**Side effect discovered:** Three test files (`base_test.py`, `test_buttons.py`, `test_searchpanes.py`) had mocks that never set `estimated_document_count.return_value`, relying silently on the dead fallback path to handle the `MagicMock < 100000` comparison failure. Fixed all three mocks to explicitly set `return_value = 0`.

**Tests:** 350 passing (+8 from mock fixes + 1 new test in `test_count_optimization.py`)
**Lines removed:** 9 (dead code) | **Lines added:** ~20 (mock fixes + 1 new test)

## Iteration 14 (v1.18.0 → v1.18.1) — 2026-03-14
**Type:** Quality (bug fix)
**Focus:** Correctness of `count_total()` with `custom_filter`

### Problem
`count_total()` called `count_documents({})` (empty filter) in both the main branch and the PyMongoError fallback, even when `custom_filter` was set. This caused `recordsTotal` to reflect the entire collection size rather than the filtered base dataset, producing incorrect DataTables pagination info (e.g., "Showing 10 of 50,000" when the actual filtered set was 1,200).

### Fix
Two-line change in `count_total()`: `count_documents({})` → `count_documents(self.custom_filter or {})` in both the main branch and the error fallback.

### Tests
2 new tests in `test_count_optimization.py`:
- `test_count_total_with_custom_filter_large_collection`: large collection (500k) with `status="active"` — verifies `count_documents({"status": "active"})` is called
- `test_count_total_with_custom_filter_small_collection`: small collection (50) with `role="admin"` — verifies `count_documents({"role": "admin"})` is called

### Result
361 tests passing. Backward compatible — `custom_filter or {}` is identical to `{}` when no custom_filter is set.

---

## Iteration 8 — v1.20.0 — `allow_disk_use` Aggregation Support
**Date:** 2026-03-14  
**Type:** Feature  
**Version:** 1.19.1 → 1.20.0

### Problem
MongoDB enforces a 100 MB in-memory limit on aggregation pipelines. Large datasets
with complex SearchBuilder criteria trees, SearchPanes facet counts, RowGroup
aggregations, or full-collection exports could silently fail or raise a
`QueryExceededMemoryLimitNoPushdown` error in production.

### Solution
Added `allow_disk_use: bool = False` parameter to `DataTables.__init__()`. When
`True`, `allowDiskUse=True` is forwarded to every `collection.aggregate()` call
in the class (6 call sites: `results()`, `count_filtered()`, `get_export_data()`,
`get_searchpanes_options()` ×2, `_get_rowgroup_data()`).

### Changes
- `mongo_datatables/datatables.py`: new `allow_disk_use` param + 6 aggregate call updates
- `tests/test_allow_disk_use.py`: 7 new tests covering default=False, True propagation,
  and backward compatibility
- `setup.py`: version bump 1.19.1 → 1.20.0

### Test Results
- New tests: 7/7 passed
- Full suite: 410 passed, 59 subtests — zero failures

### Backward Compatibility
Fully backward compatible. Default is `False` (existing behaviour unchanged).
Existing call sites require no modification.

## Iteration 16 (v1.19.1 → v1.19.2) — 2026-03-14

**Type:** Bug Fix / Correctness
**Focus:** Regex metacharacter escaping in colon-syntax column search

### Problem
`build_column_specific_search` in `query_builder.py` passed user-supplied values directly
into MongoDB `$regex` without `re.escape()`. This caused regex metacharacters (`.`, `+`,
`*`, `?`, `[`, `]`, `(`, `)`, `^`, `$`, `|`) to be interpreted as regex operators instead
of literal characters. For example, searching `email:user@domain.com` would match
`user@domainXcom` because `.` matched any character.

The same bug existed in the fallback branches of `_build_number_condition` and
`_build_date_condition`.

By contrast, `build_column_search` and `build_global_search` already correctly applied
`re.escape()` when `regex_flag=False` — making this an inconsistency.

### Fix
Applied `re.escape(value)` in three locations in `query_builder.py`:
1. `build_column_specific_search` — string-type else branch
2. `_build_number_condition` — except fallback
3. `_build_date_condition` — except fallback

### Tests Added
- `tests/test_regex_escape_colon_search.py` — 9 new tests covering dot, plus, brackets,
  caret, dollar, parentheses, star, question mark, and pipe metacharacters

### Test Count
410 → 419 tests passing
