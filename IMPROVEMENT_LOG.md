# mongo-datatables Improvement Log

This log tracks iterative improvements made to the mongo-datatables library.

---

## v1.32.0 — FEATURE: Editor `action=search` (autocomplete/tags lookup) (2026-03-15)

**Type:** Feature  
**Iteration:** 2 of 12

### Problem
The DataTables Editor `autocomplete` and `tags` field types send `action=search` requests to the server for dynamic option lookup. The `Editor` class had no handler for this action — it returned `{"error": "Unsupported action: search"}`, breaking any Editor form that used these field types with server-side Ajax.

### Changes
- `editor.py`: added `import re`
- `editor.py`: added `search()` method (~15 lines) after `remove()`:
  - Reads `field`, `search`, `values` from `request_args`
  - Maps UI field name → DB field name via `field_mapper`
  - `search=<term>` mode: `{db_field: {"$regex": re.escape(term), "$options": "i"}}` (case-insensitive prefix match)
  - `values=[...]` mode: `{db_field: {"$in": values}}` (exact lookup)
  - Neither present: returns `{"data": []}` immediately
  - Queries with `.find(query, {db_field: 1}).limit(100)`, deduplicates by value
  - Returns `{"data": [{"label": str_val, "value": str_val}, ...]}`
- `editor.py`: registered `"search": self.search` in `process()` actions dict (bypasses validators — read-only)
- `EDITOR_GAPS.md`: item #3 marked ✅ DONE

### Tests
- `tests/test_editor_search_action.py`: 7 new tests (search by term, search by values, unknown field, empty term, deduplication, no params, registered in process)
- **706 passed** (was 699), 59 subtests passed

### Backward Compatibility
Fully backward compatible. Previously `action=search` returned `{"error": "Unsupported action: search"}`. Now it returns the correct protocol response. No existing behavior changed.

---

## v1.31.1 — Quality: Narrow exception handling in `_sb_number` and `_sb_date` (2026-03-15)

**Type:** Quality / Code Correctness  
**Iteration:** 9 of 10

### Problem
`_sb_number` and `_sb_date` used bare `except Exception: pass` to handle value-conversion failures. This silently swallowed ALL exceptions — including programming errors like `AttributeError`, `NameError`, or unexpected `PyMongoError` — returning `{}` and making the SearchBuilder filter a no-op. Only `ValueError`, `TypeError`, and `FieldMappingError` (the custom exception raised by `TypeConverter.to_number` and `DateHandler.parse_iso_date`) are expected conversion failures.

### Changes
- `datatables.py`: added `from mongo_datatables.exceptions import FieldMappingError` import
- `_sb_number`: `except Exception:` → `except (ValueError, TypeError, FieldMappingError):`
- `_sb_date`: `except Exception:` → `except (ValueError, TypeError, FieldMappingError):`

### Tests
- `tests/test_sb_exception_narrowing.py`: 8 new tests (invalid number returns empty, invalid number between returns empty, valid number works, valid number gt works, invalid date returns empty, invalid date between returns empty, valid date works, valid date gt works)
- **674 passed** (was 666), 59 subtests passed

### Backward Compatibility
Fully backward compatible. The only behavioral change is that unexpected exceptions (not `ValueError`/`TypeError`/`FieldMappingError`) now propagate instead of being silently swallowed — which is the correct behavior.

---

## v1.30.2 — BUG FIX: Type-aware `null`/`!null` in SearchBuilder (2026-03-15)

### Problem
`_sb_criterion` used `{"$in": [None, "", False]}` for `null` and `{"$nin": [None, "", False]}` for `!null` regardless of column type. For `num`, `num-fmt`, `html-num`, `html-num-fmt`, `date`, `moment`, and `luxon` types, `""` and `False` are not valid null representations — a numeric field containing `""` or `False` is a data error, not a null value. The query was semantically incorrect for these types.

### Changes
- `datatables.py` — `_sb_criterion`: replaced the two unconditional null/!null returns with type-aware dispatch:
  - num/date types: `null` → `{field: None}`, `!null` → `{field: {"$ne": None}}`
  - string/html types: `null` → `{"$in": [None, ""]}`, `!null` → `{"$nin": [None, ""]}`
  - Also removed `False` from string null checks (a boolean `False` is not a null string)

### Tests
- Replaced 3 stale null tests with 8 type-specific tests in `tests/test_search_builder.py`
- **652 passed** (was 647), 59 subtests passed

---

## v1.30.1 — bson.Regex Serialization (2026-03-15)

### Problem
`bson.Regex` values (used to store regex patterns in MongoDB documents) passed through `_format_result_values` unhandled, causing `TypeError: Object of type Regex is not JSON serializable` at response time. This is the same class of bug as Decimal128 (v1.29.5) and Binary (v1.29.6).

### Changes
- `datatables.py`: added `Regex` to `from bson import ...` import
- `_format_result_values`: added `Regex → '/pattern/flags'` string serialization in both the top-level field branch and the list-items branch. Flags encoded as standard regex flag characters (`i`, `m`, `s`, `x`).
- `tests/test_regex_serialization.py`: 7 new tests (regex with flags, regex without flags, multiple flags, regex in list, JSON serializable top-level, non-regex fields unaffected, regex in list JSON serializable)

### Backward Compatibility
Fully backward compatible — only adds handling for a previously-unhandled BSON type.

### Test Results
- New tests: 7/7 passed
- Full suite: 647 passed (was 640)
- Version: 1.30.1

---

## v1.29.6 — bson.Binary / UUID Serialization (2026-03-15)

### Problem
`bson.Binary` values (used for UUIDs and raw binary data) passed through `_format_result_values` unhandled, causing `TypeError` at JSON serialization time. MongoDB commonly stores UUIDs as `bson.Binary` with subtype 3 (old UUID) or 4 (standard UUID). This is the same class of bug as the Decimal128 fix in v1.29.5.

### Changes
- `datatables.py`: added `import uuid`; added `Binary` to `from bson import ...` import
- `_format_result_values`: added `Binary → str(uuid.UUID(...))` for subtypes 3/4, `Binary → hex()` for other subtypes — in both the top-level field branch and the list-items branch
- `tests/test_binary_serialization.py`: 7 new tests (UUID subtype 4, UUID subtype 3, non-UUID binary, unaffected fields, UUID in list, non-UUID binary in list, JSON serializable)

### Backward Compatibility
Fully backward compatible — only adds handling for a previously-unhandled BSON type.

### Test Results
- New tests: 7/7 passed
- Full suite: 631 passed (was 624)
- Branch: feature/binary-uuid-serialization, commit: 492d7e3

---

## v1.29.5 — Decimal128 Serialization (2026-03-15)

### Problem
`bson.Decimal128` values (used for precise monetary/financial data) passed through `_format_result_values` unhandled, causing JSON serialization errors at the response layer. Also, `get_searchpanes_options` would raise `TypeError: cannot use 'bson.decimal128.Decimal128' as a dict key` when a SearchPanes column contained Decimal128 values.

### Changes
- `datatables.py`: `from bson import Decimal128, ObjectId` (was `from bson.objectid import ObjectId`)
- `_format_result_values`: added `Decimal128 → float(val.to_decimal())` branch for top-level fields and list items
- `get_searchpanes_options`: added `_hashable()` inline helper to normalize Decimal128 (unhashable) to string before building `total_map`/`count_map` dicts
- `tests/test_decimal128_serialization.py`: 8 new tests (top-level, nested, list, zero, negative, unaffected types, mixed list, SearchPanes)

### Backward Compatibility
Fully backward compatible — only adds handling for a previously-unhandled type.

### Test Results
- New tests: 8/8 passed
- Full suite: 624 passed (was 616)
- Branch: feature/decimal128-serialization, commit: be7a578

---

## v1.29.4 — 2026-03-15 — BUG FIX: `_sb_date` ISO datetime string handling

**Type:** Bug Fix  
**Iteration:** 1 of 10

### Problem
`_sb_date` in `datatables.py` called `DateHandler.parse_iso_date(v)` directly. `parse_iso_date` only accepts `YYYY-MM-DD` format. DataTables SearchBuilder sends full ISO datetime strings (e.g. `2024-01-15T00:00:00.000Z`) as the value for date conditions. When such a value was received, `parse_iso_date` raised `FieldMappingError`, caught by `except Exception: pass` in `_sb_date`, silently returning `{}` — the SearchBuilder date filter appeared to work in the UI but had no effect on results.

This is the same class of bug fixed in v1.28.2 (`_build_column_control_condition`) and v1.29.2 (`_parse_searchpanes_filters`).

### Fix
One-line change in `_sb_date`'s inner `_d` helper:

**Before:** `return DateHandler.parse_iso_date(v)`  
**After:** `return DateHandler.parse_iso_date(v.split('T')[0])`

`v.split('T')[0]` extracts the date portion from any ISO string:
- `"2024-01-15"` → `"2024-01-15"` (unchanged)
- `"2024-01-15T00:00:00.000Z"` → `"2024-01-15"` (date extracted)

### Tests Added
4 new tests in `tests/test_sb_date_iso_datetime.py`:
- `test_equal_iso_datetime_string` — ISO datetime with `=` produces day-range condition
- `test_greater_iso_datetime_string` — ISO datetime with `>` produces `$gt` condition
- `test_less_iso_datetime_string` — ISO datetime with `<` produces `$lt` condition
- `test_plain_date_string_unchanged` — plain `YYYY-MM-DD` still works correctly

### Test Results
- 4 new tests added, all passing
- Full suite: 616 passed (was 612), 0 regressions

### Backward Compatibility
Fully backward compatible. `YYYY-MM-DD` values are unchanged (split on `T` returns the original string as `[0]`). Only ISO datetime strings (previously silently broken) now work correctly.

---

## v1.29.3 — Quality: Cache `search_terms` property (Iteration 5 of 10)

**Date:** 2026-03-15  
**Type:** Quality / Performance  
**Backward Compatible:** Yes

### Problem
`search_terms` was an uncached property that called `SearchTermParser.parse(self.search_value)` on every access. It is accessed via `search_terms_without_a_colon`, `search_terms_with_a_colon`, and `_parse_search_fixed` — meaning the parser ran 3+ times per request even though `search_value` is immutable for the lifetime of a DataTables instance.

### Fix
- Added `self._search_terms_cache = None` in `__init__` alongside existing cache attributes.
- `search_terms` property now memoizes: parses once on first access, returns cached list on subsequent calls.
- 2 lines changed in `datatables.py`.

### Tests
- Added `tests/test_search_terms_cache.py` (5 tests): cache initialized to None, populated on first access, `SearchTermParser.parse` called exactly once across multiple accesses, same object identity returned, empty string cached correctly.
- **Suite: 612 passed, 59 subtests passed.**

---

## v1.29.2 — 2026-03-15 — BUG FIX: SearchPanes date value conversion in `_parse_searchpanes_filters`

**Type:** Quality / Bug Fix
**Iteration:** 4 of 10

### Problem
`_parse_searchpanes_filters` in `datatables.py` converted `number` and `objectid` values from their string representations but silently skipped `date` fields — leaving them as raw strings. MongoDB stores dates as `datetime` objects, so a `$in` filter with string values never matched any documents. SearchPanes date column filters appeared to work in the UI but had no effect on results.

This is the same class of bug fixed in v1.28.2 (`_build_column_control_condition`) and v1.28.1 (`_build_column_control_condition` notContains/notEqual).

### Fix
Added a `date` branch to the value-conversion loop in `_parse_searchpanes_filters`:

**Before:** `date` values fell through to the `else` branch, remaining as raw strings.

**After:**
```python
elif field_type == "date":
    try:
        converted_values.append(DateHandler.parse_iso_date(value.split('T')[0]))
    except Exception:
        converted_values.append(value)
```

`value.split('T')[0]` handles both `"YYYY-MM-DD"` and `"YYYY-MM-DDTHH:MM:SS.sssZ"` forms (same pattern as v1.28.2). On parse failure, falls back to the raw string.

### Tests Added
4 new tests in `tests/test_searchpanes_date_filter.py`:
- `test_date_iso_date_string_converted_to_datetime` — `"YYYY-MM-DD"` → `datetime`
- `test_date_iso_datetime_string_converted_to_datetime` — `"YYYY-MM-DDTHH:MM:SS.sssZ"` → `datetime`
- `test_date_invalid_falls_back_to_string` — unparseable value stays as string
- `test_date_multiple_values` — multiple values all converted

### Test Results
- 4 new tests added, all passing
- Full suite: 607 passed (was 603), 0 regressions

### Backward Compatibility
Fully backward compatible. Only `date`-typed SearchPanes columns are affected. Previously broken (silently no-op), now correct.

---

## v1.29.1 — 2026-03-15 — BUG FIX: BSON-serializable `$not` in `_sb_string`

**Type:** Bug Fix  
**Iteration:** 3 of 10

### Problem
`_sb_string` in `datatables.py` used `re.compile()` objects as the value of `$not` for all four negation conditions (`!=`, `!contains`, `!starts`, `!ends`). `re.Pattern` objects are not BSON/JSON-serializable, causing failures when these conditions appear in aggregation pipelines (e.g. SearchPanes, count_filtered) or any serialization path.

### Fix
Four one-line changes in `_sb_string`:

**Before:**
```python
if condition == "!=":       return {field: {"$not": re.compile(f"^{s}$", re.IGNORECASE)}}
if condition == "!contains": return {field: {"$not": re.compile(s, re.IGNORECASE)}}
if condition == "!starts":   return {field: {"$not": re.compile(f"^{s}", re.IGNORECASE)}}
if condition == "!ends":     return {field: {"$not": re.compile(f"{s}$", re.IGNORECASE)}}
```

**After:**
```python
if condition == "!=":       return {field: {"$not": {"$regex": f"^{s}$", "$options": "i"}}}
if condition == "!contains": return {field: {"$not": {"$regex": s, "$options": "i"}}}
if condition == "!starts":   return {field: {"$not": {"$regex": f"^{s}", "$options": "i"}}}
if condition == "!ends":     return {field: {"$not": {"$regex": f"{s}$", "$options": "i"}}}
```

Same class of fix as v1.28.1 (`_build_column_control_condition`) — pure-dict `{"$not": {"$regex": ..., "$options": "i"}}` is fully BSON-serializable and consistent with the rest of the codebase.

### Tests Updated
- `tests/test_sb_string_not_bson.py`: Replaced 8 bug-documenting tests (asserting `re.Pattern`) with 8 correctness tests (asserting dict form + JSON serializability)
- `tests/test_search_builder.py`: Updated 4 negation test methods (`test_not_equals`, `test_not_contains`, `test_not_starts`, `test_not_ends`) to assert dict equality; removed unused `import re`

### Test Results
- 603 passed (unchanged count — replaced tests, not added), 0 regressions
- Flask-demo integration validated: `test_searchbuilder_simple.py` and `test_searchbuilder_focused.py` both pass

### Backward Compatibility
Fully backward compatible. The MongoDB query semantics are identical — `{"$not": {"$regex": ...}}` produces the same results as `{"$not": re.compile(...)}` in PyMongo, but is now serializable.

---

## v1.28.2 — 2026-03-15 — BUG FIX: ColumnControl date search with ISO datetime strings

**Type:** Bug Fix  
**Iteration:** 1 of 10

### Problem
`_build_column_control_condition` in `query_builder.py` called `DateHandler.parse_iso_date(value)` for `stype == "date"` conditions. `parse_iso_date` only accepts `YYYY-MM-DD` format. The DataTables ColumnControl extension sends full ISO datetime strings (e.g. `2024-01-15T00:00:00.000Z`) as the search value. When such a value was received, `parse_iso_date` raised `FieldMappingError`, caught by the outer `except Exception: pass`, silently producing no filter condition — the column filter appeared to work in the UI but had no effect on results.

### Fix
One-character change: `DateHandler.parse_iso_date(value)` → `DateHandler.parse_iso_date(value.split('T')[0])`

`value.split('T')[0]` extracts the date portion from any ISO string:
- `"2024-01-15"` → `"2024-01-15"` (unchanged)
- `"2024-01-15T00:00:00.000Z"` → `"2024-01-15"` (date extracted)

### Tests Added
3 new tests in `tests/test_column_control.py` (`TestColumnControlDate`):
- `test_equal_iso_datetime_string` — ISO datetime value with `equal` logic produces day-range condition
- `test_greater_iso_datetime_string` — ISO datetime value with `greater` logic produces `$gt` condition  
- `test_less_iso_datetime_string` — ISO datetime value with `less` logic produces `$lt` condition

### Test Results
- 3 new tests added, all passing
- Full suite: 596 passed (was 593), 0 regressions

### Backward Compatibility
Fully backward compatible. `YYYY-MM-DD` values are unchanged (split on `T` returns the original string as `[0]`). Only ISO datetime strings (previously silently broken) now work correctly.

---

## v1.28.1 — 2026-03-15 — BUG FIX: BSON-serializable `$not` in `_build_column_control_condition`

**Type:** Quality / Bug Fix
**Iteration:** 4 of 10

### What changed

Fixed `_build_column_control_condition` in `query_builder.py`: the `notContains` and `notEqual` string logic branches used `re.compile()` objects as the value of `$not`, making the resulting query dicts non-BSON/JSON-serializable.

**Before:**
```python
elif logic == "notContains":
    conditions.append({db_field: {"$not": re.compile(escaped, re.IGNORECASE)}})
elif logic == "notEqual":
    conditions.append({db_field: {"$not": re.compile(f"^{escaped}$", re.IGNORECASE)}})
```

**After:**
```python
elif logic == "notContains":
    conditions.append({db_field: {"$not": {"$regex": escaped, "$options": "i"}}})
elif logic == "notEqual":
    conditions.append({db_field: {"$not": {"$regex": f"^{escaped}$", "$options": "i"}}})
```

This is the same class of bug fixed in `_sb_string` in v1.18.1. `re.compile()` objects cannot be serialized to BSON, causing failures when these conditions appear in aggregation pipelines (e.g. SearchPanes, count_filtered). The pure-dict `{"$not": {"$regex": ..., "$options": "i"}}` form is fully BSON-serializable and consistent with the rest of the codebase.

### Test results
- 10 new tests in `tests/test_column_control_not_bson.py`
- Updated 2 existing tests in `tests/test_column_control.py` (were asserting old buggy behavior)
- Full suite: 593 passed (was 583), 0 regressions

### Why this fix
- `re.compile()` in `$not` is not BSON-serializable — fails silently or raises in aggregation pipelines
- Consistent with the identical fix applied to `_sb_string` in v1.18.1
- Pure-dict form works correctly in both `find()` and `aggregate()` contexts

---

## v1.27.5 — 2026-03-15 — BUG FIX: Multi-colon search terms + html-num SearchBuilder types

**Type:** Bug Fix (2 fixes)
**Iteration:** 2 of 10

### What changed

**Fix 1 — `search_terms_with_a_colon` (datatables.py)**
- Changed filter from `term.count(":") == 1` to `":" in term`
- Fixes silent data loss: terms like `url:https://example.com` (3 colons) were excluded from both `search_terms_with_a_colon` (count != 1) and `search_terms_without_a_colon` (contains `:`) — silently dropped with zero results
- `build_column_specific_search` already uses `split(":", 1)` so multi-colon values parse correctly as `field=url`, `value=https://example.com`

**Fix 2 — `_sb_criterion` html-num/html-num-fmt dispatch (datatables.py)**
- Added `"html-num"` and `"html-num-fmt"` to the numeric type dispatch in `_sb_criterion`
- These are valid DataTables SearchBuilder column types per the spec; previously they fell through to `_sb_string`, producing `$regex` conditions instead of numeric comparisons

### Test results
- 11 new tests in `tests/test_multi_colon_search.py`
- Full suite: 575 passed (was 564), 0 regressions

### Why these fixes
- Multi-colon silent drop: real user-facing data loss for URL fields, email fields, or any colon-containing value in field:value syntax
- html-num dispatch: spec compliance — numeric HTML columns sent wrong query type to MongoDB

---

## v1.27.0 — 2026-03-15 — Feature: columns[i][orderData] support

**Type:** Feature
**Iteration:** 6 of 10

### What changed
- `get_sort_specification()` in `datatables.py`: added `orderData` handling after column resolution
- When `column.get("orderData")` is present, normalize to list of ints and expand sort to those target columns (same direction)
- Falls back to original behavior when `orderData` is absent
- 8 new tests in `tests/test_order_data.py`

### Test results
- New tests: 8/8 passed
- Full suite: 526 passed (was 518), 0 regressions

### Why this feature
- DataTables `orderData` is a standard DT config option that was completely unhandled server-side
- Enables display columns (e.g. formatted names) to sort by underlying data columns (e.g. last_name)
- Complements existing ColReorder + multi-column sort support
- Zero impact on existing behavior (purely additive)

---

## Iteration 7 (v1.22.1) — 2026-03-14 — BUG FIX: Invalid Number Search Fallback

**Change:** Fixed `_build_number_condition` silently returning a `$regex` condition when number conversion fails.

**Problem:** When a `number`-typed field received an invalid search value (e.g. `price:abc` or `price:>notanumber`), `_build_number_condition` caught the `FieldMappingError` and returned `{field: {"$regex": ..., "$options": "i"}}`. This is semantically wrong — MongoDB regex matching never matches numeric BSON values, so the condition always returns zero results while silently appearing to work. The correct behavior is to return `None` (skip the condition), consistent with how `build_column_search` already handles invalid numbers.

**Fix:**
- Changed `except Exception: return {field: {"$regex": ...}}` to `except Exception: return None` in `_build_number_condition`
- One-line change in `query_builder.py`

**Tests:** `tests/test_invalid_number_search.py` — 7 new tests covering:
- Invalid colon-syntax value returns empty filter
- Invalid value produces no `$regex` on number field
- Invalid value with operator returns empty filter
- Valid number colon search still works
- Valid operator colon search still works
- Invalid column search returns empty filter
- Invalid column search produces no `$regex` on number field

**Suite:** 491 tests passing (was 484 before this iteration's new tests)

---

## Iteration 6 (v1.22.0) — 2026-03-14 — FEATURE: searchFixed Support



**Change:** Extracted inline `draw` validation from `get_rows()` into a dedicated `draw` property.

**Problem:** The inline ternary called `request_args.get("draw")` twice, used `.lstrip("-").isdigit()` which accepted negative values (e.g. `-5` passed through as `-5`), and was inconsistent with the clean try/except pattern used by `start` and `limit`.

**Fix:**
- Added `draw` property using `max(1, int(...))` with try/except — consistent with `start`/`limit`
- `get_rows()` now uses `self.draw` (single reference)
- Negative and zero draw values are clamped to 1 (DataTables protocol requires positive echo)

**Tests:** `tests/test_draw_property.py` — 9 new tests (negative clamp, zero clamp, non-numeric, None, float string, missing key, large value)
**Suite:** 476 tests passing (was 467 before this iteration's new tests)

---

## Iteration 4 (v1.21.1) — 2026-03-14 — QUALITY: Input Validation

**Change:** Added defensive input validation for `start`, `limit`, and `draw` request parameters.

**Problem:** `int(request_args.get(...))` raised unhandled `ValueError` on non-numeric input (e.g. malformed/adversarial requests), crashing the endpoint. Negative `start` values would also cause a MongoDB error.

**Fix:**
- `start` property: try/except with `max(0, ...)` clamp — invalid/negative input returns 0
- `limit` property: try/except — invalid input returns 10 (default page size)
- `draw` in `get_rows`: inline isdigit guard — invalid input returns 1

**Tests:** Added `tests/test_input_validation.py` with 13 tests covering valid, invalid string, negative, None, and missing values for all three parameters.

**Result:** 467 tests passing (0 failures). Backward compatible — valid inputs unchanged.

---

## Iteration 3 (v1.20.2 → v1.21.0) — 2026-03-14
**Type:** Feature
**Focus:** Custom `row_id` field for `DT_RowId`

### Problem
`_process_cursor` always used MongoDB `_id` as `DT_RowId`. Users with natural keys (e.g., `employee_id`, `sku`, `order_number`) could not use those fields as the DataTables row identifier, breaking row selection and Editor integration for collections where `_id` is not the meaningful key.

### Solution
Added `row_id: Optional[str] = None` parameter to `DataTables.__init__()`. When set:
- The specified field's value is used as `DT_RowId` in each result row
- The field is NOT removed from the row data (unlike `_id` which is popped in default mode)
- The field is always included in the MongoDB projection even if absent from the `columns` list
- Default behavior (`row_id=None`) is unchanged: `_id` is popped and used as `DT_RowId`

### Changes
- `mongo_datatables/datatables.py`: `row_id` param in `__init__`, updated `_process_cursor`, updated `projection`
- `tests/test_row_id.py`: 8 new tests
- `setup.py`: version bump 1.20.2 → 1.21.0

### Test Results
- 8 new tests in `tests/test_row_id.py`
- Full suite: 454 passed, 59 subtests — zero failures

### Backward Compatibility
Fully backward compatible. `row_id` defaults to `None`, preserving existing `_id`-based behavior.

---

## Iteration 10 — v1.20.1 → v1.20.2 (Quality)
**Date:** 2026-03-14
**Type:** Quality / Dead Code Removal
**Change:** Removed dead `startRender`/`endRender` parsing from `_parse_rowgroup_config()` (datatables.py)
**Problem:** `_parse_rowgroup_config()` parsed `startRender` and `endRender` from the request into the config dict, but `_get_rowgroup_data()` — the only caller — only ever reads `dataSrc`. The parsed values were built and immediately discarded. These are client-side DataTables rendering callbacks; the server has no use for them.
**Fix:** Collapsed `_parse_rowgroup_config` from 20 lines to 6: reads `dataSrc` directly, returns `{"dataSrc": value}` or `None`. Removed the dead `startRender`/`endRender` branches entirely.
**Tests:** 428 → 429 (+1 test: `test_rowgroup_config_no_datasrc_returns_none`). Updated `test_rowgroup_config_parsing` to assert the keys are absent (correct behavior).
**Risk:** Minimal — identical behavior on all real code paths. `startRender`/`endRender` were never used server-side.

---


**Date:** 2026-03-14
**Type:** Quality / Bug Fix
**Change:** Fixed dead `except PyMongoError` block in `count_filtered()` (datatables.py)
**Problem:** The outer `except PyMongoError` was unreachable dead code — the inner `except Exception` caught all exceptions first, silently preventing the double-failure fallback (returning 0) from ever executing. If both `aggregate()` and `count_documents()` failed, the exception would propagate uncaught.
**Fix:** Collapsed nested try/except into a flat 3-level fallback: aggregate → count_documents → return 0. Eliminated the dead outer handler.
**Tests:** 420 → 421 (+1 test: `test_count_filtered_both_aggregate_and_count_documents_fail`)
**Risk:** Minimal — identical behavior on happy path, correct behavior on double-failure path.

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

---

## Iteration 9 (v1.19.2 → v1.19.3) — 2026-03-14
**Quality:** Replaced 4 near-identical extension config parser methods with a single generic helper
- Removed `_parse_fixed_columns_config`, `_parse_responsive_config`, `_parse_buttons_config`, `_parse_select_config` (~100 lines of boilerplate)
- Added `_parse_extension_config(key)`: returns `request_args[key]` if dict, `{}` if True, None otherwise
- Replaced 16-line block in `get_rows()` with a 3-line loop over extension keys
- Updated 3 test files to match new pass-through semantics
- Net: -102 lines. 420 passed, 59 subtests. No regressions.

## Iteration 10 (v1.19.3 → v1.20.0) — 2026-03-14
**Type:** Feature / Bug Fix
**Focus:** SearchBuilder date `<=` and `>=` operator support

### Problem
`_sb_date` was missing `<=` and `>=` conditions that `_sb_number` already supported.
DataTables SearchBuilder sends these operators for date comparisons (e.g. "on or before",
"on or after"). Without them, the conditions silently returned `{}` (no filter applied),
causing incorrect results when users selected these operators in the SearchBuilder UI.

### Solution
Added two conditions to `_sb_date`:
- `<=`: returns `{"$lt": parse(v0) + timedelta(days=1)}` — includes all of the given day
- `>=`: returns `{"$gte": parse(v0)}` — includes from start of the given day

This is consistent with the existing `=` (day-inclusive range) and `between` semantics.

### Changes
- `mongo_datatables/datatables.py`: 2 lines added to `_sb_date`
- `tests/test_sb_date_operators.py`: 7 new tests
- `setup.py`: version bump 1.19.3 → 1.20.0

### Test Results
- New tests: 7/7 passed
- Full suite: 428 passed — zero failures

### Backward Compatibility
Fully backward compatible. Existing `<`, `>`, `=`, `!=`, `between`, `!between` conditions
are unchanged. The new `<=` and `>=` conditions previously returned `{}` (no-op), so
adding them can only make filtering more correct, never less.

---

## Iteration 10 (Quality Pass) — v1.20.2 (no version bump, non-functional)

### Changes
1. **Removed 6 unused imports** from `mongo_datatables/datatables.py`:
   - `import json` — no `json.` calls in the file
   - `datetime` from `from datetime import datetime, timedelta` → `from datetime import timedelta`
   - `Tuple`, `Set` from typing import → `from typing import Dict, List, Any, Optional`
   - Entire `from mongo_datatables.exceptions import DatabaseOperationError, QueryBuildError` line (neither used in datatables.py)

2. **Updated CHANGELOG.md** to cover all versions v1.14.0–v1.20.2 (was missing 7+ versions: v1.17.4 through v1.20.2)

### Test Results
- No new tests (non-functional changes)
- Full suite: 429 passed, 59 subtests — zero failures

## Iteration 17 — v1.20.1 (2026-03-14)

**Type:** Bug Fix  
**Feature:** `columns[i][searchable]` string coercion

### Problem
DataTables sends `columns[i][searchable]` as the string `"true"` or `"false"` from HTTP form data. Three locations used `column.get("searchable", False)` which treats the non-empty string `"false"` as truthy — causing non-searchable columns to be included in global search, column search, and SearchPanes options generation.

This is the same class of bug fixed in Iterations 1, 6, and 7 for the `regex` flag.

### Fix
Replaced all three occurrences with the membership-test pattern:
```python
column.get("searchable") in (True, "true", "True", 1)
```

### Files Changed
- `mongo_datatables/datatables.py`: `searchable_columns` property (line ~211), `get_searchpanes_options` guard (line ~313)
- `mongo_datatables/query_builder.py`: `build_column_search` guard (line ~63)
- `tests/test_searchable_coercion.py`: 12 new tests covering all truthy/falsy variants

### Test Results
- 441 tests passing (12 new tests added)
- 0 regressions

## Iteration 2 (Enhancement) — 2026-03-14
**Type:** Bug fix / coercion consistency  
**Version:** 1.20.1 → 1.20.2  
**Change:** Fixed `orderable` string/bool coercion in `get_sort_specification()`  
**Details:** `column.get("orderable", "true") != "false"` failed when `orderable` was the Python boolean `False` (since `False != "false"` evaluates to `True`). Changed to `column.get("orderable") not in (False, "false", "False", 0)`, consistent with the `searchable` fix from Iter 17.  
**Tests added:** `tests/test_orderable_coercion.py` (5 tests)  
**Test results:** 446 passed, 59 subtests passed  

## Iteration 6 — v1.22.0 (2026-03-14)

**Type:** Feature
**Focus:** searchFixed named searches (DataTables 2.0+)

### What was implemented
- `_parse_search_fixed()` method in `DataTables` class
- Parses `searchFixed` dict from request args (DataTables 2.0 sends `searchFixed[name]=value`)
- Each named search value is treated as a global search term across all searchable columns
- Multiple named searches are ANDed together
- Integrated into `_build_filter()` alongside SearchBuilder, SearchPanes, and global search
- 8 new tests in `tests/test_search_fixed.py`

### Test results
- New tests: 8 passed
- Full suite: 484 passed, 59 subtests (0 failures)

### Backward compatibility
- Fully backward compatible: `searchFixed` key is optional; absent or empty dict is a no-op

## Iteration 24 (v1.22.1 → v1.23.0) — Per-Column searchFixed Support

**Type:** Feature
**Date:** 2026-03-14

### Problem
DataTables 2.0+ supports `columns[i][searchFixed]` — a dict of named, persistent fixed searches scoped to a specific column. The existing `_parse_search_fixed()` only handled the top-level `request_args["searchFixed"]` (global fixed searches). Per-column fixed searches were silently ignored.

### Solution
Added `_parse_column_search_fixed()` method that iterates `self.columns`, reads each column's `searchFixed` dict, and for each non-empty value builds a column-scoped condition via `query_builder.build_column_search()`. Wired into `_build_filter()` alongside the existing global searchFixed handling.

### Changes
- `mongo_datatables/datatables.py`: Added `_parse_column_search_fixed()` method; wired into `_build_filter()`
- `tests/test_column_search_fixed.py`: 8 new tests covering all cases

### Tests
- 8 new tests added, all passing
- Full suite: 499 passed, 0 failures

---

## Iteration 25 (v1.23.0 → v1.23.1) — 2026-03-15

### Type: Quality / Bug Fix

### Problem
`_sb_string()` negative conditions (`!=`, `!contains`, `!starts`, `!ends`) produced invalid BSON:
```python
# Invalid — $not does not accept $options as a sibling key
{field: {"$not": {"$regex": "^foo$", "$options": "i"}}}
```
MongoDB requires the value of `$not` to be a compiled regex (BSON Regex), not a plain dict. Executing this query against MongoDB raises a server error.

The existing test `test_sb_string_not_bson.py` was written to verify JSON serializability but inadvertently asserted the *wrong* structure (checking for `$options` key in the plain dict), masking the bug.

### Fix
- `mongo_datatables/datatables.py` — `_sb_string()`: replaced all four `$not` plain-dict patterns with `re.compile(..., re.IGNORECASE)`, which PyMongo serializes as a valid BSON regex.
- `tests/test_sb_string_not_bson.py`: updated to assert `$not` value is a `re.Pattern` (not a plain dict) and verify `.pattern` and `re.IGNORECASE` flag.
- `tests/test_search_builder.py`: updated 4 stale assertions (`test_not_equals`, `test_not_contains`, `test_not_starts`, `test_not_ends`) that were checking the old invalid structure.

### Result
- 499 passed, 59 subtests passed (0 regressions)
- All negative SearchBuilder string conditions now produce valid BSON
- Backward compatible: query semantics unchanged, only serialization format corrected

## v1.23.2 — SearchBuilder `!between` fix for number and date types

**Date:** 2026-03-15  
**Type:** Bug Fix  
**Iteration:** 2 of 10

### Problem
`_sb_number` and `_sb_date` used `{field: {"$not": {"$gte": ..., "$lte": ...}}}` for the `!between` condition. MongoDB's `$not` operator cannot wrap a compound range expression — this raised a server error at query time. The same class of bug was fixed for `_sb_string` in v1.23.1 but the number and date handlers were missed.

### Fix
Replaced invalid `$not` compound with correct `$or` exclusion pattern in both methods:
- `_sb_number !between`: `{"$or": [{field: {"$lt": v0}}, {field: {"$gt": v1}}]}`
- `_sb_date !between`: `{"$or": [{field: {"$lt": v0}}, {field: {"$gt": v1}}]}`

### Files Changed
- `mongo_datatables/datatables.py`: 2 lines changed (`_sb_number` line 557, `_sb_date` line 578)
- `tests/test_search_builder.py`: `test_not_between` updated + `test_not_between_date` added
- `setup.py`: version 1.23.1 → 1.23.2

### Test Results
500 tests passing (499 → 500), 0 failures
---

## v1.25.0 — 2026-03-15

**Type:** Code Quality / Refactor  
**Iteration:** 1 of 10 (Balanced Development Workflow)

### Change: Extract `_build_pipeline()` shared helper

**Problem:** `results()` and `get_export_data()` duplicated identical pipeline construction logic (match → sort → project), differing only in the presence of `$skip`/`$limit` stages. This DRY violation meant any future pipeline change required two edits.

**Solution:**
- Added `_build_pipeline(paginate: bool = True)` private method that builds the aggregation pipeline once
- `results()` calls `_build_pipeline(paginate=True)` — includes `$skip`/`$limit`
- `get_export_data()` calls `_build_pipeline(paginate=False)` — omits pagination stages
- Changed `DataField.VALID_TYPES` from `list` to `frozenset` for O(1) membership testing; updated error message to use `sorted()` for deterministic output

**Tests:** Added `tests/test_build_pipeline.py` with 10 tests covering pipeline structure and consistency between paginated/export paths.

**Results:** 510 tests passing (10 new). Zero regressions. Backward compatible.

## Iteration 26 (v1.25.0 → v1.26.0) - 2026-03-15
- **Type**: Feature (DataTables 2.x protocol)
- **Change**: `search.return` optimization — when `search[return]=false`, `get_rows()` returns `recordsFiltered=-1` instead of running the count aggregation. Saves a MongoDB round-trip on large collections where the client doesn't need the filtered count.
- **Tests**: 7 new tests in `tests/test_search_return.py` (517 total)
- **Status**: ✅ COMPLETED

## Iteration 4 (v1.26.0 → v1.26.1) — 2026-03-15 — QUALITY: Stale Mock Fix + Operator Parsing Cleanup

**Type:** Quality / Bug Fix  
**Iteration:** 4 of 10 (Balanced Development Workflow)

### Changes

**1. Fixed stale `$facet` mock in `tests/test_searchpanes.py` (failing test)**

`test_searchpanes_options_generation` used a flat `aggregate.return_value` list:
```python
[{"_id": "Active", "count": 5}, {"_id": "Inactive", "count": 3}]
```
But `get_searchpanes_options` calls `aggregate` twice and expects `$facet`-shaped output — a single document keyed by column name. The mock was stale relative to the `$facet` refactor in v1.18.0. Fixed to use `side_effect` with the correct shape:
```python
facet_doc = {"name": [], "age": [], "status": [{"_id": "Active", "count": 5}, ...]}
self.collection.aggregate.side_effect = [[facet_doc], [facet_doc]]
```

**2. Added `IndexError` guard in `get_searchpanes_options` (`datatables.py`)**

The two `list(aggregate(...))[0]` calls would raise `IndexError` on empty collections. Changed to safe pattern:
```python
docs = list(self.collection.aggregate(...))
result = docs[0] if docs else {}
```

**3. Simplified operator prefix parsing in `query_builder.py`**

Reordered `>=`/`<=` checks before `>`/`<` — eliminates fragile negative guards (`and not value.startswith(">=")`). Behavior identical, logic cleaner:
```python
# Before (fragile):
if value.startswith(">") and not value.startswith(">="):
# After (clean):
if value.startswith(">="):
    ...
elif value.startswith(">"):
```

### Test Results
- 518 passed, 59 subtests passed (0 failures) — was 517 with 1 failure before this iteration
- Fixed the pre-existing failing test `test_searchpanes_options_generation`

### Backward Compatibility
Fully backward compatible. All changes are either test fixes or defensive guards with identical happy-path behavior.

## v1.26.2 — 2026-03-15

**Type:** Quality / Code Clarity  
**Iteration:** 5 of 10 (Balanced Development Workflow)

### Change: Remove redundant `or {}` guards in `count_total()`

**Problem:** `count_total()` used `self.custom_filter or {}` in two places. `self.custom_filter` is always a dict (assigned from `**custom_filter` kwargs in `__init__`), so the `or {}` guard is dead code that misleads readers into thinking `custom_filter` could be `None` or falsy. It was also inconsistent with the rest of the file (`_build_filter` uses `if self.custom_filter:` directly, `count_filtered` uses `self.filter` directly).

**Fix:** Replaced `self.custom_filter or {}` with `self.custom_filter` in both occurrences in `count_total()`.

**Tests:** No new tests needed — behavior is identical. All 518 existing tests pass.

**Backward compatible:** Yes — `count_documents({})` and `count_documents(self.custom_filter)` when `custom_filter == {}` are identical.

## Iteration 7 (Session 2) — v1.27.0 → v1.27.1 — Date Range Upper Bound Bug Fix

**Date:** 2026-03-15
**Type:** Bug Fix
**Branch:** fix/date-range-upper-bound-v1.27.1

### Problem
In `query_builder.py` `build_column_search()`, the date range upper bound was stored under the wrong MongoDB operator key. When a user searched with a pipe-delimited date range (e.g. `2024-01-01|2024-12-31`), the upper bound condition was built as:
```python
{"date_field": {"$gte": datetime(2024,1,1), "$lte": datetime(2025,1,1)}}
```
Using `$lte` with the next day's midnight (`datetime(2025,1,1,0,0,0)`) incorrectly includes documents timestamped at exactly midnight on January 1, 2025 — one instant beyond the intended range end.

### Root Cause
`DateHandler.get_date_range_for_comparison(date_str, '<=')` returns `{"$lt": next_day}` (exclusive upper bound). The code correctly retrieved `.get('$lt')` to get the next-day datetime value, but stored it under the key `'$lte'` instead of `'$lt'`.

### Fix
One-character key change in `query_builder.py`:
```python
# Before (buggy)
range_cond['$lte'] = date_range.get('$lt')
# After (correct)
range_cond['$lt'] = date_range.get('$lt')
```
This matches the behavior of `_build_date_condition` and `_sb_date` for the `<=` case, and is consistent with how `DateHandler` works throughout the codebase.

### Tests Updated
- `tests/test_range_filter.py`: Updated `test_both_bounds` and `test_upper_bound_only` assertions from `$lte` → `$lt` key.

### Result
- 526/526 tests pass
- No API changes — behavior change is a correctness fix (documents at exact midnight boundary no longer incorrectly included)

## v1.27.2 — 2026-03-15

**Type:** Quality / Performance
**Iteration:** 9 of 10 (Balanced Development Workflow)

### Change: Pre-compute field mapper lookups in `build_global_search`

**Problem:** In `query_builder.py`, `build_global_search()` called `self.field_mapper.get_field_type(column)` and `self.field_mapper.get_db_field(column)` inside the inner `for term in search_terms` loop. For a search with N terms and M searchable columns, this caused O(N×M) field mapper lookups where O(M) is sufficient — the field type and db field name for a column do not change between terms.

**Fix:** Extracted a `col_meta` list (built once before the term loop) containing `(db_field, field_type)` tuples for all non-date columns. The inner loop now iterates over `col_meta` directly.

```python
# Before: O(N×M) lookups
for term in search_terms:
    for column in searchable_columns:
        field_type = self.field_mapper.get_field_type(column)  # repeated N times
        ...
        or_conditions.append({self.field_mapper.get_db_field(column): ...})  # repeated N times

# After: O(M) lookups
col_meta = []
for c in searchable_columns:
    ft = self.field_mapper.get_field_type(c)   # called once per column
    if ft != "date":
        col_meta.append((self.field_mapper.get_db_field(c), ft))  # called once per column

for term in search_terms:
    for db_field, field_type in col_meta:  # no lookups here
        ...
```

**Tests added:** `tests/test_global_search_perf.py` (4 tests)
- `test_field_mapper_called_once_per_column_not_per_term` — verifies call counts via mock
- `test_global_search_multi_term_produces_correct_or_conditions` — 2 terms × 2 columns = 4 $or conditions
- `test_global_search_quoted_phrase_word_boundary` — quoted phrase uses \\b word-boundary regex
- `test_global_search_skips_date_columns` — date-typed columns excluded from results

**Result:** 559/559 tests pass. No API changes. Backward compatible.
---

## Iteration 10 (Quality) — v1.27.3
**Date**: 2026-03-15
**Type**: Quality / Performance
**Focus**: Remove dead computation from `_get_rowgroup_data`

### Problem
`_get_rowgroup_data` in `datatables.py` computed `$sum` and `$avg` MongoDB accumulators for every numeric `DataField` on every RowGroup aggregation query. These values were copied into `group_data` but never consumed by any caller — the DataTables RowGroup extension only uses `count`. This was confirmed by:
- No caller reading `_sum`/`_avg` keys from the `rowGroup` response
- The existing test `test_rowgroup_data_generation` not asserting on those keys
- Coverage showing the numeric-summary passthrough path was effectively untested

### Solution
- Removed the `for field in self.data_fields` loop that added `$sum`/`$avg` accumulators to the `$group` stage
- Simplified the result-processing loop to a single dict comprehension: `{str(g['_id']) if g['_id'] is not None else 'null': {'count': g['count']} for g in groups}`
- Net reduction: ~10 lines of dead code removed from `datatables.py`

### Tests
- Updated `test_rowgroup_data_generation` mock to return only `_id` + `count` (no `value_sum`/`value_avg`)
- Added `test_rowgroup_no_numeric_summaries`: asserts no key in any group dict ends with `_sum` or `_avg`
- **Test count**: 559 → 560 (+1)

### Validation
- All 560 tests pass (0.72s)
- Flask demo imports OK
- Backward compatible: `rowGroup` response shape unchanged (only `dataSrc` + `groups` with `count` per group — same as before)

### Files Changed
- `mongo_datatables/datatables.py`: simplified `_get_rowgroup_data`
- `tests/test_rowgroup.py`: updated mock + added 1 test
- `mongo_datatables/__init__.py`, `setup.py`: version 1.27.2 → 1.27.3

## Iteration 1 (Session 2) — v1.27.4 — Error Response Protocol Compliance (2026-03-15)

**Type:** Feature / Protocol Compliance
**Focus:** DataTables `error` field in `get_rows()` response

### Problem
The DataTables server-side processing protocol specifies that the server response MAY include an `error` string key to signal failures to the client UI. mongo-datatables had no such handling — unhandled exceptions in `get_rows()` would propagate to the caller (crashing the endpoint), and `_check_text_index()` would raise `PyMongoError` on connection failure during construction.

### Solution
- Wrapped `get_rows()` body in `try/except Exception`, returning `{"draw": self.draw, "error": str(e), "recordsTotal": 0, "recordsFiltered": 0, "data": []}` on failure
- Guarded `_check_text_index()` `list_indexes()` call with `try/except PyMongoError`, falling back to `_has_text_index = False`
- Added 4 new tests covering both error paths and the success (no-error-key) path

### Changes
- `mongo_datatables/datatables.py`: `get_rows()` try/except wrapper, `_check_text_index()` PyMongoError guard
- `tests/test_datatables_error_handling.py`: 4 new test methods

### Results
- 564 tests pass (was 560)
- flask-demo: 1 passed
- django-demo: 3 passed
- Version: 1.27.3 → 1.27.4

## Iteration 26 — v1.28.0 (2026-03-15)

**Type:** Feature
**Focus:** `search[smart]` AND semantics for multi-word global search

### Problem
DataTables sends `search[smart]=true` by default. When a user types multiple words (e.g. "John Smith"), the expected behavior is that EACH word must appear somewhere in the matching row (AND semantics). The previous implementation used a flat `$or` across all terms × columns, meaning ANY word matching ANY column returned the row — semantically incorrect for the default DataTables configuration.

### Solution
Added `search_smart` parameter to `build_global_search()` in `query_builder.py`. When `search_smart=True` and there are multiple search terms, the method builds `$and` of per-term `$or`s instead of a flat `$or`. The `global_search_condition` property in `datatables.py` now reads `search[smart]` from the request (defaulting to `True`) and passes it through.

### Changes
- `mongo_datatables/query_builder.py`: Added `search_smart=True` param; multi-term path now builds `{"$and": [{"$or": [...]}, {"$or": [...]}]}` when smart=True
- `mongo_datatables/datatables.py`: `global_search_condition` reads `search.get("smart", True)` with string coercion and passes to `build_global_search`
- `tests/test_smart_search.py`: 8 new tests covering AND/OR semantics, string coercion, default behavior, empty search
- Updated 2 existing tests that were asserting old flat-$or behavior for multi-term searches

### Results
- 583 tests passing (8 new tests added)
- Backward compatible: single-term, quoted-phrase, and text-index paths unchanged
- `smart=false` preserves legacy flat-$or behavior

---

## v1.29.0 — Enhancement: Editor multi-pymongo-type support

**Date:** 2026-03-15
**Type:** Enhancement / Bug Fix
**Iteration:** 2 of 10

### Problem
Editor.db property hardcoded `self.mongo.db` (Flask-PyMongo only). Passing a plain `pymongo.MongoClient` or `pymongo.database.Database` caused `AttributeError`. DataTables already handled all types via `_get_collection()` but Editor did not.

### Solution
- Added `_resolve_collection(pymongo_object, collection_name)` static method to Editor, mirroring DataTables._get_collection logic
- Collection resolved at `__init__` time and stored as `self._collection`
- `collection` property returns `self._collection`
- `db` property updated to resolve for all types (backward compatible)
- Supports: Flask-PyMongo (`obj.db`), MongoClient (`obj.get_database()`), raw Database (`isinstance`), dict-style fallback

### Tests
- Added `tests/test_editor_pymongo_types.py` with 7 new tests
- Full suite: 603 passed (was 596)

### Compatibility
- Fully backward compatible — Flask-PyMongo usage unchanged
- No API changes

## v1.30.0 — Code Quality: Structural Fixes & Date Semantics (2026-03-15)

### Changes
1. **Fix: `build_column_search` block nesting** (`query_builder.py`)
   - The `if search_value and searchable` and `if has_cc` blocks were siblings of the outer `if (search_value and searchable) or has_cc` block, creating a fragile dependency where `db_field`/`field_type` could theoretically be unbound
   - Both inner blocks are now properly nested inside the outer `if`, making the control flow explicit and safe

2. **Fix: `_hashable` closure moved outside loop** (`datatables.py`)
   - `_hashable` was redefined on every iteration of the `for col_name, _ in eligible` loop in `get_searchpanes_options`
   - Moved to just before the loop — defined once, reused across all iterations

3. **Fix: `_sb_date` between/!between day-inclusive semantics** (`datatables.py`)
   - `between` used `$lte: _d(v1)` which excluded the rest of v1's day (matched only midnight)
   - Now uses `$lt: _d(v1) + timedelta(days=1)` — consistent with how `<=` is handled
   - `!between` complement updated to `$gte: _d(v1) + timedelta(days=1)` for correctness

### Tests
- Added `tests/test_regression_v1_30_0.py` with 9 targeted regression tests
- **640 passed** (was 631), 59 subtests passed

## v1.30.2 → v1.31.0 (Iteration 8, Session 2, 2026-03-15)

**Type:** Feature
**Focus:** `search[caseInsensitive]` support

### Problem
All regex searches unconditionally used `"$options": "i"` (case-insensitive). There was no way
to opt into case-sensitive search, even though DataTables exposes `search.caseInsensitive` and
DataTables defaults to case-insensitive (so the default behavior was correct, but the flag was
silently ignored).

### Solution
- Added `case_insensitive: bool = True` parameter to `build_global_search`, `build_column_search`,
  and `build_column_specific_search` in `query_builder.py`.
- All regex `$options` now use `"i" if case_insensitive else ""`.
- `build_column_search` also reads per-column `columns[i][search][caseInsensitive]` to allow
  column-level overrides (coerced from string/bool/int).
- `datatables.py` reads `search[caseInsensitive]` from the request and passes it to all three
  query builder methods.
- Default is `True` — fully backward compatible.

### Tests
14 new tests in `tests/test_case_insensitive.py`. 666 total passing.

## v1.31.2 — DataField empty-name validation

**Date:** 2026-03-15  
**Type:** Quality / Input Validation  
**Tests:** 684 passed (10 new, +10 from 674 baseline)

### Problem
`DataField("", "string")` and `DataField("   ", "string")` silently created fields with empty or whitespace-only names, which would produce broken MongoDB queries (`{"": ...}`) at runtime with no clear error.

### Fix
Added a guard as the first statement in `DataField.__init__`:
```python
if not name or not name.strip():
    raise ValueError("DataField name must be a non-empty string")
```
Also normalized the existing invalid-type error message to use consistent formatting (`Invalid data_type '...'`).

### Tests Added
New file `tests/test_datafield.py` — 10 tests covering:
- Valid construction (name, type, alias defaulting, explicit alias)
- All 8 valid type strings (case-insensitive)
- `ValueError` on invalid type
- `ValueError` on empty name
- `ValueError` on whitespace-only name
- `__repr__` with and without alias

### Files Changed
- `mongo_datatables/datatables.py` — 2-line guard added to `DataField.__init__`
- `tests/test_datafield.py` — new file, 10 tests

---

## v1.32.1 — 2026-03-15

**Type:** Quality / Bug Fix  
**Focus:** NaN/Inf float sanitization inside lists

### Problem
`_format_result_values` correctly converted scalar `NaN`/`Inf` floats to `None`, but skipped the same check for floats inside lists. Any MongoDB document with a list containing a non-finite float caused `json.dumps()` to raise `ValueError`, crashing the response.

### Fix
Added one `elif` clause to the list-item branch in `_format_result_values`:
```python
elif isinstance(item, float) and not math.isfinite(item):
    val[i] = None
```

### Test Changes
- Renamed `test_nan_in_list_unchanged` → `test_nan_in_list_converted_to_none`
- Updated assertions to verify `None` is returned for NaN and Inf list items

### Results
- 706 tests passing (no regressions)
- Backward compatible: `None` is the same sentinel already used for scalar NaN/Inf
- Eliminates asymmetry between scalar and list float handling

## v1.33.0 — Editor action=upload (Gap #4)

**Date:** 2026-03-15
**Type:** EDITOR protocol gap
**Tests:** 716 passing (+10 new in test_editor_upload.py)

### Changes
- `editor.py`: Added `StorageAdapter` base class with `store(field, filename, content_type, data) -> str` and `retrieve(file_id) -> bytes` protocol methods
- `editor.py`: `Editor.__init__` accepts optional `storage_adapter=` kwarg
- `editor.py`: `Editor.upload()` method handles `action=upload`, calls `adapter.store()`, optionally calls `adapter.files_for_field()` for the `files` response dict
- `editor.py`: `process()` dispatches `action=upload` to `upload()`; missing adapter or field returns `{"error": "..."}` gracefully
- `EDITOR_GAPS.md`: item #4 marked ✅ DONE
