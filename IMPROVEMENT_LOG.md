# mongo-datatables Improvement Log

This log tracks iterative improvements made to the mongo-datatables library.

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
