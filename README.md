# mongo-datatables

[![PyPI version](https://badge.fury.io/py/mongo-datatables.svg)](https://badge.fury.io/py/mongo-datatables)
[![Downloads](https://static.pepy.tech/badge/mongo-datatables)](https://pepy.tech/project/mongo-datatables)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![codecov](https://codecov.io/gh/pjosols/mongo-datatables/branch/main/graph/badge.svg)](https://codecov.io/gh/pjosols/mongo-datatables)
[![Tests](https://github.com/pjosols/mongo-datatables/actions/workflows/python-tests.yml/badge.svg)](https://github.com/pjosols/mongo-datatables/actions/workflows/python-tests.yml)
[![Sponsor](https://img.shields.io/badge/sponsor-♥-ea4aaa?logo=github-sponsors)](https://github.com/sponsors/pjosols)

Server-side processing for jQuery DataTables with MongoDB.

Translates DataTables Ajax requests into MongoDB aggregation pipelines, handling pagination, sorting, filtering, search, SearchPanes, SearchBuilder, and Editor (full CRUD).

## Links

- [mongo-datatables.com](https://mongo-datatables.com) — project homepage
- [docs.mongo-datatables.com](https://docs.mongo-datatables.com) — full documentation with configuration details, examples, and API reference
- [Vinyl Archives](https://vinyl-archives.com) — full-featured showcase app with CRUD, cover art, SearchPanes, SearchBuilder, and Editor
- [Flask Demo](https://flask-demo.net) · [Django Demo](https://django-demo.net) · [FastAPI Demo](https://fastapi-demo.net) — minimal starter apps showing framework integration

## Installation

```bash
pip install mongo-datatables
# or
uv add mongo-datatables
```

## Quick Start

```python
from pymongo import MongoClient
from mongo_datatables import DataTables, DataField

db = MongoClient("mongodb://localhost:27017/")["mydb"]

data_fields = [
    DataField('title', 'string'),
    DataField('artist', 'string'),
    DataField('year', 'number'),
    DataField('genre', 'string'),
]

# args is the DataTables Ajax request body (a dict)
result = DataTables(db, 'albums', args, data_fields).get_rows()
```

`db` is any PyMongo `Database` object. `args` is the JSON body from a DataTables Ajax POST.
`get_rows()` returns the standard server-side response:

```json
{
    "draw": 1,
    "recordsTotal": 1000000,
    "recordsFiltered": 4821,
    "data": [...]
}
```

## Framework Examples

### Flask

```python
@app.route('/api/data', methods=['POST'])
def data():
    args = request.get_json()
    dt = DataTables(db, 'albums', args, data_fields)
    return jsonify(dt.get_rows())
```

### FastAPI

```python
@app.post('/api/data')
async def data(request: Request):
    args = await request.json()
    dt = DataTables(db, 'albums', args, data_fields)
    return JSONResponse(dt.get_rows())
```

### Django

```python
class DataView(View):
    def post(self, request):
        args = json.loads(request.body)
        dt = DataTables(db, 'albums', args, data_fields)
        return JsonResponse(dt.get_rows())
```

### Litestar

```python
@post('/api/data')
async def data(request: Request) -> dict:
    args = await request.json()
    return DataTables(db, 'albums', args, data_fields).get_rows()
```

### Quart

```python
@app.route('/api/data', methods=['POST'])
async def data():
    args = await request.get_json()
    dt = DataTables(db, 'albums', args, data_fields)
    return jsonify(dt.get_rows())
```

---

## DataField

`DataField(name, data_type, alias=None)` maps a MongoDB field to a DataTables column.

```python
DataField('title', 'string')               # basic field
DataField('release_date', 'date')          # date comparison
DataField('track_count', 'number')         # numeric comparison
DataField('PublisherInfo.label', 'string', 'label')  # nested + alias
DataField('_id', 'objectid')              # string in response
```

**Valid types:**

| Type | Search behaviour | Uses index? | Operators |
|---|---|---|---|
| `keyword` | Exact equality match | Yes (regular index) | — |
| `string` | Case-insensitive regex (substring) | No | — |
| `number` | Exact equality or numeric comparison | Yes (regular index) | `>` `>=` `<` `<=` `=` |
| `date` | Date comparison (ISO `YYYY-MM-DD`) | Yes (regular index) | `>` `>=` `<` `<=` `=` |
| `array` | Regex against array elements | No | — |
| `objectid` | Serialized as string in response | — | — |
| `boolean`, `object`, `null` | Treated as string (regex) | No | — |

Use `keyword` for categorical/code fields (country codes, status values, tags) where exact matching is always intended and index performance matters. Use `string` for free-text fields where substring and partial matching is useful.

```python
# country:US  →  {"country_code": "US"}  — uses index
DataField('country_code', 'keyword')
# name:york   →  regex, finds "New York", "Yorkshire"
DataField('name',         'string')
# year:>1990  →  {"year": {"$gt": 1990}}  — uses index
DataField('year',         'number')
# released:>=2020-01-01  — uses index
DataField('released',     'date')
# serialized as string in response
DataField('_id',          'objectid')
# nested field with UI alias
DataField('PublisherInfo.label', 'string', 'label')
```

The `alias` is the name DataTables uses for the column (`columns[i][data]`). Defaults to the last segment of the field path (`PublisherInfo.label` → `label`).

---

## Search

Search is where this library earns its keep. The global search box supports several modes:

### Text index search (fast)

When a MongoDB text index exists, global search uses `$text` — fast even on multi-million-row collections:

```python
db.albums.create_index([
    ("title", "text"),
    ("artist", "text"),
    ("genre", "text"),
])
```

Without a text index, the library falls back to per-column regex (much slower on large collections).

### Phrase search

Wrap in quotes for exact phrase matching:

```
"Dark Side of the Moon"
```

### Multi-word AND search

With `search[smart]=true` (DataTables default), each word must match at least one searchable column:

```
pink floyd 1973   →  all three terms must appear across the row
```

### Colon syntax — field-specific search

Target a specific field without needing a separate input:

```
artist:Bowie                →  artist contains "Bowie" (regex)
artist:"David Bowie"        →  exact phrase in artist field
country_code:US             →  equals "US" (keyword, uses index)
year:1972                   →  equals 1972 (number, uses index)
year:>1990                  →  greater than
year:>=1990 year:<2000      →  combine conditions (ANDed)
release_date:>2020-01-01    →  date comparison
```

### Column search with ranges

Per-column search supports pipe-delimited `min|max` for numbers and dates:

```
1990|2000          →  1990 ≤ year ≤ 2000
2020-01-01|2020-12-31
```

### Regex mode

Set `search[regex]=true` to treat the search value as a raw MongoDB regex:

```
^Dark             →  starts with "Dark"
(Floyd|Bowie)     →  matches either
```

### Case sensitivity

Case-insensitive by default. Pass `search[caseInsensitive]=false` for case-sensitive matching.
Per-column override via `columns[i][search][caseInsensitive]`.

---

## SearchPanes

No server-side configuration needed — call `get_searchpanes_options()` to populate panes on page load:

```python
@app.route('/searchpanes', methods=['POST'])
def searchpanes():
    dt = DataTables(db, 'albums', request.get_json(), data_fields)
    return jsonify(dt.get_searchpanes_options())
```

---

## SearchBuilder

Full server-side support with nested AND/OR criteria trees. Works automatically — no extra configuration needed.

---

## Sorting

Multi-column sorting, ColReorder (`order[i][name]` name-based ordering), and `orderData` column redirect are all supported.

---

## Custom Filters

Scope all queries to a subset of the collection by passing extra filter criteria as keyword arguments:

```python
DataTables(
    db, 'albums', args, data_fields,
    status='active', label='Merge Records',
)
```

---

## Editor

Full CRUD support for DataTables Editor.

```python
from mongo_datatables import Editor

@app.route('/editor', methods=['POST'])
def editor():
    data = request.get_json()
    result = Editor(
        db, 'albums', data,
        doc_id=request.args.get('id'),
        data_fields=data_fields,
    ).process()
    return jsonify(result)
```

Editor also handles `action=search` for `autocomplete` and `tags` field types:

```python
@app.route('/editor/search', methods=['POST'])
def editor_search():
    data = request.get_json()
    editor = Editor(db, 'albums', data, data_fields=data_fields)
    return jsonify(editor.search())
```

**Optional Editor parameters:**

| Parameter | Description |
|---|---|
| `validators` | `dict` mapping field names to `callable(value) -> str\|None` |
| `hooks` | `pre_create`, `pre_edit`, `pre_remove` callables — return falsy to cancel |
| `options` | `dict` or zero-arg callable for select/radio/checkbox field options |
| `dependent_handlers` | `dict` mapping field names to callables for dependent field Ajax |
| `file_fields` + `storage_adapter` | file upload support (subclass `StorageAdapter`) |
| `row_class`, `row_data`, `row_attr` | per-row metadata (static value or callable) |

### File Uploads

File uploads are validated for security before storage:

- **Magic bytes**: verified against declared MIME type (JPEG, PNG, GIF, WebP, PDF, plain text, CSV)
- **Filename safety**: rejects path traversal, null bytes, URL-encoded characters, Windows-reserved characters, and blocked executable extensions
- **Size limits**: per-type limits (10 MB images, 25 MB PDF, 5 MB text/CSV) with 50 MB global cap
- **Virus scanning**: optional integration point for antivirus/malware detection

```python
from mongo_datatables.editor.validators.upload_security import validate_upload_data

# Validate before storage
validate_upload_data({
    "filename": "report.pdf",
    "content_type": "application/pdf",
    "data": file_bytes,
}, scanner=optional_scanner_instance)
```

To plug in a virus scanner at the `Editor` level, pass it via `virus_scanner`. The scanner must implement `scan(filename: str, data: bytes) -> bool` — returning `False` rejects the file:

```python
editor = Editor(mongo, "files", request_args, data_fields, virus_scanner=my_scanner)
```

---

## Performance & Indexes

For large collections, indexes are critical — the library uses aggregation pipelines on every request.

### Text index

```python
db.albums.create_index([
    ("title", "text"),
    ("artist", "text"),
    ("genre", "text"),
])
```

With a text index, global search runs in ~100–300ms on multi-million-row collections.
Without one, regex fallback can take 5–10+ seconds.

> MongoDB allows only one text index per collection, but it can cover multiple fields.

To force regex search even when a text index exists (for substring matching):

```python
DataTables(db, 'albums', args, data_fields, use_text_index=False)
```

### Regular indexes

Create indexes for fields used in sorting, column search, or custom filters:

```python
db.albums.create_index("year")
db.albums.create_index("artist")
db.albums.create_index([("artist", 1), ("year", -1)])  # compound
```

---

## Advanced

**`pipeline_stages`** — inject aggregation stages (`$lookup`, `$addFields`, `$unwind`) before the `$match`, useful for computed or joined fields.

**`allow_disk_use=True`** — pass `allowDiskUse` to aggregation pipelines when complex filters exceed MongoDB's 100 MB in-memory limit.

**`get_export_data()`** — returns all matching rows without pagination for CSV/Excel export.

**`row_id`, `row_class`, `row_data`, `row_attr`** — per-row `DT_Row*` metadata, accepts a static value or a callable receiving the raw document.

See the [full documentation](https://docs.mongo-datatables.com) for details.

---

## Development

Run tests:

```bash
uv run pytest tests/
```

Run with coverage:

```bash
uv run pytest --cov=mongo_datatables tests/ \
    --cov-report=term --cov-report=html
```

## License

Released under the MIT License.
