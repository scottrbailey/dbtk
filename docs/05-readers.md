# File Readers

**The problem:** Each file format has its own quirks and APIs. You end up writing different code for CSV vs Excel vs JSON, making your ETL pipelines fragile and hard to maintain.

**The solution:** DBTK provides a single, consistent interface for reading all common file formats. Whether you're reading CSV, Excel, JSON, XML, or fixed-width files, the API is identical. Even better — `get_reader()` automatically detects the format from the file extension.
All "Large" readers will automatically display progress trackers.

For the complete parameter/method reference, see the [API Reference](11-api-reference.md#readers) or the full [Sphinx API docs](https://dbtk.readthedocs.io/en/latest/api.html).

## Quick Navigation

**General**
- [Quick Start](#quick-start)
- [Automatic Format Detection](#automatic-format-detection)
- [Common Reader Parameters](#common-reader-parameters)
- [Common Methods and Properties](#common-methods-and-properties)
- [Encoding](#encoding)
- [Compressed and Archived Files](#compressed-and-archived-files)
- [Filtering Records](#filtering-records)
- [Dual Field Name Access](#dual-field-name-access)

**Most Common Formats**
- [CSV Files](#csv-files)
- [Excel Files](#excel-files)

**Other Formats**
- [JSON Files](#json-files)
- [NDJSON Files](#ndjson-files)
- [XML Files](#xml-files)
- [DataFrame Reader](#dataframe-reader)

**Less Common Formats**
- [Fixed-Width Files](#fixed-width-files)
- [EDI / Multi-Record-Type Fixed-Width Files](#edi--multi-record-type-fixed-width-files)

## Quick Start

```python
from dbtk import readers

# CSV files — use utf-8-sig instead of utf-8 to avoid BOM issues (corrupted column names)
with readers.CSVReader(open('northern_water_tribe_census.csv', encoding='utf-8-sig')) as reader:
    for waterbender in reader:
        print(f"Waterbender: {waterbender.name}, Village: {waterbender.village}")

# Or let DBTK detect the format and handle encoding/compression for you
with readers.get_reader('fire_nation_army.xlsx', sheet_index=1) as reader:
    for soldier in reader:
        print(f"Rank: {soldier.military_rank}, Firebending Level: {soldier.flame_intensity}")
```

Every format below follows the same pattern: open (or point at) a source, iterate, get `Record` objects back.

## Automatic Format Detection

Let DBTK figure out what you're reading:

```python
# Automatically detects format from extension
with readers.get_reader('data.xlsx') as reader:
    for record in reader:
        process(record)
```

`get_reader()` also transparently handles compression and ZIP extraction — see [Compressed and Archived Files](#compressed-and-archived-files) — and accepts format-specific kwargs (`sheet_name`/`sheet_index` for Excel, `delimiter` for CSV, `flatten` for JSON, `zip_member` for ZIP archives, etc.), which it forwards to the underlying reader.

## Common Reader Parameters

Every reader (`CSVReader`, `ExcelReader`, `XLSReader`, `JSONReader`, `NDJSONReader`, `XMLReader`, `FixedReader`, `DataFrameReader`) accepts these on top of its format-specific ones:

| Parameter     | Default | Description                                                          |
|---------------|---------|------------------------------------------------------------------------|
| `add_row_num` | `True`  | Add a `_row_num` field to each record (1-based, tracks source position) |
| `skip_rows`   | `0`     | Number of data rows to skip after the header row                      |
| `n_rows`      | `None`  | Maximum number of rows to read, or `None` for all rows                |
| `headers`     | `None`  | Explicit header names to use instead of reading row 0                 |
| `null_values` | `None`  | String, or collection of strings, to convert to `None` (e.g. `'\N'`, `'NULL'`, `''`) |

```python
# Skip first 10 data rows, read only 100 rows
reader = readers.CSVReader(
    open('data.csv', encoding='utf-8-sig'),
    skip_rows=10,
    n_rows=100,
    null_values=['NULL', 'NA', ''],
)

# Row numbers track position in source file
with readers.get_reader('data.csv', skip_rows=5) as reader:
    for record in reader:
        print(f"Row {record._row_num}: {record.name}")  # _row_num starts at 6 (after skip)
```

## Common Methods and Properties

All readers are context managers and iterators, and share this surface:

| Method / Property   | Description                                                        |
|----------------------|---------------------------------------------------------------------|
| `add_filter(func)`   | Add a predicate to the filter pipeline; returns `self` for chaining (see [Filtering Records](#filtering-records)) |
| `source`             | Source file path                                                    |
| `row_count`          | Number of records read so far                                       |
| `headers`             | Original column headers                                             |
| `fieldnames`         | Normalized field names (see [Dual Field Name Access](#dual-field-name-access)) |

## Encoding

All text-based readers default to `utf-8-sig`, which handles UTF-8 with or without a BOM. If your source files have inconsistent encoding — for example, Excel's "CSV UTF-8" and "CSV" exports produce different encodings — use `encoding='detect'` to auto-detect via `charset-normalizer`:

```python
# Auto-detect encoding — useful when source encoding is inconsistent
with readers.get_reader('export.csv', encoding='detect') as reader:
    for record in reader:
        print(record.name)

# Works transparently with compressed files too
with readers.get_reader('export.csv.gz', encoding='detect') as reader:
    for record in reader:
        print(record.name)
```

`encoding='detect'` requires `charset-normalizer` (`pip install charset-normalizer`, included in `dbtk[recommended]`). If not installed, a warning is logged and encoding falls back to `utf-8-sig`.

## Compressed and Archived Files

DBTK transparently handles compressed files (`.gz`, `.bz2`, `.xz`, `.zip`) with zero configuration. Just pass the compressed filename — decompression happens automatically:

```python
# GZIP compressed CSV — automatically decompressed
with readers.get_reader('census_data.csv.gz') as reader:
    for record in reader:
        process(record)

# BZ2 compressed JSON
with readers.get_reader('api_response.json.bz2') as reader:
    for record in reader:
        process(record)

# XZ compressed TSV
with readers.get_reader('large_dataset.tsv.xz') as reader:
    for record in reader:
        process(record)
```

For ZIP files, DBTK automatically selects the right file to read:

```python
# Single file in ZIP — automatically selected
with readers.get_reader('data.csv.zip') as reader:
    for record in reader:
        process(record)

# Archive name matches member name — automatically selected
# name.subset.zip containing name.subset.tsv
with readers.get_reader('name.subset.zip') as reader:
    for record in reader:
        process(record)

# Multiple files — specify which one to read
with readers.get_reader('archive.zip', zip_member='data.csv') as reader:
    for record in reader:
        process(record)

# Works with TSV delimiter too
with readers.get_reader('names.zip', delimiter='\t') as reader:
    for record in reader:
        process(record)
```

**Performance characteristics:**
- **Progress tracking** — GZIP and ZIP files show accurate progress bars without decompressing entire file
- **Memory efficient** — Streaming decompression, constant memory usage regardless of file size
- **Real-world speed** — 500k+ records/sec reading compressed IMDB dataset (14.7M rows) with full transforms

```python
# Configure buffer size if needed (default is 1MB)
from dbtk.defaults import settings
settings['compressed_file_buffer_size'] = 2 * 1024 * 1024  # 2MB buffer
```

## Filtering Records

Use `add_filter()` to selectively process records. Multiple filters accumulate in a pipeline — all must return True for a record to be included.

```python
# Filter by column value
with readers.get_reader('soldiers.csv') as reader:
    reader.add_filter(lambda r: r.rank == 'Captain')
    reader.add_filter(lambda r: r.age >= 25)  # Both conditions must be True
    for record in reader:
        process(record)

# Filter using ValidationCollector (seen in first pass)
from dbtk.etl import ValidationCollector

# First pass: collect valid IDs
valid_titles = ValidationCollector()
with readers.get_reader('titles.csv') as reader:
    for record in reader:
        valid_titles(record.tconst)

# Second pass: only process records with valid title references
with readers.get_reader('title_principals.csv') as reader:
    reader.add_filter(lambda r: r.tconst in valid_titles)
    for record in reader:
        process(record)  # Only records with valid tconst
```

**Key behaviors:**
- Filters applied after `skip_rows` and null value conversion
- Filters applied before `n_rows` limit
- Multiple `add_filter()` calls create an AND pipeline (all must pass)
- Operates on final Record objects with normalized field names

## Dual Field Name Access

DBTK automatically handles messy field names by providing dual access — original names are preserved while normalized versions are auto-generated for convenient attribute access:

```python
# Original headers from file: ["ID #", "Student Name", "Residency Code", "GPA Score", "Has Holds?"]

with readers.CSVReader(open('data.csv')) as reader:
    for record in reader:
        # Access by original field names (preserved exactly as they appear)
        print(record['ID #'], record['Student Name'])

        # Access by normalized field names (lowercased, underscored)
        print(record.id, record.student_name)

        # Both access the same data
        assert record['ID #'] == record.id
        assert record['Student Name'] == record.student_name

# Normalization rules:
# - Lowercase conversion
# - Non-alphanumeric characters → underscore
# - Leading underscores preserved (_row_num stays _row_num)
# - Trailing underscores removed

# Examples:
# "ID #"         → id
# "Student Name" → student_name
# "GPA Score"    → gpa_score
# "Has Holds?"   → has_holds
# "_row_num"     → _row_num (preserved)
```

This is particularly useful when processing files from multiple vendors — use normalized attribute access in your code while original names are preserved for exports.

---

## CSV Files

`CSVReader` reads comma- (or otherwise-) delimited text. It's the most common reader and needs the least configuration — pass an open file, iterate.

```python
CSVReader(fp, dialect=csv.excel, headers=None, add_row_num=True, skip_rows=0, n_rows=None, null_values=None, **kwargs)
```

`**kwargs` pass straight through to `csv.reader()` (`delimiter`, `quotechar`, `quoting`, etc.). A `delimiter='\t'` shortcut is recognized without needing a custom dialect.

```python
# Tab-delimited file
with readers.CSVReader(open('data.tsv'), delimiter='\t') as reader:
    for record in reader:
        process(record)

# File has no header row — supply names explicitly
headers = ['id', 'name', 'email', 'created']
with readers.CSVReader(open('data.csv'), headers=headers) as reader:
    for record in reader:
        print(record.id, record.name)
```

`get_reader()` also works for CSV and infers the dialect/delimiter from the extension (`.csv`, `.tsv`).

## Excel Files

`ExcelReader` (via `openpyxl`, for `.xlsx`) and `XLSReader` (via `xlrd`, for legacy `.xls`) share the same interface and the [common parameters](#common-reader-parameters). The easiest entry point is `get_reader()`, which picks the right class and resolves the worksheet for you:

```python
# By sheet index (0-based) or sheet_name
with readers.get_reader('fire_nation_army.xlsx', sheet_index=1) as reader:
    for soldier in reader:
        print(soldier.military_rank)

with readers.get_reader('report.xlsx', sheet_name='Q1 Intelligence') as reader:
    for row in reader:
        process(row)
```

For direct control over the workbook/worksheet — e.g. inspecting sheet names first — use `open_workbook()` and `get_sheet_by_index()`/`get_sheet_by_name()` and construct the reader yourself:

```python
from dbtk.readers import open_workbook, get_sheet_by_index, ExcelReader

wb = open_workbook('report.xlsx')
ws = get_sheet_by_index(wb, 0)
with ExcelReader(ws, skip_rows=0, n_rows=None) as reader:
    for record in reader:
        process(record)
```

## JSON Files

`JSONReader` and friends exist to **iterate** over a dataset, record by record. If your JSON is a single object with no array to iterate — just metadata, or a one-off config blob — reach for `json.load()` directly instead; that's not what this reader is for.

```python
JSONReader(fp, record_path=None, flatten=True, add_row_num=True, skip_rows=0, n_rows=None, null_values=None, **kwargs)
```

By default, `JSONReader` expects the document root to be an array of objects:

```python
# [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}]
with readers.JSONReader(open('users.json')) as reader:
    for user in reader:
        print(user.id, user.name)
```

### Nested Arrays — `record_path`

APIs commonly wrap the array you actually want inside a response envelope alongside metadata:

```json
{
  "page": 1,
  "total_results": 2,
  "results": [
    {"id": 1, "name": "Alice"},
    {"id": 2, "name": "Bob"}
  ]
}
```

Use `record_path` (a dot-notation path, resolved from the document root) to tell `JSONReader` where the array lives:

```python
with readers.JSONReader(open('response.json'), record_path='results') as reader:
    for user in reader:
        print(user.id, user.name)

# Works for deeper nesting too, e.g. {"data": {"results": [...]}}
with readers.JSONReader(open('response.json'), record_path='data.results') as reader:
    for user in reader:
        print(user.id, user.name)
```

The metadata alongside the array (`page`, `total_results`, etc.) is not read — only the array at `record_path` is iterated.

### Flattening Nested Objects

Nested objects are flattened to dot notation by default; pass `flatten=False` to keep them as dicts:

```python
# [{"id": 1, "user": {"name": "Alice", "email": "a@example.com"}}]
with readers.JSONReader(open('nested.json')) as reader:
    for record in reader:
        print(record.id, record['user.name'], record['user.email'])

with readers.JSONReader(open('nested.json'), flatten=False) as reader:
    for record in reader:
        print(record.user)  # {'name': 'Alice', 'email': 'a@example.com'}
```

## NDJSON Files

`NDJSONReader` reads newline-delimited JSON — one JSON object per line, common for streaming APIs and log formats. There's no `record_path` or `flatten` option; each line is its own record.

```python
NDJSONReader(fp, add_row_num=True, skip_rows=0, n_rows=None, null_values=None)
```

```python
# api_events.ndjson — one JSON object per line
with readers.NDJSONReader(open('api_events.ndjson')) as reader:
    for event in reader:
        print(f"Event: {event.type}, User: {event.user_id}")
```

## XML Files

`XMLReader` needs a `record_xpath` to locate the repeating record elements.

```python
XMLReader(fp, record_xpath='//record', columns=None, sample_size=10, add_row_num=True, skip_rows=0, n_rows=None, null_values=None)
```

### Auto-Discovered Columns

**Text content of direct child elements is discovered automatically** — no `XMLColumn` definitions needed. The reader samples the first few records to find all element names and creates columns for them.

```xml
<!-- avatar_chronicles.xml -->
<avatars>
  <avatar reincarnation_cycle="148">
    <name>Korra</name>
    <origin>Southern Water Tribe</origin>
    <mastered_elements>4</mastered_elements>
  </avatar>
</avatars>
```

```python
# name, origin, and mastered_elements are discovered automatically
with readers.XMLReader(open('avatar_chronicles.xml'), record_xpath='//avatar') as reader:
    for avatar in reader:
        print(f"{avatar.name} from {avatar.origin}")
```

### Defining XMLColumn for Attributes and Nested Data

`XMLColumn` is only needed when the data you want is **not** the text of a direct child element:

```python
xml_columns = [
    # Attribute value on the record element itself
    readers.XMLColumn('cycle',     xpath='@reincarnation_cycle'),

    # Nested sub-element (not a direct child)
    readers.XMLColumn('air_move',  xpath='.//airbending/signature_move/text()'),

    # Explicit text node when you need a specific child (not just element matching)
    readers.XMLColumn('full_name', xpath='./name/text()'),
]

with readers.XMLReader(open('avatar_chronicles.xml'),
                       record_xpath='//avatar',
                       columns=xml_columns) as reader:
    for avatar in reader:
        print(f"Cycle {avatar.cycle}: {avatar.full_name} — {avatar.air_move}")
```

**When you need XMLColumn:**

| Situation                       | XPath pattern            |
|---------------------------------|--------------------------|
| Attribute on the record element | `@attr_name`             |
| Attribute on a child element    | `./child/@attr_name`     |
| Nested sub-element text         | `.//parent/child/text()` |
| Element in a specific namespace | `./ns:child/text()`      |

**When you don't need XMLColumn:** any element whose text content is a direct child of the record node is auto-discovered. You only need to add explicit definitions for the fields listed above.

## DataFrame Reader

For maximum throughput, use [polars](https://pola.rs) to read files and `DataFrameReader` to stream rows into DBTK pipelines. This works with both polars and Pandas and can use any file format that either library supports. Tip: use the Lazy API and streaming to prevent loading massive files into memory.

```python
DataFrameReader(df, add_row_num=True, skip_rows=0, n_rows=None, null_values=None)
```

```python
import polars as pl
from dbtk.etl import DataSurge
from dbtk.readers import DataFrameReader

# polars rips through CSV files at incredible speed
df = pl.scan_csv('massive_file.csv.gz').collect(engine='streaming')  # Handles compression natively

with DataFrameReader(df) as reader:
    surge = DataSurge(table)
    surge.insert(reader)
```

`DataFrameReader` iterates whatever `df` supports (`iter_rows(named=True)` for polars, `itertuples()`/`to_dict('records')`-style access for pandas) and wraps each row as a `Record`, so it composes with `add_filter()`, `select_columns()`, and every writer exactly like a cursor or file reader.

---

## Fixed-Width Files

Fixed-width files have no delimiters — every field occupies a specific character range within each line. You must define all fields explicitly using `FixedColumn`.

```python
FixedReader(fp, columns, auto_trim=True, add_row_num=False, skip_rows=0, n_rows=None, null_values=None)
```

### Defining Columns

`FixedColumn(name, start_pos, end_pos, column_type='text', align=None, pad_char=None, comment=None)`

Positions are **1-indexed** (the first character is position 1, not 0) and the end position is **inclusive**. While programmers 
typically think in zero indexed arrays and strings, most interface file specifications use 1-indexed positions. 
FixedColumn can also be initialized with start position and width as many specification are given in this format.

```python
columns = [
    readers.FixedColumn('claim_id',    1,  12),           # text (default) — strips whitespace
    readers.FixedColumn('amount',     13,  22, 'float'),  # parsed to float; None if blank
    readers.FixedColumn('claim_date', 23,  32, 'date'),   # parsed to Python date object
    readers.FixedColumn('status',     33,  width=2),      # 2-char status code, text
]

with readers.FixedReader(open('claims.txt'), columns) as reader:
    for claim in reader:
        print(f"{claim.claim_id}: ${claim.amount} on {claim.claim_date}")
```

**Column types:**

| Type             | Behavior                                              |
|------------------|-------------------------------------------------------|
| `text` (default) | Strips leading/trailing whitespace, returns string    |
| `int`            | Converts to integer; returns `None` if field is blank |
| `float`          | Converts to float; returns `None` if field is blank   |
| `date`           | Parses to `datetime.date`                             |
| `datetime`       | Parses to `datetime.datetime`                         |
| `timestamp`      | Parses to `datetime.datetime` (with timezone)         |

**`align` and `pad_char` — output formatting for `to_line()`**

These parameters only affect how `FixedWidthRecord.to_line()` reconstructs a line; they are ignored during reading. When not set, defaults are inferred from `column_type`:

| `column_type`              | Default alignment  | Default pad_char |
|----------------------------|--------------------|------------------|
| `text`, `date`, `datetime` | left               | `' '` (space)    |
| `int`, `float`             | right              | `'0'` (zero)     |

Accepted alignment values: `'left'`/`'l'`/`'<'`, `'right'`/`'r'`/`'>'`, `'center'`/`'c'`.

```python
# Numeric field — zero-padded, right-aligned by default
readers.FixedColumn('amount', 1, 10, 'int')
# value 42  →  '0000000042'

# Override to space-padded right-aligned (common for routing numbers)
readers.FixedColumn('routing_number', 1, 10, align='right', pad_char=' ')
# value '061000104'  →  ' 061000104'
```

> **Note:** `align` and `pad_char` are independent. Explicitly setting `align='left'`
> on an `int` column does *not* automatically change the pad character — it will still default
> to `'0'` and produce left-aligned zero-padded output (`'42        '` becomes `'4200000000'`)!
> When overriding alignment on a numeric column, set `pad_char=' '` explicitly too.

### Verifying Column Layout

When working from a file specification, use `visualize()` to confirm your positions match the actual data:

```python
with readers.FixedReader(open('claims.txt'), columns) as reader:
    print(reader.visualize())
# Output:
#          1         2         3         4         5         6         7         8         9
# 1234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234
# ├├┤├────────┤├────────┤├────┤├──┤├├─┤├┤├├─────────────────────┤├─────────────────────┤├──────┤
# 101 02100002101234567892603011200A094101TEST BANK              TEST COMPANY
```

This prints a character ruler with column boundary markers (`|`) over several sample lines from the file, making it easy to spot off-by-one errors before processing millions of rows.

### Reconstructing Lines — `to_line()`

Records returned by `FixedReader` are `FixedWidthRecord` instances that know their column layout. Call `to_line()` to reconstruct the original fixed-width line — useful for writing modified records back to a file without disturbing untouched columns.

```python
with readers.FixedReader(open('claims.txt'), columns) as reader:
    with open('updated_claims.txt', 'w') as out:
        for claim in reader:
            if claim.status == 'P':
                claim['status'] = 'A'
            out.write(claim.to_line() + '\n')
```

`to_line()` builds its output by position, not by column order: it creates a space-filled buffer of the total line length and splices each formatted value into its exact byte range. Gaps between defined columns remain as spaces, and columns defined out of position order are placed correctly.

```python
# truncate_overflow=True silently trims values that exceed their column width
# truncate_overflow=False (default) raises ValueError, naming the offending field
claim.to_line(truncate_overflow=True)
```

See [Record Objects — FixedWidthRecord](04-record.md#fixedwidthrecord) for full details.

## EDI / Multi-Record-Type Fixed-Width Files

EDI (Electronic Data Interchange) formats interleave different record types in the same file — each line starts with a type code that determines its layout. Use `EDIReader` with a dict mapping type codes to column lists.

```python
EDIReader(fp, columns, type_name_map=None, strict=False, **kwargs)  # kwargs pass through to FixedReader
```

DBTK ships with pre-defined layouts for NACHA ACH files:

```python
from dbtk.readers.fixed_width import EDIReader
from dbtk.formats.edi import ACH_COLUMNS

with EDIReader(open('payroll.ach'), ACH_COLUMNS) as reader:
    for record in reader:
        if record.record_type_code == '6':   # Entry Detail
            print(f"{record.individual_name}: ${int(record.amount) / 100:.2f}")
```

`ACH_COLUMNS` covers all standard NACHA record types:

| Key   | Record type   |
|-------|---------------|
| `'1'` | File Header   |
| `'5'` | Batch Header  |
| `'6'` | Entry Detail  |
| `'7'` | Addenda       |
| `'8'` | Batch Control |
| `'9'` | File Control  |

For custom multi-record formats, supply your own dict:

```python
custom_layouts = {
    'H': [FixedColumn('record_type', 1, 1), FixedColumn('file_date', 2, 9)],
    'D': [FixedColumn('record_type', 1, 1), FixedColumn('account_id', 2, 11), ...],
    'T': [FixedColumn('record_type', 1, 1), FixedColumn('record_count', 2, 9, 'int')],
}

with EDIReader(open('data.txt'), custom_layouts) as reader:
    for record in reader:
        process(record)
```

The type-code key can be any length — `EDIReader` slices the beginning of each line to match the longest key in your dict.
