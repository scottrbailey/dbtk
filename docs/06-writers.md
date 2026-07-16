# Data Writers

**The problem:** You've queried your data, now you need to export it. Do you write CSV? Excel? JSON? Load it into another database? Each format requires different code and libraries.

**The solution:** DBTK writers provide a unified interface for exporting to any format. All writers accept either a cursor or materialized results (lists of Records/namedtuples/dicts), making it trivial to export the same data to multiple formats.

For the complete parameter/method reference, see the [API Reference](11-api-reference.md#writers) or the full [Sphinx API docs](https://dbtk.readthedocs.io/en/latest/api.html).

## Quick Navigation

**General**
- [Basic Usage](#basic-usage)
- [Common Writer Parameters](#common-writer-parameters)
- [Export Once, Write Everywhere](#export-once-write-everywhere)
- [Quick Preview to Stdout](#quick-preview-to-stdout)
- [Compressed Output](#compressed-output)
- [Writing in Batches](#writing-in-batches)
- [Reshaping the Row Stream](#reshaping-the-row-stream)

**Formats**
- [CSV Files](#csv-files)
- [Excel Files](#excel-files)
- [JSON and NDJSON](#json-and-ndjson)
- [XML Files](#xml-files)
- [Streaming XML with XMLStreamer](#streaming-xml-with-xmlstreamer)
- [Database Writer](#database-writer)
- [Fixed-Width Files with FixedWidthWriter](#fixed-width-files-with-fixedwidthwriter)
- [EDI (Electronic Data Interchange) Fixed-Width with EDIWriter](#edi-electronic-data-interchange-fixed-width-with-ediwriter)

**Reference**
- [Performance Comparison](#performance-comparison)
- [Writing Additional Formats](#writing-additional-formats)

## Basic Usage

```python
from dbtk import writers

# CSV export
writers.to_csv(cursor, 'northern_tribe_waterbenders.csv', delimiter='\t')

# Excel workbooks with multiple sheets
writers.to_excel(cursor, 'fire_nation_report.xlsx', sheet='Q1 Intelligence')

# JSON output
writers.to_json(cursor, 'air_temples/meditation_records.json')

# NDJSON (newline-delimited JSON) for streaming
writers.to_ndjson(cursor, 'battle_logs.ndjson')

# XML with custom elements
writers.to_xml(cursor, 'citizens.xml', record_element='earth_kingdom_citizen')

# Fixed-width format for legacy systems
from dbtk.utils import FixedColumn
columns = [FixedColumn('name', 1, 20), FixedColumn('region', 21, 35), FixedColumn('population', 36, 45)]
writers.to_fixed_width(cursor, columns, 'ba_sing_se_daily_announcements.txt')

# Direct database-to-database transfer
source_cursor.execute("SELECT * FROM water_tribe_defenses")
count = writers.cursor_to_cursor(source_cursor, target_cursor, 'intel_archive')
print(f"Transferred {count} strategic records")
```

## Common Writer Parameters

Every file-based writer (`CSVWriter`, `JSONWriter`, `NDJSONWriter`, `XMLWriter`, `ExcelWriter`, `FixedWidthWriter`, `EDIWriter`) shares this shape:

| Parameter       | Default    | Description                                                                 |
|-----------------|------------|-------------------------------------------------------------------------------|
| `data`          | `None`     | Cursor, or iterable of Records/dicts/namedtuples/lists. `None` for streaming-only (`write_batch()`) |
| `file`          | `None`     | Output path or open file handle. `None` writes to stdout (20-row preview)     |
| `columns`       | `None`     | Column names, only needed for list-of-lists data with no header of its own    |
| `encoding`      | `'utf-8'`  | Output encoding for text formats                                              |

A second tier of parameters is common to the writers that produce a header row — `CSVWriter` and `ExcelWriter`:

| Parameter       | Default    | Description                                                                 |
|-----------------|------------|-------------------------------------------------------------------------------|
| `headers`       | `None`     | Header row text override. Falls back to `cursor.description` / detected column names |
| `write_headers` | `True`     | Whether to write the header row at all                                        |
| `compression`   | `'infer'`  | See [Compressed Output](#compressed-output) — `CSVWriter`/`JSONWriter`/`NDJSONWriter` expose this explicitly; others inherit `'infer'` |

Each writer also takes format-specific arguments (`delimiter` for CSV, `indent` for JSON, `root_element` for XML, `truncate_overflow` for fixed-width, etc.) — see that writer's own section below.

## Export Once, Write Everywhere

If your result set fits comfortably in memory you can fetch it once and export to multiple formats:

```python
# Fetch once, write many times
data = cursor.fetchall()
writers.to_csv(data, 'output.csv')
writers.to_excel(data, 'output.xlsx')
writers.to_json(data, 'output.json')
```

For large result sets, skip the `fetchall()` entirely and pass the cursor directly to a writer — it streams row-by-row without materializing anything:

```python
cursor.execute("SELECT * FROM large_table")
writers.to_csv(cursor, 'output.csv')  # Cursor consumed once, no list in memory
```

## Quick Preview to Stdout

Pass `None` as the filename to preview data to stdout — perfect for debugging or quick checks:

```python
# Preview first 20 records to console before writing to file
cursor.execute("SELECT * FROM soldiers")
writers.to_csv(cursor, None)  # Prints to stdout

# Then export the full dataset
cursor.execute("SELECT * FROM soldiers")
writers.to_csv(cursor, 'soldiers.csv')
```

## Compressed Output

All file writers support transparent compression. By default `compression='infer'` detects the format from the file extension — no extra code required:

```python
writers.to_csv(cursor, 'archive.csv.gz')       # gzip
writers.to_csv(cursor, 'archive.csv.bz2')      # bz2
writers.to_csv(cursor, 'archive.csv.xz')       # lzma
writers.to_ndjson(cursor, 'events.ndjson.gz')  # gzip
writers.to_json(cursor, 'data.json.gz')        # gzip
```

Pass an explicit value to override extension inference, or `None` to disable it:

```python
# Force gzip even though the extension doesn't say so
writers.to_csv(cursor, 'output.csv', compression='gzip')

# Write plain text despite the .gz extension
writers.to_csv(cursor, 'output.csv.gz', compression=None)
```

Supported values: `'infer'` (default), `'gzip'`, `'bz2'`, `'lzma'`, `None`.

Compression also works with batch writers — the file is opened compressed once on entry and closed on exit:

```python
from dbtk.writers import CSVWriter

with CSVWriter(file='large_archive.csv.gz') as writer:
    while batch := cursor.fetchmany(10_000):
        writer.write_batch(batch)
```

## Writing in Batches

The `to_*` helper functions are single-shot: they create a writer, write all data, then close and discard the writer. 
For incremental writes — pagination, chunked ETL, or appending from multiple sources — you need to instantiate a `BatchWriter` 
subclass directly and call `write_batch()` in a loop. Calling a `to_*` function multiple times with the same file will overwrite
the previous file. The exception is `to_excel` which will overwrite a worksheet if it already exists but not the entire workbook.

Supported batch writers: `CSVWriter`, `NDJSONWriter`, `ExcelWriter`, `LinkedExcelWriter`, `XMLStreamer`, `FixedWidthWriter`, `EDIWriter`.

```python
from dbtk.writers import CSVWriter

# Open writer once, write in pages, close at the end
with CSVWriter(file='large_export.csv') as writer:
    while batch := cursor.fetchmany(10_000):
        writer.write_batch(batch)
```

The first `write_batch()` call writes the header row; subsequent calls append data rows without repeating it.

**Why not just use `to_csv()` for this?**
`to_csv(cursor, 'output.csv')` works fine for a single cursor — it streams row-by-row internally. But if your data comes 
from multiple queries, or multiple cursors, or you need to write large datasets to multiple targets, `write_batch()` is the way to go.

```python
from dbtk.writers import ExcelWriter

with ExcelWriter(file='combined.xlsx') as writer:
    for region in ['north', 'south', 'east', 'west']:
        cursor.execute("SELECT * FROM sales WHERE region = :r", {'r': region})
        writer.write_batch(cursor, sheet_name=region)
```

## Reshaping the Row Stream

Need to drop columns, keep only some, reorder them, or rename headers before writing? Don't
materialize the whole result set to do it — DBTK provides small generator functions
(`select_columns`, `exclude_columns`, `rename_columns`, `tuples_to_records`) that reshape a row
stream one record at a time, so any writer below can consume the result directly. See
[Working with Streaming Records](04-record.md#working-with-streaming-records) in the Record
documentation for the full rundown.

---

## CSV Files

`CSVWriter` / `to_csv()` is the default choice for most exports — fastest for large datasets, universally readable.

```python
to_csv(data, file=None, headers=None, write_headers=True, null_string=None, compression='infer', **csv_kwargs)
```

`**csv_kwargs` pass straight through to `csv.writer()` (`delimiter`, `quotechar`, `quoting`, etc.). `null_string` controls how `None` values render (default `''`; set `settings['null_string_csv']` to change the global default).

```python
# Custom delimiter and null representation
writers.to_csv(cursor, 'output.tsv', delimiter='\t', null_string='NULL')
```

## Excel Files

`ExcelWriter` / `to_excel()` writes `.xlsx` workbooks. By default it auto-sizes columns by sampling the first 15 rows, bolds and freezes the header row, and applies date formatting automatically — no configuration needed.

```python
to_excel(data, file, sheet='Data', headers=None, write_headers=True)
```

```python
writers.to_excel(cursor, 'report.xlsx', sheet='Q1 Intelligence')
```

`ExcelWriter` (the batch-capable class) additionally takes a `formatting` dict/`ExcelFormat` for column styles, and is the right tool for multi-sheet reports — it keeps the workbook open across `write_batch()` calls and saves on context exit:

```python
from dbtk.writers import ExcelWriter

with ExcelWriter(file='monthly_report.xlsx') as wb:
    cursor.execute("SELECT * FROM sales WHERE month = 'January'")
    wb.write_batch(cursor, sheet_name='Sales')

    cursor.execute("SELECT * FROM expenses WHERE month = 'January'")
    wb.write_batch(cursor, sheet_name='Expenses')
# Workbook saved and closed automatically
```

For column styles, auto-rotating headers, hyperlinked reports (`LinkedExcelWriter`), and the full `formatting` dict reference, see **[Excel Reports](06b-excel.md)**.

## JSON and NDJSON

`JSONWriter` / `to_json()` writes a single JSON array; `NDJSONWriter` / `to_ndjson()` writes one JSON object per line (streaming/log-friendly, and batchable).

```python
to_json(data, file=None, indent=2, compression='infer', **json_kwargs)
to_ndjson(data, file=None, compression='infer', **json_kwargs)
```

`indent` (JSON only) controls pretty-printing — `2` by default, `None`/`0` for compact output. `**json_kwargs` pass through to `json.dump()`. Both preserve native types (numbers, bools, `None`) and stringify only `date`/`datetime`/`time` values.

```python
# Compact JSON
writers.to_json(cursor, 'data.json', indent=None)

# NDJSON is batchable — one line per write_batch() call's worth of rows
from dbtk.writers import NDJSONWriter
with NDJSONWriter(file='events.ndjson') as writer:
    while batch := cursor.fetchmany(10_000):
        writer.write_batch(batch)
```

## XML Files

`XMLWriter` / `to_xml()` builds the full XML tree in memory — best for small-to-medium datasets. Column names are sanitized into valid XML element names automatically.

```python
to_xml(data, file=None, root_element='data', record_element='record', pretty=True)
```

```python
writers.to_xml(cursor, 'citizens.xml', root_element='citizens', record_element='earth_kingdom_citizen')
```

## Streaming XML with XMLStreamer

For large XML exports, `XMLStreamer` writes records incrementally without building the entire tree in memory:

```python
from dbtk.writers import XMLStreamer

# Stream millions of records to XML
with XMLStreamer(file='large_export.xml', root_element='records',
                 record_element='item') as writer:
    for batch in data_source.batches(10000):
        writer.write_batch(batch)
```

This is memory-efficient for large datasets where `to_xml()` would consume too much memory building the DOM.

**XMLStreamer vs to_xml():**

| Feature      | XMLStreamer                 | to_xml()                   |
|--------------|-----------------------------|----------------------------|
| Memory usage | Constant (streaming)        | O(n) — loads all in memory |
| Best for     | Millions of records         | < 100K records             |
| Control      | Fine-grained batching       | Single operation           |
| Speed        | Slower (incremental writes) | Faster (bulk write)        |

**When to use XMLStreamer:**
- Exporting > 100K records to XML
- Memory-constrained environments
- Long-running exports that need progress tracking
- Need to process multiple cursors into one XML file

## Database Writer

`DatabaseWriter` / `cursor_to_cursor()` copies records directly from a source cursor (or any iterable of Records/dicts) into a target table via batched `INSERT`s — no intermediate file.

```python
cursor_to_cursor(source_data, target_cursor, target_table, batch_size=1000, commit_frequency=10000)
```

```python
source_cursor.execute("SELECT * FROM water_tribe_defenses")
count = writers.cursor_to_cursor(source_cursor, target_cursor, 'intel_archive')
print(f"Transferred {count} strategic records")
```

The INSERT statement's column list is built from the source data's columns — use [`select_columns()` or `exclude_columns()`](04-record.md#working-with-streaming-records) first if the source has columns the target table doesn't (or shouldn't) have. `batch_size` controls rows per `executemany()`; `commit_frequency` controls how often the target connection commits.

---

## Fixed-Width Files with FixedWidthWriter

`FixedWidthWriter` writes fixed-width text files driven by a `List[FixedColumn]` schema — the same schema used by `FixedReader` on the read side. Each column definition specifies position, width, alignment, and padding, so the writer handles all formatting automatically.

```python
FixedWidthWriter(data=None, file=None, columns=None, encoding='utf-8', truncate_overflow=True)
```

```python
from dbtk.utils import FixedColumn
from dbtk.writers import FixedWidthWriter, to_fixed_width

COLS = [
    FixedColumn('record_type',  1,  2),
    FixedColumn('account',      3, 14, align='right', pad_char='0'),
    FixedColumn('amount',      15, 24, align='right', pad_char='0', column_type='int'),
    FixedColumn('description', 25, 54),
]

# Single-shot
to_fixed_width(records, COLS, 'output.txt')

# Batch / streaming
with FixedWidthWriter(file='output.txt', columns=COLS) as w:
    for batch in source.batches(10_000):
        w.write_batch(batch)
```

Input records can be `FixedWidthRecord` instances (written directly via `to_line()`), dicts, lists, tuples, or namedtuples — all are cast positionally into the column schema.

By default `truncate_overflow=True` silently truncates values that exceed their column width. Pass `truncate_overflow=False` to raise `ValueError` instead.

### Building a typed record class with `fixed_record_factory`

When you're *generating* fixed-width output rather than transforming existing records, `fixed_record_factory` lets you define a named record type from a compact column spec — similar to `collections.namedtuple`. Pass a list of `(name, width)` tuples (positions are assigned automatically) or `FixedColumn` objects (used as-is), or mix both.

```python
from dbtk import fixed_record_factory

AchDetail = fixed_record_factory([
    ('record_type',    1),
    ('priority_code',  2),
    ('routing_number', 9),
    ('account_number', 17),
    ('amount',         10),
], name='AchDetail')

record = AchDetail('6', '22', '123456789', '00012345678', 100)
print(record.to_line())
# '622123456789000123456780000000100'
```

For columns that need explicit alignment, padding, or type coercion, drop in a `FixedColumn` — positions auto-advance past it:

```python
from dbtk import fixed_record_factory
from dbtk.utils import FixedColumn

AchHeader = fixed_record_factory([
    FixedColumn('record_type', 1, 1),
    ('priority_code', 2),
    FixedColumn('routing_number', 4, 12, column_type='int', align='right'),
    ('filler', 39),
])
```

The returned class is a full `FixedWidthRecord` subclass — you can pass its instances directly to `FixedWidthWriter` or call `.to_line()` yourself.

## EDI (Electronic Data Interchange) Fixed-Width with EDIWriter

`EDIWriter` is the write-side counterpart to `EDIReader`. It handles Electronic Data Interchange files where different record types have different layouts — NACHA ACH, COBOL bank extracts, X12 835 remittances, and similar formats.

```python
EDIWriter(data=None, file=None, columns=None, encoding='utf-8', truncate_overflow=False)
```

The schema is a `Dict[str, List[FixedColumn]]` mapping type codes to column definitions. The type code is always the first field of each record; `EDIWriter` reads it to select the right layout for each row.

**Read-modify-write EDI Files:**

```python
from dbtk.readers.fixed_width import EDIReader
from dbtk.writers.fixed_width import EDIWriter
from dbtk.formats.edi import ACH_COLUMNS

with open('in.ach') as fp, EDIWriter(file='out.ach', columns=ACH_COLUMNS) as w:
    batch = []
    for record in EDIReader(fp, ACH_COLUMNS):
        # records are FixedWidthRecord — modify fields, then write
        if record[0] == '6':   # Entry Detail
            record.update(amount=record.amount + 100)
            batch.append(record)
    w.write_batch(batch)
```

**Single-shot from a list:**

```python
from dbtk.writers import to_edi

records = list(EDIReader(open('in.ach'), ACH_COLUMNS))
to_edi(records, ACH_COLUMNS, 'out.ach')
```

**Pre-built schemas** for common formats are in `dbtk.formats.edi`:

```python
from dbtk.formats.edi import ACH_COLUMNS, COBOL_BANK_EXTRACT_COLUMNS, X12_835_COLUMNS, FORMATS
```

By default `truncate_overflow=False` — EDI files are length-strict and silent truncation could result in data loss. Pass `truncate_overflow=True` only if you know what you're doing.

---

## Performance Comparison

For large datasets, here's when to use each writer:

| Records    | CSV    | Excel         | JSON     | NDJSON    | XML      | XMLStreamer |
|------------|--------|---------------|----------|-----------|----------|-------------|
| < 10K      | ✅ Fast | ✅ Fast        | ✅ Fast   | ✅ Fast    | ✅ Fast   | ❌ Overkill  |
| 10K – 100K | ✅ Fast | ✅ OK          | ✅ OK     | ✅ OK      | ⚠️ Slow  | ⚠️ Better   |
| 100K – 1M  | ✅ Fast | ⚠️ Slow       | ⚠️ Slow  | ✅ OK      | ❌ Memory | ✅ Use this  |
| > 1M       | ✅ Fast | ❌ Not Allowed | ❌ Memory | ✅ OK      | ❌ Memory | ✅ Use this  |

**Recommendations:**
- **CSV**: Best for all sizes, especially large datasets
- **Excel**: Great for < 100K records, business reports
- **JSON**: Good for < 100K records, API integration
- **NDJSON**: Good for all sizes, streaming/log formats, API integration
- **XML**: Use XMLStreamer for > 100K records
- **ExcelWriter / LinkedExcelWriter**: Multi-sheet reports (any size per sheet < 1M)
- **Fixed-width / EDI**: Legacy system integration, NACHA ACH, mainframe extracts

## Writing Additional Formats

For formats DBTK doesn't cover natively — parquet, avro, Arrow IPC, and anything else polars or pandas supports — no adapter code is needed. DBTK Records are dict-compatible, so both libraries consume them directly:

```python
import polars as pl
import pandas as pd

# polars — write parquet, avro, Arrow IPC, and more
df = pl.from_dicts(cursor)
df.write_parquet('output.parquet')
df.write_avro('output.avro')

# pandas — write parquet, feather, HDF5, and more
with dbtk.readers.get_reader('data.csv') as reader:
    df = pd.DataFrame(reader)
    df.to_parquet('output.parquet')
```
