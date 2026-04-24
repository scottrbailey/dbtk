# Data Writers

**The problem:** You've queried your data, now you need to export it. Do you write CSV? Excel? JSON? Load it into another database? Each format requires different code and libraries.

**The solution:** DBTK writers provide a unified interface for exporting to any format. All writers accept either a cursor or materialized results (lists of Records/namedtuples/dicts), making it trivial to export the same data to multiple formats.

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
## Multiple Sheets and Excel Formatting

`ExcelWriter` keeps the workbook open across `write_batch()` calls and saves on context exit, making it the right tool for multi-sheet reports. For column styles, auto-rotating headers, hyperlinked reports, and the full `formatting` dict reference, see **[Excel Reports](06b-excel.md)**.

```python
from dbtk.writers import ExcelWriter

with ExcelWriter(file='monthly_report.xlsx') as wb:
    cursor.execute("SELECT * FROM sales WHERE month = 'January'")
    wb.write_batch(cursor, sheet_name='Sales')

    cursor.execute("SELECT * FROM expenses WHERE month = 'January'")
    wb.write_batch(cursor, sheet_name='Expenses')
# Workbook saved and closed automatically
```

For hyperlinked reports with internal navigation or external URLs, see [`LinkedExcelWriter`](06b-excel.md#hyperlinked-reports-with-linkedexcelwriter).

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

## Fixed-Width Files with FixedWidthWriter

`FixedWidthWriter` writes fixed-width text files driven by a `List[FixedColumn]` schema — the same schema used by `FixedReader` on the read side. Each column definition specifies position, width, alignment, and padding, so the writer handles all formatting automatically.

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
