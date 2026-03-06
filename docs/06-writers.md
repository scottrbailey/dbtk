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
## Multiple Sheets with ExcelWriter

`ExcelWriter` keeps the workbook open across `write_batch()` calls and saves on context exit, making it the right tool for multi-sheet reports. Each `write_batch()` call takes an optional `sheet_name` argument.

```python
from dbtk.writers import ExcelWriter

with ExcelWriter(file='monthly_report.xlsx') as wb:
    cursor.execute("SELECT * FROM sales WHERE month = 'January'")
    wb.write_batch(cursor, sheet_name='Sales')

    cursor.execute("SELECT * FROM expenses WHERE month = 'January'")
    wb.write_batch(cursor, sheet_name='Expenses')

    summary = [
        {'category': 'Revenue', 'amount': 100_000},
        {'category': 'Expenses', 'amount': 75_000},
        {'category': 'Profit', 'amount': 25_000},
    ]
    wb.write_batch(summary, sheet_name='Summary')
# Workbook saved and closed automatically
```
If you are writing to different worksheets, you can call `to_excel` on the same file multiple times.
`ExcelWriter.write_batch` will overwrite an existing sheet (with same name) the first time writing to that sheet name, 
but will append data on subsequent writes to that sheet_name.

## Hyperlinked Reports with LinkedExcelWriter

`LinkedExcelWriter` extends `ExcelWriter` with internal and external hyperlinks. It is for creating navigable reports.

The workflow: define one or more `LinkSource` objects describing linkable entities, register them with the writer, write 
the source sheets first, then write detail sheets specifying which columns should become hyperlinks.
Links can be either internal (to sheet, row and column where record was written on `source_sheet`) or external (https:// or mailto:)

Because the link text is constructed and cached on the source sheet (master), the queries for subsequent sheets (detail)
often only need to return the key which can simplify queries. See linked_spreadsheet.py in the examples directory.
Rows in the Cast and Crew tabs contain internal links for up to four movies and displays title and release year for each. 
When the Movies tab (source sheet) was written, text_template ('{primary_title} ({start_year})}') was formatted and cached.
The queries for the Cast and Crew simply return the keys (tconst) instead of needing complex subqueries to look them up.


```python
from dbtk.writers import LinkedExcelWriter, LinkSource

# Define the linkable entity
customer_link = LinkSource(
    name="customer",            # link_source_name 
    source_sheet="Customers",   # Sheet where internal links will point to. Sheet must be written before internal link used
    key_column="customer_id",   # Column that uniquely identifies each row
    url_template="https://crm.company.com/customers/{customer_id}", # external link
    text_template="{company_name} ({customer_id})" # Link text 
)

with LinkedExcelWriter(file='sales_report.xlsx') as writer:
    writer.register_link_source(customer_link)

    # Write source sheet first — location and link_text are cached as they're written
    writer.write_batch(customers_data, sheet_name='Customers')

    # Write detail sheet — 'customer' column becomes a hyperlink
    writer.write_batch(
        orders_data,
        sheet_name='Orders',
        links={'customer': 'customer:external'}   # column_name: link_source_name
    )
```

**Link types:**

| Suffix                | Result                                                    |
|-----------------------|-----------------------------------------------------------|
| `"customer"`          | External URL (from `url_template`), or internal if no URL |
| `"customer:internal"` | Always links to the row in the source sheet               |
| `"customer:external"` | Always links to the external URL                          |

**External-only links** (`external_only=True`) generate URLs directly from the current row without caching row location for internal links.

```python
imdb_link = LinkSource(
    name="imdb",
    url_template="https://imdb.com/title/{tconst}",
    text_template="{primary_title} ({start_year})",
    external_only=True   # No source_sheet needed
)

with LinkedExcelWriter(file='movies.xlsx') as writer:
    writer.register_link_source(imdb_link)
    writer.write_batch(movies, sheet_name='All Movies',
                       links={'primary_title': 'imdb'})
    writer.write_batch(top_rated, sheet_name='Top Rated',
                       links={'primary_title': 'imdb'})
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
