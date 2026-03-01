# Readers & Writers

## File Readers

**The problem:** Each file format has its own quirks and APIs. You end up writing different code for CSV vs Excel vs JSON, making your ETL pipelines fragile and hard to maintain.

**The solution:** DBTK provides a single, consistent interface for reading all common file formats. Whether you're reading CSV, Excel, JSON, XML, or fixed-width files, the API is identical. Even better - `get_reader()` automatically detects the format from the file extension. 
All "Large" readers will automatically display progress trackers.

### Basic Usage

```python
from dbtk import readers

# CSV files - Use utf-8-sig instead of utf-8 to avoid BOM issues (corrupted column names)
with readers.CSVReader(open('northern_water_tribe_census.csv', encoding='utf-8-sig')) as reader:
    for waterbender in reader:
        print(f"Waterbender: {waterbender.name}, Village: {waterbender.village}")

# Excel spreadsheets
with readers.get_reader('fire_nation_army.xlsx', sheet_index=1) as reader:
    for soldier in reader:
        print(f"Rank: {soldier.military_rank}, Firebending Level: {soldier.flame_intensity}")

# Fixed-width text files
columns = [
    readers.FixedColumn('earthbender_name', 1, 25),
    readers.FixedColumn('rock_throwing_distance', 26, 35, 'float'),
    readers.FixedColumn('training_complete_date', 36, 46, 'date')
]
with readers.FixedReader(open('earth_kingdom_records.txt'), columns) as reader:
    for earthbender in reader:
        print(f"{earthbender.earthbender_name}: {earthbender.rock_throwing_distance} meters")

# JSON files (array of objects)
with readers.JSONReader(open('eastern_air_temple.json')) as reader:
    for monk in reader:
        print(f"Air Nomad: {monk.monk_name}, Sky Bison: {monk.sky_bison_companion}")

# NDJSON files (one JSON object per line - common for streaming/logs)
with readers.NDJSONReader(open('api_events.ndjson')) as reader:
    for event in reader:
        print(f"Event: {event.type}, User: {event.user_id}")

# XML files with XPath - data in elements will be detected without defining XMLColumn
xml_columns = [
    readers.XMLColumn('avatar_id', xpath='@reincarnation_cycle'),
    readers.XMLColumn('avatar_name', xpath='./name/text()'),
    readers.XMLColumn('mastered_elements', xpath='.//elements/mastered')
]
with readers.XMLReader(open('avatar_chronicles.xml'),
                       record_xpath='//avatar',
                       columns=xml_columns) as reader:
    for avatar in reader:
        print(f"Avatar {avatar.avatar_name}: {avatar.mastered_elements}")
```

### Automatic Format Detection

Let DBTK figure out what you're reading:

```python
# Automatically detects format from extension
with dbtk.readers.get_reader('data.xlsx') as reader:
    for record in reader:
        process(record)
```

### Compressed Files - Automatic Decompression

DBTK transparently handles compressed files (`.gz`, `.bz2`, `.xz`, `.zip`) with zero configuration. Just pass the compressed filename - decompression happens automatically:

```python
# GZIP compressed CSV - automatically decompressed
with dbtk.readers.get_reader('census_data.csv.gz') as reader:
    for record in reader:
        process(record)

# BZ2 compressed JSON
with dbtk.readers.get_reader('api_response.json.bz2') as reader:
    for record in reader:
        process(record)

# XZ compressed TSV
with dbtk.readers.get_reader('large_dataset.tsv.xz') as reader:
    for record in reader:
        process(record)
```

### ZIP Archives - Smart Member Selection

For ZIP files, DBTK automatically selects the right file to read:

```python
# Single file in ZIP - automatically selected
with dbtk.readers.get_reader('data.csv.zip') as reader:
    for record in reader:
        process(record)

# Archive name matches member name - automatically selected
# name.subset.zip containing name.subset.tsv
with dbtk.readers.get_reader('name.subset.zip') as reader:
    for record in reader:
        process(record)

# Multiple files - specify which one to read
with dbtk.readers.get_reader('archive.zip', zip_member='data.csv') as reader:
    for record in reader:
        process(record)

# Works with TSV delimiter too
with dbtk.readers.get_reader('names.zip', delimiter='\t') as reader:
    for record in reader:
        process(record)
```

### Performance Characteristics

- **Large buffer (1MB default)** - Optimized for fast decompression of large files
- **Progress tracking** - GZIP and ZIP files show accurate progress bars without decompressing entire file
- **Memory efficient** - Streaming decompression, constant memory usage regardless of file size
- **Real-world speed** - 500k+ records/sec reading compressed IMDB dataset (14.7M rows) with full transforms

```python
# Configure buffer size if needed (default is 1MB)
from dbtk.defaults import settings
settings['compressed_file_buffer_size'] = 2 * 1024 * 1024  # 2MB buffer
```

### Common Reader Parameters

All readers support these parameters for controlling input processing:

```python
# Skip first 10 data rows, read only 100 rows
reader = dbtk.readers.CSVReader(
    open('data.csv', encoding='utf-8-sig'), # Use 'utf-8-sig' instead of 'utf-8' to avoid BOM issues
    skip_rows=10,         # Skip N rows after headers (useful for bad data)
    n_rows=100,           # Only read first N rows (useful for testing/sampling)
    add_row_num=True,     # Add '_row_num' field to each record (default True)
)

# Row numbers track position in source file
with dbtk.readers.get_reader('data.csv', skip_rows=5) as reader:
    for record in reader:
        print(f"Row {record._row_num}: {record.name}")  # _row_num starts at 6 (after skip)
```

### Filtering Records

Use `add_filter()` to selectively process records. Multiple filters accumulate in a pipeline - all must return True for a record to be included.

```python
# Filter by column value
with dbtk.readers.get_reader('soldiers.csv') as reader:
    reader.add_filter(lambda r: r.rank == 'Captain')
    reader.add_filter(lambda r: r.age >= 25)  # Both conditions must be True
    for record in reader:
        process(record)

# Filter using ValidationCollector (seen in first pass)
from dbtk.etl import ValidationCollector

# First pass: collect valid IDs
valid_titles = ValidationCollector()
with dbtk.readers.get_reader('titles.csv') as reader:
    for record in reader:
        valid_titles(record.tconst)

# Second pass: only process records with valid title references
with dbtk.readers.get_reader('title_principals.csv') as reader:
    reader.add_filter(lambda r: r.tconst in valid_titles)
    for record in reader:
        process(record)  # Only records with valid tconst

# Complex filtering logic
with dbtk.readers.get_reader('orders.csv') as reader:
    reader.add_filter(lambda r: r.status == 'active' and r.total > 100)
    reader.add_filter(lambda r: r.country in {'US', 'CA', 'MX'})
    for record in reader:
        process(record)
```

**Key behaviors:**
- Filters applied after `skip_rows` and null value conversion
- Filters applied before `n_rows` limit
- Multiple `add_filter()` calls create an AND pipeline (all must pass)
- Operates on final Record objects with normalized field names

### DataFrame Readers

For maximum throughput, use [polars](https://pola.rs) to read files and DataFrameReader to stream rows into DBTK pipelines.  This works with both polars and Pandas and can use any file format that either library supports. Tip: use Lazy API and streaming to prevent loading massive files into memory.

```python
import polars as pl
from dbtk.etl import DataSurge
from dbtk.readers import DataFrameReader

# polars rips through CSV files at incredible speed
df = pl.read_csv('massive_file.csv.gz').collect(engine='streaming')  # Handles compression natively

with DataFrameReader(df) as reader:
    surge = DataSurge(table)
    surge.insert(reader)  
```

### Dual Field Name Access

DBTK automatically handles messy field names by providing dual access - original names are preserved while normalized versions are auto-generated for convenient attribute access:

```python
# Original headers from file: ["ID #", "Student Name", "Residency Code", "GPA Score", "Has Holds?"]

with dbtk.readers.CSVReader(open('data.csv')) as reader:
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
# "ID #" → id (attribute access)
# "Student Name" → student_name
# "GPA Score" → gpa_score
# "Has Holds?" → has_holds
# "_row_num" → _row_num (preserved)
```

This is particularly useful when processing files from multiple vendors - use normalized attribute access in your code while original names are preserved for exports.

## Data Writers

**The problem:** You've queried your data, now you need to export it. Do you write CSV? Excel? JSON? Load it into another database? Each format requires different code and libraries.

**The solution:** DBTK writers provide a unified interface for exporting to any format. All writers accept either a cursor or materialized results (lists of Records/namedtuples/dicts), making it trivial to export the same data to multiple formats.

### Basic Usage

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
column_widths = [20, 15, 10, 12]
writers.to_fixed_width(cursor, column_widths, 'ba_sing_se_daily_announcements.txt')

# Direct database-to-database transfer
source_cursor.execute("SELECT * FROM water_tribe_defenses")
count = writers.cursor_to_cursor(source_cursor, target_cursor, 'intel_archive')
print(f"Transferred {count} strategic records")
```

### Export Once, Write Everywhere

Since all writers accept materialized results, you can fetch once and export to multiple formats:

```python
# Fetch once
data = cursor.fetchall()

# Export to multiple formats
writers.to_csv(data, 'output.csv')
writers.to_excel(data, 'output.xlsx')
writers.to_json(data, 'output.json')
```

### Quick Preview to Stdout

Pass `None` as the filename to preview data to stdout - perfect for debugging or quick checks:

```python
# Preview first 20 records to console before writing to file
cursor.execute("SELECT * FROM soldiers")
writers.to_csv(cursor, None)  # Prints to stdout

# Then export the full dataset
cursor.execute("SELECT * FROM soldiers")
writers.to_csv(cursor, 'soldiers.csv')
```

### Streaming XML with XMLStreamer

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

#### XMLStreamer Detailed Usage

`XMLStreamer` provides fine-grained control for streaming large XML exports:

```python
from dbtk.writers import XMLStreamer

# Basic usage with cursor
cursor.execute("SELECT * FROM users WHERE active = true")

with XMLStreamer(file='active_users.xml', root_element='users',
                 record_element='user', encoding='utf-8') as streamer:
    for record in cursor:
        streamer.write_record(record)
# Auto-closes and finalizes XML

# Batch processing for performance
cursor.execute("SELECT * FROM orders")

with XMLStreamer(file='orders.xml', root_element='orders',
                 record_element='order') as streamer:
    batch = []
    for record in cursor:
        batch.append(record)
        if len(batch) >= 10000:
            streamer.write_batch(batch)
            batch = []
    if batch:  # Write remaining
        streamer.write_batch(batch)

# Custom element attributes
with XMLStreamer(file='products.xml', root_element='catalog',
                 record_element='product') as streamer:
    for record in cursor:
        # Records with nested data work automatically
        streamer.write_record(record)

# Manual control (without context manager)
streamer = XMLStreamer(file='data.xml', root_element='data',
                       record_element='item')
streamer.write_batch(records_list_1)
streamer.write_batch(records_list_2)
streamer.close()  # Must call close() to finalize XML
```

**XMLStreamer vs to_xml():**

| Feature | XMLStreamer | to_xml() |
|---------|-------------|----------|
| Memory usage | Constant (streaming) | O(n) - loads all in memory |
| Best for | Millions of records | < 100K records |
| Control | Fine-grained batching | Single operation |
| Speed | Slower (incremental writes) | Faster (bulk write) |

**When to use XMLStreamer:**
- Exporting > 100K records to XML
- Memory-constrained environments
- Long-running exports that need progress tracking
- Need to process multiple cursors into one XML

### Multiple Sheets with LinkedExcelWriter

`LinkedExcelWriter` creates Excel workbooks with multiple sheets from different data sources:

```python
from dbtk.writers import LinkedExcelWriter

# Create workbook with multiple sheets
with LinkedExcelWriter(file='monthly_report.xlsx') as workbook:
    # Sheet 1: Sales data
    cursor.execute("SELECT * FROM sales WHERE month = :month", {'month': 'January'})
    workbook.write_sheet(cursor, 'Sales')

    # Sheet 2: Expenses
    cursor.execute("SELECT * FROM expenses WHERE month = :month", {'month': 'January'})
    workbook.write_sheet(cursor, 'Expenses')

    # Sheet 3: Summary (from materialized data)
    summary_data = [
        {'category': 'Revenue', 'amount': 100000},
        {'category': 'Expenses', 'amount': 75000},
        {'category': 'Profit', 'amount': 25000}
    ]
    workbook.write_sheet(summary_data, 'Summary')

# Workbook is automatically saved and closed

# Complex multi-source report
with LinkedExcelWriter(file='quarterly_report.xlsx') as wb:
    # Active customers
    cursor.execute("SELECT * FROM customers WHERE status = 'active'")
    wb.write_sheet(cursor, 'Active Customers')

    # Churned customers
    cursor.execute("SELECT * FROM customers WHERE status = 'churned'")
    wb.write_sheet(cursor, 'Churned Customers')

    # Regional breakdown
    cursor.execute("""
        SELECT region, COUNT(*) as customer_count, SUM(revenue) as total_revenue
        FROM customers GROUP BY region
    """)
    wb.write_sheet(cursor, 'By Region')

    # Year-over-year comparison
    yoy_data = calculate_yoy_comparison()  # Your function
    wb.write_sheet(yoy_data, 'YoY Comparison')
```

**LinkedExcelWriter vs multiple to_excel() calls:**

```python
# ❌ WRONG: This overwrites the file each time
writers.to_excel(sales_data, 'report.xlsx', sheet='Sales')
writers.to_excel(expenses_data, 'report.xlsx', sheet='Expenses')  # Overwrites!

# ❌ WRONG: Even with append=True, requires re-reading file each time
writers.to_excel(sales_data, 'report.xlsx', sheet='Sales')
writers.to_excel(expenses_data, 'report.xlsx', sheet='Expenses', append=True)
# Works but inefficient - reopens file

# ✅ CORRECT: Efficient multi-sheet creation
with LinkedExcelWriter(file='report.xlsx') as wb:
    wb.write_sheet(sales_data, 'Sales')
    wb.write_sheet(expenses_data, 'Expenses')
# File written once at close
```

**Methods:**

- `write_sheet(data, sheet_name)` - Add sheet with data (cursor or list)
- `save()` - Save workbook (called automatically on exit)
- `close()` - Close workbook (called automatically on exit)

**Use cases:**
- Multi-tab reports for business users
- Quarterly/annual reports with multiple breakdowns
- Data exports with raw data + summaries
- Combining data from multiple databases into one file

### Performance Comparison

For large datasets, here's when to use each writer:

| Records | CSV | Excel | JSON | XML | XMLStreamer |
|---------|-----|-------|------|-----|-------------|
| < 10K | ✅ Fast | ✅ Fast | ✅ Fast | ✅ Fast | ❌ Overkill |
| 10K - 100K | ✅ Fast | ✅ OK | ✅ OK | ⚠️ Slow | ⚠️ Better |
| 100K - 1M | ✅ Fast | ⚠️ Slow | ⚠️ Slow | ❌ Memory | ✅ Use this |
| > 1M | ✅ Fast | ❌ Very slow | ❌ Memory | ❌ Memory | ✅ Use this |

**Recommendations:**
- **CSV**: Best for all sizes, especially large datasets
- **Excel**: Great for < 100K records, business reports
- **JSON**: Good for < 100K records, API integration
- **XML**: Use XMLStreamer for > 100K records
- **LinkedExcelWriter**: Multi-sheet reports (any size per sheet < 1M)
