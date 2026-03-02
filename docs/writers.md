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
column_widths = [20, 15, 10, 12]
writers.to_fixed_width(cursor, column_widths, 'ba_sing_se_daily_announcements.txt')

# Direct database-to-database transfer
source_cursor.execute("SELECT * FROM water_tribe_defenses")
count = writers.cursor_to_cursor(source_cursor, target_cursor, 'intel_archive')
print(f"Transferred {count} strategic records")
```

## Export Once, Write Everywhere

Since all writers accept materialized results, you can fetch once and export to multiple formats:

```python
# Fetch once
data = cursor.fetchall()

# Export to multiple formats
writers.to_csv(data, 'output.csv')
writers.to_excel(data, 'output.xlsx')
writers.to_json(data, 'output.json')
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

### XMLStreamer Detailed Usage

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
| Memory usage | Constant (streaming) | O(n) — loads all in memory |
| Best for | Millions of records | < 100K records |
| Control | Fine-grained batching | Single operation |
| Speed | Slower (incremental writes) | Faster (bulk write) |

**When to use XMLStreamer:**
- Exporting > 100K records to XML
- Memory-constrained environments
- Long-running exports that need progress tracking
- Need to process multiple cursors into one XML file

## Multiple Sheets with LinkedExcelWriter

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
```

**LinkedExcelWriter vs multiple to_excel() calls:**

```python
# ❌ WRONG: This overwrites the file each time
writers.to_excel(sales_data, 'report.xlsx', sheet='Sales')
writers.to_excel(expenses_data, 'report.xlsx', sheet='Expenses')  # Overwrites!

# ✅ CORRECT: Efficient multi-sheet creation
with LinkedExcelWriter(file='report.xlsx') as wb:
    wb.write_sheet(sales_data, 'Sales')
    wb.write_sheet(expenses_data, 'Expenses')
# File written once at close
```

**Methods:**

- `write_sheet(data, sheet_name)` — Add a sheet with data (cursor or list)
- `save()` — Save workbook (called automatically on context manager exit)
- `close()` — Close workbook (called automatically on context manager exit)

**Use cases:**
- Multi-tab reports for business users
- Quarterly/annual reports with multiple breakdowns
- Data exports with raw data + summary sheets
- Combining data from multiple databases into one file

## Performance Comparison

For large datasets, here's when to use each writer:

| Records | CSV | Excel | JSON | XML | XMLStreamer |
|---------|-----|-------|------|-----|-------------|
| < 10K | ✅ Fast | ✅ Fast | ✅ Fast | ✅ Fast | ❌ Overkill |
| 10K – 100K | ✅ Fast | ✅ OK | ✅ OK | ⚠️ Slow | ⚠️ Better |
| 100K – 1M | ✅ Fast | ⚠️ Slow | ⚠️ Slow | ❌ Memory | ✅ Use this |
| > 1M | ✅ Fast | ❌ Very slow | ❌ Memory | ❌ Memory | ✅ Use this |

**Recommendations:**
- **CSV**: Best for all sizes, especially large datasets
- **Excel**: Great for < 100K records, business reports
- **JSON**: Good for < 100K records, API integration
- **XML**: Use XMLStreamer for > 100K records
- **LinkedExcelWriter**: Multi-sheet reports (any size per sheet < 1M)
