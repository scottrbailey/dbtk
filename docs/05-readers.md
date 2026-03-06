# File Readers

**The problem:** Each file format has its own quirks and APIs. You end up writing different code for CSV vs Excel vs JSON, making your ETL pipelines fragile and hard to maintain.

**The solution:** DBTK provides a single, consistent interface for reading all common file formats. Whether you're reading CSV, Excel, JSON, XML, or fixed-width files, the API is identical. Even better — `get_reader()` automatically detects the format from the file extension.
All "Large" readers will automatically display progress trackers.

## Quick Start

```python
from dbtk import readers

# CSV files — use utf-8-sig instead of utf-8 to avoid BOM issues (corrupted column names)
with readers.CSVReader(open('northern_water_tribe_census.csv', encoding='utf-8-sig')) as reader:
    for waterbender in reader:
        print(f"Waterbender: {waterbender.name}, Village: {waterbender.village}")

# Excel spreadsheets
with readers.get_reader('fire_nation_army.xlsx', sheet_index=1) as reader:
    for soldier in reader:
        print(f"Rank: {soldier.military_rank}, Firebending Level: {soldier.flame_intensity}")

# Fixed-width text files — see section below for column definition details
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

# NDJSON files (one JSON object per line — common for streaming/logs)
with readers.NDJSONReader(open('api_events.ndjson')) as reader:
    for event in reader:
        print(f"Event: {event.type}, User: {event.user_id}")

# XML files — text child elements are auto-discovered; see section below for XMLColumn usage
with readers.XMLReader(open('avatar_chronicles.xml'), record_xpath='//avatar') as reader:
    for avatar in reader:
        print(f"Avatar {avatar.name}: {avatar.origin}")
```

## Automatic Format Detection

Let DBTK figure out what you're reading:

```python
# Automatically detects format from extension
with readers.get_reader('data.xlsx') as reader:
    for record in reader:
        process(record)
```

## Compressed Files — Automatic Decompression

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

## ZIP Archives — Smart Member Selection

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

## Performance Characteristics

- **Progress tracking** — GZIP and ZIP files show accurate progress bars without decompressing entire file
- **Memory efficient** — Streaming decompression, constant memory usage regardless of file size
- **Real-world speed** — 500k+ records/sec reading compressed IMDB dataset (14.7M rows) with full transforms

```python
# Configure buffer size if needed (default is 1MB)
from dbtk.defaults import settings
settings['compressed_file_buffer_size'] = 2 * 1024 * 1024  # 2MB buffer
```

## Fixed-Width Files

Fixed-width files have no delimiters — every field occupies a specific character range within each line. You must define all fields explicitly using `FixedColumn`.

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

### EDI / Multi-Record-Type Fixed-Width Files

Some fixed-width formats interleave different record types in the same file — each line starts with a type code that determines its layout. Use `EDIReader` with a dict mapping type codes to column lists.

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

## XML Files

`XMLReader` needs a `record_xpath` to locate the repeating record elements.


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

## Common Reader Parameters

All readers support these parameters for controlling input processing:

```python
# Skip first 10 data rows, read only 100 rows
reader = readers.CSVReader(
    open('data.csv', encoding='utf-8-sig'),
    skip_rows=10,       # Skip N rows after headers (useful for bad data)
    n_rows=100,         # Only read first N rows (useful for testing/sampling)
    add_row_num=True,   # Add '_row_num' field to each record (default True)
)

# Row numbers track position in source file
with readers.get_reader('data.csv', skip_rows=5) as reader:
    for record in reader:
        print(f"Row {record._row_num}: {record.name}")  # _row_num starts at 6 (after skip)
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

# Complex filtering logic
with readers.get_reader('orders.csv') as reader:
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

## DataFrame Reader

For maximum throughput, use [polars](https://pola.rs) to read files and `DataFrameReader` to stream rows into DBTK pipelines. This works with both polars and Pandas and can use any file format that either library supports. Tip: use the Lazy API and streaming to prevent loading massive files into memory.

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
