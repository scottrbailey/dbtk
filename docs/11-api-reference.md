# API Reference

Complete reference for all public methods, properties, and functions in DBTK.

This page is a quick-lookup index. For narrative usage guides and worked examples, see
[File Readers](05-readers.md) / [Data Writers](06-writers.md) / [Excel Reports](06b-excel.md).
For exhaustive, always-current signatures and docstrings, see the
[Sphinx API docs](https://dbtk.readthedocs.io/en/latest/api.html).

## Quick Navigation

- [Database & Connections](#database--connections)
- [Cursors](#cursors)
- [Table Class](#table-class)
- [DataSurge](#datasurge)
- [BulkSurge](#bulksurge)
- [Readers](#readers)
- [Writers](#writers)
- [Transform Functions](#transform-functions)
- [IdentityManager](#identitymanager)
- [ValidationCollector](#validationcollector)
- [Record Objects](#record-objects)
- [Utility Functions](#utility-functions)

---

## Database & Connections

### Connection Functions

```python
from dbtk.database import postgres, oracle, mysql, sqlserver, sqlite

# PostgreSQL
db = postgres(user, password, database, host='localhost', port=5432, **kwargs)

# Oracle
db = oracle(user, password, database, host='localhost', port=1521, **kwargs)

# MySQL
db = mysql(user, password, database, host='localhost', port=3306, **kwargs)

# SQL Server
db = sqlserver(user, password, database, host='localhost', port=1433, **kwargs)

# SQLite
db = sqlite(database, **kwargs)
```

### Database Class

#### Properties
- `database_type` → str - Database type ('postgres', 'oracle', 'mysql', 'sqlserver', 'sqlite')
- `database_name` → str - Database/schema name
- `driver` → module - Underlying driver module (psycopg2, oracledb, etc.)
- `placeholder` → str - Parameter placeholder for this driver ('%s', '?', ':name', etc.)

#### Methods

**`cursor(batch_size=None, debug=False, return_cursor=False, **kwargs)`**
- Creates a new cursor with specified settings
- Returns: `Cursor` object

**`transaction()`**
- Context manager for transactions
- Auto-commits on success, rolls back on exception
- Returns: context manager

**`commit()`**
- Commits current transaction

**`rollback()`**
- Rolls back current transaction

**`close()`**
- Closes database connection

**`param_help()`**
- Prints parameter style help for this driver

**`create(connection_name, config_file=None)`** *(classmethod)*
- Factory method to create Database from config
- Returns: `Database` object

---

## Cursors

### Cursor Class

#### Properties
- `connection` → Database - Parent database connection
- `columns(normalized=False)` → list - Column names from last query
- `rowcount` → int - Rows affected by last operation
- `description` → tuple - DB-API cursor description

#### Execution Methods

**`execute(query, bind_vars=(), convert_params=False)`**
- Executes a single SQL statement
- `bind_vars`: tuple/list (positional) or dict (named) — passed directly to the driver by default
- `convert_params=True`: rewrites the query to the cursor's paramstyle and converts named `bind_vars` dict automatically (same as `execute_file` / `PreparedStatement`)
- Returns: cursor (if return_cursor=True) or None

**`executemany(query, params_list)`**
- Executes query multiple times with different parameters
- Params_list: list of dicts or list of tuples

**`execute_file(file, params=None)`**
- Loads SQL from file and executes with parameter conversion
- Automatically converts :named or %(name)s to driver's paramstyle

**`prepare_file(file)`**
- Creates PreparedStatement from SQL file
- Returns: `PreparedStatement` object

**`selectinto(query, params=None)`**
- Executes query expecting exactly one row
- Raises: ValueError if 0 or >1 rows returned
- Returns: Record object

#### Fetching Methods

**`fetchone()`**
- Fetches next record
- Returns: Record object or None

**`fetchmany(size=None)`**
- Fetches specified number of records
- Returns: list of Record objects

**`fetchall()`**
- Fetches all remaining records
- Returns: list of Record objects

**`__iter__()`**
- Allows iteration over cursor
- Memory-efficient for large result sets

#### Helper Methods

**`prepare_params(param_names, params, paramstyle=None)`**
- Converts dict to list/tuple for positional paramstyles
- Returns: list/tuple of values

---

## Table Class

Located in `dbtk.etl.Table`

### Constructor

```python
Table(table_name, columns_config, cursor, null_values=('', 'NULL', '<null>', '\\N'), is_temp=False)
```

### Properties

- `name` → str - Table name
- `columns` → dict - Column configuration
- `paramstyle` → str - Driver parameter style
- `cursor` → Cursor - Associated cursor
- `req_cols` → tuple - Required column names
- `key_cols` → tuple - Primary key column names
- `row_count` → int - Rows processed
- `counts` → dict - Operation counts (insert, update, incomplete, etc.)
- `values` → dict - Current row values
- `last_error` → ErrorDetail - Last database error (or None)

### Core Methods

**`set_values(record)`**
- Processes record through column config (transforms, defaults, validation)
- Updates `values` dict and readiness state

**`execute(operation, raise_error=True)`**
- Executes operation: 'insert', 'update', 'delete', 'merge', 'upsert'
- Returns: 0 on success, 1 on error (if raise_error=False)

**`fetch()`**
- Retrieves existing record by primary key
- Returns: Record object or None

### Readiness Checking

**`is_ready(operation)`** → bool
- Fast O(1) cached check if record ready for operation
- Use this in loops

**`reqs_met(operation)`** → bool
- Slower non-cached validation
- Only use after direct `values` modifications

**`reqs_missing(operation)`** → Set[str]
- Returns set of missing column names for operation
- Empty set = ready

**`refresh_readiness()`**
- Updates cached readiness state after direct `values` modifications

### SQL Generation

**`get_sql(operation)`** → str
- Returns generated SQL for operation

**`get_bind_params(operation)`** → dict/list
- Returns parameters for current `values`

**`get_column_definitions(all_cols)`** → list
- Returns column definitions for SQL CREATE TABLE statement

### Advanced Methods

**`force_positional()`**
- Forces positional parameters for bulk operations

**`bind_name_column(col_name)`** → str
- Returns bind variable name for column

**`calc_update_excludes()`** → set
- Returns columns excluded from UPDATE

---

## DataSurge

Located in `dbtk.etl.DataSurge`

### Constructor

```python
DataSurge(table, batch_size=None, use_transaction=False)
```

### Methods

**`insert(records)`** → int
- Bulk insert records
- Returns: number of errors

**`update(records)`** → int
- Bulk update records
- Returns: number of errors

**`delete(records)`** → int
- Bulk delete records
- Returns: number of errors

**`merge(records)`** → int
- Bulk merge (upsert) records
- Uses native UPSERT or MERGE with temp table
- Returns: number of errors

**`load(records)`** → int
- Generic load method (same as insert)

### Properties

- `total_read` → int - Total records read
- `total_loaded` → int - Total records successfully loaded
- `skipped` → int - Records skipped (incomplete/invalid)

---

## BulkSurge

Located in `dbtk.etl.BulkSurge`

### Constructor

```python
BulkSurge(table, batch_size=None)
```

### Methods

**`load(records, method='direct', dump_path=None)`** → int
- Bulk load using native database mechanisms
- Method: 'direct' (default) or 'external'
- Returns: number of records loaded

**`dump(records, file=None, write_headers=True, delimiter=',', encoding='utf-8', **csv_args)`** → int
- Exports transformed records to CSV
- Auto-generates Oracle control file if connected to Oracle
- Returns: number of records written

### Supported Databases

- **PostgreSQL**: COPY FROM STDIN (direct)
- **Oracle**: direct_path_load (direct), SQL*Loader (external)
- **MySQL**: LOAD DATA LOCAL INFILE (direct)
- **SQL Server**: bcp (external only)

---

## Readers

Located in `dbtk.readers`. Full guide: [File Readers](05-readers.md).

**`get_reader(file_path, **kwargs)`** — auto-detects format from extension (and handles compression/ZIP); returns the appropriate Reader object.

### Common Parameters

All readers (`CSVReader`, `ExcelReader`, `XLSReader`, `JSONReader`, `NDJSONReader`, `XMLReader`, `FixedReader`, `DataFrameReader`) accept:
`add_row_num=True`, `skip_rows=0`, `n_rows=None`, `headers=None`, `null_values=None`.
See [Common Reader Parameters](05-readers.md#common-reader-parameters) for what each does.

### Common Methods and Properties

- `add_filter(func)` → self — adds a predicate to the filter pipeline (AND logic across calls)
- `__iter__()`, `__enter__()`/`__exit__()` — iteration and context manager support
- `source`, `row_count`, `headers`, `fieldnames` — see [Common Methods and Properties](05-readers.md#common-methods-and-properties)

### Reader Signatures

```python
CSVReader(fp, dialect=csv.excel, headers=None, add_row_num=True, skip_rows=0, n_rows=None, null_values=None, **kwargs)
ExcelReader(worksheet, headers=None, add_row_num=True, skip_rows=0, n_rows=None, null_values=None)
XLSReader(worksheet, headers=None, add_row_num=True, skip_rows=0, n_rows=None, null_values=None)
```
Details: [CSV Files](05-readers.md#csv-files), [Excel Files](05-readers.md#excel-files)

```python
JSONReader(fp, record_path=None, flatten=True, add_row_num=True, skip_rows=0, n_rows=None, null_values=None, **kwargs)
NDJSONReader(fp, add_row_num=True, skip_rows=0, n_rows=None, null_values=None)
XMLReader(fp, record_xpath='//record', columns=None, sample_size=10, add_row_num=True, skip_rows=0, n_rows=None, null_values=None)
DataFrameReader(df, add_row_num=True, skip_rows=0, n_rows=None, null_values=None)
```
Details: [JSON Files](05-readers.md#json-files), [NDJSON Files](05-readers.md#ndjson-files), [XML Files](05-readers.md#xml-files), [DataFrame Reader](05-readers.md#dataframe-reader)

```python
FixedReader(fp, columns, auto_trim=True, add_row_num=False, skip_rows=0, n_rows=None, null_values=None)
EDIReader(fp, columns, type_name_map=None, strict=False, **kwargs)  # kwargs pass through to FixedReader
FixedColumn(name, start_pos, end_pos, column_type='text', align=None, pad_char=None, comment=None)
```
Details: [Fixed-Width Files](05-readers.md#fixed-width-files), [EDI](05-readers.md#edi--multi-record-type-fixed-width-files)

`FixedReader.visualize()` prints a character-ruler diagnostic of column boundaries over sample lines — see [Verifying Column Layout](05-readers.md#verifying-column-layout).

---

## Writers

Located in `dbtk.writers`. Full guide: [Data Writers](06-writers.md). All writers accept a cursor or materialized results (Records/dicts/namedtuples/lists).

### Common Parameters

File-based writers share `data=None`, `file=None`, `columns=None`, `encoding='utf-8'`; `CSVWriter`/`ExcelWriter` add `headers=None`, `write_headers=True`; most add `compression='infer'`.
See [Common Writer Parameters](06-writers.md#common-writer-parameters) for the full breakdown.

### Writer Functions

```python
to_csv(data, file=None, headers=None, write_headers=True, null_string=None, compression='infer', **csv_kwargs)
to_excel(data, file, sheet='Data', headers=None, write_headers=True)
```
Details: [CSV Files](06-writers.md#csv-files), [Excel Files](06-writers.md#excel-files), [Excel Reports](06b-excel.md)

```python
to_json(data, file=None, indent=2, compression='infer', **json_kwargs)
to_ndjson(data, file=None, compression='infer', **json_kwargs)
to_xml(data, file=None, root_element='data', record_element='record', pretty=True)
cursor_to_cursor(source_data, target_cursor, target_table, batch_size=1000, commit_frequency=10000)
```
Details: [JSON and NDJSON](06-writers.md#json-and-ndjson), [XML Files](06-writers.md#xml-files), [Database Writer](06-writers.md#database-writer)

```python
to_fixed_width(data, columns, file=None, encoding='utf-8', truncate_overflow=True)
to_edi(data, columns, file=None, encoding='utf-8', truncate_overflow=False)
```
Details: [Fixed-Width Files](06-writers.md#fixed-width-files-with-fixedwidthwriter), [EDI](06-writers.md#edi-electronic-data-interchange-fixed-width-with-ediwriter)

### select_columns

```python
select_columns(rows, col_names, action='include', source_columns=None)
```
Lazily projects/reorders/drops columns from a row stream before it reaches a writer. Details: [Dropping or Reordering Columns with select_columns](06-writers.md#dropping-or-reordering-columns-with-select_columns).

### XMLStreamer

For large XML exports — writes incrementally, constant memory:

```python
XMLStreamer(data=None, file=None, columns=None, encoding='utf-8', root_element='data', record_element='record')
# write_batch(records), close()
```
Details: [Streaming XML with XMLStreamer](06-writers.md#streaming-xml-with-xmlstreamer).

### LinkedExcelWriter

`ExcelWriter` subclass with internal/external hyperlink management. Same `write_batch(data, sheet_name=...)` / context-manager interface as `ExcelWriter`, plus `register_link_source()`. Full reference: [Hyperlinked Reports with LinkedExcelWriter](06b-excel.md#hyperlinked-reports-with-linkedexcelwriter).

---

## Transform Functions

Located in `dbtk.etl.transforms`

### Type Conversions

- `get_int(value, default=None)` - Parse integer, return default if invalid
- `get_float(value, default=None)` - Parse float
- `get_bool(value)` - Parse boolean
- `get_digits(value)` - Extract digits only
- `to_number(value)` - Convert to int/float (auto-detects)

### String Operations

- `capitalize(value)` - Capitalize first letter
- `normalize_whitespace(value)` - Collapse multiple spaces
- `maxlen(value, length)` - Truncate to length

### Date/Time

- `parse_date(value)` - Parse date from various formats
- `parse_datetime(value)` - Parse datetime with timezone support

### Email

- `email_validate(value)` - Validate email address
- `email_clean(value)` - Clean and validate email

### Phone

- `phone_validate(value, country='US')` - Validate phone number
- `phone_clean(value, country='US')` - Clean and format phone
- `phone_format(value, format=PhoneFormat.NATIONAL)` - Format phone number
- `phone_get_type(value)` - Get phone type (mobile, landline, etc.)

### Address

- `validate_us_address(value)` - Validate US address
- `standardize_address(value)` - Standardize address format

### Lists

- `parse_list(value, delimiter=',')` - Split string to list
- `split_and_get(val, index, delimiter=',')` - Split string and get nth item

### Utilities

- `coalesce(*values)` - Return first non-None value
- `indicator(value, true_val='Y', false_val=None, invert=False)` - Boolean indicator
- `format_number(value, decimals=2, thousands_sep=',')` - Format number

### String Shorthand

Use string shorthand in Table column config:

```python
'fn': 'int'                  # None-safe int (empty string → None)
'fn': 'float'                # None-safe float
'fn': 'bool'                 # None-safe bool
'fn': 'digits'               # Extract digit characters only
'fn': 'maxlen:100'           # Truncate to 100 chars
'fn': 'str.lower'            # str(val).lower()
'fn': 'str.upper'            # str(val).upper()
'fn': 'str.strip'            # str(val).strip()
'fn': 'str.strip:="'         # Strip specific characters
'fn': 'indicator'            # True → 'Y', False/None → None
'fn': 'indicator:Y:N'        # Custom true/false values
'fn': 'indicator:None:Y'     # Inverted (False → 'Y', True → None)
'fn': 'split_and_get:0'      # First comma-delimited field
'fn': 'split_and_get:1:\t'   # Second tab-delimited field
'fn': 'str.split:,'          # Split on comma → list
'fn': 'str.rjust:+9:0'       # Right-justify to width 9, pad with '0'
'fn': 'nth:0'                # First element of a list/sequence
'fn': 'coalesce'             # First non-empty, non-None value
'fn': 'lookup:table:key:return'   # Database lookup
'fn': 'validate:table:key'        # Database validation
```

---

## IdentityManager

Located in `dbtk.etl.IdentityManager`

### Constructor

```python
IdentityManager(source_key, target_key, resolver=None, alternate_keys=None)
```

### Methods

**`resolve(value)`** → dict
- Resolves source key to target key
- Value: scalar, dict, or Record
- Returns: entity dict with '_status' and keys

**`add_error(source_value, error)`**
- Attaches error to entity
- Error: ErrorDetail object or string

**`add_message(source_value, message)`**
- Attaches message to entity

**`set_id(source_value, key_name, value)`**
- Sets alternate key value

**`get_id(source_value, key_name)`**
- Gets alternate key value

**`batch_resolve(additional_statuses=None)`**
- Re-resolves all PENDING and NOT_FOUND entities
- additional_statuses: list of EntityStatus to also retry

**`calc_stats()`** → dict
- Returns stats dict: {pending: N, resolved: N, ...}

**`save_state(file)`**
- Persists state to JSON

**`load_state(file, resolver=None)`** *(classmethod)*
- Loads state from JSON
- Returns: IdentityManager object

### EntityStatus Constants

```python
from dbtk.etl import EntityStatus

EntityStatus.PENDING     # 'pending'
EntityStatus.RESOLVED    # 'resolved'
EntityStatus.STAGED      # 'staged'
EntityStatus.NOT_FOUND   # 'not_found'
EntityStatus.ERROR       # 'error'
EntityStatus.SKIPPED     # 'skipped'
```

---

## ValidationCollector

Located in `dbtk.etl.ValidationCollector`

### Constructor

```python
ValidationCollector(lookup=None, return_col=None)
```

- `lookup` — optional `TableLookup`; if supplied, codes are validated against it on first encounter
- `return_col` — field name to extract from the lookup result and return from `__call__`; `None` (default) always returns the raw code

### Methods

**`__call__(value)`** → value
- Collects value, checks it against the lookup if configured
- Returns: raw code if `return_col` is None; the named field for existing codes, `None` for new codes if `return_col` is set

**`__contains__(value)`** → bool
- Checks if value has been seen (in either existing or added)

**`collect_new(code, **fields)`**
- Annotates a newly-added code with extra fields from the source row
- No-op if the code was not just added (safe to call unconditionally after `set_values`)

**`get_valid_mapping()`** → dict
- Returns `{code: return_col_value}` for codes found in the reference table

**`get_all()`** → set
- Returns all collected codes (existing + new)

**`get_all_mapping()`** → dict
- Returns all codes with their `return_col` value; new unannotated codes map to `None`

---

## Record Objects

Located in `dbtk.record.Record`

### Access Methods

- `record['field']` - Dict-style access (original names)
- `record.field` - Attribute access (normalized names)
- `record[0]` - Index access
- `record[1:3]` - Slicing
- `record.get('field', default)` - Safe access with default

### Conversion Methods

- `dict(record)` - Convert to dict
- `record.to_dict(normalized=True)` - Convert with normalized keys
- `tuple(record)` - Convert to tuple
- `list(record)` - Convert to list

### Iteration Methods

- `record.keys(normalized=False)` - Get keys
- `record.values()` - Get values
- `record.items()` - Get key-value pairs

### Mutation Methods

- `record['field'] = value` - Set value
- `record.update(dict)` - Update multiple values
- `record.coalesce(dict)` - Update only None values
- `del record['field']` - Delete field
- `record.pop('field', default)` - Pop with default

### Utility Methods

- `record.pprint(normalized=False)` - Pretty print
- `len(record)` - Number of fields
- `'field' in record` - Check field exists

---

## Utility Functions

### Configuration

```python
from dbtk import set_config_file, connect

set_config_file(path)    # Set config file path
connect(name)            # Connect to named database
```

### Password Encryption

```python
from dbtk.config import generate_encryption_key, encrypt_config_file

generate_encryption_key()           # Generate Fernet key
encrypt_config_file(path)           # Encrypt passwords in config
```

### Logging

```python
from dbtk import setup_logging, cleanup_old_logs, errors_logged

setup_logging(name=None, log_dir='./logs', level='INFO', **kwargs)
cleanup_old_logs(log_dir='./logs', retention_days=30, dry_run=False)
errors_logged()  # Returns error log path or None
```

### Column Definition Generator

```python
from dbtk.etl import column_defs_from_db

column_defs_from_db(cursor, table_name, add_comments=False)
# Returns: string containing a Python dict literal of column configurations
```

### SQL Parameter Processing

```python
from dbtk.utils import process_sql_parameters, ParamStyle

process_sql_parameters(sql, target_paramstyle)
# Returns: (converted_sql, param_names)

ParamStyle.get_positional_style(paramstyle)
# Returns: positional equivalent of paramstyle
```

---

## CLI Commands

```bash
# Check installation
dbtk checkup

# Password encryption
dbtk generate-key                    # Generate encryption key
dbtk store-key [key]                 # Store key in system keyring
dbtk encrypt-password <password>     # Encrypt single password
dbtk encrypt-config [file]           # Encrypt all passwords in config
dbtk migrate-config <old> <new>      # Migrate to new encryption key

# Interactive setup
dbtk config-setup                    # Configuration wizard
```

---

## See Also

- [Getting Started](01-getting-started.md) - Quick start guide
- [Database Connections](03-database-connections.md) - Detailed connection guide
- [ETL: Table & Transforms](07-table.md) - Column config and transforms
- [ETL: DataSurge & BulkSurge](08-datasurge.md) - Bulk operations
- [Troubleshooting](12-troubleshooting.md) - Common issues and solutions
