# API Reference

Complete reference for all public methods, properties, and functions in DBTK.

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

**`execute(query, params=None)`**
- Executes a single SQL statement
- Params: dict (named) or tuple/list (positional)
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

**`generate_sql(operation)`**
- Generates and caches SQL for operation

**`get_bind_params(operation)`** → dict/list
- Returns parameters for current `values`

**`get_column_definitions()`** → str
- Returns SQL CREATE TABLE statement for this table

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

### Base Reader

Located in `dbtk.readers`

**`get_reader(file_path, **kwargs)`**
- Auto-detects format from extension
- Returns: appropriate Reader object

### Common Parameters

All readers support:
- `skip_rows` - Skip N rows after headers
- `n_rows` - Only read N rows
- `add_row_num` - Add `_row_num` field (default True)

### Common Methods

**`add_filter(func)`**
- Adds filter function to pipeline
- Multiple calls accumulate (AND logic)
- Returns: self (for chaining)

**`__iter__()`**
- Iterates over records

**`__enter__()`, `__exit__()`**
- Context manager support

### Properties

- `source` → str/Path - Source file path
- `row_count` → int - Records read
- `headers` → list - Column headers
- `fieldnames` → list - Normalized field names

### Specific Readers

**CSVReader**
```python
CSVReader(file, delimiter=',', skip_rows=0, n_rows=None, **csv_args)
```

**XLSXReader**
```python
XLSXReader(file, sheet_name=0, sheet_index=None, skip_rows=0, n_rows=None)
```

**JSONReader**
```python
JSONReader(file, skip_rows=0, n_rows=None)
```

**NDJSONReader**
```python
NDJSONReader(file, skip_rows=0, n_rows=None)
```

**XMLReader**
```python
XMLReader(file, record_xpath, columns=None, skip_rows=0, n_rows=None)
```

**FixedReader**
```python
FixedReader(file, columns, skip_rows=0, n_rows=None)
```

**DataFrameReader**
```python
DataFrameReader(dataframe, skip_rows=0, n_rows=None)
```

### FixedReader Utilities

**`FixedColumn(name, start, end, dtype='str')`**
- Defines column for fixed-width files

**`FixedReader.infer_columns(sample_lines, **kwargs)`** *(classmethod)*
- Infers column positions from sample data

**`FixedReader.visualize_columns(sample_lines, columns)`** *(classmethod)*
- Visualizes column boundaries

---

## Writers

All writers accept cursor or materialized results (list of Records/dicts/tuples).

### Writer Functions

**`to_csv(data, file, delimiter=',', **kwargs)`**
- Writes to CSV file
- Pass `None` for file to print to stdout

**`to_excel(data, file, sheet='Sheet1', append=False, **kwargs)`**
- Writes to Excel file (.xlsx)

**`to_json(data, file, indent=2, **kwargs)`**
- Writes to JSON file (array of objects)

**`to_ndjson(data, file, **kwargs)`**
- Writes to NDJSON file (one object per line)

**`to_xml(data, file, root_element='root', record_element='record', **kwargs)`**
- Writes to XML file

**`to_fixed_width(data, column_widths, file, **kwargs)`**
- Writes to fixed-width text file

**`cursor_to_cursor(source_cursor, dest_cursor, table_name, batch_size=None)`**
- Direct database-to-database transfer
- Returns: number of records transferred

### XMLStreamer

For large XML exports:

```python
XMLStreamer(file, root_element='root', record_element='record', encoding='utf-8')

# Methods
write_batch(records)  # Write batch of records
close()               # Finalize XML
```

### LinkedExcelWriter

For multiple sheets in same workbook:

```python
LinkedExcelWriter(file)

# Methods
write_sheet(data, sheet_name)  # Add sheet
save()                          # Save workbook
```

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
- `get_list_item(lst, index, default=None)` - Get item by index

### Utilities

- `coalesce(*values)` - Return first non-None value
- `indicator(value, true_val='Y', false_val=None, invert=False)` - Boolean indicator
- `format_number(value, decimals=2, thousands_sep=',')` - Format number

### String Shorthand

Use string shorthand in Table column config:

```python
'fn': 'int:0'           # get_int with default 0
'fn': 'float'           # get_float
'fn': 'bool'            # get_bool
'fn': 'digits'          # get_digits
'fn': 'maxlen:100'      # Truncate to 100 chars
'fn': 'lower'           # str.lower()
'fn': 'upper'           # str.upper()
'fn': 'strip'           # str.strip()
'fn': 'indicator'       # indicator()
'fn': 'indicator:Y/N'   # indicator with custom values
'fn': 'indicator:inv'   # inverted indicator
'fn': 'split:,'         # Split on comma
'fn': 'nth:0'           # Get first item
'fn': 'lookup:table:key:return'          # Database lookup
'fn': 'validate:table:key'               # Database validation
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
ValidationCollector(lookup=None, desc_field=None)
```

### Methods

**`__call__(value)`** → value
- Collects value, optionally enriches
- Returns: original or enriched value

**`__contains__(value)`** → bool
- Checks if value has been collected

**`get_valid_mapping()`** → dict
- Returns {code: description} for codes found in reference

**`get_new_codes()`** → list
- Returns sorted list of codes not in reference

**`get_all()`** → set
- Returns all collected codes

**`get_all_mapping()`** → dict
- Returns all codes with descriptions (enriched or original)

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

column_defs_from_db(cursor, table_name, schema=None)
# Returns: dict of column configurations
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

- [Getting Started](getting-started.md) - Quick start guide
- [Database Connections](database-connections.md) - Detailed connection guide
- [ETL: Table & Transforms](table.md) - Column config and transforms
- [ETL: DataSurge & BulkSurge](datasurge.md) - Bulk operations
- [Troubleshooting](troubleshooting.md) - Common issues and solutions
