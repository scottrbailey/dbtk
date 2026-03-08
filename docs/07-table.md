# ETL: Table, Transforms & Lookups

<div style="float: right; padding: 20px">
    <img src="assets/table.png" />
</div>

**The problem:** Production ETL pipelines need field mapping, data validation, type conversions, database function integration, and error handling. Building all of this from scratch for each pipeline is time-consuming and error-prone.

**The solution:** DBTK's `Table` class provides everything you need for production data pipelines, from simple inserts to complex merge operations with validation and transformation. Use `:named` or `%(pyformat)s` SQL parameters throughout — DBTK converts them to whatever your database requires.

For SQL file execution and `PreparedStatement`, see [Database Connections](03-database-connections.md).

## Table Class for ETL

The Table class provides a stateful interface for complex ETL operations with field mapping, transformations, and validations:

```python
import dbtk
from dbtk.etl import transforms

cursor = dbtk.connect('intel_prod')
# Auto-generate configuration from existing table
config = dbtk.etl.column_defs_from_db(cursor, 'soldier_training')

# Define ETL mapping with transformations
phoenix_king_army = dbtk.etl.Table('fire_nation_soldiers', {
    'soldier_id': {'field': 'recruit_number', 'primary_key': True},
    'name': {'field': 'full_name', 'nullable': False},
    'home_village': {'field': 'birthplace', 'nullable': False},
    'firebending_skill': {'field': 'flame_control_level', 'fn': transforms.get_int},
    'enlistment_date': {'field': 'joined_army', 'fn': transforms.parse_date},
    'combat_name': {'field': 'full_name', 'db_expr': 'generate_fire_nation_callsign(#)'},
    'last_drill': {'db_expr': 'CURRENT_TIMESTAMP'},
    'conscription_source': {'default': 'Sozin Recruitment Drive'}},
    cursor=cursor)

# Process records with validation and upsert logic
with dbtk.readers.get_reader('fire_nation_conscripts.csv') as reader:
    for recruit in reader:
        phoenix_king_army.set_values(recruit)
        if phoenix_king_army.is_ready('insert'):  # Fast O(1) cached check
            # phoenix_king_army.execute('merge') also available
            existing_soldier = phoenix_king_army.fetch()
            if existing_soldier:
                phoenix_king_army.execute('update')
            else:
                phoenix_king_army.execute('insert')
                
        else:
            missing = phoenix_king_army.reqs_missing('insert')
            print(f"Recruit {phoenix_king_army.values['name']} ({phoenix_king_army.values['soldier_id']}) rejected: missing {missing}")

print(f"Processed {phoenix_king_army.counts['insert'] + phoenix_king_army.counts['update']} soldiers")
```

## Column Configuration Schema

Each database column is configured with a dictionary specifying how to source and transform its value.

```python
{
    'database_column_name': {
        # DATA SOURCE
        'field': 'source_field_name',       # Map from input record field
        'default': 'static_value',          # Use a static or callable default for all records
        'fn': transform_function,           # Python function to transform field value, no parens!
        'db_expr': 'DATABASE_FUNCTION(#)',  # Call database function (e.g., CURRENT_TIMESTAMP, UPPER(#))

        # VALIDATION - optional:
        'nullable': False,                  # Column cannot be NULL (anti-alias of required=True)
        'required': True,                   # Column is required (anti-alias of nullable=False)

        'primary_key': True,                # Mark as primary key (alias: key)
        'key': True,                        # Alias for primary_key

        # UPDATE CONTROL - optional:
        'no_update': True,                  # Exclude from UPDATE operations (default: False)
    }
}
```

**Column configuration examples:**

```python
columns_config = {
    # Simple field mapping
    'user_id': {'field': 'id', 'primary_key': True},

    # Empty dict shorthand - field name matches column name
    'first_name': {},  # Equivalent to {'field': 'first_name'}
    'last_name': {},
    'email': {},

    # Field with transformation
    'email_clean': {'field': 'email_address', 'fn': email_clean},

    # Field with validation
    'full_name': {'field': 'name', 'nullable': False},

    # Multiple transformations (compose your own function)
    'phone': {'field': 'phone_number', 'fn': lambda x: phone_format(phone_clean(x))},
    # same as above but with a pipeline
    'phone': {'field': 'phone_number', 'fn': [phone_clean, phone_format]}
  
    # Whole record access for multi-field decisions
    'vip_status': {
        'field': '*',  # Asterisk passes entire record to function
        'fn': lambda record: 'VIP' if record.get('age', 0) > 65 or record.get('purchases', 0) > 100 else 'Regular'
    },

    # Whole record in pipelines - first function gets record, rest get values
    'discount': {
        'field': '*',
        'fn': [
            lambda record: 0.25 if record.get('loyalty_years', 0) > 10 else 0.10,
            lambda x: round(x, 2)
        ]
    },

    # Static value for all records
    'status': {'default': 'active'},

    # Callable default — resolved at set_values() time, not column-definition time.
    # Useful when the value comes from runtime context (CLI args, job config, etc.)
    # that isn't available yet when columns are defined.
    'user_id':   {'default': lambda: conf_vars['user_id']},
    'import_job': {'default': lambda: conf_vars.get('job_id', 'unknown')},
    'import_date': {'db_expr': 'CURRENT_DATE'},

    # Database function with parameter (# is placeholder for field value)
    'full_name_upper': {'field': 'name', 'db_expr': 'UPPER(#)'},

    # Computed value using database function
    'age': {'field': 'birthdate', 'db_expr': 'EXTRACT(YEAR FROM AGE(#))'},

    # Primary key that never updates (redundant - primary keys never update)
    'record_id': {'field': 'id', 'primary_key': True, 'no_update': True},

    # Field that inserts but never updates (useful for created_at timestamps)
    'created_at': {'db_expr': 'CURRENT_TIMESTAMP', 'no_update': True},
}
```

## Value Resolution Process

For each column, `set_values()` processes data in this order:

### 1. Value Sourcing
- **field**: Extract from source record.
- If `field` is a list, value will also be a list.
- If `field` is `'*'`, the entire record is passed to the transformation function instead of extracting a specific field.

### 2. Null Conversion
The value matches any entries in table.null_values it will be set to `None`.
This is configurable but the default is: `('', 'NULL', '<null>', '\\N')`

### 3. Default Fallback
If value is `None` or `''`, apply **default** if defined. If `default` is callable,
it is called with no arguments at this point — the return value is used. This lets
you bind a column to a runtime value (CLI arg, job context, etc.) without knowing
it at column-definition time:

```python
conf_vars = {}  # create before column defs, populate later

columns = {
    'user_id': {'default': lambda: conf_vars['user_id']},
    'name':    {'field': 'name'},
}

table = Table('my_table', columns, cursor=cursor)

# Populate conf_vars after arg parsing, before processing records
conf_vars['user_id'] = args.user_id
```

### 4. Transformation
Apply **fn** if defined. Functions can:
- Transform existing values
- Generate new values from scratch
- If `fn` is a list, execute in order (pipeline).

### 5. Database Expression
If **db_expr** is defined:
- **With `#`**: Pass value from steps 1-4 as parameter
  Example: `{'field': 'name', 'db_expr': 'UPPER(#)'}`
- **Without `#`**: Standalone function (ignores steps 1-4)
  Example: `{'db_expr': 'CURRENT_TIMESTAMP'}`

**Key features:**
- Field mapping and renaming
- Data type transformations
- Database function integration (`db_expr` lets you leverage database capabilities)
- Required field validation with clear error messages
- Primary key management
- Automatic UPDATE exclusions
- Support for INSERT, UPDATE, DELETE, MERGE operations
- Incomplete record tracking with `counts['incomplete']`
- `fetch()` method to retrieve existing record by primary key after `set_values()`
- `last_error` attribute — set to an `ErrorDetail` on `DatabaseError` (when `raise_error=False`), `None` on success; useful for feeding errors directly into `IdentityManager.add_error()`

## Handling Incomplete Records

DBTK supports three patterns for handling incomplete records:

```python
# Pattern 1: Tables expected to be complete - let execute() handle validation
# Use this when you want to track all incomplete records
for record in records:
    soldier_table.set_values(record)
    # execute() automatically validates keys and required columns have values
    soldier_table.execute('insert', raise_error=False)  # Track incomplete, don't raise

print(f"Inserted: {soldier_table.counts['insert']}")
print(f"Skipped (incomplete data): {soldier_table.counts['incomplete']}")

# Pattern 2: "Optional" tables, check requirements before executing DML
# Use this when missing data is expected and you want to skip incomplete records.
# If you call execute(raise_error=False) with many incomplete records you will flood
# your logs.
for record in records:
    recruit_table.set_values(record)
    if recruit_table.is_ready('insert'):  # Fast cached check
        recruit_table.execute('insert')
    # Records with missing data are silently skipped.

print(f"Inserted: {recruit_table.counts['insert']}")

# Pattern 3: Strict mode - raise errors on incomplete data
# Use this when all data must be complete
for record in records:
    critical_table.set_values(record)
    critical_table.execute('insert', raise_error=True)  # Raises ValueError if incomplete
```

### Readiness Checking Methods

Table provides three methods for checking if a record is ready for execution:

**`is_ready(operation)` → bool** - Fast O(1) cached readiness check (RECOMMENDED)
```python
# ✅ Use this in loops and hot paths
table.set_values(record)
if table.is_ready('insert'):
    table.execute('insert')

# Conditional operations based on data completeness
if table.is_ready('update'):
    table.execute('update')
elif table.is_ready('insert'):
    table.execute('insert')
```
- Returns True if all required columns have values for the operation
- Calculated automatically every time `set_values()` is called
- **Use this for performance-critical code**

If you directly modify table.values, you must recalculate requirements 
**`refresh_readiness()`** Recalculates requirements and caches results
**`reqs_met(operation)`** Recalculates requirements no caching

```python
table.set_values(record)
# reqs_met calculates requirements but does not cache results
if table.reqs_met('insert'):
    table.execute('insert')
else:
  table.values['status'] = 'active'  # Direct modification
  table.refresh_readiness()          # Recalculates requirements and caches results
  if table.is_ready('insert'):
      table.execute('insert')
```

**`reqs_missing(operation)` → Set[str]** - Get missing column names
```python
# Useful for error reporting and diagnostics
table.set_values(record)
if not table.is_ready('insert'):
    missing = table.reqs_missing('insert')
    logger.warning(f"Cannot insert record {record.id}: missing {missing}")
    # Output: Cannot insert record 123: missing {'email', 'status'}

# Debug incomplete records
for record in reader:
    table.set_values(record)
    missing = table.reqs_missing('insert')
    if missing:
        print(f"Row {record._row_num}: missing required fields: {missing}")
```
- Returns set of column names that are missing values
- Empty set means record is ready
- Perfect for error messages and validation reporting

### Fetching Existing Records

The `fetch()` method retrieves an existing record from the database using the primary key values from the current `table.values`. This is essential for update-or-insert logic.

```python
# Standard update-or-insert pattern
for record in reader:
    table.set_values(record)
    existing = table.fetch()  # SELECT using primary key

    if existing:
        # Record exists - update it
        table.execute('update')
    else:
        # Record doesn't exist - insert it
        table.execute('insert')

# Check specific fields before deciding
for record in reader:
    table.set_values(record)
    existing = table.fetch()

    if existing and existing.status == 'archived':
        logger.info(f"Skipping archived record: {existing.id}")
        continue
    elif existing:
        table.execute('update')
    else:
        table.execute('insert')
```

**Key behaviors:**
- Returns a Record object if found, `None` if not found
- Uses primary key columns from `table.values` for the SELECT
- Raises `ValueError` if primary key values are missing or None
- Executes immediately (not batched)
- Returns the Record with all columns from the database

**Common pattern with IdentityManager:**
```python
from dbtk.etl import IdentityManager, EntityStatus

im = IdentityManager('source_id', 'target_id', resolver=stmt)

for record in reader:
    entity = im.resolve(record)  # Gets target_id
    if entity['_status'] != EntityStatus.RESOLVED:
        continue

    table.set_values(record)  # Now has target_id populated
    if table.fetch():  # Check if exists in database
        table.execute('update')
    else:
        table.execute('insert')
```

### Error Tracking with last_error

The `last_error` attribute is automatically set when database operations fail with `raise_error=False`. It contains an `ErrorDetail` object for structured error handling.

```python
from dbtk.utils import ErrorDetail

# Pattern 1: Simple error tracking
for record in reader:
    table.set_values(record)
    if table.execute('insert', raise_error=False):  # Returns 1 on error
        print(f"Insert failed: {table.last_error}")
        # Output: Insert failed: ErrorDetail(message='duplicate key', field='email', ...)

# Pattern 2: Feed errors to IdentityManager
from dbtk.etl import IdentityManager, EntityStatus

im = IdentityManager('student_id', 'person_id', resolver=stmt)

for record in reader:
    entity = im.resolve(record)
    if entity['_status'] != EntityStatus.RESOLVED:
        continue

    table.set_values(record)
    if table.execute('insert', raise_error=False):
        # On error: mark entity as ERROR and attach error details
        entity['_status'] = EntityStatus.ERROR
        im.add_error(record['student_id'], table.last_error)

# Pattern 3: Collect errors for reporting
errors = []
for record in reader:
    table.set_values(record)
    if table.execute('insert', raise_error=False):
        errors.append({
            'row': record._row_num,
            'id': record.id,
            'error': table.last_error
        })

# Generate error report
for err in errors:
    print(f"Row {err['row']} (ID: {err['id']}): {err['error'].message}")
```

**ErrorDetail attributes:**
- `message` - Error description
- `field` - Column/field that caused the error (if applicable)
- `code` - Error code (database-specific)
- `value` - The value that caused the error (if applicable)

**Key behaviors:**
- Set to `ErrorDetail` object on DatabaseError
- Set to `None` on successful execution
- Only populated when `raise_error=False`
- Preserved until next `execute()` call
- Perfect for integration with IdentityManager error tracking

## Data Transformations

Built-in transformation functions handle common data cleaning tasks:

```python
from dbtk.etl import transforms as tx

# Date and time parsing with flexible formats
tx.parse_date("20 May 2025")
tx.parse_datetime("2025-05-20T18:13:27Z")  # With timezone support

# International phone number handling (requires phonenumbers library)
tx.phone_clean("5551234567")              # -> "(555) 123-4567"
tx.phone_validate("+1-800-AVATAR")        # Validation

# For advanced phone operations, import from the submodule
from dbtk.etl.transforms.phone import phone_format, phone_get_type, PhoneFormat
phone_format("+44 20 7946 0958", PhoneFormat.NATIONAL)  # UK format
phone_get_type("+1-800-CABBAGES")      # -> "toll_free"

# Email validation and cleaning
tx.email_validate("guru.pathik@eastern.air.temple")  # -> True
tx.email_clean("  TOPH@BEIFONG.EARTHKINGDOM ")      # -> "toph@beifong.earthkingdom"

# Utility functions
tx.coalesce([None, "", "Jasmine Tea", "Ginseng Tea"])  # -> "Jasmine Tea"
tx.indicator("Firebender", true_val="Fire Nation Citizen")  # Conditional values
tx.get_int("123.45 gold pieces")  # -> 123
```

### Transform Functions Reference

Complete list of built-in transform functions in `dbtk.etl.transforms`:

#### Type Conversions

| Function                         | Description                              | Example                        | Returns |
|----------------------------------|------------------------------------------|--------------------------------|---------|
| `get_int(value, default=None)`   | Parse integer, return default if invalid | `get_int('123')`               | `123` |
|                                  |                                          | `get_int('abc', 0)`            | `0` |
| `get_float(value, default=None)` | Parse float, return default if invalid   | `get_float('12.34')`           | `12.34` |
|                                  |                                          | `get_float('$1,234.56')`       | `1234.56` |
| `get_bool(value)`                | Parse boolean from various formats       | `get_bool('yes')`              | `True` |
|                                  |                                          | `get_bool('0')`                | `False` |
| `get_digits(value)`              | Extract digits only                      | `get_digits('(555) 123-4567')` | `'5551234567'` |
| `to_number(value)`               | Auto-detect int/float                    | `to_number('42')`              | `42` (int) |
|                                  |                                          | `to_number('3.14')`            | `3.14` (float) |

#### String Operations

| Function                      | Description              | Example                            | Returns |
|-------------------------------|--------------------------|------------------------------------|---------|
| `capitalize(value)`           | Capitalize first letter  | `capitalize('john')`               | `'John'` |
| `normalize_whitespace(value)` | Collapse multiple spaces | `normalize_whitespace('a  b   c')` | `'a b c'` |

#### Date and Time

| Function                 | Description                          | Example                                  | Notes                       |
|--------------------------|--------------------------------------|------------------------------------------|-----------------------------|
| `parse_date(value)`      | Parse date from various formats      | `parse_date('2024-01-15')`               | Auto-detects format         |
| `parse_datetime(value)`  | Parse datetime with timezone support | `parse_datetime('2024-01-15T10:30:00Z')` | Returns `datetime.datetime` |

#### Email

| Function                | Description                | Example                              | Returns               |
|-------------------------|----------------------------|--------------------------------------|-----------------------|
| `email_validate(value)` | Validate email format      | `email_validate('user@example.com')` | `True`                |
|                         |                            | `email_validate('invalid')`          | `False`               |
| `email_clean(value)`    | Clean and lowercase email  | `email_clean(' USER@EXAMPLE.COM ')`  | `'user@example.com'`  |

#### Phone Numbers

Requires `phonenumbers` library: `pip install phonenumbers`

| Function                                           | Description                | Example                                             | Notes              |
|----------------------------------------------------|----------------------------|-----------------------------------------------------|--------------------|
| `phone_validate(value, country='US')`              | Validate phone number      | `phone_validate('555-1234')`                        | Returns bool       |
| `phone_clean(value, country='US')`                 | Clean and format phone     | `phone_clean('5551234567')`                         | `'(555) 123-4567'` |
| `phone_format(value, format=PhoneFormat.NATIONAL)` | Format with specific style | `phone_format('+1-555-123-4567', PhoneFormat.E164)` | `'+15551234567'`   |
| `phone_get_type(value)`                            | Get phone type             | `phone_get_type('+1-800-555-0100')`                 | `'toll_free'`      |

**PhoneFormat options:** `E164`, `INTERNATIONAL`, `NATIONAL`, `RFC3966`

#### Address Validation

Requires `usaddress` library: `pip install usaddress`

| Function                      | Description                 | Example                                    | Notes           |
|-------------------------------|-----------------------------|--------------------------------------------|-----------------|
| `validate_us_address(value)`  | Validate US address format  | `validate_us_address('123 Main St')`       | Returns bool    |
| `standardize_address(value)` | Standardize address format  | `standardize_address('123 main street')`   | `'123 Main St'` |

#### Lists and Parsing

| Function                                  | Description              | Example                               | Returns           |
|-------------------------------------------|--------------------------|---------------------------------------|-------------------|
| `parse_list(value, delimiter=',')`        | Split string to list     | `parse_list('a,b,c')`                 | `['a', 'b', 'c']` |
| `get_list_item(lst, index, default=None)` | Get item by index safely | `get_list_item(['a','b'], 5, 'N/A')`  | `'N/A'`           |

#### Utilities

| Function                                                      | Description                 | Example                                 | Returns      |
|---------------------------------------------------------------|-----------------------------|-----------------------------------------|--------------|
| `coalesce(*values)`                                           | Return first non-None value | `coalesce(None, '', 'first', 'second')` | `'first'`    |
| `indicator(value, true_val='Y', false_val=None, invert=False)` | Boolean to indicator       | `indicator(True)`                       | `'Y'`        |
|                                                               |                             | `indicator(False)`                      | `None`       |
|                                                               |                             | `indicator(True, 'Active', 'Inactive')` | `'Active'`   |
| `format_number(value, decimals=2, thousands_sep=',')`         | Format number               | `format_number(1234.567)`               | `'1,234.57'` |

#### Using in Table Definitions

```python
from dbtk.etl import Table, transforms as tx

# Direct function reference
table = Table('users', {
    'age': {'field': 'age_str', 'fn': tx.get_int},
    'email': {'field': 'email_raw', 'fn': tx.email_clean},
    'amount': {'field': 'price', 'fn': tx.get_float},
}, cursor=cursor)

# Custom function combining transforms
def clean_phone(value):
    digits = tx.get_digits(value)
    return tx.phone_format(digits) if digits else None

table = Table('contacts', {
    'phone': {'field': 'phone_raw', 'fn': clean_phone},
}, cursor=cursor)

# Transform pipelines (executed in order)
table = Table('users', {
    'username': {'field': 'email', 'fn': [tx.email_clean, lambda x: x.split('@')[0]]},
}, cursor=cursor)
```

## String Shorthand for Transformations

**The problem:** Writing transformation functions for Table columns means imports, lambdas, and verbose syntax.

**The solution:** DBTK supports **string shorthand** for transformations — just write `'fn': 'int:0'` and it works. No imports, no lambdas, just clean configuration.

```python
import dbtk

# OLD WAY - verbose, needs imports
from dbtk.etl.transforms import get_int, Lookup
table = dbtk.etl.Table('movies', {
    'year': {'field': 'startYear', 'fn': lambda x: get_int(x) or 0},
    'title_short': {'field': 'primaryTitle', 'fn': lambda x: str(x or '')[:255]},
    'first_genre': {'field': 'genres', 'fn': lambda x: x.split(',')[0] if x else None},
    'state_abbrev': {'field': 'location', 'fn': Lookup('states', 'name', 'abbrev')},
}, cursor=db.cursor())

# String shorthand - clean, no imports needed
table = dbtk.etl.Table('movies', {
    'year': {'field': 'startYear', 'fn': 'int:0'},
    'title_short': {'field': 'primaryTitle', 'fn': 'maxlen:255'},
    'first_genre': {'field': 'genres', 'fn': 'nth:0'},
    'state_abbrev': {'field': 'location', 'fn': 'lookup:states:name:abbrev'},
}, cursor=db.cursor())
```

**Supported shorthands:**

| Shorthand                         | Function               | Example                                           |
|-----------------------------------|------------------------|---------------------------------------------------|
| `'int'`                           | Parse integer          | `'123'` → `123`                                   |
| `'int:0'`                         | Parse int with default | `''` → `0`                                        |
| `'float'`                         | Parse float            | `'$1,234.56'` → `1234.56`                         |
| `'bool'`                          | Parse boolean          | `'yes'` → `True`                                  |
| `'digits'`                        | Extract digits only    | `'(800) 123-4567'` → `'8001234567'`               |
| `'number'`                        | Convert to number      | `'$42.35'` → `42.35`                              | 
| `'lower'` / `'upper'` / `'strip'` | String ops             | `'  AANG  '` → `'aang'`                           |
| `'maxlen:n'`                      | Truncate to n chars    | `'maxlen:10'` on `'Avatar Aang'` → `'Avatar Aan'` |
| `'indicator'`                     | Boolean → Y/None       | `True` → `'Y'`, `False` → `None`                  |
| `'indicator:inv'`                 | Inverted indicator     | `False` → `'Y'`, `True` → `None`                  |         |
| `'indicator:Y/N'`                 | Custom true/false      | `True` → `'Y'`, `False` → `'N'`                   |
| `'split:,'`                       | Split on delimiter     | `'a,b,c'` → `['a', 'b', 'c']`                     |
| `'split:\t'`                      | Split on tab           | `'a\tb\tc'` → `['a', 'b', 'c']`                   |
| `'nth:0'`                         | Get first item         | `'action,comedy,drama'` → `'action'`              |
| `'nth:2:\t'`                      | Get 3rd tab-delimited  | `'a\tb\tc'` → `'c'`                               |
| `'lookup:...'`                    | Database lookup        | See below ↓                                       |
| `'validate:...'`                  | Database validation    | See below ↓                                       |

**Chaining transformations:**

```python
# Works in lists - functions are applied in order
table = dbtk.etl.Table('users', {
    'username': {'field': 'email', 'fn': ['lower', 'strip', 'maxlen:50']},
    'is_admin': {'field': 'role', 'fn': ['upper', 'indicator:Y/N']},
}, cursor=cursor)
```

**Real-world example (IMDB dataset):**

```python
# Loading 12 million IMDB titles with clean configuration
titles_table = dbtk.etl.Table('imdb_titles', {
    'tconst': {'field': 'tconst', 'primary_key': True},
    'title_type': {'field': 'titleType', 'fn': 'maxlen:50'},
    'primary_title': {'field': 'primaryTitle', 'fn': 'maxlen:500'},
    'original_title': {'field': 'originalTitle', 'fn': 'maxlen:500'},
    'is_adult': {'field': 'isAdult', 'fn': 'indicator:Y:N'},
    'start_year': {'field': 'startYear', 'fn': 'int:0'},
    'end_year': {'field': 'endYear', 'fn': 'int'},
    'runtime_minutes': {'field': 'runtimeMinutes', 'fn': 'int'},
    'first_genre': {'field': 'genres', 'fn': 'nth:0'},  # Extract first genre
    'all_genres': {'field': 'genres', 'fn': 'split:,'},  # Or keep all as list
}, cursor=cursor)

# Process file
with open('title.basics.tsv') as f:
    reader = dbtk.readers.CSVReader(f, delimiter='\t', header_clean=2)
    for record in reader:
        titles_table.set_values(record)
        titles_table.execute('insert')

```

## Database Lookups and Validation

**The power move:** `TableLookup` transforms any database table into a reusable lookup function with intelligent caching. Use it directly or via string shorthand for zero-boilerplate data enrichment and validation.

`TableLookup` uses `PreparedStatement` internally, so queries are portable across databases — write the lookup once, run it anywhere. Use the high-level `Lookup()` and `Validate()` factories directly in your Table column definitions to resolve codes, enrich records, or enforce referential integrity with almost no code.

```python
import dbtk
from dbtk.etl.transforms import TableLookup, Lookup, Validate
db = dbtk.connect('states_db')
cur = db.cursor()

# TableLookup requires an active cursor
state_lookup = TableLookup(cursor=cur, table='states', key_cols='state', return_cols='abbrev',
                           cache=TableLookup.CACHE_PRELOAD)
state_lookup({'state': 'Pennsylvania'})  # -> 'PA'

# Multiple return_cols - return type matches cursor type (Record, dict, namedtuple, list)
state_details = TableLookup(cursor=cur, table='states', key_cols='code', return_cols=['state', 'capital', 'region'])
state_details({'code': 'CA'})  # -> Record('California', 'Sacramento', 'West')

# String shorthand makes lookups clean
customer_etl = dbtk.etl.Table('customers', {
    # Enrich with state data
    'state_code': {'field': 'state_name', 'fn': 'lookup:states:name:code'},
    'state_capital': {'field': 'state_name', 'fn': 'lookup:states:name:capital'},
    'state_region': {'field': 'state_name', 'fn': 'lookup:states:name:region'},

    # Validate against reference tables
    'country': {'field': 'country_name', 'fn': 'validate:countries:name'},  # Warns if invalid
    'industry': {'field': 'industry_code', 'fn': 'validate:industries:code'},

    # Multiple keys and caching strategies
    'product_name': {'field': ['vendor_id', 'sku'], 'fn': 'lookup:products:vendor_id,sku:name:preload'},
}, cursor=cur)

# OLD WAY (still supported):
customer_etl = dbtk.etl.Table('customers', {
    'state_code': {'field': 'state_name', 'fn': Lookup('states', 'name', 'code')},
    'country': {'field': 'country_name', 'fn': Validate('countries', 'name')},
}, cursor=cur)
```

**Lookup/Validate string syntax:**

```
# Lookup syntax
'lookup:table:key_col:return_col[:cache]'

Examples:
'lookup:states:name:code'              # Basic lookup
'lookup:states:name:code:preload'      # With preloading (small tables)
'lookup:states:name:code:lazy'         # Lazy caching (default)
'lookup:states:name:code:no_cache'     # No caching (large tables)
'lookup:products:id,sku:name'          # Multiple key columns (comma-separated)

# Validate syntax
'validate:table:key_col[:cache]'

Examples:
'validate:countries:country_code'      # Basic validation
'validate:regions:name:preload'        # With preloading
'validate:users:email,dept:no_cache'   # Multiple keys, no caching
```

**Caching strategies:**

- **`preload`** (CACHE_PRELOAD): Load entire table into memory upfront. Best for small lookup tables (<10k rows)
- **`lazy`** (CACHE_LAZY): Cache results as encountered. Best for medium tables or selective lookups
- **`no_cache`** (CACHE_NONE): Query database every time. Best for large tables or frequently changing data

```python
# Practical example: Customer data enrichment
orders_etl = dbtk.etl.Table('orders', {
    'order_id': {'field': 'id', 'primary_key': True},

    # Small table - preload it
    'state_name': {'field': 'state_code', 'fn': 'lookup:states:code:name:preload'},

    # Medium table - lazy cache
    'customer_name': {'field': 'customer_id', 'fn': 'lookup:customers:id:name:lazy'},

    # Large table - no cache
    'product_desc': {'field': 'product_id', 'fn': 'lookup:products:id:description:no_cache'},

    # Validate referential integrity
    'category': {'field': 'category_code', 'fn': 'validate:categories:code:preload'},
}, cursor=cursor)

# Missing lookup keys raise clear errors immediately:
# ValueError: TableLookup for 'states' missing required keys: ['code']. Provided keys: ['state']
```

**Custom transformations:**

Create your own transformation functions for domain-specific logic:

```python
def standardize_nation(val):
    """Standardize nation names to official designations."""
    nation_map = {
        'Fire Nation Colonies': 'Earth Kingdom',
        'Foggy Swamp Tribe': 'Earth Kingdom',
        'Kyoshi Warriors': 'Earth Kingdom'
    }
    return nation_map.get(val, val)

# Use in Table configuration
four_nations_census = dbtk.etl.Table('population_registry', {
    'nation': {'field': 'home_nation', 'fn': standardize_nation},
    # ... other fields
})
```

## See Also

- [Database Connections](03-database-connections.md) - SQL file execution and PreparedStatement
- [ETL: DataSurge & BulkSurge](08-datasurge.md) - High-performance bulk loading
- [ETL: Tools & Logging](09-etl-tools.md) - IdentityManager, ValidationCollector, logging
- [Record Objects](04-record.md) - DBTK's universal data structure
