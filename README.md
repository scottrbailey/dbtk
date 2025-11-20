# DBTK - Data Benders Toolkit

<div style="float: right; padding: 20px">
    <img src="/docs/assets/databender.png" height="240" align="right" />
</div>

**Control and Manipulate the Flow of Data** - A lightweight Python toolkit for data integration, transformation, and movement between systems.

Like the elemental benders of Avatar, this library gives you precise control over data, the world's most rapidly growing element. Extract data from various sources, transform it through powerful operations, and load it exactly where it needs to go. This library is designed by and for data integrators.

**Design philosophy:** Modern databases excel at aggregating and transforming data at scale. DBTK embraces
this by focusing on what Python does well: flexible record-by-record transformations,
connecting disparate systems, and orchestrating data movement.

If you need to pivot, aggregate, or perform complex SQL operations - write SQL and let
your database handle it. If you need dataframes and heavy analytics - reach for Pandas
or polars. DBTK sits in between: getting your data where it needs to be, cleaned and
validated along the way.

## Table of Contents

- [Features](#features)
- [Installation](#installation)
- [Quick Start](#quick-start)
- [Core Components](#core-components)
  - [Database Connections](#database-connections) - Write once, run anywhere
  - [File Readers](#file-readers) - Unified interface for all formats
  - [Data Writers](#data-writers) - Export anywhere with one call
  - [ETL Operations](#etl-operations) - Production-ready data pipelines
- [Configuration & Security](#configuration--security)
  - [Password Encryption](#password-encryption)
  - [Command Line Tools](#command-line-tools)
- [Advanced Features](#advanced-features)
- [Database Support](#database-support)
- [Performance Tips](#performance-tips)

## Features

- **Universal Database Connectivity** - Unified interface across PostgreSQL, Oracle, MySQL, SQL Server, and SQLite with intelligent driver auto-detection
- **Portable SQL Queries** - Write SQL once with named parameters, run on any database regardless of parameter style
- **Flexible File Reading** - CSV, Excel (XLS/XLSX), JSON, NDJSON, XML, and fixed-width text files with consistent API
- **Multiple Export Formats** - Write to CSV, Excel, JSON, NDJSON, XML, fixed-width text, or directly between databases
- **Advanced ETL Framework** - Full-featured Table class for complex data transformations, validations, and upserts
- **Bulk Operations** - DataSurge class for high-performance batch INSERT/UPDATE/DELETE/MERGE operations
- **Data Transformations** - Built-in functions for dates, phones, emails, and custom data cleaning with international support
- **Integration Logging** - Timestamped log files with automatic cleanup, split error logs, and zero-config setup
- **Encrypted Configuration** - YAML-based config with password encryption and environment variable support
- **Smart Cursors** - Multiple result formats: Records, named tuples, dictionaries, or plain lists

## Installation

```bash
pip install dbtk

# For encrypted passwords
pip install dbtk[encryption]  # installs cryptography and keyring 

# For reading/writing XML and Excel files
pip install dbtk[formats]     # lxml and openpyxl 

# Full functionality
pip install dbtk[all]         # all optional dependencies

# Database adapters (install as needed)
pip install psycopg2          # PostgreSQL
pip install oracledb          # Oracle
pip install mysqlclient       # MySQL
```

## Quick Start

### Sample Outbound Integration - Export Data

Extract data from your database and export to multiple formats with portable SQL queries:

```python
import dbtk

# One-line setup creates timestamped log - all operations automatically logged
dbtk.setup_logging()  # Location, filename format are controlled by your configuration

# queries/monthly_report.sql - Write once with named parameters
# SELECT soldier_id, name, rank, missions_completed, last_mission_date
# FROM soldiers
# WHERE rank >= :min_rank
#   AND enlistment_date >= :start_date
#   AND (region = :region OR :region IS NULL)
#   AND status = :status

# queries/officer_summary.sql
# SELECT rank, COUNT(*) as count, AVG(missions_completed) as avg_missions
# FROM soldiers
# WHERE rank >= :min_rank
#   AND status = :status
# GROUP BY rank

with dbtk.connect('fire_nation_db') as db:
    cursor = db.cursor()

    # Same parameters work for both queries
    # Extra params (start_date, region) ignored by officer_summary
    # Missing params automatically become NULL
    params = {
        'min_rank': 'Captain',
        'start_date': '2024-01-01',
        'region': 'Western Fleet',  # Only used by monthly_report
        'status': 'active'
    }

    # Execute first query - DBTK converts parameters to match database style
    cursor.execute_file('queries/monthly_report.sql', params)
    monthly_data = cursor.fetchall()

    # Execute second query - reuses same params, ignores unused ones
    cursor.execute_file('queries/officer_summary.sql', params)
    summary_data = cursor.fetchall()

    # Export to multiple formats trivially
    dbtk.writers.to_csv(monthly_data, 'reports/soldiers_monthly.csv')
    dbtk.writers.to_excel(summary_data, 'reports/officer_summary.xlsx',
                          sheet='Officer Stats')

    # writers will automatically log summary info to the logs and will also be
    # echoed to stdout in default configuration

# Database queries, file operations - all automatically logged by DBTK
# The default error handler keeps track of whether errors or critical events were logged
# and errors_logged() will return the filename of the error log only if there were errors.
error_log_fn = dbtk.errors_logged()
if error_log_fn:
    print("⚠️  Export completed with errors - check log file")
    # send_notification_email(subject="Export errors", attachment=error_log)
```

**What makes this easy:**
- Write SQL once with named parameters (`:param`), works on any database
- Pass the same dict to multiple queries - extra params ignored, missing params = NULL
- Export to CSV/Excel/JSON with one line
- No parameter style conversions needed - DBTK handles it automatically

### Sample Inbound Integration - Import Data

Import data with field mapping, transformations, and validation:

```python
# /integrations/hr/import_new_recruits.py 
import dbtk
from dbtk.etl import Table
from dbtk.etl.transforms import parse_date, email_clean, get_int

# Custom transform - so only ranks of General, Admiral & Commander have master_codes 
def get_master_code(value):
    """Return master code or None"""
    mc_map = {'General': 'G', 'Admiral': 'A', 'Commander': 'C'}
    if value in mc_map:
        return mc_map[value]
    return None

# One-line setup with automatic script name detection (see Logging configuration)
dbtk.setup_logging()  # Creates logs/import_new_recruits_20251031.log

with dbtk.connect('fire_nation_db') as db:
    cursor = db.cursor()

    # Main soldier table - all records must have complete data
    soldier_table = Table('soldiers', {
        'soldier_id': {'field': 'id', 'primary_key': True},
        'name': {'field': 'full_name', 'nullable': False},
        'rank': {'field': 'officer_rank', 'nullable': False},
        'email': {'field': 'contact_email', 'default': 'intel@firenation.com', 'fn': email_clean},
        'enlistment_date': {'field': 'join_date', 'fn': parse_date},
        'missions_completed': {'field': 'mission_count', 'fn': get_int},
        'status': {'default': 'active'}  # Default value for all
    }, cursor=cursor)

    # Optional specialization table - only insert if specialty data exists
    # master_code required for senior ranks, but that's validated by nullable=False
    specialization_table = Table('soldier_specializations', {
        'soldier_id': {'field': 'id', 'primary_key': True},
        'specialty': {'field': 'special_training', 'nullable': False},
        'master_code': {'field': 'officer_rank', 'fn': get_master_code,
                       'nullable': False},
        'certification_date': {'field': 'cert_date', 'fn': parse_date}
    }, cursor=cursor)

    # Process incoming CSV file
    with dbtk.readers.get_reader('incoming/new_recruits.csv') as reader:
        for record in reader:
            # Main table - track all records, even incomplete ones
            soldier_table.set_values(record)
            soldier_table.exec_insert(raise_error=False)

            # Optional table - check if specialty fields are present
            # Only insert if requirements met (specialty and master_code not null)
            specialization_table.set_values(record)
            if specialization_table.reqs_met:
                specialization_table.exec_insert(reqs_checked=True) 
        print(f"Inserted {soldier_table.counts["inserts"]} rows into {soldier_table.name}, skips: {soldier_table.counts["incomplete"]}")
    db.commit()

# Check for errors - DBTK automatically logs all database operations and file errors
error_log_fn = dbtk.errors_logged()
if error_log_fn:
    print(f"⚠️  Errors occurred - check {error_log_fn}")
    # send_notification_email(subject="Import errors", attachment=error_log_fn)
```

**What makes this easy:**
- Field mapping separates database schema from source data format
- Built-in transforms (dates, emails, integers) with custom transform support
- Transform functions receive field values, can provide defaults or clean data
- Optional tables with `reqs_met` pattern - no noisy logs for expected missing data
- Automatic tracking with `counts` dictionary
- One connection, multiple tables, all validated and transformed


## Core Components

### Database Connections

**The problem:** Every database has different connection parameters, drivers, and parameter styles. Writing portable database code is painful.

**The solution:** DBTK provides a unified interface that handles all the complexity. Write your code once, and it works seamlessly across PostgreSQL, Oracle, MySQL, SQL Server, and SQLite. The library automatically detects available drivers and handles parameter style conversions transparently.

```python
import dbtk

# Direct connections - same API for all databases
fire_db = dbtk.database.postgres(user='azula', password='blue_flames', database='fire_nation')
earth_db = dbtk.database.oracle(user='toph', password='metal_bending', database='ba_sing_se')

# From configuration - store credentials securely
db = dbtk.connect('northern_water_tribe')

# Different cursor types for different needs
cursor = db.cursor('record')        # Record class - row['name'], row.name, row[0], row.get('name')
cursor_t = db.cursor('tuple')       # namedtuple   - row.name, row[0]
cursor_l = db.cursor('list')        # simple list  - row[0]
cursor_d = db.cursor('dict')        # OrderedDict  - row['name'], row.get('name')
```

**Access the full stack when you need it:**

DBTK maintains a clean reference hierarchy that gives you access to the underlying driver for maximum flexibility:

```python
# The connection maintains a reference to the driver on the interface attribute
print(db.interface.__name__)  # 'psycopg2', 'oracledb', etc.
print(db.interface.paramstyle)  # 'named', 'qmark', etc.

# The cursor maintains a reference to the connection
try:
    cursor.execute(sql)
except cursor.connection.interface.DatabaseError as e:
    logger.error(f"Database error: {e}")

# Access driver-specific features when needed
if db.interface.__name__ == 'psycopg2':
    cursor.execute("LISTEN channel_name")
```

**Transaction management:**

Context managers make transactions safe and simple:

```python
with db.transaction():
    cursor = db.cursor()
    cursor.execute("INSERT INTO battles ...")
    cursor.execute("UPDATE casualties ...")
    # Auto-commit on success, rollback on exception
```

**Cursor types:**

Choose the right cursor return type for your use case to balance functionality and performance:

```python
# Record cursor - memory efficient and most flexible (recommended for most use cases)
cursor = db.cursor('record') # default
row = cursor.fetchone()
print(row['name'], row.name, row[0], '\t'.join(row[:5]))  # All work!

# Tuple cursor - lightweight namedtuple
cursor = db.cursor('tuple')
row = cursor.fetchone()
print(row.name, row[0], '\t'.join(row[:5]))  # Named and indexed access

# Dict cursor - dictionary-only access
cursor = db.cursor('dict')
row = cursor.fetchone()
print(row['name'])  # Dictionary access only

# List cursor - minimal overhead for simple iteration
cursor = db.cursor('list')
row = cursor.fetchone()
print(row[0], '\t'.join(row[:5]))  # Index access only

# Cursor chaining
cursor = db.cursor(return_cursor=True)
cursor.execute("SELECT * FROM firebenders WHERE rank = 'general'").fetchone()
```

**Record objects - Maximum flexibility:**

The Record cursor type (default) provides the most flexible interface for working with database and file data:

```python
cursor = db.cursor('record')  # or just db.cursor()
row = cursor.fetchone()

# Dictionary-style access
print(row['soldier_id'], row['name'])

# Attribute access (like namedtuple)
print(row.soldier_id, row.name)

# Index access (like tuple/list)
print(row[0], row[1])

# Safe access with default values
rank = row.get('rank', 'Unknown')

# Slicing for subsets
first_five = row[:5]
last_three = row[-3:]

# Iteration and joining
print('\t'.join(row))            # Join all fields
print(', '.join(str(v) for v in row))  # Custom formatting

# Length and membership
print(len(row))                  # Number of columns
print('email' in row)            # Check if column exists

# Convert to dict or tuple when needed
row_dict = dict(row)
row_tuple = tuple(row)

# Pretty print
>>> row.pprint()
trainee_id       : 1
monk_name        : Master Aang
home_temple      : Northern Air Temple
mastery_rank     : 10
bison_companion  : Lefty
daily_meditation : 9.93
birth_date       : 1965-12-16
last_training    : 2024-12-25 15:51:42
_row_num         : 1
```

If it quacks like a dict, walks like a tuple, and iterates like a list - it's a Record! This duck-typed design gives you the flexibility to access data however makes sense for your code, without sacrificing performance. Records are memory-efficient and fast while providing the most flexible API. They're the recommended default for most use cases.

**Supported databases:**
- PostgreSQL (psycopg2, psycopg3, pgdb)
- Oracle (oracledb, cx_Oracle)
- MySQL (mysqlclient, mysql.connector, pymysql, MySQLdb)
- SQL Server (pyodbc, pymssql)
- SQLite (built-in)

See [Database Support](#database-support) for detailed driver information.

### File Readers

**The problem:** Each file format has its own quirks and APIs. You end up writing different code for CSV vs Excel vs JSON, making your ETL pipelines fragile and hard to maintain.

**The solution:** DBTK provides a single, consistent interface for reading all common file formats. Whether you're reading CSV, Excel, JSON, XML, or fixed-width files, the API is identical. Even better - `get_reader()` automatically detects the format from the file extension.

```python
from dbtk import readers

# CSV files
with readers.CSVReader(open('northern_water_tribe_census.csv')) as reader:
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

# JSON files
with readers.JSONReader(open('eastern_air_temple.json')) as reader:
    for monk in reader:
        print(f"Air Nomad: {monk.monk_name}, Sky Bison: {monk.sky_bison_companion}")

# XML files with XPath
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

**Automatic format detection:**

Let DBTK figure out what you're reading:

```python
# Automatically detects format from extension
with dbtk.readers.get_reader('data.xlsx') as reader:
    for record in reader:
        process(record)
```

**Common reader parameters:**

All readers support these parameters for controlling input processing:

```python
# Skip first 10 data rows, read only 100 records, return dicts instead of Records
reader = dbtk.readers.CSVReader(
    open('data.csv'),
    skip_records=10,      # Skip N records after headers (useful for bad data)
    max_records=100,      # Only read first N records (useful for testing/sampling)
    return_type='dict',   # 'record' (default) or 'dict' for OrderedDict
    add_rownum=True,      # Add '_row_num' field to each record (default True)
    clean_headers=dbtk.readers.Clean.LOWER_NOSPACE  # Header cleaning level
)

# Row numbers track position in source file
with dbtk.readers.get_reader('data.csv', skip_records=5) as reader:
    for record in reader:
        print(f"Row {record._row_num}: {record.name}")  # _row_num starts at 6 (after skip)
```

**Header cleaning for messy data:**

Real-world data has messy headers. DBTK can standardize them automatically:

```python
from dbtk.readers import Clean

headers = ["ID #", "Student Name", "Residency Code", "GPA Score", "Has Holds?"]

# C
[Clean.normalize(h, Clean.LOWER_NOSPACE) for h in headers]
# ['id_#', 'student_name', 'residency_code', 'gpa_score', 'has_holds?']

[Clean.normalize(h, Clean.STANDARDIZE) for h in headers]
# ['id', 'studentname', 'residency', 'gpascore', 'hasholds']

# Apply when opening a reader
reader = dbtk.readers.CSVReader(fp, clean_headers=Clean.STANDARDIZE) 
```

This is particularly useful when processing similar files from multiple vendors - standardize the headers and your downstream code stays simple.

### Data Writers

**The problem:** You've queried your data, now you need to export it. Do you write CSV? Excel? JSON? Load it into another database? Each format requires different code and libraries.

**The solution:** DBTK writers provide a unified interface for exporting to any format. All writers accept either a cursor or materialized results (lists of Records/namedtuples/dicts), making it trivial to export the same data to multiple formats.

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

**Export once, write everywhere:**

Since all writers accept materialized results, you can fetch once and export to multiple formats:

```python
# Fetch once
data = cursor.fetchall()

# Export to multiple formats
writers.to_csv(data, 'output.csv')
writers.to_excel(data, 'output.xlsx')
writers.to_json(data, 'output.json')
```

**Quick preview to stdout:**

Pass `None` as the filename to preview data to stdout - perfect for debugging or quick checks:

```python
# Preview first 20 records to console before writing to file
cursor.execute("SELECT * FROM soldiers")
writers.to_csv(cursor, None)  # Prints to stdout

# Then export the full dataset
cursor.execute("SELECT * FROM soldiers")
writers.to_csv(cursor, 'soldiers.csv')
```

### ETL Operations

**The problem:** Production ETL pipelines need field mapping, data validation, type conversions, database function integration, and error handling. Building all of this from scratch for each pipeline is time-consuming and error-prone.

**The solution:** DBTK's ETL framework provides everything you need for production data pipelines, from simple inserts to complex merge operations with validation and transformation.

#### SQL File Execution

Write SQL once with named parameters, run it anywhere. DBTK automatically converts between parameter styles, making your queries truly portable across databases.

```python
# query.sql - write once with named parameters
# SELECT * FROM users WHERE status = :status AND created > :start_date

# Works on ANY database - dbtk handles the conversion
cursor.execute_file('queries/get_users.sql', {
    'status': 'active', 
    'start_date': '2025-01-01'
})

# Behind the scenes:
# Oracle:    :status, :start_date  
# Postgres:  %(status)s, %(start_date)s
# MySQL:     %s, %s (parameters reordered automatically)
# SQLite:    ?, ? (parameters reordered automatically)
```

**One-off queries:**

Portable query handling with `execute_file()`:
- Loads query from file. Use NAMED parameter format (:customer_id) in query
- Converts query to match cursor's parmstyle
- Stores query parameter metadata and automatically converts parameters into format needed to execute query.

```python
# Execute SQL from file with parameters
cursor.execute_file('queries/monthly_report.sql', {
    'start_date': '2025-01-01',
    'end_date': '2025-01-31'
})
results = cursor.fetchall()
```

**Prepared statements for repeated execution:**

When you need to execute the same query many times use `cursor.prepare_file()`. Does query and parameter transformations like `execute_file`, but returns a PreparedStatement object that can be executed repeatedly and behaves like a cursor.

```python
# queries/kingdom_report.sql
# SELECT soldier_id, name, rank, missions_completed
# FROM soldiers
# WHERE kingdom = :kingdom
#   AND rank >= :min_rank
# ORDER BY missions_completed DESC

# Prepare once, execute many times with different parameters
stmt = cursor.prepare_file('queries/kingdom_report.sql')

# Define parameters for each kingdom
kingdoms = [
    {'kingdom': 'Fire Nation', 'min_rank': 'Captain'},
    {'kingdom': 'Earth Kingdom', 'min_rank': 'General'},
    {'kingdom': 'Water Tribe', 'min_rank': 'Warrior'},
    {'kingdom': 'Air Nomad', 'min_rank': 'Master'}
]

# Execute query for each kingdom and export to separate files
for params in kingdoms:
    stmt.execute(params)
    data = stmt.fetchall()  # PreparedStatement acts like a cursor

    filename = f"reports/{params['kingdom'].replace(' ', '_')}.csv"
    dbtk.writers.to_csv(data, filename)
    print(f"Exported {len(data)} {params['kingdom']} soldiers")
```

**Benefits of SQL files:**
- Keep SQL separate from Python code for better organization
- Get syntax highlighting and linting in your editor
- Test queries independently before integration
- Reuse queries across different scripts
- Version control SQL changes separately
- **Write once, run on any database** (the big win!)

#### Table Class for ETL

The Table class provides a stateful interface for complex ETL operations with field mapping, transformations, and validations:

```python
import dbtk
from dbtk.etl import transforms
from dbtk.database import ParamStyle

cursor = dbtk.connect('intel_prod')
# Auto-generate configuration from existing table
config = dbtk.etl.generate_table_config(cursor, 'soldier_training', add_comments=True)

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
        if phoenix_king_army.reqs_met:
            existing_soldier = phoenix_king_army.fetch()
            if existing_soldier:
                phoenix_king_army.exec_update(reqs_checked=True)
            else:
                phoenix_king_army.exec_insert(reqs_checked=True)
        else:
            print(f"Recruit {phoenix_king_army.values['name']}({phoenix_king_army.values['soldier_id']}) rejected: missing {phoenix_king_army.reqs_missing}")
            

print(f"Processed {phoenix_king_army.counts['insert'] + phoenix_king_army.counts['update']} soldiers")
```

**Column configuration schema:**

Each database column is configured with a dictionary specifying how to source and transform its value.

```python
{
    'database_column_name': {
        # DATA SOURCE
        'field': 'source_field_name',      # Map from input record field       
        'default': 'static_value',           # Use a default value for all records
        'fn': transform_function,          # Python function to transform field value, no parens!
        'db_expr': 'DATABASE_FUNCTION(#)',   # Call database function (e.g., CURRENT_TIMESTAMP, UPPER(#))    

        # VALIDATION - optional:
        'nullable': False,                 # Require value (default: True allows NULL)
        'primary_key': True,               # Mark as primary key (implies nullable=False)

        # UPDATE CONTROL - optional:
        'no_update': True,                 # Exclude from UPDATE operations (default: False)
    }
}
```

**Column configuration examples:**

```python
columns_config = {
    # Simple field mapping
    'user_id': {'field': 'id', 'primary_key': True},

    # Field with transformation
    'email': {'field': 'email_address', 'fn': email_clean},

    # Field with validation
    'full_name': {'field': 'name', 'nullable': False},

    # Multiple transformations (compose your own function)
    'phone': {'field': 'phone_number', 'fn': lambda x: phone_format(phone_clean(x))},

    # Static value for all records
    'status': {'default': 'active'},
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

### 2. Null Conversion
The value matches any entries in table.null_values it will be set to `None`. 
This is configurable but the default is: `('', 'NULL', '<null>', '\\N')`

### 3. Default Fallback
If value is `None` or `''`, apply **default** if defined.

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

**Handling Incomplete Records:**

DBTK supports two patterns for handling incomplete records:

```python
# Pattern 1: Tables expected to be complete - let exec methods handle validation
# Use this when you want to track all incomplete records
for record in records:
    soldier_table.set_values(record)
    # exec_* functions automatically validate keys and required columns have values
    soldier_table.exec_insert(raise_error=False)  # Track incomplete, don't raise

print(f"Inserted: {soldier_table.counts['insert']}")
print(f"Skipped (incomplete data): {soldier_table.counts['incomplete']}")

# Pattern 2: "Optional" tables, check requirements before executing DML
# Use this when missing data is expected and you want to skip incomplete records.
# If you call exec_insert(raise_error=False) with many incomplete records you will flood 
# your logs.
for record in records:
    recruit_table.set_values(record)
    if recruit_table.reqs_met: #
        recruit_table.exec_insert(reqs_checked=True)  # Skip redundant validation
    # Records with missing data are silently skipped. 

print(f"Inserted: {recruit_table.counts['insert']}")

# Pattern 3: Strict mode - raise errors on incomplete data
# Use this when all data must be complete
for record in records:
    critical_table.set_values(record)
    critical_table.exec_insert(raise_error=True)  # Raises ValueError if incomplete
```

**Performance tip:** Use `reqs_checked=True` when you've already validated requirements to avoid redundant checks:

```python
if address_table.reqs_met:  # Check once
    address_table.exec_insert(reqs_checked=True)  # Skip redundant check
```

#### Bulk Operations with DataSurge

**The problem:** Processing thousands or millions of records row-by-row is painfully slow. You need batching, but implementing it correctly is complex.

**The solution:** DataSurge handles batching, error tracking, and optimal merge strategies automatically. It's built for high-volume data processing.

```python
from dbtk.etl import DataSurge

# Define table configuration
recruit_table = dbtk.etl.Table('fire_nation_soldiers', columns_config, cursor)

# Create DataSurge instance for bulk operations
bulk_writer = DataSurge(recruit_table)

# Bulk insert with batching
with dbtk.readers.get_reader('massive_conscript_list.csv') as reader:
    errors = bulk_writer.insert(cursor, reader, batch_size=2000)
    print(f"Inserted {recruit_table.counts['insert']} records with {errors} errors")

# Bulk merge (upsert) operations
with dbtk.readers.get_reader('soldier_updates.csv') as reader:
    errors = bulk_writer.merge(cursor, reader, batch_size=1000)
```

**DataSurge features:**
- Automatic batching for optimal performance
- Smart merge strategies (native MERGE vs temp table based on database capabilities)
- Configurable error handling
- Progress tracking and logging
- Support for INSERT, UPDATE, DELETE, MERGE operations

**Performance impact:** DataSurge can be 10-100x faster than row-by-row operations, depending on your database and network latency.

#### Data Transformations

Built-in transformation functions handle common data cleaning tasks:

```python
from dbtk.etl import transforms as tx

# Date and time parsing with flexible formats
tx.parse_date("Year 100 AG, Day 15")
tx.parse_datetime("100 AG Summer Solstice T14:30:00Z")  # With timezone support
tx.parse_timestamp("1642262200")  # Unix timestamp support

# International phone number handling (requires phonenumbers library)
tx.phone_clean("5551234567")              # -> "(555) 123-4567"
tx.phone_format("+44 20 7946 0958", tx.PhoneFormat.NATIONAL)  # UK format
tx.phone_validate("+1-800-AVATAR")        # Validation
tx.phone_get_type("+1-800-CABBAGES")      # -> "toll_free"

# Email validation and cleaning
tx.email_validate("guru.pathik@eastern.air.temple")  # -> True
tx.email_clean("  TOPH@BEIFONG.EARTHKINGDOM ")      # -> "toph@beifong.earthkingdom"

# Utility functions
tx.coalesce([None, "", "Jasmine Tea", "Ginseng Tea"])  # -> "Jasmine Tea"
tx.indicator("Firebender", true_val="Fire Nation Citizen")  # Conditional values
tx.get_int("123.45 gold pieces")  # -> 123

```

### Database Lookups and Validation

TableLookup is a fast, cache-aware transform that turns any database table or view into a reusable lookup function.  It uses PrepareStatement, so it is portable across databases. Use the high-level Lookup() and Validate() factories directly in your Table column definitions to resolve codes, enrich records, or enforce referential integrity with almost no code.

```python
import dbtk
from dbtk.etl.transforms import TableLookup, Lookup, Validate
db = dbtk.connect('states_db') 
cur = db.cursor()

# TableLookup requires an active cursor
state_lookup = TableLookup(cursor=cur, table='states', key_cols='state', return_cols='abbrev', 
                           cache=TableLookup.CACHE_PRELOAD)
state_lookup({'state': 'Pennsylvania'}) # -> 'PA'

# Multiple return_cols return type will be based on cursor type (record, dict, namedtuple, list) 
state_details = TableLookup(cursor=cur, table='states', key_cols='code', return_cols=['state', 'capital', 'region'])
state_details({'code': 'CA'}) # -> Record('California', 'Sacramento', 'West')

# Lookup and Validate defer cursor binding until Table is initialized
recruit_table = dbtk.etl.Table('recruit', columns={
    'id': {'field': 'soldier_id', 'key': True},
    'name': {'field': 'name', 'nullable': False},
    'state_code1': {'field': 'state_name', 'fn': Lookup('states', 'state', 'code', cache=TableLookup.CACHE_PRELOAD)}, # lookup code from state name
    'state_code2': {'field': 'state_name', 'fn': 'lookup:states:state:code:preload'}, # string shortcut for lookup above
    'capital': {'field': 'state_name', 'fn': 'lookup:states:state:capital'},
    'region1': {'field': 'region', 'fn': Validate('valid_regions','region_name')}, 
    'region2': {'field': 'region', 'fn': 'validate:valid_regions:region_name'}}, # short cut for validation above
    cursor=cur)
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

#### Logging for Integration Scripts

**The problem:** Integration scripts need proper logging with timestamped files, separate error logs, and easy cleanup. Setting this up manually is repetitive and error-prone.

**The solution:** DBTK provides `setup_logging()` and `cleanup_old_logs()` to handle the common pattern of creating timestamped log files like `script_name_20251031_154505.log`.

```python
import dbtk
import logging

# One-line setup with automatic script name detection
dbtk.setup_logging()  # Creates logs/my_script_20251031_154505.log

# Or specify name and options
dbtk.setup_logging('fire_nation_etl', log_dir='/var/log/etl', level='DEBUG')

# Now use standard Python logging
logger = logging.getLogger(__name__)
logger.info("Starting ETL process...")
logger.error("Failed to process record")
```

**Configuration options** (via `dbtk.yml` or function parameters):

```yaml
settings:
  logging:
    directory: ./logs                   # Where to write logs
    level: INFO                          # DEBUG, INFO, WARNING, ERROR, CRITICAL
    format: '%(asctime)s [%(levelname)s] %(name)s: %(message)s'
    timestamp_format: '%Y-%m-%d %H:%M:%S'  # For log message timestamps
    filename_format: '%Y%m%d_%H%M%S'    # For log filenames
    split_errors: true                   # Separate _error.log for errors
    console: true                        # Also output to console
    retention_days: 30                   # For cleanup_old_logs()
```

**Filename patterns:**

```python
# One log per run (default)
# filename_format: '%Y%m%d_%H%M%S'
# Creates: script_20251031_154505.log

# One log per day
# filename_format: '%Y%m%d'
# Creates: script_20251031.log

# Single rolling log file
# filename_format: ''
# Creates: script.log (overwrites each run)
```

**Automatic log cleanup:**

```python
# Clean logs older than retention period (default: 30 days)
deleted = dbtk.cleanup_old_logs()
print(f"Deleted {len(deleted)} old log files")

# Custom retention
dbtk.cleanup_old_logs(retention_days=7)

# Dry run to see what would be deleted
would_delete = dbtk.cleanup_old_logs(dry_run=True)
```

**Error detection for notifications:**

When running unattended integration scripts, you often want to send notification emails if errors occurred. The `errors_logged()` function makes this trivial:

```python
import dbtk
import logging

# Setup logging with split_errors=True (default)
dbtk.setup_logging('fire_nation_etl')

logger = logging.getLogger(__name__)

# ... do your ETL work ...
try:
    process_data()
except Exception as e:
    logger.error(f"Processing failed: {e}")

# Check if any errors were logged
error_log = dbtk.errors_logged()
if error_log:
    print(f"Errors detected! See: {error_log}")
    # send_notification_email(subject="ETL errors", attachment=error_log)
else:
    print("Integration completed successfully")
```

**How it works:**
- Returns `None` if no errors were logged
- Returns error log path if `split_errors=True` (separate _error.log file)
- Returns main log path if `split_errors=False` (errors in combined log)
- Automatically tracks ERROR and CRITICAL level messages
- Works regardless of logging configuration

**Note for advanced users:** Error tracking is implemented via a custom `ErrorCountHandler` that's automatically added to the root logger by `setup_logging()`. This handler maintains an error counter that `errors_logged()` checks. You can access this handler directly via `logging.getLogger().handlers` if you need custom error tracking logic.

**What DBTK logs automatically:**

DBTK logs all operations without you writing any log statements:
- Database connections and queries
- File reading operations and errors
- Table operations (INSERT/UPDATE/MERGE counts, validation failures)
- Data transformation errors
- Parameter conversions and SQL generation

You only need to add custom logging for your specific business logic.

**When to add custom logging:**

Add your own log statements when you have:
- Custom validation or business rules
- External API calls
- Complex decision logic
- Non-standard error handling

**Complete integration script example with custom logging:**

```python
#!/usr/bin/env python3
"""Fire Nation intelligence ETL with custom validation logging."""

import dbtk
import logging

# Set up logging - creates dated log files automatically
dbtk.setup_logging()

# Optional: Create logger only if you need custom log messages
logger = logging.getLogger(__name__)

def validate_combat_readiness(soldier_data):
    """Custom business rule - log only your specific logic."""
    if soldier_data['missions_completed'] < 5 and soldier_data['rank'] == 'General':
        logger.warning(f"General {soldier_data['name']} has insufficient mission experience")
        return False
    return True

def main():
    with dbtk.connect('fire_nation_db') as db:
        cursor = db.cursor()

        soldier_table = dbtk.etl.Table('soldiers', config, cursor)

        with dbtk.readers.get_reader('conscripts.csv') as reader:
            for record in reader:
                soldier_table.set_values(record)

                # Custom validation - log only when YOU need to
                if soldier_table.reqs_met and not validate_combat_readiness(record):
                    continue  # Skip this record

                soldier_table.exec_insert(raise_error=False)
                # ↑ DBTK automatically logs all insert operations, errors, validation failures

        # Summary output (or log it if you prefer)
        print(f"Processed {soldier_table.counts['insert']} soldiers")
        print(f"Skipped {soldier_table.counts['incomplete']} incomplete records")

        db.commit()

if __name__ == '__main__':
    main()
    dbtk.cleanup_old_logs()

    # Check if errors occurred (DBTK tracked them automatically)
    error_log = dbtk.errors_logged()
    if error_log:
        print(f"Errors occurred - check {error_log}")
        # send_notification_email(subject="ETL Errors", attachment=error_log)
```

**Key takeaway:** DBTK does the logging heavy lifting. You only add custom log statements for your specific business logic, not for database operations, file reading, or ETL mechanics.

**Benefits:**
- **Automatic setup** - Sample config created at `~/.config/dbtk.yml` on first use
- **Timestamped files** - Never overwrite important logs
- **Split error logs** - Easy monitoring and alerting
- **Standard logging** - Works with all Python logging features
- **Configurable** - Control via config file or function arguments

## Configuration & Security

**The problem:** Hardcoded credentials are a security nightmare, and managing different database connections across environments is tedious.

**The solution:** DBTK uses YAML configuration files with support for encrypted passwords and environment variables. Store credentials securely, version control your configuration (without passwords), and switch between environments effortlessly.

### Configuration File Format

Create a `dbtk.yml` file for your database connections. Configurations can be project-scoped (`./dbtk.yml`) or user-scoped (`~/.config/dbtk.yml`).

```yaml
settings:
  default_timezone: UTC
  default_country: US
  default_paramstyle: named

connections:
  # PostgreSQL with encrypted password
  water_tribe_main:
    type: postgres
    host: localhost
    port: 5432
    database: northern_water_tribe
    user: waterbender_admin
    encrypted_password: gAAAAABh...

  # Oracle with environment variable
  earth_kingdom_prod:
    type: oracle
    host: ba-sing-se.earthkingdom.gov
    port: 1521
    database: CITIZEN_DB
    user: dai_li_ops
    password: ${EARTH_KINGDOM_PASSWORD}

  # MySQL with custom driver
  fire_nation_archive:
    type: mysql
    driver: mysqlclient
    host: fire-lord-palace.fn.gov
    port: 3306
    database: historical_records
    user: phoenix_king_admin
    password: sozins_comet_2024

  # SQLite local database
  air_nomad_local:
    type: sqlite
    database: /path/to/air_temples.db

passwords:
  # Standalone encrypted passwords for API keys, etc.
  api_key_avatar_hotline:
    encrypted_password: gAAAAABh...
    description: Avatar hotline API key
  
  # Environment variable password
  secret_tunnel:
    password: ${SECRET_TUNNEL_PASSWORD}
    description: Secret tunnel access code
```

### Password Encryption

Secure your credentials with encryption:

```python
import dbtk

# Generate encryption key (store in DBTK_ENCRYPTION_KEY environment variable)
key = dbtk.config._generate_encryption_key()

# Encrypt all passwords in configuration file
dbtk.config.encrypt_config_file('fire_nation_secrets.yml')

# Retrieve encrypted password
sozin_secret = dbtk.config.get_password('phoenix_king_battle_plans')

# Manually encrypt a single password
encrypted = dbtk.config.encrypt_password('only_azula_knows_this')

# Migrate configuration with new encryption key
new_key = dbtk.config._generate_encryption_key()
dbtk.config.migrate_config('old_regime.yml', 'phoenix_king_era.yml',
                           new_encryption_key=new_key)
```

### Command Line Tools

DBTK provides command-line utilities for managing encryption keys and configuration files. These are especially useful for automating deployment and configuration management in CI/CD pipelines:

```bash
# Generate a new encryption key
# Store the output in DBTK_ENCRYPTION_KEY environment variable
dbtk generate-key

# Store key on system keyring. If no key is provided, a new one will be generated
# Use --force to overwrite an existing key
# The DBTK_ENCRYPTION_KEY environment variable takes precedence
dbtk store-key [your_key] --force 

# Encrypt all passwords in a configuration file
# Prompts for each plaintext password and replaces with encrypted_password
dbtk encrypt-config ./dbtk.yml

# Encrypt a specific password
# Returns the encrypted string you can paste into your config
dbtk encrypt-password "sozins_comet_2024"

# Migrate config file to a new encryption key
# Useful when rotating encryption keys
export DBTK_ENCRYPTION_KEY="old_key_here"
dbtk migrate-config old_config.yml new_config.yml --new-key "new_key_here"

# Run a check of which recommended libraries, database drivers are installed
# and check configuration, encryption keys, etc.
dbtk checkup
```

**Common workflow for new deployments:**

```bash
# 1. Generate encryption key and save to environment
export DBTK_ENCRYPTION_KEY=$(dbtk generate-key)

# 2. Create config file with plaintext passwords
cat > dbtk.yml <<EOF
connections:
  production_db:
    type: postgres
    host: db.example.com
    user: admin
    password: my_secret_password
EOF

# 3. Encrypt all passwords in config
dbtk encrypt-config dbtk.yml

# 4. Verify - passwords should now be encrypted_password entries
cat dbtk.yml
```

**Key rotation workflow:**

```bash
# When rotating encryption keys for security
export DBTK_ENCRYPTION_KEY="current_key"
NEW_KEY=$(dbtk generate-key)

# Decrypt with old key, encrypt with new key
dbtk migrate-config dbtk.yml dbtk_new.yml --new-key "$NEW_KEY"

# Update environment variable and swap files
export DBTK_ENCRYPTION_KEY="$NEW_KEY"
mv dbtk.yml dbtk_old.yml
mv dbtk_new.yml dbtk.yml
```

## Advanced Features

### Multiple Configuration Locations

DBTK searches for configuration files in this order:
1. `./dbtk.yml` (project-specific)
2. `~/.config/dbtk.yml` (user-specific)
3. Custom path via `set_config_file()`

This lets you maintain per-project configurations while having a fallback for personal databases.

### Custom Driver Registration

If you're using a database driver not built into DBTK, you can register it:

```python
from dbtk.database import register_user_drivers

custom_drivers = {
    'my_postgres_fork': {
        'database_type': 'postgres',
        'priority': 10,
        'param_map': {'database': 'dbname'},
        'required_params': [{'host', 'database', 'user'}],
        'optional_params': {'port', 'password'},
        'connection_method': 'kwargs',
        'default_port': 5432
    }
}

register_user_drivers(custom_drivers)
```

## Database Support

DBTK supports multiple database adapters with automatic detection and fallback:

| Database    | Driver           | Install Command                      | Notes                                                     |
|-------------|------------------|--------------------------------------|-----------------------------------------------------------|
| PostgreSQL  | psycopg2         | `pip install psycopg2-binary`        | Recommended, most mature                                  |
| PostgreSQL  | psycopg (3)      | `pip install psycopg-binary`         | Newest version, async support                             |
| PostgreSQL  | pgdb             | `pip install pgdb`                   | DB-API compliant                                          |
| Oracle      | oracledb         | `pip install oracledb`               | Thin mode - no Oracle client required (recommended)       |
| Oracle      | cx_Oracle        | `pip install cx_Oracle`              | Requires Oracle client installation                       |
| MySQL       | mysqlclient      | `pip install mysqlclient`            | Fastest option, C extension                               |
| MySQL       | mysql.connector  | `pip install mysql-connector-python` | Official MySQL connector, pure Python                     |
| MySQL       | pymysql          | `pip install pymysql`                | Pure Python, lightweight                                  |
| SQL Server  | pyodbc           | `pip install pyodbc`                 | ODBC driver required on system                            |
| SQL Server  | pymssql          | `pip install pymssql`                | Lightweight, DB-API compliant, no ODBC needed             |
| SQLite      | sqlite3          | Built-in                             | No installation needed                                    |

**Driver priority:** DBTK automatically selects the best available driver based on priority. You can override this by specifying `driver='driver_name'` in your connection.

**Note:** ODBC adapters require appropriate ODBC drivers to be installed on the system.

## Performance Tips

1. **Use appropriate batch sizes** - Larger batches are faster but use more memory:
   ```python
   bulk_writer.insert(cursor, records, batch_size=5000)  # Tune based on your data
   ```

2. **Choose the right cursor type** - Records offer the best balance of functionality and performance:
   ```python
   cursor = db.cursor('record')  # Recommended default
   cursor = db.cursor('list')    # When you only need positional access
   ```

3. **Materialize results when needed** - Don't fetch twice:
   ```python
   data = cursor.fetchall()  # Fetch once
   dbtk.writers.to_csv(data, 'output.csv')
   dbtk.writers.to_excel(data, 'output.xlsx')
   ```

4. **Use transactions for bulk operations** - Commit once for many inserts:
   ```python
   with db.transaction():
       for record in records:
           table.set_values(record) 
           table.exec_insert()
   ```

5. **Use DataSurge for bulk operations** - Much faster than row-by-row:
   ```python
   bulk_writer = DataSurge(table)
   bulk_writer.insert(cursor, records, batch_size=2000)
   ```

6. **Use prepared statements for repeated queries** - Read and parse SQL once:
   ```python
   stmt = cursor.prepare_file('query.sql')
   for params in parameter_sets:
       stmt.execute(params)
   ```

7. **Let the database do the work** - Use `db_expr` in Table definitions to leverage database functions instead of processing in Python.

## License

MIT License - see LICENSE file for details.

## Acknowledgments

Documentation, testing and architectural improvements assisted by [Claude](https://claude.ai) (Anthropic).

## Support

- **Issues**: [GitHub Issues](https://github.com/yourusername/dbtk/issues)
- **Discussions**: [GitHub Discussions](https://github.com/yourusername/dbtk/discussions)
- **Documentation**: [Full Documentation](https://dbtk.readthedocs.io)

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.