# DBTK - Data Benders Toolkit
<img src="/docs/assets/databender.png" height="320" style="float: right; padding-left: 50px"/>

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

```python
import dbtk

# Connect using YAML config
with dbtk.connect('fire_nation_prod') as db:
    cursor = db.cursor()
    cursor.execute("SELECT * FROM firebenders WHERE rank = 'General'")
    
    # Materialize results for multiple outputs
    generals = cursor.fetchall()
    
    # Export to different formats
    dbtk.writers.to_excel(generals, 'fire_nation_generals.xlsx')
    dbtk.writers.to_csv(generals, 'fire_nation_generals.csv')
```

**ETL Example with transformations:**

```python
import dbtk
from dbtk.etl import Table
from dbtk.etl.transforms import 
from dbtk.etl.transforms.datetime import parse_date, parse_datetime
from dbtk.etl.transforms.email import email_clean
from dbtk.etl.transforms.phone import Phone, phone_clean

with dbtk.connect('avatar_training_grounds') as db:
    cursor = db.cursor()

    # Define ETL table with transformations
    recruit_table = Table('air_nomads', {
        'nomad_id': {'field': 'id', 'primary_key': True},
        'name': {'field': 'full_name', 'nullable': False},
        'phone': {'field': 'phone_number', 'fn': phone_clean},
        'email': {'field': 'contact_scroll', 'fn': email_clean, 'nullable': False},
        'sky_bison': {'field': 'companion_name'},
        'training_date': {'field': 'started_training', 'fn': parse_date},
        'airbending_level': {'field': 'mastery_level', 'db_fn': 'calculate_airbending_rank(#)'},
        'last_meditation': {'db_fn': 'CURRENT_TIMESTAMP'},
        'temple_origin': {'value': 'Eastern Air Temple'}},
        cursor=cursor)

    # Process records with validation and transformation
    with dbtk.readers.get_reader('new_air_nomad_recruits.csv') as reader:
        for recruit in reader:
            recruit_table.set_values(recruit)
            if recruit_table.reqs_met:
                recruit_table.exec_insert(reqs_checked=True)  # Skip redundant check
            else:
                print(f"Recruit needs more training: missing {recruit_table.reqs_missing}")

    # Check processing results
    print(f"Inserted: {recruit_table.counts['insert']} recruits")
    print(f"Skipped: {len([r for r in reader if not recruit_table.reqs_met])} incomplete records")
```

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

**Cursor types matter:**

Choose the right cursor type for your use case to balance functionality and performance:

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

When you need to execute the same query many times, `prepare_file()`. Does query and parameter transformations like `execute_file`, but returns a PreparedStatement object that can be executed repeatedly and behaves like a cursor.

```python
# Prepare once
stmt = cursor.prepare_file('queries/insert_user.sql')

# Execute many times
for user_data in import_data:
    stmt.execute({
        'user_id': user_data['id'],
        'name': user_data['name'],
        'email': user_data['email']
    })

# PreparedStatement acts like a cursor - fetch directly
stmt = cursor.prepare_file('queries/get_active_users.sql')
stmt.execute({'status': 'active', 'min_logins': 5})
for user in stmt:
    process_user(user) 
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
    'combat_name': {'field': 'full_name', 'db_fn': 'generate_fire_nation_callsign(#)'},
    'last_drill': {'db_fn': 'CURRENT_TIMESTAMP'},
    'conscription_source': {'value': 'Sozin Recruitment Drive'}},
                                   cursor=cursor)

# Process records with validation and upsert logic
with dbtk.readers.get_reader('fire_nation_conscripts.csv') as reader:
    phoenix_king_army.calc_update_excludes(reader.headers)
    for recruit in reader:
        phoenix_king_army.set_values(recruit)
        if phoenix_king_army.reqs_met:
            existing_soldier = phoenix_king_army.fetch()
            if existing_soldier:
                phoenix_king_army.exec_update(reqs_checked=True)
            else:
                phoenix_king_army.exec_insert(reqs_checked=True)
        else:
            print(f"Recruit rejected: missing {phoenix_king_army.reqs_missing}")

print(f"Processed {phoenix_king_army.counts['insert'] + phoenix_king_army.counts['update']} soldiers")
```

**Key features:**
- Field mapping and renaming
- Data type transformations
- Database function integration (`db_fn` lets you leverage database capabilities)
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

**Complete integration script example:**

```python
#!/usr/bin/env python3
"""Fire Nation intelligence ETL."""

import dbtk
import logging

# Set up logging - creates dated log files automatically
dbtk.setup_logging()

logger = logging.getLogger(__name__)

def main():
    logger.info("Starting Fire Nation ETL")

    try:
        with dbtk.connect('fire_nation_db') as db:
            cursor = db.cursor()

            # Your ETL logic
            soldier_table = dbtk.etl.Table('soldiers', config, cursor)

            with dbtk.readers.get_reader('conscripts.csv') as reader:
                for record in reader:
                    soldier_table.set_values(record)
                    soldier_table.exec_insert(raise_error=False)

            logger.info(f"Processed {soldier_table.counts['insert']} soldiers")
            logger.info(f"Skipped {soldier_table.counts['incomplete']} incomplete records")

            db.commit()

    except Exception as e:
        logger.error(f"ETL failed: {e}", exc_info=True)
        raise

if __name__ == '__main__':
    main()

    # Clean up old logs
    dbtk.cleanup_old_logs()
```

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
key = dbtk.config.generate_encryption_key()

# Encrypt all passwords in configuration file
dbtk.config.encrypt_config_file_cli('fire_nation_secrets.yml')

# Retrieve encrypted password
sozin_secret = dbtk.config.get_password('phoenix_king_battle_plans')

# Manually encrypt a single password
encrypted = dbtk.config.encrypt_password_cli('only_azula_knows_this')

# Migrate configuration with new encryption key
new_key = dbtk.config.generate_encryption_key()
dbtk.config.migrate_config_cli('old_regime.yml', 'phoenix_king_era.yml', 
                                new_encryption_key=new_key)
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
   writers.to_csv(data, 'output.csv')
   writers.to_excel(data, 'output.xlsx')
   ```

4. **Use transactions for bulk operations** - Commit once for many inserts:
   ```python
   with db.transaction():
       for record in records:
           table.exec_insert(cursor)
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

7. **Let the database do the work** - Use `db_fn` in Table definitions to leverage database functions instead of processing in Python.

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