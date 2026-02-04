# DBTK - Data Benders Toolkit

<div style="float: right; padding: 20px">
    <img src="/docs/assets/databender.png" height="240" align="right" />
</div>

**Control and Manipulate the Flow of Data** - A lightweight Python toolkit for data integration, transformation, and movement between systems.

Like the elemental benders of Avatar, this library gives you precise control over data, the world's most rapidly growing element.
Extract data from various sources, transform it through powerful operations, and load it exactly where it needs to go.
This library is designed by and for data integrators.

**DBTK aims to be fast and memory-efficient at every turn.** But it was designed to boost your productivity first and foremost.
You have dozens (possibly hundreds) of interfaces, impossible deadlines, and multiple projects all happening at once. Your
environment has three or more different relational databases. You just want to get stuff done instead of writing the same
boilerplate code over and over or stressing because that database system you hardly ever use is so different from the one
you use every day.  

**Design philosophy:** Modern databases excel at aggregating and transforming data at scale. DBTK embraces
this by focusing on what Python does well: flexible record-by-record transformations,
connecting disparate systems, and orchestrating data movement.

If you need to pivot, aggregate, or perform complex SQL operations - write SQL and let
your database handle it. If you need dataframes and heavy analytics - reach for Pandas
or polars. DBTK sits in between: getting your data where it needs to be, cleaned and
validated along the way.

## The Record Class

Every cursor query and file reader in DBTK returns **Record** objects - a hybrid data structure that works like a dict, tuple, and object simultaneously.

**Why not just use dicts?** Dicts are optimized for n=1: one object with many keys you look up dynamically. But ETL pipelines process hundreds of thousands or millions of rows, all with the same columns. Record stores column names once on a shared class, not on every row - giving you dict-like flexibility with tuple-like memory efficiency.

```python
for row in cursor:
    row['name']           # Dict-style access
    row.name              # Attribute access
    row[0]                # Index access (dicts can't do this)
    row[1:3]              # Slicing (dicts can't do this)
    id, name, email = row # Tuple unpacking (dicts can't do this)
    row.get('phone', '')  # Safe access with default
    dict(row)             # Convert to dict when needed
```

**Normalized field names** let you write resilient code. Whether your source column is `Employee_ID`, `EMPLOYEE ID`, or `employee_id`, you can always access it as `row.employee_id`. This means your Table field mappings work regardless of how the source system names its columns.

See [Record Objects](docs/record.md) for complete documentation.

## Features

- **Universal Database Connectivity** - Unified interface across PostgreSQL, Oracle, MySQL, SQL Server, and SQLite with intelligent driver auto-detection
- **Portable SQL Queries** - Write SQL once with named parameters, runs on any database regardless of parameter style
- **Smart Cursors** - All cursors return Record objects with the speed of tuples and the flexibility of dicts
- **Flexible File Reading** - CSV, Excel (XLS/XLSX), JSON, NDJSON, XML, and fixed-width text files with consistent API
- **Transparent Compression** - Automatic decompression of .gz, .bz2, .xz, and .zip files with smart member selection
- **Multiple Export Formats** - Write to CSV, Excel, JSON, NDJSON, XML, fixed-width text, or directly between databases
- **Advanced ETL Framework** - Full-featured Table class for complex data transformations, validations, and upserts
- **Data Transformations** - Built-in functions for dates, phones, emails, and custom data cleaning with international support
- **High-Performance Bulk Operations** - DataSurge for blazing-fast batch operations; BulkSurge for even faster direct loading when supported
- **Integration Logging** - Timestamped log files with automatic cleanup, split error logs, and zero-config setup
- **Encrypted Configuration** - YAML-based config with password encryption and environment variable support

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
dbtk.setup_logging()

with dbtk.connect('fire_nation_db') as db:
    cursor = db.cursor()

    # SQL with named parameters - works on ANY database
    # Supports both :named and %(pyformat)s parameter styles!
    params = {
        'min_rank': 'Captain',
        'start_date': '2024-01-01',
        'region': 'Western Fleet',
        'status': 'active'
    }

    # DBTK transforms the query and parameters to match your database's style
    cursor.execute_file('queries/monthly_report.sql', params)
    monthly_data = cursor.fetchall()

    cursor.execute_file('queries/officer_summary.sql', params)
    summary_data = cursor.fetchall()

    # Export to multiple formats trivially
    dbtk.writers.to_csv(monthly_data, 'reports/soldiers_monthly.csv')
    dbtk.writers.to_excel(summary_data, 'reports/officer_summary.xlsx',
                          sheet='Officer Stats')

# Check for errors
if dbtk.errors_logged():
    print("⚠️  Export completed with errors - check log file")
```

**What makes this easy:**
- Write SQL once with named (`:param`) or pyformat (`%(param)s`) parameters - works on any database
- Pass the same dict to multiple queries - extra params ignored, missing params become NULL
- DBTK handles parameter conversion automatically - no manual string formatting needed
- Export to CSV/Excel/JSON with one line of code

### Sample Inbound Integration - Import Data

Import data with field mapping, transformations, and validation:

```python
import dbtk
from dbtk.etl import Table
from dbtk.etl.transforms import email_clean

dbtk.setup_logging()  # Timestamped logs with auto-cleanup

with dbtk.connect('fire_nation_db') as db:
    cursor = db.cursor()

    # Define table schema with field mapping and transformations
    soldier_table = Table('soldiers', {
        'soldier_id': {'field': 'id', 'key': True},  # Maps CSV 'id' to DB 'soldier_id', marks as primary key
        'name': {'field': 'full_name', 'nullable': False},  # Required field, will error if missing
        'rank': {'field': 'officer_rank', 'nullable': False,
                 'fn': 'validate:ranks:rank_code:preload'},  # Validates against 'ranks' table, preloads cache
        'email': {'field': 'contact_email', 'default': 'intel@firenation.com',
                  'fn': email_clean},  # Cleans/validates email, uses default if missing
        'enlistment_date': {'field': 'join_date', 'fn': 'date'},  # Parses various date formats
        'missions_completed': {'field': 'mission_count', 'fn': 'int'},  # Converts to integer, NULL if fails
        'status': {'default': 'active'}  # Sets default, no source field needed
    }, cursor=cursor)

    # Process incoming compressed CSV
    with dbtk.readers.get_reader('incoming/new_recruits.csv.gz') as reader:  # Auto-detects .gz, decompresses
        # DataSurge batches inserts, uses fastest method for this database driver
        surge = dbtk.etl.DataSurge(soldier_table, use_transaction=True)  # Wraps in transaction
        surge.insert(reader)  # Auto-shows progress bar for large files

if dbtk.errors_logged():  # Check global error flag
    # send notification email or call 911
    print("⚠️  Export completed with errors - check log file")
```

**What makes this easy:**
- Field mapping separates database schema from source data format - change one without touching the other
- Built-in transforms (dates, emails, integers) with string shorthand - `'fn': 'date'` instead of importing functions
- Table class auto-validates required data before operations - no silent failures or cryptic database errors
- Built-in table lookups and validation with deferred cursor binding and intelligent caching
- Readers auto-detect file size and show progress on large files - never wonder if your pipeline has stalled
- Automatic statistics tracking - records processed, skipped, inserted, etc.
- Automatic logging with sensible global defaults - override per-pipeline when needed
- Error tracking built-in - `dbtk.errors_logged()` tells you if anything went wrong

## Documentation

- **[Record Objects](docs/record.md)** - DBTK's universal data structure with dict, tuple, and attribute access
- **[Configuration & Security](docs/configuration.md)** - Set up encrypted passwords, YAML config files, and command-line tools
- **[Database Connections](docs/database-connections.md)** - Connect to any database, use smart cursors, manage transactions
- **[Readers & Writers](docs/readers-writers.md)** - Read from and write to CSV, Excel, JSON, XML, fixed-width files
- **[ETL Framework](docs/etl.md)** - Build production ETL pipelines with Table, DataSurge, BulkSurge, transforms, and logging
- **[Advanced Features](docs/advanced.md)** - Custom drivers, multiple config locations, and performance tuning

## Performance Highlights

**Driver optimizations enabled automatically** - If your database driver supports faster batch operations (psycopg2, pyodbc), DBTK detects and uses them automatically.

Real-world benchmarks from production systems:

- **DataFrameReader**: 1.3M rec/s reading compressed CSV with polars + transforms
- **BulkSurge (Postgres)**: 220K rec/s transforming, validating, and bulk loading
- **DataSurge (Oracle/SQL Server/MySQL)**: 90-120K rec/s with native executemany
- **IMDB Dataset**: 132K rec/s loading 12M titles with transforms and validation

These aren't toy benchmarks - they're real ETL pipelines with field mapping, data validation, type conversions, and database constraints.

## License

MIT License - see LICENSE file for details.

## Acknowledgments

Documentation, testing and architectural improvements assisted by [Claude](https://claude.ai) (Anthropic).
Architectural review and witty banter by [Grok](https://grok.com/) (xAI).

## Support

- **Issues**: [GitHub Issues](https://github.com/yourusername/dbtk/issues)
- **Discussions**: [GitHub Discussions](https://github.com/yourusername/dbtk/discussions)
- **Documentation**: [Full Documentation](https://dbtk.readthedocs.io)

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.
