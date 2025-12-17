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

## Features

- **Universal Database Connectivity** - Unified interface across PostgreSQL, Oracle, MySQL, SQL Server, and SQLite with intelligent driver auto-detection
- **Portable SQL Queries** - Write SQL once with named parameters, run on any database regardless of parameter style
- **Flexible File Reading** - CSV, Excel (XLS/XLSX), JSON, NDJSON, XML, and fixed-width text files with consistent API
- **Transparent Compression** - Automatic decompression of .gz, .bz2, .xz, and .zip files with smart member selection
- **Multiple Export Formats** - Write to CSV, Excel, JSON, NDJSON, XML, fixed-width text, or directly between databases
- **Advanced ETL Framework** - Full-featured Table class for complex data transformations, validations, and upserts
- **High-Performance Bulk Operations** - DataSurge and BulkSurge classes for blazing-fast batch INSERT/UPDATE/DELETE/MERGE operations
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
dbtk.setup_logging()

with dbtk.connect('fire_nation_db') as db:
    cursor = db.cursor()

    # SQL with named parameters - works on ANY database
    params = {
        'min_rank': 'Captain',
        'start_date': '2024-01-01',
        'region': 'Western Fleet',
        'status': 'active'
    }

    # DBTK converts parameters to match database style automatically
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
- Write SQL once with named parameters (`:param`), works on any database
- Pass the same dict to multiple queries - extra params ignored, missing params = NULL
- Export to CSV/Excel/JSON with one line
- No parameter style conversions needed - DBTK handles it automatically

### Sample Inbound Integration - Import Data

Import data with field mapping, transformations, and validation:

```python
import dbtk
from dbtk.etl import Table
from dbtk.etl.transforms import parse_date, email_clean, get_int

dbtk.setup_logging()

with dbtk.connect('fire_nation_db') as db:
    cursor = db.cursor()

    # Define table with field mapping and transforms
    soldier_table = Table('soldiers', {
        'soldier_id': {'field': 'id', 'primary_key': True},
        'name': {'field': 'full_name', 'nullable': False},
        'rank': {'field': 'officer_rank', 'nullable': False},
        'email': {'field': 'contact_email', 'default': 'intel@firenation.com', 'fn': email_clean},
        'enlistment_date': {'field': 'join_date', 'fn': parse_date},
        'missions_completed': {'field': 'mission_count', 'fn': get_int},
        'status': {'default': 'active'}
    }, cursor=cursor)

    # Process incoming data
    with dbtk.readers.get_reader('incoming/new_recruits.csv') as reader:
        for record in reader:
            soldier_table.set_values(record)
            soldier_table.exec_insert(raise_error=False)

    print(f"Inserted {soldier_table.counts['inserts']} rows")
    db.commit()
```

**What makes this easy:**
- Field mapping separates database schema from source data format
- Built-in transforms (dates, emails, integers) with custom transform support
- Automatic tracking with `counts` dictionary
- One connection, multiple tables, all validated and transformed

## Documentation

- **[Configuration & Security](docs/configuration.md)** - Set up encrypted passwords, YAML config files, and command-line tools
- **[Database Connections](docs/database-connections.md)** - Connect to any database, use smart cursors, manage transactions
- **[Readers & Writers](docs/readers-writers.md)** - Read from and write to CSV, Excel, JSON, XML, fixed-width files
- **[ETL Framework](docs/etl.md)** - Build production ETL pipelines with Table, DataSurge, BulkSurge, transforms, and logging
- **[Advanced Features](docs/advanced.md)** - Custom drivers, multiple config locations, and performance tuning

## Performance Highlights

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

## Support

- **Issues**: [GitHub Issues](https://github.com/yourusername/dbtk/issues)
- **Discussions**: [GitHub Discussions](https://github.com/yourusername/dbtk/discussions)
- **Documentation**: [Full Documentation](https://dbtk.readthedocs.io)

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.
