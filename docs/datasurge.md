# ETL: DataSurge & BulkSurge

High-performance bulk loading for any database. Both classes wrap a configured `Table` and handle batching, progress tracking, and database-specific optimisations automatically.

## DataSurge

**The problem:** Processing thousands or millions of records row-by-row is painfully slow. You need batching, but implementing it correctly is complex.

**The solution:** DataSurge handles batching, error tracking, and optimal merge strategies automatically. It's built for high-volume data processing.

```python
from dbtk.etl import DataSurge

# Define table configuration
recruit_table = dbtk.etl.Table('fire_nation_soldiers', columns_config, cursor)

# Create DataSurge instance for bulk operations
bulk_writer = DataSurge(recruit_table, batch_size=2000)

# Bulk insert with batching
with dbtk.readers.get_reader('massive_conscript_list.csv') as reader:
    errors = bulk_writer.insert(reader)
    print(f"Inserted {recruit_table.counts['insert']} records with {errors} errors")

# Bulk merge (upsert) operations
with dbtk.readers.get_reader('soldier_updates.csv') as reader:
    errors = bulk_writer.merge(reader)
```

**DataSurge features:**
- Automatic batching for optimal performance
- Smart merge strategies (native MERGE vs temp table based on database capabilities)
- Configurable error handling
- Progress tracking and logging
- Support for INSERT, UPDATE, DELETE, MERGE operations

**Performance impact:** DataSurge can be 10-100x faster than row-by-row operations, depending on your database and network latency.

## BulkSurge

BulkSurge provides maximum throughput by leveraging database-specific bulk loading mechanisms. It supports both **direct streaming** (zero temp files) and **external tool-based** loading depending on your database and requirements.

### Supported Databases

**PostgreSQL & Redshift**
- Uses COPY FROM STDIN protocol with background writer thread
- Streaming with zero temp files
- 200K+ rec/s sustained throughput

**Oracle**
- **Direct (default):** python-oracledb direct_path_load API (requires 3.4+)
- **External:** SQL\*Loader (sqlldr) with auto-generated control files
- Both methods: 200K+ rec/s

**MySQL & MariaDB**
- **Direct (default):** LOAD DATA LOCAL INFILE with streaming buffer
- **External:** Checks local_infile setting, streams or dumps CSV
- Requires local_infile=1 on server for direct loading

**SQL Server**
- **External only:** Uses bcp (bulk copy program) utility
- Requires named connection from config for credentials
- Supports SQL auth and Windows integrated auth

### Basic Usage

```python
from dbtk.etl import BulkSurge

# Define table (no db_expr columns allowed - BulkSurge loads raw data)
sensor_table = dbtk.etl.Table('sensor_readings', {
    'sensor_id': {'field': 'id', 'primary_key': True},
    'timestamp': {'field': 'ts', 'fn': 'datetime'},
    'value': {'field': 'reading', 'fn': 'float'},
}, cursor=cursor)

# Direct streaming (default) - zero temp files
surge = BulkSurge(sensor_table, batch_size=50000)
with dbtk.readers.get_reader('sensor_data.csv.gz') as reader:
    count = surge.load(reader)  # Streams data directly
    print(f"Loaded {count:,} records")
```

### Loading Methods

BulkSurge automatically selects the optimal loading strategy, but you can override with the `method` parameter:

**Direct Method (default)**
```python
# Streams data using native Python drivers - no temp files
surge.load(reader)  # method='direct' is default

# Works for: PostgreSQL, Oracle (with python-oracledb 3.4+), MySQL (with local_infile=1)
```

**External Method**
```python
# Uses command-line tools - requires named connection
surge.load(reader, method='external')

# Oracle: Uses SQL*Loader (sqlldr)
# MySQL: Falls back to direct if local_infile enabled, else dumps CSV
# SQL Server: Uses bcp (only method available)
```

### Database-Specific Examples

**PostgreSQL - Direct Streaming**
```python
db = dbtk.connect('postgres_prod')
table = dbtk.etl.Table('events', columns_config, cursor=db.cursor())
surge = BulkSurge(table)

with dbtk.readers.get_reader('events.csv.gz') as reader:
    surge.load(reader)  # COPY FROM STDIN, zero temp files
```

**Oracle - SQL\*Loader**
```python
# Requires named connection for credentials
db = dbtk.connect('oracle_prod')  # Named connection required
table = dbtk.etl.Table('schema.table_name', columns_config, cursor=db.cursor())
surge = BulkSurge(table)

with dbtk.readers.get_reader('data.csv') as reader:
    surge.load(reader, method='external', dump_path='/staging')
    # Creates CSV + control file, invokes sqlldr, cleans up temp files
```

**MySQL - Streaming with Fallback**
```python
db = dbtk.connect('mysql_prod')
table = dbtk.etl.Table('orders', columns_config, cursor=db.cursor())
surge = BulkSurge(table)

# Direct method checks server configuration automatically
surge.load(reader, method='direct')
# If local_infile=1: streams with LOAD DATA LOCAL INFILE
# If local_infile=0: dumps CSV and logs manual load instructions
```

**SQL Server - bcp with Named Connection**
```python
# IMPORTANT: SQL Server requires named connection from config
db = dbtk.connect('mssql_prod')  # Must use named connection
table = dbtk.etl.Table('dbo.orders', columns_config, cursor=db.cursor())
surge = BulkSurge(table)

with dbtk.readers.get_reader('orders.csv') as reader:
    surge.load(reader)  # Uses bcp automatically
    # Supports both SQL auth (-U/-P) and Windows integrated auth (-T)
```

**SQL Server - Windows Integrated Auth**
```yaml
# Config file (dbtk.yml) - no user/password for integrated auth
connections:
  mssql_prod:
    type: sqlserver
    host: sql-server.company.com
    database: production
    # No user/password = uses Windows integrated auth
```

### Controlling Temp File Location

For external methods that create temp files, control the location with `dump_path`:

```python
# Specify exact file path
surge.load(reader, method='external', dump_path='/staging/mydata.csv')

# Specify directory (auto-generates timestamped filename)
surge.load(reader, method='external', dump_path='/staging')
# Creates: /staging/orders_20260206_143022.csv

# Use configured directory from settings
# settings['data_dump_dir'] = '/data/staging'
surge.load(reader, method='external')  # Uses configured directory

# Fallback to temp directory if not specified
surge.load(reader, method='external')  # Uses tempfile.gettempdir()
```

### Named Connections for External Tools

External tools (bcp, sqlldr) require credentials, which are retrieved securely from your named connection:

```yaml
# dbtk.yml
connections:
  oracle_prod:
    type: oracle
    host: oracle-db.company.com
    port: 1521
    database: PRODDB
    user: etl_user
    encrypted_password: gAAAAABh...  # Encrypted with dbtk encrypt-config

  mssql_prod:
    type: sqlserver
    host: sql-server.company.com
    database: production
    user: sa
    encrypted_password: gAAAAABh...
```

**Why named connections?**
- Credentials stored securely in config (encrypted)
- External tools need username/password at runtime
- Direct database connections don't expose credentials
- BulkSurge retrieves them only when needed for external tools

**Error if connection_name missing:**
```python
# This will fail for SQL Server (requires named connection)
db = dbtk.sqlserver(host='server', user='sa', password='secret')  # Direct connection
surge = BulkSurge(table)
surge.load(reader)  # RuntimeError: BCP needs credentials. Please set up a named connection in the config file.

# Solution: Use named connection
db = dbtk.connect('mssql_prod')  # Named connection ✓
surge.load(reader)  # Works!
```

### Manual CSV Export

For databases not yet supported, or when you need custom loading parameters, use `dump()` to export transformed data:

```python
# Export to CSV for manual loading
surge = BulkSurge(table)
with dbtk.readers.get_reader('source.csv.gz') as reader:
    csv_path = surge.dump(reader, 'staging/transformed.csv')

# Then load manually:
# SQL Server: bcp mydb.dbo.mytable in staging/transformed.csv -c -t, -S server -U user -P pass
# MySQL: LOAD DATA INFILE 'staging/transformed.csv' INTO TABLE mytable ...
# Snowflake: COPY INTO mytable FROM 'staging/transformed.csv' ...
```

**Oracle Auto-generation:**

When connected to Oracle, `dump()` automatically generates a SQL\*Loader control file (.ctl) alongside the CSV and logs the `sqlldr` command:

```python
db = dbtk.connect('oracle_prod')  # Oracle connection
surge = BulkSurge(table)
surge.dump(reader, '/staging/export.csv')

# Automatically creates:
#   /staging/export.csv           (data file)
#   /staging/export_a1b2c3d4.ctl  (control file with unique suffix)
#
# Logs show the sqlldr command to run:
#   sqlldr userid=USER/PASS@DB control=/staging/export_a1b2c3d4.ctl data=/staging/export.csv
```

This saves time by eliminating manual control file creation and ensures the column mappings match your Table definition.

### BulkSurge vs DataSurge Comparison

| Feature | DataSurge | BulkSurge |
|---------|-----------|-----------|
| **Speed** | 90-120K rec/s | 200K+ rec/s |
| **Method** | executemany batching | Native bulk loading |
| **Temp files** | Never | Only for external tools |
| **db_expr support** | Yes | No (raw data only) |
| **MERGE/upsert** | Yes | No (INSERT only) |
| **Databases** | All (universal) | PostgreSQL, Oracle, MySQL, SQL Server |
| **Setup** | Works everywhere | May require server config (MySQL local_infile) |
| **Credentials** | Uses connection | External tools need named connection |

### When to Use BulkSurge

**Use BulkSurge when:**
- Loading millions of rows (5M+ records)
- Simple INSERT operations (no upsert/merge needed)
- No database functions required (`db_expr` not used)
- Maximum throughput is critical
- You have appropriate server permissions (MySQL local_infile, etc.)

**Use DataSurge when:**
- Need MERGE/upsert operations
- Using `db_expr` for database functions
- Loading moderate datasets (< 5M records)
- Want universal compatibility without configuration
- Don't have server-level permissions

### Troubleshooting

**MySQL: "LOAD DATA LOCAL INFILE forbidden"**
```python
# Check server configuration
cursor.execute("SELECT @@local_infile")
# If 0: contact DBA to enable or use external method
surge.load(reader, method='external')  # Dumps CSV with instructions
```

**SQL Server: "connection_name missing"**
```python
# Use named connection, not direct connection
db = dbtk.connect('mssql_prod')  # Not: dbtk.sqlserver(host=..., user=..., password=...)
```

**Oracle: "direct_path_load not supported"**
```python
# Requires python-oracledb 3.4+
# Solution: Upgrade driver or use external method
surge.load(reader, method='external')  # Uses SQL*Loader
```

**"Table has db_expr columns"**
```python
# BulkSurge doesn't support database expressions
# Solution: Use DataSurge or remove db_expr from column config
```

## See Also

- [ETL: Table & Transforms](table.md) - Table configuration, field mapping, transforms
- [ETL: Tools & Logging](etl-tools.md) - IdentityManager, ValidationCollector, logging
- [Database Connections](database-connections.md) - Connections, cursors, SQL file execution
