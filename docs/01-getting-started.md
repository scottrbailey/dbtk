# Getting Started with DBTK

Welcome to DBTK! This guide will get you up and running in 5 minutes.

## What is DBTK?

DBTK (Data Benders Toolkit) is a Python library for ETL (Extract, Transform, Load) operations. It provides:
- **Universal database connectivity** - One API for PostgreSQL, Oracle, MySQL, SQL Server, SQLite
- **Flexible file I/O** - Read and write CSV, Excel, JSON, XML, compressed files
- **Smart data transformations** - Field mapping, validation, type conversion
- **High-performance bulk operations** - Process millions of records efficiently
- **Production-ready logging** - Timestamped logs with automatic cleanup

## Installation

```bash
# Basic installation
pip install dbtk

# Recommended (includes keyring, XML, Excel, phone/date helpers)
pip install dbtk[recommended]

# Database drivers (install as needed)
pip install psycopg2-binary      # PostgreSQL
pip install oracledb             # Oracle
pip install mysqlclient          # MySQL
pip install pyodbc               # SQL Server
```

## Your First ETL Pipeline (5 Minutes)

### Step 1: Connect to a Database

```python
import dbtk

# Option 1: From configuration file (recommended for production)
db = dbtk.connect('my_database')
cur = db.cursor()

# Option 2: Direct connection (quick for dev/testing)
from dbtk.database import postgres
db = postgres(user='admin', password='secret', database='mydb')
cur = db.cursor()

# Option 3: Cursor maintains reference to connection (cursor.connection) 
# and driver (cursor.connection.driver)
cur = dbtk.connect('my_database').cursor()
cur.connection.close()
```

### Step 2: Read Data from a File

```python
# Reads CSV, automatically detects format, handles compression
with dbtk.readers.get_reader('data.csv.gz') as reader:
    for record in reader:
        # Access data multiple ways
        print(record.name)        # Attribute access
        print(record['email'])    # Dict access
        print(record[0])          # Index access
```

### Export Data

```python
cursor.execute("SELECT * FROM users WHERE is_active = true")

# Excel — ready to open, no extra work required
dbtk.writers.to_excel(cursor, 'active_users.xlsx', sheet='Active Users')
```

**What DBTK handles automatically:**
- Columns are sampled and auto-sized to fit the data
- Header row is bold and frozen so it stays visible while scrolling
- Date and datetime values are formatted correctly
- `None` values become blank cells

Need other formats? Same data, one line each:

```python
import dbtk.writers

cursor.execute("SELECT * FROM users WHERE is_active = true")
data = cursor.fetchall()  # must materialize when outputting to multiple sources

dbtk.writers.to_csv(data, 'active_users.csv')
dbtk.writers.to_json(data, 'active_users.json')
dbtk.writers.to_ndjson(data, 'active_users.ndjson')
dbtk.writers.to_xml(data, 'active_users.xml', root_element='users', record_element='user')
```

## Complete Example: CSV to Database

```python
import dbtk

# Setup logging (creates log file with defaults from user's configuration)
dbtk.setup_logging()

# Connect to database
with dbtk.connect('production_db') as db:
    cursor = db.cursor()

    # Define table with transforms
    orders_table = dbtk.etl.Table('orders', {
        'order_id': {'field': 'id', 'key': True},
        'customer_email': {'field': 'email', 'fn': 'email', 'nullable': False},
        'order_date': {'field': 'date', 'fn': 'date'},
        'amount': {'field': 'total', 'fn': 'float'},
        'status': {'default': 'pending'}
    }, cursor=cursor)

    # Load data with automatic batching and progress
    with dbtk.readers.get_reader('orders.csv.gz') as reader:
        surge = dbtk.etl.DataSurge(orders_table, use_transaction=True)
        errors = surge.insert(reader)

    print(f"Loaded {orders_table.counts['insert']} orders")
    print(f"Skipped {orders_table.counts['incomplete']} incomplete records")

# Check for errors
if dbtk.errors_logged():
    print("⚠️  Some errors occurred - check log file")
```

## Core Concepts

### 1. Record Objects

Every cursor and file reader returns **Record** objects - that strike a balance between the memory efficiency and speed of lists
and the functionality of dicts.

```python
for row in cursor:
    row.name              # Attribute access
    row['email']          # Dict access
    row[0]                # Index access
    row[:3]               # Slicing
    id, name, email = row # Unpacking
```

See [Record Objects](04-record.md) for details.

### 2. Portable SQL

Write SQL once with named parameters - parameter style is determined by your database connection

```python
# Write SQL with :named or %(pyformat)s parameters
import dbtk 

cursor = dbtk.connect('warehouse_prod').cursor()

# Create PreparedStatement from file
statement = cursor.prepare_file('users.sql')
statement.execute({'status': 'active', 'age': 18})

# Create PreparedStatement from query
sql = "SELECT * FROM users WHERE status = :status AND age > :age",
statement = cursor.prepare_query(sql)
statement.execute({'status': 'active', 'age': 18})

# One time query from file
cursor.execute_file('users.sql', {'status': 'active', 'age': 18})

# on the fly parameter conversion
cursor.execute(sql, {'status': 'active', 'age': 18}, convert_params=True)

# DBTK converts to your database's native format automatically
# Oracle: :status, :age
# PostgreSQL: %(status)s, %(age)s
# MySQL/SQLite: ?, ? (and reorders parameters)
```

See [Database Connections](03-database-connections.md) for details.

### 3. Field Mapping and Transforms

Separate your database schema from your source data format:

```python
table = dbtk.etl.Table('employees', {
    # Database column: source field + transforms
    'emp_id': {'field': 'employee_number', 'key': True},
    'full_name': {'field': 'name', 'fn': 'maxlen:100'},
    'hire_date': {'field': 'start_date', 'fn': 'date'},
    'department': {'field': 'dept_code', 'fn': 'lookup:departments:code:name'}
}, cursor=cursor)

# generate initial column configuration template from database schema
emp_cols = dbtk.etl.column_defs_from_db(cursor=cursor, table_name='employees')
```

See [ETL: Table & Transforms](07-table.md) for details.

### 4. Bulk Operations

Process millions of records efficiently:

```python
from dbtk.etl import Table, DataSurge, BulkSurge
contacts = Table('contacts', columns=contact_cols, cursor=cursor)
reader = dbtk.readers.get_reader('contacts.csv.gz')

# DataSurge: Fast batching with executemany 
surge = DataSurge(contacts)
surge.insert(reader)     # Supports MERGE, UPDATE, DELETE too

# BulkSurge: Maximum speed with native loading
surge = BulkSurge(contacts)
surge.load(reader)       # Uses COPY/direct-path/bcp depending on database
```

See [ETL: DataSurge & BulkSurge](08-datasurge.md) for details.

## Configuration

Create `dbtk.yml` in your project or `~/.config/dbtk.yml`:

```yaml
connections:
  production_db:
    type: postgres
    host: db.example.com
    database: production
    user: app_user
    encrypted_password: gAAAAABh...  # Use: dbtk encrypt-password

settings:
  default_batch_size: 2000
  logging:
    directory: ./logs
    level: INFO
    split_errors: true
```

Encrypt passwords:

```bash
# Generate encryption key and store in system keyring
dbtk store-key

# Encrypt a password
dbtk encrypt-password mypassword

# Encrypt all passwords in config file
dbtk encrypt-config dbtk.yml
```

See [Configuration & Security](02-configuration.md) for details.

## Common Patterns

### Update or Insert Logic

```python
for record in reader:
    table.set_values(record)
    existing = table.fetch()  # Check if record exists
    operation = 'update' if existing else 'insert'
    
    if table.is_ready(operation):
        err = table.execute(operation, raise_error=False)
        if err:
            logger.warning(f"{operation} failed: {table.last_error.message}")
        elif operation == 'insert':
            id = cursor.lastrowid
# note: DataSurge supports merge/upsert for high-performance, bulk operations
```

### Filtering Records

```python
with dbtk.readers.get_reader('data.csv') as reader:
    reader.add_filter(lambda r: r.status == 'active')
    reader.add_filter(lambda r: r.age >= 18)
    for record in reader:
        process(record)
```

### Multi-Stage ETL with State Persistence

```python
from dbtk.etl import IdentityManager, EntityStatus

stmt = cursor.prepare_file('identity_lookup.sql')

# Map source IDs to target IDs with caching
im = IdentityManager('source_id', 'target_id', resolver=stmt)

for record in reader:
    entity = im.resolve(record)  # Gets target_id, writes to record
    if entity['_status'] == EntityStatus.RESOLVED:
        table.set_values(record)
        if not table.is_ready('insert'):
            missing = ', '.join(table.reqs_missing('insert'))
            im.add_message(record.source_id, f"Skipped - missing values: {missing}")
        err = table.execute('insert')
        if err:
            im.add_error(record.source_id, table.last_error)

# Save state for next run
im.save_state('state/entities.json')
```

See [ETL: Tools & Logging](09-etl-tools.md) for details.

## Next Steps

### Essential Documentation
- **[Database Connections](03-database-connections.md)** - Cursors, transactions, SQL files
- **[Record Objects](04-record.md)** - Universal data structure
- **[Readers](05-readers.md)** - Reading data from files and databases
- **[Writers](06-writers.md)** - Writing data to files and databases
- **[ETL: Table & Transforms](07-table.md)** - Field mapping, transformations
- **[ETL: DataSurge & BulkSurge](08-datasurge.md)** - High-performance loading

### Advanced Topics
- **[ETL: Tools & Logging](09-etl-tools.md)** - IdentityManager, ValidationCollector
- **[Configuration & Security](02-configuration.md)** - Encryption, environment variables
- **[Advanced Features](10-advanced.md)** - Performance tuning, custom drivers

### Get Help
- Check [Troubleshooting](12-troubleshooting.md) for common issues
- Review [API Reference](api.rst) for all methods
- See examples in the `/examples` folder
- Report issues at [GitHub Issues](https://github.com/yourusername/dbtk/issues)

## Quick Command Reference

```bash
# Check installation and available drivers
dbtk checkup

# Password encryption
dbtk generate-key                    # Generate encryption key
dbtk store-key                       # Store key in system keyring
dbtk encrypt-password <password>     # Encrypt a single password
dbtk encrypt-config dbtk.yml         # Encrypt all passwords in config

# Log management
# Cleanup handled automatically with dbtk.cleanup_old_logs()
```

Ready to build production ETL pipelines? Start with the examples above and explore the documentation links!
