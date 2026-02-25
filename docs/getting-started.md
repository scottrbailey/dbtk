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

# Option 2: Direct connection (quick for dev/testing)
from dbtk.database import postgres
db = postgres(user='admin', password='secret', database='mydb')
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

### Step 3: Transform and Load Data

```python
from dbtk.etl import Table, DataSurge

# Define table schema with field mapping and transforms
user_table = Table('users', {
    'user_id': {'field': 'id', 'key': True},
    'username': {'field': 'user_name', 'nullable': False, 'fn': 'lower'},
    'email': {'field': 'email_address', 'fn': 'email'},
    'signup_date': {'field': 'created_at', 'fn': 'date'},
    'is_active': {'default': True}
}, cursor=db.cursor())

# Bulk insert with automatic batching
with dbtk.readers.get_reader('users.csv.gz') as reader:
    surge = DataSurge(user_table)
    surge.insert(reader)  # Automatically shows progress bar

print(f"Inserted {user_table.counts['insert']} users")
```

### Step 4: Export Data

```python
# Query database
cursor = db.cursor()
cursor.execute("SELECT * FROM users WHERE is_active = true")

# Export to multiple formats
dbtk.writers.to_csv(cursor, 'active_users.csv')
dbtk.writers.to_excel(cursor, 'active_users.xlsx', sheet='Active Users')
dbtk.writers.to_json(cursor, 'active_users.json')
```

## Complete Example: CSV to Database

```python
import dbtk

# Setup logging (creates timestamped log file)
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

Every cursor and file reader returns **Record** objects - a hybrid data structure:

```python
for row in cursor:
    row.name              # Attribute access
    row['email']          # Dict access
    row[0]                # Index access
    row[:3]               # Slicing
    id, name, email = row # Unpacking
```

See [Record Objects](record.md) for details.

### 2. Portable SQL

Write SQL once with named parameters - works on any database:

```python
# Write SQL with :named or %(named)s parameters
cursor.execute("SELECT * FROM users WHERE status = :status AND age > :age",
               {'status': 'active', 'age': 18})

# DBTK converts to your database's native format automatically
# Oracle: :status, :age
# PostgreSQL: %(status)s, %(age)s
# MySQL/SQLite: ?, ? (and reorders parameters)
```

See [Database Connections](database-connections.md) for details.

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
```

See [ETL: Table & Transforms](table.md) for details.

### 4. Bulk Operations

Process millions of records efficiently:

```python
from dbtk.etl import DataSurge, BulkSurge

# DataSurge: Fast batching with executemany (90-120K rec/s)
surge = DataSurge(table)
surge.insert(records)     # Supports MERGE, UPDATE, DELETE too

# BulkSurge: Maximum speed with native loading (200K+ rec/s)
surge = BulkSurge(table)
surge.load(records)       # Uses COPY/direct-path/bcp depending on database
```

See [ETL: DataSurge & BulkSurge](datasurge.md) for details.

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

See [Configuration & Security](configuration.md) for details.

## Common Patterns

### Update or Insert Logic

```python
for record in reader:
    table.set_values(record)
    existing = table.fetch()  # Check if record exists

    if existing:
        table.execute('update')
    else:
        table.execute('insert')
```

### Error Handling

```python
for record in reader:
    table.set_values(record)
    if table.execute('insert', raise_error=False):
        # On error, last_error contains details
        print(f"Failed: {table.last_error.message}")
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

# Map source IDs to target IDs with caching
im = IdentityManager('source_id', 'target_id', resolver=stmt)

for record in reader:
    entity = im.resolve(record)  # Gets target_id, writes to record
    if entity['_status'] == EntityStatus.RESOLVED:
        table.set_values(record)
        table.execute('insert')

# Save state for next run
im.save_state('state/entities.json')
```

See [ETL: Tools & Logging](etl-tools.md) for details.

## Next Steps

### Essential Documentation
- **[Database Connections](database-connections.md)** - Cursors, transactions, SQL files
- **[Record Objects](record.md)** - Universal data structure
- **[Readers & Writers](readers-writers.md)** - File I/O for all formats
- **[ETL: Table & Transforms](table.md)** - Field mapping, transformations
- **[ETL: DataSurge & BulkSurge](datasurge.md)** - High-performance loading

### Advanced Topics
- **[ETL: Tools & Logging](etl-tools.md)** - IdentityManager, ValidationCollector
- **[Configuration & Security](configuration.md)** - Encryption, environment variables
- **[Advanced Features](advanced.md)** - Performance tuning, custom drivers

### Get Help
- Check [Troubleshooting](troubleshooting.md) for common issues
- Review [API Reference](api-reference.md) for all methods
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
