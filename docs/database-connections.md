# Database Connections

DBTK provides a unified interface for connecting to multiple database types with consistent APIs and smart cursor handling.

## Quick Start

```python
import dbtk

# From configuration file
db = dbtk.connect('production_db')

# Direct connection
from dbtk.database import postgres, oracle, mysql, sqlserver, sqlite

db = postgres(user='admin', password='secret', database='mydb', host='localhost')
db = oracle(user='admin', password='secret', database='ORCL', host='localhost')
db = mysql(user='admin', password='secret', database='mydb')
db = sqlserver(user='admin', password='secret', database='mydb', host='localhost')
db = sqlite('path/to/database.db')
```

## Supported Databases

DBTK supports multiple database adapters with automatic detection and fallback:

| Database | Driver | Install Command | Notes |
|----------|--------|-----------------|-------|
| PostgreSQL | psycopg2 | `pip install psycopg2-binary` | Recommended, most mature |
| PostgreSQL | psycopg (3) | `pip install psycopg-binary` | Newest version, async support |
| PostgreSQL | pgdb | `pip install pgdb` | DB-API compliant |
| Oracle | oracledb | `pip install oracledb` | Thin mode - no Oracle client required |
| Oracle | cx_Oracle | `pip install cx_Oracle` | Requires Oracle client installation |
| MySQL | mysqlclient | `pip install mysqlclient` | Fastest option, C extension |
| MySQL | mysql.connector | `pip install mysql-connector-python` | Official MySQL connector |
| MySQL | pymysql | `pip install pymysql` | Pure Python, lightweight |
| SQL Server | pyodbc | `pip install pyodbc` | ODBC driver required on system |
| SQL Server | pymssql | `pip install pymssql` | Lightweight, no ODBC needed |
| SQLite | sqlite3 | Built-in | No installation needed |

**Driver priority:** DBTK automatically selects the best available driver. Override with `driver='driver_name'` in your connection config or function call.

## The Database Object

The `Database` class wraps database connections and provides a consistent interface:

```python
db = dbtk.connect('my_database')

# Connection info
print(db.database_type)  # 'postgres', 'oracle', 'mysql', 'sqlserver', 'sqlite'
print(db.database_name)  # Database/schema name
print(db.driver)         # The underlying driver module (psycopg2, oracledb, etc.)
print(db.placeholder)    # Parameter placeholder for this driver

# Create cursors
cursor = db.cursor()

# Transaction management
db.commit()
db.rollback()
db.close()

# Parameter style help
db.param_help()  # Shows this driver's parameter style with examples
```

### Context Managers

```python
# Connection automatically closed
with dbtk.connect('production_db') as db:
    cursor = db.cursor()
    cursor.execute("SELECT * FROM users")
    users = cursor.fetchall()

# Transaction - auto-commit on success, rollback on exception
with db.transaction():
    cursor.execute("UPDATE accounts SET balance = balance - 100 WHERE id = 1")
    cursor.execute("UPDATE accounts SET balance = balance + 100 WHERE id = 2")
```

## Access the Full Stack

DBTK maintains a clean reference hierarchy for accessing the underlying driver:

```python
# The Database holds the driver module
print(db.driver.__name__)       # 'psycopg2', 'oracledb', etc.
print(db.driver.paramstyle)     # 'named', 'qmark', etc.

# Access the wrapped connection
raw_conn = db._connection

# Use driver exceptions
try:
    cursor.execute(sql)
except db.driver.DatabaseError as e:
    logger.error(f"Database error: {e}")
```

## Cursors and Records

All DBTK cursors return **Record** objects - a flexible data structure supporting multiple access patterns:

```python
cursor = db.cursor()
cursor.execute("SELECT id, name, email FROM users WHERE status = :status",
               {'status': 'active'})

for user in cursor:
    # Dictionary-style access
    print(user['name'])

    # Attribute access
    print(user.email)

    # Index access
    user_id = user[0]

    # Safe access with default
    phone = user.get('phone', 'N/A')

    # Slicing
    first_three = user[:3]

    # Iteration and joining
    print('\t'.join(str(v) for v in user))

    # Conversion
    row_dict = dict(user)
    row_tuple = tuple(user)
```

### Column Name Handling

Record objects store both the original column names from the database and normalized (lowercased, cleaned) names. You can access fields using either form:

```python
cursor.execute("SELECT Employee_ID, FULL_NAME FROM users")
for row in cursor:
    row.employee_id       # Normalized name access
    row['Employee_ID']    # Original name access
    row['employee_id']    # Normalized also works in dict access

# See both forms
columns = cursor.columns()                    # Original names
normalized = cursor.columns(normalized=True)  # Normalized names
```

### Cursor Configuration

```python
cursor = db.cursor(
    batch_size=5000,           # Rows per batch in bulk operations
    debug=True,                # Print SQL queries and bind variables
    return_cursor=True,        # execute() returns cursor for chaining
)

# With return_cursor=True, you can chain calls
cursor.execute("SELECT * FROM users WHERE status = 'active'").fetchone()
```

Default cursor settings can be configured per-connection in the YAML config file or passed to `dbtk.connect()`:

```yaml
connections:
  my_database:
    type: postgres
    host: localhost
    database: mydb
    user: myuser
    cursor:
      batch_size: 4000
      debug: false
      return_cursor: true
```

## Parameter Styles

DBTK handles different parameter styles automatically. You can use named parameters (`:name`) with any database - DBTK converts to the driver's native style:

```python
# Named parameters (recommended - works everywhere)
cursor.execute(
    "SELECT * FROM users WHERE name = :name AND age > :age",
    {'name': 'Alice', 'age': 25}
)
```

### Native Parameter Styles

| Database | Native Style | Placeholder |
|----------|-------------|-------------|
| PostgreSQL | pyformat | `%(name)s` |
| Oracle | named | `:name` |
| MySQL | format | `%s` |
| SQL Server (pyodbc) | qmark | `?` |
| SQLite | qmark | `?` |

DBTK's `execute_file()` method converts named parameters to whatever your driver needs, making SQL truly portable across databases.

## Cursor Methods

### Executing Queries

```python
# Single query
cursor.execute("SELECT * FROM users WHERE id = :id", {'id': 42})

# Multiple rows (batch)
cursor.executemany(
    "INSERT INTO users (name, email) VALUES (:name, :email)",
    [
        {'name': 'Alice', 'email': 'alice@example.com'},
        {'name': 'Bob', 'email': 'bob@example.com'}
    ]
)

# Execute from SQL file with portable parameter conversion
cursor.execute_file('queries/create_schema.sql', {'status': 'active'})
```

### Fetching Results

```python
# Fetch one record
user = cursor.fetchone()

# Fetch many records
users = cursor.fetchmany(100)

# Fetch all records
all_users = cursor.fetchall()

# Iterate (memory efficient for large result sets)
for user in cursor:
    process(user)

# Fetch exactly one (raises error if 0 or >1 rows)
user = cursor.selectinto("SELECT * FROM users WHERE id = :id", {'id': 42})
```

### Cursor Properties

```python
# Column names (after executing a query)
columns = cursor.columns()                   # Original names
normalized = cursor.columns(normalized=True) # Sanitized for Python identifiers

# Row count (if available)
count = cursor.rowcount

# Description (DB-API standard)
desc = cursor.description
```

## Transaction Management

### Manual Transactions

```python
try:
    cursor.execute("UPDATE accounts SET balance = balance - 100 WHERE id = 1")
    cursor.execute("UPDATE accounts SET balance = balance + 100 WHERE id = 2")
    db.commit()
except Exception:
    db.rollback()
    raise
```

### Context Manager Transactions

```python
with db.transaction():
    cursor.execute("UPDATE accounts SET balance = balance - 100 WHERE id = 1")
    cursor.execute("UPDATE accounts SET balance = balance + 100 WHERE id = 2")
    # Commits automatically on success, rolls back on exception
```

## Direct Connection Functions

Each database type has a convenience function with appropriate defaults:

```python
from dbtk.database import postgres, oracle, mysql, sqlserver, sqlite

# PostgreSQL (default port: 5432)
db = postgres(user='admin', password='secret', database='mydb',
              host='localhost', port=5432)

# Oracle (default port: 1521)
db = oracle(user='admin', password='secret', database='ORCL',
            host='localhost', port=1521)

# MySQL (default port: 3306)
db = mysql(user='admin', password='secret', database='mydb',
           host='localhost', port=3306)

# SQL Server (default port: 1433)
db = sqlserver(user='admin', password='secret', database='mydb',
               host='localhost', port=1433)

# SQLite (no host/user/password needed)
db = sqlite('path/to/database.db')
db = sqlite(':memory:')  # In-memory database
```

All functions accept `**kwargs` for driver-specific parameters.

## Error Handling

```python
try:
    cursor.execute("SELECT * FROM nonexistent_table")
except db.driver.DatabaseError as e:
    print(f"Database error: {e}")
except db.driver.IntegrityError as e:
    print(f"Integrity constraint violated: {e}")
```

## Best Practices

1. **Use context managers** - Ensures connections are properly closed
2. **Use named parameters** - More readable and portable across databases
3. **Iterate large result sets** - Don't `fetchall()` millions of rows
4. **Use `transaction()` context manager** - Safe commit/rollback handling
5. **Use configuration files** - Keep credentials out of code
6. **Use `execute_file()`** - Portable SQL with automatic parameter conversion

## See Also

- [Configuration & Security](configuration.md) - YAML config files and password encryption
- [ETL Framework](etl.md) - Using cursors with Table, DataSurge, and BulkSurge
- [Readers & Writers](readers-writers.md) - Moving data between databases and files
