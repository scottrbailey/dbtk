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
db = oracle(user='admin', password='secret', service_name='ORCL')
db = mysql(user='admin', password='secret', database='mydb')
db = sqlserver(user='admin', password='secret', database='mydb', server='localhost')
db = sqlite('path/to/database.db')
```

## Supported Databases

| Database | Driver | Connection Function |
|----------|--------|-------------------|
| PostgreSQL | `psycopg2` | `dbtk.database.postgres()` |
| Oracle | `oracledb` | `dbtk.database.oracle()` |
| MySQL | `mysqlclient` | `dbtk.database.mysql()` |
| SQL Server | `pyodbc` / `pymssql` | `dbtk.database.sqlserver()` |
| SQLite | `sqlite3` | `dbtk.database.sqlite()` |

## The Database Object

The `Database` class wraps database connections and provides a consistent interface regardless of the underlying driver:

```python
db = dbtk.connect('my_database')

# Connection info
print(db.database_type)  # 'postgres', 'oracle', 'mysql', etc.
print(db.database_name)  # Database/schema name
print(db.driver)         # The underlying driver module (psycopg2, oracledb, etc.)

# Create cursors
cursor = db.cursor()

# Transaction management
db.commit()
db.rollback()
db.close()
```

### Context Managers

Use context managers for automatic cleanup:

```python
# Connection automatically closed
with dbtk.connect('production_db') as db:
    cursor = db.cursor()
    cursor.execute("SELECT * FROM users")
    users = cursor.fetchall()

# Transaction automatically committed or rolled back
with db.transaction():
    cursor.execute("UPDATE accounts SET balance = balance - 100 WHERE id = 1")
    cursor.execute("UPDATE accounts SET balance = balance + 100 WHERE id = 2")
    # Commits on success, rolls back on exception
```

## Cursors and Records

All DBTK cursors return **Record** objects by default - a flexible data structure supporting multiple access patterns:

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

    # Tuple unpacking
    uid, name, email = user
```

### Column Name Normalization

By default, column names are normalized to lowercase for consistency across databases:

```python
# Oracle returns "USER_ID", Postgres returns "user_id"
# Both become accessible as:
record.user_id
record['user_id']
```

Control this behavior with `column_case`:

```python
from dbtk.cursors import ColumnCase

# Preserve database casing
cursor = db.cursor(column_case=ColumnCase.PRESERVE)

# Force uppercase
cursor = db.cursor(column_case=ColumnCase.UPPER)

# Force lowercase (default)
cursor = db.cursor(column_case=ColumnCase.LOWER)
```

## Parameter Styles

DBTK handles different parameter styles automatically:

```python
# Named parameters (recommended - works everywhere)
cursor.execute(
    "SELECT * FROM users WHERE name = :name AND age > :age",
    {'name': 'Alice', 'age': 25}
)

# Positional parameters (qmark style)
cursor.execute(
    "SELECT * FROM users WHERE name = ? AND age > ?",
    ('Alice', 25)
)

# Check your database's style
db.param_help()
```

### Parameter Style by Database

- **PostgreSQL**: `pyformat` - `%(name)s`
- **Oracle**: `named` - `:name`
- **MySQL**: `format` - `%s`
- **SQL Server**: `qmark` - `?`
- **SQLite**: `qmark` - `?`

DBTK's unified parameter handling lets you use named parameters (`:name` or `%(name)s`) with any database.

## Cursor Methods

### Executing Queries

```python
# Single query
cursor.execute("SELECT * FROM users WHERE id = :id", {'id': 42})

# Multiple queries (batch)
cursor.executemany(
    "INSERT INTO users (name, email) VALUES (:name, :email)",
    [
        {'name': 'Alice', 'email': 'alice@example.com'},
        {'name': 'Bob', 'email': 'bob@example.com'}
    ]
)

# Execute from SQL file
cursor.executefile('queries/create_schema.sql')
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
# Column names
columns = cursor.columns()

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
# Automatic commit/rollback
with db.transaction():
    cursor.execute("UPDATE accounts SET balance = balance - 100 WHERE id = 1")
    cursor.execute("UPDATE accounts SET balance = balance + 100 WHERE id = 2")
    # Commits automatically on success, rolls back on exception
```

### Nested Transactions

```python
with db.transaction():
    cursor.execute("INSERT INTO orders (user_id) VALUES (1)")

    with db.transaction():
        cursor.execute("INSERT INTO order_items (order_id, product_id) VALUES (1, 100)")
        # Inner transaction commits

    # Outer transaction commits
```

## Connection Pooling

For production applications with connection pooling libraries:

```python
from dbtk.database import Database
import psycopg2.pool

# Create pool
pool = psycopg2.pool.SimpleConnectionPool(1, 20,
    host='localhost',
    database='mydb',
    user='admin',
    password='secret'
)

# Get connection from pool
conn = pool.getconn()
db = Database(conn, psycopg2, 'mydb')

# Use it
cursor = db.cursor()
cursor.execute("SELECT * FROM users")

# Return to pool
pool.putconn(conn)
```

## Advanced Connection Options

### Custom Cursor Settings

Set default cursor behavior for all cursors from a connection:

```python
db = dbtk.connect('production_db', cursor_settings={
    'column_case': 'preserve',
    'batch_size': 5000,
    'debug': True
})

# All cursors from this connection inherit these settings
cursor = db.cursor()
```

### Direct Driver Access

Access underlying driver features when needed:

```python
# Access the wrapped connection
raw_conn = db._connection

# Access the driver module
driver = db.driver
print(driver.paramstyle)
print(driver.apilevel)

# Call driver-specific methods
if db.database_type == 'oracle':
    # Oracle-specific feature
    cursor.setinputsizes(...)
```

## Error Handling

```python
from dbtk.database import Database

try:
    cursor.execute("SELECT * FROM nonexistent_table")
except db.driver.DatabaseError as e:
    print(f"Database error: {e}")
except db.driver.IntegrityError as e:
    print(f"Integrity constraint violated: {e}")
```

## Best Practices

1. **Use context managers** - Ensures connections are properly closed
2. **Use named parameters** - More readable and works across all databases
3. **Iterate large result sets** - Don't fetchall() millions of rows
4. **Handle transactions explicitly** - Use `transaction()` context manager
5. **Normalize column names** - Default lowercase makes code portable
6. **Use configuration files** - Keep credentials out of code

## Configuration File Connections

The recommended approach for production is to use YAML configuration files:

```yaml
# config.yml
databases:
  production_db:
    driver: postgres
    host: db.example.com
    database: mydb
    user: app_user
    password: !encrypted AQECAHi...  # Use dbtk-encrypt
```

Then connect with:

```python
import dbtk

dbtk.set_config_file('config.yml')
db = dbtk.connect('production_db')
```

See [Configuration & Security](configuration.md) for details on encrypted passwords and config file management.

## See Also

- [Configuration & Security](configuration.md) - YAML config files and password encryption
- [ETL Framework](etl.md) - Using cursors with Table, DataSurge, and BulkSurge
- [Readers & Writers](readers-writers.md) - Moving data between databases and files
