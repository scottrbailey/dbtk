# Database Connections

DBTK provides a unified interface for connecting to multiple database types with consistent APIs and smart cursor handling.

## Quick Start

```python
import dbtk

# From configuration file
db = dbtk.connect('production_db')

# Direct connection
from dbtk.database import postgres, oracle, mysql, sqlserver, sqlite
# non-standard connection parameters are automatically mapped
db = postgres(user='admin', password='secret', database='mydb', host='localhost')
db = oracle(user='admin', password='secret', database='ORCL', host='localhost')
db = mysql(user='admin', password='secret', database='mydb')
db = sqlserver(user='admin', password='secret', database='mydb', host='localhost')
db = sqlite('path/to/database.db')
```

## Supported Databases

DBTK supports multiple database drivers with automatic detection and fallback:

| Database | Driver | Install Command | Notes                                                     |
|----------|--------|-----------------|-----------------------------------------------------------|
| PostgreSQL | psycopg2 | `pip install psycopg2-binary` | Recommended, most mature                                  |
| PostgreSQL | psycopg (3) | `pip install psycopg-binary` | Newest version, async support                             |
| PostgreSQL | pgdb | `pip install pgdb` | DB-API compliant                                          |
| Oracle | oracledb | `pip install oracledb` | Thin mode - no Oracle client required                     |
| Oracle | cx_Oracle | `pip install cx_Oracle` | Requires Oracle client installation                       |
| MySQL | mysqlclient | `pip install mysqlclient` | Fastest option, C extension, module name MySQLdb          |
| MySQL | mariadb | `pip install mariadb` | Official MariaDB connector, C extension, MySQL compatible |
| MySQL | mysql.connector | `pip install mysql-connector-python` | Official MySQL connector                                  |
| MySQL | pymysql | `pip install pymysql` | Pure Python, lightweight                                  |
| SQL Server | pyodbc | `pip install pyodbc` | ODBC driver required on system                            |
| SQL Server | pymssql | `pip install pymssql` | Lightweight, no ODBC needed                               |
| SQLite | sqlite3 | Built-in | No installation needed                                    |

**Driver priority:** DBTK automatically selects the best available driver based on priority. Override with `driver='driver_name'` in your connection config or function call.

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
db = dbtk.connect('imdb')
# The Database maintains a reference to the driver
print(db.driver.__name__)       # 'psycopg2', 'oracledb', etc.

cursor = db.cursor()
# The Cursor maintains a reference to the Database connection
print(cursor.connection.connection_name) # imdb

# Access the wrapped connection or cursor
db._connection
cursor._cursor

# Use driver exceptions
try:
    cursor.execute(sql)
except cursor.connection.driver.DatabaseError as e:
    logger.error(f"Database error: {e}")
```

## Cursors and Records

All DBTK cursors return **Record** objects - a hybrid data structure that works like a dict, tuple, and object simultaneously:

```python
cursor = db.cursor()
cursor.execute("SELECT id, name, email FROM users WHERE status = :status",
               {'status': 'active'})

for user in cursor:
    user['name']            # Dict-style access
    user.email              # Attribute access
    user[0]                 # Index access
    user[:2]                # Slicing
    id, name, email = user  # Tuple unpacking
```

Records also normalize column names for attribute access, so `row.employee_id` works whether the source column is `Employee_ID`, `EMPLOYEE ID`, or `employee_id`. This makes your Table field mappings resilient to source naming inconsistencies.

See [Record Objects](record.md) for full documentation on access patterns, normalization, mutation, and performance characteristics.

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

### Native Parameter Styles 

| Database            | Dictionary Style | Positional Style | Placeholder     |
|---------------------|------------------|------------------|-----------------|
| PostgreSQL          | pyformat         | format           | `%(name)s` `%s` |
| Oracle              | named            | numeric          | `:name` `:1`    | 
| MySQL               |                  | format           | `%s`            |
| SQL Server (pyodbc) |                  | qmark            | `?`             |
| SQLite              |                  | qmark            | `?`             |  


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

### Prepared Statements
For queries executed repeatedly with different parameters, PreparedStatement loads the SQL once and caches the parameter mapping:

```python
from dbtk.cursors import PreparedStatement

# Load and prepare from file or from query string
users_stmt = PreparedStatement(cursor, filename='queries/get_user.sql')
orders_stmt = PreparedStatement(cursor, query="SELECT * FROM orders WHERE customer_id = :id")

users_stmt.execute({'location': 'CA'})
# Execute many times efficiently
for user in user_stmt.fetchall():
    orders_stmt.execute({'id': user.id})
    order = orders_stmt.fetchone()
    process(user, order)
```
### Parameter Conversion

DBTK has tools to handle different parameter styles. You can use _named_ (`:name`) or _pyformat_ (`%(name)s`) in queries - DBTK can convert to the driver's native style. 
`cursor.execute_file()` and `PreparedStatment` will automatically rewrite the query and format parameters to match your database's paramstyle, making your queries portable across databases.

Oracle and PostgreSQL support both dictionary and positional parameters. Their default (db.driver.paramstyle) will be 
the dictionary style. If you want force positional mode ()

```python
from dbtk.utils import process_sql_parameters, ParamStyle
sql = 'SELECT * FROM users WHERE name = :name AND age > :age'
# query automatically rewritten and parameters formatted to match paramstyle
statement = dbtk.cursors.PreparedStatement(cursor, sql)

# Use a positional style if you are doing a large executemany,
positional_style = ParamStyle.get_positional_style(cur.paramstyle)
# `process_sql_parameters` to manually convert a query and get parameter order
query, param_names = process_sql_parameters(sql, positional_style)
print(param_names) # ['name', 'age']
# extra params ignored, missing defaulted
params = {'name': 'Aang', 
          'rank': 'Avatar'} 
# `prepare_params` converts params to whatever is needed by the query
bind_vars = cursor.prepare_params(param_names, params, 
                                  paramstyle=positional_style)
# ['Aang', None] or {'name': 'Aang', 'age': None} depending on paramstyle
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

Each database type has a convenience function with appropriate defaults. Some drivers have non-standard connection 
parameters. 

```python
from dbtk.database import postgres, oracle, mysql, sqlserver, sqlite

# PostgreSQL (default port: 5432)
db = postgres(user='admin', password='secret', database='mydb',
              host='localhost')

# Oracle (default port: 1521)
db = oracle(user='admin', password='secret', database='ORCL',
            host='localhost')

# MySQL (default port: 3306)
db = mysql(user='admin', password='secret', database='mydb',
           host='localhost', port=3307)

# SQL Server (default port: 1433)
db = sqlserver(user='admin', password='secret', database='mydb',
               host='localhost')

# SQLite (no host/user/password needed)
db = sqlite('path/to/database.db')
db = sqlite(':memory:')  # In-memory database
```

All functions take the standard `user`, `password`, `database` parameters and map to any driver specific non-standard names automatically, and accept `**kwargs` for driver-specific parameters.

## Error Handling

```python
try:
    cursor.execute("SELECT * FROM nonexistent_table")
except db.driver.DatabaseError as e:
    print(f"Database error: {e}")
except db.driver.IntegrityError as e:
    print(f"Integrity constraint violated: {e}")
```

## SQL File Execution and Prepared Statements

Write SQL once with `:named` or `%(pyformat)s` parameters — DBTK runs it on any database.

```python
# query.sql - write once, run anywhere
# SELECT * FROM users WHERE status = :status AND created > :start_date
#   -- or equivalently --
# SELECT * FROM users WHERE status = %(status)s AND created > %(start_date)s

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

**One-off queries with `execute_file()`:**

- Loads query from file and accepts `:named` or `%(pyformat)s` parameter format
- Converts parameters to match the cursor's native paramstyle
- Extra parameters in the dict are ignored; missing parameters default to NULL

```python
cursor.execute_file('queries/monthly_report.sql', {
    'start_date': '2025-01-01',
    'end_date': '2025-01-31'
})
results = cursor.fetchall()
```

**Prepared statements for repeated execution:**

`cursor.prepare_file()` does the same query and parameter transformation but returns a
`PreparedStatement` that can be executed many times and behaves like a cursor:

```python
# queries/kingdom_report.sql
# SELECT soldier_id, name, rank, missions_completed
# FROM soldiers
# WHERE kingdom = :kingdom
#   AND rank >= :min_rank
# ORDER BY missions_completed DESC

stmt = cursor.prepare_file('queries/kingdom_report.sql')

kingdoms = [
    {'kingdom': 'Fire Nation', 'min_rank': 'Captain'},
    {'kingdom': 'Earth Kingdom', 'min_rank': 'General'},
    {'kingdom': 'Water Tribe', 'min_rank': 'Warrior'},
]

for params in kingdoms:
    stmt.execute(params)
    data = stmt.fetchall()  # PreparedStatement acts like a cursor
    dbtk.writers.to_csv(data, f"reports/{params['kingdom'].replace(' ', '_')}.csv")
```

`PreparedStatement` is also the resolver type accepted by `IdentityManager` — see
[ETL: Tools & Logging](etl-tools.md).

**Benefits of SQL files:**
- Keep SQL separate from Python for better organisation and editor syntax highlighting
- Test queries independently before integration
- Reuse the same query across different scripts
- Write once, run on any database

## Best Practices

1. **Use context managers** - Ensures connections are properly closed
2. **Use named/pyformat parameters** - More readable and portable across databases
3. **Iterate large result sets** - Don't `fetchall()` millions of rows
4. **Use `transaction()` context manager** - Safe commit/rollback handling
5. **Use configuration files** - Keep credentials out of code
6. **Use `execute_file()`** - Portable SQL with automatic parameter conversion
7. **Use `PreparedStatement`** - Portable SQL to be executed repeatedly

## See Also

- [Record Objects](record.md) - Full documentation on DBTK's universal data structure
- [Configuration & Security](configuration.md) - YAML config files and password encryption
- [ETL: Table & Transforms](table.md) - Using cursors with Table and DataSurge
- [ETL: Tools & Logging](etl-tools.md) - IdentityManager and ValidationCollector
- [Readers & Writers](readers-writers.md) - Moving data between databases and files
