# Database Connections

**The problem:** Every database has different connection parameters, drivers, and parameter styles. Writing portable database code is painful.

**The solution:** DBTK provides a unified interface that handles all the complexity. Write your code once, and it works seamlessly across PostgreSQL, Oracle, MySQL, SQL Server, and SQLite. The library automatically detects available drivers and handles parameter style conversions transparently.

## Quick Start

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

## Access the Full Stack

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

## Transaction Management

Context managers make transactions safe and simple:

```python
with db.transaction():
    cursor = db.cursor()
    cursor.execute("INSERT INTO battles ...")
    cursor.execute("UPDATE casualties ...")
    # Auto-commit on success, rollback on exception
```

## Cursor Types

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

## Record Objects - Maximum Flexibility

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
