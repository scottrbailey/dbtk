# Troubleshooting Guide

Common issues and solutions when using DBTK.

## Installation Issues

### Import Errors

**Problem:** `ImportError: No module named 'dbtk'`

**Solution:**
```bash
# Verify installation
pip show dbtk

# Reinstall if needed
pip install --upgrade dbtk
```

### Driver Import Errors

**Problem:** `ModuleNotFoundError: No module named 'psycopg2'`

**Solution:** Install the appropriate database driver:
```bash
pip install psycopg2-binary  # PostgreSQL
pip install oracledb         # Oracle
pip install mysqlclient      # MySQL
pip install pyodbc           # SQL Server
```

**Check available drivers:**
```bash
dbtk checkup
```

---

## Database Connection Issues

### Connection Refused

**Problem:** `psycopg2.OperationalError: could not connect to server: Connection refused`

**Causes:**
1. Database server not running
2. Wrong host/port
3. Firewall blocking connection

**Solution:**
```python
# Verify connection parameters
from dbtk.database import postgres
db = postgres(
    host='localhost',  # Try 127.0.0.1 if localhost fails
    port=5432,         # Check actual port
    database='mydb',
    user='myuser',
    password='mypassword'
)

# Test connection
try:
    cursor = db.cursor()
    cursor.execute("SELECT 1")
    print("Connected!")
except Exception as e:
    print(f"Connection failed: {e}")
```

### Authentication Failed

**Problem:** `psycopg2.OperationalError: FATAL:  password authentication failed`

**Solutions:**
```python
# 1. Verify credentials
# 2. Check user permissions
# 3. For PostgreSQL, check pg_hba.conf

# Test with different authentication method
db = postgres(
    host='localhost',
    database='mydb',
    user='myuser',
    password='correctpassword'  # Verify this is correct
)
```

### Connection Timeout

**Problem:** Connection hangs or times out

**Solution:**
```python
# Add timeout parameter (driver-specific)
db = postgres(
    host='remote-db.example.com',
    database='mydb',
    user='myuser',
    password='mypassword',
    connect_timeout=10  # Seconds
)
```

### Encrypted Password Not Working

**Problem:** `cryptography.fernet.InvalidToken`

**Causes:**
1. Wrong encryption key
2. Key not found
3. Password encrypted with different key

**Solution:**
```bash
# Verify encryption key is set
echo $DBTK_ENCRYPTION_KEY

# Or check system keyring
dbtk store-key --show

# Re-encrypt with correct key
dbtk encrypt-config dbtk.yml
```

---

## SQL and Query Issues

### Parameter Style Errors

**Problem:** `TypeError: not all arguments converted during string formatting`

**Cause:** Using wrong parameter style for your database

**Solution:**
```python
# ✓ CORRECT: Use named parameters - DBTK converts automatically
cursor.execute(
    "SELECT * FROM users WHERE status = :status AND age > :age",
    {'status': 'active', 'age': 18}
)

# ✗ WRONG: Don't mix parameter styles
cursor.execute(
    "SELECT * FROM users WHERE status = %s AND age > :age",  # Mixed!
    ['active', 18]
)

# Use PreparedStatement or execute_file() for automatic conversion
stmt = cursor.prepare_file('query.sql')
stmt.execute({'status': 'active', 'age': 18})
```

### SQL Syntax Errors

**Problem:** `psycopg2.errors.SyntaxError: syntax error at or near "..."`

**Debugging:**
```python
# Enable debug mode to see generated SQL
cursor = db.cursor(debug=True)
cursor.execute("SELECT * FROM users WHERE id = :id", {'id': 123})
# Prints: SELECT * FROM users WHERE id = %(id)s
# Params: {'id': 123}

# Or use get_sql() with Table
table.set_values(record)
sql = table.get_sql('insert')
print(f"Generated SQL: {sql}")
```

### Column Not Found

**Problem:** `psycopg2.errors.UndefinedColumn: column "user_name" does not exist`

**Solution:**
```python
# Check column names in database
cursor.execute("SELECT * FROM users LIMIT 1")
print(cursor.columns())  # Show actual column names

# Use correct mapping in Table
table = Table('users', {
    'username': {'field': 'user_name'},  # Map to correct source field
    # 'user_name': {}  # ✗ If DB column is 'username' not 'user_name'
}, cursor=cursor)
```

---

## File I/O Issues

### File Not Found

**Problem:** `FileNotFoundError: [Errno 2] No such file or directory: 'data.csv'`

**Solution:**
```python
import os
from pathlib import Path

# Use absolute paths
file_path = Path('/full/path/to/data.csv')
with dbtk.readers.get_reader(file_path) as reader:
    process(reader)

# Or check current directory
print(f"Current directory: {os.getcwd()}")
print(f"File exists: {Path('data.csv').exists()}")
```

### Character Encoding Errors

**Problem:** `UnicodeDecodeError: 'utf-8' codec can't decode byte 0xff`

**Solutions:**
```python
# 1. Use utf-8-sig to handle BOM (Byte Order Mark)
with dbtk.readers.CSVReader(open('data.csv', encoding='utf-8-sig')) as reader:
    process(reader)

# 2. Try different encodings
encodings = ['utf-8-sig', 'latin-1', 'cp1252', 'iso-8859-1']
for enc in encodings:
    try:
        with open('data.csv', encoding=enc) as f:
            reader = dbtk.readers.CSVReader(f)
            print(f"Success with encoding: {enc}")
            break
    except UnicodeDecodeError:
        continue

# 3. Detect encoding
# pip install chardet
import chardet
with open('data.csv', 'rb') as f:
    result = chardet.detect(f.read(10000))
    print(f"Detected encoding: {result['encoding']}")
```

### Corrupted Compressed Files

**Problem:** `gzip.BadGzipFile: Not a gzipped file`

**Solution:**
```python
# Verify file is actually compressed
import gzip
try:
    with gzip.open('data.csv.gz', 'rt') as f:
        f.read(100)  # Try reading first 100 bytes
    print("File is valid gzip")
except gzip.BadGzipFile:
    print("File is not gzipped or is corrupted")
    # Try reading as plain file
    with open('data.csv.gz', 'r') as f:
        reader = dbtk.readers.CSVReader(f)
```

### Excel File Errors

**Problem:** `ModuleNotFoundError: No module named 'openpyxl'`

**Solution:**
```bash
# Install Excel support
pip install openpyxl

# Or install full format support
pip install dbtk[formats]
```

---

## ETL and Table Issues

### Missing Required Fields

**Problem:** `ValueError: Required columns missing: {'email', 'name'}`

**Debugging:**
```python
# Check what's missing
table.set_values(record)
if not table.is_ready('insert'):
    missing = table.reqs_missing('insert')
    print(f"Missing fields: {missing}")
    print(f"Current values: {table.values}")

# Check source data
print(f"Record keys: {list(record.keys())}")
print(f"Record (normalized): {list(record.keys(normalized=True))}")
```

**Solutions:**
```python
# 1. Add defaults for missing fields
table = Table('users', {
    'email': {'field': 'email_address', 'nullable': False},
    'status': {'default': 'active'},  # Always has value
}, cursor=cursor)

# 2. Skip incomplete records
for record in reader:
    table.set_values(record)
    if table.is_ready('insert'):
        table.execute('insert')
    # Silently skip incomplete
```

### Transform Function Errors

**Problem:** `ValueError: invalid literal for int() with base 10: 'N/A'`

**Solution:**
```python
# Use transforms that handle None/empty gracefully
from dbtk.etl import transforms as tx

# ✓ CORRECT: get_int() returns None for invalid values
table = Table('users', {
    'age': {'field': 'age_string', 'fn': tx.get_int},  # Returns None if invalid
}, cursor=cursor)

# Or use string shorthand with default
table = Table('users', {
    'age': {'field': 'age_string', 'fn': 'int:0'},  # Returns 0 if invalid
}, cursor=cursor)

# For custom error handling
def safe_int(value):
    try:
        return int(value) if value else None
    except (ValueError, TypeError):
        logger.warning(f"Invalid integer: {value}")
        return None

table = Table('users', {
    'age': {'field': 'age_string', 'fn': safe_int},
}, cursor=cursor)
```

### Database Expression Errors

**Problem:** `RuntimeError: BulkSurge does not support db_expr columns`

**Solution:**
```python
# BulkSurge can't use database functions (db_expr)
# Use DataSurge instead
surge = DataSurge(table)  # Supports db_expr
surge.insert(records)

# Or remove db_expr from column config for BulkSurge
table = Table('users', {
    'created_at': {'db_expr': 'CURRENT_TIMESTAMP'},  # ✗ Won't work with BulkSurge
}, cursor=cursor)

# For BulkSurge, compute in Python
table = Table('users', {
    'created_at': {'default': lambda: datetime.now()},  # ✓ Works with BulkSurge
}, cursor=cursor)
```

---

## Performance Issues

### Slow Bulk Loading

**Problem:** Insert/update operations taking too long

**Solutions:**
```python
# 1. Increase batch size
surge = DataSurge(table, batch_size=5000)  # Default is 1000

# 2. Use BulkSurge for maximum speed
surge = BulkSurge(table)  # 2-3x faster than DataSurge
surge.load(records)

# 3. Use transactions
surge = DataSurge(table, use_transaction=True)

# 4. Disable indexes during load, re-enable after
cursor.execute("ALTER TABLE users DISABLE TRIGGER ALL")  # PostgreSQL
surge.insert(records)
cursor.execute("ALTER TABLE users ENABLE TRIGGER ALL")
db.commit()
```

### Memory Issues with Large Files

**Problem:** `MemoryError` when processing large files

**Solutions:**
```python
# 1. Don't call fetchall() on huge result sets
# ✗ BAD: Loads everything into memory
results = cursor.fetchall()

# ✓ GOOD: Iterate (constant memory)
for row in cursor:
    process(row)

# 2. Use reader streaming (already efficient)
with dbtk.readers.get_reader('huge_file.csv.gz') as reader:
    for record in reader:  # Streams, doesn't load all
        process(record)

# 3. For DataFrames, use lazy/streaming
import polars as pl
df = pl.scan_csv('huge.csv').collect(streaming=True)

# 4. Increase compressed file buffer if needed
from dbtk.defaults import settings
settings['compressed_file_buffer_size'] = 2 * 1024 * 1024  # 2MB
```

### Slow Queries

**Problem:** Queries taking too long

**Debugging:**
```python
import time

# Time the query
start = time.time()
cursor.execute("SELECT * FROM huge_table WHERE status = :status",
               {'status': 'active'})
elapsed = time.time() - start
print(f"Query took {elapsed:.2f} seconds")

# Check query plan (PostgreSQL)
cursor.execute("EXPLAIN ANALYZE SELECT * FROM huge_table WHERE status = 'active'")
print(cursor.fetchall())
```

**Solutions:**
1. Add database indexes on frequently queried columns
2. Use `LIMIT` for development/testing
3. Filter data in database, not Python
4. Use database-side aggregation (GROUP BY, etc.)

---

## BulkSurge Specific Issues

### MySQL: "LOAD DATA LOCAL INFILE forbidden"

**Problem:** `mysql.connector.errors.DatabaseError: 1148: LOAD DATA LOCAL INFILE command is denied`

**Solution:**
```python
# Check server setting
cursor.execute("SELECT @@local_infile")
result = cursor.fetchone()
print(f"local_infile: {result[0]}")  # Must be 1

# If 0, contact DBA or use external method
surge.load(reader, method='external')  # Dumps CSV with instructions
```

### SQL Server: "connection_name missing"

**Problem:** `RuntimeError: BCP needs credentials. Please set up a named connection in the config file.`

**Solution:**
```python
# ✗ WRONG: Direct connection won't work for BCP
db = dbtk.sqlserver(host='server', user='sa', password='secret')

# ✓ CORRECT: Use named connection from config
db = dbtk.connect('mssql_prod')  # From dbtk.yml

# In dbtk.yml:
# connections:
#   mssql_prod:
#     type: sqlserver
#     host: server
#     database: mydb
#     user: sa
#     encrypted_password: gAAAAABh...
```

### Oracle: "direct_path_load not supported"

**Problem:** `AttributeError: module 'oracledb' has no attribute 'direct_path_load'`

**Solution:**
```bash
# Upgrade python-oracledb to 3.4+
pip install --upgrade oracledb

# Or use external method (SQL*Loader)
surge.load(reader, method='external')
```

---

## IdentityManager Issues

### Resolver Not Finding Records

**Problem:** All entities end up as `NOT_FOUND`

**Debugging:**
```python
# Test resolver independently
stmt = cursor.prepare_file('sql/resolve_user.sql')
stmt.execute({'user_id': '12345'})
result = stmt.fetchone()
print(f"Resolver result: {result}")

# Check IdentityManager configuration
im = IdentityManager('user_id', 'person_id', resolver=stmt)
entity = im.resolve({'user_id': '12345'})
print(f"Status: {entity['_status']}")
print(f"Entity: {entity}")
```

**Common Issues:**
1. SQL query returns no rows (check WHERE clause)
2. Wrong key column name in resolver query
3. Source data has wrong key values

### State File Errors

**Problem:** `JSONDecodeError: Expecting value`

**Solution:**
```python
# State file corrupted or incomplete
# Delete and regenerate
import os
os.remove('state/entities.json')

# Or validate before loading
import json
try:
    with open('state/entities.json') as f:
        data = json.load(f)
    im = IdentityManager.load_state('state/entities.json', resolver=stmt)
except json.JSONDecodeError:
    print("State file corrupted, starting fresh")
    im = IdentityManager('source_id', 'target_id', resolver=stmt)
```

---

## Logging Issues

### Log Files Not Created

**Problem:** No log files in logs directory

**Solution:**
```python
# Ensure logging is initialized
import dbtk
dbtk.setup_logging()  # Must call this first

# Check settings
from dbtk.defaults import settings
print(settings['logging'])

# Verify directory exists and is writable
from pathlib import Path
log_dir = Path('./logs')
log_dir.mkdir(parents=True, exist_ok=True)
print(f"Log directory exists: {log_dir.exists()}")
print(f"Log directory writable: {os.access(log_dir, os.W_OK)}")
```

### Errors Not Detected

**Problem:** `errors_logged()` returns None but errors occurred

**Solution:**
```python
# Ensure split_errors is enabled in config
# dbtk.yml:
# settings:
#   logging:
#     split_errors: true  # Required for errors_logged() to work

# Or check main log file if split_errors=false
dbtk.setup_logging()
# ... your code ...
error_log = dbtk.errors_logged()
if error_log:
    print(f"Errors in: {error_log}")
else:
    # Check if any ERROR/CRITICAL was logged
    import logging
    logger = logging.getLogger()
    # Review log file manually
```

---

## Getting Help

If you're still stuck:

1. **Enable debug mode:**
   ```python
   cursor = db.cursor(debug=True)  # Prints SQL and parameters
   ```

2. **Check the examples:**
   - Look in `/examples` folder for working code
   - Review [Getting Started](01-getting-started.md)

3. **Search documentation:**
   - [Database Connections](03-database-connections.md)
   - [ETL: Table & Transforms](07-table.md)
   - [ETL: DataSurge & BulkSurge](08-datasurge.md)
   - [API Reference](api.rst)

4. **Report issues:**
   - GitHub Issues: https://github.com/yourusername/dbtk/issues
   - Include: DBTK version, Python version, database type, error message

5. **Check dependencies:**
   ```bash
   dbtk checkup  # Shows installed drivers and configuration
   ```
