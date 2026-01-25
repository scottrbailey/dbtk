# Configuration & Security

DBTK uses YAML configuration files to manage database connections and keep credentials secure with encryption.

## Quick Start

Running the `dbtk config-setup` command line is the quickest way to get started. It will set up an encryption key (if keyring) is installed and copy the sample config file to your `~/.config` directory. 

```yaml
databases:
  dev_db:
    driver: postgres
    host: localhost
    database: myapp_dev
    user: developer
    password: dev_password

  prod_db:
    driver: postgres
    host: db.example.com
    database: myapp_prod
    user: app_user
    password: !encrypted AQECAHi7L5...  # Use dbtk-encrypt
```

Connect from code:

```python
import dbtk

# Uses dbtk.yml in current directory by default
db = dbtk.connect('prod_db')
```

## Configuration File Locations

DBTK searches for config files in this order:

1. Explicitly set path: `dbtk.set_config_file('path/to/config.yml')`
2. Current directory: `./dbtk.yml`
3. User home: `~/.dbtk/dbtk.yml`
4. System-wide: `/etc/dbtk/dbtk.yml` (Unix/Linux)

```python
import dbtk

# Use specific config file
dbtk.set_config_file('/path/to/production.yml')
db = dbtk.connect('database_name')

# Use default search path
db = dbtk.connect('database_name')
```

## Configuration File Structure

### Basic Database Connection

```yaml
databases:
  my_database:
    driver: postgres           # Database type
    host: localhost           # Server hostname
    port: 5432               # Port (optional, uses driver default)
    database: mydb           # Database name
    user: myuser            # Username
    password: mypassword    # Plaintext works, but encrypt it! See dbtk-encrypt below
```

### Driver-Specific Options

**PostgreSQL:**
```yaml
databases:
  postgres_db:
    driver: postgres
    host: localhost
    database: mydb
    user: myuser
    password: secret
    # Optional psycopg2 parameters
    connect_timeout: 10
    sslmode: require
```

**Oracle:**
```yaml
databases:
  oracle_db:
    driver: oracle
    user: myuser
    password: secret
    # Choose one connection method:
    service_name: ORCL              # TNS service name
    # OR
    host: localhost
    port: 1521
    sid: ORCL
    # OR
    dsn: "(DESCRIPTION=...)"        # Full TNS string
```

**MySQL:**
```yaml
databases:
  mysql_db:
    driver: mysql
    host: localhost
    database: mydb
    user: myuser
    password: secret
    charset: utf8mb4
    connect_timeout: 10
```

**SQL Server:**
```yaml
databases:
  sqlserver_db:
    driver: sqlserver
    server: localhost           # Note: 'server' not 'host'
    database: mydb
    user: myuser
    password: secret
    # Optional
    driver_name: "ODBC Driver 17 for SQL Server"
    TrustServerCertificate: yes
```

**SQLite:**
```yaml
databases:
  sqlite_db:
    driver: sqlite
    database: /path/to/database.db
    # SQLite doesn't need host/user/password
```

### Default Cursor Settings

Set default cursor behavior for all connections:

```yaml
databases:
  my_database:
    driver: postgres
    host: localhost
    database: mydb
    user: myuser
    password: secret
    cursor_settings:
      column_case: preserve      # 'lower', 'upper', 'preserve'
      batch_size: 5000          # Batch size for executemany
      debug: false              # Print SQL queries
```

## Password Encryption

### Encrypting Passwords

Use the `dbtk-encrypt` command-line tool to encrypt passwords:

```bash
# Encrypt a password
$ dbtk-encrypt mypassword
!encrypted AQECAHi7L5WLo...

# Generate a new encryption key
$ dbtk-encrypt --new-key
New encryption key: fernet:d09af5b3c...

# Encrypt with specific key
$ dbtk-encrypt --key fernet:d09af5b3c... mypassword
!encrypted AQECAHi7L5WLo...
```

### Using Encrypted Passwords

Add the encrypted password to your config file with the `!encrypted` tag:

```yaml
databases:
  prod_db:
    driver: postgres
    host: db.example.com
    database: production
    user: app_user
    password: !encrypted AQECAHi7L5WLo4Kg8ZYE...
```

### Encryption Keys

DBTK looks for encryption keys in this order:

1. Environment variable: `DBTK_ENCRYPTION_KEY`
2. Key file: `~/.dbtk/encryption.key`
3. System keyring (if available)

**Recommended setup for production:**

```bash
# Generate key
$ dbtk-encrypt --new-key > ~/.dbtk/encryption.key
$ chmod 600 ~/.dbtk/encryption.key

# Encrypt password
$ dbtk-encrypt --key-file ~/.dbtk/encryption.key mypassword
!encrypted AQECAHi7L5WLo...

# Set environment variable (optional, for containers/CI)
$ export DBTK_ENCRYPTION_KEY=fernet:d09af5b3c...
```

**Security best practices:**

1. Never commit encryption keys to version control
2. Use different keys for dev/staging/production
3. Rotate keys periodically
4. Store keys in secure key management systems (AWS KMS, HashiCorp Vault, etc.)
5. Use environment variables in containerized environments

## Multiple Configuration Files

Maintain separate configs for different environments:

**development.yml:**
```yaml
databases:
  app_db:
    driver: postgres
    host: localhost
    database: myapp_dev
    user: developer
    password: dev_password
```

**production.yml:**
```yaml
databases:
  app_db:
    driver: postgres
    host: db.example.com
    database: myapp_prod
    user: app_user
    password: !encrypted AQECAHi...
```

**In your application:**
```python
import os
import dbtk

# Load environment-specific config
env = os.getenv('ENVIRONMENT', 'development')
dbtk.set_config_file(f'{env}.yml')

db = dbtk.connect('app_db')
```

## Connection String Format

For simple cases, you can use connection strings instead of config files:

```python
from dbtk.database import postgres, oracle, mysql

# PostgreSQL
db = postgres('postgresql://user:pass@localhost/mydb')

# Oracle
db = oracle('oracle://user:pass@localhost:1521/ORCL')

# MySQL
db = mysql('mysql://user:pass@localhost/mydb')
```

## Environment Variables

Override config values with environment variables:

```yaml
databases:
  app_db:
    driver: postgres
    host: ${DB_HOST:localhost}           # Default to localhost
    database: ${DB_NAME}                 # Required
    user: ${DB_USER}
    password: !encrypted ${DB_PASSWORD}  # Can encrypt env vars too
```

```bash
export DB_HOST=db.example.com
export DB_NAME=production
export DB_USER=app_user
export DB_PASSWORD=AQECAHi7L5WLo...
```

## Connection Aliases

Create shortcuts for common connections:

```yaml
databases:
  # Full definitions
  prod_warehouse:
    driver: postgres
    host: warehouse.example.com
    database: analytics
    user: etl_user
    password: !encrypted AQECAHi...

  prod_app:
    driver: postgres
    host: app.example.com
    database: myapp
    user: app_user
    password: !encrypted AQECAHi...

  # Alias for convenience
  default: prod_app
  warehouse: prod_warehouse
```

```python
# These are equivalent
db = dbtk.connect('prod_app')
db = dbtk.connect('default')
```

## Advanced Configuration

### Connection Pooling

Configure connection pool settings:

```yaml
databases:
  app_db:
    driver: postgres
    host: localhost
    database: mydb
    user: myuser
    password: secret
    pool:
      min_size: 2
      max_size: 20
      timeout: 30
```

### Read-Only Connections

Ensure connections are read-only:

```yaml
databases:
  reporting_db:
    driver: postgres
    host: replica.example.com
    database: analytics
    user: readonly_user
    password: !encrypted AQECAHi...
    options:
      read_only: true
```

### Custom Driver Parameters

Pass any driver-specific parameters:

```yaml
databases:
  oracle_db:
    driver: oracle
    user: myuser
    password: !encrypted AQECAHi...
    service_name: ORCL
    # Oracle-specific parameters
    threaded: true
    events: true
    encoding: UTF-8
```

## Validation and Testing

### Validate Configuration

```python
import dbtk

# Test connection
try:
    db = dbtk.connect('prod_db')
    cursor = db.cursor()
    cursor.execute("SELECT 1")
    print("✓ Connection successful")
    db.close()
except Exception as e:
    print(f"✗ Connection failed: {e}")
```

### Configuration Linting

Use `dbtk-validate` to check config files:

```bash
$ dbtk-validate dbtk.yml
✓ Configuration valid
✓ 3 database connections defined
⚠ Warning: 'dev_db' uses unencrypted password
```

## Migration from Other Tools

### From SQLAlchemy

```python
# SQLAlchemy
from sqlalchemy import create_engine
engine = create_engine('postgresql://user:pass@localhost/mydb')

# DBTK equivalent
import dbtk
db = dbtk.database.postgres(user='user', password='pass',
                            host='localhost', database='mydb')
```

### From Django

```python
# Django settings.py
DATABASES = {
    'default': {
        'ENGINE': 'django.db.backends.postgresql',
        'NAME': 'mydb',
        'USER': 'user',
        'PASSWORD': 'pass',
        'HOST': 'localhost',
    }
}

# DBTK config.yml
databases:
  default:
    driver: postgres
    database: mydb
    user: user
    password: !encrypted AQECAHi...
    host: localhost
```

## Troubleshooting

### Connection Fails

```python
# Enable debug logging
import logging
logging.basicConfig(level=logging.DEBUG)

import dbtk
db = dbtk.connect('my_database')  # Will show detailed connection info
```

### Encryption Key Not Found

```bash
# Check key location
$ cat ~/.dbtk/encryption.key

# Or set environment variable
$ export DBTK_ENCRYPTION_KEY=fernet:d09af5b3c...
```

### Config File Not Found

```python
import dbtk

# Show config search paths
print(dbtk.config.get_config_paths())

# Use explicit path
dbtk.set_config_file('/absolute/path/to/config.yml')
```

## Security Checklist

- [ ] Use encrypted passwords in production config files
- [ ] Never commit encryption keys to version control
- [ ] Set restrictive permissions on config files (`chmod 600`)
- [ ] Use environment variables for sensitive values in CI/CD
- [ ] Rotate encryption keys periodically
- [ ] Use separate configs for dev/staging/production
- [ ] Store encryption keys in secure key management systems
- [ ] Enable SSL/TLS for database connections
- [ ] Use read-only users where appropriate
- [ ] Audit database access logs regularly

## See Also

- [Database Connections](database-connections.md) - Using the Database and Cursor classes
- [ETL Framework](etl.md) - Building production ETL pipelines
- [Advanced Features](advanced.md) - Custom drivers and performance tuning
