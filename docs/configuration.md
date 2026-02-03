# Configuration & Security

DBTK uses YAML configuration files to manage database connections and keep credentials secure with encryption. Running `dbtk checkup` will copy a well commented sample config file to _~/.config/dbtk_sample.yml_. 
DBTK also has several [command line tools](#command-line-tools) to assist with configuration and encryption.

## Quick Start

Create a `dbtk.yml` file in your project folder or in ~/.config folder:

```yaml
connections:
  dev_db:
    type: postgres
    host: localhost
    database: myapp_dev
    user: developer
    password: dev_password

  prod_db:
    type: postgres
    host: db.example.com
    database: myapp_prod
    user: app_user
    encrypted_password: gAAAAABh...  # Use dbtk encrypt-password
```

Connect from code:

```python
import dbtk

db = dbtk.connect('prod_db')
# or with context manager
with dbtk.connect('prod_db') as db:
    cur = db.cursor()
```

## Configuration File Locations

DBTK searches for config files in this order:

1. Explicitly set path: `dbtk.set_config_file('path/to/config.yml')`
2. Current directory: `./dbtk.yml` or `./dbtk.yaml`
3. User config: `~/.config/dbtk.yml` or `~/.config/dbtk.yaml`

If no config file is found, a sample is automatically created at `~/.config/dbtk_sample.yml`.

```python
import dbtk

# Use specific config file
dbtk.set_config_file('/path/to/production.yml')
db = dbtk.connect('database_name')

# Use default search path
db = dbtk.connect('database_name')
```

## Configuration File Structure

The config file has three main sections: `settings`, `connections`, and optionally `passwords` and `drivers`.

### Settings

```yaml
settings:
  default_batch_size: 1000
  default_country: US
  default_timezone: UTC

  # Logging configuration for integration scripts
  logging:
    directory: ./logs
    level: INFO
    format: '%(asctime)s [%(levelname)s] %(name)s: %(message)s'
    timestamp_format: '%Y-%m-%d %H:%M:%S'
    filename_format: '%Y%m%d_%H%M%S'
    split_errors: true  # If True, separate error log will be created (only if critical or errors are encountered
    console: true
    retention_days: 30
```

### Database Connections

Each connection is defined under the `connections:` key with a `type` field indicating the database type:

```yaml
connections:
  my_database:
    type: postgres              # Database type: postgres, oracle, mysql, sqlserver, sqlite
    host: localhost             # Server hostname
    port: 5432                  # Port (optional, uses driver default)
    database: mydb              # Database name
    user: myuser                # Username
    password: mypassword        # Plaintext - encrypt it! See below
    encrypted_password: gAAAAABh... #Use `dbtk encrypt-password mypassword` 
```

**Driver selection:**

You can optionally specify a `driver` field to choose a specific database adapter. If omitted, DBTK automatically selects the best available driver by priority. If you specify `driver`, `type` is optional since DBTK can infer it from the driver name.

```yaml
connections:
  # Use psycopg (v3) instead of default psycopg2
  pg_v3:
    driver: psycopg
    host: localhost
    database: mydb
    user: myuser
    encrypted_password: gAAAAABh...

  # ODBC connection with DSN
  odbc_db:
    driver: pyodbc_postgres
    dsn: MY_DSN
    password: '${MY_PASSWORD}'    # Pull from environment variable
```

### Cursor Settings

Set default cursor behavior for all cursors created from a connection:

```yaml
connections:
  my_database:
    type: postgres
    host: localhost
    database: mydb
    user: myuser
    encrypted_password: gAAAAABh...
    cursor:
      batch_size: 4000          # Rows to process at once in bulk operations
      debug: false              # Print SQL queries and bind variables
      return_cursor: true       # execute() returns cursor for method chaining
      fast_executemany: true    # For pyodbc SQL Server bulk inserts
```

### Driver-Specific Examples

**PostgreSQL:**
```yaml
connections:
  postgres_db:
    type: postgres
    host: localhost
    port: 5432
    database: mydb
    user: myuser
    encrypted_password: gAAAAABh...
```

**Oracle:**
```yaml
connections:
  oracle_db:
    type: oracle
    host: oracle.company.com
    port: 1521
    database: prod.company.com   # Service name
    user: app_user
    encrypted_password: gAAAAABh...
```

**MySQL:**
```yaml
connections:
  mysql_db:
    type: mysql
    host: localhost
    port: 3306
    database: mydb
    user: myuser
    password: secret
```

**SQL Server:**
```yaml
connections:
  sqlserver_db:
    driver: pyodbc_sqlserver
    type: sqlserver
    host: localhost\SQLEXPRESS    # Instance name supported
    database: mydb
    user: myuser
    password: secret
    cursor:
      fast_executemany: true     # Recommended for bulk operations
```

**SQLite:**
```yaml
connections:
  sqlite_db:
    type: sqlite
    database: /path/to/database.db
```

## Password Encryption

DBTK uses Fernet symmetric encryption (from the `cryptography` library) for password storage. Before you can begin encrypting and decrypting passwords, you must generate and store an encryption key.
If system keyring is available, this is as easy as running `dbtk store-key`. See [Command-Line Tools](#command-line-tools) and [Programatic Encryption](#programmatic-encryption) sections for help.

### Encryption Key Management

DBTK looks for encryption keys in this order:

1. Environment variable: `DBTK_ENCRYPTION_KEY`
2. System keyring: service `dbtk`, key `encryption_key` (requires `keyring` library)

### Command-Line Tools

All encryption operations use the `dbtk` CLI with subcommands:

```bash
# Generate a new encryption key
$ dbtk generate-key              # Generate a key to manually store in environmental variable

# Store key in system keyring (requires keyring library)
$ dbtk store-key [key]            # Store provided key or generate new one
$ dbtk store-key --force          # Overwrite existing key

# Encrypt a single password
$ dbtk encrypt-password mypassword
gAAAAABh...

# Encrypt all passwords in a config file
# Finds plaintext 'password:' entries and converts to 'encrypted_password:'
$ dbtk encrypt-config dbtk.yml

# Migrate config to a new encryption key
$ dbtk migrate-config old_config.yml new_config.yml --new-key "new_key_here"

# Check dependencies, drivers, and configuration
$ dbtk checkup

# Interactive configuration setup wizard
$ dbtk config-setup
```

### Using Encrypted Passwords

In your config file, use `encrypted_password` instead of `password`:

```yaml
connections:
  prod_db:
    type: postgres
    host: db.example.com
    database: production
    user: app_user
    encrypted_password: gAAAAABh...
```

### Programmatic Encryption

```python
from dbtk import config

# Generate encryption key
key = config.generate_encryption_key()

# Encrypt/decrypt passwords
cfg = config.ConfigManager()
encrypted = cfg.encrypt_password('my_secret')
decrypted = cfg.decrypt_password(encrypted)

# Encrypt all passwords in a config file
config.encrypt_config_file('dbtk.yml')

# Store key in system keyring
config.store_key(key, force=True)
```

### Environment Variables in Connection Config

Reference environment variables in any connection parameter using `${VAR_NAME}` syntax. You can also provide a default value with `${VAR_NAME:default}`:

```yaml
connections:
  # Required env var (fails if not set)
  prod_db:
    type: postgres
    host: '${PROD_HOST}'
    password: '${PROD_PASSWORD}'

  # With defaults (uses default if env var not set)
  dev_db:
    type: postgres
    host: '${DB_HOST:localhost}'
    port: '${DB_PORT:5432}'
    database: '${DB_NAME:myapp_dev}'
    user: '${DB_USER:developer}'
    password: '${DB_PASSWORD:dev_password}'
```

This is especially useful for Docker/CI environments where you want a config that works both locally (using defaults) and in production (using env vars).

### Recommended Setup for Production

```bash
# 1. Generate encryption key
$ dbtk generate-key
# Output: kL7xP9... (your Fernet key)

# 2. Store key securely
$ export DBTK_ENCRYPTION_KEY=kL7xP9...   # For containers/CI
# OR
$ dbtk store-key kL7xP9...               # For workstations with keyring

# 3. Encrypt passwords in config
$ dbtk encrypt-config dbtk.yml
```

**Key rotation:**

```bash
export DBTK_ENCRYPTION_KEY="current_key"
NEW_KEY=$(dbtk generate-key)
dbtk migrate-config dbtk.yml dbtk_new.yml --new-key "$NEW_KEY"
export DBTK_ENCRYPTION_KEY="$NEW_KEY"
mv dbtk_new.yml dbtk.yml
```

## Standalone Passwords

Store non-database credentials (API keys, etc.) in the `passwords` section:

```yaml
passwords:
  openai_key:
    description: "OpenAI API key for data processing"
    encrypted_password: gAAAAABh...
```

## Custom Driver Registration

Register custom database drivers in the config file:

```yaml
drivers:
  firebird:
    database_type: firebird
    module: firebird.driver        # Only needed if name doesn't match module
    priority: 1
    param_map: {}                  # Map non-standard parameter names
    required_params: [{'host', 'database', 'user'}, {'dsn'}]
    optional_params: {'port', 'password'}
    connection_method: kwargs
    default_port: 3050
```

Or register programmatically:

```python
from dbtk.database import register_user_drivers

register_user_drivers({
    'my_driver': {
        'database_type': 'postgres',
        'priority': 10,
        'param_map': {'database': 'dbname'},
        'required_params': [{'host', 'database', 'user'}],
        'optional_params': {'port', 'password'},
        'connection_method': 'kwargs',
        'default_port': 5432
    }
})
```

## Security Best Practices

1. Use `encrypted_password` in production config files
2. Never commit encryption keys to version control
3. Use `DBTK_ENCRYPTION_KEY` environment variable in containerized environments
4. Use system keyring on workstations (`dbtk store-key`)
5. Rotate keys periodically with `dbtk migrate-config`
6. Set restrictive permissions on config files (`chmod 600`)
7. Use separate configs for dev/staging/production

## See Also

- [Database Connections](database-connections.md) - Using the Database and Cursor classes
- [ETL Framework](etl.md) - Building production ETL pipelines
- [Advanced Features](advanced.md) - Custom drivers and performance tuning
