# DBTK Configuration Documentation

The `dbtk.config` module provides comprehensive configuration management for database connections and secure password storage using YAML configuration files with optional encryption.

## Table of Contents

- [Configuration File Setup](#configuration-file-setup)
- [Environment Setup](#environment-setup)
- [Configuration File Format](#configuration-file-format)
- [Database Connections](#database-connections)
- [Password Management](#password-management)
- [CLI Utilities](#cli-utilities)
- [API Reference](#api-reference)
- [Examples](#examples)


## Configuration File Setup

DBTK looks for configuration files in the following order:

1. `dbtk.yml` in the current directory
2. `dbtk.yaml` in the current directory
3. `~/.config/dbtk.yml`
4. `~/.config/dbtk.yaml`

You can also specify a custom config file path when initializing.

## Environment Setup

For password encryption functionality, set up an encryption key:

```bash
# Generate a new encryption key
python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"

# On Windows, save the generated key as a User or System variable named DBTK_ENCRYPTION_KEY

# On Linux/Mac add to your shell profile (.bashrc, .zshrc, etc.)
echo 'export DBTK_ENCRYPTION_KEY="your_generated_key_here"' >> ~/.bashrc
```

## Configuration File Format

### Basic Structure

```yaml
settings: 
  default_timezone: "America/Los_Angeles" 
  
connections:
  # Database connection configurations
  dev_db:
    type: postgres
    host: localhost
    port: 5432
    database: myapp
    user: dbuser
    password: mypassword
  prod_db:
    type: oracle
    host: dbserver.company.com
    database: myapp
    user: dbuser
    encrypted_password: "gAAAAABh..."  # encrypted
    character_set: AL32UTF8    

passwords:
  # Standalone password storage
  api_key:
    description: "OpenAI API Key"
    encrypted_password: "gAAAAABh..."  # encrypted
  
  jwt_secret:
    description: "JWT signing secret"
    password: "my-secret-key"  # plain text
```

### Supported Database Types

- **postgres**: PostgreSQL
- **mysql**: MySQL/MariaDB
- **oracle**: Oracle Database
- **sqlserver**: Microsoft SQL Server
- **sqlite**: SQLite

### Connection Parameters by Database Type
#### Oracle
```yaml
connections:
  prod_oracle:
    type: oracle
    host: dbserver.company.com
    port: 1521
    database: prod_db
    user: username
    encrypted_password: "gAAAAABo..."
    character_set: AL32UTF8
```

#### PostgreSQL
```yaml
connections:
  my_postgres:
    type: postgres
    host: localhost
    port: 5432
    database: mydb
    user: username
    password: password
    sslmode: prefer
    connect_timeout: 10
    application_name: myapp
    uri: postgresql://user:pass@host:port/db  # Alternative to individual params
```

#### MySQL
```yaml
connections:
  my_mysql:
    type: mysql
    host: localhost
    port: 3306
    database: mydb
    user: username
    password: password
    charset: utf8mb4
    ssl: true
```

#### SQLite
```yaml
connections:
  my_sqlite:
    type: sqlite
    database: /path/to/database.db
```

### Password Security Options

#### 1. Plain Text (not recommended for production)
```yaml
connections:
  dev_db:
    type: postgres
    host: localhost
    password: mypassword
```

#### 2. Environment Variables
```yaml
connections:
  prod_db:
    type: postgres
    host: prod-server
    password: ${DB_PASSWORD}  # References $DB_PASSWORD env var
```

#### 3. Encrypted Passwords
```yaml
connections:
  secure_db:
    type: postgres
    host: secure-server
    encrypted_password: "gAAAAABh4K8..."  # Encrypted with Fernet
```

#### 4. Standalone Encrypted Passwords
```yaml
passwords:
  database_master_key:
    description: "Master encryption key for database"
    encrypted_password: "gAAAAABh4K8..."
  
  api_keys:
    description: "Third-party API credentials"
    password: ${EXTERNAL_API_KEY}  # Environment variable reference
```

## Database Connections

### Basic Usage

```python
from dbtk.config import connect

# Connect using config
db = connect('prod_db')
cursor = db.cursor()
cursor.execute("SELECT * FROM users")
results = cursor.fetchall()
```

### Using Custom Config File

```python
from dbtk.config import connect, set_config_file

# Set global config file
set_config_file('/path/to/my/config.yml')
db = connect('my_connection')

# Or specify per connection
db = connect('my_connection', config_file='/path/to/config.yml')
```

### Managing Connections

```python
from dbtk.config import ConfigManager

config = ConfigManager()

# List available connections
print(config.list_connections())
# Output: ['dev_db', 'prod_db', 'test_db']

# Get connection details
conn_config = config.get_connection_config('prod_db')
print(conn_config)
# Output: {'type': 'postgres', 'host': 'prod-server', 'database': 'myapp', ...}
```

### Interactive Connection Management

```python
from dbtk.config import ConfigManager

config = ConfigManager()

# Add a new connection interactively
config.edit_connection_interactive('new_db', 'postgres')

# Edit existing connection
config.edit_connection_interactive('existing_db')
```

## Password Management

### Storing and Retrieving Passwords

```python
from dbtk.config import get_password, ConfigManager

# Get a stored password
api_key = get_password('openai_api_key')
jwt_secret = get_password('jwt_signing_key')

# Using ConfigManager directly
config = ConfigManager()
secret = config.get_password('my_secret')
```

### Managing Passwords Programmatically

```python
from dbtk.config import ConfigManager

config = ConfigManager()

# Add a password (encrypted by default)
config.add_password('new_secret', 'my_secret_value', 
                   description='My application secret')

# Add unencrypted password
config.add_password('dev_key', 'dev_value', encrypt=False)

# List all stored passwords
passwords = config.list_passwords()
print(passwords)  # ['api_key', 'jwt_secret', 'new_secret']

# Remove a password
config.remove_password('old_secret')
```

### Interactive Password Management

```python
from dbtk.config import ConfigManager

config = ConfigManager()

# Add or edit password interactively
config.edit_password_interactive('my_api_key')
# Prompts for description, password, and encryption preference
```

## CLI Utilities

### Encrypt Individual Passwords

```python
from dbtk.config import encrypt_password_cli

# Encrypt a password for manual config file editing
encrypted = encrypt_password_cli("my_secret_password")
print(f"encrypted_password: {encrypted}")
```

### Encrypt All Passwords in Config File

```python
from dbtk.config import encrypt_config_file_cli

# Encrypt all plain text passwords in a config file
encrypt_config_file_cli('dbtk.yml')
# Output: Encrypted 3 passwords in dbtk.yml
```

This utility will:
- Convert `password` fields to `encrypted_password` in connections
- Convert `value` fields to `encrypted_password` in passwords section
- Preserve existing encrypted passwords
- Create backup of original file

## API Reference

### ConfigManager Class

#### Constructor
```python
ConfigManager(config_file: Optional[str] = None)
```

#### Connection Methods
- `get_connection_config(name: str) -> Dict[str, Any]`
- `list_connections() -> List[str]`
- `edit_connection_interactive(name: str, db_type: Optional[str] = None) -> None`

#### Password Methods
- `get_password(name: str) -> str`
- `list_passwords() -> List[str]`
- `add_password(name: str, password: str, description: str = None, encrypt: bool = True) -> None`
- `remove_password(name: str) -> None`
- `edit_password_interactive(name: str) -> None`

#### Encryption Methods
- `encrypt_password(password: str) -> str`
- `decrypt_password(encrypted_password: str) -> str`

### Module Functions
- `connect(name: str, config_file: Optional[str] = None) -> Database`
- `get_password(name: str, config_file: Optional[str] = None) -> str`
- `set_config_file(config_file: str) -> None`
- `encrypt_password_cli(password: str) -> str`
- `encrypt_config_file_cli(filename: str) -> None`

## Examples

### Complete Configuration Example

```yaml
# dbtk.yml
settings:
  default_timezone: "America/Los_Angeles" 
connections:
  # Development database with plain password
  dev:
    type: postgres
    host: localhost
    port: 5432
    database: myapp_dev
    user: developer
    password: devpass123

  # Production database with encrypted password
  prod:
    type: postgres
    host: prod-db.company.com
    port: 5432
    database: myapp_prod
    user: app_user
    encrypted_password: "gAAAAABhABCDEF..."

  # Database using environment variable
  staging:
    type: postgres
    host: staging-db.company.com
    port: 5432
    database: myapp_staging
    user: staging_user
    password: ${STAGING_DB_PASSWORD}

  # MySQL connection
  analytics:
    type: mysql
    host: analytics-db
    port: 3306
    database: warehouse
    user: analyst
    encrypted_password: "gAAAAABhXYZ789..."

  # SQLite for local development
  local:
    type: sqlite
    database: /tmp/local.db

passwords:
  # API keys and secrets
  openai_api_key:
    description: "OpenAI API key for AI features"
    encrypted_password: "gAAAAABhAPIKEY..."
  
  jwt_secret:
    description: "JWT signing secret"
    encrypted_password: "gAAAAABhJWTSEC..."
  
  # Environment variable reference
  external_api_key:
    description: "Third-party service API key"
    password: ${EXTERNAL_SERVICE_API_KEY}
  
  # Development secret (unencrypted)
  dev_webhook_secret:
    description: "Development webhook validation secret"
    password: "dev-webhook-secret-123"
```

### Application Usage Example

```python
#!/usr/bin/env python3
"""
Example application using DBTK configuration
"""

from dbtk.config import connect, get_password
import requests

def main():
    # Connect to different databases
    dev_db = connect('dev')
    prod_db = connect('prod')
    
    # Get API credentials
    openai_key = get_password('openai_api_key')
    jwt_secret = get_password('jwt_secret')
    
    # Use in application
    cursor = prod_db.cursor()
    cursor.execute("SELECT id, email FROM users WHERE active = true")
    
    for user_id, email in cursor.fetchall():
        # Make API call with stored credentials
        response = requests.post(
            'https://api.openai.com/v1/completions',
            headers={'Authorization': f'Bearer {openai_key}'},
            json={'prompt': f'Generate welcome email for {email}'}
        )
        print(f"Generated content for user {user_id}")

if __name__ == '__main__':
    main()
```

### Setup Script Example

```python
#!/usr/bin/env python3
"""
Setup script to configure DBTK interactively
"""

from dbtk.config import ConfigManager
import os

def setup_config():
    # Check if encryption key is set
    if not os.environ.get('DBTK_ENCRYPTION_KEY'):
        print("Please set DBTK_ENCRYPTION_KEY environment variable first:")
        print("export DBTK_ENCRYPTION_KEY=\"$(python -c 'from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())')\"")
        return
    
    config = ConfigManager()
    
    # Setup database connections
    print("Setting up database connections...")
    config.edit_connection_interactive('prod', 'postgres')
    config.edit_connection_interactive('dev', 'postgres')
    
    # Setup passwords
    print("\nSetting up API keys and secrets...")
    config.edit_password_interactive('openai_api_key')
    config.edit_password_interactive('jwt_secret')
    
    print("\nConfiguration complete!")
    print(f"Connections: {config.list_connections()}")
    print(f"Passwords: {config.list_passwords()}")

if __name__ == '__main__':
    setup_config()
```

## Security Best Practices

1. **Always use encryption for production passwords**
   ```yaml
   connections:
     prod:
       type: postgres
       host: prod-server
       encrypted_password: "gAAAAABh..."  # Good
       # password: "plaintext"  # Bad for production
   ```

2. **Protect your encryption key**
   - Store `DBTK_ENCRYPTION_KEY` in environment variables, not in code
   - Use different keys for different environments
   - Consider using secret management systems (AWS Secrets Manager, HashiCorp Vault)

3. **Use environment variables for sensitive data**
   ```yaml
   connections:
     prod:
       password: ${PROD_DB_PASSWORD}  # References environment variable
   ```

4. **Set appropriate file permissions**
   ```bash
   chmod 600 ~/.config/dbtk.yml  # Read/write for owner only
   ```

5. **Separate environments**
   ```bash
   # Different config files for different environments
   DBTK_CONFIG=prod.yml python app.py
   DBTK_CONFIG=dev.yml python app.py
   ```

6. **Regularly rotate passwords and keys**
   - Update database passwords periodically
   - Generate new encryption keys when compromised
   - Use the CLI tools to re-encrypt with new keys

## Troubleshooting

### Common Issues

#### 1. Config File Not Found
```
FileNotFoundError: No config file found. Looked in: dbtk.yml, dbtk.yaml, ~/.config/dbtk.yml, ~/.config/dbtk.yaml
```

**Solution:**
```bash
# Create config directory and file
mkdir -p ~/.config
touch ~/.config/dbtk.yml

# Or create in current directory
touch dbtk.yml
```

#### 2. Encryption Key Not Set
```
ValueError: DBTK_ENCRYPTION_KEY environment variable not set
```

**Solution:**
```bash
# Generate and set encryption key
export DBTK_ENCRYPTION_KEY="$(python -c 'from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())')"
```

#### 3. Invalid Connection Configuration
```
ValueError: Connection 'mydb' not found in config. Available connections: ['dev', 'prod']
```

**Solution:**
```python
from dbtk.config import ConfigManager

config = ConfigManager()
print("Available connections:", config.list_connections())

# Add missing connection
config.edit_connection_interactive('mydb', 'postgres')
```

### Migration Scripts

#### Migrating from Plain Text to Encrypted Passwords
Rather than encrypting passwords one at a time.  You can generate a config file that has all of your passwords in plain text and run `encrypt_config_file_cli` to encrypt them all at once.
```python
#!/usr/bin/env python3
"""
Migrate existing plain text passwords to encrypted format
"""

from dbtk.config import encrypt_config_file_cli
encrypt_config_file_cli('dbtk.yml')
```

#### Environment Variable Setup Script

```bash
#!/bin/bash
# setup_dbtk_env.sh

# Generate encryption key if not already set
if [ -z "$DBTK_ENCRYPTION_KEY" ]; then
    echo "Generating new DBTK encryption key..."
    KEY=$(python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())")
    echo "export DBTK_ENCRYPTION_KEY=\"$KEY\"" >> ~/.bashrc
    echo "export DBTK_ENCRYPTION_KEY=\"$KEY\"" >> ~/.zshrc
    echo "Encryption key added to shell profiles"
    echo "Please restart your shell or run: source ~/.bashrc"
else
    echo "DBTK_ENCRYPTION_KEY already set"
fi

# Create config directory
mkdir -p ~/.config/dbtk
echo "Created config directory: ~/.config/dbtk"

# Create sample config if it doesn't exist
if [ ! -f ~/.config/dbtk.yml ]; then
    cat > ~/.config/dbtk.yml << EOF
# DBTK Configuration File
# Add your database connections and passwords here

connections:
  # Example PostgreSQL connection
  # my_postgres:
  #   type: postgres
  #   host: localhost
  #   port: 5432
  #   database: mydb
  #   user: myuser
  #   encrypted_password: "your_encrypted_password_here"

passwords:
  # Example password storage
  # my_api_key:
  #   description: "API key for external service"
  #   encrypted_password: "your_encrypted_password_here"

EOF
    echo "Created sample config: ~/.config/dbtk.yml"
else
    echo "Config file already exists: ~/.config/dbtk.yml"
fi

chmod 600 ~/.config/dbtk.yml
echo "Set secure permissions on config file"
```

## Integration Examples

### Data Pipeline Integration

```python
#!/usr/bin/env python3
"""
ETL Pipeline using DBTK configuration
"""

import logging
from dbtk.config import connect, get_password
import pandas as pd

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DataPipeline:
    def __init__(self):
        self.source_db = connect('source_warehouse')
        self.target_db = connect('target_analytics')
        self.api_key = get_password('external_api_key')
    
    def extract_data(self):
        """Extract data from source database"""
        logger.info("Extracting data from source...")
        query = """
        SELECT customer_id, order_date, total_amount, status
        FROM orders 
        WHERE created_at >= CURRENT_DATE - INTERVAL '1 day'
        """
        return pd.read_sql(query, self.source_db.connection)
    
    def transform_data(self, df):
        """Transform extracted data"""
        logger.info("Transforming data...")
        df['order_date'] = pd.to_datetime(df['order_date'])
        df['revenue'] = df['total_amount'].astype(float)
        return df.groupby(['customer_id', 'order_date']).agg({
            'revenue': 'sum',
            'status': 'count'
        }).rename(columns={'status': 'order_count'})
    
    def load_data(self, df):
        """Load data to target database"""
        logger.info("Loading data to target...")
        df.to_sql('daily_customer_summary', self.target_db.connection, 
                  if_exists='append', index=False)
    
    def run(self):
        """Run the complete pipeline"""
        try:
            data = self.extract_data()
            transformed_data = self.transform_data(data)
            self.load_data(transformed_data)
            logger.info(f"Pipeline completed successfully. Processed {len(data)} records.")
        except Exception as e:
            logger.error(f"Pipeline failed: {e}")
            raise
        finally:
            self.source_db.close()
            self.target_db.close()

if __name__ == '__main__':
    pipeline = DataPipeline()
    pipeline.run()
```

## Configuration Templates

### Multi-Environment Template
You can define connections for your different dev, test, production environments and deploy that to the various servers where your integrations are running.  But a better approach is to have different files for each environment with the same name but that point to the database server for that environment.
```yaml
# dbtk.yml on production server
connections:
  app_db:
    type: postgres
    host: prod-db-cluster.company.com
    port: 5432
    database: myapp_prod
    user: app_user
    encrypted_password: "gAAAAABhPROD..."
  
  analytics_db:
    type: postgres
    host: prod-analytics-db.company.com
    database: warehouse_prod
    user: analytics_user
    encrypted_password: "gAAAAABhANAL..."

passwords:
  jwt_secret:
    description: "JWT signing secret for production"
    encrypted_password: "gAAAAABhJWT_PROD..."
  
  openai_api_key:
    description: "OpenAI API key - production"
    encrypted_password: "gAAAAABhOPEN_PROD..."
```

```yaml
# dbtk.yml on development server
connections:
  app_db:
    type: postgres
    host: dev-db-cluster.company.com
    port: 5432
    database: myapp_prod
    user: app_user
    encrypted_password: "gAAAAABhDEV..."
  
  analytics_db:
    type: postgres
    host: dev-analytics-db.company.com
    database: warehouse_prod
    user: analytics_user
    encrypted_password: "gAAAAABhANAL..."

passwords:
  jwt_secret:
    description: "JWT signing secret for production"
    encrypted_password: "gAAAAABhJWT_PROD..."
  
  openai_api_key:
    description: "OpenAI API key - production"
    encrypted_password: "gAAAAABhOPEN_PROD..."
```