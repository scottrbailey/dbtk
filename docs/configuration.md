# Configuration & Security

**The problem:** Hardcoded credentials are a security nightmare, and managing different database connections across environments is tedious.

**The solution:** DBTK uses YAML configuration files with support for encrypted passwords and environment variables. Store credentials securely, version control your configuration (without passwords), and switch between environments effortlessly.

## Configuration File Format

Create a `dbtk.yml` file for your database connections. Configurations can be project-scoped (`./dbtk.yml`) or user-scoped (`~/.config/dbtk.yml`).

```yaml
settings:
  default_timezone: UTC
  default_country: US
  default_paramstyle: named

connections:
  # PostgreSQL with encrypted password
  water_tribe_main:
    type: postgres
    host: localhost
    port: 5432
    database: northern_water_tribe
    user: waterbender_admin
    encrypted_password: gAAAAABh...
    cursor:
      type: dict
      column_case: preserve
      return_cursor: True

  # Oracle with environment variable
  earth_kingdom_prod:
    type: oracle
    host: ba-sing-se.earthkingdom.gov
    port: 1521
    database: CITIZEN_DB
    user: dai_li_ops
    password: ${EARTH_KINGDOM_PASSWORD}

  # MySQL with custom driver
  fire_nation_archive:
    type: mysql
    driver: mysqlclient
    host: fire-lord-palace.fn.gov
    port: 3306
    database: historical_records
    user: phoenix_king_admin
    password: sozins_comet_2024

  # SQLite local database
  air_nomad_local:
    type: sqlite
    database: /path/to/air_temples.db

passwords:
  # Standalone encrypted passwords for API keys, etc.
  api_key_avatar_hotline:
    encrypted_password: gAAAAABh...
    description: Avatar hotline API key

  # Environment variable password
  secret_tunnel:
    password: ${SECRET_TUNNEL_PASSWORD}
    description: Secret tunnel access code
```

## Password Encryption

Secure your credentials with encryption:

```python
import dbtk

# Generate encryption key (store in DBTK_ENCRYPTION_KEY environment variable)
key = dbtk.config._generate_encryption_key()

# Encrypt all passwords in configuration file
dbtk.config.encrypt_config_file('fire_nation_secrets.yml')

# Retrieve encrypted password
sozin_secret = dbtk.config.get_password('phoenix_king_battle_plans')

# Manually encrypt a single password
encrypted = dbtk.config.encrypt_password('only_azula_knows_this')

# Migrate configuration with new encryption key
new_key = dbtk.config._generate_encryption_key()
dbtk.config.migrate_config('old_regime.yml', 'phoenix_king_era.yml',
                           new_encryption_key=new_key)
```

## Command Line Tools

DBTK provides command-line utilities for managing encryption keys and configuration files. These are especially useful for automating deployment and configuration management in CI/CD pipelines:

```bash
# Generate a new encryption key
# Store the output in DBTK_ENCRYPTION_KEY environment variable
dbtk generate-key

# Store key on system keyring. If no key is provided, a new one will be generated
# Use --force to overwrite an existing key
# The DBTK_ENCRYPTION_KEY environment variable takes precedence
dbtk store-key [your_key] --force

# Encrypt all passwords in a configuration file
# Prompts for each plaintext password and replaces with encrypted_password
dbtk encrypt-config ./dbtk.yml

# Encrypt a specific password
# Returns the encrypted string you can paste into your config
dbtk encrypt-password "sozins_comet_2024"

# Migrate config file to a new encryption key
# Useful when rotating encryption keys
export DBTK_ENCRYPTION_KEY="old_key_here"
dbtk migrate-config old_config.yml new_config.yml --new-key "new_key_here"

# Run a check of which recommended libraries, database drivers are installed
# and check configuration, encryption keys, etc.
dbtk checkup
```

**Common workflow for new deployments:**

```bash
# 1. Generate encryption key and save to environment
export DBTK_ENCRYPTION_KEY=$(dbtk generate-key)

# 2. Create config file with plaintext passwords
cat > dbtk.yml <<EOF
connections:
  production_db:
    type: postgres
    host: db.example.com
    user: admin
    password: my_secret_password
EOF

# 3. Encrypt all passwords in config
dbtk encrypt-config dbtk.yml

# 4. Verify - passwords should now be encrypted_password entries
cat dbtk.yml
```

**Key rotation workflow:**

```bash
# When rotating encryption keys for security
export DBTK_ENCRYPTION_KEY="current_key"
NEW_KEY=$(dbtk generate-key)

# Decrypt with old key, encrypt with new key
dbtk migrate-config dbtk.yml dbtk_new.yml --new-key "$NEW_KEY"

# Update environment variable and swap files
export DBTK_ENCRYPTION_KEY="$NEW_KEY"
mv dbtk.yml dbtk_old.yml
mv dbtk_new.yml dbtk.yml
```
