# examples.py - dbtk Usage Examples

import os
import dbtk
from dbtk.database import postgres, sqlite
from dbtk.config import encrypt_password_cli


# =============================================================================
# 1. Configuration Setup
# =============================================================================

def setup_encryption_key():
    """Generate and set encryption key for password storage."""
    from cryptography.fernet import Fernet

    # Generate a new key (do this once and store securely)
    key = Fernet.generate_key()
    print(f"Set this as your DBTK_ENCRYPTION_KEY environment variable:")
    print(key.decode())

    # Set in current session
    os.environ['DBTK_ENCRYPTION_KEY'] = key.decode()


def encrypt_password_example():
    """Example of encrypting a password for config file."""
    # First ensure encryption key is set
    if 'DBTK_ENCRYPTION_KEY' not in os.environ:
        setup_encryption_key()

    password = "my_secret_password"
    encrypted = encrypt_password_cli(password)
    print(f"Encrypted password: {encrypted}")
    print("Add this to your YAML config as 'encrypted_password'")


# =============================================================================
# 2. Basic Database Connections
# =============================================================================

def direct_connection_example():
    """Example of direct database connections."""

    # SQLite connection (simplest)
    with sqlite('example.db') as db:
        cursor = db.cursor()

        # Create sample table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS users (
                id INTEGER PRIMARY KEY,
                name TEXT,
                email TEXT,
                created_date DATE
            )
        """)

        # Insert sample data
        cursor.execute("INSERT INTO users (name, email, created_date) VALUES (?, ?, ?)",
                       ("John Doe", "john@example.com", "2024-01-15"))

        db.commit()
        print("SQLite example data created")


def config_connection_example():
    """Example using YAML configuration."""

    # Assuming you have a dbtk.yml file configured
    try:
        with dbtk.connect('local_db') as db:
            cursor = db.cursor()
            cursor.execute("SELECT * FROM users")

            for record in cursor:
                print(f"User: {record.name} ({record.email})")

    except FileNotFoundError:
        print("No dbtk.yml config file found. Create one first.")
    except Exception as e:
        print(f"Config connection failed: {e}")


# =============================================================================
# 3. Cursor Types Examples
# =============================================================================

def cursor_types_example():
    """Demonstrate different cursor types."""

    with sqlite('example.db') as db:
        # Default: RecordCursor - returns Record objects
        print("=== RecordCursor (default) ===")
        cursor = db.cursor()  # or db.cursor('record')
        cursor.execute("SELECT * FROM users LIMIT 1")
        record = cursor.fetchone()

        if record:
            print(f"Access by attribute: {record.name}")
            print(f"Access by key: {record['email']}")
            print(f"Access by index: {record[0]}")
            print(f"As dict: {record.copy()}")
            print()

        # TupleCursor - returns namedtuples
        print("=== TupleCursor ===")
        cursor = db.cursor('tuple')
        cursor.execute("SELECT * FROM users LIMIT 1")
        record = cursor.fetchone()

        if record:
            print(f"Namedtuple: {record}")
            print(f"Access: {record.name}")
            print()

        # DictCursor - returns OrderedDict
        print("=== DictCursor ===")
        cursor = db.cursor('dict')
        cursor.execute("SELECT * FROM users LIMIT 1")
        record = cursor.fetchone()

        if record:
            print(f"OrderedDict: {record}")
            print(f"Keys: {list(record.keys())}")
            print()

        # Basic Cursor - returns lists
        print("=== Basic Cursor ===")
        cursor = db.cursor('list')
        cursor.execute("SELECT * FROM users LIMIT 1")
        record = cursor.fetchone()

        if record:
            print(f"List: {record}")


# =============================================================================
# 4. Writers Examples
# =============================================================================

def writers_example():
    """Demonstrate various export formats."""

    with sqlite('example.db') as db:
        cursor = db.cursor()
        cursor.execute("SELECT * FROM users")

        # CSV export
        dbtk.writers.to_csv(cursor, 'users.csv')
        print("Exported to users.csv")

        # Excel export (requires openpyxl)
        try:
            cursor.execute("SELECT * FROM users")  # Re-execute since cursor is consumed
            dbtk.writers.to_excel(cursor, 'users.xlsx', sheet='Users')
            print("Exported to users.xlsx")
        except ImportError:
            print("Excel export requires: pip install openpyxl")

        # Fixed width export
        cursor.execute("SELECT * FROM users")
        column_widths = [5, 20, 30, 12]  # id, name, email, date
        dbtk.writers.to_fixed_width(cursor, column_widths, 'users.txt')
        print("Exported to users.txt")

        # Export to stdout
        cursor.execute("SELECT * FROM users")
        print("\n=== CSV to stdout ===")
        dbtk.writers.to_csv(cursor)  # No filename = stdout


# =============================================================================
# 5. Database-to-Database Copy
# =============================================================================

def database_copy_example():
    """Example of copying data between databases."""

    # Create source data
    with sqlite('source.db') as source_db:
        cursor = source_db.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS products (
                id INTEGER PRIMARY KEY,
                name TEXT,
                price REAL,
                category TEXT
            )
        """)

        # Insert sample data
        products = [
            (1, 'Laptop', 999.99, 'Electronics'),
            (2, 'Book', 19.99, 'Education'),
            (3, 'Headphones', 79.99, 'Electronics')
        ]

        cursor.executemany("INSERT OR REPLACE INTO products VALUES (?, ?, ?, ?)", products)
        source_db.commit()

    # Copy to target database
    with sqlite('source.db') as source_db, sqlite('target.db') as target_db:
        # Prepare target table
        target_cursor = target_db.cursor()
        target_cursor.execute("""
            CREATE TABLE IF NOT EXISTS products_copy (
                id INTEGER PRIMARY KEY,
                name TEXT,
                price REAL,
                category TEXT
            )
        """)

        # Get source data
        source_cursor = source_db.cursor()
        source_cursor.execute("SELECT * FROM products")

        # Copy data
        count = dbtk.writers.cursor_to_cursor(
            source_cursor,
            target_cursor,
            'products_copy'
        )

        print(f"Copied {count} records between databases")


# =============================================================================
# 6. Transaction Example
# =============================================================================

def transaction_example():
    """Example of transaction management."""

    with sqlite('example.db') as db:
        # Transaction context manager
        try:
            with db.transaction():
                cursor = db.cursor()

                # Multiple operations in transaction
                cursor.execute("INSERT INTO users (name, email) VALUES (?, ?)",
                               ("Alice", "alice@example.com"))
                cursor.execute("INSERT INTO users (name, email) VALUES (?, ?)",
                               ("Bob", "bob@example.com"))

                # If we reach here, transaction commits automatically
                print("Transaction committed successfully")

        except Exception as e:
            print(f"Transaction rolled back due to error: {e}")


# =============================================================================
# 7. Advanced Configuration
# =============================================================================

def advanced_config_example():
    """Example of advanced configuration features."""

    # Set specific config file
    dbtk.set_config_file('production.yml')

    try:
        # List available connections
        from dbtk.config import ConfigManager
        config = ConfigManager()
        connections = config.list_connections()
        print(f"Available connections: {connections}")

        # Get connection details
        if connections:
            details = config.get_connection_config(connections[0])
            print(f"Connection details: {details}")

    except FileNotFoundError:
        print("Config file not found")


# =============================================================================
# Main Demo
# =============================================================================

if __name__ == '__main__':
    print("=== dbtk Examples ===\n")

    # Setup
    direct_connection_example()

    # Basic usage
    print("1. Cursor Types:")
    cursor_types_example()

    print("\n2. Export Examples:")
    writers_example()

    print("\n3. Database Copy:")
    database_copy_example()

    print("\n4. Transactions:")
    transaction_example()

    print("\n5. Configuration:")
    config_connection_example()

    print("\n6. Password Encryption:")
    print("Run setup_encryption_key() and encrypt_password_example() as needed")

    print("\nExamples completed! Check the generated files.")

# =============================================================================
# Sample YAML Configuration
# =============================================================================

SAMPLE_CONFIG = """
# Save this as dbtk.yml in your project directory or ~/.config/dbtk.yml
connections:
  local_db:
    type: sqlite
    database: example.db

  dev_postgres:
    type: postgres
    host: localhost
    port: 5432
    database: dev_db
    user: developer
    password: ${DEV_DB_PASSWORD}  # from environment

  prod_warehouse:
    type: postgres
    host: warehouse.company.com
    database: analytics
    user: etl_user
    encrypted_password: "gAAAAABh..."  # encrypted password

  reporting_mysql:
    type: mysql
    host: mysql.company.com
    database: reports
    user: reporter
    password: secret123
    charset: utf8mb4
    
passwords:
  database_master_key:
    description: "Master encryption key for database"
    encrypted_password: "gAAAAABh4K8..."
  
  api_keys:
    description: "Third-party API credentials"
    password: ${EXTERNAL_API_KEY}  # Environment variable reference
"""


def create_sample_config():
    """Create a sample configuration file."""
    with open('dbtk.yml.example', 'w') as f:
        f.write(SAMPLE_CONFIG)
    print("Created dbtk.yml.example - copy to dbtk.yml and customize")