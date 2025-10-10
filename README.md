# Data Bender Toolkit (dbtk)

**Control and Manipulate the Flow of Data** - A lightweight Python toolkit for data integration, transformation, and movement between systems.

Like the elemental benders of Avatar, this library gives you precise control over data - the world's most rapidly growing element. Extract data from various sources, transform it through powerful operations, and load it exactly where it needs to go. This library is designed by and for data integrators.

**Design philosophy:** This library is designed to get data to and from your databases with minimal hassle. It is well suited for data integration and ELT jobs. Modern databases do an amazing job at aggregating and transforming data, and we believe in leveraging those strengths. However, if you are doing heavy transforms in Python, we recommend looking at other tool chains like Pandas and polars.

## Features

- **Universal Database Connectivity** - Unified interface across PostgreSQL, Oracle, MySQL, SQL Server, and SQLite with intelligent driver auto-detection
- **Flexible File Reading** - CSV, Excel (XLS/XLSX), JSON, NDJSON, XML, and fixed-width text files with consistent API
- **Multiple Export Formats** - Write to CSV, Excel, JSON, NDJSON, XML, fixed-width text, or directly between databases
- **Advanced ETL Framework** - Full-featured Table class for complex data transformations, validations, and upserts
- **Bulk Operations** - DataSurge class for high-performance batch INSERT/UPDATE/DELETE/MERGE operations
- **Data Transformations** - Built-in functions for dates, phones, emails, and custom data cleaning with international support
- **Encrypted Configuration** - YAML-based config with password encryption and environment variable support
- **Smart Cursors** - Multiple result formats: Records, named tuples, dictionaries, or plain lists

## Quick Start

### Installation

```bash
pip install dbtk

# For encrypted passwords
pip install dbtk[encryption]  # installs cryptography and keyring 

# For reading/writing XML and Excel files
pip install dbtk[formats]     # lxml and openpyxl 

# Full functionality
pip install dbtk[all]         # all optional dependencies

# Database adapters (install as needed)
pip install psycopg2          # PostgreSQL
pip install oracledb          # Oracle
pip install mysqlclient       # MySQL
```

### Basic Usage

```python
import dbtk

# Connect using YAML config
with dbtk.connect('fire_nation_prod') as db:
    cursor = db.cursor()
    cursor.execute("SELECT * FROM firebenders WHERE rank = 'General'")
    
    # Materialize results for multiple outputs
    generals = cursor.fetchall()
    
    # Export to different formats
    dbtk.writers.to_excel(generals, 'fire_nation_generals.xlsx')
    dbtk.writers.to_csv(generals, 'fire_nation_generals.csv')
```

**ETL Example with transformations:**

```python
import dbtk
from dbtk.etl import Table, transforms

with dbtk.connect('avatar_training_grounds') as db:
    cursor = db.cursor()

    # Define ETL table with transformations
    recruit_table = Table('air_nomads', {
        'nomad_id': {'field': 'id', 'primary_key': True},
        'name': {'field': 'full_name', 'nullable': False},
        'email': {'field': 'contact_scroll', 'fn': transforms.email_clean, 'nullable': False},
        'sky_bison': {'field': 'companion_name'},
        'training_date': {'field': 'started_training', 'fn': transforms.parse_date},
        'airbending_level': {'field': 'mastery_level', 'db_fn': 'calculate_airbending_rank(#)'},
        'last_meditation': {'db_fn': 'CURRENT_TIMESTAMP'},
        'temple_origin': {'value': 'Eastern Air Temple'}})
        
    # Process records with validation and transformation
    with dbtk.readers.get_reader('new_air_nomad_recruits.csv') as reader:
        for recruit in reader:
            recruit_table.set_values(recruit)
            if recruit_table.reqs_met:
                recruit_table.exec_insert(cursor)
            else:
                print(f"Recruit needs more training: missing {recruit_table.reqs_missing}")
```

### Configuration

Create a `dbtk.yml` file for your database connections. Configurations can be project-scoped (`./dbtk.yml`) or user-scoped (`~/.config/dbtk.yml`).

```yaml
settings:
  default_timezone: UTC
  default_country: US
  default_paramstyle: named

connections:
  fire_nation_census:
    type: postgres
    host: sozin.fire-nation.gov
    port: 5432
    database: population_records
    user: fire_lord_archivist
    encrypted_password: gAAAAABh...  # Encrypted password

  ba_sing_se_records:
    type: oracle
    host: earth-kingdom-db.bss
    port: 1521
    database: CITIZEN_REGISTRY
    user: dai_li_agent
    password: ${EARTH_KINGDOM_SECRET}  # Environment variable
```

## Core Components

### Database Connections

Connect to databases with automatic driver detection and unified interface. 

The Database class provides a uniform API across all database types, abstracting away driver-specific differences. This means you can write code once and easily switch between PostgreSQL, Oracle, MySQL, and other databases without rewriting your application logic.

DBTK maintains a clean reference hierarchy that gives you access to the full stack when needed. Cursor objects maintain a reference to the connection (cursor.connection), and the connection objects maintain a reference to the underlying driver (connection.interface).
```python
import dbtk

# Direct connections
fire_db = dbtk.database.postgres(user='azula', password='blue_flames', database='fire_nation')
earth_db = dbtk.database.oracle(user='toph', password='metal_bending', database='ba_sing_se')

# From configuration
db = dbtk.connect('northern_water_tribe')

# Different cursor types for different needs
cursor = db.cursor('record')        # Record class - row['name'], row.name, row[0]
cursor_t = db.cursor('tuple')       # namedtuple   - row.name, row[0]
cursor_l = db.cursor('list')        # simple list  - row[0]
cursor_d = db.cursor('dict')        # OrderedDict  - row['name']

# Access the underlying driver for maximum flexibility
print(db.interface.__name__)  # 'psycopg2', 'oracledb', etc.
print(db.interface.paramstyle)  # 'named', 'qmark', etc.

# Use driver-specific exceptions for targeted error handling
try:
    cursor.execute(sql)
except cursor.connection.interface.DatabaseError as e:
    # Handle database-specific errors
    logger.error(f"Database error: {e}")

# Access driver-specific features when needed
if db.interface.__name__ == 'psycopg2':
    # Use PostgreSQL-specific functionality
    cursor.execute("LISTEN channel_name")
```

**Supported databases:**
- PostgreSQL (psycopg2, psycopg3, pgdb)
- Oracle (oracledb, cx_Oracle)
- MySQL (mysqlclient, mysql.connector, pymysql, MySQLdb)
- SQL Server (pyodbc, pymssql)
- SQLite (built-in)

### File Readers

Read data from multiple file formats with a consistent interface:

```python
from dbtk import readers

# CSV files
with readers.CSVReader(open('northern_water_tribe_census.csv')) as reader:
    for waterbender in reader:
        print(f"Waterbender: {waterbender.name}, Village: {waterbender.village}")

# Excel spreadsheets
with readers.get_reader('fire_nation_army.xlsx', sheet_index=1) as reader:
    for soldier in reader:
        print(f"Rank: {soldier.military_rank}, Firebending Level: {soldier.flame_intensity}")

# Fixed-width text files
columns = [
    readers.FixedColumn('earthbender_name', 1, 25),
    readers.FixedColumn('rock_throwing_distance', 26, 35, 'float'),
    readers.FixedColumn('training_complete_date', 36, 46, 'date')
]
with readers.FixedReader(open('earth_kingdom_records.txt'), columns) as reader:
    for earthbender in reader:
        print(f"{earthbender.earthbender_name}: {earthbender.rock_throwing_distance} meters")

# JSON files
with readers.JSONReader(open('eastern_air_temple.json')) as reader:
    for monk in reader:
        print(f"Air Nomad: {monk.monk_name}, Sky Bison: {monk.sky_bison_companion}")

# XML files with XPath
xml_columns = [
    readers.XMLColumn('avatar_id', xpath='@reincarnation_cycle'),
    readers.XMLColumn('avatar_name', xpath='./name/text()'),
    readers.XMLColumn('mastered_elements', xpath='.//elements/mastered')
]
with readers.XMLReader(open('avatar_chronicles.xml'), 
                       record_xpath='//avatar', 
                       columns=xml_columns) as reader:
    for avatar in reader:
        print(f"Avatar {avatar.avatar_name}: {avatar.mastered_elements}")
```

**Automatic format detection:**

```python
import dbtk
# Automatically detects format from extension
with dbtk.readers.get_reader('data.xlsx') as reader:
    for record in reader:
        process(record)
```

### Data Writers

Export data to multiple formats with a consistent interface. All writers can consume a result set directly from a cursor, or a materialized result set (list of Records, namedtuples, dicts).

```python
from dbtk import writers

# CSV export
writers.to_csv(cursor, 'northern_tribe_waterbenders.csv', delimiter='\t')

# Excel workbooks with multiple sheets
writers.to_excel(cursor, 'fire_nation_report.xlsx', sheet='Q1 Intelligence')

# JSON output
writers.to_json(cursor, 'air_temples/meditation_records.json')

# NDJSON (newline-delimited JSON) for streaming
writers.to_ndjson(cursor, 'battle_logs.ndjson')

# XML with custom elements
writers.to_xml(cursor, 'citizens.xml', record_element='earth_kingdom_citizen')

# Fixed-width format for legacy systems
column_widths = [20, 15, 10, 12]
writers.to_fixed_width(cursor, column_widths, 'ba_sing_se_daily_announcements.txt')

# Direct database-to-database transfer
source_cursor.execute("SELECT * FROM water_tribe_defenses")
count = writers.cursor_to_cursor(source_cursor, target_cursor, 'intel_archive')
print(f"Transferred {count} strategic records")
```

### ETL Operations

The Table class provides a stateful interface for complex ETL operations with field mapping, transformations, and validations:

```python
import dbtk
from dbtk.etl import transforms
from dbtk.database import ParamStyle

# Auto-generate configuration from existing table
config = dbtk.etl.generate_table_config(cursor, 'air_nomad_training', add_comments=True)

# Define ETL mapping with transformations
phoenix_king_army = dbtk.etl.Table('fire_nation_soldiers', {
    'soldier_id': {'field': 'recruit_number', 'primary_key': True},
    'name': {'field': 'full_name', 'nullable': False},
    'home_village': {'field': 'birthplace', 'nullable': False},
    'firebending_skill': {'field': 'flame_control_level', 'fn': transforms.get_int},
    'enlistment_date': {'field': 'joined_army', 'fn': transforms.parse_date},
    'combat_name': {'field': 'full_name', 'db_fn': 'generate_fire_nation_callsign(#)'},
    'last_drill': {'db_fn': 'CURRENT_TIMESTAMP'},
    'conscription_source': {'value': 'Sozin Recruitment Drive'}}, 
    paramstyle=ParamStyle.NAMED)

# Process records with validation
with dbtk.readers.get_reader('fire_nation_conscripts.csv') as reader:
    phoenix_king_army.calc_update_excludes(reader.headers)
    for recruit in reader:
        phoenix_king_army.set_values(recruit)
        if phoenix_king_army.reqs_met:
            existing_soldier = phoenix_king_army.get_db_record(cursor)
            if existing_soldier:
                phoenix_king_army.exec_update(cursor)
            else:
                phoenix_king_army.exec_insert(cursor)
        else:
            print(f"Recruit rejected: missing {phoenix_king_army.reqs_missing}")
```

**Key features:**
- Field mapping and renaming
- Data type transformations
- Database function integration
- Required field validation
- Primary key management
- Automatic UPDATE exclusions
- Support for INSERT, UPDATE, DELETE, MERGE operations

### Bulk Operations with DataSurge

For high-volume data processing, use DataSurge for efficient batch operations:

```python
from dbtk.etl import DataSurge

# Define table configuration
recruit_table = dbtk.etl.Table('fire_nation_soldiers', columns_config)

# Create DataSurge instance for bulk operations
bulk_writer = DataSurge(recruit_table)

# Bulk insert with batching
with dbtk.readers.get_reader('massive_conscript_list.csv') as reader:
    errors = bulk_writer.insert(cursor, reader, batch_size=2000)
    print(f"Inserted {recruit_table.counts['insert']} records with {errors} errors")

# Bulk merge (upsert) operations
with dbtk.readers.get_reader('soldier_updates.csv') as reader:
    errors = bulk_writer.merge(cursor, reader, batch_size=1000)
```

**DataSurge features:**
- Automatic batching for optimal performance
- Smart merge strategies (native upsert vs temp table based on database)
- Configurable error handling
- Progress tracking and logging
- Support for INSERT, UPDATE, DELETE, MERGE operations

### Data Transformations

Built-in transformation functions for common data cleaning tasks:

```python
from dbtk.etl import transforms as tx

# Date and time parsing
tx.parse_date("Year 100 AG, Day 15")      # Flexible date parsing
tx.parse_datetime("100 AG Summer Solstice T14:30:00Z")  # With timezone support
tx.parse_timestamp("1642262200")          # Unix timestamp support

# International phone number handling (with phonenumbers library)
tx.phone_clean("5551234567")              # -> "(555) 123-4567"
tx.phone_format("+44 20 7946 0958", tx.PhoneFormat.NATIONAL)  # UK format
tx.phone_validate("+1-800-AVATAR")        # Validation
tx.phone_get_type("+1-800-CABBAGES")      # -> "toll_free"

# Email validation and cleaning
tx.email_validate("guru.pathik@eastern.air.temple")  # -> True
tx.email_clean("  TOPH@BEIFONG.EARTHKINGDOM ")      # -> "toph@beifong.earthkingdom"

# Utility functions
tx.coalesce([None, "", "Jasmine Tea", "Ginseng Tea"])  # -> "Jasmine Tea"
tx.indicator("Firebender", true_val="Fire Nation Citizen")  # Conditional values
tx.get_int("123.45 gold pieces")  # -> 123
```

**Transformation features:**
- Flexible date/time parsing with multiple format support
- International phone number support with country-specific formatting
- Email validation and normalization
- Type coercion and conversion
- Null value handling
- Timezone support

## Advanced Features

### Encrypted Configuration

Secure database credentials with password encryption:

```python
import dbtk

# Generate encryption key (store in DBTK_ENCRYPTION_KEY environment variable)
key = dbtk.config.generate_encryption_key()

# Encrypt all passwords in configuration file
dbtk.config.encrypt_config_file_cli('fire_nation_secrets.yml')

# Retrieve encrypted password
sozin_secret = dbtk.config.get_password('phoenix_king_battle_plans')

# Manually encrypt a single password
encrypted = dbtk.config.encrypt_password_cli('only_azula_knows_this')

# Migrate configuration with new encryption key
new_key = dbtk.config.generate_encryption_key()
dbtk.config.migrate_config_cli('old_regime.yml', 'phoenix_king_era.yml', 
                                new_encryption_key=new_key)
```

### Transaction Management

Use context managers for safe transaction handling:

```python
with db.transaction():
    cursor = db.cursor()
    cursor.execute("INSERT INTO battles ...")
    cursor.execute("UPDATE casualties ...")
    # Auto-commit on success, rollback on exception
```

### Custom Transformations

Create custom transformation functions for your ETL pipelines:

```python
def standardize_nation(val):
    """Standardize nation names to official designations."""
    nation_map = {
        'Fire Nation Colonies': 'Earth Kingdom', 
        'Foggy Swamp Tribe': 'Earth Kingdom',
        'Kyoshi Warriors': 'Earth Kingdom'
    }
    return nation_map.get(val, val)

# Use in Table configuration
four_nations_census = dbtk.etl.Table('population_registry', {
    'nation': {'field': 'home_nation', 'fn': standardize_nation},
    # ... other fields
})
```

### Multiple Cursor Types

Choose the right cursor type for your use case:

```python
# Record cursor - most flexible, supports dict and attribute access
cursor = db.cursor('record')
row = cursor.fetchone()
print(row['name'], row.name, row[0], '\t'.join(row[:5]))  # All work

# Tuple cursor - lightweight namedtuple
cursor = db.cursor('tuple')
row = cursor.fetchone()
print(row.name, row[0], '\t'.join(row[:5]))  # Named and indexed access, slice

# Dict cursor - dictionary-only access
cursor = db.cursor('dict')
row = cursor.fetchone()
print(row['name'])  # Dictionary access only

# List cursor - minimal overhead
cursor = db.cursor('list')
row = cursor.fetchone()
print(row[0], '\t'.join([:5]))  # Index access and slice only
```

### Header Cleaning

Automatically clean and standardize column names. Clean.STANDARDIZE is useful when you need to process similar files from multiple sources, for instance, prospect records from multiple vendors. 

```python
import dbtk
from dbtk.readers import Clean

# Different cleaning levels
headers = ["ID #", "Student Name", "Residency Code", "GPA Score", "Has Holds?"]

[Clean.normalize(h, Clean.NOOP) for h in headers]
# ['ID #', 'Student Name', 'Residency Code', 'GPA Score', 'Has Holds?']
[Clean.normalize(h, Clean.LOWER) for h in headers]
# ['id #', 'student name', 'residency code', 'gpa score', 'has holds?']
[Clean.normalize(h, Clean.LOWER_NOSPACE) for h in headers]
# ['id_#', 'student_name', 'residency_code', 'gpa_score', 'has_holds?']
[Clean.normalize(h, Clean.LOWER_ALPHANUM) for h in headers]
# ['id', 'studentname', 'residencycode', 'gpascore', 'hasholds']
[Clean.normalize(h, Clean.STANDARDIZE) for h in headers]
# ['id', 'studentname', 'residency', 'gpascore', 'hasholds']

# Specifying how much header cleaning should be done when opening a reader
reader = dbtk.readers.CSVReader(fp, clean_headers=Clean.STANDARDIZE) 
```

## Database Support

DBTK supports multiple database adapters with automatic detection and fallback:

| Database    | Driver           | Install Command                      | Notes                                                     |
|-------------|------------------|--------------------------------------|-----------------------------------------------------------|
| PostgreSQL  | psycopg2         | `pip install psycopg2-binary`        | Recommended                                               |
| PostgreSQL  | psycopg (3)      | `pip install psycopg-binary`         | Newest version                                            |
| PostgreSQL  | pgdb             | `pip install pgdb`                   | DB-API compliant                                          |
| Oracle      | oracledb         | `pip install oracledb`               | Oracle client not required                                |
| Oracle      | cx_Oracle        | `pip install cx_Oracle`              | Requires Oracle client                                    |
| MySQL       | mysqlclient      | `pip install mysqlclient`            | Fastest option                                            |
| MySQL       | mysql.connector  | `pip install mysql-connector-python` | Official MySQL connector                                  |
| MySQL       | pymysql          | `pip install pymysql`                | Pure Python, lightweight                                  |
| SQL Server  | pyodbc           | `pip install pyodbc`                 | ODBC driver required                                      |
| SQL Server  | pymssql          | `pip install pymssql`                | Lightweight, DB-API compliant                             |
| SQLite      | sqlite3          | Built-in                             | No installation needed                                    |

**Note:** ODBC adapters require appropriate ODBC drivers to be installed on the system.

## Configuration File Format

Complete YAML configuration example:

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
  # Standalone encrypted passwords
  api_key_avatar_hotline:
    encrypted_password: gAAAAABh...
    description: Avatar hotline API key
  
  # Environment variable password
  secret_tunnel:
    password: ${SECRET_TUNNEL_PASSWORD}
    description: Secret tunnel access code
```

## Performance Tips

1. **Use appropriate batch sizes** - Larger batches are faster but use more memory:
   ```python
   bulk_writer.insert(cursor, records, batch_size=5000)  # Tune based on your data
   ```

2. **Choose the right cursor type** - Records and namedtuples offer a nice balance between functionality and performance. Dict cursors are the most interoperable but have the highest overhead.
   ```python
   from dbtk.cursors import ColumnCase
   cursor = db.cursor('dict', column_case=ColumnCase.PRESERVE)  # High functionality - High memory usage
   ```

3. **Materialize results when needed** - Don't fetch twice:
   ```python
   data = cursor.fetchall()  # Fetch once
   writers.to_csv(data, 'output.csv')
   writers.to_excel(data, 'output.xlsx')
   ```

4. **Use transactions for bulk operations** - Commit once for many inserts:
   ```python
   with db.transaction():
       for record in records:
           table.exec_insert(cursor)
   ```

5. **Use DataSurge for bulk operations** - Much faster than row-by-row:
   ```python
   bulk_writer = DataSurge(table)
   bulk_writer.insert(cursor, records, batch_size=2000)
   ```

## License

MIT License - see LICENSE file for details.

## Support

- **Issues**: [GitHub Issues](https://github.com/yourusername/dbtk/issues)
- **Discussions**: [GitHub Discussions](https://github.com/yourusername/dbtk/discussions)
- **Documentation**: [Full Documentation](https://dbtk.readthedocs.io)

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.