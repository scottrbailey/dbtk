# Data Bender Toolkit (dbtk)

**Control and Manipulate the Flow of Data** - A lightweight Python toolkit for data integration, transformation, and movement between systems.

Like the elemental benders of Avatar, this library gives you precise control over data - the world's most rapidly growing element. 
Extract data from various sources, transform it through powerful operations, and load it exactly where it needs to go.
This library is designed by and for data integrators. 

Design philosophy: This library is designed to get data to and from your databases with minimal hassle. It is well suited for
data integration and ELT jobs. Modern databases do an amazing job at aggregating and transforming data, and we believe in leveraging those strengths. 
However, if you are doing heavy transforms in Python, we recommend looking at other tool chains like Pandas and polars.  

## Features

- **Universal Database Connectivity** - Uniform interface across PostgreSQL, Oracle, MySQL, and SQL Server
- **Flexible File Reading** - CSV, Excel (XLS/XLSX), JSON, NDJSON, XML, and fixed-width text files  
- **Multiple Export Formats** - Export to CSV, Excel, JSON, NDJSON, XML, fixed-width text, or directly between databases  
- **Advanced ETL Framework** - Full-featured Table class for complex data transformations and upserts
- **Data Transformations** - Built-in functions for dates, phones, emails, and custom data cleaning
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

# full functionality - achieve Avatar State mastery over all data formats
pip install dbtk[all]         # cryptography, keyring, lxml, openpyxml, and phonenumbers

# DBTK is largely agnostic about which database adapter you use.  Install your chosen adapter(s).
pip install psycopg2 
```

### Basic Usage

```python
import dbtk

# Connect to your Fire Nation production database using YAML config 
with dbtk.connect('fire_nation_census') as db:
    cursor = db.cursor()
    cursor.execute("SELECT * FROM firebenders WHERE rank = 'General'")
    
    # Channel your results into different formats like a true master
    dbtk.writers.to_excel(cursor, 'fire_nation_generals.xlsx')
```

Using the ETL framework to bend raw recruit data into a structured form:
```python
import dbtk
from dbtk.etl import transforms
from dbtk.database import ParamStyle

with dbtk.connect('avatar_training_grounds') as db:
    cursor = db.cursor()

    # Define ETL table with transformations - like training a new airbender
    recruit_table = dbtk.etl.Table('air_nomads', {
        'nomad_id': {'field': 'id', 'primary_key': True},
        'name': {'field': 'full_name', 'nullable': False},
        'email': {'field': 'contact_scroll', 'fn': transforms.email_clean, 'nullable': False},
        'sky_bison': {'field': 'companion_name'},
        'training_date': {'field': 'started_training', 'fn': transforms.parse_date},
        'airbending_level': {'field': 'mastery_level', 'db_fn': 'calculate_airbending_rank(#)'},
        'last_meditation': {'db_fn': 'CURRENT_TIMESTAMP'},
        'temple_origin': {'value': 'Eastern Air Temple'}}, 
        paramstyle=ParamStyle.NAMED)    
        
    # Flow data from scroll archives into the temple records  
    with dbtk.readers.get_reader('new_air_nomad_recruits.csv') as reader:
        for recruit in reader:
            recruit_table.set_values(recruit)
            if recruit_table.reqs_met:
                recruit_table.exec_insert(cursor)
            else:
                print(f"Recruit needs more training: missing {recruit_table.reqs_missing}")
```

### Configuration

Create a `dbtk.yml` file for your database connections - your personal scroll of database wisdom. 
Configurations can either be scoped to the project `'./dbtk.yml` or for the user `~/.config/dbtk.yml`.

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
    encrypted_password: gAAAAABh...  # Sealed with the power of firebending

  ba_sing_se_records:
    type: oracle
    host: earth-kingdom-db.bss
    port: 1521
    database: CITIZEN_REGISTRY
    user: dai_li_agent
    password: ${EARTH_KINGDOM_SECRET}  # There is no war in Ba Sing Se
```

## Core Components

### Database Connections

Connect to databases like mastering the four nations:

```python
import dbtk

# Direct connections to the four nations
fire_db = dbtk.database.postgres(user='azula', password='blue_flames', database='fire_nation')
earth_db = dbtk.database.oracle(user='toph', password='metal_bending', database='ba_sing_se')

# From configuration scrolls
water_db = dbtk.connect('northern_water_tribe')

# Different cursor types - like different bending styles
# There is a tradeoff between usability and memory efficiency
records = db.cursor('record')    # Record class - row['name'], row.name, row[0], row[0:5]
tuples = db.cursor('tuple')      # namedtuple   - row.name, row[0], row[0:5]   
lists = db.cursor('list')        # simple list  - row[0], row[0:5]
dicts = db.cursor('dict')        # OrderedDict  - row['name']
```

### File Readers - Gathering Intel From All Sources

Read scrolls and documents from across the Four Nations:

```python
from dbtk import readers

# Water Tribe census scrolls (CSV format)
with readers.CSVReader(open('northern_water_tribe_census.csv'),
                       skip_records=100,
                       max_records=40) as reader:
    for waterbender in reader:
        print(f"Waterbender: {waterbender.name}, Village: {waterbender.village}")

# Fire Nation military records (Excel spreadsheets)  
with readers.get_reader('fire_nation_army.xlsx', sheet_index=1) as reader:
    for soldier in reader:
        print(f"Rank: {soldier.military_rank}, Firebending Level: {soldier.flame_intensity}")

# Earth Kingdom stone tablets (fixed-width format)
columns = [
    readers.FixedColumn('earthbender_name', 1, 25),
    readers.FixedColumn('rock_throwing_distance', 26, 35, 'float'),
    readers.FixedColumn('training_complete_date', 36, 46, 'date')
]
with readers.FixedReader(open('earth_kingdom_records.txt'), columns) as reader:
    for earthbender in reader:
        print(f"{earthbender.earthbender_name}: {earthbender.rock_throwing_distance} meters")

# Air Nomad temple records (JSON format)
with readers.JSONReader(open('eastern_air_temple.json')) as reader:
    for monk in reader:
        print(f"Air Nomad: {monk.monk_name}, Sky Bison: {monk.sky_bison_companion}")

# Ancient scrolls with complex markings (XML format)
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

### Data Writers - Sharing Knowledge Across Nations

Export intelligence reports in formats suitable for different nations:

```python
from dbtk import writers

# Send waterbender data to Ice Palace archives (CSV)
writers.to_csv(cursor, 'northern_tribe_waterbenders.csv', delimiter='\t')

# Compile Fire Nation intelligence report (Excel)
writers.to_excel(cursor, 'fire_nation_threat_assessment.xlsx', sheet='Q1 Intelligence')

# Create Earth Kingdom stone inscription format (XML) - to stdout limited to 20 entries for security
writers.to_xml(cursor, record_element='earth_kingdom_citizen')

# Air Nomad temple records (JSON) with spiritual path integration
from pathlib import Path
writers.to_json(cursor, Path.home() / "air_temples" / "meditation_records.json")

# Ba Sing Se official notices (fixed-width for posting on city walls)
column_widths = [20, 15, 10, 12]
writers.to_fixed_width(cursor, column_widths, 'ba_sing_se_daily_announcements.txt')

# Transfer captured intelligence between Fire Nation databases
fire_nation_intel.execute("SELECT * FROM water_tribe_defenses") 
earth_kingdom_db_cursor = earth_kingdom_db.cursor()
count = writers.cursor_to_cursor(fire_nation_intel, earth_kingdom_db_cursor, 'enemy_intelligence')
print(f"Transferred {count} strategic records")
```

### ETL Operations - Training Data Like Training Benders

Advanced ETL with the Table class for molding data like a master earthbender:

```python
import dbtk
from dbtk.etl import transforms
from dbtk.database import ParamStyle

# Auto-generate training regimen from existing Air Temple records
config = dbtk.etl.generate_table_config(cursor, 'air_nomad_training', add_comments=True)
print(config)  # Prints Table(...) configuration like ancient airbender scrolls

# Define ETL mapping for Fire Nation recruitment
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

# Process new recruits with the precision of a firebending master
with dbtk.readers.get_reader('fire_nation_conscripts.csv') as reader:
    # prevent current records from being set to null if their source is not included in the document
    phoenix_king_army.calc_update_excludes(reader.headers)
    for recruit in reader:
        phoenix_king_army.set_values(recruit)       
        if phoenix_king_army.reqs_met:
            existing_soldier = phoenix_king_army.get_db_record(cursor)
            # most databases support merge/upsert phoenix_king_army.exec_merge(cursor)
            if existing_soldier:
                phoenix_king_army.exec_update(cursor)  # Update existing soldier record
            else:
                phoenix_king_army.exec_insert(cursor)  # New recruit joins the ranks
        else:
            print(f"Recruit rejected from service: missing {phoenix_king_army.reqs_missing}")
```

### Handling Large Volumes of Data with DataSurge

Many database drivers support efficient batch handling of large data sets with executemany().  The DataSurge class allows batch
operations on a Table object. The merge statement is not compatible with batch handling.  Instead, a temporary table is created and batch loaded, 
and merge is executed against the temp table.

```python
import dbtk
from dbtk.etl import transforms
from dbtk.database import ParamStyle

# Define ETL mapping for Fire Nation recruitment
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

bulk_writer = dbtk.etl.DataSurge(table=phoenix_king_army)
with dbtk.readers.get_reader('fire_nation_conscripts.csv') as reader:
    bulk_writer.insert(cursor, reader, batch_size=2000)
```


### Data Transformations - Bending Raw Data Into Perfect Form

Built-in functions for purifying data like a waterbender purifies polluted rivers:

```python
from dbtk.etl import transforms as tx

# Parse dates from various Fire Nation calendar formats
tx.parse_date("Year 100 AG, Day 15")      # -> Properly formatted date
tx.parse_date("Sozin's Comet Festival")   # -> Handles named events  
tx.parse_datetime("100 AG Summer Solstice T14:30:00Z")  # -> With Fire Nation timezone

# Clean messenger hawk contact information (with earthbender precision)
tx.phone_clean("5551234567")          # -> "(555) 123-4567" 
tx.phone_format("+44 20 7946 0958", tx.PhoneFormat.NATIONAL)  # -> Ba Sing Se format
tx.phone_validate("+1-800-AVATAR")   # -> Validates Avatar hotline
tx.phone_get_type("+1-800-CABBAGES") # -> "toll_free" (My cabbages!)

# Purify messenger scroll addresses like a waterbender
tx.email_validate("guru.pathik@eastern.air.temple") # -> True
tx.email_clean("  TOPH@BEIFONG.EARTHKINGDOM ") # -> "toph@beifong.earthkingdom"

# Utility functions with the wisdom of Uncle Iroh
tx.coalesce([None, "", "Jasmine Tea", "Ginseng Tea"])  # -> "Jasmine Tea" (always choose tea)
tx.indicator("Firebender", true_val="Fire Nation Citizen")  # -> "Fire Nation Citizen"  
tx.indicator("False", true_val=None, false_val="Earth Kingdom Refugee") # -> "Earth Kingdom Refugee"
tx.get_int("123.45 gold pieces")  # -> 123 (rounded like Sokka's planning)
```

## Advanced Features

### Encrypted Configuration - Protecting State Secrets

Secure your database passwords like the Fire Nation protects their war plans:

```python
import dbtk

# Generate encryption key (guard it like the location of the last airbenders)
key = dbtk.config.generate_encryption_key()

# Encrypt all passwords in your intelligence files  
dbtk.config.encrypt_config_file_cli('fire_nation_secrets.yml')

# Access encrypted war plans
sozin_secret_files = dbtk.config.get_password('phoenix_king_battle_plans')

# Manually encrypt a single password (like Azula's blue fire technique)
dbtk.config.encrypt_password_cli('only_azula_knows_this')

# Create new encrypted files when changing Fire Lords
new_key = dbtk.config.generate_encryption_key()
dbtk.config.migrate_config_cli('old_regime.yml', 'phoenix_king_era.yml', new_encryption_key=new_key)
```

### Advanced ETL Patterns 

```python
import dbtk
from dbtk.etl import Table

# Custom transformations worthy of the White Lotus
def standardize_nation(val):
    nation_map = {
        'Fire Nation Colonies': 'Earth Kingdom', 
        'Foggy Swamp Tribe': 'Earth Kingdom',
        'Kyoshi Warriors': 'Earth Kingdom'
    }
    return nation_map.get(val, val)

# Create comprehensive Four Nations census
four_nations_census = Table('population_registry',
     columns={
    'id': {'field': 'village_id'},
    'nation': {'field': 'home_nation', 'fn': standardize_nation},
    'settlement_name': {'field': 'village_or_city'},
    'population_count': {'field': 'citizen_count'},
    'primary_element': {'field': 'dominant_bending_type'},
    'recorded_by': {'value': 'Avatar Census Bureau', 'no_update': True}  # Never change the source
    },
    req_fields=('nation', 'settlement_name'),
    key_fields=('id'),
    paramstyle='named')

# Handle missing data like a true master - flexibility over rigidity
reader = dbtk.readers.get_reader('post_war_census.csv')

# Gracefully handle incomplete village records (some were lost in the war)
four_nations_census.calc_update_excludes(reader.headers)
villages_processed = 0
for village_record in reader:
    four_nations_census.set_values(village_record)
    if four_nations_census.reqs_met:
        existing = four_nations_census.current_row(cursor)
        if existing:
            four_nations_census.exec_update(cursor)
        else:
            four_nations_census.exec_insert(cursor)
        villages_processed += 1

print(f"Successfully processed {villages_processed} settlements across the Four Nations")
```

## Database Support

The following database adapters are supported out of the box.  They are listed in order of priority.  If you have multiple adapters for a database installed, the one with the lowest priority number will be chosen if you do not specify a driver in your configuration.

| Database    | Driver           | Install Command                      | Notes                                                     |
|-------------|------------------|--------------------------------------|-----------------------------------------------------------|
| PostgreSQL  | psycopg2         | `pip install psycopg2-binary`        | Recommended                                               |
| PostgreSQL  | psycopg (3)      | `pip install psycopg-binary`         | Newest version of psycopg, the 3 is silent                |
| PostgreSQL  | pgdb             | `pip install pgdb`                   | simple DB-API compliant                                   |
| PostgreSQL* | pyodbc_postgres  | `pip install pyodbc`                 | ODBC with helper settings for Postgres                    |
| ODBC*       | pyodbc           | `pip install pyodbc`                 | ODBC drivers for database must be installed on the system |
| Oracle      | oracledb         | `pip install oracledb`               | Oracle client not required                                |
| Oracle      | cx_Oracle        | `pip install cx_Oracle`              | Requires Oracle client (support ended at Python 3.10)     |
| Oracle*     | pyodbc_oracle    | `pip install pyodbc`                 | ODBC with helper settings for Oracle                      |
| MySQL       | mysqlclient      | `pip install mysqlclient`            | Fastest option                                            |
| MySQL       | mysql.connector  | `pip install mysql-connector-python` | Official MySQL connector, feature-rich                    |
| MySQL       | pymysql          | `pip install pymysql`                | Pure Python, lightweight                                  |
| MySQL       | MySQLdb          | `pip install MySQL-python`           | Legacy, C-based                                           |
| MySQL*      | pyodbc_mysql     | `pip install pyodbc`                 | ODBC with helper settings for MySQL                       |
| SQL Server* | pyodbc_sqlserver | `pip install pyodbc`                 | ODBC with helper settings for SQL Server                  |
| SQL Server  | pymysql          | `pip install pymssql`                | lightweight, DB-API compliant                             |
| SQLite      | sqlite3          | Built-in                             | No installation needed                                    |
*ODBC adapters require the actual ODBC drivers for Oracle, PostgreSQL, etc. to be installed on the system 

Other databases and adapters can be configured in your dbtk.yml file.

## License

MIT License - see LICENSE file for details.

## Support

- **Issues**: [GitHub Issues](https://github.com/yourusername/dbtk/issues)
- **Discussions**: [GitHub Discussions](https://github.com/yourusername/dbtk/discussions)
- **Documentation**: [Full Documentation](https://dbtk.readthedocs.io)