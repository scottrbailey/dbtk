# ETL: Tools & Logging 

<div style="float: right; padding: 20px">
    <img src="assets/etl_tools.png" height="240" align="right" />
</div>

Utilities for orchestrating and monitoring production ETL imports: identity resolution,
code validation/collection, and structured logging.

---

## IdentityManager

`IdentityManager` is a lightweight, resumable cache that maps source-system primary keys
to target-system identifiers. It tracks every entity's resolution status, messages, and
errors across an entire import run, and can persist that state to JSON so multi-stage
pipelines can resume without re-querying resolved records.

The `source_key` should be the primary key of the source system (i.e. CRM_ID) and should be 
present on every record coming from the source system. 

The `target_key` should be the primary key of the target system (i.e. ERP_ID). The resolver may
match additional keys but the entity will not be marked as resolved until a target_key has been found.

The `alternate_keys` parameter can be used to define additional identifiers either used to aid 
identification (i.e. SSN, email, username) or used during processing (i.e. staging_id). When `set_id`
method is used to manually set an identifier, the `id_type` must be the value of `target_key` or
listed in `alternate_keys`.

```python
from dbtk.etl import IdentityManager, EntityStatus
```

### EntityStatus

Every entity in the cache carries a `_status` value from these constants:

| Status      | Value         | Meaning                                                                                                                 |
|-------------|---------------|-------------------------------------------------------------------------------------------------------------------------|
| `PENDING`   | `"pending"`   | Registered but resolution not yet attempted                                                                             |
| `RESOLVED`  | `"resolved"`  | Successfully matched; `target_key` is populated                                                                         |
| `STAGED`    | `"staged"`    | Exists in a staging table; not yet confirmed in target                                                                  |
| `NOT_FOUND` | `"not_found"` | Resolution attempted but no match found                                                                                 |
| `ERROR`     | `"error"`     | An error occurred while creating or updating the entity — downstream table processing for this entity should be skipped |
| `SKIPPED`   | `"skipped"`   | Resolution intentionally bypassed                                                                                       |

### Instantiation

```python
stmt = cursor.prepare_file('sql/resolve_student.sql')

# With a resolver — looks up target_key via SQL on first encounter
im = IdentityManager(
    source_key='student_id',
    target_key='erp_person_id',
    resolver=stmt,
    alternate_keys=['banner_id'],   # Optional extra keys to track per entity
)

# Without a resolver — tracking/logging mode only
# Use when the target_key is already present in incoming records (e.g. crosswalk file),
# or when you only need per-entity status/error tracking with no DB lookup.
im = IdentityManager('source_id', 'target_id')   # no resolver
```

When `source_key == target_key`, pass the same string for both — IdentityManager
recognises this and marks entities as `RESOLVED` immediately without staging them.

### resolve()

The primary method. Accepts a scalar, dict, or Record:

```python
# Scalar — treated as the raw source_key value.
# The resolver is called; the caller's record is NOT mutated.
entity = im.resolve(row['student_id'])

# dict or Record — source_key extracted from the mapping.
# On success, target_key is written back into the caller's record.
entity = im.resolve(row)

if entity is None:
    pass  # source_key not present in value

if entity['_status'] == EntityStatus.RESOLVED:
    # entity[target_key] is populated; row[target_key] has been set too
    table.set_values(row)
    if table.execute('insert'):
        entity['_status'] = EntityStatus.ERROR
        im.add_error(row['student_id'], table.last_error)
elif entity['_status'] == EntityStatus.NOT_FOUND:
    im.add_error(row['student_id'],
                 ErrorDetail('Student not found', field='student_id'))
```

**Cache behaviour:** already-RESOLVED entities are returned from cache without querying the
database again. Non-RESOLVED entities (PENDING, NOT_FOUND, STAGED) are re-attempted on
every `resolve()` call.

### alternate_keys

Track additional identifiers per entity alongside `target_key`:

```python
im = IdentityManager('crm_id', 'erp_id', resolver=stmt,
                     alternate_keys=['staging_id', 'legacy_id'])

entity = im.resolve(row)
# entity['banner_id'] is populated if the resolver returns it

# Read or write any tracked key directly
im.set_id(source_id, 'staging_id', 'B00123')
banner = im.get_id(source_id, 'staging_id')
```

### add_error / add_message

Attach structured errors and informational messages to a cached entity:

```python
from dbtk.utils import ErrorDetail

im.add_error(source_id, ErrorDetail('Insert failed', field='name', code='DB_ERROR'))
im.add_message(source_id, 'Mapped via legacy crosswalk')
```

Both lists (`_errors`, `_messages`) are preserved in `save_state()` / `load_state()`.

### batch_resolve()

Re-run the resolver for all `PENDING` and `NOT_FOUND` entities. Useful after a staging
table has been populated and you want to retry resolution in bulk.

```python
im.batch_resolve()

# Also retry STAGED entities (e.g. after a staging→target promotion step)
im.batch_resolve(additional_statuses=[EntityStatus.STAGED])

# Retry multiple additional statuses
im.batch_resolve(additional_statuses=[EntityStatus.STAGED, EntityStatus.ERROR])
```

`PENDING` and `NOT_FOUND` are always retried. `additional_statuses` extends that set —
it does not replace it.

### save_state / load_state

Persist the full entity cache to JSON and restore it later:

```python
# At end of run
im.save_state('state/students.json')

# Next run — restore cache, attach a (possibly updated) resolver
im = IdentityManager.load_state('state/students.json', resolver=stmt)

# Retry anything that failed or wasn't found last time
im.batch_resolve()

# Or add STAGED entities to the retry set
im.batch_resolve(additional_statuses=[EntityStatus.STAGED])
```

The JSON file includes `source_key`, `target_key`, `alternate_keys`, field order (for
factory reconstruction), summary stats, and every entity with its errors and messages.
`ErrorDetail` objects are serialised/deserialised automatically.

### calc_stats()

```python
stats = im.calc_stats()
# {'pending': 0, 'resolved': 142, 'staged': 5, 'error': 3, 'skipped': 0, 'not_found': 11}
```

### Complete examples

**Pattern 1 — Standard resolver-based import:**

```python
import dbtk
from dbtk.etl import IdentityManager, EntityStatus
from dbtk.utils import ErrorDetail

dbtk.setup_logging()

with dbtk.connect('erp_db') as db:
    cursor = db.cursor()
    stmt = cursor.prepare_file('sql/resolve_student.sql')

    im = IdentityManager('student_id', 'erp_person_id', resolver=stmt,
                         alternate_keys=['banner_id'])

    student_table = dbtk.etl.Table('students', columns_config, cursor)

    with dbtk.readers.get_reader('incoming/students.csv.gz') as reader:
        for row in reader:
            entity = im.resolve(row)          # Looks up erp_person_id; writes it into row
            if entity['_status'] == EntityStatus.ERROR:
                continue                       # Skip downstream tables
            if entity['_status'] != EntityStatus.RESOLVED:
                im.add_error(row['student_id'],
                             ErrorDetail('Not found', field='student_id'))
                continue

            student_table.set_values(row)
            if student_table.execute('insert', raise_error=False):
                entity['_status'] = EntityStatus.ERROR
                im.add_error(row['student_id'], student_table.last_error)

    im.save_state('state/students.json')
    print(im.calc_stats())
```

**Pattern 2 — Crosswalk pre-load (no resolver needed):**

```python
# crosswalk.csv has both crm_id and erp_id — no DB lookup required
im = IdentityManager('crm_id', 'erp_id')   # no resolver

with dbtk.readers.get_reader('crosswalk.csv') as reader:
    for row in reader:
        im.resolve(row)   # Caches entity as RESOLVED (erp_id present) or STAGED

with dbtk.readers.get_reader('data.csv') as reader:
    for row in reader:
        entity = im.resolve(row)   # Cache hit — no DB call
        if entity and entity['_status'] == EntityStatus.RESOLVED:
            table.set_values(row)
            table.execute('insert')
```

---

## ValidationCollector

A callable that collects and optionally enriches coded values during row-by-row
processing. Useful for accumulating all values seen in a field (e.g. title codes,
department codes) and validating or enriching them against a reference table.

```python
from dbtk.etl import ValidationCollector
from dbtk.etl.transforms import TableLookup

# Pure collection — no lookup
title_collector = ValidationCollector()
for record in reader:
    title_collector(record['tconst'])   # Collects the value; returns it unchanged

# With lookup — enrich codes with descriptions on first encounter
genre_lookup = TableLookup(cursor=cur, table='genres', key_cols='code',
                           return_cols='name', cache=TableLookup.CACHE_PRELOAD)
genre_collector = ValidationCollector(lookup=genre_lookup, desc_field='name')
for record in reader:
    genre_name = genre_collector(record['genre_code'])  # Returns enriched name
```

For detailed `TableLookup` documentation including caching strategies and string shorthand syntax, see [Table Lookups and Validation](07-table.md#database-lookups-and-validation).

**Filtering with `in` operator:**

```python
# Collect valid title IDs in first pass
title_collector = ValidationCollector()
for record in titles_reader:
    title_collector(record['tconst'])

# Filter a second file to only matching titles
with dbtk.readers.get_reader('title.principals.tsv.gz') as reader:
    reader.add_filter(lambda r: r.tconst in title_collector)
    for record in reader:
        process(record)
```

**Reporting:**

```python
# Valid codes (seen in reference table)
mapping = collector.get_valid_mapping()  # {code: description}

# New codes (not in reference table)
new_codes = collector.get_new_codes()  # sorted list

# All codes as a set (useful for polars filtering)
all_codes = collector.get_all()
df = pl.scan_csv('data.tsv').add_filter(pl.col('code').is_in(all_codes))
```

---

## Logging for Integration Scripts

**The problem:** Integration scripts need proper logging with timestamped files, separate
error logs, and easy cleanup. Setting this up manually is repetitive and error-prone.

**The solution:** DBTK provides `setup_logging()` and `cleanup_old_logs()` to handle the
common pattern of creating timestamped log files like `script_name_20251031_154505.log`.

```python
import dbtk
import logging

# One-line setup with automatic script name detection
dbtk.setup_logging()  # Log settings in config (dbtk.yml), see below

# Or specify name and options
dbtk.setup_logging('fire_nation_etl', log_dir='/var/log/etl', level='DEBUG')

# Now use standard Python logging
logger = logging.getLogger(__name__)
logger.info("Starting ETL process...")
logger.error("Failed to process record")
```

**Configuration options** (via `dbtk.yml` or function parameters):

```yaml
settings:
  logging:
    directory: ./logs                   # Where to write logs
    level: INFO                          # DEBUG, INFO, WARNING, ERROR, CRITICAL
    format: '%(asctime)s [%(levelname)s] %(name)s: %(message)s'
    timestamp_format: '%Y-%m-%d %H:%M:%S'  # For log message timestamps
    filename_format: '%Y%m%d_%H%M%S'    # For log filenames
    split_errors: true                   # Separate _error.log for errors
    console: true                        # Also output to console
    retention_days: 30                   # For cleanup_old_logs()
```

**Filename patterns:**

```python
# One log per run (default)
# filename_format: '%Y%m%d_%H%M%S'
# Creates: script_20251031_154505.log

# One log per day
# filename_format: '%Y%m%d'
# Creates: script_20251031.log

# Single rolling log file
# filename_format: ''
# Creates: script.log (overwrites each run)
```

**Automatic log cleanup:**

```python
# Clean logs older than retention period (default: 30 days)
deleted = dbtk.cleanup_old_logs()
print(f"Deleted {len(deleted)} old log files")

# Custom retention
dbtk.cleanup_old_logs(retention_days=7)

# Dry run to see what would be deleted
would_delete = dbtk.cleanup_old_logs(dry_run=True)
```

**Error detection for notifications:**

When running unattended integration scripts, you often want to send notification emails
if errors occurred. The `errors_logged()` function makes this trivial:

```python
import dbtk
import logging

# Setup logging with split_errors=True (default)
dbtk.setup_logging('fire_nation_etl')

logger = logging.getLogger(__name__)

# ... do your ETL work ...
try:
    process_data()
except Exception as e:
    logger.error(f"Processing failed: {e}")

# Check if any errors were logged
error_log = dbtk.errors_logged()
if error_log:
    print(f"Errors detected! See: {error_log}")
    # send_notification_email(subject="ETL errors", attachment=error_log)
else:
    print("Integration completed successfully")
```

**How it works:**
- Returns `None` if no errors were logged
- Returns error log path if `split_errors=True` (separate _error.log file)
- Returns main log path if `split_errors=False` (errors in combined log)
- Automatically tracks ERROR and CRITICAL level messages
- Works regardless of logging configuration

**What DBTK logs automatically:**

DBTK logs all operations without you writing any log statements:
- Database connections and queries
- File reading operations and errors
- Table operations (INSERT/UPDATE/MERGE counts, validation failures)
- Data transformation errors
- Parameter conversions and SQL generation

You only need to add custom logging for your specific business logic.

**Complete integration script example:**

```python
#!/usr/bin/env python3
"""Fire Nation intelligence ETL with custom validation logging."""

import dbtk
import logging

# Set up logging - creates dated log files automatically
dbtk.setup_logging()

# Optional: Create logger only if you need custom log messages
logger = logging.getLogger(__name__)

def validate_combat_readiness(soldier_data):
    """Custom business rule - log only your specific logic."""
    if soldier_data['missions_completed'] < 5 and soldier_data['rank'] == 'General':
        logger.warning(f"General {soldier_data['name']} has insufficient mission experience")
        return False
    return True

def main():
    with dbtk.connect('fire_nation_db') as db:
        cursor = db.cursor()

        soldier_table = dbtk.etl.Table('soldiers', config, cursor)

        with dbtk.readers.get_reader('conscripts.csv') as reader:
            for record in reader:
                soldier_table.set_values(record)

                # Custom validation - log only when YOU need to
                if soldier_table.is_ready('insert') and not validate_combat_readiness(record):
                    continue  # Skip this record

                soldier_table.execute('insert', raise_error=False)
                # ↑ DBTK automatically logs all insert operations, errors, validation failures

        # Summary output (or log it if you prefer)
        print(f"Processed {soldier_table.counts['insert']} soldiers")
        print(f"Skipped {soldier_table.counts['incomplete']} incomplete records")

        db.commit()

if __name__ == '__main__':
    main()
    dbtk.cleanup_old_logs()

    # Check if errors occurred (DBTK tracked them automatically)
    error_log = dbtk.errors_logged()
    if error_log:
        print(f"Errors occurred - check {error_log}")
        # send_notification_email(subject="ETL Errors", attachment=error_log)
```

**Key takeaway:** DBTK does the logging heavy lifting. You only add custom log statements
for your specific business logic, not for database operations, file reading, or ETL mechanics.

**Benefits:**
- **Automatic setup** - Sample config created at `~/.config/dbtk.yml` on first use
- **Timestamped files** - Never overwrite important logs
- **Split error logs** - Easy monitoring and alerting
- **Standard logging** - Works with all Python logging features
- **Configurable** - Control via config file or function arguments

---

## See Also

- [ETL: Table & Transforms](07-table.md) - Table configuration, transforms, TableLookup
- [ETL: DataSurge & BulkSurge](08-datasurge.md) - High-performance bulk loading
- [Database Connections](03-database-connections.md) - Connections, cursors, SQL file execution, PreparedStatement

> **Examples:**
> - [`examples/data_load_imdb_subset.py`](../examples/data_load_imdb_subset.py) — `ValidationCollector` and `IdentityManager` used together in a complete ETL pipeline
> - [`examples/data_load_names.py`](../examples/data_load_names.py) — profession normalization with `TableLookup` and array column handling
