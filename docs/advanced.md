# Advanced Features & Performance

## Multiple Configuration Locations

DBTK searches for configuration files in this order:
1. Explicitly set path via `dbtk.set_config_file('path/to/config.yml')`
2. `./dbtk.yml` or `./dbtk.yaml` (project-specific)
3. `~/.config/dbtk.yml` or `~/.config/dbtk.yaml` (user-specific)

This lets you maintain per-project configurations while having a fallback for personal databases. If no config is found, a sample is created at `~/.config/dbtk_sample.yml`.

## Custom Driver Registration

If you're using a database driver not built into DBTK, you can register it:

```python
from dbtk.database import register_user_drivers

custom_drivers = {
    'my_postgres_fork': {
        'database_type': 'postgres',
        'priority': 10,
        'param_map': {'database': 'dbname'},
        'required_params': [{'host', 'database', 'user'}],
        'optional_params': {'port', 'password'},
        'connection_method': 'kwargs',
        'default_port': 5432
    }
}

register_user_drivers(custom_drivers)
```

## Performance Tips

1. **Use appropriate batch sizes** - Larger batches are faster but use more memory:
   ```python
    import dbtk
    db = dbtk.connect('fire_nation_archive')
    cur = db.cursor()
    ...
    table = dbtk.etl.Table('intel', intel_cols, cursor=cur)
    bulk_writer = dbtk.etl.DataSurge(table, batch_size=5000) # Tune based on your data
    bulk_writer.insert(reader)
   ```

2. **Materialize results when needed** - Don't fetch twice:
   ```python
   data = cursor.fetchall()  # Fetch once
   dbtk.writers.to_csv(data, 'output.csv')
   dbtk.writers.to_excel(data, 'output.xlsx')
   ```

3. **Use transactions for bulk operations** - Commit once for many inserts:
   ```python
   with db.transaction():
       for record in records:
           table.set_values(record)
           table.execute('insert')
   ```

4. **Use DataSurge for bulk operations** - Much faster than row-by-row:
   ```python
   bulk_writer = DataSurge(table)
   bulk_writer.insert(records)
   ```

5. **Use prepared statements for repeated queries** - Read and parse SQL once:
   ```python
   stmt = cursor.prepare_file('query.sql')
   for params in parameter_sets:
       stmt.execute(params)
   ```

6. **Let the database do the work** - Use `db_expr` in Table definitions to leverage database functions instead of processing in Python.

## IdentityManager

`IdentityManager` resolves source-system keys to target-system identifiers, caching results so each entity is queried at most once. It tracks status, errors, and messages per entity — useful across a range of import patterns: a single-pass load that spans multiple source files, a multi-file import where only some files carry the linking key, or a multi-stage pipeline that needs to be resumed between runs.

```python
from dbtk.etl import IdentityManager, EntityStatus
from dbtk.utils import ErrorDetail

# Map source CRM IDs to ERP person IDs, also tracking a banner_id per entity
stmt = cursor.prepare_file("sql/resolve_person.sql")
im = IdentityManager(
    source_key="crm_id",       # Reliable source-system primary key
    target_key="erp_person_id",  # ID returned by resolver query
    resolver=stmt,
    alternate_keys=["banner_id"],  # Additional IDs to track
)

# Process records - resolve() caches results and writes target_key back into row
for row in reader:
    entity = im.resolve(row)          # Looks up crm_id, writes erp_person_id into row
    if entity["_status"] == EntityStatus.RESOLVED:
        table.set_values(row)         # row now has erp_person_id populated
        if table.execute("insert"):   # returns 1 on DatabaseError
            im.add_error(row["crm_id"], table.last_error)
    elif entity["_status"] == EntityStatus.NOT_FOUND:
        im.add_message(row["crm_id"], "No match in ERP — will retry after staging")

# Optionally persist state for audit or resumption on a later run
im.save_state("state/persons.json")

# Resume: already-resolved entities are restored from cache, not re-queried
im = IdentityManager.load_state("state/persons.json", resolver=stmt)
```

### Entity statuses

| Status | Meaning |
|--------|---------|
| `pending` | Registered but not yet resolved |
| `resolved` | Matched; `target_key` is populated |
| `staged` | Exists in staging but not yet confirmed in target system |
| `not_found` | Resolver ran but returned no match |
| `error` | Resolution failed with an unexpected error |
| `skipped` | Intentionally bypassed |

```python
from dbtk.etl import EntityStatus

if entity["_status"] == EntityStatus.RESOLVED:
    ...
elif entity["_status"] == EntityStatus.NOT_FOUND:
    ...
```

### resolve()

`resolve(value)` accepts a scalar, dict, or Record:

- **scalar** — treated as the raw `source_key` value; calls the resolver and caches the result without mutating anything.
- **dict / Record** — extracts `source_key` from the mapping; on success, writes `target_key` back into the caller's record so it's immediately available for `table.set_values()`.

Already-resolved entities are returned from cache without hitting the database.

### Tracking errors and messages

```python
# Attach a structured error to a cached entity
im.add_error(row["crm_id"], table.last_error)          # ErrorDetail from Table
im.add_error(row["crm_id"], ErrorDetail("Duplicate", field="email"))

# Attach a plain informational message
im.add_message(row["crm_id"], "Staged — will retry in phase 2")
```

### Managing alternate keys

```python
# Store a separately resolved ID against a cached entity
im.set_id(row["crm_id"], "banner_id", banner_result["id"])

# Retrieve it later
banner_id = im.get_id(row["crm_id"], "banner_id")
```

### Batch re-resolution

After a bulk-load staging step, retry all unresolved entities in one call:

```python
im.batch_resolve()   # Re-runs resolver for all PENDING and NOT_FOUND entities
```

### Stats and state

```python
stats = im.calc_stats()
# {'pending': 0, 'resolved': 142, 'staged': 5, 'not_found': 8, 'error': 3, 'skipped': 1}

im.save_state("state/persons.json")   # Persist to JSON

# Restore — all entities re-hydrated, ErrorDetail objects deserialized
im = IdentityManager.load_state("state/persons.json", resolver=stmt)
```

**Use cases:**
- Imports spanning multiple source files where only some files carry the entity identifier
- Any import where the same source key may appear in many rows — resolver runs once, cache handles the rest
- CRM/ERP integrations where records have IDs from multiple systems
- Multi-stage pipelines (stage → confirm → load) that benefit from resumption between runs
- Auditing: per-entity error and message tracking regardless of pipeline complexity
