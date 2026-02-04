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

## EntityManager for Multi-Stage Imports

`EntityManager` provides resumable, multi-stage ETL for complex imports where entities have multiple IDs that need to be resolved:

```python
from dbtk.etl import EntityManager

# Track entities by primary ID, resolve secondary IDs on demand
manager = EntityManager(
    primary_id="crm_id",           # Reliable source ID (e.g., from CRM system)
    secondary_ids=["recruit_id", "sis_id"]  # IDs to resolve
)

# Set up resolver query
stmt = cursor.prepare_file("sql/resolve_person.sql")
manager.set_main_resolver(stmt)

# Process records - manager tracks state (PENDING, RESOLVED, ERROR, SKIPPED)
for row in reader:
    entity = manager.process_row(row["ApplicationID"])
    if entity.status == "RESOLVED":
        # Entity has all IDs resolved - proceed with import
        table.set_values(entity)
        table.execute('insert')

# Save state for resumption
manager.save("import_state.json")

# Later: resume from saved state
manager = EntityManager.load("import_state.json")
```

**Use cases:**
- CRM integrations where records have multiple ID systems
- Data migrations requiring ID crosswalks
- Imports that may fail mid-process and need resumption
- Tracking which records have been successfully processed
