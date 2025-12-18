# Advanced Features & Performance

## Multiple Configuration Locations

DBTK searches for configuration files in this order:
1. `./dbtk.yml` (project-specific)
2. `~/.config/dbtk.yml` (user-specific)
3. Custom path via `set_config_file()`

This lets you maintain per-project configurations while having a fallback for personal databases.

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

2. **Choose the right cursor type** - Records offer the best balance of functionality and performance:
   ```python
   cursor = db.cursor('record')  # Recommended default
   cursor = db.cursor('list')    # When you only need positional access
   ```

3. **Materialize results when needed** - Don't fetch twice:
   ```python
   data = cursor.fetchall()  # Fetch once
   dbtk.writers.to_csv(data, 'output.csv')
   dbtk.writers.to_excel(data, 'output.xlsx')
   ```

4. **Use transactions for bulk operations** - Commit once for many inserts:
   ```python
   with db.transaction():
       for record in records:
           table.set_values(record)
           table.exec_insert()
   ```

5. **Use DataSurge for bulk operations** - Much faster than row-by-row:
   ```python
   bulk_writer = DataSurge(table)
   bulk_writer.insert(records)
   ```

6. **Use prepared statements for repeated queries** - Read and parse SQL once:
   ```python
   stmt = cursor.prepare_file('query.sql')
   for params in parameter_sets:
       stmt.execute(params)
   ```

7. **Let the database do the work** - Use `db_expr` in Table definitions to leverage database functions instead of processing in Python.
