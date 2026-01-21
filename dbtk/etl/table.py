# dbtk/etl/table.py

"""
Schema-aware table operations and SQL generation.

Provides the Table class which manages table metadata and generates
parameterized SQL statements for common operations.
"""

import logging
from textwrap import dedent
from typing import Union, Tuple, Optional, Set, Dict, Any

from ..cursors import Cursor
from ..database import ParamStyle
from ..utils import wrap_at_comma, process_sql_parameters, validate_identifier, quote_identifier, sanitize_identifier, RecordLike
from .transforms.core import fn_resolver
from .transforms.database import _DeferredTransform

logger = logging.getLogger(__name__)

DB_CONSTANTS = frozenset(['sysdate', 'systimestamp', 'user', 'current_timestamp', 'current_date', 'current_time'])

class Table:
    """
    Stateful table class for ETL operations with schema-aware SQL generation.

    The Table class provides a high-level interface for database operations by maintaining
    table metadata and current record state. It automatically generates parameterized SQL
    statements and handles field mapping, data transformations, and requirement validation.

    Key features:
    - Field mapping: Map source record fields to database columns
    - Transformations: Apply functions to clean/transform data before database operations
    - Database functions: Use database-side functions (e.g., CURRENT_TIMESTAMP)
    - Default values: Provide constant values for columns
    - Requirement validation: Track required/nullable columns and validate before operations
    - Automatic SQL generation: Generate INSERT, UPDATE, SELECT, DELETE, MERGE statements
    - Operation tracking: Count successful operations and incomplete records via self.counts

        Column Configuration
    --------------------
        Each column in the columns dict is configured with a dict containing:

        **Shorthand:** An empty dict ``{}`` defaults the field name to the column name.
        For example: ``'email': {}`` is equivalent to ``'email': {'field': 'email'}``.

        * **field** (str or list of str or '*'):
          Source field name(s) from input records. If list, extracts multiple fields
          as a list value. If '*', passes the entire record to the transformation
          function instead of a single field value. If omitted, column is populated
          via 'default' or 'db_expr'.

        * **default** (any, optional):
          Default/constant value to use for this column. Applied when source field
          is missing, empty, or None.

        * **fn** (callable | list[callable] | str, optional):
          Transformation function(s) to apply to the source value.
          - callable → applied directly
          - list → functions applied in order (pipeline)
          - str → magic shorthand:
              • Pure Python: 'int', 'int:0', 'maxlen:255', 'nth:0', 'indicator:inv', 'split:\t'
              • Database:   'lookup:states:name:abbrev', 'validate:countries:code'
          See ``dbtk.etl.transforms.core.fn_resolver()`` for full shorthand reference.

        * **db_expr** (str, optional):
          Database-side function call (e.g., 'CURRENT_TIMESTAMP', 'UPPER(#)').
          Use '#' as placeholder for the bind parameter. If specified without '#',
          no bind parameter is created (useful for CURRENT_TIMESTAMP, etc.).

        * **primary_key** (bool, optional, default False):
          Marks column as primary key. Automatically sets key=True and required=True.

        * **key** (bool, optional, default False):
          Marks column as key for WHERE clauses in SELECT, UPDATE, DELETE operations.

        * **auto_key** (bool, optional, default False):
          Convenience flag: sets both ``primary_key=True`` and ``auto_gen=True``.
          Ideal for typical auto-increment primary keys.

        * **nullable** (bool, optional, default True):
          If False, marks column as required (must have non-None value for INSERT/MERGE).

        * **required** (bool, optional, default False):
          Explicitly marks column as required.

        * **auto_gen** (bool, optional, default False):
          If True, the column is omitted from INSERT statements.
          The database is expected to provide the value (e.g. AUTO_INCREMENT,
          DEFAULT CURRENT_TIMESTAMP, GENERATED ALWAYS, etc.).
          The column remains fully included in all other operations.

        * **no_update** (bool, optional, default False):
          If True, excludes column from UPDATE and MERGE operations.

        * **bind_name** (str, auto-generated):
          Sanitized parameter name for SQL bind variables. Automatically created from
          column name (replaces special chars with underscores).

    Example
    -------
    ::

        import dbtk
        from dbtk.etl import Table

        with dbtk.connect('fire_nation_db') as db:
            cursor = db.cursor()

            soldiers = Table('fire_nation_army', {
                # Primary key from source 'recruit_id' field
                'soldier_id': {
                    'field': 'recruit_id',
                    'primary_key': True
                },

                # Required field with transformation
                'enlistment_date': {
                    'field': 'join_date',
                    'fn': 'date',
                    'nullable': False
                },

                # Optional field with chained transformations
                'firebending_level': {
                    'field': 'flame_skill',
                    'fn': [str.strip, 'int']  # Clean then convert
                },

                # Constant value for all records
                'status': {
                    'default': 'active'
                },

                # Database-side function with parameter
                'combat_name': {
                    'field': 'full_name',
                    'db_expr': 'generate_callsign(#)'
                },

                # Database-side function, no parameter
                'created_at': {
                    'db_expr': 'CURRENT_TIMESTAMP'
                },

                # Multiple source fields as list
                'contact_methods': {
                    'field': ['email', 'phone', 'pigeon']
                },

                # Empty dict shorthand - field name matches column name
                'rank': {},  # Equivalent to {'field': 'rank'}
                'division': {},

                # Whole record access for multi-field decisions
                'vip_status': {
                    'field': '*',
                    'fn': lambda record: 'VIP' if record.get('years_service', 0) > 10 else 'Regular'
                }
            }, cursor=cursor)

            # Set values from source record
            soldiers.set_values({
                'recruit_id': 'FN001',
                'join_date': '2024-03-15',
                'flame_skill': '  7  ',
                'full_name': 'Zuko'
            })

            # Execute operations
            soldiers.execute('insert')  # Automatically validates requirements
            print(soldiers.counts)  # {'insert': 1, 'update': 0, ...}

    Attributes
    ----------
        values (dict): Current record values (dict of column_name: value)
        counts (dict): Operation counters (insert, update, delete, select, merge, records, incomplete)

    """

    OPERATIONS = ('insert', 'select', 'update', 'delete', 'merge')

    def __init__(
            self,
            name: str,
            columns: Dict[str, Dict[str, Any]],
            cursor: Cursor,
            null_values: Tuple[str, ...] = ('', 'NULL', '<null>', r'\N'),
    ):
        """
        Initialize Table with schema configuration and database cursor.

        Creates a Table instance that manages the mapping between source data fields
        and database columns, along with all metadata needed for SQL generation and
        data validation.

        Args:
            name: Database table name. Must be a valid SQL identifier.

            columns: Dictionary mapping database column names to their configuration.
                Each column is configured with a dict containing options like 'field',
                'fn', 'default', 'db_expr', 'primary_key', 'nullable', etc.
                See class docstring for complete column configuration options.

            cursor: Database cursor instance. Provides connection to database and
                determines SQL parameter style (qmark, named, format, pyformat, numeric).

            null_values: Tuple of string values that should be treated as NULL.
                When set_values() encounters these strings, they are converted to None.
                Default: ('', 'NULL', '<null>', '\\N')

        Raises:
            ValueError: If table name or column names are invalid SQL identifiers.

        Example
        -------
        ::

            cursor = db.cursor()

            table = Table('users', {
                'user_id': {'field': 'id', 'primary_key': True},
                'email': {'field': 'email_address', 'nullable': False},
                'created': {'db_expr': 'CURRENT_TIMESTAMP'}
            }, cursor=cursor)
        """
        validate_identifier(name)
        self._name = name
        self._cursor = cursor
        self._paramstyle = cursor.connection.driver.paramstyle

        validated_columns = {}
        req_cols = []
        key_cols = []
        gen_cols = []

        for col, col_def in columns.items():
            # Empty dict shorthand: default field to column name
            if col_def == {}:
                col_def['field'] = col

            validate_identifier(col)

            if col_def.get('auto_key'):
                col_def['primary_key'] = True
                col_def['auto_gen'] = True

            if col_def.get('primary_key'):
                col_def['key'] = True

            bind_name = sanitize_identifier(col)
            col_def['bind_name'] = bind_name

            if col_def.get('key'):
                key_cols.append(bind_name)

            if col_def.get('key') or bool(col_def.get('nullable', True)) is False or col_def.get('required'):
                req_cols.append(bind_name)

            if col_def.get('auto_gen'):
                gen_cols.append(bind_name)

            validated_columns[col] = col_def

        self._columns = validated_columns
        self.null_values = tuple(null_values)

        self._req_cols = tuple(req_cols)
        self._key_cols = tuple(key_cols)
        self._gen_cols = tuple(gen_cols)

        self._bind_name_map = {col_def['bind_name']: col for col, col_def in columns.items()}

        self._sql_statements: Dict[str, Optional[str]] = {op: None for op in self.OPERATIONS}
        self._param_config: Dict[str, Tuple[str, ...]] = {op: () for op in self.OPERATIONS}
        self.counts: Dict[str, int] = {op: 0 for op in self.OPERATIONS}
        self.counts['records'] = 0
        self.counts['incomplete'] = 0

        self._record_fields = set()
        self._update_excludes: Set[str] = set()
        self._update_excludes_calculated = False

        self.values: Dict[str, Any] = {}
        self._ops_ready: int = 0

        self.generate_sql('insert')

        for col_name, col_def in self._columns.items():
            fn = col_def.get('fn')
            if fn is None:
                continue

            if isinstance(fn, str):
                try:
                    fn_def = fn.strip()
                    if fn_def.startswith(('lookup:', 'validate:')):
                        col_def['fn'] = _DeferredTransform.from_string(fn_def)
                    else:
                        col_def['fn'] = fn_resolver(fn_def)
                except ValueError as e:
                    logger.debug(f"Column {col_name}: {e}")
                    continue

            new_fn = col_def['fn']
            if isinstance(new_fn, _DeferredTransform):
                col_def['fn'] = new_fn.bind(self._cursor)
            elif isinstance(new_fn, (list, tuple)):
                pipeline = []
                for f in new_fn:
                    if isinstance(f, _DeferredTransform):
                        pipeline.append(f.bind(self._cursor))
                    elif isinstance(f, str):
                        pipeline.append(fn_resolver(f))
                    else:
                        pipeline.append(f)
                col_def['fn'] = pipeline

    @property
    def name(self) -> str:
        return self._name

    @property
    def columns(self) -> dict:
        return self._columns

    @property
    def paramstyle(self) -> str:
        return self._paramstyle

    @property
    def param_config(self) -> Dict[str, Tuple[str, ...]]:
        return self._param_config

    @property
    def cursor(self) -> Cursor:
        return self._cursor

    @cursor.setter
    def cursor(self, value: Cursor):
        old_paramstyle = self._paramstyle
        self._cursor = value
        self._paramstyle = value.connection.driver.paramstyle

        if old_paramstyle != self._paramstyle:
            self._reset()
            logger.info(
                f"Table {self._name}: paramstyle changed from {old_paramstyle} "
                f"to {self._paramstyle}, cache reset"
            )
        else:
            self._reset_counts()
            logger.info(f"Table {self._name}: cursor changed, counts reset")

    @property
    def req_cols(self) -> Tuple[str]:
        return self._req_cols

    @property
    def key_cols(self) -> Tuple[str]:
        return self._key_cols

    @property
    def row_count(self) -> int:
        return self.counts['records']

    def reqs_met(self, operation: str) -> bool:
        if operation == 'insert':
            required = [col for col in self._req_cols if col not in self._gen_cols]
        elif operation in ('update', 'merge'):
            required = list(set(self._req_cols) | set(self._key_cols))
        elif operation in ('select', 'delete'):
            required = list(self._key_cols)
        else:
            raise ValueError(f"Invalid operation '{operation}'")

        return all(self.values.get(col) is not None for col in required)

    def reqs_missing(self, operation: str) -> Set[str]:
        if operation == 'insert':
            required = [col for col in self._req_cols if col not in self._gen_cols]
        elif operation in ('update', 'merge'):
            required = list(set(self._req_cols) | set(self._key_cols))
        elif operation in ('select', 'delete'):
            required = list(self._key_cols)
        else:
            raise ValueError(f"Invalid operation '{operation}'")

        return {col for col in required if self.values.get(col) is None}

    def is_ready(self, operation: str) -> bool:
        """Fast O(1) check if the current record is ready for the given operation."""
        if operation not in self.OPERATIONS:
            raise ValueError(f"Invalid operation '{operation}'")
        bit = 1 << self.OPERATIONS.index(operation)
        return bool(self._ops_ready & bit)

    def refresh_readiness(self) -> None:
        """Re-evaluate which operations can be executed based on current values."""
        if self.reqs_met('update'):
            self._ops_ready = 0b11111  # all 5 operations ready
            return

        ready = 0
        if self.reqs_met('insert'):
            ready |= 1 << self.OPERATIONS.index('insert')
        if self.reqs_met('select'):
            ready |= (
                (1 << self.OPERATIONS.index('select')) |
                (1 << self.OPERATIONS.index('delete'))
            )
        if self.reqs_met('merge'):
            ready |= 1 << self.OPERATIONS.index('merge')

        self._ops_ready = ready

    def _wrap_db_expr(self, col_name: str, db_expr: str = None) -> str:
        """Wrap column placeholder with database function if provided."""
        if db_expr in (None, ''):
            return f':{col_name}'
        db_expr = db_expr.strip()
        if not db_expr:
            return f':{col_name}'
        if '#' in db_expr:
            return db_expr.replace('#', f':{col_name}')
        if db_expr.lower() in DB_CONSTANTS:
            return db_expr
        if '(' in db_expr and ')' in db_expr:
            return db_expr
        return f'{db_expr}(:{col_name})'

    def _finalize_sql(self, operation: str, sql: str) -> None:
        self._sql_statements[operation], self._param_config[operation] = process_sql_parameters(sql, self._paramstyle)

    def _create_select(self) -> str:
        """Generate SELECT statement with named parameters."""
        if not self._key_cols:
            raise ValueError(f"Cannot create SELECT for table {self._name}: no key columns defined")
        table_name = quote_identifier(self._name)
        quoted_cols = []
        conditions = []

        for col, col_def in self._columns.items():
            ident = quote_identifier(col)
            quoted_cols.append(ident)
            bind_name = col_def['bind_name']
            if bind_name in self._key_cols:
                db_expr = col_def.get('db_expr')
                placeholder = self._wrap_db_expr(bind_name, db_expr)
                conditions.append(f"{ident} = {placeholder}")

        cols_str = ', '.join(quoted_cols)
        if len(quoted_cols) > 4:
            cols_str = wrap_at_comma(cols_str)
        sql = f"SELECT {cols_str} \nFROM {table_name}"
        if conditions:
            conditions_str = '\n  AND '.join(conditions)
            sql += f"\nWHERE {conditions_str}"

        logger.debug(f"Generated select SQL for {self._name}:\n{sql}")
        return sql

    def _create_insert(self) -> str:
        table_name = quote_identifier(self._name)

        insert_cols = []
        placeholders = []

        for col, col_def in self._columns.items():
            bind_name = col_def['bind_name']
            if bind_name in self._gen_cols:
                continue
            insert_cols.append(col)
            db_expr = col_def.get('db_expr')
            placeholders.append(self._wrap_db_expr(bind_name, db_expr))

        if not insert_cols:
            raise ValueError(f"Table {self._name} has no columns to insert (all auto_gen?)")

        cols_str = ', '.join(quote_identifier(col) for col in insert_cols)
        placeholders_str = ', '.join(placeholders)

        if len(insert_cols) > 4:
            cols_str = wrap_at_comma(cols_str)
            placeholders_str = wrap_at_comma(placeholders_str)

        sql = f"INSERT INTO {table_name} ({cols_str})\nVALUES\n({placeholders_str})"
        logger.debug(f"Generated insert SQL for {self._name}:\n{sql}")
        return sql

    def _create_update(self) -> str:
        """Generate UPDATE statement with named parameters."""
        if not self._key_cols:
            raise ValueError(f"Cannot create UPDATE for table {self._name}: no key columns defined")

        table_name = quote_identifier(self._name)
        update_cols = []
        conditions = []

        for col, col_def in self._columns.items():
            ident = quote_identifier(col)
            bind_name = col_def['bind_name']
            db_expr = col_def.get('db_expr')
            placeholder = self._wrap_db_expr(bind_name, db_expr)
            if bind_name in self._key_cols:
                conditions.append(f'{ident} = {placeholder}')
            elif bind_name not in self._update_excludes:
                update_cols.append(f'{ident} = {placeholder}')
        set_clause_str = ', '.join(update_cols)
        if len(update_cols) > 4:
            set_clause_str = wrap_at_comma(set_clause_str)
        conditions_str = '\n    AND '.join(conditions)

        sql = f"UPDATE {table_name} SET {set_clause_str} \nWHERE {conditions_str}"
        logger.debug(f"Generated update SQL for {self._name}:\n{sql}")
        return sql

    def _create_delete(self) -> str:
        """Generate DELETE statement with named parameters."""
        if not self._key_cols:
            raise ValueError(f"Cannot create DELETE for table {self._name}: no key columns defined")

        table_name = quote_identifier(self._name)
        conditions = []

        for col, col_def in self._columns.items():
            bind_name = col_def['bind_name']
            if bind_name not in self._key_cols:
                continue
            quoted_col = quote_identifier(col)
            db_expr = col_def.get('db_expr')
            placeholder = self._wrap_db_expr(bind_name, db_expr)
            conditions.append(f"{quoted_col} = {placeholder}")
        conditions_str = '\n    AND '.join(conditions)
        sql = f"DELETE FROM {table_name} \nWHERE {conditions_str}"
        logger.debug(f"Generated delete SQL for {self._name}:\n{sql}")
        return sql

    def _should_use_upsert(self) -> bool:
        """Determine whether to use upsert syntax vs MERGE statement."""
        db_type = self._cursor.connection.database_type

        if db_type in ('mysql', 'postgres', 'sqlite'):
            return True
        else:
            return False

    def _create_upsert(self) -> str:
        """Create INSERT ... ON DUPLICATE KEY/CONFLICT statement with named parameters."""
        db_type = self._cursor.connection.database_type
        table_name = quote_identifier(self._name)

        # Build INSERT portion
        cols = []
        placeholders = []
        key_cols = []
        update_cols = []

        for col, col_def in self._columns.items():
            ident = quote_identifier(col)
            bind_name = col_def['bind_name']
            db_expr = col_def.get('db_expr')
            placeholder = self._wrap_db_expr(bind_name, db_expr)

            cols.append(ident)
            placeholders.append(placeholder)

            if bind_name in self._key_cols:
                key_cols.append(ident)
            elif bind_name not in self._update_excludes:
                update_cols.append((col, ident, bind_name, db_expr))

        cols_str = ', '.join(cols)
        placeholders_str = ', '.join(placeholders)

        if len(cols) > 4:
            cols_str = wrap_at_comma(cols_str)
            placeholders_str = wrap_at_comma(placeholders_str)

        if db_type == 'mysql':
            # MySQL: INSERT ... ON DUPLICATE KEY UPDATE
            update_assignments = []
            for col, ident, bind_name, db_expr in update_cols:
                if db_expr and '#' in db_expr:
                    # Use alias syntax for MySQL 8.0.19+
                    assignment = f"{ident} = {db_expr.replace('#', f'new_vals.{ident}')}"
                elif db_expr:
                    assignment = f"{ident} = {db_expr}"
                else:
                    assignment = f"{ident} = new_vals.{ident}"
                update_assignments.append(assignment)

            update_clause = ', '.join(update_assignments)
            if len(update_assignments) > 4:
                update_clause = wrap_at_comma(update_clause)

            sql = dedent(f"""\
            INSERT INTO {table_name} ({cols_str})
            VALUES ({placeholders_str}) AS new_vals
            ON DUPLICATE KEY UPDATE {update_clause}""")

        elif db_type in ('postgres', 'sqlite'):
            # PostgreSQL/SQLite: INSERT ... ON CONFLICT DO UPDATE
            conflict_cols = ', '.join(key_cols)

            update_assignments = []
            for col, ident, bind_name, db_expr in update_cols:
                if db_expr and '#' in db_expr:
                    assignment = f"{ident} = {db_expr.replace('#', f'EXCLUDED.{ident}')}"
                elif db_expr:
                    assignment = f"{ident} = {db_expr}"
                else:
                    assignment = f"{ident} = EXCLUDED.{ident}"
                update_assignments.append(assignment)

            update_clause = ', '.join(update_assignments)
            if len(update_assignments) > 4:
                update_clause = wrap_at_comma(update_clause)

            sql = dedent(f"""\
            INSERT INTO {table_name} ({cols_str})
            VALUES ({placeholders_str})
            ON CONFLICT ({conflict_cols}) DO UPDATE SET {update_clause}""")

        else:
            raise NotImplementedError(f"Upsert not supported for database: {db_type}")

        logger.debug(f"Generated upsert SQL for {self._name}:\n{sql}")
        return sql

    def _create_merge_statement(self) -> str:
        """Create traditional MERGE statement with named parameters."""
        db_type = self._cursor.connection.database_type
        table_name = quote_identifier(self._name)

        # Build column lists and placeholders
        all_cols = []
        placeholders = []
        key_conditions = []
        update_cols = []

        for col, col_def in self._columns.items():
            ident = quote_identifier(col)
            bind_name = col_def['bind_name']
            db_expr = col_def.get('db_expr')
            placeholder = self._wrap_db_expr(bind_name, db_expr)

            all_cols.append((col, ident, placeholder))
            placeholders.append(placeholder)

            if bind_name in self._key_cols:
                key_conditions.append(f"t.{ident} = s.{ident}")
            elif bind_name not in self._update_excludes:
                update_cols.append((col, ident))

        # Database-specific MERGE templates
        if db_type == 'oracle':
            # Oracle MERGE with dual table
            source_items = []
            for col, ident, placeholder in all_cols:
                source_items.append(f"{placeholder} AS {ident}")

            source_cols = ', '.join(source_items)
            if len(all_cols) > 4:
                source_cols = wrap_at_comma(source_cols)

            source_clause = f"SELECT {source_cols} FROM dual"
            table_alias = "s"

            update_assignments = []
            for col, ident in update_cols:
                update_assignments.append(f"t.{ident} = s.{ident}")

            insert_cols = ', '.join(ident for _, ident, _ in all_cols)
            insert_values = ', '.join(f"s.{ident}" for _, ident, _ in all_cols)

        elif db_type == 'sqlserver':
            # SQL Server MERGE
            source_items = []
            for col, ident, placeholder in all_cols:
                source_items.append(f"{placeholder} AS {ident}")

            source_cols = ', '.join(source_items)
            if len(all_cols) > 4:
                source_cols = wrap_at_comma(source_cols)

            source_clause = f"SELECT {source_cols}"
            table_alias = "s"

            update_assignments = []
            for col, ident in update_cols:
                update_assignments.append(f"t.{ident} = s.{ident}")

            insert_cols = ', '.join(ident for _, ident, _ in all_cols)
            insert_values = ', '.join(f"s.{ident}" for _, ident, _ in all_cols)

        else:
            raise NotImplementedError(f"MERGE not supported for database: {db_type}")

        # Build final clauses
        update_set = ', '.join(update_assignments)
        if len(update_assignments) > 4:
            update_set = wrap_at_comma(update_set)

        if len(all_cols) > 4:
            insert_cols = wrap_at_comma(insert_cols)
            insert_values = wrap_at_comma(insert_values)

        # Assemble final SQL
        sql = dedent(f"""\
        MERGE INTO {table_name} t
        USING ({source_clause}) {table_alias}
        ON ({' AND '.join(key_conditions)})
        WHEN MATCHED THEN
            UPDATE SET {update_set}
        WHEN NOT MATCHED THEN
            INSERT ({insert_cols})
            VALUES ({insert_values})""")

        logger.debug(f"Generated merge SQL for {self._name}:\n{sql}")
        return sql

    def _create_merge(self) -> str:
        """Generate MERGE or upsert statement for the cursor's database type."""
        if not self._key_cols:
            raise ValueError(f"Cannot create MERGE for table {self._name}: no key columns defined")

        use_upsert = self._should_use_upsert()

        if use_upsert:
            return self._create_upsert()
        else:
            return self._create_merge_statement()

    def generate_sql(self, operation: str) -> None:
        if operation not in self.OPERATIONS:
            raise ValueError(f"Invalid operation '{operation}'. Must be one of {self.OPERATIONS}")

        if operation in ('select', 'update', 'delete', 'merge') and not self._key_cols:
            raise ValueError(f"Cannot generate {operation} SQL for table {self._name}: no key columns defined")

        if operation == 'insert':
            sql = self._create_insert()
        elif operation == 'select':
            sql = self._create_select()
        elif operation == 'update':
            sql = self._create_update()
        elif operation == 'delete':
            sql = self._create_delete()
        elif operation == 'merge':
            sql = self._create_merge()

        self._finalize_sql(operation, sql)

    def get_sql(self, operation: str) -> str:
        if operation not in self.OPERATIONS:
            raise ValueError(f"Invalid operation '{operation}'. Must be one of {self.OPERATIONS}")

        if self._sql_statements[operation] is None:
            self.generate_sql(operation)
        return self._sql_statements[operation]

    def get_bind_params(self, operation: str, mode: str = None) -> Union[dict, tuple]:
        # unchanged original implementation
        if operation not in self.OPERATIONS:
            raise ValueError(f"Invalid operation '{operation}'. Must be one of {self.OPERATIONS}")
        if operation in ('update', 'delete', 'merge') and not self._key_cols:
            raise ValueError(f"Cannot get {operation} params: no key columns defined")
        param_names = self._param_config[operation]
        if not param_names:
            _ = self.get_sql(operation)
            param_names = self._param_config[operation]
        if mode is None or mode not in ('positional', 'named'):
            mode = 'positional' if self._paramstyle in ParamStyle.positional_styles() else 'named'

        if not param_names:
            return () if self._paramstyle in ParamStyle.positional_styles() else {}

        filtered_values = {}

        for bind_name in param_names:
            if bind_name in self.values:
                filtered_values[bind_name] = self.values[bind_name]

        if mode == 'positional':
            return tuple(filtered_values.get(param, None) for param in param_names)
        else:
            return filtered_values

    def set_values(self, record: RecordLike):
        self.counts['records'] += 1

        warn_missing = self.counts['records'] == 1
        if not self._record_fields:
            # Cache fields so we can calculate missing fields to exclude from updates/merges
            # Include both original and normalized field names for dual access support
            try:
                # Union of original and normalized field names
                self._record_fields = set(record.keys()) | set(record.keys(normalized=True))
            except TypeError:
                # Not a Record object or doesn't support normalized parameter
                self._record_fields = set(record.keys())

        values = {}
        for col, col_def in self.columns.items():
            val = None
            field = col_def.get('field')

            if field == '*':
                # Pass whole record to function - no field extraction
                val = record
            elif isinstance(field, list):
                val = list()
                for f in field:
                    if f in record:
                        val.append(record.get(f))
                    elif warn_missing:
                        logger.warning(f'Table {self.name}: field "{f}" not found in record')
            elif field:
                if warn_missing and field not in record:
                    logger.warning(f'Table {self.name}: field "{field}" not in record')
                val = record.get(field)

            # Only apply null_values conversion if val is not the whole record
            if field != '*' and isinstance(val, str) and val in self.null_values:
                val = None

            if val in ('', None) and 'default' in col_def:
                val = col_def['default']

            if 'fn' in col_def:
                fn = col_def['fn']
                if isinstance(fn, (list, tuple)):
                    for func in fn:
                        val = func(val)
                else:
                    val = fn(val)
            # Store values using bind_name as key (not column name)
            bind_name = col_def['bind_name']
            values[bind_name] = val
        self.values = values

        # Automatically update readiness after normal record processing
        self.refresh_readiness()

    def _reset_counts(self):
        self.counts = {op: 0 for op in self.OPERATIONS}
        self.counts['records'] = 0
        self.counts['incomplete'] = 0

    def _reset(self):
        self._sql_statements = {op: None for op in self.OPERATIONS}
        self._param_config = {op: () for op in self.OPERATIONS}
        self._update_excludes = set()
        self._update_excludes_calculated = False
        self._record_fields = set()
        self._reset_counts()
        self.values = {}
        self._ops_ready = 0

    def fetch(self) -> Dict[str, Any]:
        err = self.execute('select')
        if not err:
            return self._cursor.fetchone()

    def get_column_definitions(self) -> list:
        """
        Introspect database table columns to get type information.

        Executes a SELECT * query against the database table and returns type
        information for columns defined in this Table object. Validates that
        all Table columns exist in the database.

        Returns:
            List of tuples: (column_name, type_obj, internal_size, precision, scale)
            where type_obj is the database driver's type object.

        Raises:
            ValueError: If a column defined in this Table doesn't exist in the database

        Example:
            >>> table = Table('users', {'id': {}, 'email': {}}, cursor=cursor)
            >>> col_defs = table.get_column_definitions()
            >>> for name, type_obj, size, prec, scale in col_defs:
            ...     print(f"{name}: {type_obj}")
        """
        # Query all columns from database table
        self._cursor.execute(f"SELECT * FROM {self._name} WHERE 1=0")

        # Build case-insensitive map of column name to type info
        db_columns = {
            desc[0].upper(): (desc[0], desc[1], desc[3], desc[4], desc[5])
            for desc in self._cursor.description
        }

        # Validate and collect type info for Table-defined columns
        result = []
        for col_name in self._columns.keys():
            col_name_upper = col_name.upper()
            if col_name_upper not in db_columns:
                raise ValueError(
                    f"Column '{col_name}' defined in Table but not found in database table '{self._name}'"
                )
            db_col_name, type_obj, internal_size, precision, scale = db_columns[col_name_upper]
            # Return with database's actual column name for accurate SQL generation
            result.append((db_col_name, type_obj, internal_size, precision, scale))

        return result

    def bind_name_column(self, bind_name):
        return self._bind_name_map.get(bind_name)

    def calc_update_excludes(self, record_fields: Optional[Set[str]] = None):
        # unchanged original implementation
        if record_fields is None:
            record_fields = self._record_fields

        if not record_fields:
            logger.debug(f"No record_fields available for {self.name}, skipping exclude calculation")
            return

        current_excludes = self._update_excludes
        excludes = []
        for col, col_def in self._columns.items():
            bind_name = col_def['bind_name']
            field = col_def.get('field')

            if col_def.get('no_update'):
                excludes.append(bind_name)
                continue

            if field:
                if isinstance(field, list):
                    missing_fields = [f for f in field if f not in record_fields]
                    if missing_fields:
                        if bind_name in self.key_cols:
                            raise ValueError(
                                f"A key column {col} is sourced from {field}, "
                                f"but {missing_fields} are missing from source."
                            )
                        else:
                            excludes.append(bind_name)
                else:
                    if field not in record_fields:
                        if bind_name in self.key_cols:
                            raise ValueError(
                                f"A key column {col} is sourced from {field}, "
                                f"but is missing from source."
                            )
                        else:
                            excludes.append(bind_name)

        if excludes:
            logger.debug(
                f"Columns excluded from update/merge because source field is missing "
                f"or no_update attribute set:\n{excludes}"
            )
        self._update_excludes = set(excludes)
        self._update_excludes_calculated = True
        if current_excludes != self._update_excludes:
            self._sql_statements['update'] = None
            self._sql_statements['merge'] = None

    def _exec_sql(self, sql: str, params: Union[dict, tuple],
                  operation: str, raise_error: bool) -> int:
        try:
            self._cursor.execute(sql, params)
            self.counts[operation] += 1
            return 0
        except self._cursor.connection.driver.DatabaseError as e:
            error_msg = f"SQL failed: {sql}\nParams: {params}\nError: {str(e)}"
            logger.error(error_msg)
            if raise_error:
                raise
            return 1

    def execute(self, operation: str, raise_error: bool = False) -> int:
        """
        Execute the specified database operation using current record values.

        Returns 0 on success, 1 on skip/error.
        """
        if operation not in self.OPERATIONS:
            raise ValueError(f"Invalid operation '{operation}'")

        if operation in ('select', 'update', 'delete', 'merge') and not self._key_cols:
            msg = f"Cannot {operation} table {self._name}: no key columns defined"
            logger.error(msg)
            raise ValueError(msg)

        if operation in ('update', 'merge') and self._record_fields and not self._update_excludes_calculated:
            self.calc_update_excludes(self._record_fields)

        if not self.is_ready(operation):
            missing = self.reqs_missing(operation)
            msg = f"{operation} requirements not met: columns {missing} are null"
            if raise_error:
                logger.error(f"Cannot {operation} table {self._name}: {msg}")
                raise ValueError(msg)
            else:
                logger.warning(f"Skipping {operation} on table {self._name}: {msg}")
                self.counts['incomplete'] += 1
                return 1

        sql = self.get_sql(operation)
        params = self.get_bind_params(operation)
        return self._exec_sql(sql, params, operation, raise_error)

    def force_positional(self):
        if self._paramstyle in ParamStyle.positional_styles():
            return
        if self._paramstyle == 'named':
            self._paramstyle = 'numeric'
        elif self._paramstyle == 'pyformat':
            self._paramstyle = 'format'
        # rebuild SQL and parameter maps
        self._reset()

    def __repr__(self) -> str:
        return f"Table('{self.name}', {len(self.columns)} columns, {self.paramstyle})"