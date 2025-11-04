# dbtk/etl/table.py

"""
Schema-aware table operations and SQL generation.

Provides the Table class which manages table metadata and generates
parameterized SQL statements for common operations.
"""

import logging
import re
from textwrap import dedent
from typing import Union, Tuple, Optional, Set, Dict, Any

from ..cursors import Cursor
from ..database import ParamStyle
from ..utils import wrap_at_comma, process_sql_parameters, validate_identifier, quote_identifier

logger = logging.getLogger(__name__)


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

        * **field** (str or list of str):
          Source field name(s) from input records. If list, extracts multiple fields
          as a list value. If omitted, column is populated via 'value' or 'db_fn'.

        * **value** (any, optional):
          Default/constant value to use for this column. Applied when source field
          is missing, empty, or None.

        * **fn** (callable or list of callables, optional):
          Transform function(s) applied to field value. If list, functions are
          applied in order (pipeline). Function receives field value and returns
          transformed value.

        * **db_fn** (str, optional):
          Database-side function call (e.g., 'CURRENT_TIMESTAMP', 'UPPER(#)').
          Use '#' as placeholder for the bind parameter. If specified without '#',
          no bind parameter is created (useful for CURRENT_TIMESTAMP, etc.).

        * **primary_key** (bool, optional, default False):
          Marks column as primary key. Automatically sets key=True and required=True.

        * **key** (bool, optional, default False):
          Marks column as key for WHERE clauses in SELECT, UPDATE, DELETE operations.

        * **nullable** (bool, optional, default True):
          If False, marks column as required (must have non-None value for INSERT/MERGE).

        * **required** (bool, optional, default False):
          Explicitly marks column as required.

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
        from dbtk.etl.transforms import parse_date, get_int

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
                    'fn': parse_date,
                    'nullable': False
                },

                # Optional field with chained transformations
                'firebending_level': {
                    'field': 'flame_skill',
                    'fn': [str.strip, get_int]  # Clean then convert
                },

                # Constant value for all records
                'status': {
                    'value': 'active'
                },

                # Database-side function with parameter
                'combat_name': {
                    'field': 'full_name',
                    'db_fn': 'generate_callsign(#)'
                },

                # Database-side function, no parameter
                'created_at': {
                    'db_fn': 'CURRENT_TIMESTAMP'
                },

                # Multiple source fields as list
                'contact_methods': {
                    'field': ['email', 'phone', 'pigeon']
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
            soldiers.exec_insert()  # Automatically validates requirements
            print(soldiers.counts)  # {'insert': 1, 'update': 0, ...}

    Attributes
    ----------
        values (dict): Current record values (dict of column_name: value)
        counts (dict): Operation counters (insert, update, delete, select, merge, records, incomplete)

    Note:
        Properties `name`, `columns`, `cursor`, `paramstyle`, `req_cols`, `key_cols`,
        `reqs_met`, `reqs_missing`, `has_all_keys`, and `keys_missing` are documented
        separately below.
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
                'fn', 'value', 'db_fn', 'primary_key', 'nullable', etc.
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
                'created': {'db_fn': 'CURRENT_TIMESTAMP'}
            }, cursor=cursor)
        """
        validate_identifier(name)
        self._name = name
        self._cursor = cursor
        self.__paramstyle = cursor.connection.interface.paramstyle

        # Validate each column and add sanitized bind_name
        validated_columns = {}
        req_cols = []
        key_cols = []
        for col, col_def in columns.items():
            validate_identifier(col)
            bind_name = self._sanitize_for_bind_param(col)
            col_def['bind_name'] = bind_name
            if col_def.get('primary_key') or col_def.get('key'):
                key_cols.append(bind_name)
                req_cols.append(bind_name)
            elif bool(col_def.get('nullable', True)) is False or col_def.get('required'):
                req_cols.append(bind_name)
            validated_columns[col] = col_def
        self.__columns = validated_columns
        self.null_values = tuple(null_values)
        # Required columns (nullable=False or required=True)
        self._req_cols = tuple(req_cols)
        # Key columns (primary_key=True or key=True)
        self._key_cols = tuple(key_cols)

        # Initialize operation-specific dictionaries using OPERATIONS tuple
        self._sql_statements: Dict[str, Optional[str]] = {op: None for op in self.OPERATIONS}
        self._param_config: Dict[str, Tuple[str, ...]] = {op: () for op in self.OPERATIONS}
        self.counts: Dict[str, int] = {op: 0 for op in self.OPERATIONS}
        self.counts['records'] = 0  # Add special records counter
        self.counts['incomplete'] = 0  # Track records skipped due to missing requirements

        # Caches field names when self.set_values is called and used to calculate _update_excludes fields
        self._record_fields = set()
        # Fields that shouldn't be updated in UPDATEs/MERGEs because the source fields were not in the record
        self._update_excludes: Set[str] = set()
        self._update_excludes_calculated = False

        self.values: Dict[str, Any] = {}

        # Generate INSERT SQL immediately to filter req_cols
        self.generate_sql('insert')
        # Only require params that are actually used in this operation's SQL
        self._req_cols = tuple(col for col in self._req_cols
                               if col in self._param_config['insert'])

    @property
    def name(self) -> str:
        """Table name as used in SQL statements."""
        return self._name

    @property
    def columns(self) -> dict:
        """Column metadata dictionary with types and constraints."""
        return self.__columns

    @property
    def paramstyle(self) -> str:
        """Parameter style for SQL placeholders (from cursor)."""
        return self.__paramstyle

    @property
    def cursor(self) -> Cursor:
        """Database cursor for executing SQL operations."""
        return self._cursor

    @property
    def req_cols(self) -> Tuple[str]:
        """List of required (non-nullable) column names."""
        return self._req_cols

    @property
    def key_cols(self) -> Tuple[str]:
        """List of key/primary key column names."""
        return self._key_cols

    def get_sql(self, operation: str) -> str:
        """
        Get SQL statement for the specified operation.
        Generates the SQL if it hasn't been created yet (lazy initialization).

        Args:
            operation: One of 'select', 'insert', 'update', 'delete', 'merge'

        Returns:
            The SQL statement string

        Raises:
            ValueError: If operation is not valid
        """
        if operation not in self.OPERATIONS:
            raise ValueError(f"Invalid operation '{operation}'. Must be one of {self.OPERATIONS}")

        if self._sql_statements[operation] is None:
            self.generate_sql(operation)
        return self._sql_statements[operation]

    def __repr__(self) -> str:
        """String representation for debugging."""
        return f"Table('{self.name}', {len(self.columns)} columns, {self.paramstyle})"

    def _sanitize_for_bind_param(self, name: str) -> str:
        """Convert a column name to a valid bind parameter name."""
        # Replace non-alphanumeric chars with underscore, collapse multiple underscores
        sanitized = re.sub(r'[^a-z0-9_]+', '_', name.lower())

        # Ensure it starts with a letter
        if not sanitized[0].isalpha():
            sanitized = 'col_' + sanitized

        # Remove trailing underscore if present
        return sanitized.rstrip('_')

    def _wrap_db_function(self, col_name: str, db_fn: str = None) -> str:
        """Wrap column placeholder with database function if provided."""
        if db_fn in (None, ''):
            return f':{col_name}'
        # Strip whitespace to be defensive against typos
        db_fn = db_fn.strip()
        if not db_fn:
            # incase '' or ' ' were passed
            return f':{col_name}'
        if '#' in db_fn:
            return db_fn.replace('#', f':{col_name}')
        if '(' in db_fn and ')' in db_fn:
            # Contains parentheses - assume it's a complete expression
            return db_fn
        if db_fn.lower() in ('sysdate', 'systimestamp', 'user', 'current_timestamp', 'current_date', 'current_time'):
            return db_fn
        return f'{db_fn}(:{col_name})'

    def _finalize_sql(self, operation: str, sql: str) -> None:
        """Process SQL and store results."""
        self._sql_statements[operation], self._param_config[operation] = process_sql_parameters(sql, self.__paramstyle)

    def _create_insert(self) -> str:
        """Generate INSERT statement with named parameters."""
        table_name = quote_identifier(self._name)
        cols = list(self.__columns.keys())
        placeholders = []

        for col in cols:
            bind_name = self.__columns[col]['bind_name']
            db_fn = self.__columns[col].get('db_fn')
            placeholders.append(self._wrap_db_function(bind_name, db_fn))

        cols_str = ', '.join(quote_identifier(col) for col in cols)
        placeholders_str = ', '.join(placeholders)

        if len(cols) > 4:
            cols_str = wrap_at_comma(cols_str)
            placeholders_str = wrap_at_comma(placeholders_str)

        sql = f"INSERT INTO {table_name} ({cols_str})\nVALUES\n({placeholders_str})"
        logger.debug(f"Generated insert SQL for {self._name}:\n{sql}")
        return sql

    def _create_select(self) -> str:
        """Generate SELECT statement with named parameters."""
        if not self._key_cols:
            raise ValueError(f"Cannot create SELECT for table {self._name}: no key columns defined")
        table_name = quote_identifier(self._name)
        quoted_cols = []
        conditions = []

        for col, col_def in self.__columns.items():
            ident = quote_identifier(col)
            quoted_cols.append(ident)
            bind_name = col_def['bind_name']
            if bind_name in self._key_cols:
                db_fn = col_def.get('db_fn')
                placeholder = self._wrap_db_function(bind_name, db_fn)
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

    def _create_update(self) -> str:
        """Generate UPDATE statement with named parameters."""
        if not self._key_cols:
            raise ValueError(f"Cannot create UPDATE for table {self._name}: no key columns defined")

        table_name = quote_identifier(self._name)
        update_cols = []
        conditions = []

        for col, col_def in self.__columns.items():
            ident = quote_identifier(col)
            bind_name = col_def['bind_name']
            db_fn = col_def.get('db_fn')
            placeholder = self._wrap_db_function(bind_name, db_fn)
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

        for col in self._key_cols:
            quoted_col = quote_identifier(col)
            bind_name = self.__columns[col]['bind_name']
            db_fn = self.__columns[col].get('db_fn')
            placeholder = self._wrap_db_function(bind_name, db_fn)
            conditions.append(f"{quoted_col} = {placeholder}")
        conditions_str = '\n    AND '.join(conditions)
        sql = f"DELETE FROM {table_name} \nWHERE {conditions_str}"
        logger.debug(f"Generated delete SQL for {self._name}:\n{sql}")
        return sql

    def _should_use_upsert(self) -> bool:
        """Determine whether to use upsert syntax vs MERGE statement."""
        db_type = self._cursor.connection.server_type

        if db_type in ('mysql', 'postgres', 'sqlite'):
            return True
        else:
            return False

    def _create_upsert(self) -> str:
        """Create INSERT ... ON DUPLICATE KEY/CONFLICT statement with named parameters."""
        db_type = self._cursor.connection.server_type
        table_name = quote_identifier(self._name)
        cols = list(self.__columns.keys())
        placeholders = []

        # Build INSERT portion with named placeholders
        for col in cols:
            bind_name = self.__columns[col]['bind_name']
            db_fn = self.__columns[col].get('db_fn')
            placeholders.append(self._wrap_db_function(bind_name, db_fn))

        # Get non-key columns for updates (excluding update_excludes)
        update_cols = []
        for col in cols:
            bind_name = self.__columns[col]['bind_name']
            if bind_name not in self._key_cols and bind_name not in self._update_excludes:
                update_cols.append(col)

        cols_str = ', '.join(quote_identifier(col) for col in cols)
        placeholders_str = ', '.join(placeholders)

        if len(cols) > 4:
            cols_str = wrap_at_comma(cols_str)
            placeholders_str = wrap_at_comma(placeholders_str)

        if db_type == 'mysql':
            # MySQL: INSERT ... ON DUPLICATE KEY UPDATE
            update_assignments = []
            for col in update_cols:
                quoted_col = quote_identifier(col)
                bind_name = self.__columns[col]['bind_name']
                db_fn = self.__columns[col].get('db_fn')

                if db_fn and '#' in db_fn:
                    # Use alias syntax for MySQL 8.0.19+
                    assignment = f"{quoted_col} = {db_fn.replace('#', f'new_vals.{quoted_col}')}"
                elif db_fn:
                    assignment = f"{quoted_col} = {db_fn}"
                else:
                    assignment = f"{quoted_col} = new_vals.{quoted_col}"
                update_assignments.append(assignment)

            update_clause = ', '.join(update_assignments)
            if len(update_assignments) > 4:
                update_clause = wrap_at_comma(update_clause)

            sql = dedent(f"""/
            INSERT INTO {table_name} ({cols_str})
            VALUES ({placeholders_str}) AS new_vals
            ON DUPLICATE KEY UPDATE {update_clause}""")

        elif db_type in ('postgres', 'sqlite'):
            # PostgreSQL: INSERT ... ON CONFLICT DO UPDATE
            key_cols = []
            for col, col_def in self.__columns.items():
                bind_name = col_def['bind_name']
                if bind_name in self._key_cols:
                    key_cols.append(col)

            conflict_cols = ', '.join(quote_identifier(col) for col in key_cols)

            update_assignments = []
            for col in update_cols:
                quoted_col = quote_identifier(col)
                bind_name = self.__columns[col]['bind_name']
                db_fn = self.__columns[col].get('db_fn')

                if db_fn and '#' in db_fn:
                    assignment = f"{quoted_col} = {db_fn.replace('#', f'EXCLUDED.{quoted_col}')}"
                elif db_fn:
                    assignment = f"{quoted_col} = {db_fn}"
                else:
                    assignment = f"{quoted_col} = EXCLUDED.{quoted_col}"
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
        db_type = self._cursor.connection.server_type
        table_name = quote_identifier(self._name)
        cols = list(self.__columns.keys())
        placeholders = []

        # Build placeholders for source
        for col in cols:
            bind_name = self.__columns[col]['bind_name']
            db_fn = self.__columns[col].get('db_fn')
            placeholders.append(self._wrap_db_function(bind_name, db_fn))

        # Build key conditions for matching
        key_conditions = []
        key_cols = []
        for col, col_def in self.__columns.items():
            bind_name = col_def['bind_name']
            if bind_name in self._key_cols:
                quoted_col = quote_identifier(col)
                key_conditions.append(f"t.{quoted_col} = s.{quoted_col}")
                key_cols.append(col)

        # Get non-key columns for updates (excluding update_excludes)
        update_cols = []
        for col in cols:
            bind_name = self.__columns[col]['bind_name']
            if bind_name not in self._key_cols and bind_name not in self._update_excludes:
                update_cols.append(col)

        # Database-specific MERGE templates
        if db_type == 'oracle':
            # Oracle MERGE with dual table
            source_items = []
            for col, placeholder in zip(cols, placeholders):
                quoted_col = quote_identifier(col)
                source_items.append(f"{placeholder} AS {quoted_col}")

            source_cols = ', '.join(source_items)
            if len(cols) > 4:
                source_cols = wrap_at_comma(source_cols)

            source_clause = f"SELECT {source_cols} FROM dual"
            table_alias = "AS s"

            update_assignments = []
            for col in update_cols:
                quoted_col = quote_identifier(col)
                update_assignments.append(f"t.{quoted_col} = s.{quoted_col}")

            insert_cols = ', '.join(quote_identifier(col) for col in cols)
            insert_values = ', '.join(f"s.{quote_identifier(col)}" for col in cols)

        elif db_type == 'sqlserver':
            # SQL Server MERGE
            source_items = []
            for col, placeholder in zip(cols, placeholders):
                quoted_col = quote_identifier(col)
                source_items.append(f"{placeholder} AS {quoted_col}")

            source_cols = ', '.join(source_items)
            if len(cols) > 4:
                source_cols = wrap_at_comma(source_cols)

            source_clause = f"SELECT {source_cols}"
            table_alias = "AS s"

            update_assignments = []
            for col in update_cols:
                quoted_col = quote_identifier(col)
                update_assignments.append(f"t.{quoted_col} = s.{quoted_col}")

            insert_cols = ', '.join(quote_identifier(col) for col in cols)
            insert_values = ', '.join(f"s.{quote_identifier(col)}" for col in cols)

        else:
            raise NotImplementedError(f"MERGE not supported for database: {db_type}")

        # Build final clauses
        update_set = ', '.join(update_assignments)
        if len(update_assignments) > 4:
            update_set = wrap_at_comma(update_set)

        if len(cols) > 4:
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
        """
        Force generation of SQL for the specified operation.

        Populates self._sql_statements[operation] and self._param_config[operation].
        Useful for testing and debugging.

        Args:
            operation: One of 'insert', 'select', 'update', 'delete', 'merge'

        Raises:
            ValueError: If operation is invalid
            ValueError: If operation requires key_cols but none are defined
        """
        if operation not in self.OPERATIONS:
            raise ValueError(f"Invalid operation '{operation}'. Must be one of {self.OPERATIONS}")

        # Validate key_cols requirements
        if operation in ('select', 'update', 'delete', 'merge') and not self._key_cols:
            raise ValueError(f"Cannot generate {operation} SQL for table {self._name}: no key columns defined")

        # Call the appropriate creation method
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

        # Process and cache the SQL
        self._finalize_sql(operation, sql)

    def get_bind_params(self, operation: str) -> Union[dict, tuple]:
        """Get prepared bind parameters for the specified operation using current self.values."""
        if operation not in self.OPERATIONS:
            raise ValueError(f"Invalid operation '{operation}'. Must be one of {self.OPERATIONS}")
        if operation in ('update', 'delete', 'merge') and not self._key_cols:
            raise ValueError(f"Cannot get {operation} params: no key columns defined")

        param_names = self._param_config[operation]
        if not param_names:
            return () if self.__paramstyle in ParamStyle.positional_styles() else {}

        # Map bind_names back to values using column lookup
        bind_to_col = {col_def['bind_name']: col for col, col_def in self.__columns.items()}
        filtered_values = {}

        for bind_name in param_names:
            if bind_name in bind_to_col:
                col = bind_to_col[bind_name]
                if col in self.values:
                    filtered_values[bind_name] = self.values[col]

        if self.__paramstyle in ParamStyle.positional_styles():
            # Return tuple in the order parameters appear in SQL
            return tuple(filtered_values.get(param, None) for param in param_names)
        else:
            # Return dict for named/pyformat styles
            return filtered_values

    def set_values(self, record: Dict[str, Any]):
        """
        Set table values from source record.

        Args:
            record: Source record (from readers)
        """
        # Track record processing
        self.counts['records'] += 1

        # Only warn about missing fields on first record
        warn_missing = self.counts['records'] == 1
        # Cache fields so we can calculate missing fields to exclude from updates/merges
        if not self._record_fields:
            self._record_fields = set(record.keys())

        values = {}
        for col, col_def in self.columns.items():
            val = None
            field = col_def.get('field')

            if isinstance(field, list):
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

            # First, convert custom null indicators
            if isinstance(val, str) and val in self.null_values:
                val = None

            # Then apply default values for empty/None
            if val in ('', None) and 'value' in col_def:
                val = col_def['value']

            # Apply transforms
            if 'fn' in col_def:
                fn = col_def['fn']
                if isinstance(fn, (list, tuple)):
                    for func in fn:
                        val = func(val)
                else:
                    val = fn(val)
            values[col] = val
        self.values = values

    def set_cursor(self, cursor: Cursor):
        """
        Switch to a different cursor (e.g., different database/connection).

        If paramstyle changes, invalidates cached SQL statements.

        Args:
            cursor: New cursor to use for this table
        """
        old_paramstyle = self.__paramstyle
        self._cursor = cursor
        self.__paramstyle = cursor.connection.interface.paramstyle

        if old_paramstyle != self.__paramstyle:
            self._reset()
            logger.info(
                f"Table {self._name}: paramstyle changed from {old_paramstyle} "
                f"to {self.__paramstyle}, cache reset"
            )

    def _reset(self):
        """Reset all cached state including SQL statements, parameters, counts, and values."""
        self._sql_statements = {op: None for op in self.OPERATIONS}
        self._param_config = {op: () for op in self.OPERATIONS}
        self.counts = {op: 0 for op in self.OPERATIONS}
        self.counts['records'] = 0
        self.counts['incomplete'] = 0
        self._update_excludes = set()
        self._update_excludes_calculated = False
        self._record_fields = set()
        self.values = {}

    @property
    def reqs_met(self) -> bool:
        """Check if required columns are set and not None."""
        return all(col in self.values and self.values[col] is not None for col in self._req_cols)

    @property
    def reqs_missing(self) -> Set[str]:
        """Get required columns that are missing or None."""
        return {col for col in self._req_cols if col not in self.values or self.values[col] is None}

    @property
    def has_all_keys(self) -> bool:
        """Check if all key columns are set and not None."""
        return all(col in self.values and self.values[col] is not None for col in self._key_cols)

    @property
    def keys_missing(self) -> Set[str]:
        """Get key columns that are missing or None."""
        return {col for col in self._key_cols if col not in self.values or self.values[col] is None}

    def fetch(self) -> Dict[str, Any]:
        """Fetch a record from the database using current key values."""
        err = self.exec_select()
        if not err:
            return self._cursor.fetchone()

    def calc_update_excludes(self, file_columns: Set[str]):
        """
        Calculate columns to exclude from updates/merges because source field is missing from record.
        This prevents us from unintentionally NULLing out values because a field was missing or misnamed
        in a data source. Columns can be explicitly excluded from updates by setting the 'no_update' attribute.

        This can be called manually. But when self.set_values() is called, the source fields are cached to self._record_fields.
        And when self.execute_update() or self.execute_merge() is called, the cached fields are used to call self.calc_update_excludes().

        Args:
            file_columns: Set of column names from the source file.
        """
        current_excludes = self._update_excludes
        excludes = []
        for col, col_def in self.__columns.items():
            bind_name = col_def['bind_name']
            field = col_def.get('field')
            if field and field not in file_columns:
                if field in self.key_cols:
                    raise ValueError(f"A key column {col} is sourced from {field}, but is missing from source.")
                else:
                    excludes.append(bind_name)
            elif col_def.get('no_update'):
                excludes.append(bind_name)
        if excludes:
            logger.debug(f"Columns excluded from update/merge because source field is missing or no_update attribute set:\n{excludes}")
        self._update_excludes = set(excludes)
        self._update_excludes_calculated = True
        if current_excludes != self._update_excludes:
            # Excluded columns have changed, invalidate the SQL statements so they are regenerated
            self._sql_statements['update'] = None
            self._sql_statements['merge'] = None

    def _exec_sql(self, sql: str, params: Union[dict, tuple],
                  operation: str, raise_error: bool) -> int:
        """Execute SQL with error handling."""
        try:
            self._cursor.execute(sql, params)
            self.counts[operation] += 1
            return 0
        except self._cursor.connection.interface.DatabaseError as e:
            error_msg = f"SQL failed: {sql}\nParams: {params}\nError: {str(e)}"
            logger.error(error_msg)
            if raise_error:
                raise
            return 1

    def exec_select(self, raise_error: bool = False, reqs_checked: bool = False) -> int:
        """
        Execute SELECT statement for current record.

        Args:
            raise_error: If True, raise exceptions on errors or missing requirements.
                        If False, log and return error code.
            reqs_checked: If True, skip requirement validation (caller has already checked).

        Returns:
            0 on success, 1 on error or incomplete data.
        """
        if not self._key_cols:
            msg = f"Cannot select from table {self._name}: no key columns defined"
            logger.error(msg)
            raise ValueError(msg)

        if not reqs_checked and not self.has_all_keys:
            msg = f"key columns {self.keys_missing} are null"
            if raise_error:
                logger.error(f"Cannot select from table {self._name}: {msg}")
                raise ValueError(f"Cannot select from table {self._name}: {msg}")
            else:
                logger.warning(f"Skipping select on table {self._name}: {msg}")
                self.counts['incomplete'] += 1
                return 1

        sql = self.get_sql('select')
        params = self.get_bind_params('select')
        return self._exec_sql(sql, params, 'select', raise_error)

    def exec_insert(self, raise_error: bool = False, reqs_checked: bool = False) -> int:
        """
        Execute INSERT statement for current record.

        Args:
            raise_error: If True, raise exceptions on errors or missing requirements.
                        If False, log and return error code.
            reqs_checked: If True, skip requirement validation (caller has already checked).

        Returns:
            0 on success, 1 on error or incomplete data.
        """
        if not reqs_checked and not self.reqs_met:
            msg = f"required columns {self.reqs_missing} are null"
            if raise_error:
                logger.error(f"Cannot insert into table {self._name}: {msg}")
                raise ValueError(f"Cannot insert into table {self._name}: {msg}")
            else:
                logger.warning(f"Skipping insert on table {self._name}: {msg}")
                self.counts['incomplete'] += 1
                return 1

        sql = self.get_sql('insert')
        params = self.get_bind_params('insert')
        return self._exec_sql(sql, params, 'insert', raise_error)

    def exec_update(self, raise_error: bool = False, reqs_checked: bool = False) -> int:
        """
        Execute UPDATE statement for current record.

        Args:
            raise_error: If True, raise exceptions on errors or missing requirements.
                        If False, log and return error code.
            reqs_checked: If True, skip requirement validation (caller has already checked).

        Returns:
            0 on success, 1 on error or incomplete data.
        """
        # Calculate columns we don't want to update to NULL because the field is missing from the source record
        if self._record_fields and not self._update_excludes_calculated:
            self.calc_update_excludes(self._record_fields)

        if not reqs_checked and not self.reqs_met:
            msg = f"required columns {self.reqs_missing} are null"
            if raise_error:
                logger.error(f"Cannot update table {self._name}: {msg}")
                raise ValueError(f"Cannot update table {self._name}: {msg}")
            else:
                logger.warning(f"Skipping update on table {self._name}: {msg}")
                self.counts['incomplete'] += 1
                return 1

        if not self._key_cols:
            msg = f"Cannot update table {self._name}: no key columns defined"
            logger.error(msg)
            raise ValueError(msg)

        sql = self.get_sql('update')
        params = self.get_bind_params('update')
        return self._exec_sql(sql, params, 'update', raise_error)

    def exec_delete(self, raise_error: bool = False, reqs_checked: bool = False) -> int:
        """
        Execute DELETE statement for current record.

        Args:
            raise_error: If True, raise exceptions on errors or missing requirements.
                        If False, log and return error code.
            reqs_checked: If True, skip requirement validation (caller has already checked).

        Returns:
            0 on success, 1 on error or incomplete data.
        """
        if not self._key_cols:
            msg = f"Cannot delete from table {self._name}: no key columns defined"
            logger.error(msg)
            raise ValueError(msg)

        if not reqs_checked and not self.has_all_keys:
            msg = f"key columns {self.keys_missing} are null"
            if raise_error:
                logger.error(f"Cannot delete from table {self._name}: {msg}")
                raise ValueError(f"Cannot delete from table {self._name}: {msg}")
            else:
                logger.warning(f"Skipping delete on table {self._name}: {msg}")
                self.counts['incomplete'] += 1
                return 1

        sql = self.get_sql('delete')
        params = self.get_bind_params('delete')
        return self._exec_sql(sql, params, 'delete', raise_error)

    def exec_merge(self, raise_error: bool = False, reqs_checked: bool = False) -> int:
        """
        Execute MERGE statement for current record.

        Args:
            raise_error: If True, raise exceptions on errors or missing requirements.
                        If False, log and return error code.
            reqs_checked: If True, skip requirement validation (caller has already checked).

        Returns:
            0 on success, 1 on error or incomplete data.
        """
        # Calculate columns we don't want to update to NULL because the field is missing from the source record
        if self._record_fields and not self._update_excludes_calculated:
            self.calc_update_excludes(self._record_fields)

        if not reqs_checked and not self.reqs_met:
            msg = f"required columns {self.reqs_missing} are null"
            if raise_error:
                logger.error(f"Cannot merge table {self._name}: {msg}")
                raise ValueError(f"Cannot merge table {self._name}: {msg}")
            else:
                logger.warning(f"Skipping merge on table {self._name}: {msg}")
                self.counts['incomplete'] += 1
                return 1

        if not self._key_cols:
            msg = f"Cannot merge table {self._name}: no key columns defined"
            logger.error(msg)
            raise ValueError(msg)

        sql = self.get_sql('merge')
        params = self.get_bind_params('merge')
        return self._exec_sql(sql, params, 'merge', raise_error)