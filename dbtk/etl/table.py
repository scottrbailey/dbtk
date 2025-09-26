# dbtk/etl/table.py

import logging
import re
from textwrap import dedent
from typing import Union, Tuple, Optional, Set, Dict, Any

from ..cursors import Cursor
from ..database import ParamStyle
from ..utils import wrap_at_comma, process_sql_parameters

logger = logging.getLogger(__name__)


def validate_identifier(identifier: str, max_length: int = 64) -> str:
    """
    Validate that an identifier is safe for use (even if it needs quoting).
    Returns the identifier if valid, raises ValueError if invalid.
    """
    if '.' in identifier:
        # Split and recursively validate each part
        parts = identifier.split('.')
        validated_parts = [validate_identifier(part, max_length) for part in parts]
        return '.'.join(validated_parts)

    # Single identifier validation
    if not identifier:
        raise ValueError("Identifier cannot be empty")
    if not identifier[0].isalpha():
        raise ValueError(f"Identifier must start with a letter: {identifier}")
    if not identifier:
        raise ValueError("Identifier cannot be empty")
    if len(identifier) > max_length:
        raise ValueError(f"Identifier exceeds max length of {max_length}")

    # Check for characters/sequences that could enable injection or break SQL parsing
    dangerous_patterns = ['\x00', '\n', '\r', '"', ';', '\x1a', '--', '/*', '*/']
    for pattern in dangerous_patterns:
        if pattern in identifier:
            raise ValueError(f"Invalid identifier contains dangerous pattern '{pattern}': {identifier}")

    if identifier.startswith(' ') or identifier.endswith(' '):
        raise ValueError(f"Invalid identifier has leading/trailing spaces: {identifier}")

    return identifier


def identifier_needs_quoting(identifier: str) -> bool:
    """Check if identifier needs quoting."""
    return not re.match(r'^([a-z][a-z0-9_]*|[A-Z][A-Z0-9_]*)$', identifier)


def quote_identifier(identifier: str) -> str:
    """Quote identifier, handling qualified names by splitting on dots."""
    if '.' in identifier:
        # Split and recursively quote each part
        parts = identifier.split('.')
        quoted_parts = [quote_identifier(part) for part in parts]
        return '.'.join(quoted_parts)

    # Single identifier quoting
    if identifier_needs_quoting(identifier):
        return f'"{identifier}"'
    else:
        return identifier


def wrap_db_function(bind_name: str, db_fn: str = None) -> str:
    """Wrap column placeholder with database function if provided."""
    placeholder = f':{bind_name}'
    if db_fn is None:
        return placeholder
    if '#' in db_fn:
        return db_fn.replace('#', placeholder)
    if db_fn.endswith('()') :
        return db_fn
    if db_fn.lower() in ('sysdate', 'systimestamp', 'user', 'current_timestamp'):
        return db_fn

    raise ValueError(f"Unrecognized db_fn pattern: {db_fn}. Add '()' or '(#)', or use recognized constant.")


class Table:
    """
    Stateful table class for generating and executing SQL statements.
    Maintains current record state in self.values.
    """

    def __init__(
            self,
            name: str,
            columns: Dict[str, Dict[str, Any]],
            paramstyle: str = ParamStyle.NAMED,
            null_values: Tuple[str, ...] = ('NULL', '<null>', r'\N'),
    ):
        """
        Initialize Table with configuration.

        Args:
            name: Table name.
            columns: Dict of column names to their configuration (e.g., {'field', 'db_fn'}).
            paramstyle: Parameter style (from ParamStyle).
            null_values: String values to be considered as null values.
        """
        validate_identifier(name)
        self._name = name

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
        self.__paramstyle = paramstyle
        self.null_values = tuple(null_values)

        self._req_cols = tuple(req_cols)
        self._key_cols = tuple(key_cols)

        self._sql_statements: Dict[str, Optional[str]] = {
            'insert': None, 'select': None, 'update': None, 'delete': None, 'merge': None
        }
        self._param_config: Dict[str, Tuple[str, ...]] = {
            'insert': (), 'select': (), 'update': (), 'delete': (), 'merge': ()
        }
        self._update_excludes: Set[str] = set()
        self.counts: Dict[str, int] = {'insert': 0, 'select': 0, 'update': 0, 'delete': 0, 'merge': 0, 'records': 0}
        self.values: Dict[str, Any] = {}

    @property
    def name(self) -> str:
        return self._name

    @property
    def columns(self) -> dict:
        return self.__columns

    @property
    def paramstyle(self) -> str:
        return self.__paramstyle

    @property
    def sql_statements(self) -> Dict[str, Optional[str]]:
        return self._sql_statements

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
        if db_fn is None:
            return self._get_param_placeholder(col_name)
        if '#' in db_fn:
            return db_fn.replace('#', self._get_param_placeholder(col_name))
        if db_fn.endswith('()') :
            return db_fn
        if db_fn.lower() in ('sysdate', 'systimestamp', 'user', 'current_timestamp'):
            return db_fn
        return f'{db_fn}({self._get_param_placeholder(col_name)})'

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
            placeholders.append(wrap_db_function(bind_name, db_fn))

        cols_str = ', '.join(quote_identifier(col) for col in cols)
        placeholders_str = ', '.join(placeholders)

        if len(cols) > 4:
            cols_str = wrap_at_comma(cols_str)
            placeholders_str = wrap_at_comma(placeholders_str)

        sql = f"INSERT INTO {table_name} ({cols_str})\n VALUES \n({placeholders_str})"
        logger.debug(f"Generated insert SQL for {self._name}: {sql}")
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
                placeholder = wrap_db_function(bind_name, db_fn)
                conditions.append(f"{ident} = {placeholder}")

        cols_str = ', '.join(quoted_cols)
        if len(quoted_cols) > 4:
            cols_str = wrap_at_comma(cols_str)
        sql = f"SELECT {cols_str} FROM {table_name}"
        if conditions:
            conditions_str = '\n    AND '.join(conditions)
            sql += f" WHERE {conditions_str}"

        logger.debug(f"Generated select SQL for {self._name}: {sql}")
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
            placeholder = wrap_db_function(bind_name, db_fn)
            if bind_name in self._key_cols:
                conditions.append(f'{ident} = {placeholder}')
            elif bind_name not in self._update_excludes:
                update_cols.append(f'{ident} = {placeholder}')
        set_clause_str = ', '.join(update_cols)
        if len(update_cols) > 4:
            set_clause_str = wrap_at_comma(set_clause_str)
        conditions_str = '\n    AND '.join(conditions)

        sql = f"UPDATE {table_name} SET {set_clause_str} \nWHERE {conditions_str}"
        logger.debug(f"Generated update SQL for {self._name}: {sql}")
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
            placeholder = wrap_db_function(bind_name, db_fn)
            conditions.append(f"{quoted_col} = {placeholder}")
        conditions_str = '\n    AND '.join(conditions)
        sql = f"DELETE FROM {table_name} \nWHERE {conditions_str}"
        logger.debug(f"Generated delete SQL for {self._name}: {sql}")
        return sql

    def _should_use_upsert(self, db_type: str) -> bool:
        """Determine whether to use upsert syntax vs MERGE statement."""
        if db_type == 'mysql':
            return True
        elif db_type == 'postgres':
            # Check PostgreSQL version - use upsert for < 15, can use either for >= 15
            try:
                version = getattr(self.connection, '_connection', {}).get('server_version', 150000)
                return version < 150000
            except:
                return True  # Default to upsert for safety
        else:
            return False  # Use MERGE for Oracle, SQL Server, etc.

    def _create_upsert(self, db_type: str) -> str:
        """Create INSERT ... ON DUPLICATE KEY/CONFLICT statement with named parameters."""
        table_name = quote_identifier(self._name)
        cols = list(self.__columns.keys())
        placeholders = []

        # Build INSERT portion with named placeholders
        for col in cols:
            bind_name = self.__columns[col]['bind_name']
            db_fn = self.__columns[col].get('db_fn')
            placeholders.append(wrap_db_function(bind_name, db_fn))

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

            sql = f"""INSERT INTO {table_name} ({cols_str})
    VALUES ({placeholders_str}) AS new_vals
    ON DUPLICATE KEY UPDATE {update_clause}"""

        elif db_type == 'postgres':
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

        logger.debug(f"Generated upsert SQL for {self._name}: {sql}")
        return sql

    def _create_merge_statement(self, db_type: str) -> str:
        """Create traditional MERGE statement with named parameters."""
        table_name = quote_identifier(self._name)
        cols = list(self.__columns.keys())
        placeholders = []

        # Build placeholders for source
        for col in cols:
            bind_name = self.__columns[col]['bind_name']
            db_fn = self.__columns[col].get('db_fn')
            placeholders.append(wrap_db_function(bind_name, db_fn))

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

        elif db_type == 'postgres':
            # PostgreSQL MERGE with VALUES
            source_values = ', '.join(placeholders)
            if len(cols) > 4:
                source_values = wrap_at_comma(source_values)

            source_clause = f"VALUES ({source_values})"
            quoted_cols = [quote_identifier(col) for col in cols]
            table_alias = f"AS s ({', '.join(quoted_cols)})"

            update_assignments = []
            for col in update_cols:
                quoted_col = quote_identifier(col)
                update_assignments.append(f"{quoted_col} = EXCLUDED.{quoted_col}")

            insert_cols = ', '.join(quoted_cols)
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
        if db_type == 'postgres':
            sql = dedent(f"""\
            MERGE INTO {table_name} t
            USING ({source_clause}) {table_alias}
            ON ({' AND '.join(key_conditions)})
            WHEN MATCHED THEN
                UPDATE SET {update_set}
            WHEN NOT MATCHED THEN
                INSERT ({insert_cols})
                VALUES ({insert_values})""")
        else:
            sql = dedent(f"""\
            MERGE INTO {table_name} t
            USING ({source_clause}) {table_alias}
            ON ({' AND '.join(key_conditions)})
            WHEN MATCHED THEN
                UPDATE SET {update_set}
            WHEN NOT MATCHED THEN
                INSERT ({insert_cols})
                VALUES ({insert_values})""")

        logger.debug(f"Generated merge SQL for {self._name}: {sql}")
        return sql

    def _create_merge(self, db_type: str) -> str:
        """
        Generate MERGE or upsert statement for the specified database type.
        """
        if not self._key_cols:
            raise ValueError(f"Cannot create MERGE for table {self._name}: no key columns defined")

        # Choose strategy based on database capabilities
        use_upsert = self._should_use_upsert(db_type)

        if use_upsert:
            return self._create_upsert(db_type)
        else:
            return self._create_merge_statement(db_type)

    def generate_sql(self, operation: str, db_type: str = None) -> None:
        """
        Force generation of SQL for the specified operation.

        Populates self._sql_statements[operation] and self._param_config[operation].
        Useful for testing and debugging.

        Args:
            operation: One of 'insert', 'select', 'update', 'delete', 'merge'
            db_type: Required for 'merge' operation, ignored for others
            finalize: If True, cache the SQL and param config after generation

        Raises:
            ValueError: If operation is invalid or if merge is requested without db_type
            ValueError: If operation requires key_cols but none are defined
        """
        valid_operations = {'insert', 'select', 'update', 'delete', 'merge'}
        if operation not in valid_operations:
            raise ValueError(f"Invalid operation: {operation}. Must be one of {valid_operations}")

        # Validate key_cols requirements
        if operation in ('select', 'update', 'delete', 'merge') and not self._key_cols:
            raise ValueError(f"Cannot generate {operation} SQL for table {self._name}: no key columns defined")

        # Special handling for merge which needs db_type
        if operation == 'merge':
            if db_type is None:
                raise ValueError("db_type parameter is required for merge operation")
            sql = self._create_merge(db_type)
        else:
            # Call the appropriate creation method
            if operation == 'insert':
                sql = self._create_insert()
            elif operation == 'select':
                sql = self._create_select()
            elif operation == 'update':
                sql = self._create_update()
            elif operation == 'delete':
                sql = self._create_delete()

        # Process and cache the SQL
        self._finalize_sql(operation, sql)

    def get_bind_params(self, operation: str) -> Union[dict, tuple]:
        """Get prepared bind parameters for the specified operation using current self.values."""
        if operation not in self._sql_statements:
            raise ValueError(f"Invalid operation: {operation}")
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
                    logger.warning(f'Table {self.name}: field "{field}" not found in record')
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

    def reset_counts(self):
        """Reset operation counts."""
        self.counts = {'insert': 0, 'update': 0, 'delete': 0, 'merge': 0, 'records': 0, 'select': 0}

    @property
    def reqs_met(self) -> bool:
        """Check if required columns are set and not None."""
        return all(col in self.values and self.values[col] is not None for col in self._req_cols)

    @property
    def reqs_missing(self) -> Set[str]:
        """Get required columns that are missing or None."""
        return {col for col in self._req_cols if col not in self.values or self.values[col] is None}

    def get_db_record(self, cursor: Cursor) -> Dict[str, Any]:
        """Fetch a record from the database using current key values."""
        keys_missing = {col for col in self._key_cols if col not in self.values or self.values[col] is None}
        if keys_missing:
            msg = f"Cannot get record from table {self._name}: missing key columns: {keys_missing}"
            logger.error(msg)
        else:
            err = self.exec_select(cursor)
            if not err:
                return cursor.fetchone()

    def calc_update_excludes(self, file_columns: Set[str]):
        """Calculate columns to exclude from updates."""
        excludes = []
        for col, col_def in self.__columns.items():
            bind_name = col_def['bind_name']
            field = col_def.get('field')
            if field not in file_columns:
                excludes.append(bind_name)
            elif col_def.get('no_update'):
                excludes.append(bind_name)

        self._update_excludes = set(excludes)


    def _exec_sql(self, cursor: Cursor, sql: str, params: Union[dict, tuple],
                  operation: str, raise_error: bool) -> int:
        """Execute SQL with error handling."""
        try:
            cursor.execute(sql, params)
            self.counts[operation] += 1
            return 0
        except cursor.connection.interface.DatabaseError as e:
            error_msg = f"SQL failed: {sql}\nParams: {params}\nError: {str(e)}"
            logger.error(error_msg)
            if raise_error:
                raise
            return 1

    def exec_select(self, cursor: Cursor, raise_error: bool = False) -> int:
        if not self._key_cols:
            msg = f"Cannot select from table {self._name}: no key columns defined"
            logger.error(msg)
            raise ValueError(msg)

        if self._sql_statements['select'] is None:
            sql = self._create_select()
            self._finalize_sql('select', sql)

        params = self.get_bind_params('select')
        return self._exec_sql(cursor, self._sql_statements['select'], params, 'select', raise_error)


    def exec_insert(self, cursor: Cursor, raise_error: bool = False) -> int:
        """Execute INSERT statement for current record."""
        if not self.reqs_met:
            msg = f"Cannot insert into table {self._name}: required columns {self.reqs_missing} are null"
            logger.error(msg)
            raise ValueError(msg)

        if self._sql_statements['insert'] is None:
            sql = self._create_insert()
            self._finalize_sql('insert', sql)

        params = self.get_bind_params('insert')
        return self._exec_sql(cursor, self._sql_statements['insert'], params, 'insert', raise_error)

    def exec_update(self, cursor: Cursor, raise_error: bool = False) -> int:
        """Execute UPDATE statement for current record."""
        if not self.reqs_met:
            msg = f"Cannot update table {self._name}: required columns {self.reqs_missing} are null"
            logger.error(msg)
            raise ValueError(msg)

        if not self._key_cols:
            msg = f"Cannot update table {self._name}: no key columns defined"
            logger.error(msg)
            raise ValueError(msg)

        if self._sql_statements['update'] is None:
            sql = self._create_update()
            self._finalize_sql('update', sql)

        params = self.get_bind_params('update')
        return self._exec_sql(cursor, self._sql_statements['update'], params, 'update', raise_error)

    def exec_delete(self, cursor: Cursor, raise_error: bool = False) -> int:
        """Execute DELETE statement for current record."""
        if not self._key_cols:
            msg = f"Cannot delete from table {self._name}: no key columns defined"
            logger.error(msg)
            raise ValueError(msg)

        if self._sql_statements['delete'] is None:
            sql = self._create_delete()
            self._finalize_sql('delete', sql)

        params = self.get_bind_params('delete')
        return self._exec_sql(cursor, self._sql_statements['delete'], params, 'delete', raise_error)

    def exec_merge(self, cursor: Cursor, raise_error: bool = False) -> int:
        """Execute MERGE statement for current record."""
        if not self.reqs_met:
            msg = f"Cannot merge table {self._name}: required columns {self.reqs_missing} are null"
            logger.error(msg)
            raise ValueError(msg)

        if not self._key_cols:
            msg = f"Cannot merge table {self._name}: no key columns defined"
            logger.error(msg)
            raise ValueError(msg)

        if self._sql_statements['merge'] is None:
            db_type = cursor.connection.server_type
            if db_type == 'postgres' and cursor.connection.server_version < 150000:
                raise NotImplementedError(
                    f"PostgreSQL MERGE requires version >= 15, found {cursor.connection.server_version}")
            sql = self._create_merge(db_type)
            self._finalize_sql('merge', sql)

        params = self.get_bind_params('merge')
        return self._exec_sql(cursor, self._sql_statements['merge'], params, 'merge', raise_error)
