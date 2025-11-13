# dbtk/etl/config_generators.py
"""
Generate column definitions for Table class from database schema.

Extracts table metadata and formats it as Python dictionary code
that can be copied into Table() constructor calls.
"""

from typing import Dict, Any
from ..cursors import DictCursor


def column_defs_from_db(cursor, table_name: str, add_comments: bool = False) -> str:
    """
    Generate column definitions from database table schema.

    Inspects the database table structure and returns a Python dictionary
    string representation of column configurations ready to use with the
    Table class.

    Args:
        cursor: Database cursor (any cursor type)
        table_name: Name of table to analyze (supports schema.table format)
        add_comments: Include table/column comments from database metadata

    Returns:
        String containing Python dict of column definitions

    Example:
        >>> print(column_defs_from_db(cursor, 'users'))
        {
            'id': {'field': 'id', 'primary_key': True},
            'name': {'field': 'name', 'nullable': False},
            'email': {'field': 'email'},
            'created_at': {'db_fn': 'CURRENT_TIMESTAMP'}
        }

        >>> # Copy output into your code:
        >>> table = Table('users', columns={
        ...     'id': {'field': 'id', 'primary_key': True},
        ...     'name': {'field': 'name', 'nullable': False},
        ...     'email': {'field': 'email'},
        ...     'created_at': {'db_fn': 'CURRENT_TIMESTAMP'}
        ... }, cursor=cursor)
    """
    # Swap DictCursor for RecordCursor to allow positional access in metadata functions
    if isinstance(cursor, DictCursor):
        cursor = cursor.connection.cursor()  # Default is RecordCursor

    db_type = cursor.connection.database_type

    # Dispatch to database-specific metadata extractor
    if db_type == 'oracle':
        metadata = _get_oracle_metadata(cursor, table_name, add_comments)
    elif db_type == 'postgres':
        metadata = _get_postgres_metadata(cursor, table_name, add_comments)
    elif db_type == 'mysql':
        metadata = _get_mysql_metadata(cursor, table_name, add_comments)
    elif db_type in ('sqlserver', 'mssql'):
        metadata = _get_sqlserver_metadata(cursor, table_name, add_comments)
    else:
        raise ValueError(f"Column generation not supported for database type: {db_type}")

    # Format metadata dict as Python code string
    return _format_columns_dict(metadata, add_comments)


def _format_columns_dict(metadata: Dict[str, Any], add_comments: bool) -> str:
    """
    Format metadata dictionary as Python code string.

    Args:
        metadata: Dict with 'columns', 'table_comment', 'column_comments' keys
        add_comments: Whether to include comments in output

    Returns:
        Formatted Python dictionary string
    """
    lines = []

    # Add table comment if present
    if add_comments and metadata.get('table_comment'):
        lines.append(f"# {metadata['table_comment']}")

    lines.append("{")

    for col_name, col_def in metadata['columns'].items():
        # Add column comment if present
        if add_comments and col_name in metadata.get('column_comments', {}):
            comment = metadata['column_comments'][col_name]
            lines.append(f"    # {comment}")

        # Build column definition
        parts = []
        for key, value in col_def.items():
            if isinstance(value, str):
                # String values need quotes
                parts.append(f"'{key}': '{value}'")
            elif isinstance(value, bool):
                # Boolean values
                parts.append(f"'{key}': {value}")
            else:
                # Numbers or other types
                parts.append(f"'{key}': {value}")

        col_str = "{" + ", ".join(parts) + "}"
        lines.append(f"    '{col_name}': {col_str},")

    lines.append("}")

    return "\n".join(lines)


def _get_oracle_metadata(cursor, table_name: str, add_comments: bool = False) -> Dict[str, Any]:
    """Extract table and column metadata from Oracle database."""
    table_name = table_name.upper()
    tab_info = table_name.split('.')
    schema_name = None
    if len(tab_info) == 2:
        schema_name = tab_info[0]
        table_name = tab_info[1]

    # Get table comment if requested
    table_comment = None
    if add_comments:
        cmt_query = '''SELECT cmt.comments FROM all_tab_comments cmt
        WHERE cmt.table_name = :table_name AND cmt.owner = COALESCE(:schema_name, cmt.owner)'''
        cursor.execute(cmt_query, {'table_name': table_name, 'schema_name': schema_name})
        row = cursor.fetchone()
        if row and row[0]:
            table_comment = row[0]

    # Query column information
    col_query = '''
        SELECT LOWER(atc.column_name) column_name, atc.data_type, atc.nullable,
            CASE WHEN pkc.position IS NOT NULL THEN 'Y' ELSE 'N' END key_column,
            cc.comments
        FROM all_tab_cols atc
        LEFT JOIN all_constraints pk ON atc.owner = pk.owner
          AND atc.table_name = pk.table_name
          AND pk.constraint_type = 'P'
        LEFT JOIN all_col_comments cc ON atc.owner = cc.owner
          AND atc.table_name = cc.table_name
          AND atc.column_name = cc.column_name
        LEFT JOIN all_cons_columns pkc ON atc.owner = pkc.owner
          AND atc.table_name = pkc.table_name
          AND atc.column_name = pkc.column_name
          AND pk.constraint_name = pkc.constraint_name
        WHERE atc.table_name = :table_name
          AND atc.owner = COALESCE(:schema_name, atc.owner)
          AND atc.virtual_column = 'NO'
        ORDER BY atc.column_id
    '''

    cursor.execute(col_query, {'table_name': table_name, 'schema_name': schema_name})

    columns = {}
    column_comments = {}

    for row in cursor:
        col_name = row[0]
        data_type = row[1]
        is_nullable = row[2]
        is_key = row[3]
        comment = row[4]

        # Store comment if present
        if add_comments and comment:
            column_comments[col_name] = comment

        # Build column config
        col_config = {'field': col_name}

        # Add transform function based on data type
        if data_type == 'DATE':
            col_config['fn'] = 'parse_datetime'  # Oracle DATE includes time
        elif data_type in ('TIMESTAMP', 'TIMESTAMP WITH TIME ZONE', 'TIMESTAMP WITH LOCAL TIME ZONE'):
            col_config['fn'] = 'parse_timestamp'

        # Add primary key or nullable constraint
        if is_key == 'Y':
            col_config['primary_key'] = True
        elif is_nullable == 'N':
            col_config['nullable'] = False

        columns[col_name] = col_config

    return {
        'name': table_name,
        'columns': columns,
        'table_comment': table_comment,
        'column_comments': column_comments
    }


def _get_postgres_metadata(cursor, table_name: str, add_comments: bool = False) -> Dict[str, Any]:
    """Extract table and column metadata from PostgreSQL database."""
    tab_info = table_name.lower().split('.')
    schema = None
    if len(tab_info) == 2:
        schema = tab_info[0]
        table_name = tab_info[1]

    # Get table comment if requested
    table_comment = None
    if add_comments:
        cmt_query = '''
            SELECT obj_description(c.oid) as comments
            FROM pg_class c
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE c.relname = %(table_name)s
              AND n.nspname = COALESCE(%(schema)s, n.nspname)
        '''
        cursor.execute(cmt_query, {'table_name': table_name, 'schema': schema})
        row = cursor.fetchone()
        if row and row[0]:
            table_comment = row[0]

    # Query column information
    col_query = '''
        SELECT
            c.column_name,
            c.data_type,
            c.is_nullable,
            CASE WHEN kcu.column_name IS NOT NULL THEN 'Y' ELSE 'N' END as key_column,
            COALESCE(col_description(pgc.oid, c.ordinal_position), '') as comments
        FROM information_schema.columns c
        LEFT JOIN information_schema.table_constraints tc
            ON c.table_name = tc.table_name
            AND tc.constraint_type = 'PRIMARY KEY'
        LEFT JOIN information_schema.key_column_usage kcu
            ON c.column_name = kcu.column_name
            AND c.table_name = kcu.table_name
            AND tc.constraint_name = kcu.constraint_name
        LEFT JOIN pg_class pgc ON pgc.relname = c.table_name
        WHERE c.table_name = %(table_name)s
          AND c.table_schema = COALESCE(%(schema)s::varchar, c.table_schema)
        ORDER BY c.ordinal_position
    '''

    cursor.execute(col_query, {'table_name': table_name, 'schema': schema})

    columns = {}
    column_comments = {}

    for row in cursor:
        col_name = row[0]
        data_type = row[1]
        is_nullable = row[2]
        is_key = row[3]
        comment = row[4]

        # Store comment if present
        if add_comments and comment:
            column_comments[col_name] = comment

        # Handle special timestamp columns (Rails/Django pattern)
        if col_name.endswith('_at') and data_type in ('timestamp', 'timestamptz', 'timestamp without time zone', 'timestamp with time zone'):
            columns[col_name] = {'db_fn': 'CURRENT_TIMESTAMP'}
            continue

        # Build column config
        col_config = {'field': col_name}

        # Add transform function based on data type
        if data_type == 'date':
            col_config['fn'] = 'parse_date'
        elif data_type in ('timestamp', 'timestamp without time zone'):
            col_config['fn'] = 'parse_datetime'
        elif data_type in ('timestamptz', 'timestamp with time zone'):
            col_config['fn'] = 'parse_timestamp'
        elif data_type in ('time', 'time without time zone', 'time with time zone'):
            col_config['fn'] = 'parse_time'

        # Add primary key or nullable constraint
        if is_key == 'Y':
            col_config['primary_key'] = True
        elif is_nullable == 'NO':
            col_config['nullable'] = False

        columns[col_name] = col_config

    return {
        'name': table_name,
        'columns': columns,
        'table_comment': table_comment,
        'column_comments': column_comments
    }


def _get_mysql_metadata(cursor, table_name: str, add_comments: bool = False) -> Dict[str, Any]:
    """Extract table and column metadata from MySQL database."""
    # Get table comment if requested
    table_comment = None
    if add_comments:
        cmt_query = '''
            SELECT table_comment
            FROM information_schema.tables
            WHERE table_name = %s AND table_schema = DATABASE()
        '''
        cursor.execute(cmt_query, (table_name,))
        row = cursor.fetchone()
        if row and row[0]:
            table_comment = row[0]

    # Query column information
    col_query = '''
        SELECT
            c.column_name,
            c.data_type,
            c.is_nullable,
            CASE WHEN c.column_key = 'PRI' THEN 'Y' ELSE 'N' END as key_column,
            COALESCE(c.column_comment, '') as comments
        FROM information_schema.columns c
        WHERE c.table_name = %s
          AND c.table_schema = DATABASE()
        ORDER BY c.ordinal_position
    '''

    cursor.execute(col_query, (table_name,))

    columns = {}
    column_comments = {}

    for row in cursor:
        col_name = row[0]
        data_type = row[1]
        is_nullable = row[2]
        is_key = row[3]
        comment = row[4]

        # Store comment if present
        if add_comments and comment:
            column_comments[col_name] = comment

        # Handle special timestamp columns (Laravel/Rails pattern)
        if col_name in ('created_at', 'updated_at') and data_type in ('datetime', 'timestamp'):
            columns[col_name] = {'db_fn': 'CURRENT_TIMESTAMP'}
            continue

        # Build column config
        col_config = {'field': col_name}

        # Add transform function based on data type
        if data_type == 'date':
            col_config['fn'] = 'parse_date'
        elif data_type in ('datetime', 'timestamp'):
            col_config['fn'] = 'parse_datetime'
        elif data_type == 'time':
            col_config['fn'] = 'parse_time'

        # Add primary key or nullable constraint
        if is_key == 'Y':
            col_config['primary_key'] = True
        elif is_nullable == 'NO':
            col_config['nullable'] = False

        columns[col_name] = col_config

    return {
        'name': table_name,
        'columns': columns,
        'table_comment': table_comment,
        'column_comments': column_comments
    }


def _get_sqlserver_metadata(cursor, table_name: str, add_comments: bool = False) -> Dict[str, Any]:
    """Extract table and column metadata from SQL Server database."""
    # Get table comment if requested
    table_comment = None
    if add_comments:
        cmt_query = '''
            SELECT ep.value as comments
            FROM sys.tables t
            LEFT JOIN sys.extended_properties ep
                ON ep.major_id = t.object_id
                AND ep.minor_id = 0
                AND ep.name = 'MS_Description'
            WHERE t.name = ?
        '''
        cursor.execute(cmt_query, (table_name,))
        row = cursor.fetchone()
        if row and row[0]:
            table_comment = row[0]

    # Query column information
    col_query = '''
        SELECT
            c.column_name,
            c.data_type,
            c.is_nullable,
            CASE WHEN pk.column_name IS NOT NULL THEN 'Y' ELSE 'N' END as key_column,
            COALESCE(ep.value, '') as comments
        FROM information_schema.columns c
        LEFT JOIN (
            SELECT kcu.column_name, kcu.table_name
            FROM information_schema.table_constraints tc
            JOIN information_schema.key_column_usage kcu
                ON tc.constraint_name = kcu.constraint_name
            WHERE tc.constraint_type = 'PRIMARY KEY'
        ) pk ON c.column_name = pk.column_name AND c.table_name = pk.table_name
        LEFT JOIN sys.tables t ON t.name = c.table_name
        LEFT JOIN sys.columns sc ON sc.object_id = t.object_id AND sc.name = c.column_name
        LEFT JOIN sys.extended_properties ep
            ON ep.major_id = t.object_id
            AND ep.minor_id = sc.column_id
            AND ep.name = 'MS_Description'
        WHERE c.table_name = ?
        ORDER BY c.ordinal_position
    '''

    cursor.execute(col_query, (table_name,))

    columns = {}
    column_comments = {}

    for row in cursor:
        col_name = row[0]
        data_type = row[1]
        is_nullable = row[2]
        is_key = row[3]
        comment = row[4]

        # Store comment if present
        if add_comments and comment:
            column_comments[col_name] = comment

        # Handle special timestamp columns (SQL Server pattern)
        if col_name in ('CreatedDate', 'ModifiedDate') and data_type in ('datetime', 'datetime2'):
            columns[col_name] = {
                'value': 'GETDATE()',
                'db_fn': 'GETDATE()'
            }
            continue

        # Build column config
        col_config = {'field': col_name}

        # Add transform function based on data type
        if data_type == 'date':
            col_config['fn'] = 'parse_date'
        elif data_type in ('datetime', 'datetime2', 'smalldatetime'):
            col_config['fn'] = 'parse_datetime'
        elif data_type == 'datetimeoffset':
            col_config['fn'] = 'parse_timestamp'
        elif data_type == 'time':
            col_config['fn'] = 'parse_time'

        # Add primary key or nullable constraint
        if is_key == 'Y':
            col_config['primary_key'] = True
        elif is_nullable == 'NO':
            col_config['nullable'] = False

        columns[col_name] = col_config

    return {
        'name': table_name,
        'columns': columns,
        'table_comment': table_comment,
        'column_comments': column_comments
    }
