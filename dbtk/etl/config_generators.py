# dbtk/etl/config_generators.py


def generate_table_config(cursor, table_name: str, add_comments: bool = False) -> str:
    """
    Generate Table configuration based on database table structure.

    Args:
        cursor: Database cursor
        table_name: Name of table to analyze
        add_comments: Include table/column comments if available

    Returns:
        String containing Table() configuration code

    Example:
        config = generate_table_config(cursor, 'users')
        print(config)  # Prints Table(...) configuration
    """
    db_type = cursor.connection.server_type

    if db_type == 'oracle':
        return _generate_oracle_config(cursor, table_name, add_comments)
    elif db_type == 'postgres':
        return _generate_postgres_config(cursor, table_name, add_comments)
    elif db_type == 'mysql':
        return _generate_mysql_config(cursor, table_name, add_comments)
    elif db_type in ('sqlserver', 'mssql'):
        return _generate_sqlserver_config(cursor, table_name, add_comments)
    else:
        raise ValueError(f"Table config generation not supported for database type: {db_type}")


def _generate_oracle_config(cursor, table_name: str, add_comments: bool = False) -> str:
    """Generate Oracle table configuration."""
    table_name = table_name.upper()
    tab_info = table_name.split('.')
    schema_name = None
    if len(tab_info) == 2:
        schema_name = tab_info[0]
        table_name = tab_info[1]
    table_config = ''


    if add_comments:
        cmt_query = '''SELECT cmt.comments FROM all_tab_comments cmt 
        WHERE cmt.table_name = :table_name AND cmt.owner = COALESCE(:schema_name, cmt.owner)'''
        cursor.execute(cmt_query, {'table_name': table_name, 'schema_name': schema_name})
        row = cursor.fetchone()
        if row and row.comments:
            table_config = f'# {row.comments}\n'

    table_config += f"Table('{table_name}', columns={{\n"

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

    for row in cursor:
        if add_comments and row.comments:
            table_config += f'    # {row.comments}\n'

        # Determine function based on data type
        col_atts = ''
        if row.data_type == 'DATE':
            col_atts = ", 'fn': parse_datetime"  # Oracle DATE includes time
        elif row.data_type in ('TIMESTAMP', 'TIMESTAMP WITH TIME ZONE', 'TIMESTAMP WITH LOCAL TIME ZONE'):
            col_atts = ", 'fn': parse_timestamp"
        if row['key_column'] == 'Y':
            col_atts += ", 'primary_key': True"
        elif row['nullable'] == 'N':
            col_atts += ", 'nullable': False"

        table_config += f"    '{row.column_name}': {{'field': '{row.column_name}'{col_atts}}},\n"


    table_config += '    },\n)'
    return table_config


def _generate_postgres_config(cursor, table_name: str, add_comments: bool = False) -> str:
    """Generate PostgreSQL table configuration."""
    tab_info = table_name.lower().split('.')
    schema = None
    if len(tab_info) == 2:
        schema = tab_info[0]
        table_name = tab_info[1]

    table_config = ''

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
            table_config = f'# {row[0]}\n'

    table_config += f"Table('{table_name}', columns={{\n"

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
    req_fields = []
    key_fields = []

    for row in cursor:
        if add_comments and row[4]:
            table_config += f'    # {row[4]}\n'

        # Determine function based on data type
        fx = ''
        if row[1] == 'date':
            fx = ", 'fn': parse_date"
        elif row[1] in ('timestamp', 'timestamp without time zone'):
            fx = ", 'fn': parse_datetime"
        elif row[1] in ('timestamptz', 'timestamp with time zone'):
            fx = ", 'fn': parse_timestamp"
        elif row[1] in ('time', 'time without time zone', 'time with time zone'):
            fx = ", 'fn': parse_time"
        elif row[0].endswith('_at') and row[1] in ('timestamp', 'timestamptz'):
            # Common Rails/Django pattern for timestamps
            table_config += f"    '{row[0]}': {{'value': 'CURRENT_TIMESTAMP', 'db_fn': 'CURRENT_TIMESTAMP'}},\n"
            continue

        table_config += f"    '{row[0]}': {{'field': '{row[0]}'{fx}}},\n"

        if row[2] == 'NO':
            req_fields.append(row[0])
        if row[3] == 'Y':
            key_fields.append(row[0])

    table_config += '    },\n'

    if req_fields:
        fields = ', '.join([f"'{c}'" for c in req_fields])
        table_config += f"    req_fields=({fields}),\n"
    if key_fields:
        fields = ', '.join([f"'{c}'" for c in key_fields])
        table_config += f"    key_fields=({fields})\n"
    table_config += ')'
    return table_config


def _generate_mysql_config(cursor, table_name: str, add_comments: bool = False) -> str:
    """Generate MySQL table configuration."""
    table_config = ''

    if add_comments:
        cmt_query = '''
            SELECT table_comment 
            FROM information_schema.tables 
            WHERE table_name = %s AND table_schema = DATABASE()
        '''
        cursor.execute(cmt_query, (table_name,))
        row = cursor.fetchone()
        if row and row[0]:
            table_config = f'# {row[0]}\n'

    table_config += f"Table('{table_name}', columns={{\n"

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
    req_fields = []
    key_fields = []

    for row in cursor:
        if add_comments and row[4]:
            table_config += f'    # {row[4]}\n'

        # Determine function based on data type
        fx = ''
        if row[1] == 'date':
            fx = ", 'fn': parse_date"
        elif row[1] in ('datetime', 'timestamp'):
            fx = ", 'fn': parse_datetime"
        elif row[1] == 'time':
            fx = ", 'fn': parse_time"
        elif row[0] in ('created_at', 'updated_at') and row[1] in ('datetime', 'timestamp'):
            # Common Laravel/Rails pattern
            default_val = 'CURRENT_TIMESTAMP' if row[
                                                     0] == 'created_at' else 'CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP'
            table_config += f"    '{row[0]}': {{'value': '{default_val}', 'db_fn': '{default_val}'}},\n"
            continue

        table_config += f"    '{row[0]}': {{'field': '{row[0]}'{fx}}},\n"

        if row[2] == 'NO':
            req_fields.append(row[0])
        if row[3] == 'Y':
            key_fields.append(row[0])

    table_config += '    },\n'

    if req_fields:
        fields = ', '.join([f"'{c}'" for c in req_fields])
        table_config += f"    req_fields=({fields}),\n"
    if key_fields:
        fields = ', '.join([f"'{c}'" for c in key_fields])
        table_config += f"    key_fields=({fields})\n"
    table_config += ')'
    return table_config


def _generate_sqlserver_config(cursor, table_name: str, add_comments: bool = False) -> str:
    """Generate SQL Server table configuration."""
    table_config = ''

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
            table_config = f'# {row[0]}\n'

    table_config += f"Table('{table_name}', columns={{\n"

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
    req_fields = []
    key_fields = []

    for row in cursor:
        if add_comments and row[4]:
            table_config += f'    # {row[4]}\n'

        # Determine function based on data type
        fx = ''
        if row[1] == 'date':
            fx = ", 'fn': parse_date"
        elif row[1] in ('datetime', 'datetime2', 'smalldatetime'):
            fx = ", 'fn': parse_datetime"
        elif row[1] in ('datetimeoffset'):
            fx = ", 'fn': parse_timestamp"
        elif row[1] == 'time':
            fx = ", 'fn': parse_time"
        elif row[0] in ('CreatedDate', 'ModifiedDate') and row[1] in ('datetime', 'datetime2'):
            # Common SQL Server pattern
            table_config += f"    '{row[0]}': {{'value': 'GETDATE()', 'db_fn': 'GETDATE()'}},\n"
            continue

        table_config += f"    '{row[0]}': {{'field': '{row[0]}'{fx}}},\n"

        if row[2] == 'NO':
            req_fields.append(row[0])
        if row[3] == 'Y':
            key_fields.append(row[0])

    table_config += '    },\n'

    if req_fields:
        fields = ', '.join([f"'{c}'" for c in req_fields])
        table_config += f"    req_fields=({fields}),\n"
    if key_fields:
        fields = ', '.join([f"'{c}'" for c in key_fields])
        table_config += f"    key_fields=({fields})\n"
    table_config += ')'
    return table_config
