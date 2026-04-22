# dbtk/dialects/postgres.py
from .base import DatabaseDialect


class PostgresDialect(DatabaseDialect):
    """PostgreSQL dialect. Inherits ON CONFLICT upsert and MERGE template from base."""

    def table_metadata(self, cursor, table_name: str, add_comments: bool) -> dict:
        tab_info = table_name.lower().split('.')
        schema = None
        if len(tab_info) == 2:
            schema = tab_info[0]
            table_name = tab_info[1]

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
            col_name, data_type, is_nullable, is_key, comment = row

            if add_comments and comment:
                column_comments[col_name] = comment

            if col_name.endswith('_at') and data_type in (
                'timestamp', 'timestamptz',
                'timestamp without time zone', 'timestamp with time zone'
            ):
                columns[col_name] = {'db_fn': 'CURRENT_TIMESTAMP'}
                continue

            col_config = {'field': col_name}
            if data_type == 'date':
                col_config['fn'] = 'parse_date'
            elif data_type in ('timestamp', 'timestamp without time zone'):
                col_config['fn'] = 'parse_datetime'
            elif data_type in ('timestamptz', 'timestamp with time zone'):
                col_config['fn'] = 'parse_timestamp'
            elif data_type in ('time', 'time without time zone', 'time with time zone'):
                col_config['fn'] = 'parse_time'

            if is_key == 'Y':
                col_config['primary_key'] = True
            elif is_nullable == 'NO':
                col_config['nullable'] = False

            columns[col_name] = col_config

        return {
            'name': table_name,
            'columns': columns,
            'table_comment': table_comment,
            'column_comments': column_comments,
        }
