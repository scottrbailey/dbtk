# dbtk/dialects/postgres.py
from .base import DatabaseDialect


class PostgresDialect(DatabaseDialect):
    """PostgreSQL dialect. Inherits ON CONFLICT upsert and MERGE template from base."""

    def table_metadata(self, cursor, table_name: str, add_comments: bool) -> dict:
        database, schema, table_name = self.split_table_name(table_name.lower())

        if database is not None:
            # PostgreSQL can't read another database's catalog over this connection.
            cursor.execute('SELECT current_database()')
            current = cursor.fetchone()[0]
            if database != current.lower():
                raise ValueError(
                    f"Can't read table metadata for {database}.{schema}.{table_name} while "
                    f"connected to database {current!r}; PostgreSQL has no cross-database "
                    "catalog access. Connect to that database instead."
                )

        if schema is None:
            # Resolve an unqualified name the way a query would: through search_path.
            # Falls back to matching any schema if the table isn't on the search path.
            cursor.execute('''
                SELECT n.nspname
                FROM pg_class c
                JOIN pg_namespace n ON n.oid = c.relnamespace
                WHERE c.oid = to_regclass(quote_ident(%(table_name)s))
            ''', {'table_name': table_name})
            row = cursor.fetchone()
            if row:
                schema = row[0]

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

        # Every join matches on schema as well as name, so same-named tables in
        # different schemas can't borrow each other's primary keys or comments.
        col_query = '''
            SELECT
                c.column_name,
                c.data_type,
                c.numeric_scale,
                c.is_nullable,
                CASE WHEN pk.column_name IS NOT NULL THEN 'Y' ELSE 'N' END as key_column,
                COALESCE(col_description(pgc.oid, c.ordinal_position), '') as comments
            FROM information_schema.columns c
            LEFT JOIN (
                SELECT kcu.table_schema, kcu.table_name, kcu.column_name
                FROM information_schema.table_constraints tc
                JOIN information_schema.key_column_usage kcu
                    ON kcu.constraint_schema = tc.constraint_schema
                    AND kcu.constraint_name = tc.constraint_name
                    AND kcu.table_name = tc.table_name
                WHERE tc.constraint_type = 'PRIMARY KEY'
            ) pk ON pk.table_schema = c.table_schema
                AND pk.table_name = c.table_name
                AND pk.column_name = c.column_name
            LEFT JOIN pg_namespace pgn ON pgn.nspname = c.table_schema
            LEFT JOIN pg_class pgc
                ON pgc.relname = c.table_name
                AND pgc.relnamespace = pgn.oid
            WHERE c.table_name = %(table_name)s
              AND c.table_schema = COALESCE(%(schema)s::varchar, c.table_schema)
            ORDER BY c.table_schema, c.ordinal_position
        '''
        cursor.execute(col_query, {'table_name': table_name, 'schema': schema})

        columns = {}
        column_comments = {}
        for row in cursor:
            col_name, data_type, numeric_scale, is_nullable, is_key, comment = row

            if add_comments and comment:
                column_comments[col_name] = comment

            if col_name.endswith('_at') and data_type in (
                'timestamp', 'timestamptz',
                'timestamp without time zone', 'timestamp with time zone'
            ):
                columns[col_name] = {'db_expr': 'CURRENT_TIMESTAMP'}
                continue

            col_config = {'field': col_name}
            if data_type == 'date':
                col_config['fn'] = 'date'
            elif data_type in ('timestamp', 'timestamp without time zone'):
                col_config['fn'] = 'datetime'
            elif data_type in ('timestamptz', 'timestamp with time zone'):
                col_config['fn'] = 'timestamp'
            elif data_type in ('time', 'time without time zone', 'time with time zone'):
                col_config['fn'] = 'time'
            elif data_type in ('smallint', 'integer', 'bigint'):
                col_config['fn'] = 'int'
            elif data_type in ('numeric', 'decimal'):
                col_config['fn'] = 'int' if numeric_scale == 0 else 'float'
            elif data_type in ('real', 'double precision'):
                col_config['fn'] = 'float'
            elif data_type == 'boolean':
                col_config['fn'] = 'bool'

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
