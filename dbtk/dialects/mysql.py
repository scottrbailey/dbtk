# dbtk/dialects/mysql.py
from textwrap import dedent
from typing import List, Tuple

from .base import DatabaseDialect
from ..utils import wrap_at_comma


class MySQLDialect(DatabaseDialect):
    """MySQL/MariaDB dialect. Uses INSERT … ON DUPLICATE KEY UPDATE."""

    def upsert_sql(self, table_name: str, cols_str: str, placeholders_str: str,
                   key_cols: List[str], update_cols: List[Tuple]) -> str:
        # MySQL/MariaDB: VALUES(col) syntax works on both MySQL and MariaDB
        update_assignments = []
        for col, ident, bind_name, db_expr in update_cols:
            if db_expr and '#' in db_expr:
                assignment = f"{ident} = {db_expr.replace('#', f'VALUES({ident})')}"
            elif db_expr:
                assignment = f"{ident} = {db_expr}"
            else:
                assignment = f"{ident} = VALUES({ident})"
            update_assignments.append(assignment)

        update_clause = ', '.join(update_assignments)
        if len(update_assignments) > 4:
            update_clause = wrap_at_comma(update_clause)

        return dedent(f"""\
        INSERT INTO {table_name} ({cols_str})
        VALUES ({placeholders_str})
        ON DUPLICATE KEY UPDATE {update_clause}""")

    def table_metadata(self, cursor, table_name: str, add_comments: bool) -> dict:
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
            col_name, data_type, is_nullable, is_key, comment = row

            if add_comments and comment:
                column_comments[col_name] = comment

            if col_name in ('created_at', 'updated_at') and data_type in ('datetime', 'timestamp'):
                columns[col_name] = {'db_fn': 'CURRENT_TIMESTAMP'}
                continue

            col_config = {'field': col_name}
            if data_type == 'date':
                col_config['fn'] = 'parse_date'
            elif data_type in ('datetime', 'timestamp'):
                col_config['fn'] = 'parse_datetime'
            elif data_type == 'time':
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
