# dbtk/dialects/oracle.py
import re
from typing import Any, Optional

from .base import DatabaseDialect


class OracleDialect(DatabaseDialect):
    """Oracle dialect. Uses MERGE with FROM dual and SQL*Loader-compatible temp tables."""

    use_upsert = False
    temp_table_all_cols = True
    temp_table_cleanup_commit = True  # TRUNCATE on GTT needs an explicit commit

    def _merge_source_clause(self, source_cols: str) -> str:
        return f"SELECT {source_cols} FROM dual"

    # ------------------------------------------------------------------
    # SQL type mapping
    # ------------------------------------------------------------------

    def sql_type(self, type_obj: Any, internal_size: Optional[int],
                 precision: Optional[int], scale: Optional[int]) -> str:
        if hasattr(type_obj, 'name'):
            name = type_obj.name
            if 'VARCHAR' in name:
                return f"VARCHAR2({internal_size})" if internal_size else "VARCHAR2(4000)"
            if 'CHAR' in name and 'VARCHAR' not in name:
                return f"CHAR({internal_size})" if internal_size else "CHAR(1)"
            if 'NUMBER' in name:
                if precision and scale:
                    return f"NUMBER({precision},{scale})"
                if precision:
                    return f"NUMBER({precision})"
                return "NUMBER"
            if 'DATE' in name:
                return "DATE"
            if 'TIMESTAMP' in name:
                return "TIMESTAMP"
            if 'CLOB' in name:
                return "CLOB"
            if 'BLOB' in name:
                return "BLOB"
        return "VARCHAR2(4000)"

    # ------------------------------------------------------------------
    # Schema introspection
    # ------------------------------------------------------------------

    def table_metadata(self, cursor, table_name: str, add_comments: bool) -> dict:
        table_name = table_name.upper()
        tab_info = table_name.split('.')
        schema_name = None
        if len(tab_info) == 2:
            schema_name = tab_info[0]
            table_name = tab_info[1]

        table_comment = None
        if add_comments:
            cmt_query = '''SELECT cmt.comments FROM all_tab_comments cmt
            WHERE cmt.table_name = :table_name AND cmt.owner = COALESCE(:schema_name, cmt.owner)'''
            cursor.execute(cmt_query, {'table_name': table_name, 'schema_name': schema_name})
            row = cursor.fetchone()
            if row and row[0]:
                table_comment = row[0]

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
            col_name, data_type, is_nullable, is_key, comment = row

            if add_comments and comment:
                column_comments[col_name] = comment

            col_config = {'field': col_name}
            if data_type == 'DATE':
                col_config['fn'] = 'datetime'  # Oracle DATE includes time
            elif data_type in ('TIMESTAMP', 'TIMESTAMP WITH TIME ZONE', 'TIMESTAMP WITH LOCAL TIME ZONE'):
                col_config['fn'] = 'timestamp'

            if is_key == 'Y':
                col_config['primary_key'] = True
            elif is_nullable == 'N':
                col_config['nullable'] = False

            columns[col_name] = col_config

        return {
            'name': table_name,
            'columns': columns,
            'table_comment': table_comment,
            'column_comments': column_comments,
        }

    # ------------------------------------------------------------------
    # Temp table (GLOBAL TEMPORARY TABLE … ON COMMIT PRESERVE ROWS)
    # ------------------------------------------------------------------

    def create_temp_table_ddl(self, table_name: str, col_info: list):
        temp_name = re.sub(r'[^A-Z0-9]+', '_', f"GTT_{table_name.upper()}")
        col_defs = ', '.join(
            f"{col_name} {sql_type}" for col_name, _, _, _, _, sql_type in col_info
        )
        create_sql = (
            f"CREATE GLOBAL TEMPORARY TABLE {temp_name} ({col_defs}) ON COMMIT PRESERVE ROWS"
        )
        return temp_name, create_sql

    def cleanup_temp_table_sql(self, temp_name: str) -> str:
        return f"TRUNCATE TABLE {temp_name}"
