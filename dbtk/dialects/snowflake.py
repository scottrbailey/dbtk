# dbtk/dialects/snowflake.py
import re
from typing import Any, Optional

from .base import DatabaseDialect


class SnowflakeDialect(DatabaseDialect):
    """Snowflake dialect. Uses MERGE (standard ANSI) and session-scoped temp tables."""

    use_upsert = False
    # _merge_source_clause inherited from base — standard ANSI SELECT, correct for Snowflake

    # ------------------------------------------------------------------
    # SQL type mapping
    # ------------------------------------------------------------------

    def sql_type(self, type_obj: Any, internal_size: Optional[int],
                 precision: Optional[int], scale: Optional[int]) -> str:
        type_str = str(type_obj).upper() if type_obj else 'VARCHAR'

        if 'FIXED' in type_str or 'NUMBER' in type_str or 'DECIMAL' in type_str or 'NUMERIC' in type_str:
            if precision and scale is not None:
                return f"NUMBER({precision},{scale})"
            if precision:
                return f"NUMBER({precision})"
            return "NUMBER(38,0)"
        if 'INT' in type_str or 'LONG' in type_str:
            return "NUMBER(38,0)"
        if 'FLOAT' in type_str or 'REAL' in type_str or 'DOUBLE' in type_str:
            return "FLOAT"
        if 'BOOL' in type_str:
            return "BOOLEAN"
        if 'TIMESTAMP_TZ' in type_str:
            return "TIMESTAMP_TZ"
        if 'TIMESTAMP_LTZ' in type_str:
            return "TIMESTAMP_LTZ"
        if 'TIMESTAMP' in type_str or 'DATETIME' in type_str:
            return "TIMESTAMP_NTZ"
        if 'DATE' in type_str:
            return "DATE"
        if 'TIME' in type_str:
            return "TIME"
        if 'BINARY' in type_str or 'BYTES' in type_str:
            return "BINARY"
        if 'TEXT' in type_str or 'CLOB' in type_str:
            return "VARCHAR"
        if 'STRING' in type_str or 'VARCHAR' in type_str or 'CHAR' in type_str:
            if internal_size and internal_size > 0:
                return f"VARCHAR({internal_size})"
            return "VARCHAR"
        return "VARCHAR"

    # ------------------------------------------------------------------
    # Schema introspection
    # ------------------------------------------------------------------

    def table_metadata(self, cursor, table_name: str, add_comments: bool) -> dict:
        parts = table_name.upper().split('.')
        schema = parts[-2] if len(parts) >= 2 else None
        tname = parts[-1]

        col_query = """
            SELECT
                c.column_name,
                c.data_type,
                c.is_nullable,
                CASE WHEN c.column_name IN (
                    SELECT kcu.column_name
                    FROM information_schema.table_constraints tc
                    JOIN information_schema.key_column_usage kcu
                        ON tc.constraint_name = kcu.constraint_name
                        AND tc.table_schema = kcu.table_schema
                        AND tc.table_name = kcu.table_name
                    WHERE tc.constraint_type = 'PRIMARY KEY'
                      AND tc.table_name = %(table_name)s
                      AND tc.table_schema = COALESCE(%(schema)s, CURRENT_SCHEMA())
                ) THEN 'Y' ELSE 'N' END AS key_column,
                COALESCE(c.comment, '') AS comments
            FROM information_schema.columns c
            WHERE c.table_name = %(table_name)s
              AND c.table_schema = COALESCE(%(schema)s, CURRENT_SCHEMA())
            ORDER BY c.ordinal_position
        """
        cursor.execute(col_query, {'table_name': tname, 'schema': schema})

        columns = {}
        column_comments = {}
        for row in cursor:
            col_name, data_type, is_nullable, is_key, comment = row
            col_name = col_name.lower()
            data_type = (data_type or '').upper()

            if add_comments and comment:
                column_comments[col_name] = comment

            col_config = {'field': col_name}
            if 'TIMESTAMP_TZ' in data_type or 'TIMESTAMP_LTZ' in data_type:
                col_config['fn'] = 'timestamp'
            elif 'TIMESTAMP' in data_type or 'DATETIME' in data_type:
                col_config['fn'] = 'datetime'
            elif data_type == 'DATE':
                col_config['fn'] = 'date'
            elif data_type == 'TIME':
                col_config['fn'] = 'time'
            elif data_type in ('FIXED', 'NUMBER', 'INTEGER', 'INT', 'BIGINT', 'SMALLINT', 'TINYINT'):
                col_config['fn'] = 'int'
            elif data_type in ('FLOAT', 'REAL', 'DOUBLE'):
                col_config['fn'] = 'float'
            elif data_type == 'BOOLEAN':
                col_config['fn'] = 'bool'

            if is_key == 'Y':
                col_config['primary_key'] = True
            elif is_nullable == 'NO':
                col_config['nullable'] = False

            columns[col_name] = col_config

        return {
            'name': table_name,
            'columns': columns,
            'table_comment': None,
            'column_comments': column_comments,
        }

    # ------------------------------------------------------------------
    # Temp table (session-scoped, auto-dropped on disconnect)
    # ------------------------------------------------------------------

    def create_temp_table_ddl(self, table_name: str, col_info: list):
        temp_name = f"DBTK_TEMP_{re.sub(r'[^A-Z0-9]+', '_', table_name.upper())}"
        col_defs = ', '.join(
            f"{col_name} {sql_type}" for col_name, _, _, _, _, sql_type in col_info
        )
        create_sql = f"CREATE TEMPORARY TABLE {temp_name} ({col_defs})"
        return temp_name, create_sql

    def cleanup_temp_table_sql(self, temp_name: str) -> str:
        return f"DROP TABLE IF EXISTS {temp_name}"
