# dbtk/dialects/sqlserver.py
import re
from typing import Any, Optional

from .base import DatabaseDialect


class SQLServerDialect(DatabaseDialect):
    """SQL Server dialect. Uses MERGE (no FROM dual) and #temp tables."""

    use_upsert = False
    # _merge_source_clause inherited from base — already correct for SQL Server (no FROM clause)

    # ------------------------------------------------------------------
    # SQL type mapping
    # ------------------------------------------------------------------

    def sql_type(self, type_obj: Any, internal_size: Optional[int],
                 precision: Optional[int], scale: Optional[int]) -> str:
        type_str = str(type_obj).upper() if type_obj else 'VARCHAR'

        if 'STRING' in type_str or 'VARCHAR' in type_str or 'CHAR' in type_str:
            if internal_size and internal_size > 0:
                return f"VARCHAR({internal_size})"
            return "VARCHAR(MAX)"
        if 'INT' in type_str or 'LONG' in type_str:
            return "BIGINT" if precision and precision > 9 else "INT"
        if 'DECIMAL' in type_str or 'NUMERIC' in type_str or 'NUMBER' in type_str:
            if precision and scale is not None:
                return f"DECIMAL({precision},{scale})"
            if precision:
                return f"DECIMAL({precision})"
            return "DECIMAL(18,0)"
        if 'FLOAT' in type_str or 'REAL' in type_str or 'DOUBLE' in type_str:
            return "FLOAT"
        if 'DATE' in type_str or 'TIME' in type_str:
            return "DATETIME" if 'DATETIME' in type_str else "DATE"
        if 'BINARY' in type_str or 'BLOB' in type_str:
            if internal_size and internal_size > 0:
                return f"VARBINARY({internal_size})"
            return "VARBINARY(MAX)"
        if 'TEXT' in type_str or 'CLOB' in type_str:
            return "VARCHAR(MAX)"
        return "VARCHAR(MAX)"

    # ------------------------------------------------------------------
    # Schema introspection
    # ------------------------------------------------------------------

    def table_metadata(self, cursor, table_name: str, add_comments: bool) -> dict:
        database, schema, table_name = self.split_table_name(table_name)

        # Catalog views are per-database; prefix them to read another database's.
        # The name can't be a bind parameter, so bracket-quote it ("]" -> "]]").
        cat = f"[{database.replace(']', ']]')}]." if database else ''

        if schema is None:
            # Resolve an unqualified name the way a query would (the user's default
            # schema, then dbo). Falls back to matching any schema if not found.
            cursor.execute('SELECT OBJECT_SCHEMA_NAME(OBJECT_ID(QUOTENAME(?)))', (table_name,))
            row = cursor.fetchone()
            if row and row[0]:
                schema = row[0]

        table_comment = None
        if add_comments:
            cmt_query = f'''
                SELECT ep.value as comments
                FROM {cat}sys.tables t
                JOIN {cat}sys.schemas s ON s.schema_id = t.schema_id
                LEFT JOIN {cat}sys.extended_properties ep
                    ON ep.major_id = t.object_id
                    AND ep.minor_id = 0
                    AND ep.name = 'MS_Description'
                WHERE t.name = ?
                  AND s.name = COALESCE(?, s.name)
            '''
            cursor.execute(cmt_query, (table_name, schema))
            row = cursor.fetchone()
            if row and row[0]:
                table_comment = row[0]

        # Every join matches on schema as well as name, so same-named tables in
        # different schemas can't borrow each other's primary keys or comments.
        col_query = f'''
            SELECT
                c.column_name,
                c.data_type,
                c.numeric_scale,
                c.is_nullable,
                CASE WHEN pk.column_name IS NOT NULL THEN 'Y' ELSE 'N' END as key_column,
                COALESCE(ep.value, '') as comments
            FROM {cat}information_schema.columns c
            LEFT JOIN (
                SELECT kcu.column_name, kcu.table_name, kcu.table_schema
                FROM {cat}information_schema.table_constraints tc
                JOIN {cat}information_schema.key_column_usage kcu
                    ON tc.constraint_name = kcu.constraint_name
                    AND tc.constraint_schema = kcu.constraint_schema
                WHERE tc.constraint_type = 'PRIMARY KEY'
            ) pk ON c.column_name = pk.column_name
                AND c.table_name = pk.table_name
                AND c.table_schema = pk.table_schema
            LEFT JOIN {cat}sys.schemas s ON s.name = c.table_schema
            LEFT JOIN {cat}sys.tables t
                ON t.name = c.table_name
                AND t.schema_id = s.schema_id
            LEFT JOIN {cat}sys.columns sc ON sc.object_id = t.object_id AND sc.name = c.column_name
            LEFT JOIN {cat}sys.extended_properties ep
                ON ep.major_id = t.object_id
                AND ep.minor_id = sc.column_id
                AND ep.name = 'MS_Description'
            WHERE c.table_name = ?
              AND c.table_schema = COALESCE(?, c.table_schema)
            ORDER BY c.table_schema, c.ordinal_position
        '''
        cursor.execute(col_query, (table_name, schema))

        columns = {}
        column_comments = {}
        for row in cursor:
            col_name, data_type, numeric_scale, is_nullable, is_key, comment = row

            if add_comments and comment:
                column_comments[col_name] = comment

            if col_name in ('CreatedDate', 'ModifiedDate') and data_type in ('datetime', 'datetime2'):
                columns[col_name] = {'db_expr': 'GETDATE()'}
                continue

            col_config = {'field': col_name}
            if data_type == 'date':
                col_config['fn'] = 'date'
            elif data_type in ('datetime', 'datetime2', 'smalldatetime'):
                col_config['fn'] = 'datetime'
            elif data_type == 'datetimeoffset':
                col_config['fn'] = 'timestamp'
            elif data_type == 'time':
                col_config['fn'] = 'time'
            elif data_type in ('int', 'bigint', 'smallint', 'tinyint'):
                col_config['fn'] = 'int'
            elif data_type in ('decimal', 'numeric'):
                col_config['fn'] = 'int' if numeric_scale == 0 else 'float'
            elif data_type in ('float', 'real', 'money', 'smallmoney'):
                col_config['fn'] = 'float'
            elif data_type == 'bit':
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

    # ------------------------------------------------------------------
    # Temp table (#temp, dropped after MERGE)
    # ------------------------------------------------------------------

    def create_temp_table_ddl(self, table_name: str, col_info: list):
        temp_name = f"#{re.sub(r'[^A-Z0-9]+', '_', table_name.upper())}"
        col_defs = ', '.join(
            f"[{col_name}] {sql_type} NULL" for col_name, _, _, _, _, sql_type in col_info
        )
        create_sql = f"CREATE TABLE {temp_name} ({col_defs})"
        return temp_name, create_sql

    def cleanup_temp_table_sql(self, temp_name: str) -> str:
        return f"DROP TABLE {temp_name}"
