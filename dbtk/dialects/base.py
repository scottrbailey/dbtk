# dbtk/dialects/base.py
from textwrap import dedent
from typing import List, Tuple, Any, Optional

from ..utils import wrap_at_comma


class DatabaseDialect:
    """
    Base database dialect. Default implementations match PostgreSQL/SQLite behavior.

    Subclass and override to support a different database. Adding support for a new
    engine means writing one class — no grep-and-branch across the ETL layer.
    """

    use_upsert = True           # False for MERGE-based dialects (Oracle, SQL Server)
    temp_table_all_cols = False  # True when temp table must mirror all DB columns (Oracle)
    temp_table_cleanup_commit = False  # True when cleanup DDL needs an explicit commit (Oracle)

    # ------------------------------------------------------------------
    # Upsert (INSERT … ON CONFLICT / ON DUPLICATE KEY)
    # ------------------------------------------------------------------

    def upsert_sql(self, table_name: str, cols_str: str, placeholders_str: str,
                   key_cols: List[str], update_cols: List[Tuple]) -> str:
        """INSERT … ON CONFLICT DO UPDATE SET — Postgres/SQLite default."""
        conflict_cols = ', '.join(key_cols)
        update_assignments = []
        for col, ident, bind_name, db_expr in update_cols:
            if db_expr and '#' in db_expr:
                assignment = f"{ident} = {db_expr.replace('#', f'EXCLUDED.{ident}')}"
            elif db_expr:
                assignment = f"{ident} = {db_expr}"
            else:
                assignment = f"{ident} = EXCLUDED.{ident}"
            update_assignments.append(assignment)
        update_clause = ', '.join(update_assignments)
        if len(update_assignments) > 4:
            update_clause = wrap_at_comma(update_clause)
        return dedent(f"""\
        INSERT INTO {table_name} ({cols_str})
        VALUES ({placeholders_str})
        ON CONFLICT ({conflict_cols}) DO UPDATE SET {update_clause}""")

    # ------------------------------------------------------------------
    # Merge (MERGE … USING … ON … WHEN MATCHED / NOT MATCHED)
    # ------------------------------------------------------------------

    def _merge_source_clause(self, source_cols: str) -> str:
        """Return the USING subquery string. Override to add a FROM clause (e.g. Oracle's FROM dual)."""
        return f"SELECT {source_cols}"

    def merge_sql(self, table_name: str, all_cols: List[Tuple],
                  key_conditions: List[str], update_cols: List[Tuple]) -> str:
        """Full MERGE statement. SQL Server-style by default (no FROM in USING subquery)."""
        source_items = [f"{placeholder} AS {ident}" for _, ident, placeholder in all_cols]
        source_cols = ', '.join(source_items)
        if len(all_cols) > 4:
            source_cols = wrap_at_comma(source_cols)

        source_clause = self._merge_source_clause(source_cols)

        update_assignments = [f"t.{ident} = s.{ident}" for _, ident in update_cols]
        update_set = ', '.join(update_assignments)
        if len(update_assignments) > 4:
            update_set = wrap_at_comma(update_set)

        insert_cols = ', '.join(ident for _, ident, _ in all_cols)
        insert_values = ', '.join(f"s.{ident}" for _, ident, _ in all_cols)
        if len(all_cols) > 4:
            insert_cols = wrap_at_comma(insert_cols)
            insert_values = wrap_at_comma(insert_values)

        return dedent(f"""\
        MERGE INTO {table_name} t
        USING ({source_clause}) s
        ON ({' AND '.join(key_conditions)})
        WHEN MATCHED THEN
            UPDATE SET {update_set}
        WHEN NOT MATCHED THEN
            INSERT ({insert_cols})
            VALUES ({insert_values})""")

    # ------------------------------------------------------------------
    # SQL type mapping (used when generating temp table DDL)
    # ------------------------------------------------------------------

    def sql_type(self, type_obj: Any, internal_size: Optional[int],
                 precision: Optional[int], scale: Optional[int]) -> str:
        """Map a driver type descriptor to a SQL type string. Generic fallback."""
        return "VARCHAR(255)"

    # ------------------------------------------------------------------
    # Schema introspection
    # ------------------------------------------------------------------

    def table_metadata(self, cursor, table_name: str, add_comments: bool) -> dict:
        """Extract column definitions from the database catalog."""
        raise NotImplementedError(f"{type(self).__name__} does not support table_metadata")

    # ------------------------------------------------------------------
    # Temp table hooks (MERGE-based dialects only: Oracle, SQL Server)
    # ------------------------------------------------------------------

    def create_temp_table_ddl(self, table_name: str, col_info: list) -> Tuple[str, str]:
        """Return (temp_table_name, CREATE TABLE sql) for the given column info list."""
        raise NotImplementedError(f"{type(self).__name__} does not use temp-table MERGE")

    def cleanup_temp_table_sql(self, temp_name: str) -> str:
        """Return SQL to clean up the temp table after a MERGE (TRUNCATE or DROP)."""
        raise NotImplementedError(f"{type(self).__name__} does not use temp-table MERGE")
