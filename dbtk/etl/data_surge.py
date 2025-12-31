# dbtk/etl/data_surge.py

import logging
import re
import time
from typing import Iterable, Optional

from .base_surge import BaseSurge
from ..utils import batch_iterable
from ..record import Record

logger = logging.getLogger(__name__)


class DataSurge(BaseSurge):
    """
    Handles bulk ETL operations by delegating to a stateful Table instance.

    Note: The Table instance's state (self.values) is modified during processing.
    Ensure the Table is not used concurrently by other operations or threads.

    Example
    -------
    ::

        table = Table(..., cursor=cursor)
        surge = DataSurge(table, batch_size=1000, use_transaction=True))
        errors = surge.insert(records, raise_error=False)
    """

    def __init__(self, table, batch_size: Optional[int] = None, use_transaction: bool = False):
        """
        Initialize DataSurge for bulk operations.

        Args:
            table: Table instance with schema metadata
            batch_size: Number of records per batch
            use_transaction: Use transaction for all operations (default: False)
        """
        super().__init__(table, batch_size=batch_size)
        self.use_transaction = use_transaction
        self.skips = 0
        # Swap to positional parameters if named to save memory in bind parameters
        self.table.force_positional()
        self._sql_statements = {}  # Only for modified SQL (merge temp table hack)

    def get_sql(self, operation: str) -> str:
        """Get SQL for operation, checking local modifications first."""
        if operation in self._sql_statements:
            return self._sql_statements[operation]
        return self.table.get_sql(operation)

    def insert(self, records: Iterable[Record], raise_error: bool = True) -> int:
        """Perform bulk INSERT on records."""
        return self.load(records, operation="insert", raise_error=raise_error)

    def update(self, records: Iterable[Record], raise_error: bool = True) -> int:
        """Perform bulk UPDATE on records."""
        return self.load(records, operation="update", raise_error=raise_error)

    def delete(self, records: Iterable[Record], raise_error: bool = True) -> int:
        """Perform bulk DELETE on records."""
        return self.load(records, operation="delete", raise_error=raise_error)

    def merge(self, records: Iterable[Record], raise_error: bool = True) -> int:
        """
        Perform bulk MERGE using either direct upsert or temporary table strategy.
        """
        use_upsert = self.table._should_use_upsert()

        if use_upsert:
            return self.load(records, operation="merge", raise_error=raise_error)
        else:
            return self._merge_with_temp_table(records, raise_error)

    def _execute_batches(self, records, operation, sql, raise_error):
        """Execute batches with executemany."""
        errors = 0
        skipped = 0

        for batch in batch_iterable(records, self.batch_size):
            batch_params = []
            for record in batch:
                params = self._transform_row(record)
                if params is None:
                    skipped += 1
                    continue
                batch_params.append(params)

            if batch_params:
                try:
                    self.cursor.executemany(sql, batch_params)
                    self.table.counts[operation] += len(batch_params)
                except self.cursor.connection.driver.DatabaseError as e:
                    logger.error(f"{operation.capitalize()} batch failed for {self.table.name}: {str(e)}")
                    if raise_error:
                        raise
                    errors += len(batch_params)

        return errors, skipped

    def load(
        self,
        records: Iterable[Record],
        operation: Optional[str] = None,
        raise_error: bool = True,
    ) -> int:
        """
        Core bulk execution using executemany() — shared path for insert/update/delete/merge.
        """
        operation = (operation or self.operation).lower()
        if operation not in ("insert", "update", "delete", "merge"):
            raise ValueError(f"Invalid operation: {operation}")
        self.operation = operation
        sql = self.get_sql(operation)

        if self.use_transaction:
            with self.cursor.connection.transaction():
                errors, skipped = self._execute_batches(records, operation, sql, raise_error)
        else:
            errors, skipped = self._execute_batches(records, operation, sql, raise_error)

        logger.info(
            f"Batched `{self.table.name}` <{operation}s: {self.table.counts[operation]:,}; errors: {errors:,}; skips: {skipped:,}>")
        self.skips += skipped
        return errors

    def _merge_with_temp_table(self, records: Iterable[Record], raise_error: bool) -> int:
        """Perform bulk merge using temporary table (for databases requiring true MERGE)."""
        records_list = list(records)
        if not records_list:
            return 0

        db_type = self.cursor.connection.database_type
        if db_type == 'postgres' and self.cursor.connection.server_version < 150000:
            raise NotImplementedError(
                f"PostgreSQL MERGE requires version >= 15, found {self.cursor.connection.server_version}"
            )

        temp_name = f"tmp_{self.table.name}_{int(time.time())}"
        if db_type == 'sqlserver':
            temp_name = f"#{temp_name}"

        create_sql = f"CREATE TEMPORARY TABLE {temp_name} AS SELECT * FROM {self.table.name} WHERE 1=0"
        try:
            self.cursor.execute(create_sql)
        except self.cursor.connection.driver.DatabaseError as e:
            logger.error(f"Failed to create temp table {temp_name}: {e}")
            if raise_error:
                raise
            return len(records_list)

        # Use temporary table for bulk insert
        from .table import Table
        temp_table = Table(
            name=temp_name,
            columns=self.table.columns,
            cursor=self.cursor
        )
        temp_surge = DataSurge(temp_table, batch_size=self.batch_size)
        errors = temp_surge.insert(records_list, raise_error=raise_error)

        if errors:
            self.cursor.execute(f"DROP TABLE IF EXISTS {temp_name}")
            return errors

        # Generate MERGE SQL if not already done
        self.table.generate_sql('merge')
        merge_sql = self.table.sql_statements['merge']

        # Replace the USING clause to point to temp table and store modified version
        modified_merge = re.sub(
            r'USING\s*\(.*?\)\s*s',
            f'USING {temp_name} s',
            merge_sql,
            flags=re.DOTALL
        )
        self._sql_statements['merge'] = modified_merge

        try:
            if self.use_transaction:
                with self.cursor.connection.transaction():
                    self.cursor.execute(self.get_sql('merge'))
            else:
                self.cursor.execute(self.get_sql('merge'))

            loaded = len(records_list) - errors
            self.table.counts['merge'] += loaded
            logger.info(f"MERGE via temp table → {loaded:,} records into {self.table.name}")
        except self.cursor.connection.driver.DatabaseError as e:
            logger.error(f"Merge failed: {e}")
            if raise_error:
                raise
            errors += len(records_list) - errors
        finally:
            try:
                self.cursor.execute(f"DROP TABLE IF EXISTS {temp_name}")
            except Exception as e:
                logger.warning(f"Failed to drop temp table {temp_name}: {e}")

        return errors