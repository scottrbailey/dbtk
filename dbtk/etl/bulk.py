# dbtk/etl/bulk.py

"""
Bulk ETL operations for high-performance data loading.

Provides the DataSurge class for executing batch INSERT, UPDATE, DELETE,
and MERGE operations with automatic transaction management and batch sizing.
"""

import logging
import re
import time
from .table import Table

from typing import Iterable, Optional

try:
    from typing import Mapping
except ImportError:
    from collections.abc import Mapping

from ..utils import batch_iterable, RecordLike

logger = logging.getLogger(__name__)


class DataSurge:
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

    def __init__(self, table: Table, batch_size: Optional[int] = None, use_transaction: bool = False):
        """
            Initialize DataSurge for bulk operations.

            Args:
                table: Table instance with schema metadata
                batch_size: Number of records per batch
                use_transaction: Use transaction for all operations (default: False)
            """
        self.table = table
        self.cursor = table.cursor
        if batch_size is not None:
            self.cursor.batch_size = batch_size
            self.batch_size = batch_size
        else:
            self.batch_size = table.cursor.batch_size
        if self.batch_size < 500:
            logger.warning(f"Batch size for {self.table.name} is {self.batch_size}. This may be needed for wide tables or large records.")
        self.use_transaction = use_transaction
        self.skips = 0
        self._sql_statements = {}  # Only for modified SQL

    def get_sql(self, operation: str) -> str:
        """Get SQL for operation, checking local modifications first."""
        if operation in self._sql_statements:
            return self._sql_statements[operation]
        return self.table.get_sql(operation)

    def insert(self, records: Iterable[RecordLike],
               raise_error: bool = True) -> int:
        """Perform bulk INSERT on records."""
        return self._process_records(records, 'insert', raise_error)

    def update(self, records: Iterable[RecordLike],
               raise_error: bool = True) -> int:
        """Perform bulk UPDATE on records."""
        return self._process_records(records, 'update', raise_error)

    def delete(self, records: Iterable[RecordLike],
               raise_error: bool = True) -> int:
        """Perform bulk DELETE on records."""
        return self._process_records(records, 'delete', raise_error)

    def merge(self, records: Iterable[RecordLike],
              raise_error: bool = True) -> int:
        """
        Perform bulk MERGE using either direct upsert or temporary table strategy.
        """
        use_upsert = self.table._should_use_upsert()

        if use_upsert:
            return self._process_records(records, 'merge', raise_error)
        else:
            return self._merge_with_temp_table(records, raise_error)

    def _process_records(self, records: Iterable[RecordLike], operation: str,
                         raise_error: bool) -> int:
        """
        Common processing logic for insert, update, delete, and upsert operations.
        Validates records, batches them, and executes with executemany().
        """
        sql = self.get_sql(operation)

        errors = 0
        skipped = 0

        def process_batches():
            nonlocal errors, skipped
            for batch in batch_iterable(records, self.batch_size):
                batch_params = []
                for record in batch:
                    self.table.set_values(record)
                    # Validate: delete needs keys, all others need full requirements
                    if ((operation == 'delete' and not self.table.has_all_keys) or
                            (operation != 'delete' and not self.table.reqs_met)):
                        skipped += 1
                        if operation == 'delete':
                            msg = f"Skipped {operation} record for {self.table.name}: missing key columns {self.table.keys_missing}"
                        else:
                            msg = f"Skipped {operation} record for {self.table.name}: missing required fields {self.table.reqs_missing}"
                        logger.warning(msg)
                        continue
                    params = self.table.get_bind_params(operation)
                    batch_params.append(params)

                if batch_params:
                    try:
                        self.cursor.executemany(sql, batch_params)
                        self.table.counts[operation] += len(batch_params)
                    except self.cursor.connection.interface.DatabaseError as e:
                        error_msg = f"{operation.capitalize()} batch failed for {self.table.name}: {str(e)}"
                        logger.error(error_msg)
                        if raise_error:
                            raise
                        errors += len(batch_params)

        if self.use_transaction:
            with self.cursor.connection.transaction():
                process_batches()
        else:
            process_batches()

        logger.info(
            f"Batched `{self.table.name}` <{operation}s: {self.table.counts[operation]:,}; errors: {errors:,}; skips: {skipped:,}>")
        self.skips += skipped
        return errors

    def _merge_with_temp_table(self, records: Iterable[RecordLike],
                               raise_error: bool) -> int:
        """Perform bulk merge using temporary table (for databases requiring true MERGE)."""

        # Convert iterator to list since we need to know the count and potentially retry
        records_list = list(records)
        if not records_list:
            return 0

        db_type = self.cursor.connection.database_type
        if db_type == 'postgres' and self.cursor.connection.server_version < 150000:
            raise NotImplementedError(
                f"PostgreSQL MERGE requires version >= 15, found {self.cursor.connection.server_version}")

        temp_name = f"tmp_{self.table.name}_{int(time.time())}"
        if db_type == 'sqlserver':
            temp_name = f"#{temp_name}"

        create_sql = f"CREATE TEMPORARY TABLE {temp_name} AS SELECT * FROM {self.table.name} WHERE 1=0"
        try:
            self.cursor.execute(create_sql)
        except self.cursor.connection.interface.DatabaseError as e:
            error_msg = f"Failed to create temp table {temp_name}: {str(e)}"
            logger.error(error_msg)
            if raise_error:
                raise
            return len(records_list)

        # Use temporary table for bulk insert
        temp_table = Table(
            name=temp_name,
            columns=self.table.columns,
            cursor=self.cursor
        )
        temp_surge = DataSurge(temp_table)
        errors = temp_surge.insert(records_list, raise_error=raise_error)

        if errors:
            self.cursor.execute(f"DROP TABLE IF EXISTS {temp_name}")
            return errors

        # Generate MERGE SQL if not already done
        self.table.generate_sql('merge')
        merge_sql = self.table.sql_statements['merge']

        # Replace the USING clause to point to temp table and store modified version
        modified_merge = re.sub(r'USING\s*\(.*?\)\s*s', f'USING {temp_name} s', merge_sql, flags=re.DOTALL)
        self._sql_statements['merge'] = modified_merge

        def execute_merge():
            nonlocal errors
            try:
                self.cursor.execute(self.get_sql('merge'))
                self.table.counts['merge'] += len(records_list) - errors
            except self.cursor.connection.interface.DatabaseError as e:
                error_msg = f"Merge failed for {self.table.name}: {str(e)}"
                logger.error(error_msg)
                if raise_error:
                    raise
                errors += len(records_list) - errors

        try:
            if self.use_transaction:
                with self.cursor.connection.transaction():
                    execute_merge()
            else:
                execute_merge()
        finally:
            try:
                self.cursor.execute(f"DROP TABLE IF EXISTS {temp_name}")
            except self.cursor.connection.interface.DatabaseError as e:
                error_msg = f"Failed to drop temp table {temp_name}: {str(e)}"
                logger.warning(error_msg)

        logger.info(
            f"Successfully merged {self.table.counts['merge']} records for {self.table.name} with {errors} errors")
        return errors