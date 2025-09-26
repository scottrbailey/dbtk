# dbtk/etl/bulk.py

import logging
import re
import time
from .table import Table

from typing import Iterable

try:
    from typing import Mapping
except ImportError:
    from collections.abc import Mapping

from ..cursors import Cursor
from ..database import ParamStyle
from ..utils import batch_iterable, RecordLike

logger = logging.getLogger(__name__)

class DataSurge:
    """
    Handles bulk ETL operations by delegating to a stateful Table instance.

    Note: The Table instance's state (self.values) is modified during processing.
    Ensure the Table is not used concurrently by other operations or threads.

    Example:
        table = Table(...)
        surge = DataSurge(table)
        errors = surge.merge(cursor, records, batch_size=500, ignore_errors=True)
    """

    def __init__(self, table: Table):
        self.table = table
        self.skips = 0

    def insert(self, cursor: Cursor, records: Iterable[RecordLike],
               batch_size: int = 1000,  raise_error: bool = True) -> int:
        """Perform bulk INSERT on records."""
        if self.table._sql_statements['insert'] is None:
            self.table.generate_sql('insert')
        sql = self.table.sql_statements['insert']

        errors = 0
        skipped = 0

        for batch in batch_iterable(records, batch_size):
            batch_params = []
            for record in batch:
                self.table.set_values(record)
                if not self.table.reqs_met:
                    skipped += 1
                    msg = f"Skipped insert record for {self.table._name}: missing required fields {self.table.reqs_missing}"
                    logger.info(msg)
                    continue
                params = self.table.get_bind_params('insert')
                batch_params.append(params)

            if batch_params:
                try:
                    cursor.executemany(sql, batch_params)
                    self.table.counts['insert'] += len(batch_params)
                except cursor.connection.interface.DatabaseError as e:
                    error_msg = f"Insert batch failed for {self.table._name}: {str(e)}"
                    logger.error(error_msg)
                    if raise_error:
                        raise
                    errors += len(batch_params)

        logger.info(f"Successfully inserted {self.table.counts['insert']} records for {self.table._name} with {errors} errors, {skipped} skipped")
        self.skips += skipped
        return errors

    def update(self, cursor: Cursor, records: Iterable[RecordLike],
               batch_size: int = 1000, raise_error: bool = True) -> int:
        """Perform bulk UPDATE on records."""
        if self.table._sql_statements['update'] is None:
            self.table.generate_sql('update')

        sql = self.table.sql_statements['update']
        errors = 0
        skipped = 0

        for batch in batch_iterable(records, batch_size):
            batch_params = []
            for record in batch:
                self.table.set_values(record)
                if not self.table.reqs_met:
                    skipped += 1
                    msg = f"Skipped update record for {self.table._name}: missing required fields {self.table.reqs_missing}"
                    logger.info(msg)
                    continue
                params = self.table.get_bind_params('update')
                batch_params.append(params)

            if batch_params:
                try:
                    cursor.executemany(sql, batch_params)
                    self.table.counts['update'] += len(batch_params)
                except cursor.connection.interface.DatabaseError as e:
                    error_msg = f"Update batch failed for {self.table._name}: {str(e)}"
                    logger.error(error_msg)
                    if raise_error:
                        raise
                    errors += len(batch_params)

        logger.info(f"Successfully updated {self.table.counts['update']} records for {self.table._name} with {errors} errors, {skipped} skipped")
        self.skips += skipped
        return errors

    def delete(self, cursor: Cursor, records: Iterable[RecordLike],
               batch_size: int = 1000, raise_error: bool = True) -> int:
        """Perform bulk DELETE on records."""
        if self.table.sql_statements['delete'] is None:
            self.table.generate_sql('delete')

        sql = self.table.sql_statements['delete']
        errors = 0
        skipped = 0

        for batch in batch_iterable(records, batch_size):
            batch_params = []
            for record in batch:
                self.table.set_values(record)
                if not self.table.reqs_met:
                    skipped += 1
                    msg = f"Skipped delete record for {self.table._name}: missing required fields {self.table.reqs_missing}"
                    logger.info(msg)
                    continue
                params = self.table.get_bind_params('delete')
                batch_params.append(params)

            if batch_params:
                try:
                    cursor.executemany(sql, batch_params)
                    self.table.counts['delete'] += len(batch_params)
                except cursor.connection.interface.DatabaseError as e:
                    error_msg = f"Delete batch failed for {self.table._name}: {str(e)}"
                    logger.error(error_msg)
                    if raise_error:
                        raise
                    errors += len(batch_params)

        logger.info(f"Successfully deleted {self.table.counts['delete']} records for {self.table._name} with {errors} errors, {skipped} skipped")
        return errors

    def merge(self, cursor: Cursor, records: Iterable[RecordLike],
              batch_size: int = 1000, raise_error: bool = True) -> int:
        """
        Perform bulk MERGE using either direct upsert or temporary table strategy.
        """
        db_type = cursor.connection.server_type
        if db_type == 'postgres' and cursor.connection.server_version < 150000:
            # PostgreSQL < 15 doesn't have MERGE, must use upsert
            use_upsert = True
        else:
            # Check if we should use upsert based on database capabilities
            use_upsert = self.table._should_use_upsert(db_type)

        if use_upsert:
            return self._merge_with_upsert(cursor, records, batch_size=batch_size, raise_error=raise_error)
        else:
            return self._merge_with_temp_table(cursor, records, batch_size=batch_size, raise_error=raise_error)

    def _merge_with_upsert(self, cursor: Cursor, records: Iterable[RecordLike],
                           raise_error: bool, batch_size: int) -> int:
        """Perform bulk merge using native INSERT ... ON DUPLICATE KEY/CONFLICT syntax."""
        if self.table.sql_statements['merge'] is None:
            db_type = cursor.connection.server_type
            self.table.generate_sql('merge', db_type=db_type)

        base_sql = self.table.sql_statements['merge']
        errors = 0
        skipped = 0

        for batch in batch_iterable(records, batch_size):
            valid_records = []
            batch_params = []

            # Process records and collect valid ones
            for record in batch:
                self.table.set_values(record)
                if not self.table.reqs_met:
                    skipped += 1
                    msg = f"Skipped merge record for {self.table._name}: missing required fields {self.table.reqs_missing}"
                    logger.info(msg)
                    continue

                valid_records.append(record)
                params = self.table.get_bind_params('merge')
                batch_params.append(params)

            if not valid_records:
                continue

            if len(valid_records) == 1:
                # Single record - use base SQL as-is
                try:
                    cursor.execute(base_sql, batch_params[0])
                    self.table.counts['merge'] += 1
                except cursor.connection.interface.DatabaseError as e:
                    error_msg = f"Upsert failed for {self.table._name}: {str(e)}"
                    logger.error(error_msg)
                    if raise_error:
                        raise
                    errors += 1
            else:
                # Multiple records - build multi-row VALUES if using positional params
                if self.table.__paramstyle in ParamStyle.positional_styles():
                    try:
                        batch_sql, flat_params = self._build_multi_row_upsert(base_sql, batch_params)
                        cursor.execute(batch_sql, flat_params)
                        self.table.counts['merge'] += len(valid_records)
                    except cursor.connection.interface.DatabaseError as e:
                        error_msg = f"Upsert batch failed for {self.table._name}: {str(e)}"
                        logger.error(error_msg)
                        if raise_error:
                            raise
                        errors += len(valid_records)
                else:
                    # For named parameters, execute individually
                    for i, params in enumerate(batch_params):
                        try:
                            cursor.execute(base_sql, params)
                            self.table.counts['merge'] += 1
                        except cursor.connection.interface.DatabaseError as e:
                            error_msg = f"Upsert failed for {self.table._name}: {str(e)}"
                            self._log_error(error_msg, log)
                            if not ignore_errors:
                                raise
                            errors += 1

        if log_success:
            logger.info(
                f"Successfully merged {self.table.counts['merge']} records for {self.table._name} with {errors} errors, {skipped} skipped")
        return errors + skipped

    def _build_multi_row_upsert(self, base_sql: str, batch_params: list) -> tuple:
        """Build multi-row VALUES clause for upsert statements."""
        # Build placeholders for each row
        if self.table.__paramstyle == ParamStyle.QMARK:
            placeholder = '?'
        elif self.table.__paramstyle == ParamStyle.FORMAT:
            placeholder = '%s'
        elif self.table.__paramstyle == ParamStyle.NUMERIC:
            # For numeric, we need to renumber all parameters
            placeholder = None  # Will be handled specially

        if placeholder:
            # Build VALUES clauses
            param_count = len(batch_params[0]) if batch_params else 0
            row_placeholders = ', '.join([placeholder] * param_count)
            value_clauses = [f"({row_placeholders})" for _ in batch_params]
            multi_values = f"VALUES {', '.join(value_clauses)}"

            # Replace single VALUES clause with multi-row
            batch_sql = re.sub(r'VALUES\s*\([^)]*\)', multi_values, base_sql, flags=re.IGNORECASE)

            # Flatten parameters
            flat_params = []
            for params in batch_params:
                if isinstance(params, (list, tuple)):
                    flat_params.extend(params)
                else:
                    flat_params.append(params)

            return batch_sql, flat_params
        else:
            # Handle NUMERIC style by renumbering parameters
            flat_params = []
            param_counter = 1
            modified_sql = base_sql

            for i, params in enumerate(batch_params):
                if i == 0:
                    # First row uses existing SQL
                    flat_params.extend(params)
                    param_counter += len(params)
                else:
                    # Additional rows need new parameter numbers
                    row_placeholders = []
                    for _ in params:
                        row_placeholders.append(f':{param_counter}')
                        param_counter += 1

                    additional_values = f", ({', '.join(row_placeholders)})"
                    # Find the end of the VALUES clause and append
                    values_end = modified_sql.find(')')
                    if 'ON DUPLICATE KEY' in modified_sql:
                        insert_pos = modified_sql.find('ON DUPLICATE KEY')
                    elif 'ON CONFLICT' in modified_sql:
                        insert_pos = modified_sql.find('ON CONFLICT')
                    else:
                        insert_pos = len(modified_sql)

                    # Find the last ) before the ON clause
                    values_section = modified_sql[:insert_pos]
                    last_paren = values_section.rfind(')')
                    modified_sql = modified_sql[:last_paren] + additional_values + modified_sql[last_paren:]

                    flat_params.extend(params)

            return modified_sql, flat_params

    def _merge_with_temp_table(self, cursor: Cursor, records: Iterable[RecordLike],
                               raise_error: bool, batch_size: int) -> int:
        """Perform bulk merge using temporary table (original implementation)."""

        # Convert iterator to list since we need to know the count and potentially retry
        records_list = list(records)
        if not records_list:
            return 0

        db_type = cursor.connection.server_type
        if db_type == 'postgres' and cursor.connection._connection.server_version < 150000:
            raise NotImplementedError(
                f"PostgreSQL MERGE requires version >= 15, found {cursor.connection._connection.server_version}")

        temp_name = f"tmp_{self.table._name}_{int(time.time())}"
        if db_type == 'sqlserver':
            temp_name = f"#{temp_name}"

        create_sql = f"CREATE TEMPORARY TABLE {temp_name} AS SELECT * FROM {self.table._name} WHERE 1=0"
        try:
            cursor.execute(create_sql)
        except cursor.connection.interface.DatabaseError as e:
            error_msg = f"Failed to create temp table {temp_name}: {str(e)}"
            logger.error(error_msg)
            if raise_error:
                raise
            return len(records_list)

        # Use temporary table for bulk insert
        temp_table = Table(
            name=temp_name,
            columns=self.table.__columns,
            paramstyle=self.table.__paramstyle
        )
        temp_surge = DataSurge(temp_table)
        errors = temp_surge.insert(cursor, records_list, raise_error=raise_error, batch_size=batch_size)

        if errors:
            cursor.execute(f"DROP TABLE IF EXISTS {temp_name}")
            return errors

        # Generate MERGE SQL if not already done
        if self.table.sql_statements['merge'] is None:
            self.table.generate_sql('merge', db_type=db_type)

        merge_sql = self.table.sql_statements['merge']
        # Replace the USING clause to point to temp table
        modified_merge = re.sub(r'USING\s*\(.*?\)\s*s', f'USING {temp_name} s', merge_sql, flags=re.DOTALL)

        try:
            with cursor.connection.transaction():
                cursor.execute(modified_merge)
                self.table.counts['merge'] += len(records_list) - errors
        except cursor.connection.interface.DatabaseError as e:
            error_msg = f"Merge failed for {self.table._name}: {str(e)}"
            logger.error(error_msg)
            if raise_error:
                raise
            errors += len(records_list) - errors
        finally:
            try:
                cursor.execute(f"DROP TABLE IF EXISTS {temp_name}")
            except cursor.connection.interface.DatabaseError as e:
                error_msg = f"Failed to drop temp table {temp_name}: {str(e)}"
                logger.warning(error_msg)

        logger.info(f"Successfully merged {self.table.counts['merge']} records for {self.table._name} with {errors} errors")
        return errors