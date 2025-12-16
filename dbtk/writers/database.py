# dbtk/writers/database.py
"""
Database writing utilities and ETL table operations.
"""

import logging
from typing import Optional, Tuple

from .base import BaseWriter
from ..database import ParamStyle
from ..utils import process_sql_parameters

logger = logging.getLogger(__name__)


class DatabaseWriter(BaseWriter):
    """Database writer that extends BaseWriter."""

    def __init__(self,
                 data,
                 target_cursor,
                 target_table: str,
                 batch_size: int = 1000,
                 commit_frequency: int = 10000):
        """
        Initialize database writer.

        Args:
            data: Source cursor or list of records
            target_cursor: Target database cursor
            target_table: Name of target table
            batch_size: Number of records to insert per batch
            commit_frequency: How often to commit (in number of records)
        """
        # Call BaseWriter with no filename (we're writing to database)
        super().__init__(data, file=None)

        self.target_cursor = target_cursor
        self.target_table = target_table
        self.batch_size = batch_size
        self.commit_frequency = commit_frequency

        # Get parameter style from target connection
        self.paramstyle = target_cursor.connection.interface.paramstyle

        # Pre-generate INSERT statement and param order
        self.insert_sql, self.param_names = self._create_insert_statement()

    def _create_insert_statement(self) -> Tuple[str, Tuple[str, ...]]:
        """Create INSERT statement for the target table."""
        # Build with named parameters
        columns_str = ', '.join(self.columns)
        params_str = ', '.join([f':{col}' for col in self.columns])
        sql = f'INSERT INTO {self.target_table} ({columns_str}) VALUES ({params_str})'

        # Convert to target paramstyle and get parameter order
        return process_sql_parameters(sql, self.paramstyle)

    def _write_data(self, file_obj) -> None:
        """Write data to database using batched inserts."""
        # file_obj is None and unused - we're writing to database

        logger.info(f"Starting copy to {self.target_table}")
        logger.debug(f"Using INSERT statement: {self.insert_sql}")

        batch = []

        try:
            for record in self.data_iterator:
                # Extract values from record
                values = self._row_to_tuple(record)

                # Handle parameter style
                if self.paramstyle in ParamStyle.named_styles():
                    # Convert to dict for named parameters, using param_names order
                    params = {name: values[i] for i, name in enumerate(self.param_names)}
                    batch.append(params)
                else:
                    # Positional parameters - values already in correct order
                    batch.append(tuple(values))

                # Execute batch when it reaches batch_size
                if len(batch) >= self.batch_size:
                    if len(batch) == 1:
                        self.target_cursor.execute(self.insert_sql, batch[0])
                    else:
                        self.target_cursor.executemany(self.insert_sql, batch)

                    self._row_num += len(batch)
                    batch = []

                    # Commit periodically
                    if self._row_num % self.commit_frequency == 0:
                        self.target_cursor.connection.commit()
                        logger.info(f"Committed {self._row_num} records")

            # Execute remaining batch
            if batch:
                if len(batch) == 1:
                    self.target_cursor.execute(self.insert_sql, batch[0])
                else:
                    self.target_cursor.executemany(self.insert_sql, batch)
                self._row_num += len(batch)

            # Final commit
            self.target_cursor.connection.commit()
            logger.info(f"Copy completed: {self._row_num} records inserted into {self.target_table}")

        except self.target_cursor.connection.interface.DatabaseError as e:
            logger.error(f"Error during copy: {e}")
            self.target_cursor.connection.rollback()
            raise

    def write(self) -> int:
        """Override to bypass file handle creation and call _write_data directly."""
        try:
            self._write_data(None)  # Pass None since we don't need file_obj
            return self._row_num
        except Exception as e:
            logger.error(f"Error writing database data: {e}")
            raise


def cursor_to_cursor(source_data,
                     target_cursor,
                     target_table: str,
                     batch_size: int = 1000,
                     commit_frequency: int = 10000) -> int:
    """
    Copy data from source cursor/results to target database table.

    Args:
        source_data: Source cursor or list of records
        target_cursor: Target database cursor
        target_table: Name of target table
        batch_size: Number of records to insert per batch
        commit_frequency: How often to commit (in number of records)

    Returns:
        Number of records inserted

    Example:
        # Copy between databases
        source_cursor = source_db.cursor()
        source_cursor.execute("SELECT * FROM users")

        target_cursor = target_db.cursor()
        count = cursor_to_cursor(source_cursor, target_cursor, 'users_copy')
        print(f"Copied {count} records")
    """
    writer = DatabaseWriter(
        data=source_data,
        target_cursor=target_cursor,
        target_table=target_table,
        batch_size=batch_size,
        commit_frequency=commit_frequency
    )
    return writer.write()