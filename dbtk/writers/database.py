# dbtk/writers/database.py
"""
Database writing utilities and ETL table operations.
"""

import logging

from ..database import ParamStyle
from .utils import get_data_iterator, create_insert_statement

logger = logging.getLogger(__name__)


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

    Examples:
        # Copy between databases
        source_cursor = source_db.cursor()
        source_cursor.execute("SELECT * FROM users")

        target_cursor = target_db.cursor()
        count = cursor_to_cursor(source_cursor, target_cursor, 'users_copy')
        print(f"Copied {count} records")
    """
    rows, columns = get_data_iterator(source_data)

    if not rows:
        logger.warning("No data to copy")
        return 0

    # Create INSERT statement
    paramstyle = target_cursor.connection.interface.__paramstyle
    insert_sql = create_insert_statement(target_table, columns, paramstyle)

    logger.info(f"Starting copy to {target_table}, {len(rows)} records to process")
    logger.debug(f"Using INSERT statement: {insert_sql}")

    total_inserted = 0
    batch = []

    try:
        for i, record in enumerate(rows):
            # Convert record to tuple/list for parameter binding
            if hasattr(record, '_fields'):
                # namedtuple or Record
                record_values = tuple(record)
            elif hasattr(record, 'values') and callable(record.values):
                # dict-like object
                record_values = tuple(record[col] for col in columns)
            elif isinstance(record, (list, tuple)):
                # Already a sequence
                record_values = tuple(record)
            else:
                # Try to extract values by column names
                record_values = tuple(getattr(record, col, None) for col in columns)

            # Handle parameter style
            if paramstyle in ParamStyle.named_styles():
                # Convert to dict for named parameters
                params = {col: val for col, val in zip(columns, record_values)}
                batch.append(params)
            else:
                # Use positional parameters
                batch.append(record_values)

            # Execute batch when it reaches batch_size
            if len(batch) >= batch_size:
                if len(batch) == 1:
                    target_cursor.execute(insert_sql, batch[0])
                else:
                    target_cursor.executemany(insert_sql, batch)

                total_inserted += len(batch)
                batch = []

                # Commit periodically
                if total_inserted % commit_frequency == 0:
                    target_cursor.connection.commit()
                    logger.info(f"Committed {total_inserted} records")

        # Execute remaining batch
        if batch:
            if len(batch) == 1:
                target_cursor.execute(insert_sql, batch[0])
            else:
                target_cursor.executemany(insert_sql, batch)
            total_inserted += len(batch)

        # Final commit
        target_cursor.connection.commit()
        logger.info(f"Copy completed: {total_inserted} records inserted into {target_table}")

    except target_cursor.connection.interface.DatabaseError as e:
        logger.error(f"Error during copy: {e}")
        target_cursor.connection.rollback()
        raise

    return total_inserted

