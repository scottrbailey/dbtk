# dbtk/etl/base_surge.py
import logging
from abc import ABC, abstractmethod
from typing import Iterable, Generator, Optional
import datetime as dt
import tempfile
from pathlib import Path

from . import Table
from ..defaults import settings
from ..utils import RecordLike, batch_iterable, sanitize_identifier
from ..record import Record

logger = logging.getLogger(__name__)


class BaseSurge(ABC):
    """
    Base class for all Surge loaders.

    Provides common iteration, transformation, validation, and skip tracking
    for loading data into database tables.

    Parameters
    ----------
    table : Table
        Table instance with column definitions and cursor
    batch_size : int, optional
        Number of records per batch (default: cursor.batch_size or 1000)
    pass_through : bool, optional
        Skip transformation and validation (default: False)

    Attributes
    ----------
    total_read : int
        Total rows read from source. 1-based (first row = 1). Includes
        both loaded and skipped rows.
    total_loaded : int
        Total rows successfully transformed, validated and loaded.
    skipped : int
        Total rows skipped due to missing required fields.
    skip_details : dict
        Skip tracking grouped by reason. Key is a frozenset of missing
        required field names. Value is a dict with:

        - ``count``: total rows skipped for this reason
        - ``sample``: list of up to 20 1-based row numbers (for debugging)

        Example::

            {frozenset({'primary_name'}): {'count': 5, 'sample': [937887, 957847, ...]}}
    """
    def __init__(
        self,
        table: Table,
        batch_size: Optional[int] = None,
        pass_through: bool = False
    ):
        self.table = table
        self.cursor = table.cursor
        self.batch_size = batch_size or getattr(self.cursor, "batch_size", 1000)
        self.pass_through = pass_through
        self.operation = 'insert'
        # force positional parameter style
        self.table.force_positional()
        # stats
        self.total_read = 0
        self.total_loaded = 0
        self.skipped = 0
        self.skip_details = {}  # key: frozenset of missing fields, value: {'count': int, 'sample': [row_nums]}

        self._RecordClass = None  # Built on first use

    def _get_record_class(self, operation: Optional[str] = None):
        """Build or return the Record subclass for this operation's columns."""
        if self._RecordClass is None:
            if operation is not None:
                # Force SQL generation to populate _param_config[operation]
                _ = self.table.get_sql(operation)
                cols = list(self.table._param_config[operation])
            else:
                cols = list(self.table.columns.keys())

            self._RecordClass = type("Record", (Record,), {})
            self._RecordClass.set_columns(cols)
        return self._RecordClass

    def _get_columns(self, operation: Optional[str] = None):
        """ Get column names in the correct order for this operation. """
        if operation is None:
            operation = self.operation
        bind_params = self.table._param_config[operation]
        return [self.table.bind_name_column(name) for name in bind_params]

    def _transform_row(self, record, mode=None):
        """
        Transform and validate a single row.

        Applies column transforms and checks required fields. On failure,
        records the 1-based row number in skip_details for debugging (up to
        20 samples per unique set of missing fields).

        Returns None if validation fails (caller should skip the row).
        """
        self.table.set_values(record)
        if not self.table.is_ready(self.operation):
            missing = self.table.reqs_missing(self.operation)
            if missing:
                missing_set = frozenset(missing)
                # Initialize skip tracking for this reason if needed
                if missing_set not in self.skip_details:
                    self.skip_details[missing_set] = {'count': 0, 'sample': []}

                self.skip_details[missing_set]['count'] += 1

                # Keep first 20 row numbers as samples for debugging
                if self.skip_details[missing_set]['count'] <= 20:
                    row_num = record.get('_row_num', self.total_read)
                    self.skip_details[missing_set]['sample'].append(row_num)
            return None
        return self.table.get_bind_params(self.operation, mode=mode)

    def records(self, source: Iterable[RecordLike]) -> Generator[tuple, None, None]:
        """Yield individual transformed and validated records, updating stats."""
        for raw in source:
            self.total_read += 1
            if self.pass_through:
                params = raw
            else:
                params = self._transform_row(raw)
            if params is not None:
                self.total_loaded += 1
                yield params
            else:
                self.skipped += 1

    def batched(self, source: Iterable[RecordLike]) -> Generator[list, None, None]:
        """
        Primary batch interface.

        Returns an iterator that yields batches of fully transformed and validated
        Record objects. This is the canonical way to consume data from a Surge.

        Used by:
            - DataSurge.load() → executemany
            - BulkSurge.dump() → CSVWriter
            - Any custom streaming pipeline

        Example
        -------
        >>> for batch in surge.batched(reader):
        >>>     writer.write_batch(batch)
        """

        batch = []

        for raw in source:
            self.total_read += 1
            if self.pass_through:
                params = raw
            else:
                params = self._transform_row(raw)
            if params is not None:
                batch.append(params)
                self.total_loaded += 1
            else:
                self.skipped += 1
            if len(batch) >= self.batch_size:
                yield batch
                batch = []

        if batch:
            yield batch

    def _resolve_file_path(self, path_input: Optional[str | Path] = None, extension: str = '.csv') -> Path:
        """
        Resolve an output file path from user input.

        Handles both file paths and directory paths, generating timestamped
        filenames when a directory is provided. Sanitizes table names for
        safe filesystem use.

        Resolution Priority
        -------------------
        1. User-provided path_input
           - If existing file or valid file path: use exactly
           - If existing directory: generate timestamped file inside it
        2. Configured settings['data_dump_dir']
           - If directory exists: generate timestamped file inside it
        3. System temp directory (fallback)
        """
        if extension and not extension.startswith('.'):
            extension = '.' + extension

        if path_input:
            p = Path(path_input)
            if p.is_file() or (p.suffix == extension and p.parent.exists()):
                return p
            elif p.is_dir() and p.exists():
                timestamp = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
                safe_name = sanitize_identifier(self.table.name)
                return p / f"{safe_name}_{timestamp}{extension}"

        configured = settings.get('data_dump_dir')
        if configured:
            p = Path(configured)
            if p.is_dir() and p.exists():
                timestamp = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
                safe_name = sanitize_identifier(self.table.name)
                return p / f"{safe_name}_{timestamp}{extension}"
            else:
                logger.warning(f"Configured data_dump_dir '{configured}' invalid. Using temp dir.")

        temp_dir = Path(tempfile.gettempdir())
        timestamp = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
        safe_name = sanitize_identifier(self.table.name)
        return temp_dir / f"{safe_name}_{timestamp}{extension}"

    @abstractmethod
    def load(self,
             records: Iterable[RecordLike],
             operation: Optional[str] = 'insert',
             raise_error: bool = True) -> int:
        """ Load records into database. """
        pass

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type is None:
            logger.info(
                f"{self.__class__.__name__} [{self.operation.upper()}]: "
                f"{self.total_loaded:,} loaded, {self.skipped:,} skipped → {self.table.name}"
            )
            if self.skipped:
                logger.info(f"Skipped {self.skipped:,} rows total.")
                # Sort by count (descending) for consistent reporting
                sorted_skips = sorted(self.skip_details.items(), key=lambda x: x[1]['count'], reverse=True)
                for reason_set, details in sorted_skips:
                    fields_str = ', '.join(sorted(reason_set)) or "<unknown reason>"
                    logger.info(f"  - {details['count']:,} rows skipped due to missing: {fields_str}")
                    # Log first few samples if debug mode
                    for row_idx in details['sample'][:3]:  # first 3 per reason
                        logger.debug(f"    Sample skip at row #{row_idx}: missing {reason_set}")
        return None