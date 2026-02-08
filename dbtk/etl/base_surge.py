# dbtk/etl/base_surge.py
import logging
from abc import ABC, abstractmethod
from typing import Iterable, Generator, Optional
import datetime as dt
import tempfile
from pathlib import Path

from ..utils import RecordLike, batch_iterable
from ..record import Record

logger = logging.getLogger(__name__)


class BaseSurge(ABC):
    def __init__(
        self,
        table,
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
        """Your perfect method — unchanged, just moved and documented."""
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

    def _transform_row(self, record, mode=None):
        """Transform and validate a row. Shared logic for all surges."""
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
        """Yield individual transformed and validated records."""
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

    def _resolve_file_path(self, path_input: Optional[str | Path] = None) -> Path:
        """ """
        if path_input is None:
            base = Path(tempfile.gettempdir())
        else:
            p = Path(path_input)
            if p.is_dir():
                base = p
            elif p.parent.exists() and p.parent.is_dir():
                return p
            else:
                base = Path(tempfile.gettempdir())
                return base / p.name

        timestamp = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
        return base / f"{self.table.name}_{timestamp}.csv"

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