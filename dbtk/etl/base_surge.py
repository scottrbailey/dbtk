# dbtk/etl/base_surge.py
from abc import ABC, abstractmethod
from typing import Iterable, Optional, Generator
from ..record import Record
import logging

logger = logging.getLogger(__name__)


class BaseSurge(ABC):
    def __init__(
        self,
        table,
        batch_size: Optional[int] = None,
        operation: str = "insert",
        param_mode: Optional[str] = None,
    ):
        self.table = table
        self.cursor = table.cursor
        self.batch_size = batch_size or getattr(self.cursor, "batch_size", 1_000)
        self.operation = operation.lower()
        self.param_mode = param_mode  # None → Table default, 'positional' → force tuple

        self.total_processed = 0
        self.total_loaded = 0
        self.skipped = 0

        # Cached dynamic Record class — one per table
        self._RecordClass = None

    def _get_record_class(self, operation = None):
        if self._RecordClass is None:
            if operation is not None:
                # make sure _param_config is populated by generating SQL for operation
                _ = self.table.get_sql(operation)
                cols = list(self.table._param_config[operation])
            else:
                cols = list(self.table.columns.keys())
            self._RecordClass = type('BulkRecord', (Record,), {})
            self._RecordClass.set_columns(cols)
        return self._RecordClass

    def _yield_valid_records(self, records: Iterable[Record]) -> Generator:
        """Core shared logic — identical transformation path for DataSurge & BulkSurge"""
        RecordClass = self._get_record_class()
        # mode = self.param_mode  # BulkSurge: 'positional', DataSurge: None
        # @TODO: see if all db adapters are happy taking Records for named params
        mode = 'positional'
        for raw in records:
            self.total_processed += 1
            self.table.set_values(raw)

            # Validation — same rules as DataSurge
            if self.operation == "delete":
                if not self.table.has_all_keys:
                    self.skipped += 1
                    continue
            else:
                if not self.table.reqs_met:
                    self.skipped += 1
                    continue

            params = self.table.get_bind_params(self.operation, mode=mode)
            yield RecordClass(*params)
            self.total_loaded += 1

    @abstractmethod
    def load(self, records: Iterable[Record]) -> int:
        """Subclasses implement the sink: executemany vs native bulk"""
        pass

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type is None:
            logger.info(
                f"{self.__class__.__name__} [{self.operation.upper()}]: "
                f"{self.total_loaded:,} loaded, {self.skipped:,} skipped, "
                f"{self.total_processed:,} total → {self.table.name}"
            )