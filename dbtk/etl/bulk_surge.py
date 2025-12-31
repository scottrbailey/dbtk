# dbtk/etl/bulk_surge.py
import datetime as dt
import logging
import tempfile
import queue
from pathlib import Path
from textwrap import dedent
from typing import Iterable
from ..defaults import settings
from .base_surge import BaseSurge
from ..writers.csv import CSVWriter
from ..record import Record


logger = logging.getLogger(__name__)


class DequeBuffer:
    """File-like queue buffer for streaming to copy_expert."""

    def __init__(self, max_rows = 50_000):
        self._queue = queue.Queue(maxsize=max_rows)
        self.closed = False

    def write(self, data):
        self._queue.put(data)

    def read(self, size=-1):
        if self.closed and self._queue.empty():
            return ''  # EOF

        try:
            # Block for up to 0.1 seconds waiting for data
            return self._queue.get(timeout=0.1)
        except queue.Empty:
            # If closed and empty, EOF. Otherwise keep trying
            if self.closed:
                return ''
            return self.read(size)  # Retry

    def close(self):
        self.closed = True

class BulkSurge(BaseSurge):
    """
    Lightning-fast native bulk loading using COPY, bcp, sqlldr, etc.
    Zero temp files when possible. Streaming. Memory-safe.
    """

    def __init__(self, table, batch_size: int = 10_000, pass_through: bool = False):
        super().__init__(table, batch_size=batch_size, pass_through=pass_through)
        path = None
        # Make sure Table columns are compatible with bulk processing
        self._valid_for_bulk()
        if settings.get('data_dump_dir'):
            path = Path(settings.get('data_dump_dir'))
        if not path or not path.exists():
            path = Path(tempfile.gettempdir())
        self.fallback_dir = path

    def _valid_for_bulk(self):
        """ Determine if table is compatible with bulk loading. """
        expr_cols = []
        for col, info in self.table.columns.items():
            if info.get('db_expr', None) is not None:
                expr_cols.append(col)
        if expr_cols:
            cols = ','.join(expr_cols)
            msg = f"Columns with `db_expr` are incompatible with BulkSurge. Use DataSurge instead.  cols: {cols}"
            logger.exception(msg)
            raise RuntimeError(msg)

    def load(self, records: Iterable[Record]) -> int:
        db_type = self.cursor.connection.database_type.lower()

        if "postgres" in db_type or "redshift" in db_type:
            return self._load_postgres(records)
        elif "oracle" in db_type:
            return self._load_oracle(records)
        elif "mysql" in db_type or "maria" in db_type:
            msg = dedent("""\
            BulkSurge can not load directly into MySQL.  
            Either use DataSurge.import() or 
            BulkSurge.dump() to generate a transformed CSV file with LOAD DATA LOCAL INFILE (often disabled for security reasons). """)
            raise NotImplementedError(msg)
        elif "sqlserver" in db_type or "mssql" in db_type:
            if self.cursor.connection.driver.__name__ == 'pyodbc':
                msg = "Pyodbc has a very fast executemany implementation. Use DataSurge instead, and skip the hassle of bcp!"
            else:
                msg = "If you switch to pyodbc, you can use DataSurge for blazing fast speeds. Otherwise, you can call BulkSurge.dump() to generate a CSV to use with bcp."
            raise NotImplementedError(msg)
        else:
            raise NotImplementedError(f"BulkSurge not supported for {db_type}")

    def _load_postgres(self, records: Iterable[Record]) -> int:
        import threading

        _ = self.table.get_sql('insert')
        cols = ", ".join(self.table._param_config['insert'])
        sql = f"COPY {self.table.name} ({cols}) FROM STDIN WITH (FORMAT csv, NULL '\\N')"

        buf = DequeBuffer(max_rows=self.batch_size * 3)
        exception = None

        def writer_thread():
            nonlocal exception
            try:
                writer = CSVWriter(data=None, file=buf, write_headers=False, null_string='\\N')
                for batch in self.batched(records):
                    writer.write_batch(batch)
            except Exception as e:
                exception = e
            finally:
                buf.close()

        thread = threading.Thread(target=writer_thread, daemon=True)
        thread.start()

        self.cursor.copy_expert(sql, buf)
        thread.join()

        if exception:
            raise exception

        return self.total_loaded

    def _load_oracle(self, records: Iterable[Record]) -> int:
        """
        Load data into Oracle using python-oracledb's direct_path_load.

        This method uses Oracle's direct path load mechanism for maximum performance.
        It bypasses the SQL engine and writes directly to data files, offering
        significantly higher throughput than standard INSERT.  However, DataSurge
        (using normal inserts and executemany) is MUCH more forgiving and many times
        as fast or even faster.

        Parameters
        ----------
        records : Iterable[Record]
            Stream of transformed and validated Record objects from batched().

        Returns
        -------
        int
            Total number of records successfully loaded.

        Raises
        ------
        ValueError
            If table name is not in schema.table format.
        NotImplementedError
            If the oracledb driver version does not support direct_path_load
            (requires python-oracledb >= 3.4).
        RuntimeError
            If direct_path_load fails (e.g., due to constraints, triggers,
            or data type issues).

        Notes
        -----
        - direct_path_load is extremely fast but has strict requirements:
            * Table must allow direct path loads (no active triggers, foreign keys,
              or certain constraints unless disabled).
            * Primary keys and unique indexes should typically be disabled or
              deferred.
            * The load is non-logged in some configurations (faster but less recoverable).
        - This method is only used when BulkSurge is instantiated — it is not
          suitable for tables with db_expr columns or active DML constraints.
        - For more forgiving loads, use DataSurge.insert().
        """
        _ = self.table.get_sql('insert')
        cols = list(self.table.param_config['insert'])

        tabname = self.table.name.split('.')
        if len(tabname) == 2:
            schema, table_name = tabname
        else:
            raise ValueError("Schema is required, use [schema].[table] format")

        # Check if driver supports direct_path_load
        if not hasattr(self.cursor.connection, 'direct_path_load'):
            raise NotImplementedError(
                "direct_path_load requires python-oracledb 3.4+. "
                "Current driver does not support it."
            )
        for batch in self.batched(records):
            # Execute direct path load
            self.cursor.connection.direct_path_load(
                schema_name=schema,
                table_name=table_name,
                batch_size=self.batch_size,
                data=batch,
                column_names=cols
            )
        return self.total_loaded

    def _resolve_file_path(self, file_name: str = None) -> str:
        now = dt.datetime.now()
        if file_name is None:
            return Path(self.fallback_dir) / Path(f"{self.table.name}_{now.strftime('%Y%m%d_%H%M%S')}.csv")
        path = Path(file_name)
        if path.is_dir():
            # got a directory, use default filename
            return path / Path(f"{self.table.name}_{now.strftime('%Y%m%d_%H%M%S')}.csv")
        if path.parent.exists() and path.parent.is_dir():
            # got a full path, use it
            return path
        return Path(self.fallback_dir) / Path(f"{self.table.name}_{now.strftime('%Y%m%d_%H%M%S')}.csv")

    def dump(self, records: Iterable[Record],
             file_name: str = None,
             write_headers: bool = True,
             delimiter: str = ",",
             quotechar: str = '"',
             encoding: str = 'utf-8-sig') -> int:
        path = self._resolve_file_path(file_name)
        with open(path, "w", encoding=encoding, newline='') as fp:
            writer = CSVWriter(data=None, file=fp, write_headers=write_headers, null_string='\\N',
                               delimiter=delimiter, quotechar=quotechar)
            for batch in self.batched(records):
                writer.write_batch(batch)
        logger.info(f"Dumped {self.total_loaded} records to {path}")
        return self.total_loaded