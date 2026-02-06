# dbtk/etl/bulk_surge.py
import datetime as dt
import logging
import os
import queue
import subprocess
import tempfile
import threading

from pathlib import Path
from textwrap import dedent
from typing import Iterable, Union, Optional

import dbtk.config
from ..defaults import settings
from .base_surge import BaseSurge
from ..writers.csv import CSVWriter
from ..record import Record
from ..utils import sanitize_identifier


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

    def _get_connection_config(self):
        if not self.cursor.connection.connection_name:
            raise RuntimeError('BCP needs credentials. Please set up a named connection in the config file.')
        cm = dbtk.config.ConfigManager()
        config = cm.get_connection_config(self.cursor.connection_name)
        return config

    def load(self, records: Iterable[Record],
             method: str = 'direct',
             dump_path: Optional[Union[str, Path]] = None) -> int:
        db_type = self.cursor.connection.database_type.lower()
        if "postgres" in db_type or "redshift" in db_type:
            return self._load_postgres_direct(records)
        elif "oracle" in db_type:
            if method == "direct":
                return self._load_oracle_direct(records)
            else:
                return self._load_oracle_sqlldr(records, dump_path=dump_path)
        elif "mysql" in db_type or "maria" in db_type:
            if method == "direct":
                return self._load_mysql_local_stream(records)
            else:
                return self._load_mysql_external(records, dump_path=dump_path)
        elif "sqlserver" in db_type or "mssql" in db_type:
            return self._load_mssql_bcp(records, dump_path=dump_path)
        else:
            raise NotImplementedError(f"BulkSurge not supported for {db_type}")

    def _load_postgres_direct(self, records: Iterable[Record]) -> int:
        _ = self.table.get_sql('insert')
        cols = ", ".join(self.table._param_config['insert'])
        sql = f"COPY {self.table.name} ({cols}) FROM STDIN WITH (FORMAT csv, NULL '\\N')"

        buffer = DequeBuffer(max_rows=self.batch_size * 3)
        exception = None

        def writer_thread():
            nonlocal exception
            try:
                writer = CSVWriter(data=None, file=buffer, write_headers=False, null_string='\\N')
                for batch in self.batched(records):
                    writer.write_batch(batch)
            except Exception as e:
                exception = e
            finally:
                buffer.close()

        thread = threading.Thread(target=writer_thread, daemon=True)
        thread.start()

        try:
            self.cursor.copy_expert(sql, buffer)
        finally:
            buffer.close()
            thread.join(timeout=30)
            if thread.is_alive():
                logger.warning("Writer thread did not finish in time")

        if exception:
            raise exception

        return self.total_loaded

    def _load_oracle_direct(self, records: Iterable[Record]) -> int:
        """
        Load data into Oracle using python-oracledb's direct_path_load.

        This method uses Oracle's direct path load mechanism for maximum performance.
        It bypasses the SQL engine and writes directly to data files, offering
        significantly higher throughput than standard INSERT.  However, DataSurge
        (using normal inserts and executemany) is MUCH more forgiving and in most cases, fast enough.

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
                "Try method=external to use SQL*Loader"
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

    def _load_oracle_sqlldr(self, records: Iterable[Record],
                            dump_path: Optional[Union[str, Path]] = None) -> int:
        import uuid
        config = self._get_connection_config()
        user = config.get('user')
        password = config.get('password')
        db = config.get('database') or config.get('dsn')

        # Dump CSV
        csv_path = self._resolve_dump_path(dump_path, 'csv')
        self.dump(records, file_name=csv_path, delimiter=',', quotechar='"', encoding='utf-8-sig')

        # Unique ctl name (avoid collisions)
        unique = uuid.uuid4().hex[:8]
        ctl_path = csv_path.with_name(f"{csv_path.stem}_{unique}.ctl")

        # Generate control file from current Table schema
        cols = ', '.join(f"{col} CHAR" for col in self.table.columns.keys())
        ctl_content = dedent(f"""\
        LOAD DATA
        INFILE '{csv_path.name}'
        INTO TABLE {self.table.name}
        FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"'
        TRAILING NULLCOLS
        ({cols})
        """)
        ctl_path.write_text(ctl_content)

        # Env for subprocess (creds not in cmd line)
        env = os.environ.copy()
        env['ORACLE_USER'] = user
        env['ORACLE_PASS'] = password
        env['ORACLE_DB'] = db

        cmd = ['sqlldr', f'userid={user}/{password}@{db}', f'control={ctl_path}', f'data={csv_path}']

        try:
            result = subprocess.run(cmd, env=env, capture_output=True, text=True, check=True)
            logger.info(f"sql*loader success: {result.stdout}")
        except subprocess.CalledProcessError as e:
            logger.error(f"sql*loader failed: {e.stderr}")
            raise RuntimeError("sql*loader failed") from e
        finally:
            csv_path.unlink(missing_ok=True)
            ctl_path.unlink(missing_ok=True)

        return self.total_loaded

    def _load_mssql_bcp(self, records: Iterable[Record],
                        dump_path: Optional[Union[str, Path]] = None) -> int:
        config = self._get_connection_config()
        user = config.get('user')
        password = config.get('password')
        host = config.get('host')
        db = config.get('database')

        csv_path = self._resolve_dump_path(dump_path, 'tsv')
        self.dump(records, file_name=csv_path, delimiter='\t')  # bcp prefers tab-delimited

        if user and password:
            cmd = ['bcp', self.table.name, 'in', str(csv_path), f'-S {host}', f'-d {db}', f'-U {user}', f'-P {password}']
        else:
            cmd = ['bcp', self.table.name, 'in', str(csv_path), f'-S {host}', f'-d {db}', '-T']  # integrated auth

        result = subprocess.run(cmd, capture_output=True, text=True)
        if result.returncode != 0:
            logger.error(f"bcp failed: {result.stderr}")
            raise RuntimeError("bcp failed")

        csv_path.unlink()
        return self.total_loaded

    def _load_mysql_local_stream(self, records: Iterable[Record]) -> int:
        """
        Stream bulk load into MySQL using LOAD DATA LOCAL INFILE with zero temp file.
        Uses a background writer thread to populate the buffer while the main thread
        executes the LOAD command.
        """
        if 'mysql' not in self.cursor.connection.database_type.lower():
            raise RuntimeError("This method is for MySQL/MariaDB only")

        buffer = DequeBuffer(max_rows=self.batch_size * 10)  # generous buffer

        # Background thread writes CSV to buffer
        def writer_thread():
            try:
                writer = CSVWriter(
                    data=None,
                    file=buffer,
                    write_headers=True,
                    null_string='\\N',
                    delimiter=',',
                    quotechar='"',
                    escapechar='\\',
                    lineterminator='\n'
                )
                for batch in self.batched(records):
                    writer.write_batch(batch)
                writer.close()
            except Exception as e:
                logger.error(f"CSV writer thread failed: {e}")
                buffer.close()  # signal EOF

        thread = threading.Thread(target=writer_thread, daemon=True)
        thread.start()

        # Execute LOAD DATA LOCAL INFILE using the buffer as file-like object
        sql = dedent(f"""\
        LOAD DATA LOCAL INFILE 'buffer_stream'
        INTO TABLE {self.table.name}
        FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"'
        ESCAPED BY '\\\\'
        LINES TERMINATED BY '\\n'
        IGNORE 1 LINES
        """)

        try:
            # Most MySQL connectors accept file-like objects for LOCAL INFILE
            self.cursor.execute(sql, params=None)  # buffer is used internally
            thread.join(timeout=30)  # wait for writer to finish
            if thread.is_alive():
                logger.warning("Writer thread did not finish in time")
                buffer.close()
            return self.total_loaded
        except Exception as e:
            logger.error(f"LOAD DATA LOCAL failed: {e}")
            buffer.close()
            thread.join(timeout=5)
            raise
        finally:
            buffer.close()

    def _load_mysql_external(self, records: Iterable[Record],
                             dump_path: Optional[Union[str, Path]] = None) -> int:
        self.cursor.execute("SELECT @@local_infile")
        if self.cursor.fetchone()[0] == 1:
            # Streaming with DequeBuffer instead of file
            return self._load_mysql_local_stream(records)
        else:
            csv_path = self._resolve_dump_path(dump_path, 'csv')
            self.dump(records, file_name=csv_path)
            logger.info(
                f"local_infile is OFF on server. CSV dumped to {csv_path}. "
                "To load manually (server-side file):\n"
                f"LOAD DATA INFILE '{csv_path}' INTO TABLE {self.table.name} "
                f"FIELDS TERMINATED BY ',' ENCLOSED BY '\"' IGNORE 1 LINES;"
            )
            return self.total_loaded

    def _resolve_dump_path(self, dump_path: Optional[Union[str, Path]] = None, extension: str = '.csv') -> Path:
        """
        Resolve the final CSV path based on user input or fallbacks.

        Priority:
        1. User-provided dump_path (file or dir)
        2. settings['data_dump_dir'] (if set and valid)
        3. tempfile.gettempdir()
        """
        if dump_path:
            p = Path(dump_path)
            if p.is_file() or (p.suffix == extension and p.parent.exists()):
                return p  # full file path → use exactly
            elif p.is_dir() and p.exists():
                # dir → generate timestamped name inside it
                timestamp = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
                safe_name = sanitize_identifier(self.table.name)
                return p / f"{safe_name}_{timestamp}{extension}"

        # Configured dir fallback
        configured = settings.get('data_dump_dir')
        if configured:
            p = Path(configured)
            if p.is_dir() and p.exists():
                timestamp = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
                safe_name = sanitize_identifier(self.table.name)
                return p / f"{safe_name}_{timestamp}{extension}"
            else:
                logger.warning(f"Configured data_dump_dir '{configured}' invalid. Using temp dir.")

        # Last resort: temp dir
        temp_dir = Path(tempfile.gettempdir())
        timestamp = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
        safe_name = sanitize_identifier(self.table.name)
        return temp_dir / f"{safe_name}_{timestamp}{extension}"

    def dump(self, records: Iterable[Record],
             file_name: str = None,
             write_headers: bool = True,
             delimiter: str = ",",
             quotechar: str = '"',
             encoding: str = 'utf-8-sig') -> int:
        path = self._resolve_dump_path(file_name)
        with open(path, "w", encoding=encoding, newline='') as fp:
            writer = CSVWriter(data=None, file=fp, write_headers=write_headers, null_string='\\N',
                               delimiter=delimiter, quotechar=quotechar)
            for batch in self.batched(records):
                writer.write_batch(batch)
        logger.info(f"Dumped {self.total_loaded} records to {path}")
        return self.total_loaded