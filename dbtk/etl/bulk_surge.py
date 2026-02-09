# dbtk/etl/bulk_surge.py
import csv
import datetime as dt
import logging
import os
import queue
import subprocess
import threading

from pathlib import Path
from textwrap import dedent
from typing import Iterable, Union, Optional

import dbtk.config
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
    Lightning-fast bulk loading using native database tools and streaming.

    BulkSurge provides high-performance data loading by leveraging database-specific
    bulk loading mechanisms. It supports both direct streaming (zero temp files) and
    external tool-based loading (bcp, SQL*Loader) depending on the database and method chosen.

    Supported Databases
    -------------------
    * **PostgreSQL/Redshift**: COPY FROM STDIN (streaming, no temp files)
    * **Oracle**: direct_path_load (streaming) or SQL*Loader (external tool)
    * **MySQL/MariaDB**: LOAD DATA LOCAL INFILE (streaming) with automatic fallback
    * **SQL Server**: bcp utility (external tool, requires named connection)

    Loading Methods
    ---------------
    * **direct** (default): Uses native Python drivers for streaming bulk loads
        - Postgres: COPY protocol with background writer thread
        - Oracle: direct_path_load API (requires python-oracledb 3.4+)
        - MySQL: LOAD DATA LOCAL INFILE with in-memory buffer

    * **external**: Uses command-line tools (requires named connection from config)
        - Oracle: SQL*Loader (sqlldr) with auto-generated control file
        - MySQL: Falls back to direct if local_infile enabled, else dumps CSV
        - SQL Server: bcp utility (only external method available)

    Performance Notes
    -----------------
    - BulkSurge is memory-efficient: uses batching and streaming to handle large datasets
    - Direct methods are typically faster and require no temp files
    - External tools require credentials from config file (see connection_name)
    - Tables with db_expr columns are incompatible (use DataSurge instead)

    Parameters
    ----------
    table : Table
        Table instance with column definitions and cursor
    batch_size : int, optional
        Number of records per batch (default: 10,000)
    pass_through : bool, optional
        Skip transformation/validation (default: False)

    Attributes
    ----------
    total_read : int
        Total rows read from source. 1-based (first row = 1). Includes
        both loaded and skipped rows.
    total_loaded : int
        Total rows successfully loaded.
    skipped : int
        Total rows skipped due to missing required fields.
    skip_details : dict
        Skip tracking grouped by reason. Key is a frozenset of missing
        required field names. Value is a dict with:

        - ``count``: total rows skipped for this reason
        - ``sample``: list of up to 20 1-based row numbers (for debugging)

        Example::

            {frozenset({'primary_name'}): {'count': 5, 'sample': [937887, 957847, ...]}}
    dump_path : Path or None
        Path of the last file written by dump(). Set after each dump() call.

    Examples
    --------
    PostgreSQL streaming (zero temp files)::

        with BulkSurge(table) as surge:
            surge.load(reader)  # Uses COPY FROM STDIN

    Oracle with SQL*Loader::

        surge = BulkSurge(table)
        surge.load(reader, method='external')  # Uses sqlldr

    MySQL with custom dump location::

        surge = BulkSurge(table)
        surge.load(reader, method='direct', dump_path='/data/staging')

    SQL Server with bcp (requires config)::

        db = dbtk.connect('prod_mssql')  # Named connection required
        surge = BulkSurge(table)
        surge.load(reader)  # Uses bcp

    See Also
    --------
    DataSurge : Standard bulk operations using executemany
    Table : Table definition and schema management
    """

    def __init__(self, table, batch_size: int = 10_000, pass_through: bool = False):
        super().__init__(table, batch_size=batch_size, pass_through=pass_through)
        # Make sure Table had built out insert query and parameter info
        self.operation = 'insert'
        self.table.get_sql('insert')
        # Make sure Table columns are compatible with bulk processing
        self._valid_for_bulk()
        self.dump_path = None
        self.control_path = None

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
        config = cm.get_connection_config(self.cursor.connection.connection_name)
        return config

    def load(self, records: Iterable[Record],
             method: str = 'direct',
             dump_path: Optional[Union[str, Path]] = None) -> int:
        """
        Bulk load records using database-specific mechanisms.

        Automatically selects the appropriate loading strategy based on database type
        and method parameter. Direct methods use streaming with zero temp files when
        possible. External methods use command-line tools and require a named connection.

        Parameters
        ----------
        records : Iterable[Record]
            Iterator of Record objects to load
        method : str, optional
            Loading method to use (default: 'direct')
            - 'direct': Stream data using native drivers (Postgres COPY, Oracle direct_path_load, MySQL LOCAL INFILE)
            - 'external': Use external tools (Oracle sqlldr, SQL Server bcp, MySQL fallback)
        dump_path : str or Path, optional
            Path for temp CSV files (only used by external methods)
            - If file path: use exactly as specified
            - If directory: generate timestamped file in that directory
            - If None: use settings['data_dump_dir'] or temp directory

        Returns
        -------
        int
            Number of records successfully loaded

        Raises
        ------
        RuntimeError
            If external method requires credentials but connection_name is not set
        NotImplementedError
            If database type is not supported or required driver features unavailable

        Notes
        -----
        **PostgreSQL/Redshift:**
        - Only uses direct method (COPY FROM STDIN)
        - Streaming with background writer thread, no temp files
        - If you need to use `psql \\copy`, used BulkSurge.dump() to generate transformed CSV file

        **Oracle:**
        - Direct: Uses direct_path_load (requires python-oracledb 3.4+)
        - External: Uses SQL*Loader (sqlldr) with auto-generated control file
        - External method requires named connection from config

        **MySQL/MariaDB:**
        - Direct: LOAD DATA LOCAL INFILE with streaming buffer
        - External: Checks local_infile setting, falls back to direct or dumps CSV
        - Direct method requires local_infile=1 on server

        **SQL Server:**
        - Only external method (uses bcp utility)
        - Requires named connection from config for credentials
        - Supports integrated auth (Windows) if no user/password in config

        Examples
        --------
        Direct streaming (default)::

            surge = BulkSurge(table)
            surge.load(reader)  # Streams data, zero temp files

        Oracle SQL*Loader::

            surge = BulkSurge(table)
            surge.load(reader, method='external', dump_path='/staging')

        SQL Server with bcp (requires config connection)::

            db = dbtk.connect('prod_mssql')  # Must use named connection
            table = Table('dbo.orders', columns=..., cursor=db.cursor())
            surge = BulkSurge(table)
            surge.load(reader)  # Uses bcp with credentials from config

        See Also
        --------
        dump : Export records to CSV file
        """
        db_type = self.cursor.connection.database_type.lower()
        if method == 'direct':
           if db_type in ('postgres', 'redshift'):
               return self._load_postgres_direct(records)
           elif db_type == 'oracle':
               return self._load_oracle_direct(records)
           elif db_type in ('mysql', 'mariadb'):
               return self._load_mysql_local_stream(records)
           else:
               raise NotImplementedError(f'Direct load not available for {db_type}')
        elif method == 'external':
            if db_type == 'oracle':
                return self._load_oracle_sqlldr(records, dump_path=dump_path)
            elif db_type in ('sqlserver', 'mssql'):
                return self._load_mssql_bcp(records, dump_path=dump_path)
            elif db_type in ('mysql', 'mariadb'):
                return self._load_mysql_external(records, dump_path=dump_path)
            else:
                raise NotImplementedError(f'External load not available for {db_type}')
        else:
            raise ValueError(f'Method {method} not supported. Must be either direct or external.')

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
        """
        Load data into Oracle using SQL*Loader (sqlldr) external utility.

        Dumps records to CSV, generates a control file, and invokes sqlldr with
        credentials from the named connection. Both CSV and control files are
        cleaned up after loading completes.

        Parameters
        ----------
        records : Iterable[Record]
            Records to load
        dump_path : str or Path, optional
            Path for CSV file (control file placed alongside with unique suffix)

        Returns
        -------
        int
            Number of records loaded

        Raises
        ------
        RuntimeError
            If connection was not created from named config (no connection_name)
            If sqlldr command fails

        Notes
        -----
        - Requires connection via dbtk.connect('connection_name') for credentials
        - Auto-generates control file with CHAR type for all columns
        - Uses CSV format with comma delimiter and quoted fields
        - Credentials passed via command line (sqlldr limitation)
        - Temp files (CSV + .ctl) are deleted after load completes

        Examples
        --------
        ::

            db = dbtk.connect('oracle_prod')  # Named connection required
            table = Table('schema.table_name', columns=..., cursor=db.cursor())
            surge = BulkSurge(table)
            surge.load(reader, method='external')  # Uses SQL*Loader
        """

        config = self._get_connection_config()
        user = config.get('user')
        password = config.get('password')
        db = config.get('database') or config.get('dsn')

        # Dump CSV
        csv_path = self._resolve_file_path(dump_path, 'csv')
        self.dump(records, file_name=csv_path, delimiter=',', quotechar='"')
        # dump automatically creates .ctl file if connected to Oracle
        ctl_path = self.control_path
        log_path = self.log_dir +  self.dump_path.stem + '.log'
        cmd = ['sqlldr', f'userid={user}/{password}@{db}', f'control={ctl_path}', f'log={log_path}']
        logger.debug(f'sqlldr userid={user}/<PASSWORD>@{db} control={ctl_path} log={log_path}')

        try:
            result = subprocess.run(cmd, capture_output=True, text=True)

            if result.returncode == 0:
                logger.info("SQL*Loader completed successfully")
            elif result.returncode == 2:
                logger.warning(f"SQL*Loader completed with warnings:")
            else:
                logger.error(f"SQL*Loader failed with exit code {result.returncode}:\n{result.stderr}")
                raise RuntimeError(f"sql*loader failed with exit code {result.returncode}")
            lines = result.stdout.strip().split('\n')
            msg = "sql*loader success: " + '\n'.join(lines[-6:])
            logger.info(msg)
            logger.info(f"See sql*loader log for details: {log_path}")
        except subprocess.CalledProcessError as e:
            logger.error(f"sql*loader failed: {e.stderr}")
            raise RuntimeError("sql*loader failed") from e

        csv_path.unlink(missing_ok=True)
        ctl_path.unlink(missing_ok=True)

        return self.total_loaded

    def _load_mssql_bcp(self, records: Iterable[Record],
                        dump_path: Optional[Union[str, Path]] = None) -> int:
        """
        Load data into SQL Server using bcp (bulk copy program) utility.

        Dumps records to a delimited file using ASCII Unit Separator (\\x1f)
        as the field delimiter — a control character that never appears in
        real-world data, eliminating the need for quoting or escaping. Invokes
        bcp with credentials from the named connection.

        Parameters
        ----------
        records : Iterable[Record]
            Records to load
        dump_path : str or Path, optional
            Path for the data file (directory or full path)

        Returns
        -------
        int
            Number of records loaded

        Raises
        ------
        RuntimeError
            If connection was not created from named config (no connection_name)
            If bcp command fails

        Notes
        -----
        - Requires connection via dbtk.connect('connection_name') for credentials
        - Uses ASCII Unit Separator (\\x1f) as field delimiter — no quoting needed
        - Uses ``-u`` flag (TrustServerCertificate) for ODBC Driver 18 compatibility
        - If user/password in config: uses SQL authentication (-U, -P)
        - If no user/password: uses Windows integrated auth (-T)
        - Credentials passed via command line (bcp limitation)
        - Temp data file is deleted after load completes
        - Alternative: Use DataSurge with pyodbc for fast executemany (no bcp needed)

        Examples
        --------
        SQL Authentication::

            # Config file has user/password
            db = dbtk.connect('mssql_prod')
            surge = BulkSurge(table)
            surge.load(reader)  # Uses bcp with -U/-P

        Windows Integrated Auth::

            # Config file has no user/password
            db = dbtk.connect('mssql_prod')
            surge = BulkSurge(table)
            surge.load(reader)  # Uses bcp with -T
        """
        config = self._get_connection_config()
        user = config.get('user')
        password = config.get('password')
        host = config.get('host')
        db = config.get('database')

        self.dump(
            records,
            file_name=dump_path,
            write_headers=False,
            delimiter='\x1f',  # Unit Separator — super safe
            quotechar=None,    # No quoting needed
            quoting=csv.QUOTE_NONE,
            escapechar=None    # No escaping
        )

        csv_path = self.dump_path
        cmd = ['bcp', self.table.name, 'in', str(csv_path), '-S', host, '-d', db,  '-c', '-u', '-t\x1f', '-r\\n']
        logger.debug('BCP command (minus auth):' + ' '.join(cmd))
        if user and password:
            cmd += ['-U',  user, '-P', password]
        else:
            cmd += ['-T',]  # integrated auth
        # Run BCP
        result = subprocess.run(cmd, capture_output=True, text=True)
        if result.returncode != 0:
            error_msg = f"bcp failed with exit code {result.returncode}"
            if result.stderr:
                error_msg += f"\n{result.stderr}"
            elif result.stdout:
                # Stderr empty - show last 10 lines of stdout as fallback
                lines = result.stdout.strip().split('\n')
                error_msg += f"\nstdout (last 10 lines):\n" + '\n'.join(lines[-10:])
            logger.error(error_msg)
            raise RuntimeError(error_msg)
        else:
            # On success, show summary (last 5 lines)
            if result.stdout:
                lines = result.stdout.strip().split('\n')
                logger.info("bcp completed:\n" + '\n'.join(lines[-5:]))
        csv_path.unlink(missing_ok=True)
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
        """
        Load MySQL data with automatic method selection based on server configuration.

        Checks the server's local_infile setting and either streams data directly
        using LOAD DATA LOCAL INFILE or dumps to CSV with instructions for manual loading.

        Parameters
        ----------
        records : Iterable[Record]
            Records to load
        dump_path : str or Path, optional
            Path for CSV dump if local_infile is disabled

        Returns
        -------
        int
            Number of records loaded

        Notes
        -----
        If local_infile=1: streams data with zero temp files using _load_mysql_local_stream()
        If local_infile=0: dumps CSV and logs instructions for manual LOAD DATA INFILE
        """
        self.cursor.execute("SELECT @@local_infile")
        if self.cursor.fetchone()[0] == 1:
            # Streaming with DequeBuffer instead of file
            return self._load_mysql_local_stream(records)
        else:
            csv_path = self._resolve_file_path(dump_path, 'csv')
            self.dump(records, file_name=csv_path)
            logger.info(
                f"local_infile is OFF on server. CSV dumped to {csv_path}. "
                "To load manually (server-side file):\n"
                f"LOAD DATA INFILE '{csv_path}' INTO TABLE {self.table.name} "
                f"FIELDS TERMINATED BY ',' ENCLOSED BY '\"' IGNORE 1 LINES;"
            )
            return self.total_loaded

    def _generate_control_file(self, csv_path: Path, write_headers: bool =True) -> Path:
        """
        Generate SQL*Loader control file for Oracle.

        Parameters
        ----------
        csv_path : Path
            Path to the CSV data file
        write_headers : bool, optional
            If headers were written to data file, the first row will be skipped.

        Returns
        -------
        Path
            Path to the generated control file

        Notes
        -----
        Control file is placed alongside the CSV with .ctl extension.
        Uses CHAR type for all columns with CSV format.
        """
        ctl_path = csv_path.with_name(f"{csv_path.stem}.ctl")
        # Generate control file from current Table schema
        cols = ',\n        '.join(f"{col} CHAR" for col in self._get_columns('insert'))
        ctl_content = dedent(f"""\
        OPTIONS (DIRECT=TRUE, ROWS={self.batch_size}, SKIP={int(write_headers)})
        LOAD DATA
        INFILE '{csv_path.absolute()}'
        BADFILE '{csv_path.with_suffix(".bad").absolute()}'
        DISCARDFILE '{csv_path.with_suffix(".dsc").absolute()}'
        INTO TABLE {self.table.name}
        FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"'
        TRAILING NULLCOLS(
        {cols})
        """)
        ctl_path.write_text(ctl_content)
        self.control_path = ctl_path
        return ctl_path

    def dump(self, records: Iterable[Record],
             file_name: str = None,
             write_headers: bool = True,
             delimiter: str = ",",
             encoding: str = 'utf-8',
             **csv_args) -> int:
        """
        Export records to a delimited file.

        Resolves the output path, writes all records via CSVWriter, and sets
        ``self.dump_path`` to the resolved path for callers that need it.
        For Oracle connections, automatically generates a SQL*Loader control
        file alongside the data file.

        Parameters
        ----------
        records : Iterable[Record]
            Records to export
        file_name : str or Path, optional
            Target file path (directory or full path). See _resolve_file_path
            for resolution priority.
        write_headers : bool, optional
            Include column headers (default: True)
        delimiter : str, optional
            Field delimiter character (default: ','). Extension is inferred:
            '\\t' → .tsv, anything else → .csv
        encoding : str, optional
            File encoding (default: 'utf-8')
        **csv_args : optional
            Additional keyword arguments passed to csv.writer (e.g.
            quoting, quotechar, escapechar)

        Returns
        -------
        int
            Number of records written

        Side Effects
        ------------
        Sets ``self.dump_path`` to the resolved output Path.

        Notes
        -----
        **Oracle Auto-generation:**
        When connected to Oracle, automatically generates a SQL*Loader
        control file (.ctl) alongside the data file and logs the sqlldr
        command to run.

        Examples
        --------
        Export with auto-generated Oracle control file::

            db = dbtk.connect('oracle_prod')
            surge = BulkSurge(table)
            surge.dump(reader, '/staging/export.csv')
            # Creates: /staging/export.csv + /staging/export_a1b2c3d4.ctl
            # Logs sqlldr command, e.g.:
            #   sqlldr userid=USER/PASS@DB control=... data=...

        Custom delimiter with no quoting (e.g. for bcp)::

            surge.dump(reader, '/staging/data.csv', delimiter='\\x1f',
                       quoting=csv.QUOTE_NONE, escapechar=None)
        """
        ext = '.tsv' if delimiter == '\t' else '.csv'
        headers = self._get_columns('insert')
        logger.debug(f'Dump column headers: {headers}')
        dump_path = self._resolve_file_path(file_name, extension=ext)
        self.dump_path = dump_path
        with open(dump_path, "w", encoding=encoding, newline='') as fp:
            writer = CSVWriter(data=None,
                               file=fp,
                               write_headers=write_headers,
                               headers=headers,
                               delimiter=delimiter, **csv_args)
            for batch in self.batched(records):
                writer.write_batch(batch)

        logger.info(f"Dumped {self.total_loaded:,} records to {dump_path}")

        # Oracle: auto-generate control file and provide sqlldr command
        db_type = self.cursor.connection.database_type.lower()
        if 'oracle' in db_type:
            ctl_path = self._generate_control_file(dump_path, write_headers=write_headers)
            logger.info(f"Generated SQL*Loader control file: {ctl_path}")

            # Show sqlldr command with placeholders
            if self.cursor.connection.connection_name:
                logger.info(
                    f"To load with SQL*Loader:\n"
                    f"  sqlldr userid=USER/PASS@DB control={ctl_path} data={dump_path}\n"
                    "   (or use DataSurge.load(method='external') for automatic  loading"
                )
            else:
                logger.info(
                    f"To load with SQL*Loader:\n"
                    f"  sqlldr userid=USER/PASS@DB control={ctl_path} data={dump_path}"
                )

        return self.total_loaded