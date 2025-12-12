# dbtk/etl/bulk_surge.py
import datetime as dt
import io
import logging
import subprocess
import os
import tempfile
from pathlib import Path
from io import TextIOWrapper
from textwrap import dedent
from typing import Iterable
from ..defaults import settings
from .base_surge import BaseSurge
from ..writers.csv import CSVWriter, csv
from ..record import Record


logger = logging.getLogger(__name__)


class BulkSurge(BaseSurge):
    """
    Lightning-fast native bulk loading using COPY, bcp, sqlldr, etc.
    Zero temp files when possible. Streaming. Memory-safe.
    """

    def __init__(self, table, batch_size: int = 50_000, operation: str = "insert"):
        super().__init__(table, batch_size=batch_size, operation=operation, param_mode="positional")
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
        elif "sqlserver" in db_type or "mssql" in db_type:
            return self._load_sqlserver(records)
        elif "oracle" in db_type:
            return self._load_oracle(records)
        elif "mysql" in db_type or "maria" in db_type:
            return self._load_mysql(records)
        else:
            raise NotImplementedError(f"BulkSurge not supported for {db_type}")

    def _load_postgres(self, records: Iterable[Record]) -> int:
        cols = ", ".join(self.table.columns.keys())
        sql = f"COPY {self.table.name} ({cols}) FROM STDIN WITH (FORMAT csv, NULL '\\N', ESCAPE '\\')"
        batch_iter = self.batched(records)
        with TextIOWrapper(io.StringIO(), encoding="utf-8") as text_stream:
            writer = CSVWriter(data=next(batch_iter), file=text_stream, include_headers=False, null_string='\\N')
            text_stream.seek(0)
            self.cursor.copy_expert(sql, text_stream)
            for batch in batch_iter:
                writer.write_batch(batch)
        return self.total_loaded

    def _load_sqlserver(self, records: Iterable[Record]) -> int:
        conn = self.cursor.connection
        cmd = [
            "bcp", f"{conn.database}.dbo.{self.table.name}", "in", "-",
            "-S", conn.server or "(local)",
            "-d", conn.database,
            "-c", "-t,", "-r\n",
        ]
        if getattr(conn, "username", None):
            cmd += ["-U", conn.username, "-P", conn.password or ""]
        else:
            cmd.append("-T")

        proc = subprocess.Popen(cmd, stdin=subprocess.PIPE, text=False, bufsize=0)
        wrapper = TextIOWrapper(proc.stdin, encoding="utf-8", line_buffering=True)

        try:
            writer = CSVWriter(
                data=self._yield_valid_records(records),
                file=wrapper,
                include_headers=False,
                delimiter=",",
            )
            writer.write()
            wrapper.flush()
        finally:
            wrapper.detach()
            proc.stdin.close()

        rc = proc.wait()
        if rc != 0:
            raise RuntimeError(f"bcp failed with return code {rc}")
        return self.total_loaded

    def _load_oracle(self, records: Iterable[Record]) -> int:
        if os.name == "nt":
            raise NotImplementedError("Oracle on Windows requires sqlldr + control file (temp file fallback)")
        pipe_path = f"/tmp/oracle_bulk_{os.getpid()}_{id(self)}.pipe"
        os.mkfifo(pipe_path)

        ctl = dedent(f"""\
        LOAD DATA
        INFILE '{pipe_path}' "str '\\n'"
        INTO TABLE {self.table.name}
        FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"'
        TRAILING NULLCOLS
        ({", ".join(self.table.columns.keys())})
        """)
        ctl_path = f"/tmp/oracle_bulk_{os.getpid()}.ctl"
        with open(ctl_path, "w") as f:
            f.write(ctl.strip())

        conn_str = f"{self.cursor.connection.username}/{self.cursor.connection.password}@{self.cursor.connection.dsn}"
        proc = subprocess.Popen([
            "sqlldr", conn_str, f"control={ctl_path}", "direct=true", "errors=0", "silent=header,feedback"
        ])

        try:
            with open(pipe_path, "w", encoding="utf-8") as pipe:
                writer = CSVWriter(
                    data=self._yield_valid_records(records),
                    file=pipe,
                    include_headers=False,
                    delimiter=",",
                )
                writer.write()
        finally:
            os.unlink(pipe_path)
            os.unlink(ctl_path)

        rc = proc.wait()
        if rc != 0:
            raise RuntimeError(f"sqlldr failed with code {rc}")
        return self.total_loaded

    def _load_mysql(self, records: Iterable[Record]) -> int:
        conn = self.cursor.connection
        cmd = [
            "mysql",
            f"--host={conn.host}", f"--port={conn.port or 3306}",
            f"--user={conn.username}", f"--password={conn.password or ''}",
            "--local-infile=1",
            "--silent",
            conn.database,
            "-e", f"LOAD DATA LOCAL INFILE '-' INTO TABLE {self.table.name} "
                  f"FIELDS TERMINATED BY ',' ENCLOSED BY '\"' LINES TERMINATED BY '\\n'"
        ]
        proc = subprocess.Popen(cmd, stdin=subprocess.PIPE, text=True)

        try:
            writer = CSVWriter(
                data=self._yield_valid_records(records),
                file=proc.stdin,
                include_headers=False,
                delimiter=",",
            )
            writer.write()
        finally:
            proc.stdin.close()

        rc = proc.wait()
        if rc != 0:
            raise RuntimeError(f"mysql LOAD DATA failed with code {rc}")
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
             include_headers: bool = True,
             delimiter: str = ",",
             quotechar: str = '"',
             encoding: str = 'utf-8-sig') -> int:
        path = self._resolve_file_path(file_name)
        with open(path, "w", encoding=encoding, newline='') as fp:
            writer = CSVWriter(data=None, file=fp, include_headers=include_headers, null_string='\\N',
                               delimiter=delimiter, quotechar=quotechar)
            for batch in self.batched(records):
                writer.write_batch(batch)
        logger.info(f"Dumped {self.total_loaded} records to {path}")
        return self.total_loaded