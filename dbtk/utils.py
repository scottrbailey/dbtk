# dbtk/utils.py
"""
Utility functions for dbtk.
"""

import logging
import re
import shutil
import datetime as dt
from pathlib import Path
from typing import Tuple, List, Any, Union, Dict, Iterable, Optional
from .defaults import settings
try:
    from typing import Mapping
except ImportError:
    from collections.abc import Mapping

logger = logging.getLogger(__name__)

MIDNIGHT = dt.time(0, 0, 0)
# cache format strings for performance
_format_cache = None


class ErrorDetail:
    """
    Structured error record for ETL and database operations.

    Captures a single error with optional field attribution and driver-specific
    error code. Used by :class:`dbtk.etl.table.Table` (``last_error``) and
    :class:`dbtk.etl.managers.IdentityManager` (per-entity ``_errors`` list),
    and round-trips cleanly through JSON via ``save_state`` / ``load_state``.

    Attributes
    ----------
    message : str
        Human-readable description of the error.
    field : str, optional
        Name of the source or target field the error is associated with.
        ``None`` when the error is not specific to a single field.
    code : str, optional
        Database- or application-level error code (e.g. ``pgcode`` from
        psycopg2, an ORA- number, or a custom application string).
        ``None`` when no structured code is available.
    """

    __slots__ = ("message", "field", "code")

    def __init__(self, message: str, field: str = None, code: str = None):
        """
        Create an ErrorDetail.

        Parameters
        ----------
        message : str
            Human-readable description of the error.
        field : str, optional
            Field the error relates to, or ``None``.
        code : str, optional
            Structured error code, or ``None``.
        """
        self.message = message
        self.field = field
        self.code = code

    def __repr__(self) -> str:
        return f"ErrorDetail(message={self.message!r}, field={self.field!r}, code={self.code!r})"


class ParamStyle:
    """
    SQL parameter placeholder styles for different database drivers.

    Different database drivers use different parameter placeholder formats.
    This class provides constants and utilities for working with these formats:

    - QMARK: Question mark placeholders (?, ?) - SQLite, ODBC
    - NUMERIC: Numeric placeholders (:1, :2) - Oracle
    - NAMED: Named placeholders (:name, :email) - Oracle, psycopg2
    - FORMAT: Printf-style (%s, %s) - MySQL (MySQLdb)
    - PYFORMAT: Python format (%(name)s) - psycopg2, pymysql

    Example
    -------
    ::
        >>> ParamStyle.get_placeholder('qmark')
        '?'
        >>> ParamStyle.get_placeholder('named')
        ':1'
    """
    QMARK = 'qmark'         # id = ?
    NUMERIC = 'numeric'     # id = :1
    NAMED = 'named'         # id = :id  also :1 for positional
    FORMAT = 'format'       # id = %s
    PYFORMAT = 'pyformat'   # id = %(id)s also %s for positional
    DEFAULT = NAMED

    @classmethod
    def values(cls):
        """ Return all available parameter placeholder styles. """
        return [getattr(cls, attr) for attr in dir(cls) if not attr.startswith('_')]

    @classmethod
    def positional_styles(cls):
        """ Parameter styles where parameters must be in properly ordered tuple instead of dict"""
        return (cls.QMARK, cls.NUMERIC, cls.FORMAT)

    @classmethod
    def named_styles(cls):
        """ Parameter styles where parameters must be in dict instead of tuple"""
        return (cls.NAMED, cls.PYFORMAT)

    @classmethod
    def get_positional_style(cls, paramstyle: str) -> str:
        """ Return a positional paramstyle, mapping named style to the corresponding positional style if needed. """
        if paramstyle == cls.NAMED:
            return cls.NUMERIC
        elif paramstyle == cls.PYFORMAT:
            return cls.FORMAT
        else:
            return paramstyle

    @classmethod
    def get_placeholder(cls, paramstyle: str) -> str:
        """ Return the placeholder string used for a single bind variable in this style. """
        if paramstyle == cls.QMARK:
            return '?'
        elif paramstyle == cls.FORMAT:
            return '%s'
        elif paramstyle == cls.NUMERIC:
            return ':1'
        elif paramstyle == cls.NAMED:
            # adapters that use named parameters can also use :1 for positional parameters
            return ':1'
        elif paramstyle == cls.PYFORMAT:
            # adapters that use pyformat parameters can also use %s for positional parameters
            return '%s'
        return ''


class FixedColumn(object):
    """ Column definition for fixed width files """

    def __init__(self, name:str, start_pos:int, end_pos:int=None,
                 column_type:str='text',
                 comment: Optional[str] = None,
                 align: Optional[str] = None,
                 pad_char: Optional[str] = None,
                 width: int = None):
        """
        :param str name:  database column name
        :param int start_pos: start position of field, first position is 1 not 0
        :param int end_pos: end position of field (mutually exclusive with width)
        :param str column_type: text, int, float, date
        :param str comment: discription for column usage/options
        :param str align: override alignment (left, right, center)
        :param str pad_char: override pad character
        :param int width: field width in characters (mutually exclusive with end_pos)

        FixedColumn('birthdate', 25, 35, 'date')
        FixedColumn('birthdate', 25, width=11, column_type='date')
        """
        if end_pos is not None and width is not None:
            raise ValueError("Specify end_pos or width, not both")
        if width is not None:
            end_pos = start_pos + width - 1

        align_map = {'left': 'left', 'l': 'left', '<': 'left',
                     'right': 'right', 'r': 'right', '>': 'right',
                     'center': 'center', 'c': 'center'}

        self.name = name
        self.start_pos = start_pos
        self.end_pos = end_pos if end_pos else start_pos
        self.column_type = column_type
        self.start_idx = start_pos - 1
        self.comment = comment
        self.align = align_map.get(str(align).lower())
        self.pad_char = pad_char[0] if pad_char else None

    @property
    def width(self) -> int:
        return self.end_pos - self.start_pos + 1

    def __repr__(self):
        parts = [f"'{self.name}'", str(self.start_pos), str(self.end_pos), f"'{self.column_type}'"]
        if self.comment:
            parts.append(f"comment='{self.comment}'")
        if self.align:
            parts.append(f"align='{self.align}'")
        if self.pad_char:
            parts.append(f"pad_char='{self.pad_char}'")
        return f"FixedColumn({', '.join(parts)})"

class QueryLogger:
    """Simple query logger."""

    def __init__(self, level: int = logging.INFO):
        """
        Initialize query logger.

        Args:
            level: Logging level
        """
        self.logger = logging.getLogger('dbtk.queries')
        self.logger.setLevel(level)

        if not self.logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
            )
            handler.setFormatter(formatter)
            self.logger.addHandler(handler)

    def __call__(self, connection: str, query: str, params: Any = None) -> None:
        """
        Log a query.

        Args:
            connection: Connection identifier
            query: SQL query
            params: Query parameters
        """
        message = f"[{connection}] {query}"
        if params:
            message += f" | Params: {params}"

        self.logger.info(message)

def _build_format_strings():
    """Build format strings for datetime and date objects."""
    return {
        'date': settings.get('date_format', '%Y-%m-%d'),
        'datetime': settings.get('datetime_format', '%Y-%m-%d %H:%M:%S'),
        'datetime_tz': settings.get('datetime_format', '%Y-%m-%d %H:%M:%S') + \
                       settings.get('tz_suffix',  '%z'),
        'timestamp': settings.get('timestamp_format', '%Y-%m-%d %H:%M:%S.%f'),
        'timestamp_tz': settings.get('timestamp_format', '%Y-%m-%d %H:%M:%S.%f') + \
                        settings.get('tz_suffix',  '%z'),
        'time': settings.get('time_format', '%H:%M:%S'),
        'time_micro': settings.get('time_format', '%H:%M:%S') + '.%f',
        'time_tz': settings.get('time_format', '%H:%M:%S') + \
                   settings.get('tz_suffix',  '%z'),
        'null': settings.get('null_string', ''),
    }

def reset_format_cache():
    """Clear format cache to force rebuilding on next call."""
    global _format_cache
    _format_cache = None

def _get_format_strings():
    global _format_cache
    if _format_cache is None:
        _format_cache = _build_format_strings()
    return _format_cache

def to_string(obj: Any) -> str:
    """
    Convert a value to string representation.

    Args:
        obj: Value to convert

    Returns:
        String representation
    """
    fmts = _get_format_strings()
    if obj is None:
        return fmts['null']
    elif isinstance(obj, dt.datetime):
        if obj.microsecond:
            if obj.tzinfo:
                return obj.strftime(fmts['timestamp_tz'])
            else:
                return obj.strftime(fmts['timestamp'])
        else:
            if obj.tzinfo:
                return obj.strftime(fmts['datetime_tz'])
            if obj.time() == MIDNIGHT:
                return obj.strftime(fmts['date'])
            else:
                return obj.strftime(fmts['datetime'])
    elif isinstance(obj, dt.date):
        return obj.strftime(fmts['date'])
    elif isinstance(obj, dt.time):
        if obj.microsecond:
            if obj.tzinfo:
                return obj.strftime(fmts['time_tz'])
            else:
                return obj.strftime(fmts['time_micro'])
        else:
            if obj.tzinfo:
                return obj.strftime(fmts['time_tz'])
            else:
                return obj.strftime(fmts['time'])
    elif isinstance(obj, (int, float)):
        return str(obj)
    elif isinstance(obj, str):
        return obj
    elif hasattr(obj, 'read'):
        # Handle LOB objects
        return str(obj.read())
    else:
        return str(obj)

def wrap_at_comma(text: str) -> str:
    """Wrap text at commas, avoiding breaks inside parentheses."""
    # Split on parentheses to identify protected regions
    parts = re.split(r'(\([^)]*\))', text)

    wrapped_parts = []
    for i, part in enumerate(parts):
        if i % 2 == 0:  # Outside parentheses
            wrapped = re.sub(r'(.{70}[^,]*), ', r'\1,\n    ', part)
            wrapped_parts.append(wrapped)
        else:  # Inside parentheses - don't wrap
            wrapped_parts.append(part)

    return ''.join(wrapped_parts)


# Matches single-quoted string literals (with '' as an escaped quote), line
# comments, and block comments, so bind-parameter scanning can skip over
# them - e.g. the ':FMMIAM' inside to_char(dt, 'FMHH:FMMIAM') is text, not a
# bind placeholder.
_SQL_LITERAL_RE = re.compile(r"'(?:[^']|'')*'|--[^\n]*|/\*.*?\*/", re.DOTALL)


def _split_sql_segments(sql: str) -> List[Tuple[str, bool]]:
    """Split SQL into (text, is_code) segments, isolating string literals and comments."""
    segments = []
    pos = 0
    for m in _SQL_LITERAL_RE.finditer(sql):
        if m.start() > pos:
            segments.append((sql[pos:m.start()], True))
        segments.append((m.group(), False))
        pos = m.end()
    if pos < len(sql):
        segments.append((sql[pos:], True))
    return segments


def process_sql_parameters(sql: str, paramstyle: str) -> Tuple[str, Tuple[str, ...]]:
    """
    Process SQL parameters according to the specified paramstyle.

    Supports SQL input in either 'named' (:param) or 'pyformat' (%(param)s) format.
    Auto-detects the input format and converts to the target paramstyle.
    String literals and comments are ignored when detecting and substituting
    parameters, so text such as ``to_char(dt, 'FMHH:FMMIAM')`` is left untouched.

    Parameters:
        sql: SQL query with named (:name) or pyformat (%(name)s) parameters
        paramstyle: The desired parameter style for the resulting SQL string

    Returns:
        A tuple containing the processed SQL query string and a tuple of all named parameters
        extracted in the order in which they appear in the original query.

    Raises:
        ValueError: If the SQL contains mixed parameter formats or unsupported paramstyle.

    Examples:
        >>> # Named input, convert to pyformat
        >>> process_sql_parameters("SELECT * FROM users WHERE id = :user_id", "pyformat")
        ("SELECT * FROM users WHERE id = %(user_id)s", ('user_id',))

        >>> # Pyformat input, convert to qmark
        >>> process_sql_parameters("SELECT * FROM users WHERE id = %(user_id)s", "qmark")
        ("SELECT * FROM users WHERE id = ?", ('user_id',))
    """
    segments = _split_sql_segments(sql)
    code = ''.join(text for text, is_code in segments if is_code)

    # Detect input format
    # Use negative lookbehind (?<![:\w]) to exclude PostgreSQL :: cast syntax
    # and colons glued to a preceding identifier/number (e.g. a stray format
    # mask like 'FMHH:FMMIAM' that wasn't already stripped as a literal).
    has_named = bool(re.search(r'(?<![:\w]):(\w+)', code))
    has_pyformat = bool(re.search(r'%\((\w+)\)s', code))

    if has_named and has_pyformat:
        raise ValueError(
            "SQL contains mixed parameter formats. "
            "Use either :named or %(pyformat)s, not both."
        )

    # Determine source pattern
    if has_pyformat:
        source_pattern = r'%\((\w+)\)s'
    elif has_named:
        source_pattern = r'(?<![:\w]):(\w+)'
    else:
        # No parameters found - return as-is
        return sql, tuple()

    # Extract parameter names using detected pattern
    param_names = tuple(re.findall(source_pattern, code))

    # Convert to target paramstyle
    if paramstyle == ParamStyle.NAMED:
        replacement = r':\1'
    elif paramstyle == ParamStyle.PYFORMAT:
        replacement = r'%(\1)s'
    elif paramstyle == ParamStyle.QMARK:
        replacement = r'?'
    elif paramstyle == ParamStyle.FORMAT:
        replacement = r'%s'
    elif paramstyle == ParamStyle.NUMERIC:
        counter = iter(range(1, len(param_names) + 1))
        replacement = lambda m: f':{next(counter)}'
    else:
        raise ValueError(f"Unsupported paramstyle: {paramstyle}")

    sql = ''.join(
        re.sub(source_pattern, replacement, text) if is_code else text
        for text, is_code in segments
    )

    return sql, param_names

def validate_identifier(identifier: str, max_length: int = 64, allow_temp: bool = False) -> str:
    """
    Validate that an identifier is safe for use (even if it needs quoting).
    Returns the identifier if valid, raises ValueError if invalid.

    Args:
        identifier: The identifier to validate
        max_length: Maximum length for identifier
        allow_temp: If True, allow underscore or hash prefix for temp tables
    """
    if '.' in identifier:
        # Split and recursively validate each part
        parts = identifier.split('.')
        validated_parts = [validate_identifier(part, max_length, allow_temp) for part in parts]
        return '.'.join(validated_parts)

    # Single identifier validation
    if not identifier:
        raise ValueError("Invalid identifier: cannot be empty")
    if not identifier[0].isalpha():
        # Allow underscore or hash prefix for temp tables
        if allow_temp and identifier[0] in ('_', '#'):
            pass  # Valid temp table prefix
        else:
            raise ValueError(f"Invalid identifier: must start with a letter: {identifier}")
    if len(identifier) > max_length:
        raise ValueError(f"Invalid identifier: exceeds max length of {max_length}")

    # Check for characters/sequences that could enable injection or break SQL parsing
    dangerous_patterns = ['\x00', '\n', '\r', '"', ';', '\x1a', '--', '/*', '*/']
    for pattern in dangerous_patterns:
        if pattern in identifier:
            raise ValueError(f"Invalid identifier: contains dangerous pattern '{pattern}': {identifier}")

    if identifier.startswith(' ') or identifier.endswith(' '):
        raise ValueError(f"Invalid identifier: has leading/trailing spaces: {identifier}")

    return identifier


def identifier_needs_quoting(identifier: str) -> bool:
    """Check if identifier needs quoting."""
    return not re.match(r'^([a-z][a-z0-9_]*|[A-Z][A-Z0-9_]*)$', identifier)


def quote_identifier(identifier: str) -> str:
    """Quote identifier, handling qualified names by splitting on dots."""
    if '.' in identifier:
        # Split and recursively quote each part
        parts = identifier.split('.')
        quoted_parts = [quote_identifier(part) for part in parts]
        return '.'.join(quoted_parts)

    # Single identifier quoting
    if identifier_needs_quoting(identifier):
        return f'"{identifier}"'
    else:
        return identifier

def sanitize_identifier(name: str, idx: int = 0) -> str:
    """Sanitize an identifier/column name."""
    if name is None or name == '':
        return f'col_{idx + 1}'

    # Replace non-alphanumeric chars with underscore, collapse multiple underscores
    sanitized = re.sub(r'[^a-z0-9_]+', '_', name.lower())

    # Ensure it starts with a letter
    if not sanitized[0].isalpha():
        sanitized = 'col_' + sanitized

    # Remove trailing underscore if present
    return sanitized.rstrip('_')


def normalize_field_name(name: str) -> str:
    """
    Normalize field name for attribute access.

    Converts to lowercase, replaces non-alphanumeric characters with underscores,
    collapses consecutive underscores, and strips leading/trailing underscores.
    A single leading underscore is preserved only if the original name explicitly
    started with one — underscores synthesized from other leading characters (e.g.
    ``$``, ``#``) are stripped.

    Args:
        name: Original field name

    Returns:
        Normalized field name suitable for Python attribute access

    Examples:
        >>> normalize_field_name('Start Year')
        'start_year'
        >>> normalize_field_name('Start Year!')
        'start_year'
        >>> normalize_field_name('!Status')
        'status'
        >>> normalize_field_name('_Secret_Code!')
        '_secret_code'
        >>> normalize_field_name('$Secret_Code!')
        'secret_code'
        >>> normalize_field_name('__id__')
        '_id'
        >>> normalize_field_name('_row_num')
        '_row_num'
        >>> normalize_field_name('#Term Code')
        'term_code'
        >>> normalize_field_name('2025 Sales')
        'n2025_sales'
    """
    if not name:
        return 'col'

    # 1. Lowercase and strip whitespace
    name = str(name).lower().strip()

    # 2. Note whether the original starts with an explicit underscore
    had_leading_underscore = name.startswith('_')

    # 3. Replace all non-alphanumeric chars with single underscores
    name = re.sub(r'[^a-z0-9]+', '_', name)

    # 4. Strip ALL leading and trailing underscores
    name = name.strip('_')

    # 5. Restore a single leading underscore if the original had one
    if had_leading_underscore:
        name = '_' + name

    # 6. Attributes can't start with a digit
    if name and name[0].isdigit():
        name = 'n' + name

    # 7. Ensure not empty
    return name or 'col'

def batch_iterable(iterable: Iterable[Any], batch_size: int) -> Iterable[List[Any]]:
    """
    Batch an iterable into chunks of specified size.

    Args:
        iterable: The iterable to batch
        batch_size: Size of each batch

    Yields:
        Lists of items up to batch_size length
    """
    import itertools

    iterator = iter(iterable)
    while True:
        batch = list(itertools.islice(iterator, batch_size))
        if not batch:
            break
        yield batch


def expire_files(
    src_dir: str,
    days_old: int,
    pattern: str = '*',
    archive_dir: Optional[str] = None,
    dry_run: bool = False
) -> List[str]:
    """
    Delete or archive files older than a given age.

    Common interface pattern: inbound/processed files need to be cleared out
    (or moved to an archive location) after they've aged past some retention
    window. Only regular files matching `pattern` are considered; matching
    subdirectories are left alone.

    Args:
        src_dir: Directory to scan for aged files.
        days_old: Files with an mtime older than this many days are affected.
        pattern: Glob pattern to select files within `src_dir` (default `'*'`,
            i.e. all files).
        archive_dir: If given, matching files are moved here instead of
            deleted. Created if it doesn't exist. If a file of the same name
            already exists there, the source file is left in place (skipped).
        dry_run: If True, only report what would be moved/deleted without
            actually doing it.

    Returns:
        Paths (as they were in `src_dir`) of files that were deleted or
        moved (or would be, if dry_run).

    Example
    -------
    ::

        import dbtk

        # Delete files untouched for 90+ days
        dbtk.utils.expire_files('/data/inbound', days_old=90)

        # Archive CSVs older than 30 days instead of deleting them
        dbtk.utils.expire_files('/data/inbound', days_old=30,
                                 archive_dir='/data/archive', pattern='*.csv')

        # See what would happen first
        dbtk.utils.expire_files('/data/inbound', days_old=30, dry_run=True)
    """
    src_path = Path(src_dir).expanduser()
    if not src_path.exists():
        logger.warning(f"Source directory does not exist: {src_path}")
        return []

    cutoff = dt.datetime.now() - dt.timedelta(days=days_old)
    archive_path = Path(archive_dir).expanduser() if archive_dir else None
    affected = []

    for file in src_path.glob(pattern):
        if not file.is_file():
            continue

        file_mtime = dt.datetime.fromtimestamp(file.stat().st_mtime)
        if file_mtime >= cutoff:
            continue

        if archive_path:
            dest = archive_path / file.name
            if dest.exists():
                logger.warning(f"Skipping {file}: {dest} already exists in archive")
                continue
            if dry_run:
                logger.info(f"Would move: {file} -> {dest}")
            else:
                archive_path.mkdir(parents=True, exist_ok=True)
                try:
                    shutil.move(str(file), str(dest))
                    logger.info(f"Archived: {file} -> {dest}")
                except OSError as e:
                    logger.warning(f"Failed to archive {file}: {e}")
                    continue
        else:
            if dry_run:
                logger.info(f"Would delete: {file}")
            else:
                try:
                    file.unlink()
                    logger.info(f"Deleted: {file}")
                except OSError as e:
                    logger.warning(f"Failed to delete {file}: {e}")
                    continue

        affected.append(str(file))

    return affected


# For Python 3.6 compatibility, define type aliases
RecordLike = Union[Dict[str, Any], Mapping]