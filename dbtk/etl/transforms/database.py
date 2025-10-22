# dbtk/etl/transforms/database.py
"""
Database-backed validation and lookup classes.

These classes require a database cursor and interact with database
tables for validation and code translation operations.
"""

from typing import Any

from ...defaults import settings
from ...utils import ParamStyle, quote_identifier, validate_identifier
from ...cursors import DictCursor


class CodeValidator:
    """
    CodeValidator is a class designed to validate input values against a specified column in a database table.

    This class provides efficient methods for validating values by querying the database or using preloaded
    valid codes, depending on the size of the dataset. It supports optional case sensitivity and uses
    a configurable threshold to determine if the codes should be preloaded into memory. The validation logic
    is handled dynamically based on the database's paramstyle.

    Examples:
        temple_validator = CodeValidator(cursor, table='temples', column='temple_id', case_sensitive=False)

        air_bender_table = Table('air_benders', {
                                'temple': {'field': 'home_temple', 'nullable': False, 'fn': temple_validator}},
                                cursor=cursor)
    """

    PRELOAD_THRESHOLD = 1000

    def __init__(self, cursor, table: str, column: str,
                 case_sensitive: bool = False):
        """
        Initializes a new instance of the validator class by setting up required table and column names,
        and optionally enabling case sensitivity. Automatically determines whether to preload all valid
        codes based on the size of the table and a configurable threshold.

        Parameters:
            cursor (cursor): A database cursor instance used to execute queries.
            table (str): The name of the table to validate against.
            column (str): The name of the column to validate against.
            case_sensitive (bool): Whether the validation is case-sensitive.
        """
        if isinstance(cursor, DictCursor):
            self._cursor = cursor.connection.cursor()
        else:
            self._cursor = cursor
        validate_identifier(table)
        validate_identifier(column)
        self.table = quote_identifier(table)
        self.column = quote_identifier(column)
        self.case_sensitive = case_sensitive
        self.threshold = settings.get('validator_preload_threshold', self.PRELOAD_THRESHOLD)
        self._valid_codes = set()
        self._preloaded = False

        # Get paramstyle from connection
        self._paramstyle = self._cursor.connection.interface.paramstyle
        self._placeholder = ParamStyle.get_placeholder(self._paramstyle)

        # Auto-decide whether to preload based on table size
        row_count = self._get_row_count()
        if row_count <= self.threshold:
            self._load_all_codes()
            self._preloaded = True

    def _get_row_count(self) -> int:
        """Get count of distinct values in validation column."""
        sql = f"SELECT COUNT(DISTINCT {self.column}) FROM {self.table}"
        self._cursor.execute(sql)
        return self._cursor.fetchone()[0]

    def _load_all_codes(self):
        """Preload all valid codes into memory."""
        sql = f"SELECT DISTINCT {self.column} FROM {self.table}"
        self._cursor.execute(sql)
        # Filter out None and empty strings, handle case sensitivity
        codes = {row[0] for row in self._cursor.fetchall()
                 if row[0] not in (None, '')}
        self._valid_codes = {c if self.case_sensitive else str(c).upper() for c in codes}

    def _is_valid(self, value) -> bool:
        """Check if value is valid."""
        check_val = value if self.case_sensitive else str(value).upper()

        if self._preloaded:
            return check_val in self._valid_codes

        if check_val in self._valid_codes:
            return True

        # Query database using correct paramstyle
        sql = f"SELECT {self.column} FROM {self.table} WHERE {self.column} = {self._placeholder}"

        if self._paramstyle in ParamStyle.positional_styles():
            self._cursor.execute(sql, (value,))
        else:
            self._cursor.execute(sql, {'val': value})

        if self._cursor.fetchone():
            self._valid_codes.add(check_val)
            return True

        return False

    def __call__(self, value):
        """Validate value against database table."""
        if value is None or value == '':
            return value
        return value if self._is_valid(value) else None


class CodeLookup:
    """
    Lookup/translate codes using a database reference table.

    Automatically decides whether to preload based on table size.

    Examples:
        # State abbreviation to full name
        state_lookup = CodeLookup(cursor, 'states', 'abbrev', 'full_name')
        state_lookup('CA')  # -> 'California'

        # Country code to name
        country_lookup = CodeLookup(cursor, 'countries', 'iso_code', 'name')

        # Use in table config
        table = Table('citizens', {
            'state_name': {'field': 'state_code', 'fn': state_lookup}
        }, cursor=cursor)
    """

    PRELOAD_THRESHOLD = 500

    def __init__(self, cursor, table: str, from_column: str, to_column: str,
                 case_sensitive: bool = False, default: Any = None):
        """
        Initializes an instance of a lookup system which maps values from one column to another
        within a specific database table, using a provided database cursor for operation.

        Parameters:
            cursor: A database cursor instance used to execute queries.
            table (str): The name of the database table being accessed.
            from_column (str): The source column being used as the key.
            to_column (str): The target column being used to retrieve mapped values.
            case_sensitive (bool): Optional; specifies if the lookup is case-sensitive. Defaults to False.
            default (Any): Optional; the default value returned if no match is located in the table.
        """
        # Create new RecordCursor if passed a DictCursor
        if isinstance(cursor, DictCursor):
            self._cursor = cursor.connection.cursor()
        else:
            self._cursor = cursor

        # Validate and quote identifiers
        validate_identifier(table)
        validate_identifier(from_column)
        validate_identifier(to_column)
        self.table = quote_identifier(table)
        self.from_column = quote_identifier(from_column)
        self.to_column = quote_identifier(to_column)
        self.case_sensitive = case_sensitive
        self.default = default
        self._cache = {}
        self._preloaded = False

        # Get paramstyle from connection
        self._paramstyle = self._cursor.connection.interface.paramstyle
        self._placeholder = ParamStyle.get_placeholder(self._paramstyle)

        # Get threshold from settings or use default
        threshold = settings.get('lookup_preload_threshold', self.PRELOAD_THRESHOLD)

        # Auto-decide whether to preload
        row_count = self._get_row_count()
        if row_count <= threshold:
            self._load_all()
            self._preloaded = True

    def _get_row_count(self) -> int:
        """Get count of rows in lookup table."""
        sql = f"SELECT COUNT(*) FROM {self.table}"
        self._cursor.execute(sql)
        return self._cursor.fetchone()[0]

    def _load_all(self):
        """Preload entire lookup table into cache."""
        sql = f"SELECT {self.from_column}, {self.to_column} FROM {self.table}"
        self._cursor.execute(sql)
        for row in self._cursor.fetchall():
            key = row[0]
            # Skip rows with null/empty keys
            if key in (None, ''):
                continue
            if not self.case_sensitive:
                key = str(key).upper()
            self._cache[key] = row[1]

    def _lookup(self, value):
        """Lookup value in database."""
        sql = f"SELECT {self.to_column} FROM {self.table} WHERE {self.from_column} = {self._placeholder}"

        if self._paramstyle in ParamStyle.positional_styles():
            self._cursor.execute(sql, (value,))
        else:
            self._cursor.execute(sql, {'val': value})

        row = self._cursor.fetchone()
        return row[0] if row else None

    def __call__(self, value):
        """Lookup value in reference table."""
        if value is None or value == '':
            return self.default

        lookup_key = value if self.case_sensitive else str(value).upper()

        if lookup_key not in self._cache:
            result = self._lookup(value)
            self._cache[lookup_key] = result if result is not None else self.default

        return self._cache[lookup_key]