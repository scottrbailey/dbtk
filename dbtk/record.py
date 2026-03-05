# dbtk/record.py
"""
Record classes for database result sets.
"""

from typing import List, Any, Iterator, Tuple, Union
from .utils import to_string, normalize_field_name, FixedColumn


class Record(list):
    """
    Flexible/lightweight that strikes a balance between the memory efficiency of list
    and the functionality of dicts/objects.

    Record extends list to provide a rich interface for accessing query result rows.
    It supports attribute access, dictionary-style key access, integer indexing, and
    slicing - all on the same object. This makes it a very flexible and memory efficient
    return type for both cursors and readers.

    Access Patterns
    ---------------
    * **Dictionary-style**: ``row['column_name']`` - Safe with .get() method
    * **Attribute access**: ``row.column_name`` - Clean, readable syntax
    * **Integer index**: ``row[3]`` - Positional access
    * **Slicing**: ``row[1:4]`` - Get multiple columns at once
    * **Iteration**: ``for value in row`` - Iterate over values
    * **Containment**: ``'column_name' in row`` - Check if column exists

    Key Methods
    -----------
    * **get(key, default=None)** - Safe dictionary-style access with default
    * **keys()** - Get list of column names
    * **values()** - Get list of column values
    * **items()** - Get (column, value) pairs
    * **copy()** - Create a shallow copy of the record
    * **update(dict)** - Update multiple columns from a dictionary
    * **coalesce(dict)** - Update only missing values from a dictionary
    * **pprint()** - Pretty-print the record

    Column Names: Original vs Normalized
    -------------------------------------
    Every Record stores two parallel lists of names for each column:

    * ``_fields`` — the **original** names exactly as returned by the database
      (e.g. ``'First Name'``, ``'User ID'``, ``'#term_code'``).
    * ``_fields_normalized`` — **Python-safe** versions used for attribute access
      (e.g. ``'first_name'``, ``'user_id'``, ``'term_code'``).

    Both lists are set once by :meth:`set_fields` when the cursor executes its
    first query.  Normalization converts field to be suitable for attribute access.
    It lowercases, replaces non-alphanumeric characters with underscores, collapses runs,
    strips trailing underscores, and prefixes digit-leading names with ``n``.

    **Which to use:**

    * Use **original names** (``row['First Name']``, ``row.keys()``,
      ``row.to_dict()``) when round-tripping data back to the database or to a
      CSV, where column names must match the schema exactly.
    * Use **normalized names** (``row.first_name``, ``row['first_name']``,
      ``row.keys(normalized=True)``, ``row.to_dict(normalized=True)``) in
      application code where Pythonic attribute access is preferred and when
      case and white-space insensitive matching is beneficial.

    Both forms work interchangeably for item get/set and ``in`` checks, so
    ``row['First Name']`` and ``row['first_name']`` return the same value.

    Note
    ----
    Record is dynamically subclassed when a cursor executes a query. Each unique
    set of column names gets its own Record subclass with those names set as
    class attributes. This enables attribute access while maintaining the list
    base class for compatibility.

    Example
    -------
    ::

        cursor = db.cursor('record')  # or db.cursor() - record is default
        cursor.execute("SELECT id, name, email, created FROM users WHERE id = :id",
                      {'id': 42})
        user = cursor.fetchone()

        # All these access patterns work on the same object:
        print(user['name'])            # Dictionary-style: 'Aang'
        print(user.name)               # Attribute access: 'Aang'
        print(user[1])                 # Index access: 'Aang'
        print(user[1:3])               # Slicing: ['Aang', 'aang@avatar.com']

        # Safe access with default
        print(user.get('phone', 'N/A'))  # 'N/A' if no phone column

        # Dictionary methods
        for col, val in user.items():
            print(f"{col}: {val}")

        # List compatibility
        user_id, name, email, created = user  # Unpack like a tuple
        print(' | '.join(user))                # Join like a list

        # Update columns
        user['email'] = 'newemail@avatar.com'
        user.name = 'Avatar Aang'

    See Also
    --------
    Cursor : Database cursor that returns Record objects
    """

    __slots__ = ("_added", "_deleted_fields")
    _fields: List[str] = []  # Original field names (e.g., 'Start Year')
    _fields_normalized: List[str] = []  # Normalized for access (e.g., 'start_year')
    _field_len: int = 0 # cached for fast hot path
    mutable_schema: bool = True  # Set to False in subclasses to forbid field add/delete

    def __init__(self, *args, **kwargs):
        # Lazy attributes
        object.__setattr__(self, "_deleted_fields", set())
        object.__setattr__(self, "_added", None)

        # Fast path check first
        n = self._field_len
        if len(args) == n and not kwargs:
            super().__init__(args)
            return

        # Default values
        values = [None] * n

        # Positional: fill in order, truncate if too many
        for i, val in enumerate(args):
            if i < n:
                values[i] = val

        # Keyword args: override
        for k, v in kwargs.items():
            if k in self._fields_normalized:
                idx = self._fields_normalized.index(k)
                values[idx] = v
            elif k in self._fields:
                idx = self._fields.index(k)
                values[idx] = v

        super().__init__(values)

    # ------------------------------------------------------------------ #
    # Core access methods
    # ------------------------------------------------------------------ #

    def __getitem__(self, key: Union[int, str, slice]) -> Any:
        if isinstance(key, str):
            # 1. Runtime-added fields
            if self._added and key in self._added:
                return self._added[key]
            # 2. Deleted fields (check by original name)
            if key in self._deleted_fields:
                raise KeyError(f"Column '{key}' has been deleted")
            # 3. Original field names
            try:
                return super().__getitem__(self._fields.index(key))
            except ValueError:
                pass
            # 4. Normalized field names
            try:
                idx = self._fields_normalized.index(key)
                # Check if the corresponding original field was deleted
                if self._fields[idx] in self._deleted_fields:
                    raise KeyError(f"Column '{key}' has been deleted")
                return super().__getitem__(idx)
            except ValueError:
                raise KeyError(f"Column '{key}' not found")
        return super().__getitem__(key)

    def __setitem__(self, key: Union[int, str], value: Any) -> None:
        if isinstance(key, int):
            # Positional set — update list directly
            # Note: if index > current len, list pads with None — we allow it
            super().__setitem__(key, value)
            return

        if not isinstance(key, str):
            raise TypeError("key must be int or str")

        # 1. Runtime-added field? Update it
        if self._added and key in self._added:
            self._added[key] = value
            return

        # 2. Try original field name
        if key in self._fields:
            if key in self._deleted_fields:
                # Revive deleted field
                self._deleted_fields.remove(key)
            super().__setitem__(self._fields.index(key), value)
            return

        # 3. Try normalized field name
        if key in self._fields_normalized:
            idx = self._fields_normalized.index(key)
            original_name = self._fields[idx]
            if original_name in self._deleted_fields:
                # Revive deleted field
                self._deleted_fields.remove(original_name)
            super().__setitem__(idx, value)
            return

        # 4. New field — add to runtime dict
        if not self.__class__.mutable_schema:
            raise TypeError(
                f"Cannot add field '{key}': schema is fixed (mutable_schema=False)"
            )
        if self._added is None:
            object.__setattr__(self, "_added", {})
        self._added[key] = value

    def __delitem__(self, key: Union[int, str]) -> None:
        if not isinstance(key, str):
            raise TypeError("Record supports deletion only by column name (string)")
        # Delegate to pop() — it has all the logic
        self.pop(key)

    def __getattr__(self, name: str) -> Any:
        # Called only when normal attribute lookup fails
        if self._added and name in self._added:
            return self._added[name]
        # Check normalized field names for attribute access
        if name in self._fields_normalized:
            idx = self._fields_normalized.index(name)
            if self._fields[idx] not in self._deleted_fields:
                return super(Record, self).__getitem__(idx)
        raise AttributeError(f"'Record' object has no attribute '{name}'")

    def __setattr__(self, name: str, value: Any) -> None:
        # Allow setting row.new_field = value
        self[name] = value

    def __delattr__(self, name: str) -> None:
        try:
            del self[name]
        except KeyError:
            raise AttributeError(name)

    def __contains__(self, key: object) -> bool:
        if not isinstance(key, str):
            return False
        # Check original fields
        if key in self._fields:
            return key not in self._deleted_fields
        # Check normalized fields
        if key in self._fields_normalized:
            idx = self._fields_normalized.index(key)
            return self._fields[idx] not in self._deleted_fields
        # Check runtime-added fields
        return self._added and key in self._added

    @classmethod
    def set_fields(cls, fields: List[str]) -> None:
        """
        Set the field names for this Record class.

        Stores original field names and generates normalized versions for attribute access.
        Handles collisions by appending _2, _3, etc. to duplicate normalized names.

        Args:
            fields: Original field names (e.g., ['Start Year', 'End Date'])

        Examples:
            >>> rec.set_fields(['Start Year', 'End Date'])
            >>> rec._fields
            ['Start Year', 'End Date']
            >>> rec._fields_normalized
            ['start_year', 'end_date']
        """
        cls._fields = fields
        cls._field_len = len(fields)
        # Normalize and handle collisions
        normalized = []
        seen = {}
        for field in fields:
            norm = normalize_field_name(field)
            if norm in seen:
                # Collision: append _2, _3, etc.
                count = seen[norm] + 1
                seen[norm] = count
                norm = f"{norm}_{count}"
            else:
                seen[norm] = 1
            normalized.append(norm)
        cls._fields_normalized = normalized


    # ------------------------------------------------------------------ #
    # Dict-like interface
    # ------------------------------------------------------------------ #

    def keys(self, normalized: bool = False) -> List[str]:
        """
        Get list of field names.

        Args:
            normalized: If True, return normalized field names.
                       If False (default), return original field names.

        Returns:
            List of field names
        """
        if normalized:
            # Return normalized names for non-deleted fields
            base = [self._fields_normalized[i] for i, f in enumerate(self._fields)
                    if f not in self._deleted_fields]
        else:
            base = [f for f in self._fields if f not in self._deleted_fields]
        if self._added:
            base.extend(self._added.keys())
        return base

    def values(self) -> Tuple[Any, ...]:
        """Get list of field values (in original field order)."""
        return tuple(self[k] for k in self.keys())

    def items(self, normalized: bool = False) -> Iterator[Tuple[str, Any]]:
        """
        Get (field_name, value) pairs.

        Args:
            normalized: If True, use normalized field names.
                       If False (default), use original field names.

        Yields:
            Tuples of (field_name, value)
        """
        if normalized:
            for i, field in enumerate(self._fields):
                if field not in self._deleted_fields:
                    yield self._fields_normalized[i], self[field]
        else:
            for field in self._fields:
                if field not in self._deleted_fields:
                    yield field, self[field]
        if self._added:
            yield from self._added.items()

    def get(self, key: str, default: Any = None) -> Any:
        try:
            return self[key]
        except KeyError:
            return default

    def pop(self, key: str, default: object = object()) -> Any:
        if not isinstance(key, str):
            raise TypeError("pop() key must be str")
        if not self.__class__.mutable_schema:
            raise TypeError(
                f"Cannot delete field '{key}': schema is fixed (mutable_schema=False)"
            )

        # 1. Runtime-added field?
        if self._added and key in self._added:
            return self._added.pop(key)

        # 2. Try original field names
        if key in self._fields:
            if key in self._deleted_fields:
                raise KeyError(key)
            value = self[key]
            self._deleted_fields.add(key)
            return value

        # 3. Try normalized field names
        if key in self._fields_normalized:
            idx = self._fields_normalized.index(key)
            original_name = self._fields[idx]
            if original_name in self._deleted_fields:
                raise KeyError(key)
            value = super(Record, self).__getitem__(idx)
            self._deleted_fields.add(original_name)
            return value

        # 4. Not found
        if default is not object():
            return default
        raise KeyError(key)

    def update(self, other=None, **kwargs) -> None:
        """
        Update fields from another dict, Record, or keyword arguments.

        Overwrites existing field values unconditionally. To preserve existing
        non-empty values, use :meth:`coalesce` instead.

        Accepts any mapping with an ``items()`` method, an iterable of
        ``(key, value)`` pairs, or keyword arguments. Unknown keys are added
        as runtime fields.

        Args:
            other: Optional dict, Record, or iterable of (key, value) pairs.
            **kwargs: Additional key-value pairs to set.

        Examples:
            >>> Record.set_fields(['id', 'name', 'email'])
            >>> record = Record(1, 'Scott', 'old@example.com')
            >>> record.update({'email': 'new@example.com'}, name='Scott Bailey')
            >>> record  # [1, 'Scott Bailey', 'new@example.com']
        """
        if other is not None:
            if hasattr(other, "items"):
                for k, v in other.items():
                    self[k] = v
            else:
                for k, v in other:
                    self[k] = v
        for k, v in kwargs.items():
            self[k] = v

    def coalesce(self, other=None, **kwargs) -> None:
        """
            Fill in missing or empty fields from another dict, Record, or keyword arguments.

            Updates the current Record by copying values from `other` (or `**kwargs`) only for fields
            that are currently `None` or an empty string (`''`). Existing non-empty values are preserved.

            This is a non-destructive "fill gaps" operation — it will never overwrite valid data.

            Args:
                other: Optional dict or Record containing values to coalesce. If provided, its items
                       are processed first.
                **kwargs: Additional key-value pairs to coalesce (overrides keys in `other` if both
                          provide a value).

            Returns:
                self: The updated Record (for chaining).

            Examples:
                >>> Record.set_fields(['id', 'name', 'email', 'phone', 'notes'])
                >>> record = Record(None, "Scott", "", "scott@example.com", None)
                >>> resolved = {'id': 123, 'name': 'Scott Bailey', 'notes': 'VIP'}
                >>> record.coalesce(resolved, phone="555-1234")
                >>> record  # [123, "Scott", "", "555-1234", "VIP"]
            """
        if other is not None:
            if hasattr(other, "items"):
                for k, v in other.items():
                    if k in self and self[k] in (None, ''):
                        self[k] = v
        for k, v in kwargs.items():
            if k in self and self[k] in (None, ''):
                self[k] = v
        return self

    def to_dict(self, normalized: bool = False) -> dict:
        """
        Convert Record to dictionary.

        Args:
            normalized: If True, use normalized field names as keys.
                       If False (default), use original field names.

        Returns:
            Dictionary representation of the record

        Examples:
            >>> rec = Record(2020, 2025)
            >>> rec.set_fields(['Start Year', 'End Year'])
            >>> rec.to_dict()
            {'Start Year': 2020, 'End Year': 2025}
            >>> rec.to_dict(normalized=True)
            {'start_year': 2020, 'end_year': 2025}
        """
        return dict(self.items(normalized=normalized))

    def copy(self):
        """
        Return a shallow copy of the Record.

        - Copies the underlying list values
        - Copies the field metadata (_fields, _fields_normalized)
        - Copies deleted fields set
        - Copies any runtime-added fields (_added dict)
        - Preserves the same Record subclass (so attribute access works)

        Returns:
            Record: A new Record instance with the same data and state
        """
        # Create a new instance of the same class (preserves subclass attrs)
        new = self.__class__.__new__(self.__class__)

        # Shallow copy of the underlying list (values)
        super(Record, new).__init__(super().__iter__())

        # Copy field metadata (shallow copy of lists)
        new._fields = self._fields[:]
        new._fields_normalized = self._fields_normalized[:]

        # Copy deleted fields set (shallow copy)
        new._deleted_fields = self._deleted_fields.copy()

        # Copy runtime-added fields dict (shallow copy)
        if self._added is not None:
            new._added = self._added.copy()
        else:
            new._added = None

        return new

    # ------------------------------------------------------------------ #
    # Utilities
    # ------------------------------------------------------------------ #

    def __len__(self) -> int:
        return len(self.keys())

    def __iter__(self) -> Iterator[Any]:
        return iter(self.values())

    def __str__(self) -> str:
        items = ", ".join(f"{k!r}: {v!r}" for k, v in self.items())
        return f"{self.__class__.__name__}({items})"

    def __repr__(self) -> str:
        values = ", ".join(repr(v) for v in super().__iter__())  # original order
        return f"{self.__class__.__name__}({values})"

    def __dir__(self) -> List[str]:
        return sorted(set(super().__dir__()) | set(self.keys()))

    def pprint(self, normalized: bool = False) -> None:
        """
        Pretty-print the record with aligned columns.

        Args:
            normalized: If True, use normalized field names.
                       If False (default), use original field names.
        """
        keys_to_use = self.keys(normalized=normalized)
        if not keys_to_use:
            print("<Empty Record>")
            return

        width = max(len(k) for k in keys_to_use)
        template = f"{{:<{width}}} : {{}}"

        for key in keys_to_use:
            value = self[key]
            print(template.format(key, to_string(value)))


class FixedWidthRecord(Record):
    """
    A Record subclass optimized for fixed-width data parsing and reconstruction.

    Instances represent a single row from a fixed-width file, with values accessible
    by name, attribute, or index. The class retains the original List[FixedColumn]
    definitions so that `to_line()` can reconstruct the exact source line by splicing
    each formatted value into its correct byte position.

    Designed for use with `FixedReader` or `EDIReader`, where each record type
    gets its own dynamic subclass with the appropriate column definitions.

    Class attributes (set automatically by `set_fields`):
        _columns : List[FixedColumn]
            Full column definitions in definition order, including name, position,
            type, alignment, pad character, and comment.
        _line_len : int
            Total line width in characters (max end_pos across all columns).

    Example usage:
        # In reader factory
        RecordClass.set_fields(columns)  # columns is List[FixedColumn]
        record = RecordClass(*row_values)
        original_line = record.to_line()  # reconstructs formatted string
    """
    _columns: List[FixedColumn] = []
    _line_len: int = 0
    mutable_schema: bool = False

    @classmethod
    def set_fields(cls, fields: List[FixedColumn]):
        """
        Set field names and column definitions from a list of FixedColumn objects.

        Stores the column list as-is on the class (preserving position, type,
        alignment, pad character, and comment for introspection) and pre-computes
        _line_len as the rightmost end_pos across all columns.

        Args:
            fields: FixedColumn definitions for this record type. Definition order
                    determines value order on instances; to_line() places each value
                    by start_idx so out-of-position-order definitions work correctly.
        """
        names = [col.name for col in fields]
        super().set_fields(names)
        cls._columns = list(fields)
        cls._line_len = max(col.end_pos for col in fields) if fields else 0

    def to_line(self, truncate_overflow: bool = False):
        """
        Reconstruct the original fixed-width line from this record's values.

        Builds a space-filled buffer of _line_len characters and splices each
        field value into its position using start_idx. Column order in the
        definition does not matter; gaps between columns remain as spaces.
        Iterates only the column fields (stops before _row_num or any other
        appended fields). Missing values are treated as empty strings.

        Args:
            truncate_overflow: If False (default), raise ValueError when a value
                               exceeds its column width. If True, silently truncate.

        Returns:
            A string exactly matching the fixed-width format for this record type.

        Raises:
            ValueError: If truncate_overflow=False and any value exceeds its width.

        Example:
            record.to_line()  # -> '1234567890ABC       0000012345'
        """
        cls = self.__class__
        line = [' '] * cls._line_len
        for col, (name, value) in zip(cls._columns, self.items()):
            str_val = to_string(value)
            if len(str_val) > col.width:
                if truncate_overflow:
                    str_val = str_val[:col.width]
                else:
                    raise ValueError(f'Value too large for {name} limit: {col.width}')
            elif len(str_val) < col.width:
                if col.alignment:
                    align = col.alignment[0]  # 'left'→'l', 'right'→'r', 'center'→'c'
                elif col.column_type in ('int', 'float'):
                    align = 'r'
                else:
                    align = 'l'
                pad = col.pad_char if col.pad_char is not None else (
                    '0' if col.column_type in ('int', 'float') else ' '
                )
                if align == 'r':
                    str_val = str_val.rjust(col.width, pad)
                elif align == 'c':
                    str_val = str_val.center(col.width, pad)
                else:
                    str_val = str_val.ljust(col.width, pad)
            line[col.start_idx:col.start_idx + col.width] = str_val
        return ''.join(line)

    def pprint(self, normalized: bool = False, add_comments: bool = False) -> None:
        """
        Pretty-print the record with aligned columns.

        Args:
            normalized:   If True, use normalized field names.
            add_comments: If True, append each column's comment (from the
                          FixedColumn definition) after the value.  Columns
                          without a comment are left blank in that position.
                          Has no effect when there are no _columns defined.
        """
        cls = self.__class__
        if not add_comments or not cls._columns:
            super().pprint(normalized=normalized)
            return

        keys = self.keys(normalized=normalized)
        if not keys:
            print("<Empty Record>")
            return

        col_map = {col.name: col for col in cls._columns}
        key_width = max(len(k) for k in keys)
        val_width = max(
            len(to_string(self[k])) for k in keys
        )
        template = f"{{:<{key_width}}} : {{:<{val_width}}}  {{}}"
        no_comment_template = f"{{:<{key_width}}} : {{}}"

        comments_present = any(
            col_map[k].comment for k in keys if k in col_map
        )

        for key in keys:
            value = to_string(self[key])
            col = col_map.get(key)
            comment = col.comment if col else None
            if comments_present:
                print(template.format(key, value, f'# {comment}' if comment else ''))
            else:
                print(no_comment_template.format(key, value))

    def visualize(self) -> str:
        """
        Return a diagnostic string showing column boundaries over the record value.

        Output format (4 lines)::

                     1         2    ...
            1234567890123456789012345...   ← position ruler
            || |         |         |...   ← '|' at each column start
            101 123456789  87654321...    ← to_line() output

        Returns a string; call ``print(record.visualize())`` to display.
        Consistent with ``FixedReader.visualize_columns()`` which also returns
        a string.
        """
        cls = self.__class__
        line_len = cls._line_len
        ruler_10s = ''.join(str(i // 10 % 10) if i % 10 == 0 else ' ' for i in range(1, line_len + 1))
        ruler_1s  = ''.join(str(i % 10)                               for i in range(1, line_len + 1))
        boundary_line = ['─'] * line_len
        for col in cls._columns:
            boundary_line[col.start_idx] = '├'
            if boundary_line[col.end_pos - 1] == '─':
                boundary_line[col.end_pos - 1] = '┤'
        return f'{ruler_10s}\n{ruler_1s}\n{"".join(boundary_line)}\n{self.to_line()}'
