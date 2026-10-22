# dbtk/record.py
"""
Record classes for database result sets.
"""
import itertools
import logging
import operator
from typing import Callable, Dict, Iterable, List, Any, Iterator, Optional, Sequence, Tuple, Union
from .utils import to_string, normalize_field_name

logger = logging.getLogger(__name__)

_MISSING = object()  # sentinel for pop() default argument

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

        cursor = db.cursor()
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
    _mutable_schema: bool = True  # Set to False in subclasses to forbid field add/delete
    # list of method and attribute names that normalized field names must not collide with
    _RESERVED: frozenset = frozenset({
                           # methods defined on Record
                           'append', 'clear', 'coalesce', 'copy', 'extend', 'get', 'items', 'keys',
                           'pop', 'pprint', 'remove', 'to_dict', 'update', 'values',
                           # classmethods
                           'set_fields', '_get_reserved',
                           # inherited list methods not overridden
                           'count', 'index', 'insert', 'reverse', 'sort',
                           # attributes / slots
                           '_mutable_schema', '_fields', '_fields_normalized', '_field_len', '_added',
                           '_deleted_fields',
                           # _RESERVED itself normalizes to _reserved
                           '_reserved',
                           })

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
        if not self.__class__._mutable_schema:
            raise TypeError(
                f"Cannot add field '{key}': schema is fixed (_mutable_schema=False)"
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
    def _get_reserved(cls) -> frozenset:
        """Return all reserved names, including those from parent classes."""
        reserved = cls._RESERVED

        # Walk the MRO and collect reserved names from all base classes
        for base in cls.__mro__:
            if hasattr(base, '_RESERVED'):
                reserved = reserved | base._RESERVED

        return reserved

    @classmethod
    def set_fields(cls, fields: List[str]) -> None:
        """
        Set the field names for this Record class.

        Stores original field names and generates normalized versions for attribute access.
        Handles collisions by appending _2, _3, etc. to duplicate normalized names.

        Args:
            fields: Original field names (e.g., ['Start Year', 'End Date'])

        Note:
            Call this on a private subclass, not on ``Record`` itself.
            ``_fields``/``_fields_normalized`` are plain class attributes, so
            calling ``Record.set_fields(...)`` directly mutates that shared,
            process-wide state for every piece of code still holding a
            reference to the bare ``Record`` class. Every reader, writer, and
            cursor in dbtk creates its own subclass first for exactly this
            reason - do the same in your own code.

            That subclass must also redeclare ``__slots__ = ()``, as shown
            below. ``__slots__`` is not inherited in the sense of "instances
            of subclasses stay dict-free" - each class in the MRO decides for
            itself whether its layer adds a ``__dict__``, and a subclass body
            that omits ``__slots__`` gets one by default regardless of what
            the parent declares. Skip it and ``MyRecord`` instances silently
            grow a ``__dict__`` alongside ``Record``'s own slots, quietly
            losing the memory savings ``Record`` exists for.

        Examples:
            >>> MyRecord = type('MyRecord', (Record,), {'__slots__': ()})
            >>> MyRecord.set_fields(['Start Year', 'End Date'])
            >>> MyRecord._fields
            ['Start Year', 'End Date']
            >>> MyRecord._fields_normalized
            ['start_year', 'end_date']
        """
        # Replace None/empty field names with 'col' before deduplication so
        # _fields never contains None and unnamed columns get col, col_2, etc.
        fields = [f if f else 'col' for f in fields]

        # Deduplicate original field names before normalization.
        # When a name appears more than once, rename later occurrences by
        # appending _2, _3, … — skipping any candidate already taken by an
        # earlier renamed field OR that already exists in the original list.
        deduped = []
        seen_originals = set()
        for name in fields:
            if name not in seen_originals:
                seen_originals.add(name)
                deduped.append(name)
            else:
                counter = 1
                candidate = name
                while candidate in seen_originals or candidate in fields:
                    counter += 1
                    candidate = f"{name}_{counter}"
                logger.warning(
                    f"Duplicate field '{name}' renamed to '{candidate}' in _fields."
                )
                seen_originals.add(candidate)
                deduped.append(candidate)
        fields = deduped

        cls._fields = fields
        cls._field_len = len(fields)
        # Normalize and handle collisions
        normalized = []
        seen = {}
        reserved = cls._get_reserved()

        for original in fields:
            norm = normalize_field_name(original) # normalize name for attribute access
            original_norm = norm
            counter = 1

            while norm in seen or norm in reserved:
                counter += 1
                norm = f"{original_norm}_{counter}"
            if norm != original_norm:
                logger.warning(
                    f"Attribute access for '{original}' was renamed to '{norm}' due to collision "
                    "with existing Record method/attribute."
                )

            seen[norm] = True
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

    def pop(self, key: str, default: object = _MISSING) -> Any:
        """
        Remove `key` from dict-style/attribute access and return its value.

        This hides the field from name-based access (``row[key]``, ``row.key``,
        ``in``, ``keys()``/``items()``/``to_dict()``/``str()``) and from a
        subsequent ``for v in row``. It does not free the value's slot in the
        underlying list - field layout is a class-level property (shared by
        every instance of this Record subclass), not something one instance
        can resize on its own. Positional access (``row[3]``) is unaffected by
        deletion and still returns the raw stored value.

        Args:
            key: Field name (original or normalized) to remove.
            default: Returned instead of raising when `key` isn't present.

        Raises:
            TypeError: If `key` isn't a str, or the schema is fixed
                (`_mutable_schema=False`, e.g. `FixedWidthRecord`).
            KeyError: If `key` isn't found and no `default` was given.
        """
        if not isinstance(key, str):
            raise TypeError("pop() key must be str")
        if not self.__class__._mutable_schema:
            raise TypeError(
                f"Cannot delete field '{key}': schema is fixed (_mutable_schema=False)"
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
        if default is not _MISSING:
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
            >>> MyRecord = type('MyRecord', (Record,), {'__slots__': ()})
            >>> MyRecord.set_fields(['id', 'name', 'email'])
            >>> record = MyRecord(1, 'Scott', 'old@example.com')
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
                >>> MyRecord = type('MyRecord', (Record,), {'__slots__': ()})
                >>> MyRecord.set_fields(['id', 'name', 'email', 'phone', 'notes'])
                >>> record = MyRecord(None, "Scott", "scott@example.com", "", None)
                >>> resolved = {'id': 123, 'name': 'Scott Bailey', 'notes': 'VIP'}
                >>> record.coalesce(resolved, phone="555-1234")
                >>> record  # [123, "Scott", "scott@example.com", "555-1234", "VIP"]
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
            >>> MyRecord = type('MyRecord', (Record,), {'__slots__': ()})
            >>> MyRecord.set_fields(['Start Year', 'End Year'])
            >>> rec = MyRecord(2020, 2025)
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

        # Initialize slots via object.__setattr__ before any attribute access —
        # Record.__setattr__ routes everything through __setitem__, which would
        # recurse infinitely on an uninitialized instance.
        object.__setattr__(new, '_deleted_fields', self._deleted_fields.copy())
        object.__setattr__(new, '_added', self._added.copy() if self._added is not None else None)

        # Shallow copy of the underlying list (values)
        super(Record, new).__init__(super().__iter__())

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
        # Deleted fields are skipped here too, matching __str__/items()/to_dict() -
        # pop()/del only hide a field from name-based access, they don't remove
        # its slot from the backing list, so raw list iteration would show a
        # "deleted" value that every other view of this Record hides.
        values = ", ".join(repr(self[k]) for k in self.keys())
        return f"{self.__class__.__name__}({values})"

    def __dir__(self) -> List[str]:
        return sorted(set(super().__dir__()) | set(self.keys()))

    def reverse(self) -> None:
        raise TypeError("Record.reverse() is not supported — it would break field-index mappings")

    def sort(self, *args, **kwargs) -> None:
        raise TypeError("Record.sort() is not supported — it would break field-index mappings")

    def insert(self, index: int, value: object) -> None:
        raise TypeError("Record.insert() is not supported — it would break field-index mappings")

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

    Example usage::

        # In reader factory
        RecordClass.set_fields(columns)  # columns is List[FixedColumn]
        record = RecordClass(*row_values)
        original_line = record.to_line()  # reconstructs formatted string

        # Parse a raw fixed-width string directly
        record = RecordClass.from_line(raw_line)  # corollary to to_line()
    """
    __slots__ = ()
    _columns: List[FixedColumn] = []
    _line_len: int = 0
    _mutable_schema: bool = False
    _RESERVED: frozenset = frozenset({'_columns', '_line_len', 'from_line', 'to_line', 'visualize'})

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

    @classmethod
    def from_line(cls, line: str, auto_trim: bool = True) -> 'FixedWidthRecord':
        """
        Parse a fixed-width string and return a new instance of this record type.

        The corollary to ``to_line()``: slice each field from its declared position,
        apply type conversion based on ``column_type``, and construct the record.
        Mirrors the parsing logic in :class:`FixedReader` so that records created
        here behave identically to those produced by the reader.

        Args:
            line:      A fixed-width string. Should be at least ``_line_len``
                       characters; shorter strings are handled gracefully (missing
                       positions return an empty string).
            auto_trim: If True (default), strip leading/trailing whitespace from
                       ``text`` fields before storing. Set to False to preserve the
                       raw padded value exactly as it appears in the source line.

        Returns:
            A new instance of this :class:`FixedWidthRecord` subclass with values
            populated from the parsed string.

        Raises:
            TypeError: If ``line`` is not a string.

        Example::

            MyRecord = fixed_record_factory([
                ('record_type', 1),
                FixedColumn('amount', 2, 11, 'int'),
            ])
            record = MyRecord.from_line('6        42')
            assert record.record_type == '6'
            assert record.amount == 42

            # Round-trip
            assert record.to_line() == MyRecord.from_line(record.to_line()).to_line()
        """
        from .etl.transforms.datetime import parse_date, parse_datetime, parse_timestamp
        row_data = []
        for col in cls._columns:
            val = line[col.start_idx:col.end_pos]
            try:
                if col.column_type == 'text' and auto_trim:
                    val = str(val).strip()
                elif col.column_type == 'date':
                    val = parse_date(val)
                elif col.column_type == 'datetime':
                    val = parse_datetime(val)
                elif col.column_type == 'timestamp':
                    val = parse_timestamp(val)
                elif col.column_type == 'int':
                    val = int(val.strip()) if val.strip() else None
                elif col.column_type == 'float':
                    val = float(val.strip()) if val.strip() else None
                else:
                    val = str(val)
            except (ValueError, TypeError):
                val = str(val).strip() if auto_trim else str(val)
            row_data.append(val)
        return cls(*row_data)

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
                if col.align:
                    align = col.align[0]  # 'left'→'l', 'right'→'r', 'center'→'c'
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
        Consistent with ``FixedReader.visualize()`` which also returns a string.
        """
        cls = self.__class__
        line_len = cls._line_len
        ruler_10s = ''.join(str(i // 10 % 10) if i % 10 == 0 else ' ' for i in range(1, line_len + 1))
        ruler_10s = '0' + ruler_10s[1:]
        ruler_1s  = ''.join(str(i % 10)                               for i in range(1, line_len + 1))
        boundary_line = ['─'] * line_len
        for col in cls._columns:
            boundary_line[col.start_idx] = '├'
            if boundary_line[col.end_pos - 1] == '─':
                boundary_line[col.end_pos - 1] = '┤'
        return f'{ruler_10s}\n{ruler_1s}\n{"".join(boundary_line)}\n{self.to_line()}'


def fixed_record_factory(columns, name='FixedRecord'):
    """
    Class factory: build a :class:`FixedWidthRecord` subclass from a compact column spec.

    Follows the same convention as :func:`collections.namedtuple` — returns a *class*,
    not an instance.

    Parameters
    ----------
    columns : list of FixedColumn or (name, width) tuple
        May be mixed.  Tuples specify ``(name, width)`` and are assigned sequential
        positions starting at 1 (or immediately after the previous column).
        :class:`FixedColumn` objects are used as-is and advance the auto-position
        cursor to ``col.end_pos + 1``.
    name : str, optional
        Class name of the returned type.  Defaults to ``'FixedRecord'``.

    Returns
    -------
    type
        A :class:`FixedWidthRecord` subclass with ``set_fields()`` already called.

    Examples
    --------
    ::

        # Tuple-only: positions calculated automatically
        AchDetail = fixed_record_factory([
            ('record_type',    1),
            ('priority_code',  2),
            ('routing_number', 9),
            ('account_number', 17),
            ('amount',         10),
        ], name='AchDetail')

        line = AchDetail('6', '22', '123456789', '00012345678', 100)
        print(line.to_line())

        # Mixed: use FixedColumn where you need explicit control
        AchHeader = fixed_record_factory([
            FixedColumn('record_type', 1, 1),
            ('priority_code', 2),
            FixedColumn('routing_number', 4, 12, column_type='int', align='right'),
            ('filler', 39),
        ])
    """
    pos = 1
    fixed_cols = []
    for col in columns:
        if isinstance(col, FixedColumn):
            fixed_cols.append(col)
            pos = col.end_pos + 1
        else:
            col_name, width = col[0], col[1]
            fixed_cols.append(FixedColumn(col_name, pos, width=width))
            pos += width

    cls = type(name, (FixedWidthRecord,), {'__slots__': ()})
    cls.set_fields(fixed_cols)
    return cls


def _make_getter(keys: Sequence[Any]):
    """
    Build a callable that pulls `keys` worth of values from an object via
    __getitem__, in order. Keys may be strings (dict/Record lookup) or ints
    (positional lookup) — __getitem__ doesn't care which.

    Handles the single-key case explicitly since operator.itemgetter returns
    a bare value (not a 1-tuple) when given only one argument.

    Shared with RecordShaper's select()/exclude()/rename().
    """
    if len(keys) == 1:
        key = keys[0]
        return lambda obj: (obj[key],)
    return operator.itemgetter(*keys)


def _shaped_record_class(names: List[str], class_name: str) -> type:
    """Build a fresh, throwaway Record subclass for one RecordShaper stage."""
    cls = type(class_name, (Record,), {'__slots__': ()})
    cls.set_fields(names)
    return cls


class _ShaperState:
    """
    Tracks whether a RecordShaper lineage has begun iteration.

    Shared by reference across a root RecordShaper and every stage chained
    from it. Once one stage starts reading, every *other* stage in the
    lineage refuses to - including sibling stages built from a common
    ancestor (a "fork") that would otherwise silently draw from the same
    half-drained source. The stage that started reading keeps its own
    ``_started`` flag so it can continue across ``next()`` calls and loops.
    """
    __slots__ = ('consumed',)

    def __init__(self) -> None:
        self.consumed = False


class RecordShaper:
    """
    Chainable, single-pass column shaping for a stream of self-describing rows.

    Wraps an iterable of dict/Record/namedtuple rows (or a cursor) and lets
    you select, exclude, or rename columns via lazy, chainable calls. Nothing
    is read from the source until the final result is iterated - each call
    just adds another lazy step and returns a new ``RecordShaper``.

    Reach for this only when you need to reshape a *stream* of rows before
    handing it to a writer or another consumer. It is not needed for
    everyday work with individual Record objects.

    Single-pass iterator
    --------------------
    A ``RecordShaper`` is an iterator, like DBTK's readers and cursors:
    ``next(shaper)`` returns one row at a time, and a ``for`` loop picks up
    wherever ``next()`` left off. Once exhausted, it yields nothing more.

    The wrapped source can be drained only once, no matter how many
    ``RecordShaper`` objects reference it. Once one stage starts reading,
    iterating or calling ``next()`` on any *other* stage in the lineage
    raises ``RuntimeError`` rather than silently returning partial results.

    Calling ``select()``/``exclude()``/``rename()`` on a stage returns a new
    stage and retires the one it was called on: iterating the retired stage
    raises ``RuntimeError`` naming the method, which catches the common slip
    of calling ``shaper.rename(...)`` without keeping its result. Chaining
    from a stage after reading has started also raises. ``columns`` still
    works on a retired stage.

    Column validation
    ------------------
    ``select()``/``exclude()``/``rename()`` validate column names eagerly,
    against the schema as of that point in the chain - not the original
    source. Renaming a column and then selecting it by its *new* name works;
    selecting it by its *old* name (after the rename) fails, because that
    name no longer exists at that point in the chain.

    A ``RecordShaper`` always emits Records. Record sources (cursor results,
    file readers) pass through as-is; dict and namedtuple rows are converted
    to Records as they are read. A column can be named by either its
    original name or its normalized name - ``'Start Year'`` or
    ``'start_year'`` - just as with ``record['...']``. An exact original-name
    match wins over a normalized one. Output columns always keep their
    original names unless ``rename()`` changes them.

    Args:
        rows: Source rows - dicts, Records, namedtuples, or a cursor.

    Examples:
        >>> to_csv(RecordShaper(cursor).select(['name', 'email']), 'report.csv')

        >>> (RecordShaper(cursor)
        ...     .rename({'signup_date': 'Joined On'})
        ...     .exclude(['internal_id']))
    """
    __slots__ = ('_rows', '_columns', '_normalized', '_empty', '_state',
                 '_chained_by', '_started')

    def __init__(self, rows: Iterable[Any]) -> None:
        self._rows: Iterator[Any] = iter(rows)
        self._columns: Optional[List[str]] = None
        self._normalized: Optional[List[str]] = None
        self._empty: bool = False
        self._state = _ShaperState()
        self._chained_by: Optional[str] = None
        self._started: bool = False

    @classmethod
    def from_tuples(cls, rows: Iterable[Sequence[Any]],
                     columns: Sequence[Optional[str]]) -> 'RecordShaper':
        """
        Start a RecordShaper from positional rows (tuples/lists) by naming them.

        Positional data carries no column names of its own, so `columns` must
        be supplied explicitly, one entry per position. Unlike ``select()``/
        ``exclude()``/``rename()``, this never peeks at the data - the schema
        comes entirely from `columns`.

        Args:
            rows: Source rows - tuples, lists, or anything positionally indexable.
            columns: Column name for each position in a row. A falsy entry
                (``None`` or ``''``) drops that position entirely, rather than
                naming it - handy for filler/padding columns you never want.

        Returns:
            RecordShaper: ready to iterate directly or chain further.

        Raises:
            ValueError: If `columns` has no non-empty names, or (lazily,
                during iteration) a row's length doesn't match `columns`.

        Examples:
            >>> list(RecordShaper.from_tuples(rows, ['id', 'name', None, 'email']))
        """
        columns = list(columns)
        kept_indices = [i for i, c in enumerate(columns) if c]
        kept_names = [columns[i] for i in kept_indices]
        if not kept_names:
            raise ValueError("columns must include at least one non-empty name")

        output_cls = _shaped_record_class(kept_names, 'TupleRecord')
        n = len(columns)
        all_kept = len(kept_indices) == n
        get = None if all_kept else _make_getter(kept_indices)

        def _gen() -> Iterator['Record']:
            for row in rows:
                if len(row) != n:
                    raise ValueError(f"Row length ({len(row)}) doesn't match columns length ({n})")
                yield output_cls(*row) if all_kept else output_cls(*get(row))

        instance = cls.__new__(cls)
        instance._rows = _gen()
        # Take names from the record class, not `kept_names`: set_fields() renames a
        # repeated name (e.g. a second 'id' becomes 'id_2'), and _columns must stay
        # parallel to _normalized for name resolution to pick the right column.
        instance._columns = list(output_cls._fields)
        instance._normalized = list(output_cls._fields_normalized)
        instance._empty = False
        instance._state = _ShaperState()
        instance._chained_by = None
        instance._started = False
        return instance

    def _resolve_schema(self) -> None:
        """Peek the source's first row to learn its columns, if not already known.

        Idempotent - a no-op once `_columns` is set. The peeked row is spliced
        back in via ``itertools.chain``, so nothing is lost to a later
        iteration; this is bookkeeping internal to schema resolution, not
        stream consumption, and does not touch `_state.consumed`.
        """
        if self._columns is not None:
            return
        try:
            first = next(self._rows)
        except StopIteration:
            self._columns = []
            self._normalized = []
            self._empty = True
            self._rows = iter(())
            return
        rows = itertools.chain([first], self._rows)
        if isinstance(first, Record):
            self._columns = first.keys()
            self._normalized = first.keys(normalized=True)
            self._rows = rows
            return

        # Dicts and namedtuples are converted to Records once, here, so every
        # stage - including a bare RecordShaper or a no-op step - emits Records
        # and supports original-or-normalized name lookup.
        if hasattr(first, 'keys'):
            src_names = list(first.keys())
            get = _make_getter(src_names)
        elif hasattr(first, '_fields'):
            src_names = list(first._fields)
            get = tuple
        else:
            raise TypeError(
                f"can't determine column names from {type(first).__name__} rows; "
                "use RecordShaper.from_tuples() first to attach column names to positional data."
            )
        output_cls = _shaped_record_class(src_names, 'ShapedRecord')

        def _gen() -> Iterator['Record']:
            for row in rows:
                yield output_cls(*get(row))

        self._columns = list(output_cls._fields)
        self._normalized = list(output_cls._fields_normalized)
        self._rows = _gen()

    def _resolve_names(self, names: Iterable[str]) -> Tuple[List[Optional[str]], List[str]]:
        """Map requested names to original column names at this point in the chain.

        Returns ``(resolved, missing)``: `resolved` is parallel to `names`, with
        ``None`` for each name that isn't a column; `missing` lists those names.
        An exact original-name match wins; otherwise a normalized-name match is
        used when the rows are Records.
        """
        originals = set(self._columns)
        by_normalized = dict(zip(self._normalized, self._columns)) if self._normalized else {}
        resolved: List[Optional[str]] = []
        missing: List[str] = []
        for name in names:
            if name in originals:
                resolved.append(name)
            elif name in by_normalized:
                resolved.append(by_normalized[name])
            else:
                resolved.append(None)
                missing.append(name)
        return resolved, missing

    @staticmethod
    def _check_unique(names: List[str], what: str) -> None:
        """Raise ValueError if `names` contains duplicates."""
        seen = set()
        dupes = []
        for name in names:
            if name in seen and name not in dupes:
                dupes.append(name)
            seen.add(name)
        if dupes:
            raise ValueError(f"{what}: {dupes}")

    def _check_chainable(self, method: str) -> None:
        """Refuse to chain from a lineage that has already started reading."""
        if self._state.consumed:
            raise RuntimeError(
                f"can't call {method}() on a RecordShaper whose stream has already "
                "started being read; chain every step before iterating."
            )

    def _chain(self, method: str, gen: Iterator[Any], output_cls: Optional[type] = None,
               empty: bool = False) -> 'RecordShaper':
        """A new stage over `gen`; retires this stage, recording `method` for errors."""
        new = RecordShaper.__new__(RecordShaper)
        new._rows = gen
        new._columns = list(output_cls._fields) if output_cls else []
        new._normalized = list(output_cls._fields_normalized) if output_cls else []
        new._empty = empty
        new._state = self._state
        new._chained_by = None
        new._started = False
        self._chained_by = method
        return new

    def _passthrough(self, method: str) -> 'RecordShaper':
        """A new stage over the same (already schema-resolved) rows, unchanged.

        Retires this stage just like ``_chain()`` does, so a no-op call still
        catches a discarded result.
        """
        new = RecordShaper.__new__(RecordShaper)
        new._rows = self._rows
        new._columns = self._columns
        new._normalized = self._normalized
        new._empty = self._empty
        new._state = self._state
        new._chained_by = None
        new._started = False
        self._chained_by = method
        return new

    @property
    def columns(self) -> List[str]:
        """Column names as of this point in the chain. Forces schema resolution."""
        self._resolve_schema()
        return list(self._columns)

    def select(self, col_names: List[str], strict: bool = True) -> 'RecordShaper':
        """
        Select and reorder columns.

        `col_names` is an allow-list that also sets the output column order.
        For Record rows, each name may be an original or normalized column
        name; the output always uses the original name.

        Args:
            col_names: Column names to keep, in the desired output order.
            strict: If True (default), a name not present in the current
                schema raises ValueError. If False, missing names are
                silently dropped (output order is preserved for the rest).

        Returns:
            RecordShaper: a new stage; this instance is unchanged.

        Raises:
            ValueError: If col_names is empty, no requested column survives
                (empty result after applying `strict`), the same column is
                requested more than once (including by both its original and
                normalized name), or (strict only) a requested column isn't
                present in the current schema.
            TypeError: If column names can't be determined from the row type.
        """
        col_names = list(col_names)
        if not col_names:
            raise ValueError("col_names must not be empty")
        self._check_chainable('select')
        self._resolve_schema()
        if self._empty:
            return self._chain('select', iter(()), empty=True)

        resolved, missing = self._resolve_names(col_names)
        if strict and missing:
            raise ValueError(f"Column(s) not found in source data: {missing}")
        keep = [c for c in resolved if c is not None]
        if not keep:
            raise ValueError("no columns remain to select")
        self._check_unique(keep, "Column(s) selected more than once")

        output_cls = _shaped_record_class(keep, 'SelectedRecord')
        get = _make_getter(keep)
        rows = self._rows

        def _gen() -> Iterator['Record']:
            for row in rows:
                yield output_cls(*get(row))

        return self._chain('select', _gen(), output_cls)

    def exclude(self, col_names: Iterable[str], strict: bool = True) -> 'RecordShaper':
        """
        Drop columns; source order is preserved for the rest.

        For Record rows, each name may be an original or normalized column
        name.

        Args:
            col_names: Column names to drop. Order doesn't matter; a list,
                tuple, or set all work. An empty collection is a no-op.
            strict: If True (default), a name not present in the current
                schema raises ValueError. If False, unknown names are
                silently ignored.

        Returns:
            RecordShaper: a new stage; this instance is unchanged.

        Raises:
            ValueError: If dropping the named columns would remove every
                column, or (strict only) a named column isn't present in
                the current schema.
            TypeError: If column names can't be determined from the row type.
        """
        col_names = list(col_names)
        self._check_chainable('exclude')
        self._resolve_schema()
        if not col_names:
            return self._passthrough('exclude')
        if self._empty:
            return self._chain('exclude', iter(()), empty=True)

        resolved, missing = self._resolve_names(col_names)
        if strict and missing:
            raise ValueError(f"Column(s) not found in source data: {sorted(missing)}")
        src_cols = self._columns
        exclude_set = {c for c in resolved if c is not None}
        keep = [c for c in src_cols if c not in exclude_set]
        if not keep:
            raise ValueError("excluding these columns would remove every column")

        output_cls = _shaped_record_class(keep, 'ExcludedRecord')
        get = _make_getter(keep)
        rows = self._rows

        def _gen() -> Iterator['Record']:
            for row in rows:
                yield output_cls(*get(row))

        return self._chain('exclude', _gen(), output_cls)

    def rename(self, mapping: Union[Dict[str, Optional[str]], Callable[[str], Optional[str]]],
               strict: bool = True) -> 'RecordShaper':
        """
        Rename some columns; everything else passes through unchanged, in
        its original position - this is a pure rename, not a select (use
        ``select()``/``exclude()`` to also narrow the columns).

        Args:
            mapping: Either a dict of source name -> new name, or a function
                called once per column with its current (original) name that
                returns the new name. In both forms a falsy new name (``''``
                or ``None``) keeps the column's current name. For Record rows,
                dict keys may be original or normalized column names. An
                empty dict, or a function that renames nothing, is a no-op.
                The function runs when ``rename()`` is called, not per row.
            strict: Dict mappings only. If True (default), a source name not
                present in the current schema raises ValueError. If False,
                such entries are silently ignored. Ignored for a function.

        Returns:
            RecordShaper: a new stage; this instance is unchanged.

        Raises:
            ValueError: If two dict keys name the same column (e.g. by its
                original and normalized name), two columns would end up with
                the same name, the function raises (chained to the original
                exception), or (strict, dict only) a named column isn't
                present in the current schema.
            TypeError: If the function returns something other than a str or
                a falsy value, or column names can't be determined from the
                row type.

        Examples:
            >>> RecordShaper(cursor).rename({'signup_date': 'Joined On'})

            >>> # Strip a prefix, whatever its case
            >>> RecordShaper(reader).rename(
            ...     lambda c: c[4:] if c.lower().startswith('src_') else None)
        """
        self._check_chainable('rename')
        self._resolve_schema()
        if not callable(mapping) and not mapping:
            return self._passthrough('rename')
        if self._empty:
            return self._chain('rename', iter(()), empty=True)

        src_cols = self._columns
        if callable(mapping):
            output_names = []
            for col in src_cols:
                try:
                    new_name = mapping(col)
                except Exception as e:
                    raise ValueError(f"rename function failed on column {col!r}: {e}") from e
                if new_name and not isinstance(new_name, str):
                    raise TypeError(
                        f"rename function returned {new_name!r} ({type(new_name).__name__}) "
                        f"for column {col!r}; expected a str or a falsy value to keep the name"
                    )
                output_names.append(new_name or col)
        else:
            keys = list(mapping)
            resolved, missing = self._resolve_names(keys)
            if strict and missing:
                raise ValueError(f"Column(s) not found in source data: {missing}")
            targets = {}
            dupes = []
            for key, col in zip(keys, resolved):
                if col is None:
                    continue
                if col in targets:
                    dupes.append(col)
                targets[col] = mapping[key]
            if dupes:
                raise ValueError(f"Column(s) named more than once in mapping: {dupes}")
            output_names = [targets.get(c) or c for c in src_cols]

        if output_names == src_cols:
            return self._passthrough('rename')

        collisions: Dict[str, List[str]] = {}
        for src, out in zip(src_cols, output_names):
            collisions.setdefault(out, []).append(src)
        collisions = {out: srcs for out, srcs in collisions.items() if len(srcs) > 1}
        if collisions:
            detail = '; '.join(f"{srcs} -> {out!r}" for out, srcs in collisions.items())
            raise ValueError(f"rename would give multiple columns the same name: {detail}")

        output_cls = _shaped_record_class(output_names, 'RenamedRecord')
        get = _make_getter(src_cols)
        rows = self._rows

        def _gen() -> Iterator['Record']:
            for row in rows:
                yield output_cls(*get(row))

        return self._chain('rename', _gen(), output_cls)

    def _claim(self) -> None:
        """Make this stage the lineage's reader, or raise if it can't be."""
        if self._chained_by is not None:
            raise RuntimeError(
                f"{self._chained_by}() was chained from this RecordShaper; "
                f"iterate the RecordShaper that {self._chained_by}() returned instead."
            )
        if self._started:
            return
        if self._state.consumed:
            raise RuntimeError(
                "This RecordShaper's stream is already being read from another "
                "point in its chain - a stream can only be iterated once, from a "
                "single point in its chain."
            )
        # Resolve before handing out rows, so dict/namedtuple sources are
        # converted and a later `.columns` never has to peek mid-stream.
        self._resolve_schema()
        self._state.consumed = True
        self._started = True

    def __iter__(self) -> Iterator[Any]:
        self._claim()
        # Hand loops the underlying generator directly, skipping a Python-level
        # __next__ per row. __next__ pulls from the same generator, so mixing
        # next() calls and loops stays in step.
        return self._rows

    def __next__(self) -> Any:
        if not self._started:
            self._claim()
        return next(self._rows)
