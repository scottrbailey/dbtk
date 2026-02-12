# dbtk/record.py
"""
Record classes for database result sets.
"""

import re
from typing import List, Any, Iterator, Tuple, Union
from .utils import to_string


def normalize_field_name(name: str) -> str:
    """
    Normalize field name for attribute access.

    Converts to lowercase, replaces non-alphanumeric characters with underscores,
    collapses consecutive underscores, and strips trailing underscores.
    Leading underscores are preserved to maintain Python's convention for private/internal fields.

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
        >>> normalize_field_name('__id__')
        '__id'
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

    # 2. Replace all non-alphanumeric with underscore (consecutive become single _)
    name = re.sub(r'[^a-z0-9]+', '_', name)

    # 3. Strip trailing underscores only (preserve leading for Python convention)
    name = name.rstrip('_')

    # 4. Attributes can't start with digit
    if name and name[0].isdigit():
        name = 'n' + name

    # 5. Ensure not empty
    return name or 'col'


class Record(list):
    """
    Flexible/lightweight row object supporting that behaves like a dict, list, and object.

    Record extends list to provide a rich interface for accessing query result rows.
    It supports attribute access, dictionary-style key access, integer indexing, and
    slicing - all on the same object. This makes it the most flexible return type
    for both cursors and readers. Ideal when you need different access patterns in
    different parts of your code.

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
    * **pprint()** - Pretty-print the record

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
        print(user['name'])           # Dictionary-style: 'Aang'
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
                    self[k] = v
            else:
                for k, v in other:
                    self[k] = v
        for k, v in kwargs.items():
            self[k] = v

    def coalesce(self, other=None, **kwargs) -> None:
        """"""
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
