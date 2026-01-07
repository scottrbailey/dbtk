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
    collapses consecutive underscores, and strips leading/trailing underscores.

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
        'id'
        >>> normalize_field_name('#Term Code')
        'term_code'
    """
    if not name:
        return 'col'

    # 1. Lowercase and strip whitespace
    name = str(name).lower().strip()

    # 2. Replace all non-alphanumeric with underscore (consecutive become single _)
    name = re.sub(r'[^a-z0-9]+', '_', name)

    # 3. Strip leading/trailing underscores
    name = name.strip('_')

    # 4. Ensure not empty
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

    def __init__(self, *values: Any) -> None:
        # Fast path: initialize list directly in C code — no __setitem__ calls
        super().__init__(values)

        # Lazy attributes — only allocated only when needed
        object.__setattr__(self, "_deleted_fields", set())
        object.__setattr__(self, "_added", None)

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

    def __setitem__(self, key: Union[int, str, slice], value: Any) -> None:
        if isinstance(key, str):
            # Try original field names first
            if key in self._fields:
                # Existing column — revive if deleted
                self._deleted_fields.discard(key)
                super().__setitem__(self._fields.index(key), value)
            # Then try normalized field names
            elif key in self._fields_normalized:
                idx = self._fields_normalized.index(key)
                self._deleted_fields.discard(self._fields[idx])
                super().__setitem__(idx, value)
            else:
                # New column — store in _added
                if self._added is None:
                    object.__setattr__(self, "_added", {})
                self._added[key] = value
        else:
            super().__setitem__(key, value)

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

    @classmethod
    def set_columns(cls, columns: List[str]) -> None:
        """Deprecated: Use set_fields() instead."""
        cls.set_fields(columns)

    # ------------------------------------------------------------------ #
    # Dict-like interface
    # ------------------------------------------------------------------ #

    def keys(self, prefer_original: bool = True) -> List[str]:
        """
        Get list of field names.

        Args:
            prefer_original: If True (default), return original field names.
                           If False, return normalized field names.

        Returns:
            List of field names
        """
        if prefer_original:
            base = [f for f in self._fields if f not in self._deleted_fields]
        else:
            # Return normalized names for non-deleted fields
            base = [self._fields_normalized[i] for i, f in enumerate(self._fields)
                    if f not in self._deleted_fields]
        if self._added:
            base.extend(self._added.keys())
        return base

    def values(self) -> Tuple[Any, ...]:
        """Get list of field values (in original field order)."""
        return tuple(self[k] for k in self.keys(prefer_original=True))

    def items(self, prefer_original: bool = True) -> Iterator[Tuple[str, Any]]:
        """
        Get (field_name, value) pairs.

        Args:
            prefer_original: If True (default), use original field names.
                           If False, use normalized field names.

        Yields:
            Tuples of (field_name, value)
        """
        if prefer_original:
            for field in self._fields:
                if field not in self._deleted_fields:
                    yield field, self[field]
        else:
            for i, field in enumerate(self._fields):
                if field not in self._deleted_fields:
                    yield self._fields_normalized[i], self[field]
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
        if other is not None:
            if hasattr(other, "items"):
                for k, v in other.items():
                    self[k] = v
            else:
                for k, v in other:
                    self[k] = v
        for k, v in kwargs.items():
            self[k] = v

    def to_dict(self, use_original: bool = True) -> dict:
        """
        Convert Record to dictionary.

        Args:
            use_original: If True (default), use original field names as keys.
                        If False, use normalized field names.

        Returns:
            Dictionary representation of the record

        Examples:
            >>> rec = Record(2020, 2025)
            >>> rec.set_fields(['Start Year', 'End Year'])
            >>> rec.to_dict()
            {'Start Year': 2020, 'End Year': 2025}
            >>> rec.to_dict(use_original=False)
            {'start_year': 2020, 'end_year': 2025}
        """
        return dict(self.items(prefer_original=use_original))

    # ------------------------------------------------------------------ #
    # Utilities
    # ------------------------------------------------------------------ #

    def __len__(self) -> int:
        return len(self.keys())

    def __iter__(self) -> Iterator[str]:
        return iter(self.keys())

    def __str__(self) -> str:
        items = ", ".join(f"{k!r}: {v!r}" for k, v in self.items())
        return f"{self.__class__.__name__}({items})"

    def __repr__(self) -> str:
        values = ", ".join(repr(v) for v in super().__iter__())  # original order
        return f"{self.__class__.__name__}({values})"

    def __dir__(self) -> List[str]:
        return sorted(set(super().__dir__()) | set(self.keys()))

    def pprint(self) -> None:
        """Pretty-print the record with aligned columns."""
        if not self.keys():
            print("<Empty Record>")
            return

        width = max(len(k) for k in self.keys())
        template = f"{{:<{width}}} : {{}}"

        for key in self.keys():
            value = self[key]
            print(template.format(key, to_string(value)))
