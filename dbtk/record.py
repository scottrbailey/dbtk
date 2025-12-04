# dbtk/record.py
"""
Record classes for database result sets.
"""

from typing import List, Any, Iterator, Tuple, Union
from .utils import to_string

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
    RecordCursor : Cursor that returns Record objects
    TupleCursor : Lighter-weight alternative returning namedtuples
    DictCursor : Dictionary-only alternative
    """

    __slots__ = ("_added", "_deleted_fields")
    _fields: List[str] = []

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
            # 2. Deleted fields
            if key in self._deleted_fields:
                raise KeyError(f"Column '{key}' has been deleted")
            # 3. Original columns
            try:
                return super().__getitem__(self._fields.index(key))
            except ValueError:
                raise KeyError(f"Column '{key}' not found")
        return super().__getitem__(key)

    def __setitem__(self, key: Union[int, str, slice], value: Any) -> None:
        if isinstance(key, str):
            if key in self._fields:
                # Existing column — revive if deleted
                self._deleted_fields.discard(key)
                super().__setitem__(self._fields.index(key), value)
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
        if name in self._fields and name not in self._deleted_fields:
            return self[name]
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
        return (key in self._fields and key not in self._deleted_fields) or \
            (self._added and key in self._added)

    @classmethod
    def set_columns(cls, columns: List[str]) -> None:
        """Set the column names for this Record class."""
        cls._fields = columns

    # ------------------------------------------------------------------ #
    # Dict-like interface
    # ------------------------------------------------------------------ #

    def keys(self) -> List[str]:
        base = [f for f in self._fields if f not in self._deleted_fields]
        if self._added:
            base.extend(self._added.keys())
        return base

    def values(self) -> Tuple[Any, ...]:
        return tuple(self[k] for k in self.keys())

    def items(self) -> Iterator[Tuple[str, Any]]:
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

        # 2. Already deleted original field?
        if key in self._deleted_fields:
            raise KeyError(key)

        # 3. Original field — delete and return
        if key in self._fields:
            value = self[key]
            self._deleted_fields.add(key)
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

    def to_dict(self) -> dict:
        return dict(self.items())

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
