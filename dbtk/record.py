# dbtk/record.py
"""
Record classes for database result sets.
"""

from typing import List, Any, Iterator, Tuple, Union
from datetime import date as _date


class Record(list):
    """
    Flexible row object supporting multiple access patterns.

    Record extends list to provide a rich interface for accessing query result rows.
    It supports attribute access, dictionary-style key access, integer indexing, and
    slicing - all on the same object. This makes it the most flexible cursor type,
    ideal when you need different access patterns in different parts of your code.

    The Record class is dynamically subclassed for each query to set column names
    as class attributes, enabling attribute access while maintaining list semantics.

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

    __slots__ = ('_deleted_fields',)
    _fields: List[str] = []

    def __init__(self, *args):
        super().__init__()
        self[:] = args
        self._deleted_fields = set()  # Track deleted fields per instance

    @classmethod
    def set_columns(cls, columns: List[str]) -> None:
        """Set the column names for this Record class."""
        cls._fields = columns

    def __getattr__(self, attr: str) -> Any:
        """Allow attribute access to columns."""
        if attr in self._fields and attr not in self._deleted_fields:
            return self[self._fields.index(attr)]
        raise AttributeError(f"'Record' object has no attribute '{attr}'")

    def __getitem__(self, item: Union[int, str, slice]) -> Any:
        """Allow access by column name, index, or slice."""
        if isinstance(item, str):
            if item in self._deleted_fields:
                raise KeyError(f"Column '{item}' has been deleted")
            try:
                return super().__getitem__(self._fields.index(item))
            except ValueError:
                raise KeyError(f"Column '{item}' not found")
        return super().__getitem__(item)

    def __setitem__(self, item: Union[int, str, slice], val: Any) -> None:
        """Allow setting by column name, index, or slice."""
        if isinstance(item, str):
            # If we're setting a deleted field, bring it back to life
            if item in self._deleted_fields:
                self._deleted_fields.remove(item)
            try:
                super().__setitem__(self._fields.index(item), val)
            except ValueError:
                raise KeyError(f"Column '{item}' not found")
        else:
            super().__setitem__(item, val)

    def __delitem__(self, item: Union[int, str]) -> None:
        """Allow deletion by column name or index."""
        if isinstance(item, str):
            if item not in self._fields:
                raise KeyError(f"Column '{item}' not found")
            if item in self._deleted_fields:
                raise KeyError(f"Column '{item}' already deleted")
            self._deleted_fields.add(item)
        else:
            # For numeric index deletion, we'd need to track by position
            # which gets complex with the current design. Could implement if needed.
            raise TypeError("Cannot delete by numeric index")

    def __dir__(self) -> List[str]:
        """Return list of available attributes."""
        active_fields = [f for f in self._fields if f not in self._deleted_fields]
        return active_fields + ['copy', 'get', 'items', 'keys', 'values', 'pprint', 'pop', 'update']

    def __str__(self) -> str:
        """String representation as key-value pairs."""
        return (
                self.__class__.__name__ + '{' +
                ', '.join(f"'{k}': {v!r}" for k, v in self.items()) +
                '}'
        )

    def __repr__(self) -> str:
        """Representation showing values only."""
        return (
                self.__class__.__name__ + '(' +
                ', '.join(f"{v!r}" for v in self.values()) +
                ')'
        )

    def __contains__(self, key: str) -> bool:
        """Support 'in' operator for key checking."""
        return key in self._fields and key not in self._deleted_fields

    def __len__(self) -> int:
        """Return number of active fields."""
        return len([f for f in self._fields if f not in self._deleted_fields])

    def __iter__(self) -> Iterator[str]:
        """Iterate over active keys (field names)."""
        return iter(f for f in self._fields if f not in self._deleted_fields)

    def items(self) -> Iterator[Tuple[str, Any]]:
        """Return iterator of (key, value) pairs for active fields."""
        for field in self._fields:
            if field not in self._deleted_fields:
                yield field, self[field]

    def keys(self) -> List[str]:
        """Return list of active column names."""
        return [f for f in self._fields if f not in self._deleted_fields]

    def values(self) -> Tuple[Any, ...]:
        """Return tuple of values for active fields."""
        return tuple(self[field] for field in self._fields if field not in self._deleted_fields)

    def get(self, key: str, default: Any = None) -> Any:
        """Get value by key with optional default."""
        try:
            return self[key]
        except KeyError:
            return default

    def pop(self, key: str, *args) -> Any:
        """Remove and return value for key, with optional default."""
        if len(args) > 1:
            raise TypeError(f"pop expected at most 2 arguments, got {1 + len(args)}")

        try:
            if key in self._deleted_fields:
                raise KeyError(f"Column '{key}' already deleted")
            value = self[key]  # Get current value
            self._deleted_fields.add(key)
            return value
        except KeyError:
            if args:
                return args[0]
            raise

    def update(self, other=None, **kwargs) -> None:
        """Update record with key-value pairs from dict or kwargs."""
        if other:
            if hasattr(other, 'items'):
                for key, value in other.items():
                    self[key] = value
            else:
                for key, value in other:
                    self[key] = value

        for key, value in kwargs.items():
            self[key] = value

    def setdefault(self, key: str, default: Any = None) -> Any:
        """Get key value or set and return default if key doesn't exist."""
        try:
            return self[key]
        except KeyError:
            # Can't add new columns to Record, so just return default
            return default

    def to_dict(self) -> dict:
        """Return dictionary representation of active fields."""
        return dict(self.items())

    def copy(self) -> dict:
        """Return dictionary representation of active fields. The name is a bit misleading, but is needed for dict compatibility."""
        return self.to_dict()

    def pprint(self) -> None:
        """Pretty print the record."""
        active_fields = self.keys()
        if not active_fields:
            print("Empty record")
            return

        col_width = max(len(field) for field in active_fields)
        template = f"{{0:<{col_width}}} : {{1}}"

        for field in active_fields:
            value = self[field]
            print(template.format(field, _format_value(value)))


def _format_value(obj: Any) -> str:
    """Convert a record value to string representation."""
    if obj is None:
        return '(NULL)'
    elif isinstance(obj, _date):
        # datetime.datetime is subclassed from date
        if hasattr(obj, 'microsecond') and obj.microsecond:
            return obj.strftime('%Y-%m-%d %H:%M:%S.%f %Z')
        elif hasattr(obj, 'hour') and (obj.hour or obj.minute or obj.second):
            return obj.strftime('%Y-%m-%d %H:%M:%S')
        else:
            return obj.strftime('%Y-%m-%d')
    else:
        return str(obj)