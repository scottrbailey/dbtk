# dbtk/record.py
"""
Record classes for database result sets.
"""

from typing import List, Any, Iterator, Tuple, Union
from datetime import date as _date


class Record(list):
    """
    Row object that allows access by:
       column name   row['column_name']
       attributes    row.column_name
       column index  row[3]
       slicing       row[1:4]

    Record will be dynamically subclassed each time a cursor is executed
    with different column names.
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