# tests/test_cursors.py
"""
Tests for Cursor._convert_db_object and _convert_row using duck-typed mocks
that mimic oracledb DbObject/DbObjectType/DbObjectAttr without importing oracledb.
"""

import pytest
from unittest.mock import MagicMock
from dbtk.cursors import Cursor


# ---------------------------------------------------------------------------
# Helpers — mock oracledb structures via duck-typing
# ---------------------------------------------------------------------------

def _make_attr(name):
    attr = MagicMock()
    attr.name = name
    return attr


def _make_obj_type(*attr_names):
    """DbObjectType for a plain OBJECT (not a collection)."""
    typ = MagicMock()
    typ.iscollection = False
    typ.attributes = [_make_attr(n) for n in attr_names]
    return typ


def _make_collection_type():
    """DbObjectType for a VARRAY / nested TABLE."""
    typ = MagicMock()
    typ.iscollection = True
    return typ


def _make_db_object(type_obj, **field_values):
    """Mimic a DbObject instance with a .type and attribute access."""
    obj = MagicMock()
    obj.type = type_obj
    for name, val in field_values.items():
        setattr(obj, name, val)
    return obj


def _make_db_collection(type_obj, items):
    """Mimic a DbObject collection (iterable)."""
    obj = MagicMock()
    obj.type = type_obj
    obj.__iter__ = MagicMock(return_value=iter(items))
    return obj


# ---------------------------------------------------------------------------
# Tests for _convert_db_object
# ---------------------------------------------------------------------------

class TestConvertDbObject:

    def test_plain_object_to_dict(self):
        typ = _make_obj_type('CRN', 'LEVEL1', 'LEVEL2')
        obj = _make_db_object(typ, CRN=1001, LEVEL1='UG', LEVEL2='GRAD')
        result = Cursor._convert_db_object(obj)
        assert result == {'CRN': 1001, 'LEVEL1': 'UG', 'LEVEL2': 'GRAD'}

    def test_collection_to_list(self):
        typ = _make_collection_type()
        obj = _make_db_collection(typ, ['one', 'two', 'three'])
        result = Cursor._convert_db_object(obj)
        assert result == ['one', 'two', 'three']

    def test_nested_object_in_object(self):
        inner_typ = _make_obj_type('X', 'Y')
        inner_obj = _make_db_object(inner_typ, X=10, Y=20)

        outer_typ = _make_obj_type('NAME', 'INNER')
        outer_obj = _make_db_object(outer_typ, NAME='foo', INNER=inner_obj)

        result = Cursor._convert_db_object(outer_obj)
        assert result == {'NAME': 'foo', 'INNER': {'X': 10, 'Y': 20}}

    def test_collection_of_objects(self):
        item_typ = _make_obj_type('ID', 'VAL')
        items = [
            _make_db_object(item_typ, ID=1, VAL='a'),
            _make_db_object(item_typ, ID=2, VAL='b'),
        ]
        coll_typ = _make_collection_type()
        coll = _make_db_collection(coll_typ, items)

        result = Cursor._convert_db_object(coll)
        assert result == [{'ID': 1, 'VAL': 'a'}, {'ID': 2, 'VAL': 'b'}]

    def test_none_attribute_preserved(self):
        typ = _make_obj_type('A', 'B')
        obj = _make_db_object(typ, A=42, B=None)
        result = Cursor._convert_db_object(obj)
        assert result == {'A': 42, 'B': None}

    def test_non_db_object_passthrough(self):
        assert Cursor._convert_db_object('hello') == 'hello'
        assert Cursor._convert_db_object(123) == 123
        assert Cursor._convert_db_object(None) is None
        assert Cursor._convert_db_object({'a': 1}) == {'a': 1}
        assert Cursor._convert_db_object([1, 2, 3]) == [1, 2, 3]


# ---------------------------------------------------------------------------
# Tests for _convert_row
# ---------------------------------------------------------------------------

class TestConvertRow:

    def _make_cursor(self, object_columns):
        """Build a minimal Cursor-like object with _object_columns set."""
        cur = MagicMock(spec=Cursor)
        cur._object_columns = object_columns
        cur._convert_row = lambda row: Cursor._convert_row(cur, row)
        return cur

    def test_no_object_columns_returns_row_unchanged(self):
        cur = self._make_cursor(())
        row = ('hello', 42, None)
        assert Cursor._convert_row(cur, row) is row

    def test_converts_flagged_columns_only(self):
        typ = _make_obj_type('A')
        obj = _make_db_object(typ, A=99)

        cur = self._make_cursor((1,))
        row = ('plain', obj, 'also_plain')
        result = Cursor._convert_row(cur, row)
        assert result[0] == 'plain'
        assert result[1] == {'A': 99}
        assert result[2] == 'also_plain'

    def test_null_object_column_stays_none(self):
        cur = self._make_cursor((0,))
        row = (None, 'other')
        result = Cursor._convert_row(cur, row)
        assert result[0] is None
        assert result[1] == 'other'

    def test_multiple_object_columns(self):
        obj_typ = _make_obj_type('X')
        obj = _make_db_object(obj_typ, X=7)

        coll_typ = _make_collection_type()
        coll = _make_db_collection(coll_typ, [1, 2])

        cur = self._make_cursor((1, 2))
        row = ('id', obj, coll)
        result = Cursor._convert_row(cur, row)
        assert result[0] == 'id'
        assert result[1] == {'X': 7}
        assert result[2] == [1, 2]
