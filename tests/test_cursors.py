# tests/test_cursors.py
"""
Tests for Cursor and PreparedStatement.

Uses a real SQLite database for fetch, execute, paramstyle, and record-factory
behavior. Uses duck-typed mocks only for Oracle-specific DB object conversion
(which cannot be tested without a real Oracle connection).
"""

import pytest
import sqlite3
from pathlib import Path
from unittest.mock import MagicMock, patch, call
from collections import namedtuple

from dbtk.cursors import Cursor, PreparedStatement
from dbtk.database import Database
from dbtk.record import Record


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture(scope='module')
def sqlite_db():
    """In-memory SQLite database with a small test table."""
    db = Database.create('sqlite', database=':memory:')
    cur = db.cursor()
    cur.execute("""
        CREATE TABLE warriors (
            id      INTEGER PRIMARY KEY,
            name    TEXT NOT NULL,
            nation  TEXT,
            rank    INTEGER
        )
    """)
    cur.execute("""
        INSERT INTO warriors (id, name, nation, rank) VALUES
            (1, 'Aang',   'Air', 10),
            (2, 'Katara', 'Water', 8),
            (3, 'Zuko',   'Fire', 9),
            (4, 'Toph',   'Earth', 9)
    """)
    db.commit()
    yield db
    db.close()


@pytest.fixture
def cur(sqlite_db):
    """Fresh cursor for each test."""
    return sqlite_db.cursor()


# ---------------------------------------------------------------------------
# Helpers — mock oracledb structures via duck-typing
# ---------------------------------------------------------------------------

def _make_attr(name):
    attr = MagicMock()
    attr.name = name
    return attr


def _make_obj_type(*attr_names):
    typ = MagicMock()
    typ.iscollection = False
    typ.attributes = [_make_attr(n) for n in attr_names]
    return typ


def _make_collection_type():
    typ = MagicMock()
    typ.iscollection = True
    return typ


def _make_db_object(type_obj, **field_values):
    obj = MagicMock()
    obj.type = type_obj
    for name, val in field_values.items():
        setattr(obj, name, val)
    return obj


def _make_db_collection(type_obj, items):
    obj = MagicMock()
    obj.type = type_obj
    obj.__iter__ = MagicMock(return_value=iter(items))
    return obj


# ---------------------------------------------------------------------------
# Cursor — basic fetch behavior
# ---------------------------------------------------------------------------

class TestCursorFetch:

    def test_fetchone_returns_record(self, cur):
        cur.execute("SELECT id, name FROM warriors WHERE id = 1")
        row = cur.fetchone()
        assert isinstance(row, Record)
        assert row['name'] == 'Aang'
        assert row['id'] == 1

    def test_fetchone_exhausted_returns_none(self, cur):
        cur.execute("SELECT id FROM warriors WHERE id = 999")
        assert cur.fetchone() is None

    def test_fetchall_returns_list_of_records(self, cur):
        cur.execute("SELECT id, name FROM warriors ORDER BY id")
        rows = cur.fetchall()
        assert len(rows) == 4
        assert all(isinstance(r, Record) for r in rows)
        assert [r['name'] for r in rows] == ['Aang', 'Katara', 'Zuko', 'Toph']

    def test_fetchmany_respects_size(self, cur):
        cur.execute("SELECT id FROM warriors ORDER BY id")
        rows = cur.fetchmany(2)
        assert len(rows) == 2

    def test_fetchmany_default_uses_arraysize(self, cur):
        cur.execute("SELECT id FROM warriors ORDER BY id")
        cur._cursor.arraysize = 3
        rows = cur.fetchmany()
        assert len(rows) == 3

    def test_iteration_yields_records(self, cur):
        cur.execute("SELECT id FROM warriors ORDER BY id")
        rows = list(cur)
        assert len(rows) == 4
        assert all(isinstance(r, Record) for r in rows)

    def test_record_supports_dict_and_attr_access(self, cur):
        cur.execute("SELECT id, name, nation FROM warriors WHERE id = 2")
        row = cur.fetchone()
        assert row['name'] == 'Katara'
        assert row.name == 'Katara'
        assert row[1] == 'Katara'

    def test_null_value_in_result(self, cur):
        cur.execute("SELECT id, nation FROM warriors WHERE nation IS NULL")
        # No nulls in our data, but verify None handling via explicit insert
        cur.execute("INSERT INTO warriors (id, name, nation) VALUES (99, 'Unknown', NULL)")
        cur.execute("SELECT nation FROM warriors WHERE id = 99")
        row = cur.fetchone()
        assert row['nation'] is None
        cur.execute("DELETE FROM warriors WHERE id = 99")

    def test_columns_method(self, cur):
        cur.execute("SELECT id, name, nation FROM warriors WHERE id = 1")
        assert cur.columns() == ['id', 'name', 'nation']

    def test_columns_normalized(self, cur):
        cur.execute("SELECT id AS 'My ID', name AS 'Full Name' FROM warriors WHERE id = 1")
        normalized = cur.columns(normalized=True)
        assert normalized == ['my_id', 'full_name']

    def test_columns_empty_before_execute(self, cur):
        assert cur.columns() == []


# ---------------------------------------------------------------------------
# Cursor — execute variants
# ---------------------------------------------------------------------------

class TestCursorExecute:

    def test_execute_basic(self, cur):
        cur.execute("SELECT COUNT(*) as cnt FROM warriors")
        row = cur.fetchone()
        assert row['cnt'] == 4

    def test_execute_with_positional_params(self, cur):
        cur.execute("SELECT name FROM warriors WHERE id = ?", (1,))
        assert cur.fetchone()['name'] == 'Aang'

    def test_execute_convert_params_named(self, cur):
        cur.execute(
            "SELECT name FROM warriors WHERE id = :id",
            {'id': 3},
            convert_params=True
        )
        assert cur.fetchone()['name'] == 'Zuko'

    def test_execute_convert_params_missing_defaults_none(self, cur):
        # :rank not provided — should default to None and match IS NULL
        cur.execute(
            "SELECT name FROM warriors WHERE rank = :rank OR (:rank IS NULL AND rank IS NULL)",
            {'id': 1},
            convert_params=True
        )
        # Just verify it doesn't raise

    def test_execute_convert_params_requires_dict(self, cur):
        with pytest.raises(ValueError):
            cur.execute(
                "SELECT * FROM warriors WHERE id = :id",
                (1,),
                convert_params=True
            )

    def test_execute_return_cursor_false(self, cur):
        result = cur.execute("SELECT 1")
        assert result is None

    def test_execute_return_cursor_true(self, sqlite_db):
        cur = sqlite_db.cursor(return_cursor=True)
        result = cur.execute("SELECT 1")
        assert result is cur

    def test_execute_sets_row_factory_invalid(self, cur):
        cur.execute("SELECT id FROM warriors")
        cur._row_factory_invalid = False
        cur.execute("SELECT name FROM warriors")
        assert cur._row_factory_invalid is True


# ---------------------------------------------------------------------------
# Cursor — selectinto
# ---------------------------------------------------------------------------

class TestSelectInto:

    def test_returns_single_row(self, cur):
        row = cur.selectinto("SELECT name FROM warriors WHERE id = 1")
        assert row['name'] == 'Aang'

    def test_raises_on_no_rows(self, cur):
        with pytest.raises(Exception):
            cur.selectinto("SELECT name FROM warriors WHERE id = 999")

    def test_raises_on_multiple_rows(self, cur):
        with pytest.raises(Exception):
            cur.selectinto("SELECT name FROM warriors WHERE rank = 9")


# ---------------------------------------------------------------------------
# Cursor — executemany
# ---------------------------------------------------------------------------

class TestExecuteMany:

    def test_executemany_inserts_multiple_rows(self, sqlite_db):
        db = Database.create('sqlite', database=':memory:')
        cur = db.cursor()
        cur.execute("CREATE TABLE items (id INTEGER, val TEXT)")
        cur.executemany(
            "INSERT INTO items VALUES (?, ?)",
            [(1, 'a'), (2, 'b'), (3, 'c')]
        )
        db.commit()
        cur.execute("SELECT COUNT(*) as cnt FROM items")
        assert cur.fetchone()['cnt'] == 3

    def test_executemany_return_cursor_true(self, sqlite_db):
        db = Database.create('sqlite', database=':memory:')
        cur = db.cursor(return_cursor=True)
        cur.execute("CREATE TABLE t (x INTEGER)")
        result = cur.executemany("INSERT INTO t VALUES (?)", [(1,), (2,)])
        assert result is cur


# ---------------------------------------------------------------------------
# Cursor — prepare_params
# ---------------------------------------------------------------------------

class TestPrepareParams:

    def test_positional_returns_tuple(self, cur):
        result = cur.prepare_params(['a', 'b'], {'a': 1, 'b': 2}, paramstyle='qmark')
        assert result == (1, 2)

    def test_named_returns_dict(self, cur):
        result = cur.prepare_params(['a', 'b'], {'a': 1, 'b': 2}, paramstyle='named')
        assert result == {'a': 1, 'b': 2}

    def test_missing_param_defaults_to_none(self, cur):
        result = cur.prepare_params(['a', 'b', 'c'], {'a': 1}, paramstyle='qmark')
        assert result == (1, None, None)

    def test_extra_params_ignored(self, cur):
        result = cur.prepare_params(['a'], {'a': 1, 'b': 99, 'c': 42}, paramstyle='qmark')
        assert result == (1,)

    def test_named_style_subset(self, cur):
        result = cur.prepare_params(['a'], {'a': 1, 'extra': 99}, paramstyle='named')
        assert result == {'a': 1}


# ---------------------------------------------------------------------------
# Cursor — prepared statements
# ---------------------------------------------------------------------------

class TestPreparedStatement:

    def test_prepare_query_and_execute(self, cur):
        stmt = cur.prepare_query("SELECT name FROM warriors WHERE id = :id")
        stmt.execute({'id': 1})
        row = cur.fetchone()
        assert row['name'] == 'Aang'

    def test_prepare_query_multiple_executions(self, cur):
        stmt = cur.prepare_query("SELECT name FROM warriors WHERE id = :id")
        for warrior_id, expected in [(1, 'Aang'), (2, 'Katara'), (3, 'Zuko')]:
            stmt.execute({'id': warrior_id})
            assert cur.fetchone()['name'] == expected

    def test_prepare_query_no_params(self, cur):
        stmt = cur.prepare_query("SELECT COUNT(*) as cnt FROM warriors")
        stmt.execute()
        assert cur.fetchone()['cnt'] == 4

    def test_prepare_file(self, cur, tmp_path):
        sql_file = tmp_path / 'test.sql'
        sql_file.write_text("SELECT name FROM warriors WHERE id = :id")
        stmt = cur.prepare_file(sql_file)
        stmt.execute({'id': 2})
        assert cur.fetchone()['name'] == 'Katara'

    def test_prepare_statement_init_no_source_raises(self, cur):
        with pytest.raises(ValueError):
            PreparedStatement(cur)

    def test_prepared_statement_getattr_delegates(self, cur):
        stmt = cur.prepare_query("SELECT 1")
        # description delegates to the underlying cursor
        assert stmt.description == cur.description

    def test_prepared_statement_iterable(self, cur):
        stmt = cur.prepare_query("SELECT id FROM warriors ORDER BY id")
        stmt.execute()
        rows = list(stmt)
        assert len(rows) == 4


# ---------------------------------------------------------------------------
# Cursor — execute_file
# ---------------------------------------------------------------------------

class TestExecuteFile:

    def test_execute_file_basic(self, cur, tmp_path):
        sql_file = tmp_path / 'query.sql'
        sql_file.write_text("SELECT name FROM warriors WHERE id = :id")
        cur.execute_file(sql_file, {'id': 4})
        assert cur.fetchone()['name'] == 'Toph'

    def test_execute_file_no_params(self, cur, tmp_path):
        sql_file = tmp_path / 'all.sql'
        sql_file.write_text("SELECT COUNT(*) as cnt FROM warriors")
        cur.execute_file(sql_file)
        assert cur.fetchone()['cnt'] == 4

    def test_execute_file_missing_raises(self, cur):
        with pytest.raises(FileNotFoundError):
            cur.execute_file('/nonexistent/path/query.sql')


# ---------------------------------------------------------------------------
# Cursor — attribute routing
# ---------------------------------------------------------------------------

class TestCursorAttributeRouting:

    def test_getattr_delegates_to_underlying_cursor(self, cur):
        # arraysize lives on the underlying cursor
        assert hasattr(cur, 'arraysize')

    def test_setattr_local_stays_local(self, cur):
        cur.debug = True
        assert cur.__dict__['debug'] is True

    def test_statement_fallback_when_cursor_lacks_it(self, cur):
        cur.execute("SELECT 1")
        # SQLite cursors don't expose .statement — our wrapper stores it
        stmt = cur.statement
        assert stmt is not None

    def test_description_delegates(self, cur):
        cur.execute("SELECT id, name FROM warriors WHERE id = 1")
        desc = cur.description
        assert desc is not None
        col_names = [col[0] for col in desc]
        assert col_names == ['id', 'name']

    def test_dir_includes_local_and_cursor_attrs(self, cur):
        attrs = dir(cur)
        assert 'fetchone' in attrs
        assert 'connection' in attrs
        assert 'debug' in attrs


# ---------------------------------------------------------------------------
# Cursor — record factory lifecycle
# ---------------------------------------------------------------------------

class TestRecordFactory:

    def test_record_factory_created_on_first_fetch(self, cur):
        assert cur.record_factory is None
        cur.execute("SELECT id FROM warriors")
        cur.fetchone()
        assert cur.record_factory is not None

    def test_record_factory_recreated_on_column_change(self, cur):
        cur.execute("SELECT id FROM warriors")
        cur.fetchone()
        first_factory = cur.record_factory

        cur.execute("SELECT id, name FROM warriors")
        cur.fetchone()
        assert cur.record_factory is not first_factory

    def test_is_ready_raises_before_execute(self, cur):
        with pytest.raises(Exception):
            cur._is_ready()

    def test_object_columns_empty_for_sqlite(self, cur):
        cur.execute("SELECT id, name FROM warriors")
        cur.fetchone()
        assert cur._object_columns == ()


# ---------------------------------------------------------------------------
# Cursor — DB object conversion (duck-typed mocks, no Oracle required)
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

    def test_empty_collection(self):
        typ = _make_collection_type()
        obj = _make_db_collection(typ, [])
        assert Cursor._convert_db_object(obj) == []

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
        assert Cursor._convert_db_object(obj) == {'A': 42, 'B': None}

    def test_non_db_object_passthrough(self):
        assert Cursor._convert_db_object('hello') == 'hello'
        assert Cursor._convert_db_object(123) == 123
        assert Cursor._convert_db_object(None) is None
        assert Cursor._convert_db_object({'a': 1}) == {'a': 1}
        assert Cursor._convert_db_object([1, 2, 3]) == [1, 2, 3]


class TestConvertRow:

    def _cur(self, object_columns):
        cur = MagicMock(spec=Cursor)
        cur._object_columns = object_columns
        return cur

    def test_no_object_columns_returns_same_object(self):
        cur = self._cur(())
        row = ('hello', 42, None)
        assert Cursor._convert_row(cur, row) is row

    def test_converts_flagged_column_only(self):
        typ = _make_obj_type('A')
        obj = _make_db_object(typ, A=99)
        cur = self._cur((1,))
        result = Cursor._convert_row(cur, ('plain', obj, 'also_plain'))
        assert result[0] == 'plain'
        assert result[1] == {'A': 99}
        assert result[2] == 'also_plain'

    def test_null_object_column_stays_none(self):
        cur = self._cur((0,))
        result = Cursor._convert_row(cur, (None, 'other'))
        assert result[0] is None
        assert result[1] == 'other'

    def test_multiple_object_columns(self):
        obj_typ = _make_obj_type('X')
        obj = _make_db_object(obj_typ, X=7)
        coll_typ = _make_collection_type()
        coll = _make_db_collection(coll_typ, [1, 2])
        cur = self._cur((1, 2))
        result = Cursor._convert_row(cur, ('id', obj, coll))
        assert result[0] == 'id'
        assert result[1] == {'X': 7}
        assert result[2] == [1, 2]


# ---------------------------------------------------------------------------
# Cursor — debug mode
# ---------------------------------------------------------------------------

class TestDebugMode:

    def test_debug_logs_query(self, sqlite_db):
        cur = sqlite_db.cursor(debug=True)
        with patch.object(cur.connection.driver, '__name__', 'sqlite3'):
            import logging
            with patch('dbtk.cursors.logger') as mock_log:
                cur.execute("SELECT 1")
                assert mock_log.debug.called
