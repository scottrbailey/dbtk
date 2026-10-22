# tests/test_dialects.py
"""
Tests for dialect-level schema introspection helpers.

No live databases here: a stub cursor records every execute() call and hands
back scripted fetchone() results, so these tests check the SQL and bind values
each dialect generates for one-, two-, and three-part table names.
"""

from typing import Any, List, Optional, Tuple
from unittest.mock import MagicMock

import pytest

from dbtk.dialects import (DatabaseDialect, MySQLDialect, OracleDialect,
                           PostgresDialect, SQLServerDialect)


def _mock_cursor(fetchone_results: Optional[List[Any]] = None) -> MagicMock:
    """Cursor stub: records execute() calls, returns `fetchone_results` in order
    (then None), and yields no column rows."""
    cursor = MagicMock()
    results = list(fetchone_results or [])
    cursor.fetchone.side_effect = lambda: results.pop(0) if results else None
    cursor.__iter__.return_value = iter([])
    return cursor


def _calls(cursor: MagicMock) -> List[Tuple[str, Any]]:
    """(sql, params) for each cursor.execute() call, in order."""
    return [(c.args[0], c.args[1] if len(c.args) > 1 else None)
            for c in cursor.execute.call_args_list]


class TestSplitTableName:
    """DatabaseDialect.split_table_name() shared parsing."""

    @pytest.mark.parametrize('name, expected', [
        ('t', (None, None, 't')),
        ('s.t', (None, 's', 't')),
        ('d.s.t', ('d', 's', 't')),
    ])
    def test_parts(self, name: str, expected: tuple) -> None:
        assert DatabaseDialect().split_table_name(name) == expected

    @pytest.mark.parametrize('name', ['d..t', '.t', 's.'])
    def test_empty_part_raises(self, name: str) -> None:
        with pytest.raises(ValueError, match='empty name part'):
            DatabaseDialect().split_table_name(name)

    def test_too_many_parts_raises(self) -> None:
        with pytest.raises(ValueError, match='at most 3-part'):
            DatabaseDialect().split_table_name('a.b.c.d')


class TestPostgresTableMetadataNames:
    """PostgresDialect.table_metadata() name handling."""

    def test_unqualified_name_resolved_through_search_path(self) -> None:
        cursor = _mock_cursor([('ba_sing_se',)])
        PostgresDialect().table_metadata(cursor, 'earth_kingdom', add_comments=False)

        calls = _calls(cursor)
        assert 'to_regclass(quote_ident(' in calls[0][0]
        assert calls[0][1] == {'table_name': 'earth_kingdom'}
        assert calls[-1][1] == {'table_name': 'earth_kingdom', 'schema': 'ba_sing_se'}

    def test_unqualified_name_not_on_search_path_matches_any_schema(self) -> None:
        cursor = _mock_cursor([None])
        PostgresDialect().table_metadata(cursor, 'earth_kingdom', add_comments=False)
        assert _calls(cursor)[-1][1] == {'table_name': 'earth_kingdom', 'schema': None}

    def test_two_part_name_skips_lookups(self) -> None:
        cursor = _mock_cursor()
        PostgresDialect().table_metadata(cursor, 'Ba_Sing_Se.Earth_Kingdom', add_comments=True)

        calls = _calls(cursor)
        assert len(calls) == 2  # comment query + column query
        for _, params in calls:
            assert params == {'table_name': 'earth_kingdom', 'schema': 'ba_sing_se'}

    def test_three_part_name_in_current_database(self) -> None:
        cursor = _mock_cursor([('Omashu',)])
        PostgresDialect().table_metadata(cursor, 'omashu.ba_sing_se.earth_kingdom',
                                         add_comments=False)

        calls = _calls(cursor)
        assert calls[0][0] == 'SELECT current_database()'
        assert calls[-1][1] == {'table_name': 'earth_kingdom', 'schema': 'ba_sing_se'}

    def test_three_part_name_in_other_database_raises(self) -> None:
        cursor = _mock_cursor([('omashu',)])
        with pytest.raises(ValueError, match="connected to database 'omashu'"):
            PostgresDialect().table_metadata(cursor, 'republic_city.ba_sing_se.earth_kingdom',
                                             add_comments=False)

    def test_four_part_name_raises(self) -> None:
        with pytest.raises(ValueError, match='at most 3-part'):
            PostgresDialect().table_metadata(_mock_cursor(), 'a.b.c.d', add_comments=False)

    def test_joins_are_schema_aware(self) -> None:
        cursor = _mock_cursor()
        PostgresDialect().table_metadata(cursor, 'ba_sing_se.earth_kingdom', add_comments=False)

        col_query = _calls(cursor)[-1][0]
        assert 'pk.table_schema = c.table_schema' in col_query
        assert 'kcu.constraint_schema = tc.constraint_schema' in col_query
        assert 'pgc.relnamespace = pgn.oid' in col_query


class TestOracleTableMetadataNames:
    """OracleDialect.table_metadata() name handling."""

    @pytest.mark.parametrize('name, schema, table', [
        ('fire_nation', None, 'FIRE_NATION'),
        ('ozai.fire_nation', 'OZAI', 'FIRE_NATION'),
    ])
    def test_schema_and_table_extracted(self, name: str, schema: str, table: str) -> None:
        """One- and two-part names resolve to schema/table parts, upper-cased."""
        cursor = _mock_cursor()
        OracleDialect().table_metadata(cursor, name, add_comments=True)

        params = [p for _, p in _calls(cursor)]
        assert params, "expected table_metadata to query the catalog"
        for p in params:
            assert p['table_name'] == table
            assert p['schema_name'] == schema

    def test_three_part_name_raises_with_hint(self) -> None:
        with pytest.raises(ValueError, match='database link'):
            OracleDialect().table_metadata(_mock_cursor(), 'caldera.ozai.fire_nation',
                                           add_comments=False)


class TestSQLServerTableMetadataNames:
    """SQLServerDialect.table_metadata() name handling."""

    def test_unqualified_name_resolved_like_a_query(self) -> None:
        cursor = _mock_cursor([('north',)])
        SQLServerDialect().table_metadata(cursor, 'WaterTribe', add_comments=False)

        calls = _calls(cursor)
        assert calls[0] == ('SELECT OBJECT_SCHEMA_NAME(OBJECT_ID(QUOTENAME(?)))', ('WaterTribe',))
        assert calls[-1][1] == ('WaterTribe', 'north')

    def test_unqualified_name_not_found_matches_any_schema(self) -> None:
        cursor = _mock_cursor([(None,)])
        SQLServerDialect().table_metadata(cursor, 'WaterTribe', add_comments=False)
        assert _calls(cursor)[-1][1] == ('WaterTribe', None)

    def test_two_part_name_uses_current_database_catalog(self) -> None:
        cursor = _mock_cursor()
        SQLServerDialect().table_metadata(cursor, 'north.WaterTribe', add_comments=True)

        calls = _calls(cursor)
        assert len(calls) == 2  # comment query + column query, no schema lookup
        for sql, params in calls:
            assert params == ('WaterTribe', 'north')
            assert '[' not in sql  # no database prefix
        assert 'FROM sys.tables t' in calls[0][0]

    def test_three_part_name_queries_that_databases_catalog(self) -> None:
        cursor = _mock_cursor()
        SQLServerDialect().table_metadata(cursor, 'Agna_Qel_a.north.WaterTribe',
                                          add_comments=True)

        cmt_query, col_query = (sql for sql, _ in _calls(cursor))
        assert 'FROM [Agna_Qel_a].sys.tables t' in cmt_query
        assert 'JOIN [Agna_Qel_a].sys.schemas s' in cmt_query
        assert 'FROM [Agna_Qel_a].information_schema.columns c' in col_query
        assert 'JOIN [Agna_Qel_a].sys.extended_properties ep' in col_query
        # SCHEMA_NAME() only sees the current database, so it must not be used
        assert 'SCHEMA_NAME(' not in cmt_query + col_query
        for _, params in _calls(cursor):
            assert params == ('WaterTribe', 'north')

    def test_database_name_is_bracket_escaped(self) -> None:
        cursor = _mock_cursor()
        SQLServerDialect().table_metadata(cursor, 'odd]name.north.WaterTribe', add_comments=False)
        assert 'FROM [odd]]name].information_schema.columns c' in _calls(cursor)[-1][0]

    def test_joins_are_schema_aware(self) -> None:
        """Same-named tables in different schemas must not cross-match PKs or comments."""
        cursor = _mock_cursor()
        SQLServerDialect().table_metadata(cursor, 'north.WaterTribe', add_comments=False)

        col_query = _calls(cursor)[-1][0]
        assert 'c.table_schema = pk.table_schema' in col_query
        assert 'tc.constraint_schema = kcu.constraint_schema' in col_query
        assert 't.schema_id = s.schema_id' in col_query
        assert 'c.table_schema = COALESCE(?, c.table_schema)' in col_query


class TestMySQLTableMetadataNames:
    """MySQLDialect.table_metadata() name handling."""

    @pytest.mark.parametrize('name, schema, table', [
        ('air_temples', None, 'air_temples'),
        ('nomads.air_temples', 'nomads', 'air_temples'),
    ])
    def test_schema_and_table_extracted(self, name: str, schema: str, table: str) -> None:
        """A `db.table` name binds the database as the schema; unqualified falls back to DATABASE()."""
        cursor = _mock_cursor()
        MySQLDialect().table_metadata(cursor, name, add_comments=True)

        calls = _calls(cursor)
        assert [p for _, p in calls] == [(table, schema), (table, schema)]
        for sql, _ in calls:
            assert 'COALESCE(%s, DATABASE())' in sql

    def test_three_part_name_raises_with_hint(self) -> None:
        with pytest.raises(ValueError, match='database.table'):
            MySQLDialect().table_metadata(_mock_cursor(), 'a.nomads.air_temples',
                                          add_comments=False)
