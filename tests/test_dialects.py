# tests/test_dialects.py
"""
Tests for dialect-level schema introspection helpers.
"""

from unittest.mock import MagicMock

import pytest

from dbtk.dialects import MySQLDialect, OracleDialect, PostgresDialect, SQLServerDialect


def _mock_cursor() -> MagicMock:
    """Cursor stub that records execute() calls and yields no column rows."""
    cursor = MagicMock()
    cursor.fetchone.return_value = None
    cursor.__iter__.return_value = iter([])
    return cursor


def _bind_params(cursor: MagicMock) -> list:
    """Bind-parameter dicts passed to each cursor.execute() call, in order."""
    return [call.args[1] for call in cursor.execute.call_args_list]


class TestPostgresTableMetadataNames:
    """PostgresDialect.table_metadata() table-name parsing."""

    @pytest.mark.parametrize('name, schema, table', [
        ('earth_kingdom', None, 'earth_kingdom'),
        ('ba_sing_se.earth_kingdom', 'ba_sing_se', 'earth_kingdom'),
        ('omashu.ba_sing_se.earth_kingdom', 'ba_sing_se', 'earth_kingdom'),
    ])
    def test_schema_and_table_extracted(self, name: str, schema: str, table: str) -> None:
        """One-, two-, and three-part names resolve to the trailing schema/table parts."""
        cursor = _mock_cursor()
        PostgresDialect().table_metadata(cursor, name, add_comments=True)

        params = _bind_params(cursor)
        assert params, "expected table_metadata to query the catalog"
        for p in params:
            assert p == {'table_name': table, 'schema': schema}

    def test_three_part_name_is_lowercased(self) -> None:
        """Postgres folds unquoted identifiers to lower case."""
        cursor = _mock_cursor()
        PostgresDialect().table_metadata(cursor, 'Omashu.Ba_Sing_Se.Earth_Kingdom', add_comments=False)
        assert _bind_params(cursor) == [{'table_name': 'earth_kingdom', 'schema': 'ba_sing_se'}]


class TestOracleTableMetadataNames:
    """OracleDialect.table_metadata() table-name parsing."""

    @pytest.mark.parametrize('name, schema, table', [
        ('fire_nation', None, 'FIRE_NATION'),
        ('ozai.fire_nation', 'OZAI', 'FIRE_NATION'),
        ('caldera.ozai.fire_nation', 'OZAI', 'FIRE_NATION'),
    ])
    def test_schema_and_table_extracted(self, name: str, schema: str, table: str) -> None:
        """One-, two-, and three-part names resolve to the trailing schema/table parts, upper-cased."""
        cursor = _mock_cursor()
        OracleDialect().table_metadata(cursor, name, add_comments=True)

        params = _bind_params(cursor)
        assert params, "expected table_metadata to query the catalog"
        for p in params:
            assert p['table_name'] == table
            assert p['schema_name'] == schema


class TestSQLServerTableMetadataNames:
    """SQLServerDialect.table_metadata() table-name parsing."""

    @pytest.mark.parametrize('name, schema, table', [
        ('WaterTribe', None, 'WaterTribe'),
        ('north.WaterTribe', 'north', 'WaterTribe'),
        ('Agna_Qel_a.north.WaterTribe', 'north', 'WaterTribe'),
    ])
    def test_schema_and_table_extracted(self, name: str, schema: str, table: str) -> None:
        """One-, two-, and three-part names bind the trailing schema/table parts, case preserved."""
        cursor = _mock_cursor()
        SQLServerDialect().table_metadata(cursor, name, add_comments=True)

        params = _bind_params(cursor)
        assert params == [(table, schema), (table, schema)]

    def test_joins_are_schema_aware(self) -> None:
        """Same-named tables in different schemas must not cross-match PKs or comments."""
        cursor = _mock_cursor()
        SQLServerDialect().table_metadata(cursor, 'north.WaterTribe', add_comments=False)

        col_query = cursor.execute.call_args.args[0]
        assert 'c.table_schema = pk.table_schema' in col_query
        assert 'SCHEMA_NAME(t.schema_id) = c.table_schema' in col_query
        assert 'c.table_schema = COALESCE(?, c.table_schema)' in col_query


class TestMySQLTableMetadataNames:
    """MySQLDialect.table_metadata() table-name parsing."""

    @pytest.mark.parametrize('name, schema, table', [
        ('air_temples', None, 'air_temples'),
        ('nomads.air_temples', 'nomads', 'air_temples'),
    ])
    def test_schema_and_table_extracted(self, name: str, schema: str, table: str) -> None:
        """A `db.table` name binds the database as the schema; unqualified falls back to DATABASE()."""
        cursor = _mock_cursor()
        MySQLDialect().table_metadata(cursor, name, add_comments=True)

        params = _bind_params(cursor)
        assert params == [(table, schema), (table, schema)]
        for call in cursor.execute.call_args_list:
            assert 'COALESCE(%s, DATABASE())' in call.args[0]
