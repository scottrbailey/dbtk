# tests/test_dialects.py
"""
Tests for dialect-level schema introspection helpers.
"""

from unittest.mock import MagicMock

import pytest

from dbtk.dialects import OracleDialect, PostgresDialect


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
