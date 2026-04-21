# tests/test_query_lookup.py
"""Tests for QueryLookup — PreparedStatement-based Table column transforms."""

import pytest
from pathlib import Path

from dbtk.database import Database
from dbtk.etl.table import Table
from dbtk.etl.transforms.database import QueryLookup


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def sqlite_db():
    db = Database.create('sqlite', database=':memory:')
    yield db
    db.close()


@pytest.fixture
def cursor(sqlite_db):
    return sqlite_db.cursor()


@pytest.fixture
def nations_schema(cursor):
    """Small nations reference table used by lookup tests."""
    cursor.execute("""
        CREATE TABLE nations (
            nation_id   TEXT PRIMARY KEY,
            name        TEXT NOT NULL,
            capital     TEXT NOT NULL,
            bender_type TEXT
        )
    """)
    cursor.execute("""
        INSERT INTO nations VALUES
            ('FN', 'Fire Nation',  'Fire Nation Capital', 'firebending'),
            ('EK', 'Earth Kingdom','Ba Sing Se',          'earthbending'),
            ('WA', 'Water Tribes', 'North Pole',          'waterbending'),
            ('AN', 'Air Nomads',   'Eastern Air Temple',  'airbending')
    """)
    cursor.connection.commit()


@pytest.fixture
def people_schema(cursor):
    """People table for multi-column key tests."""
    cursor.execute("""
        CREATE TABLE people (
            first_name  TEXT NOT NULL,
            last_name   TEXT NOT NULL,
            person_id   INTEGER PRIMARY KEY,
            nation_id   TEXT
        )
    """)
    cursor.execute("""
        INSERT INTO people VALUES
            ('Aang',  'Avatar',  1, 'AN'),
            ('Zuko',  'Ozai',    2, 'FN'),
            ('Katara','Tribe',   3, 'WA'),
            ('Toph',  'Beifong', 4, 'EK')
    """)
    cursor.connection.commit()


# ---------------------------------------------------------------------------
# Init validation
# ---------------------------------------------------------------------------

class TestQueryLookupInit:

    def test_query_only(self):
        q = QueryLookup(query='SELECT 1')
        assert q.query == 'SELECT 1'
        assert q.filename is None
        assert q.return_col is None
        assert q.missing is None

    def test_filename_only(self, tmp_path):
        f = tmp_path / 'lookup.sql'
        f.write_text('SELECT 1')
        q = QueryLookup(filename=f)
        assert q.filename == f

    def test_all_params(self):
        q = QueryLookup(query='SELECT 1', return_col='id', missing=0)
        assert q.return_col == 'id'
        assert q.missing == 0

    def test_return_col_star(self):
        q = QueryLookup(query='SELECT 1', return_col='*')
        assert q.return_col == '*'

    def test_requires_query_or_filename(self):
        with pytest.raises(ValueError, match="query.*filename"):
            QueryLookup()


# ---------------------------------------------------------------------------
# Bind and callable behaviour
# ---------------------------------------------------------------------------

class TestQueryLookupBind:

    def test_bind_returns_callable(self, cursor, nations_schema):
        q = QueryLookup(query='SELECT nation_id FROM nations WHERE name = :name')
        fn = q.bind(cursor)
        assert callable(fn)

    def test_scalar_field_lookup(self, cursor, nations_schema):
        # Single-param SQL, scalar input — _make_bind_vars maps value to first param
        fn = QueryLookup(
            query='SELECT nation_id FROM nations WHERE name = :name'
        ).bind(cursor)
        assert fn({'name': 'Fire Nation'}) == 'FN'

    def test_return_col_by_name(self, cursor, nations_schema):
        fn = QueryLookup(
            query='SELECT nation_id, capital FROM nations WHERE name = :name',
            return_col='capital'
        ).bind(cursor)
        assert fn({'name': 'Air Nomads'}) == 'Eastern Air Temple'

    def test_return_col_star_returns_row(self, cursor, nations_schema):
        fn = QueryLookup(
            query='SELECT nation_id, name FROM nations WHERE nation_id = :nation_id',
            return_col='*'
        ).bind(cursor)
        row = fn({'nation_id': 'EK'})
        # Row object supports named access
        assert row['nation_id'] == 'EK'
        assert row['name'] == 'Earth Kingdom'

    def test_no_return_col_returns_first_column(self, cursor, nations_schema):
        fn = QueryLookup(
            query='SELECT capital, bender_type FROM nations WHERE nation_id = :nation_id'
        ).bind(cursor)
        # no return_col → row[0] → capital
        assert fn({'nation_id': 'WA'}) == 'North Pole'

    def test_missing_default_none(self, cursor, nations_schema):
        fn = QueryLookup(
            query='SELECT nation_id FROM nations WHERE name = :name'
        ).bind(cursor)
        assert fn({'name': 'Spirit World'}) is None

    def test_missing_custom_value(self, cursor, nations_schema):
        fn = QueryLookup(
            query='SELECT nation_id FROM nations WHERE name = :name',
            missing='UNKNOWN'
        ).bind(cursor)
        assert fn({'name': 'Spirit World'}) == 'UNKNOWN'

    def test_multi_param_dict_input(self, cursor, people_schema):
        fn = QueryLookup(
            query='SELECT person_id FROM people WHERE first_name = :first_name AND last_name = :last_name',
            return_col='person_id'
        ).bind(cursor)
        assert fn({'first_name': 'Toph', 'last_name': 'Beifong'}) == 4

    def test_extra_keys_in_dict_ignored(self, cursor, nations_schema):
        # PreparedStatement ignores bind-var keys not referenced in the SQL
        fn = QueryLookup(
            query='SELECT capital FROM nations WHERE nation_id = :nation_id'
        ).bind(cursor)
        result = fn({'nation_id': 'FN', 'irrelevant_column': 'noise', 'another': 123})
        assert result == 'Fire Nation Capital'

    def test_sql_file(self, cursor, nations_schema, tmp_path):
        sql_file = tmp_path / 'nation_lookup.sql'
        sql_file.write_text('SELECT capital FROM nations WHERE nation_id = :nation_id')
        fn = QueryLookup(filename=sql_file).bind(cursor)
        assert fn({'nation_id': 'AN'}) == 'Eastern Air Temple'


# ---------------------------------------------------------------------------
# Integration with Table.set_values()
# ---------------------------------------------------------------------------

class TestQueryLookupInTable:

    @pytest.fixture
    def target_schema(self, cursor):
        cursor.execute("""
            CREATE TABLE benders (
                bender_id   TEXT PRIMARY KEY,
                name        TEXT,
                nation      TEXT,
                capital     TEXT,
                bender_type TEXT
            )
        """)
        cursor.connection.commit()

    def test_single_field_lookup(self, cursor, nations_schema, target_schema):
        table = Table('benders', {
            'bender_id': {'field': 'id', 'primary_key': True},
            'name': {'field': 'name'},
            'nation': {'field': 'nation_name'},
            'capital': {
                'field': 'nation_name',
                'fn': QueryLookup(
                    query='SELECT capital FROM nations WHERE name = :nation_name'
                )
            },
        }, cursor=cursor)

        table.set_values({'id': 'B001', 'name': 'Aang', 'nation_name': 'Air Nomads'})
        assert table.values['capital'] == 'Eastern Air Temple'

    def test_full_row_field_star(self, cursor, people_schema, nations_schema, target_schema):
        table = Table('benders', {
            'bender_id': {'field': 'person_id', 'primary_key': True},
            'name': {'field': 'first_name'},
            'bender_type': {
                'field': '*',
                'fn': QueryLookup(
                    query="""
                        SELECT n.bender_type
                        FROM people p
                        JOIN nations n ON n.nation_id = p.nation_id
                        WHERE p.first_name = :first_name AND p.last_name = :last_name
                    """,
                    return_col='bender_type'
                )
            },
        }, cursor=cursor)

        table.set_values({'person_id': 'B002', 'first_name': 'Zuko', 'last_name': 'Ozai'})
        assert table.values['bender_type'] == 'firebending'

    def test_lookup_in_pipeline(self, cursor, nations_schema, target_schema):
        # QueryLookup result feeds into a lambda
        table = Table('benders', {
            'bender_id': {'field': 'id', 'primary_key': True},
            'name': {'field': 'name'},
            'bender_type': {
                'field': 'nation_name',
                'fn': [
                    QueryLookup(
                        query='SELECT bender_type FROM nations WHERE name = :nation_name'
                    ),
                    str.upper,
                ]
            },
        }, cursor=cursor)

        table.set_values({'id': 'B003', 'name': 'Katara', 'nation_name': 'Water Tribes'})
        assert table.values['bender_type'] == 'WATERBENDING'

    def test_missing_row_in_table(self, cursor, nations_schema, target_schema):
        table = Table('benders', {
            'bender_id': {'field': 'id', 'primary_key': True},
            'name': {'field': 'name'},
            'capital': {
                'field': 'nation_name',
                'fn': QueryLookup(
                    query='SELECT capital FROM nations WHERE name = :nation_name',
                    missing='Unknown Capital'
                )
            },
        }, cursor=cursor)

        table.set_values({'id': 'B004', 'name': 'Wan', 'nation_name': 'Spirit World'})
        assert table.values['capital'] == 'Unknown Capital'

    def test_duck_type_bind_detection(self, cursor, nations_schema, target_schema):
        """Any object with a bind(cursor) method is treated as a deferred transform."""

        class CustomLookup:
            def bind(self, cursor):
                def fn(value):
                    return value.upper() if value else None
                return fn

        table = Table('benders', {
            'bender_id': {'field': 'id', 'primary_key': True},
            'name': {'field': 'name'},
            'nation': {'field': 'nat', 'fn': CustomLookup()},
        }, cursor=cursor)

        table.set_values({'id': 'B005', 'name': 'Wan', 'nat': 'fire nation'})
        assert table.values['nation'] == 'FIRE NATION'
