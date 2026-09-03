# tests/test_database.py
"""
Tests for dbtk.database - connection string building and driver plumbing.
"""

import pytest

from dbtk.database import (
    _get_connection_string,
    _get_odbc_string,
    _quote_libpq_value,
    _quote_odbc_value,
    postgres,
)


class TestQuoteLibpqValue:
    """libpq connection strings need quoting for values with spaces, quotes,
    or backslashes - _get_connection_string() used to skip this entirely,
    breaking any password containing one of those (spaces are common)."""

    def test_plain_value_unquoted(self):
        assert _quote_libpq_value('localhost') == 'localhost'

    def test_value_with_space_is_quoted(self):
        assert _quote_libpq_value('p@ss word') == "'p@ss word'"

    def test_internal_quote_is_escaped(self):
        assert _quote_libpq_value("o'brien") == r"'o\'brien'"

    def test_internal_backslash_is_escaped(self):
        assert _quote_libpq_value('a\\b') == "'a\\\\b'"

    def test_empty_value_is_quoted(self):
        assert _quote_libpq_value('') == "''"

    def test_non_string_value_passes_through(self):
        assert _quote_libpq_value(5432) == '5432'


class TestGetConnectionString:
    def test_no_special_characters(self):
        s = _get_connection_string(host='localhost', dbname='mydb', user='postgres')
        assert s == 'host=localhost dbname=mydb user=postgres'

    def test_password_with_space_is_quoted_not_broken(self):
        s = _get_connection_string(host='localhost', dbname='mydb', user='postgres', password='p@ss word')
        # Quoted as one password value, not split into a bogus 5th "word"
        # pair by the space inside it.
        assert s == "host=localhost dbname=mydb user=postgres password='p@ss word'"


class TestQuoteOdbcValue:
    """ODBC connection strings use ';' as the pair separator and quote with
    braces, not backslash-escaping - a different (and previously also
    entirely missing) quoting rule than libpq's."""

    def test_plain_value_unquoted(self):
        assert _quote_odbc_value('myhost') == 'myhost'

    def test_value_with_semicolon_is_braced(self):
        assert _quote_odbc_value('p;ss=word') == '{p;ss=word}'

    def test_internal_brace_is_doubled(self):
        assert _quote_odbc_value('a}b') == '{a}}b}'

    def test_leading_trailing_whitespace_is_braced(self):
        assert _quote_odbc_value(' pwd ') == '{ pwd }'

    def test_empty_value_is_braced(self):
        assert _quote_odbc_value('') == '{}'


class TestGetOdbcString:
    def test_no_special_characters(self):
        s = _get_odbc_string(server='myhost', database='mydb', uid='sa', pwd='plainpass')
        assert s == 'SERVER=myhost;DATABASE=mydb;UID=sa;PWD=plainpass'

    def test_password_with_semicolon_is_braced_not_broken(self):
        s = _get_odbc_string(server='myhost', database='mydb', uid='sa', pwd='p;ss=word')
        # Braced as one PWD value, not split into a bogus 5th "ss=word" pair
        # by the semicolon inside the password.
        assert s == 'SERVER=myhost;DATABASE=mydb;UID=sa;PWD={p;ss=word}'

    def test_driver_name_still_wrapped_in_braces(self):
        s = _get_odbc_string(server='h', database='d', uid='u', pwd='x',
                              odbc_driver_name='ODBC Driver 18 for SQL Server')
        assert s.startswith('DRIVER={ODBC Driver 18 for SQL Server};')

    def test_dsn_style_password_is_quoted(self):
        s = _get_odbc_string(dsn='mydsn', pwd='p;ss')
        assert s == 'DSN=mydsn;PWD={p;ss}'


@pytest.fixture
def postgres_db():
    try:
        db = postgres(user='postgres', password='postgres', database='dbtk_test',
                       host='localhost', driver='psycopg2')
    except Exception as e:
        pytest.skip(f"No live postgres reachable for this test: {e}")
    yield db
    db.close()


class TestLivePostgresConnectionQuoting:
    """End-to-end proof the quoting fix actually lets psycopg2 connect, not
    just that the string looks right - requires a live postgres reachable at
    localhost with user=postgres; skips cleanly otherwise."""

    def test_connects_with_password_containing_a_space(self, postgres_db):
        cur = postgres_db.cursor()
        cur.execute("ALTER USER postgres PASSWORD 'p@ss word'")
        postgres_db.commit()
        try:
            db2 = postgres(user='postgres', password='p@ss word', database='dbtk_test',
                            host='localhost', driver='psycopg2')
            cur2 = db2.cursor()
            cur2.execute('SELECT 1')
            assert cur2.fetchone()[0] == 1
            db2.close()
        finally:
            cur.execute("ALTER USER postgres PASSWORD 'postgres'")
            postgres_db.commit()
