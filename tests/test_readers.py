# tests/test_readers.py
"""
Tests for dbtk readers module.
Testing the four nations of data formats with the precision of a master bender.
"""

import pytest
from pathlib import Path
from datetime import date, datetime

from dbtk.readers import (
    CSVReader, XLSReader, XLSXReader, JSONReader, NDJSONReader,
    XMLReader, FixedReader, FixedColumn, Clean, get_reader
)
from dbtk.record import Record


# Fixtures
@pytest.fixture
def fixtures_dir():
    """Path to test fixtures directory."""
    return Path(__file__).parent / 'fixtures' / 'readers'


@pytest.fixture
def csv_file(fixtures_dir):
    """Path to CSV test file."""
    return fixtures_dir / 'sample_data.csv'


@pytest.fixture
def excel_file(fixtures_dir):
    """Path to Excel test file."""
    return fixtures_dir / 'sample_data.xlsx'


@pytest.fixture
def json_file(fixtures_dir):
    """Path to JSON test file."""
    return fixtures_dir / 'sample_data.json'


@pytest.fixture
def ndjson_file(fixtures_dir):
    """Path to NDJSON test file."""
    return fixtures_dir / 'sample_data.ndjson'


@pytest.fixture
def xml_file(fixtures_dir):
    """Path to XML test file."""
    return fixtures_dir / 'sample_data.xml'


@pytest.fixture
def fixed_file(fixtures_dir):
    """Path to fixed-width test file."""
    return fixtures_dir / 'sample_data.txt'


@pytest.fixture
def fixed_columns():
    """Column configuration for fixed-width file."""
    return [
        FixedColumn('trainee_id', 1, 5, 'int'),
        FixedColumn('monk_name', 6, 35, 'text'),
        FixedColumn('home_temple', 36, 65, 'text'),
        FixedColumn('mastery_rank', 66, 70, 'int'),
        FixedColumn('bison_companion', 71, 82, 'text'),
        FixedColumn('daily_meditation', 83, 90, 'float'),
        FixedColumn('birth_date', 91, 102, 'date'),
        FixedColumn('last_training', 103, 122, 'datetime')
    ]


@pytest.fixture
def expected_columns():
    """Expected column names from all readers."""
    return ['trainee_id', 'monk_name', 'home_temple', 'mastery_rank',
            'bison_companion', 'daily_meditation', 'birth_date', 'last_training']


@pytest.fixture
def expected_first_record():
    """Expected first record data (trainee_id=1)."""
    return {
        'trainee_id': 1,
        'monk_name': 'Master Aang',
        'home_temple': 'Northern Air Temple',
        'mastery_rank': 10,
        'bison_companion': 'Lefty',
        'daily_meditation': 9.93,
        'birth_date': date(1965, 12, 16),
        'last_training': datetime(2024, 12, 25, 15, 51, 42)
    }


@pytest.fixture
def expected_last_record():
    """Expected last record data (trainee_id=100)."""
    return {
        'trainee_id': 100,
        'monk_name': 'Master Opal',
        'home_temple': 'Western Air Temple',
        'mastery_rank': 10,
        'bison_companion': 'Gale',
        'daily_meditation': 9.99,
        'birth_date': date(1964, 12, 19),
        'last_training': datetime(2023, 1, 24, 22, 51, 57)
    }


# Helper function to create reader based on type
def get_test_reader(reader_type, csv_file, excel_file, json_file, ndjson_file,
                    xml_file, fixed_file, fixed_columns, **kwargs):
    """Factory function to create appropriate reader for testing."""
    if reader_type == 'csv':
        return CSVReader(open(csv_file, encoding='utf-8'), **kwargs)
    elif reader_type == 'excel':
        from dbtk.readers.excel import open_workbook, get_sheet_by_index
        wb = open_workbook(str(excel_file))
        ws = get_sheet_by_index(wb, 0)
        if ws.__class__.__name__ == 'Worksheet':
            return XLSXReader(ws, **kwargs)
        else:
            return XLSReader(ws, **kwargs)
    elif reader_type == 'json':
        return JSONReader(open(json_file, encoding='utf-8'), **kwargs)
    elif reader_type == 'ndjson':
        return NDJSONReader(open(ndjson_file, encoding='utf-8'), **kwargs)
    elif reader_type == 'xml':
        return XMLReader(open(xml_file, 'rb'), record_xpath='//record', **kwargs)
    elif reader_type == 'fixed':
        return FixedReader(open(fixed_file, encoding='utf-8'), fixed_columns, **kwargs)
    else:
        raise ValueError(f"Unknown reader type: {reader_type}")


# Base Reader Tests
class TestReaderBase:
    """Tests for base reader functionality across all formats."""

    @pytest.mark.parametrize("reader_type", ['csv', 'excel', 'json', 'ndjson', 'xml', 'fixed'])
    def test_basic_iteration(self, reader_type, csv_file, excel_file, json_file,
                             ndjson_file, xml_file, fixed_file, fixed_columns):
        """Test basic iteration returns correct number of records."""
        with get_test_reader(reader_type, csv_file, excel_file, json_file,
                             ndjson_file, xml_file, fixed_file, fixed_columns) as reader:
            records = list(reader)
            assert len(records) == 100, f"{reader_type} should return 100 records"

    @pytest.mark.parametrize("reader_type", ['csv', 'excel', 'json', 'ndjson', 'xml', 'fixed'])
    def test_first_record(self, reader_type, csv_file, excel_file, json_file,
                          ndjson_file, xml_file, fixed_file, fixed_columns):
        """Test first record has correct values."""
        with get_test_reader(reader_type, csv_file, excel_file, json_file,
                             ndjson_file, xml_file, fixed_file, fixed_columns) as reader:
            first = next(reader)

            # Check trainee_id (will be string for some formats, int for others)
            if isinstance(first['trainee_id'], str):
                assert first['trainee_id'] == '1', f"{reader_type} first trainee_id should be '1'"
            else:
                assert first['trainee_id'] == 1, f"{reader_type} first trainee_id should be 1"

            # Check name
            assert 'Aang' in first['monk_name'], f"{reader_type} first monk should be Aang"

    @pytest.mark.parametrize("reader_type", ['csv', 'excel', 'json', 'ndjson', 'xml', 'fixed'])
    def test_last_record(self, reader_type, csv_file, excel_file, json_file,
                         ndjson_file, xml_file, fixed_file, fixed_columns):
        """Test last record has correct values."""
        with get_test_reader(reader_type, csv_file, excel_file, json_file,
                             ndjson_file, xml_file, fixed_file, fixed_columns) as reader:
            records = list(reader)
            last = records[-1]

            # Check trainee_id
            if isinstance(last['trainee_id'], str):
                assert last['trainee_id'] == '100', f"{reader_type} last trainee_id should be '100'"
            else:
                assert last['trainee_id'] == 100, f"{reader_type} last trainee_id should be 100"

            # Check name
            assert 'Opal' in last['monk_name'], f"{reader_type} last monk should be Opal"

    @pytest.mark.parametrize("reader_type", ['csv', 'excel', 'json', 'ndjson', 'xml', 'fixed'])
    def test_headers(self, reader_type, csv_file, excel_file, json_file,
                     ndjson_file, xml_file, fixed_file, fixed_columns, expected_columns):
        """Test that headers are correctly read."""
        with get_test_reader(reader_type, csv_file, excel_file, json_file,
                             ndjson_file, xml_file, fixed_file, fixed_columns) as reader:
            headers = reader.headers
            assert len(headers) == 9, f"{reader_type} should have 9 columns (8 data + _row_num)"

            # XML and JSON sort headers alphabetically, others preserve order
            if reader_type in ('xml', 'json', 'ndjson'):
                # Check all expected columns are present
                assert set(headers[:8]) == set(expected_columns), \
                    f"{reader_type} should have all expected columns"
            else:
                # CSV, Excel, Fixed maintain original order
                assert headers[:8] == expected_columns, f"{reader_type} column names don't match"

            assert headers[-1] == '_row_num', f"{reader_type} should add _row_num at the end"

    @pytest.mark.parametrize("reader_type", ['csv', 'excel', 'json', 'ndjson', 'xml', 'fixed'])
    def test_skip_rows(self, reader_type, csv_file, excel_file, json_file,
                       ndjson_file, xml_file, fixed_file, fixed_columns):
        """Test skip_rows skips the correct number of rows."""
        with get_test_reader(reader_type, csv_file, excel_file, json_file,
                             ndjson_file, xml_file, fixed_file, fixed_columns,
                             skip_rows=10) as reader:
            records = list(reader)
            assert len(records) == 90, f"{reader_type} should return 90 records after skipping 10"

            first = records[0]
            # First record should be trainee_id=11 after skipping 10
            if isinstance(first['trainee_id'], str):
                assert first['trainee_id'] == '11', f"{reader_type} first record should be 11"
            else:
                assert first['trainee_id'] == 11, f"{reader_type} first record should be 11"

    @pytest.mark.parametrize("reader_type", ['csv', 'excel', 'json', 'ndjson', 'xml', 'fixed'])
    def test_n_rows(self, reader_type, csv_file, excel_file, json_file,
                    ndjson_file, xml_file, fixed_file, fixed_columns):
        """Test n_rows limits the number of rows returned."""
        with get_test_reader(reader_type, csv_file, excel_file, json_file,
                             ndjson_file, xml_file, fixed_file, fixed_columns,
                             n_rows=25) as reader:
            records = list(reader)
            assert len(records) == 25, f"{reader_type} should return exactly 25 records"

    @pytest.mark.parametrize("reader_type", ['csv', 'excel', 'json', 'ndjson', 'xml', 'fixed'])
    def test_skip_and_n_rows_combined(self, reader_type, csv_file, excel_file, json_file,
                                      ndjson_file, xml_file, fixed_file, fixed_columns):
        """Test skip_rows and n_rows work together."""
        with get_test_reader(reader_type, csv_file, excel_file, json_file,
                             ndjson_file, xml_file, fixed_file, fixed_columns,
                             skip_rows=50, n_rows=10) as reader:
            records = list(reader)
            assert len(records) == 10, f"{reader_type} should return 10 records"

            first = records[0]
            # First record should be trainee_id=51
            if isinstance(first['trainee_id'], str):
                assert first['trainee_id'] == '51', f"{reader_type} first record should be 51"
            else:
                assert first['trainee_id'] == 51, f"{reader_type} first record should be 51"

    @pytest.mark.parametrize("reader_type", ['csv', 'excel', 'json', 'ndjson', 'xml', 'fixed'])
    def test_add_row_num_true(self, reader_type, csv_file, excel_file, json_file,
                              ndjson_file, xml_file, fixed_file, fixed_columns):
        """Test add_row_num=True adds _row_num field."""
        with get_test_reader(reader_type, csv_file, excel_file, json_file,
                             ndjson_file, xml_file, fixed_file, fixed_columns,
                             add_row_num=True) as reader:
            records = list(reader)
            first = records[0]
            assert '_row_num' in first, f"{reader_type} should have _row_num field"
            assert first['_row_num'] == 1, f"{reader_type} first _row_num should be 1"

            last = records[-1]
            assert last['_row_num'] == 100, f"{reader_type} last _row_num should be 99"

    @pytest.mark.parametrize("reader_type", ['csv', 'excel', 'json', 'ndjson', 'xml', 'fixed'])
    def test_add_row_num_false(self, reader_type, csv_file, excel_file, json_file,
                               ndjson_file, xml_file, fixed_file, fixed_columns):
        """Test add_row_num=False excludes _row_num field."""
        with get_test_reader(reader_type, csv_file, excel_file, json_file,
                             ndjson_file, xml_file, fixed_file, fixed_columns,
                             add_row_num=False) as reader:
            headers = reader.headers
            assert '_row_num' not in headers, f"{reader_type} should not have _row_num in headers"
            assert len(headers) == 8, f"{reader_type} should have 8 columns without _row_num"

    @pytest.mark.parametrize("reader_type", ['csv', 'excel', 'json', 'ndjson', 'xml', 'fixed'])
    def test_returns_records(self, reader_type, csv_file, excel_file, json_file,
                             ndjson_file, xml_file, fixed_file, fixed_columns):
        """Test that all readers return Record objects by default."""
        with get_test_reader(reader_type, csv_file, excel_file, json_file,
                             ndjson_file, xml_file, fixed_file, fixed_columns) as reader:
            first = next(reader)
            assert isinstance(first, Record), f"{reader_type} should return Record objects"

            # Test Record access methods
            assert first['trainee_id'] is not None, "Should support dict-style access"
            assert first.monk_name is not None, "Should support attribute access"

    @pytest.mark.parametrize("reader_type", ['csv', 'excel', 'json', 'ndjson', 'xml', 'fixed'])
    def test_clean_headers_noop(self, reader_type, csv_file, excel_file, json_file,
                                ndjson_file, xml_file, fixed_file, fixed_columns):
        """Test Clean.NOOP leaves headers unchanged."""
        with get_test_reader(reader_type, csv_file, excel_file, json_file,
                             ndjson_file, xml_file, fixed_file, fixed_columns,
                             clean_headers=Clean.NOOP) as reader:
            headers = reader.headers
            # Original headers should be preserved (except _row_num)
            assert 'trainee_id' in headers or 'Trainee_Id' in headers or 'trainee_ID' in headers

    @pytest.mark.parametrize("reader_type", ['csv', 'excel', 'json', 'ndjson', 'xml', 'fixed'])
    def test_clean_headers_lower(self, reader_type, csv_file, excel_file, json_file,
                                 ndjson_file, xml_file, fixed_file, fixed_columns):
        """Test Clean.LOWER converts to lowercase."""
        with get_test_reader(reader_type, csv_file, excel_file, json_file,
                             ndjson_file, xml_file, fixed_file, fixed_columns,
                             clean_headers=Clean.LOWER) as reader:
            headers = reader.headers
            assert all(h == h.lower() for h in headers if h != '_row_num'), \
                f"{reader_type} headers should be lowercase"

    @pytest.mark.parametrize("reader_type", ['csv', 'excel', 'json', 'ndjson', 'xml', 'fixed'])
    def test_clean_headers_lower_nospace(self, reader_type, csv_file, excel_file, json_file,
                                         ndjson_file, xml_file, fixed_file, fixed_columns):
        """Test Clean.LOWER_NOSPACE converts to lowercase and replaces spaces."""
        with get_test_reader(reader_type, csv_file, excel_file, json_file,
                             ndjson_file, xml_file, fixed_file, fixed_columns,
                             clean_headers=Clean.LOWER_NOSPACE) as reader:
            headers = reader.headers
            assert all(h == h.lower() for h in headers if h != '_row_num'), \
                f"{reader_type} headers should be lowercase"
            assert all(' ' not in h for h in headers), \
                f"{reader_type} headers should not contain spaces"

    @pytest.mark.parametrize("reader_type", ['csv', 'excel', 'json', 'ndjson', 'xml', 'fixed'])
    def test_context_manager(self, reader_type, csv_file, excel_file, json_file,
                             ndjson_file, xml_file, fixed_file, fixed_columns):
        """Test context manager properly cleans up resources."""
        reader = get_test_reader(reader_type, csv_file, excel_file, json_file,
                                 ndjson_file, xml_file, fixed_file, fixed_columns)

        with reader:
            _ = next(reader)

        # After context exit, should not be able to iterate
        # (This is implementation-dependent, but good practice)
        # Just verify it doesn't crash

    @pytest.mark.parametrize("reader_type", ['csv', 'json', 'ndjson', 'xml', 'fixed'])
    def test_empty_file_handling(self, reader_type, tmp_path, fixed_columns):
        """Test handling of empty files."""
        empty_file = tmp_path / f"empty.{reader_type}"

        if reader_type == 'csv':
            # CSV with just headers - valid, returns 0 records
            empty_file.write_text("trainee_id,monk_name\n")
            with CSVReader(open(empty_file, encoding='utf-8')) as reader:
                records = list(reader)
                assert len(records) == 0

        elif reader_type == 'json':
            # Empty JSON array - should raise error
            empty_file.write_text("[]")
            with pytest.raises(ValueError, match="empty"):
                JSONReader(open(empty_file, encoding='utf-8'))

        elif reader_type == 'ndjson':
            # Empty NDJSON file - valid, returns 0 records
            empty_file.write_text("")
            with pytest.raises(ValueError, match="No keys"):
                reader = NDJSONReader(open(empty_file, encoding='utf-8'))
                # NDJSONReader is lazy and doesn't raise error until we try to evaluate schema
                _ = list(reader)

        elif reader_type == 'xml':
            # XML with no records - should raise error
            empty_file.write_text("<root></root>")
            with pytest.raises(ValueError, match="No records"):
                XMLReader(open(empty_file, 'rb'), record_xpath='//trainee')

        elif reader_type == 'fixed':
            # Empty fixed-width file - valid, returns 0 records
            empty_file.write_text("")
            with FixedReader(open(empty_file, encoding='utf-8'), fixed_columns) as reader:
                records = list(reader)
                assert len(records) == 0


class TestCSVReader:
    """Tests specific to CSV reader."""

    def test_custom_delimiter_tab(self, tmp_path):
        """Test reading tab-delimited files."""
        tsv_file = tmp_path / "test.tsv"
        tsv_file.write_text("trainee_id\tmonk_name\ttemple\n1\tAang\tSouthern\n2\tKatara\tNorthern\n")

        with CSVReader(open(tsv_file, encoding='utf-8'), delimiter='\t') as reader:
            records = list(reader)
            assert len(records) == 2
            assert records[0]['monk_name'] == 'Aang'

    def test_custom_delimiter_pipe(self, tmp_path):
        """Test reading pipe-delimited files."""
        pipe_file = tmp_path / "test.csv"
        pipe_file.write_text("trainee_id|monk_name|temple\n1|Aang|Southern\n2|Katara|Northern\n")

        with CSVReader(open(pipe_file, encoding='utf-8'), delimiter='|') as reader:
            records = list(reader)
            assert len(records) == 2
            assert records[0]['monk_name'] == 'Aang'

    def test_provided_headers(self, tmp_path):
        """Test providing custom headers instead of reading from file."""
        csv_file = tmp_path / "no_headers.csv"
        csv_file.write_text("1,Aang,Southern\n2,Katara,Northern\n")

        custom_headers = ['id', 'name', 'origin']
        with CSVReader(open(csv_file, encoding='utf-8'),
                       headers=custom_headers) as reader:
            assert reader.headers[:3] == custom_headers
            records = list(reader)
            assert len(records) == 2
            assert records[0]['name'] == 'Aang'

    def test_custom_quotechar(self, tmp_path):
        """Test custom quote character."""
        csv_file = tmp_path / "quoted.csv"
        csv_file.write_text("trainee_id,monk_name\n1,'Master Aang, the Avatar'\n")

        with CSVReader(open(csv_file, encoding='utf-8'), quotechar="'") as reader:
            records = list(reader)
            assert 'Avatar' in records[0]['monk_name']

    def test_csv_with_empty_values(self, csv_file):
        """Test CSV handles empty values correctly."""
        with CSVReader(open(csv_file, encoding='utf-8')) as reader:
            records = list(reader)
            # Find a record with empty bison_companion
            empty_bison = [r for r in records if not r['bison_companion']]
            assert len(empty_bison) > 0, "Should have records with empty bison_companion"

    def test_fieldnames_property(self, csv_file):
        """Test fieldnames property (alias for headers)."""
        with CSVReader(open(csv_file, encoding='utf-8')) as reader:
            assert reader.fieldnames == reader.headers
            assert 'trainee_id' in reader.fieldnames


class TestGetReader:
    """Tests for the get_reader utility function."""

    def test_get_reader_csv(self, csv_file):
        """Test get_reader with CSV file."""
        with get_reader(str(csv_file)) as reader:
            assert isinstance(reader, CSVReader)
            records = list(reader)
            assert len(records) == 100

    def test_get_reader_excel(self, excel_file):
        """Test get_reader with Excel file."""
        with get_reader(str(excel_file)) as reader:
            assert isinstance(reader, (XLSReader, XLSXReader))
            records = list(reader)
            assert len(records) == 100

    def test_get_reader_json(self, json_file):
        """Test get_reader with JSON file."""
        with get_reader(str(json_file)) as reader:
            assert isinstance(reader, JSONReader)
            records = list(reader)
            assert len(records) == 100

    def test_get_reader_xml(self, xml_file):
        """Test get_reader with XML file."""
        with get_reader(str(xml_file)) as reader:
            assert isinstance(reader, XMLReader)
            records = list(reader)
            assert len(records) == 100

    def test_get_reader_fixed_width(self, fixed_file, fixed_columns):
        """Test get_reader with fixed-width file."""
        with get_reader(str(fixed_file), fixed_config=fixed_columns) as reader:
            assert isinstance(reader, FixedReader)
            records = list(reader)
            assert len(records) == 100

    def test_get_reader_unsupported_extension(self, tmp_path):
        """Test get_reader with unsupported extension."""
        unknown_file = tmp_path / "data.unknown"
        unknown_file.write_text("some data")

        with pytest.raises(ValueError, match="Unsupported file extension"):
            get_reader(str(unknown_file))


class TestReaderFiltering:
    """Tests for Reader.filter() method."""

    @pytest.fixture
    def csv_data(self, tmp_path):
        """Create a CSV file with test data."""
        csv_file = tmp_path / "test.csv"
        csv_file.write_text("""name,age,country
Alice,25,US
Bob,17,US
Charlie,30,UK
Diana,22,US
Eve,16,UK
Frank,35,US""")
        return csv_file

    def test_single_filter(self, csv_data):
        """Test filtering with a single condition."""
        with CSVReader(open(csv_data)) as reader:
            reader.filter(lambda r: int(r.age) >= 18)
            results = list(reader)
            assert len(results) == 4
            assert all(int(r.age) >= 18 for r in results)
            names = [r.name for r in results]
            assert names == ['Alice', 'Charlie', 'Diana', 'Frank']

    def test_multiple_filters_pipeline(self, csv_data):
        """Test multiple filters accumulating in a pipeline."""
        with CSVReader(open(csv_data)) as reader:
            reader.filter(lambda r: int(r.age) >= 18)
            reader.filter(lambda r: r.country == 'US')
            results = list(reader)
            assert len(results) == 3
            names = [r.name for r in results]
            assert names == ['Alice', 'Diana', 'Frank']

    def test_filter_with_n_rows(self, csv_data):
        """Test that n_rows applies after filtering."""
        with CSVReader(open(csv_data)) as reader:
            reader.filter(lambda r: int(r.age) >= 18)
            reader.n_rows = 2
            results = list(reader)
            assert len(results) == 2
            names = [r.name for r in results]
            assert names == ['Alice', 'Charlie']

    def test_filter_no_matches(self, csv_data):
        """Test filter that matches no records."""
        with CSVReader(open(csv_data)) as reader:
            reader.filter(lambda r: int(r.age) > 100)
            results = list(reader)
            assert len(results) == 0

    def test_filter_all_match(self, csv_data):
        """Test filter that matches all records."""
        with CSVReader(open(csv_data)) as reader:
            reader.filter(lambda r: len(r.name) > 0)
            results = list(reader)
            assert len(results) == 6

    def test_filter_with_skip_rows(self, csv_data):
        """Test filter with skip_rows (should work but log warning)."""
        with CSVReader(open(csv_data), skip_rows=1) as reader:
            reader.filter(lambda r: int(r.age) >= 18)
            results = list(reader)
            # Skipped Bob (row 1 after header), so should get Charlie, Diana, Frank
            assert len(results) == 3
            names = [r.name for r in results]
            assert names == ['Charlie', 'Diana', 'Frank']

    def test_filter_returns_self(self, csv_data):
        """Test that filter() returns self for chaining."""
        with CSVReader(open(csv_data)) as reader:
            result = reader.filter(lambda r: True)
            assert result is reader

    def test_filter_row_num_field(self, csv_data):
        """Test that _row_num field is sequential for filtered records."""
        with CSVReader(open(csv_data)) as reader:
            reader.filter(lambda r: int(r.age) >= 18)
            results = list(reader)
            row_nums = [r._row_num for r in results]
            assert row_nums == [1, 2, 3, 4]  # Sequential, not file row numbers

    def test_filter_invalid_callable(self, csv_data):
        """Test that filter() rejects non-callable."""
        with CSVReader(open(csv_data)) as reader:
            with pytest.raises(TypeError, match="filter\\(\\) requires a callable"):
                reader.filter("not a callable")

    def test_filter_complex_function(self, csv_data):
        """Test filter with complex multi-condition function."""
        def is_young_american(record):
            return record.country == 'US' and int(record.age) < 25

        with CSVReader(open(csv_data)) as reader:
            reader.filter(is_young_american)
            results = list(reader)
            assert len(results) == 2
            names = [r.name for r in results]
            # Bob (17) and Diana (22) are both US and < 25
            assert names == ['Bob', 'Diana']

    def test_rows_read_tracking(self, csv_data):
        """Test that _rows_read tracks all rows, not just filtered."""
        with CSVReader(open(csv_data)) as reader:
            reader.filter(lambda r: int(r.age) >= 18)
            results = list(reader)
            # Should have read 6 rows, returned 4
            assert reader._rows_read == 6
            assert reader._row_num == 4
            assert len(results) == 4