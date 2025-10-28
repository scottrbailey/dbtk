# tests/test_writers.py
"""
Tests for dbtk writers module.
Channeling data out to various formats like a master waterbender directing flow.
"""

import pytest
import json
from pathlib import Path
from datetime import date, datetime
from collections import namedtuple, OrderedDict

from dbtk.readers import CSVReader
from dbtk.writers import (
    CSVWriter, ExcelWriter, FixedWidthWriter,
    JSONWriter, NDJSONWriter, XMLWriter,
    to_csv, to_excel, to_fixed_width, to_json, to_ndjson, to_xml
)


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
def sample_records(excel_file):
    """Load first 10 records from sample_data.xlsx as Records with types preserved."""
    from dbtk.readers.excel import open_workbook, get_sheet_by_index
    from dbtk.readers import XLSXReader, XLReader

    wb = open_workbook(str(excel_file))
    ws = get_sheet_by_index(wb, 0)

    reader_class = XLSXReader if ws.__class__.__name__ == 'Worksheet' else XLReader

    with reader_class(ws, add_rownum=False) as reader:
        records = []
        for i, record in enumerate(reader):
            if i >= 10:
                break
            records.append(record)
    return records


@pytest.fixture
def sample_dicts(sample_records):
    """Convert sample records to list of dicts."""
    return [record.to_dict() for record in sample_records]


@pytest.fixture
def sample_namedtuples(sample_records):
    """Convert sample records to list of namedtuples."""
    if not sample_records:
        return []

    # Create namedtuple type from first record's keys
    RecordTuple = namedtuple('RecordTuple', sample_records[0].keys())
    return [RecordTuple(**record.to_dict()) for record in sample_records]


@pytest.fixture
def sample_lists(sample_records):
    """Convert sample records to list of lists."""
    return [list(record.values()) for record in sample_records]


@pytest.fixture
def sample_columns(sample_records):
    """Get column names from sample records."""
    if sample_records:
        return list(sample_records[0].keys())
    return []


# Base Writer Tests using CSV
class TestBaseWriter:
    """Tests for base writer functionality using CSVWriter."""

    def test_write_from_records(self, tmp_path, sample_records):
        """Test writing from list of Record objects."""
        output_file = tmp_path / "output.csv"

        to_csv(sample_records, output_file)

        # Read back and verify
        with CSVReader(open(output_file, encoding='utf-8')) as reader:
            records = list(reader)
            assert len(records) == 10
            assert records[0]['trainee_id'] == '1'

    def test_write_from_dicts(self, tmp_path, sample_dicts):
        """Test writing from list of dictionaries."""
        output_file = tmp_path / "output.csv"

        to_csv(sample_dicts, output_file)

        # Read back and verify
        with CSVReader(open(output_file, encoding='utf-8')) as reader:
            records = list(reader)
            assert len(records) == 10
            assert 'trainee_id' in records[0]

    def test_write_from_namedtuples(self, tmp_path, sample_namedtuples):
        """Test writing from list of namedtuples."""
        output_file = tmp_path / "output.csv"

        to_csv(sample_namedtuples, output_file)

        # Read back and verify
        with CSVReader(open(output_file, encoding='utf-8')) as reader:
            records = list(reader)
            assert len(records) == 10

    def test_write_from_lists(self, tmp_path, sample_lists, sample_columns):
        """Test writing from list of lists with explicit columns."""
        output_file = tmp_path / "output.csv"

        writer = CSVWriter(sample_lists, output_file, columns=sample_columns)
        writer.write()

        # Read back and verify
        with CSVReader(open(output_file, encoding='utf-8'), add_rownum=False) as reader:
            records = list(reader)
            assert len(records) == 10
            assert reader.headers == sample_columns

    def test_include_headers_true(self, tmp_path, sample_records):
        """Test include_headers=True writes header row."""
        output_file = tmp_path / "output.csv"

        to_csv(sample_records, output_file, include_headers=True)

        with open(output_file, encoding='utf-8') as f:
            first_line = f.readline().strip()
            assert 'trainee_id' in first_line

    def test_include_headers_false(self, tmp_path, sample_records):
        """Test include_headers=False omits header row."""
        output_file = tmp_path / "output.csv"

        to_csv(sample_records, output_file, include_headers=False)

        with open(output_file, encoding='utf-8') as f:
            first_line = f.readline().strip()
            # First line should be data, not headers
            assert first_line.startswith('1,') or first_line.startswith('"1"')

    def test_to_string_date(self, tmp_path, sample_records):
        """Test BaseWriter.to_string() handles dates correctly."""
        output_file = tmp_path / "output.csv"
        writer = CSVWriter(sample_records, output_file)

        test_date = date(2024, 12, 25)
        result = writer.to_string(test_date)
        assert result == '2024-12-25'

    def test_to_string_datetime_no_microseconds(self, tmp_path, sample_records):
        """Test BaseWriter.to_string() handles datetime without microseconds."""
        output_file = tmp_path / "output.csv"
        writer = CSVWriter(sample_records, output_file)

        test_datetime = datetime(2024, 12, 25, 15, 30, 45)
        result = writer.to_string(test_datetime)
        assert result == '2024-12-25 15:30:45'

    def test_to_string_datetime_with_microseconds(self, tmp_path, sample_records):
        """Test BaseWriter.to_string() handles datetime with microseconds."""
        output_file = tmp_path / "output.csv"
        writer = CSVWriter(sample_records, output_file)

        test_datetime = datetime(2024, 12, 25, 15, 30, 45, 123456)
        result = writer.to_string(test_datetime)
        assert result == '2024-12-25 15:30:45.123456'

    def test_to_string_datetime_at_midnight(self, tmp_path, sample_records):
        """Test BaseWriter.to_string() handles datetime at midnight as date."""
        output_file = tmp_path / "output.csv"
        writer = CSVWriter(sample_records, output_file)

        test_datetime = datetime(2024, 12, 25, 0, 0, 0)
        result = writer.to_string(test_datetime)
        assert result == '2024-12-25'

    def test_to_string_none(self, tmp_path, sample_records):
        """Test BaseWriter.to_string() handles None."""
        output_file = tmp_path / "output.csv"
        writer = CSVWriter(sample_records, output_file)

        result = writer.to_string(None)
        assert result == ''

    def test_to_string_number(self, tmp_path, sample_records):
        """Test BaseWriter.to_string() handles numbers."""
        output_file = tmp_path / "output.csv"
        writer = CSVWriter(sample_records, output_file)

        assert writer.to_string(42) == '42'
        assert writer.to_string(3.14) == '3.14'

    def test_row_count(self, tmp_path, sample_records):
        """Test that row_count is correctly tracked."""
        output_file = tmp_path / "output.csv"

        writer = CSVWriter(sample_records, output_file)
        count = writer.write()

        assert count == 10
        assert writer.row_count == 10


class TestCSVWriter:
    """Tests specific to CSV writer."""

    def test_custom_delimiter_tab(self, tmp_path, sample_records):
        """Test writing with tab delimiter."""
        output_file = tmp_path / "output.tsv"

        to_csv(sample_records, output_file, delimiter='\t')

        with open(output_file, encoding='utf-8') as f:
            first_line = f.readline()
            assert '\t' in first_line

    def test_custom_delimiter_pipe(self, tmp_path, sample_records):
        """Test writing with pipe delimiter."""
        output_file = tmp_path / "output.csv"

        to_csv(sample_records, output_file, delimiter='|')

        with open(output_file, encoding='utf-8') as f:
            first_line = f.readline()
            assert '|' in first_line

    def test_custom_quotechar(self, tmp_path, sample_records):
        """Test writing with custom quote character."""
        output_file = tmp_path / "output.csv"

        to_csv(sample_records, output_file, quotechar="'")

        # Just verify file was written successfully
        assert output_file.exists()

    def test_round_trip_preserves_data(self, tmp_path, sample_records):
        """Test that writing and reading back preserves data."""
        output_file = tmp_path / "output.csv"

        # Write
        to_csv(sample_records, output_file)

        # Read back
        with CSVReader(open(output_file, encoding='utf-8')) as reader:
            records_back = list(reader)

        # Compare (values will be strings after CSV round-trip)
        assert len(records_back) == len(sample_records)
        assert str(sample_records[0]['trainee_id']) == records_back[0]['trainee_id']
        assert sample_records[0]['monk_name'] == records_back[0]['monk_name']


class TestExcelWriter:
    """Tests specific to Excel writer."""

    def test_write_to_excel(self, tmp_path, sample_records):
        """Test basic Excel writing."""
        output_file = tmp_path / "output.xlsx"

        to_excel(sample_records, output_file)

        assert output_file.exists()

        # Read back with openpyxl
        from openpyxl import load_workbook
        wb = load_workbook(output_file)
        ws = wb.active

        # Check headers
        assert ws.cell(1, 1).value == 'trainee_id'

        # Check first data row
        assert ws.cell(2, 1).value == 1

    def test_custom_sheet_name(self, tmp_path, sample_records):
        """Test writing to custom sheet name."""
        output_file = tmp_path / "output.xlsx"

        to_excel(sample_records, output_file, sheet='Trainees')

        from openpyxl import load_workbook
        wb = load_workbook(output_file)

        assert 'Trainees' in wb.sheetnames

    def test_multiple_sheets(self, tmp_path, sample_records):
        """Test writing to multiple sheets in same workbook."""
        output_file = tmp_path / "output.xlsx"

        # Write first sheet
        to_excel(sample_records[:5], output_file, sheet='First')

        # Write second sheet
        to_excel(sample_records[5:], output_file, sheet='Second')

        from openpyxl import load_workbook
        wb = load_workbook(output_file)

        assert 'First' in wb.sheetnames
        assert 'Second' in wb.sheetnames

    def test_overwrite_sheet_true(self, tmp_path, sample_records):
        """Test overwrite_sheet=True replaces existing sheet."""
        output_file = tmp_path / "output.xlsx"

        # Write initial data
        to_excel(sample_records, output_file, sheet='Data')

        # Overwrite with less data
        to_excel(sample_records[:3], output_file, sheet='Data', overwrite_sheet=True)

        from openpyxl import load_workbook
        wb = load_workbook(output_file)
        ws = wb['Data']

        # Should have 3 data rows + 1 header
        assert ws.max_row == 4

    def test_date_type_preservation(self, tmp_path, sample_records):
        """Test that dates are written as date types in Excel."""
        output_file = tmp_path / "output.xlsx"

        to_excel(sample_records, output_file)

        from openpyxl import load_workbook
        wb = load_workbook(output_file)
        ws = wb.active

        # Find birth_date column (column 7)
        birth_date_cell = ws.cell(2, 7)

        # Should be a datetime object, not string
        assert isinstance(birth_date_cell.value, datetime)


class TestFixedWidthWriter:
    """Tests specific to fixed-width writer."""

    def test_write_fixed_width(self, tmp_path, sample_records):
        """Test basic fixed-width writing."""
        output_file = tmp_path / "output.txt"
        column_widths = [5, 30, 30, 5, 12, 8, 12, 20]

        to_fixed_width(sample_records, column_widths, output_file)

        assert output_file.exists()

        # Read back and check structure
        with open(output_file, encoding='utf-8') as f:
            first_line = f.readline()
            # Line should be sum of column widths
            assert len(first_line.rstrip('\n')) == sum(column_widths)

    def test_right_align_numbers_true(self, tmp_path, sample_records):
        """Test right_align_numbers=True aligns numbers to the right."""
        output_file = tmp_path / "output.txt"
        column_widths = [5, 30, 30, 5, 12, 8, 12, 20]

        to_fixed_width(sample_records, column_widths, output_file, right_align_numbers=True)

        with open(output_file, encoding='utf-8') as f:
            first_line = f.readline()
            # trainee_id (int) should be right-aligned in first 5 chars
            trainee_id = first_line[:5]
            assert trainee_id.strip() == '1'
            # Should have leading spaces
            assert trainee_id.startswith(' ')

    def test_right_align_numbers_false(self, tmp_path, sample_records):
        """Test right_align_numbers=False left-aligns all values."""
        output_file = tmp_path / "output.txt"
        column_widths = [5, 30, 30, 5, 12, 8, 12, 20]

        to_fixed_width(sample_records, column_widths, output_file, right_align_numbers=False)

        with open(output_file, encoding='utf-8') as f:
            first_line = f.readline()
            # trainee_id should be left-aligned
            trainee_id = first_line[:5]
            assert trainee_id.strip() == '1'
            # Should have trailing spaces
            assert trainee_id.endswith(' ')

    def test_truncate_overflow_true(self, tmp_path, sample_records):
        """Test truncate_overflow=True truncates values exceeding column width."""
        output_file = tmp_path / "output.txt"
        # Make columns too narrow
        column_widths = [5, 10, 10, 5, 12, 8, 12, 20]

        # Should not raise error with truncate=True
        to_fixed_width(sample_records, column_widths, output_file, truncate_overflow=True)

        assert output_file.exists()

    def test_truncate_overflow_false(self, tmp_path, sample_records):
        """Test truncate_overflow=False raises error on overflow."""
        output_file = tmp_path / "output.txt"
        # Make columns too narrow
        column_widths = [5, 10, 10, 5, 12, 8, 12, 20]

        with pytest.raises(ValueError, match="exceeds column width"):
            to_fixed_width(sample_records, column_widths, output_file, truncate_overflow=False)

    def test_column_width_mismatch(self, tmp_path, sample_records):
        """Test error when column widths don't match column count."""
        output_file = tmp_path / "output.txt"
        # Wrong number of widths
        column_widths = [5, 30, 30]

        with pytest.raises(ValueError, match="must match"):
            to_fixed_width(sample_records, column_widths, output_file)


class TestJSONWriter:
    """Tests specific to JSON writer."""

    def test_write_json_array(self, tmp_path, sample_records):
        """Test writing JSON as array of objects."""
        output_file = tmp_path / "output.json"

        to_json(sample_records, output_file)

        assert output_file.exists()

        # Parse and verify
        with open(output_file, encoding='utf-8') as f:
            data = json.load(f)

        assert isinstance(data, list)
        assert len(data) == 10
        assert data[0]['trainee_id'] == 1

    def test_json_pretty_print(self, tmp_path, sample_records):
        """Test JSON with indentation (pretty print)."""
        output_file = tmp_path / "output.json"

        to_json(sample_records, output_file, indent=2)

        with open(output_file, encoding='utf-8') as f:
            content = f.read()

        # Should have newlines and indentation
        assert '\n' in content
        assert '  ' in content

    def test_json_compact(self, tmp_path, sample_records):
        """Test JSON without indentation (compact)."""
        output_file = tmp_path / "output.json"

        to_json(sample_records, output_file, indent=None)

        with open(output_file, encoding='utf-8') as f:
            content = f.read()

        # Should be mostly on one line (no pretty printing)
        lines = content.strip().split('\n')
        assert len(lines) == 1

    def test_json_date_serialization(self, tmp_path, sample_records):
        """Test that dates are serialized as strings in JSON."""
        output_file = tmp_path / "output.json"

        to_json(sample_records, output_file)

        with open(output_file, encoding='utf-8') as f:
            data = json.load(f)

        # Dates should be strings
        assert isinstance(data[0]['birth_date'], str)
        assert data[0]['birth_date'] == '1965-12-16'


class TestNDJSONWriter:
    """Tests specific to NDJSON writer."""

    def test_write_ndjson(self, tmp_path, sample_records):
        """Test writing NDJSON (one JSON object per line)."""
        output_file = tmp_path / "output.ndjson"

        to_ndjson(sample_records, output_file)

        assert output_file.exists()

        # Parse and verify - each line should be valid JSON
        with open(output_file, encoding='utf-8') as f:
            lines = f.readlines()

        assert len(lines) == 10

        # Parse first line
        first_record = json.loads(lines[0])
        assert first_record['trainee_id'] == 1

    def test_ndjson_no_indentation(self, tmp_path, sample_records):
        """Test NDJSON has no indentation."""
        output_file = tmp_path / "output.ndjson"

        to_ndjson(sample_records, output_file)

        with open(output_file, encoding='utf-8') as f:
            first_line = f.readline()

        # Should be compact (no indentation within the line)
        assert '  ' not in first_line


class TestXMLWriter:
    """Tests specific to XML writer."""

    def test_write_xml(self, tmp_path, sample_records):
        """Test basic XML writing."""
        output_file = tmp_path / "output.xml"

        to_xml(sample_records, output_file)

        assert output_file.exists()

        # Parse and verify
        from lxml import etree
        tree = etree.parse(str(output_file))
        root = tree.getroot()

        assert root.tag == 'data'
        records = root.findall('record')
        assert len(records) == 10

    def test_custom_element_names(self, tmp_path, sample_records):
        """Test custom root and record element names."""
        output_file = tmp_path / "output.xml"

        to_xml(sample_records, output_file, root_element='trainees', record_element='trainee')

        from lxml import etree
        tree = etree.parse(str(output_file))
        root = tree.getroot()

        assert root.tag == 'trainees'
        records = root.findall('trainee')
        assert len(records) == 10

    def test_xml_pretty_print(self, tmp_path, sample_records):
        """Test XML with pretty printing."""
        output_file = tmp_path / "output.xml"

        to_xml(sample_records, output_file, pretty=True)

        with open(output_file, encoding='utf-8') as f:
            content = f.read()

        # Should have indentation
        assert '\n  ' in content or '\n    ' in content

    def test_xml_streaming(self, tmp_path, sample_records):
        """Test XML streaming mode."""
        output_file = tmp_path / "output.xml"

        to_xml(sample_records, output_file, stream=True)

        assert output_file.exists()

        # Verify it's valid XML
        from lxml import etree
        tree = etree.parse(str(output_file))
        root = tree.getroot()

        records = root.findall('record')
        assert len(records) == 10


class TestDatabaseWriter:
    """Tests specific to DatabaseWriter."""

    def test_database_writer_init(self, sample_records):
        """Test DatabaseWriter initialization."""
        from unittest.mock import Mock
        from dbtk.writers.database import DatabaseWriter

        # Create mock cursor with required attributes
        mock_cursor = Mock()
        mock_cursor.connection.interface.paramstyle = 'named'

        writer = DatabaseWriter(
            data=sample_records,
            target_cursor=mock_cursor,
            target_table='test_table'
        )

        assert writer.target_table == 'test_table'
        assert writer.batch_size == 1000
        assert writer.commit_frequency == 10000
        assert writer.paramstyle == 'named'

    def test_database_writer_custom_params(self, sample_records):
        """Test DatabaseWriter with custom batch and commit settings."""
        from unittest.mock import Mock
        from dbtk.writers.database import DatabaseWriter

        mock_cursor = Mock()
        mock_cursor.connection.interface.paramstyle = 'qmark'

        writer = DatabaseWriter(
            data=sample_records,
            target_cursor=mock_cursor,
            target_table='test_table',
            batch_size=500,
            commit_frequency=5000
        )

        assert writer.batch_size == 500
        assert writer.commit_frequency == 5000

    def test_database_writer_insert_statement_generation(self, sample_records):
        """Test that INSERT statement is generated correctly."""
        from unittest.mock import Mock
        from dbtk.writers.database import DatabaseWriter

        mock_cursor = Mock()
        mock_cursor.connection.interface.paramstyle = 'named'

        writer = DatabaseWriter(
            data=sample_records,
            target_cursor=mock_cursor,
            target_table='test_table'
        )

        # Verify INSERT statement was created
        assert writer.insert_sql is not None
        assert 'INSERT INTO test_table' in writer.insert_sql
        assert writer.param_names is not None
        assert len(writer.param_names) > 0

    def test_cursor_to_cursor_function(self, sample_records):
        """Test cursor_to_cursor convenience function."""
        from unittest.mock import Mock, patch
        from dbtk.writers.database import cursor_to_cursor

        mock_cursor = Mock()
        mock_cursor.connection.interface.paramstyle = 'format'

        # Patch DatabaseWriter.write to avoid actual execution
        with patch('dbtk.writers.database.DatabaseWriter.write', return_value=10):
            count = cursor_to_cursor(
                source_data=sample_records,
                target_cursor=mock_cursor,
                target_table='test_table'
            )

        assert count == 10

    def test_database_writer_different_paramstyles(self, sample_dicts):
        """Test DatabaseWriter handles different paramstyles."""
        from unittest.mock import Mock
        from dbtk.writers.database import DatabaseWriter

        paramstyles = ['qmark', 'format', 'named', 'pyformat', 'numeric']

        for paramstyle in paramstyles:
            mock_cursor = Mock()
            mock_cursor.connection.interface.paramstyle = paramstyle

            writer = DatabaseWriter(
                data=sample_dicts,
                target_cursor=mock_cursor,
                target_table='test_table'
            )

            # Should create writer without errors
            assert writer.paramstyle == paramstyle
            assert writer.insert_sql is not None

    def test_custom_element_names(self, tmp_path, sample_records):
        """Test custom root and record element names."""
        output_file = tmp_path / "output.xml"

        to_xml(sample_records, output_file, root_element='trainees', record_element='trainee')

        from lxml import etree
        tree = etree.parse(str(output_file))
        root = tree.getroot()

        assert root.tag == 'trainees'
        records = root.findall('trainee')
        assert len(records) == 10

    def test_xml_pretty_print(self, tmp_path, sample_records):
        """Test XML with pretty printing."""
        output_file = tmp_path / "output.xml"

        to_xml(sample_records, output_file, pretty=True)

        with open(output_file, encoding='utf-8') as f:
            content = f.read()

        # Should have indentation
        assert '\n  ' in content or '\n    ' in content

    def test_xml_streaming(self, tmp_path, sample_records):
        """Test XML streaming mode."""
        output_file = tmp_path / "output.xml"

        to_xml(sample_records, output_file, stream=True)

        assert output_file.exists()

        # Verify it's valid XML
        from lxml import etree
        tree = etree.parse(str(output_file))
        root = tree.getroot()

        records = root.findall('record')
        assert len(records) == 10


class TestDatabaseWriter:
    """Tests specific to DatabaseWriter."""

    def test_database_writer_init(self, sample_records):
        """Test DatabaseWriter initialization."""
        from unittest.mock import Mock
        from dbtk.writers.database import DatabaseWriter

        # Create mock cursor with required attributes
        mock_cursor = Mock()
        mock_cursor.connection.interface.paramstyle = 'named'

        writer = DatabaseWriter(
            data=sample_records,
            target_cursor=mock_cursor,
            target_table='test_table'
        )

        assert writer.target_table == 'test_table'
        assert writer.batch_size == 1000
        assert writer.commit_frequency == 10000
        assert writer.paramstyle == 'named'

    def test_database_writer_custom_params(self, sample_records):
        """Test DatabaseWriter with custom batch and commit settings."""
        from unittest.mock import Mock
        from dbtk.writers.database import DatabaseWriter

        mock_cursor = Mock()
        mock_cursor.connection.interface.paramstyle = 'qmark'

        writer = DatabaseWriter(
            data=sample_records,
            target_cursor=mock_cursor,
            target_table='test_table',
            batch_size=500,
            commit_frequency=5000
        )

        assert writer.batch_size == 500
        assert writer.commit_frequency == 5000

    def test_database_writer_insert_statement_generation(self, sample_records):
        """Test that INSERT statement is generated correctly."""
        from unittest.mock import Mock
        from dbtk.writers.database import DatabaseWriter

        mock_cursor = Mock()
        mock_cursor.connection.interface.paramstyle = 'named'

        writer = DatabaseWriter(
            data=sample_records,
            target_cursor=mock_cursor,
            target_table='test_table'
        )

        # Verify INSERT statement was created
        assert writer.insert_sql is not None
        assert 'INSERT INTO test_table' in writer.insert_sql
        assert writer.param_names is not None
        assert len(writer.param_names) > 0

    def test_cursor_to_cursor_function(self, sample_records):
        """Test cursor_to_cursor convenience function."""
        from unittest.mock import Mock, patch
        from dbtk.writers.database import cursor_to_cursor

        mock_cursor = Mock()
        mock_cursor.connection.interface.paramstyle = 'format'

        # Patch DatabaseWriter.write to avoid actual execution
        with patch('dbtk.writers.database.DatabaseWriter.write', return_value=10):
            count = cursor_to_cursor(
                source_data=sample_records,
                target_cursor=mock_cursor,
                target_table='test_table'
            )

        assert count == 10

    def test_database_writer_different_paramstyles(self, sample_dicts):
        """Test DatabaseWriter handles different paramstyles."""
        from unittest.mock import Mock
        from dbtk.writers.database import DatabaseWriter

        paramstyles = ['qmark', 'format', 'named', 'pyformat', 'numeric']

        for paramstyle in paramstyles:
            mock_cursor = Mock()
            mock_cursor.connection.interface.paramstyle = paramstyle

            writer = DatabaseWriter(
                data=sample_dicts,
                target_cursor=mock_cursor,
                target_table='test_table'
            )

            # Should create writer without errors
            assert writer.paramstyle == paramstyle
            assert writer.insert_sql is not None