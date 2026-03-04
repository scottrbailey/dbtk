# tests/test_edi_writer.py
"""Tests for EDIWriter and to_edi()."""

import pytest
from pathlib import Path

from dbtk.utils import FixedColumn
from dbtk.record import FixedWidthRecord
from dbtk.readers.fixed_width import EDIReader
from dbtk.readers.edi_formats import ACH_COLUMNS
from dbtk.writers.fixed_width import EDIWriter, to_edi


# ------------------------------------------------------------------ #
# Minimal two-type schema for fast unit tests
# ------------------------------------------------------------------ #

MINI_COLS = {
    'H': [
        FixedColumn('record_type', 1, 1),
        FixedColumn('name',        2, 11),
    ],
    'D': [
        FixedColumn('record_type', 1, 1),
        FixedColumn('amount',      2, 11, 'int', pad_char='0'),
    ],
}


def _make_record(type_code: str, schema=MINI_COLS) -> FixedWidthRecord:
    """Create a typed FixedWidthRecord using the given schema."""
    cls = type(f'R_{type_code}', (FixedWidthRecord,), {})
    cls.set_fields(schema[type_code])
    return cls


@pytest.fixture
def fixtures_dir():
    return Path(__file__).parent / 'fixtures' / 'readers'


@pytest.fixture
def ach_file(fixtures_dir):
    return fixtures_dir / 'sample_ach.ach'


# ------------------------------------------------------------------ #
# Construction / validation
# ------------------------------------------------------------------ #

class TestEDIWriterInit:

    def test_columns_required(self, tmp_path):
        with pytest.raises(ValueError, match="columns"):
            EDIWriter(file=tmp_path / 'out.ach')

    def test_inconsistent_key_lengths_raise(self, tmp_path):
        bad = {
            'H':  [FixedColumn('record_type', 1, 1)],
            'DX': [FixedColumn('record_type', 1, 2)],
        }
        with pytest.raises(ValueError, match="same length"):
            EDIWriter(file=tmp_path / 'out.ach', columns=bad)


# ------------------------------------------------------------------ #
# Writing FixedWidthRecord instances (fast path)
# ------------------------------------------------------------------ #

class TestEDIWriterFixedWidthRecordInput:

    def test_write_produces_correct_lines(self, tmp_path):
        H = _make_record('H')
        D = _make_record('D')
        records = [H('H', 'ACME CORP '), D('D', 100), D('D', 250)]

        out = tmp_path / 'out.txt'
        to_edi(records, MINI_COLS, out)

        lines = out.read_text().splitlines()
        assert len(lines) == 3
        assert lines[0] == 'HACME CORP '
        assert lines[1] == 'D0000000100'
        assert lines[2] == 'D0000000250'

    def test_write_batch_streaming(self, tmp_path):
        H = _make_record('H')
        D = _make_record('D')
        out = tmp_path / 'out.txt'

        with EDIWriter(file=out, columns=MINI_COLS) as w:
            w.write_batch([H('H', 'BATCH ONE ')])
            w.write_batch([D('D', 1), D('D', 2)])

        lines = out.read_text().splitlines()
        assert len(lines) == 3

    def test_unknown_type_code_raises(self, tmp_path):
        # build a FixedWidthRecord for type 'X' which is not in MINI_COLS
        X_cols = [FixedColumn('record_type', 1, 1), FixedColumn('val', 2, 11)]
        cls = type('R_X', (FixedWidthRecord,), {})
        cls.set_fields(X_cols)
        record = cls('X', 'something  ')

        out = tmp_path / 'out.txt'
        with pytest.raises(ValueError, match="not in schema"):
            to_edi([record], MINI_COLS, out)

    def test_row_count(self, tmp_path):
        H = _make_record('H')
        D = _make_record('D')
        records = [H('H', 'HDR       '), D('D', 1), D('D', 2), D('D', 3)]

        out = tmp_path / 'out.txt'
        writer = EDIWriter(records, out, MINI_COLS)
        count = writer.write()
        assert count == 4


# ------------------------------------------------------------------ #
# Writing from dicts (cast path)
# ------------------------------------------------------------------ #

class TestEDIWriterDictInput:

    def test_write_from_dicts(self, tmp_path):
        rows = [
            {'record_type': 'H', 'name': 'ACME CORP '},
            {'record_type': 'D', 'amount': 100},
            {'record_type': 'D', 'amount': 250},
        ]
        out = tmp_path / 'out.txt'
        to_edi(rows, MINI_COLS, out)

        lines = out.read_text().splitlines()
        assert lines[0] == 'HACME CORP '
        assert lines[1] == 'D0000000100'

    def test_write_from_lists(self, tmp_path):
        rows = [
            ['H', 'ACME CORP '],
            ['D', 99],
        ]
        out = tmp_path / 'out.txt'
        to_edi(rows, MINI_COLS, out)
        lines = out.read_text().splitlines()
        assert lines[0] == 'HACME CORP '
        assert lines[1] == 'D0000000099'


# ------------------------------------------------------------------ #
# ACH round-trip
# ------------------------------------------------------------------ #

class TestEDIWriterACHRoundTrip:

    def test_ach_round_trip_line_count(self, tmp_path, ach_file):
        """Read all ACH records and write them back; line count must match."""
        with open(ach_file) as fp:
            records = list(EDIReader(fp, ACH_COLUMNS))

        out = tmp_path / 'out.ach'
        to_edi(records, ACH_COLUMNS, out)

        original_lines = [l for l in ach_file.read_text().splitlines() if l.strip()]
        written_lines  = [l for l in out.read_text().splitlines() if l.strip()]
        assert len(written_lines) == len(original_lines)

    def test_ach_round_trip_line_width(self, tmp_path, ach_file):
        """Every written line must be exactly 94 characters (ACH record length)."""
        with open(ach_file) as fp:
            records = list(EDIReader(fp, ACH_COLUMNS))

        out = tmp_path / 'out.ach'
        to_edi(records, ACH_COLUMNS, out)

        for line in out.read_text().splitlines():
            assert len(line) == 94, f"Expected 94, got {len(line)}: {line!r}"

    def test_ach_round_trip_content(self, tmp_path, ach_file):
        """Spot-check that file header (type '1') round-trips correctly."""
        with open(ach_file) as fp:
            records = list(EDIReader(fp, ACH_COLUMNS))

        out = tmp_path / 'out.ach'
        to_edi(records, ACH_COLUMNS, out)

        original_header = ach_file.read_text().splitlines()[0]
        written_header  = out.read_text().splitlines()[0]
        assert written_header == original_header
