"""Tests for ExcelWriter BatchWriter modes and options."""

import pytest
pytest.importorskip("openpyxl")

from pathlib import Path
from dbtk.writers.excel import ExcelWriter

DATA1 = [
    {"id": 1, "name": "Alice", "score": 95},
    {"id": 2, "name": "Bob",   "score": 87},
]
DATA2 = [
    {"id": 3, "name": "Charlie", "score": 92},
    {"id": 4, "name": "Diana",   "score": 88},
]
DATA3 = [
    {"id": 5, "name": "Eve", "score": 91},
]


def test_mode1_data_on_init(tmp_path):
    """Mode 1: data passed to __init__, written via write()."""
    filepath = tmp_path / "out.xlsx"
    writer = ExcelWriter(DATA1, str(filepath))
    assert writer.write() == 2
    assert filepath.exists()


def test_mode2_write_batch(tmp_path):
    """Mode 2: no data on init, streamed via write_batch()."""
    filepath = tmp_path / "out.xlsx"
    with ExcelWriter(file=str(filepath)) as writer:
        writer.write_batch(DATA1)
        writer.write_batch(DATA2)
    assert filepath.exists()


def test_mode3_hybrid(tmp_path):
    """Mode 3: data on init + additional write_batch() calls."""
    filepath = tmp_path / "out.xlsx"
    with ExcelWriter(DATA1, str(filepath)) as writer:
        writer.write()
        writer.write_batch(DATA2)
        writer.write_batch(DATA3)
    assert filepath.exists()


def test_sheet_name(tmp_path):
    """Custom sheet_name is accepted without error."""
    filepath = tmp_path / "out.xlsx"
    with ExcelWriter(file=str(filepath), sheet_name="TestSheet") as writer:
        writer.write_batch(DATA1)
    assert filepath.exists()
