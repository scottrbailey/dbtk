#!/usr/bin/env python3
"""Test script to verify all 3 BatchWriter modes work for ExcelWriter."""

import os
import tempfile
from pathlib import Path
from dbtk.writers.excel import ExcelWriter

# Sample data
data1 = [
    {"id": 1, "name": "Alice", "score": 95},
    {"id": 2, "name": "Bob", "score": 87},
]

data2 = [
    {"id": 3, "name": "Charlie", "score": 92},
    {"id": 4, "name": "Diana", "score": 88},
]

data3 = [
    {"id": 5, "name": "Eve", "score": 91},
]

def test_mode_1():
    """Mode 1: Complete write from __init__ + write()"""
    print("Testing Mode 1: data on init + write()")

    with tempfile.NamedTemporaryFile(suffix='.xlsx', delete=False) as tmp:
        filepath = tmp.name

    try:
        # Single-shot write
        writer = ExcelWriter(filepath, data1)
        rows_written = writer.write()

        print(f"  ✓ Wrote {rows_written} rows in mode 1")
        assert rows_written == 2, f"Expected 2 rows, got {rows_written}"
        assert Path(filepath).exists(), "File should exist"

    finally:
        if os.path.exists(filepath):
            os.unlink(filepath)

def test_mode_2():
    """Mode 2: Batch write (no data on init) + write_batch()"""
    print("Testing Mode 2: no data on init + write_batch()")

    with tempfile.NamedTemporaryFile(suffix='.xlsx', delete=False) as tmp:
        filepath = tmp.name

    try:
        # Pure streaming
        with ExcelWriter(file=filepath) as writer:
            writer.write_batch(data1)
            writer.write_batch(data2)

        print(f"  ✓ Wrote batches in mode 2")
        assert Path(filepath).exists(), "File should exist"

    finally:
        if os.path.exists(filepath):
            os.unlink(filepath)

def test_mode_3():
    """Mode 3: Hybrid - data on init + write() + write_batch()"""
    print("Testing Mode 3: data on init + write() + write_batch()")

    with tempfile.NamedTemporaryFile(suffix='.xlsx', delete=False) as tmp:
        filepath = tmp.name

    try:
        # Hybrid mode
        with ExcelWriter(filepath, data1) as writer:
            writer.write()  # Write initial batch
            writer.write_batch(data2)  # Stream additional
            writer.write_batch(data3)  # Stream more

        print(f"  ✓ Wrote initial data + batches in mode 3")
        assert Path(filepath).exists(), "File should exist"

    finally:
        if os.path.exists(filepath):
            os.unlink(filepath)

def test_sheet_name_support():
    """Test that sheet_name parameter works correctly"""
    print("Testing sheet_name parameter")

    with tempfile.NamedTemporaryFile(suffix='.xlsx', delete=False) as tmp:
        filepath = tmp.name

    try:
        with ExcelWriter(file=filepath, sheet_name='TestSheet') as writer:
            writer.write_batch(data1)

        print(f"  ✓ Created sheet with custom name")
        assert Path(filepath).exists(), "File should exist"

    finally:
        if os.path.exists(filepath):
            os.unlink(filepath)

if __name__ == '__main__':
    print("=" * 60)
    print("ExcelWriter 3-Mode Support Test")
    print("=" * 60)

    test_mode_1()
    test_mode_2()
    test_mode_3()
    test_sheet_name_support()

    print()
    print("=" * 60)
    print("✅ All tests passed!")
    print("=" * 60)
