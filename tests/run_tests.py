# dbtk/tests/run_tests.py
# !/usr/bin/env python3
"""
Test runner script for dbtk library.

Usage:
    python dbtk/tests/run_tests.py                    # Run all dbtk_tests
    python dbtk/tests/run_tests.py --verbose          # Verbose output
    python dbtk/tests/run_tests.py --coverage         # With coverage report
    python dbtk/tests/run_tests.py --module utils     # Test specific module
"""

import sys
import os
import argparse
import subprocess
from pathlib import Path




def run_tests(args):
    """Run the test suite with specified options."""

    # Base pytest command
    cmd = ['python', '-m', 'pytest']

    # Add test directory
    test_dir = Path(__file__).parent
    cmd.append(str(test_dir))

    # Add options based on arguments
    if args.verbose:
        cmd.append('-v')

    if args.coverage:
        cmd.extend([
            '--cov=dbtk',
            '--cov-report=html',
            '--cov-report=term-missing'
        ])

    if args.module:
        # Run specific test module
        test_file = test_dir / f'test_{args.module}.py'
        if test_file.exists():
            cmd = ['python', '-m', 'pytest', str(test_file)]
            if args.verbose:
                cmd.append('-v')
        else:
            print(f"Error: Test file {test_file} not found")
            return 1

    if args.fast:
        # Skip slow dbtk_tests
        cmd.append('-m not slow')

    if args.pattern:
        cmd.extend(['-k', args.pattern])

    # Run the dbtk_tests
    print(f"Running: {' '.join(cmd)}")
    result = subprocess.run(cmd)

    return result.returncode


def check_dependencies():
    """Check if test dependencies are installed."""
    try:
        import pytest
        import dbtk
        print("✓ Core dependencies found")
    except ImportError as e:
        print(f"✗ Missing dependency: {e}")
        print("Install test requirements with: pip install -r dbtk_tests/requirements.txt")
        return False

    # Check optional dependencies
    optional_deps = {
        'openpyxl': 'Excel support',
        'dateutil': 'Enhanced date parsing',
    }

    for module, description in optional_deps.items():
        try:
            __import__(module)
            print(f"✓ {description} available")
        except ImportError:
            print(f"! {description} not available (optional)")

    return True


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(description='Run dbtk test suite')
    parser.add_argument('--verbose', '-v', action='store_true',
                        help='Verbose output')
    parser.add_argument('--coverage', '-c', action='store_true',
                        help='Generate coverage report')
    parser.add_argument('--module', '-m', type=str,
                        help='Run dbtk_tests for specific module (e.g., utils, config)')
    parser.add_argument('--fast', '-f', action='store_true',
                        help='Skip slow dbtk_tests')
    parser.add_argument('--pattern', '-k', type=str,
                        help='Run dbtk_tests matching pattern')
    parser.add_argument('--check-deps', action='store_true',
                        help='Check test dependencies and exit')

    args = parser.parse_args()

    # Check dependencies
    if not check_dependencies():
        return 1

    if args.check_deps:
        return 0

    # Run dbtk_tests
    return run_tests(args)


if __name__ == '__main__':
    sys.exit(main())
