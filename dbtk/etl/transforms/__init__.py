# dbtk/etl/transforms/__init__.py
"""
Data transformation and parsing functions.

This package provides utilities for:
- Date/time parsing with timezone support (transforms.datetime)
- Phone number validation and formatting (transforms.phone)
- Email validation and cleaning (transforms.email)
- Database-backed code validation and lookups (transforms.database)
- Basic data type conversions and text manipulation (transforms.core - exported here)

Core functions are available directly from this module for convenience.
Specialized functionality requires explicit submodule imports.

Usage:
    # Core functions - direct import
    from dbtk.etl.transforms import get_int, coalesce, capitalize

    # Specialized functions - submodule import
    from dbtk.etl.transforms.datetime import parse_date, parse_datetime
    from dbtk.etl.transforms.phone import Phone, PhoneFormat
    from dbtk.etl.transforms.email import email_validate
    from dbtk.etl.transforms.database import CodeValidator

Optional Dependencies:
    phonenumbers - For robust international phone number support
        pip install phonenumbers

    dateutil - For additional date/time parsing flexibility
        pip install python-dateutil
"""

# Import and re-export all core functions
from .core import (
    capitalize,
    coalesce,
    indicator,
    get_int,
    get_float,
    get_list_item,
    get_digits,
    to_number,
    get_bool,
    normalize_whitespace,
    format_number,
    parse_list,
    intsOnlyPattern,
    numbersOnlyPattern,
)

__all__ = [
    # Core text utilities
    'capitalize',
    'normalize_whitespace',

    # Logic helpers
    'coalesce',
    'indicator',
    'get_bool',

    # Type conversion
    'get_int',
    'get_float',
    'to_number',
    'get_digits',

    # List/string utilities
    'get_list_item',
    'parse_list',
    'format_number',

    # Patterns
    'intsOnlyPattern',
    'numbersOnlyPattern',
]