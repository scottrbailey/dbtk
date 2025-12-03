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

    usaddress - For parsing and cleaning address strings
        pip install usaddress
"""

# Import and re-export all core functions
from .core import (
    capitalize,
    coalesce,
    fn_resolver,
    format_number,
    get_bool,
    get_digits,
    get_float,
    get_int,
    get_list_item,
    indicator,
    intsOnlyPattern,
    normalize_whitespace,
    numbersOnlyPattern,
    parse_list,
    to_number,
)

from .phone import phone_validate, phone_clean
from .email import email_validate, email_clean
from .address import validate_us_address, standardize_address
from .datetime import parse_date, parse_datetime
from .database import TableLookup, Lookup, Validate

__all__ = [
    # Core text utilities
    'capitalize',
    'normalize_whitespace',
    'fn_resolver',

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

    # Address parsing
    'validate_us_address',
    'standardize_address',

    # Date/time parsing
    'parse_date',
    'parse_datetime',

    # Email
    'email_validate',
    'email_clean',

    # Phone number
    'phone_validate',
    'phone_clean',

    # Database
    'TableLookup',
    'Lookup',
    'Validate'
]