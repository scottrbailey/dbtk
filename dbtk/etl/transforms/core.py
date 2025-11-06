# dbtk/etl/transforms/core.py
"""
Core transformation functions for basic data manipulation.

Includes text processing, type conversion, and basic utility functions.
"""

import re
from typing import Any, List, Optional, Union

# Number regex patterns
intsOnlyPattern = re.compile(r'^[\-\+]?\d+$')
numbersOnlyPattern = re.compile(r'^[\-\+]?\d+(\.\d+)?$')


def capitalize(val: Any) -> Any:
    """
    Title case a string, but only if it's already in all uppercase or all lowercase.

    Args:
        val: String value to capitalize

    Returns:
        Title-cased string if applicable, otherwise original value

    Example:
        capitalize("HELLO WORLD")  # "Hello World"
        capitalize("hello world")  # "Hello World"
        capitalize("Hello World")  # "Hello World" (unchanged)
    """
    if val and hasattr(val, 'title'):
        if val in (val.upper(), val.lower()):
            return val.title()
    return val


def coalesce(vals: Union[List[Any], tuple]) -> Any:
    """
    Returns the first non-empty and non-None value from a list or tuple.

    Args:
        vals: List or tuple of values to check

    Returns:
        First non-empty, non-None value, or the original input if not a list/tuple

    Example:
        coalesce([None, '', 'first', 'second'])  # "first"
        coalesce([0, 1, 2])                      # 0
        coalesce([None, None])                   # None
    """
    if isinstance(vals, (list, tuple)):
        for v in vals:
            if v not in ('', None):
                return v


def get_bool(val: Any) -> Optional[bool]:
    """
    Parse a value as a boolean.

    Args:
        val: Value to parse

    Returns:
        True for truthy values, False for falsy values, None for None

    Truthy: True, 'T', 'TRUE', 'YES', 'Y', '1', 1, 1.0, non-zero numbers
    Falsy: False, 'F', 'FALSE', 'NO', 'N', '0', 0, 0.0, empty string
    None: None

    Example:
        get_bool(True)           # True
        get_bool('yes')          # True
        get_bool('Y')            # True
        get_bool(1)              # True
        get_bool(1.0)            # True
        get_bool(False)          # False
        get_bool('no')           # False
        get_bool(0)              # False
        get_bool(None)           # None
    """
    if val is None:
        return None

    # Handle boolean type directly
    if isinstance(val, bool):
        return val

    # Handle numeric types
    if isinstance(val, (int, float)):
        return bool(val)

    # Handle string values
    val_str = str(val).strip().upper()

    if val_str in ('T', 'TRUE', 'YES', 'Y', '1'):
        return True
    elif val_str in ('F', 'FALSE', 'NO', 'N', '0', ''):
        return False

    # Default: treat non-empty strings as truthy
    return bool(val_str)


def indicator(val: Any, true_val: str = 'Y', false_val: Any = None) -> Any:
    """
    Convert a value to a boolean indicator.

    Uses get_bool() for consistent boolean interpretation.

    Args:
        val: Value to test for truthiness
        true_val: Value to return if val is truthy (default: 'Y')
        false_val: Value to return if val is falsy (default: None)

    Returns:
        true_val if val is truthy, false_val otherwise, or None if val is None

    Example:
        indicator(True)           # "Y"
        indicator('yes')          # "Y"
        indicator(1)              # "Y"
        indicator(False)          # None
        indicator(0, 'T', 'F')    # "F"
        indicator(None)           # None
    """
    parsed = get_bool(val)
    if parsed is None:
        return None
    return true_val if parsed else false_val


def get_digits(val: Any) -> str:
    """
    Extract only numeric digits from a value, preserving leading zeros.

    Removes all non-digit characters except leading +/- signs.
    Returns a string to preserve leading zeros.

    Args:
        val: Value to extract digits from

    Returns:
        String containing only digits (and possibly leading +/-), or empty string if no value

    Example:
        get_digits("(800) 123-4567")   # "8001234567"
        get_digits("012-34-5678")      # "012345678"
        get_digits("$-42.35")          # "4235"
        get_digits("+1-555-123-4567")  # "15551234567"
    """
    if not val:
        return None

    val_str = str(val)

    # Extract only digits
    digits = re.sub(r'\D', '', val_str)

    return digits if digits else None


def to_number(val: Any) -> Optional[float]:
    """
    Convert a value to a number by extracting digits and converting to float.

    Strips out all non-numeric characters (except leading +/- and decimal point).

    Args:
        val: Value to convert to number

    Returns:
        Float value or None if no valid number can be extracted

    Example:
        to_number("$42.35")       # 42.35
        to_number("$-42.35")      # -42.35
        to_number("1,234.56")     # 1234.56
        to_number("N/A")          # None
    """
    if not val:
        return None

    val_str = str(val).strip()
    if not val_str:
        return None

    # Remove everything except digits, +, -, and .
    cleaned = re.sub(r'[^\d+\-.]', '', val_str)

    # Extract first number (same pattern as numbersOnlyPattern but without anchors)
    match = re.search(r'[\-\+]?\d+(\.\d+)?', cleaned)

    if match:
        try:
            return float(match.group())
        except ValueError:
            return None

    return None


def get_int(val: Any) -> Optional[int]:
    """
    Convert value to integer.

    Handles string numbers with decimals and currency symbols by using to_number().

    Args:
        val: Value to convert

    Returns:
        Integer value or None if val is falsy or cannot be converted

    Example:
        get_int("123")       # 123
        get_int("123.45")    # 123
        get_int("$123.45")   # 123
        get_int(123.45)      # 123
        get_int(None)        # None
    """
    num = to_number(val)
    if num is not None:
        return int(num)
    return None


def get_float(val: Any) -> Optional[float]:
    """
    Convert value to float.

    Handles currency symbols and formatting by using to_number().

    Args:
        val: Value to convert

    Returns:
        Float value or None if val is falsy or cannot be converted

    Example:
        get_float("123.45")   # 123.45
        get_float("$123.45")  # 123.45
        get_float("123")      # 123.0
        get_float(None)       # None
    """
    return to_number(val)


def normalize_whitespace(val: Any) -> str:
    """
    Normalize whitespace in a string.

    - Strips leading and trailing whitespace
    - Collapses multiple spaces/tabs/newlines into single spaces

    Args:
        val: Value to normalize

    Returns:
        String with normalized whitespace, or empty string if no value

    Example:
        normalize_whitespace("  hello   world  ")     # "hello world"
        normalize_whitespace("hello\\n\\nworld")      # "hello world"
        normalize_whitespace("hello\\t\\tworld")      # "hello world"
    """
    if not val:
        return ''

    val_str = str(val)
    # Replace all whitespace sequences with single space
    normalized = re.sub(r'\s+', ' ', val_str)
    return normalized.strip()


def format_number(val: Any, pattern: str) -> str:
    """
    Format a number string according to a pattern.

    Extracts digits from the value and applies the pattern if the number
    of digits matches the number of '#' characters in the pattern.

    Args:
        val: Number value to format (string or numeric)
        pattern: Format pattern using '#' for digit positions

    Returns:
        Formatted string if digit count matches, otherwise original string

    Example:
        format_number('8001234567', '(###) ###-####')     # "(800) 123-4567"
        format_number('012345678', '###-##-####')         # "012-34-5678"
        format_number('(800) 123-4567', '###.###.####')   # "800.123.4567"
        format_number('12345', '###-##-####')             # "12345" (wrong length)
    """
    if not val:
        return ''

    # Extract digits from input
    digits = get_digits(val)
    # Remove leading +/- for formatting
    if digits and digits[0] in ('+', '-'):
        digits = digits[1:]

    # Count expected digits in pattern
    expected_digits = pattern.count('#')

    # If digit count doesn't match, return original value
    if len(digits) != expected_digits:
        return str(val)

    # Apply pattern
    result = []
    digit_index = 0

    for char in pattern:
        if char == '#':
            if digit_index < len(digits):
                result.append(digits[digit_index])
                digit_index += 1
        else:
            result.append(char)

    return ''.join(result)


def parse_list(val: Any, delimiter: Optional[str] = None) -> List[str]:
    """
    Parse a delimited string into a list of items.

    Args:
        val: Delimited string to parse
        delimiter: Delimiter to use. If None, auto-detects comma, tab, or pipe.
                  Raises error if multiple delimiter types found.

    Returns:
        List of stripped items

    Raises:
        ValueError: If auto-detection finds multiple delimiter types

    Example:
        parse_list("a,b,c")           # ["a", "b", "c"]
        parse_list("a|b|c", "|")      # ["a", "b", "c"]
        parse_list("a\\tb\\tc")       # ["a", "b", "c"]
        parse_list("a, b, c")         # ["a", "b", "c"] (strips spaces)
    """
    if not val:
        return []

    val_str = str(val)

    # Auto-detect delimiter if not specified
    if delimiter is None:
        delimiters_found = []
        if ',' in val_str:
            delimiters_found.append(',')
        if '\t' in val_str:
            delimiters_found.append('\t')
        if '|' in val_str:
            delimiters_found.append('|')

        if len(delimiters_found) > 1:
            raise ValueError(
                f"Multiple delimiters found: {delimiters_found}. "
                "Please specify delimiter explicitly."
            )
        elif len(delimiters_found) == 1:
            delimiter = delimiters_found[0]
        else:
            # No delimiter found, return single-item list
            return [val_str.strip()]

    # Split and strip each item
    items = val_str.split(delimiter)
    return [item.strip() for item in items]


def get_list_item(val: str, index: int, delimiter: str = ',') -> Optional[str]:
    """
    Get an item from a delimited string list.

    Args:
        val: Delimited string
        index: Zero-based index of item to retrieve
        delimiter: Delimiter character (default: ',')

    Returns:
        Item at specified index (stripped), or None if index out of range

    Example:
        get_list_item("a,b,c", 1)       # "b"
        get_list_item("a|b|c", 0, "|")  # "a"
        get_list_item("a,b", 5)         # None
    """
    if val:
        values = val.split(delimiter)
        if index < len(values):
            return values[index].strip()
    return None