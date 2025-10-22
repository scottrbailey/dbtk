# dbtk/etl/transforms/email.py
"""
Email address validation and cleaning functions.

Uses a practical regex that catches most valid emails and rejects
obviously invalid ones. Not RFC 5322 compliant but good enough for
most data validation needs.
"""

import re

# Email validation regex - practical but not RFC-compliant
emailPattern = re.compile(
    r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}'
)


def email_clean(val: str) -> str:
    """
    Clean and normalize email address.

    Args:
        val: Email address string

    Returns:
        Cleaned email address (lowercase, stripped) or empty string if invalid

    Examples:
        email_clean("  User@Example.COM  ")  # "user@example.com"
        email_clean("invalid.email")         # ""
        email_clean("user@domain.co")        # "user@domain.co"
    """
    if not val:
        return ''

    val_clean = str(val).strip().lower()

    if email_validate(val_clean):
        return val_clean
    else:
        return ''


def email_validate(val: str) -> bool:
    """
    Validate email address format.

    Uses a practical regex that catches most valid emails and rejects
    obviously invalid ones. Not RFC 5322 compliant but good enough
    for most data validation needs.

    Args:
        val: Email address string to validate

    Returns:
        True if email appears valid, False otherwise

    Examples:
        email_validate("user@example.com")     # True
        email_validate("user.name@domain.co")  # True
        email_validate("invalid.email")        # False
        email_validate("@domain.com")          # False
        email_validate("")                     # False
    """
    if not val:
        return False

    val_clean = str(val).strip()
    if not val_clean:
        return False

    return bool(emailPattern.match(val_clean))