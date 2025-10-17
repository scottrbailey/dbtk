# dbtk/defaults.py
"""Default settings - no imports to avoid circular dependencies."""

settings = {
    'default_column_case': 'lower',
    'default_country': 'US',
    'default_cursor_type': 'record',
    'default_header_clean': 2,  # Clean.LOWER_NOSPACE as int
    'default_timezone': 'UTC',
    'lookup_preload_threshold': 500,
    'validator_preload_threshold': 1000
}