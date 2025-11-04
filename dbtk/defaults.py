# dbtk/defaults.py
"""Default settings - no imports to avoid circular dependencies."""

settings = {
    'default_column_case': 'lower',
    'default_country': 'US',
    'default_cursor_type': 'record',
    'default_db_type': 'postgres',
    'default_header_clean': 2,  # Clean.LOWER_NOSPACE as int
    'default_timezone': 'UTC',
    'lookup_preload_threshold': 500,
    'validator_preload_threshold': 1000,

    'logging': {
        'directory': './logs',
        'level': 'INFO',
        'format': '%(asctime)s [%(levelname)s] %(name)s: %(message)s',
        'timestamp_format': '%Y-%m-%d %H:%M:%S',
        'filename_format': '%Y%m%d_%H%M%S',  # Set to '' for single log file (no timestamp)
        'split_errors': True,
        'console': True,
        'retention_days': 30,
    }
}