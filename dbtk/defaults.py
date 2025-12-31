# dbtk/defaults.py
"""Default settings - no imports to avoid circular dependencies."""

settings = {
    'default_batch_size': 1000,
    'default_column_case': 'lower',
    'default_country': 'US',
    'default_db_type': 'postgres',
    'default_header_clean': 2,  # Clean.LOWER_NOSPACE as int
    'data_dump_dir': '/tmp', # default directory for data dumps
    'compressed_file_buffer_size': 1024 * 1024,  # 1MB buffer for .gz/.bz2/.xz files
    'null_string': '',       # how null is represented in text outputs
    'null_string_csv': '',   # how null is represented in CSV outputs
    'default_timezone': 'UTC',
    'date_format': '%Y-%m-%d',
    'time_format': '%H:%M:%S',
    'datetime_format': '%Y-%m-%d %H:%M:%S',
    'timestamp_format': '%Y-%m-%d %H:%M:%S.%f',  # with microseconds
    'tz_suffix': ' %z',
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