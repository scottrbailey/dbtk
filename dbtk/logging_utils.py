# dbtk/logging_utils.py
"""
Logging utilities for integration scripts.

Provides convenient logging setup for ETL and integration scripts that follow
the pattern of creating timestamped log files like script_name_YYYYMMDD_HHMMSS.log
"""

import logging
import sys
from pathlib import Path
from datetime import datetime, timedelta
from typing import Optional, Tuple, List

logger = logging.getLogger(__name__)

# Module-level state for error tracking
_error_handler: Optional['ErrorCountHandler'] = None
_main_log_path: Optional[str] = None
_error_log_path: Optional[str] = None
_split_errors: bool = False


class ErrorCountHandler(logging.Handler):
    """Custom handler that counts ERROR and CRITICAL level messages and lazily creates error log."""

    def __init__(self, error_log_path: Optional[str] = None, formatter: Optional[logging.Formatter] = None):
        super().__init__()
        self.error_count = 0
        self.error_log_path = error_log_path
        self.formatter = formatter
        self._error_file_handler = None

    def emit(self, record):
        """Count errors and lazily create error log file on first error."""
        if record.levelno >= logging.ERROR:
            self.error_count += 1

            # Lazily create error file handler on first error
            if self.error_log_path and self._error_file_handler is None:
                try:
                    self._error_file_handler = logging.FileHandler(self.error_log_path, encoding='utf-8')
                    self._error_file_handler.setLevel(logging.ERROR)
                    if self.formatter:
                        self._error_file_handler.setFormatter(self.formatter)
                    # Add to root logger
                    logging.getLogger().addHandler(self._error_file_handler)
                    logger.debug(f"Created error log file: {self.error_log_path}")
                except Exception as e:
                    logger.warning(f"Failed to create error log file: {e}")


def setup_logging(
    script_name: Optional[str] = None,
    log_dir: Optional[str] = None,
    level: Optional[str] = None,
    split_errors: Optional[bool] = None,
    console: Optional[bool] = None
) -> Tuple[str, Optional[str]]:
    """
    Configure logging for integration scripts.

    Creates log files with pattern: {script_name}_{datetime}.log
    Optionally creates separate error log: {script_name}_{datetime}_error.log

    Args:
        script_name: Base name for log files (defaults to script filename without extension)
        log_dir: Directory for log files (defaults to config setting or './logs')
        level: Logging level string - DEBUG, INFO, WARNING, ERROR (defaults to config or 'INFO')
        split_errors: Create separate error log file (defaults to config or True)
        console: Also log to console/stdout (defaults to config or True)

    Returns:
        Tuple of (log_file_path, error_log_path or None)

    Example
    -------
    ::
        import dbtk

        # Simple - uses defaults from config
        dbtk.setup_logging('fire_nation_etl')

        # Custom settings
        dbtk.setup_logging('my_script', log_dir='/var/log/etl', level='DEBUG')

        # Single log file per day (set filename_format in config to '%Y%m%d')
        dbtk.setup_logging('daily_job')

        # Single rolling log file (set filename_format in config to '')
        dbtk.setup_logging('rolling_log')

    Note:
        To customize filename patterns, set 'logging.filename_format' in dbtk.yml:
        - '%Y%m%d_%H%M%S' - One log per run with date and time (default)
        - '%Y%m%d' - One log per day
        - '' - Single rolling log file (overwrites)
    """
    from dbtk.config import get_setting

    # Get script name from command line if not provided
    if script_name is None:
        script_name = Path(sys.argv[0]).stem

    # Load logging config dict
    logging_config = get_setting('logging', {})

    # Get settings with fallbacks
    log_dir = log_dir or logging_config.get('directory', './logs')
    level = level or logging_config.get('level', 'INFO')
    split_errors = split_errors if split_errors is not None else logging_config.get('split_errors', True)
    console = console if console is not None else logging_config.get('console', True)

    log_format = logging_config.get('format', '%(asctime)s [%(levelname)s] %(name)s: %(message)s')
    timestamp_format = logging_config.get('timestamp_format', '%Y-%m-%d %H:%M:%S')
    filename_format = logging_config.get('filename_format', '%Y%m%d_%H%M%S')

    # Create log directory
    log_dir_path = Path(log_dir)
    log_dir_path.mkdir(parents=True, exist_ok=True)

    # Generate filename with optional timestamp
    if filename_format:
        timestamp = datetime.now().strftime(filename_format)
        log_file = log_dir_path / f"{script_name}_{timestamp}.log"
        error_file = log_dir_path / f"{script_name}_{timestamp}_error.log" if split_errors else None
    else:
        # No timestamp - single rolling log file
        log_file = log_dir_path / f"{script_name}.log"
        error_file = log_dir_path / f"{script_name}_error.log" if split_errors else None

    # Configure root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(getattr(logging, level.upper()))

    # Remove existing handlers to avoid duplicates
    root_logger.handlers.clear()

    # Create formatter
    formatter = logging.Formatter(log_format, datefmt=timestamp_format)

    # Create and add error counting handler with lazy error log creation
    global _error_handler
    _error_handler = ErrorCountHandler(
        error_log_path=str(error_file) if split_errors and error_file else None,
        formatter=formatter
    )
    _error_handler.setLevel(logging.ERROR)
    root_logger.addHandler(_error_handler)

    # Main log file handler (all messages)
    file_handler = logging.FileHandler(log_file, encoding='utf-8')
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(formatter)
    root_logger.addHandler(file_handler)

    # Note: Error log file handler is created lazily by ErrorCountHandler on first error

    # Console handler
    if console:
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(getattr(logging, level.upper()))
        console_handler.setFormatter(formatter)
        root_logger.addHandler(console_handler)

    # Log startup message
    logging.info(f"Logging initialized: {log_file}")
    if split_errors and error_file:
        logging.info(f"Error log will be created at: {error_file} (if errors occur)")

    # Store state for errors_logged() function
    global _main_log_path, _error_log_path, _split_errors
    _main_log_path = str(log_file)
    _error_log_path = str(error_file) if error_file else None
    _split_errors = split_errors

    return (str(log_file), str(error_file) if error_file else None)


def errors_logged() -> Optional[str]:
    """
    Check if any ERROR or CRITICAL messages were logged during this run.

    Returns the path to the log file containing errors if any were logged,
    None otherwise. This allows integration scripts to easily detect if
    errors occurred and take action (e.g., send notification emails).

    Returns
    -------
    str or None
        Path to error log (if split_errors=True) or main log (if split_errors=False)
        when errors were logged. Returns None if no errors were logged or
        setup_logging() was not called.

    Example
    -------
    ::

        import dbtk
        import logging

        dbtk.setup_logging('my_integration')

        # ... do ETL work ...
        try:
            process_data()
        except Exception as e:
            logging.error(f"Processing failed: {e}")

        # Check if errors occurred
        error_log = dbtk.errors_logged()
        if error_log:
            print(f"Errors detected! See: {error_log}")
            # send_notification_email(subject="Integration errors", attachment=error_log)
        else:
            print("Integration completed successfully")
    """
    if _error_handler is None:
        logger.warning("errors_logged() called but setup_logging() was not called")
        return None

    if _error_handler.error_count == 0:
        return None

    # Errors were logged - return the appropriate log file
    if _split_errors and _error_log_path:
        return _error_log_path
    else:
        return _main_log_path


def cleanup_old_logs(
    log_dir: Optional[str] = None,
    retention_days: Optional[int] = None,
    pattern: str = "*.log",
    dry_run: bool = False
) -> List[str]:
    """
    Remove log files older than retention period.

    Args:
        log_dir: Directory to clean (defaults to config setting or './logs')
        retention_days: Keep logs newer than this many days (defaults to config or 30)
        pattern: Glob pattern for log files (default: ``'*.log'``)
        dry_run: If True, only report what would be deleted without actually deleting

    Returns:
        List of deleted (or would-be-deleted if dry_run) file paths

    Example
    -------
    ::

        import dbtk

        # Clean logs older than 30 days (from config)
        deleted = dbtk.cleanup_old_logs()
        print(f"Deleted {len(deleted)} old log files")

        # Custom retention
        deleted = dbtk.cleanup_old_logs(retention_days=7)

        # Dry run to see what would be deleted
        would_delete = dbtk.cleanup_old_logs(dry_run=True)
        print(f"Would delete: {would_delete}")

        # Clean specific pattern
        deleted = dbtk.cleanup_old_logs(pattern="error_*.log")
    """
    from dbtk.config import get_setting

    # Load logging config dict
    logging_config = get_setting('logging', {})

    # Get settings with fallbacks
    log_dir = log_dir or logging_config.get('directory', './logs')
    retention_days = retention_days or logging_config.get('retention_days', 30)

    log_dir_path = Path(log_dir)
    if not log_dir_path.exists():
        logger.warning(f"Log directory does not exist: {log_dir_path}")
        return []

    cutoff = datetime.now() - timedelta(days=retention_days)
    deleted = []

    for log_file in log_dir_path.glob(pattern):
        if not log_file.is_file():
            continue

        file_mtime = datetime.fromtimestamp(log_file.stat().st_mtime)
        if file_mtime < cutoff:
            if dry_run:
                logger.info(f"Would delete: {log_file}")
            else:
                try:
                    log_file.unlink()
                    logger.info(f"Deleted old log: {log_file}")
                except Exception as e:
                    logger.warning(f"Failed to delete {log_file}: {e}")
                    continue
            deleted.append(str(log_file))

    if not dry_run and deleted:
        logger.info(f"Cleaned up {len(deleted)} old log files")

    return deleted
