# tests/test_logging_utils.py
import pytest
import logging
import tempfile
from pathlib import Path

from dbtk.logging_utils import setup_logging, errors_logged, ErrorCountHandler


@pytest.fixture
def temp_log_dir():
    """Create temporary directory for log files."""
    with tempfile.TemporaryDirectory() as tmpdir:
        yield tmpdir


class TestErrorCountHandler:
    """Test ErrorCountHandler class functionality."""

    def test_counts_errors(self):
        """Test that handler counts ERROR messages."""
        handler = ErrorCountHandler()

        # Create test records
        error_record = logging.LogRecord(
            name='test', level=logging.ERROR, pathname='', lineno=0,
            msg='test error', args=(), exc_info=None
        )
        info_record = logging.LogRecord(
            name='test', level=logging.INFO, pathname='', lineno=0,
            msg='test info', args=(), exc_info=None
        )

        # Emit records
        handler.emit(error_record)
        handler.emit(info_record)
        handler.emit(error_record)

        assert handler.error_count == 2

    def test_counts_critical(self):
        """Test that handler counts CRITICAL messages."""
        handler = ErrorCountHandler()

        critical_record = logging.LogRecord(
            name='test', level=logging.CRITICAL, pathname='', lineno=0,
            msg='test critical', args=(), exc_info=None
        )

        handler.emit(critical_record)
        assert handler.error_count == 1

    def test_ignores_lower_levels(self):
        """Test that handler ignores DEBUG, INFO, WARNING."""
        handler = ErrorCountHandler()

        debug_record = logging.LogRecord(
            name='test', level=logging.DEBUG, pathname='', lineno=0,
            msg='test debug', args=(), exc_info=None
        )
        warning_record = logging.LogRecord(
            name='test', level=logging.WARNING, pathname='', lineno=0,
            msg='test warning', args=(), exc_info=None
        )

        handler.emit(debug_record)
        handler.emit(warning_record)

        assert handler.error_count == 0


class TestErrorsLogged:
    """Test errors_logged() function."""

    def test_no_errors_returns_none(self, temp_log_dir):
        """Test that errors_logged() returns None when no errors."""
        setup_logging('test_script', log_dir=temp_log_dir, console=False)

        logging.info("This is just info")
        logging.warning("This is a warning")

        result = errors_logged()
        assert result is None

    def test_with_errors_split_true(self, temp_log_dir):
        """Test errors_logged() returns error log path when split_errors=True."""
        main_log, error_log = setup_logging(
            'test_script',
            log_dir=temp_log_dir,
            split_errors=True,
            console=False
        )

        logging.error("This is an error")

        result = errors_logged()
        assert result is not None
        assert result == error_log
        assert Path(result).exists()

    def test_with_errors_split_false(self, temp_log_dir):
        """Test errors_logged() returns main log path when split_errors=False."""
        main_log, error_log = setup_logging(
            'test_script',
            log_dir=temp_log_dir,
            split_errors=False,
            console=False
        )

        assert error_log is None  # No separate error log

        logging.error("This is an error")

        result = errors_logged()
        assert result is not None
        assert result == main_log
        assert Path(result).exists()

    def test_multiple_errors(self, temp_log_dir):
        """Test errors_logged() with multiple errors."""
        setup_logging('test_script', log_dir=temp_log_dir, console=False)

        logging.error("Error 1")
        logging.critical("Critical error")
        logging.error("Error 2")

        result = errors_logged()
        assert result is not None

    def test_mixed_messages(self, temp_log_dir):
        """Test errors_logged() with mixed log levels."""
        main_log, error_log = setup_logging(
            'test_script',
            log_dir=temp_log_dir,
            split_errors=True,
            console=False
        )

        logging.info("Info message")
        logging.warning("Warning message")
        logging.error("Error message")
        logging.debug("Debug message")

        result = errors_logged()
        assert result == error_log

    def test_integration_pattern(self, temp_log_dir):
        """Test the integration pattern: setup, work, check errors."""
        setup_logging('integration_test', log_dir=temp_log_dir, console=False)

        # Simulate ETL work with an error
        try:
            logging.info("Starting ETL process")
            # Simulate error
            raise ValueError("Data validation failed")
        except Exception as e:
            logging.error(f"ETL failed: {e}")

        # Check for errors
        error_log = errors_logged()
        assert error_log is not None

        # Verify we can read the error log
        with open(error_log, 'r') as f:
            content = f.read()
            assert 'ERROR' in content
            assert 'Data validation failed' in content
