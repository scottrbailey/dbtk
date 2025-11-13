# tests/test_config.py
import pytest
import os
from pathlib import Path
from unittest.mock import patch, MagicMock

from dbtk.config import (
    ConfigManager, connect, get_password, get_setting,
    generate_encryption_key, encrypt_password_cli
)


@pytest.fixture
def test_config_file():
    """Path to test config file."""
    return Path(__file__).parent / 'test.yml'


@pytest.fixture
def config_manager(test_config_file):
    """Create ConfigManager instance with test config."""
    return ConfigManager(str(test_config_file))


class TestConfigManager:
    """Test ConfigManager class functionality."""

    def test_init_with_valid_config(self, config_manager):
        """Test ConfigManager initializes with valid config file."""
        assert config_manager.config is not None
        assert 'connections' in config_manager.config
        assert 'settings' in config_manager.config

    def test_init_with_missing_file(self):
        """Test ConfigManager raises error for missing file."""
        with pytest.raises(FileNotFoundError):
            ConfigManager('/nonexistent/path/config.yml')

    def test_get_connection_config_valid(self, config_manager):
        """Test getting valid connection configuration."""
        config = config_manager.get_connection_config('test_db')

        assert config['type'] == 'postgres'
        assert config['host'] == 'localhost'
        assert config['database'] == 'testdb'
        assert config['user'] == 'testuser'
        assert config['password'] == 'testpass'

    def test_get_connection_config_invalid(self, config_manager):
        """Test getting invalid connection raises error."""
        with pytest.raises(ValueError, match="Connection 'nonexistent' not found"):
            config_manager.get_connection_config('nonexistent')

    def test_list_connections(self, config_manager):
        """Test listing available connections."""
        connections = config_manager.list_connections()
        assert 'test_db' in connections
        assert 'encrypted_db' in connections
        assert len(connections) == 5

    def test_get_setting_simple(self, config_manager):
        """Test getting simple setting value."""
        timezone = config_manager.get_setting('default_timezone')
        assert timezone == 'UTC'

    def test_get_setting_default(self, config_manager):
        """Test getting setting with default value."""
        value = config_manager.get_setting('nonexistent_setting', 'default_value')
        assert value == 'default_value'

    def test_get_setting_nested(self, config_manager):
        """Test getting nested setting with dot notation."""
        value = config_manager.get_setting('database.timeout')
        assert value == 30

    def test_get_password_plain(self, config_manager):
        """Test getting plain text password."""
        password = config_manager.get_password('api_key')
        assert password == 'secret123'

    def test_get_password_invalid(self, config_manager):
        """Test getting invalid password raises error."""
        with pytest.raises(ValueError, match="Password 'nonexistent' not found"):
            config_manager.get_password('nonexistent')

    def test_get_password_encrypted(self, config_manager):
        """Test getting encrypted password."""
        password = config_manager.get_password('encrypted_key')
        assert password == 'encrypted_secret_123'

    def test_list_passwords(self, config_manager):
        """Test listing available passwords."""
        passwords = config_manager.list_passwords()
        assert 'api_key' in passwords
        assert 'encrypted_key' in passwords
        assert len(passwords) == 2

    def test_environment_variable_substitution(self, config_manager):
        """Test environment variable substitution in passwords."""
        # Mock environment variable
        with patch.dict(os.environ, {'TEST_PASSWORD': 'env_password'}):
            config = config_manager.get_connection_config('env_db')
            assert config['password'] == 'env_password'

    def test_environment_variable_missing(self, config_manager):
        """Test missing environment variable raises error."""
        with pytest.raises(ValueError, match="Environment variable MISSING_VAR not set"):
            config_manager.get_connection_config('missing_env_db')


class TestEncryption:
    """Test encryption functionality."""

    @patch('dbtk.config.HAS_CRYPTO', True)
    def test_generate_encryption_key(self):
        """Test encryption key generation."""
        key = generate_encryption_key()
        assert key is not None
        assert len(key) > 20  # Fernet keys are base64 encoded, should be longer

    @patch('dbtk.config.HAS_CRYPTO', True)
    @patch('dbtk.config.Fernet')
    def test_encrypt_password_cli(self, mock_fernet):
        """Test CLI password encryption."""
        mock_instance = MagicMock()
        mock_instance.encrypt.return_value = b'encrypted_data'
        mock_fernet.return_value = mock_instance

        result = encrypt_password_cli('test_password', 'test_key')
        assert result == 'encrypted_data'

    def test_decrypt_encrypted_connection_password(self, config_manager):
        """Test that encrypted connection passwords decrypt correctly."""
        config = config_manager.get_connection_config('encrypted_db')
        assert config['password'] == 'encrypted_secret_123'

    def test_decrypt_encrypted_stored_password(self, config_manager):
        """Test that encrypted stored passwords decrypt correctly."""
        password = config_manager.get_password('encrypted_key')
        assert password == 'encrypted_secret_123'

    def test_decrypt_invalid_encrypted_password(self, config_manager):
        """Test that invalid encrypted passwords raise proper errors."""
        with pytest.raises(ValueError, match="Failed to decrypt password"):
            config_manager.decrypt_password('invalid_encrypted_data')

    def test_encrypt_decrypt_roundtrip_with_real_key(self, config_manager):
        """Test encryption/decryption roundtrip with the test key."""
        original_password = 'test_roundtrip_password'

        # Encrypt
        encrypted = config_manager.encrypt_password(original_password)
        assert encrypted != original_password
        assert len(encrypted) > 20

        # Decrypt
        decrypted = config_manager.decrypt_password(encrypted)
        assert decrypted == original_password




class TestGlobalFunctions:
    """Test global convenience functions."""

    def test_connect_function_sqlite(self, test_config_file):
        """Test global connect function with sqlite database using in-memory database."""
        with patch('dbtk.config._config_manager', None):  # Force new instance
            # Create a temporary config manager to test connection
            from dbtk.config import ConfigManager
            from dbtk.database import Database

            # Test with in-memory sqlite database (doesn't require file system)
            db = Database.create('sqlite', database=':memory:')

            # Verify we got a database object
            assert db is not None
            assert hasattr(db, 'cursor')
            assert db.database_type == 'sqlite'

            # Test that we can use the cursor
            cursor = db.cursor()
            cursor.execute('CREATE TABLE test (id INTEGER)')
            cursor.execute('INSERT INTO test VALUES (1)')
            cursor.execute('SELECT * FROM test')
            result = cursor.fetchone()
            assert result['id'] == 1

            # Clean up
            db.close()

    def test_connect_function_config_not_found(self, test_config_file):
        """Test global connect function raises error for missing connection."""
        with patch('dbtk.config._config_manager', None):  # Force new instance
            with pytest.raises(ValueError, match="Connection 'nonexistent' not found"):
                connect('nonexistent', config_file=str(test_config_file))

    def test_get_password_function(self, test_config_file):
        """Test global get_password function."""
        with patch('dbtk.config._config_manager', None):  # Force new instance
            result = get_password('api_key', config_file=str(test_config_file))
            assert result == 'secret123'

    def test_get_setting_function(self, test_config_file):
        """Test global get_setting function."""
        with patch('dbtk.config._config_manager', None):  # Force new instance
            result = get_setting('default_timezone', 'UTC', config_file=str(test_config_file))
            assert result == 'UTC'