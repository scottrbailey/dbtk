# dbtk/config.py
"""
Configuration management for database connections.
Supports YAML configuration files with optional password encryption and global settings.
"""

import os
from textwrap import dedent
import logging
from pathlib import Path
from typing import Dict, Any, Optional, List, Tuple

from .defaults import settings # noqa: F401
from .database import Database, _get_params_for_database
from .cursors import Cursor

try:
    import yaml
except ImportError:
    raise ImportError("PyYAML is required. Install with: pip install PyYAML")

try:
    from cryptography.fernet import Fernet
    HAS_CRYPTO = True
except ImportError:
    HAS_CRYPTO = False

try:
    import keyring
    HAS_KEYRING = True
except ImportError:
    HAS_KEYRING = False

logger = logging.getLogger(__name__)


def _ensure_sample_config():
    """Copy sample config to ~/.config if no config exists and sample doesn't exist there."""
    import shutil

    # Define paths
    user_config_dir = Path.home() / '.config'
    user_config_file = user_config_dir / 'dbtk.yml'

    # If user config already exists, nothing to do
    if user_config_file.exists():
        return

    # Find sample config in package
    package_dir = Path(__file__).parent
    sample_config = package_dir / 'dbtk_sample.yml'
    sample_target = user_config_dir / 'dbtk_sample.yml'

    if not sample_config.exists():
        return  # No sample to copy

    try:
        # Create ~/.config if it doesn't exist
        user_config_dir.mkdir(parents=True, exist_ok=True)

        # Copy sample config
        shutil.copy(sample_config, sample_target)
        logger.info(f"Created sample config at {sample_target}")
        import sys
        print(f"Created sample DBTK config at {sample_target}", file=sys.stderr)

    except Exception as e:
        logger.debug(f"Could not create sample config: {e}")
        # Don't fail - just continue without config


def diagnose_config(config_file: Optional[str] = None) -> List[Tuple[str, str]]:
    """Full config health check using a real ConfigManager instance."""
    results = []

    # 1. Spin up a REAL manager — handles path, sample creation, everything
    try:
        mgr = ConfigManager(config_file)
        results.append(('✓', f"Config loaded: {mgr.config_file}"))
    except Exception as e:
        results.append(('✗', f"Config failed: {e}"))
        return results

    # 2. Deps
    results.append(('✓', "cryptography ready") if HAS_CRYPTO else ('✗', "cryptography missing"))
    results.append(('✓', "keyring ready") if HAS_KEYRING else ('?', "keyring optional"))

    # 3. Keys — safe peek, no decrypt
    env_key = os.getenv('DBTK_ENCRYPTION_KEY')
    keyring_key = keyring.get_password('dbtk', 'encryption_key') if HAS_KEYRING else None

    if env_key:
        results.append(('✓', "DBTK_ENCRYPTION_KEY set"))
        results.append(('✓', "Env key valid") if _valid_fernet(env_key) else ('✗', "Env key invalid"))
    else:
        results.append(('?', "No env key"))

    if keyring_key:
        results.append(('✓', "Keyring key set"))
        results.append(('✓', "Keyring key valid") if _valid_fernet(keyring_key) else ('✗', "Keyring key invalid"))
    elif HAS_KEYRING:
        results.append(('?', "Keyring empty"))

    if env_key and keyring_key:
        results.append(('✓', "Keys match") if env_key == keyring_key else ('✗', "KEYS MISMATCH"))

    # 4. Encrypted passwords? — ask the manager, not the file
    enc_count = sum(
        1 for c in mgr.config.get('connections', {}).values()
        if 'encrypted_password' in c
    ) + sum(
        1 for p in mgr.config.get('passwords', {}).values()
        if 'encrypted_password' in p
    )

    results.append(('✓', f"{enc_count} encrypted passwords") if enc_count else ('✓', "No encrypted passwords"))

    # 5. Unencrypted passwords
    uenc_count = sum(
        1 for c in mgr.config.get('connections', {}).values()
        if 'password' in c and not(c.get('password', '').startswith('${'))
    ) + sum(
        1 for p in mgr.config.get('passwords', {}).values()
        if 'password' in p and not(p.get('password', '').startswith('${'))
    )
    results.append(("✗", f"{uenc_count} unencrypted passwords!") if uenc_count else ('✓', "No unencrypted passwords"))
    return results


def _valid_fernet(key: str) -> bool:
    try:
        Fernet(key.encode())
        return True
    except Exception:
        return False


class ConfigManager:
    """
    Manage DBTK configuration from YAML files.

    ConfigManager handles loading and parsing YAML configuration files that define
    database connections, encrypted passwords, and global settings. It searches for
    configuration files in standard locations, validates the structure, and provides
    methods for accessing connections and passwords.

    The manager supports encrypted passwords using Fernet symmetric encryption,
    environment variable substitution, and automatic sample config generation for
    new users.

    Configuration File Structure
    ----------------------------
    ::

        # dbtk.yml
        settings:
          default_timezone: UTC
          default_country: US
          default_paramstyle: named

        connections:
          my_db:
            type: postgres
            host: localhost
            database: myapp
            user: admin
            encrypted_password: gAAAAABh...

        passwords:
          api_key:
            encrypted_password: gAAAAABh...
            description: API key for external service

    Configuration Locations
    -----------------------
    ConfigManager searches for configuration files in this order:

    1. File specified in config_file parameter
    2. ``./dbtk.yml`` (current directory)
    3. ``./dbtk.yaml`` (current directory)
    4. ``~/.config/dbtk.yml`` (user config directory)
    5. ``~/.config/dbtk.yaml`` (user config directory)

    If no config is found, creates a sample config at ``~/.config/dbtk.yml``.

    Parameters
    ----------
    config_file : str or Path, optional
        Path to YAML config file. If None, searches standard locations.

    Attributes
    ----------
    config_file : Path
        Path to the loaded configuration file
    config : dict
        Parsed configuration dictionary

    Example
    -------
    ::

        from dbtk.config import ConfigManager

        # Load from default location
        config_mgr = ConfigManager()

        # Access connection settings
        conn_params = config_mgr.get_connection('production_db')

        # Get encrypted password
        api_key = config_mgr.get_password('external_api')

        # Load specific config file
        config_mgr = ConfigManager('/path/to/custom.yml')

    See Also
    --------
    dbtk.connect : Connect to database using config
    generate_encryption_key : Create encryption key for passwords
    encrypt_config_file : Encrypt passwords in config file

    Notes
    -----
    * YAML files must have .yml or .yaml extension
    * Connections require 'type' field (postgres, oracle, mysql, etc.)
    * Encrypted passwords require DBTK_ENCRYPTION_KEY environment variable
    * Environment variables can be used with ${VAR_NAME} syntax
    * Sample config is created at ~/.config/dbtk.yml on first run if no config exists
    """

    def __init__(self, config_file: Optional[str] = None):
        """
        Initialize config manager and load configuration.

        Parameters
        ----------
        config_file : str or Path, optional
            Path to YAML config file. If None, searches for:

            * ``dbtk.yml`` in current directory
            * ``dbtk.yaml`` in current directory
            * ``~/.config/dbtk.yml``
            * ``~/.config/dbtk.yaml``

        Raises
        ------
        FileNotFoundError
            If no config file found in any search location
        ValueError
            If config file is invalid or malformed

        Example
        -------
        ::

            # Use default config location
            config = ConfigManager()

            # Use specific config file
            config = ConfigManager('/etc/dbtk/production.yml')

            # Config auto-creates sample if none exists
            config = ConfigManager()  # Creates ~/.config/dbtk.yml if needed
        """
        self.config_file = self._find_config_file(config_file)
        self.config = self._load_config()
        self._fernet = None

        # Apply global settings
        self._apply_settings()

    def _find_config_file(self, config_file: Optional[str]) -> Path:
        """Find the configuration file."""
        if config_file:
            path = Path(config_file)
            if not path.exists():
                raise FileNotFoundError(f"Config file not found: {config_file}")
            return path

        # Look for default locations
        candidates = [
            Path("dbtk.yml"),
            Path("dbtk.yaml"),
            Path.home() / ".config" / "dbtk.yml",
            Path.home() / ".config" / "dbtk.yaml"
        ]

        for candidate in candidates:
            if candidate.exists():
                return candidate

        # No config found - try to create sample config
        _ensure_sample_config()

        # Check again after creating sample
        for candidate in candidates:
            if candidate.exists():
                return candidate

        raise FileNotFoundError(
            "No config file found. Looked in: " +
            ", ".join(str(c) for c in candidates)
        )

    def _load_config(self) -> Dict[str, Any]:
        """Load and validate configuration file."""
        try:
            with open(self.config_file, 'r') as f:
                config = yaml.safe_load(f) or {}
            if not isinstance(config, dict):
                raise ValueError(f"Invalid config file {self.config_file}.")

            # Validate connections section if it exists
            if 'connections' in config:
                for name, conn in config.get('connections', {}).items():
                    if not isinstance(conn, dict) or ('type' not in conn and 'driver' not in conn):
                        raise ValueError(f"Invalid connection '{name}' in {self.config_file}: 'type' or 'driver' is required")

            # Validate passwords section if it exists
            if 'passwords' in config:
                if not isinstance(config['passwords'], dict):
                    raise ValueError(f"Invalid config file {self.config_file}: 'passwords' must be a dictionary")
                for name, password_data in config['passwords'].items():
                    if not isinstance(password_data, dict):
                        raise ValueError(f"Invalid password entry '{name}' in {self.config_file}: must be a dictionary")
                    if 'password' not in password_data and 'encrypted_password' not in password_data:
                        raise ValueError(
                            f"Invalid password entry '{name}' in {self.config_file}: 'password' or 'encrypted_password' is required")

            # Validate settings section if it exists
            if 'settings' in config:
                if not isinstance(config['settings'], dict):
                    raise ValueError(f"Invalid config file {self.config_file}: 'settings' must be a dictionary")

            logger.info(f"Loaded config from {self.config_file}")
            return config
        except Exception as e:
            raise ValueError(f"Failed to load config file {self.config_file}: {e}")

    def _apply_settings(self) -> None:
        """Apply global settings from config."""
        global settings

        config_settings = self.config.get('settings', {})
        settings.update(config_settings)

        # Apply specific settings that need special handling
        default_tz = settings.get('default_timezone')
        if default_tz:
            try:
                from .etl.transforms.datetime import set_default_timezone
                set_default_timezone(default_tz)
                logger.info(f"Set default timezone to: {default_tz}")
            except ValueError as e:
                logger.warning(f"Failed to set default timezone '{default_tz}': {e}")


    def get_setting(self, key: str, default: Any = None) -> Any:
        """
        Get a setting value from the config.

        Args:
            key: Setting key (supports dot notation like 'database.timeout')
            default: Default value if key not found

        Returns:
            Setting value or default

        Example:
            timeout = config.get_setting('database.timeout', 30)
            tz = config.get_setting('default_timezone', 'UTC')
        """
        settings = self.config.get('settings', {})

        # Support dot notation for nested settings
        keys = key.split('.')
        value = settings

        for k in keys:
            if isinstance(value, dict) and k in value:
                value = value[k]
            else:
                return default

        return value

    def set_setting(self, key: str, value: Any) -> None:
        """
        Set a setting value and save config.

        Args:
            key: Setting key (supports dot notation)
            value: Setting value
        """
        settings = self.config.setdefault('settings', {})

        # Support dot notation for nested settings
        keys = key.split('.')
        current = settings

        for k in keys[:-1]:
            current = current.setdefault(k, {})

        current[keys[-1]] = value
        self._save_config()

        # Re-apply settings
        self._apply_settings()

    def _get_encryption_key(self) -> bytes:
        """Get encryption key from keyring or environment variable."""
        # environment variable takes precedence
        key_str = os.environ.get('DBTK_ENCRYPTION_KEY')
        if key_str:
            logger.debug("Using DBTK_ENCRYPTION_KEY from environment")
            return key_str.encode()

        if HAS_KEYRING:
            try:
                key_str = keyring.get_password('dbtk', 'encryption_key')
                if key_str:
                    logger.debug("Using encryption key from keyring")
                    return key_str.encode()
                else:
                    raise ValueError("Keyring entry exists but is empty")
            except Exception as e:
                logger.warning(f"Keyring access failed: {e}")
                raise ValueError(
                    "Encryption key not found in keyring and DBTK_ENCRYPTION_KEY not set.\n"
                    "Run: python -c \"import dbtk.config as c, keyring; "
                    "keyring.set_password('dbtk', 'encryption_key', c.generate_encryption_key())\""
                )
        if HAS_CRYPTO:
            if HAS_KEYRING:
                msg = dedent("""\
                Encryption key not found in environment or keyring.
                Run: `dbtk store-key` to generate and store a new encryption key in the keyring.
                """)
            else:
                msg = dedent("""\
                Encryption key not found in environment or keyring.
                Run `dbtk generate-key` to generate and a new encryption key
                then in the DBTK_ENCRYPTION_KEY environment variable.""")
            raise ValueError(msg)
        else:
            raise ValueError("Encryption not available. Install cryptography package to enable encryption.")

    def _get_fernet(self) -> Fernet:
        """Get or create Fernet instance for encryption/decryption."""
        if self._fernet is None:
            self._fernet = Fernet(self._get_encryption_key())
        return self._fernet

    def decrypt_password(self, encrypted_password: str) -> str:
        """Decrypt an encrypted password."""
        try:
            fernet = self._get_fernet()
            return fernet.decrypt(encrypted_password.encode()).decode()
        except Exception as e:
            raise ValueError(f"Failed to decrypt password: {e}")

    def encrypt_password(self, password: str) -> str:
        """Encrypt a password for storage."""
        try:
            fernet = self._get_fernet()
            return fernet.encrypt(password.encode()).decode()
        except Exception as e:
            raise ValueError(f"Failed to encrypt password: {e}")

    def get_connection_config(self, name: str) -> Dict[str, Any]:
        """Get configuration for a named connection."""
        connections = self.config.get('connections', {})

        if name not in connections:
            available = list(connections.keys())
            raise ValueError(
                f"Connection '{name}' not found in config. "
                f"Available connections: {available}"
            )

        config = connections[name].copy()

        # Handle password decryption
        if 'encrypted_password' in config:
            config['password'] = self.decrypt_password(config['encrypted_password'])
            del config['encrypted_password']

        # Handle environment variable substitution for password
        if 'password' in config and isinstance(config['password'], str):
            if config['password'].startswith('${') and config['password'].endswith('}'):
                env_var = config['password'][2:-1]
                config['password'] = os.environ.get(env_var)
                if config['password'] is None:
                    raise ValueError(f"Environment variable {env_var} not set")

        return config

    def list_connections(self) -> list:
        """List all available connection names."""
        return list(self.config.get('connections', {}).keys())

    def get_password(self, name: str) -> str:
        """
        Get a stored password by name.

        Args:
            name: Password name/key

        Returns:
            Decrypted password string

        Raises:
            ValueError: If password not found or decryption fails
        """
        passwords = self.config.get('passwords', {})

        if name not in passwords:
            available = list(passwords.keys())
            raise ValueError(
                f"Password '{name}' not found in config. "
                f"Available passwords: {available}"
            )

        password_entry = passwords[name]

        # Handle encrypted passwords
        if 'encrypted_password' in password_entry:
            return self.decrypt_password(password_entry['encrypted_password'])

        # Handle plain text passwords
        if 'password' in password_entry:
            password = password_entry['password']
            # Handle environment variable substitution
            if isinstance(password, str) and password.startswith('${') and password.endswith('}'):
                env_var = password[2:-1]
                env_password = os.environ.get(env_var)
                if env_password is None:
                    raise ValueError(f"Environment variable {env_var} not set")
                return env_password
            return password

        raise ValueError(f"Invalid password entry '{name}': no password or encrypted_password found")

    def list_passwords(self) -> list:
        """List all available password names."""
        return list(self.config.get('passwords', {}).keys())

    def add_password(self, name: str, password: str, description: str = None, encrypt: bool = True) -> None:
        """
        Add or update a password entry.

        Args:
            name: Password name/key
            password: Password value
            description: Optional description
            encrypt: Whether to encrypt the password (default: True)
        """
        passwords = self.config.setdefault('passwords', {})

        entry = {}
        if description:
            entry['description'] = description

        if encrypt:
            entry['encrypted_password'] = self.encrypt_password(password)
        else:
            entry['password'] = password

        passwords[name] = entry
        self._save_config()

        logger.info(f"Password '{name}' {'updated' if name in passwords else 'added'} successfully")

    def remove_password(self, name: str) -> None:
        """
        Remove a password entry.

        Args:
            name: Password name to remove

        Raises:
            ValueError: If password not found
        """
        passwords = self.config.get('passwords', {})

        if name not in passwords:
            available = list(passwords.keys())
            raise ValueError(
                f"Password '{name}' not found in config. "
                f"Available passwords: {available}"
            )

        del passwords[name]
        self._save_config()

        logger.info(f"Password '{name}' removed successfully")


    def _save_config(self) -> None:
        """Save config with consistent key ordering."""
        ordered_config = {}

        # Settings first
        if 'settings' in self.config:
            ordered_config['settings'] = self.config['settings']

        # Connections second, sorted by name
        if 'connections' in self.config:
            ordered_connections = {}
            connection_key_order = ['type', 'database', 'user', 'password', 'encrypted_password', 'host', 'port']

            for conn_name in sorted(self.config['connections'].keys()):
                connection = self.config['connections'][conn_name]
                ordered_connection = {}

                # Add keys in preferred order
                for key in connection_key_order:
                    if key in connection:
                        ordered_connection[key] = connection[key]

                # Add remaining keys alphabetically
                remaining_keys = sorted(set(connection.keys()) - set(connection_key_order))
                for key in remaining_keys:
                    ordered_connection[key] = connection[key]

                ordered_connections[conn_name] = ordered_connection

            ordered_config['connections'] = ordered_connections

        # Passwords last, sorted by name
        if 'passwords' in self.config:
            ordered_config['passwords'] = dict(sorted(self.config['passwords'].items()))

        with open(self.config_file, 'w') as f:
            yaml.safe_dump(ordered_config, f, default_flow_style=False, sort_keys=False)


# Global config manager instance
_config_manager: Optional[ConfigManager] = None


def store_key(key: Optional[str] = None, force: bool = False) -> None:
    """CLI utility to store encryption key in system keyring."""
    if not HAS_CRYPTO:
        raise ValueError("Encryption not available. Install cryptography package to enable encryption.")

    if not HAS_KEYRING:
        raise ValueError("Keyring not available. Install keyring package to store key in system keyring.")

    try:
        # check if we have a current key
        current_key = keyring.get_password("dbtk", "encryption_key")
    except Exception:
        current_key = None

    if current_key:
        if force:
            msg = "Encryption key already stored in system keyring. Overwriting!"
            logger.warning(msg)
            print(msg)
        else:
            msg = "Encryption key already stored in system keyring. Use --force to overwrite."
            logger.warning(msg)
            print(msg)
            return

    if key is None:
        key = _generate_encryption_key()
    else:
        # make sure passed in key is valid
        if not _valid_fernet(key):
           raise ValueError("Invalid encryption key. Must be 32 url-safe base64-encoded bytes.")

    try:
        keyring.set_password("dbtk", "encryption_key", key)
        msg = "Stored encryption key in system keyring"
        logger.info(msg)
        print(msg)
        return
    except Exception as e:
        msg = f"Failed to store encryption key in system keyring: {e}"
        logger.error(msg)
        raise ValueError(msg)


def _generate_encryption_key() -> str:
    """

    """
    return Fernet.generate_key().decode()


def generate_encryption_key() -> str:
    """
    Generate a random encryption key.

    This function generates a random encryption key that can be used to encrypt
    and decrypt data securely. The key is returned as a string and should be
    stored in the DBTK_ENCRYPTION_KEY environment variable
    or on keyring by calling `dbtk store-key [your key]`


    Returns:
        str: A randomly generated encryption key."""
    key = _generate_encryption_key()
    if HAS_KEYRING:
        msg = "Key generated.  Store in system keyring with `dbtk store-key [your key]`"
    else:
        msg = "Key generated.  Store in DBTK_ENCRYPTION_KEY environment variable"
    print(msg)
    return key


def set_config_file(config_file: str) -> None:
    """Set the configuration file to use globally."""
    global _config_manager
    _config_manager = ConfigManager(config_file)


def connect(name: str, password: str = None, config_file: Optional[str] = None) -> Database:
    """
    Connect to a named database from configuration.

    Args:
        name: Connection name from config file
        password: Optional password if not stored in config
        config_file: Optional path to config file

    Returns:
        Database connection instance

    Example:
        db = connect('prod_warehouse')
        cursor = db.cursor()
        cursor.execute("SELECT * FROM users")
    """
    global _config_manager

    # Use provided config file or global instance
    if config_file:
        config_mgr = ConfigManager(config_file)
    else:
        if _config_manager is None:
            _config_manager = ConfigManager()
        config_mgr = _config_manager

    config = config_mgr.get_connection_config(name)
    if password:
        config['password'] = password
    info = {key:val for key, val in config.items() if key != 'password'}
    logger.debug(f"Connecting to database {name} with config: {info}")

    # Extract database type
    db_type = config.pop('type', None)
    if not db_type:
        db_type = config.pop('database_type', 'postgres')
    # Extract driver if specified
    driver = config.pop('driver', None)
    cursor_settings = config.pop('cursor', None)
    if cursor_settings is not None:
        unknown = set(cursor_settings.keys()) - set(Cursor.WRAPPER_SETTINGS)
        if unknown:
            logger.warning(f"Unknown cursor settings (ignored): {unknown}")

    # remove any params that are not allowed for the database type
    allowed_params = _get_params_for_database(db_type)
    config = {key: val for key, val in config.items() if key in allowed_params}

    # Create database connection
    return Database.create(db_type, driver=driver, cursor_settings=cursor_settings, **config)


def get_password(name: str, config_file: Optional[str] = None) -> str:
    """
    Get a stored password from configuration.

    Args:
        name: Password name from config file
        config_file: Optional path to config file

    Returns:
        Decrypted password string

    Example:
        api_key = get_password('openai_api_key')
        secret = get_password('jwt_secret')
    """
    global _config_manager

    # Use provided config file or global instance
    if config_file:
        config_mgr = ConfigManager(config_file)
    else:
        if _config_manager is None:
            _config_manager = ConfigManager()
        config_mgr = _config_manager

    return config_mgr.get_password(name)


def get_setting(key: str, default: Any = None, config_file: Optional[str] = None) -> Any:
    """
    Get a setting value from configuration.

    Args:
        key: Setting key (supports dot notation like 'database.timeout')
        default: Default value if key not found
        config_file: Optional path to config file

    Returns:
        Setting value or default

    Example:
        timeout = get_setting('database.timeout', 30)
        tz = get_setting('default_timezone', 'UTC')
    """
    global _config_manager

    # Use provided config file or global instance
    if config_file:
        config_mgr = ConfigManager(config_file)
    else:
        if _config_manager is None:
            _config_manager = ConfigManager()
        config_mgr = _config_manager

    return config_mgr.get_setting(key, default)


def encrypt_password(password: str = None, encryption_key: str = None) -> str:
    """
    CLI utility function to encrypt a password.

    Args:
        password: Password to encrypt (if None, prompts for input)
        encryption_key: Optional encryption key. If None, uses DBTK_ENCRYPTION_KEY env var

    Returns:
        str: Encrypted password
    """
    if password is None:
        import getpass
        password = getpass.getpass("Enter password to encrypt: ")

    if encryption_key:
        # Use provided key
        fernet = Fernet(encryption_key.encode())
        encrypted = fernet.encrypt(password.encode()).decode()
    else:
        # Use DBTK_ENCRYPTION_KEY to encrypt
        temp_config = ConfigManager.__new__(ConfigManager)
        temp_config._fernet = None
        encrypted = temp_config.encrypt_password(password)

    print(encrypted)
    return encrypted


def encrypt_config_file(filename: str) -> None:
    """CLI Utility to encrypt all passwords in a config file."""
    temp_config = ConfigManager.__new__(ConfigManager)
    temp_config._fernet = None
    with open(filename) as fp:
        config = yaml.safe_load(fp)
    changes = 0
    if config:
        # Encrypt connection passwords
        for key, val in config.get('connections', {}).items():
            password = val.pop('password', None)
            if password:
                enc_password = temp_config.encrypt_password(password)
                if enc_password:
                    val['encrypted_password'] = enc_password
                    changes += 1
                else:
                    # encryption didn't work, put password back
                    val['password'] = password

        # Encrypt standalone passwords
        for key, val in config.get('passwords', {}).items():
            if 'password' in val and 'encrypted_password' not in val:
                password = val.pop('password')
                if password:
                    enc_password = temp_config.encrypt_password(password)
                    if enc_password:
                        val['encrypted_password'] = enc_password
                        changes += 1
                    else:
                        # encryption didn't work, put password back
                        val['password'] = password

        if changes > 0:
            with open(filename, 'w') as fp:
                yaml.safe_dump(config, fp, default_flow_style=False)
            print(f"Encrypted {changes} passwords in {filename}")
        else:
            print(f"No passwords to encrypt in {filename}")


def migrate_config(source_file: str, target_file: str, new_encryption_key: str) -> None:
    """Migrate config file with new encryption key."""
    from copy import deepcopy
    source_config_mgr = ConfigManager(source_file)
    new_config = deepcopy(source_config_mgr.config)

    # Re-encrypt all passwords
    for conn_name, conn_config in new_config.get('connections', {}).items():
        if 'encrypted_password' in conn_config:
            password = source_config_mgr.decrypt_password(conn_config['encrypted_password'])
            conn_config['encrypted_password'] = encrypt_password(password, new_encryption_key)

    for pwd_name, pwd_config in new_config.get('passwords', {}).items():
        if 'encrypted_password' in pwd_config:
            password = source_config_mgr.decrypt_password(pwd_config['encrypted_password'])
            pwd_config['encrypted_password'] = encrypt_password(password, new_encryption_key)

    with open(target_file, 'w') as f:
        yaml.safe_dump(new_config, f, default_flow_style=False)

def setup_config(interactive: bool = False, location: Optional[str] = None, example: Optional[str] = None) -> None:
    """
    Initialize DBTK configuration file.
    
    Creates a configuration file with optional example connections. More user-friendly
    than the automatic config creation that happens on first connect().
    
    Args:
        interactive: If True, run interactive setup wizard
        location: Where to create config ('project' or 'user'). If None, prompts or defaults to 'user'
        example: Database type to include as example connection (postgres, oracle, mysql, sqlserver, sqlite)
    
    Example:
        # Basic setup
        dbtk config-setup
        
        # Interactive wizard  
        dbtk config-setup -i
        
        # Create with Postgres example
        dbtk config-setup --example postgres --location user
    """
    import getpass
    
    # Determine config file location
    if interactive and not location:
        print("Where should I create the config file?")
        print("  1. ./dbtk.yml (this project only)")
        print("  2. ~/.config/dbtk.yml (all your projects) [default]")
        choice = input("Choice [2]: ").strip()
        location = 'project' if choice == '1' else 'user'
    elif not location:
        location = 'user'
    
    if location == 'project':
        config_path = Path('dbtk.yml')
    else:
        config_path = Path.home() / '.config' / 'dbtk.yml'
        config_path.parent.mkdir(parents=True, exist_ok=True)
    
    # Check if config already exists
    if config_path.exists():
        print(f"⚠ Config file already exists at {config_path}")
        overwrite = input("Overwrite? [y/N]: ").strip().lower()
        if overwrite not in ('y', 'yes'):
            print("Cancelled.")
            return
    
    # Start with basic config structure
    config_data = {
        'settings': {},
        'connections': {},
        'passwords': {}
    }
    
    # Interactive mode - walk through connection setup
    if interactive:
        print("\n" + "="*60)
        print("DBTK Configuration Setup")
        print("="*60)
        
        add_connection = input("\nAdd a database connection? [y/N]: ").strip().lower()
        if add_connection in ('y', 'yes'):
            conn_name = input("Connection name: ").strip() or 'my_database'
            
            db_types = ['postgres', 'oracle', 'mysql', 'sqlserver', 'sqlite']
            print(f"Database type: {', '.join(db_types)}")
            db_type = input(f"Type [postgres]: ").strip().lower() or 'postgres'
            
            if db_type not in db_types:
                print(f"Unknown type '{db_type}', using 'postgres'")
                db_type = 'postgres'
            
            conn_config = {'type': db_type}
            
            if db_type != 'sqlite':
                conn_config['host'] = input("Host [localhost]: ").strip() or 'localhost'
                
                default_ports = {
                    'postgres': 5432,
                    'oracle': 1521,
                    'mysql': 3306,
                    'sqlserver': 1433
                }
                default_port = default_ports.get(db_type, 5432)
                port_input = input(f"Port [{default_port}]: ").strip()
                conn_config['port'] = int(port_input) if port_input else default_port
                
                conn_config['database'] = input("Database name: ").strip() or 'mydb'
                conn_config['user'] = input("Username: ").strip() or 'admin'
                
                password = getpass.getpass("Password (leave empty to skip): ")
                if password:
                    encrypt = input("Encrypt this password? [Y/n]: ").strip().lower()
                    if encrypt in ('', 'y', 'yes'):
                        # Generate and store key
                        try:
                            key = generate_encryption_key()
                            if HAS_KEYRING:
                                store_key(key)
                                print("✓ Generated encryption key and stored in system keyring")
                            else:
                                print(f"✓ Generated encryption key (add to DBTK_ENCRYPTION_KEY env var):")
                                print(f"  export DBTK_ENCRYPTION_KEY='{key}'")
                            
                            # Encrypt password
                            encrypted = encrypt_password(password, key)
                            conn_config['encrypted_password'] = encrypted
                        except Exception as e:
                            print(f"⚠ Encryption failed: {e}")
                            print("Storing password in plaintext (not recommended for production)")
                            conn_config['password'] = password
                    else:
                        conn_config['password'] = password
            else:
                # SQLite
                db_path = input("Database file path: ").strip() or './data.db'
                conn_config['database'] = db_path
            
            config_data['connections'][conn_name] = conn_config
    
    # Non-interactive with example
    elif example:
        example_configs = {
            'postgres': {
                'type': 'postgres',
                'host': 'localhost',
                'port': 5432,
                'database': 'mydb',
                'user': 'postgres',
                'password': 'changeme'
            },
            'oracle': {
                'type': 'oracle',
                'host': 'localhost',
                'port': 1521,
                'database': 'ORCL',
                'user': 'system',
                'password': 'changeme'
            },
            'mysql': {
                'type': 'mysql',
                'host': 'localhost',
                'port': 3306,
                'database': 'mydb',
                'user': 'root',
                'password': 'changeme'
            },
            'sqlserver': {
                'type': 'sqlserver',
                'host': 'localhost',
                'port': 1433,
                'database': 'master',
                'user': 'sa',
                'password': 'changeme'
            },
            'sqlite': {
                'type': 'sqlite',
                'database': './data.db'
            }
        }
        config_data['connections'][f'my_{example}'] = example_configs[example]
    
    # Write config file
    with open(config_path, 'w') as f:
        yaml.safe_dump(config_data, f, default_flow_style=False, sort_keys=False)
    
    print(f"\n✓ Created config at {config_path}")
    
    # Show next steps
    if config_data['connections']:
        conn_names = list(config_data['connections'].keys())
        has_plaintext = any('password' in c for c in config_data['connections'].values())
        
        print("\nNext steps:")
        if has_plaintext:
            print(f"  1. Edit {config_path} to update connection details")
            print(f"  2. Encrypt passwords: dbtk encrypt-config")
            print(f"  3. Test connection: python -c \"import dbtk; db = dbtk.connect('{conn_names[0]}')\"")
        else:
            print(f"  1. Edit {config_path} to update connection details")
            print(f"  2. Test connection: python -c \"import dbtk; db = dbtk.connect('{conn_names[0]}')\"")
    else:
        print("\nTo add a connection, edit the config file:")
        print(f"  {config_path}")
        print("\nOr run: dbtk config-setup -i")
