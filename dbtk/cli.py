# dbtk/cli.py

import argparse
import importlib.util
import sys
from pathlib import Path
from .database import _get_all_drivers
from . import config

try:
    from importlib.metadata import metadata, requires
except ImportError:
    from importlib_metadata import metadata, requires


def _name_cleanup(name):
    """Cleanup module names for search and display"""
    return name.lower().replace('-', '_')


def _get_optional_deps(extra_name='recommended'):
    """ Get optional dependencies for dbtk """
    reqs = requires('dbtk') or []
    deps = []
    # Parse requirements like: 'psycopg2-binary>=2.8; extra == "recommended"'
    for req in reqs:
        req = req.replace("'", '"') # quoting changed between versions
        if f'extra == "{extra_name}"' in req:
            # Extract just the package name and version spec
            pkg = req.split(';')[0].strip()
            deps.append(pkg)
    return deps


def _is_installed(pkg: str) -> bool:
    """find_spec fails on tomli → this never does."""
    pkg = _name_cleanup(pkg)
    return (
        importlib.util.find_spec(pkg) is not None
        or pkg in sys.modules
        or pkg in {_name_cleanup(d.name) for d in importlib.metadata.distributions()}
    )

def checkup():
    """ Check which optional dependencies are installed."""
    deps = []
    for dep in _get_optional_deps('recommended'):
        if ';' in dep:
            dep = dep.split(';')[0].strip()
        if dep and not dep.startswith('#'):
            deps.append(dep.split('>=')[0].split('==')[0].split('<')[0].strip())

    installed = {_name_cleanup(d.name): d.version for d in metadata.distributions()}

    print(f"{'Package':<20} {'Status':<8} {'Version'}")
    print("-" * 40)

    for dep in deps:
        clean = dep.replace('-', '_')
        status = "✓" if _is_installed(clean) else "✗"
        version = installed.get(clean.lower(), '-')
        print(f"{dep:<20} {status:<8} {version}")

    print("\nDB Drivers           Priority* Status   Version")
    print("-" * 56)
    all_drivers = _get_all_drivers()
    by_type = {}

    for name, info in all_drivers.items():
        db_type = info['database_type']
        by_type.setdefault(db_type, []).append((info['priority'], name, info))

    if _is_installed('pyodbc'):
        import pyodbc
        odbc_drivers = pyodbc.drivers()
    else:
        odbc_drivers = []

    for db_type in sorted(by_type):
        drivers = sorted(by_type[db_type], key=lambda x: x[0])
        print(f"{db_type}")  # ← bold header
        for pri, name, info in drivers:
            # ── 2-space indent for hierarchy
            display_name = f"  {name}"  # ← indented

            try:
                # see if driver has 'module' attribute to use instead of name
                module_name = info.get('module', name)
                spec = importlib.util.find_spec(module_name)
                version = installed.get(_name_cleanup(module_name), '--')
                status = "✓" if spec else "✗"
            except ModuleNotFoundError:
                version = '--'
                status = "✗"
            odbc_driver_name = info.get("odbc_driver_name")
            if odbc_driver_name:
                odbc_status = "✓" if odbc_driver_name in odbc_drivers else "✗"
                note = f'({odbc_status} {odbc_driver_name})'
            else:
                note = ''

            print(f"{display_name:<20} {pri:<9} {status:<8} {version} {note}")

    print("\n* Lower priority = preferred")

    print("\nConfig Health")
    print("-" * 40)
    for status, msg in config.diagnose_config():
        print(f"{status} {msg}")


def main():
    parser = argparse.ArgumentParser(prog='dbtk', description='DBTK command-line utilities')
    subparsers = parser.add_subparsers(dest='command', required=True)

    # checkup
    subparsers.add_parser('checkup', help='Check for dependencies and configuration issues')

    # config-setup
    setup_parser = subparsers.add_parser('config-setup', help='Initialize DBTK configuration')
    setup_parser.add_argument('--interactive', '-i', action='store_true',
                             help='Interactive setup wizard')
    setup_parser.add_argument('--location', choices=['project', 'user'],
                             help="Config location: 'project' (./dbtk.yml) or 'user' (~/.config/dbtk.yml)")
    setup_parser.add_argument('--example',
                             choices=['postgres', 'oracle', 'mysql', 'sqlserver', 'sqlite'],
                             help='Include example connection for database type')

    # generate-key
    subparsers.add_parser('generate-key', help='Generate encryption key')

    # store-key
    key_parser = subparsers.add_parser('store-key',
                                       help='Store encryption key in system keyring (generate if not provided)')
    key_parser.add_argument('key', nargs='?', default=None,
                            help='Encryption key to store. If omitted, a new key is generated and stored.')
    key_parser.add_argument('--force', action='store_true',
                            help='Overwrite existing encryption key in system keyring')

    # encrypt-config
    encrypt_parser = subparsers.add_parser('encrypt-config', help='Encrypt passwords in config file')
    encrypt_parser.add_argument('config_file', nargs='?', help='Config file path')

    # encrypt-password
    pwd_parser = subparsers.add_parser('encrypt-password', help='Encrypt a password')
    pwd_parser.add_argument('password', nargs='?', help='Password to encrypt')

    # migrate-config
    migrate_parser = subparsers.add_parser('migrate-config', help='Migrate config to new key')
    migrate_parser.add_argument('old_file', help='Old config file')
    migrate_parser.add_argument('new_file', help='New config file')
    migrate_parser.add_argument('--new-key', help='New encryption key')

    args = parser.parse_args()

    if args.command == 'checkup':
        return checkup()
    elif args.command == 'config-setup':
        return config.setup_config(
            interactive=args.interactive,
            location=args.location,
            example=args.example
        )
    elif args.command == 'generate-key':
        return config.generate_encryption_key()
    elif args.command == 'store-key':
        return config.store_key(args.key)
    elif args.command == 'encrypt-config':
        return config.encrypt_config_file(args.config_file)
    elif args.command == 'encrypt-password':
        return config.encrypt_password(args.password)
    elif args.command == 'migrate-config':
        return config.migrate_config(args.old_file, args.new_file, args.new_key)


if __name__ == '__main__':
    main()