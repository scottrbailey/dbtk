# dbtk/cli.py

import argparse
import importlib.util
import importlib.metadata
import sys
from pathlib import Path
from .database import _get_all_drivers
from . import config
from .config import diagnose_config

def _load_toml():
    if sys.version_info >= (3, 11):
        import tomllib
        return tomllib.load
    try:
        import tomli
        return tomli.load
    except ImportError:
        print("tomli not installed. Run: pip install \"dbtk[all]\"")
        return None


def _is_installed(pkg: str) -> bool:
    """find_spec fails on tomli → this never does."""
    pkg = pkg.replace('-', '_')
    return (
        importlib.util.find_spec(pkg) is not None
        or pkg in sys.modules
        or pkg in {d.name.lower().replace('-', '_') for d in importlib.metadata.distributions()}
    )

def checkup():
    load_toml = _load_toml()
    if load_toml is None:
        return

    toml_path = Path('pyproject.toml')
    if not toml_path.exists():
        print("pyproject.toml not found")
        return

    with open(toml_path, 'rb') as f:
        toml = load_toml(f)

    # ← EXTRACT RAW DEP STRINGS — NO PARSING
    raw_deps = toml.get('project', {}) \
        .get('optional-dependencies', {}) \
        .get('all', [])

    # ← FILTER: only pure package names (no markers)
    deps = []
    for dep in raw_deps:
        if ';' in dep:
            dep = dep.split(';')[0].strip()
        if dep and not dep.startswith('#'):
            if dep.startswith('tomli') and sys.version_info >= (3, 11):
                continue
            deps.append(dep.split('>=')[0].split('==')[0].split('<')[0].strip())

    installed = {d.name.lower().replace('-', '_'): d.version
                 for d in importlib.metadata.distributions()}

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

    for db_type in sorted(by_type):
        drivers = sorted(by_type[db_type], key=lambda x: x[0])
        print(f"\n{db_type}")  # ← bold header
        for pri, name, info in drivers:
            # ── TWEAK 1: 2-space indent for hierarchy
            display_name = f"  {name}"  # ← indented

            # ── TWEAK 2: pyodbc_* checks 'pyodbc' only
            check_name = 'pyodbc' if name.startswith('pyodbc_') else name
            spec = importlib.util.find_spec(check_name)
            version = installed.get(check_name.lower().replace('-', '_'), '-')
            status = "✓" if spec else "✗"

            note = f'({info.get("odbc_driver_name")})' if info.get('odbc_driver_name') else ''

            print(f"{display_name:<20} {pri:<8} {status:<8} {version} {note}")

    print("\n* Lower priority = preferred")

    print("\nConfig Health")
    print("-" * 40)
    for status, msg in diagnose_config():
        print(f"{status} {msg}")


def main():
    parser = argparse.ArgumentParser(prog='dbtk', description='DBTK command-line utilities')
    subparsers = parser.add_subparsers(dest='command', required=True)

    # checkup
    subparsers.add_parser('checkup', help='Check for dependencies and configuration issues')

    # generate-key
    subparsers.add_parser('generate-key', help='Generate encryption key')

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

    if args.command == 'generate-key':
        return config.generate_encryption_key()
    elif args.command == 'checkup':
        checkup()
    elif args.command == 'encrypt-config':
        config.encrypt_config_file_cli(args.config_file)
    elif args.command == 'encrypt-password':
        return config.encrypt_password_cli(args.password)
    elif args.command == 'migrate-config':
        config.migrate_config_cli(args.old_file, args.new_file, args.new_key)


if __name__ == '__main__':
    main()