# dbtk/cli.py

import argparse
import importlib.util
import sys
from .database import _get_all_drivers
from .record import fixed_record_factory
from . import config

try:
    from importlib.metadata import metadata, distributions, requires
except ImportError:
    # backport to older versions
    from importlib_metadata import metadata, distributions, requires


def _name_cleanup(name):
    """Cleanup module names for search and display"""
    return name.lower().replace('-', '_')


def _version_tuple(v):
    return tuple(int(x) for x in v.split('.'))


def _marker_passes(marker_str: str) -> bool:
    """Evaluate python_version constraints in a PEP 508 marker string."""
    import re
    py = _version_tuple(f"{sys.version_info.major}.{sys.version_info.minor}")
    ops = {'>=': py.__ge__, '>': py.__gt__, '<=': py.__le__,
           '<': py.__lt__, '==': py.__eq__, '!=': py.__ne__}
    for m in re.finditer(r'python_version\s*([><=!]+)\s*"([^"]+)"', marker_str):
        op, ver = m.group(1), m.group(2)
        if op in ops and not ops[op](_version_tuple(ver)):
            return False
    return True


def _get_optional_deps(extra_name='recommended'):
    """Get optional dependencies for dbtk that apply to the current Python version."""
    import re
    reqs = requires('dbtk') or []
    deps = []
    seen = set()
    for req in reqs:
        req = req.replace("'", '"')
        if f'extra == "{extra_name}"' not in req:
            continue
        marker_str = req.split(';', 1)[1].strip() if ';' in req else ''
        if marker_str and not _marker_passes(marker_str):
            continue
        pkg = req.split(';')[0].strip()
        base = re.split(r'[><=!]', pkg)[0].strip().lower()
        if base not in seen:
            seen.add(base)
            deps.append(pkg)
    return deps


def _is_installed(pkg: str) -> bool:
    """find_spec fails on tomli → this never does."""
    pkg = _name_cleanup(pkg)
    return (
        importlib.util.find_spec(pkg) is not None
        or pkg in sys.modules
        or pkg in {_name_cleanup(d.name) for d in distributions()}
    )

def checkup():
    """ Check which optional dependencies are installed."""
    deps = []
    for dep in _get_optional_deps('recommended'):
        if dep and not dep.startswith('#'):
            deps.append(dep.split('>=')[0].split('==')[0].split('<')[0].strip())

    installed = {_name_cleanup(d.name): d.version for d in distributions()}
    package_rec = fixed_record_factory([('Package', 22), ('Status', 8), ('Version', 15)])
    driver_rec = fixed_record_factory([('DB Driver', 22), ('Priority*', 10),
                                      ('Status', 9), ('Version', 8), ('Notes', 41)])
    row = package_rec(*package_rec._fields)
    print(row.to_line(True))
    print("-" * row._line_len)

    for dep in deps:
        clean = dep.replace('-', '_')
        status = "✓" if _is_installed(clean) else "✗"
        version = installed.get(clean.lower(), '-')
        row = package_rec(dep, status, version)
        print(row.to_line(True))
    print("")
    row = driver_rec(*driver_rec._fields)
    print(row.to_line(True))
    print("-" * 80)
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
            try:
                # see if driver has 'module' attribute to use instead of name
                module_name = info.get('module', name)
                spec = importlib.util.find_spec(module_name)
                version = installed.get(_name_cleanup(module_name), '--    ')
                status = "✓" if spec else "✗"
            except ModuleNotFoundError:
                version = '--'
                status = "✗"
            odbc_driver_name = info.get("odbc_driver_name")
            if odbc_driver_name:
                odbc_status = "✓" if odbc_driver_name in odbc_drivers else "✗"
                note = f'({odbc_status} {odbc_driver_name})'
            else:
                if  'note' in info:
                    note = f'({info.get("note")})'
                else:
                    note = ''
            row = driver_rec(f'  {name}', pri, status, version, note)
            print(row.to_line(True))

    print("\n* Lower priority = preferred")

    print("\nConfig Health")
    print("-" * 40)
    for status, msg in config.diagnose_config():
        print(f"{status} {msg}")


def main():
    parser = argparse.ArgumentParser(prog='dbtk', description='DBTK command-line utilities')
    subparsers = parser.add_subparsers(dest='command')
    subparsers.required = True  # required= kwarg not supported in Python 3.6

    # checkup
    subparsers.add_parser('checkup', help='Check for dependencies and configuration issues')

    # config-setup
    subparsers.add_parser('config-setup', help='Interactive configuration setup wizard')

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
        return config.setup_config()
    elif args.command == 'generate-key':
        key = config.generate_encryption_key()
        print(key)
        return
    elif args.command == 'store-key':
        return config.store_key(args.key, force=args.force)
    elif args.command == 'encrypt-config':
        return config.encrypt_config_file(args.config_file)
    elif args.command == 'encrypt-password':
        key = config.encrypt_password(args.password)
        print(key)
        return
    elif args.command == 'migrate-config':
        return config.migrate_config(args.old_file, args.new_file, args.new_key)


if __name__ == '__main__':
    main()