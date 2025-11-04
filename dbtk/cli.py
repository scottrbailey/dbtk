# dbtk/cli.py

import argparse
from dbtk import config


def main():
    parser = argparse.ArgumentParser(prog='dbtk', description='DBTK command-line utilities')
    subparsers = parser.add_subparsers(dest='command', required=True)

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
    elif args.command == 'encrypt-config':
        config.encrypt_config_file_cli(args.config_file)
    elif args.command == 'encrypt-password':
        return config.encrypt_password_cli(args.password)
    elif args.command == 'migrate-config':
        config.migrate_config_cli(args.old_file, args.new_file, args.new_key)


if __name__ == '__main__':
    main()