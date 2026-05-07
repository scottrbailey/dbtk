# dbtk/etl/config_generators.py
"""
Generate column definitions for Table class from database schema.

Extracts table metadata and formats it as Python dictionary code
that can be copied into Table() constructor calls.
"""

from typing import Dict, Any


def column_defs_from_db(cursor, table_name: str, add_comments: bool = False) -> str:
    """
    Generate column definitions from database table schema.

    Inspects the database table structure and returns a Python dictionary
    string representation of column configurations ready to use with the
    Table class.

    Args:
        cursor: Database cursor (any cursor type)
        table_name: Name of table to analyze (supports schema.table format)
        add_comments: Include table/column comments from database metadata

    Returns:
        String containing Python dict of column definitions

    Example:
        >>> print(column_defs_from_db(cursor, 'users'))
        {
            'id': {'field': 'id', 'primary_key': True},
            'name': {'field': 'name', 'nullable': False},
            'email': {'field': 'email'},
            'created_at': {'db_fn': 'CURRENT_TIMESTAMP'}
        }

        >>> # Copy output into your code:
        >>> table = Table('users', columns={
        ...     'id': {'field': 'id', 'primary_key': True},
        ...     'name': {'field': 'name', 'nullable': False},
        ...     'email': {'field': 'email'},
        ...     'created_at': {'db_fn': 'CURRENT_TIMESTAMP'}
        ... }, cursor=cursor)
    """
    metadata = cursor.connection.dialect.table_metadata(cursor, table_name, add_comments)
    return _format_columns_dict(metadata, add_comments)


def _format_columns_dict(metadata: Dict[str, Any], add_comments: bool) -> str:
    """
    Format metadata dictionary as Python code string.

    Args:
        metadata: Dict with 'columns', 'table_comment', 'column_comments' keys
        add_comments: Whether to include comments in output

    Returns:
        Formatted Python dictionary string
    """
    lines = []

    # Add table comment if present
    if add_comments and metadata.get('table_comment'):
        lines.append(f"# {metadata['table_comment']}")

    lines.append("{")

    for col_name, col_def in metadata['columns'].items():
        # Add column comment if present
        if add_comments and col_name in metadata.get('column_comments', {}):
            comment = metadata['column_comments'][col_name]
            lines.append(f"    # {comment}")

        # Build column definition
        parts = []
        for key, value in col_def.items():
            if isinstance(value, str):
                parts.append(f"'{key}': '{value}'")
            elif isinstance(value, bool):
                parts.append(f"'{key}': {value}")
            else:
                parts.append(f"'{key}': {value}")

        col_str = "{" + ", ".join(parts) + "}"
        lines.append(f"    '{col_name}': {col_str},")

    lines.append("}")

    return "\n".join(lines)
