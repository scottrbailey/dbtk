# dbtk/formats/__init__.py
"""
Pre-defined schemas and format metadata for fixed-width EDI-like file formats.

Schemas are format-neutral — suitable for both EDIReader and EDIWriter.

Example
-------
::
    from dbtk.formats.edi import ACH_COLUMNS
    from dbtk.readers.fixed_width import EDIReader
    from dbtk.writers.fixed_width import EDIWriter

    with open('in.ach') as fp, EDIWriter('out.ach', ACH_COLUMNS) as w:
        w.write_batch(EDIReader(fp, ACH_COLUMNS))
"""

from .edi import ACH_COLUMNS, COBOL_BANK_EXTRACT_COLUMNS, X12_835_COLUMNS, FORMATS
