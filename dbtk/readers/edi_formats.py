# dbtk/readers/edi_formats.py
"""
Pre-defined column layouts for common multi-record fixed-width EDI-like formats.

Use with EDIReader:

    from dbtk.readers.fixed_width import EDIReader, FixedColumn
    from dbtk.readers.edi_formats import ACH_COLUMNS

    reader = EDIReader(
        fp=open('ach_file.ach'),
        columns=ACH_COLUMNS,
    )
"""

from .fixed_width import FixedColumn

# ───────────────────────────────────────────────
# NACHA ACH (US Automated Clearing House)
# Record length: 94 characters
# ───────────────────────────────────────────────
ACH_COLUMNS = {
    '1': [  # File Header Record
        FixedColumn('record_type_code',          1,   1,   comment='Always "1" (File Header)'),
        FixedColumn('priority_code',             2,   3,   comment='Usually "01" (high priority)'),
        FixedColumn('immediate_destination',     4,  13,   alignment='right', pad_char='0', comment='10-digit routing number of destination bank (leading space or zero)'),
        FixedColumn('immediate_origin',         14,  23,   alignment='right', pad_char='0', comment='10-digit originator ID (usually "1" + tax ID)'),
        FixedColumn('file_creation_date',       24,  29,   comment='YYMMDD format (date file was created)'),
        FixedColumn('file_creation_time',       30,  33,   comment='HHMM 24-hour format (time file was created)'),
        FixedColumn('file_id_modifier',         34,  34,   comment='A-Z or 0-9 to make file unique on same day'),
        FixedColumn('record_size',              35,  37,   comment='Always "094" (bytes per record)'),
        FixedColumn('blocking_factor',          38,  39,   comment='Always "10" (records per block)'),
        FixedColumn('format_code',              40,  40,   comment='Always "1"'),
        FixedColumn('immediate_destination_name',41, 63,   comment='Destination bank name (left-justified, space-padded)'),
        FixedColumn('immediate_origin_name',    64,  86,   comment='Originator/company name (left-justified, space-padded)'),
        FixedColumn('reference_code',           87,  94,   comment='Optional reference or spaces'),
    ],
    '5': [  # Batch Header Record
        FixedColumn('record_type_code',          1,   1,   comment='Always "5" (Batch Header)'),
        FixedColumn('service_class_code',        2,   4,   comment='200=credits only, 220=debits only, 225=mixed credits/debits'),
        FixedColumn('company_name',              5,  40,   comment='Company/originator name (left-justified, space-padded)'),
        FixedColumn('company_discretionary_data',41, 54,   comment='Optional company-defined data (left-justified)'),
        FixedColumn('company_identification',   55,  64,   alignment='right', pad_char='0', comment='Company ID (usually "1" + 9-digit tax ID)'),
        FixedColumn('standard_entry_class_code',65,  67,   comment='SEC code (e.g., PPD=direct deposit, CCD=corporate credit, WEB=internet-initiated)'),
        FixedColumn('company_entry_description',68,  87,   comment='Description of entry (e.g., "PAYROLL", "VENDOR PMT")'),
        FixedColumn('company_descriptive_date', 88,  94,   comment='Optional YYMMDD or descriptive text'),
        FixedColumn('effective_entry_date',     95, 100,   comment='Effective date of entries YYMMDD'),
        FixedColumn('settlement_date',         101, 103,   comment='Julian day of settlement (optional)'),
        FixedColumn('originator_status_code',  104, 104,   comment='Always "1" (ACH operator)'),
        FixedColumn('originating_dfi_id',      105, 113,   alignment='right', pad_char='0', comment='Originating bank routing number (9 digits)'),
        FixedColumn('batch_number',            114, 120,   alignment='right', pad_char='0', comment='Batch number (sequential per file)'),
    ],
    '6': [  # Entry Detail Record
        FixedColumn('record_type_code',          1,   1,   comment='Always "6" (Entry Detail)'),
        FixedColumn('transaction_code',          2,   3,   comment='e.g., 22=checking credit, 27=checking debit, 32=savings credit, 37=savings debit'),
        FixedColumn('receiving_dfi_id',          4,  11,   alignment='right', pad_char='0', comment='Receiving bank routing number (8 digits)'),
        FixedColumn('check_digit',              12,  12,   comment='Check digit for receiving DFI ID'),
        FixedColumn('dfi_account_number',       13,  29,   comment='Receiving account number (left-justified, space-padded)'),
        FixedColumn('amount',                   30,  39,   alignment='right', pad_char='0', comment='Amount in cents (right-justified, zero-padded, implied decimal)'),
        FixedColumn('individual_id_number',     40,  54,   comment='Individual identification number'),
        FixedColumn('individual_name',          55,  76,   comment='Receiving individual/company name (left-justified)'),
        FixedColumn('discretionary_data',       77,  78,   comment='Optional company use'),
        FixedColumn('addenda_indicator',        79,  79,   comment='"0"=no addenda, "1"=addenda follows'),
        FixedColumn('trace_number',             80,  94,   alignment='right', pad_char='0', comment='Trace number (originating DFI + batch + entry seq)'),
    ],
    '7': [  # Addenda Record (same layout for all addenda types)
        FixedColumn('record_type_code',          1,   1,   comment='Always "7" (Addenda)'),
        FixedColumn('addenda_type_code',         2,   3,   comment='Usually "05" for payment-related info'),
        FixedColumn('payment_related_info',      4,  83,   comment='Free-form payment-related information (left-justified)'),
        FixedColumn('addenda_sequence_number',  84,  87,   alignment='right', pad_char='0', comment='Sequence number within entry'),
        FixedColumn('entry_detail_sequence_number',88, 94,   alignment='right', pad_char='0', comment='Sequence number of related entry detail record'),
    ],
    '8': [  # Batch Control Record
        FixedColumn('record_type_code',          1,   1,   comment='Always "8" (Batch Control)'),
        FixedColumn('service_class_code',        2,   4,   comment='Same as batch header (200/220/225)'),
        FixedColumn('entry_addenda_count',       5,  10,   alignment='right', pad_char='0', comment='Total entry and addenda records in batch'),
        FixedColumn('entry_hash',               11,  20,   alignment='right', pad_char='0', comment='Sum of receiving DFI IDs (right 8 digits, modulo 10^10)'),
        FixedColumn('total_debit',              21,  40,   alignment='right', pad_char='0', comment='Total debit amount in cents (implied decimal)'),
        FixedColumn('total_credit',             41,  60,   alignment='right', pad_char='0', comment='Total credit amount in cents (implied decimal)'),
        FixedColumn('company_identification',   61,  70,   alignment='right', pad_char='0', comment='Same as batch header company ID'),
        FixedColumn('message_authentication_code',71, 80,   comment='MAC for authentication (optional)'),
        FixedColumn('originating_dfi_id',       81,  89,   alignment='right', pad_char='0', comment='Originating DFI routing number'),
        FixedColumn('batch_number',             90,  94,   alignment='right', pad_char='0', comment='Same as batch header batch number'),
    ],
    '9': [  # File Control Record
        FixedColumn('record_type_code',          1,   1,   comment='Always "9" (File Control)'),
        FixedColumn('batch_count',               2,   7,   alignment='right', pad_char='0', comment='Total number of batches in file'),
        FixedColumn('block_count',               8,  13,   alignment='right', pad_char='0', comment='Total number of 10-record blocks (including padding)'),
        FixedColumn('entry_addenda_count',      14,  21,   alignment='right', pad_char='0', comment='Total entry and addenda records in file'),
        FixedColumn('entry_hash',               22,  41,   alignment='right', pad_char='0', comment='Sum of all receiving DFI IDs (right 8 digits, modulo 10^10)'),
        FixedColumn('total_debit',              42,  61,   alignment='right', pad_char='0', comment='Total debit amount in cents (implied decimal)'),
        FixedColumn('total_credit',             62,  81,   alignment='right', pad_char='0', comment='Total credit amount in cents (implied decimal)'),
        FixedColumn('reserved',                 82,  94,   comment='Reserved (spaces)'),
    ],
}

# ───────────────────────────────────────────────
# COBOL Copybook-style Mainframe Banking Extract (example)
# Typical 200-byte record layout from legacy systems
# ───────────────────────────────────────────────
COBOL_BANK_EXTRACT_COLUMNS = {
    'HD': [  # Header Record
        FixedColumn('record_type',   1,   2, comment='Always "HD"'),
        FixedColumn('file_date',     3,  10, comment='YYYYMMDD file creation date'),
        FixedColumn('bank_id',      11,  20, comment='Bank/routing identifier'),
        FixedColumn('run_id',       21,  30, comment='Batch/run identifier'),
        FixedColumn('filler',       31, 200, comment='Reserved/spaces'),
    ],
    'DT': [  # Detail Record (account-level)
        FixedColumn('record_type',   1,   2, comment='Always "DT"'),
        FixedColumn('account_number',3,  22, alignment='right', pad_char='0', comment='Account number (left-padded)'),
        FixedColumn('customer_name',23,  62, comment='Customer full name (left-justified)'),
        FixedColumn('current_balance',63,  80, alignment='right', pad_char='0', comment='Current balance (implied decimal, e.g., 00000123456 = 123.456)'),
        FixedColumn('last_activity_date',81,  88, comment='YYYYMMDD last activity'),
        FixedColumn('status_code',   89,  90, comment='Account status (e.g., "AC" active)'),
        FixedColumn('filler',        91, 200, comment='Reserved/spaces'),
    ],
    'TR': [  # Trailer Record
        FixedColumn('record_type',   1,   2, comment='Always "TR"'),
        FixedColumn('record_count',  3,  12, alignment='right', pad_char='0', comment='Total detail records'),
        FixedColumn('total_balance',13,  30, alignment='right', pad_char='0', comment='Sum of all balances (implied decimal)'),
        FixedColumn('filler',        31, 200, comment='Reserved/spaces'),
    ],
}

# ───────────────────────────────────────────────
# X12 835 Remittance Advice (Health Care Claim Payment/Advice)
# Partial - most common segments only (835 has 100+ possible segments)
# Record length varies per segment
# ───────────────────────────────────────────────
X12_835_COLUMNS = {
    'ISA': [  # Interchange Control Header (common first segment)
        FixedColumn('segment_id',     1,   3, comment='Always "ISA"'),
        FixedColumn('auth_info_qual', 4,   5, comment='Authorization information qualifier'),
        FixedColumn('auth_info',      6,  15, comment='Authorization information'),
        FixedColumn('security_qual', 16,  17, comment='Security information qualifier'),
        FixedColumn('security_info', 18,  27, comment='Security information'),
        FixedColumn('sender_id_qual',28,  29, comment='Sender ID qualifier'),
        FixedColumn('sender_id',     30,  50, comment='Sender ID'),
        FixedColumn('receiver_id_qual',51,  52, comment='Receiver ID qualifier'),
        FixedColumn('receiver_id',   53,  73, comment='Receiver ID'),
        FixedColumn('date',          74,  79, comment='YYMMDD interchange date'),
        FixedColumn('time',          80,  83, comment='HHMM interchange time'),
        FixedColumn('standards_id',  84,  89, comment='Standards identifier'),
        FixedColumn('version',       90,  94, comment='Interchange version (e.g., "00501")'),
        # ... more ISA fields if needed ...
    ],
    'GS': [  # Functional Group Header
        FixedColumn('segment_id',     1,   2, comment='Always "GS"'),
        FixedColumn('functional_id',  3,   4, comment='Functional group identifier (e.g., "HP" for 835)'),
        FixedColumn('sender_id',      5,  15, comment='Application sender code'),
        FixedColumn('receiver_id',   16,  26, comment='Application receiver code'),
        FixedColumn('date',          27,  33, comment='CCYYMMDD group date'),
        FixedColumn('time',          34,  37, comment='HHMM group time'),
        FixedColumn('group_control', 38,  43, comment='Group control number'),
        FixedColumn('agency',        44,  46, comment='Responsible agency code (e.g., "X")'),
        FixedColumn('version',       47,  52, comment='Version/release (e.g., "005010X221A1")'),
    ],
    # ... add ST, CLP, NM1, REF, DTM, etc. as needed for full 835 ...
    # This is a starter; real 835 files have 20–50+ segment types
}

# ───────────────────────────────────────────────
# Registry for easy lookup / from_format support
# ───────────────────────────────────────────────
FORMATS = {
    'nach_ach': {
        'columns': ACH_COLUMNS,
        'name': 'NACHA ACH (US Automated Clearing House)',
        'description': 'Standard US ACH file format (94-char records)',
    },
    'cobol_bank_extract': {
        'columns': COBOL_BANK_EXTRACT_COLUMNS,
        'name': 'COBOL Mainframe Bank Extract (example)',
        'description': 'Typical legacy banking account extract layout (200-byte records)',
    },
    'x12_835_remittance': {
        'columns': X12_835_COLUMNS,
        'name': 'X12 835 Health Care Claim Payment/Advice',
        'description': 'Partial US healthcare remittance advice format (variable segment lengths)',
    },
}
