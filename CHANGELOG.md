# Changelog

All notable changes to DBTK will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

---

## [Unreleased]

### Added

- **`ExcelWriter` / `LinkedExcelWriter` `formatting` parameter** — a single dict
  controls all worksheet presentation without touching openpyxl directly. Keys:

  - `styles` — named style definitions (`bg_color`, `font`, `number_format`,
    `alignment`). Eight built-in styles are pre-registered on every workbook:
    `date_style`, `datetime_style`, `hyperlink_style`, `bold_style`,
    `header_vert_style` (bold + 90° centred), `currency_style`, `percent_style`,
    `comma_style`. Declaring a reserved name logs a `WARNING`.
  - `columns` — wildcard pattern → properties (`format`, `header_format`, `width`,
    `hidden`). Patterns are matched case-insensitively via `fnmatch`; later rules
    override earlier ones per property, so `resv_*_desc: {hidden: 0}` can
    selectively un-hide columns matched by a broader `resv_*: {hidden: 1}` rule.
    `header_format` styles only the header cell and is excluded from auto-rotation.
  - `rows` — row index → properties (`height`); `0` targets the header row.
    A `'style'` key accepts a callable `lambda rec: style_name | None` for
    data-driven row colouring (alternating rows, conditional highlighting, etc.).
    Row style overrides column `format`; `hyperlink_style` overrides both.
  - `freeze` — cell reference string (e.g. `'D2'`); defaults to `'A2'`, pass
    `None` to disable.
  - `header_auto_rotate` — float ratio or `{'ratio', 'min_length', 'height_factor'}`
    dict. Applies `header_vert_style` to columns where `header_len >= min_length`
    (default 8) **and** `header_len > data_width × ratio` (default 1.5), avoiding
    spurious rotation of short headers like `'OPEN'` over `'Y'`/`'N'` data.
    Auto header-row height is computed from the longest rotated name at 6.5
    pt/character (overridden by explicit `rows[0]['height']`). Auto-rotated columns
    use sampled data width for column sizing; non-rotated columns use
    `max(header_len, data_width)` as before.
  - `min_column_width` — floor for auto-sized columns (default `6`); lower for
    narrow indicator columns.

  Inline format dicts for `format` / `header_format` are deduplicated via
  content-based MD5 `NamedStyle` names. Header and data widths are tracked
  separately; `_finalize_headers()` consolidates all post-write worksheet setup
  across all three write paths (`write()`, `write_batch()`,
  `LinkedExcelWriter.write_batch()`). Documented in `docs/06b-excel.md`.

- **`ExcelWriter.write_batch(headers=None)`** / **`LinkedExcelWriter.write_batch(headers=None)`** —
  optional per-call header override for workbooks where each sheet uses different display
  labels over the same underlying columns. Raises `ValueError` if the supplied list length
  does not match the column count. When omitted, the writer-level `headers=` (or raw field
  names) are used as before.

- **`ExcelWriter` pre-1900 date fallback** — date and datetime values whose year is before
  1900 are written as formatted strings rather than raising an openpyxl error. Excel cannot
  represent dates before 1900-01-01.

- **`examples/formatted_spreadsheet.py`** — runnable multi-sheet workbook using 1927 Lahman
  Baseball data (top 4 finishers in each league). Demonstrates named styles, `group_label`
  merged headers, `header_auto_rotate`, conditional per-cell `style` callable, overlapping
  column rules that compose, column comments, hidden columns, row striping, and the
  underscore-field-name / space-display-label pattern. `output/1927_baseball.parquet` is
  committed to the repo so the example runs without any additional setup.
  `examples/prep_1927_data.py` is provided to rebuild the parquet from the raw Lahman CSVs.

- **`DatabaseDialect` class hierarchy** — dialect-specific SQL generation and schema
  introspection (upsert syntax, MERGE templates, temp table DDL, type mapping,
  `column_defs_from_db`) is now centralised in `dbtk/dialects/`. Adding support for a
  new database engine requires writing one `DatabaseDialect` subclass. `BulkSurge` bulk
  load mechanisms remain database-specific and are unaffected.

- **`'type.method'` cast-and-call shorthand for `fn`** — `fn_resolver` now supports
  `'int.to_bytes'`, `'str.encode'`, `'float.hex'`, `'bytes.hex'`, and any other
  no-argument method on `int`, `float`, `str`, or `bytes`. The value is cast to the
  named type and the method is called with no arguments. Documented in `docs/07-table.md`.

- **`QueryLookup(query=..., filename=..., return_col=..., missing=...)`** — new deferred
  transform for `Table` column pipelines, wrapping `PreparedStatement` for lookups that
  require joins, subqueries, or multi-column keys beyond what the `'lookup:...'` shorthand
  can express. Use with `field='*'` to pass the full source row as bind variables;
  `PreparedStatement` uses only the parameters its SQL declares, ignoring the rest.
  `return_col='*'` returns the full row for downstream pipeline steps; omitting
  `return_col` returns `row[0]`. Exported from `dbtk.etl`. Documented in
  `docs/07-table.md` alongside `TableLookup`/`Lookup`/`Validate`.

- **`FixedWidthRecord.from_line(line, auto_trim=True)`** — classmethod that parses a
  raw fixed-width string into a record instance; the corollary to `to_line()`. Slices
  each field by its declared position and applies the same type conversion (`int`,
  `float`, `date`, `datetime`, `timestamp`, `text`) used by `FixedReader`. Unparseable
  values fall back to a trimmed string. `from_line` added to `_RESERVED`.

- **`FixedReader.add_row_num` default changed to `False`** — fixed-width files have
  explicit column specs, making row numbers less useful than in CSV/Excel. Pass
  `add_row_num=True` to restore the previous behaviour.

- **SQL Server ODBC Driver 18 support** — `pyodbc_sqlserver` (priority 11) now targets
  `ODBC Driver 18 for SQL Server`. A new `pyodbc_sqlserver_17` entry (priority 12)
  provides fallback support for systems with only Driver 17 installed. `pymssql`
  priority bumped to 13.

- **Windows authentication for SQL Server** — `trusted_connection` added as a valid
  alternative required-parameter set (`{host, database, trusted_connection}`) for both
  `pyodbc_sqlserver` and `pyodbc_sqlserver_17`, so `user`/`password` are not required
  when using Windows auth.

### Changed

- **`XLSXReader` renamed to `ExcelReader`** — matches the naming convention of `ExcelWriter`.
  `XLSReader` (legacy `.xls` format) is unchanged.

- **`FixedReader._generate_rows()` / `EDIReader._generate_rows()`** — both now delegate
  to `FixedWidthRecord.from_line()`, eliminating duplicated type-conversion logic.
  `EDIReader` previously only stripped whitespace regardless of `column_type`; it now
  correctly converts `int`, `float`, `date`, and `datetime` columns.

- **`FixedReader.visualize()` / `EDIReader.visualize()`** — simplified to use
  `from_line()` instead of manual positional slicing.

- **`dbtk checkup` output** — package and driver tables now use `fixed_record_factory`
  and `to_line()` for column alignment instead of hand-rolled format strings.

- **ODBC `param_map` values lowercased** — all built-in ODBC drivers now map
  `{'host': 'server', 'user': 'uid', 'password': 'pwd'}`. `_get_odbc_string` uppercases
  keys when building the connection string, so wire behaviour is unchanged.
  `register_user_drivers` now also lowercases `param_map` values from user-defined
  drivers for consistency.

- **Boolean values in ODBC connection strings** — Python `True`/`False` values (e.g.
  from YAML `yes`/`no` without quotes) are now converted to `'yes'`/`'no'` in
  `_get_odbc_string` rather than appearing as `True`/`False`.

### Fixed

- **`ExcelWriter` multi-session sheet truncation** — re-running a script against an existing
  workbook no longer clears sheets that were already written in the current session.
  `_sheets_written_this_session` now guards both `write_batch()` and `_write_data()`.

- **`ExcelWriter` header width calculation** — column auto-sizing now uses the display names
  supplied via `headers=` rather than raw field names, so columns are not over-widened when
  field names (e.g. `home_runs`) differ from their display labels (e.g. `Home Runs`).

- **`ExcelWriter` overlapping column format rules now compose** — when multiple column-range
  patterns match the same column, `format` values are accumulated and merged via
  `_compose_styles()` rather than the later rule replacing the earlier one. A broad range can
  set `bg_color` while a sub-range adds `number_format` without losing the background color.

- **Connection parameter key casing** — `_validate_connection_params` now lowercases
  all incoming parameter keys before validation. Mixed-case kwargs such as
  `TrustServerCertificate='yes'` or `Port=1433` were previously silently ignored.
  `register_user_drivers` similarly lowercases `optional_params` keys from user-defined
  drivers.

- **Unknown connection parameters now logged** — parameters that are not recognised
  for the selected driver now emit a `WARNING` instead of being silently discarded.

- **DSN connections with password** — `_get_odbc_string` correctly reads the `pwd` key
  (lowercase, post-normalisation) when appending the password to a DSN connection string.

- **`Table` pipeline deferred-binding bug** — `'lookup:...'` / `'validate:...'` strings
  inside a list `fn` pipeline had their `bind()` return value discarded; the unbound
  `_DeferredTransform` was appended instead, causing a `RuntimeError` at call time.
  Fixed by switching from `isinstance(_DeferredTransform)` to duck-typing on `bind`,
  which also enables any user-defined deferred transform with a `bind(cursor)` method
  to work in pipelines.

- **`LinkedExcelWriter` duplicate key links** — `LinkSource.cache_record()` now
  preserves the first occurrence of a key rather than overwriting it on each
  subsequent row, so internal hyperlinks point to the first matching row.

---

## [0.8.3] - 2026-04-06

### Added

- **`cursor.prepare_query(query)`** — prepares an inline SQL string for repeated
  execution, mirroring `cursor.prepare_file()` for query strings. Parameter conversion
  is performed once; the returned `PreparedStatement` can be executed many times
  efficiently.

- **`PreparedStatement` top-level export** — now importable directly as `dbtk.PreparedStatement`
  in addition to `dbtk.cursors.PreparedStatement`.

- **Compression support for file writers** — `to_csv()`, `to_json()`, `to_ndjson()`,
  `CSVWriter`, `JSONWriter`, and `NDJSONWriter` now accept a `compression` parameter.
  The default `'infer'` detects the format from the file extension (`.gz` → gzip,
  `.bz2` → bz2, `.xz`/`.lzma` → lzma). Pass an explicit value (`'gzip'`, `'bz2'`,
  `'lzma'`) to override inference, or `None` to write plain text regardless of
  extension. Compression is implemented once in `BaseWriter._open_file_handle()` so
  all writers inherit it automatically, including batch writers.

- **`Record._RESERVED` completeness** — `_RESERVED` is used to ensure normalized field 
  names do not conflict with Record methods and attributes.  It now covers all non-dunder 
  names on `Record` and `FixedWidthRecord`, including  inherited list methods 
  (`count`, `index`, `insert`, `reverse`, `sort`) and classmethods 
  (`set_fields`, `_get_reserved`). A regression test in `test_record.py` asserts 
  `dir(Record)` ⊆ `_RESERVED` so future additions are caught automatically.

- **`cursor.execute(convert_params=True)`** — opt-in query rewriting and paramstyle
  conversion for one-off queries. Accepts a named-parameter dict, rewrites the query
  to the cursor's paramstyle, defaults missing parameters to `None`, and ignores extra
  parameters. Equivalent to what `execute_file()` and `PreparedStatement` do
  automatically.

### Fixed

- **`normalize_field_name` leading underscore handling** — a leading underscore is now
  preserved only if the original field name explicitly started with one. Previously,
  leading characters like `$` or `#` were replaced with `_` by the regex, causing
  `'$Secret_Code'` to normalize to `'_secret_code'` instead of `'secret_code'`. Also
  fixes a long-standing doctest discrepancy where `'#Term Code'` was documented as
  `'term_code'` but actually produced `'_term_code'`.

- **`Record.reverse()`, `Record.sort()`, `Record.insert()` blocked** — these inherited
  `list` methods would silently reorder or shift the underlying value array, breaking
  all field-index mappings. They now raise `TypeError` with a descriptive message.

---

## [0.8.2] - 2026-03-10

### Fixed

- **Python 3.6 compatibility** — additional fixes discovered after 0.8.1:
  - Replace `type[Record]` annotation with `typing.Type[Record]` in `FixedWidthReader`
    (`type[X]` syntax requires Python 3.9+)
  - Replace `add_subparsers(required=True)` with a post-creation attribute assignment in
    `cli.py` (`required=` kwarg was added in Python 3.7)
  - Replace remaining `tuple[...]` / `X | Y` union syntax in docstrings for
    `writers/base.py`, `writers/xml.py`, and `etl/table.py` with `typing` equivalents
- **Import order** — move `readers` and `writers` imports before `etl` in `dbtk/__init__.py`
  to prevent `dbtk/readers/csv.py` from shadowing the stdlib `csv` module during ETL init
- **Package discovery** — add `[tool.setuptools.packages.find]` to `pyproject.toml` so
  setuptools < 61 (shipped with Python 3.6) correctly locates all `dbtk` sub-packages

### Fixed (Documentation)

- Fix two Sphinx errors in `api.rst`: invalid `:undoc-members: TableLookup` directive
  and a mismatched section underline length
- Add missing modules to `api.rst`: `dbtk.record` (`Record`, `FixedWidthRecord`),
  `dbtk.utils` (`ErrorDetail`, `FixedColumn`, `ParamStyle`), `dbtk.readers.data_frame`
  (`DataFrameReader`), and `dbtk.formats.edi` (EDI/ACH layout definitions)
- Reorder `api.rst` sections to match natural workflow:
  Configuration → Database → Cursors → Record → Readers → Writers → ETL → Utilities →
  Logging → CLI → Formats

---

## [0.8.1] - 2026-03-10

### Fixed

- **Python 3.6 compatibility** across the codebase:
  - Replace `str | Path` union syntax with `Union[str, Path]` from `typing` (3.10+ syntax was used)
  - Replace built-in `tuple[...]` type annotations with `Tuple[...]` from `typing` (3.9+ syntax was used)
  - Replace walrus operator `:=` with explicit assignment (3.8+ syntax was used)
  - Replace `subprocess.run(capture_output=True)` with explicit `stdout=PIPE, stderr=PIPE` (3.7+ syntax was used)
  - Pin `usaddress` to `==0.5.10` for Python 3.6; Python 3.7+ continues to use the latest version

---

## [0.8.0] - 2026-03-08

Initial public release of DBTK — Data Benders Toolkit.

### Added

#### Core

- **Universal database connectivity** — unified `Database` class for PostgreSQL, Oracle, MySQL,
  SQL Server, and SQLite with auto-detection of available drivers
- **`connection_name` support** on the `Database` class for named/multi-connection configurations
- **Portable SQL** — named-parameter queries that run unchanged across all supported databases
  regardless of each engine's native parameter style
- **`Record`** — ergonomic row type with the speed of tuples and the flexibility of dicts;
  supports field access by name or index, `copy()`, and mutable/locked schema modes
- **`Table`** — field mapping, transforms, validation, key-column enforcement, and safe
  partial updates (missing source fields are excluded from `UPDATE` rather than written as `NULL`)
- **`TableLookup`** shorthand — declare lookups and validations in a single string;
  supports `preload` hint for pre-caching before pipeline execution

#### ETL — DataSurge / BulkSurge

- **`DataSurge`** — batched insert/upsert/merge engine with configurable batch size,
  progress logging, retry logic, and summary statistics
  - `pass_through` mode restricts to insert-only operations and is automatically disabled
    when columns carry `db_expr` expressions
  - Skip tracking: captures per-reason skip counts and first-occurrence row numbers
    (up to 20 samples per reason) in a `skip_details` dictionary
  - Unique temp table names to prevent collisions in concurrent integrations
- **`BulkSurge`** — direct bulk-load engine targeting database-native loaders for
  maximum throughput:
  - **SQL Server** — BCP with unit-separator (`\x1f`) delimited format and stderr capture
  - **Oracle** — SQL\*Loader with auto-generated control file (`direct_path_load`)
  - **MySQL** — `LOAD DATA` server-side and streaming modes with Windows path handling
    and explicit pre-load `COMMIT`
  - `dump()` writes a delimited flat file and the loader control/format file
  - `load()` accepts a caller-supplied dump location
  - `_resolve_file_path()` consolidates dump path resolution across all loaders
- All writers default to **UTF-8**; readers default to **UTF-8-sig**

#### File Readers & Writers

- **Readers** — CSV, Excel (XLS/XLSX), JSON, NDJSON, XML, DataFrame, Fixed-width, EDI;
  consistent `Record`-returning API across all formats
- **Writers** — CSV, Excel (`ExcelWriter` / `LinkedExcelWriter`), JSON, NDJSON, XML,
  Fixed-width (`FixedWidthWriter`), EDI (`EDIWriter`); all default to UTF-8
- **Transparent compression** — automatic decompression of `.gz`, `.bz2`, `.xz`, and
  `.zip` files with smart member selection on read
- **`FixedColumn`** — column descriptor with `width=`, `align=`, and padding; used by
  both `FixedWidthReader` and `FixedWidthWriter`
- **`FixedWidthRecord`** — fixed-schema `Record` subclass that regenerates a
  properly-formatted fixed-width line via `to_line()` (positional splicing)
  - `pprint()` override with optional `add_comments` parameter (prefixes column
    comments with `# `)
  - `visualize()` — renders a box-drawing ruler with field boundaries for debugging
- **`EDIReader`** / **`EDIWriter`** — read and write EDI and ACH fixed-width formats;
  ACH `immediate_destination` / `immediate_origin` zero-padding corrected
- EDI format definitions moved to `dbtk/formats/edi.py` for format-neutral packaging

#### Developer Experience

- **Zero-config logging** — timestamped log files with auto-cleanup and a global error flag
- **`Table.generate_sql`** made private; `get_sql()` is the public accessor
- **`Record.copy()`** added; field names starting with a digit are prefixed with `'n'`
- Empty strings treated as `NULL`/invalid in `reqs_met` and `Table` validation
- `FixedColumn` parameter renamed from `alignment=` to `align=` for brevity

#### Documentation

- Full [Sphinx](https://www.sphinx-es.io) documentation published to
  [dbtk.readthedocs.io](https://dbtk.readthedocs.io)
- Separate `readers.md` and `writers.md` reference pages
- `BulkSurge` deep-dive with database-specific loader details
- Performance recommendations for NDJSON, fixed-width, and EDI workloads
- Examples directory with inline callouts (`examples/README.md`)

---

[0.8.2]: https://github.com/scottrbailey/dbtk/compare/v0.8.1...v0.8.2
[0.8.1]: https://github.com/scottrbailey/dbtk/compare/v0.8.0...v0.8.1
[0.8.0]: https://github.com/scottrbailey/dbtk/releases/tag/v0.8.0
