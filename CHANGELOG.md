# Changelog

All notable changes to DBTK will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

---

## [Unreleased]

### Added

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

### Added

- **Compression support for file writers** — `to_csv()`, `to_json()`, `to_ndjson()`,
  `CSVWriter`, `JSONWriter`, and `NDJSONWriter` now accept a `compression` parameter.
  The default `'infer'` detects the format from the file extension (`.gz` → gzip,
  `.bz2` → bz2, `.xz`/`.lzma` → lzma). Pass an explicit value (`'gzip'`, `'bz2'`,
  `'lzma'`) to override inference, or `None` to write plain text regardless of
  extension. Compression is implemented once in `BaseWriter._open_file_handle()` so
  all writers inherit it automatically, including batch writers.

- **`Record._RESERVED` completeness** — `_RESERVED` now covers all non-dunder names
  on `Record` and `FixedWidthRecord`, including inherited list methods (`count`, `index`,
  `insert`, `reverse`, `sort`) and classmethods (`set_fields`, `_get_reserved`). A
  regression test in `test_record.py` asserts `dir(Record)` ⊆ `_RESERVED` so future
  additions are caught automatically.

- **`cursor.execute(convert_params=True)`** — opt-in query rewriting and paramstyle
  conversion for one-off queries. Accepts a named-parameter dict, rewrites the query
  to the cursor's paramstyle, defaults missing parameters to `None`, and ignores extra
  parameters. Equivalent to what `execute_file()` and `PreparedStatement` do
  automatically.

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
