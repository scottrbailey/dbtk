# Excel Reports

DBTK provides two Excel writers built on [openpyxl](https://openpyxl.readthedocs.io/):

| Class | Use when |
|---|---|
| `ExcelWriter` | Single or multi-sheet reports with column formatting, styles, and auto-sizing |
| `LinkedExcelWriter` | Reports with internal navigation links or external hyperlinks (extends `ExcelWriter`) |

Both writers keep the workbook open across `write_batch()` calls and save only on context exit, making them efficient for multi-sheet reports.

---

## Quick Start

```python
from dbtk.writers import ExcelWriter

# Single sheet — one shot
ExcelWriter(cursor, 'report.xlsx').write()

stmt = cursor.prepare_file('quarterly_sales.sql')
# Multi-sheet — context manager
with ExcelWriter(file='report.xlsx') as writer:
    for qtr in (1, 2, 3, 4):
        stmt.execute({'quarter': qtr})
        writer.write_batch(stmt, sheet_name=f'Q{qtr}')
# Workbook saved on exit
```

The first `write_batch()` to a sheet writes the header row; subsequent calls to the **same** sheet append rows without repeating it. Writing to a different `sheet_name` starts a new sheet.

### Custom Headers

Pass `headers` explicitly when the column names in the query aren't what you want in the report — for example, Oracle's 30-character column name limit forces aliases that make poor headers, or you simply want friendlier labels:

```python
# Oracle alias → readable header
cursor.execute("SELECT acad_plan_owner_org_id AS owner_org, ...")
ExcelWriter(cursor, 'report.xlsx',
            headers=['Owner Org', ...]).write()

# Any column names → display labels
cursor.execute("SELECT crse_numb, subj_code, cred_hrs FROM courses")
ExcelWriter(cursor, 'courses.xlsx',
            headers=['Course Number', 'Subject', 'Credits']).write()
```

---

## Automatic Behaviors

Out of the box, without any `formatting` configuration, `ExcelWriter` does the following automatically:

- **Bold header row** — column names are written in the first row with bold font
- **Auto-sized columns** — the first 15 data rows are sampled to estimate content width; columns are sized to fit, between 6 and 60 characters wide by default
- **Frozen header row** — the top row is frozen so it stays visible while scrolling (`freeze_panes = 'A2'`)
- **Date and datetime formatting** — `date` values get `YYYY-MM-DD` format; `datetime` values with a non-midnight time get `YYYY-MM-DD HH:MM:SS` format, automatically
- **None → empty cell** — `None` values are written as blank cells rather than the string `"None"`

These defaults are all overridable via the `formatting` parameter. Auto-sizing limits are controlled by `min_column_width` (default `6`) and `max_column_width` (default `60`); freezing is controlled by `freeze` (pass `None` to disable).

---

## Worksheet Formatting

Pass a `formatting` dict (or an `ExcelFormat` object) to `ExcelWriter` (or `LinkedExcelWriter`) to control styles, column widths, hidden columns, freeze panes, and header rotation. All keys are optional.

```python
fmt = {
    'styles':            {...},   # named style definitions
    'columns':           {...},   # pattern → column properties
    'rows':              {...},   # row key → row properties
    'freeze':            'D2',    # freeze panes cell reference
    'header_auto_rotate': 1.5,   # auto-rotate long headers
    'min_column_width':  4,       # minimum auto-sized column width
    'max_column_width':  40,      # maximum auto-sized column width
    'auto_filter':       True,    # enable dropdown filter on header row
}

with ExcelWriter(file='report.xlsx', formatting=fmt) as writer:
    writer.write_batch(cursor, sheet_name='Data')
```

### ExcelFormat and ColumnRule Dataclasses

For discoverable, IDE-friendly formatting configuration, `ExcelFormat` and `ColumnRule` are available as typed dataclasses:

```python
from dbtk.writers import ExcelWriter, ExcelFormat, ColumnRule

fmt = ExcelFormat(
    styles={
        'fmt_fees': {'bg_color': '#d5f1cc'},
        'fmt_warn': {'bg_color': '#ffcccc', 'font': {'bold': True}},
    },
    columns={
        '*_fee*': ColumnRule(format='fmt_fees'),
        'notes':  ColumnRule(width=40, comment='Free-text field'),
        'gpa':    ColumnRule(format={'number_format': '0.00'}),
    },
    rows={
        '*':    {'height': 15},
        'data': {'odd': {'format': 'fmt_stripe'}},
    },
    freeze='D2',
    min_column_width=4,
    auto_filter=True,
)

with ExcelWriter(file='report.xlsx', formatting=fmt) as writer:
    writer.write_batch(cursor, sheet_name='Data')
```

`ExcelFormat` and `ColumnRule` are fully interchangeable with dicts — pass either form. `ColumnRule` fields mirror the column rule dict keys exactly.

### Named Styles

Define reusable styles in `formatting['styles']`. Each entry is a style name mapped to a properties dict:

```python
'styles': {
    'fmt_fees':    {'bg_color': '#d5f1cc'},                      # green fill
    'fmt_alerts':  {'bg_color': '#ffcccc', 'font': {'bold': True, 'color': 'FF0000'}},
    'fmt_pct':     {'number_format': '0.0%'},
    'fmt_wrap':    {'alignment': {'wrap_text': True, 'vertical': 'top'}},
}
```

**Supported properties:**

| Property | Type | Effect |
|---|---|---|
| `bg_color` | hex string (`'#d5f1cc'` or `'d5f1cc'`) | Solid background fill |
| `font` | dict of [`Font`](https://openpyxl.readthedocs.io/en/stable/api/openpyxl.styles.fonts.html) kwargs | `bold`, `italic`, `color`, `size`, `underline`, … |
| `number_format` | string | Excel number format code |
| `alignment` | dict of [`Alignment`](https://openpyxl.readthedocs.io/en/stable/api/openpyxl.styles.alignment.html) kwargs | `horizontal`, `vertical`, `wrap_text`, `text_rotation`, … |

#### Built-in Styles

The following styles are registered on every workbook and can be used directly by name without defining them in `styles`:

| Style name | Description |
|---|---|
| `date_style` | Number format `YYYY-MM-DD` |
| `datetime_style` | Number format `YYYY-MM-DD HH:MM:SS` |
| `hyperlink_style` | Blue underlined font (used automatically by `LinkedExcelWriter`) |
| `bold_style` | Bold font |
| `header_vert_style` | Bold + 90° rotation + centred (see [Auto-rotating Headers](#auto-rotating-headers)) |
| `currency_style` | Number format `#,##0.00` |
| `percent_style` | Number format `0.00%` |
| `comma_style` | Number format `#,##0` |

> **Overriding built-ins:** To replace a built-in style, define it in `formatting['styles']` using the same name. User-defined styles are registered before built-ins, so a `comma_style` entry in `styles` replaces the default. Any property not specified in your definition falls back to the openpyxl default (no font, no fill, etc.), so include all properties you want.

> **Note:** Named styles are embedded in the workbook file. If you rerun a script that writes to an existing `.xlsx` file, styles from the previous run are already baked in — changed style definitions won't take effect. Delete the file before rerunning to pick up style changes.

---

### Column Rules

`formatting['columns']` maps patterns to per-column properties. Three pattern forms are supported, all matched **case-insensitively**:

- **Literal** — exact column name: `'notes'`
- **Glob** — [`fnmatch`](https://docs.python.org/3/library/fnmatch.html) wildcards (`*`, `?`, `[seq]`): `'*_fee*'`, `'resv_*'`
- **Range** — `'start:end'` applies to all columns from `start` through `end` inclusive, based on their position in the result set. Open-ended forms: `':end'` (from first column) and `'start:'` (to last column). Raises `ValueError` if either endpoint isn't found. A pattern containing `:` is only treated as a range if it has no wildcard characters and doesn't itself match a column name.

```python
'columns': {
    '*_fee*':           {'format': 'fmt_fees', 'header_format': 'header_vert_style'},
    'sales_*':          {'hidden': 1},
    'sales_*_total':    {'hidden': 0, 'comment': 'Additional columns are hidden'},
    'notes':            {'width': 40},
    'unit_price:revenue':  {'format': 'fmt_numeric'},   # range
    ':subj_code':       {'hidden': 1},               # hide everything up to subj_code
}
```

**Column properties:**

| Key | Type | Effect |
|---|---|---|
| `format` | style name or inline dict | Applied to every **data** cell in this column |
| `style` | callable or list of callables | `lambda rec: style_name_or_None` — per-cell conditional style(s); composed on top of all other styles when non-None |
| `header_format` | style name or inline dict | Applied to the **header** cell only; owns the cell entirely (include `font: {bold: True}` if needed) |
| `width` | float | Column width in Excel character units; overrides auto-sizing |
| `hidden` | 0 or 1 | Hide (`1`) or explicitly un-hide (`0`) the column |
| `comment` | string | Adds an Excel comment/note to the header cell |
| `filter` | 0 or 1 | Show a filter dropdown on this column's header; hides dropdowns on all other columns |
| `group_label` | string | Merged super-header label above this column range (range patterns only) |

**Precedence:** Rules are applied in definition order. For most properties (width, hidden, filter, etc.), later patterns override earlier ones. The `format` property is the exception — when multiple patterns match the same column and both provide `format`, the styles are **composed**: properties from the later rule take precedence per property (fill, font, number format), but non-conflicting properties from the earlier rule are preserved.

```python
# wide rule sets background; narrower rule adds number format without losing the background
'columns': {
    'g:slg':   {'format': 'hits_style'},               # green background for all batting cols
    'avg:slg': {'format': {'number_format': '0.000'}}, # composed on top — keeps green bg
}

# scalar properties (width, hidden) always override
'columns': {
    'sales_*':      {'hidden': 1},
    'sales_*_desc': {'hidden': 0, 'comment': 'Additional columns are hidden'},
}
```

**Per-cell conditional styles** use a `style` callable in the column rule. The lambda receives the full record and returns a style name (or `None`). This overrides both the column's static `format` and any row-level style when non-None:

```python
'columns': {
    'max_capacity': {'style': lambda rec: 'fmt_warn' if rec.max_capacity < 10 else None},
}
```

**Group headers** use `group_label` on a range pattern to add a merged super-header row above the column names. Only range patterns are supported — using `group_label` on a wildcard logs a warning and is ignored. Columns not covered by any group are left blank in the group header row.

```python
'columns': {
    'q1_sales:q4_sales':       {'format': 'fmt_sales',    'group_label': 'Quarterly Sales'},
    'q1_revenue:q4_revenue':   {'format': 'fmt_revenue', 'group_label': 'Quarterly Revenue'},
}
```

Group headers are written bold and centered in row 1; column headers shift to row 2; data starts at row 3. The freeze pane default shifts from `'A2'` to `'A3'` automatically.

**Inline style dicts** work anywhere a style name is accepted. Equivalent inline dicts are deduplicated automatically:

```python
'columns': {
    'gpa': {'format': {'number_format': '0.00', 'alignment': {'horizontal': 'center'}}},
    'title': ColumnRule(format={'bg_color': '#60CCFF'}, width=40,
                        comment='This comment will appear in the header row', filter=1),
}
```

---

### Row Formatting

`formatting['rows']` is a dict with four named keys. All are optional:

| Key | Applies to |
|---|---|
| `'*'` | Every row (header, group header, and all data rows) |
| `'header'` | The column-name header row only |
| `'group_header'` | The group label row only (only relevant when `group_label` columns are used) |
| `'data'` | Data rows only |

Each key maps to a dict that may contain `height` and/or `format`. The `'data'` key additionally supports `odd`, `even`, and `style`:

```python
'rows': {
    '*':           {'height': 15},                    # all rows: default height
    'header':      {'height': 30},                    # override header height
    'group_header': {'height': 20},                   # override group label row height
    'data': {
        'height': 15,                                 # data row height
        'odd':    {'format': 'fmt_stripe'},           # rows 1, 3, 5, …
        'even':   {'format': 'fmt_alt'},              # rows 2, 4, 6, …
        'style':  lambda rec: 'fmt_alerts' if rec['status'] == 'OVERDUE' else None,
    },
}
```

**Height cascade:** `'*'` sets the default; `'header'`, `'group_header'`, and `'data'` override it for their respective rows. Setting only `'*'` is equivalent to setting the same height everywhere.

**Alternating row colors** use `odd` and `even` nested under `'data'`:

```python
'rows': {
    'data': {
        'odd':  {'format': 'fmt_stripe'},   # rows 1, 3, 5, …
        'even': {'format': 'fmt_alt'},      # rows 2, 4, 6, …
    },
}
```

You can define only one side if you only want every-other-row coloring:

```python
'rows': {'data': {'odd': {'format': 'fmt_stripe'}}}   # only odd rows get a background
```

**Conditional row styles** use `style` under `'data'`. It accepts a callable or a list of callables; each receives the full record and returns a style name or `None`. Multiple callables are composed in order, later ones taking precedence:

```python
'rows': {
    'data': {
        'style': lambda rec: 'fmt_alerts' if rec['status'] == 'OVERDUE' else None,
    },
}

# Multiple callables — both applied, last non-None wins per property
'rows': {
    'data': {
        'style': [
            lambda rec: 'fmt_stripe' if rec['row_num'] % 2 else None,
            lambda rec: 'fmt_alerts' if rec['overdue'] else None,
        ],
    },
}
```

**Style cascade (lowest → highest priority):**

1. Date/datetime base format (applied automatically by type)
2. Column `format`
3. `'*'` row format (all rows)
4. `'odd'` / `'even'` alternating format
5. `'style'` callable results (composed in list order)
6. Column `style` callable result
7. `hyperlink_style` (applied by `LinkedExcelWriter` to linked cells)

Styles at higher priority levels are composed on top — they override individual properties (fill, font, number format) rather than replacing the whole style.

---

### Freeze Panes

Set the top-left unfrozen cell. Defaults to `'A2'` (freeze the header row). Pass `None` to disable:

```python
'freeze': 'D2',    # freeze columns A-C and the header row
'freeze': None,    # no freeze panes
```

---

### Auto-rotating Headers

Long column names over narrow data columns waste horizontal space. `header_auto_rotate` detects these columns and applies `header_vert_style` (bold, 90° rotation, centred) automatically.

Two conditions must **both** hold for a column to be rotated:

1. `header_len >= min_length` — prevents short headers like `'OPEN'` from rotating
2. `header_len > data_width × ratio` — the header must be meaningfully longer than the data

```python
# Scalar shorthand — uses defaults (min_length=8, ratio=1.5)
'header_auto_rotate': 1.5

# Dict form — full control
'header_auto_rotate': {
    'ratio':         2.5,    # header must be 2.5× the data sample width
    'min_length':    8,      # ignore headers shorter than 8 characters
    'height_factor': 6.5,    # pts per character for auto header row height
}
```

| Example header | Length | Sampled data | Rotated? |
|---|---|---|---|
| `OPEN` | 4 | `Y` / `N` | No — fails `min_length` |
| `STATUS` | 6 | `Y` / `N` | No — fails `min_length` |
| `GRADABLE_IND` | 12 | `Y` / `N` | Yes |
| `REG_AUTH_ACTIVE_CDE` | 19 | `Y` / `N` | Yes |
| `student_name` | 12 | `Smith, Jane` (11) | No — header not wider than data |

**Header row height** is computed automatically from the longest rotated header name using `height_factor` (default 6.5 pt/character) unless `rows['header']['height']` is set explicitly. `REG_AUTH_ACTIVE_CDE` (19 chars) produces a height of ~123 pt.

**Columns with an explicit `header_format`** are excluded from auto-rotation — your explicit choice takes precedence.

**Auto-rotated columns** use the sampled data width for column sizing (not the header length). Non-rotated columns use `max(header_length, data_width)` as before.

---

### Minimum Column Width

`min_column_width` sets the floor for auto-sized columns (default `6`). Lower it for indicator columns where the data is always short:

```python
'min_column_width': 3    # Y/N flag columns don't need width 6
```

Explicit `width` values in column rules are not affected.

---

### Maximum Column Width

`max_column_width` sets the ceiling for auto-sized columns (default `60`). Useful when a few long text columns would otherwise dominate the sheet:

```python
'max_column_width': 40   # no column wider than 40 units
```

Explicit `width` values in column rules are not affected.

---

### Auto-filter

`auto_filter: True` enables Excel's dropdown filter on the header row across all columns:

```python
'auto_filter': True
```

To show filter dropdowns on **specific columns only**, use the `filter` column rule instead. This implies auto-filter but hides the dropdown on all unmarked columns:

```python
'columns': {
    'subj_code': {'filter': 1},
    'term_code': {'filter': 1},
}
```

If both `auto_filter: True` and column-level `filter: 1` are set, the column-level rules win — only the marked columns show dropdowns.

---

### Formatted Excel Example

```python
fmt = {
    'styles': {        
        'fmt_sales':     {'bg_color': '#e3f3fe'},
        'fmt_volume':    {'bg_color': '#ffeed9'},
        'fmt_shrinkage': {'bg_color': '#d5f1cc', 'font': {'bold': 1}},
        'fmt_warning':   {'bg_color': '#fec76f'},
        'fmt_stripe':    {'bg_color': '#dde8ed'}
    },
    'columns': {
        'sales_*':      {'format': 'fmt_sales'},
        'volume*':      {'format': 'fmt_volume'},
        'shrink*':      {'format': 'fmt_shrinkage', 'hidden': 1},
        'shrink_pct':   {'hidden': 0, 
                         'style': lambda x: 'fmt_warning' if x.shrink_pct > 8.0 else None},
    },
    'freeze':             'D3',
    'header_auto_rotate': {'min_length': 8, 'ratio': 2.5},
    'min_column_width':   3,
}
stmt = cursor.prepare_file('quarterly_sales')
with ExcelWriter(file='quarterly_sales.xlsx', formatting=fmt) as writer:
    for qtr in fy_quarters:
        stmt.execute({'quarter': qtr})
        writer.write_batch(stmt, sheet_name=f'Q{qtr}')
```

See [examples/formatted_spreadsheet.py](../examples/README.md#formatted_spreadsheetpy) for a complete, runnable example.
---

## Per-sheet Formatting

The `formatting` dict is set at the writer level and applies to all sheets written by that writer instance. When different sheets need different formatting, use separate writer instances — `ExcelWriter` opens existing workbooks without overwriting other sheets:

```python
fees_fmt   = {'columns': {'*_fee*': {'format': 'fmt_fees'}}, ...}
roster_fmt = {'columns': {'gpa': {'format': 'fmt_pct'}}, ...}

ExcelWriter(fees_data,   'report.xlsx', sheet_name='Fees',   formatting=fees_fmt).write()
ExcelWriter(roster_data, 'report.xlsx', sheet_name='Roster', formatting=roster_fmt).write()
```

Each writer opens the file, adds its sheet with its own formatting, saves, and closes. Note that openpyxl stores named styles on the workbook, not the worksheet — the second writer can reference `fmt_fees` by name, but cannot redefine it.

---

## Hyperlinked Reports with LinkedExcelWriter

`LinkedExcelWriter` extends `ExcelWriter` to create navigable reports with internal worksheet links and external URL hyperlinks.

### Workflow

1. Define one or more `LinkSource` objects describing linkable entities
2. Register them with the writer
3. Write **source sheets first** — row locations and display text are cached as they're written
4. Write **detail sheets** — specify which columns become hyperlinks

```python
from dbtk.writers import LinkedExcelWriter, LinkSource

student_link = LinkSource(
    name="student",
    source_sheet="Students",
    key_column="student_id",
    text_template="{last_name}, {first_name}",
    url_template="https://sis.university.edu/student/{student_id}",
)

with LinkedExcelWriter(file='enrollment_report.xlsx') as writer:
    writer.register_link_source(student_link)

    # Source sheet — locations cached as written
    writer.write_batch(students_cursor, sheet_name='Students')

    # Detail sheet — student_id column becomes a hyperlink
    writer.write_batch(
        enrollments_cursor,
        sheet_name='Enrollments',
        links={'student_id': 'student'}           # column → link source name
    )
```

### LinkSource Parameters

| Parameter       | Required | Description                                                                       |
|-----------------|----------|-----------------------------------------------------------------------------------|
| `name`          | Yes      | Identifier used in `links=` dict                                                  |
| `source_sheet`  | Yes*     | Sheet whose rows are the link targets                                             |
| `key_column`    | Yes*     | Column that uniquely identifies each row                                          |
| `text_template` | No       | Python format string for link display text (uses `str.format_map`)                |
| `url_template`  | No       | Python format string for external URL                                             |
| `missing_text`  | No       | Fallback text when a key can't be resolved                                        |
| `external_only` | No       | If `True`, generate URLs from current row without caching; reusable across sheets |

*Not required when `external_only=True`.

### Link Types

```python
links={
    'student_name': 'student',           # external URL (falls back to internal)
    'student_name': 'student:internal',  # always links to source sheet row
    'student_name': 'student:external',  # always uses url_template
}
```

### External-only Links

When `external_only=True`, the `LinkSource` generates URLs directly from the current row's data without caching. The same source can be reused across multiple sheets:

```python
imdb_link = LinkSource(
    name="imdb",
    url_template="https://imdb.com/title/{tconst}",
    text_template="{primary_title} ({start_year})",
    external_only=True,
)

with LinkedExcelWriter(file='movies.xlsx') as writer:
    writer.register_link_source(imdb_link)
    writer.write_batch(all_movies, sheet_name='All',
                       links={'primary_title': 'imdb'})
    writer.write_batch(top_rated, sheet_name='Top Rated',
                       links={'primary_title': 'imdb'})
```

### Combining Formatting and Links

`LinkedExcelWriter` accepts the same `formatting` dict as `ExcelWriter`. Column styles and row styles apply to non-linked cells; linked cells always get `hyperlink_style`:

```python
with LinkedExcelWriter(file='report.xlsx', formatting=fmt) as writer:
    writer.register_link_source(student_link)
    writer.write_batch(students, sheet_name='Students')
    writer.write_batch(enrollments, sheet_name='Enrollments',
                       links={'student_id': 'student:internal'})
```

---

## Formatting Quick Reference

| Key | Type | Default | Description |
|---|---|---|---|
| `styles` | `dict[name, props]` | `{}` | Named style definitions |
| `columns` | `dict[pattern, props]` | `{}` | Wildcard column rules |
| `rows` | `dict` | `{}` | Row height/style; keys: `'*'` (all rows), `'header'`, `'group_header'`, `'data'`; `data` supports nested `odd`, `even`, `style`, `height` |
| `freeze` | `str \| None` | `'A2'` | Freeze panes cell reference |
| `header_auto_rotate` | `float \| dict` | off | Auto-rotate long headers; see [above](#auto-rotating-headers) |
| `min_column_width` | `float` | `6` | Floor for auto-sized column widths |
| `max_column_width` | `float` | `60` | Ceiling for auto-sized column widths |
| `auto_filter` | `bool` | `False` | Enable dropdown filter on header row |

**Column rule keys:**

| Key | Type | Description |
|---|---|---|
| `format` | style name or dict | Style applied to data cells |
| `style` | callable or list | Per-cell conditional style(s); composed on top of all other styles |
| `header_format` | style name or dict | Style applied to the header cell only |
| `width` | float | Override auto-sized width |
| `hidden` | 0 or 1 | Hide or explicitly un-hide the column |
| `comment` | string | Excel comment/note on the header cell |
| `filter` | 0 or 1 | Show filter dropdown on this column; hides dropdowns on all others |
| `group_label` | string | Merged super-header label (range patterns only) |

**`header_auto_rotate` dict keys:**

| Key | Default | Description |
|---|---|---|
| `ratio` | `1.5` | `header_len > data_width × ratio` to trigger rotation |
| `min_length` | `8` | Minimum header character count to be considered for rotation |
| `height_factor` | `6.5` | Points per character for auto header row height calculation |


See [examples/linked_spreadsheet.py](../examples/README.md#linked_spreadsheetpy) for a complete, runnable example.