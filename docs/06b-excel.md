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

# Multi-sheet — context manager
with ExcelWriter(file='report.xlsx') as writer:
    writer.write_batch(q1_cursor, sheet_name='Q1')
    writer.write_batch(q2_cursor, sheet_name='Q2')
    writer.write_batch(q3_cursor, sheet_name='Q3')
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

Pass a `formatting` dict to `ExcelWriter` (or `LinkedExcelWriter`) to control styles, column widths, hidden columns, freeze panes, and header rotation. All keys are optional.

```python
fmt = {
    'styles':            {...},   # named style definitions
    'columns':           {...},   # wildcard pattern → column properties
    'rows':              {...},   # row index → row properties
    'freeze':            'D2',    # freeze panes cell reference
    'header_auto_rotate': 1.5,   # auto-rotate long headers
    'min_column_width':  4,       # minimum auto-sized column width
    'max_column_width':  40,      # maximum auto-sized column width
    'auto_filter':       True,    # enable dropdown filter on header row
}

with ExcelWriter(file='report.xlsx', formatting=fmt) as writer:
    writer.write_batch(cursor, sheet_name='Data')
```

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

> **Note:** Using a built-in name in `formatting['styles']` logs a `WARNING` and the definition is ignored. Choose a different name for your custom style.

> **Note:** Named styles are embedded in the workbook file. If you rerun a script that writes to an existing `.xlsx` file, styles from the previous run are already baked in — changed style definitions won't take effect. Delete the file before rerunning to pick up style changes.

---

### Column Rules

`formatting['columns']` maps patterns to per-column properties. Three pattern forms are supported, all matched **case-insensitively**:

- **Glob** — [`fnmatch`](https://docs.python.org/3/library/fnmatch.html) wildcards (`*`, `?`, `[seq]`): `'*_fee*'`, `'resv_*'`
- **Literal** — exact column name: `'notes'`
- **Range** — `'start:end'` applies to all columns from `start` through `end` inclusive, based on their position in the result set. Open-ended forms: `':end'` (from first column) and `'start:'` (to last column). Raises `ValueError` if either endpoint isn't found. A pattern containing `:` is only treated as a range if it has no wildcard characters and doesn't itself match a column name.

```python
'columns': {
    '*_fee*':           {'format': 'fmt_fees', 'header_format': 'header_vert_style'},
    'resv_*':           {'hidden': 1},
    'resv_*_desc':      {'hidden': 0, 'comment': 'Additional columns are hidden'},
    'notes':            {'width': 40},
    'credit_hrs:enrl':  {'format': 'fmt_numeric'},   # range
    ':subj_code':       {'hidden': 1},               # hide everything up to subj_code
}
```

**Column properties:**

| Key | Type | Effect |
|---|---|---|
| `format` | style name or inline dict | Applied to every **data** cell in this column |
| `style` | callable | `lambda rec: style_name_or_None` — per-cell conditional style; overrides `format` and row styles when non-None |
| `header_format` | style name or inline dict | Applied to the **header** cell only; owns the cell entirely (include `font: {bold: True}` if needed) |
| `width` | float | Column width in Excel character units; overrides auto-sizing |
| `hidden` | 0 or 1 | Hide (`1`) or explicitly un-hide (`0`) the column |
| `comment` | string | Adds an Excel comment/note to the header cell |
| `filter` | 0 or 1 | Show a filter dropdown on this column's header; hides dropdowns on all other columns |
| `group_header` | string | Merged super-header label above this column range (range patterns only) |

**Precedence:** Rules are applied in definition order. Later patterns override earlier ones *per property*, so you can use a broad pattern to set a default and a narrower pattern to override it:

```python
'columns': {
    'resv_*':      {'hidden': 1},
    'resv_*_desc': {'hidden': 0, 'comment': 'Additional columns are hidden'},
}
```

**Per-cell conditional styles** use a `style` callable in the column rule. The lambda receives the full record and returns a style name (or `None`). This overrides both the column's static `format` and any row-level style when non-None:

```python
'columns': {
    'wait_capacity': {'style': lambda rec: 'fmt_warn' if rec.wait_capacity < 10 else None},
}
```

**Group headers** use `group_header` on a range pattern to add a merged super-header row above the column names. Only range patterns are supported — using `group_header` on a wildcard logs a warning and is ignored. Columns not covered by any group are left blank in the group header row.

```python
'columns': {
    'credit_hrs:tuition_waiver_ind': {'format': 'fmt_billing',    'group_header': 'Billing'},
    'enrl:wait_avail':               {'format': 'fmt_enrollment', 'group_header': 'Enrollment'},
    'resv_1_desc:resv_5_desc':       {'hidden': 1,                'group_header': 'Reservations'},
    'crse_fee_amount:sec_fees':      {'group_header': 'Fees'},
}
```

Group headers are written bold and centered in row 1; column headers shift to row 2; data starts at row 3. The freeze pane default shifts from `'A2'` to `'A3'` automatically.

**Inline style dicts** work anywhere a style name is accepted. Equivalent inline dicts are deduplicated automatically:

```python
'columns': {
    'gpa': {'format': {'number_format': '0.00', 'alignment': {'horizontal': 'center'}}},
}
```

---

### Row Formatting

`formatting['rows']` is a dict keyed by row index. Index `0` is the **header** row; positive integers are 1-based **data** row indices.

```python
'rows': {
    0: {'height': 120},          # header row: set height
    1: {'height': 30},           # first data row: set height
    'style': lambda rec: 'fmt_alerts' if rec['status'] == 'OVERDUE' else None,
}
```

**Alternating row colors** use the `'odd'` and `'even'` keys — no callable needed:

```python
'rows': {
    'odd':  {'format': 'fmt_stripe'},   # rows 1, 3, 5, …
    'even': {'format': 'fmt_alt'},      # rows 2, 4, 6, …
}
```

You can define only one if you only want every-other-row coloring:

```python
'rows': {'odd': {'format': 'fmt_stripe'}}   # only odd rows get a background
```

**The `'style'` key** accepts a callable that receives each data record and returns a style name (or `None`). This enables conditional highlighting and status indicators:

```python
'rows': {
    'style': lambda rec: 'fmt_alerts' if rec['status'] == 'OVERDUE' else None,
}
```

**Style cascade:** column `format` is applied first; `'odd'`/`'even'` or `'style'` row format overrides it; `hyperlink_style` (from `LinkedExcelWriter`) overrides both.

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

**Header row height** is computed automatically from the longest rotated header name using `height_factor` (default 6.5 pt/character) unless `rows[0]['height']` is set explicitly. `REG_AUTH_ACTIVE_CDE` (19 chars) produces a height of ~123 pt.

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

### Complete Example

```python
fmt = {
    'styles': {
        'fmt_fees':   {'bg_color': '#d5f1cc'},
        'fmt_rest':   {'bg_color': '#e3f3fe'},
        'fmt_coreq':  {'bg_color': '#ffeed9'},
    },
    'columns': {
        'CORQ_*':       {'format': 'fmt_coreq'},
        '*_FEE*':       {'format': 'fmt_fees'},
        'res*':         {'format': 'fmt_rest'},
        'RESV*':        {'hidden': 1},
        'RESV_*_DESC':  {'hidden': 0},
        'TERM_CODE':    {'format': 'fmt_fees'},
    },
    'rows': {
        'style': lambda rec: 'fmt_fees' if rec['waitlisted'] else None,
    },
    'freeze':               'D2',
    'header_auto_rotate':   {'min_length': 8, 'ratio': 2.5},
    'min_column_width':     3,
}

with ExcelWriter(file='crn_review.xlsx', formatting=fmt) as writer:
    for term_code in active_terms:
        cursor.execute_file('crn_review.sql', {'term_code': term_code})
        writer.write_batch(cursor, sheet_name=term_code)
```

---

## Per-sheet Formatting

The `formatting` dict is set at the writer level and applies to all sheets written by that writer instance. When different sheets need different formatting, use separate writer instances — `ExcelWriter` opens existing workbooks without overwriting other sheets:

```python
fees_fmt   = {'columns': {'*_fee*': {'format': 'fmt_fees'}}, ...}
roster_fmt = {'columns': {'gpa': {'format': 'fmt_pct'}}, ...}

ExcelWriter(fees_data,   'report.xlsx', sheet_name='Fees',   formatting=fees_fmt).write()
ExcelWriter(roster_data, 'report.xlsx', sheet_name='Roster', formatting=roster_fmt).write()
```

Each writer opens the file, adds its sheet with its own formatting, saves, and closes.

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

| Parameter | Required | Description |
|---|---|---|
| `name` | Yes | Identifier used in `links=` dict |
| `source_sheet` | Yes* | Sheet whose rows are the link targets |
| `key_column` | Yes* | Column that uniquely identifies each row |
| `text_template` | No | Python format string for link display text (uses `str.format_map`) |
| `url_template` | No | Python format string for external URL |
| `missing_text` | No | Fallback text when a key can't be resolved |
| `external_only` | No | If `True`, generate URLs from current row without caching; reusable across sheets |

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
| `rows` | `dict` | `{}` | Row height/style; `0` = header; `'odd'`/`'even'` = alternating styles; `'style'` = callable |
| `freeze` | `str \| None` | `'A2'` | Freeze panes cell reference |
| `header_auto_rotate` | `float \| dict` | off | Auto-rotate long headers; see [above](#auto-rotating-headers) |
| `min_column_width` | `float` | `6` | Floor for auto-sized column widths |
| `max_column_width` | `float` | `60` | Ceiling for auto-sized column widths |
| `auto_filter` | `bool` | `False` | Enable dropdown filter on header row |

**Column rule keys:**

| Key | Type | Description |
|---|---|---|
| `format` | style name or dict | Style applied to data cells |
| `header_format` | style name or dict | Style applied to the header cell only |
| `width` | float | Override auto-sized width |
| `hidden` | 0 or 1 | Hide or explicitly un-hide the column |
| `comment` | string | Excel comment/note on the header cell |
| `filter` | 0 or 1 | Show filter dropdown on this column; hides dropdowns on all others |
| `group_header` | string | Merged super-header label (range patterns only) |

**`header_auto_rotate` dict keys:**

| Key | Default | Description |
|---|---|---|
| `ratio` | `1.5` | `header_len > data_width × ratio` to trigger rotation |
| `min_length` | `8` | Minimum header character count to be considered for rotation |
| `height_factor` | `6.5` | Points per character for auto header row height calculation |
