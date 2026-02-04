# Record Objects

The **Record** class is DBTK's universal data structure. Every cursor query and file reader returns Records, and every `*Surge` class expects them as input. Understanding Record unlocks the full power of DBTK's pipeline architecture.

## Why Record?

ETL pipelines process hundreds of thousands or millions of rows, all with the same columns. This creates a design tension:

- **Dicts** give you flexible key-based access, but store keys on every instance. A million rows means a million copies of the same column names.
- **Tuples** are memory-efficient, but force you to remember column positions. `row[7]` is fragile and unreadable.
- **Named tuples** are immutable - you can't add or modify fields during transformation.

Record solves this by storing column names once on a shared class while each instance holds only its values. You get dict-like flexibility with tuple-like memory efficiency.

```python
# All of these work on the same Record object:
row['name']              # Dict-style access
row.name                 # Attribute access
row[1]                   # Index access
row[1:3]                 # Slicing
name, email = row[1:3]   # Unpack a slice
row.get('phone', 'N/A')  # Safe access with default
dict(row)                # Convert to dict when needed
'\t'.join(str(v) for v in row.values())  # Join values
```

## Access Patterns

### Dict-Style Access

```python
# Key access
user_name = row['name']

# Safe access with default
phone = row.get('phone', 'unlisted')

# Check if column exists
if 'email' in row:
    send_notification(row['email'])

# Iterate over keys
for column in row.keys():
    print(column)

# Iterate over key-value pairs
for column, value in row.items():
    print(f"{column}: {value}")
```

### Attribute Access

```python
# Clean, readable syntax
print(f"Welcome, {row.name}!")

# Works with normalized names (see below)
row.employee_id  # Works even if column is "Employee_ID"
```

### Index and Slice Access

```python
# Positional access
first_col = row[0]
last_col = row[-1]

# Slicing
first_three = row[:3]
middle = row[2:5]

# Unpack specific positions
id, name, email = row[:3]
```

### Tuple Unpacking

```python
# Unpack entire row
id, name, email, created = row

# Unpack in loop
for id, name, status in cursor:
    process(id, name, status)
```

## Normalized Field Names

Record stores both original column names and normalized versions. Normalization exists because Python attribute syntax doesn't allow `row.FULL NAME` or `row.Employee-ID` - only valid Python identifiers work as attributes.

Normalization lowercases and replaces non-alphanumeric characters with underscores:

| Original Column | Normalized Name |
|-----------------|-----------------|
| `Employee_ID`   | `employee_id`   |
| `FULL NAME`     | `full_name`     |
| `Start Year`    | `start_year`    |
| `Contact Email!`| `contact_email` |

**Access rules:**
- **Normalized names** work with both attribute access (`row.employee_id`) and dict access (`row['employee_id']`)
- **Original names** only work with dict access (`row['FULL NAME']`) - they may contain characters invalid for Python attributes

```python
cursor.execute("SELECT Employee_ID, FULL NAME FROM users")
for row in cursor:
    # Normalized - works both ways
    row.employee_id       # Attribute access
    row['employee_id']    # Dict access

    # Original - dict only
    row['FULL NAME']      # Works
    row.FULL NAME         # SyntaxError!
```

### Why This Matters

When defining Table field mappings, use normalized names and your code works regardless of source column casing:

```python
# This Table config works whether the CSV has
# "Employee ID", "employee_id", or "EMPLOYEE_ID"
employee_table = Table('employees', {
    'emp_id': {'field': 'employee_id', 'key': True},
    'full_name': {'field': 'full_name'},
    'hire_date': {'field': 'start_date', 'fn': 'date'},
}, cursor=cursor)
```

You write the mapping once. DBTK handles the column name variations.

### Collision Handling

If normalization creates duplicates, Record appends `_2`, `_3`, etc:

```python
# Columns: "Status", "STATUS", "status!"
# Normalized: "status", "status_2", "status_3"
```

### Accessing Both Forms

```python
# Get original column names
columns = row.keys()                    # ['Employee_ID', 'FULL NAME']

# Get normalized column names
columns = row.keys(normalized=True)     # ['employee_id', 'full_name']

# Convert to dict with normalized keys
data = row.to_dict(normalized=True)     # {'employee_id': 123, 'full_name': 'Aang'}
```

## Modifying Records

Records are mutable - you can add, update, and delete fields during transformation.

### Update Existing Fields

```python
# By key
row['status'] = 'active'

# By attribute
row.status = 'active'

# Multiple updates
row.update({'status': 'active', 'modified': datetime.now()})
```

### Add New Fields

```python
# Add computed fields
row['full_name'] = f"{row.first_name} {row.last_name}"

# Add via attribute
row.processed_at = datetime.now()
```

### Delete Fields

```python
# Remove a field
del row['temp_field']

# Or use pop to get the value
old_status = row.pop('old_status', None)
```

## Conversion Methods

```python
# Convert to dict (standard Python idiom)
data = dict(row)

# Use to_dict() when you need normalized keys
data = row.to_dict(normalized=True)

# Get just the values as a tuple
values = tuple(row.values())
```

## Debugging

### Pretty Print

```python
row.pprint()
# Employee_ID : 12345
# FULL NAME   : Avatar Aang
# Start Date  : 2024-01-15
# Status      : active

# With normalized names
row.pprint(normalized=True)
# employee_id : 12345
# full_name   : Avatar Aang
# start_date  : 2024-01-15
# status      : active
```

### String Representation

```python
print(row)
# Record('employee_id': 12345, 'full_name': 'Avatar Aang', ...)

print(repr(row))
# Record(12345, 'Avatar Aang', datetime.date(2024, 1, 15), 'active')
```

## Performance Note

Record achieves tuple-like speed through a subclass-per-query pattern. Each time a cursor executes a query, DBTK dynamically creates a Record subclass with those column names as class attributes. Every row returned from that query shares the subclass - column names exist once in memory, not on every row.

```python
cursor.execute("SELECT id, name, email FROM users")
# DBTK creates a Record subclass with _fields = ['id', 'name', 'email']
# All 10,000 rows share this class, each storing only [id_val, name_val, email_val]

cursor.execute("SELECT order_id, total, status FROM orders")
# New query = new subclass with different fields
# Previous subclass is eligible for garbage collection
```

This is why Record can offer dict-like convenience without dict-like memory overhead when processing millions of rows. The same pattern applies to file readers - each reader creates a Record subclass based on the file's header row.
