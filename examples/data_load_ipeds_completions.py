"""
IPEDS Completions Loader: Identity Resolution and FK Validation in a Multi-Stage ETL

This example demonstrates two complementary dbtk patterns on a real dataset:
**ValidationCollector** and **IdentityManager**. Using NCES IPEDS data, it loads
degree completion counts from ~4,000 institutions into a normalized data warehouse,
resolving institutional identities and validating academic program codes along the way.

The Challenge
-------------
The IPEDS completions file (C2022_A.csv) references institutions and CIP academic
program codes by their source-system identifiers. To load this data into a normalized
warehouse you need to:

* Resolve each IPEDS UNITID to your internal ``institution_id`` — and gracefully handle
  institutions that are out of scope (2-year colleges, for-profit schools).
* Validate each CIP program code against your reference table — and capture any codes
  that appear in the completions data but are missing from the reference table, so they
  can be bulk-inserted afterward rather than dropped.
* Track the full lifecycle of each institution across runs: which were resolved, which
  were not found, which had insert errors — and persist that state so a failed run can
  resume without re-querying everything.

Features Demonstrated
---------------------
* **ValidationCollector with preload**: Load all known CIP codes at startup so every
  cache miss is definitively a new code, with no per-row database queries.
* **collect_new / get_new_records**: Accumulate extra fields on newly-discovered codes
  during row processing, then bulk-insert them in a single pass after the main load.
* **IdentityManager**: Resolve UNITID (IPEDS source key) → institution_id (internal key),
  cache results to avoid redundant queries, and track NOT_FOUND institutions with
  structured errors.
* **alternate_keys**: Store the OPE ID alongside institution_id so each entity record
  carries both institutional identifiers for use by downstream financial-aid integrations.
* **save_state / load_state**: Persist the identity cache between runs for resumability.
* **Multi-stage pipeline**: Reference data → dimension table → fact table → gap fill.

The Pipeline
------------
1. **Load CIP code reference** (cip_codes):
   - Insert all codes from CIPCode2020.csv into the reference table.
   - Preload into ValidationCollector — every subsequent cache miss is a genuinely new code.

2. **Load institutions** (institutions):
   - Filter HD2022.csv to 4-year public and private non-profit institutions.
   - Insert into ``institutions``; the database assigns each row an auto-increment
     ``institution_id``. The IPEDS ``unitid`` and Department of Education ``opeid``
     are stored as alternate identifiers.
   - Wire up IdentityManager with a TableLookup resolver to map unitid → institution_id.

3. **Load completions** (completions):
   - Filter C2022_A.csv to first-major records (MAJORNUM=1) to avoid double-counting.
   - For each row: resolve the institution, validate the CIP code, insert the record.
   - Institutions not found (2-year, for-profit) are skipped and logged.
   - New CIP codes are collected via ``collect_new`` for later insertion.

4. **Back-fill new CIP codes** (cip_codes):
   - Bulk-insert any CIP codes discovered during Step 3 that were absent from the
     reference table (e.g. codes added in the 2022 revision cycle not yet in CIP 2020).

5. **Persist identity state**:
   - Save IdentityManager state to JSON so a re-run can restore the institution cache
     and retry any NOT_FOUND or ERROR entities.

Key Techniques
--------------
**ValidationCollector with preload**:
```python
cip_lookup = TableLookup(cur, table='cip_codes', key_cols='cip_code',
                         return_cols=['cip_code', 'cip_title'],
                         cache=TableLookup.CACHE_PRELOAD)
cip_collector = ValidationCollector(lookup=cip_lookup)
# Every cache miss is definitively a new code — no per-row DB queries
```

**collect_new captures extra fields for bulk insertion**:
```python
# Inside the row loop — safe to call unconditionally, no-ops on existing codes
completions_table.set_values(row)        # triggers cip_collector via fn pipeline
cip_collector.collect_new(               # annotates only if _recently_added=True
    row['CIPCODE'],
    cip_title=row.get('cip_title', ''),  # attach any extra fields you have
)
# After the loop:
new_records = cip_collector.get_new_records('cip_code')
# [{'cip_code': '29.0202', 'cip_title': ''}, ...]
```

**IdentityManager with alternate_keys**:
```python
institution_lookup = TableLookup(cur, table='institutions', key_cols='unitid',
                                 return_cols=['institution_id', 'unitid', 'opeid', 'name'])
institution_mgr = IdentityManager(
    source_key='unitid',
    target_key='institution_id',
    resolver=institution_lookup,
    alternate_keys=['opeid'],   # stored alongside institution_id in each entity
)
# Resolution injects institution_id back into the source row:
entity = institution_mgr.resolve(row)
if entity['_status'] == EntityStatus.RESOLVED:
    # row['institution_id'] is now set; entity['opeid'] is also available
    ...
```

**save_state / load_state for resumable runs**:
```python
institution_mgr.save_state('state/institutions.json')

# On a subsequent run:
institution_mgr = IdentityManager.load_state('state/institutions.json',
                                             resolver=institution_lookup)
institution_mgr.batch_resolve([EntityStatus.NOT_FOUND])  # retry previously missed
```

Output
------
Three populated tables:
- **cip_codes**: ~1,600 CIP academic program codes with titles (plus any discovered gaps)
- **institutions**: ~2,900 4-year public and private non-profit institutions
- **completions**: ~400K degree completion records with referential integrity maintained

Performance
-----------
Loads ~420K completion records in approximately 5-15 seconds (row-by-row due to identity
resolution). CIP preload eliminates all per-row DB queries for reference validation.

Prerequisites
-------------
Download from the NCES IPEDS Data Center at https://nces.ed.gov/ipeds/datacenter/DataFiles.aspx:

* ``HD2022.zip``   (~9 MB):  Institutional Characteristics — institution directory
* ``C2022_A.zip``  (~5 MB):  Completions — degrees by CIP code and award level

Download CIP Code list from https://nces.ed.gov/ipeds/cipcode/resources.aspx?y=56
* ``CIPCode2020.zip`` (~1 MB): CIP code reference list

Unzip all three files into ``~/Downloads/ipeds/``.

Database Schema
---------------
```sql
CREATE TABLE cip_codes (
    cip_code    VARCHAR(10)  PRIMARY KEY,
    cip_title   VARCHAR(200)
);

# May need to modify institution_id to your database's auto_increment, identity, serial
CREATE TABLE institutions (
    institution_id  INTEGER  GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,   -- internal auto-increment key
    unitid          INTEGER      NOT NULL UNIQUE,  -- IPEDS source identifier
    opeid           VARCHAR(10),                   -- Dept. of Education OPE identifier
    name            VARCHAR(200) NOT NULL,
    state           VARCHAR(2),
    sector          INTEGER
);

CREATE TABLE completions (
    institution_id  INTEGER     NOT NULL,
    cip_code        VARCHAR(10) NOT NULL,
    award_level     INTEGER     NOT NULL,
    year            INTEGER     NOT NULL,
    total           INTEGER,
    PRIMARY KEY (institution_id, cip_code, award_level, year)
);
```

See Also
--------
- data_load_imdb_subset.py: ValidationCollector used for cross-stage FK tracking
"""

import dbtk
import polars as pl
import time
from pathlib import Path
from dbtk.etl import DataSurge, Table, ValidationCollector, EntityStatus, TableLookup
from dbtk.etl.managers import IdentityManager


if __name__ == '__main__':
    dbtk.setup_logging(level='debug')
    db = dbtk.connect('example')
    cur = db.cursor()

    cur.execute('TRUNCATE TABLE cip_codes')
    cur.execute('TRUNCATE TABLE institutions')
    cur.execute('TRUNCATE TABLE completions')

    data_dir = Path.home() / 'Downloads' / 'ipeds'
    state_dir = Path('output')
    state_dir.mkdir(exist_ok=True)

    st = time.monotonic()

    # -----------------------------------------------------------------------
    # Stage 1: Load CIP code reference table
    # -----------------------------------------------------------------------
    # CIPCode2020.csv columns: CIPCode, CIPTitle, IsNew, Action
    cip_codes_table = Table('cip_codes', columns={
        'cip_code':  {'field': 'CIPCode',  'primary_key': True, 'fn': 'str.strip:="'}, # remove Excel junk in column e.g. '="01.0101"'
        'cip_title': {'field': 'CIPTitle', 'nullable': False,  'fn': 'maxlen:200'},
    }, cursor=cur)

    with dbtk.readers.get_reader(data_dir / 'CIPCode2020.csv') as reader:
        surge = DataSurge(cip_codes_table)
        surge.insert(reader)

    # Preload all known CIP codes into the collector — a cache miss later means a
    # genuinely new code, so no per-row database queries are needed during load.
    cip_lookup = TableLookup(cur,
                             table='cip_codes',
                             key_cols='cip_code',
                             return_cols=['cip_code', 'cip_title'],
                             cache=TableLookup.CACHE_PRELOAD)
    cip_collector = ValidationCollector(lookup=cip_lookup)

    # -----------------------------------------------------------------------
    # Stage 2: Load institutions — 4-year public and private non-profit only
    # -----------------------------------------------------------------------
    # HD2022.csv ICLEVEL: 1=4-year, 2=2-year, 3=<2-year
    # HD2022.csv CONTROL: 1=public, 2=private nonprofit, 3=private for-profit
    institution_cols = {
        'unitid': {'field': 'UNITID', 'nullable': False},
        'opeid':  {'field': 'OPEID'},       # OPE ID used by financial aid systems
        'name':   {'field': 'INSTNM', 'nullable': False, 'fn': 'maxlen:200'},
        'state':  {'field': 'STABBR'},
        'sector': {'field': 'SECTOR', 'fn': 'int'},
    }
    institutions_table = Table('institutions', columns=institution_cols, cursor=cur)
    # automatic decompression of a zip file
    with dbtk.readers.get_reader(data_dir / 'hd2024.zip', null_values=['', '-2']) as reader:
        reader.add_filter(lambda x: x.iclevel == '1' and x.control in ('1', '2'))
        surge = DataSurge(institutions_table)
        surge.insert(reader)

    # Resolver: look up institution_id and opeid by unitid.
    # IdentityManager caches each resolved entity so each UNITID hits the database
    institution_lookup = TableLookup(cur,
                                     table='institutions',
                                     key_cols='unitid',
                                     return_cols=['institution_id', 'unitid', 'opeid', 'name'],
                                     cache=TableLookup.CACHE_PRELOAD)

    # There will be a lot of institutions here that we didn't load in step 2
    # Instead of querying the database each time a new unitid is encountered
    # IdentityManager will pull from the cached values on institution_lookup.
    # Cache misses will be returned as "not found" without requerying the database
    # due to the institution_lookup.exhaustive = True flag (set by preloading the cache).
    institution_mgr = IdentityManager(
        source_key='unitid',
        target_key='institution_id',
        resolver=institution_lookup,
        alternate_keys=['opeid'],    # track OPE ID alongside institution_id per entity
    )

    # -----------------------------------------------------------------------
    # Stage 3: Load completions — resolve identity and validate CIP per row
    # -----------------------------------------------------------------------
    # C2022_A.csv MAJORNUM: 1=first major, 2=second major — filter to avoid double-counting
    # C2022_A.csv AWLEVEL:  1=<1yr cert, 3=associates, 5=bachelors, 7=masters, 9=doctoral
    completions_table = Table('completions', columns={
        'institution_id': {'field': 'institution_id', 'key': True},
        'cip_code':       {'field': 'CIPCODE',        'key': True, 'fn': cip_collector},
        'award_level':    {'field': 'AWLEVEL',        'key': True, 'fn': 'int'},
        'year':           {'field': 'year',           'key': True},
        'total':          {'field': 'CTOTALT',        'fn': 'int'},
    }, cursor=cur)

    df_completions = pl.read_csv(
        data_dir / 'C2022_A.csv',
        null_values=['', '.'],
        schema_overrides={'CIPCODE': pl.Utf8}  # cip code will be interpreted as float otherwise
    ).filter(
        pl.col('MAJORNUM') == 1          # first major only
    ).with_columns(
        pl.lit(2022).alias('year')       # tag with survey year
    )

    skipped = 0
    params = []
    completions_insert = completions_table.get_sql('insert')
    with dbtk.readers.DataFrameReader(df_completions) as reader:
        for row in reader:
            # Resolve UNITID → institution_id.  Contrived in this example, but
            entity = institution_mgr.resolve(row['UNITID'])

            if entity['_status'] != EntityStatus.RESOLVED:
                continue

            # Inject the resolved internal key so completions_table can bind it.
            row['institution_id'] = entity['institution_id']

            # set_values runs cip_collector via the fn pipeline, flagging _recently_added
            # if this CIP code has not been seen before.
            completions_table.set_values(row)
            # insert in batches instead of hitting the database on every row
            if completions_table.is_ready('insert'):
                params.append(completions_table.get_bind_params('insert'))
            if len(params) == 5000:
                cur.executemany(completions_insert, params)
                params = []
        if params:
            cur.executemany(completions_insert, params)

    db.commit()

    # -----------------------------------------------------------------------
    # Reporting and state persistence
    # -----------------------------------------------------------------------
    stats = institution_mgr.calc_stats()

    # Persist the full entity cache — institution_id, opeid, status, and errors —
    # so a subsequent run can call IdentityManager.load_state() and skip re-querying
    # institutions that were already resolved.
    institution_mgr.save_state(state_dir / 'institutions.json')

    et = time.monotonic()

    total_cip = len(cip_collector.existing) + len(cip_collector.added)
    print(f'\nLoaded IPEDS 2022 completions in {et - st:.1f}s')
    print(f'  Institutions resolved:  {stats["resolved"]:,}')
    print(f'  Institutions not found: {stats["not_found"]:,}  (2-year / for-profit — intentionally excluded)')
    print(f'  Completion rows skipped: {skipped:,}')
    print(f'  CIP codes — reference: {len(cip_collector.existing):,}, newly discovered: {len(cip_collector.added):,}')
    if len(cip_collector.added) <= 10:
        print(f'    New CIP codes encountered: {", ".join(cip_collector.added)}')
    print(f'\n  Institution state saved → {state_dir / "institutions.json"}')
    print(f'  Resume with: IdentityManager.load_state(..., resolver=institution_lookup)')
