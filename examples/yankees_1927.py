"""
1927 New York Yankees — example dataset for ExcelWriter demonstrations.

Data source: Sean Lahman Baseball Database
  https://github.com/chadwickbureau/baseballdatabank/archive/refs/heads/master.zip

Extract the zip and set LAHMAN_DIR to the 'core' subdirectory, e.g.:
  LAHMAN_DIR = '/path/to/baseballdatabank-master/core'

Produces a Polars DataFrame with one row per player, combining:
  - Biographical data (name, age, position, handedness, height, weight)
  - Batting stats    (G, AB, R, H, 2B, 3B, HR, RBI, BB, SO, AVG, OBP, SLG)
  - Fielding stats   (PO, A, E, DP, FPCT) for the player's primary position
"""

from pathlib import Path

import dbtk.readers
import polars as pl

LAHMAN_DIR = Path(r'~\Downloads\lahman')

TEAM   = 'NYA'
YEAR   = 1927


def load_yankees_1927(lahman_dir: Path = LAHMAN_DIR) -> pl.DataFrame:
    # ── Biographical ──────────────────────────────────────────────────────────
    people = (
        pl.read_csv(lahman_dir / 'People.csv')
        .select(['playerID', 'nameFirst', 'nameLast',
                 'birthYear', 'birthCity', 'birthState', 'birthCountry',
                 'height', 'weight', 'bats', 'throws'])
        .with_columns(
            (pl.col('nameFirst') + ' ' + pl.col('nameLast')).alias('name'),
            pl.col('birthYear').alias('birth_year'),
            (pl.lit(YEAR) - pl.col('birthYear')).alias('age'),
        )
        .drop(['nameFirst', 'nameLast', 'birthYear'])
    )

    # ── Batting ───────────────────────────────────────────────────────────────
    batting = (
        pl.read_csv(lahman_dir / 'Batting.csv')
        .filter((pl.col('yearID') == YEAR) & (pl.col('teamID') == TEAM))
        .group_by('playerID').agg(
            pl.col('G').sum(),
            pl.col('AB').sum(),
            pl.col('R').sum(),
            pl.col('H').sum(),
            pl.col('2B').sum(),
            pl.col('3B').sum(),
            pl.col('HR').sum(),
            pl.col('RBI').sum(),
            pl.col('BB').sum(),
            pl.col('SO').sum(),
            pl.col('HBP').fill_null(0).sum(),
            pl.col('SF').fill_null(0).sum(),
        )
        .with_columns(
            # Batting average: H / AB
            (pl.col('H') / pl.col('AB')).alias('AVG'),
            # On-base percentage: (H + BB + HBP) / (AB + BB + HBP + SF)
            ((pl.col('H') + pl.col('BB') + pl.col('HBP')) /
             (pl.col('AB') + pl.col('BB') + pl.col('HBP') + pl.col('SF')))
            .alias('OBP'),
            # Slugging: (H + 2B + 2×3B + 3×HR) / AB
            ((pl.col('H') + pl.col('2B') + 2 * pl.col('3B') + 3 * pl.col('HR')) /
             pl.col('AB'))
            .alias('SLG'),
        )
        .drop(['HBP', 'SF'])
    )

    # ── Fielding — primary position only ─────────────────────────────────────
    # Each player may appear multiple times (one row per position played).
    # Keep totals for the position where they played the most games.
    fielding_raw = (
        pl.read_csv(lahman_dir / 'Fielding.csv')
        .filter((pl.col('yearID') == YEAR) & (pl.col('teamID') == TEAM))
    )
    primary_pos = (
        fielding_raw
        .group_by(['playerID', 'POS']).agg(pl.col('G').sum().alias('G_pos'))
        .sort('G_pos', descending=True)
        .group_by('playerID').first()   # highest-G position per player
        .select(['playerID', 'POS'])
    )
    fielding = (
        fielding_raw
        .group_by('playerID').agg(
            pl.col('PO').sum(),
            pl.col('A').sum(),
            pl.col('E').sum(),
            pl.col('DP').sum(),
        )
        .join(primary_pos, on='playerID', how='left')
        .with_columns(
            ((pl.col('PO') + pl.col('A')) /
             (pl.col('PO') + pl.col('A') + pl.col('E')))
            .alias('FPCT')
        )
    )

    # ── Combine ───────────────────────────────────────────────────────────────
    df = (
        batting
        .join(fielding, on='playerID', how='left')
        .join(people,   on='playerID', how='left')
        .select([
            'name', 'POS', 'bats', 'throws', 'age', 'birth_year',
            'birthCity', 'birthState', 'birthCountry',
            'height', 'weight',
            'G', 'AB', 'R', 'H', '2B', '3B', 'HR', 'RBI', 'BB', 'SO',
            'AVG', 'OBP', 'SLG',
            'PO', 'A', 'E', 'DP', 'FPCT',
        ])
        .sort('name')
    )
    return df


if __name__ == '__main__':
    df = load_yankees_1927()
    reader = dbtk.readers.DataFrameReader(df, add_row_num=False)
    out_fn = Path(__file__).parent / 'output' / 'Yankees-1927.xlsx'
    fmt = {
        'styles': {
            'demo_style': {'bg_color': '#C2E6F6'},
            'hits_style': {'bg_color': '#D3F5C1'},
            'fielding_style': {'bg_color': '#F5ECC1'},
            'pct_style': {'number_format': '0.000'},
            'alert_style': {'bg_color': '#E9B47A'},
            'stripe_style': {'bg_color': '#DDDDDD'},
        },
        'columns': {
            'pos:weight': {'format': 'demo_style', 'group_label': 'Demographics'},
            'pos': {'filter': 1}, # add filter
            'g:slg': {'format': 'hits_style', 'group_label': 'Batting', 'header_format': 'header_vert_style'},
            'po:fpct': {'format': 'fielding_style', 'group_label': 'Fielding', 'header_format': 'header_vert_style'},
            'hr': {'style': lambda rec: 'alert_style' if rec.hr >= 15 else None},
            'avg:slg': {'format': {'number_format': '0.000'}, 'width': 7},
            'fpct': {'format': {'number_format': '0.000'}, 'width': 7},
        },
        'rows': {
            'data': {'odd': {'format': 'stripe_style'}}
        },
        'freeze': 'B3'
    }
    columns = ['Name', 'Pos', 'Bats', 'Throws', 'Age', 'Birth Year', 'Birth City', 'Birth State', 'Birth Country',
               'Height', 'Weight', 'Games Played', 'At Bats', 'Runs', 'Hits', 'Doubles', 'Triples', 'Home Runs',
               'Runs Batted In', 'Walks', 'Strikeouts', 'Batting Avg', 'On Base Percentage', 'Slugging Percentage',
               'Putouts', 'Assists', 'Errors', 'Double Plays', 'Fielding Pct']
    with dbtk.writers.ExcelWriter(reader, out_fn, sheet_name='Yankees', headers=columns, formatting=fmt) as writer:
        writer.write()
