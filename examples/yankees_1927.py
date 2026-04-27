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

import polars as pl

LAHMAN_DIR = Path('/path/to/baseballdatabank-master/core')

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
    print(df)
