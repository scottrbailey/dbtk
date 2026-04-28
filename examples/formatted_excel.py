"""
Exports "report quality" Excel workbook with baseball stats for the 1927 season.

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
COLUMNS = ['Name', 'Pos', 'Bats', 'Throws', 'Age', 'Birth Year', 'Birth City', 'Birth State', 'Birth Country',
           'Height', 'Weight', 'Games Played', 'At Bats', 'Runs', 'Hits', 'Doubles', 'Triples', 'Home Runs',
           'Runs Batted In', 'Walks', 'Strikeouts', 'Batting Avg', 'On Base Percentage', 'Slugging Percentage',
           'Putouts', 'Assists', 'Errors', 'Double Plays', 'Fielding Pct']

def load_team(year: int = YEAR, team_id: str = TEAM, lahman_dir: Path = LAHMAN_DIR) -> pl.DataFrame:
    # ── Biographical ──────────────────────────────────────────────────────────
    people = (
        pl.read_csv(lahman_dir / 'People.csv')
        .select(['playerID', 'nameFirst', 'nameLast',
                 'birthYear', 'birthCity', 'birthState', 'birthCountry',
                 'height', 'weight', 'bats', 'throws'])
        .with_columns(
            (pl.col('nameFirst') + ' ' + pl.col('nameLast')).alias('name'),
            pl.col('birthYear').alias('birth_year'),
            (pl.lit(year) - pl.col('birthYear')).alias('age'),
        )
        .drop(['nameFirst', 'nameLast', 'birthYear'])
    )

    # ── Batting ───────────────────────────────────────────────────────────────
    batting = (
        pl.read_csv(lahman_dir / 'Batting.csv')
        .filter((pl.col('yearID') == year) & (pl.col('teamID') == team_id))
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
        .filter((pl.col('yearID') == year) & (pl.col('teamID') == team_id) )
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
    out_fn = Path(__file__).parent / 'output' / f'MLB-{YEAR}.xlsx'
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
            'g:slg': {'format': 'hits_style', 'group_label': 'Batting', 'width': 5, 'header_format': 'header_vert_style'},
            'po:fpct': {'format': 'fielding_style', 'group_label': 'Fielding', 'width': 5, 'header_format': 'header_vert_style'},
            'hr': {'style': lambda rec: 'alert_style' if rec.hr >= 15 else None},
            'avg:slg': {'format': {'number_format': '0.000'}, 'width': 7},
            'fpct': {'format': {'number_format': '0.000'}, 'width': 7},
        },
        'rows': {
            'data': {'odd': {'format': 'stripe_style'}}
        },
        'freeze': 'B3',
        'min_column_width': 4,
        'header_auto_rotate': {'min_length': 4}
    }

    writer = dbtk.writers.ExcelWriter(None, out_fn, formatting=fmt, headers=COLUMNS)
    # Get top 4 teams from both American League and National League for YEAR
    teams_df = df = pl.read_csv(LAHMAN_DIR / 'Teams.csv').filter(
        (pl.col('yearID') == YEAR) & (pl.col('lgID').is_in(['AL', 'NL'])) & (pl.col('Rank') <= 4)
    ).select(['teamID', 'name'])
    for team in teams_df.iter_rows(named=True):
        df = load_team(year=YEAR, team_id=team['teamID'])
        reader = dbtk.readers.DataFrameReader(df, add_row_num=False)
        writer.write_batch(reader, sheet_name=team['name'])
    writer.close()
