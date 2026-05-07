"""
Build the 1927 baseball dataset from the Sean Lahman Baseball Database.

Downloads: https://github.com/chadwickbureau/baseballdatabank/archive/refs/heads/master.zip
Extract the zip and set LAHMAN_DIR to the 'core' subdirectory.

Writes output/1927_baseball.parquet — one row per player with biographical,
batting, and fielding stats.  Run this once; formatted_excel.py reads the
parquet directly.
"""

from pathlib import Path

import polars as pl

LAHMAN_DIR = Path(r'~\Downloads\lahman')
YEAR = 1927

# Top 4 teams from each league
TEAMS = (
    pl.read_csv(LAHMAN_DIR / 'Teams.csv')
    .filter(
        (pl.col('yearID') == YEAR) &
        (pl.col('lgID').is_in(['AL', 'NL'])) &
        (pl.col('Rank') <= 4)
    )
    .select(['teamID', 'name', 'lgID', 'Rank'])
    .sort(['lgID', 'Rank'])
)


def load_team(team_id: str) -> pl.DataFrame:
    people = (
        pl.read_csv(LAHMAN_DIR / 'People.csv')
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

    batting = (
        pl.read_csv(LAHMAN_DIR / 'Batting.csv')
        .filter((pl.col('yearID') == YEAR) & (pl.col('teamID') == team_id))
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
            (pl.col('H') / pl.col('AB')).alias('AVG'),
            ((pl.col('H') + pl.col('BB') + pl.col('HBP')) /
             (pl.col('AB') + pl.col('BB') + pl.col('HBP') + pl.col('SF'))).alias('OBP'),
            ((pl.col('H') + pl.col('2B') + 2 * pl.col('3B') + 3 * pl.col('HR')) /
             pl.col('AB')).alias('SLG'),
        )
        .drop(['HBP', 'SF'])
    )

    fielding_raw = (
        pl.read_csv(LAHMAN_DIR / 'Fielding.csv')
        .filter((pl.col('yearID') == YEAR) & (pl.col('teamID') == team_id))
    )
    primary_pos = (
        fielding_raw
        .group_by(['playerID', 'POS']).agg(pl.col('G').sum().alias('G_pos'))
        .sort('G_pos', descending=True)
        .group_by('playerID').first()
        .select(['playerID', 'POS'])
    )
    fielding = (
        fielding_raw
        .group_by('playerID').agg(
            pl.col('PO').sum(), pl.col('A').sum(),
            pl.col('E').sum(),  pl.col('DP').sum(),
        )
        .join(primary_pos, on='playerID', how='left')
        .with_columns(
            ((pl.col('PO') + pl.col('A')) /
             (pl.col('PO') + pl.col('A') + pl.col('E'))).alias('FPCT')
        )
    )

    return (
        batting
        .join(fielding, on='playerID', how='left')
        .join(people,   on='playerID', how='left')
        .select([
            'name', 'POS', 'bats', 'throws', 'age', 'birth_year',
            'birthCity', 'birthState', 'birthCountry', 'height', 'weight',
            'G', 'AB', 'R', 'H', '2B', '3B', 'HR', 'RBI', 'BB', 'SO',
            'AVG', 'OBP', 'SLG',
            'PO', 'A', 'E', 'DP', 'FPCT',
        ])
        .rename({
            'name': 'Name',       'POS': 'Pos',          'bats': 'Bats',
            'throws': 'Throws',   'age': 'Age',          'birth_year': 'Birth_Year',
            'birthCity': 'Birth_City', 'birthState': 'Birth_State', 'birthCountry': 'Birth_Country',
            'height': 'Height',   'weight': 'Weight',
            'G': 'Games_Played',  'AB': 'At_Bats',       'R': 'Runs',
            'H': 'Hits',          '2B': 'Doubles',       '3B': 'Triples',
            'HR': 'Home_Runs',    'RBI': 'Runs_Batted_In', 'BB': 'Walks',
            'SO': 'Strikeouts',   'AVG': 'Batting_Avg',  'OBP': 'On_Base_Pct',
            'SLG': 'Slugging_Pct', 'PO': 'Putouts',      'A': 'Assists',
            'E': 'Errors',        'DP': 'Double_Plays',  'FPCT': 'Fielding_Pct',
        })
        .sort('Name')
    )


if __name__ == '__main__':
    out_dir = Path(__file__).parent / 'output'
    out_dir.mkdir(exist_ok=True)

    frames = []
    for row in TEAMS.iter_rows(named=True):
        df = load_team(row['teamID'])
        df = df.with_columns(pl.lit(row['name']).alias('team_name'))
        frames.append(df)
        print(f"  {row['lgID']} #{row['Rank']}  {row['name']:30s}  {len(df)} players")

    combined = pl.concat(frames)
    out_file = out_dir / '1927_baseball.parquet'
    combined.write_parquet(out_file)
    print(f"\nWrote {len(combined)} rows → {out_file}")
