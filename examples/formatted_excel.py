"""
Exports a formatted Excel workbook with 1927 baseball stats.

Requires output/1927_baseball.parquet — run prep_1927_data.py first.
"""

from pathlib import Path

import polars as pl

import dbtk.readers
import dbtk.writers

DATA_FILE = Path(__file__).parent / 'output' / '1927_baseball.parquet'
OUT_FILE  = Path(__file__).parent / 'output' / 'MLB-1927.xlsx'

HEADERS = [
    'Name', 'Pos', 'Bats', 'Throws', 'Age', 'Birth Year',
    'Birth City', 'Birth State', 'Birth Country', 'Height', 'Weight',
    'Games Played', 'At Bats', 'Runs', 'Hits', 'Doubles', 'Triples', 'Home Runs',
    'Runs Batted In', 'Walks', 'Strikeouts',
    'Batting Avg', 'On Base Pct', 'Slugging Pct',
    'Putouts', 'Assists', 'Errors', 'Double Plays', 'Fielding Pct',
]

fmt = {
    'styles': {
        'demo_style':     {'bg_color': '#C2E6F6'},
        'hits_style':     {'bg_color': '#D3F5C1'},
        'fielding_style': {'bg_color': '#F5ECC1'},
        'alert_style':    {'bg_color': '#E9B47A'},
        'stripe_style':   {'bg_color': '#DDDDDD'},
    },
    'columns': {
        'pos:weight':  {'format': 'demo_style',     'group_label': 'Demographics'},
        'pos':         {'filter': 1},
        'g:slg':       {'format': 'hits_style',     'group_label': 'Batting',
                        'width': 5, 'header_format': 'header_vert_style'},
        'po:fpct':     {'format': 'fielding_style', 'group_label': 'Fielding',
                        'width': 5, 'header_format': 'header_vert_style'},
        'hr':          {'style': lambda rec: 'alert_style' if rec.hr >= 15 else None},
        'avg:slg':     {'format': {'number_format': '0.000'}, 'width': 7},
        'fpct':        {'format': {'number_format': '0.000'}, 'width': 7},
    },
    'rows': {
        'data': {'odd': {'format': 'stripe_style'}},
    },
    'freeze':              'B3',
    'min_column_width':    4,
    'header_auto_rotate':  {'min_length': 4},
}

if __name__ == '__main__':
    all_data = pl.read_parquet(DATA_FILE)

    with dbtk.writers.ExcelWriter(None, OUT_FILE, formatting=fmt) as writer:
        for team_name in all_data['team_name'].unique(maintain_order=True):
            team_df = all_data.filter(pl.col('team_name') == team_name).drop('team_name')
            reader  = dbtk.readers.DataFrameReader(team_df, add_row_num=False)
            writer.write_batch(reader, sheet_name=team_name, headers=HEADERS)
            print(f"  {team_name}")

    print(f"\nSaved → {OUT_FILE}")
