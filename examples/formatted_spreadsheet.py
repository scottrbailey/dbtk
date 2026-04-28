"""
Exports a formatted multi-sheet Excel workbook with 1927 baseball stats —
one sheet per team for the top 4 finishers in each league.

output/1927_baseball.parquet is included in the repo — run directly.
prep_1927_data.py is provided if you need to rebuild it from the Lahman CSV files.

Key techniques shown:
  - Named styles applied to column ranges and alternating row stripes
  - Merged group-header row (group_label) labelling Demographics / Batting / Fielding
  - Auto-rotating headers for narrow stat columns (header_auto_rotate)
  - Conditional per-cell style via a callable (home_runs >= 15 → alert_style)
  - Overlapping column rules that compose: range sets background, sub-range adds
    number format without losing the background color
  - Column comment on the Fielding Pct header cell
  - Hidden column (team_name used for filtering, not shown in the sheet)
  - Writer-level headers= replacing underscores with spaces for display labels
    while keeping underscore field names for column rule pattern matching
"""
import dbtk
import polars as pl
from pathlib import Path

DATA_FILE = Path(__file__).parent / 'output' / '1927_baseball.parquet'
OUT_FILE  = Path(__file__).parent / 'output' / 'MLB-1927.xlsx'

fmt = {
    'styles': {
        'demo_style':     {'bg_color': '#C2E6F6'},  # light blue background
        'batting_style':  {'bg_color': '#D3F5C1'},  # light green background
        'fielding_style': {'bg_color': '#F5ECC1'},  # light orange background
        'alert_style':    {'bg_color': '#E9B47A'},  # orange background for alerts
        'stripe_style':   {'bg_color': '#DDDDDD'},  # light gray background for row striping
    },
    'columns': {
        'pos:weight':  {'format': 'demo_style',     'group_label': 'Demographics'},
        'pos':         {'filter': 1},
        'games_played:slugging_pct': {'format': 'batting_style', 'group_label': 'Batting',
                        'width': 5, 'header_format': 'header_vert_style'},
        'putouts:fielding_pct': {'format': 'fielding_style', 'group_label': 'Fielding',
                        'width': 5, 'header_format': 'header_vert_style'},
        'home_runs':   {'style': lambda rec: 'alert_style' if rec.home_runs >= 15 else None},
        'batting_avg:slugging_pct':     {'format': {'number_format': '0.000'}, 'width': 7},
        'fielding_pct': {'format': {'number_format': '0.000'}, 'width': 7,
                         'comment': 'Putouts + Assists / total chances'},
        'team_name': {'hidden': 1}             # Hide team_name (sheet name is team_name, column not needed)
    },
    'rows': {
        'data': {'odd': {'format': 'stripe_style'}},
    },
    'freeze':              'B3',                # Freeze so player name and headers are always visible
    'min_column_width':    4,                   # Decrease min column width from 6 to 4
    'header_auto_rotate':  {'min_length': 4},   # Turns on header rotations if header > 4 and header / data ratio > 1.5 (default)
}

if __name__ == '__main__':
    all_data = pl.read_parquet(DATA_FILE)
    # replace _'s in headers for a more readable display
    columns = [col.replace('_', ' ') for col in all_data.columns]
    with dbtk.writers.ExcelWriter(None, OUT_FILE, headers=columns, formatting=fmt) as writer:
        for team_name in all_data['team_name'].unique(maintain_order=True):
            team_df = all_data.filter(pl.col('team_name') == team_name)
            reader  = dbtk.readers.DataFrameReader(team_df, add_row_num=False)
            writer.write_batch(reader, sheet_name=team_name)
            print(f"  {team_name}")

    print(f"\nSaved → {OUT_FILE}")
