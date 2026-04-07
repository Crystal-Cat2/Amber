"""
Generate JSON payloads for updating car_jam_rt sheet vGlH8r
in spreadsheet GUO7sGtUBhkCBJtnxgWcGxNYnwf.

Reads the new CSV, aggregates by date+ad_format+group,
outputs the values arrays for lark-cli sheets +write.
"""
import csv
import json
from collections import defaultdict
from datetime import date

CSV_PATH = "D:/Work/Amber/daily_tasks/data/2026-04-03_car_jam_tp_adx_ab_tradplus_rt.csv"

# Aggregate CSV
data = defaultdict(lambda: defaultdict(lambda: {
    'no_tp_adx': {'imp': 0, 'rev': 0.0},
    'have_tp_adx': {'imp': 0, 'rev': 0.0}
}))

with open(CSV_PATH, 'r', encoding='utf-8-sig') as f:
    for row in csv.DictReader(f):
        dt = row['date']
        fmt = row['ad_format']
        grp = row['experiment_group']
        data[dt][fmt][grp]['imp'] += int(row['impression'])
        data[dt][fmt][grp]['rev'] += float(row['revenue'])

dates = sorted(data.keys())
print(f"Dates: {dates[0]} ~ {dates[-1]}, count={len(dates)}")

# Section config: (ad_format, start_row_for_2026-01-27, end_row_current)
# BANNER: row 138-201 (64 rows), INTER: row 337-400, REWARD: row 536-599
SECTIONS = [
    ('BANNER', 138, 201),
    ('INTER', 337, 400),
    ('REWARD', 536, 599),
]

for fmt, start_row, end_row in SECTIONS:
    rows = []
    for i, dt in enumerate(dates):
        d = data[dt][fmt]
        no = d['no_tp_adx']
        have = d['have_tp_adx']
        rows.append([no['imp'], have['imp'], round(no['rev'], 6), round(have['rev'], 6)])

    # First 64 rows overwrite existing (row start_row to end_row)
    overwrite = rows[:64]
    extra = rows[64:]  # should be 1 row (2026-04-01)

    print(f"\n{fmt}: {len(overwrite)} rows to overwrite (row {start_row}-{end_row}), {len(extra)} extra rows")
    if extra:
        print(f"  Extra row data: {extra[0]}")

    # Output JSON for overwrite (F:I columns)
    print(f"  Sample first row: {overwrite[0]}")
    print(f"  Sample last row: {overwrite[-1]}")

    # Save to file for batch write
    with open(f"D:/Work/Amber/daily_tasks/data/sheet_update_{fmt.lower()}.json", 'w') as f:
        json.dump(overwrite, f)

    if extra:
        with open(f"D:/Work/Amber/daily_tasks/data/sheet_extra_{fmt.lower()}.json", 'w') as f:
            json.dump(extra, f)
