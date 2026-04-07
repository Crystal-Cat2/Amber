"""Parse lark-table elements from fetched doc markdown and extract clean data."""
import re
import json

with open("D:/Work/Amber/daily_tasks/data/_tmp_doc_part1.txt", "r", encoding="utf-8") as f:
    md = f.read()

# Find all lark-table blocks
table_pattern = re.compile(r'<lark-table[^>]*>(.*?)</lark-table>', re.DOTALL)
row_pattern = re.compile(r'<lark-tr>(.*?)</lark-tr>', re.DOTALL)
cell_pattern = re.compile(r'<lark-td>(.*?)</lark-td>', re.DOTALL)

# Section headers before each table to infer names
# Split by table and look backwards for ## or ### headers
table_matches = list(table_pattern.finditer(md))
print(f"Found {len(table_matches)} tables")

# Find section context for each table
def find_table_name(md, table_start):
    """Look backwards from table_start to build a hierarchical name like 'section > subsection'."""
    before = md[:table_start]
    # Collect all headings
    h2_matches = list(re.finditer(r'^##\s+(.+)', before, re.MULTILINE))
    h3_matches = list(re.finditer(r'^###\s+(.+)', before, re.MULTILINE))

    parts = []
    if h2_matches:
        parts.append(h2_matches[-1].group(1).strip())
    if h3_matches:
        parts.append(h3_matches[-1].group(1).strip())

    return ' > '.join(parts) if parts else "unknown"

def clean_value(raw):
    """Clean a cell value: remove formatting, convert units."""
    v = raw.strip()
    # Remove bold markers
    v = v.replace('**', '')
    v = v.strip()

    if not v:
        return ""

    # Date values - keep as-is
    if re.match(r'^\d{4}-\d{2}-\d{2}$', v):
        return v

    # Text labels - keep as-is
    if v in ('TOTAL', 'classic_revive_back', 'overall'):
        return v

    # pp values (percentage points) -> decimal
    pp_match = re.match(r'^([+-]?)([\d.]+)pp$', v)
    if pp_match:
        sign = pp_match.group(1)
        num = float(pp_match.group(2))
        result = num / 100
        if sign == '-':
            result = -result
        return round(result, 6)

    # Percentage values -> decimal
    pct_match = re.match(r'^([+-]?)([\d.]+)%$', v)
    if pct_match:
        sign = pct_match.group(1)
        num = float(pct_match.group(2))
        result = num / 100
        if sign == '-':
            result = -result
        elif sign == '+':
            result = result  # keep positive
        return round(result, 6)

    # Dollar values
    dollar_match = re.match(r'^([+-]?)[\$]([\d,.]+)$', v)
    if dollar_match:
        sign = dollar_match.group(1)
        num = float(dollar_match.group(2).replace(',', ''))
        if sign == '-':
            num = -num
        # Return int if whole number, else float
        if num == int(num) and '.' not in dollar_match.group(2):
            return int(num)
        return num

    # Plain numbers with commas
    num_match = re.match(r'^([+-]?)([\d,]+)$', v)
    if num_match:
        sign = num_match.group(1)
        num = int(num_match.group(2).replace(',', ''))
        if sign == '-':
            num = -num
        return num

    # Plain decimal numbers
    dec_match = re.match(r'^([+-]?)([\d,.]+)$', v)
    if dec_match:
        sign = dec_match.group(1)
        num = float(dec_match.group(2).replace(',', ''))
        if sign == '-':
            num = -num
        return num

    # Fallback: return as string
    return v

tables = []
for i, m in enumerate(table_matches):
    table_html = m.group(1)
    name = find_table_name(md, m.start())

    rows = row_pattern.findall(table_html)
    parsed_rows = []
    for row in rows:
        cells = cell_pattern.findall(row)
        parsed_rows.append([c.strip() for c in cells])

    if not parsed_rows:
        continue

    headers = parsed_rows[0]
    data_rows = parsed_rows[1:]

    # Clean headers
    clean_headers = [h.replace('**', '').strip() for h in headers]

    # Clean data
    clean_rows = []
    for row in data_rows:
        clean_rows.append([clean_value(c) for c in row])

    tables.append({
        "name": name,
        "headers": clean_headers,
        "rows": clean_rows
    })
    print(f"Table {i+1}: '{name}' - {len(clean_headers)} cols x {len(clean_rows)} rows")

output = {"tables": tables}
out_path = "D:/Work/Amber/daily_tasks/data/sheet_extra_tables_data.json"
with open(out_path, "w", encoding="utf-8") as f:
    json.dump(output, f, ensure_ascii=False, indent=2)

print(f"\nSaved {len(tables)} tables to {out_path}")
