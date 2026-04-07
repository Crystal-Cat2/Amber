"""修复表4和表5：加上 imp 的 GAP 列"""
import json
from datetime import date
import os

OUT_DIR = r"D:\Work\Amber\daily_tasks\data"

def ds(d_str):
    return (date.fromisoformat(d_str) - date(1899, 12, 30)).days

def f(text):
    return {"type": "formula", "text": text}

# 表4: T5NOFp - MAX 口径变现 分天趋势
# 新列: dt | GAP_imp | GAP_rev | GAP_ecpm | imp_A | imp_B | rev_A | rev_B | ecpm_A | ecpm_B
# A      B          C         D          E       F       G       H       I        J
data4 = [
    ("2026-03-23", 5350, 116.11, 6185, 113.83),
    ("2026-03-24", 15714, 300.14, 16848, 291.76),
    ("2026-03-25", 28429, 526.04, 29036, 435.33),
    ("2026-03-26", 33072, 552.40, 34104, 479.12),
    ("2026-03-27", 35102, 573.03, 34857, 521.42),
    ("2026-03-28", 34907, 595.58, 36815, 589.25),
    ("2026-03-29", 37942, 613.75, 39501, 633.07),
    ("2026-03-30", 36949, 574.78, 37288, 586.45),
    ("2026-03-31", 36585, 564.21, 36639, 509.59),
    ("2026-04-01", 35258, 557.98, 36714, 531.42),
    ("2026-04-02", 35754, 536.21, 36636, 525.52),
    ("2026-04-03", 36395, 592.18, 37439, 591.32),
    ("2026-04-04", 37296, 621.54, 39098, 582.44),
    ("2026-04-05", 38042, 661.05, 39304, 640.82),
]

h1 = ["", "GAP", "GAP", "GAP", "imp", "imp", "rev", "rev", "ecpm", "ecpm"]
h2 = ["dt", "imp", "rev", "ecpm", "A", "B", "A", "B", "A", "B"]
rows4 = [h1, h2]
for i, (dt, a_imp, a_rev, b_imp, b_rev) in enumerate(data4):
    r = i + 3
    rows4.append([
        ds(dt),
        f(f"=E{r}/F{r}-1"),    # GAP imp = A_imp / B_imp - 1
        f(f"=G{r}/H{r}-1"),    # GAP rev = A_rev / B_rev - 1
        f(f"=I{r}/J{r}-1"),    # GAP ecpm = A_ecpm / B_ecpm - 1
        a_imp, b_imp,
        a_rev, b_rev,
        f(f"=G{r}/E{r}*1000"), # A ecpm
        f(f"=H{r}/F{r}*1000"), # B ecpm
    ])

with open(os.path.join(OUT_DIR, "sheet_write_4_fix.json"), "w") as fp:
    json.dump(rows4, fp, ensure_ascii=False)

# 表5: Br4aGk - MAX 口径变现 汇总
h1_s5 = ["", "GAP", "GAP", "GAP", "imp", "imp", "rev", "rev", "ecpm", "ecpm"]
h2_s5 = ["", "imp", "rev", "ecpm", "A", "B", "A", "B", "A", "B"]
rows5 = [h1_s5, h2_s5, [
    "TOTAL",
    f("=E3/F3-1"),    # GAP imp
    f("=G3/H3-1"),    # GAP rev
    f("=I3/J3-1"),    # GAP ecpm
    446795, 460464,
    7385, 7031,
    f("=G3/E3*1000"),
    f("=H3/F3*1000"),
]]

with open(os.path.join(OUT_DIR, "sheet_write_5_fix.json"), "w") as fp:
    json.dump(rows5, fp, ensure_ascii=False)

# 样式 JSON
def style_json(items):
    return {"data": [{"ranges": r, "style": s} for r, s in items]}

s4 = style_json([
    (["T5NOFp!A3:A16"], {"formatter": "yyyy/MM/dd"}),
    (["T5NOFp!B3:D16"], {"formatter": "0.00%"}),
    (["T5NOFp!E3:F16"], {"formatter": "#,##0"}),
    (["T5NOFp!G3:H16"], {"formatter": "#,##0.00"}),
    (["T5NOFp!I3:J16"], {"formatter": "#,##0.00"}),
    (["T5NOFp!A1:J2"], {"font": {"bold": True}}),
])
with open(os.path.join(OUT_DIR, "sheet_style_4_fix.json"), "w") as fp:
    json.dump(s4, fp, ensure_ascii=False)

s5 = style_json([
    (["Br4aGk!B3:D3"], {"formatter": "0.00%"}),
    (["Br4aGk!E3:F3"], {"formatter": "#,##0"}),
    (["Br4aGk!G3:H3"], {"formatter": "#,##0.00"}),
    (["Br4aGk!I3:J3"], {"formatter": "#,##0.00"}),
    (["Br4aGk!A1:J2"], {"font": {"bold": True}}),
    (["Br4aGk!A3:A3"], {"font": {"bold": True}}),
])
with open(os.path.join(OUT_DIR, "sheet_style_5_fix.json"), "w") as fp:
    json.dump(s5, fp, ensure_ascii=False)

print("Fix JSON files generated.")
