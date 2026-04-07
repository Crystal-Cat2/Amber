"""生成所有 5 个表格的 values JSON 文件，然后用 bash 调用 lark-cli 写入"""
import json
from datetime import date
import os

OUT_DIR = r"D:\Work\Amber\daily_tasks\data"

def ds(d_str):
    return (date.fromisoformat(d_str) - date(1899, 12, 30)).days

def f(text):
    return {"type": "formula", "text": text}

# ============================================================
# 表1: classic_revive_back 展示率 - 分天趋势
# 列: dt | GAP(show_rate) | trigger(A,B) | show(A,B) | show_rate(A,B)
# ============================================================
data1 = [
    ("2026-03-23", 1645, 1332, 1763, 1569),
    ("2026-03-24", 4434, 3369, 4861, 4107),
    ("2026-03-25", 8242, 6115, 8478, 7164),
    ("2026-03-26", 9896, 6976, 9954, 8247),
    ("2026-03-27", 10456, 7554, 10300, 8585),
    ("2026-03-28", 11017, 7772, 10949, 9074),
    ("2026-03-29", 11771, 8221, 11713, 9618),
    ("2026-03-30", 11757, 8035, 10909, 8861),
    ("2026-03-31", 11690, 7751, 11341, 9113),
    ("2026-04-01", 11263, 7464, 11182, 9056),
    ("2026-04-02", 11290, 7355, 11331, 9075),
    ("2026-04-03", 11533, 7768, 11322, 9229),
    ("2026-04-04", 11766, 7927, 11417, 9270),
    ("2026-04-05", 11806, 7963, 11571, 9492),
    ("2026-04-06", 10928, 7894, 11017, 9310),
]

h1 = ["", "GAP", "trigger", "trigger", "show", "show", "show_rate", "show_rate"]
h2 = ["dt", "show_rate", "A", "B", "A", "B", "A", "B"]
rows1 = [h1, h2]
for i, (dt, a_trig, a_show, b_trig, b_show) in enumerate(data1):
    r = i + 3
    rows1.append([
        ds(dt),
        f(f"=H{r}/G{r}-1"),
        a_trig, b_trig,
        a_show, b_show,
        f(f"=E{r}/C{r}"),
        f(f"=F{r}/D{r}"),
    ])

with open(os.path.join(OUT_DIR, "sheet_write_1.json"), "w") as fp:
    json.dump(rows1, fp, ensure_ascii=False)

# ============================================================
# 表2: classic_revive_back 展示率 - 汇总
# ============================================================
h1_s2 = ["", "GAP", "trigger", "trigger", "show", "show", "show_rate", "show_rate"]
h2_s2 = ["", "show_rate", "A", "B", "A", "B", "A", "B"]
rows2 = [h1_s2, h2_s2, [
    "TOTAL",
    f("=H3/G3-1"),
    149494, 148108, 103496, 121770,
    f("=E3/C3"),
    f("=F3/D3"),
]]

with open(os.path.join(OUT_DIR, "sheet_write_2.json"), "w") as fp:
    json.dump(rows2, fp, ensure_ascii=False)

# ============================================================
# 表3: Overall 展示率 & Hudi Paid 变现
# 列: scope | GAP(show_rate, paid_ecpm) | trigger(A,B) | show(A,B) | show_rate(A,B) | paid_pv(A,B) | paid_rev(A,B) | paid_ecpm(A,B)
# 15 列
# ============================================================
h1_s3 = [
    "", "GAP", "GAP",
    "trigger", "trigger",
    "show", "show",
    "show_rate", "show_rate",
    "paid_pv", "paid_pv",
    "paid_rev", "paid_rev",
    "paid_ecpm", "paid_ecpm"
]
h2_s3 = [
    "scope", "show_rate", "paid_ecpm",
    "A", "B", "A", "B", "A", "B",
    "A", "B", "A", "B", "A", "B"
]
rows3 = [h1_s3, h2_s3]
# overall row (r=3)
rows3.append([
    "overall",
    f("=I3/H3-1"),   # GAP show_rate = B_show_rate / A_show_rate - 1
    f("=O3/N3-1"),   # GAP paid_ecpm = B_paid_ecpm / A_paid_ecpm - 1
    604422, 595135,   # trigger A, B
    482957, 497350,   # show A, B
    f("=F3/D3"),      # A show_rate
    f("=G3/E3"),      # B show_rate
    480410, 494847,   # paid_pv A, B
    7956, 7613,       # paid_rev A, B
    f("=L3/J3*1000"), # A paid_ecpm
    f("=M3/K3*1000"), # B paid_ecpm
])
# classic_revive_back row (r=4)
rows3.append([
    "classic_revive_back",
    f("=I4/H4-1"),
    f("=O4/N4-1"),
    162278, 156713,
    103496, 121770,
    f("=F4/D4"),
    f("=G4/E4"),
    103094, 121192,
    1991, 2195,
    f("=L4/J4*1000"),
    f("=M4/K4*1000"),
])

with open(os.path.join(OUT_DIR, "sheet_write_3.json"), "w") as fp:
    json.dump(rows3, fp, ensure_ascii=False)

# ============================================================
# 表4: MAX 口径变现 - 分天趋势
# 列: dt | GAP(rev, ecpm) | imp(A,B) | rev(A,B) | ecpm(A,B)
# 10 列
# ============================================================
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

h1_s4 = ["", "GAP", "GAP", "imp", "imp", "rev", "rev", "ecpm", "ecpm"]
h2_s4 = ["dt", "rev", "ecpm", "A", "B", "A", "B", "A", "B"]
rows4 = [h1_s4, h2_s4]
for i, (dt, a_imp, a_rev, b_imp, b_rev) in enumerate(data4):
    r = i + 3
    rows4.append([
        ds(dt),
        f(f"=F{r}/G{r}-1"),  # GAP rev = A_rev / B_rev - 1
        f(f"=H{r}/I{r}-1"),  # GAP ecpm = A_ecpm / B_ecpm - 1
        a_imp, b_imp,
        a_rev, b_rev,
        f(f"=F{r}/D{r}*1000"),  # A ecpm
        f(f"=G{r}/E{r}*1000"),  # B ecpm
    ])

with open(os.path.join(OUT_DIR, "sheet_write_4.json"), "w") as fp:
    json.dump(rows4, fp, ensure_ascii=False)

# ============================================================
# 表5: MAX 口径变现 - 汇总
# ============================================================
h1_s5 = ["", "GAP", "GAP", "imp", "imp", "rev", "rev", "ecpm", "ecpm"]
h2_s5 = ["", "rev", "ecpm", "A", "B", "A", "B", "A", "B"]
rows5 = [h1_s5, h2_s5, [
    "TOTAL",
    f("=F3/G3-1"),
    f("=H3/I3-1"),
    446795, 460464,
    7385, 7031,
    f("=F3/D3*1000"),
    f("=G3/E3*1000"),
]]

with open(os.path.join(OUT_DIR, "sheet_write_5.json"), "w") as fp:
    json.dump(rows5, fp, ensure_ascii=False)

print("All 5 JSON files generated.")
