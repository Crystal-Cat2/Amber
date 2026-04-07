"""修复 GAP 方向：百分比用 A-B 差值，非百分比用 A/B-1"""
import json, os

OUT_DIR = r"D:\Work\Amber\daily_tasks\data"

def f(text):
    return {"type": "formula", "text": text}

# qH0SGv B3:B18: =G{r}-H{r} (A_show_rate - B_show_rate)
gap_fix_1 = []
for r in range(3, 18):
    gap_fix_1.append([f(f"=G{r}-H{r}")])
gap_fix_1.append([f("=G18-H18")])  # TOTAL row

with open(os.path.join(OUT_DIR, "gap_fix_1.json"), "w") as fp:
    json.dump(gap_fix_1, fp, ensure_ascii=False)

# wc7YGo B3:E4:
# B=show_rate GAP: =J{r}-K{r} (A-B 差值)
# C=paid_pv GAP: =L{r}/M{r}-1 (A/B-1)
# D=paid_rev GAP: =N{r}/O{r}-1 (A/B-1)
# E=paid_ecpm GAP: =P{r}/Q{r}-1 (A/B-1)
gap_fix_2 = []
for r in [3, 4]:
    gap_fix_2.append([
        f(f"=J{r}-K{r}"),
        f(f"=L{r}/M{r}-1"),
        f(f"=N{r}/O{r}-1"),
        f(f"=P{r}/Q{r}-1"),
    ])

with open(os.path.join(OUT_DIR, "gap_fix_2.json"), "w") as fp:
    json.dump(gap_fix_2, fp, ensure_ascii=False)

print("Done")
