"""生成样式设置的 JSON 文件和色阶背景色"""
import json
import os

OUT_DIR = r"D:\Work\Amber\daily_tasks\data"

def style_json(sheet_id, ranges_styles):
    """生成 styles_batch_update 的 data payload"""
    data = []
    for ranges, style in ranges_styles:
        data.append({"ranges": ranges, "style": style})
    return {"data": data}

# ============================================================
# 表1: zuSlZE - classic_revive_back 展示率 分天趋势
# A=dt, B=GAP(show_rate), C=trigger_A, D=trigger_B, E=show_A, F=show_B, G=show_rate_A, H=show_rate_B
# 17 rows (2 header + 15 data)
# ============================================================
s1 = style_json("zuSlZE", [
    (["zuSlZE!A3:A17"], {"formatter": "yyyy/MM/dd"}),
    (["zuSlZE!B3:B17"], {"formatter": "0.00%"}),
    (["zuSlZE!C3:D17"], {"formatter": "#,##0"}),
    (["zuSlZE!E3:F17"], {"formatter": "#,##0"}),
    (["zuSlZE!G3:H17"], {"formatter": "0.00%"}),
    # 表头粗体
    (["zuSlZE!A1:H2"], {"font": {"bold": True}}),
])

with open(os.path.join(OUT_DIR, "sheet_style_1.json"), "w") as fp:
    json.dump(s1, fp, ensure_ascii=False)

# ============================================================
# 表2: cImBpJ - classic_revive_back 展示率 汇总
# 3 rows (2 header + 1 data)
# ============================================================
s2 = style_json("cImBpJ", [
    (["cImBpJ!B3:B3"], {"formatter": "0.00%"}),
    (["cImBpJ!C3:D3"], {"formatter": "#,##0"}),
    (["cImBpJ!E3:F3"], {"formatter": "#,##0"}),
    (["cImBpJ!G3:H3"], {"formatter": "0.00%"}),
    (["cImBpJ!A1:H2"], {"font": {"bold": True}}),
    (["cImBpJ!A3:A3"], {"font": {"bold": True}}),
])

with open(os.path.join(OUT_DIR, "sheet_style_2.json"), "w") as fp:
    json.dump(s2, fp, ensure_ascii=False)

# ============================================================
# 表3: wc7YGo - Overall 展示率 & Hudi Paid 变现
# A=scope, B=GAP_show_rate, C=GAP_paid_ecpm, D=trigger_A, E=trigger_B,
# F=show_A, G=show_B, H=show_rate_A, I=show_rate_B,
# J=paid_pv_A, K=paid_pv_B, L=paid_rev_A, M=paid_rev_B,
# N=paid_ecpm_A, O=paid_ecpm_B
# 4 rows (2 header + 2 data)
# ============================================================
s3 = style_json("wc7YGo", [
    (["wc7YGo!B3:C4"], {"formatter": "0.00%"}),
    (["wc7YGo!D3:E4"], {"formatter": "#,##0"}),
    (["wc7YGo!F3:G4"], {"formatter": "#,##0"}),
    (["wc7YGo!H3:I4"], {"formatter": "0.00%"}),
    (["wc7YGo!J3:K4"], {"formatter": "#,##0"}),
    (["wc7YGo!L3:M4"], {"formatter": "#,##0.00"}),
    (["wc7YGo!N3:O4"], {"formatter": "#,##0.00"}),
    (["wc7YGo!A1:O2"], {"font": {"bold": True}}),
])

with open(os.path.join(OUT_DIR, "sheet_style_3.json"), "w") as fp:
    json.dump(s3, fp, ensure_ascii=False)

# ============================================================
# 表4: T5NOFp - MAX 口径变现 分天趋势
# A=dt, B=GAP_rev, C=GAP_ecpm, D=imp_A, E=imp_B, F=rev_A, G=rev_B, H=ecpm_A, I=ecpm_B
# 16 rows (2 header + 14 data)
# ============================================================
s4 = style_json("T5NOFp", [
    (["T5NOFp!A3:A16"], {"formatter": "yyyy/MM/dd"}),
    (["T5NOFp!B3:C16"], {"formatter": "0.00%"}),
    (["T5NOFp!D3:E16"], {"formatter": "#,##0"}),
    (["T5NOFp!F3:G16"], {"formatter": "#,##0.00"}),
    (["T5NOFp!H3:I16"], {"formatter": "#,##0.00"}),
    (["T5NOFp!A1:I2"], {"font": {"bold": True}}),
])

with open(os.path.join(OUT_DIR, "sheet_style_4.json"), "w") as fp:
    json.dump(s4, fp, ensure_ascii=False)

# ============================================================
# 表5: Br4aGk - MAX 口径变现 汇总
# 3 rows (2 header + 1 data)
# ============================================================
s5 = style_json("Br4aGk", [
    (["Br4aGk!B3:C3"], {"formatter": "0.00%"}),
    (["Br4aGk!D3:E3"], {"formatter": "#,##0"}),
    (["Br4aGk!F3:G3"], {"formatter": "#,##0.00"}),
    (["Br4aGk!H3:I3"], {"formatter": "#,##0.00"}),
    (["Br4aGk!A1:I2"], {"font": {"bold": True}}),
    (["Br4aGk!A3:A3"], {"font": {"bold": True}}),
])

with open(os.path.join(OUT_DIR, "sheet_style_5.json"), "w") as fp:
    json.dump(s5, fp, ensure_ascii=False)

# ============================================================
# 色阶: GAP 列背景色
# 红(-0.1) -> 白(0) -> 绿(0.1)
# ============================================================
def gap_color(val):
    val = max(-0.1, min(0.1, val))
    if val < 0:
        ratio = val / -0.1
        r = 255
        g = int(255 * (1 - ratio))
        b = int(255 * (1 - ratio))
    else:
        ratio = val / 0.1
        r = int(255 * (1 - ratio))
        g = 255
        b = int(255 * (1 - ratio))
    return f"#{r:02x}{g:02x}{b:02x}"

# 表1 GAP 值 (B3:B17) - show_rate GAP
# 从原始数据计算: B_show_rate / A_show_rate - 1
data1 = [
    (1645, 1332, 1763, 1569),
    (4434, 3369, 4861, 4107),
    (8242, 6115, 8478, 7164),
    (9896, 6976, 9954, 8247),
    (10456, 7554, 10300, 8585),
    (11017, 7772, 10949, 9074),
    (11771, 8221, 11713, 9618),
    (11757, 8035, 10909, 8861),
    (11690, 7751, 11341, 9113),
    (11263, 7464, 11182, 9056),
    (11290, 7355, 11331, 9075),
    (11533, 7768, 11322, 9229),
    (11766, 7927, 11417, 9270),
    (11806, 7963, 11571, 9492),
    (10928, 7894, 11017, 9310),
]

gap1_colors = []
for a_trig, a_show, b_trig, b_show in data1:
    a_rate = a_show / a_trig
    b_rate = b_show / b_trig
    gap = b_rate / a_rate - 1
    gap1_colors.append(gap_color(gap))

color_data_1 = {"data": []}
for i, color in enumerate(gap1_colors):
    r = i + 3
    color_data_1["data"].append({
        "ranges": [f"zuSlZE!B{r}:B{r}"],
        "style": {"backColor": color}
    })

with open(os.path.join(OUT_DIR, "sheet_color_1.json"), "w") as fp:
    json.dump(color_data_1, fp, ensure_ascii=False)

# 表2 GAP (B3) - single cell
a_rate_total = 103496 / 149494
b_rate_total = 121770 / 148108
gap_total = b_rate_total / a_rate_total - 1
color_data_2 = {"data": [{"ranges": ["cImBpJ!B3:B3"], "style": {"backColor": gap_color(gap_total)}}]}

with open(os.path.join(OUT_DIR, "sheet_color_2.json"), "w") as fp:
    json.dump(color_data_2, fp, ensure_ascii=False)

# 表3 GAP (B3:C4) - 4 cells
# overall show_rate gap
a_sr_ov = 482957 / 604422
b_sr_ov = 497350 / 595135
gap_sr_ov = b_sr_ov / a_sr_ov - 1
# overall paid_ecpm gap
a_pe_ov = 7956 / 480410 * 1000
b_pe_ov = 7613 / 494847 * 1000
gap_pe_ov = b_pe_ov / a_pe_ov - 1
# crb show_rate gap
a_sr_crb = 103496 / 162278
b_sr_crb = 121770 / 156713
gap_sr_crb = b_sr_crb / a_sr_crb - 1
# crb paid_ecpm gap
a_pe_crb = 1991 / 103094 * 1000
b_pe_crb = 2195 / 121192 * 1000
gap_pe_crb = b_pe_crb / a_pe_crb - 1

color_data_3 = {"data": [
    {"ranges": ["wc7YGo!B3:B3"], "style": {"backColor": gap_color(gap_sr_ov)}},
    {"ranges": ["wc7YGo!C3:C3"], "style": {"backColor": gap_color(gap_pe_ov)}},
    {"ranges": ["wc7YGo!B4:B4"], "style": {"backColor": gap_color(gap_sr_crb)}},
    {"ranges": ["wc7YGo!C4:C4"], "style": {"backColor": gap_color(gap_pe_crb)}},
]}

with open(os.path.join(OUT_DIR, "sheet_color_3.json"), "w") as fp:
    json.dump(color_data_3, fp, ensure_ascii=False)

# 表4 GAP (B3:C16) - rev and ecpm gaps
data4 = [
    (5350, 116.11, 6185, 113.83),
    (15714, 300.14, 16848, 291.76),
    (28429, 526.04, 29036, 435.33),
    (33072, 552.40, 34104, 479.12),
    (35102, 573.03, 34857, 521.42),
    (34907, 595.58, 36815, 589.25),
    (37942, 613.75, 39501, 633.07),
    (36949, 574.78, 37288, 586.45),
    (36585, 564.21, 36639, 509.59),
    (35258, 557.98, 36714, 531.42),
    (35754, 536.21, 36636, 525.52),
    (36395, 592.18, 37439, 591.32),
    (37296, 621.54, 39098, 582.44),
    (38042, 661.05, 39304, 640.82),
]

color_data_4 = {"data": []}
for i, (a_imp, a_rev, b_imp, b_rev) in enumerate(data4):
    r = i + 3
    gap_rev = a_rev / b_rev - 1
    a_ecpm = a_rev / a_imp * 1000
    b_ecpm = b_rev / b_imp * 1000
    gap_ecpm = a_ecpm / b_ecpm - 1
    color_data_4["data"].append({"ranges": [f"T5NOFp!B{r}:B{r}"], "style": {"backColor": gap_color(gap_rev)}})
    color_data_4["data"].append({"ranges": [f"T5NOFp!C{r}:C{r}"], "style": {"backColor": gap_color(gap_ecpm)}})

with open(os.path.join(OUT_DIR, "sheet_color_4.json"), "w") as fp:
    json.dump(color_data_4, fp, ensure_ascii=False)

# 表5 GAP (B3:C3)
gap_rev_total = 7385 / 7031 - 1
a_ecpm_t = 7385 / 446795 * 1000
b_ecpm_t = 7031 / 460464 * 1000
gap_ecpm_total = a_ecpm_t / b_ecpm_t - 1

color_data_5 = {"data": [
    {"ranges": ["Br4aGk!B3:B3"], "style": {"backColor": gap_color(gap_rev_total)}},
    {"ranges": ["Br4aGk!C3:C3"], "style": {"backColor": gap_color(gap_ecpm_total)}},
]}

with open(os.path.join(OUT_DIR, "sheet_color_5.json"), "w") as fp:
    json.dump(color_data_5, fp, ensure_ascii=False)

print("All style and color JSON files generated.")
