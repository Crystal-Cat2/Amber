"""
ad_kill DAU 差异分析报告生成器
读取 CSV 数据，生成 HTML 分析报告
"""
import csv
import json
from pathlib import Path

BASE = Path("ad_kill")
DATA = BASE / "data"
OUT = BASE / "outputs"
OUT.mkdir(exist_ok=True)


def read_csv(path):
    with open(path, "rb") as f:
        raw = f.read()
    text = raw.decode("utf-8-sig").replace("\r\n", "\n")
    lines = text.strip().split("\n")
    return list(csv.DictReader(lines))


# === 数据加载 ===

# 1. 活跃事件口径 (仅 user_engagement)
old_rows = read_csv(DATA / "ad_kill_dau_by_game_model.csv")
# 2. 全部事件口径 (detail 表全事件)
new_rows = read_csv(DATA / "ad_kill_dau_user_composition.csv")
# 3. 口径对齐 (detail 表 engagement vs all)
align_rows = read_csv(DATA / "daily_dau_eng_vs_all.csv")
# 4. 无 engagement 用户分析
no_eng_rows = read_csv(DATA / "daily_no_eng_analysis.csv")


# === Section 1: 分日 DAU 对比 ===

def build_section1_data():
    """活跃事件 vs 全部事件，按 (product, date) 汇总"""
    old_data = {}
    for r in old_rows:
        k = (r["product"], r["event_date"])
        old_data[k] = old_data.get(k, 0) + int(r["dau"])

    new_data = {}
    seen = set()
    for r in new_rows:
        k = (r["product"], r["event_date"])
        ab_k = (r["product"], r["event_date"], r["ab_group"])
        if ab_k not in seen:
            seen.add(ab_k)
            new_data[k] = new_data.get(k, 0) + int(r["total_dau"])

    result = {"ball_sort": [], "ios_nuts_sort": []}
    for k in sorted(old_data):
        if k not in new_data or k[1] >= "2026-02-27":
            continue
        product, date = k
        o, n = old_data[k], new_data[k]
        gap = n - o
        pct = round(gap / o * 100, 1) if o else 0
        result[product].append({"date": date, "old": o, "new": n, "gap": gap, "pct": pct})
    return result


# === Section 2: 口径对齐 ===

def build_section2_data():
    """三条线: 活跃事件(原始表) / 全部事件(detail) / detail+engagement"""
    # detail 表数据按 (product, date) 汇总
    detail_all = {}
    detail_eng = {}
    for r in align_rows:
        k = (r["product"], r["event_date"])
        detail_all[k] = detail_all.get(k, 0) + int(r["dau_all"])
        detail_eng[k] = detail_eng.get(k, 0) + int(r["dau_eng"])

    # 活跃事件口径
    old_data = {}
    for r in old_rows:
        k = (r["product"], r["event_date"])
        old_data[k] = old_data.get(k, 0) + int(r["dau"])

    result = {"ball_sort": [], "ios_nuts_sort": []}
    for k in sorted(detail_all):
        if k[1] >= "2026-02-27":
            continue
        product, date = k
        d_all = detail_all[k]
        d_eng = detail_eng[k]
        o = old_data.get(k, 0)
        if o == 0:
            continue
        gap_aligned = d_eng - o
        pct_aligned = round(gap_aligned / o * 100, 1) if o else 0
        result[product].append({
            "date": date, "old": o, "detail_all": d_all, "detail_eng": d_eng,
            "gap_aligned": gap_aligned, "pct_aligned": pct_aligned,
        })
    return result


# === Section 3: 无 engagement 用户分析 ===

def build_section3_data():
    # 3a: 分日新老占比
    user_type_data = {"ball_sort": {}, "ios_nuts_sort": {}}
    # 3b: 事件分布汇总
    event_data = {"ball_sort": {}, "ios_nuts_sort": {}}

    for r in no_eng_rows:
        product = r["product"]
        date = r["event_date"]
        dim = r["dim"]
        val = r["val"]
        cnt = int(r["cnt"])

        if dim == "user_type":
            if date not in user_type_data[product]:
                user_type_data[product][date] = {"new": 0, "old": 0}
            user_type_data[product][date][val] = cnt
        elif dim == "event":
            event_data[product][val] = event_data[product].get(val, 0) + cnt

    # 3a: 转为列表
    user_type_series = {}
    for product in user_type_data:
        series = []
        for date in sorted(user_type_data[product]):
            if date >= "2026-02-27":
                continue
            d = user_type_data[product][date]
            total = d["new"] + d["old"]
            new_pct = round(d["new"] / total * 100, 1) if total else 0
            series.append({"date": date, "new": d["new"], "old": d["old"], "total": total, "new_pct": new_pct})
        user_type_series[product] = series

    # 3b: top 10 事件
    event_top10 = {}
    for product in event_data:
        sorted_events = sorted(event_data[product].items(), key=lambda x: -x[1])[:10]
        event_top10[product] = [{"event": e, "users": c} for e, c in sorted_events]

    return user_type_series, event_top10


# === 构建所有数据 ===
s1 = build_section1_data()
s2 = build_section2_data()
s3_user_type, s3_events = build_section3_data()

# 计算摘要统计
def calc_avg(data, key):
    vals = [d[key] for d in data]
    return round(sum(vals) / len(vals), 1) if vals else 0

# ball_sort 稳定期 (02-04 ~ 02-26)
bs_stable = [d for d in s1["ball_sort"] if "2026-02-04" <= d["date"] <= "2026-02-26"]
ios_stable = [d for d in s1["ios_nuts_sort"] if "2026-02-08" <= d["date"] <= "2026-02-26"]
bs_avg_pct = calc_avg(bs_stable, "pct")
ios_avg_pct = calc_avg(ios_stable, "pct")

# 对齐后 gap
bs_align_stable = [d for d in s2["ball_sort"] if "2026-02-04" <= d["date"] <= "2026-02-26"]
ios_align_stable = [d for d in s2["ios_nuts_sort"] if "2026-02-08" <= d["date"] <= "2026-02-26"]
bs_align_avg = calc_avg(bs_align_stable, "pct_aligned")
ios_align_avg = calc_avg(ios_align_stable, "pct_aligned")


# === HTML 生成 ===

html = f"""<!DOCTYPE html>
<html lang="zh-CN">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>ad_kill DAU 差异分析</title>
<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.7/dist/chart.umd.min.js"></script>
<style>
/* PLACEHOLDER_CSS */
</style>
</head>
<body>
<div class="container">

<header>
  <h1>ad_kill DAU 口径差异分析</h1>
  <p class="subtitle">活跃事件口径 (user_engagement) vs 全部事件口径 (all events)</p>
</header>

<!-- 摘要 -->
<section class="summary">
  <h2>分析结论</h2>
  <div class="summary-grid">
    <div class="summary-card">
      <div class="card-label">差异根因</div>
      <div class="card-body">全部事件口径包含了大量后台心跳 <code>universal_alive</code> 等非活跃事件用户，导致 DAU 虚高</div>
    </div>
    <div class="summary-card accent-a">
      <div class="card-label">Ball Sort 平均偏差</div>
      <div class="card-value">+{bs_avg_pct}%</div>
      <div class="card-note">口径对齐后: {bs_align_avg}%</div>
    </div>
    <div class="summary-card accent-b">
      <div class="card-label">iOS Nuts Sort 平均偏差</div>
      <div class="card-value">+{ios_avg_pct}%</div>
      <div class="card-note">口径对齐后: {ios_align_avg}%</div>
    </div>
    <div class="summary-card accent-c">
      <div class="card-label">多出用户构成</div>
      <div class="card-body">90% 仅有后台心跳事件，不是真实活跃用户</div>
    </div>
  </div>
</section>

<!-- PLACEHOLDER_SECTION1 -->

<!-- PLACEHOLDER_SECTION2 -->

<!-- PLACEHOLDER_SECTION3 -->

</div>

<script>
// PLACEHOLDER_JS
</script>
</body>
</html>"""

# === CSS ===
CSS = """
:root {
  --bg: #f4f2ea; --panel: #fffdf8; --border: #d8d0c2;
  --text: #2b2722; --muted: #746a5d;
  --accent-a: #1967d2; --accent-b: #d94f30; --accent-c: #13795b;
  --shadow: 0 12px 30px rgba(55,42,24,0.08);
}
* { margin: 0; padding: 0; box-sizing: border-box; }
body {
  font-family: "Segoe UI", "PingFang SC", "Microsoft YaHei", sans-serif;
  background: var(--bg); color: var(--text); line-height: 1.6;
}
.container { max-width: 1240px; margin: 0 auto; padding: 24px 20px; }
header {
  text-align: center; padding: 32px 24px; margin-bottom: 24px;
  background: linear-gradient(135deg, rgba(25,103,210,0.08), rgba(19,121,91,0.08));
  border-radius: 22px;
}
h1 { font-size: 28px; font-weight: 700; margin-bottom: 6px; }
.subtitle { color: var(--muted); font-size: 15px; }
h2 { font-size: 20px; font-weight: 600; margin-bottom: 16px; color: var(--text); }
h3 { font-size: 16px; font-weight: 600; margin-bottom: 12px; color: var(--muted); }
section {
  background: var(--panel); border: 1px solid var(--border);
  border-radius: 18px; padding: 24px; margin-bottom: 20px;
  box-shadow: var(--shadow);
}
.summary-grid {
  display: grid; grid-template-columns: repeat(auto-fit, minmax(260px, 1fr));
  gap: 16px;
}
.summary-card {
  background: rgba(255,255,255,0.7); border: 1px solid var(--border);
  border-radius: 14px; padding: 18px;
}
.summary-card.accent-a { border-left: 4px solid var(--accent-a); }
.summary-card.accent-b { border-left: 4px solid var(--accent-b); }
.summary-card.accent-c { border-left: 4px solid var(--accent-c); }
.card-label { font-size: 13px; color: var(--muted); margin-bottom: 6px; font-weight: 600; }
.card-value { font-size: 32px; font-weight: 700; }
.card-note { font-size: 13px; color: var(--muted); margin-top: 4px; }
.card-body { font-size: 14px; line-height: 1.5; }
code { background: rgba(0,0,0,0.06); padding: 2px 6px; border-radius: 4px; font-size: 13px; }
.chart-row { display: grid; grid-template-columns: 1fr 1fr; gap: 20px; margin-top: 16px; }
.chart-box { position: relative; }
.chart-box canvas { width: 100% !important; }
.stat-note { font-size: 13px; color: var(--muted); margin-top: 10px; text-align: center; }
.section-desc { font-size: 14px; color: var(--muted); margin-bottom: 16px; line-height: 1.6; }
@media (max-width: 900px) {
  .chart-row { grid-template-columns: 1fr; }
  .summary-grid { grid-template-columns: 1fr; }
}
"""

html = html.replace("/* PLACEHOLDER_CSS */", CSS)

# === Section 1 HTML ===
s1_html = """
<section>
  <h2>1. 分日 DAU 对比：活跃事件 vs 全部事件</h2>
  <p class="section-desc">
    活跃事件口径：仅统计触发 <code>user_engagement</code> 事件的用户（来自原始埋点表）<br>
    全部事件口径：统计触发任意事件的用户（来自 <code>ad_kill_detail</code> 中间表）
  </p>
  <div class="chart-row">
    <div class="chart-box">
      <h3>Ball Sort (Android)</h3>
      <canvas id="s1_bs"></canvas>
      <p class="stat-note">稳定期 (02-04~02-26) 平均偏差: <b>+""" + str(bs_avg_pct) + """%</b></p>
    </div>
    <div class="chart-box">
      <h3>iOS Nuts Sort</h3>
      <canvas id="s1_ios"></canvas>
      <p class="stat-note">稳定期 (02-08~02-26) 平均偏差: <b>+""" + str(ios_avg_pct) + """%</b></p>
    </div>
  </div>
</section>
"""
html = html.replace("<!-- PLACEHOLDER_SECTION1 -->", s1_html)

# === Section 2 HTML ===
s2_html = """
<section>
  <h2>2. 口径对齐验证</h2>
  <p class="section-desc">
    将全部事件口径限定为仅 <code>user_engagement</code> 后，与活跃事件口径对比。<br>
    若差异消失，则确认偏差完全由事件范围导致。
  </p>
  <div class="chart-row">
    <div class="chart-box">
      <h3>Ball Sort — 三条线对比</h3>
      <canvas id="s2_bs"></canvas>
      <p class="stat-note">对齐后平均偏差: <b>""" + str(bs_align_avg) + """%</b></p>
    </div>
    <div class="chart-box">
      <h3>iOS Nuts Sort — 三条线对比</h3>
      <canvas id="s2_ios"></canvas>
      <p class="stat-note">对齐后平均偏差: <b>""" + str(ios_align_avg) + """%</b></p>
    </div>
  </div>
</section>
"""
html = html.replace("<!-- PLACEHOLDER_SECTION2 -->", s2_html)

# === Section 3 HTML ===
s3_html = """
<section>
  <h2>3. 无 engagement 用户画像</h2>
  <p class="section-desc">
    分析全部事件口径中多出的用户（有事件但无 <code>user_engagement</code>）的特征。
  </p>
  <div class="chart-row">
    <div class="chart-box">
      <h3>Ball Sort — 无 engagement 用户中新用户占比趋势</h3>
      <canvas id="s3a_bs"></canvas>
    </div>
    <div class="chart-box">
      <h3>iOS Nuts Sort — 无 engagement 用户中新用户占比趋势</h3>
      <canvas id="s3a_ios"></canvas>
    </div>
  </div>
  <div class="chart-row" style="margin-top:20px;">
    <div class="chart-box">
      <h3>Ball Sort — 无 engagement 用户 Top 10 事件</h3>
      <canvas id="s3b_bs"></canvas>
    </div>
    <div class="chart-box">
      <h3>iOS Nuts Sort — 无 engagement 用户 Top 10 事件</h3>
      <canvas id="s3b_ios"></canvas>
    </div>
  </div>
</section>
"""
html = html.replace("<!-- PLACEHOLDER_SECTION3 -->", s3_html)

# === JS 数据 + 图表 ===
# PLACEHOLDER_JS_CONTENT
js_data = "// === 内嵌数据 ===\n"
js_data += f"const s1 = {json.dumps(s1)};\n"
js_data += f"const s2 = {json.dumps(s2)};\n"
js_data += f"const s3_user_type = {json.dumps(s3_user_type)};\n"
js_data += f"const s3_events = {json.dumps(s3_events)};\n"

js_charts = """
// === 工具函数 ===
function shortDate(d) { return d.slice(5); }
const COLORS = {
  engagement: '#1967d2', allEvents: '#d94f30', detailEng: '#13795b',
  newUser: '#d94f30', oldUser: '#1967d2',
};

// === Section 1: 折线图 ===
function drawS1(canvasId, data) {
  new Chart(document.getElementById(canvasId), {
    type: 'line',
    data: {
      labels: data.map(d => shortDate(d.date)),
      datasets: [
        { label: '活跃事件 DAU', data: data.map(d => d.old), borderColor: COLORS.engagement, backgroundColor: COLORS.engagement + '18', fill: true, tension: 0.3, pointRadius: 2 },
        { label: '全部事件 DAU', data: data.map(d => d.new), borderColor: COLORS.allEvents, backgroundColor: COLORS.allEvents + '18', fill: true, tension: 0.3, pointRadius: 2 },
      ]
    },
    options: { responsive: true, interaction: { mode: 'index', intersect: false },
      scales: { y: { beginAtZero: true, ticks: { callback: v => (v/1000).toFixed(0)+'k' } } },
      plugins: { legend: { position: 'bottom' } }
    }
  });
}
drawS1('s1_bs', s1.ball_sort);
drawS1('s1_ios', s1.ios_nuts_sort);

// === Section 2: 三条线 ===
function drawS2(canvasId, data) {
  new Chart(document.getElementById(canvasId), {
    type: 'line',
    data: {
      labels: data.map(d => shortDate(d.date)),
      datasets: [
        { label: '活跃事件口径 (原始表)', data: data.map(d => d.old), borderColor: COLORS.engagement, tension: 0.3, pointRadius: 2 },
        { label: '全部事件口径 (detail)', data: data.map(d => d.detail_all), borderColor: COLORS.allEvents, tension: 0.3, pointRadius: 2 },
        { label: 'detail + 仅 engagement', data: data.map(d => d.detail_eng), borderColor: COLORS.detailEng, borderDash: [6,3], tension: 0.3, pointRadius: 2 },
      ]
    },
    options: { responsive: true, interaction: { mode: 'index', intersect: false },
      scales: { y: { beginAtZero: true, ticks: { callback: v => (v/1000).toFixed(0)+'k' } } },
      plugins: { legend: { position: 'bottom' } }
    }
  });
}
drawS2('s2_bs', s2.ball_sort);
drawS2('s2_ios', s2.ios_nuts_sort);

// === Section 3a: 新用户占比趋势 ===
function drawS3a(canvasId, data) {
  new Chart(document.getElementById(canvasId), {
    type: 'line',
    data: {
      labels: data.map(d => shortDate(d.date)),
      datasets: [
        { label: '新用户占比 (%)', data: data.map(d => d.new_pct), borderColor: COLORS.newUser, tension: 0.3, pointRadius: 2, fill: true, backgroundColor: COLORS.newUser + '18' },
      ]
    },
    options: { responsive: true,
      scales: { y: { min: 0, max: 100, ticks: { callback: v => v+'%' } } },
      plugins: { legend: { position: 'bottom' } }
    }
  });
}
drawS3a('s3a_bs', s3_user_type.ball_sort);
drawS3a('s3a_ios', s3_user_type.ios_nuts_sort);

// === Section 3b: Top 10 事件横向柱状图 ===
function drawS3b(canvasId, data) {
  new Chart(document.getElementById(canvasId), {
    type: 'bar',
    data: {
      labels: data.map(d => d.event),
      datasets: [{
        label: '用户数 (汇总)',
        data: data.map(d => d.users),
        backgroundColor: '#1967d2aa',
        borderColor: '#1967d2',
        borderWidth: 1,
      }]
    },
    options: { indexAxis: 'y', responsive: true,
      scales: { x: { ticks: { callback: v => (v/1000).toFixed(0)+'k' } } },
      plugins: { legend: { display: false } }
    }
  });
}
drawS3b('s3b_bs', s3_events.ball_sort);
drawS3b('s3b_ios', s3_events.ios_nuts_sort);
"""

html = html.replace("// PLACEHOLDER_JS", js_data + js_charts)

# === 写出 ===
out_path = OUT / "ad_kill_dau_gap_analysis.html"
out_path.write_text(html, encoding="utf-8")
print(f"报告已生成: {out_path}")
