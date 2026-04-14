"""读取 ad_kill CSV 数据，生成交互式 HTML dashboard"""
import csv
import json
import os

DATA_DIR = os.path.join(os.path.dirname(__file__), '..', 'data')
OUT_DIR = os.path.join(os.path.dirname(__file__), '..', 'outputs')

def read_csv(filename):
    rows = []
    with open(os.path.join(DATA_DIR, filename), encoding='utf-8-sig') as f:
        reader = csv.DictReader(f)
        for r in reader:
            rows.append(r)
    return rows

# 读取数据
lt_ball = read_csv('ball_sort_ad_kill_lt_lti_ltv.csv')
lt_nuts = read_csv('ios_nuts_sort_ad_kill_lt_lti_ltv.csv')
dau_raw = read_csv('ad_kill_dau_by_game_model.csv')
level_ball = read_csv('ball_sort_level_survival.csv')
level_nuts = read_csv('ios_nuts_sort_level_survival.csv')
gs_ball = read_csv('ball_sort_ad_kill_lt_game_starts.csv')
gs_nuts = read_csv('ios_nuts_sort_ad_kill_lt_game_starts.csv')
daily_gs_raw = read_csv('ad_kill_daily_game_starts.csv')
max_daily_ball = read_csv('ball_sort_ad_kill_max_daily.csv')
max_daily_nuts = read_csv('ios_nuts_sort_ad_kill_max_daily.csv')

def parse_float(v):
    if v is None or v == '':
        return None
    return float(v)

def parse_int(v):
    if v is None or v == '':
        return None
    return int(v)

# 构建 LT 数据结构: {product: {ab_group: {ad_format: {lt_day: {metric: val}}}}}
def build_lt_data(rows, product_name):
    result = {}
    for r in rows:
        ab = r['ab_group']
        fmt = r['ad_format']
        lt = int(r['lt_day'])
        if ab not in result:
            result[ab] = {}
        if fmt not in result[ab]:
            result[ab][fmt] = {}
        result[ab][fmt][lt] = {
            'retention_rate': parse_float(r['retention_rate']),
            'retained_users': parse_int(r['retained_users']),
            'total_new_users': parse_int(r['total_new_users']),
            'avg_cum_hudi_lti': parse_float(r['avg_cum_hudi_lti']),
            'avg_cum_hudi_ltv': parse_float(r['avg_cum_hudi_ltv']),
            'avg_cum_max_lti': parse_float(r['avg_cum_max_lti']),
            'avg_cum_max_ltv': parse_float(r['avg_cum_max_ltv']),
        }
    return result

# 构建 DAU 数据: {product: {ab_group: [{date, dau}]}}
def build_dau_data(rows):
    result = {}
    for r in rows:
        prod = r['product']
        ab = r['ab_group']
        if prod not in result:
            result[prod] = {}
        if ab not in result[prod]:
            result[prod][ab] = []
        result[prod][ab].append({
            'date': r['event_date'],
            'dau': parse_int(r['dau']),
        })
    return result

lt_data = {
    'ball_sort': build_lt_data(lt_ball, 'ball_sort'),
    'ios_nuts_sort': build_lt_data(lt_nuts, 'ios_nuts_sort'),
}
dau_data = build_dau_data(dau_raw)

def build_level_data(rows):
    result = {}
    for r in rows:
        ab = r['ab_group']
        if ab not in result:
            result[ab] = []
        result[ab].append({
            'level': parse_int(r['level']),
            'survival_rate': parse_float(r['survival_rate']),
        })
    for ab in result:
        result[ab].sort(key=lambda x: x['level'])
    return result

level_data = {
    'ball_sort': build_level_data(level_ball),
    'ios_nuts_sort': build_level_data(level_nuts),
}

# 构建 LT game starts 数据: {product: {ab_group: [{lt_day, avg_cum_game_starts}]}}
def build_gs_lt_data(rows):
    result = {}
    for r in rows:
        ab = r['ab_group']
        if ab not in result:
            result[ab] = []
        result[ab].append({
            'lt_day': parse_int(r['lt_day']),
            'avg_cum_game_starts': parse_float(r['avg_cum_game_starts']),
            'retention_rate': parse_float(r['retention_rate']),
        })
    for ab in result:
        result[ab].sort(key=lambda x: x['lt_day'])
        # 计算累计 LT = sum(retention_rate) from LT0 to LTN
        cum = 0
        for d in result[ab]:
            rr = d['retention_rate'] if d['retention_rate'] is not None else 0
            cum += rr
            d['cum_lt'] = round(cum, 6)
    return result

gs_lt_data = {
    'ball_sort': build_gs_lt_data(gs_ball),
    'ios_nuts_sort': build_gs_lt_data(gs_nuts),
}

# 构建 daily game starts 数据: {product: {ab_group: [{date, avg_game_starts_per_user}]}}
def build_daily_gs_data(rows):
    result = {}
    for r in rows:
        prod = r['product']
        ab = r['ab_group']
        if prod not in result:
            result[prod] = {}
        if ab not in result[prod]:
            result[prod][ab] = []
        result[prod][ab].append({
            'date': r['event_date'],
            'avg_starts': parse_float(r['avg_game_starts_per_user']),
        })
    return result

daily_gs_data = build_daily_gs_data(daily_gs_raw)

# 构建分日广告数据: {product: {ab_group: {ad_format: [{date, impressions, revenue, ecpm}]}}}
def build_daily_ad_data(rows):
    result = {}
    for r in rows:
        ab = r['ab_group']
        fmt = r['ad_format']
        if ab not in result:
            result[ab] = {}
        if fmt not in result[ab]:
            result[ab][fmt] = []
        impr = parse_int(r['impressions'])
        rev = parse_float(r['revenue_usd'])
        ecpm = (rev / impr * 1000) if impr and impr > 0 else None
        result[ab][fmt].append({
            'date': r['event_date'],
            'impressions': impr,
            'revenue': rev,
            'ecpm': ecpm,
        })
    return result

max_daily_data = {
    'ball_sort': build_daily_ad_data(max_daily_ball),
    'ios_nuts_sort': build_daily_ad_data(max_daily_nuts),
}

data_json = json.dumps({'lt': lt_data, 'dau': dau_data, 'level': level_data, 'gs_lt': gs_lt_data, 'daily_gs': daily_gs_data, 'max_daily': max_daily_data}, ensure_ascii=False)

# HTML 模板
html = f'''<!DOCTYPE html>
<html lang="zh-CN">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Ad Kill 实验 Dashboard</title>
<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.7/dist/chart.umd.min.js"></script>
<style>
PLACEHOLDER_CSS
</style>
</head>
<body>
<div class="header">
  <h1>Ad Kill 实验 Dashboard</h1>
  <div class="controls">
    <label>产品：</label>
    <select id="productSelect">
      <option value="ball_sort">Ball Sort (Android)</option>
      <option value="ios_nuts_sort">iOS Nuts Sort</option>
    </select>
  </div>
</div>

<details class="info-panel">
  <summary>指标与实验说明</summary>
  <div class="info-content">
    <h3>实验背景</h3>
    <p>Ad Kill 实验测试关闭或减少广告对用户留存和广告收入的影响。通过 game_model 参数将新用户随机分为 A/B 两组。</p>
    <h3>A/B 组定义</h3>
    <ul>
      <li><b>Ball Sort</b>：A 组 = game_model 含 12a，B 组 = 含 12b</li>
      <li><b>iOS Nuts Sort</b>：A 组 = game_model 含 2a，B 组 = 含 2b</li>
    </ul>
    <h3>指标说明</h3>
    <table class="info-table">
      <tr><th>指标</th><th>含义</th></tr>
      <tr><td>Retention Rate</td><td>留存率。第 N 天仍活跃的新用户占比（retained_users / total_new_users）</td></tr>
      <tr><td>LTI (Lifetime Impressions)</td><td>累计人均广告展示次数。从 LT0 到 LTN 的广告展示总量 / 新用户总数</td></tr>
      <tr><td>LTV (Lifetime Value)</td><td>累计人均广告收入（美元）。从 LT0 到 LTN 的广告收入总量 / 新用户总数</td></tr>
      <tr><td>DAU</td><td>日活跃用户数，按日期统计</td></tr>
      <tr><td>分关卡流失率</td><td>存活曲线。对 LT30 内留存用户，统计到达第 N 关的用户占比（max_level &ge; N 的用户数 / 有游戏记录的用户总数）</td></tr>
    </table>
    <h3>广告格式</h3>
    <table class="info-table">
      <tr><th>格式</th><th>说明</th></tr>
      <tr><td>Banner</td><td>横幅广告，通常在屏幕底部持续展示</td></tr>
      <tr><td>Interstitial</td><td>插屏广告，全屏展示，通常在关卡间出现</td></tr>
      <tr><td>Rewarded</td><td>激励视频，用户主动观看以获取游戏内奖励</td></tr>
    </table>
    <h3>数据源</h3>
    <ul>
      <li><b>Hudi</b>：来自 hudi_ods 表的事件数据，通过 SDK 上报</li>
      <li><b>MAX</b>：来自 AppLovin MAX 广告聚合平台的服务端数据</li>
    </ul>
    <h3>观察窗口</h3>
    <p>新用户入组窗口：Ball Sort 2026-01-30 ~ 2026-03-08，iOS Nuts Sort 2026-02-02 ~ 2026-03-08。观察期延续至 2026-04-07，确保所有用户可观察到 LT30。</p>
  </div>
</details>

<div class="conclusion">
  <h2>📊 实验结论</h2>
  <h3>Ball Sort (Android) — 样本量 A=1,576,925 / B=1,577,766</h3>
  <table class="info-table">
    <tr><th>指标</th><th>A 组</th><th>B 组</th><th>差异</th></tr>
    <tr><td>LT1 留存</td><td>33.01%</td><td>33.16%</td><td class="highlight">+0.5%</td></tr>
    <tr><td>LT7 留存</td><td>13.40%</td><td>13.52%</td><td class="highlight">+0.9%</td></tr>
    <tr><td>LT30 留存</td><td>5.87%</td><td>5.91%</td><td class="highlight">+0.7%</td></tr>
    <tr><td>LT30 总 LTV (MAX)</td><td>$0.188</td><td>$0.192</td><td class="highlight">+2.1%</td></tr>
    <tr><td>LT30 Banner LTI (MAX)</td><td>89.1</td><td>93.8</td><td class="highlight">+5.3%</td></tr>
    <tr><td>LT30 Game Starts</td><td>61.67</td><td>62.82</td><td class="highlight">+1.9%</td></tr>
  </table>
  <p>B 组全面优于 A 组，各项指标均有 <span class="highlight-warn">2~5%</span> 的提升，差距较小但方向一致。</p>

  <h3>iOS Nuts Sort — 样本量 A=89,575 / B=88,675</h3>
  <table class="info-table">
    <tr><th>指标</th><th>A 组</th><th>B 组</th><th>差异</th></tr>
    <tr><td>LT1 留存</td><td>24.85%</td><td>26.08%</td><td class="highlight">+5.0%</td></tr>
    <tr><td>LT7 留存</td><td>8.04%</td><td>8.83%</td><td class="highlight">+9.8%</td></tr>
    <tr><td>LT30 留存</td><td>2.70%</td><td>3.34%</td><td class="highlight">+23.7%</td></tr>
    <tr><td>LT30 总 LTV (MAX)</td><td>$0.809</td><td>$0.883</td><td class="highlight">+9.1%</td></tr>
    <tr><td>LT30 Banner LTI (MAX)</td><td>99.6</td><td>126.3</td><td class="highlight">+26.8%</td></tr>
    <tr><td>LT30 Game Starts</td><td>27.43</td><td>32.39</td><td class="highlight">+18.1%</td></tr>
  </table>
  <p>B 组大幅优于 A 组，留存、LTV、开局次数均有 <span class="highlight">双位数提升</span>。LT30 留存提升 23.7%，Banner LTI 提升 26.8%，效果显著。</p>

  <h3>总结</h3>
  <p>B 组策略（杀广告后允许用户跳关）在两个产品上均带来正向收益。iOS Nuts Sort 效果尤为突出，留存和 LTV 均有大幅提升。
  提升的核心驱动力是：允许跳关减少了用户因反复卡关而流失，留存提升带动了更多广告展示和收入。</p>
</div>

<div class="section">
  <h2>Retention Rate <span class="hint">留存率 = 第N天活跃用户 / 新用户总数</span></h2>
  <div class="chart-full"><canvas id="chartRetention"></canvas></div>
</div>

<div class="section">
  <h2>DAU 趋势 <span class="hint">日活跃用户数</span></h2>
  <div class="chart-full"><canvas id="chartDAU"></canvas></div>
</div>

<div class="section">
  <h2>分关卡流失率 <span class="hint">存活曲线：到达第 N 关的用户占比（LT30 内）</span></h2>
  <div class="chart-full"><canvas id="chartLevelSurvival"></canvas></div>
</div>

<div class="section">
  <h2>新用户 LT（累计人均生命周期） <span class="hint">LT = SUM(留存率) from LT0 to LTN</span></h2>
  <div class="chart-full"><canvas id="chartCumLt"></canvas></div>
</div>

<div class="section">
  <h2>新用户累计人均开局次数 <span class="hint">LT0~LT30 累计 game_new_start / 新用户总数</span></h2>
  <div class="chart-full"><canvas id="chartGsLt"></canvas></div>
</div>

<div class="section">
  <h2>全部用户分日人均开局次数 <span class="hint">当日 game_new_start / DAU</span></h2>
  <div class="chart-full"><canvas id="chartDailyGs"></canvas></div>
</div>

<div class="section">
  <h2>全部用户分日广告指标 <span class="hint">Max 平台每日展示与收入</span></h2>
  <div class="grid-header">
    <div class="grid-label"></div>
    <div class="grid-col-header">Banner<br><span class="hint">横幅广告</span></div>
    <div class="grid-col-header">Interstitial<br><span class="hint">插屏广告</span></div>
    <div class="grid-col-header">Rewarded<br><span class="hint">激励视频</span></div>
  </div>
  <div class="grid-row">
    <div class="grid-label">展示次数<br><span class="hint">每日总数</span></div>
    <div class="grid-cell"><canvas id="chart_daily_impressions_banner"></canvas></div>
    <div class="grid-cell"><canvas id="chart_daily_impressions_interstitial"></canvas></div>
    <div class="grid-cell"><canvas id="chart_daily_impressions_rewarded"></canvas></div>
  </div>
  <div class="grid-row">
    <div class="grid-label">收入 (USD)<br><span class="hint">每日总额</span></div>
    <div class="grid-cell"><canvas id="chart_daily_revenue_banner"></canvas></div>
    <div class="grid-cell"><canvas id="chart_daily_revenue_interstitial"></canvas></div>
    <div class="grid-cell"><canvas id="chart_daily_revenue_rewarded"></canvas></div>
  </div>
  <div class="grid-row">
    <div class="grid-label">eCPM<br><span class="hint">每千次展示收益</span></div>
    <div class="grid-cell"><canvas id="chart_daily_ecpm_banner"></canvas></div>
    <div class="grid-cell"><canvas id="chart_daily_ecpm_interstitial"></canvas></div>
    <div class="grid-cell"><canvas id="chart_daily_ecpm_rewarded"></canvas></div>
  </div>
</div>

<div class="section">
  <h2>广告指标 <span class="hint">累计人均值，按广告格式分列</span></h2>
  <div class="grid-header">
    <div class="grid-label"></div>
    <div class="grid-col-header">Banner<br><span class="hint">横幅广告</span></div>
    <div class="grid-col-header">Interstitial<br><span class="hint">插屏广告</span></div>
    <div class="grid-col-header">Rewarded<br><span class="hint">激励视频</span></div>
  </div>
  <div class="grid-row">
    <div class="grid-label">Hudi LTI<br><span class="hint">累计人均展示次数</span></div>
    <div class="grid-cell"><canvas id="chart_hudi_lti_banner"></canvas></div>
    <div class="grid-cell"><canvas id="chart_hudi_lti_interstitial"></canvas></div>
    <div class="grid-cell"><canvas id="chart_hudi_lti_rewarded"></canvas></div>
  </div>
  <div class="grid-row">
    <div class="grid-label">Hudi LTV<br><span class="hint">累计人均收入 ($)</span></div>
    <div class="grid-cell"><canvas id="chart_hudi_ltv_banner"></canvas></div>
    <div class="grid-cell"><canvas id="chart_hudi_ltv_interstitial"></canvas></div>
    <div class="grid-cell"><canvas id="chart_hudi_ltv_rewarded"></canvas></div>
  </div>
  <div class="grid-row">
    <div class="grid-label">MAX LTI<br><span class="hint">累计人均展示次数</span></div>
    <div class="grid-cell"><canvas id="chart_max_lti_banner"></canvas></div>
    <div class="grid-cell"><canvas id="chart_max_lti_interstitial"></canvas></div>
    <div class="grid-cell"><canvas id="chart_max_lti_rewarded"></canvas></div>
  </div>
  <div class="grid-row">
    <div class="grid-label">MAX LTV<br><span class="hint">累计人均收入 ($)</span></div>
    <div class="grid-cell"><canvas id="chart_max_ltv_banner"></canvas></div>
    <div class="grid-cell"><canvas id="chart_max_ltv_interstitial"></canvas></div>
    <div class="grid-cell"><canvas id="chart_max_ltv_rewarded"></canvas></div>
  </div>
</div>

<script>
const DATA = {data_json};
PLACEHOLDER_JS
</script>
</body>
</html>'''

# CSS
css = '''
* { margin: 0; padding: 0; box-sizing: border-box; }
body { font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif; background: #f5f6fa; color: #333; padding: 20px; }
.header { display: flex; align-items: center; justify-content: space-between; margin-bottom: 24px; flex-wrap: wrap; gap: 12px; }
.header h1 { font-size: 22px; font-weight: 600; }
.controls { display: flex; align-items: center; gap: 8px; }
.controls select { padding: 6px 12px; border: 1px solid #ccc; border-radius: 6px; font-size: 14px; }
.section { background: #fff; border-radius: 10px; padding: 20px; margin-bottom: 24px; box-shadow: 0 1px 3px rgba(0,0,0,0.08); }
.section h2 { font-size: 16px; font-weight: 600; margin-bottom: 12px; }
.hint { font-size: 12px; color: #888; font-weight: 400; }
.chart-full { width: 100%; max-height: 300px; }
.chart-full canvas { width: 100% !important; height: 280px !important; }
.grid-header, .grid-row { display: grid; grid-template-columns: 140px 1fr 1fr 1fr; gap: 8px; margin-bottom: 4px; }
.grid-header { margin-bottom: 8px; }
.grid-col-header { text-align: center; font-size: 13px; font-weight: 600; padding: 6px; }
.grid-label { font-size: 13px; font-weight: 600; display: flex; align-items: center; padding-right: 8px; }
.grid-label .hint { display: block; margin-top: 2px; }
.grid-cell { background: #fafbfc; border-radius: 6px; padding: 4px; }
.grid-cell canvas { width: 100% !important; height: 180px !important; }
.grid-row { margin-bottom: 8px; }
.info-panel { background: #fff; border-radius: 10px; padding: 16px; margin-bottom: 24px; box-shadow: 0 1px 3px rgba(0,0,0,0.08); }
.info-panel summary { font-size: 15px; font-weight: 600; cursor: pointer; padding: 4px 0; }
.info-content { padding-top: 12px; font-size: 13px; line-height: 1.7; }
.info-content h3 { font-size: 14px; margin: 12px 0 6px; }
.info-content h3:first-child { margin-top: 0; }
.info-table { border-collapse: collapse; width: 100%; margin: 8px 0; }
.info-table th, .info-table td { border: 1px solid #e0e0e0; padding: 6px 10px; text-align: left; font-size: 13px; }
.info-table th { background: #f5f6fa; font-weight: 600; }
.conclusion { background: #fff; border-radius: 10px; padding: 20px; margin-bottom: 24px; box-shadow: 0 1px 3px rgba(0,0,0,0.08); border-left: 4px solid #22c55e; }
.conclusion h2 { font-size: 16px; font-weight: 600; margin-bottom: 12px; }
.conclusion h3 { font-size: 14px; margin: 16px 0 8px; }
.conclusion h3:first-child { margin-top: 0; }
.conclusion p { font-size: 13px; line-height: 1.7; margin: 6px 0; }
.conclusion .highlight { color: #22c55e; font-weight: 600; }
.conclusion .highlight-warn { color: #f59e0b; font-weight: 600; }
'''

# JS
js = '''
const COLOR_A = 'rgb(59, 130, 246)';
const COLOR_B = 'rgb(239, 68, 68)';
const COLOR_A_BG = 'rgba(59, 130, 246, 0.1)';
const COLOR_B_BG = 'rgba(239, 68, 68, 0.1)';

const charts = {};
const ltDays = Array.from({length: 31}, (_, i) => i);

function destroyAll() {
  Object.values(charts).forEach(c => c.destroy());
  for (const k in charts) delete charts[k];
}

function getRetentionData(product) {
  const lt = DATA.lt[product];
  if (!lt) return null;
  const result = {};
  for (const ab of ['A', 'B']) {
    if (!lt[ab]) continue;
    // retention is same across ad_formats for same lt_day, pick banner
    const fmt = lt[ab]['banner'];
    if (!fmt) continue;
    result[ab] = ltDays.map(d => fmt[d] ? fmt[d].retention_rate : null);
  }
  return result;
}

function getAdMetricData(product, metric, adFormat) {
  const lt = DATA.lt[product];
  if (!lt) return null;
  const result = {};
  for (const ab of ['A', 'B']) {
    if (!lt[ab] || !lt[ab][adFormat]) continue;
    const fmt = lt[ab][adFormat];
    result[ab] = ltDays.map(d => fmt[d] ? fmt[d][metric] : null);
  }
  return result;
}

function getDauData(product) {
  const d = DATA.dau[product];
  if (!d) return null;
  return d;
}

function getDailyAdData(product) {
  const d = DATA.max_daily[product];
  if (!d) return null;
  return d;
}

function makeLineChart(canvasId, labels, dataA, dataB, yLabel, isPercent, opts) {
  opts = opts || {};
  const pr = opts.pointRadius != null ? opts.pointRadius : 2;
  const bw = opts.borderWidth != null ? opts.borderWidth : 2;
  const ctx = document.getElementById(canvasId);
  if (!ctx) return;
  const datasets = [];
  if (dataA) datasets.push({
    label: 'A 组', data: dataA, borderColor: COLOR_A, backgroundColor: COLOR_A_BG,
    borderWidth: bw, pointRadius: pr, tension: 0.3, fill: false, yAxisID: 'y',
  });
  if (dataB) datasets.push({
    label: 'B 组', data: dataB, borderColor: COLOR_B, backgroundColor: COLOR_B_BG,
    borderWidth: bw, pointRadius: pr, tension: 0.3, fill: false, yAxisID: 'y',
  });
  // GAP line: percent → diff (B-A), value → ratio (B/A - 1)
  if (dataA && dataB) {
    const gap = dataA.map((a, i) => {
      const b = dataB[i];
      if (a === null || b === null || a === undefined || b === undefined) return null;
      if (isPercent) return b - a;
      if (a === 0) return null;
      return b / a - 1;
    });
    datasets.push({
      label: isPercent ? 'GAP (B-A)' : 'GAP (B/A-1)',
      data: gap, borderColor: '#FF9800', backgroundColor: 'rgba(255,152,0,0.1)',
      borderWidth: 1.5, pointRadius: pr > 0 ? 1.5 : 0, tension: 0.3, borderDash: [4, 3],
      fill: false, yAxisID: 'yGap',
    });
  }
  const noData = datasets.length === 0 || datasets.every(ds => ds.data.every(v => v === null));
  if (noData) {
    datasets.push({ label: '暂无数据', data: labels.map(() => null), borderColor: '#ccc', borderWidth: 1, yAxisID: 'y' });
  }
  const hasGap = datasets.some(ds => ds.yAxisID === 'yGap');
  charts[canvasId] = new Chart(ctx, {
    type: 'line',
    data: { labels, datasets },
    options: {
      responsive: true, maintainAspectRatio: false,
      plugins: {
        legend: { position: 'top', labels: { font: { size: 11 }, usePointStyle: true, pointStyle: 'line' } },
        tooltip: {
          callbacks: {
            label: function(ctx) {
              let v = ctx.parsed.y;
              if (v === null) return ctx.dataset.label + ': N/A';
              if (ctx.dataset.yAxisID === 'yGap') return ctx.dataset.label + ': ' + (v * 100).toFixed(2) + '%';
              if (isPercent) return ctx.dataset.label + ': ' + (v * 100).toFixed(2) + '%';
              return ctx.dataset.label + ': ' + (v < 1 ? v.toFixed(4) : v.toFixed(2));
            }
          }
        }
      },
      scales: {
        x: { grid: { display: false }, ticks: { font: { size: 10 } } },
        y: {
          position: 'left',
          ticks: {
            font: { size: 10 },
            callback: function(v) {
              if (isPercent) return (v * 100).toFixed(0) + '%';
              return v < 1 ? v.toFixed(3) : v.toLocaleString();
            }
          },
          title: { display: !!yLabel, text: yLabel || '', font: { size: 11 } }
        },
        yGap: {
          position: 'right',
          display: hasGap,
          grid: { drawOnChartArea: false },
          ticks: {
            font: { size: 9 }, color: '#FF9800',
            callback: function(v) { return (v * 100).toFixed(1) + '%'; }
          },
          title: { display: hasGap, text: 'GAP', font: { size: 10 }, color: '#FF9800' }
        }
      },
      interaction: { mode: 'index', intersect: false },
    }
  });
}

function render() {
  destroyAll();
  const product = document.getElementById('productSelect').value;

  // Retention
  const ret = getRetentionData(product);
  makeLineChart('chartRetention', ltDays.map(d => 'LT' + d),
    ret && ret.A, ret && ret.B, '留存率', true);

  // DAU
  const dau = getDauData(product);
  if (dau) {
    const dates = dau.A ? dau.A.map(d => d.date) : (dau.B ? dau.B.map(d => d.date) : []);
    makeLineChart('chartDAU', dates,
      dau.A ? dau.A.map(d => d.dau) : null,
      dau.B ? dau.B.map(d => d.dau) : null,
      'DAU', false);
  } else {
    makeLineChart('chartDAU', [], null, null, 'DAU', false);
  }

  // Level Survival
  const levelData = DATA.level[product];
  if (levelData) {
    const levelsA = levelData.A || [];
    const levelsB = levelData.B || [];
    const allLevels = [...new Set([
      ...levelsA.map(d => d.level),
      ...levelsB.map(d => d.level)
    ])].sort((a, b) => a - b);
    const labels = allLevels.map(l => 'L' + l);
    const mapA = Object.fromEntries(levelsA.map(d => [d.level, d.survival_rate]));
    const mapB = Object.fromEntries(levelsB.map(d => [d.level, d.survival_rate]));
    makeLineChart('chartLevelSurvival', labels,
      allLevels.map(l => mapA[l] ?? null),
      allLevels.map(l => mapB[l] ?? null),
      '存活率', true, { pointRadius: 0, borderWidth: 1.5 });
  } else {
    makeLineChart('chartLevelSurvival', [], null, null, '存活率', true);
  }

  // 新用户 LT（累计人均生命周期）
  const gsLt = DATA.gs_lt[product];
  if (gsLt) {
    const gsLabels = ltDays.map(d => 'LT' + d);
    const mapLtA = gsLt.A ? Object.fromEntries(gsLt.A.map(d => [d.lt_day, d.cum_lt])) : {};
    const mapLtB = gsLt.B ? Object.fromEntries(gsLt.B.map(d => [d.lt_day, d.cum_lt])) : {};
    makeLineChart('chartCumLt', gsLabels,
      ltDays.map(d => mapLtA[d] ?? null),
      ltDays.map(d => mapLtB[d] ?? null),
      'LT (天)', false);

    // 新用户累计人均开局次数
    const mapA = Object.fromEntries(gsLt.A ? gsLt.A.map(d => [d.lt_day, d.avg_cum_game_starts]) : []);
    const mapB = Object.fromEntries(gsLt.B ? gsLt.B.map(d => [d.lt_day, d.avg_cum_game_starts]) : []);
    makeLineChart('chartGsLt', gsLabels,
      ltDays.map(d => mapA[d] ?? null),
      ltDays.map(d => mapB[d] ?? null),
      '累计人均开局', false);
  } else {
    makeLineChart('chartCumLt', [], null, null, 'LT (天)', false);
    makeLineChart('chartGsLt', [], null, null, '累计人均开局', false);
  }

  // 全部用户分日人均开局次数
  const dailyGs = DATA.daily_gs[product];
  if (dailyGs) {
    const dates = dailyGs.A ? dailyGs.A.map(d => d.date) : (dailyGs.B ? dailyGs.B.map(d => d.date) : []);
    makeLineChart('chartDailyGs', dates,
      dailyGs.A ? dailyGs.A.map(d => d.avg_starts) : null,
      dailyGs.B ? dailyGs.B.map(d => d.avg_starts) : null,
      '人均开局', false);
  } else {
    makeLineChart('chartDailyGs', [], null, null, '人均开局', false);
  }

  // 全部用户分日广告指标
  const dailyAd = getDailyAdData(product);
  if (dailyAd) {
    const formats = ['banner', 'interstitial', 'rewarded'];

    for (const fmt of formats) {
      // 收集所有日期
      const allDates = new Set();
      if (dailyAd.A && dailyAd.A[fmt]) {
        dailyAd.A[fmt].forEach(d => allDates.add(d.date));
      }
      if (dailyAd.B && dailyAd.B[fmt]) {
        dailyAd.B[fmt].forEach(d => allDates.add(d.date));
      }
      const dates = Array.from(allDates).sort();

      // 构建数据映射
      const mapA_impr = {};
      const mapA_rev = {};
      const mapA_ecpm = {};
      const mapB_impr = {};
      const mapB_rev = {};
      const mapB_ecpm = {};

      if (dailyAd.A && dailyAd.A[fmt]) {
        dailyAd.A[fmt].forEach(d => {
          mapA_impr[d.date] = d.impressions;
          mapA_rev[d.date] = d.revenue;
          mapA_ecpm[d.date] = d.ecpm;
        });
      }
      if (dailyAd.B && dailyAd.B[fmt]) {
        dailyAd.B[fmt].forEach(d => {
          mapB_impr[d.date] = d.impressions;
          mapB_rev[d.date] = d.revenue;
          mapB_ecpm[d.date] = d.ecpm;
        });
      }

      // 展示次数
      makeLineChart('chart_daily_impressions_' + fmt, dates,
        dates.map(d => mapA_impr[d] ?? null),
        dates.map(d => mapB_impr[d] ?? null),
        '', false);

      // 收入
      makeLineChart('chart_daily_revenue_' + fmt, dates,
        dates.map(d => mapA_rev[d] ?? null),
        dates.map(d => mapB_rev[d] ?? null),
        '', false);

      // eCPM
      makeLineChart('chart_daily_ecpm_' + fmt, dates,
        dates.map(d => mapA_ecpm[d] ?? null),
        dates.map(d => mapB_ecpm[d] ?? null),
        '', false);
    }
  }

  // Ad metrics grid
  const metrics = [
    { key: 'avg_cum_hudi_lti', prefix: 'hudi_lti' },
    { key: 'avg_cum_hudi_ltv', prefix: 'hudi_ltv' },
    { key: 'avg_cum_max_lti', prefix: 'max_lti' },
    { key: 'avg_cum_max_ltv', prefix: 'max_ltv' },
  ];
  const formats = ['banner', 'interstitial', 'rewarded'];
  const labels = ltDays.map(d => 'LT' + d);

  for (const m of metrics) {
    for (const fmt of formats) {
      const d = getAdMetricData(product, m.key, fmt);
      const id = 'chart_' + m.prefix + '_' + fmt;
      makeLineChart(id, labels,
        d && d.A, d && d.B,
        '', false);
    }
  }
}

document.getElementById('productSelect').addEventListener('change', render);
render();
'''

html = html.replace('PLACEHOLDER_CSS', css).replace('PLACEHOLDER_JS', js)

os.makedirs(OUT_DIR, exist_ok=True)
out_path = os.path.join(OUT_DIR, 'ad_kill_dashboard.html')
with open(out_path, 'w', encoding='utf-8') as f:
    f.write(html)

print(f'Dashboard generated: {out_path}')
