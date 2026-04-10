"""读取 ad_kill scene 分析 CSV 数据，生成交互式 HTML dashboard"""
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


def pf(v):
    if v is None or v == '':
        return None
    return float(v)


def pi(v):
    if v is None or v == '':
        return None
    return int(v)


# 读取数据
scene_all = read_csv('ad_kill_level5_scene_distribution.csv')
survival_raw = read_csv('ad_kill_scene_level_survival.csv')


def build_scene_data(rows):
    """构建 {product: {ab_group: [{scene, level5_dau, pv, uv, uv_ratio}]}}"""
    result = {}
    for r in rows:
        prod = r['product']
        ab = r['ab_group']
        result.setdefault(prod, {}).setdefault(ab, [])
        result[prod][ab].append({
            'scene': r['ad_kill_scene'],
            'level5_dau': pi(r['level5_dau']),
            'pv': pi(r['pv']),
            'uv': pi(r['uv']),
            'uv_ratio': pf(r['uv_ratio']),
        })
    return result


def build_survival_data(rows):
    """构建 {product: {ab_group: {scene_group: [{level, retention_rate, ...}]}}}"""
    result = {}
    for r in rows:
        prod = r['product']
        ab = r['ab_group']
        sg = r['scene_group']
        result.setdefault(prod, {}).setdefault(ab, {}).setdefault(sg, [])
        result[prod][ab][sg].append({
            'level': pi(r['level']),
            'retention_rate': pf(r['retention_rate']),
            'retained_users': pi(r['retained_users']),
            'total_users': pi(r['total_users']),
        })
    for prod in result:
        for ab in result[prod]:
            for sg in result[prod][ab]:
                result[prod][ab][sg].sort(key=lambda x: x['level'])
    return result


scene_data = build_scene_data(scene_all)
survival_data = build_survival_data(survival_raw)

data_json = json.dumps({
    'scene': scene_data,
    'survival': survival_data,
}, ensure_ascii=False)

# ============================================================
# CSS
# ============================================================
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
.chart-full { width: 100%; max-height: 350px; }
.chart-full canvas { width: 100% !important; height: 320px !important; }
.chart-row { display: flex; gap: 16px; flex-wrap: wrap; }
.chart-half { flex: 1; min-width: 300px; max-height: 300px; }
.chart-half canvas { width: 100% !important; height: 280px !important; }
.chart-third { flex: 1; min-width: 280px; max-height: 300px; }
.chart-third canvas { width: 100% !important; height: 280px !important; }
.cards { display: flex; gap: 16px; margin-bottom: 16px; flex-wrap: wrap; }
.card { background: #f8f9fc; border-radius: 8px; padding: 16px 24px; flex: 1; min-width: 140px; text-align: center; }
.card .label { font-size: 12px; color: #888; margin-bottom: 4px; }
.card .value { font-size: 24px; font-weight: 700; }
.card .sub { font-size: 12px; color: #666; margin-top: 4px; }
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

# ============================================================
# JS
# ============================================================
js = '''
const COLOR_A = 'rgb(59, 130, 246)';
const COLOR_B = 'rgb(239, 68, 68)';
const SCENE_COLORS = {
  'long_watch_kill': 'rgb(239, 68, 68)',
  'short_watch_repeat_kill': 'rgb(245, 158, 11)',
  'no_ad_kill': 'rgb(34, 197, 94)',
  'none': 'rgb(34, 197, 94)',
  'no_scene': 'rgb(156, 163, 175)',
};
const SCENE_LABELS = {
  'long_watch_kill': '看≥10s后杀广告',
  'short_watch_repeat_kill': '同关卡累计杀≥2次',
  'no_ad_kill': '无杀广告行为',
  'none': '无杀广告行为',
  'no_scene': '其他杀广告 (win插屏/<10s单次)',
};
const PRODUCT_LABELS = {
  'ball_sort': 'Ball Sort (Android)',
  'ios_nuts_sort': 'iOS Nuts Sort',
};

const charts = {};
function destroyAll() {
  Object.values(charts).forEach(c => c.destroy());
  for (const k in charts) delete charts[k];
}
function getProduct() { return document.getElementById('productSelect').value; }
function getMaxLevel() { return parseInt(document.getElementById('maxLevelSelect').value); }
function fmt(n) { return n == null ? '-' : n.toLocaleString(); }
function pct(v) { return v == null ? '-' : (v * 100).toFixed(2) + '%'; }

// ---- Section 1: 第5关 DAU 概览 ----

function renderDauCards(product) {
  const el = document.getElementById('dauCards');
  const sd = DATA.scene[product] || {};
  const dauA = (sd.A || [])[0]?.level5_dau || 0;
  const dauB = (sd.B || [])[0]?.level5_dau || 0;
  el.innerHTML = `
    <div class="card"><div class="label">A 组 Level5 DAU</div><div class="value">${fmt(dauA)}</div></div>
    <div class="card"><div class="label">B 组 Level5 DAU</div><div class="value">${fmt(dauB)}</div></div>
    <div class="card"><div class="label">差异 (B-A)</div><div class="value">${fmt(dauB - dauA)}</div>
      <div class="sub">${((dauB - dauA) / dauA * 100).toFixed(2)}%</div></div>
  `;
}

function renderSceneBar(product) {
  const canvasId = 'chartSceneUV';
  const sd = DATA.scene[product] || {};
  const scenes = ['long_watch_kill', 'short_watch_repeat_kill', 'no_scene'];
  const labels = scenes.map(s => SCENE_LABELS[s] || s);
  const getRow = (ab, scene) => (sd[ab] || []).find(x => x.scene === scene);
  const dsA = scenes.map(s => { const r = getRow('A', s); return r ? r.uv_ratio : 0; });
  const dsB = scenes.map(s => { const r = getRow('B', s); return r ? r.uv_ratio : 0; });
  const uvA = scenes.map(s => { const r = getRow('A', s); return r ? r.uv : 0; });
  const uvB = scenes.map(s => { const r = getRow('B', s); return r ? r.uv : 0; });

  const ctx = document.getElementById(canvasId);
  charts[canvasId] = new Chart(ctx, {
    type: 'bar',
    data: {
      labels,
      datasets: [
        { label: 'A 组', data: dsA, backgroundColor: 'rgba(59,130,246,0.6)', borderColor: COLOR_A, borderWidth: 1, uvData: uvA },
        { label: 'B 组', data: dsB, backgroundColor: 'rgba(239,68,68,0.6)', borderColor: COLOR_B, borderWidth: 1, uvData: uvB },
      ]
    },
    options: {
      responsive: true, maintainAspectRatio: false,
      plugins: {
        title: { display: true, text: '第5关 Scene UV 占比 (UV / Level5 DAU)', font: { size: 14 } },
        tooltip: {
          callbacks: {
            label: function(ctx) {
              const uv = ctx.dataset.uvData[ctx.dataIndex];
              return ctx.dataset.label + ': ' + (ctx.parsed.y * 100).toFixed(2) + '%  (UV: ' + fmt(uv) + ')';
            }
          }
        }
      },
      scales: {
        y: {
          beginAtZero: true,
          title: { display: true, text: 'UV 占比' },
          ticks: { callback: v => (v * 100).toFixed(1) + '%' }
        }
      }
    }
  });
}'''

# ---- Survival chart functions ----
js += '''

// ---- Section 2: 留存曲线 ----

function makeLineChart(canvasId, labels, datasetsArr, yLabel, yPct) {
  const ctx = document.getElementById(canvasId);
  if (!ctx) return;
  charts[canvasId] = new Chart(ctx, {
    type: 'line',
    data: { labels, datasets: datasetsArr },
    options: {
      responsive: true, maintainAspectRatio: false,
      plugins: { legend: { position: 'top' } },
      interaction: { mode: 'index', intersect: false },
      scales: {
        y: {
          beginAtZero: true,
          title: { display: true, text: yLabel },
          ticks: yPct ? { callback: v => (v * 100).toFixed(0) + '%' } : {}
        },
        x: { title: { display: true, text: '关卡' } }
      },
      elements: { point: { radius: 0 }, line: { borderWidth: 2 } }
    }
  });
}

function renderSurvivalByGroup(product) {
  const sv = DATA.survival[product];
  if (!sv) return;
  const maxLevel = getMaxLevel();
  const scenes = ['long_watch_kill', 'short_watch_repeat_kill', 'none', 'no_scene'];

  for (const ab of ['A', 'B']) {
    const canvasId = 'chartSurvival' + ab;
    const datasets = [];
    const abData = sv[ab] || {};
    let allLevels = new Set();
    for (const sg of scenes) {
      if (abData[sg]) abData[sg].forEach(d => { if (d.level <= maxLevel) allLevels.add(d.level); });
    }
    const levels = Array.from(allLevels).sort((a, b) => a - b);
    const labels = levels.map(l => '' + l);

    for (const sg of scenes) {
      const sgData = abData[sg] || [];
      const dataMap = {};
      sgData.forEach(d => { dataMap[d.level] = d.retention_rate; });
      datasets.push({
        label: SCENE_LABELS[sg] || sg,
        data: levels.map(l => dataMap[l] || null),
        borderColor: SCENE_COLORS[sg] || '#999',
        backgroundColor: 'transparent',
        tension: 0.1,
      });
    }
    makeLineChart(canvasId, labels, datasets, '留存率', true);
    const chart = charts[canvasId];
    if (chart) {
      chart.options.plugins.title = { display: true, text: ab + ' 组留存曲线 (按 Scene 分组)', font: { size: 14 } };
      chart.update();
    }
  }
}

function renderSurvivalAB(product) {
  const sv = DATA.survival[product];
  if (!sv) return;
  const maxLevel = getMaxLevel();
  const scenes = ['long_watch_kill', 'short_watch_repeat_kill', 'none', 'no_scene'];
  const container = document.getElementById('survivalAbContainer');
  container.innerHTML = '';

  for (const sg of scenes) {
    const div = document.createElement('div');
    div.className = 'chart-full';
    const canvas = document.createElement('canvas');
    const cid = 'chartSurvAB_' + sg;
    canvas.id = cid;
    div.appendChild(canvas);
    container.appendChild(div);

    const aData = ((sv.A || {})[sg] || []).filter(d => d.level <= maxLevel);
    const bData = ((sv.B || {})[sg] || []).filter(d => d.level <= maxLevel);
    const bMap = {};
    bData.forEach(d => { bMap[d.level] = d.retention_rate; });
    const labels = aData.map(d => '' + d.level);
    const gapData = aData.map(d => {
      const bVal = bMap[d.level];
      return (bVal != null && d.retention_rate != null) ? (bVal - d.retention_rate) : null;
    });

    const datasets = [
      {
        label: 'A 组',
        data: aData.map(d => d.retention_rate),
        borderColor: COLOR_A,
        backgroundColor: 'transparent',
        tension: 0.1,
        yAxisID: 'y',
      },
      {
        label: 'B 组',
        data: aData.map(d => bMap[d.level] ?? null),
        borderColor: COLOR_B,
        backgroundColor: 'transparent',
        tension: 0.1,
        yAxisID: 'y',
      },
      {
        label: 'GAP (B-A)',
        data: gapData,
        borderColor: 'rgb(168, 85, 247)',
        backgroundColor: 'rgba(168, 85, 247, 0.08)',
        borderDash: [4, 3],
        tension: 0.1,
        fill: true,
        yAxisID: 'y1',
      },
    ];

    charts[cid] = new Chart(canvas, {
      type: 'line',
      data: { labels, datasets },
      options: {
        responsive: true, maintainAspectRatio: false,
        plugins: {
          title: { display: true, text: SCENE_LABELS[sg] || sg, font: { size: 13 } },
          legend: { position: 'top' },
          tooltip: {
            callbacks: {
              label: function(ctx) {
                const val = ctx.parsed.y;
                if (val == null) return '';
                if (ctx.dataset.yAxisID === 'y1') {
                  return ctx.dataset.label + ': ' + (val >= 0 ? '+' : '') + (val * 100).toFixed(2) + '%';
                }
                return ctx.dataset.label + ': ' + (val * 100).toFixed(2) + '%';
              }
            }
          },
        },
        interaction: { mode: 'index', intersect: false },
        scales: {
          y: { beginAtZero: true, position: 'left', ticks: { callback: v => (v * 100).toFixed(0) + '%' } },
          y1: {
            position: 'right',
            grid: { drawOnChartArea: false },
            ticks: { callback: v => (v >= 0 ? '+' : '') + (v * 100).toFixed(1) + '%' },
            title: { display: true, text: 'GAP (B-A)' },
          },
          x: { title: { display: true, text: '关卡' } }
        },
        elements: { point: { radius: 0 }, line: { borderWidth: 2 } }
      }
    });
  }
}

// ---- Main render ----

function render() {
  destroyAll();
  const product = getProduct();
  renderDauCards(product);
  renderSceneBar(product);
  renderSurvivalByGroup(product);
  renderSurvivalAB(product);
}

document.getElementById('productSelect').addEventListener('change', render);
document.getElementById('maxLevelSelect').addEventListener('change', render);
render();
'''

# ============================================================
# HTML 模板
# ============================================================
html_tpl = '''<!DOCTYPE html>
<html lang="zh-CN">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Ad Kill Scene 分析 Dashboard</title>
<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.7/dist/chart.umd.min.js"></script>
<style>
__CSS__
</style>
</head>
<body>
<div class="header">
  <h1>Ad Kill Scene 分析 Dashboard</h1>
  <div class="controls">
    <label>产品：</label>
    <select id="productSelect">
      <option value="ball_sort">Ball Sort (Android)</option>
      <option value="ios_nuts_sort">iOS Nuts Sort</option>
    </select>
  </div>
</div>

<details class="info-panel" open>
  <summary>分析说明</summary>
  <div class="info-content">
    <p>本 Dashboard 分析 ad_kill 实验中第5关不同 scene 对用户行为的影响。数据源：<code>transferred.hudi_ods.*</code></p>
    <h3>Scene 判定逻辑</h3>
    <p>ad_kill_scene 按优先级判定：① impression_scene='win'（win 插屏广告被杀）→ ② time_to_kill ≥ 10s → ③ 同 level_id 累计 lib_fullscreen_ad_killed ≥ 2。
    B 组满足条件时允许用户跳关，A 组保持原逻辑（仅上报 scene 不跳关）。</p>
    <p>用户在同一关卡内可能有多次 game_new_start / game_win 事件，每次上报的 ad_kill_scene 可能不同。
    取每用户第5关<b>最晚一条</b> game_new_start 或 game_win 事件的 ad_kill_scene 作为最终 scene。</p>
    <table class="info-table">
      <tr><th>Scene</th><th>说明</th></tr>
      <tr><td>long_watch_kill</td><td>看了 ≥10s 后杀广告 (time_to_kill ≥ 10s)</td></tr>
      <tr><td>short_watch_repeat_kill</td><td>同关卡累计杀广告 ≥2 次 (lib_fullscreen_ad_killed ≥ 2)</td></tr>
      <tr><td>no_scene</td><td>有杀广告行为但不满足上述两个条件（如 win 插屏被杀、看了 &lt;10s 且只杀了1次）</td></tr>
      <tr><td>no_ad_kill (none)</td><td>无杀广告行为（正常广告展示）</td></tr>
    </table>
  </div>
</details>

<div class="conclusion">
  <h2>📊 Scene 维度结论</h2>

  <h3>Scene 分布（第5关杀广告用户占比）</h3>
  <table class="info-table">
    <tr><th>Scene</th><th colspan="2">Ball Sort</th><th colspan="2">iOS Nuts Sort</th></tr>
    <tr><th></th><th>A 组</th><th>B 组</th><th>A 组</th><th>B 组</th></tr>
    <tr><td>看≥10s后杀 (long_watch_kill)</td><td>7.5%</td><td>9.3%</td><td>13.5%</td><td>16.6%</td></tr>
    <tr><td>同关卡累计杀≥2次 (short_watch_repeat_kill)</td><td>0.49%</td><td>0.44%</td><td>2.5%</td><td>2.2%</td></tr>
    <tr><td>其他杀广告 (no_scene)</td><td>2.5%</td><td>2.5%</td><td>2.1%</td><td>2.0%</td></tr>
  </table>
  <p>注：第5关的 scene 分布反映的是用户在第5关结束时的广告行为，此时 AB 分组策略尚未生效（策略从第5关之后才开始），因此 AB 之间的 scene 占比差异属于自然波动。</p>

  <h3>关卡留存对比（从第5关开始）</h3>
  <table class="info-table">
    <tr><th>Scene</th><th colspan="2">Ball Sort (Level 10)</th><th colspan="2">iOS Nuts Sort (Level 10)</th></tr>
    <tr><th></th><th>A 组</th><th>B 组</th><th>A 组</th><th>B 组</th></tr>
    <tr><td>long_watch_kill</td><td>50.1%</td><td class="highlight">53.4%</td><td>30.5%</td><td class="highlight">46.9% (+54%)</td></tr>
    <tr><td>short_watch_repeat_kill</td><td>31.2%</td><td class="highlight">46.0% (+47%)</td><td>29.1%</td><td class="highlight">50.3% (+73%)</td></tr>
    <tr><td>无杀广告 (none)</td><td>60.1%</td><td>61.4%</td><td>40.9%</td><td>43.4%</td></tr>
    <tr><td>其他杀广告 (no_scene)</td><td>84.5%</td><td>85.7%</td><td>79.1%</td><td>83.2%</td></tr>
  </table>

  <h3>核心发现</h3>
  <p>B 组策略（杀广告后允许跳关）对 <span class="highlight">long_watch_kill</span> 和 <span class="highlight">short_watch_repeat_kill</span> 用户的关卡留存提升最为显著。
  iOS Nuts Sort 中，short_watch_repeat_kill 用户的 Level 10 留存从 29.1% 提升到 50.3%（<span class="highlight">+73%</span>），效果极为突出。</p>
  <p>无杀广告用户（none）的留存也有小幅提升，说明 B 组策略对整体生态有正向溢出效应。</p>
</div>

<div class="section">
  <h2>第5关 DAU 概览</h2>
  <div class="cards" id="dauCards"></div>
  <div class="chart-row">
    <div class="chart-full"><canvas id="chartSceneUV"></canvas></div>
  </div>
</div>

<div class="section">
  <h2>留存曲线：按第5关 Scene 分组 <span class="hint">到达第5关的用户，从第5关开始的关卡留存率</span></h2>
  <div class="controls" style="margin-bottom:12px;">
    <label>最大关卡：</label>
    <select id="maxLevelSelect">
      <option value="50">50</option>
      <option value="100" selected>100</option>
      <option value="200">200</option>
      <option value="500">500</option>
    </select>
  </div>
  <div class="chart-full"><canvas id="chartSurvivalA"></canvas></div>
  <div style="height:12px;"></div>
  <div class="chart-full"><canvas id="chartSurvivalB"></canvas></div>
</div>

<div class="section">
  <h2>留存曲线 A vs B (同 Scene) <span class="hint">同一 scene 下 A/B 组对比</span></h2>
  <div id="survivalAbContainer"></div>
</div>

<script>
const DATA = __DATA__;
__JS__
</script>
</body>
</html>'''

html_out = html_tpl.replace('__CSS__', css).replace('__JS__', js).replace('__DATA__', data_json)

os.makedirs(OUT_DIR, exist_ok=True)
out_path = os.path.join(OUT_DIR, 'ad_kill_scene_dashboard.html')
with open(out_path, 'w', encoding='utf-8') as f:
    f.write(html_out)

print(f'Dashboard generated: {out_path}')
