"""生成 ball-sort 测试分析页面（假数据）"""
import os
import csv

def color_to_rgba(hex_color):
    """将16进制颜色转换为rgba格式"""
    hex_color = hex_color.lstrip('#')
    r = int(hex_color[0:2], 16)
    g = int(hex_color[2:4], 16)
    b = int(hex_color[4:6], 16)
    return f"{r}, {g}, {b}, 0.1"

OUTPUT_PATH = os.path.join(os.path.dirname(__file__), "..", "outputs", "2026-04-10_ball_sort_test_page.html")
CSV_PATH = os.path.join(os.path.dirname(__file__), "..", "data", "fake_user_metrics.csv")
LTV_CSV_PATH = os.path.join(os.path.dirname(__file__), "..", "data", "fake_user_ltv.csv")
AD_MONETIZATION_CSV_PATH = os.path.join(os.path.dirname(__file__), "..", "data", "fake_ad_monetization.csv")

# 读取CSV数据
user_metrics = []
if os.path.exists(CSV_PATH):
    with open(CSV_PATH, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        user_metrics = list(reader)
else:
    user_metrics = []

user_ltv = []
if os.path.exists(LTV_CSV_PATH):
    with open(LTV_CSV_PATH, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        user_ltv = list(reader)
else:
    user_ltv = []

ad_monetization = []
if os.path.exists(AD_MONETIZATION_CSV_PATH):
    with open(AD_MONETIZATION_CSV_PATH, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        ad_monetization = list(reader)
else:
    ad_monetization = []

HTML = """<!DOCTYPE html>
<html lang="zh-CN">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Ball Sort - 项目分析测试页</title>
<script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
<style>
  * { margin: 0; padding: 0; box-sizing: border-box; }
  body { font-family: -apple-system, "Segoe UI", Roboto, sans-serif; background: #f5f6fa; color: #333; padding: 24px; }
  .container { max-width: 1100px; margin: 0 auto; }
  h1 { font-size: 22px; margin-bottom: 6px; }
  .subtitle { color: #888; font-size: 13px; margin-bottom: 24px; }
  /* KPI cards */
  .kpi-row { display: grid; grid-template-columns: repeat(4, 1fr); gap: 16px; margin-bottom: 28px; }
  .kpi-card { background: #fff; border-radius: 10px; padding: 18px 20px; box-shadow: 0 1px 4px rgba(0,0,0,.06); }
  .kpi-label { font-size: 12px; color: #999; margin-bottom: 4px; }
  .kpi-value { font-size: 26px; font-weight: 700; }
  .kpi-delta { font-size: 12px; margin-top: 4px; }
  .up { color: #2ecc71; }
  .down { color: #e74c3c; }
  /* sections */
  .section { background: #fff; border-radius: 10px; padding: 22px 24px; margin-bottom: 20px; box-shadow: 0 1px 4px rgba(0,0,0,.06); }
  .section h2 { font-size: 16px; margin-bottom: 14px; border-left: 3px solid #4a6cf7; padding-left: 10px; }
  /* table */
  table { width: 100%; border-collapse: collapse; font-size: 13px; }
  th { background: #f0f2f8; text-align: left; padding: 10px 12px; font-weight: 600; }
  td { padding: 9px 12px; border-bottom: 1px solid #eee; }
  tr:hover td { background: #fafbff; }
  /* glossary */
  .glossary-grid { display: grid; grid-template-columns: 1fr 1fr; gap: 10px 24px; font-size: 13px; }
  .glossary-item { display: flex; gap: 8px; }
  .glossary-item dt { font-weight: 600; white-space: nowrap; min-width: 100px; }
  .glossary-item dd { color: #666; }
  /* conclusion */
  .conclusion ul { padding-left: 20px; font-size: 14px; line-height: 1.9; }
  .highlight { background: #fff8e1; padding: 2px 6px; border-radius: 3px; }
  /* charts */
  .chart-row { display: grid; grid-template-columns: 1fr 1fr; gap: 20px; margin-top: 16px; }
  .chart-row.single-chart { grid-template-columns: 1fr; }
  .chart-container { position: relative; height: 300px; }
  @media (max-width: 900px) { .chart-row { grid-template-columns: 1fr; } }
</style>
</head>
<body>
<div class="container">

<h1>Ball Sort 项目分析</h1>
<p class="subtitle">数据周期：2026-03-01 ~ 2026-03-31 | 测试页面（假数据）</p>

<!-- KPI -->
<div class="kpi-row">
  <div class="kpi-card">
    <div class="kpi-label">DAU</div>
    <div class="kpi-value">128,450</div>
    <div class="kpi-delta up">▲ 12.3% vs 上月</div>
  </div>
  <div class="kpi-card">
    <div class="kpi-label">次日留存</div>
    <div class="kpi-value">42.7%</div>
    <div class="kpi-delta up">▲ 1.8pp</div>
  </div>
  <div class="kpi-card">
    <div class="kpi-label">人均时长 (min)</div>
    <div class="kpi-value">18.3</div>
    <div class="kpi-delta down">▼ 0.5 min</div>
  </div>
  <div class="kpi-card">
    <div class="kpi-label">广告 LTV D7 ($)</div>
    <div class="kpi-value">0.87</div>
    <div class="kpi-delta up">▲ 8.6%</div>
  </div>
</div>

<!-- 指标解释 -->
<div class="section">
  <h2>指标解释</h2>
  <div class="glossary-grid">
    <dl class="glossary-item"><dt>DAU</dt><dd>日活跃用户数，当日启动过游戏的去重用户</dd></dl>
    <dl class="glossary-item"><dt>次日留存</dt><dd>新用户次日回访比例 = D1 回访人数 / D0 新增人数</dd></dl>
    <dl class="glossary-item"><dt>人均时长</dt><dd>活跃用户平均单日游戏时长（分钟）</dd></dl>
    <dl class="glossary-item"><dt>广告 LTV D7</dt><dd>新用户 7 日内累计广告收入（美元）</dd></dl>
    <dl class="glossary-item"><dt>关卡通过率</dt><dd>进入该关卡的用户中成功通关的比例</dd></dl>
    <dl class="glossary-item"><dt>ARPPU</dt><dd>付费用户人均收入 = 总收入 / 付费用户数</dd></dl>
  </div>
</div>

<!-- 数据表 -->
<div class="section">
  <h2>关卡漏斗数据</h2>
  <table>
    <thead>
      <tr><th>关卡</th><th>进入人数</th><th>通过人数</th><th>通过率</th><th>平均耗时 (s)</th><th>平均尝试次数</th></tr>
    </thead>
    <tbody>
      <tr><td>Level 1</td><td>128,450</td><td>125,320</td><td>97.6%</td><td>23</td><td>1.0</td></tr>
      <tr><td>Level 5</td><td>98,200</td><td>89,740</td><td>91.4%</td><td>45</td><td>1.3</td></tr>
      <tr><td>Level 10</td><td>72,600</td><td>58,080</td><td>80.0%</td><td>68</td><td>1.8</td></tr>
      <tr><td>Level 20</td><td>45,300</td><td>30,804</td><td>68.0%</td><td>92</td><td>2.4</td></tr>
      <tr><td>Level 30</td><td>28,100</td><td>16,298</td><td>58.0%</td><td>115</td><td>3.1</td></tr>
      <tr><td>Level 50</td><td>12,400</td><td>5,580</td><td>45.0%</td><td>148</td><td>4.2</td></tr>
    </tbody>
  </table>
  <div class="chart-row single-chart">
    <div class="chart-container"><canvas id="funnelChart"></canvas></div>
  </div>
</div>

<div class="section">
  <h2>渠道获客数据</h2>
  <table>
    <thead>
      <tr><th>渠道</th><th>新增用户</th><th>CPI ($)</th><th>次日留存</th><th>LTV D7 ($)</th><th>ROI D7</th></tr>
    </thead>
    <tbody>
      <tr><td>Google Ads</td><td>42,300</td><td>0.35</td><td>44.2%</td><td>0.92</td><td>163%</td></tr>
      <tr><td>Facebook</td><td>31,500</td><td>0.41</td><td>40.8%</td><td>0.85</td><td>107%</td></tr>
      <tr><td>Unity Ads</td><td>18,700</td><td>0.28</td><td>38.5%</td><td>0.78</td><td>179%</td></tr>
      <tr><td>TikTok</td><td>15,200</td><td>0.52</td><td>46.1%</td><td>0.98</td><td>88%</td></tr>
      <tr><td>Organic</td><td>21,800</td><td>—</td><td>48.3%</td><td>1.05</td><td>—</td></tr>
    </tbody>
  </table>
  <div class="chart-row">
    <div class="chart-container"><canvas id="channelUsersChart"></canvas></div>
    <div class="chart-container"><canvas id="channelROIChart"></canvas></div>
  </div>
</div>

<!-- 用户指标数据 -->
<div class="section">
  <h2>用户指标数据</h2>
  <table>
    <thead>
      <tr><th>日期</th><th>国家</th><th>新增用户</th><th>DAU</th><th>次日留存</th><th>七留率</th><th>广告收入 ($)</th><th>IAP收入 ($)</th><th>人均时长 (min)</th></tr>
    </thead>
    <tbody>
      {user_metrics_rows}
    </tbody>
  </table>
  <div class="chart-row">
    <div class="chart-container"><canvas id="userMetricsChart"></canvas></div>
    <div class="chart-container"><canvas id="retentionChart"></canvas></div>
  </div>
</div>

<!-- 用户LTV数据 -->
<div class="section">
  <h2>用户LTV数据</h2>
  <table>
    <thead>
      <tr><th>用户ID</th><th>安装日期</th><th>国家</th><th>设备</th><th>LTV D1 ($)</th><th>LTV D3 ($)</th><th>LTV D7 ($)</th><th>LTV D14 ($)</th><th>LTV D30 ($)</th><th>总会话数</th></tr>
    </thead>
    <tbody>
      {user_ltv_rows}
    </tbody>
  </table>
  <div class="chart-row">
    <div class="chart-container"><canvas id="ltvChart"></canvas></div>
    <div class="chart-container"><canvas id="ltvByCountryChart"></canvas></div>
  </div>
</div>

<!-- 广告变现数据 -->
<div class="section">
  <h2>广告变现数据</h2>
  <table>
    <thead>
      <tr><th>日期</th><th>广告格式</th><th>展示次数</th><th>收入 ($)</th><th>ECPM ($)</th><th>填充率</th></tr>
    </thead>
    <tbody>
      {ad_monetization_rows}
    </tbody>
  </table>
  <div class="chart-row">
    <div class="chart-container"><canvas id="adFormatChart"></canvas></div>
    <div class="chart-container"><canvas id="adRevenueChart"></canvas></div>
  </div>
</div>

<!-- 结论 -->
<div class="section conclusion">
  <h2>分析结论</h2>
  <ul>
    <li><span class="highlight">增长驱动</span>：DAU 环比增长 12.3%，主要由 Google Ads 放量驱动</li>
    <li><span class="highlight">难度优化</span>：Level 20 之后通过率骤降至 68%，建议重点优化 Level 15-25 的难度曲线</li>
    <li><span class="highlight">渠道评估</span>：TikTok 渠道虽 CPI 最高（$0.52），但次日留存最优（46.1%），D7 ROI 尚未回本（88%），需持续观察长期 LTV 表现</li>
    <li><span class="highlight">预算建议</span>：Unity Ads ROI 表现最优（179%），建议适度增加投放预算</li>
    <li><span class="highlight">用户结构</span>：人均时长下降 0.5 min，主要因新用户占比提升，老用户时长保持稳定</li>
  </ul>
</div>

</div>
<script>
const funnelCtx = document.getElementById('funnelChart').getContext('2d');
new Chart(funnelCtx, {
  type: 'bar',
  data: {
    labels: ['Level 1', 'Level 5', 'Level 10', 'Level 20', 'Level 30', 'Level 50'],
    datasets: [{
      label: '进入人数',
      data: [128450, 98200, 72600, 45300, 28100, 12400],
      backgroundColor: '#4a6cf7',
      borderRadius: 4,
      yAxisID: 'y'
    }, {
      label: '通过人数',
      data: [125320, 89740, 58080, 30804, 16298, 5580],
      backgroundColor: '#2ecc71',
      borderRadius: 4,
      yAxisID: 'y'
    }, {
      label: '通过率',
      data: [97.6, 91.4, 80.0, 68.0, 58.0, 45.0],
      borderColor: '#f39c12',
      backgroundColor: 'rgba(243, 156, 18, 0.1)',
      borderWidth: 2,
      fill: true,
      tension: 0.4,
      pointRadius: 5,
      pointBackgroundColor: '#f39c12',
      type: 'line',
      yAxisID: 'y1'
    }]
  },
  options: {
    responsive: true,
    maintainAspectRatio: false,
    plugins: { legend: { position: 'top' } },
    scales: {
      y: { beginAtZero: true, position: 'left' },
      y1: { beginAtZero: true, max: 100, position: 'right', grid: { drawOnChartArea: false } }
    }
  }
});



const channelUsersCtx = document.getElementById('channelUsersChart').getContext('2d');
new Chart(channelUsersCtx, {
  type: 'line',
  data: {
    labels: ['Google Ads', 'Facebook', 'Unity Ads', 'TikTok', 'Organic'],
    datasets: [{
      label: '新增用户',
      data: [42300, 31500, 18700, 15200, 21800],
      borderColor: '#4a6cf7',
      backgroundColor: 'rgba(74, 108, 247, 0.1)',
      borderWidth: 2,
      fill: true,
      tension: 0.4,
      pointRadius: 5,
      pointBackgroundColor: '#4a6cf7',
      yAxisID: 'y'
    }, {
      label: 'CPI ($)',
      data: [0.35, 0.41, 0.28, 0.52, 0],
      borderColor: '#e74c3c',
      backgroundColor: 'rgba(231, 76, 60, 0.1)',
      borderWidth: 2,
      fill: true,
      tension: 0.4,
      pointRadius: 5,
      pointBackgroundColor: '#e74c3c',
      yAxisID: 'y1'
    }]
  },
  options: {
    responsive: true,
    maintainAspectRatio: false,
    plugins: { legend: { position: 'top' } },
    scales: {
      y: { beginAtZero: true, position: 'left' },
      y1: { beginAtZero: true, position: 'right', grid: { drawOnChartArea: false } }
    }
  }
});

const channelROICtx = document.getElementById('channelROIChart').getContext('2d');
new Chart(channelROICtx, {
  type: 'bar',
  data: {
    labels: ['Google Ads', 'Facebook', 'Unity Ads', 'TikTok', 'Organic'],
    datasets: [{
      label: 'ROI D7',
      data: [163, 107, 179, 88, 0],
      backgroundColor: '#2ecc71',
      borderRadius: 4
    }]
  },
  options: {
    responsive: true,
    maintainAspectRatio: false,
    plugins: { legend: { position: 'top' } },
    scales: { y: { beginAtZero: true } }
  }
});

const userMetricsCtx = document.getElementById('userMetricsChart').getContext('2d');
new Chart(userMetricsCtx, {
  type: 'line',
  data: {
    labels: {dates_json},
    datasets: [{
      label: '新增用户',
      data: {new_users_json},
      borderColor: '#4a6cf7',
      backgroundColor: 'rgba(74, 108, 247, 0.1)',
      borderWidth: 2,
      fill: true,
      tension: 0.4,
      yAxisID: 'y'
    }, {
      label: 'DAU',
      data: {active_users_json},
      borderColor: '#2ecc71',
      backgroundColor: 'rgba(46, 204, 113, 0.1)',
      borderWidth: 2,
      fill: true,
      tension: 0.4,
      yAxisID: 'y'
    }]
  },
  options: {
    responsive: true,
    maintainAspectRatio: false,
    plugins: { legend: { position: 'top' } },
    scales: {
      y: { beginAtZero: true, position: 'left' }
    }
  }
});

const retentionCtx = document.getElementById('retentionChart').getContext('2d');
new Chart(retentionCtx, {
  type: 'line',
  data: {
    labels: {dates_json},
    datasets: [{
      label: '次日留存 (D1)',
      data: {retention_d1_json},
      borderColor: '#f39c12',
      backgroundColor: 'rgba(243, 156, 18, 0.1)',
      borderWidth: 2,
      fill: true,
      tension: 0.4,
      pointRadius: 3
    }, {
      label: '七留率 (D7)',
      data: {retention_d7_json},
      borderColor: '#e74c3c',
      backgroundColor: 'rgba(231, 76, 60, 0.1)',
      borderWidth: 2,
      fill: true,
      tension: 0.4,
      pointRadius: 3
    }]
  },
  options: {
    responsive: true,
    maintainAspectRatio: false,
    plugins: { legend: { position: 'top' } },
    scales: { y: { beginAtZero: true, max: 100 } }
  }
});

const ltvCtx = document.getElementById('ltvChart').getContext('2d');
new Chart(ltvCtx, {
  type: 'line',
  data: {
    labels: {ltv_dates_json},
    datasets: [{
      label: 'LTV D1',
      data: {ltv_d1_json},
      borderColor: '#4a6cf7',
      backgroundColor: 'rgba(74, 108, 247, 0.1)',
      borderWidth: 2,
      fill: true,
      tension: 0.4,
      pointRadius: 3
    }, {
      label: 'LTV D7',
      data: {ltv_d7_json},
      borderColor: '#2ecc71',
      backgroundColor: 'rgba(46, 204, 113, 0.1)',
      borderWidth: 2,
      fill: true,
      tension: 0.4,
      pointRadius: 3
    }, {
      label: 'LTV D30',
      data: {ltv_d30_json},
      borderColor: '#e74c3c',
      backgroundColor: 'rgba(231, 76, 60, 0.1)',
      borderWidth: 2,
      fill: true,
      tension: 0.4,
      pointRadius: 3
    }]
  },
  options: {
    responsive: true,
    maintainAspectRatio: false,
    plugins: { legend: { position: 'top' } },
    scales: { y: { beginAtZero: true, position: 'left' } }
  }
});

const ltvByCountryCtx = document.getElementById('ltvByCountryChart').getContext('2d');
new Chart(ltvByCountryCtx, {
  type: 'bar',
  data: {
    labels: {ltv_countries_json},
    datasets: [{
      label: 'LTV D7 平均值',
      data: {ltv_d7_by_country_json},
      backgroundColor: '#4a6cf7',
      borderRadius: 4
    }]
  },
  options: {
    responsive: true,
    maintainAspectRatio: false,
    plugins: { legend: { position: 'top' } },
    scales: { y: { beginAtZero: true, position: 'left' } }
  }
});

const adFormatCtx = document.getElementById('adFormatChart').getContext('2d');
new Chart(adFormatCtx, {
  type: 'bar',
  data: {
    labels: {ad_formats_json},
    datasets: [{
      label: '展示次数',
      data: {ad_impressions_json},
      backgroundColor: '#4a6cf7',
      borderRadius: 4,
      yAxisID: 'y'
    }, {
      label: 'ECPM ($)',
      data: {ad_ecpms_json},
      borderColor: '#f39c12',
      backgroundColor: 'rgba(243, 156, 18, 0.1)',
      borderWidth: 2,
      fill: true,
      tension: 0.4,
      pointRadius: 5,
      pointBackgroundColor: '#f39c12',
      type: 'line',
      yAxisID: 'y1'
    }]
  },
  options: {
    responsive: true,
    maintainAspectRatio: false,
    plugins: { legend: { position: 'top' } },
    scales: {
      y: { beginAtZero: true, position: 'left' },
      y1: { beginAtZero: true, position: 'right', grid: { drawOnChartArea: false } }
    }
  }
});

const adRevenueCtx = document.getElementById('adRevenueChart').getContext('2d');
new Chart(adRevenueCtx, {
  type: 'line',
  data: {
    labels: {revenue_dates_json},
    datasets: [
      {datasets_js}
    ]
  },
  options: {
    responsive: true,
    maintainAspectRatio: false,
    plugins: { legend: { position: 'top' } },
    scales: { y: { beginAtZero: true } }
  }
});
</script>
</body>
</html>
"""

# 生成用户指标表格行
user_metrics_rows = ""
if user_metrics:
    for row in user_metrics:
        d1 = f"{float(row['retention_d1']):.0%}"
        d7 = f"{float(row['retention_d7']):.0%}"
        user_metrics_rows += f"""      <tr><td>{row['date']}</td><td>{row['country']}</td><td>{row['new_users']}</td><td>{row['dau']}</td><td>{d1}</td><td>{d7}</td><td>{row['ad_revenue']}</td><td>{row['iap_revenue']}</td><td>{row['avg_session_min']}</td></tr>
"""

HTML = HTML.replace("{user_metrics_rows}", user_metrics_rows)

# 生成用户LTV表格行
user_ltv_rows = ""
if user_ltv:
    for row in user_ltv:
        user_ltv_rows += f"""      <tr><td>{row['user_id']}</td><td>{row['install_date']}</td><td>{row['country']}</td><td>{row['device']}</td><td>{row['ltv_d1']}</td><td>{row['ltv_d3']}</td><td>{row['ltv_d7']}</td><td>{row['ltv_d14']}</td><td>{row['ltv_d30']}</td><td>{row['total_sessions']}</td></tr>
"""

HTML = HTML.replace("{user_ltv_rows}", user_ltv_rows)

# 准备图表数据
if user_metrics:
    dates = [row['date'] for row in user_metrics]
    new_users = [int(row['new_users']) for row in user_metrics]
    active_users = [int(row['dau']) for row in user_metrics]
    retention_d1 = [round(float(row['retention_d1']) * 100, 1) for row in user_metrics]
    retention_d3 = [0 for _ in user_metrics]
    retention_d7 = [round(float(row['retention_d7']) * 100, 1) for row in user_metrics]
    
    dates_json = str(dates).replace("'", '"')
    new_users_json = str(new_users)
    active_users_json = str(active_users)
    retention_d1_json = str(retention_d1)
    retention_d3_json = str(retention_d3)
    retention_d7_json = str(retention_d7)
else:
    dates_json = '[]'
    new_users_json = '[]'
    active_users_json = '[]'
    retention_d1_json = '[]'
    retention_d3_json = '[]'
    retention_d7_json = '[]'

HTML = HTML.replace("{dates_json}", dates_json)
HTML = HTML.replace("{new_users_json}", new_users_json)
HTML = HTML.replace("{active_users_json}", active_users_json)
HTML = HTML.replace("{retention_d1_json}", retention_d1_json)
HTML = HTML.replace("{retention_d3_json}", retention_d3_json)
HTML = HTML.replace("{retention_d7_json}", retention_d7_json)

# 准备LTV图表数据
if user_ltv:
    ltv_dates = [row['install_date'] for row in user_ltv]
    ltv_d1 = [float(row['ltv_d1']) for row in user_ltv]
    ltv_d7 = [float(row['ltv_d7']) for row in user_ltv]
    ltv_d30 = [float(row['ltv_d30']) for row in user_ltv]

    # 按国家计算LTV D7平均值
    country_ltv = {}
    for row in user_ltv:
        country = row['country']
        ltv = float(row['ltv_d7'])
        if country not in country_ltv:
            country_ltv[country] = []
        country_ltv[country].append(ltv)

    ltv_countries = list(country_ltv.keys())
    ltv_d7_by_country = [round(sum(country_ltv[c]) / len(country_ltv[c]), 2) for c in ltv_countries]

    ltv_dates_json = str(ltv_dates).replace("'", '"')
    ltv_d1_json = str(ltv_d1)
    ltv_d7_json = str(ltv_d7)
    ltv_d30_json = str(ltv_d30)
    ltv_countries_json = str(ltv_countries).replace("'", '"')
    ltv_d7_by_country_json = str(ltv_d7_by_country)
else:
    ltv_dates_json = '[]'
    ltv_d1_json = '[]'
    ltv_d7_json = '[]'
    ltv_d30_json = '[]'
    ltv_countries_json = '[]'
    ltv_d7_by_country_json = '[]'

HTML = HTML.replace("{ltv_dates_json}", ltv_dates_json)
HTML = HTML.replace("{ltv_d1_json}", ltv_d1_json)
HTML = HTML.replace("{ltv_d7_json}", ltv_d7_json)
HTML = HTML.replace("{ltv_d30_json}", ltv_d30_json)
HTML = HTML.replace("{ltv_countries_json}", ltv_countries_json)
HTML = HTML.replace("{ltv_d7_by_country_json}", ltv_d7_by_country_json)

# 生成广告变现表格行
ad_monetization_rows = ""
if ad_monetization:
    for row in ad_monetization:
        fill_rate = f"{float(row['fill_rate']):.0%}"
        ad_monetization_rows += f"""      <tr><td>{row['date']}</td><td>{row['ad_format']}</td><td>{row['impressions']}</td><td>${row['revenue_usd']}</td><td>${row['ecpm']}</td><td>{fill_rate}</td></tr>
"""

HTML = HTML.replace("{ad_monetization_rows}", ad_monetization_rows)

# 准备广告数据图表
if ad_monetization:
    # 按广告格式统计
    ad_format_stats = {}
    for row in ad_monetization:
        fmt = row['ad_format']
        if fmt not in ad_format_stats:
            ad_format_stats[fmt] = {'impressions': 0, 'revenue': 0, 'ecpm': 0, 'count': 0}
        ad_format_stats[fmt]['impressions'] += int(row['impressions'])
        ad_format_stats[fmt]['revenue'] += float(row['revenue_usd'])
        ad_format_stats[fmt]['ecpm'] += float(row['ecpm'])
        ad_format_stats[fmt]['count'] += 1

    # 计算平均ECPM
    for fmt in ad_format_stats:
        ad_format_stats[fmt]['ecpm'] /= ad_format_stats[fmt]['count']

    ad_formats = list(ad_format_stats.keys())
    ad_impressions = [ad_format_stats[fmt]['impressions'] for fmt in ad_formats]
    ad_revenues = [ad_format_stats[fmt]['revenue'] for fmt in ad_formats]
    ad_ecpms = [round(ad_format_stats[fmt]['ecpm'], 2) for fmt in ad_formats]

    # 按日期和格式统计收入
    date_format_revenue = {}
    unique_dates = set()
    for row in ad_monetization:
        date = row['date']
        fmt = row['ad_format']
        revenue = float(row['revenue_usd'])
        unique_dates.add(date)
        if date not in date_format_revenue:
            date_format_revenue[date] = {}
        if fmt not in date_format_revenue[date]:
            date_format_revenue[date][fmt] = 0
        date_format_revenue[date][fmt] += revenue

    sorted_dates = sorted(unique_dates)
    format_names = sorted(ad_format_stats.keys())

    revenue_dates_json = str(sorted_dates).replace("'", '"')

    # 为每个格式生成数据序列
    revenue_datasets = []
    colors = ['#4a6cf7', '#2ecc71', '#f39c12']
    for i, fmt in enumerate(format_names):
        fmt_revenue = [date_format_revenue.get(d, {}).get(fmt, 0) for d in sorted_dates]
        revenue_datasets.append({
            'fmt': fmt,
            'data': fmt_revenue,
            'color': colors[i % len(colors)]
        })

    # 生成 JSON 格式的数据集
    datasets_json = []
    for ds in revenue_datasets:
        color_rgb = ds['color']
        datasets_json.append(f"""{{
      label: '{ds['fmt']}',
      data: {str(ds['data'])},
      borderColor: '{color_rgb}',
      backgroundColor: 'rgba({color_to_rgba(color_rgb)})',
      borderWidth: 2,
      fill: true,
      tension: 0.4,
      pointRadius: 4,
      pointBackgroundColor: '{color_rgb}'
    }}""")

    datasets_js = ','.join(datasets_json)

    ad_formats_json = str(ad_formats).replace("'", '"')
    ad_impressions_json = str(ad_impressions)
    ad_revenues_json = str(ad_revenues)
    ad_ecpms_json = str(ad_ecpms)
else:
    ad_formats_json = '[]'
    ad_impressions_json = '[]'
    ad_revenues_json = '[]'
    ad_ecpms_json = '[]'
    revenue_dates_json = '[]'
    datasets_js = ''

HTML = HTML.replace("{ad_formats_json}", ad_formats_json)
HTML = HTML.replace("{ad_impressions_json}", ad_impressions_json)
HTML = HTML.replace("{ad_revenues_json}", ad_revenues_json)
HTML = HTML.replace("{ad_ecpms_json}", ad_ecpms_json)
HTML = HTML.replace("{revenue_dates_json}", revenue_dates_json)
HTML = HTML.replace("{datasets_js}", datasets_js)

os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)
os.makedirs(os.path.dirname(CSV_PATH), exist_ok=True)
with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
    f.write(HTML)

print(f"已生成: {os.path.abspath(OUTPUT_PATH)}")
