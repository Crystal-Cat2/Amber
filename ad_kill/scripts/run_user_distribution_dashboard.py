"""执行 ad_kill 用户分布 SQL，并生成独立 HTML 页面。"""

from __future__ import annotations

import argparse
import csv
import json
import shutil
import subprocess
import sys
from datetime import datetime
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
SQL_DIR = ROOT / "sql"
DATA_DIR = ROOT / "data"
OUTPUT_DIR = ROOT / "outputs"

DEFAULT_COMPOSITION_SQL = SQL_DIR / "ad_kill_dau_user_composition.sql"
DEFAULT_SCENE_SQL = SQL_DIR / "ad_kill_scene_user_analysis.sql"
DEFAULT_COMPOSITION_CSV = DATA_DIR / "ad_kill_dau_user_composition.csv"
DEFAULT_SCENE_CSV = DATA_DIR / "ad_kill_scene_user_analysis.csv"
DEFAULT_HTML = OUTPUT_DIR / "ad_kill_user_distribution.html"

PRODUCT_OPTIONS = ["ball_sort", "ios_nuts_sort"]
PRODUCT_LABELS = {
    "ball_sort": "Ball Sort (Android)",
    "ios_nuts_sort": "iOS Nuts Sort",
}


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run ad_kill user distribution SQL and render standalone HTML."
    )
    parser.add_argument("--composition-sql", type=Path, default=DEFAULT_COMPOSITION_SQL)
    parser.add_argument("--scene-sql", type=Path, default=DEFAULT_SCENE_SQL)
    parser.add_argument("--composition-csv", type=Path, default=DEFAULT_COMPOSITION_CSV)
    parser.add_argument("--scene-csv", type=Path, default=DEFAULT_SCENE_CSV)
    parser.add_argument("--html", type=Path, default=DEFAULT_HTML)
    parser.add_argument(
        "--skip-query",
        action="store_true",
        help="Skip BigQuery execution and render from existing CSV files.",
    )
    return parser.parse_args(argv)


def run_bq_query(sql_path: Path, csv_path: Path) -> None:
    sql_text = sql_path.read_text(encoding="utf-8")
    command = resolve_bq_command() + [
        "query",
        "--use_legacy_sql=false",
        "--format=csv",
        "--max_rows=1000000",
    ]
    result = subprocess.run(
        command,
        input=sql_text,
        text=True,
        capture_output=True,
        encoding="utf-8",
        errors="strict",
        check=False,
    )
    if result.returncode != 0:
        raise RuntimeError(
            "bq query failed.\n"
            f"SQL: {sql_path}\n"
            f"stderr:\n{result.stderr.strip()}\n\n"
            "Please confirm:\n"
            "1. `gcloud auth login` 或本地 ADC 已完成\n"
            "2. `bq` CLI 可直接访问目标项目\n"
            "3. SQL 中引用的表在当前环境可读"
        )
    csv_path.parent.mkdir(parents=True, exist_ok=True)
    csv_path.write_text(result.stdout, encoding="utf-8", newline="")


def resolve_bq_command() -> list[str]:
    """Windows 下优先使用 bq.cmd，避免 CreateProcess 直接找不到 bq。"""
    bq_cmd = shutil.which("bq.cmd")
    if bq_cmd:
        return [bq_cmd]
    bq_binary = shutil.which("bq")
    if bq_binary:
        return [bq_binary]
    return ["bq.cmd"]


def read_csv_rows(csv_path: Path) -> list[dict[str, str]]:
    with csv_path.open("r", encoding="utf-8-sig", newline="") as file_obj:
        reader = csv.DictReader(file_obj)
        rows: list[dict[str, str]] = []
        for row in reader:
            normalized: dict[str, str] = {}
            for key, value in row.items():
                if key is None:
                    continue
                normalized[str(key).strip().lstrip("\ufeff")] = value or ""
            rows.append(normalized)
        return rows


def parse_int(value: str | None) -> int:
    if value in (None, "", "None"):
        return 0
    return int(float(str(value)))


def parse_float(value: str | None) -> float:
    if value in (None, "", "None"):
        return 0.0
    return float(str(value))


def build_composition_data(rows: list[dict[str, str]]) -> dict[str, dict[str, dict[str, dict[str, float | int]]]]:
    data: dict[str, dict[str, dict[str, dict[str, float | int]]]] = {}
    for row in rows:
        product = row.get("product", "")
        event_date = row.get("event_date", "")
        ab_group = row.get("ab_group", "")
        user_type = row.get("user_type", "").lower()
        if not product or not event_date or not ab_group:
            continue

        product_bucket = data.setdefault(product, {})
        date_bucket = product_bucket.setdefault(event_date, {})
        record = date_bucket.setdefault(
            ab_group,
            {
                "total_dau": 0,
                "new_dau": 0,
                "old_dau": 0,
                "new_ratio": 0.0,
                "old_ratio": 0.0,
            },
        )
        record["total_dau"] = parse_int(row.get("total_dau"))
        if user_type == "new":
            record["new_dau"] = parse_int(row.get("type_dau"))
            record["new_ratio"] = parse_float(row.get("type_ratio"))
        elif user_type == "old":
            record["old_dau"] = parse_int(row.get("type_dau"))
            record["old_ratio"] = parse_float(row.get("type_ratio"))
    return data


def _scene_metrics(row: dict[str, str]) -> dict[str, float | int]:
    return {
        "total_users": parse_int(row.get("total_users")),
        "long_kill_users": parse_int(row.get("long_kill_users")),
        "long_kill_ratio": parse_float(row.get("long_kill_ratio")),
        "short_kill_users": parse_int(row.get("short_kill_users")),
        "short_kill_ratio": parse_float(row.get("short_kill_ratio")),
        "any_kill_users": parse_int(row.get("any_kill_users")),
        "any_kill_ratio": parse_float(row.get("any_kill_ratio")),
    }


def build_scene_data(rows: list[dict[str, str]]) -> dict[str, dict[str, Any]]:
    data: dict[str, dict[str, Any]] = {}
    for row in rows:
        product = row.get("product", "")
        view_type = row.get("view_type", "")
        ab_group = row.get("ab_group", "")
        user_type = row.get("user_type", "").lower()
        if not product or not view_type or not ab_group:
            continue

        product_bucket = data.setdefault(product, {"overall": {}, "by_user_type": {}})
        if view_type == "overall":
            product_bucket["overall"][ab_group] = _scene_metrics(row)
        elif view_type == "by_user_type":
            type_bucket = product_bucket["by_user_type"].setdefault(user_type, {})
            type_bucket[ab_group] = _scene_metrics(row)
    return data


def build_payload(
    composition_rows: list[dict[str, str]],
    scene_rows: list[dict[str, str]],
    composition_sql: Path,
    scene_sql: Path,
) -> dict[str, Any]:
    composition = build_composition_data(composition_rows)
    scene = build_scene_data(scene_rows)
    available_products = [
        product
        for product in PRODUCT_OPTIONS
        if product in composition or product in scene
    ]
    if not available_products:
        available_products = PRODUCT_OPTIONS.copy()

    return {
        "products": available_products,
        "productLabels": PRODUCT_LABELS,
        "composition": composition,
        "scene": scene,
        "generatedAt": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "sources": {
            "compositionSql": composition_sql.name,
            "sceneSql": scene_sql.name,
            "table": "commercial-adx.lmh.ad_kill_detail",
        },
    }


def build_html(payload: dict[str, Any]) -> str:
    data_json = json.dumps(payload, ensure_ascii=False)
    return f"""<!DOCTYPE html>
<html lang="zh-CN">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>Ad Kill 用户分布</title>
  <script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.7/dist/chart.umd.min.js"></script>
  <style>
    :root {{
      --bg: #f4f2ea;
      --panel: #fffdf8;
      --border: #d8d0c2;
      --text: #2b2722;
      --muted: #746a5d;
      --accent-a: #1967d2;
      --accent-b: #d94f30;
      --accent-c: #13795b;
      --shadow: 0 12px 30px rgba(55, 42, 24, 0.08);
    }}
    * {{ box-sizing: border-box; }}
    body {{
      margin: 0;
      font-family: "Segoe UI", "PingFang SC", "Microsoft YaHei", sans-serif;
      background:
        radial-gradient(circle at top right, rgba(217, 79, 48, 0.08), transparent 24%),
        radial-gradient(circle at top left, rgba(25, 103, 210, 0.08), transparent 20%),
        var(--bg);
      color: var(--text);
      padding: 28px;
    }}
    .page {{
      max-width: 1240px;
      margin: 0 auto;
    }}
    .hero {{
      display: flex;
      justify-content: space-between;
      gap: 20px;
      align-items: end;
      margin-bottom: 18px;
    }}
    .hero h1 {{
      margin: 0 0 8px;
      font-size: 34px;
      line-height: 1.1;
    }}
    .subtle {{
      color: var(--muted);
      font-size: 14px;
      line-height: 1.6;
    }}
    .controls {{
      min-width: 220px;
      background: var(--panel);
      border: 1px solid var(--border);
      border-radius: 16px;
      padding: 14px 16px;
      box-shadow: var(--shadow);
    }}
    label {{
      display: block;
      font-size: 13px;
      color: var(--muted);
      margin-bottom: 6px;
    }}
    select {{
      width: 100%;
      padding: 10px 12px;
      border: 1px solid var(--border);
      border-radius: 10px;
      background: #fff;
      color: var(--text);
      font-size: 14px;
    }}
    .meta {{
      background: var(--panel);
      border: 1px solid var(--border);
      border-radius: 18px;
      padding: 16px 18px;
      box-shadow: var(--shadow);
      margin-bottom: 20px;
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(220px, 1fr));
      gap: 12px;
    }}
    .meta strong {{
      display: block;
      font-size: 12px;
      letter-spacing: 0.04em;
      text-transform: uppercase;
      color: var(--muted);
      margin-bottom: 4px;
    }}
    .section {{
      background: var(--panel);
      border: 1px solid var(--border);
      border-radius: 22px;
      padding: 22px;
      box-shadow: var(--shadow);
      margin-bottom: 20px;
    }}
    .section h2 {{
      margin: 0 0 8px;
      font-size: 22px;
    }}
    .section p {{
      margin: 0 0 16px;
      color: var(--muted);
      line-height: 1.6;
    }}
    .summary-grid {{
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(180px, 1fr));
      gap: 14px;
    }}
    .summary-card {{
      background: linear-gradient(180deg, rgba(255,255,255,0.96), rgba(250,247,240,0.96));
      border: 1px solid var(--border);
      border-radius: 18px;
      padding: 16px;
    }}
    .summary-card strong {{
      display: block;
      color: var(--muted);
      font-size: 12px;
      margin-bottom: 8px;
      text-transform: uppercase;
      letter-spacing: 0.06em;
    }}
    .summary-card .value {{
      font-size: 28px;
      font-weight: 700;
    }}
    .summary-card .note {{
      margin-top: 6px;
      color: var(--muted);
      font-size: 13px;
    }}
    .chart-grid {{
      display: grid;
      grid-template-columns: minmax(0, 1.25fr) minmax(0, 1fr);
      gap: 18px;
      align-items: stretch;
    }}
    .chart-panel {{
      background: rgba(255, 255, 255, 0.74);
      border: 1px solid rgba(216, 208, 194, 0.8);
      border-radius: 18px;
      padding: 16px;
    }}
    .chart-panel h3 {{
      margin: 0 0 10px;
      font-size: 16px;
    }}
    .chart-panel canvas {{
      width: 100% !important;
      height: 330px !important;
    }}
    .scene-layout {{
      display: grid;
      grid-template-columns: minmax(0, 1.2fr) minmax(320px, 0.8fr);
      gap: 18px;
      align-items: start;
    }}
    .counts-table {{
      width: 100%;
      border-collapse: collapse;
      font-size: 13px;
    }}
    .counts-table th,
    .counts-table td {{
      border-bottom: 1px solid var(--border);
      padding: 8px 6px;
      text-align: right;
    }}
    .counts-table th:first-child,
    .counts-table td:first-child {{
      text-align: left;
    }}
    .counts-table thead th {{
      color: var(--muted);
      font-weight: 600;
    }}
    .counts-table tbody tr:last-child td {{
      border-bottom: none;
    }}
    .info-list {{
      margin: 0;
      padding-left: 18px;
      color: var(--muted);
      line-height: 1.7;
    }}
    .empty {{
      color: var(--muted);
      padding: 20px 0 6px;
    }}
    @media (max-width: 960px) {{
      body {{ padding: 16px; }}
      .hero,
      .chart-grid,
      .scene-layout {{
        grid-template-columns: 1fr;
        display: grid;
      }}
    }}
  </style>
</head>
<body>
  <div class="page">
    <div class="hero">
      <div>
        <h1>Ad Kill 用户分布</h1>
        <div class="subtle">聚合 ad_kill 实验中的新老用户构成与 ad_kill_scene 分布，聚焦 A/B 两组在用户结构上的差异。</div>
      </div>
      <div class="controls">
        <label for="productSelect">产品</label>
        <select id="productSelect"></select>
      </div>
    </div>

    <div class="meta">
      <div><strong>数据源</strong><span id="metaSource"></span></div>
      <div><strong>Composition SQL</strong><span id="metaCompositionSql"></span></div>
      <div><strong>Scene SQL</strong><span id="metaSceneSql"></span></div>
      <div><strong>生成时间</strong><span id="metaGeneratedAt"></span></div>
    </div>

    <section class="section">
      <h2>概览卡片</h2>
      <p>优先展示当前产品的用户规模、A/B 总量差以及 scene 整体占比，用来快速判断实验组结构是否均衡。</p>
      <div class="summary-grid" id="summaryGrid"></div>
    </section>

    <section class="section">
      <h2>新老用户构成</h2>
      <p>左图使用 100% stacked bar 展示按日期拆分的 A/B 新老用户占比；右图补充 total DAU 走势，避免只看比例忽略绝对量。</p>
      <div class="chart-grid">
        <div class="chart-panel">
          <h3>按日期的新老用户占比</h3>
          <canvas id="compositionChart"></canvas>
          <div id="compositionEmpty" class="empty" hidden>当前产品没有 composition 数据。</div>
        </div>
        <div class="chart-panel">
          <h3>A/B 总 DAU</h3>
          <canvas id="dauChart"></canvas>
          <div id="dauEmpty" class="empty" hidden>当前产品没有 DAU 数据。</div>
        </div>
      </div>
    </section>

    <section class="section">
      <h2>ad_kill_scene 分布</h2>
      <p>上图对比整体与新老用户中的 long / short / any 三类 scene ratio，下方表格保留 counts，便于核对样本规模。</p>
      <div class="scene-layout">
        <div class="chart-panel">
          <h3>scene ratio 对比</h3>
          <canvas id="sceneChart"></canvas>
          <div id="sceneEmpty" class="empty" hidden>当前产品没有 scene 数据。</div>
        </div>
        <div class="chart-panel">
          <h3>counts 明细</h3>
          <div id="countsTableWrap"></div>
        </div>
      </div>
    </section>

    <section class="section">
      <h2>指标说明</h2>
      <ul class="info-list">
        <li><strong>新老用户构成</strong>：来自 `ad_kill_dau_user_composition.sql`，按 `product / event_date / ab_group / user_type` 聚合 distinct user。</li>
        <li><strong>scene 分布</strong>：来自 `ad_kill_scene_user_analysis.sql`，基于第 5 关 `game_new_start` 事件中的 `ad_kill_scene`。</li>
        <li><strong>overall</strong>：所有用户整体占比；<strong>by_user_type</strong>：在 `new / old` 两类中分别观察 A/B 差异。</li>
      </ul>
    </section>
  </div>

  <script>
    const DATA = {data_json};
    const chartStore = {{}};
    const COLOR_A = '#1967d2';
    const COLOR_B = '#d94f30';
    const COLOR_A_LIGHT = 'rgba(25, 103, 210, 0.65)';
    const COLOR_B_LIGHT = 'rgba(217, 79, 48, 0.65)';
    const COLOR_NEW_A = 'rgba(25, 103, 210, 0.88)';
    const COLOR_OLD_A = 'rgba(25, 103, 210, 0.36)';
    const COLOR_NEW_B = 'rgba(217, 79, 48, 0.88)';
    const COLOR_OLD_B = 'rgba(217, 79, 48, 0.36)';

    function destroyChart(id) {{
      if (chartStore[id]) {{
        chartStore[id].destroy();
        delete chartStore[id];
      }}
    }}

    function formatInt(value) {{
      return Number(value || 0).toLocaleString('en-US');
    }}

    function formatPct(value) {{
      return (Number(value || 0) * 100).toFixed(1) + '%';
    }}

    function getCompositionRows(product) {{
      const byDate = DATA.composition[product] || {{}};
      return Object.keys(byDate).sort().map((date) => {{
        const a = byDate[date].A || {{}};
        const b = byDate[date].B || {{}};
        return {{
          date,
          A: Object.assign({{ total_dau: 0, new_dau: 0, old_dau: 0, new_ratio: 0, old_ratio: 0 }}, a),
          B: Object.assign({{ total_dau: 0, new_dau: 0, old_dau: 0, new_ratio: 0, old_ratio: 0 }}, b),
        }};
      }});
    }}

    function getSceneSeries(product) {{
      const scene = DATA.scene[product] || {{ overall: {{}}, by_user_type: {{}} }};
      const groups = [];
      const overallA = scene.overall.A;
      const overallB = scene.overall.B;
      if (overallA || overallB) {{
        groups.push({{ key: 'overall', label: 'overall', A: overallA || {{}}, B: overallB || {{}} }});
      }}
      ['new', 'old'].forEach((userType) => {{
        const typeBucket = scene.by_user_type[userType] || {{}};
        if (typeBucket.A || typeBucket.B) {{
          groups.push({{ key: userType, label: userType, A: typeBucket.A || {{}}, B: typeBucket.B || {{}} }});
        }}
      }});
      return groups;
    }}

    function renderMeta() {{
      document.getElementById('metaSource').textContent = DATA.sources.table;
      document.getElementById('metaCompositionSql').textContent = DATA.sources.compositionSql;
      document.getElementById('metaSceneSql').textContent = DATA.sources.sceneSql;
      document.getElementById('metaGeneratedAt').textContent = DATA.generatedAt;
    }}

    function renderSummary(product) {{
      const rows = getCompositionRows(product);
      const sceneGroups = getSceneSeries(product);
      const totalA = rows.reduce((sum, row) => sum + Number(row.A.total_dau || 0), 0);
      const totalB = rows.reduce((sum, row) => sum + Number(row.B.total_dau || 0), 0);
      const latest = rows.length ? rows[rows.length - 1] : null;
      const overall = sceneGroups.find((item) => item.key === 'overall');
      const summaryGrid = document.getElementById('summaryGrid');
      const cards = [
        {{
          title: 'A 组总 DAU',
          value: formatInt(totalA),
          note: latest ? '最新日期 ' + latest.date : '无日期数据',
        }},
        {{
          title: 'B 组总 DAU',
          value: formatInt(totalB),
          note: latest ? '最新日期 ' + latest.date : '无日期数据',
        }},
        {{
          title: 'B - A 总量差',
          value: formatInt(totalB - totalA),
          note: '用于判断组间规模是否偏移',
        }},
        {{
          title: 'scene any 占比',
          value: overall ? formatPct(overall.B.any_kill_ratio || overall.A.any_kill_ratio || 0) : '0.0%',
          note: overall ? '默认展示 B 组；若缺失则回落到 A 组' : '无 scene 数据',
        }},
      ];
      summaryGrid.innerHTML = cards.map((card) => `
        <div class="summary-card">
          <strong>${{card.title}}</strong>
          <div class="value">${{card.value}}</div>
          <div class="note">${{card.note}}</div>
        </div>
      `).join('');
    }}

    function renderComposition(product) {{
      const rows = getCompositionRows(product);
      const empty = rows.length === 0;
      document.getElementById('compositionEmpty').hidden = !empty;
      document.getElementById('dauEmpty').hidden = !empty;
      destroyChart('compositionChart');
      destroyChart('dauChart');
      if (empty) {{
        return;
      }}

      const labels = rows.map((row) => row.date);
      const compositionCtx = document.getElementById('compositionChart');
      chartStore.compositionChart = new Chart(compositionCtx, {{
        type: 'bar',
        data: {{
          labels,
          datasets: [
            {{ label: 'A 新用户', data: rows.map((row) => row.A.new_ratio), backgroundColor: COLOR_NEW_A, stack: 'A' }},
            {{ label: 'A 老用户', data: rows.map((row) => row.A.old_ratio), backgroundColor: COLOR_OLD_A, stack: 'A' }},
            {{ label: 'B 新用户', data: rows.map((row) => row.B.new_ratio), backgroundColor: COLOR_NEW_B, stack: 'B' }},
            {{ label: 'B 老用户', data: rows.map((row) => row.B.old_ratio), backgroundColor: COLOR_OLD_B, stack: 'B' }},
          ],
        }},
        options: {{
          responsive: true,
          maintainAspectRatio: false,
          scales: {{
            x: {{ stacked: true, ticks: {{ maxRotation: 45, minRotation: 45 }} }},
            y: {{
              stacked: true,
              min: 0,
              max: 1,
              ticks: {{
                callback: (value) => (Number(value) * 100).toFixed(0) + '%'
              }}
            }},
          }},
          plugins: {{
            tooltip: {{
              callbacks: {{
                label: (ctx) => `${{ctx.dataset.label}}: ${{formatPct(ctx.parsed.y)}}`
              }}
            }}
          }}
        }},
      }});

      const dauCtx = document.getElementById('dauChart');
      chartStore.dauChart = new Chart(dauCtx, {{
        type: 'line',
        data: {{
          labels,
          datasets: [
            {{
              label: 'A 组 total DAU',
              data: rows.map((row) => row.A.total_dau),
              borderColor: COLOR_A,
              backgroundColor: COLOR_A_LIGHT,
              pointRadius: 2,
              tension: 0.25,
            }},
            {{
              label: 'B 组 total DAU',
              data: rows.map((row) => row.B.total_dau),
              borderColor: COLOR_B,
              backgroundColor: COLOR_B_LIGHT,
              pointRadius: 2,
              tension: 0.25,
            }},
          ],
        }},
        options: {{
          responsive: true,
          maintainAspectRatio: false,
          scales: {{
            y: {{
              ticks: {{
                callback: (value) => Number(value).toLocaleString('en-US')
              }}
            }}
          }},
          plugins: {{
            tooltip: {{
              callbacks: {{
                label: (ctx) => `${{ctx.dataset.label}}: ${{formatInt(ctx.parsed.y)}}`
              }}
            }}
          }}
        }},
      }});
    }}

    function renderScene(product) {{
      const sceneGroups = getSceneSeries(product);
      const empty = sceneGroups.length === 0;
      document.getElementById('sceneEmpty').hidden = !empty;
      destroyChart('sceneChart');
      if (empty) {{
        document.getElementById('countsTableWrap').innerHTML = '<div class="empty">当前产品没有 scene 明细。</div>';
        return;
      }}

      const labels = [];
      const aValues = [];
      const bValues = [];
      const metricKeys = [
        ['long_kill_ratio', 'long'],
        ['short_kill_ratio', 'short'],
        ['any_kill_ratio', 'any'],
      ];
      sceneGroups.forEach((group) => {{
        metricKeys.forEach(([key, metricLabel]) => {{
          labels.push(`${{group.label}} · ${{metricLabel}}`);
          aValues.push(Number(group.A[key] || 0));
          bValues.push(Number(group.B[key] || 0));
        }});
      }});

      const sceneCtx = document.getElementById('sceneChart');
      chartStore.sceneChart = new Chart(sceneCtx, {{
        type: 'bar',
        data: {{
          labels,
          datasets: [
            {{ label: 'A 组', data: aValues, backgroundColor: COLOR_A_LIGHT }},
            {{ label: 'B 组', data: bValues, backgroundColor: COLOR_B_LIGHT }},
          ],
        }},
        options: {{
          responsive: true,
          maintainAspectRatio: false,
          scales: {{
            y: {{
              beginAtZero: true,
              ticks: {{
                callback: (value) => (Number(value) * 100).toFixed(0) + '%'
              }}
            }}
          }},
          plugins: {{
            tooltip: {{
              callbacks: {{
                label: (ctx) => `${{ctx.dataset.label}}: ${{formatPct(ctx.parsed.y)}}`
              }}
            }}
          }}
        }},
      }});

      const rows = [];
      sceneGroups.forEach((group) => {{
        ['A', 'B'].forEach((abGroup) => {{
          const metrics = group[abGroup] || {{}};
          rows.push(`
            <tr>
              <td>${{group.label}}</td>
              <td>${{abGroup}}</td>
              <td>${{formatInt(metrics.total_users || 0)}}</td>
              <td>${{formatInt(metrics.long_kill_users || 0)}}</td>
              <td>${{formatInt(metrics.short_kill_users || 0)}}</td>
              <td>${{formatInt(metrics.any_kill_users || 0)}}</td>
            </tr>
          `);
        }});
      }});
      document.getElementById('countsTableWrap').innerHTML = `
        <table class="counts-table">
          <thead>
            <tr>
              <th>view</th>
              <th>ab</th>
              <th>total_users</th>
              <th>long</th>
              <th>short</th>
              <th>any</th>
            </tr>
          </thead>
          <tbody>${{rows.join('')}}</tbody>
        </table>
      `;
    }}

    function render() {{
      const product = document.getElementById('productSelect').value;
      renderSummary(product);
      renderComposition(product);
      renderScene(product);
    }}

    function bootstrap() {{
      const select = document.getElementById('productSelect');
      const products = DATA.products.length ? DATA.products : ['ball_sort', 'ios_nuts_sort'];
      select.innerHTML = products.map((product) => `
        <option value="${{product}}">${{DATA.productLabels[product] || product}}</option>
      `).join('');
      renderMeta();
      select.addEventListener('change', render);
      render();
    }}

    bootstrap();
  </script>
</body>
</html>
"""


def write_html(html_path: Path, html_text: str) -> None:
    html_path.parent.mkdir(parents=True, exist_ok=True)
    html_path.write_text(html_text, encoding="utf-8", newline="")


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)

    if not args.skip_query:
        run_bq_query(args.composition_sql, args.composition_csv)
        run_bq_query(args.scene_sql, args.scene_csv)

    composition_rows = read_csv_rows(args.composition_csv)
    scene_rows = read_csv_rows(args.scene_csv)
    payload = build_payload(
        composition_rows,
        scene_rows,
        args.composition_sql,
        args.scene_sql,
    )
    html_text = build_html(payload)
    write_html(args.html, html_text)

    print(f"composition csv: {args.composition_csv}")
    print(f"scene csv: {args.scene_csv}")
    print(f"html: {args.html}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
