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

try:
    from google.cloud import bigquery
except ImportError:  # pragma: no cover - 本地缺依赖时触发
    bigquery = None


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
    try:
        result = subprocess.run(
            command,
            input=sql_text,
            text=True,
            capture_output=True,
            encoding="utf-8",
            errors="strict",
            check=False,
        )
    except FileNotFoundError:
        query_to_csv_via_bigquery_client(sql_text, csv_path)
        return

    if result.returncode == 0:
        csv_path.parent.mkdir(parents=True, exist_ok=True)
        csv_path.write_text(result.stdout, encoding="utf-8", newline="")
        return

    try:
        query_to_csv_via_bigquery_client(sql_text, csv_path)
        return
    except Exception as fallback_error:
        raise RuntimeError(
            "bq query failed.\n"
            f"SQL: {sql_path}\n"
            f"stderr:\n{result.stderr.strip()}\n\n"
            f"fallback_error:\n{fallback_error}\n\n"
            "Please confirm:\n"
            "1. `gcloud auth login` 或本地 ADC 已完成\n"
            "2. `bq` CLI 或本地 ADC 可直接访问目标项目\n"
            "3. SQL 中引用的表在当前环境可读"
        ) from fallback_error


def resolve_bq_command() -> list[str]:
    """Windows 下优先使用 bq.cmd，避免 CreateProcess 直接找不到 bq。"""
    bq_cmd = shutil.which("bq.cmd")
    if bq_cmd:
        return [bq_cmd]
    bq_binary = shutil.which("bq")
    if bq_binary:
        return [bq_binary]
    return ["bq.cmd"]


def query_to_csv_via_bigquery_client(sql_text: str, csv_path: Path) -> None:
    """当 bq CLI 不可用时，回退到 Python BigQuery 客户端。"""
    if bigquery is None:
        raise ImportError("未安装 google-cloud-bigquery，无法走客户端回退。")

    client = bigquery.Client(project="commercial-adx")
    rows_iter = client.query(sql_text).result(page_size=50000)
    field_names = [field.name for field in rows_iter.schema]
    csv_path.parent.mkdir(parents=True, exist_ok=True)
    with csv_path.open("w", encoding="utf-8-sig", newline="") as file_obj:
        writer = csv.DictWriter(file_obj, fieldnames=field_names)
        writer.writeheader()
        for row in rows_iter:
            writer.writerow({field: row.get(field) for field in field_names})


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
        "level5_dau": parse_int(row.get("level5_dau")),
        "pv": parse_int(row.get("pv")),
        "uv": parse_int(row.get("uv")),
        "uv_ratio": parse_float(row.get("uv_ratio")),
    }


def build_scene_data(rows: list[dict[str, str]]) -> dict[str, dict[str, Any]]:
    """按 product -> scene -> overall/by_user_type -> ab_group 组织数据。"""
    data: dict[str, dict[str, Any]] = {}
    for row in rows:
        product = row.get("product", "")
        view_type = row.get("view_type", "")
        ab_group = row.get("ab_group", "")
        user_type = row.get("user_type", "").lower()
        scene = row.get("ad_kill_scene", "")
        if not product or not view_type or not ab_group or not scene:
            continue

        product_bucket = data.setdefault(product, {})
        scene_bucket = product_bucket.setdefault(scene, {"overall": {}, "by_user_type": {}})
        if view_type == "overall":
            scene_bucket["overall"][ab_group] = _scene_metrics(row)
        elif view_type == "by_user_type":
            type_bucket = scene_bucket["by_user_type"].setdefault(user_type, {})
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
    .scene-charts-row {{
      display: grid;
      grid-template-columns: 1fr 1fr;
      gap: 18px;
    }}
    .scene-charts-row .chart-panel canvas {{
      width: 100% !important;
      height: 220px !important;
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
      .scene-layout,
      .scene-charts-row {{
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
      <p>分别展示新增用户和老用户中 long_watch_kill / short_watch_repeat_kill 的占比，确认 A/B 组间 DAU 构成差异。</p>
      <div class="scene-charts-row">
        <div class="chart-panel">
          <h3>新增用户 scene 占比</h3>
          <canvas id="sceneChartNew"></canvas>
          <div id="sceneEmptyNew" class="empty" hidden>无数据</div>
        </div>
        <div class="chart-panel">
          <h3>老用户 scene 占比</h3>
          <canvas id="sceneChartOld"></canvas>
          <div id="sceneEmptyOld" class="empty" hidden>无数据</div>
        </div>
      </div>
      <div style="margin-top: 18px;">
        <div id="sceneTableWrap"></div>
      </div>
    </section>

    <section class="section">
      <h2>指标说明</h2>
      <ul class="info-list">
        <li><strong>新老用户构成</strong>：来自 `ad_kill_dau_user_composition.sql`，仅统计有 `user_engagement` 事件的用户。</li>
        <li><strong>scene 分布</strong>：来自 `ad_kill_scene_user_analysis.sql`，基于第 5 关 `game_new_start` 事件中的 `ad_kill_scene`。</li>
        <li><strong>long_watch_kill</strong>：杀广告时 time_to_kill &ge; 10s；<strong>short_watch_repeat_kill</strong>：同关卡累计杀广告 &ge; 2 次。两者互斥。</li>
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


    function renderMeta() {{
      document.getElementById('metaSource').textContent = DATA.sources.table;
      document.getElementById('metaCompositionSql').textContent = DATA.sources.compositionSql;
      document.getElementById('metaSceneSql').textContent = DATA.sources.sceneSql;
      document.getElementById('metaGeneratedAt').textContent = DATA.generatedAt;
    }}

    function renderSummary(product) {{
      const rows = getCompositionRows(product);
      const scene = DATA.scene[product] || {{}};
      // 优先用周期去重行
      const totalRow = rows.find((r) => r.date === 'total');
      const periodA = totalRow ? totalRow.A.total_dau : 0;
      const periodB = totalRow ? totalRow.B.total_dau : 0;
      const latest = rows.filter((r) => r.date !== 'total');
      const latestDate = latest.length ? latest[latest.length - 1].date : '无日期数据';
      // scene: long_watch_kill overall B uv_ratio
      const longScene = scene['long_watch_kill'] || {{ overall: {{}} }};
      const longBRatio = (longScene.overall.B || {{}}).uv_ratio || 0;
      const summaryGrid = document.getElementById('summaryGrid');
      const cards = [
        {{
          title: 'A 组周期去重 UV',
          value: formatInt(periodA),
          note: '最新日期 ' + latestDate,
        }},
        {{
          title: 'B 组周期去重 UV',
          value: formatInt(periodB),
          note: '最新日期 ' + latestDate,
        }},
        {{
          title: 'B - A 总量差',
          value: formatInt(periodB - periodA),
          note: '用于判断组间规模是否偏移',
        }},
        {{
          title: 'scene long 占比 (B)',
          value: formatPct(longBRatio),
          note: longBRatio ? 'B 组 long_watch_kill UV 占第5关 DAU' : '无 scene 数据',
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
      const allRows = getCompositionRows(product);
      const rows = allRows.filter((r) => r.date !== 'total');
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
      const scene = DATA.scene[product] || {{}};
      const sceneNames = Object.keys(scene).filter((s) => s !== 'none').sort();

      destroyChart('sceneChartNew');
      destroyChart('sceneChartOld');

      const hasData = sceneNames.length > 0;
      document.getElementById('sceneEmptyNew').hidden = hasData;
      document.getElementById('sceneEmptyOld').hidden = hasData;

      if (hasData) {{
        // 新用户: 每个 scene 的 uv_ratio (A vs B)
        const newRatioA = sceneNames.map((s) => Number(((scene[s].by_user_type['new'] || {{}}).A || {{}}).uv_ratio || 0));
        const newRatioB = sceneNames.map((s) => Number(((scene[s].by_user_type['new'] || {{}}).B || {{}}).uv_ratio || 0));
        const newCtx = document.getElementById('sceneChartNew');
        chartStore.sceneChartNew = new Chart(newCtx, {{
          type: 'bar',
          data: {{
            labels: sceneNames,
            datasets: [
              {{ label: 'A 组', data: newRatioA, backgroundColor: COLOR_A_LIGHT }},
              {{ label: 'B 组', data: newRatioB, backgroundColor: COLOR_B_LIGHT }},
            ],
          }},
          options: {{
            indexAxis: 'y',
            responsive: true,
            maintainAspectRatio: false,
            scales: {{ x: {{ beginAtZero: true, ticks: {{ callback: (v) => (Number(v) * 100).toFixed(1) + '%' }} }} }},
            plugins: {{ tooltip: {{ callbacks: {{ label: (ctx) => `${{ctx.dataset.label}}: ${{formatPct(ctx.parsed.x)}}` }} }} }},
          }},
        }});

        // 老用户
        const oldRatioA = sceneNames.map((s) => Number(((scene[s].by_user_type['old'] || {{}}).A || {{}}).uv_ratio || 0));
        const oldRatioB = sceneNames.map((s) => Number(((scene[s].by_user_type['old'] || {{}}).B || {{}}).uv_ratio || 0));
        const oldCtx = document.getElementById('sceneChartOld');
        chartStore.sceneChartOld = new Chart(oldCtx, {{
          type: 'bar',
          data: {{
            labels: sceneNames,
            datasets: [
              {{ label: 'A 组', data: oldRatioA, backgroundColor: COLOR_A_LIGHT }},
              {{ label: 'B 组', data: oldRatioB, backgroundColor: COLOR_B_LIGHT }},
            ],
          }},
          options: {{
            indexAxis: 'y',
            responsive: true,
            maintainAspectRatio: false,
            scales: {{ x: {{ beginAtZero: true, ticks: {{ callback: (v) => (Number(v) * 100).toFixed(1) + '%' }} }} }},
            plugins: {{ tooltip: {{ callbacks: {{ label: (ctx) => `${{ctx.dataset.label}}: ${{formatPct(ctx.parsed.x)}}` }} }} }},
          }},
        }});
      }}

      // 表格：每个 scene 的新老用户 PV / UV / UV占比
      const gap = (a, b) => {{
        const diff = Number(b || 0) - Number(a || 0);
        return (diff >= 0 ? '+' : '') + (diff * 100).toFixed(2) + 'pp';
      }};
      const m = (obj) => obj || {{ level5_dau: 0, pv: 0, uv: 0, uv_ratio: 0 }};
      const tableRows = sceneNames.map((s) => {{
        const newA = m((scene[s].by_user_type['new'] || {{}}).A);
        const newB = m((scene[s].by_user_type['new'] || {{}}).B);
        const oldA = m((scene[s].by_user_type['old'] || {{}}).A);
        const oldB = m((scene[s].by_user_type['old'] || {{}}).B);
        return `<tr>
          <td>${{s}}</td>
          <td>${{formatInt(newA.pv)}}</td>
          <td>${{formatInt(newA.uv)}} (${{formatPct(newA.uv_ratio)}})</td>
          <td>${{formatInt(newB.pv)}}</td>
          <td>${{formatInt(newB.uv)}} (${{formatPct(newB.uv_ratio)}})</td>
          <td>${{gap(newA.uv_ratio, newB.uv_ratio)}}</td>
          <td>${{formatInt(oldA.pv)}}</td>
          <td>${{formatInt(oldA.uv)}} (${{formatPct(oldA.uv_ratio)}})</td>
          <td>${{formatInt(oldB.pv)}}</td>
          <td>${{formatInt(oldB.uv)}} (${{formatPct(oldB.uv_ratio)}})</td>
          <td>${{gap(oldA.uv_ratio, oldB.uv_ratio)}}</td>
        </tr>`;
      }});
      // 第5关 DAU 行
      const firstScene = sceneNames[0] || '';
      const dauNewA = firstScene ? formatInt(m((scene[firstScene].by_user_type['new'] || {{}}).A).level5_dau) : '-';
      const dauNewB = firstScene ? formatInt(m((scene[firstScene].by_user_type['new'] || {{}}).B).level5_dau) : '-';
      const dauOldA = firstScene ? formatInt(m((scene[firstScene].by_user_type['old'] || {{}}).A).level5_dau) : '-';
      const dauOldB = firstScene ? formatInt(m((scene[firstScene].by_user_type['old'] || {{}}).B).level5_dau) : '-';
      document.getElementById('sceneTableWrap').innerHTML = `
        <table class="counts-table">
          <thead>
            <tr>
              <th rowspan="2">scene</th>
              <th colspan="3">新用户</th>
              <th colspan="2"></th>
              <th colspan="3">老用户</th>
              <th colspan="2"></th>
            </tr>
            <tr>
              <th>A PV</th><th>A UV(占比)</th>
              <th>B PV</th><th>B UV(占比)</th><th>B-A gap</th>
              <th>A PV</th><th>A UV(占比)</th>
              <th>B PV</th><th>B UV(占比)</th><th>B-A gap</th>
            </tr>
          </thead>
          <tbody>
            <tr style="color:var(--muted);font-size:12px">
              <td>第5关 DAU</td>
              <td colspan="2">${{dauNewA}}</td>
              <td colspan="2">${{dauNewB}}</td>
              <td></td>
              <td colspan="2">${{dauOldA}}</td>
              <td colspan="2">${{dauOldB}}</td>
              <td></td>
            </tr>
            ${{tableRows.join('')}}
          </tbody>
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
