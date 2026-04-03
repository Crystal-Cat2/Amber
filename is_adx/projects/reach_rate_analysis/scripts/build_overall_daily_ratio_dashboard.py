"""构建旧版整体日级比例看板，并新增 hudi/max 比例。"""

from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Any

from google.cloud import bigquery

PROJECT_DIR = Path(__file__).resolve().parents[1]
OUTPUT_DIR = PROJECT_DIR / "outputs"
SQL_DIR = PROJECT_DIR / "sql"
PROJECT_ID = "commercial-adx"
START_DATE = "2025-08-01"
END_DATE = "2026-03-28"
ECHARTS_RELATIVE_PATH = "../../ab_dashboard/assets/echarts.min.js"
CSV_PATH = OUTPUT_DIR / "overall_daily_ratio_20250801_20260328.csv"
JSON_PATH = OUTPUT_DIR / "overall_daily_ratio_20250801_20260328.json"
HTML_PATH = OUTPUT_DIR / "overall_daily_ratio_dashboard_20250801_20260328.html"
SQL_TEMPLATE_PATH = SQL_DIR / "overall_daily_ratio_dashboard.sql"
CHANNEL_SQL_TEMPLATE_PATH = SQL_DIR / "overall_daily_ratio_channel_dashboard.sql"
CHANNEL_CSV_PATH = OUTPUT_DIR / "overall_daily_ratio_channel_20250801_20260328.csv"
CHANNEL_DAILY_CSV_PATH = OUTPUT_DIR / "overall_daily_ratio_channel_daily_20250801_20260328.csv"
CHANNEL_SUMMARY_CSV_PATH = OUTPUT_DIR / "overall_daily_ratio_channel_summary_20250801_20260328.csv"
CHANNEL_SQL_TEMPLATE_PATH = SQL_DIR / "overall_daily_ratio_channel_dashboard.sql"

PRODUCT_META = {
    "screw_puzzle": {"label": "Android", "slug": "android"},
    "ios_screw_puzzle": {"label": "iOS", "slug": "ios"},
}
AD_FORMAT_META = {
    "interstitial": {"label": "Interstitial", "slug": "interstitial"},
    "rewarded": {"label": "Rewarded", "slug": "rewarded"},
}
SECTION_ORDER = [
    ("screw_puzzle", "interstitial"),
    ("screw_puzzle", "rewarded"),
    ("ios_screw_puzzle", "interstitial"),
    ("ios_screw_puzzle", "rewarded"),
]

CHANNEL_MAPPING_NOTES = [
    "Google AdMob = ADMOB_BIDDING + ADMOB_NETWORK",
    "Chartboost = CHARTBOOST_BIDDING + CHARTBOOST_NETWORK",
    "CUSTOM_NETWORK_SDK = IsAdxCustomAdapter + Liftoff_custom + TpAdxCustomAdapter + MaticooCustomAdapter",
    "Liftoff Monetize = VUNGLE_BIDDING",
    "DT Exchange = FYBER_BIDDING",
    "未匹配长尾渠道归入 UNMAPPED",
]


def build_sql(start_date: str, end_date: str) -> str:
    """整体页 SQL：不分 AB、不分渠道，只看 Hudi 与 MAX。"""
    table_suffix_start = start_date.replace("-", "")
    table_suffix_end = end_date.replace("-", "")
    template = SQL_TEMPLATE_PATH.read_text(encoding="utf-8")
    return (
        template.replace("{{ start_date }}", start_date)
        .replace("{{ end_date }}", end_date)
        .replace("{{ table_suffix_start }}", table_suffix_start)
        .replace("{{ table_suffix_end }}", table_suffix_end)
        .strip()
    )


def build_channel_sql(start_date: str, end_date: str) -> str:
    """渠道映射 SQL：输出 Hudi / MAX 的渠道级对比。"""
    table_suffix_start = start_date.replace("-", "")
    table_suffix_end = end_date.replace("-", "")
    template = CHANNEL_SQL_TEMPLATE_PATH.read_text(encoding="utf-8")
    return (
        template.replace("{{ start_date }}", start_date)
        .replace("{{ end_date }}", end_date)
        .replace("{{ table_suffix_start }}", table_suffix_start)
        .replace("{{ table_suffix_end }}", table_suffix_end)
        .strip()
    )


def build_channel_sql(start_date: str, end_date: str) -> str:
    """渠道日级 SQL：按映射后的渠道对比 Hudi 与 MAX impression。"""
    table_suffix_start = start_date.replace("-", "")
    table_suffix_end = end_date.replace("-", "")
    template = CHANNEL_SQL_TEMPLATE_PATH.read_text(encoding="utf-8")
    return (
        template.replace("{{ start_date }}", start_date)
        .replace("{{ end_date }}", end_date)
        .replace("{{ table_suffix_start }}", table_suffix_start)
        .replace("{{ table_suffix_end }}", table_suffix_end)
        .strip()
    )


def run_query(client: bigquery.Client, sql_text: str) -> tuple[list[str], list[dict[str, Any]]]:
    rows_iter = client.query(sql_text).result()
    field_names = [field.name for field in rows_iter.schema]
    return field_names, [dict(row.items()) for row in rows_iter]


def write_csv(path: Path, field_names: list[str], rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8-sig", newline="") as file_obj:
        writer = csv.DictWriter(file_obj, fieldnames=field_names)
        writer.writeheader()
        for row in rows:
            writer.writerow({key: row.get(key) for key in field_names})


def safe_int(value: Any) -> int:
    if value in (None, ""):
        return 0
    return int(value)


def safe_float(value: Any) -> float:
    if value in (None, ""):
        return 0.0
    return float(value)


def build_channel_summary_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """按产品、广告格式、渠道汇总 Hudi / MAX impression。"""
    totals: dict[tuple[str, str, str], dict[str, Any]] = {}
    for row in rows:
        key = (str(row["product"]), str(row["ad_format"]), str(row["mapped_channel"]))
        current = totals.setdefault(
            key,
            {
                "product": key[0],
                "ad_format": key[1],
                "mapped_channel": key[2],
                "hudi_impression_pv": 0,
                "max_impression_pv": 0,
            },
        )
        current["hudi_impression_pv"] += safe_int(row.get("hudi_impression_pv"))
        current["max_impression_pv"] += safe_int(row.get("max_impression_pv"))

    summary_rows: list[dict[str, Any]] = []
    for current in totals.values():
        hudi_impression_pv = safe_int(current["hudi_impression_pv"])
        max_impression_pv = safe_int(current["max_impression_pv"])
        current["hudi_max_rate"] = (hudi_impression_pv / max_impression_pv) if max_impression_pv else 0.0
        current["impression_delta_pv"] = hudi_impression_pv - max_impression_pv
        summary_rows.append(current)

    return sorted(
        summary_rows,
        key=lambda row: (
            row["product"],
            row["ad_format"],
            -safe_int(row["hudi_impression_pv"]),
            row["mapped_channel"],
        ),
    )


def build_channel_summary_rows(channel_daily_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    summary_map: dict[tuple[str, str, str], dict[str, Any]] = {}
    for row in channel_daily_rows:
        key = (
            str(row["product"]),
            str(row["ad_format"]),
            str(row["mapped_channel"]),
        )
        entry = summary_map.setdefault(
            key,
            {
                "product": key[0],
                "ad_format": key[1],
                "mapped_channel": key[2],
                "hudi_impression_pv": 0,
                "max_impression_pv": 0,
            },
        )
        entry["hudi_impression_pv"] += safe_int(row["hudi_impression_pv"])
        entry["max_impression_pv"] += safe_int(row["max_impression_pv"])

    rows = []
    for entry in summary_map.values():
        hudi_impression_pv = safe_int(entry["hudi_impression_pv"])
        max_impression_pv = safe_int(entry["max_impression_pv"])
        rows.append(
            {
                **entry,
                "hudi_max_rate": (hudi_impression_pv / max_impression_pv) if max_impression_pv else 0.0,
                "impression_delta_pv": hudi_impression_pv - max_impression_pv,
            }
        )

    rows.sort(
        key=lambda item: (
            -abs(safe_int(item["impression_delta_pv"])),
            -max(safe_int(item["hudi_impression_pv"]), safe_int(item["max_impression_pv"])),
            str(item["mapped_channel"]),
        )
    )
    return rows


def build_payload(
    overall_rows: list[dict[str, Any]],
    channel_daily_rows: list[dict[str, Any]],
    start_date: str,
    end_date: str,
) -> dict[str, Any]:
    channel_summary_rows = build_channel_summary_rows(channel_daily_rows)
    sections = []
    for product, ad_format in SECTION_ORDER:
        product_meta = PRODUCT_META[product]
        ad_meta = AD_FORMAT_META[ad_format]
        points = [
            {
                "date": str(row["event_date"]),
                "show_pv": safe_int(row["show_pv"]),
                "impression_pv": safe_int(row["impression_pv"]),
                "display_failed_pv": safe_int(row["display_failed_pv"]),
                "max_impression_pv": safe_int(row.get("max_impression_pv")),
                "impression_show_rate": safe_float(row["impression_show_rate"]),
                "impression_plus_failed_show_rate": safe_float(row["impression_plus_failed_show_rate"]),
                "hudi_max_rate": safe_float(row.get("hudi_max_rate")),
            }
            for row in overall_rows
            if str(row["product"]) == product
            and str(row["ad_format"]) == ad_format
            and str(row["event_date"]) <= end_date
        ]
        points.sort(key=lambda item: item["date"])
        channel_summary = [
            {
                "mapped_channel": str(row["mapped_channel"]),
                "hudi_impression_pv": safe_int(row["hudi_impression_pv"]),
                "max_impression_pv": safe_int(row["max_impression_pv"]),
                "hudi_max_rate": safe_float(row["hudi_max_rate"]),
                "impression_delta_pv": safe_int(row["impression_delta_pv"]),
            }
            for row in channel_summary_rows
            if str(row["product"]) == product and str(row["ad_format"]) == ad_format
        ]
        channel_daily: dict[str, list[dict[str, Any]]] = {}
        channel_options = []
        for row in channel_daily_rows:
            if (
                str(row["product"]) != product
                or str(row["ad_format"]) != ad_format
                or str(row["event_date"]) > end_date
            ):
                continue
            mapped_channel = str(row["mapped_channel"])
            if mapped_channel not in channel_daily:
                channel_daily[mapped_channel] = []
                channel_options.append(mapped_channel)
            channel_daily[mapped_channel].append(
                {
                    "date": str(row["event_date"]),
                    "hudi_impression_pv": safe_int(row["hudi_impression_pv"]),
                    "max_impression_pv": safe_int(row["max_impression_pv"]),
                    "hudi_max_rate": safe_float(row["hudi_max_rate"]),
                }
            )
        for daily_points in channel_daily.values():
            daily_points.sort(key=lambda item: item["date"])
        ordered_channels = [row["mapped_channel"] for row in channel_summary]
        for channel_name in channel_options:
            if channel_name not in ordered_channels:
                ordered_channels.append(channel_name)
        default_channel = ordered_channels[0] if ordered_channels else "UNMAPPED"
        sections.append(
            {
                "product": product,
                "product_label": product_meta["label"],
                "ad_format": ad_format,
                "ad_format_label": ad_meta["label"],
                "chart_id": f"{product_meta['slug']}-{ad_meta['slug']}-chart",
                "channel_chart_id": f"{product_meta['slug']}-{ad_meta['slug']}-channel-chart",
                "channel_summary_id": f"{product_meta['slug']}-{ad_meta['slug']}-channel-summary",
                "channel_selector_id": f"{product_meta['slug']}-{ad_meta['slug']}-channel-selector",
                "points": points,
                "latest": points[-1] if points else None,
                "channel_summary": channel_summary,
                "channel_daily": channel_daily,
                "channel_options": ordered_channels,
                "default_channel": default_channel,
            }
        )
    return {
        "title": "show / MAX 对比日级趋势",
        "subtitle": "整体口径，不分 AB、不分渠道；Hudi 与 MAX 均按 UTC-0 天聚合，新增 hudi/max 比例。",
        "start_date": start_date,
        "end_date": end_date,
        "channel_mapping_notes": CHANNEL_MAPPING_NOTES,
        "sections": sections,
    }


def build_html(
    overall_rows: list[dict[str, Any]],
    channel_daily_rows: list[dict[str, Any]],
    start_date: str,
    end_date: str,
) -> str:
    payload = build_payload(overall_rows, channel_daily_rows, start_date, end_date)
    payload_json = json.dumps(payload, ensure_ascii=False)
    cards = []
    for section in payload["sections"]:
        latest = section["latest"] or {
            "date": "-",
            "show_pv": 0,
            "impression_pv": 0,
            "display_failed_pv": 0,
            "max_impression_pv": 0,
            "impression_show_rate": 0.0,
            "impression_plus_failed_show_rate": 0.0,
            "hudi_max_rate": 0.0,
        }
        cards.append(
            f"""
            <section class="card">
              <h2>{section["product_label"]} {section["ad_format_label"]}</h2>
              <p>最新日期 {latest["date"]}：show {latest["show_pv"]:,}，hudi imp {latest["impression_pv"]:,}，failed {latest["display_failed_pv"]:,}，max imp {latest["max_impression_pv"]:,}</p>
              <div class="stats">
                <span class="stat">imp/show：{latest["impression_show_rate"] * 100:.2f}%</span>
                <span class="stat">(imp+failed)/show：{latest["impression_plus_failed_show_rate"] * 100:.2f}%</span>
                <span class="stat">hudi/max：{latest["hudi_max_rate"] * 100:.2f}%</span>
              </div>
              <div id="{section["chart_id"]}" class="chart"></div>
              <div class="channel-panel">
                <div class="channel-head">
                  <h3>渠道汇总</h3>
                  <label>渠道：
                    <select id="{section["channel_selector_id"]}" class="channel-selector"></select>
                  </label>
                </div>
                <div id="{section["channel_summary_id"]}" class="table-wrap"></div>
                <div id="{section["channel_chart_id"]}" class="chart channel-chart"></div>
              </div>
            </section>
            """.strip()
        )

    return f"""<!DOCTYPE html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>整体日级比例看板</title>
  <script src="{ECHARTS_RELATIVE_PATH}"></script>
  <style>
    :root {{
      --bg: #f4efe3;
      --panel: #fffdf7;
      --line-a: #c65b1e;
      --line-b: #1b63c6;
      --line-c: #1a8f4a;
      --text: #2f2c24;
      --muted: #6f6a5d;
      --border: #d8cfbd;
    }}
    * {{ box-sizing: border-box; }}
    body {{ margin: 0; background: radial-gradient(circle at top, #fbf6eb 0%, var(--bg) 58%); color: var(--text); font: 14px/1.5 "Microsoft YaHei", "PingFang SC", sans-serif; }}
    .page {{ max-width: 1560px; margin: 0 auto; padding: 24px; }}
    .hero, .card {{ border: 1px solid var(--border); border-radius: 22px; background: var(--panel); box-shadow: 0 14px 32px rgba(104, 84, 43, 0.08); }}
    .hero {{ padding: 24px 28px; }}
    .hero h1 {{ margin: 0 0 8px; font-size: 30px; }}
    .hero p {{ margin: 0; color: var(--muted); }}
    .meta {{ margin-top: 14px; display: flex; flex-wrap: wrap; gap: 10px; }}
    .tag {{ padding: 6px 10px; border-radius: 999px; background: #f0e8d6; color: var(--text); font-size: 12px; }}
    .grid {{ display: grid; grid-template-columns: repeat(2, minmax(0, 1fr)); gap: 18px; margin-top: 22px; }}
    .card {{ padding: 18px 18px 10px; min-width: 0; }}
    .card h2 {{ margin: 0 0 4px; font-size: 22px; }}
    .card h3 {{ margin: 0; font-size: 16px; }}
    .card p {{ margin: 0 0 10px; color: var(--muted); }}
    .stats {{ display: flex; flex-wrap: wrap; gap: 10px; margin-bottom: 8px; }}
    .stat {{ padding: 6px 10px; border-radius: 999px; background: #f8f2e6; font-size: 12px; }}
    .chart {{ width: 100%; height: 360px; }}
    .channel-chart {{ height: 320px; }}
    .mapping-notes {{ margin-top: 16px; border: 1px dashed var(--border); border-radius: 16px; padding: 12px 14px; background: #faf5ea; }}
    .mapping-notes summary {{ cursor: pointer; font-weight: 700; }}
    .mapping-notes ul {{ margin: 10px 0 0; padding-left: 18px; color: var(--muted); }}
    .channel-panel {{ margin-top: 16px; padding-top: 16px; border-top: 1px solid var(--border); }}
    .channel-head {{ display: flex; align-items: center; justify-content: space-between; gap: 12px; margin-bottom: 10px; flex-wrap: wrap; }}
    .channel-head label {{ color: var(--muted); font-size: 13px; }}
    .channel-head select {{ min-width: 200px; padding: 6px 8px; border-radius: 10px; border: 1px solid var(--border); background: #fff; }}
    .table-wrap {{ overflow-x: auto; border: 1px solid var(--border); border-radius: 14px; background: #fff; }}
    table {{ width: 100%; border-collapse: collapse; min-width: 520px; }}
    th, td {{ padding: 8px 10px; border-bottom: 1px solid #eee4cf; text-align: right; white-space: nowrap; }}
    th:first-child, td:first-child {{ text-align: left; }}
    thead th {{ position: sticky; top: 0; background: #f8f2e6; }}
    tbody tr.active {{ background: #eef4ff; }}
    @media (max-width: 1100px) {{ .grid {{ grid-template-columns: 1fr; }} }}
  </style>
</head>
<body>
  <div class="page">
    <section class="hero">
      <h1>{payload["title"]}</h1>
      <p>{payload["subtitle"]}</p>
      <div class="meta">
        <span class="tag">时间范围：{payload["start_date"]} 到 {payload["end_date"]}</span>
        <span class="tag">统一 UTC-0</span>
        <span class="tag">新增 hudi/max</span>
      </div>
      <details class="mapping-notes">
        <summary>渠道映射说明</summary>
        <ul>
          {"".join(f"<li>{note}</li>" for note in payload["channel_mapping_notes"])}
        </ul>
      </details>
    </section>
    <div class="grid">
      {"".join(cards)}
    </div>
  </div>
  <script>
    const payload = {payload_json};
    function formatPct(value) {{
      return (value * 100).toFixed(2) + '%';
    }}
    function buildSeries(points, key) {{
      return points.map((point) => [point.date, point[key], point.show_pv, point.impression_pv, point.display_failed_pv, point.max_impression_pv]);
    }}
    function buildChannelSeries(points, key) {{
      return points.map((point) => [point.date, point[key], point.hudi_impression_pv, point.max_impression_pv]);
    }}
    function renderChannelSummary(section, selectedChannel) {{
      const host = document.getElementById(section.channel_summary_id);
      if (!host) return;
      const rows = section.channel_summary.map((row) => {{
        const activeClass = row.mapped_channel === selectedChannel ? ' class="active"' : '';
        return `<tr data-channel="${{row.mapped_channel}}"${{activeClass}}>
          <td>${{row.mapped_channel}}</td>
          <td>${{row.hudi_impression_pv.toLocaleString()}}</td>
          <td>${{row.max_impression_pv.toLocaleString()}}</td>
          <td>${{formatPct(row.hudi_max_rate)}}</td>
          <td>${{row.impression_delta_pv.toLocaleString()}}</td>
        </tr>`;
      }}).join('');
      host.innerHTML = `<table>
        <thead><tr><th>mapped_channel</th><th>hudi_impression</th><th>max_impression</th><th>hudi/max</th><th>delta</th></tr></thead>
        <tbody>${{rows}}</tbody>
      </table>`;
      host.querySelectorAll('tbody tr').forEach((row) => {{
        row.addEventListener('click', () => {{
          const selector = document.getElementById(section.channel_selector_id);
          if (selector) {{
            selector.value = row.dataset.channel;
            selector.dispatchEvent(new Event('change'));
          }}
        }});
      }});
    }}
    function renderChannelChart(section, channelName) {{
      const chart = echarts.init(document.getElementById(section.channel_chart_id));
      const points = section.channel_daily[channelName] || [];
      chart.setOption({{
        animation: false,
        color: ['#8d4d11', '#1b63c6', '#1a8f4a'],
        legend: {{ top: 8, data: ['Hudi imp', 'MAX imp', 'hudi/max'] }},
        tooltip: {{
          trigger: 'axis',
          formatter(params) {{
            if (!params.length) return '';
            const meta = params[0].data;
            return [
              params[0].axisValue,
              'Hudi imp: ' + meta[2].toLocaleString(),
              'MAX imp: ' + meta[3].toLocaleString(),
              'hudi/max: ' + formatPct(params[2].data[1]),
            ].join('<br>');
          }}
        }},
        grid: {{ left: 58, right: 56, top: 56, bottom: 62 }},
        xAxis: {{ type: 'category', data: points.map((point) => point.date), axisLabel: {{ rotate: 35 }} }},
        yAxis: [
          {{ type: 'value', name: 'pv' }},
          {{ type: 'value', name: 'ratio', min: 0, max: 1, axisLabel: {{ formatter(value) {{ return (value * 100).toFixed(1) + '%'; }} }} }}
        ],
        dataZoom: [{{ type: 'inside', start: 0, end: 100 }}, {{ type: 'slider', height: 16, bottom: 10 }}],
        series: [
          {{ name: 'Hudi imp', type: 'line', smooth: false, symbol: 'none', data: buildChannelSeries(points, 'hudi_impression_pv') }},
          {{ name: 'MAX imp', type: 'line', smooth: false, symbol: 'none', data: buildChannelSeries(points, 'max_impression_pv') }},
          {{ name: 'hudi/max', type: 'line', smooth: false, symbol: 'none', yAxisIndex: 1, data: buildChannelSeries(points, 'hudi_max_rate') }}
        ]
      }});
    }}
    payload.sections.forEach((section) => {{
      const chart = echarts.init(document.getElementById(section.chart_id));
      chart.setOption({{
        animation: false,
        color: ['#c65b1e', '#1b63c6', '#1a8f4a'],
        legend: {{ top: 8, data: ['imp/show', '(imp+failed)/show', 'hudi/max'] }},
        tooltip: {{
          trigger: 'axis',
          formatter(params) {{
            if (!params.length) return '';
            const meta = params[0].data;
            return [
              params[0].axisValue,
              'show: ' + meta[2].toLocaleString(),
              'hudi impression: ' + meta[3].toLocaleString(),
              'display_failed: ' + meta[4].toLocaleString(),
              'max_impression_pv: ' + meta[5].toLocaleString(),
              'imp/show: ' + formatPct(params[0].data[1]),
              '(imp+failed)/show: ' + formatPct(params[1].data[1]),
              'hudi/max: ' + formatPct(params[2].data[1]),
            ].join('<br>');
          }}
        }},
        grid: {{ left: 58, right: 24, top: 56, bottom: 62 }},
        xAxis: {{ type: 'category', data: section.points.map((point) => point.date), axisLabel: {{ rotate: 35 }} }},
        yAxis: {{
          type: 'value',
          min: (value) => Math.max(0, value.min - 0.01),
          max: 1,
          axisLabel: {{ formatter(value) {{ return (value * 100).toFixed(1) + '%'; }} }}
        }},
        dataZoom: [{{ type: 'inside', start: 0, end: 100 }}, {{ type: 'slider', height: 16, bottom: 10 }}],
        series: [
          {{ name: 'imp/show', type: 'line', smooth: false, symbol: 'none', data: buildSeries(section.points, 'impression_show_rate') }},
          {{ name: '(imp+failed)/show', type: 'line', smooth: false, symbol: 'none', data: buildSeries(section.points, 'impression_plus_failed_show_rate') }},
          {{ name: 'hudi/max', type: 'line', smooth: false, symbol: 'none', data: buildSeries(section.points, 'hudi_max_rate') }}
        ]
      }});
      const selector = document.getElementById(section.channel_selector_id);
      if (selector) {{
        selector.innerHTML = section.channel_options.map((channel) => `<option value="${{channel}}">${{channel}}</option>`).join('');
        selector.value = section.default_channel;
        const render = () => {{
          renderChannelSummary(section, selector.value);
          renderChannelChart(section, selector.value);
        }};
        selector.addEventListener('change', render);
        render();
      }}
    }});
    window.addEventListener('resize', () => {{
      document.querySelectorAll('.chart').forEach((node) => {{
        const chart = echarts.getInstanceByDom(node);
        if (chart) chart.resize();
      }});
    }});
  </script>
</body>
</html>
"""


def main() -> None:
    client = bigquery.Client(project=PROJECT_ID)
    overall_sql_text = build_sql(START_DATE, END_DATE)
    channel_sql_text = build_channel_sql(START_DATE, END_DATE)
    overall_field_names, overall_rows = run_query(client, overall_sql_text)
    channel_field_names, channel_daily_rows = run_query(client, channel_sql_text)
    channel_summary_rows = build_channel_summary_rows(channel_daily_rows)
    write_csv(CSV_PATH, overall_field_names, overall_rows)
    write_csv(CHANNEL_DAILY_CSV_PATH, channel_field_names, channel_daily_rows)
    write_csv(
        CHANNEL_SUMMARY_CSV_PATH,
        [
            "product",
            "ad_format",
            "mapped_channel",
            "hudi_impression_pv",
            "max_impression_pv",
            "hudi_max_rate",
            "impression_delta_pv",
        ],
        channel_summary_rows,
    )
    payload = build_payload(overall_rows, channel_daily_rows, START_DATE, END_DATE)
    JSON_PATH.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    HTML_PATH.write_text(build_html(overall_rows, channel_daily_rows, START_DATE, END_DATE), encoding="utf-8")
    print(
        json.dumps(
            {
                "overall_rows": len(overall_rows),
                "channel_daily_rows": len(channel_daily_rows),
                "channel_summary_rows": len(channel_summary_rows),
            },
            ensure_ascii=False,
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
