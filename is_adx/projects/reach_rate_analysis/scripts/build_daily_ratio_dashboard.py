"""构建 AB 日级比例看板，支持渠道与错误分布的分天分析。"""

from __future__ import annotations

import csv
import json
from collections import defaultdict
from pathlib import Path
from typing import Any

from google.cloud import bigquery

PROJECT_DIR = Path(__file__).resolve().parents[1]
OUTPUT_DIR = PROJECT_DIR / "outputs"
SQL_DIR = PROJECT_DIR / "sql"
PROJECT_ID = "commercial-adx"
START_DATE = "2025-09-18"
END_DATE = "2026-03-28"
ECHARTS_RELATIVE_PATH = "../../ab_dashboard/assets/echarts.min.js"

OVERALL_CSV = OUTPUT_DIR / "ab_daily_ratio_20250918_20260328.csv"
CHANNEL_SUMMARY_CSV = OUTPUT_DIR / "ab_channel_summary_20250918_20260328.csv"
CHANNEL_DAILY_CSV = OUTPUT_DIR / "ab_channel_daily_20250918_20260328.csv"
ERROR_DAILY_CSV = OUTPUT_DIR / "ab_error_daily_20250918_20260328.csv"
PAYLOAD_JSON = OUTPUT_DIR / "ab_daily_ratio_dashboard_20250918_20260328.json"
HTML_PATH = OUTPUT_DIR / "ab_daily_ratio_dashboard_20250918_20260328.html"

OVERALL_SQL_PATH = SQL_DIR / "ab_daily_ratio_overall.sql"
CHANNEL_DAILY_SQL_PATH = SQL_DIR / "ab_daily_ratio_channel_daily.sql"
ERROR_DAILY_SQL_PATH = SQL_DIR / "ab_daily_ratio_error_daily.sql"

RAW_NO_NETWORK = "__NO_NETWORK__"
RAW_NO_ERR_MSG = "__NO_ERR_MSG__"
DISPLAY_NO_ERR_MSG = "未上报错误信息"

PRODUCT_META = {
    "screw_puzzle": {"label": "Android", "slug": "android"},
    "ios_screw_puzzle": {"label": "iOS", "slug": "ios"},
}
AD_FORMAT_META = {
    "interstitial": {"label": "Interstitial", "slug": "interstitial"},
    "rewarded": {"label": "Rewarded", "slug": "rewarded"},
}
GROUP_META = {
    "no_is_adx": {"label": "A", "chart_suffix": "a", "line_type": "solid"},
    "have_is_adx": {"label": "B", "chart_suffix": "b", "line_type": "dashed"},
}
SECTION_ORDER = [
    ("screw_puzzle", "interstitial"),
    ("screw_puzzle", "rewarded"),
    ("ios_screw_puzzle", "interstitial"),
    ("ios_screw_puzzle", "rewarded"),
]


def render_sql_template(template_path: Path, replacements: dict[str, str]) -> str:
    """读取 SQL 模板并替换占位符。"""
    template = template_path.read_text(encoding="utf-8")
    for key, value in replacements.items():
        template = template.replace(f"{{{{ {key} }}}}", value)
    return template.strip()


def build_overall_daily_sql(start_date: str, end_date: str) -> str:
    """整体日级 AB 数据 SQL。"""
    return render_sql_template(
        OVERALL_SQL_PATH,
        {
            "start_date": start_date,
            "end_date": end_date,
            "table_suffix_start": start_date.replace("-", ""),
            "table_suffix_end": end_date.replace("-", ""),
        },
    )


def build_channel_daily_sql(start_date: str, end_date: str) -> str:
    """渠道日级 AB 数据 SQL。"""
    return render_sql_template(
        CHANNEL_DAILY_SQL_PATH,
        {
            "start_date": start_date,
            "end_date": end_date,
        },
    )


def build_error_daily_sql(start_date: str, end_date: str) -> str:
    """失败原因日级 AB 数据 SQL。"""
    return render_sql_template(
        ERROR_DAILY_SQL_PATH,
        {
            "start_date": start_date,
            "end_date": end_date,
        },
    )


def run_query(client: bigquery.Client, sql_text: str) -> tuple[list[str], list[dict[str, Any]]]:
    """执行 BigQuery 查询并返回字段名与结果。"""
    rows_iter = client.query(sql_text).result()
    field_names = [field.name for field in rows_iter.schema]
    return field_names, [dict(row.items()) for row in rows_iter]


def write_csv(path: Path, field_names: list[str], rows: list[dict[str, Any]]) -> None:
    """写 CSV 供本地复核。"""
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8-sig", newline="") as file_obj:
        writer = csv.DictWriter(file_obj, fieldnames=field_names)
        writer.writeheader()
        for row in rows:
            writer.writerow({key: row.get(key) for key in field_names})


def safe_int(value: Any) -> int:
    """BigQuery / JSON 值转 int。"""
    if value in (None, ""):
        return 0
    return int(value)


def safe_float(value: Any) -> float:
    """BigQuery / JSON 值转 float。"""
    if value in (None, ""):
        return 0.0
    return float(value)


def normalize_failure_reason(value: Any) -> str:
    """统一空错误信息的展示文案。"""
    text = str(value or "").strip()
    if not text or text == RAW_NO_ERR_MSG:
        return DISPLAY_NO_ERR_MSG
    return text


def build_channel_summary_rows(channel_daily_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """从渠道日级数据派生渠道汇总。"""
    totals: dict[tuple[str, str, str, str], dict[str, Any]] = {}
    for row in channel_daily_rows:
        key = (
            str(row["product"]),
            str(row["ad_format"]),
            str(row["experiment_group"]),
            str(row["network_name"]),
        )
        current = totals.setdefault(
            key,
            {
                "product": key[0],
                "ad_format": key[1],
                "experiment_group": key[2],
                "network_name": key[3],
                "show_pv": 0,
                "impression_pv": 0,
                "display_failed_pv": 0,
            },
        )
        current["show_pv"] += safe_int(row.get("show_pv"))
        current["impression_pv"] += safe_int(row.get("impression_pv"))
        current["display_failed_pv"] += safe_int(row.get("display_failed_pv"))

    summary_rows: list[dict[str, Any]] = []
    for current in totals.values():
        show_pv = safe_int(current["show_pv"])
        impression_pv = safe_int(current["impression_pv"])
        failed_pv = safe_int(current["display_failed_pv"])
        current["impression_show_rate"] = (impression_pv / show_pv) if show_pv else 0.0
        current["impression_plus_failed_show_rate"] = ((impression_pv + failed_pv) / show_pv) if show_pv else 0.0
        summary_rows.append(current)

    return sorted(
        summary_rows,
        key=lambda row: (
            row["product"],
            row["ad_format"],
            row["experiment_group"],
            -safe_int(row["display_failed_pv"]),
            row["network_name"],
        ),
    )


def _sorted_points(points: list[dict[str, Any]], value_key: str | None = None) -> list[dict[str, Any]]:
    """按日期或数值排序。"""
    if value_key is None:
        return sorted(points, key=lambda item: item["date"])
    return sorted(points, key=lambda item: (-safe_int(item[value_key]), item.get("failure_reason", "")))


def _sort_reason_items(items: dict[str, int]) -> list[tuple[str, int]]:
    """真实 reason 优先，缺失文案永远放最后。"""
    return sorted(
        items.items(),
        key=lambda item: (item[0] == DISPLAY_NO_ERR_MSG, -item[1], item[0]),
    )


def calc_gap_vs_a_pp(a_value: float, b_value: float) -> float:
    """计算 B-A 的百分点差值。"""
    return (b_value - a_value) * 100


def attach_rate_gap_fields(
    grouped_points: dict[str, list[dict[str, Any]]],
    value_gap_pairs: list[tuple[str, str]],
) -> dict[str, list[dict[str, Any]]]:
    """为同一图上的 A/B 点补充按日期对齐的 GAP 字段。"""
    points_by_group_and_date = {
        experiment_group: {point["date"]: point for point in points}
        for experiment_group, points in grouped_points.items()
    }
    all_dates = sorted(
        {
            point_date
            for point_map in points_by_group_and_date.values()
            for point_date in point_map
        }
    )
    for point_date in all_dates:
        a_point = points_by_group_and_date.get("no_is_adx", {}).get(point_date)
        b_point = points_by_group_and_date.get("have_is_adx", {}).get(point_date)
        for value_key, gap_key in value_gap_pairs:
            gap_value = calc_gap_vs_a_pp(
                safe_float(a_point.get(value_key) if a_point else 0.0),
                safe_float(b_point.get(value_key) if b_point else 0.0),
            )
            if a_point is not None:
                a_point[gap_key] = gap_value
            if b_point is not None:
                b_point[gap_key] = gap_value

    for points in grouped_points.values():
        for point in points:
            for _, gap_key in value_gap_pairs:
                point.setdefault(gap_key, 0.0)
    return grouped_points


def build_payload(
    overall_rows: list[dict[str, Any]],
    channel_daily_rows: list[dict[str, Any]],
    error_rows: list[dict[str, Any]],
    start_date: str,
    end_date: str,
) -> dict[str, Any]:
    """构建 HTML 用 payload。"""
    channel_summary_rows = build_channel_summary_rows(channel_daily_rows)

    overall_map: dict[tuple[str, str, str], list[dict[str, Any]]] = defaultdict(list)
    for row in overall_rows:
        key = (str(row["product"]), str(row["ad_format"]), str(row["experiment_group"]))
        overall_map[key].append(
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
        )

    channel_daily_map: dict[tuple[str, str, str, str], list[dict[str, Any]]] = defaultdict(list)
    for row in channel_daily_rows:
        key = (
            str(row["product"]),
            str(row["ad_format"]),
            str(row["network_name"]),
            str(row["experiment_group"]),
        )
        channel_daily_map[key].append(
            {
                "date": str(row["event_date"]),
                "show_pv": safe_int(row["show_pv"]),
                "impression_pv": safe_int(row["impression_pv"]),
                "display_failed_pv": safe_int(row["display_failed_pv"]),
                "impression_show_rate": safe_float(row["impression_show_rate"]),
                "impression_plus_failed_show_rate": safe_float(row["impression_plus_failed_show_rate"]),
            }
        )

    error_totals: dict[tuple[str, str, str, str, str], int] = defaultdict(int)
    error_reason_daily_totals: dict[tuple[str, str, str, str, str, str], int] = defaultdict(int)
    error_daily_group_totals: dict[tuple[str, str, str, str, str], int] = defaultdict(int)
    for row in error_rows:
        product = str(row["product"])
        ad_format = str(row["ad_format"])
        network_name = str(row["network_name"])
        experiment_group = str(row["experiment_group"])
        event_date = str(row["event_date"])
        failure_reason = normalize_failure_reason(row["failure_reason"])
        reason_pv = safe_int(row["reason_pv"])
        total_key = (product, ad_format, network_name, experiment_group, failure_reason)
        error_totals[total_key] += reason_pv
        error_reason_daily_totals[(product, ad_format, network_name, experiment_group, failure_reason, event_date)] += reason_pv
        error_daily_group_totals[(product, ad_format, network_name, experiment_group, event_date)] += reason_pv

    sections: list[dict[str, Any]] = []
    summary_index: dict[tuple[str, str], dict[str, dict[str, Any]]] = defaultdict(dict)
    combined_failed_totals: dict[tuple[str, str, str], int] = defaultdict(int)
    for row in channel_summary_rows:
        key = (str(row["product"]), str(row["ad_format"]))
        network_name = str(row["network_name"])
        summary_index[key].setdefault(network_name, {})
        summary_index[key][network_name][str(row["experiment_group"])] = row
        combined_failed_totals[(key[0], key[1], network_name)] += safe_int(row["display_failed_pv"])

    for product, ad_format in SECTION_ORDER:
        product_meta = PRODUCT_META[product]
        ad_meta = AD_FORMAT_META[ad_format]
        slug = f"{product_meta['slug']}-{ad_meta['slug']}"
        overall = attach_rate_gap_fields(
            {
                experiment_group: _sorted_points(overall_map[(product, ad_format, experiment_group)])
                for experiment_group in GROUP_META
            },
            [
                ("impression_show_rate", "impression_show_gap_vs_a_pp"),
                ("impression_plus_failed_show_rate", "impression_plus_failed_gap_vs_a_pp"),
                ("hudi_max_rate", "hudi_max_gap_vs_a_pp"),
            ],
        )

        network_items = []
        for network_name, group_rows in summary_index[(product, ad_format)].items():
            no_row = group_rows.get("no_is_adx")
            have_row = group_rows.get("have_is_adx")
            network_items.append(
                {
                    "network_name": network_name,
                    "combined_display_failed_pv": combined_failed_totals[(product, ad_format, network_name)],
                    "no_is_adx": {
                        "show_pv": safe_int(no_row["show_pv"]) if no_row else 0,
                        "impression_pv": safe_int(no_row["impression_pv"]) if no_row else 0,
                        "display_failed_pv": safe_int(no_row["display_failed_pv"]) if no_row else 0,
                        "impression_show_rate": safe_float(no_row["impression_show_rate"]) if no_row else 0.0,
                        "impression_plus_failed_show_rate": safe_float(no_row["impression_plus_failed_show_rate"]) if no_row else 0.0,
                    },
                    "have_is_adx": {
                        "show_pv": safe_int(have_row["show_pv"]) if have_row else 0,
                        "impression_pv": safe_int(have_row["impression_pv"]) if have_row else 0,
                        "display_failed_pv": safe_int(have_row["display_failed_pv"]) if have_row else 0,
                        "impression_show_rate": safe_float(have_row["impression_show_rate"]) if have_row else 0.0,
                        "impression_plus_failed_show_rate": safe_float(have_row["impression_plus_failed_show_rate"]) if have_row else 0.0,
                    },
                }
            )

        network_items.sort(key=lambda item: (-safe_int(item["combined_display_failed_pv"]), item["network_name"]))
        channel_options = [item["network_name"] for item in network_items]
        default_channel = channel_options[0] if channel_options else RAW_NO_NETWORK

        channel_daily_payload: dict[str, dict[str, list[dict[str, Any]]]] = {}
        for network_name in channel_options:
            channel_daily_payload[network_name] = attach_rate_gap_fields(
                {
                    experiment_group: _sorted_points(channel_daily_map[(product, ad_format, network_name, experiment_group)])
                    for experiment_group in GROUP_META
                },
                [
                    ("impression_show_rate", "impression_show_gap_vs_a_pp"),
                    ("impression_plus_failed_show_rate", "impression_plus_failed_gap_vs_a_pp"),
                ],
            )

        error_summary_payload: dict[str, list[dict[str, Any]]] = {}
        error_daily_payload: dict[str, dict[str, dict[str, list[dict[str, Any]]]]] = {}
        error_options_by_channel: dict[str, list[str]] = {}
        default_error_by_channel: dict[str, str] = {}
        for network_name in channel_options:
            group_totals: dict[str, int] = {experiment_group: 0 for experiment_group in GROUP_META}
            combined_reason_totals: dict[str, int] = defaultdict(int)
            for (row_product, row_format, row_network, row_group, failure_reason), reason_total in error_totals.items():
                if row_product != product or row_format != ad_format or row_network != network_name:
                    continue
                group_totals[row_group] += reason_total
                combined_reason_totals[failure_reason] += reason_total

            sorted_reasons = _sort_reason_items(combined_reason_totals)
            error_options = [reason for reason, _ in sorted_reasons]
            default_reason = next((reason for reason in error_options if reason != DISPLAY_NO_ERR_MSG), None)
            error_options_by_channel[network_name] = error_options
            default_error_by_channel[network_name] = default_reason or (error_options[0] if error_options else DISPLAY_NO_ERR_MSG)

            summary_items: list[dict[str, Any]] = []
            for failure_reason, combined_total in sorted_reasons[:12]:
                no_share = (
                    error_totals[(product, ad_format, network_name, "no_is_adx", failure_reason)] / group_totals["no_is_adx"]
                    if group_totals["no_is_adx"]
                    else 0.0
                )
                have_share = (
                    error_totals[(product, ad_format, network_name, "have_is_adx", failure_reason)] / group_totals["have_is_adx"]
                    if group_totals["have_is_adx"]
                    else 0.0
                )
                summary_items.append(
                    {
                        "failure_reason": failure_reason,
                        "combined_reason_pv": combined_total,
                        "gap_vs_a_pp": calc_gap_vs_a_pp(no_share, have_share),
                        "no_is_adx": {
                            "reason_pv": error_totals[(product, ad_format, network_name, "no_is_adx", failure_reason)],
                            "share_in_network": no_share,
                        },
                        "have_is_adx": {
                            "reason_pv": error_totals[(product, ad_format, network_name, "have_is_adx", failure_reason)],
                            "share_in_network": have_share,
                        },
                    }
                )
            error_summary_payload[network_name] = summary_items

            error_daily_payload[network_name] = {}
            for failure_reason in error_options:
                all_dates = sorted(
                    {
                        point_date
                        for experiment_group in GROUP_META
                        for point_date in {
                            row_date
                            for row_product, row_format, row_network, row_group, row_reason, row_date in error_reason_daily_totals
                            if row_product == product
                            and row_format == ad_format
                            and row_network == network_name
                            and row_reason == failure_reason
                            and row_group == experiment_group
                        }
                    }
                )
                daily_points: dict[str, list[dict[str, Any]]] = {experiment_group: [] for experiment_group in GROUP_META}
                for point_date in all_dates:
                    no_reason_pv = error_reason_daily_totals[
                        (product, ad_format, network_name, "no_is_adx", failure_reason, point_date)
                    ]
                    have_reason_pv = error_reason_daily_totals[
                        (product, ad_format, network_name, "have_is_adx", failure_reason, point_date)
                    ]
                    no_share = (
                        no_reason_pv / error_daily_group_totals[(product, ad_format, network_name, "no_is_adx", point_date)]
                        if error_daily_group_totals[(product, ad_format, network_name, "no_is_adx", point_date)]
                        else 0.0
                    )
                    have_share = (
                        have_reason_pv / error_daily_group_totals[(product, ad_format, network_name, "have_is_adx", point_date)]
                        if error_daily_group_totals[(product, ad_format, network_name, "have_is_adx", point_date)]
                        else 0.0
                    )
                    gap_value = calc_gap_vs_a_pp(no_share, have_share)
                    daily_points["no_is_adx"].append(
                        {
                            "date": point_date,
                            "reason_pv": no_reason_pv,
                            "share_in_group": no_share,
                            "gap_vs_a_pp": gap_value,
                        }
                    )
                    daily_points["have_is_adx"].append(
                        {
                            "date": point_date,
                            "reason_pv": have_reason_pv,
                            "share_in_group": have_share,
                            "gap_vs_a_pp": gap_value,
                        }
                    )
                error_daily_payload[network_name][failure_reason] = {
                    experiment_group: _sorted_points(daily_points[experiment_group])
                    for experiment_group in GROUP_META
                }

        sections.append(
            {
                "key": f"{product}:{ad_format}",
                "slug": slug,
                "title": f"{product_meta['label']} {ad_meta['label']}",
                "product": product,
                "ad_format": ad_format,
                "overall": overall,
                "channel_summary": network_items[:12],
                "channel_options": channel_options,
                "default_channel": default_channel,
                "channel_daily": channel_daily_payload,
                "error_summary": error_summary_payload,
                "error_daily": error_daily_payload,
                "error_options_by_channel": error_options_by_channel,
                "default_error_by_channel": default_error_by_channel,
            }
        )

    return {
        "title": "AB 日级展示转化率与失败分布",
        "subtitle": "时间范围 2025-09-18 ~ 2026-03-28；AB 同图对比，A 组实线、B 组虚线；口径为同天 UTC-0 + 当天首次 lib_isx_group 之后的事件。",
        "start_date": start_date,
        "end_date": end_date,
        "sections": sections,
    }


def build_html(
    overall_rows: list[dict[str, Any]],
    channel_daily_rows: list[dict[str, Any]],
    error_rows: list[dict[str, Any]],
    start_date: str,
    end_date: str,
) -> str:
    """生成交互式 HTML。"""
    payload = build_payload(overall_rows, channel_daily_rows, error_rows, start_date, end_date)
    payload_json = json.dumps(payload, ensure_ascii=False)
    section_cards: list[str] = []
    for section in payload["sections"]:
        slug = section["slug"]
        product_slug = PRODUCT_META[section["product"]]["slug"]
        ad_slug = AD_FORMAT_META[section["ad_format"]]["slug"]
        section_cards.append(
            f"""
            <section class="section-card" id="{slug}-section">
              <div class="section-header">
                <div>
                  <h2>{section["title"]}</h2>
                  <p>A 组实线，B 组虚线；比例指标按蓝橙绿固定配色，错误分析改为单图 AB 对比。</p>
                </div>
              </div>
              <div class="chart-card">
                <h3>整体趋势</h3>
                <div id="{product_slug}-{ad_slug}-chart" class="chart"></div>
              </div>
              <div class="channel-panel">
                <div class="panel-header">
                  <h3>分渠道 AB 日级趋势</h3>
                  <label>渠道：
                    <select class="channel-selector" data-slug="{slug}"></select>
                  </label>
                </div>
                <div id="{slug}-channel-summary" class="summary-table"></div>
                <div class="chart-card">
                  <h4>选中渠道趋势</h4>
                  <div id="{slug}-channel-chart" class="chart"></div>
                </div>
              </div>
              <div class="error-panel">
                <div class="panel-header">
                  <h3>display_failed error 分析</h3>
                  <label>错误：
                    <select class="error-selector" data-slug="{slug}"></select>
                  </label>
                </div>
                <div class="chart-card">
                  <h4>选中渠道 error 汇总分布</h4>
                  <div id="{slug}-error-summary-chart" class="chart chart-short"></div>
                </div>
                <div class="chart-card">
                  <h4>选中 error 分天趋势</h4>
                  <div id="{slug}-error-daily-chart" class="chart"></div>
                </div>
              </div>
            </section>
            """.strip()
        )

    return f"""<!DOCTYPE html>
<html lang="zh-CN">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>{payload["title"]}</title>
  <script src="{ECHARTS_RELATIVE_PATH}"></script>
  <style>
    :root {{
      --bg: #f5f7fb;
      --card: #ffffff;
      --line: #d7dfeb;
      --text: #1f2937;
      --muted: #5b6475;
      --blue: #2f6fed;
      --orange: #ef8a17;
      --green: #1f9d55;
    }}
    * {{ box-sizing: border-box; }}
    html, body {{ width: 100%; overflow-x: hidden; }}
    body {{
      margin: 0;
      font-family: "Microsoft YaHei", "PingFang SC", sans-serif;
      background: linear-gradient(180deg, #eef3fb 0%, #f7f9fc 100%);
      color: var(--text);
    }}
    .page {{
      width: min(1500px, 100%);
      margin: 0 auto;
      padding: 24px;
    }}
    .hero, .section-card {{
      width: 100%;
      background: var(--card);
      border: 1px solid var(--line);
      border-radius: 18px;
      box-shadow: 0 10px 30px rgba(31, 41, 55, 0.06);
    }}
    .hero {{ padding: 20px 24px; }}
    .hero h1 {{ margin: 0 0 8px; font-size: 28px; }}
    .hero p {{ margin: 0; color: var(--muted); line-height: 1.7; }}
    .section-card {{ margin-top: 24px; padding: 20px; }}
    .section-header p {{ margin: 0; color: var(--muted); line-height: 1.6; }}
    .chart-card {{
      background: #fbfcfe;
      border: 1px solid var(--line);
      border-radius: 16px;
      padding: 14px;
      margin-top: 16px;
      min-width: 0;
    }}
    .chart-card:first-of-type {{ margin-top: 0; }}
    .chart-card h3, .chart-card h4, .panel-header h3, .section-header h2 {{ margin: 0 0 8px; }}
    .chart {{ width: 100%; height: 340px; }}
    .chart-short {{ height: 320px; }}
    .channel-panel, .error-panel {{
      margin-top: 18px;
      padding-top: 18px;
      border-top: 1px solid var(--line);
    }}
    .panel-header {{
      display: flex;
      justify-content: space-between;
      align-items: center;
      gap: 16px;
      flex-wrap: wrap;
    }}
    select {{
      min-width: 240px;
      max-width: 100%;
      padding: 8px 12px;
      border: 1px solid var(--line);
      border-radius: 10px;
      background: #fff;
      color: var(--text);
    }}
    .summary-table {{
      overflow-x: auto;
      margin-top: 12px;
      width: 100%;
    }}
    table {{ width: 100%; border-collapse: collapse; font-size: 13px; }}
    th, td {{
      padding: 10px 8px;
      border-bottom: 1px solid var(--line);
      text-align: right;
      white-space: nowrap;
    }}
    th:first-child, td:first-child {{ text-align: left; min-width: 170px; }}
    .pill {{
      display: inline-block;
      padding: 3px 8px;
      border-radius: 999px;
      background: #eef4ff;
      color: var(--blue);
      font-size: 12px;
      margin-right: 8px;
    }}
    @media (max-width: 760px) {{
      .page {{ padding: 16px; }}
      .chart {{ height: 300px; }}
      .chart-short {{ height: 280px; }}
    }}
  </style>
</head>
<body>
  <div class="page">
    <section class="hero">
      <h1>{payload["title"]}</h1>
      <p>{payload["subtitle"]}</p>
      <p><span class="pill">时间范围</span>{payload["start_date"]} 到 {payload["end_date"]}</p>
    </section>
    {"".join(section_cards)}
  </div>
  <script>
    const payload = {payload_json};
    const BLUE = '#2f6fed';
    const ORANGE = '#ef8a17';
    const GREEN = '#1f9d55';
    const RED = '#d93025';
    const NEUTRAL = '#4b5563';

    function formatPct(value) {{
      return (value * 100).toFixed(2) + '%';
    }}

    function formatGapPp(value) {{
      return `${{value > 0 ? '+' : ''}}${{value.toFixed(2)}}pp`;
    }}

    function formatGapHtml(value) {{
      const color = value > 0 ? GREEN : (value < 0 ? RED : NEUTRAL);
      return `<span style="color:${{color}};font-weight:600;">B-A GAP: ${{formatGapPp(value)}}</span>`;
    }}

    function ensureChart(chartId) {{
      const dom = document.getElementById(chartId);
      const existing = echarts.getInstanceByDom(dom);
      return existing || echarts.init(dom);
    }}

    function buildRateSeries(points, valueKey, gapKey) {{
      return points.map((point) => [
        point.date,
        point[valueKey],
        point.show_pv,
        point.impression_pv,
        point.display_failed_pv,
        point.max_impression_pv || 0,
        point[gapKey] || 0
      ]);
    }}

    function buildErrorSeries(points) {{
      return points.map((point) => [point.date, point.share_in_group, point.reason_pv, point.gap_vs_a_pp || 0]);
    }}

    function buildCombinedRateChart(chartId, title, groupPoints, showMaxSeries) {{
      const chart = ensureChart(chartId);
      const groups = [
        {{ key: 'no_is_adx', label: 'A', lineType: 'solid' }},
        {{ key: 'have_is_adx', label: 'B', lineType: 'dashed' }}
      ];
      const ratioDefs = [
        {{ key: 'impression_show_rate', gapKey: 'impression_show_gap_vs_a_pp', label: 'imp/show', color: ORANGE }},
        {{ key: 'impression_plus_failed_show_rate', gapKey: 'impression_plus_failed_gap_vs_a_pp', label: '(imp+failed)/show', color: BLUE }},
      ];
      if (showMaxSeries) {{
        ratioDefs.push({{ key: 'hudi_max_rate', gapKey: 'hudi_max_gap_vs_a_pp', label: 'hudi/max', color: GREEN }});
      }}
      const series = [];
      const legendData = [];
      groups.forEach((group) => {{
        const points = (groupPoints[group.key] || []);
        ratioDefs.forEach((ratio) => {{
          const seriesName = `${{ratio.label}} | ${{group.label}}`;
          legendData.push(seriesName);
          series.push({{
            name: seriesName,
            type: 'line',
            smooth: false,
            symbol: 'none',
            data: buildRateSeries(points, ratio.key, ratio.gapKey),
            itemStyle: {{ color: ratio.color }},
            lineStyle: {{ color: ratio.color, type: group.lineType, width: 2 }},
          }});
        }});
      }});
      chart.setOption({{
        animation: false,
        title: {{ text: title, left: 'center', textStyle: {{ fontSize: 14 }} }},
        tooltip: {{
          trigger: 'axis',
          formatter(params) {{
            if (!params.length) return '';
            const lines = [params[0].axisValue];
            const groupSeen = new Set();
            const metricSeen = new Set();
            params.forEach((item) => {{
              const metricLabel = item.seriesName.split(' | ')[0];
              lines.push(item.seriesName + ': ' + formatPct(item.data[1]));
              if (!metricSeen.has(metricLabel)) {{
                metricSeen.add(metricLabel);
                lines.push(`${{metricLabel}} | ${{formatGapHtml(item.data[6])}}`);
              }}
              const groupLabel = item.seriesName.endsWith('| A') ? 'A' : 'B';
              if (!groupSeen.has(groupLabel)) {{
                groupSeen.add(groupLabel);
                lines.push(`${{groupLabel}} counts: show=${{item.data[2].toLocaleString()}}, impression=${{item.data[3].toLocaleString()}}, failed=${{item.data[4].toLocaleString()}}, max=${{item.data[5].toLocaleString()}}`);
              }}
            }});
            return lines.join('<br>');
          }}
        }},
        legend: {{ top: 28, data: legendData }},
        grid: {{ left: 58, right: 26, top: 72, bottom: 62 }},
        xAxis: {{
          type: 'category',
          axisLabel: {{ rotate: 35 }},
          data: Array.from(new Set(groups.flatMap((group) => (groupPoints[group.key] || []).map((point) => point.date)))).sort()
        }},
        yAxis: {{
          type: 'value',
          min: (value) => Math.max(0, value.min - 0.01),
          max: 1,
          axisLabel: {{ formatter(value) {{ return (value * 100).toFixed(1) + '%'; }} }}
        }},
        dataZoom: [{{ type: 'inside', start: 0, end: 100 }}, {{ type: 'slider', height: 16, bottom: 12 }}],
        series: series
      }});
      return chart;
    }}

    function buildReasonSummaryChart(chartId, title, items) {{
      const chart = ensureChart(chartId);
      chart.setOption({{
        animation: false,
        title: {{ text: title, left: 'center', textStyle: {{ fontSize: 14 }} }},
        tooltip: {{
          trigger: 'axis',
          axisPointer: {{ type: 'shadow' }},
          formatter(params) {{
            if (!params.length) return '';
            const lines = [params[0].axisValue];
            params.forEach((item) => {{
              const meta = item.data.meta;
              lines.push(`${{item.seriesName}}: share=${{formatPct(item.data.value)}}, pv=${{meta.reason_pv.toLocaleString()}}`);
            }});
            lines.push(formatGapHtml(params[0].data.gap_vs_a_pp || 0));
            return lines.join('<br>');
          }}
        }},
        legend: {{ top: 24, data: ['A', 'B'] }},
        grid: {{ left: 180, right: 110, top: 64, bottom: 20 }},
        xAxis: {{
          type: 'value',
          axisLabel: {{ formatter(value) {{ return (value * 100).toFixed(1) + '%'; }} }}
        }},
        yAxis: {{
          type: 'category',
          data: items.map((item) => item.failure_reason),
          axisLabel: {{ width: 160, overflow: 'truncate' }}
        }},
        series: [
          {{
            name: 'A',
            type: 'bar',
            data: items.map((item) => ({{ value: item.no_is_adx.share_in_network, meta: item.no_is_adx, gap_vs_a_pp: item.gap_vs_a_pp }})),
            itemStyle: {{ color: BLUE }},
          }},
          {{
            name: 'B',
            type: 'bar',
            data: items.map((item) => ({{ value: item.have_is_adx.share_in_network, meta: item.have_is_adx, gap_vs_a_pp: item.gap_vs_a_pp }})),
            itemStyle: {{ color: ORANGE }},
          }}
        ]
      }});
      return chart;
    }}

    function buildErrorDailyChart(chartId, title, groupPoints) {{
      const chart = ensureChart(chartId);
      const groups = [
        {{ key: 'no_is_adx', label: 'A', lineType: 'solid', color: BLUE }},
        {{ key: 'have_is_adx', label: 'B', lineType: 'dashed', color: ORANGE }}
      ];
      const series = groups.map((group) => ({{
        name: group.label,
        type: 'line',
        smooth: false,
        symbol: 'none',
        data: buildErrorSeries(groupPoints[group.key] || []),
        itemStyle: {{ color: group.color }},
        lineStyle: {{ color: group.color, type: group.lineType, width: 2 }},
      }}));
      chart.setOption({{
        animation: false,
        title: {{ text: title, left: 'center', textStyle: {{ fontSize: 14 }} }},
        tooltip: {{
          trigger: 'axis',
          formatter(params) {{
            if (!params.length) return '';
            const lines = [params[0].axisValue];
            params.forEach((item) => {{
              lines.push(`${{item.seriesName}}: share=${{formatPct(item.data[1])}}, pv=${{item.data[2].toLocaleString()}}`);
            }});
            lines.push(formatGapHtml(params[0].data[3] || 0));
            return lines.join('<br>');
          }}
        }},
        legend: {{ top: 24, data: ['A', 'B'] }},
        grid: {{ left: 58, right: 26, top: 64, bottom: 62 }},
        xAxis: {{
          type: 'category',
          axisLabel: {{ rotate: 35 }},
          data: Array.from(new Set(groups.flatMap((group) => (groupPoints[group.key] || []).map((point) => point.date)))).sort()
        }},
        yAxis: {{
          type: 'value',
          min: 0,
          axisLabel: {{ formatter(value) {{ return (value * 100).toFixed(1) + '%'; }} }}
        }},
        dataZoom: [{{ type: 'inside', start: 0, end: 100 }}, {{ type: 'slider', height: 16, bottom: 12 }}],
        series: series
      }});
      return chart;
    }}

    function renderChannelSummary(container, rows, selectedChannel) {{
      container.innerHTML = '';
      const table = document.createElement('table');
      table.innerHTML = `
        <thead><tr><th>network_name</th><th>A failed</th><th>A imp/show</th><th>A (imp+failed)/show</th><th>B failed</th><th>B imp/show</th><th>B (imp+failed)/show</th></tr></thead>
      `;
      const body = document.createElement('tbody');
      rows.forEach((row) => {{
        const tr = document.createElement('tr');
        if (row.network_name === selectedChannel) tr.style.background = '#eef4ff';
        tr.innerHTML = `<td>${{row.network_name}}</td><td>${{row.no_is_adx.display_failed_pv.toLocaleString()}}</td><td>${{formatPct(row.no_is_adx.impression_show_rate)}}</td><td>${{formatPct(row.no_is_adx.impression_plus_failed_show_rate)}}</td><td>${{row.have_is_adx.display_failed_pv.toLocaleString()}}</td><td>${{formatPct(row.have_is_adx.impression_show_rate)}}</td><td>${{formatPct(row.have_is_adx.impression_plus_failed_show_rate)}}</td>`;
        body.appendChild(tr);
      }});
      table.appendChild(body);
      container.appendChild(table);
    }}

    function ensureOption(selectEl, values, defaultValue) {{
      selectEl.innerHTML = '';
      values.forEach((value) => {{
        const option = document.createElement('option');
        option.value = value;
        option.textContent = value;
        selectEl.appendChild(option);
      }});
      if (values.length) {{
        selectEl.value = values.includes(defaultValue) ? defaultValue : values[0];
      }}
    }}

    function renderSection(section) {{
      buildCombinedRateChart(`${{section.slug}}-chart`, '整体趋势', section.overall, true);
      const channelSelector = document.querySelector(`.channel-selector[data-slug="${{section.slug}}"]`);
      const errorSelector = document.querySelector(`.error-selector[data-slug="${{section.slug}}"]`);
      const summaryContainer = document.getElementById(`${{section.slug}}-channel-summary`);

      function renderError(channelName, failureReason) {{
        buildReasonSummaryChart(
          `${{section.slug}}-error-summary-chart`,
          `选中渠道 error 汇总 | ${{channelName}}`,
          section.error_summary[channelName] || []
        );
        buildErrorDailyChart(
          `${{section.slug}}-error-daily-chart`,
          `选中 error 分天趋势 | ${{failureReason}}`,
          (((section.error_daily[channelName] || {{}})[failureReason]) || {{ no_is_adx: [], have_is_adx: [] }})
        );
      }}

      function renderChannel(channelName) {{
        renderChannelSummary(summaryContainer, section.channel_summary, channelName);
        buildCombinedRateChart(`${{section.slug}}-channel-chart`, `选中渠道趋势 | ${{channelName}}`, (section.channel_daily[channelName] || {{}}), false);
        const errorOptions = section.error_options_by_channel[channelName] || [];
        ensureOption(errorSelector, errorOptions, section.default_error_by_channel[channelName]);
        renderError(channelName, errorSelector.value);
      }}

      ensureOption(channelSelector, section.channel_options, section.default_channel);
      channelSelector.addEventListener('change', () => renderChannel(channelSelector.value));
      errorSelector.addEventListener('change', () => renderError(channelSelector.value, errorSelector.value));
      renderChannel(channelSelector.value);
    }}

    payload.sections.forEach(renderSection);
    window.addEventListener('resize', () => {{
      document.querySelectorAll('.chart').forEach((node) => {{
        const instance = echarts.getInstanceByDom(node);
        if (instance) instance.resize();
      }});
    }});
  </script>
</body>
</html>
"""


def main() -> None:
    """执行 SQL 并生成 CSV / JSON / HTML。"""
    client = bigquery.Client(project=PROJECT_ID)
    overall_sql = build_overall_daily_sql(START_DATE, END_DATE)
    channel_sql = build_channel_daily_sql(START_DATE, END_DATE)
    error_sql = build_error_daily_sql(START_DATE, END_DATE)

    overall_fields, overall_rows = run_query(client, overall_sql)
    channel_daily_fields, channel_daily_rows = run_query(client, channel_sql)
    error_fields, error_rows = run_query(client, error_sql)
    channel_summary_rows = build_channel_summary_rows(channel_daily_rows)
    channel_summary_fields = [
        "product",
        "ad_format",
        "experiment_group",
        "network_name",
        "show_pv",
        "impression_pv",
        "display_failed_pv",
        "impression_show_rate",
        "impression_plus_failed_show_rate",
    ]

    write_csv(OVERALL_CSV, overall_fields, overall_rows)
    write_csv(CHANNEL_DAILY_CSV, channel_daily_fields, channel_daily_rows)
    write_csv(ERROR_DAILY_CSV, error_fields, error_rows)
    write_csv(CHANNEL_SUMMARY_CSV, channel_summary_fields, channel_summary_rows)

    payload = build_payload(overall_rows, channel_daily_rows, error_rows, START_DATE, END_DATE)
    PAYLOAD_JSON.parent.mkdir(parents=True, exist_ok=True)
    PAYLOAD_JSON.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    HTML_PATH.write_text(
        build_html(
            overall_rows=overall_rows,
            channel_daily_rows=channel_daily_rows,
            error_rows=error_rows,
            start_date=START_DATE,
            end_date=END_DATE,
        ),
        encoding="utf-8",
    )
    print(
        json.dumps(
            {
                "overall_rows": len(overall_rows),
                "channel_daily_rows": len(channel_daily_rows),
                "error_rows": len(error_rows),
            },
            ensure_ascii=False,
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
