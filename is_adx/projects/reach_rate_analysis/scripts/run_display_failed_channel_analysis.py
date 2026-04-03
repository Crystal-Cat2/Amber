"""模块功能：输出 trigger/show/impression/display_failed 的分渠道漏斗，以及 display_failed 的失败原因分布。"""

from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Any

from google.cloud import bigquery

PROJECT_DIR = Path(__file__).resolve().parents[1]
SQL_DIR = PROJECT_DIR / "sql"
OUTPUT_DIR = PROJECT_DIR / "outputs"
PROJECT_ID = "commercial-adx"
EVENT_SQL = SQL_DIR / "display_failed_channel_event_summary.sql"
REASON_SQL = SQL_DIR / "display_failed_channel_reason_distribution.sql"
EVENT_CSV = OUTPUT_DIR / "display_failed_channel_event_summary.csv"
REASON_CSV = OUTPUT_DIR / "display_failed_channel_reason_distribution.csv"
SUMMARY_JSON = OUTPUT_DIR / "display_failed_channel_analysis_summary.json"
NOTES_MD = OUTPUT_DIR / "display_failed_channel_analysis_notes.md"


def run_query(client: bigquery.Client, sql_text: str) -> tuple[list[str], list[dict[str, Any]]]:
    """执行 BigQuery 查询并返回字段名与结果列表。"""
    rows_iter = client.query(sql_text).result()
    field_names = [field.name for field in rows_iter.schema]
    rows = [dict(row.items()) for row in rows_iter]
    return field_names, rows


def write_csv(path: Path, field_names: list[str], rows: list[dict[str, Any]]) -> None:
    """把查询结果写入 UTF-8-SIG CSV，便于 Excel 直接打开复核。"""
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8-sig", newline="") as file_obj:
        writer = csv.DictWriter(file_obj, fieldnames=field_names)
        writer.writeheader()
        for row in rows:
            writer.writerow({key: row.get(key) for key in field_names})


def to_int(value: Any) -> int:
    """把 BigQuery 返回值安全转为整数。"""
    if value in (None, ""):
        return 0
    return int(value)


def to_float(value: Any) -> float:
    """把 BigQuery 返回值安全转为浮点数。"""
    if value in (None, ""):
        return 0.0
    return float(value)


def format_pct(value: Any) -> str:
    """把比例格式化为百分比文案。"""
    return f"{to_float(value) * 100:.2f}%"


def build_markdown_summary(
    event_rows: list[dict[str, Any]],
    reason_rows: list[dict[str, Any]],
) -> str:
    """基于结果表生成中文结论说明。"""
    lines = [
        "# display_failed 渠道分析说明",
        "",
        "- 时间范围：event_date 2025-09-18 到 2026-01-03",
        "- 产品：screw_puzzle、ios_screw_puzzle",
        "- 广告格式：interstitial、rewarded",
        "- AB 分组：基于 lib_isx_group 的 user_id + experiment_group 全周期 min/max 窗口",
        "- 表 1：同一渠道行同时输出 trigger / show / impression / display_failed 的 pv，以及 show_rate / impression_rate",
        "- 表 2：display_failed 的分渠道失败原因分布",
        "",
    ]

    combo_keys = sorted(
        {
            (str(row["product"]), str(row["ad_format"]), str(row["experiment_group"]))
            for row in event_rows
        }
    )

    for product, ad_format, experiment_group in combo_keys:
        lines.append(f"## {product} | {ad_format} | {experiment_group} 组")
        combo_rows = [
            row for row in event_rows
            if str(row["product"]) == product
            and str(row["ad_format"]) == ad_format
            and str(row["experiment_group"]) == experiment_group
        ]
        if not combo_rows:
            lines.append("- 无数据")
            lines.append("")
            continue

        top_rows = sorted(
            combo_rows,
            key=lambda row: (
                -to_int(row.get("display_failed_pv")),
                -to_int(row.get("trigger_pv")),
                -to_int(row.get("show_pv")),
                -to_int(row.get("impression_pv")),
                str(row.get("network_name", "")),
            ),
        )[:5]

        lines.append("| network_name | trigger_pv | show_pv | show_rate | impression_pv | impression_rate | display_failed_pv | display_failed_share |")
        lines.append("| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |")
        for row in top_rows:
            lines.append(
                "| {network} | {trigger_pv} | {show_pv} | {show_rate} | {imp_pv} | {imp_rate} | {failed_pv} | {failed_share} |".format(
                    network=row["network_name"],
                    trigger_pv=to_int(row.get("trigger_pv")),
                    show_pv=to_int(row["show_pv"]),
                    show_rate=format_pct(row.get("show_rate")),
                    imp_pv=to_int(row["impression_pv"]),
                    imp_rate=format_pct(row.get("impression_rate")),
                    failed_pv=to_int(row["display_failed_pv"]),
                    failed_share=format_pct(row["display_failed_share"]),
                )
            )

        top_failed_row = next((row for row in top_rows if to_int(row["display_failed_pv"]) > 0), None)
        if top_failed_row is None:
            lines.append("")
            lines.append("- 当前组合没有 display_failed 数据。")
            lines.append("")
            continue

        top_network = str(top_failed_row["network_name"])
        matched_reasons = [
            row for row in reason_rows
            if str(row["product"]) == product
            and str(row["ad_format"]) == ad_format
            and str(row["experiment_group"]) == experiment_group
            and str(row["network_name"]) == top_network
        ]
        matched_reasons = sorted(
            matched_reasons,
            key=lambda row: (-to_int(row.get("reason_pv")), str(row.get("failure_reason", ""))),
        )[:3]

        lines.append("")
        lines.append(
            "- 头部失败渠道：`{network}`，trigger pv `{trigger_pv}`，show pv `{show_pv}`，show_rate `{show_rate}`，impression pv `{imp_pv}`，impression_rate `{imp_rate}`，display_failed pv `{failed_pv}`。".format(
                network=top_network,
                trigger_pv=to_int(top_failed_row.get("trigger_pv")),
                show_pv=to_int(top_failed_row["show_pv"]),
                show_rate=format_pct(top_failed_row.get("show_rate")),
                imp_pv=to_int(top_failed_row["impression_pv"]),
                imp_rate=format_pct(top_failed_row.get("impression_rate")),
                failed_pv=to_int(top_failed_row["display_failed_pv"]),
            )
        )
        if matched_reasons:
            lines.append("- 该渠道头部失败原因：")
            for reason_row in matched_reasons:
                lines.append(
                    "  - `{reason}`: {pv} ({share})".format(
                        reason=reason_row["failure_reason"],
                        pv=to_int(reason_row["reason_pv"]),
                        share=format_pct(reason_row["reason_share_in_network"]),
                    )
                )
        else:
            lines.append("- 该渠道未命中可用的失败原因明细。")
        lines.append("")

    return "\n".join(lines).rstrip() + "\n"


def build_summary_payload(
    event_rows: list[dict[str, Any]],
    reason_rows: list[dict[str, Any]],
) -> dict[str, Any]:
    """输出一份简短的执行摘要，便于快速确认产物。"""
    return {
        "event_rows": len(event_rows),
        "reason_rows": len(reason_rows),
        "event_preview": event_rows[:10],
        "reason_preview": reason_rows[:10],
    }


def main() -> None:
    """执行两份 SQL 并输出 CSV、JSON 与中文说明。"""
    client = bigquery.Client(project=PROJECT_ID)

    event_fields, event_rows = run_query(client, EVENT_SQL.read_text(encoding="utf-8"))
    reason_fields, reason_rows = run_query(client, REASON_SQL.read_text(encoding="utf-8"))

    write_csv(EVENT_CSV, event_fields, event_rows)
    write_csv(REASON_CSV, reason_fields, reason_rows)

    summary = build_summary_payload(event_rows, reason_rows)
    SUMMARY_JSON.parent.mkdir(parents=True, exist_ok=True)
    SUMMARY_JSON.write_text(json.dumps(summary, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    NOTES_MD.write_text(build_markdown_summary(event_rows, reason_rows), encoding="utf-8")
    print(json.dumps(summary, ensure_ascii=False, indent=2, default=str))


if __name__ == "__main__":
    main()
