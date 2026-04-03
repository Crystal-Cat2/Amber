"""模块功能：本地执行 loaded latency 与 adslog_filled 匹配校验，并输出摘要文件。"""

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
MATCH_THRESHOLD = 0.99
VALIDATION_SQL = SQL_DIR / "loaded_latency_filled_match_validation.sql"
AUDIT_SQL = SQL_DIR / "loaded_latency_filled_duplicate_audit.sql"
VALIDATION_CSV = OUTPUT_DIR / "loaded_latency_filled_match_validation.csv"
AUDIT_CSV = OUTPUT_DIR / "loaded_latency_filled_duplicate_audit.csv"
SUMMARY_JSON = OUTPUT_DIR / "loaded_latency_filled_match_validation_summary.json"


def run_query(client: bigquery.Client, sql_text: str) -> tuple[list[str], list[dict[str, Any]]]:
    """执行 BigQuery 查询并返回字段名与结果列表。"""
    rows_iter = client.query(sql_text).result()
    field_names = [field.name for field in rows_iter.schema]
    rows = [dict(row.items()) for row in rows_iter]
    return field_names, rows


def write_csv(path: Path, field_names: list[str], rows: list[dict[str, Any]]) -> None:
    """把查询结果写入 UTF-8-SIG CSV，便于 Excel 复核。"""
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8-sig", newline="") as file_obj:
        writer = csv.DictWriter(file_obj, fieldnames=field_names)
        writer.writeheader()
        for row in rows:
            writer.writerow({key: row.get(key) for key in field_names})


def build_summary(
    rows: list[dict[str, Any]],
    audit_rows: list[dict[str, Any]],
    threshold: float = MATCH_THRESHOLD,
) -> dict[str, Any]:
    """组装供终端和 JSON 复用的摘要结构。"""
    below_expectation_rows = [
        row
        for row in rows
        if row.get("matched_ratio") is not None and float(row["matched_ratio"]) < threshold
    ]
    return {
        "threshold": threshold,
        "row_count": len(rows),
        "below_expectation": bool(below_expectation_rows),
        "below_expectation_rows": below_expectation_rows,
        "rows_preview": rows[:10],
        "audit": audit_rows[0] if audit_rows else {},
    }


def write_summary_json(path: Path, summary: dict[str, Any]) -> None:
    """落盘 UTF-8 JSON 摘要。"""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(summary, ensure_ascii=False, indent=2, default=str),
        encoding="utf-8",
    )


def format_summary_lines(rows: list[dict[str, Any]], threshold: float = MATCH_THRESHOLD) -> str:
    """把关键匹配率格式化成终端可读摘要。"""
    rendered_lines = [f"match_threshold={threshold:.2%}"]
    for row in rows:
        rendered_lines.append(
            (
                f"{row.get('product')} | {row.get('ad_format')} | "
                f"loaded_request_cnt={row.get('loaded_request_cnt')} | "
                f"matched_filled_request_cnt={row.get('matched_filled_request_cnt')} | "
                f"matched_ratio={float(row.get('matched_ratio') or 0):.2%}"
            )
        )
    return "\n".join(rendered_lines)


def main() -> None:
    """执行校验 SQL，写出明细与摘要。"""
    client = bigquery.Client(project=PROJECT_ID)

    validation_fields, validation_rows = run_query(client, VALIDATION_SQL.read_text(encoding="utf-8"))
    audit_fields, audit_rows = run_query(client, AUDIT_SQL.read_text(encoding="utf-8"))

    write_csv(VALIDATION_CSV, validation_fields, validation_rows)
    write_csv(AUDIT_CSV, audit_fields, audit_rows)

    summary = build_summary(validation_rows, audit_rows)
    write_summary_json(SUMMARY_JSON, summary)

    print(format_summary_lines(validation_rows))
    print(json.dumps(summary, ensure_ascii=False, indent=2, default=str))


if __name__ == "__main__":
    main()
