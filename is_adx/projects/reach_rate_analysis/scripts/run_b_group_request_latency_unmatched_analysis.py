"""模块功能：统计 B 组 request 未匹配 latency 的概览与 err_msg 分布。"""

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
SUMMARY_SQL = SQL_DIR / "b_group_request_latency_unmatched_summary.sql"
ERRMSG_SQL = SQL_DIR / "b_group_request_latency_unmatched_errmsg_distribution.sql"
SUMMARY_CSV = OUTPUT_DIR / "b_group_request_latency_unmatched_summary.csv"
ERRMSG_CSV = OUTPUT_DIR / "b_group_request_latency_unmatched_errmsg_distribution.csv"
SUMMARY_JSON = OUTPUT_DIR / "b_group_request_latency_unmatched_summary.json"


def run_query(client: bigquery.Client, sql_text: str) -> tuple[list[str], list[dict[str, Any]]]:
    """执行 BigQuery 查询并返回字段名与结果列表。"""
    rows_iter = client.query(sql_text).result()
    field_names = [field.name for field in rows_iter.schema]
    rows = [dict(row.items()) for row in rows_iter]
    return field_names, rows


def write_csv(path: Path, field_names: list[str], rows: list[dict[str, Any]]) -> None:
    """把查询结果写入 UTF-8-SIG CSV，便于 Excel 直接打开复核。"""
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open('w', encoding='utf-8-sig', newline='') as file_obj:
        writer = csv.DictWriter(file_obj, fieldnames=field_names)
        writer.writeheader()
        for row in rows:
            writer.writerow({key: row.get(key) for key in field_names})


def main() -> None:
    """输出未匹配 request 的汇总统计与 adslog_error 文案分布。"""
    client = bigquery.Client(project=PROJECT_ID)

    summary_fields, summary_rows = run_query(client, SUMMARY_SQL.read_text(encoding='utf-8'))
    errmsg_fields, errmsg_rows = run_query(client, ERRMSG_SQL.read_text(encoding='utf-8'))

    write_csv(SUMMARY_CSV, summary_fields, summary_rows)
    write_csv(ERRMSG_CSV, errmsg_fields, errmsg_rows)

    overview = {
        'summary_rows': len(summary_rows),
        'errmsg_rows': len(errmsg_rows),
        'summary_preview': summary_rows[:10],
        'errmsg_preview': errmsg_rows[:10],
    }
    SUMMARY_JSON.parent.mkdir(parents=True, exist_ok=True)
    SUMMARY_JSON.write_text(json.dumps(overview, ensure_ascii=False, indent=2, default=str), encoding='utf-8')
    print(json.dumps(overview, ensure_ascii=False, indent=2, default=str))


if __name__ == '__main__':
    main()
