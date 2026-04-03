"""执行成功 request 分层分析 SQL，并在本地生成独立 HTML。"""

from __future__ import annotations

import csv
import sys
from pathlib import Path

try:
    from google.cloud import bigquery
except ImportError:  # pragma: no cover - 仅在本地未安装依赖时触发
    bigquery = None

PROJECT_DIR = Path(__file__).resolve().parents[1]
SQL_DIR = PROJECT_DIR / "sql"
OUTPUT_DIR = PROJECT_DIR / "outputs"
PROJECT_ID = "commercial-adx"
SQL_TO_CSV = {
    "success_request_scope_summary.sql": "success_request_scope_summary.csv",
    "success_request_cnt_distribution.sql": "success_request_cnt_distribution.csv",
    "success_request_channel_distribution.sql": "success_request_channel_distribution.csv",
    "success_request_rank_distribution.sql": "success_request_rank_distribution.csv",
    "success_request_ecpm_distribution.sql": "success_request_ecpm_distribution.csv",
}


def run_query_to_csv(client: "bigquery.Client", sql_path: Path, csv_path: Path) -> None:
    """流式执行查询并写出 CSV，避免一次性加载大结果。"""
    print(f"start: {sql_path.name}", flush=True)
    query_job = client.query(sql_path.read_text(encoding="utf-8"))
    rows_iter = query_job.result(page_size=50000)
    field_names = [field.name for field in rows_iter.schema]
    csv_path.parent.mkdir(parents=True, exist_ok=True)
    with csv_path.open("w", encoding="utf-8-sig", newline="") as file_obj:
        writer = csv.DictWriter(file_obj, fieldnames=field_names)
        writer.writeheader()
        for row in rows_iter:
            writer.writerow({field: row.get(field) for field in field_names})
    print(f"wrote: {csv_path}", flush=True)


def main() -> None:
    """总入口：支持只跑指定 SQL；全部跑完后自动生成 HTML。"""
    if bigquery is None:
        raise ImportError("未安装 google-cloud-bigquery，无法执行 SQL。")

    client = bigquery.Client(project=PROJECT_ID)
    selected = sys.argv[1:] or list(SQL_TO_CSV.keys())
    for sql_name in selected:
        if sql_name not in SQL_TO_CSV:
            raise ValueError(f"未配置的 SQL：{sql_name}")
        run_query_to_csv(client, SQL_DIR / sql_name, OUTPUT_DIR / SQL_TO_CSV[sql_name])

    if len(selected) == len(SQL_TO_CSV):
        from build_success_request_dashboard import main as build_dashboard_main

        build_dashboard_main()


if __name__ == "__main__":
    main()
