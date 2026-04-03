"""模块功能：输出 reach_rate 对比结果，并下钻 B 组 gap 最大用户的时间线。"""

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
GROUP_SQL = SQL_DIR / "reach_rate_ab_group_compare.sql"
USER_SQL = SQL_DIR / "reach_rate_b_user_gap_rank.sql"
TIMELINE_SQL = SQL_DIR / "reach_rate_b_user_timeline.sql"
GROUP_CSV = OUTPUT_DIR / "reach_rate_ab_group_compare.csv"
USER_CSV = OUTPUT_DIR / "reach_rate_b_user_gap_top.csv"
TIMELINE_CSV = OUTPUT_DIR / "reach_rate_b_user_timeline.csv"
SUMMARY_JSON = OUTPUT_DIR / "reach_rate_analysis_summary.json"


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


def main() -> None:
    """输出分组对比、B 组 gap 最大用户，以及该用户的时间线明细。"""
    client = bigquery.Client(project=PROJECT_ID)

    group_fields, group_rows = run_query(client, GROUP_SQL.read_text(encoding="utf-8"))
    write_csv(GROUP_CSV, group_fields, group_rows)

    user_fields, user_rows = run_query(client, USER_SQL.read_text(encoding="utf-8"))
    write_csv(USER_CSV, user_fields, user_rows)
    if not user_rows:
        raise RuntimeError("未找到可用于下钻的 B 组用户")

    target_user = str(user_rows[0]["user_pseudo_id"])
    timeline_sql = TIMELINE_SQL.read_text(encoding="utf-8").format(target_user=target_user)
    timeline_fields, timeline_rows = run_query(client, timeline_sql)
    write_csv(TIMELINE_CSV, timeline_fields, timeline_rows)

    summary = {
        "target_user": target_user,
        "group_rows": len(group_rows),
        "timeline_rows": len(timeline_rows),
        "top_user": user_rows[0],
    }
    SUMMARY_JSON.parent.mkdir(parents=True, exist_ok=True)
    SUMMARY_JSON.write_text(json.dumps(summary, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    print(json.dumps(summary, ensure_ascii=False, indent=2, default=str))


if __name__ == "__main__":
    main()
