"""模块功能：执行 AB 请求结构 SQL 并流式写出本地 CSV。"""

from __future__ import annotations

import csv
import sys
from pathlib import Path

from google.cloud import bigquery

PROJECT_DIR = Path(__file__).resolve().parents[1]
SQL_DIR = PROJECT_DIR / "sql"
OUTPUT_DIR = PROJECT_DIR / "outputs"
PROJECT_ID = "commercial-adx"
SQL_TO_CSV = {
    "metric4_bidding_loaded_one_other_status_table.sql": "metric4_bidding_loaded_one_other_status_table.csv",
    "metric5_type_network_status_total_share.sql": "metric5_type_network_status_total_share.csv",
    "success_network_by_network_cnt.sql": "success_network_by_network_cnt.csv",
    "success_placement_by_placement_cnt.sql": "success_placement_by_placement_cnt.csv",
    "metric1_total_network_cnt_distribution.sql": "metric1_request_network_cnt.csv",
    "metric1_total_placement_cnt_distribution.sql": "metric1_request_placement_cnt.csv",
    "metric1_success_rank_distribution.sql": "metric1_success_rank_distribution.csv",
    "metric1_total_network_cnt_distribution_country.sql": "metric1_request_network_cnt_country.csv",
    "metric1_total_placement_cnt_distribution_country.sql": "metric1_request_placement_cnt_country.csv",
    "metric1_success_rank_distribution_country.sql": "metric1_success_rank_distribution_country.csv",
    "metric1_total_network_cnt_distribution_unit.sql": "metric1_request_network_cnt_unit.csv",
    "metric1_total_placement_cnt_distribution_unit.sql": "metric1_request_placement_cnt_unit.csv",
    "metric1_success_rank_distribution_unit.sql": "metric1_success_rank_distribution_unit.csv",
    "metric2_type_mix_distribution.sql": "metric2_network_type_status_cnt.csv",
    "metric2_type_placement_mix_distribution.sql": "metric2_type_placement_status_cnt.csv",
    "metric2_type_mix_distribution_country.sql": "metric2_network_type_status_cnt_country.csv",
    "metric2_type_placement_mix_distribution_country.sql": "metric2_type_placement_status_cnt_country.csv",
    "metric2_type_mix_distribution_unit.sql": "metric2_network_type_status_cnt_unit.csv",
    "metric2_type_placement_mix_distribution_unit.sql": "metric2_type_placement_status_cnt_unit.csv",
    "metric3_type_status_distribution.sql": "metric3_network_distribution.csv",
    "metric3_type_placement_status_distribution.sql": "metric3_placement_distribution.csv",
    "metric3_type_status_distribution_country.sql": "metric3_network_distribution_country.csv",
    "metric3_type_placement_status_distribution_country.sql": "metric3_placement_distribution_country.csv",
    "metric3_type_status_distribution_unit.sql": "metric3_network_distribution_unit.csv",
    "metric3_type_placement_status_distribution_unit.sql": "metric3_placement_distribution_unit.csv",
    "metric4_cnt_level_network_distribution.sql": "metric4_cnt_level_network_distribution.csv",
    "metric4_cnt_level_placement_distribution.sql": "metric4_cnt_level_placement_distribution.csv",
    "metric4_cnt_level_network_distribution_country.sql": "metric4_cnt_level_network_distribution_country.csv",
    "metric4_cnt_level_placement_distribution_country.sql": "metric4_cnt_level_placement_distribution_country.csv",
    "metric4_cnt_level_network_distribution_unit.sql": "metric4_cnt_level_network_distribution_unit.csv",
    "metric4_cnt_level_placement_distribution_unit.sql": "metric4_cnt_level_placement_distribution_unit.csv",
    "metric1_bucket_share.sql": "metric1_bucket_share.csv",
    "metric2_type_coverage.sql": "metric2_type_coverage.csv",
    "metric3_status_coverage.sql": "metric3_status_coverage.csv",
    "metric4_type_status_coverage.sql": "metric4_type_status_coverage.csv",
    "null_bidding_request_pv_by_unit.sql": "null_bidding_request_pv_by_unit.csv",
    "real_status_bidding_request_pv_by_unit.sql": "real_status_bidding_request_pv_by_unit.csv",
    "bidding_network_status_share_by_unit.sql": "bidding_network_status_share_by_unit.csv",
    "winning_type_network_status_hit_rate_by_unit.sql": "winning_type_network_status_hit_rate_by_unit.csv",
    "adslog_filled_duration_distribution_by_unit.sql": "adslog_filled_duration_distribution_by_unit.csv",
    "isadx_latency_distribution_by_unit.sql": "isadx_latency_distribution_by_unit.csv",
}


def run_query_to_csv(client: bigquery.Client, sql_path: Path, csv_path: Path) -> None:
    """流式执行查询并写 CSV，避免把大结果一次性加载进内存。"""
    print(f"start: {sql_path.name}", flush=True)
    rows_iter = client.query(sql_path.read_text(encoding="utf-8")).result(page_size=50000)
    field_names = [field.name for field in rows_iter.schema]
    csv_path.parent.mkdir(parents=True, exist_ok=True)
    with csv_path.open("w", encoding="utf-8", newline="") as file_obj:
        writer = csv.DictWriter(file_obj, fieldnames=field_names)
        writer.writeheader()
        for row in rows_iter:
            writer.writerow({field: row.get(field) for field in field_names})
    print(f"wrote: {csv_path}", flush=True)


def main() -> None:
    """总入口：支持传入指定 SQL 文件名，只重跑目标结果。"""
    client = bigquery.Client(project=PROJECT_ID)
    selected = sys.argv[1:] or list(SQL_TO_CSV.keys())
    for sql_name in selected:
        if sql_name not in SQL_TO_CSV:
            raise ValueError(f"未配置的 SQL：{sql_name}")
        run_query_to_csv(client, SQL_DIR / sql_name, OUTPUT_DIR / SQL_TO_CSV[sql_name])


if __name__ == "__main__":
    main()
