"""模块功能：执行请求级网络状态分布 SQL，并在终端输出 4 张表。"""

from __future__ import annotations

from collections import defaultdict
from pathlib import Path
from typing import Any

from google.cloud import bigquery


PROJECT_DIR = Path(__file__).resolve().parents[1]
SQL_DIR = PROJECT_DIR / "sql"
PROJECT_ID = "commercial-adx"
SQL_PATH = SQL_DIR / "latency_request_network_distribution.sql"
PRODUCT_ORDER = ["ios_screw_puzzle", "screw_puzzle"]
EVENT_TYPE_ORDER = ["latency", "request"]
AD_FORMAT_ORDER = ["banner", "interstitial", "rewarded"]
NETWORK_STATUS_ORDER = ["online", "offline", "unknown"]


def run_query(client: bigquery.Client, sql_text: str) -> list[dict[str, Any]]:
    """执行查询并返回字典列表，便于后续按产品与事件类型透视。"""
    rows_iter = client.query(sql_text).result()
    return [dict(row.items()) for row in rows_iter]


def pivot_distribution_rows(rows: list[dict[str, Any]]) -> dict[tuple[str, str], list[dict[str, float | str]]]:
    """把明细结果透视成 4 张表所需的结构。"""
    table_map: dict[tuple[str, str], dict[str, dict[str, float | str]]] = defaultdict(dict)
    for row in rows:
        table_key = (str(row["product"]), str(row["event_type"]))
        ad_format = str(row["ad_format"])
        row_bucket = table_map[table_key].setdefault(
            ad_format,
            {"ad_format": ad_format, "online": 0.0, "offline": 0.0, "unknown": 0.0},
        )
        row_bucket[str(row["network_status_group"])] = float(row["request_ratio"] or 0.0)

    ordered_tables: dict[tuple[str, str], list[dict[str, float | str]]] = {}
    for product in PRODUCT_ORDER:
        for event_type in EVENT_TYPE_ORDER:
            table_key = (product, event_type)
            row_map = table_map.get(table_key, {})
            ordered_tables[table_key] = [
                row_map.get(
                    ad_format,
                    {"ad_format": ad_format, "online": 0.0, "offline": 0.0, "unknown": 0.0},
                )
                for ad_format in AD_FORMAT_ORDER
            ]
    return ordered_tables


def fmt_ratio(value: float) -> str:
    """把比例格式化成百分比字符串。"""
    return f"{value:.2%}"


def format_plain_table(headers: list[str], rows: list[list[str]]) -> str:
    """把二维数据格式化为纯文本表，便于直接在终端和对话框查看。"""
    widths = [len(header) for header in headers]
    for row in rows:
        for idx, cell in enumerate(row):
            widths[idx] = max(widths[idx], len(cell))

    def render_line(parts: list[str]) -> str:
        return " | ".join(part.ljust(widths[idx]) for idx, part in enumerate(parts))

    split_line = "-+-".join("-" * width for width in widths)
    rendered = [render_line(headers), split_line]
    rendered.extend(render_line(row) for row in rows)
    return "\n".join(rendered)


def format_distribution_tables(tables: dict[tuple[str, str], list[dict[str, float | str]]]) -> str:
    """把 4 张透视表拼接成最终终端输出。"""
    rendered_tables: list[str] = []
    for product in PRODUCT_ORDER:
        for event_type in EVENT_TYPE_ORDER:
            table_key = (product, event_type)
            rows = tables.get(table_key, [])
            table_text = format_plain_table(
                ["ad_format", *NETWORK_STATUS_ORDER],
                [
                    [
                        str(row["ad_format"]),
                        fmt_ratio(float(row["online"])),
                        fmt_ratio(float(row["offline"])),
                        fmt_ratio(float(row["unknown"])),
                    ]
                    for row in rows
                ],
            )
            rendered_tables.append(f"{product} - {event_type}\n{table_text}")
    return "\n\n".join(rendered_tables)


def main() -> None:
    """执行 SQL 并直接打印 4 张网络状态分布表。"""
    client = bigquery.Client(project=PROJECT_ID)
    rows = run_query(client, SQL_PATH.read_text(encoding="utf-8"))
    tables = pivot_distribution_rows(rows)
    print(format_distribution_tables(tables))


if __name__ == "__main__":
    main()
