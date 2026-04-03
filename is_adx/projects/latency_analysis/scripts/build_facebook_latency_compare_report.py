"""模块功能：执行 Facebook latency 对比，输出独立 Markdown 报告。"""

from __future__ import annotations

import csv
from collections import defaultdict
from pathlib import Path
from typing import Any

try:
    from google.cloud import bigquery
except ImportError:  # pragma: no cover - 仅在未安装 bigquery 时保护导入
    bigquery = None


PROJECT_DIR = Path(__file__).resolve().parents[1]
SQL_DIR = PROJECT_DIR / "sql"
DOC_DIR = PROJECT_DIR / "docs"
OUTPUT_DIR = PROJECT_DIR / "outputs"
PROJECT_ID = "commercial-adx"
SQL_PATH = SQL_DIR / "facebook_latency_compare.sql"
REPORT_MD_PATH = DOC_DIR / "facebook_latency_compare_report.md"
COMPARE_CSV_PATH = OUTPUT_DIR / "facebook_latency_compare.csv"
FACEBOOK_BACKEND_CSV_PATH = OUTPUT_DIR / "facebook.csv"

SECTION_MARKERS = {
    "facebook_backend_compare": "-- section: facebook_backend_compare",
}

PRODUCT_ORDER = ["ios_screw_puzzle", "screw_puzzle"]
AD_FORMAT_ORDER = ["banner", "interstitial", "rewarded"]
PLATFORM_TO_PRODUCT = {
    "android": "screw_puzzle",
    "ios": "ios_screw_puzzle",
}
DISPLAY_FORMAT_TO_AD_FORMAT = {
    "banner": "banner",
    "interstitial": "interstitial",
    "rewarded_video": "rewarded",
}
DATE_START = "2026-01-05"
DATE_END = "2026-01-12"


def parse_sql_sections(sql_text: str) -> dict[str, str]:
    """按 section 标记切分 SQL，便于在报告中追加附录。"""
    sections: dict[str, str] = {}
    marker_items = list(SECTION_MARKERS.items())
    for idx, (name, marker) in enumerate(marker_items):
        start = sql_text.find(marker)
        if start < 0:
            raise ValueError(f"未找到 SQL 段落标记: {marker}")
        content_start = start + len(marker)
        if idx + 1 < len(marker_items):
            next_marker = marker_items[idx + 1][1]
            end = sql_text.find(next_marker, content_start)
            if end < 0:
                raise ValueError(f"未找到后续 SQL 段落标记: {next_marker}")
        else:
            end = len(sql_text)
        sections[name] = sql_text[content_start:end].strip().rstrip(";")
    return sections


def run_query(client: Any, sql_text: str) -> tuple[list[str], list[dict[str, Any]]]:
    """执行 BigQuery 查询并返回字段名与结果列表。"""
    rows_iter = client.query(sql_text).result()
    field_names = [field.name for field in rows_iter.schema]
    rows = [dict(row.items()) for row in rows_iter]
    return field_names, rows


def write_csv(path: Path, field_names: list[str], rows: list[dict[str, Any]]) -> None:
    """把查询结果写入 CSV，便于后续人工复核。"""
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as file_obj:
        writer = csv.DictWriter(file_obj, fieldnames=field_names)
        writer.writeheader()
        for row in rows:
            writer.writerow({key: row.get(key) for key in field_names})


def clean_csv_value(value: str | None) -> str:
    """清洗 Excel 导出的包裹格式，避免映射与数值转换出错。"""
    text = (value or "").strip()
    if text.startswith('="') and text.endswith('"'):
        return text[2:-1]
    return text


def parse_int_value(value: str | None) -> int:
    """把后台 CSV 的数值列安全转成整数，异常值按 0 处理。"""
    text = clean_csv_value(value).replace(",", "")
    if not text or text.lower() == "not available":
        return 0
    return int(float(text))


def fmt_num(value: Any) -> str:
    """把数值格式化成带千分位字符串。"""
    if value is None:
        return "N/A"
    return f"{int(value):,}"


def fmt_ratio(value: Any) -> str:
    """把比例格式化成百分比字符串。"""
    if value is None:
        return "N/A"
    return f"{float(value):.2%}"


def sort_product_format_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """按产品与广告格式固定顺序排序，保证 Markdown 可读性稳定。"""
    product_rank = {name: idx for idx, name in enumerate(PRODUCT_ORDER)}
    ad_format_rank = {name: idx for idx, name in enumerate(AD_FORMAT_ORDER)}
    return sorted(
        rows,
        key=lambda row: (
            product_rank.get(str(row.get("product")), 99),
            ad_format_rank.get(str(row.get("ad_format")), 99),
        ),
    )


def load_facebook_backend_rows(path: Path = FACEBOOK_BACKEND_CSV_PATH) -> list[dict[str, Any]]:
    """读取 Facebook 后台文件，并按产品与广告格式聚合 Bidding requests / Requests。"""
    rows_by_key: dict[tuple[str, str], dict[str, int]] = defaultdict(lambda: {"bidding_requests": 0, "requests": 0})
    with path.open("r", encoding="utf-8-sig", newline="") as file_obj:
        reader = csv.DictReader(file_obj)
        for raw in reader:
            report_date = clean_csv_value(raw.get("Date"))
            if report_date < DATE_START or report_date > DATE_END:
                continue

            product = PLATFORM_TO_PRODUCT.get(clean_csv_value(raw.get("Platform")).lower())
            ad_format = DISPLAY_FORMAT_TO_AD_FORMAT.get(clean_csv_value(raw.get("Display format")).lower())
            if not product or not ad_format:
                continue

            key = (product, ad_format)
            rows_by_key[key]["bidding_requests"] += parse_int_value(raw.get("Bidding requests"))
            rows_by_key[key]["requests"] += parse_int_value(raw.get("Requests"))

    rows = []
    for product in PRODUCT_ORDER:
        for ad_format in AD_FORMAT_ORDER:
            metrics = rows_by_key[(product, ad_format)]
            rows.append(
                {
                    "product": product,
                    "ad_format": ad_format,
                    "bidding_requests": metrics["bidding_requests"],
                    "requests": metrics["requests"],
                }
            )
    return rows


def build_comparison_table_rows(
    facebook_rows: list[dict[str, Any]],
    backend_rows: list[dict[str, Any]],
    backend_field: str,
    metric_field: str,
) -> list[dict[str, Any]]:
    """构造单张对比表的行数据。"""
    backend_index = {
        (str(row["product"]), str(row["ad_format"])): row
        for row in backend_rows
    }
    rows = []
    for row in facebook_rows:
        product = str(row["product"])
        ad_format = str(row["ad_format"])
        backend_row = backend_index[(product, ad_format)]
        backend_value = int(backend_row[backend_field])
        metric_value = int(row[metric_field])
        rows.append(
            {
                "product": product,
                "ad_format": ad_format,
                "backend_value": backend_value,
                "metric_value": metric_value,
                "gap": backend_value - metric_value,
                "ratio": (metric_value / backend_value) if backend_value else None,
            }
        )
    return sort_product_format_rows(rows)


def build_scope_conclusion(rows: list[dict[str, Any]], metric_label: str, backend_label: str) -> list[str]:
    """生成单张对比表的结论。"""
    ratios = [row["ratio"] for row in rows if row["ratio"] is not None]
    if not ratios:
        return [f"- `{metric_label}` 与 `{backend_label}` 暂无可比数据。"]
    max_row = max(rows, key=lambda row: row["ratio"] if row["ratio"] is not None else -1)
    min_row = min(rows, key=lambda row: row["ratio"] if row["ratio"] is not None else 10**9)
    return [
        f"- `{metric_label}` 相对 `{backend_label}` 的恢复比例在 {fmt_ratio(min(ratios))} 到 {fmt_ratio(max(ratios))} 之间。",
        f"- 最接近后台的点是 `{min_row['product']}/{min_row['ad_format']}`，比例为 {fmt_ratio(min_row['ratio'])}；偏差最大的点是 `{max_row['product']}/{max_row['ad_format']}`，比例为 {fmt_ratio(max_row['ratio'])}。",
    ]


def metric_note(lines: list[str]) -> list[str]:
    """把指标说明包装成独立说明块。"""
    return [f"> - {line}" for line in lines]


def append_markdown_table(lines: list[str], headers: list[str], rows: list[list[str]]) -> None:
    """向 Markdown 文本追加表格。"""
    header_line = "| " + " | ".join(headers) + " |"
    split_line = "| " + " | ".join(["---"] * len(headers)) + " |"
    lines.extend([header_line, split_line])
    lines.extend("| " + " | ".join(row) + " |" for row in rows)


def build_markdown(
    facebook_rows: list[dict[str, Any]],
    backend_rows: list[dict[str, Any]],
    sql_sections: dict[str, str],
) -> str:
    """生成最终 Facebook 对比报告。"""
    started_vs_bidding = build_comparison_table_rows(
        facebook_rows, backend_rows, "bidding_requests", "facebook_started_cnt"
    )
    total_minus_vs_bidding = build_comparison_table_rows(
        facebook_rows, backend_rows, "bidding_requests", "facebook_total_minus_not_started_cnt"
    )
    started_vs_requests = build_comparison_table_rows(
        facebook_rows, backend_rows, "requests", "facebook_started_cnt"
    )
    total_minus_vs_requests = build_comparison_table_rows(
        facebook_rows, backend_rows, "requests", "facebook_total_minus_not_started_cnt"
    )

    lines = [
        "# Facebook latency 对比报告",
        "",
        "## 第三部分：Facebook 对比",
        "",
        "### 结论",
        "- 这一部分复用 AdMob 第三部分的状态码口径，只把渠道从 `AdMob` 替换成 `Facebook`。",
        "- 数据按时间窗内全版本汇总，不再限制版本，也不再输出版本维度。",
        "- `facebook_started_cnt` 统计 Facebook 渠道中状态为 `-2/-3` 的数量。",
        "- `facebook_total_minus_not_started_cnt` 统计“全部 latency 总数减去 Facebook 渠道状态为 `-1` 的数量”。",
        "",
    ]

    def add_table(title: str, title_rows: list[dict[str, Any]], metric_label: str, backend_label: str) -> None:
        lines.extend([
            f"### {title}",
            "",
            "#### 结论",
            *build_scope_conclusion(title_rows, metric_label, backend_label),
            "",
            "#### 数据",
            "",
        ])
        append_markdown_table(
            lines,
            ["product", "ad_format", "backend_value", "latency_value", "gap", "latency / backend"],
            [
                [
                    row["product"],
                    row["ad_format"],
                    fmt_num(row["backend_value"]),
                    fmt_num(row["metric_value"]),
                    fmt_num(row["gap"]),
                    fmt_ratio(row["ratio"]),
                ]
                for row in title_rows
            ],
        )
        lines.append("")

    add_table(
        "facebook_started_cnt vs Facebook Bidding requests",
        started_vs_bidding,
        "facebook_started_cnt",
        "Facebook Bidding requests",
    )
    add_table(
        "facebook_total_minus_not_started_cnt vs Facebook Bidding requests",
        total_minus_vs_bidding,
        "facebook_total_minus_not_started_cnt",
        "Facebook Bidding requests",
    )
    add_table(
        "facebook_started_cnt vs Facebook Requests",
        started_vs_requests,
        "facebook_started_cnt",
        "Facebook Requests",
    )
    add_table(
        "facebook_total_minus_not_started_cnt vs Facebook Requests",
        total_minus_vs_requests,
        "facebook_total_minus_not_started_cnt",
        "Facebook Requests",
    )

    lines.extend([
        "### 指标说明",
        *metric_note([
            "`facebook_started_cnt`：Facebook 渠道中 `fill_status_code` 为 `-2/-3` 的数量。",
            "`facebook_total_minus_not_started_cnt`：全部 latency 事件总数减去 Facebook 渠道中 `fill_status_code = -1` 的数量。",
            "`Bidding requests` 与 `Requests`：来自 `facebook.csv`，按 2026-01-05 到 2026-01-12 聚合。",
        ]),
        "",
        "## SQL 附录",
        "",
        "```sql",
        sql_sections["facebook_backend_compare"],
        "```",
        "",
    ])
    return "\n".join(lines)


def main() -> None:
    """执行 SQL，读取 Facebook 后台 CSV，并写出独立 Markdown 报告。"""
    if bigquery is None:
        raise ImportError("未安装 google-cloud-bigquery，无法执行 Facebook latency 对比查询。")

    sql_text = SQL_PATH.read_text(encoding="utf-8")
    sql_sections = parse_sql_sections(sql_text)

    client = bigquery.Client(project=PROJECT_ID)
    field_names, facebook_rows = run_query(client, sql_sections["facebook_backend_compare"])
    facebook_rows = sort_product_format_rows(facebook_rows)
    backend_rows = load_facebook_backend_rows()

    write_csv(COMPARE_CSV_PATH, field_names, facebook_rows)
    markdown = build_markdown(facebook_rows=facebook_rows, backend_rows=backend_rows, sql_sections=sql_sections)
    REPORT_MD_PATH.parent.mkdir(parents=True, exist_ok=True)
    REPORT_MD_PATH.write_text(markdown, encoding="utf-8")
    print(markdown)


if __name__ == "__main__":
    main()
