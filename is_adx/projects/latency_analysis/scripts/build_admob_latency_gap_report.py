"""模块功能：执行 AdMob latency 排查，输出结构化 Markdown 报告。"""

from __future__ import annotations

import csv
from collections import defaultdict
from pathlib import Path
from typing import Any

from google.cloud import bigquery

PROJECT_DIR = Path(__file__).resolve().parents[1]
SQL_DIR = PROJECT_DIR / "sql"
DOC_DIR = PROJECT_DIR / "docs"
OUTPUT_DIR = PROJECT_DIR / "outputs"
PROJECT_ID = "commercial-adx"
SQL_PATH = SQL_DIR / "admob_latency_gap_analysis.sql"
REPORT_MD_PATH = DOC_DIR / "admob_latency_gap_report.md"
VERSION_CSV_PATH = OUTPUT_DIR / "latency_version_coverage.csv"
MATCH_CSV_PATH = OUTPUT_DIR / "request_latency_match.csv"
ADMOB_CSV_PATH = OUTPUT_DIR / "admob_backend_compare.csv"
ADMOB_BACKEND_CSV_PATH = OUTPUT_DIR / "isadx-admob-report.csv"

SECTION_MARKERS = {
    "version_coverage": "-- section: version_coverage",
    "request_latency_match": "-- section: request_latency_match",
    "admob_backend_compare": "-- section: admob_backend_compare",
}

PRODUCT_ORDER = ["ios_screw_puzzle", "screw_puzzle"]
PRODUCT_LABELS = {
    "ios_screw_puzzle": "ios_screw_puzzle",
    "screw_puzzle": "screw_puzzle",
}
AD_FORMAT_ORDER = ["banner", "interstitial", "rewarded"]
FORMAT_TO_AD_FORMAT = {
    "Banner": "banner",
    "Interstitial": "interstitial",
    "Rewarded": "rewarded",
}
PLATFORM_TO_PRODUCT = {
    "Android": "screw_puzzle",
    "iOS": "ios_screw_puzzle",
}
TARGET_VERSIONS = {
    "screw_puzzle": "1.16.0",
    "ios_screw_puzzle": "1.15.0",
}

# MAX 后台数据来自用户提供的截图，继续作为独立数据源展示。
MAX_ATTEMPTS = {
    ("screw_puzzle", "banner"): 116_621_765,
    ("ios_screw_puzzle", "banner"): 61_646_892,
    ("screw_puzzle", "interstitial"): 38_194_317,
    ("ios_screw_puzzle", "interstitial"): 10_405_705,
    ("screw_puzzle", "rewarded"): 17_423_043,
    ("ios_screw_puzzle", "rewarded"): 6_275_023,
}

METRIC_LABELS = {
    "admob_started_cnt": "已发起 AdMob 请求数",
    "admob_total_minus_not_started_cnt": "全部 latency 扣 AdMob 未发起",
}
SCOPE_LABELS = {
    "all_network_status": "全部网络状态",
    "online_only": "仅有网",
}
NETWORK_STATUS_ORDER = ["online", "offline", "unknown"]


def parse_sql_sections(sql_text: str) -> dict[str, str]:
    """按 section 标记切分总 SQL 文件，避免维护多份重复 SQL。"""
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


def run_query(client: bigquery.Client, sql_text: str) -> tuple[list[str], list[dict[str, Any]]]:
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


def load_admob_backend_rows() -> list[dict[str, Any]]:
    """读取 AdMob 后台文件，并只保留目标版本的 Bid/Matched requests。"""
    rows_by_key: dict[tuple[str, str], dict[str, int]] = defaultdict(lambda: {"bid_requests": 0, "matched_requests": 0})
    with ADMOB_BACKEND_CSV_PATH.open("r", encoding="utf-16", newline="") as file_obj:
        reader = csv.DictReader(file_obj, delimiter="\t")
        for raw in reader:
            # 优先使用 Platform 区分 Android/iOS；旧文件无该列时再回退到版本映射。
            product = PLATFORM_TO_PRODUCT.get((raw.get("Platform") or "").strip())
            app_version = (raw.get("App version") or "").strip()
            if product is None:
                if app_version == TARGET_VERSIONS["screw_puzzle"]:
                    product = "screw_puzzle"
                elif app_version == TARGET_VERSIONS["ios_screw_puzzle"]:
                    product = "ios_screw_puzzle"
            ad_format = FORMAT_TO_AD_FORMAT.get((raw.get("Format") or "").strip())
            if not product or not ad_format:
                continue
            if app_version != TARGET_VERSIONS[product]:
                continue
            key = (product, ad_format)
            rows_by_key[key]["bid_requests"] += int((raw.get("Bid requests") or "0").replace(",", ""))
            rows_by_key[key]["matched_requests"] += int((raw.get("Matched requests") or "0").replace(",", ""))

    rows = []
    for product in PRODUCT_ORDER:
        for ad_format in AD_FORMAT_ORDER:
            bid_and_matched = rows_by_key[(product, ad_format)]
            rows.append(
                {
                    "product": product,
                    "target_version": TARGET_VERSIONS[product],
                    "ad_format": ad_format,
                    "admob_requests": bid_and_matched["bid_requests"],
                    "matched_requests": bid_and_matched["matched_requests"],
                }
            )
    return rows


def build_max_backend_rows() -> list[dict[str, Any]]:
    """整理 MAX 后台数据，保持与主表相同的 product + ad_format 粒度。"""
    rows = []
    for product in PRODUCT_ORDER:
        for ad_format in AD_FORMAT_ORDER:
            rows.append(
                {
                    "product": product,
                    "target_version": TARGET_VERSIONS[product],
                    "ad_format": ad_format,
                    "max_attempts": MAX_ATTEMPTS[(product, ad_format)],
                }
            )
    return rows


def split_admob_rows(admob_rows: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    """拆分第三段 SQL 结果，便于后续生成 8 张对比表和 4 张状态表。"""
    comparison_base = []
    network_breakdown = []
    online_status_detail = []
    for row in admob_rows:
        report_section = str(row["report_section"])
        if report_section == "comparison_base":
            comparison_base.append(row)
        elif report_section == "network_breakdown":
            network_breakdown.append(row)
        elif report_section == "online_status_detail":
            online_status_detail.append(row)
    return comparison_base, network_breakdown, online_status_detail


def build_comparison_table_rows(
    comparison_base_rows: list[dict[str, Any]],
    backend_rows: list[dict[str, Any]],
    scope_name: str,
    backend_field: str,
    metric_field: str,
) -> list[dict[str, Any]]:
    """构造单张对比表的行数据。"""
    backend_index = {
        (str(row["product"]), str(row["ad_format"])): row
        for row in backend_rows
    }
    rows = []
    for row in comparison_base_rows:
        if str(row["scope_name"]) != scope_name:
            continue
        product = str(row["product"])
        ad_format = str(row["ad_format"])
        backend_row = backend_index[(product, ad_format)]
        backend_value = int(backend_row[backend_field])
        metric_value = int(row[metric_field])
        rows.append(
            {
                "product": product,
                "target_version": str(row["target_version"]),
                "ad_format": ad_format,
                "backend_value": backend_value,
                "metric_value": metric_value,
                "gap": backend_value - metric_value,
                "ratio": (metric_value / backend_value) if backend_value else None,
            }
        )
    return sort_product_format_rows(rows)


def build_network_summary_rows(
    network_rows: list[dict[str, Any]],
    basis_name: str,
) -> list[dict[str, Any]]:
    """构造两套口径下 online/offline/unknown 的网络状态分布。"""
    rows = [
      {
          "product": str(row["product"]),
          "target_version": str(row["target_version"]),
          "ad_format": str(row["ad_format"]),
          "status_name": str(row["status_name"]),
          "pv_count": int(row["pv_count"]),
          "pv_ratio": float(row["pv_ratio"]) if row["pv_ratio"] is not None else None,
      }
      for row in network_rows
      if str(row["basis_name"]) == basis_name
    ]
    order = {name: idx for idx, name in enumerate(NETWORK_STATUS_ORDER)}
    return sorted(
        rows,
        key=lambda row: (
            PRODUCT_ORDER.index(row["product"]),
            AD_FORMAT_ORDER.index(row["ad_format"]),
            order.get(row["status_name"], 99),
        ),
    )


def build_online_detail_rows(
    detail_rows: list[dict[str, Any]],
    basis_name: str,
) -> list[dict[str, Any]]:
    """构造 online 内不同 lib_net_status 的 PV 和占比明细。"""
    rows = [
        {
            "product": str(row["product"]),
            "target_version": str(row["target_version"]),
            "ad_format": str(row["ad_format"]),
            "status_name": str(row["status_name"]),
            "pv_count": int(row["pv_count"]),
            "pv_ratio": float(row["pv_ratio"]) if row["pv_ratio"] is not None else None,
        }
        for row in detail_rows
        if str(row["basis_name"]) == basis_name
    ]
    return sorted(
        rows,
        key=lambda row: (
            PRODUCT_ORDER.index(row["product"]),
            AD_FORMAT_ORDER.index(row["ad_format"]),
            -row["pv_count"],
            row["status_name"],
        ),
    )


def metric_note(lines: list[str]) -> list[str]:
    """把指标说明包装成独立说明块，和正文结论分开展示。"""
    return [f"> - {line}" for line in lines]


def append_markdown_table(lines: list[str], headers: list[str], rows: list[list[str]]) -> None:
    """向 Markdown 文本追加表格。"""
    header_line = "| " + " | ".join(headers) + " |"
    split_line = "| " + " | ".join(["---"] * len(headers)) + " |"
    lines.extend([header_line, split_line])
    lines.extend("| " + " | ".join(row) + " |" for row in rows)


def build_version_conclusion(version_rows: list[dict[str, Any]]) -> list[str]:
    """生成主版本覆盖结论。"""
    lines = []
    for product in PRODUCT_ORDER:
        product_rows = [row for row in version_rows if str(row["product"]) == product]
        if not product_rows:
            continue
        top_versions = "、".join(str(row["app_version"]) for row in product_rows[:5])
        first_row = product_rows[0]
        lines.append(
            f"- `{product}` 的前 5 个活跃版本为 {top_versions}。其中主版本 `{first_row['app_version']}` 的 latency 用户覆盖率为 {fmt_ratio(first_row.get('latency_user_coverage_ratio'))}。这一部分只能作为用户覆盖的粗略参考，不直接代表请求级流失。"
        )
    return lines


def build_match_conclusion(match_rows: list[dict[str, Any]]) -> list[str]:
    """生成请求匹配结论。"""
    lines = []
    for product in PRODUCT_ORDER:
        product_rows = [row for row in match_rows if str(row["product"]) == product]
        if not product_rows:
            continue
        request_cnt = sum(int(row["request_cnt"]) for row in product_rows)
        matched_cnt = sum(int(row["matched_request_cnt"]) for row in product_rows)
        request_without_latency_cnt = sum(int(row["request_without_latency_cnt"]) for row in product_rows)
        match_rate = matched_cnt / request_cnt if request_cnt else 0.0
        lines.append(
            f"- `{product}` 主版本的请求匹配率约为 {fmt_ratio(match_rate)}，共有 {fmt_num(request_without_latency_cnt)} 个 request 没有匹配到 latency。相比用户覆盖，这一层更接近真实链路损耗。"
        )
    return lines


def build_scope_conclusion(rows: list[dict[str, Any]], metric_label: str, backend_label: str, scope_label: str) -> list[str]:
    """生成单张对比表的结论。"""
    ratios = [row["ratio"] for row in rows if row["ratio"] is not None]
    if not ratios:
        return [f"- `{scope_label}` 下 `{metric_label}` 与 `{backend_label}` 暂无可比数据。"]
    max_row = max(rows, key=lambda row: row["ratio"] if row["ratio"] is not None else -1)
    min_row = min(rows, key=lambda row: row["ratio"] if row["ratio"] is not None else 10**9)
    return [
        f"- `{scope_label}` 下，`{metric_label}` 相对 `{backend_label}` 的恢复比例在 {fmt_ratio(min(ratios))} 到 {fmt_ratio(max(ratios))} 之间。",
        f"- 最接近 `{backend_label}` 的点是 `{min_row['product']}/{min_row['ad_format']}`，比例为 {fmt_ratio(min_row['ratio'])}；偏差最大的点是 `{max_row['product']}/{max_row['ad_format']}`，比例为 {fmt_ratio(max_row['ratio'])}。",
    ]


def build_scope_identity_note(
    started_rows: list[dict[str, Any]],
    total_minus_rows: list[dict[str, Any]],
    scope_label: str,
) -> list[str]:
    """若两套口径数值仍完全一致，明确说明这是数据事实而非 SQL 结构巧合。"""
    started_index = {
        (str(row["product"]), str(row["ad_format"])): int(row["metric_value"])
        for row in started_rows
    }
    total_index = {
        (str(row["product"]), str(row["ad_format"])): int(row["metric_value"])
        for row in total_minus_rows
    }
    if not started_index or set(started_index) != set(total_index):
        return []
    if any(started_index[key] != total_index[key] for key in started_index):
        return []
    return [
        f"- `{scope_label}` 下，`admob_started_cnt` 与 `admob_total_minus_not_started_cnt` 当前数值完全一致，这是当前数据事实，不代表两者仍然使用同一基数。"
    ]


def build_network_conclusion(rows: list[dict[str, Any]], basis_label: str) -> list[str]:
    """生成 online/offline/unknown 的网络状态分布结论。"""
    product_group: dict[tuple[str, str], dict[str, int]] = defaultdict(lambda: defaultdict(int))
    for row in rows:
        key = (str(row["product"]), str(row["ad_format"]))
        product_group[key][str(row["status_name"])] += int(row["pv_count"])

    lines = []
    for (product, ad_format), counts in sorted(
        product_group.items(),
        key=lambda item: (PRODUCT_ORDER.index(item[0][0]), AD_FORMAT_ORDER.index(item[0][1])),
    ):
        total = sum(counts.values())
        online_ratio = counts.get("online", 0) / total if total else 0.0
        offline_ratio = counts.get("offline", 0) / total if total else 0.0
        unknown_ratio = counts.get("unknown", 0) / total if total else 0.0
        lines.append(
            f"- `{product}/{ad_format}` 在 `{basis_label}` 下，`online/offline/unknown` 占比分别为 {fmt_ratio(online_ratio)} / {fmt_ratio(offline_ratio)} / {fmt_ratio(unknown_ratio)}。"
        )
    return lines


def build_online_detail_conclusion(rows: list[dict[str, Any]], basis_label: str) -> list[str]:
    """生成 online 内原始 lib_net_status 明细结论。"""
    top_items = sorted(rows, key=lambda row: row["pv_count"], reverse=True)[:6]
    if not top_items:
        return [f"- `{basis_label}` 下 online 内没有明细数据。"]
    labels = "；".join(
        f"{row['product']}/{row['ad_format']}/{row['status_name']} = {fmt_num(row['pv_count'])} ({fmt_ratio(row['pv_ratio'])})"
        for row in top_items
    )
    return [f"- `{basis_label}` 下 online 内占比最高的原始网络状态为：{labels}。"]


def build_markdown(
    version_rows: list[dict[str, Any]],
    match_rows: list[dict[str, Any]],
    admob_rows: list[dict[str, Any]],
    admob_backend_rows: list[dict[str, Any]],
    max_backend_rows: list[dict[str, Any]],
    sql_sections: dict[str, str],
) -> str:
    """生成最终分析报告。"""
    comparison_base_rows, network_rows, online_detail_rows = split_admob_rows(admob_rows)

    all_started_vs_bid = build_comparison_table_rows(comparison_base_rows, admob_backend_rows, "all_network_status", "admob_requests", "admob_started_cnt")
    all_total_vs_bid = build_comparison_table_rows(comparison_base_rows, admob_backend_rows, "all_network_status", "admob_requests", "admob_total_minus_not_started_cnt")
    all_started_vs_matched = build_comparison_table_rows(comparison_base_rows, admob_backend_rows, "all_network_status", "matched_requests", "admob_started_cnt")
    all_total_vs_matched = build_comparison_table_rows(comparison_base_rows, admob_backend_rows, "all_network_status", "matched_requests", "admob_total_minus_not_started_cnt")

    online_started_vs_bid = build_comparison_table_rows(comparison_base_rows, admob_backend_rows, "online_only", "admob_requests", "admob_started_cnt")
    online_total_vs_bid = build_comparison_table_rows(comparison_base_rows, admob_backend_rows, "online_only", "admob_requests", "admob_total_minus_not_started_cnt")
    online_started_vs_matched = build_comparison_table_rows(comparison_base_rows, admob_backend_rows, "online_only", "matched_requests", "admob_started_cnt")
    online_total_vs_matched = build_comparison_table_rows(comparison_base_rows, admob_backend_rows, "online_only", "matched_requests", "admob_total_minus_not_started_cnt")

    started_network_rows = build_network_summary_rows(network_rows, "admob_started_cnt")
    total_minus_network_rows = build_network_summary_rows(network_rows, "admob_total_minus_not_started_cnt")
    started_online_detail_rows = build_online_detail_rows(online_detail_rows, "admob_started_cnt")
    total_minus_online_detail_rows = build_online_detail_rows(online_detail_rows, "admob_total_minus_not_started_cnt")

    lines = [
        "# AdMob latency 排查报告",
        "",
        "## 第一部分：主版本覆盖",
        "",
        "### 结论",
        *build_version_conclusion(version_rows),
        "",
        "### 数据",
        "",
    ]
    append_markdown_table(
        lines,
        ["product", "app_version", "dau_user_cnt", "latency_user_cnt", "latency_user_coverage_ratio", "dau_user_share", "latency_user_share"],
        [
            [
                str(row["product"]),
                str(row["app_version"]),
                fmt_num(row["dau_user_cnt"]),
                fmt_num(row["latency_user_cnt"]),
                fmt_ratio(row.get("latency_user_coverage_ratio")),
                fmt_ratio(row.get("dau_user_share")),
                fmt_ratio(row.get("latency_user_share")),
            ]
            for row in version_rows
        ],
    )
    lines.extend([
        "",
        "### 指标说明",
        *metric_note([
            "`dau_user_cnt`：该版本在 user_engagement 中的去重活跃用户数。",
            "`latency_user_cnt`：该版本在 adslog_load_latency 中出现过的去重用户数。",
            "`latency_user_coverage_ratio = latency_user_cnt / dau_user_cnt`。",
            "这部分只用于判断主版本是否大范围上报，不直接代表请求流失。",
        ]),
        "",
        "## 第二部分：请求匹配",
        "",
        "### 结论",
        *build_match_conclusion(match_rows),
        "",
        "### 数据",
        "",
    ])
    append_markdown_table(
        lines,
        ["product", "target_version", "ad_format", "request_cnt", "latency_request_cnt", "matched_request_cnt", "request_without_latency_cnt", "latency_without_request_cnt", "request_match_rate", "latency_backfill_rate"],
        [
            [
                str(row["product"]),
                str(row["target_version"]),
                str(row["ad_format"]),
                fmt_num(row["request_cnt"]),
                fmt_num(row["latency_request_cnt"]),
                fmt_num(row["matched_request_cnt"]),
                fmt_num(row["request_without_latency_cnt"]),
                fmt_num(row["latency_without_request_cnt"]),
                fmt_ratio(row.get("request_match_rate")),
                fmt_ratio(row.get("latency_backfill_rate")),
            ]
            for row in sort_product_format_rows(match_rows)
        ],
    )
    lines.extend([
        "",
        "### 指标说明",
        *metric_note([
            "`request_cnt`：主版本 adslog_request 中按 product + ad_format + user_pseudo_id + request_id 去重后的请求数。",
            "`latency_request_cnt`：主版本 adslog_load_latency 中按相同键去重后的请求数。",
            "`matched_request_cnt`：两边 request_id 成功匹配到的请求数。",
            "`request_match_rate = matched_request_cnt / request_cnt`。",
        ]),
        "",
        "## 第三部分：AdMob 对比",
        "",
        "### 3A. all_network_status",
        "",
        "#### 表 1：已发起 AdMob 请求数 vs AdMob Bid requests",
        "",
        "##### 结论",
        *build_scope_conclusion(all_started_vs_bid, METRIC_LABELS["admob_started_cnt"], "AdMob Bid requests", SCOPE_LABELS["all_network_status"]),
        "",
        "##### 数据",
        "",
    ])

    def add_comparison_table(title_rows: list[dict[str, Any]]) -> None:
        append_markdown_table(
            lines,
            ["product", "target_version", "ad_format", "backend_value", "latency_value", "gap", "latency / backend"],
            [
                [
                    row["product"],
                    row["target_version"],
                    row["ad_format"],
                    fmt_num(row["backend_value"]),
                    fmt_num(row["metric_value"]),
                    fmt_num(row["gap"]),
                    fmt_ratio(row["ratio"]),
                ]
                for row in title_rows
            ],
        )

    add_comparison_table(all_started_vs_bid)
    lines.extend([
        "",
        "#### 表 2：全部 latency 扣 AdMob 未发起 vs AdMob Bid requests",
        "",
        "##### 结论",
        *build_scope_conclusion(all_total_vs_bid, METRIC_LABELS["admob_total_minus_not_started_cnt"], "AdMob Bid requests", SCOPE_LABELS["all_network_status"]),
        *build_scope_identity_note(all_started_vs_bid, all_total_vs_bid, SCOPE_LABELS["all_network_status"]),
        "",
        "##### 数据",
        "",
    ])
    add_comparison_table(all_total_vs_bid)
    lines.extend([
        "",
        "#### 表 3：已发起 AdMob 请求数 vs AdMob Matched requests",
        "",
        "##### 结论",
        *build_scope_conclusion(all_started_vs_matched, METRIC_LABELS["admob_started_cnt"], "AdMob Matched requests", SCOPE_LABELS["all_network_status"]),
        "",
        "##### 数据",
        "",
    ])
    add_comparison_table(all_started_vs_matched)
    lines.extend([
        "",
        "#### 表 4：全部 latency 扣 AdMob 未发起 vs AdMob Matched requests",
        "",
        "##### 结论",
        *build_scope_conclusion(all_total_vs_matched, METRIC_LABELS["admob_total_minus_not_started_cnt"], "AdMob Matched requests", SCOPE_LABELS["all_network_status"]),
        "",
        "##### 数据",
        "",
    ])
    add_comparison_table(all_total_vs_matched)

    lines.extend([
        "",
        "### 3B. online_only",
        "",
        "#### 表 5：已发起 AdMob 请求数 vs AdMob Bid requests",
        "",
        "##### 结论",
        *build_scope_conclusion(online_started_vs_bid, METRIC_LABELS["admob_started_cnt"], "AdMob Bid requests", SCOPE_LABELS["online_only"]),
        "",
        "##### 数据",
        "",
    ])
    add_comparison_table(online_started_vs_bid)
    lines.extend([
        "",
        "#### 表 6：全部 latency 扣 AdMob 未发起 vs AdMob Bid requests",
        "",
        "##### 结论",
        *build_scope_conclusion(online_total_vs_bid, METRIC_LABELS["admob_total_minus_not_started_cnt"], "AdMob Bid requests", SCOPE_LABELS["online_only"]),
        *build_scope_identity_note(online_started_vs_bid, online_total_vs_bid, SCOPE_LABELS["online_only"]),
        "",
        "##### 数据",
        "",
    ])
    add_comparison_table(online_total_vs_bid)
    lines.extend([
        "",
        "#### 表 7：已发起 AdMob 请求数 vs AdMob Matched requests",
        "",
        "##### 结论",
        *build_scope_conclusion(online_started_vs_matched, METRIC_LABELS["admob_started_cnt"], "AdMob Matched requests", SCOPE_LABELS["online_only"]),
        "",
        "##### 数据",
        "",
    ])
    add_comparison_table(online_started_vs_matched)
    lines.extend([
        "",
        "#### 表 8：全部 latency 扣 AdMob 未发起 vs AdMob Matched requests",
        "",
        "##### 结论",
        *build_scope_conclusion(online_total_vs_matched, METRIC_LABELS["admob_total_minus_not_started_cnt"], "AdMob Matched requests", SCOPE_LABELS["online_only"]),
        "",
        "##### 数据",
        "",
    ])
    add_comparison_table(online_total_vs_matched)

    lines.extend([
        "",
        "### 3C. network_status_breakdown",
        "",
        "#### 表 9：admob_started_cnt 口径下的 online/offline/unknown 分布",
        "",
        "##### 结论",
        *build_network_conclusion(started_network_rows, METRIC_LABELS["admob_started_cnt"]),
        "",
        "##### 数据",
        "",
    ])
    append_markdown_table(
        lines,
        ["product", "target_version", "ad_format", "status_name", "pv_count", "pv_ratio"],
        [
            [
                row["product"],
                row["target_version"],
                row["ad_format"],
                row["status_name"],
                fmt_num(row["pv_count"]),
                fmt_ratio(row["pv_ratio"]),
            ]
            for row in started_network_rows
        ],
    )
    lines.extend([
        "",
        "#### 表 10：admob_total_minus_not_started_cnt 口径下的 online/offline/unknown 分布",
        "",
        "##### 结论",
        *build_network_conclusion(total_minus_network_rows, METRIC_LABELS["admob_total_minus_not_started_cnt"]),
        "",
        "##### 数据",
        "",
    ])
    append_markdown_table(
        lines,
        ["product", "target_version", "ad_format", "status_name", "pv_count", "pv_ratio"],
        [
            [
                row["product"],
                row["target_version"],
                row["ad_format"],
                row["status_name"],
                fmt_num(row["pv_count"]),
                fmt_ratio(row["pv_ratio"]),
            ]
            for row in total_minus_network_rows
        ],
    )
    lines.extend([
        "",
        "#### 表 11：online 内原始 lib_net_status 明细（admob_started_cnt 口径）",
        "",
        "##### 结论",
        *build_online_detail_conclusion(started_online_detail_rows, METRIC_LABELS["admob_started_cnt"]),
        "",
        "##### 数据",
        "",
    ])
    append_markdown_table(
        lines,
        ["product", "target_version", "ad_format", "lib_net_status", "pv_count", "pv_ratio"],
        [
            [
                row["product"],
                row["target_version"],
                row["ad_format"],
                row["status_name"],
                fmt_num(row["pv_count"]),
                fmt_ratio(row["pv_ratio"]),
            ]
            for row in started_online_detail_rows
        ],
    )
    lines.extend([
        "",
        "#### 表 12：online 内原始 lib_net_status 明细（admob_total_minus_not_started_cnt 口径）",
        "",
        "##### 结论",
        *build_online_detail_conclusion(total_minus_online_detail_rows, METRIC_LABELS["admob_total_minus_not_started_cnt"]),
        "",
        "##### 数据",
        "",
    ])
    append_markdown_table(
        lines,
        ["product", "target_version", "ad_format", "lib_net_status", "pv_count", "pv_ratio"],
        [
            [
                row["product"],
                row["target_version"],
                row["ad_format"],
                row["status_name"],
                fmt_num(row["pv_count"]),
                fmt_ratio(row["pv_ratio"]),
            ]
            for row in total_minus_online_detail_rows
        ],
    )

    lines.extend([
        "",
        "### 指标说明",
        *metric_note([
            "`admob_started_cnt`：指定 6 个 AdMob placement 中，status 为 `-2/-3` 的数量。",
            "`admob_total_minus_not_started_cnt`：全部 latency 事件数减去 AdMob 未发起数。",
            "`all_network_status`：同时包含 online / offline / unknown。",
            "`online_only`：只保留全部 latency 事件中的 `network_status_group = online` 部分。",
            "`offline`：`lib_net_status = network-null`。",
            "`unknown`：`lib_net_status = network-unknown` 或空串。",
            "`online`：除 `network-null`、`network-unknown`、空串外的其他网络状态。",
        ]),
        "",
        "## 第四部分：MAX 后台数据",
        "",
        "### 结论",
        "- 这一部分只保留 MAX 后台的原始 attempts，作为第三个数据源单独展示，不和 AdMob 对比表混写。",
        "",
        "### 数据",
        "",
    ])
    append_markdown_table(
        lines,
        ["product", "target_version", "ad_format", "max_attempts"],
        [
            [
                row["product"],
                row["target_version"],
                row["ad_format"],
                fmt_num(row["max_attempts"]),
            ]
            for row in max_backend_rows
        ],
    )
    lines.extend([
        "",
        "### 指标说明",
        *metric_note([
            "`max_attempts`：来自用户提供截图的 MAX 后台 attempts 数据。",
        ]),
        "",
        "## SQL 附录",
        "",
        "### 1. 主版本覆盖 SQL",
        "```sql",
        sql_sections["version_coverage"],
        "```",
        "",
        "### 2. 请求匹配 SQL",
        "```sql",
        sql_sections["request_latency_match"],
        "```",
        "",
        "### 3. AdMob 对比 SQL",
        "```sql",
        sql_sections["admob_backend_compare"],
        "```",
        "",
    ])
    return "\n".join(lines)


def main() -> None:
    """执行三段 SQL，输出 CSV 和 Markdown。"""
    sql_text = SQL_PATH.read_text(encoding="utf-8")
    sql_sections = parse_sql_sections(sql_text)

    client = bigquery.Client(project=PROJECT_ID)

    version_fields, version_rows = run_query(client, sql_sections["version_coverage"])
    match_fields, match_rows = run_query(client, sql_sections["request_latency_match"])
    admob_fields, admob_rows = run_query(client, sql_sections["admob_backend_compare"])

    version_rows = sorted(
        version_rows,
        key=lambda row: (str(row["product"]), -int(row["dau_user_cnt"]), str(row["app_version"])),
    )
    match_rows = sort_product_format_rows(match_rows)
    admob_rows = sorted(
        admob_rows,
        key=lambda row: (
            str(row["report_section"]),
            PRODUCT_ORDER.index(str(row["product"])),
            AD_FORMAT_ORDER.index(str(row["ad_format"])),
            str(row["scope_name"] or ""),
            str(row["basis_name"] or ""),
            str(row["status_name"] or ""),
        ),
    )
    admob_backend_rows = load_admob_backend_rows()
    max_backend_rows = build_max_backend_rows()

    write_csv(VERSION_CSV_PATH, version_fields, version_rows)
    write_csv(MATCH_CSV_PATH, match_fields, match_rows)
    write_csv(ADMOB_CSV_PATH, admob_fields, admob_rows)

    markdown = build_markdown(
        version_rows=version_rows,
        match_rows=match_rows,
        admob_rows=admob_rows,
        admob_backend_rows=admob_backend_rows,
        max_backend_rows=max_backend_rows,
        sql_sections=sql_sections,
    )
    REPORT_MD_PATH.parent.mkdir(parents=True, exist_ok=True)
    REPORT_MD_PATH.write_text(markdown, encoding="utf-8")
    print(markdown)


if __name__ == "__main__":
    main()
