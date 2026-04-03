"""读取成功 request 聚合结果并生成独立 HTML。"""

from __future__ import annotations

import csv
import json
import math
import re
import sys
from collections import defaultdict
from pathlib import Path
from typing import Any

PROJECT_DIR = Path(__file__).resolve().parents[1]
AB_DASHBOARD_SCRIPT_DIR = PROJECT_DIR.parent / "ab_dashboard" / "scripts"
if str(AB_DASHBOARD_SCRIPT_DIR) not in sys.path:
    sys.path.append(str(AB_DASHBOARD_SCRIPT_DIR))

from mediation_scope import load_mediation_configuration

OUTPUT_DIR = PROJECT_DIR / "outputs"
ASSET_SCRIPT_PATH = "../../ab_dashboard/assets/echarts.min.js"
DASHBOARD_HTML = OUTPUT_DIR / "success_request_dashboard.html"
PAYLOAD_JSON = OUTPUT_DIR / "success_request_dashboard_payload.json"
MEDIATION_REPORT_CSV = Path(r"D:\Downloads\mediation_report_2026-03-25_09_41_32.csv")

GROUP_A = "no_is_adx"
GROUP_B = "have_is_adx"
GROUP_ORDER = [GROUP_A, GROUP_B]
GROUP_LABELS = {GROUP_A: "A组", GROUP_B: "B组"}
UNKNOWN_COUNTRY = "UNKNOWN"
UNKNOWN_UNIT = "UNKNOWN_UNIT"
MAX_CNT_BUCKET = 35
TAIL_P_RE = re.compile(r"p\s*(\d+)\s*$", re.IGNORECASE)
TAIL_DF_RE = re.compile(r"df\s*$", re.IGNORECASE)
CSV_FILES = {
    "scope": OUTPUT_DIR / "success_request_scope_summary.csv",
    "cnt": OUTPUT_DIR / "success_request_cnt_distribution.csv",
    "channel": OUTPUT_DIR / "success_request_channel_distribution.csv",
    "rank": OUTPUT_DIR / "success_request_rank_distribution.csv",
    "ecpm": OUTPUT_DIR / "success_request_ecpm_distribution.csv",
}

csv.field_size_limit(min(sys.maxsize, 2147483647))


def load_rows(path: Path) -> list[dict[str, Any]]:
    """读取 UTF-8-SIG CSV。"""
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8-sig", newline="") as file_obj:
        return list(csv.DictReader(file_obj))


def to_int(value: Any) -> int:
    text = str(value or "").strip()
    if not text:
        return 0
    return int(float(text))


def to_float(value: Any) -> float:
    text = str(value or "").strip()
    if not text:
        return 0.0
    return float(text)


def load_ad_unit_name_map() -> dict[str, str]:
    try:
        unit_name_map, _ = load_mediation_configuration(MEDIATION_REPORT_CSV)
        return unit_name_map
    except Exception:
        return {}


def attach_unit_labels(rows: list[dict[str, Any]], unit_name_map: dict[str, str]) -> list[dict[str, Any]]:
    enriched: list[dict[str, Any]] = []
    for row in rows:
        current = dict(row)
        unit_id = str(current.get("max_unit_id") or "").strip() or UNKNOWN_UNIT
        current["max_unit_id"] = unit_id
        current["ad_unit_name"] = unit_name_map.get(unit_id, unit_id)
        enriched.append(current)
    return enriched


def combo_key(product: str, ad_format: str) -> str:
    return f"{product}__{ad_format}"


def scope_key(product: str, ad_format: str, country: str, unit_id: str, cnt_type: str, cnt_value: str) -> str:
    return f"{product}__{ad_format}__{country}__{unit_id}__{cnt_type}__{cnt_value}"


def normalize_cnt_bucket(raw_value: Any, max_bucket: int = MAX_CNT_BUCKET) -> str:
    cnt_value = max(0, to_int(raw_value))
    if cnt_value > max_bucket:
        return f"{max_bucket}+"
    return str(cnt_value)


def cnt_bucket_sort_key(label: str) -> tuple[int, int]:
    text = str(label or "").strip()
    if text.endswith("+"):
        return (to_int(text[:-1] or 0), 1)
    return (to_int(text), 0)


def unit_tail_sort_key(unit_label: str) -> tuple[int, Any]:
    text = str(unit_label or "").strip()
    lowered = text.lower()
    p_match = TAIL_P_RE.search(lowered)
    if p_match:
        return (0, int(p_match.group(1)))
    if TAIL_DF_RE.search(lowered):
        return (1, lowered)
    return (2, lowered)


def normalize_success_network_type(raw_value: Any) -> str:
    text = str(raw_value or "").strip().lower()
    return "bidding" if text == "bidding" else "waterfall"


def build_share_distribution_rows(
    rows: list[dict[str, Any]],
    *,
    dimension_fields: list[str],
    category_field: str,
    count_field: str,
    category_transform: Any | None = None,
    category_sort_key: Any | None = None,
) -> list[dict[str, Any]]:
    totals: dict[tuple[Any, ...], dict[str, int]] = defaultdict(lambda: {GROUP_A: 0, GROUP_B: 0})
    category_counts: dict[tuple[Any, ...], dict[str, dict[str, int]]] = defaultdict(lambda: defaultdict(lambda: {GROUP_A: 0, GROUP_B: 0}))

    for row in rows:
        experiment_group = str(row.get("experiment_group") or "").strip()
        if experiment_group not in GROUP_ORDER:
            continue
        dimension_key = tuple(str(row.get(field) or "").strip() for field in dimension_fields)
        raw_category = row.get(category_field)
        category_label = category_transform(raw_category) if category_transform else str(raw_category or "").strip()
        category_label = str(category_label or "").strip()
        count_value = to_int(row.get(count_field))
        totals[dimension_key][experiment_group] += count_value
        category_counts[dimension_key][category_label][experiment_group] += count_value

    output: list[dict[str, Any]] = []
    for dimension_key, categories in category_counts.items():
        total_a = totals[dimension_key][GROUP_A]
        total_b = totals[dimension_key][GROUP_B]
        ordered_categories = sorted(
            categories.keys(),
            key=category_sort_key or (lambda value: str(value)),
        )
        for category_label in ordered_categories:
            counts = categories[category_label]
            request_pv_a = counts[GROUP_A]
            request_pv_b = counts[GROUP_B]
            share_a = request_pv_a / total_a if total_a else 0.0
            share_b = request_pv_b / total_b if total_b else 0.0
            row = {field: value for field, value in zip(dimension_fields, dimension_key, strict=False)}
            row.update(
                {
                    "category_label": category_label,
                    "request_pv_a": request_pv_a,
                    "request_pv_b": request_pv_b,
                    "share_a": share_a,
                    "share_b": share_b,
                    "share_diff": share_b - share_a,
                    "total_pv_a": total_a,
                    "total_pv_b": total_b,
                }
            )
            output.append(row)
    return output


def build_top_countries_by_combo(scope_rows: list[dict[str, Any]], limit: int = 10) -> dict[str, list[str]]:
    """按 product + ad_format 汇总 A/B 成功 request，总量取 TopN country。"""
    country_totals: dict[str, dict[str, int]] = defaultdict(lambda: defaultdict(int))
    for row in scope_rows:
        product = str(row.get("product") or "").strip()
        ad_format = str(row.get("ad_format") or "").strip()
        country = str(row.get("country") or UNKNOWN_COUNTRY).strip() or UNKNOWN_COUNTRY
        if not product or not ad_format:
            continue
        country_totals[combo_key(product, ad_format)][country] += to_int(row.get("success_request_cnt"))

    ranked: dict[str, list[str]] = {}
    for key, country_map in country_totals.items():
        ordered = sorted(country_map.items(), key=lambda item: (-item[1], item[0]))
        ranked[key] = [country for country, _ in ordered[:limit]]
    return ranked


def build_unit_options_by_country(
    scope_rows: list[dict[str, Any]],
    country_options: dict[str, list[str]],
) -> dict[str, list[str]]:
    """在 Top country 下保留全部 unit，按尾部 P/DF 规则排序。"""
    unit_labels: dict[str, dict[str, str]] = defaultdict(dict)
    unit_presence: dict[str, set[str]] = defaultdict(set)
    for row in scope_rows:
        product = str(row.get("product") or "").strip()
        ad_format = str(row.get("ad_format") or "").strip()
        country = str(row.get("country") or UNKNOWN_COUNTRY).strip() or UNKNOWN_COUNTRY
        unit_id = str(row.get("max_unit_id") or UNKNOWN_UNIT).strip() or UNKNOWN_UNIT
        unit_label = str(row.get("ad_unit_name") or unit_id).strip() or unit_id
        key = combo_key(product, ad_format)
        if country not in country_options.get(key, []):
            continue
        composite_key = f"{key}__{country}"
        unit_presence[composite_key].add(unit_id)
        unit_labels[composite_key][unit_id] = unit_label

    options: dict[str, list[str]] = {}
    for key, unit_ids in unit_presence.items():
        ordered = sorted(unit_ids, key=lambda unit_id: unit_tail_sort_key(unit_labels[key].get(unit_id, unit_id)))
        options[key] = list(ordered)
    return options


def build_adaptive_ecpm_buckets(
    freq_rows: list[dict[str, Any]],
    max_buckets: int = 12,
) -> list[dict[str, Any]]:
    """根据价格频数做非等宽桶，优先按累计频数与明显跳点切分。"""
    points = [
        {"rounded_ecpm": to_float(row.get("rounded_ecpm")), "request_pv": to_int(row.get("request_pv"))}
        for row in freq_rows
        if str(row.get("rounded_ecpm") or "").strip()
    ]
    if not points:
        return []

    points.sort(key=lambda item: item["rounded_ecpm"])
    max_buckets = max(1, min(max_buckets, len(points)))
    total_pv = sum(item["request_pv"] for item in points)
    target_pv = max(1, math.ceil(total_pv / max_buckets))

    buckets: list[dict[str, Any]] = []
    start_index = 0
    bucket_pv = 0
    point_count = 0

    for index, point in enumerate(points):
        bucket_pv += point["request_pv"]
        point_count += 1
        is_last = index == len(points) - 1
        remaining_points = len(points) - index - 1
        remaining_buckets = max_buckets - len(buckets) - 1
        should_split = False

        if not is_last:
            current_price = point["rounded_ecpm"]
            next_price = points[index + 1]["rounded_ecpm"]
            current_width = current_price - points[start_index]["rounded_ecpm"]
            gap = next_price - current_price
            ratio_gap = next_price / max(current_price, 0.01)
            large_gap = gap >= max(0.5, current_width * 1.5) or ratio_gap >= 3
            enough_mass = bucket_pv >= target_pv
            force_split = remaining_points <= remaining_buckets
            should_split = force_split or (enough_mass and (large_gap or point_count >= 1))

        if should_split or is_last:
            bucket_min = points[start_index]["rounded_ecpm"]
            bucket_max = point["rounded_ecpm"]
            buckets.append(
                {
                    "bucket_min": bucket_min,
                    "bucket_max": bucket_max,
                    "bucket_label": f"{bucket_min:.2f}-{bucket_max:.2f}",
                    "request_pv": bucket_pv,
                }
            )
            start_index = index + 1
            bucket_pv = 0
            point_count = 0

    return buckets


def format_ecpm_bound(value: float) -> str:
    text = f"{value:.2f}"
    if "." in text:
        text = text.rstrip("0").rstrip(".")
    return text or "0"


def assign_bucket_label(price: float, buckets: list[dict[str, Any]]) -> dict[str, Any] | None:
    for bucket in buckets:
        if bucket["bucket_min"] <= price <= bucket["bucket_max"]:
            return bucket
    return buckets[-1] if buckets else None


def filter_rows_by_top_countries(
    rows: list[dict[str, Any]],
    country_options: dict[str, list[str]],
) -> list[dict[str, Any]]:
    filtered: list[dict[str, Any]] = []
    for row in rows:
        key = combo_key(str(row.get("product") or "").strip(), str(row.get("ad_format") or "").strip())
        country = str(row.get("country") or UNKNOWN_COUNTRY).strip() or UNKNOWN_COUNTRY
        if country in country_options.get(key, []):
            filtered.append(row)
    return filtered


def build_weighted_mean_summary_rows(
    rows: list[dict[str, Any]],
    *,
    dimension_fields: list[str],
    value_field: str,
    count_field_a: str,
    count_field_b: str,
    value_parser: Any,
) -> list[dict[str, Any]]:
    weighted_sums: dict[tuple[Any, ...], dict[str, float]] = defaultdict(lambda: {GROUP_A: 0.0, GROUP_B: 0.0})
    totals: dict[tuple[Any, ...], dict[str, int]] = defaultdict(lambda: {GROUP_A: 0, GROUP_B: 0})

    for row in rows:
        dimension_key = tuple(str(row.get(field) or "").strip() for field in dimension_fields)
        value = value_parser(row.get(value_field))
        count_a = to_int(row.get(count_field_a))
        count_b = to_int(row.get(count_field_b))
        weighted_sums[dimension_key][GROUP_A] += value * count_a
        weighted_sums[dimension_key][GROUP_B] += value * count_b
        totals[dimension_key][GROUP_A] += count_a
        totals[dimension_key][GROUP_B] += count_b

    output: list[dict[str, Any]] = []
    for dimension_key, weighted in weighted_sums.items():
        total_a = totals[dimension_key][GROUP_A]
        total_b = totals[dimension_key][GROUP_B]
        mean_a = weighted[GROUP_A] / total_a if total_a else 0.0
        mean_b = weighted[GROUP_B] / total_b if total_b else 0.0
        row = {field: value for field, value in zip(dimension_fields, dimension_key, strict=False)}
        row.update(
            {
                "mean_a": mean_a,
                "mean_b": mean_b,
                "mean_diff": mean_b - mean_a,
                "total_pv_a": total_a,
                "total_pv_b": total_b,
            }
        )
        output.append(row)
    return output


def load_ecpm_distribution_source(
    path: Path,
    country_options: dict[str, list[str]],
) -> tuple[
    dict[tuple[str, str, str, str, str, str], dict[float, dict[str, int]]],
    dict[tuple[str, str, str, str, str, str], dict[str, int]],
]:
    price_counts_by_slice: dict[tuple[str, str, str, str, str, str], dict[float, dict[str, int]]] = defaultdict(
        lambda: defaultdict(lambda: {GROUP_A: 0, GROUP_B: 0})
    )
    totals: dict[tuple[str, str, str, str, str, str], dict[str, int]] = defaultdict(lambda: {GROUP_A: 0, GROUP_B: 0})

    if not path.exists():
        return price_counts_by_slice, totals

    with path.open("r", encoding="utf-8-sig", newline="") as file_obj:
        reader = csv.DictReader(file_obj)
        for row in reader:
            product = str(row.get("product") or "").strip()
            ad_format = str(row.get("ad_format") or "").strip()
            country = str(row.get("country") or UNKNOWN_COUNTRY).strip() or UNKNOWN_COUNTRY
            combo = combo_key(product, ad_format)
            if country not in country_options.get(combo, []):
                continue
            experiment_group = str(row.get("experiment_group") or "").strip()
            if experiment_group not in GROUP_ORDER:
                continue
            unit_id = str(row.get("max_unit_id") or UNKNOWN_UNIT).strip() or UNKNOWN_UNIT
            cnt_type = str(row.get("cnt_type") or "").strip()
            cnt_bucket = normalize_cnt_bucket(row.get("cnt_value"))
            slice_key = (
                product,
                ad_format,
                country,
                unit_id,
                cnt_type,
                cnt_bucket,
            )
            request_pv = to_int(row.get("request_pv"))
            rounded_ecpm = to_float(row.get("rounded_ecpm"))
            totals[slice_key][experiment_group] += request_pv
            price_counts_by_slice[slice_key][rounded_ecpm][experiment_group] += request_pv

    return price_counts_by_slice, totals


def build_ecpm_distribution_and_summary_from_csv(
    path: Path,
    country_options: dict[str, list[str]],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """读取超大 eCPM CSV，按当前切片自适应生成共享 eCPM 桶与均值摘要。"""
    price_counts_by_slice, totals = load_ecpm_distribution_source(path, country_options)
    output: list[dict[str, Any]] = []
    summary_rows: list[dict[str, Any]] = []
    for slice_key, price_counts in price_counts_by_slice.items():
        product, ad_format, country, unit_id, cnt_type, cnt_bucket = slice_key
        combined_rows = [
            {
                "rounded_ecpm": price,
                "request_pv": counts[GROUP_A] + counts[GROUP_B],
            }
            for price, counts in price_counts.items()
            if counts[GROUP_A] + counts[GROUP_B] > 0
        ]
        buckets = build_adaptive_ecpm_buckets(combined_rows)
        bucket_totals: dict[tuple[str, float, float], dict[str, int]] = defaultdict(lambda: {GROUP_A: 0, GROUP_B: 0})
        weighted_sum_a = 0.0
        weighted_sum_b = 0.0
        for price, counts in price_counts.items():
            matched = assign_bucket_label(price, buckets)
            if matched is None:
                continue
            bucket_key = (matched["bucket_label"], matched["bucket_min"], matched["bucket_max"])
            bucket_totals[bucket_key][GROUP_A] += counts[GROUP_A]
            bucket_totals[bucket_key][GROUP_B] += counts[GROUP_B]
            weighted_sum_a += price * counts[GROUP_A]
            weighted_sum_b += price * counts[GROUP_B]

        total_a = totals[slice_key][GROUP_A]
        total_b = totals[slice_key][GROUP_B]
        summary_rows.append(
            {
                "product": product,
                "ad_format": ad_format,
                "country": country,
                "max_unit_id": unit_id,
                "cnt_type": cnt_type,
                "cnt_bucket": cnt_bucket,
                "mean_a": weighted_sum_a / total_a if total_a else 0.0,
                "mean_b": weighted_sum_b / total_b if total_b else 0.0,
                "mean_diff": (weighted_sum_b / total_b if total_b else 0.0) - (weighted_sum_a / total_a if total_a else 0.0),
                "total_pv_a": total_a,
                "total_pv_b": total_b,
            }
        )
        ordered_bucket_keys = sorted(bucket_totals.keys(), key=lambda item: float(item[1]))
        for bucket_label, bucket_min, bucket_max in ordered_bucket_keys:
            request_pv_a = bucket_totals[(bucket_label, bucket_min, bucket_max)][GROUP_A]
            request_pv_b = bucket_totals[(bucket_label, bucket_min, bucket_max)][GROUP_B]
            share_a = request_pv_a / total_a if total_a else 0.0
            share_b = request_pv_b / total_b if total_b else 0.0
            output.append(
                {
                    "product": product,
                    "ad_format": ad_format,
                    "country": country,
                    "max_unit_id": unit_id,
                    "cnt_type": cnt_type,
                    "cnt_bucket": cnt_bucket,
                    "category_label": bucket_label,
                    "bucket_min": bucket_min,
                    "bucket_max": bucket_max,
                    "request_pv_a": request_pv_a,
                    "request_pv_b": request_pv_b,
                    "share_a": share_a,
                    "share_b": share_b,
                    "share_diff": share_b - share_a,
                    "total_pv_a": total_a,
                    "total_pv_b": total_b,
                }
            )

    return sorted(
        output,
        key=lambda row: (
            row["product"],
            row["ad_format"],
            row["country"],
            row["max_unit_id"],
            row["cnt_type"],
            cnt_bucket_sort_key(str(row["cnt_bucket"])),
            float(row["bucket_min"]),
        ),
    ), sorted(
        summary_rows,
        key=lambda row: (
            row["product"],
            row["ad_format"],
            row["country"],
            row["max_unit_id"],
            row["cnt_type"],
            cnt_bucket_sort_key(str(row["cnt_bucket"])),
        ),
    )


def build_rank_distribution_and_summary_rows(
    rank_rows: list[dict[str, Any]],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    prepared_rows = [
        {
            **row,
            "cnt_bucket": normalize_cnt_bucket(row.get("cnt_value")),
            "success_rank": str(to_int(row.get("success_rank"))),
            "success_network_type": normalize_success_network_type(row.get("success_network_type")),
        }
        for row in rank_rows
    ]

    totals: dict[tuple[Any, ...], dict[str, int]] = defaultdict(lambda: {GROUP_A: 0, GROUP_B: 0})
    rank_counts: dict[tuple[Any, ...], dict[str, dict[str, int]]] = defaultdict(lambda: defaultdict(lambda: {GROUP_A: 0, GROUP_B: 0}))
    type_counts: dict[tuple[Any, ...], dict[str, dict[str, dict[str, int]]]] = defaultdict(
        lambda: defaultdict(lambda: {GROUP_A: {"bidding": 0, "waterfall": 0}, GROUP_B: {"bidding": 0, "waterfall": 0}})
    )
    dimension_fields = ["product", "ad_format", "country", "max_unit_id", "ad_unit_name", "cnt_type", "cnt_bucket"]

    for row in prepared_rows:
        experiment_group = str(row.get("experiment_group") or "").strip()
        if experiment_group not in GROUP_ORDER:
            continue
        dimension_key = tuple(str(row.get(field) or "").strip() for field in dimension_fields)
        category_label = str(row.get("success_rank") or "").strip()
        success_type = normalize_success_network_type(row.get("success_network_type"))
        count_value = to_int(row.get("request_pv"))
        totals[dimension_key][experiment_group] += count_value
        rank_counts[dimension_key][category_label][experiment_group] += count_value
        type_counts[dimension_key][category_label][experiment_group][success_type] += count_value

    output: list[dict[str, Any]] = []
    for dimension_key, categories in rank_counts.items():
        total_a = totals[dimension_key][GROUP_A]
        total_b = totals[dimension_key][GROUP_B]
        ordered_categories = sorted(categories.keys(), key=lambda label: to_int(label))
        for category_label in ordered_categories:
            counts = categories[category_label]
            request_pv_a = counts[GROUP_A]
            request_pv_b = counts[GROUP_B]
            share_a = request_pv_a / total_a if total_a else 0.0
            share_b = request_pv_b / total_b if total_b else 0.0
            bidding_request_pv_a = type_counts[dimension_key][category_label][GROUP_A]["bidding"]
            waterfall_request_pv_a = type_counts[dimension_key][category_label][GROUP_A]["waterfall"]
            bidding_request_pv_b = type_counts[dimension_key][category_label][GROUP_B]["bidding"]
            waterfall_request_pv_b = type_counts[dimension_key][category_label][GROUP_B]["waterfall"]
            row = {field: value for field, value in zip(dimension_fields, dimension_key, strict=False)}
            row.update(
                {
                    "category_label": category_label,
                    "request_pv_a": request_pv_a,
                    "request_pv_b": request_pv_b,
                    "share_a": share_a,
                    "share_b": share_b,
                    "share_diff": share_b - share_a,
                    "bidding_request_pv_a": bidding_request_pv_a,
                    "waterfall_request_pv_a": waterfall_request_pv_a,
                    "bidding_request_pv_b": bidding_request_pv_b,
                    "waterfall_request_pv_b": waterfall_request_pv_b,
                    "bidding_share_a": bidding_request_pv_a / total_a if total_a else 0.0,
                    "waterfall_share_a": waterfall_request_pv_a / total_a if total_a else 0.0,
                    "bidding_share_b": bidding_request_pv_b / total_b if total_b else 0.0,
                    "waterfall_share_b": waterfall_request_pv_b / total_b if total_b else 0.0,
                    "bidding_ratio_in_bar_a": bidding_request_pv_a / request_pv_a if request_pv_a else 0.0,
                    "waterfall_ratio_in_bar_a": waterfall_request_pv_a / request_pv_a if request_pv_a else 0.0,
                    "bidding_ratio_in_bar_b": bidding_request_pv_b / request_pv_b if request_pv_b else 0.0,
                    "waterfall_ratio_in_bar_b": waterfall_request_pv_b / request_pv_b if request_pv_b else 0.0,
                    "total_pv_a": total_a,
                    "total_pv_b": total_b,
                }
            )
            output.append(row)

    summary_rows = build_weighted_mean_summary_rows(
        output,
        dimension_fields=dimension_fields,
        value_field="category_label",
        count_field_a="request_pv_a",
        count_field_b="request_pv_b",
        value_parser=to_float,
    )
    return output, summary_rows


def build_ecpm_distribution_rows_from_csv(
    path: Path,
    country_options: dict[str, list[str]],
) -> list[dict[str, Any]]:
    rows, _ = build_ecpm_distribution_and_summary_from_csv(path, country_options)
    return rows


def build_payload() -> dict[str, Any]:
    unit_name_map = load_ad_unit_name_map()
    scope_rows = attach_unit_labels(load_rows(CSV_FILES["scope"]), unit_name_map)
    cnt_rows = attach_unit_labels(load_rows(CSV_FILES["cnt"]), unit_name_map)
    channel_rows = attach_unit_labels(load_rows(CSV_FILES["channel"]), unit_name_map)
    rank_rows = attach_unit_labels(load_rows(CSV_FILES["rank"]), unit_name_map)
    country_options = build_top_countries_by_combo(scope_rows)
    unit_options = build_unit_options_by_country(scope_rows, country_options)
    unit_labels = {
        str(row.get("max_unit_id") or UNKNOWN_UNIT).strip() or UNKNOWN_UNIT: str(row.get("ad_unit_name") or row.get("max_unit_id") or UNKNOWN_UNIT).strip() or UNKNOWN_UNIT
        for row in scope_rows
    }

    scope_rows = filter_rows_by_top_countries(scope_rows, country_options)
    cnt_rows = filter_rows_by_top_countries(cnt_rows, country_options)
    channel_rows = filter_rows_by_top_countries(channel_rows, country_options)
    rank_rows = filter_rows_by_top_countries(rank_rows, country_options)
    cnt_distribution_rows = build_share_distribution_rows(
        cnt_rows,
        dimension_fields=["product", "ad_format", "country", "max_unit_id", "ad_unit_name", "cnt_type"],
        category_field="cnt_value",
        count_field="success_request_cnt",
        category_transform=normalize_cnt_bucket,
        category_sort_key=cnt_bucket_sort_key,
    )
    channel_distribution_rows = build_share_distribution_rows(
        [
            {**row, "cnt_bucket": normalize_cnt_bucket(row.get("cnt_value"))}
            for row in channel_rows
        ],
        dimension_fields=["product", "ad_format", "country", "max_unit_id", "ad_unit_name", "cnt_type", "cnt_bucket"],
        category_field="success_target",
        count_field="request_pv",
        category_transform=lambda value: str(value or "").strip(),
        category_sort_key=lambda label: str(label).lower(),
    )
    rank_distribution_rows, rank_summary_rows = build_rank_distribution_and_summary_rows(rank_rows)
    ecpm_distribution_rows, ecpm_summary_rows = build_ecpm_distribution_and_summary_from_csv(CSV_FILES["ecpm"], country_options)
    for row in ecpm_distribution_rows:
        unit_id = str(row.get("max_unit_id") or UNKNOWN_UNIT).strip() or UNKNOWN_UNIT
        row["ad_unit_name"] = unit_labels.get(unit_id, unit_id)
    for row in ecpm_summary_rows:
        unit_id = str(row.get("max_unit_id") or UNKNOWN_UNIT).strip() or UNKNOWN_UNIT
        row["ad_unit_name"] = unit_labels.get(unit_id, unit_id)
    for row in rank_summary_rows:
        unit_id = str(row.get("max_unit_id") or UNKNOWN_UNIT).strip() or UNKNOWN_UNIT
        row["ad_unit_name"] = unit_labels.get(unit_id, unit_id)

    combos = sorted({combo_key(str(row.get("product") or "").strip(), str(row.get("ad_format") or "").strip()) for row in scope_rows})

    return {
        "groupLabels": GROUP_LABELS,
        "groupOrder": GROUP_ORDER,
        "combos": combos,
        "countryOptions": country_options,
        "unitOptions": unit_options,
        "unitLabels": unit_labels,
        "scopeRows": scope_rows,
        "cntRows": cnt_distribution_rows,
        "channelRows": channel_distribution_rows,
        "rankRows": rank_distribution_rows,
        "rankSummaryRows": rank_summary_rows,
        "ecpmBucketRows": ecpm_distribution_rows,
        "ecpmSummaryRows": ecpm_summary_rows,
    }


def render_html(payload: dict[str, Any]) -> str:
    """生成独立 HTML，默认做 A/B 双序列对比。"""
    payload_json = json.dumps(payload, ensure_ascii=False)
    return f"""<!DOCTYPE html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>成功 Request 分层分析</title>
  <script src="{ASSET_SCRIPT_PATH}"></script>
  <style>
    :root {{
      --bg: #f4efe6;
      --panel: #fffaf1;
      --line: #d7c5a7;
      --text: #30261a;
      --muted: #6d604c;
      --shadow: 0 16px 40px rgba(71, 51, 28, 0.08);
    }}
    * {{ box-sizing: border-box; }}
    body {{
      margin: 0;
      font-family: "Segoe UI", "PingFang SC", sans-serif;
      background:
        radial-gradient(circle at top left, rgba(210, 122, 44, 0.12), transparent 26%),
        radial-gradient(circle at top right, rgba(31, 111, 178, 0.12), transparent 24%),
        var(--bg);
      color: var(--text);
    }}
    .page {{ max-width: 1440px; margin: 0 auto; padding: 24px; overflow-x: hidden; }}
    .hero,
    .control,
    .card {{ background: var(--panel); border: 1px solid var(--line); border-radius: 20px; box-shadow: var(--shadow); }}
    .hero {{ padding: 24px 28px; }}
    .hero h1 {{ margin: 0 0 10px; font-size: 32px; }}
    .hero p {{ margin: 0; color: var(--muted); line-height: 1.7; }}
    .controls {{ margin-top: 20px; display: grid; grid-template-columns: repeat(5, minmax(0, 1fr)); gap: 12px; }}
    .control {{ padding: 14px; }}
    .control label {{ display: block; margin-bottom: 6px; font-size: 12px; letter-spacing: 0.08em; color: var(--muted); text-transform: uppercase; }}
    .control select {{ width: 100%; border: 1px solid #ccb893; border-radius: 12px; padding: 10px 12px; background: #fff; color: var(--text); }}
    .kpi-grid {{ margin-top: 20px; display: grid; grid-template-columns: repeat(4, minmax(0, 1fr)); gap: 12px; }}
    .card {{ padding: 16px 18px; }}
    .card h3 {{ margin: 0; font-size: 12px; color: var(--muted); letter-spacing: 0.08em; text-transform: uppercase; }}
    .metric {{ margin-top: 10px; font-size: 28px; font-weight: 700; }}
    .submetric {{ margin-top: 8px; color: var(--muted); font-size: 13px; }}
    .section {{ margin-top: 22px; display: grid; grid-template-columns: repeat(2, minmax(0, 1fr)); gap: 14px; }}
    .wide {{ grid-column: 1 / -1; }}
    .section-title {{ margin: 0 0 12px; font-size: 18px; }}
    .summary-note {{ margin: -4px 0 12px; color: var(--muted); font-size: 13px; line-height: 1.6; }}
    .chart {{ width: 100%; height: 360px; }}
    .chart-scroll {{ overflow-x: auto; overflow-y: hidden; }}
    .chart-scroll .chart {{ min-width: 720px; }}
    @media (max-width: 1080px) {{
      .controls, .kpi-grid, .section {{ grid-template-columns: 1fr; }}
      .chart-scroll .chart {{ min-width: 640px; }}
    }}
  </style>
</head>
<body>
  <div class="page">
    <section class="hero">
      <h1>成功 Request 分层分析</h1>
      <p>固定时间窗口 2026-01-05 到 2026-01-12。页面只纳入最终有成功的 request；network / placement cnt 都按 success + fail + not attempted 计算，success rank 只按 success + fail 计算。图表默认并排对比 A / B 两组。</p>
    </section>
    <section class="controls">
      <div class="control"><label>Product</label><select id="productSelect"></select></div>
      <div class="control"><label>Ad Format</label><select id="formatSelect"></select></div>
      <div class="control"><label>Country</label><select id="countrySelect"></select></div>
      <div class="control"><label>Unit</label><select id="unitSelect"></select></div>
      <div class="control"><label>CNT Type</label><select id="cntTypeSelect"></select></div>
    </section>
    <section class="controls">
      <div class="control"><label>CNT Bucket</label><select id="cntValueSelect"></select></div>
    </section>
    <section class="kpi-grid">
      <div class="card"><h3>Country 占总成功比</h3><div class="metric" id="countryShareMetric">-</div><div class="submetric" id="countryShareSubmetric">-</div></div>
      <div class="card"><h3>Unit 占 Country 成功比</h3><div class="metric" id="unitShareMetric">-</div><div class="submetric" id="unitShareSubmetric">-</div></div>
      <div class="card"><h3>当前 Unit 成功 Request</h3><div class="metric" id="unitSuccessMetric">-</div><div class="submetric">A / B 合并后</div></div>
      <div class="card"><h3>当前 CNT Bucket 成功 Request</h3><div class="metric" id="bucketSuccessMetric">-</div><div class="submetric">A / B 合并后</div></div>
    </section>
    <section class="section">
      <div class="card">
        <h2 class="section-title">Network CNT 分布</h2>
        <div class="chart-scroll"><div id="networkCntChart" class="chart"></div></div>
      </div>
      <div class="card">
        <h2 class="section-title">Placement CNT 分布</h2>
        <div class="chart-scroll"><div id="placementCntChart" class="chart"></div></div>
      </div>
      <div class="card wide">
        <h2 class="section-title">成功 eCPM 分布</h2>
        <div class="summary-note" id="ecpmSummary">-</div>
        <div class="chart-scroll"><div id="ecpmChart" class="chart"></div></div>
      </div>
      <div class="card wide">
        <h2 class="section-title">成功渠道分布</h2>
        <div class="chart-scroll"><div id="channelChart" class="chart"></div></div>
      </div>
      <div class="card wide">
        <h2 class="section-title">Success Rank 分布</h2>
        <div class="summary-note" id="rankSummary">-</div>
        <div class="chart-scroll"><div id="rankChart" class="chart"></div></div>
      </div>
    </section>
  </div>
  <script>
    const payload = {payload_json};
    const GROUP_ORDER = payload.groupOrder;
    const GROUP_LABELS = payload.groupLabels;
    const charts = {{
      network: echarts.init(document.getElementById('networkCntChart')),
      placement: echarts.init(document.getElementById('placementCntChart')),
      ecpm: echarts.init(document.getElementById('ecpmChart')),
      channel: echarts.init(document.getElementById('channelChart')),
      rank: echarts.init(document.getElementById('rankChart')),
    }};
    const state = {{ product: '', adFormat: '', country: '', unitId: '', cntType: 'network', cntValue: '' }};
    function parseCombo(combo) {{
      const [product, adFormat] = combo.split('__');
      return {{ product, adFormat }};
    }}
    function comboKey(product, adFormat) {{ return `${{product}}__${{adFormat}}`; }}
    function sumValue(rows, field) {{ return rows.reduce((total, row) => total + Number(row[field] || 0), 0); }}
    function uniqueSorted(values, numeric = false) {{
      return [...new Set(values)].sort((a, b) => numeric ? Number(a) - Number(b) : String(a).localeCompare(String(b)));
    }}
    function orderedUnique(values) {{
      const result = [];
      const seen = new Set();
      values.forEach((value) => {{
        const key = String(value);
        if (!seen.has(key)) {{
          seen.add(key);
          result.push(value);
        }}
      }});
      return result;
    }}
    function fillSelect(select, options, value, labelMap = null) {{
      select.innerHTML = '';
      options.forEach((optionValue) => {{
        const option = document.createElement('option');
        option.value = optionValue;
        option.textContent = labelMap?.[optionValue] || optionValue;
        select.appendChild(option);
      }});
      if (options.includes(value)) {{
        select.value = value;
      }} else if (options.length) {{
        select.value = options[0];
      }}
    }}
    function getScopeRows() {{
      return payload.scopeRows.filter((row) => row.product === state.product && row.ad_format === state.adFormat);
    }}
    function getCurrentComboKey() {{ return comboKey(state.product, state.adFormat); }}
    function getCountryRows() {{ return getScopeRows().filter((row) => row.country === state.country); }}
    function getUnitRows() {{ return getCountryRows().filter((row) => row.max_unit_id === state.unitId); }}
    function getUnitLabel() {{
      return payload.unitLabels?.[state.unitId] || state.unitId;
    }}
    function fmtPct(value) {{
      return `${{(Number(value || 0) * 100).toFixed(2)}}%`;
    }}
    function fmtNum(value) {{
      return Number(value || 0).toLocaleString();
    }}
    function fmtMean(value) {{
      return Number(value || 0).toFixed(2);
    }}
    function buildMeanSummary(summary, label) {{
      if (!summary) {{
        return `${{label}}均值：-`;
      }}
      return [
        `A均值 ${{fmtMean(summary.mean_a)}}`,
        `B均值 ${{fmtMean(summary.mean_b)}}`,
        `差异(B-A) ${{fmtMean(summary.mean_diff)}}`,
      ].join(' ｜ ');
    }}
    function buildTooltip(meta) {{
      return [
        `A: share ${{fmtPct(meta.share_a)}} / pv ${{fmtNum(meta.request_pv_a)}}`,
        `B: share ${{fmtPct(meta.share_b)}} / pv ${{fmtNum(meta.request_pv_b)}}`,
        `差异(B-A): ${{fmtPct(meta.share_diff)}}`,
      ].join('<br/>');
    }}
    function buildRankTooltip(meta) {{
      return [
        `A: share ${{fmtPct(meta.share_a)}} / pv ${{fmtNum(meta.request_pv_a)}}`,
        `A-bidding: pv ${{fmtNum(meta.bidding_request_pv_a)}} / 柱内占比 ${{fmtPct(meta.bidding_ratio_in_bar_a)}}`,
        `A-waterfall: pv ${{fmtNum(meta.waterfall_request_pv_a)}} / 柱内占比 ${{fmtPct(meta.waterfall_ratio_in_bar_a)}}`,
        `B: share ${{fmtPct(meta.share_b)}} / pv ${{fmtNum(meta.request_pv_b)}}`,
        `B-bidding: pv ${{fmtNum(meta.bidding_request_pv_b)}} / 柱内占比 ${{fmtPct(meta.bidding_ratio_in_bar_b)}}`,
        `B-waterfall: pv ${{fmtNum(meta.waterfall_request_pv_b)}} / 柱内占比 ${{fmtPct(meta.waterfall_ratio_in_bar_b)}}`,
        `差异(B-A): ${{fmtPct(meta.share_diff)}}`,
      ].join('<br/>');
    }}
    function buildSeries(rows) {{
      const categories = orderedUnique(rows.map((row) => row.category_label));
      const rowByCategory = Object.fromEntries(rows.map((row) => [String(row.category_label), row]));
      const buildPoint = (category, side) => {{
        const meta = rowByCategory[String(category)] || {{
          category_label: category,
          request_pv_a: 0,
          request_pv_b: 0,
          share_a: 0,
          share_b: 0,
          share_diff: 0,
        }};
        return {{
          value: side === 'a' ? Number(meta.share_a || 0) : Number(meta.share_b || 0),
          meta,
        }};
      }};
      return {{
        categories,
        series: [
          {{
            name: GROUP_LABELS.no_is_adx,
            type: 'bar',
            barMaxWidth: 28,
            data: categories.map((category) => buildPoint(category, 'a')),
          }},
            {{
              name: GROUP_LABELS.have_is_adx,
              type: 'bar',
              barMaxWidth: 28,
              data: categories.map((category) => buildPoint(category, 'b')),
            }},
        ],
      }};
    }}
    function buildRankSeries(rows) {{
      const categories = orderedUnique(rows.map((row) => row.category_label));
      const rowByCategory = Object.fromEntries(rows.map((row) => [String(row.category_label), row]));
      const defaultMeta = (category) => {{
        const meta = rowByCategory[String(category)] || {{
          category_label: category,
          request_pv_a: 0,
          request_pv_b: 0,
          share_a: 0,
          share_b: 0,
          share_diff: 0,
          bidding_request_pv_a: 0,
          waterfall_request_pv_a: 0,
          bidding_request_pv_b: 0,
          waterfall_request_pv_b: 0,
          bidding_share_a: 0,
          waterfall_share_a: 0,
          bidding_share_b: 0,
          waterfall_share_b: 0,
          bidding_ratio_in_bar_a: 0,
          waterfall_ratio_in_bar_a: 0,
          bidding_ratio_in_bar_b: 0,
          waterfall_ratio_in_bar_b: 0,
        }};
        return meta;
      }};
      const buildPoint = (category, field) => {{
        const meta = defaultMeta(category);
        return {{
          value: Number(meta[field] || 0),
          meta,
        }};
      }};
      return {{
        categories,
        series: [
          {{
            name: 'A-bidding',
            type: 'bar',
            stack: 'A',
            barMaxWidth: 28,
            data: categories.map((category) => buildPoint(category, 'bidding_share_a')),
          }},
          {{
            name: 'A-waterfall',
            type: 'bar',
            stack: 'A',
            barMaxWidth: 28,
            data: categories.map((category) => buildPoint(category, 'waterfall_share_a')),
          }},
          {{
            name: 'B-bidding',
            type: 'bar',
            stack: 'B',
            barMaxWidth: 28,
            data: categories.map((category) => buildPoint(category, 'bidding_share_b')),
          }},
          {{
            name: 'B-waterfall',
            type: 'bar',
            stack: 'B',
            barMaxWidth: 28,
            data: categories.map((category) => buildPoint(category, 'waterfall_share_b')),
          }},
        ],
      }};
    }}
    function renderEmptyChart(chart, title) {{
      chart.setOption({{
        title: {{ text: title, left: 0, textStyle: {{ fontSize: 14, fontWeight: 600 }} }},
        xAxis: {{ type: 'category', data: [] }},
        yAxis: {{ type: 'value' }},
        series: [],
        graphic: {{
          type: 'text',
          left: 'center',
          top: 'middle',
          style: {{ text: '当前筛选条件下暂无结果', fill: '#6d604c', fontSize: 14 }},
        }},
      }}, true);
    }}
    function renderComparisonChart(chart, title, rows) {{
      if (!rows.length) {{
        renderEmptyChart(chart, title);
        return;
      }}
      const {{ categories, series }} = buildSeries(rows);
      chart.setOption({{
        animationDuration: 300,
        title: {{ text: title, left: 0, textStyle: {{ fontSize: 14, fontWeight: 600 }} }},
        tooltip: {{
          trigger: 'axis',
          formatter: (params) => {{
            const meta = params?.[0]?.data?.meta || params?.[1]?.data?.meta;
            return [`<strong>${{params?.[0]?.axisValue || ''}}</strong>`, buildTooltip(meta)].join('<br/>');
          }},
        }},
        legend: {{ top: 0, right: 0 }},
        grid: {{ left: 60, right: 20, top: 48, bottom: 72 }},
        xAxis: {{ type: 'category', data: categories, axisLabel: {{ interval: 0, rotate: categories.length > 10 ? 35 : 0 }} }},
        yAxis: {{
          type: 'value',
          axisLabel: {{
            formatter: (value) => `${{(Number(value) * 100).toFixed(0)}}%`,
          }},
        }},
        series,
        color: ['#1f6fb2', '#d27a2c'],
      }}, true);
    }}
    function renderRankStackChart(chart, title, rows) {{
      if (!rows.length) {{
        renderEmptyChart(chart, title);
        return;
      }}
      const {{ categories, series }} = buildRankSeries(rows);
      chart.setOption({{
        animationDuration: 300,
        title: {{ text: title, left: 0, textStyle: {{ fontSize: 14, fontWeight: 600 }} }},
        tooltip: {{
          trigger: 'axis',
          formatter: (params) => {{
            const meta = params?.[0]?.data?.meta || params?.[1]?.data?.meta || params?.[2]?.data?.meta || params?.[3]?.data?.meta;
            return [`<strong>${{params?.[0]?.axisValue || ''}}</strong>`, buildRankTooltip(meta)].join('<br/>');
          }},
        }},
        legend: {{ top: 0, right: 0 }},
        grid: {{ left: 60, right: 20, top: 48, bottom: 72 }},
        xAxis: {{ type: 'category', data: categories, axisLabel: {{ interval: 0, rotate: categories.length > 10 ? 35 : 0 }} }},
        yAxis: {{
          type: 'value',
          axisLabel: {{
            formatter: (value) => `${{(Number(value) * 100).toFixed(0)}}%`,
          }},
        }},
        series,
        color: ['#1d4ed8', '#b91c1c', '#60a5fa', '#f59e0b'],
      }}, true);
    }}
    function findSummary(rows) {{
      return rows.find((row) =>
        row.product === state.product &&
        row.ad_format === state.adFormat &&
        row.country === state.country &&
        row.max_unit_id === state.unitId &&
        row.cnt_type === state.cntType &&
        String(row.cnt_bucket) === String(state.cntValue)
      ) || null;
    }}
    function refreshSelectors() {{
      const comboOptions = payload.combos.map(parseCombo);
      fillSelect(document.getElementById('productSelect'), uniqueSorted(comboOptions.map((item) => item.product)), state.product);
      state.product = document.getElementById('productSelect').value;
      fillSelect(
        document.getElementById('formatSelect'),
        uniqueSorted(comboOptions.filter((item) => item.product === state.product).map((item) => item.adFormat)),
        state.adFormat
      );
      state.adFormat = document.getElementById('formatSelect').value;
      const combo = getCurrentComboKey();
      fillSelect(document.getElementById('countrySelect'), payload.countryOptions[combo] || [], state.country);
      state.country = document.getElementById('countrySelect').value;
      fillSelect(
        document.getElementById('unitSelect'),
        payload.unitOptions[`${{combo}}__${{state.country}}`] || [],
        state.unitId,
        payload.unitLabels || {{}}
      );
      state.unitId = document.getElementById('unitSelect').value;
      fillSelect(document.getElementById('cntTypeSelect'), ['network', 'placement'], state.cntType);
      state.cntType = document.getElementById('cntTypeSelect').value;
      fillSelect(
        document.getElementById('cntValueSelect'),
        orderedUnique(
          payload.cntRows
            .filter((row) =>
              row.product === state.product &&
              row.ad_format === state.adFormat &&
              row.country === state.country &&
              row.max_unit_id === state.unitId &&
              row.cnt_type === state.cntType
            )
            .map((row) => row.category_label)
        ),
        state.cntValue
      );
      state.cntValue = document.getElementById('cntValueSelect').value;
    }}
    function refreshKpis() {{
      const comboRows = getScopeRows();
      const countryRows = getCountryRows();
      const unitRows = getUnitRows();
      const totalSuccess = sumValue(comboRows, 'success_request_cnt');
      const countrySuccess = sumValue(countryRows, 'success_request_cnt');
      const unitSuccess = sumValue(unitRows, 'success_request_cnt');
      const bucketRows = payload.cntRows.filter((row) =>
        row.product === state.product &&
        row.ad_format === state.adFormat &&
        row.country === state.country &&
        row.max_unit_id === state.unitId &&
        row.cnt_type === state.cntType &&
        String(row.category_label) === String(state.cntValue)
      );
      const bucketSuccess = sumValue(
        bucketRows,
        'request_pv_a'
      );
      const bucketSuccessB = sumValue(bucketRows, 'request_pv_b');
      document.getElementById('countryShareMetric').textContent = totalSuccess ? `${{(countrySuccess / totalSuccess * 100).toFixed(2)}}%` : '-';
      document.getElementById('countryShareSubmetric').textContent = `${{countrySuccess.toLocaleString()}} / ${{totalSuccess.toLocaleString()}}`;
      document.getElementById('unitShareMetric').textContent = countrySuccess ? `${{(unitSuccess / countrySuccess * 100).toFixed(2)}}%` : '-';
      document.getElementById('unitShareSubmetric').textContent = `${{unitSuccess.toLocaleString()}} / ${{countrySuccess.toLocaleString()}}`;
      document.getElementById('unitSuccessMetric').textContent = unitSuccess.toLocaleString();
      document.getElementById('bucketSuccessMetric').textContent = (bucketSuccess + bucketSuccessB).toLocaleString();
    }}
    function refreshCharts() {{
      const titleSuffix = `${{state.country}} / ${{getUnitLabel()}}`;
      const unitCntRows = payload.cntRows.filter((row) =>
        row.product === state.product &&
        row.ad_format === state.adFormat &&
        row.country === state.country &&
        row.max_unit_id === state.unitId
      );
      renderComparisonChart(charts.network, `Network CNT 分布 | ${{titleSuffix}}`, unitCntRows.filter((row) => row.cnt_type === 'network'));
      renderComparisonChart(charts.placement, `Placement CNT 分布 | ${{titleSuffix}}`, unitCntRows.filter((row) => row.cnt_type === 'placement'));
      const sharedFilter = (row) =>
        row.product === state.product &&
        row.ad_format === state.adFormat &&
        row.country === state.country &&
        row.max_unit_id === state.unitId &&
        row.cnt_type === state.cntType &&
        String(row.cnt_bucket) === String(state.cntValue);
      document.getElementById('ecpmSummary').textContent = buildMeanSummary(findSummary(payload.ecpmSummaryRows || []), 'eCPM');
      document.getElementById('rankSummary').textContent = buildMeanSummary(findSummary(payload.rankSummaryRows || []), 'Success Rank');
      renderComparisonChart(charts.ecpm, `eCPM 分布 | ${{state.cntType}}=${{state.cntValue}}`, payload.ecpmBucketRows.filter(sharedFilter));
      renderComparisonChart(charts.channel, `成功渠道分布 | ${{state.cntType}}=${{state.cntValue}}`, payload.channelRows.filter(sharedFilter));
      renderRankStackChart(charts.rank, `Success Rank 分布 | ${{state.cntType}}=${{state.cntValue}}`, payload.rankRows.filter(sharedFilter));
    }}
    function update() {{
      refreshSelectors();
      refreshKpis();
      refreshCharts();
    }}
    ['productSelect', 'formatSelect', 'countrySelect', 'unitSelect', 'cntTypeSelect', 'cntValueSelect'].forEach((id) => {{
      document.getElementById(id).addEventListener('change', () => {{
        state.product = document.getElementById('productSelect').value;
        state.adFormat = document.getElementById('formatSelect').value;
        state.country = document.getElementById('countrySelect').value;
        state.unitId = document.getElementById('unitSelect').value;
        state.cntType = document.getElementById('cntTypeSelect').value;
        state.cntValue = document.getElementById('cntValueSelect').value;
        update();
      }});
    }});
    if (payload.combos.length) {{
      const first = parseCombo(payload.combos[0]);
      state.product = first.product;
      state.adFormat = first.adFormat;
      update();
    }}
    window.addEventListener('resize', () => Object.values(charts).forEach((chart) => chart.resize()));
  </script>
</body>
</html>
"""


def main() -> None:
    payload = build_payload()
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    PAYLOAD_JSON.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    DASHBOARD_HTML.write_text(render_html(payload), encoding="utf-8")
    print(f"wrote: {PAYLOAD_JSON}")
    print(f"wrote: {DASHBOARD_HTML}")


if __name__ == "__main__":
    main()
