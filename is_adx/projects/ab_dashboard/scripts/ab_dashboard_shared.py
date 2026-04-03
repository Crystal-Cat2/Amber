"""读取 AB 请求结构结果 CSV 并生成入口页与两个业务 HTML。"""

from __future__ import annotations

import argparse
import csv
import json
import math
import re
import sys
from collections import defaultdict
from pathlib import Path
from typing import Any

SCRIPT_DIR = Path(__file__).resolve().parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.append(str(SCRIPT_DIR))

from mediation_scope import load_mediation_configuration as load_mediation_report_configuration

PROJECT_DIR = Path(__file__).resolve().parents[1]
OUTPUT_DIR = PROJECT_DIR / "outputs"
ASSET_SCRIPT_PATH = "../assets/echarts.min.js"
ENTRY_HTML = OUTPUT_DIR / "ab_share_dashboard.html"
REQUEST_STRUCTURE_HTML = OUTPUT_DIR / "ab_request_structure_dashboard.html"
REQUEST_STRUCTURE_COUNTRY_HTML = OUTPUT_DIR / "ab_request_structure_country_dashboard.html"
REQUEST_STRUCTURE_UNIT_HTML = OUTPUT_DIR / "ab_request_structure_unit_dashboard.html"
COVERAGE_ANALYSIS_HTML = OUTPUT_DIR / "ab_coverage_analysis_dashboard.html"
NULL_BIDDING_HTML = OUTPUT_DIR / "ab_null_bidding_dashboard.html"
BIDDING_NETWORK_STATUS_HTML = OUTPUT_DIR / "ab_bidding_network_status_dashboard.html"
WINNING_TYPE_NETWORK_STATUS_HTML = OUTPUT_DIR / "ab_winning_type_network_status_dashboard.html"
SUCCESS_MAPPING_HTML = OUTPUT_DIR / "ab_success_mapping_dashboard.html"
SUCCESS_REQUEST_HTML = OUTPUT_DIR / "ab_success_request_dashboard.html"
FILLED_DURATION_HTML = OUTPUT_DIR / "ab_filled_duration_dashboard.html"
FILLED_DURATION_CSV = OUTPUT_DIR / "adslog_filled_duration_distribution_by_unit.csv"
ISADX_LATENCY_HTML = OUTPUT_DIR / "ab_isadx_latency_dashboard.html"
ISADX_LATENCY_CSV = OUTPUT_DIR / "isadx_latency_distribution_by_unit.csv"

DASHBOARD_PAGE_TITLES = {
    "request_structure": "请求结构分布",
    "request_structure_country": "请求结构分布（Country）",
    "request_structure_unit": "请求结构分布（Unit）",
    "coverage_analysis": "覆盖率分析",
    "filled_duration": "adslog_filled 时长分布",
    "isadx_latency": "IsAdx latency 分布差异",
}

REQUEST_STRUCTURE_CSVS = {
    "metric1": {
        "network": OUTPUT_DIR / "metric1_request_network_cnt.csv",
        "placement": OUTPUT_DIR / "metric1_request_placement_cnt.csv",
        "rank": OUTPUT_DIR / "metric1_success_rank_distribution.csv",
    },
    "metric2": {
        "network": OUTPUT_DIR / "metric2_network_type_status_cnt.csv",
        "placement": OUTPUT_DIR / "metric2_type_placement_status_cnt.csv",
    },
    "metric3": {
        "network": OUTPUT_DIR / "metric3_network_distribution.csv",
        "placement": OUTPUT_DIR / "metric3_placement_distribution.csv",
    },
    "metric4": {
        "network": OUTPUT_DIR / "metric4_cnt_level_network_distribution.csv",
        "placement": OUTPUT_DIR / "metric4_cnt_level_placement_distribution.csv",
    },
}
REQUEST_STRUCTURE_COUNTRY_CSVS = {
    "metric1": {
        "network": OUTPUT_DIR / "metric1_request_network_cnt_country.csv",
        "placement": OUTPUT_DIR / "metric1_request_placement_cnt_country.csv",
        "rank": OUTPUT_DIR / "metric1_success_rank_distribution_country.csv",
    },
    "metric2": {
        "network": OUTPUT_DIR / "metric2_network_type_status_cnt_country.csv",
        "placement": OUTPUT_DIR / "metric2_type_placement_status_cnt_country.csv",
    },
    "metric3": {
        "network": OUTPUT_DIR / "metric3_network_distribution_country.csv",
        "placement": OUTPUT_DIR / "metric3_placement_distribution_country.csv",
    },
    "metric4": {
        "network": OUTPUT_DIR / "metric4_cnt_level_network_distribution_country.csv",
        "placement": OUTPUT_DIR / "metric4_cnt_level_placement_distribution_country.csv",
    },
}
REQUEST_STRUCTURE_UNIT_CSVS = {
    "metric1": {
        "network": OUTPUT_DIR / "metric1_request_network_cnt_unit.csv",
        "placement": OUTPUT_DIR / "metric1_request_placement_cnt_unit.csv",
        "rank": OUTPUT_DIR / "metric1_success_rank_distribution_unit.csv",
    },
    "metric2": {
        "network": OUTPUT_DIR / "metric2_network_type_status_cnt_unit.csv",
        "placement": OUTPUT_DIR / "metric2_type_placement_status_cnt_unit.csv",
    },
    "metric3": {
        "network": OUTPUT_DIR / "metric3_network_distribution_unit.csv",
        "placement": OUTPUT_DIR / "metric3_placement_distribution_unit.csv",
    },
    "metric4": {
        "network": OUTPUT_DIR / "metric4_cnt_level_network_distribution_unit.csv",
        "placement": OUTPUT_DIR / "metric4_cnt_level_placement_distribution_unit.csv",
    },
}
COVERAGE_CSVS = {
    "metric1": OUTPUT_DIR / "metric1_bucket_share.csv",
    "metric2": OUTPUT_DIR / "metric2_type_coverage.csv",
    "metric3": OUTPUT_DIR / "metric3_status_coverage.csv",
    "metric4": OUTPUT_DIR / "metric4_type_status_coverage.csv",
}
NULL_BIDDING_CSV = OUTPUT_DIR / "null_bidding_request_pv_by_unit.csv"
REAL_STATUS_BIDDING_CSV = OUTPUT_DIR / "real_status_bidding_request_pv_by_unit.csv"
BIDDING_NETWORK_STATUS_CSV = OUTPUT_DIR / "bidding_network_status_share_by_unit.csv"
OVERALL_BIDDING_NETWORK_STATUS_CSV = OUTPUT_DIR / "metric5_type_network_status_total_share.csv"
WINNING_TYPE_NETWORK_STATUS_CSV = OUTPUT_DIR / "winning_type_network_status_hit_rate_by_unit.csv"
SUCCESS_NETWORK_BY_NETWORK_CNT_CSV = OUTPUT_DIR / "success_network_by_network_cnt.csv"
SUCCESS_PLACEMENT_BY_PLACEMENT_CNT_CSV = OUTPUT_DIR / "success_placement_by_placement_cnt.csv"
MEDIATION_REPORT_CSV = Path(r"D:\Downloads\mediation_report_2026-03-25_09_41_32.csv")
ALL_UNIT_OPTION_VALUE = "ALL_UNIT"

GROUP_A = "no_is_adx"
GROUP_B = "have_is_adx"
GROUP_LABELS = {GROUP_A: "A 组", GROUP_B: "B 组"}
NULL_BIDDING_GROUP_LABELS = {
    GROUP_A: "A组（no_is_adx）",
    GROUP_B: "B组（have_is_adx）",
}
REQ_LIMIT = 200
STATUS_OPTIONS = [
    "AD_LOADED",
    "FAILED_TO_LOAD",
    "AD_LOAD_NOT_ATTEMPTED",
    "AD_LOADED+FAILED_TO_LOAD",
    "AD_LOADED+AD_LOAD_NOT_ATTEMPTED",
    "FAILED_TO_LOAD+AD_LOAD_NOT_ATTEMPTED",
    "AD_LOADED+FAILED_TO_LOAD+AD_LOAD_NOT_ATTEMPTED",
]
STATUS_SINGLE_OPTIONS = ["AD_LOADED", "FAILED_TO_LOAD", "AD_LOAD_NOT_ATTEMPTED"]
STATUS_BUCKET_OPTIONS = ["AD_LOADED", "FAILED_TO_LOAD", "AD_LOAD_NOT_ATTEMPTED", "NULL"]
TYPE_OPTIONS = ["bidding", "waterfall"]
NULL_BIDDING_PLATFORM_ORDER = ["android", "ios"]
NULL_BIDDING_FORMAT_ORDER = ["interstitial", "rewarded"]
NULL_BIDDING_STATUS_OPTIONS = ["NULL", "FAILED_TO_LOAD", "AD_LOAD_NOT_ATTEMPTED"]
BIDDING_NETWORK_STATUS_PLATFORM_ORDER = ["android", "ios"]
BIDDING_NETWORK_STATUS_FORMAT_ORDER = ["interstitial", "rewarded"]
BIDDING_NETWORK_STATUS_TYPE_ORDER = ["bidding", "waterfall"]
BIDDING_NETWORK_STATUS_STATUS_ORDER = ["AD_LOADED", "FAILED_TO_LOAD", "AD_LOAD_NOT_ATTEMPTED", "NULL"]
WINNING_BIDDING_STATUS_ORDER = ["FAILED_TO_LOAD", "AD_LOAD_NOT_ATTEMPTED", "NULL"]
WINNING_WATERFALL_STATUS_ORDER = ["AD_LOADED", "FAILED_TO_LOAD", "AD_LOAD_NOT_ATTEMPTED"]
NULL_BIDDING_PLATFORM_LABELS = {"android": "Android", "ios": "iOS"}
NULL_BIDDING_TEXT = [
    "A组 = no_is_adx",
    "B组 = have_is_adx",
    "NULL = 无效竞价，包含 no bid 或低于 unit bid floor。",
    "cnt = 当前 unit 下无效竞价渠道个数；pv = 去重 request_id 的请求量。",
]
BIDDING_NETWORK_STATUS_TEXT = [
    "A组 = no_is_adx",
    "B组 = have_is_adx",
    "ALL UNIT 沿用旧总表口径；具体 unit 来自 max_unit_id 明细。",
    "理论上分 unit 汇总应接近 ALL UNIT；若存在少量差异，多为异常点位或缺失 unit 导致。",
    "颜色表示 status，实线/虚线表示组别。",
    "每个 unit 下分上下两块：上面 bidding，下面 waterfall。",
    "纵轴 = 当前 group + format + unit 下，该 type + network 在对应 status 的 pv / unit 总 request。",
]
WINNING_TYPE_NETWORK_STATUS_TEXT = [
    "A组 = no_is_adx",
    "B组 = have_is_adx",
    "胜利渠道用 type + network 定义，且要求该 request 内它是唯一 AD_LOADED。",
    "分子 = 这些胜利 request 里，其他 type + network 命中当前 status 的 request_pv；分母 = 胜利 request 总数。",
    "share 是命中率，不是互斥状态分布，因此同一渠道各状态占比相加不要求等于 100%。",
    "NULL 只统计 bidding；waterfall 不统计 NULL，以减少计算量。",
]
SUCCESS_MAPPING_HERO_TEXT = [
    "统计时间范围：2026-01-05 到 2026-01-12。",
    "A 组为 no_is_adx，表示未接入 is_adx 的对照组。",
    "B 组为 have_is_adx，表示已接入 is_adx 的实验组。",
    "先按 request 的全量 network_cnt 或 placement_cnt 分桶，再在桶内观察最终成功对象的占比。",
    "成功只认 AD_LOADED；若当前 request 没有任何成功，则记为 fail。",
    "network_cnt 沿用去重后的 type + network 个数；placement_cnt 按 placement 行数统计且不去重。",
]
FILLED_DURATION_HERO_TEXT = [
    "统计时间范围：2026-01-05 到 2026-01-12。",
    "A 组为 no_is_adx，表示未接入 is_adx 的对照组。",
    "B 组为 have_is_adx，表示已接入 is_adx 的实验组。",
    "数据源只使用 Hudi adslog_filled 事件；时长字段已确认使用 event_params.duration，并按毫秒转秒。",
    "SQL 只保留 0.01 秒粒度的原始时长聚合；页面默认分布仅用于当前展示，不固化到底层 SQL。",
    "默认展示规则：-1 表示 <0s；其余常规区间均按左闭右开处理，例如 0-3 表示 [0, 3)、27-30 表示 [27, 30)、30+ 表示 [30, +inf)。",
]
ISADX_LATENCY_HERO_TEXT = [
    "统计时间范围：2026-01-05 到 2026-01-12。",
    "A 组为 no_is_adx，表示未接入 is_adx 的对照组。",
    "B 组为 have_is_adx，表示已接入 is_adx 的实验组。",
    "数据源只使用 commercial-adx.lmh.isadx_adslog_latency_detail，且仅统计 IsAdxCustomAdapter。",
    "底层 SQL 保留 0.01 秒原始桶；页面仅在展示层折叠成更适合对比的可视化分桶。",
    "图中柱状表示两组占比，折线表示 B-A GAP；左右双轴分别展示占比与差异。",
]
SUCCESS_MAPPING_TEXT = {
    "network": [
        "统计对象：全量去重 request。",
        "先按 request 内全部 network 结构计算 network_cnt，再在该 cnt 桶里看最终成功的 network 是谁。",
        "分子：当前成功 network 或 fail 的 request_pv；分母：当前 network_cnt 桶的总 request_pv。",
        "share = request_pv / 分母；A / B 两组并排展示。",
    ],
    "placement": [
        "统计对象：全量去重 request。",
        "先按 request 内全部 placement 行数计算 placement_cnt，再在该 cnt 桶里看最终成功的 placement_id 是谁。",
        "分子：当前成功 placement 或 fail 的 request_pv；分母：当前 placement_cnt 桶的总 request_pv。",
        "share = request_pv / 分母；A / B 两组并排展示。",
    ],
}

COVERAGE_HERO_TEXT = [
    "统计时间范围：2026-01-05 到 2026-01-12。",
    "A 组为 no_is_adx，表示未接入 is_adx 的对照组。",
    "B 组为 have_is_adx，表示已接入 is_adx 的实验组。",
    "图中的第 1 次、第 2 次，直到第 200 次，表示同一用户在同一 product 和 ad_format 下，按时间顺序发生的前 200 次广告请求。",
    "本页所有指标均已先排除 TpAdxCustomAdapter，再进行统计。",
]
REQUEST_STRUCTURE_HERO_TEXT = [
    "统计时间范围：2026-01-05 到 2026-01-12。",
    "A 组为 no_is_adx，表示未接入 is_adx 的对照组。",
    "B 组为 have_is_adx，表示已接入 is_adx 的实验组。",
    "请求结构页前 4 个指标按全量去重 request 统计，不再按请求顺序拆轮次，也不限制前 200 次。",
    "metric1 先展示 network个数、placement个数，再单独下钻成功 request 在不同 cnt 桶里的 success_rank 分布；metric2/3 继续并排展示 network 与 placement；metric4 按 cnt 下钻展示渠道与 placement 明细。",
    "placement 口径里的 placement_cnt 按单次 request 内的 placement 行数统计，不去重 placement；request_pv 仍按 request_id 去重。",
    "本页所有指标均已先排除 TpAdxCustomAdapter，再进行统计。",
]
REQUEST_STRUCTURE_COUNTRY_HERO_TEXT = [
    "统计时间范围：2026-01-05 到 2026-01-12。",
    "A 组为 no_is_adx，表示未接入 is_adx 的对照组。",
    "B 组为 have_is_adx，表示已接入 is_adx 的实验组。",
    "本页只看前 4 个请求结构指标，并新增 country 维度筛选。",
    "country 直接来自 latency 明细表新增的 country 字段；空值统一归为 UNKNOWN。",
    "本页不提供全量国家聚合，只展示当前 product + ad_format 下已有的国家。",
]
REQUEST_STRUCTURE_UNIT_HERO_TEXT = [
    "统计时间范围：2026-01-05 到 2026-01-12。",
    "A 组为 no_is_adx，表示未接入 is_adx 的对照组。",
    "B 组为 have_is_adx，表示已接入 is_adx 的实验组。",
    "本页只看前 4 个请求结构指标，并新增 max_unit_id 维度筛选。",
    "SQL 只按 max_unit_id 聚合；可读 unit 名替换统一在 Python 中完成。",
    "本页不展示空 max_unit_id 的请求，只展示当前 product + ad_format 下已有的 unit。",
]
REQUEST_STRUCTURE_TEXT = {
    "metric1": [
        "统计对象：全量去重 request；network_cnt 按单次 request 内去重后的 type + network 计算，placement_cnt 按单次 request 内的 placement 行数计算且不去重。",
        "分子：落在当前桶的 request_pv；分母：当前 group + product + ad_format 的总 request_pv；request_pv 统一按 request_id 去重。",
        "share = request_pv / 分母；metric1 保留 network个数、placement个数两张总桶图，并新增成功 request 的 success_rank 下钻块；tooltip 展示 pv、share、分母。",
        "用途：直接对比 network 口径与 placement 口径下，结构主要集中在哪些 total count 桶；placement 横轴固定展示 1-35，35 以上合并到 35+。",
    ],
    "metric2": [
        "统计对象：固定 total count 后的 request 桶；network个数继续按 type + network 去重，placement个数按 placement 行数统计且不去重。",
        "分子：当前 Bx+Wy 组合的 request_pv；分母：当前 total count 桶的总 request_pv；横轴使用 Bx+Wy，B=bidding，W=waterfall。",
        "share = request_pv / 分母。",
        "用途：在同一 total count 下对比 network 与 placement 两种口径里，请求结构更偏 bidding 还是 waterfall。",
    ],
    "metric3": [
        "统计对象：固定 total_cnt + network_type + type count 后的 request 桶；type_network_cnt 按 type + network 去重，type_placement_cnt 按 placement 行数统计且不去重。",
        "分子：落到当前 status_bucket 的 request_pv；分母：当前桶的总 request_pv；request_pv 统一按 request_id 去重。",
        "share = request_pv / 分母；network 口径沿用现有 status 逻辑；placement 口径不做优先级归并，同一 request 可同时命中多个 placement status。",
        "用途：继续下钻看 network 与 placement 两侧分别是哪种 status 推动了结构差异。",
    ],
    "metric4": [
        "统计对象：固定 total count 后，再固定 Bx+Wy 结构组合与 status 的 request 桶；上半块看 B-/W-network，下半块看 placement_id。",
        "分子：当前桶内命中过该 status + target 的 request_pv；分母：当前 Bx+Wy 结构桶总 request_pv；request_pv 统一按 request_id 去重。",
        "network 横轴显示为 B-/W-network，用于区分 bidding 与 waterfall 下的同名渠道；placement 横轴显示 placement_id。",
        "同一 request 可命中多个 status，也可在同一 status 下命中多个 target，因此 share 加总可能大于 100%，这是正常现象。",
    ],
    "metric5": [
        "统计对象：全量 request 中，bidding 里 AD_LOADED 去重 network_cnt = 1 的 request。",
        "分子：其余 bidding-network 在当前 status + network 下命中的 request_pv。",
        "分母：满足筛选条件的总 request_pv。",
        "多状态：同一 request + network 可同时命中多个 status，彼此独立计数。",
    ],
    "metric6": [
        "统计对象：当前 group + product + ad_format 下的全量 request。",
        "分子：当前 type + network 落到各 status_bucket 的 request_pv。",
        "分母：全部 request_pv；切到“不考虑 NULL”时，分母改成三种真实状态 pv 之和。",
        "NULL 与多状态：NULL 单独展示；多状态按 SQL 当前产出直接展示，不再额外改口径。",
    ],
}
COVERAGE_TEXT = {
    "metric1": [
        "统计对象：单次广告请求。",
        "统计内容：先看每个请求轮次下，不同 network_cnt 桶的请求量与桶占比。",
        "横轴仍然是 req_index，图中的 1、2、3 等桶位表示该次请求内命中的去重 network 数量。",
        "同一个请求内会先按 network 去重，因此这里的 network_cnt 不按 placement 数量计算。",
        "该指标用于判断某个 req_index 下，大盘主要集中在哪些 network_cnt 桶。",
    ],
    "metric2": [
        "统计对象：固定 req_index 与 network_cnt 后的请求桶。",
        "统计内容：固定 req_index 和 network_cnt 后，观察 bidding 与 waterfall 在当前请求桶中的覆盖率。",
        "覆盖率的分母始终是当前 req_index + network_cnt 桶的请求总数。",
        "同一个请求可能同时命中 bidding 和 waterfall，因此两类覆盖率相加可能大于 100%，这是正常现象。",
        "该指标用于判断当前桶里的结构更偏 bidding，还是更偏 waterfall。",
    ],
    "metric3": [
        "统计对象：固定 req_index 与 network_cnt 后的请求桶。",
        "统计内容：固定 req_index 和 network_cnt 后，观察三种状态在当前请求桶中的覆盖率。",
        "覆盖率的分母始终是当前 req_index + network_cnt 桶的请求总数。",
        "同一个请求可能同时命中多个 status，因此各状态覆盖率相加可能大于 100%，这是正常现象。",
        "该指标用于判断当前桶里主要命中了哪些状态，以及问题量级是否值得继续追查。",
    ],
    "metric4": [
        "统计对象：固定 req_index 与 network_cnt 后的请求桶。",
        "统计内容：先拆成 bidding 与 waterfall 两个块，再分别观察三种状态在当前 network_type 内的覆盖率。",
        "覆盖率的分母不再是整体桶请求数，而是当前 req_index + network_cnt 桶内命中过该 network_type 的请求数。",
        "同一个请求在同一个 network_type 内仍可能同时命中多个 status，因此各状态覆盖率相加可能大于 100%，这是正常现象。",
        "该指标用于判断 bidding 和 waterfall 内部，各状态的结构差异分别由哪一侧驱动。",
    ],
}


def load_rows(path: Path) -> list[dict[str, Any]]:
    with path.open("r", encoding="utf-8-sig", newline="") as file_obj:
        return list(csv.DictReader(file_obj))


def load_optional_rows(path: Path) -> list[dict[str, Any]]:
    return load_rows(path) if path.exists() else []


def load_ad_unit_name_map() -> dict[str, str]:
    if not MEDIATION_REPORT_CSV.exists():
        return {}
    mapping: dict[str, str] = {}
    with MEDIATION_REPORT_CSV.open("r", encoding="utf-8-sig", newline="") as file_obj:
        for row in csv.DictReader(file_obj):
            raw_name = str(row.get("Ad Unit Name") or "").strip()
            match = re.match(r"^(.*)\(([0-9a-fA-F]+)\)\s*$", raw_name)
            if not match:
                continue
            label = match.group(1).strip()
            unit_id = match.group(2).strip()
            if label and unit_id:
                mapping[unit_id] = label
    return mapping


def infer_platform(product: str) -> str:
    return "ios" if str(product).startswith("ios.") else "android"


def normalize_country(row: dict[str, Any]) -> str:
    return str(row.get("country") or "").strip()


def normalize_max_unit_id(row: dict[str, Any]) -> str:
    return str(row.get("max_unit_id") or "").strip()


SUCCESS_SCOPE_ORDER = ["all", "has_success", "no_success"]
SUCCESS_SCOPE_LABELS = {
    "all": "全部",
    "has_success": "有成功",
    "no_success": "无成功",
}


def normalize_success_scope(row: dict[str, Any]) -> str:
    return str(row.get("success_scope") or "").strip()


def rewrite_rows_with_unit_label(
    rows: list[dict[str, Any]],
    unit_name_map: dict[str, str],
) -> list[dict[str, Any]]:
    rewritten_rows: list[dict[str, Any]] = []
    for row in rows:
        max_unit_id = normalize_max_unit_id(row)
        if not max_unit_id:
            continue
        current = dict(row)
        current["country"] = unit_name_map.get(max_unit_id, max_unit_id)
        rewritten_rows.append(current)
    return rewritten_rows


def build_combo_key(product: str, ad_format: str, country: str = "", success_scope: str = "") -> str:
    parts = [product, ad_format]
    if country:
        parts.append(country)
    if success_scope:
        parts.append(success_scope)
    return "__".join(parts)


def iter_combo_dimensions(
    rows: list[dict[str, Any]],
    *,
    include_success_scope: bool = False,
) -> list[tuple[str, str, str, str]]:
    return sorted(
        {
            (
                str(row.get("product") or ""),
                str(row.get("ad_format") or ""),
                normalize_country(row),
                normalize_success_scope(row) if include_success_scope else "",
            )
            for row in rows
        }
    )


def unit_sort_key(unit: str) -> tuple[int, Any]:
    lowered = str(unit).strip().lower()
    match = re.search(r"\bp\s*(\d+)\b", lowered)
    if match:
        return (0, int(match.group(1)))
    if "df" in lowered:
        return (1, lowered)
    return (2, lowered)


def build_null_bidding_payload(
    null_rows: list[dict[str, Any]],
    real_status_rows: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    unit_name_by_id: dict[str, str] = {}
    for row in list(null_rows) + list(real_status_rows or []):
        max_unit_id = str(row.get("max_unit_id") or "").strip()
        unit_name = str(row.get("ad_unit_name") or "").strip()
        if max_unit_id and unit_name:
            unit_name_by_id[max_unit_id] = unit_name

    merged_rows: list[dict[str, Any]] = []
    for row in null_rows:
        current = dict(row)
        current["status_bucket"] = str(row.get("status_bucket") or "NULL").strip() or "NULL"
        merged_rows.append(current)
    for row in real_status_rows or []:
        current = dict(row)
        max_unit_id = str(current.get("max_unit_id") or "").strip()
        if not str(current.get("ad_unit_name") or "").strip() and max_unit_id in unit_name_by_id:
            current["ad_unit_name"] = unit_name_by_id[max_unit_id]
        merged_rows.append(current)

    grouped_rows: dict[tuple[str, str, str, str, str, str], dict[str, Any]] = {}
    unit_request_totals: dict[tuple[str, str, str, str, str], float] = {}
    for row in merged_rows:
        ad_format = str(row.get("ad_format") or "").strip().lower()
        if ad_format not in NULL_BIDDING_FORMAT_ORDER:
            continue
        status_bucket = str(row.get("status_bucket") or "NULL").strip().upper()
        if status_bucket not in NULL_BIDDING_STATUS_OPTIONS:
            continue
        experiment_group = str(row.get("experiment_group") or "").strip()
        if experiment_group not in (GROUP_A, GROUP_B):
            continue
        unit_label = str(row.get("ad_unit_name") or row.get("max_unit_id") or "").strip()
        if not unit_label:
            continue
        bidding_cnt = str(row.get("bidding_cnt") or "").strip()
        if not bidding_cnt:
            continue
        payload = {
            "unit": unit_label,
            "max_unit_id": str(row.get("max_unit_id") or "").strip(),
            "share": float(row.get("share") or 0),
            "request_pv": float(row.get("request_pv") or 0),
            "bidding_cnt": float(row.get("bidding_cnt") or 0),
            "denominator_request_pv": float(row.get("denominator_request_pv") or 0),
        }
        key = (
            infer_platform(str(row.get("product") or "")),
            ad_format,
            status_bucket,
            unit_label,
            experiment_group,
            bidding_cnt,
        )
        previous = grouped_rows.get(key)
        previous_rank = (
            float(previous["share"]),
            float(previous["request_pv"]),
        ) if previous else (-1.0, -1.0)
        current_rank = (payload["share"], payload["request_pv"])
        if current_rank > previous_rank:
            grouped_rows[key] = payload
        unit_total_key = (
            infer_platform(str(row.get("product") or "")),
            ad_format,
            status_bucket,
            unit_label,
            experiment_group,
        )
        unit_request_totals[unit_total_key] = max(
            unit_request_totals.get(unit_total_key, 0.0),
            float(row.get("denominator_request_pv") or 0),
        )

    platforms: dict[str, Any] = {}
    for platform_key in NULL_BIDDING_PLATFORM_ORDER:
        formats: dict[str, Any] = {}
        for ad_format in NULL_BIDDING_FORMAT_ORDER:
            units = sorted(
                {
                    key[3]
                    for key in grouped_rows
                    if key[0] == platform_key and key[1] == ad_format
                },
                key=unit_sort_key,
            )
            status_map: dict[str, Any] = {}
            for status_bucket in NULL_BIDDING_STATUS_OPTIONS:
                series_keys = sort_num_str(
                    {
                        key[5]
                        for key in grouped_rows
                        if key[0] == platform_key and key[1] == ad_format and key[2] == status_bucket
                    }
                )
                groups = {}
                axis_max = 0.0
                for group in (GROUP_A, GROUP_B):
                    points = []
                    for unit in units:
                        series = {}
                        total_share = 0.0
                        for series_key in series_keys:
                            current = grouped_rows.get((platform_key, ad_format, status_bucket, unit, group, series_key))
                            point = current or {
                                "unit": unit,
                                "max_unit_id": "",
                                "share": 0.0,
                                "request_pv": 0.0,
                                "bidding_cnt": float(series_key),
                                "denominator_request_pv": 0.0,
                            }
                            total_share += float(point["share"])
                            series[series_key] = point
                        axis_max = max(axis_max, total_share)
                        points.append({"unit": unit, "series": series})
                    pie_items = []
                    pie_total_request_pv = 0.0
                    for unit in units:
                        request_total = unit_request_totals.get((platform_key, ad_format, status_bucket, unit, group), 0.0)
                        if request_total <= 0:
                            continue
                        pie_total_request_pv += request_total
                        pie_items.append({"unit": unit, "request_pv": request_total})
                    for item in pie_items:
                        item["share"] = (item["request_pv"] / pie_total_request_pv) if pie_total_request_pv else 0.0
                    groups[group] = {
                        "points": points if series_keys else [],
                        "pie": {
                            "items": pie_items,
                            "total_request_pv": pie_total_request_pv,
                        },
                    }
                status_map[status_bucket] = {
                    "empty": not units or not series_keys,
                    "series_keys": series_keys,
                    "axis_max": axis_max if axis_max > 0 else 0.1,
                    "groups": groups,
                }
            formats[ad_format] = {
                "label": ad_format,
                "units": units,
                "status_map": status_map,
            }
        platforms[platform_key] = {"label": NULL_BIDDING_PLATFORM_LABELS[platform_key], "formats": formats}

    return {
        "title": "Null Bidding Unit Share",
        "desc": NULL_BIDDING_TEXT,
        "format_order": NULL_BIDDING_FORMAT_ORDER,
        "status_options": NULL_BIDDING_STATUS_OPTIONS,
        "platform_order": NULL_BIDDING_PLATFORM_ORDER,
        "groups": NULL_BIDDING_GROUP_LABELS,
        "platforms": platforms,
    }


def build_null_bidding_dashboard_payload() -> dict[str, Any]:
    null_rows = load_rows(NULL_BIDDING_CSV)
    real_status_rows = load_rows(REAL_STATUS_BIDDING_CSV) if REAL_STATUS_BIDDING_CSV.exists() else []
    ad_unit_name_map = load_ad_unit_name_map()
    for row in list(null_rows) + list(real_status_rows):
        max_unit_id = str(row.get("max_unit_id") or "").strip()
        if max_unit_id and not str(row.get("ad_unit_name") or "").strip() and max_unit_id in ad_unit_name_map:
            row["ad_unit_name"] = ad_unit_name_map[max_unit_id]
    return build_null_bidding_payload(null_rows, real_status_rows)


def build_bidding_network_status_payload(
    rows: list[dict[str, Any]],
    overall_rows: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    grouped_rows: dict[tuple[str, str, str, str, str, str, str, str], dict[str, Any]] = {}
    network_totals: dict[tuple[str, str, str, str, str], float] = defaultdict(float)
    unit_labels: dict[str, str] = {ALL_UNIT_OPTION_VALUE: "ALL UNIT"}
    merged_rows = list(rows)

    for row in overall_rows or []:
        merged_rows.append(
            {
                **row,
                "max_unit_id": ALL_UNIT_OPTION_VALUE,
                "ad_unit_name": "ALL UNIT",
            }
        )

    for row in merged_rows:
        experiment_group = str(row.get("experiment_group") or "").strip()
        if experiment_group not in (GROUP_A, GROUP_B):
            continue
        ad_format = str(row.get("ad_format") or "").strip().lower()
        if ad_format not in BIDDING_NETWORK_STATUS_FORMAT_ORDER:
            continue
        status_bucket = str(row.get("status_bucket") or "").strip().upper()
        if status_bucket not in BIDDING_NETWORK_STATUS_STATUS_ORDER:
            continue
        network_type = str(row.get("network_type") or "").strip().lower()
        if network_type not in BIDDING_NETWORK_STATUS_TYPE_ORDER:
            continue
        network = str(row.get("network") or "").strip()
        unit_id = str(row.get("max_unit_id") or "").strip()
        if not network or not unit_id:
            continue
        unit_label = str(row.get("ad_unit_name") or unit_id).strip()
        unit_labels[unit_id] = unit_label
        platform = infer_platform(str(row.get("product") or ""))
        payload = {
            "network": network,
            "share": float(row.get("share") or 0),
            "request_pv": float(row.get("request_pv") or 0),
            "denominator_request_pv": float(row.get("denominator_request_pv") or 0),
            "status_bucket": status_bucket,
            "network_type": network_type,
            "unit_id": unit_id,
            "unit_label": unit_label,
        }
        key = (platform, ad_format, unit_id, experiment_group, network_type, network, status_bucket, unit_label)
        grouped_rows[key] = payload

    for key, payload in grouped_rows.items():
        network_totals[(key[0], key[1], key[2], key[4], key[5])] += float(payload["request_pv"])

    def sort_unit_ids(current_unit_id: str) -> tuple[int, Any]:
        if current_unit_id == ALL_UNIT_OPTION_VALUE:
            return (-1, "")
        return unit_sort_key(unit_labels.get(current_unit_id, current_unit_id))

    platforms: dict[str, Any] = {}
    for platform_key in BIDDING_NETWORK_STATUS_PLATFORM_ORDER:
        formats: dict[str, Any] = {}
        for ad_format in BIDDING_NETWORK_STATUS_FORMAT_ORDER:
            unit_map: dict[str, Any] = {}
            format_units = sorted(
                {
                    key[2]
                    for key in grouped_rows
                    if key[0] == platform_key and key[1] == ad_format
                },
                key=sort_unit_ids,
            )
            for unit_id in format_units:
                unit_label = unit_labels.get(unit_id, unit_id)
                network_types: dict[str, Any] = {}
                axis_max = 0.0
                for network_type in BIDDING_NETWORK_STATUS_TYPE_ORDER:
                    networks = sorted(
                        {
                            key[5]
                            for key in grouped_rows
                            if key[0] == platform_key and key[1] == ad_format and key[2] == unit_id and key[4] == network_type
                        },
                        key=lambda network: (
                            -network_totals.get((platform_key, ad_format, unit_id, network_type, network), 0.0),
                            network.lower(),
                        ),
                    )
                    if not networks:
                        network_types[network_type] = {
                            "label": network_type,
                            "networks": [],
                            "groups": {},
                            "axis_max": 0.1,
                            "status_order": BIDDING_NETWORK_STATUS_STATUS_ORDER,
                            "empty": True,
                        }
                        continue
                    groups: dict[str, Any] = {}
                    type_axis_max = 0.0
                    for group in (GROUP_A, GROUP_B):
                        series: dict[str, list[dict[str, Any]]] = {}
                        for status_bucket in BIDDING_NETWORK_STATUS_STATUS_ORDER:
                            points = []
                            for network in networks:
                                current = grouped_rows.get(
                                    (platform_key, ad_format, unit_id, group, network_type, network, status_bucket, unit_label)
                                ) or {
                                    "network": network,
                                    "network_type": network_type,
                                    "share": 0.0,
                                    "request_pv": 0.0,
                                    "denominator_request_pv": 0.0,
                                    "status_bucket": status_bucket,
                                    "unit_id": unit_id,
                                    "unit_label": unit_label,
                                }
                                type_axis_max = max(type_axis_max, float(current["share"]))
                                points.append(current)
                            series[status_bucket] = points
                        groups[group] = {"series": series}
                    network_types[network_type] = {
                        "label": network_type,
                        "networks": networks,
                        "groups": groups,
                        "axis_max": type_axis_max if type_axis_max > 0 else 0.1,
                        "status_order": BIDDING_NETWORK_STATUS_STATUS_ORDER,
                        "empty": False,
                    }
                    axis_max = max(axis_max, network_types[network_type]["axis_max"])
                if all((network_types.get(nt, {}).get("empty", True) for nt in BIDDING_NETWORK_STATUS_TYPE_ORDER)):
                    continue
                unit_map[unit_id] = {
                    "label": unit_label,
                    "network_types": network_types,
                    "axis_max": axis_max if axis_max > 0 else 0.1,
                    "status_order": BIDDING_NETWORK_STATUS_STATUS_ORDER,
                }
        combos[combo_key] = {
            "product": base.get("product", ""),
            "ad_format": base.get("ad_format", ""),
            "country": base.get("country", ""),
            "max_unit_id": base.get("max_unit_id", ""),
            "success_scope": base.get("success_scope", ""),
            "network_view": network_views.get(combo_key),
            "placement_view": placement_views.get(combo_key),
        }
    return combos




def sort_num_str(values: set[str]) -> list[str]:
    return sorted(values, key=lambda value: int(value))


def sort_count_bucket_str(values: set[str]) -> list[str]:
    def bucket_sort_key(value: str) -> int:
        return int(value[:-1]) if value.endswith("+") else int(value)

    return sorted(values, key=bucket_sort_key)

def merge_request_structure_views(
    network_views: dict[str, Any],
    placement_views: dict[str, Any],
) -> dict[str, Any]:
    combo_keys = sorted(set(network_views) | set(placement_views))
    combos = {}
    for combo_key in combo_keys:
        base = network_views.get(combo_key) or placement_views.get(combo_key) or {}
        combos[combo_key] = {
            "product": base.get("product", ""),
            "ad_format": base.get("ad_format", ""),
            "country": base.get("country", ""),
            "max_unit_id": base.get("max_unit_id", ""),
            "success_scope": base.get("success_scope", ""),
            "network_view": network_views.get(combo_key),
            "placement_view": placement_views.get(combo_key),
        }
    return combos



def build_distribution_points(
    rows: list[dict[str, Any]],
    bucket_keys: list[str],
    *,
    axis_label: str,
) -> dict[str, Any]:
    values_by_bucket: dict[str, dict[str, float]] = defaultdict(
        lambda: {"request_pv": 0.0, "share": 0.0, "denominator_request_pv": 0.0}
    )
    for row in rows:
        bucket_key = str(row["bucket_key"])
        current = values_by_bucket[bucket_key]
        current["request_pv"] += float(row.get("request_pv") or 0)
        current["share"] += float(row.get("share") or 0)
        denominator = float(row.get("denominator_request_pv") or 0)
        if denominator:
            current["denominator_request_pv"] = denominator

    points = []
    for bucket_key in bucket_keys:
        payload = values_by_bucket.get(bucket_key, {"request_pv": 0.0, "share": 0.0, "denominator_request_pv": 0.0})
        points.append(
            {
                "axis_value": bucket_key,
                "axis_label": axis_label,
                "bucket_key": bucket_key,
                "request_pv": payload["request_pv"],
                "share": payload["share"],
                "denominator_request_pv": payload["denominator_request_pv"],
            }
        )
    return {"points": points}


def calc_distribution_axis_max_from_views(*views: dict[str, Any] | None) -> float:
    max_share = 0.0
    for view in views:
        if not view:
            continue
        groups = view.get("groups") or {}
        for group_key in (GROUP_A, GROUP_B):
            for point in (groups.get(group_key) or {}).get("points", []):
                max_share = max(max_share, float(point.get("share") or 0.0))
    padded = max(max_share * 1.1, 0.1)
    return min(math.ceil(padded * 20) / 20, 1.0)


def calc_stacked_axis_max(points: list[dict[str, Any]]) -> float:
    max_share = 0.0
    for point in points:
        series = point.get("series") or {}
        total_share = sum(float((payload or {}).get("share") or 0.0) for payload in series.values())
        max_share = max(max_share, total_share)
    return max(max_share, 0.1)


def build_bucket_points(rows: list[dict[str, Any]], series_keys: list[str]) -> dict[str, Any]:
    pv_by_req: dict[int, dict[str, float]] = defaultdict(lambda: defaultdict(float))
    for row in rows:
        pv_by_req[int(row["req_index"])][str(row["series_key"])] += float(row["pv_count"])

    points = []
    for req_index in range(1, REQ_LIMIT + 1):
        current = pv_by_req.get(req_index, {})
        denominator = sum(current.values())
        series = {}
        for key in series_keys:
            pv = current.get(key, 0.0)
            series[key] = {"pv_count": pv, "share": (pv / denominator) if denominator else 0.0, "denominator_pv": denominator}
        points.append({"axis_value": str(req_index), "axis_label": "req_index", "series": series})
    return {"points": points}


def build_coverage_points(rows: list[dict[str, Any]], series_keys: list[str]) -> dict[str, Any]:
    values_by_req: dict[int, dict[str, dict[str, float]]] = defaultdict(
        lambda: defaultdict(lambda: {"pv_count": 0.0, "share": 0.0, "denominator_pv": 0.0})
    )
    for row in rows:
        current = values_by_req[int(row["req_index"])][str(row["series_key"])]
        current["pv_count"] += float(row["pv_count"])
        current["share"] += float(row["coverage"])
        current["denominator_pv"] = float(row["bucket_request_pv"])

    points = []
    for req_index in range(1, REQ_LIMIT + 1):
        current = values_by_req.get(req_index, {})
        series = {}
        for key in series_keys:
            payload = current.get(key, {"pv_count": 0.0, "share": 0.0, "denominator_pv": 0.0})
            series[key] = {"pv_count": payload["pv_count"], "share": payload["share"], "denominator_pv": payload["denominator_pv"]}
        points.append({"axis_value": str(req_index), "axis_label": "req_index", "series": series})
    return {"points": points}


def normalize_metric1_bucket_key(count_field: str, raw_value: Any) -> str:
    bucket_key = str(raw_value)
    if count_field == "placement_cnt":
        if bucket_key.endswith("+"):
            return "35+"
        if int(bucket_key) > 35:
            return "35+"
    return bucket_key


def build_metric1_fixed_count_options(
    rows: list[dict[str, Any]],
    *,
    count_field: str,
    normalize_large_bucket: bool,
) -> list[str]:
    if normalize_large_bucket and count_field == "placement_cnt":
        has_tail_bucket = False
        bucket_values = set()
        for row in rows:
            raw_bucket = str(row.get(count_field) or "").strip()
            if raw_bucket.endswith("+"):
                has_tail_bucket = True
                continue
            if raw_bucket.isdigit():
                bucket_value = int(raw_bucket)
                bucket_values.add(bucket_value)
                if bucket_value > 35:
                    has_tail_bucket = True
        if not has_tail_bucket:
            return sort_count_bucket_str({str(bucket_value) for bucket_value in bucket_values})
        count_options = [str(index) for index in range(1, 36)]
        if has_tail_bucket:
            count_options.append("35+")
        return count_options
    if normalize_large_bucket:
        return sort_count_bucket_str({normalize_metric1_bucket_key(count_field, row[count_field]) for row in rows})
    return sort_count_bucket_str({str(row[count_field]) for row in rows})


def build_top_countries_by_combo(
    rows: list[dict[str, Any]],
    *,
    limit: int = 10,
) -> dict[str, list[str]]:
    totals: dict[tuple[str, str, str], float] = defaultdict(float)
    for row in rows:
        product = str(row.get("product") or "").strip()
        ad_format = str(row.get("ad_format") or "").strip()
        country = normalize_country(row)
        if not product or not ad_format or not country:
            continue
        totals[(product, ad_format, country)] += float(row.get("request_pv") or 0)

    countries_by_combo: dict[str, list[str]] = {}
    grouped: dict[tuple[str, str], list[tuple[str, float]]] = defaultdict(list)
    for (product, ad_format, country), total_request_pv in totals.items():
        grouped[(product, ad_format)].append((country, total_request_pv))

    for (product, ad_format), items in grouped.items():
        combo_key = build_combo_key(product, ad_format)
        countries_by_combo[combo_key] = [
            country
            for country, _ in sorted(items, key=lambda item: (-item[1], item[0]))[:limit]
        ]
    return countries_by_combo


def filter_rows_by_allowed_countries(
    rows: list[dict[str, Any]],
    allowed_countries_by_combo: dict[str, list[str]],
) -> list[dict[str, Any]]:
    if not allowed_countries_by_combo:
        return rows

    allowed_sets = {
        combo_key: set(country_list)
        for combo_key, country_list in allowed_countries_by_combo.items()
    }
    filtered_rows: list[dict[str, Any]] = []
    for row in rows:
        product = str(row.get("product") or "").strip()
        ad_format = str(row.get("ad_format") or "").strip()
        combo_key = build_combo_key(product, ad_format)
        allowed_countries = allowed_sets.get(combo_key)
        if not allowed_countries:
            continue
        if normalize_country(row) in allowed_countries:
            filtered_rows.append(row)
    return filtered_rows

def build_request_structure_metric1_group_points(
    rows: list[dict[str, Any]],
    *,
    count_field: str,
    count_options: list[str],
    axis_label: str,
) -> dict[str, Any]:
    values_by_bucket: dict[str, dict[str, float]] = defaultdict(
        lambda: {"request_pv": 0.0, "denominator_request_pv": 0.0}
    )
    for row in rows:
        bucket_key = normalize_metric1_bucket_key(count_field, row[count_field])
        current = values_by_bucket[bucket_key]
        current["request_pv"] += float(row.get("request_pv") or 0)
        denominator = float(row.get("denominator_request_pv") or 0)
        if denominator:
            current["denominator_request_pv"] = denominator

    aggregated_rows = []
    for bucket_key, payload in values_by_bucket.items():
        denominator = payload["denominator_request_pv"]
        request_pv = payload["request_pv"]
        aggregated_rows.append(
            {
                "bucket_key": bucket_key,
                "request_pv": request_pv,
                "share": (request_pv / denominator) if denominator else 0.0,
                "denominator_request_pv": denominator,
            }
        )

    return build_distribution_points(aggregated_rows, count_options, axis_label=axis_label)


def build_request_structure_metric1_view(
    rows: list[dict[str, Any]],
    *,
    count_field: str,
    count_label: str | None = None,
    axis_label: str | None = None,
    normalize_large_bucket: bool = False,
    include_success_scope: bool = False,
) -> dict[str, Any]:
    combos = {}
    for product, ad_format, country, success_scope in iter_combo_dimensions(
        rows,
        include_success_scope=include_success_scope,
    ):
        combo_rows = [
            row for row in rows
            if row["product"] == product
            and row["ad_format"] == ad_format
            and normalize_country(row) == country
            and normalize_success_scope(row) == success_scope
        ]
        count_options = build_metric1_fixed_count_options(
            combo_rows,
            count_field=count_field,
            normalize_large_bucket=normalize_large_bucket,
        )
        combos[build_combo_key(product, ad_format, country, success_scope)] = {
            "product": product,
            "ad_format": ad_format,
            "country": country,
            "max_unit_id": str(combo_rows[0].get("max_unit_id") or "").strip() if combo_rows else "",
            "success_scope": success_scope,
            "count_label": count_label or count_field,
            "count_options": count_options,
            "groups": {
                GROUP_A: build_request_structure_metric1_group_points(
                    [row for row in combo_rows if row["experiment_group"] == GROUP_A],
                    count_field=count_field,
                    count_options=count_options,
                    axis_label=axis_label or count_label or count_field,
                ),
                GROUP_B: build_request_structure_metric1_group_points(
                    [row for row in combo_rows if row["experiment_group"] == GROUP_B],
                    count_field=count_field,
                    count_options=count_options,
                    axis_label=axis_label or count_label or count_field,
                ),
            },
        }
    return combos


def build_metric1_rank_bucket_options(rows: list[dict[str, Any]]) -> list[str]:
    max_rank = 0
    for row in rows:
        raw_rank = str(row.get("success_rank") or "").strip()
        if raw_rank.isdigit():
            max_rank = max(max_rank, int(raw_rank))
    return [str(index) for index in range(1, max_rank + 1)]


def build_metric1_rank_group_payload(
    rows: list[dict[str, Any]],
    *,
    rank_options: list[str],
) -> dict[str, Any]:
    summary = {
        "success_request_pv": 0.0,
        "bucket_total_request_pv": 0.0,
        "success_rate": 0.0,
    }
    distribution_rows: list[dict[str, Any]] = []
    for row in rows:
        success_request_pv = float(row.get("bucket_success_request_pv") or 0.0)
        bucket_total_request_pv = float(row.get("bucket_total_request_pv") or 0.0)
        summary["success_request_pv"] = max(summary["success_request_pv"], success_request_pv)
        summary["bucket_total_request_pv"] = max(summary["bucket_total_request_pv"], bucket_total_request_pv)
        request_pv = float(row.get("request_pv") or 0.0)
        distribution_rows.append(
            {
                "bucket_key": str(row.get("success_rank") or "").strip(),
                "request_pv": request_pv,
                "share": (request_pv / success_request_pv) if success_request_pv else 0.0,
                "denominator_request_pv": success_request_pv,
            }
        )
    if summary["bucket_total_request_pv"]:
        summary["success_rate"] = summary["success_request_pv"] / summary["bucket_total_request_pv"]
    return {
        "summary": summary,
        **build_distribution_points(distribution_rows, rank_options, axis_label="success_rank"),
    }


def pick_metric1_rank_default_cnt(bucket_map: dict[str, Any]) -> str:
    if not bucket_map:
        return ""

    def sort_key(item: tuple[str, Any]) -> tuple[float, int]:
        cnt_value, payload = item
        total_bucket_request_pv = sum(
            float((((payload.get("groups") or {}).get(group_key) or {}).get("summary") or {}).get("bucket_total_request_pv") or 0.0)
            for group_key in (GROUP_A, GROUP_B)
        )
        return (-total_bucket_request_pv, int(cnt_value))

    return min(bucket_map.items(), key=sort_key)[0]


def build_request_structure_metric1_rank_view(
    rows: list[dict[str, Any]],
    *,
    cnt_type: str,
    include_success_scope: bool = False,
) -> dict[str, Any]:
    filtered_rows = [row for row in rows if str(row.get("cnt_type") or "").strip() == cnt_type]
    combos = {}
    for product, ad_format, country, success_scope in iter_combo_dimensions(
        filtered_rows,
        include_success_scope=include_success_scope,
    ):
        combo_rows = [
            row for row in filtered_rows
            if row["product"] == product
            and row["ad_format"] == ad_format
            and normalize_country(row) == country
            and normalize_success_scope(row) == success_scope
        ]
        cnt_options = sort_num_str(
            {
                str(row.get("cnt_value") or "").strip()
                for row in combo_rows
                if str(row.get("cnt_value") or "").strip()
            }
        )
        bucket_map: dict[str, Any] = {}
        for cnt_value in cnt_options:
            cnt_rows = [row for row in combo_rows if str(row.get("cnt_value") or "").strip() == cnt_value]
            rank_options = build_metric1_rank_bucket_options(cnt_rows)
            group_payloads = {
                GROUP_A: build_metric1_rank_group_payload(
                    [row for row in cnt_rows if row["experiment_group"] == GROUP_A],
                    rank_options=rank_options,
                ),
                GROUP_B: build_metric1_rank_group_payload(
                    [row for row in cnt_rows if row["experiment_group"] == GROUP_B],
                    rank_options=rank_options,
                ),
            }
            bucket_map[cnt_value] = {
                "rank_options": rank_options,
                "groups": group_payloads,
                "axis_max": calc_distribution_axis_max_from_views({"groups": group_payloads}),
            }
        combos[build_combo_key(product, ad_format, country, success_scope)] = {
            "product": product,
            "ad_format": ad_format,
            "country": country,
            "max_unit_id": str(combo_rows[0].get("max_unit_id") or "").strip() if combo_rows else "",
            "success_scope": success_scope,
            "cnt_type": cnt_type,
            "cnt_options": cnt_options,
            "default_cnt": pick_metric1_rank_default_cnt(bucket_map),
            "bucket_map": bucket_map,
        }
    return combos


def build_empty_metric1_rank_view(
    base_view: dict[str, Any] | None,
    *,
    cnt_type: str,
) -> dict[str, Any] | None:
    if not base_view or not (base_view.get("count_options") or []):
        return None
    cnt_options = list(base_view.get("count_options") or [])
    default_cnt = cnt_options[0] if cnt_options else ""
    bucket_map = {}
    for cnt_value in cnt_options:
        groups = {
            group_key: {
                "summary": {
                    "success_request_pv": 0.0,
                    "bucket_total_request_pv": 0.0,
                    "success_rate": 0.0,
                },
                **build_distribution_points([], ["1"], axis_label="success_rank"),
            }
            for group_key in (GROUP_A, GROUP_B)
        }
        bucket_map[cnt_value] = {
            "rank_options": ["1"],
            "groups": groups,
            "axis_max": calc_distribution_axis_max_from_views({"groups": groups}),
        }
    return {
        "product": base_view.get("product", ""),
        "ad_format": base_view.get("ad_format", ""),
        "country": base_view.get("country", ""),
        "max_unit_id": base_view.get("max_unit_id", ""),
        "success_scope": base_view.get("success_scope", ""),
        "cnt_type": cnt_type,
        "cnt_options": cnt_options,
        "default_cnt": default_cnt,
        "bucket_map": bucket_map,
    }


def build_request_structure_metric1(
    network_rows: list[dict[str, Any]],
    placement_rows: list[dict[str, Any]],
    rank_rows: list[dict[str, Any]] | None = None,
    *,
    normalize_placement_large_bucket: bool = True,
    include_success_scope: bool = False,
) -> dict[str, Any]:
    rank_rows = rank_rows or []
    include_success_scope = include_success_scope or any(
        normalize_success_scope(row)
        for row in network_rows + placement_rows + rank_rows
    )
    combos = merge_request_structure_views(
        build_request_structure_metric1_view(
            network_rows,
            count_field="network_cnt",
            count_label="network个数",
            include_success_scope=include_success_scope,
        ),
        build_request_structure_metric1_view(
            placement_rows,
            count_field="placement_cnt",
            count_label="placement个数",
            normalize_large_bucket=normalize_placement_large_bucket,
            include_success_scope=include_success_scope,
        ),
    )
    network_rank_views = build_request_structure_metric1_rank_view(
        rank_rows,
        cnt_type="network",
        include_success_scope=include_success_scope,
    )
    placement_rank_views = build_request_structure_metric1_rank_view(
        rank_rows,
        cnt_type="placement",
        include_success_scope=include_success_scope,
    )
    for combo in combos.values():
        combo["axis_max"] = calc_distribution_axis_max_from_views(
            combo.get("network_view"),
            combo.get("placement_view"),
        )
    for combo_key, combo in combos.items():
        network_rank_view = network_rank_views.get(combo_key) or build_empty_metric1_rank_view(
            combo.get("network_view"),
            cnt_type="network",
        )
        placement_rank_view = placement_rank_views.get(combo_key) or build_empty_metric1_rank_view(
            combo.get("placement_view"),
            cnt_type="placement",
        )
        combo["rank_block"] = {
            "network_rank_view": network_rank_view,
            "placement_rank_view": placement_rank_view,
        }
    for combo_key in sorted(set(network_rank_views) | set(placement_rank_views)):
        if combo_key not in combos:
            base_view = network_rank_views.get(combo_key) or placement_rank_views.get(combo_key) or {}
            combos[combo_key] = {
                "product": base_view.get("product", ""),
                "ad_format": base_view.get("ad_format", ""),
                "country": base_view.get("country", ""),
                "max_unit_id": base_view.get("max_unit_id", ""),
                "success_scope": base_view.get("success_scope", ""),
                "network_view": None,
                "placement_view": None,
                "axis_max": calc_distribution_axis_max_from_views(None, None),
                "rank_block": {
                    "network_rank_view": network_rank_views.get(combo_key),
                    "placement_rank_view": placement_rank_views.get(combo_key),
                },
            }
    return {
        "title": "全量 request 的 network / placement 结构分布",
        "desc": REQUEST_STRUCTURE_TEXT["metric1"],
        "rank_desc": [
            "只看成功 request 的 success_rank 分布；每张图先固定 cnt 桶，再比较 rank 占比。",
            "图内分母 = 当前 cnt 桶内成功 request_pv；右上角摘要 = 成功 request_pv / 当前 cnt 总 request_pv / 成功率。",
            "network_cnt 沿用去重后的 type + network 个数；placement_cnt 沿用 placement 行数且不去重。",
        ],
        "combos": combos,
        "chart_mode": "distribution",
    }


def build_request_structure_metric2_view(
    rows: list[dict[str, Any]],
    *,
    count_field: str,
    bidding_field: str,
    waterfall_field: str,
    include_success_scope: bool = False,
) -> dict[str, Any]:
    combos = {}
    for product, ad_format, country, success_scope in iter_combo_dimensions(
        rows,
        include_success_scope=include_success_scope,
    ):
        combo_rows = [
            row for row in rows
            if row["product"] == product
            and row["ad_format"] == ad_format
            and normalize_country(row) == country
            and normalize_success_scope(row) == success_scope
        ]
        count_options = sort_num_str({str(row[count_field]) for row in combo_rows})
        cnt_map = {}
        for count_value in count_options:
            cnt_rows = [row for row in combo_rows if str(row[count_field]) == count_value]
            axis_label = (
                "Bx+Wy"
                if any(normalize_country(row) for row in cnt_rows)
                else f"B{{{bidding_field}}}+W{{{waterfall_field}}}"
            )
            bucket_options = [
                f"B{row[bidding_field]}+W{row[waterfall_field]}"
                for row in sorted(
                    cnt_rows,
                    key=lambda current: (int(current[bidding_field]), int(current[waterfall_field])),
                )
            ]
            bucket_options = list(dict.fromkeys(bucket_options))
            cnt_map[count_value] = {
                "bucket_options": bucket_options,
                "groups": {
                    GROUP_A: build_distribution_points(
                        [
                            {
                                "bucket_key": f"B{row[bidding_field]}+W{row[waterfall_field]}",
                                "request_pv": row["request_pv"],
                                "share": row["share"],
                                "denominator_request_pv": row["denominator_request_pv"],
                            }
                            for row in cnt_rows
                            if row["experiment_group"] == GROUP_A
                        ],
                        bucket_options,
                        axis_label=axis_label,
                    ),
                    GROUP_B: build_distribution_points(
                        [
                            {
                                "bucket_key": f"B{row[bidding_field]}+W{row[waterfall_field]}",
                                "request_pv": row["request_pv"],
                                "share": row["share"],
                                "denominator_request_pv": row["denominator_request_pv"],
                            }
                            for row in cnt_rows
                            if row["experiment_group"] == GROUP_B
                        ],
                        bucket_options,
                        axis_label=axis_label,
                    ),
                },
            }
        combos[build_combo_key(product, ad_format, country, success_scope)] = {
            "product": product,
            "ad_format": ad_format,
            "country": country,
            "max_unit_id": str(combo_rows[0].get("max_unit_id") or "").strip() if combo_rows else "",
            "success_scope": success_scope,
            "count_label": count_field,
            "bucket_label": (
                "Bx+Wy"
                if any(normalize_country(row) for row in combo_rows)
                else f"B{{{bidding_field}}}+W{{{waterfall_field}}}"
            ),
            "count_options": count_options,
            "cnt_map": cnt_map,
        }
    return combos


def build_request_structure_metric3_view(
    rows: list[dict[str, Any]],
    *,
    count_field: str,
    type_count_field: str,
    include_success_scope: bool = False,
) -> dict[str, Any]:
    combos = {}
    for product, ad_format, country, success_scope in iter_combo_dimensions(
        rows,
        include_success_scope=include_success_scope,
    ):
        combo_rows = [
            row for row in rows
            if row["product"] == product
            and row["ad_format"] == ad_format
            and normalize_country(row) == country
            and normalize_success_scope(row) == success_scope
        ]
        count_options = sort_num_str({str(row[count_field]) for row in combo_rows})
        cnt_map = {}
        for count_value in count_options:
            cnt_rows = [row for row in combo_rows if str(row[count_field]) == count_value]
            network_type_options = [
                network_type
                for network_type in TYPE_OPTIONS
                if any(row["network_type"] == network_type for row in cnt_rows)
            ]
            type_map = {}
            for network_type in network_type_options:
                type_rows = [row for row in cnt_rows if row["network_type"] == network_type]
                type_count_options = sort_num_str({str(row[type_count_field]) for row in type_rows})
                type_cnt_map = {}
                for type_count in type_count_options:
                    current_rows = [row for row in type_rows if str(row[type_count_field]) == type_count]
                    status_options = [
                        status_bucket
                        for status_bucket in STATUS_SINGLE_OPTIONS
                        if any(row["status_bucket"] == status_bucket for row in current_rows)
                    ]
                    type_cnt_map[type_count] = {
                        "status_options": status_options,
                        "groups": {
                            GROUP_A: build_distribution_points(
                                [
                                    {
                                        "bucket_key": row["status_bucket"],
                                        "request_pv": row["request_pv"],
                                        "share": row["share"],
                                        "denominator_request_pv": row["denominator_request_pv"],
                                    }
                                    for row in current_rows
                                    if row["experiment_group"] == GROUP_A
                                ],
                                status_options,
                                axis_label="status_bucket",
                            ),
                            GROUP_B: build_distribution_points(
                                [
                                    {
                                        "bucket_key": row["status_bucket"],
                                        "request_pv": row["request_pv"],
                                        "share": row["share"],
                                        "denominator_request_pv": row["denominator_request_pv"],
                                    }
                                    for row in current_rows
                                    if row["experiment_group"] == GROUP_B
                                ],
                                status_options,
                                axis_label="status_bucket",
                            ),
                        },
                    }
                type_map[network_type] = {
                    "type_count_label": type_count_field,
                    "type_count_options": type_count_options,
                    "type_cnt_map": type_cnt_map,
                }
            cnt_map[count_value] = {"network_type_options": network_type_options, "type_map": type_map}
        combos[build_combo_key(product, ad_format, country, success_scope)] = {
            "product": product,
            "ad_format": ad_format,
            "country": country,
            "max_unit_id": str(combo_rows[0].get("max_unit_id") or "").strip() if combo_rows else "",
            "success_scope": success_scope,
            "count_label": count_field,
            "type_count_label": type_count_field,
            "count_options": count_options,
            "cnt_map": cnt_map,
        }
    return combos


def build_request_structure_metric2(
    network_rows: list[dict[str, Any]],
    placement_rows: list[dict[str, Any]],
    *,
    include_success_scope: bool = False,
) -> dict[str, Any]:
    include_success_scope = include_success_scope or any(
        normalize_success_scope(row) for row in network_rows + placement_rows
    )
    return {
        "title": "固定 total count 后的 network / placement 组合分布",
        "desc": REQUEST_STRUCTURE_TEXT["metric2"],
        "combos": merge_request_structure_views(
            build_request_structure_metric2_view(
                network_rows,
                count_field="network_cnt",
                bidding_field="bidding_cnt",
                waterfall_field="waterfall_cnt",
                include_success_scope=include_success_scope,
            ),
            build_request_structure_metric2_view(
                placement_rows,
                count_field="placement_cnt",
                bidding_field="bidding_placement_cnt",
                waterfall_field="waterfall_placement_cnt",
                include_success_scope=include_success_scope,
            ),
        ),
        "chart_mode": "distribution",
    }


def build_request_structure_metric3(
    network_rows: list[dict[str, Any]],
    placement_rows: list[dict[str, Any]],
    *,
    include_success_scope: bool = False,
) -> dict[str, Any]:
    include_success_scope = include_success_scope or any(
        normalize_success_scope(row) for row in network_rows + placement_rows
    )
    return {
        "title": "固定 total count 与 type count 后的 status 分布",
        "desc": REQUEST_STRUCTURE_TEXT["metric3"],
        "network_types": TYPE_OPTIONS,
        "combos": merge_request_structure_views(
            build_request_structure_metric3_view(
                network_rows,
                count_field="network_cnt",
                type_count_field="type_network_cnt",
                include_success_scope=include_success_scope,
            ),
            build_request_structure_metric3_view(
                placement_rows,
                count_field="placement_cnt",
                type_count_field="type_placement_cnt",
                include_success_scope=include_success_scope,
            ),
        ),
        "chart_mode": "distribution",
    }


def normalize_bucket_part(value: Any) -> str:
    raw = str(value or "").strip()
    if raw.endswith(".0"):
        raw = raw[:-2]
    return raw


def format_bw_bucket_label(bidding_value: Any, waterfall_value: Any) -> str:
    return f"B{normalize_bucket_part(bidding_value)}+W{normalize_bucket_part(waterfall_value)}"


def parse_bw_bucket_label(value: str) -> tuple[int, int]:
    current = str(value or "").strip()
    try:
        left, right = current.split("+", 1)
        return int(left[1:] or 0), int(right[1:] or 0)
    except (TypeError, ValueError, IndexError):
        return (10**9, 10**9)


def format_request_structure_network_target(row: dict[str, Any]) -> str:
    network_type = str(row.get("network_type") or "").strip().lower()
    network = str(row.get("network") or "").strip()
    prefix = {"bidding": "B", "waterfall": "W"}.get(network_type, network_type[:1].upper() if network_type else "")
    return f"{prefix}-{network}" if prefix and network else network


def build_request_structure_metric4_view(
    rows: list[dict[str, Any]],
    *,
    count_field: str,
    bidding_field: str,
    waterfall_field: str,
    target_field: str,
    count_label: str,
    include_success_scope: bool = False,
) -> dict[str, Any]:
    combos: dict[str, Any] = {}
    for product, ad_format, country, success_scope in iter_combo_dimensions(
        rows,
        include_success_scope=include_success_scope,
    ):
        combo_rows = [
            row
            for row in rows
            if str(row.get("product") or "") == product
            and str(row.get("ad_format") or "") == ad_format
            and normalize_country(row) == country
            and (not include_success_scope or normalize_success_scope(row) == success_scope)
        ]
        count_options = sort_num_str(
            {str(row.get(count_field) or "") for row in combo_rows if str(row.get(count_field) or "")}
        )
        cnt_map: dict[str, Any] = {}
        for count_value in count_options:
            count_rows = [row for row in combo_rows if str(row.get(count_field) or "") == count_value]
            structure_rows: dict[str, list[dict[str, Any]]] = {}
            for row in count_rows:
                structure_key = format_bw_bucket_label(row.get(bidding_field), row.get(waterfall_field))
                if not structure_key:
                    continue
                structure_rows.setdefault(structure_key, []).append(row)
            structure_options = sorted(
                structure_rows,
                key=lambda current: (
                    parse_bw_bucket_label(current)[0],
                    parse_bw_bucket_label(current)[1],
                ),
            )
            structure_map: dict[str, Any] = {}
            for structure_key in structure_options:
                current_structure_rows = structure_rows[structure_key]
                status_options = [
                    status
                    for status in STATUS_SINGLE_OPTIONS
                    if any(str(row.get("status_bucket") or "").strip() == status for row in current_structure_rows)
                ]
                status_map: dict[str, Any] = {}
                for status_bucket in status_options:
                    current_status_rows = [
                        row
                        for row in current_structure_rows
                        if str(row.get("status_bucket") or "").strip() == status_bucket
                    ]
                    target_options = sorted(
                        {
                            str(row.get(target_field) or "").strip()
                            for row in current_status_rows
                            if str(row.get(target_field) or "").strip()
                        },
                        key=lambda target: (
                            -max(
                                float(row.get("share") or 0)
                                for row in current_status_rows
                                if str(row.get(target_field) or "").strip() == target
                            ),
                            -sum(
                                float(row.get("request_pv") or 0)
                                for row in current_status_rows
                                if str(row.get(target_field) or "").strip() == target
                            ),
                            target.lower(),
                        ),
                    )
                    status_map[status_bucket] = {
                        "target_options": target_options,
                        "groups": {
                            GROUP_A: build_distribution_points(
                                [
                                    {
                                        "bucket_key": str(row.get(target_field) or "").strip(),
                                        "request_pv": row.get("request_pv"),
                                        "share": row.get("share"),
                                        "denominator_request_pv": row.get("denominator_request_pv"),
                                    }
                                    for row in current_status_rows
                                    if str(row.get("experiment_group") or "").strip() == GROUP_A
                                ],
                                target_options,
                                axis_label=target_field,
                            ),
                            GROUP_B: build_distribution_points(
                                [
                                    {
                                        "bucket_key": str(row.get(target_field) or "").strip(),
                                        "request_pv": row.get("request_pv"),
                                        "share": row.get("share"),
                                        "denominator_request_pv": row.get("denominator_request_pv"),
                                    }
                                    for row in current_status_rows
                                    if str(row.get("experiment_group") or "").strip() == GROUP_B
                                ],
                                target_options,
                                axis_label=target_field,
                            ),
                        },
                    }
                structure_map[structure_key] = {
                    "status_options": status_options,
                    "status_map": status_map,
                }
            cnt_map[count_value] = {
                "structure_options": structure_options,
                "structure_map": structure_map,
            }
        if count_options:
            combo_key = build_combo_key(product, ad_format, country, success_scope if include_success_scope else "")
            combos[combo_key] = {
                "product": product,
                "ad_format": ad_format,
                "country": country,
                "success_scope": success_scope if include_success_scope else "",
                "count_label": count_label,
                "count_options": count_options,
                "cnt_map": cnt_map,
            }
    return combos


def build_request_structure_metric4(
    network_rows: list[dict[str, Any]],
    placement_rows: list[dict[str, Any]],
    *,
    include_success_scope: bool = False,
) -> dict[str, Any]:
    include_success_scope = include_success_scope or any(
        normalize_success_scope(row) for row in network_rows + placement_rows
    )
    network_rows = [
        {
            **row,
            "target": format_request_structure_network_target(row),
        }
        for row in network_rows
        if format_request_structure_network_target(row)
    ]
    placement_rows = [
        {
            **row,
            "target": str(row.get("placement_id") or "").strip(),
        }
        for row in placement_rows
        if str(row.get("placement_id") or "").strip()
    ]
    return {
        "title": "固定 total count 后的渠道 / placement 明细分布",
        "desc": REQUEST_STRUCTURE_TEXT["metric4"],
        "combos": merge_request_structure_views(
            build_request_structure_metric4_view(
                network_rows,
                count_field="network_cnt",
                bidding_field="bidding_cnt",
                waterfall_field="waterfall_cnt",
                target_field="target",
                count_label="network_cnt",
                include_success_scope=include_success_scope,
            ),
            build_request_structure_metric4_view(
                placement_rows,
                count_field="placement_cnt",
                bidding_field="bidding_placement_cnt",
                waterfall_field="waterfall_placement_cnt",
                target_field="target",
                count_label="placement_cnt",
                include_success_scope=include_success_scope,
            ),
        ),
        "chart_mode": "distribution",
    }


def build_request_structure_metric5_table(rows: list[dict[str, Any]]) -> dict[str, Any]:
    combos = {}
    status_order = {status: index for index, status in enumerate(STATUS_SINGLE_OPTIONS)}
    for product in sorted({row["product"] for row in rows}):
        for ad_format in sorted({row["ad_format"] for row in rows}):
            combo_rows = [row for row in rows if row["product"] == product and row["ad_format"] == ad_format]
            if not combo_rows:
                continue
            denominator_by_group = {
                group: float(
                    next(
                        (
                            row["denominator_request_pv"]
                            for row in combo_rows
                            if row["experiment_group"] == group and str(row.get("denominator_request_pv", "")) != ""
                        ),
                        0,
                    )
                    or 0
                )
                for group in (GROUP_A, GROUP_B)
            }
            grouped_rows: dict[tuple[str, str], dict[str, Any]] = {}
            for row in combo_rows:
                key = (row["status"], row["network"])
                if key not in grouped_rows:
                    grouped_rows[key] = {
                        "status": row["status"],
                        "network": row["network"],
                        "groups": {
                            GROUP_A: {"request_pv": 0.0, "share": 0.0},
                            GROUP_B: {"request_pv": 0.0, "share": 0.0},
                        },
                    }
                grouped_rows[key]["groups"][row["experiment_group"]] = {
                    "request_pv": float(row["request_pv"]),
                    "share": float(row["share"]),
                }
            rows_payload = sorted(
                grouped_rows.values(),
                key=lambda item: (status_order.get(item["status"], 99), item["network"].lower()),
            )
            combos[f"{product}__{ad_format}"] = {
                "product": product,
                "ad_format": ad_format,
                "groups": {
                    GROUP_A: {"denominator_request_pv": denominator_by_group[GROUP_A]},
                    GROUP_B: {"denominator_request_pv": denominator_by_group[GROUP_B]},
                },
                "rows": rows_payload,
            }
    return {
        "title": "其他 bidding-network 状态分布",
        "desc": REQUEST_STRUCTURE_TEXT["metric5"],
        "combos": combos,
        "table_mode": "pivot",
    }


def build_request_structure_metric6_table(rows: list[dict[str, Any]]) -> dict[str, Any]:
    combos = {}
    for product in sorted({row["product"] for row in rows}):
        for ad_format in sorted({row["ad_format"] for row in rows}):
            combo_rows = [row for row in rows if row["product"] == product and row["ad_format"] == ad_format]
            if not combo_rows:
                continue
            denominator_by_group = {
                group: float(
                    next(
                        (
                            row["denominator_request_pv"]
                            for row in combo_rows
                            if row["experiment_group"] == group and str(row.get("denominator_request_pv", "")) != ""
                        ),
                        0,
                    )
                    or 0
                )
                for group in (GROUP_A, GROUP_B)
            }
            grouped_rows: dict[tuple[str, str], dict[str, Any]] = {}
            for row in combo_rows:
                key = (row["network_type"], row["network"])
                if key not in grouped_rows:
                    grouped_rows[key] = {
                        "network_type": row["network_type"],
                        "network": row["network"],
                        "groups": {
                            GROUP_A: {
                                "statuses": {
                                    status: {"request_pv": 0.0, "share": 0.0} for status in STATUS_BUCKET_OPTIONS
                                }
                            },
                            GROUP_B: {
                                "statuses": {
                                    status: {"request_pv": 0.0, "share": 0.0} for status in STATUS_BUCKET_OPTIONS
                                }
                            },
                        },
                    }
                grouped_rows[key]["groups"][row["experiment_group"]]["statuses"][row["status_bucket"]] = {
                    "request_pv": float(row["request_pv"]),
                    "share": float(row["share"]),
                }
            rows_payload = sorted(grouped_rows.values(), key=lambda item: (item["network_type"], item["network"].lower()))
            combos[f"{product}__{ad_format}"] = {
                "product": product,
                "ad_format": ad_format,
                "groups": {
                    GROUP_A: {"denominator_request_pv": denominator_by_group[GROUP_A]},
                    GROUP_B: {"denominator_request_pv": denominator_by_group[GROUP_B]},
                },
                "rows": rows_payload,
            }
    return {
        "title": "type + network 四状态总占比",
        "desc": REQUEST_STRUCTURE_TEXT["metric6"],
        "status_buckets": STATUS_BUCKET_OPTIONS,
        "combos": combos,
        "table_mode": "matrix",
    }


def build_coverage_metric1(rows: list[dict[str, Any]]) -> dict[str, Any]:
    series_keys = sort_num_str({str(row["network_cnt"]) for row in rows})
    combos = {}
    for product in sorted({row["product"] for row in rows}):
        for ad_format in sorted({row["ad_format"] for row in rows}):
            combo_rows = [row for row in rows if row["product"] == product and row["ad_format"] == ad_format]
            combos[f"{product}__{ad_format}"] = {
                "product": product,
                "ad_format": ad_format,
                "groups": {
                    GROUP_A: build_bucket_points(
                        [
                            {"req_index": row["req_index"], "series_key": row["network_cnt"], "pv_count": row["pv_count"]}
                            for row in combo_rows
                            if row["experiment_group"] == GROUP_A
                        ],
                        series_keys,
                    ),
                    GROUP_B: build_bucket_points(
                        [
                            {"req_index": row["req_index"], "series_key": row["network_cnt"], "pv_count": row["pv_count"]}
                            for row in combo_rows
                            if row["experiment_group"] == GROUP_B
                        ],
                        series_keys,
                    ),
                },
            }
    return {"title": "每个请求轮次的 network_cnt 桶占比", "desc": COVERAGE_TEXT["metric1"], "series_keys": series_keys, "combos": combos}


def build_coverage_metric2(rows: list[dict[str, Any]]) -> dict[str, Any]:
    combos = {}
    for product in sorted({row["product"] for row in rows}):
        for ad_format in sorted({row["ad_format"] for row in rows}):
            combo_rows = [row for row in rows if row["product"] == product and row["ad_format"] == ad_format]
            cnt_options = sort_num_str({str(row["network_cnt"]) for row in combo_rows})
            cnt_map = {}
            for cnt in cnt_options:
                cnt_rows = [row for row in combo_rows if str(row["network_cnt"]) == cnt]
                left_points = build_coverage_points(
                    [
                        {"req_index": row["req_index"], "series_key": row["network_type"], "pv_count": row["pv_count"], "coverage": row["coverage"], "bucket_request_pv": row["bucket_request_pv"]}
                        for row in cnt_rows
                        if row["experiment_group"] == GROUP_A
                    ],
                    TYPE_OPTIONS,
                )
                right_points = build_coverage_points(
                    [
                        {"req_index": row["req_index"], "series_key": row["network_type"], "pv_count": row["pv_count"], "coverage": row["coverage"], "bucket_request_pv": row["bucket_request_pv"]}
                        for row in cnt_rows
                        if row["experiment_group"] == GROUP_B
                    ],
                    TYPE_OPTIONS,
                )
                cnt_map[cnt] = {"series_keys": TYPE_OPTIONS, "axis_max": max(calc_stacked_axis_max(left_points["points"]), calc_stacked_axis_max(right_points["points"])), "groups": {GROUP_A: left_points, GROUP_B: right_points}}
            combos[f"{product}__{ad_format}"] = {"product": product, "ad_format": ad_format, "network_cnt_options": cnt_options, "cnt_map": cnt_map}
    return {"title": "固定 network_cnt 后的 network_type 覆盖率", "desc": COVERAGE_TEXT["metric2"], "network_types": TYPE_OPTIONS, "combos": combos, "chart_mode": "coverage"}


def build_coverage_metric3(rows: list[dict[str, Any]]) -> dict[str, Any]:
    combos = {}
    for product in sorted({row["product"] for row in rows}):
        for ad_format in sorted({row["ad_format"] for row in rows}):
            combo_rows = [row for row in rows if row["product"] == product and row["ad_format"] == ad_format]
            cnt_options = sort_num_str({str(row["network_cnt"]) for row in combo_rows})
            cnt_map = {}
            for cnt in cnt_options:
                cnt_rows = [row for row in combo_rows if str(row["network_cnt"]) == cnt]
                left_points = build_coverage_points(
                    [
                        {"req_index": row["req_index"], "series_key": row["status"], "pv_count": row["pv_count"], "coverage": row["coverage"], "bucket_request_pv": row["bucket_request_pv"]}
                        for row in cnt_rows
                        if row["experiment_group"] == GROUP_A
                    ],
                    STATUS_SINGLE_OPTIONS,
                )
                right_points = build_coverage_points(
                    [
                        {"req_index": row["req_index"], "series_key": row["status"], "pv_count": row["pv_count"], "coverage": row["coverage"], "bucket_request_pv": row["bucket_request_pv"]}
                        for row in cnt_rows
                        if row["experiment_group"] == GROUP_B
                    ],
                    STATUS_SINGLE_OPTIONS,
                )
                cnt_map[cnt] = {"series_keys": STATUS_SINGLE_OPTIONS, "axis_max": max(calc_stacked_axis_max(left_points["points"]), calc_stacked_axis_max(right_points["points"])), "groups": {GROUP_A: left_points, GROUP_B: right_points}}
            combos[f"{product}__{ad_format}"] = {"product": product, "ad_format": ad_format, "network_cnt_options": cnt_options, "cnt_map": cnt_map}
    return {"title": "固定 network_cnt 后的 status 覆盖率", "desc": COVERAGE_TEXT["metric3"], "status_options": STATUS_SINGLE_OPTIONS, "combos": combos, "chart_mode": "coverage"}


def build_coverage_metric4(rows: list[dict[str, Any]]) -> dict[str, Any]:
    combos = {}
    for product in sorted({row["product"] for row in rows}):
        for ad_format in sorted({row["ad_format"] for row in rows}):
            combo_rows = [row for row in rows if row["product"] == product and row["ad_format"] == ad_format]
            cnt_options = sort_num_str({str(row["network_cnt"]) for row in combo_rows})
            cnt_map = {}
            for cnt in cnt_options:
                cnt_rows = [row for row in combo_rows if str(row["network_cnt"]) == cnt]
                type_map = {}
                for network_type in TYPE_OPTIONS:
                    type_rows = [row for row in cnt_rows if row["network_type"] == network_type]
                    if not type_rows:
                        continue
                    left_points = build_coverage_points(
                        [
                            {
                                "req_index": row["req_index"],
                                "series_key": row["status"],
                                "pv_count": row["pv_count"],
                                "coverage": row["coverage"],
                                "bucket_request_pv": row["type_request_pv"],
                            }
                            for row in type_rows
                            if row["experiment_group"] == GROUP_A
                        ],
                        STATUS_SINGLE_OPTIONS,
                    )
                    right_points = build_coverage_points(
                        [
                            {
                                "req_index": row["req_index"],
                                "series_key": row["status"],
                                "pv_count": row["pv_count"],
                                "coverage": row["coverage"],
                                "bucket_request_pv": row["type_request_pv"],
                            }
                            for row in type_rows
                            if row["experiment_group"] == GROUP_B
                        ],
                        STATUS_SINGLE_OPTIONS,
                    )
                    type_map[network_type] = {
                        "series_keys": STATUS_SINGLE_OPTIONS,
                        "axis_max": max(calc_stacked_axis_max(left_points["points"]), calc_stacked_axis_max(right_points["points"])),
                        "groups": {GROUP_A: left_points, GROUP_B: right_points},
                    }
                cnt_map[cnt] = {"type_map": type_map}
            combos[f"{product}__{ad_format}"] = {
                "product": product,
                "ad_format": ad_format,
                "network_cnt_options": cnt_options,
                "cnt_map": cnt_map,
            }
    return {
        "title": "固定 network_cnt 后按 network_type 拆开的 status 覆盖率",
        "desc": COVERAGE_TEXT["metric4"],
        "network_types": TYPE_OPTIONS,
        "status_options": STATUS_SINGLE_OPTIONS,
        "combos": combos,
        "chart_mode": "coverage",
    }


def build_request_structure_payload() -> dict[str, Any]:
    metric1_network_rows = load_rows(REQUEST_STRUCTURE_CSVS["metric1"]["network"])
    metric1_placement_rows = load_optional_rows(REQUEST_STRUCTURE_CSVS["metric1"]["placement"])
    metric1_rank_rows = load_optional_rows(REQUEST_STRUCTURE_CSVS["metric1"]["rank"])
    metric2_network_rows = load_rows(REQUEST_STRUCTURE_CSVS["metric2"]["network"])
    metric2_placement_rows = load_optional_rows(REQUEST_STRUCTURE_CSVS["metric2"]["placement"])
    metric3_network_rows = load_rows(REQUEST_STRUCTURE_CSVS["metric3"]["network"])
    metric3_placement_rows = load_optional_rows(REQUEST_STRUCTURE_CSVS["metric3"]["placement"])
    metric4_network_rows = load_optional_rows(REQUEST_STRUCTURE_CSVS["metric4"]["network"])
    metric4_placement_rows = load_optional_rows(REQUEST_STRUCTURE_CSVS["metric4"]["placement"])
    all_product_rows = (
        metric1_network_rows
        + metric1_placement_rows
        + metric1_rank_rows
        + metric2_network_rows
        + metric2_placement_rows
        + metric3_network_rows
        + metric3_placement_rows
        + metric4_network_rows
        + metric4_placement_rows
    )
    success_scopes = [
        scope
        for scope in SUCCESS_SCOPE_ORDER
        if any(normalize_success_scope(row) == scope for row in all_product_rows)
    ]
    return {
        "groups": GROUP_LABELS,
        "products": sorted({row["product"] for row in all_product_rows}),
        "ad_formats": sorted({row["ad_format"] for row in all_product_rows}),
        "success_scopes": success_scopes,
        "metrics": {
            "metric1": build_request_structure_metric1(
                metric1_network_rows,
                metric1_placement_rows,
                metric1_rank_rows,
                include_success_scope=True,
            ),
            "metric2": build_request_structure_metric2(
                metric2_network_rows,
                metric2_placement_rows,
                include_success_scope=True,
            ),
            "metric3": build_request_structure_metric3(
                metric3_network_rows,
                metric3_placement_rows,
                include_success_scope=True,
            ),
            "metric4": build_request_structure_metric4(
                metric4_network_rows,
                metric4_placement_rows,
                include_success_scope=True,
            ),
        },
    }


def build_request_structure_country_payload() -> dict[str, Any]:
    metric1_network_rows = load_optional_rows(REQUEST_STRUCTURE_COUNTRY_CSVS["metric1"]["network"])
    metric1_placement_rows = load_optional_rows(REQUEST_STRUCTURE_COUNTRY_CSVS["metric1"]["placement"])
    metric1_rank_rows = load_optional_rows(REQUEST_STRUCTURE_COUNTRY_CSVS["metric1"]["rank"])
    metric2_network_rows = load_optional_rows(REQUEST_STRUCTURE_COUNTRY_CSVS["metric2"]["network"])
    metric2_placement_rows = load_optional_rows(REQUEST_STRUCTURE_COUNTRY_CSVS["metric2"]["placement"])
    metric3_network_rows = load_optional_rows(REQUEST_STRUCTURE_COUNTRY_CSVS["metric3"]["network"])
    metric3_placement_rows = load_optional_rows(REQUEST_STRUCTURE_COUNTRY_CSVS["metric3"]["placement"])
    metric4_network_rows = load_optional_rows(REQUEST_STRUCTURE_COUNTRY_CSVS["metric4"]["network"])
    metric4_placement_rows = load_optional_rows(REQUEST_STRUCTURE_COUNTRY_CSVS["metric4"]["placement"])
    ranking_rows = metric1_network_rows or (
        metric1_placement_rows
        + metric1_rank_rows
        + metric2_network_rows
        + metric2_placement_rows
        + metric3_network_rows
        + metric3_placement_rows
        + metric4_network_rows
        + metric4_placement_rows
    )
    country_options_by_combo = build_top_countries_by_combo(ranking_rows, limit=10)
    metric1_network_rows = filter_rows_by_allowed_countries(metric1_network_rows, country_options_by_combo)
    metric1_placement_rows = filter_rows_by_allowed_countries(metric1_placement_rows, country_options_by_combo)
    metric1_rank_rows = filter_rows_by_allowed_countries(metric1_rank_rows, country_options_by_combo)
    metric2_network_rows = filter_rows_by_allowed_countries(metric2_network_rows, country_options_by_combo)
    metric2_placement_rows = filter_rows_by_allowed_countries(metric2_placement_rows, country_options_by_combo)
    metric3_network_rows = filter_rows_by_allowed_countries(metric3_network_rows, country_options_by_combo)
    metric3_placement_rows = filter_rows_by_allowed_countries(metric3_placement_rows, country_options_by_combo)
    metric4_network_rows = filter_rows_by_allowed_countries(metric4_network_rows, country_options_by_combo)
    metric4_placement_rows = filter_rows_by_allowed_countries(metric4_placement_rows, country_options_by_combo)
    all_rows = (
        metric1_network_rows
        + metric1_placement_rows
        + metric1_rank_rows
        + metric2_network_rows
        + metric2_placement_rows
        + metric3_network_rows
        + metric3_placement_rows
        + metric4_network_rows
        + metric4_placement_rows
    )
    return {
        "groups": GROUP_LABELS,
        "products": sorted({str(row.get("product") or "") for row in all_rows if str(row.get("product") or "")}),
        "ad_formats": sorted({str(row.get("ad_format") or "") for row in all_rows if str(row.get("ad_format") or "")}),
        "countries": sorted({normalize_country(row) for row in all_rows if normalize_country(row)}),
        "country_options_by_combo": country_options_by_combo,
        "metrics": {
            "metric1": build_request_structure_metric1(
                metric1_network_rows,
                metric1_placement_rows,
                metric1_rank_rows,
            ),
            "metric2": build_request_structure_metric2(metric2_network_rows, metric2_placement_rows),
            "metric3": build_request_structure_metric3(metric3_network_rows, metric3_placement_rows),
            "metric4": build_request_structure_metric4(metric4_network_rows, metric4_placement_rows),
        },
    }


def build_request_structure_unit_payload() -> dict[str, Any]:
    ad_unit_name_map = load_ad_unit_name_map()
    metric1_network_rows = rewrite_rows_with_unit_label(
        load_optional_rows(REQUEST_STRUCTURE_UNIT_CSVS["metric1"]["network"]),
        ad_unit_name_map,
    )
    metric1_placement_rows = rewrite_rows_with_unit_label(
        load_optional_rows(REQUEST_STRUCTURE_UNIT_CSVS["metric1"]["placement"]),
        ad_unit_name_map,
    )
    metric1_rank_rows = rewrite_rows_with_unit_label(
        load_optional_rows(REQUEST_STRUCTURE_UNIT_CSVS["metric1"]["rank"]),
        ad_unit_name_map,
    )
    metric2_network_rows = rewrite_rows_with_unit_label(
        load_optional_rows(REQUEST_STRUCTURE_UNIT_CSVS["metric2"]["network"]),
        ad_unit_name_map,
    )
    metric2_placement_rows = rewrite_rows_with_unit_label(
        load_optional_rows(REQUEST_STRUCTURE_UNIT_CSVS["metric2"]["placement"]),
        ad_unit_name_map,
    )
    metric3_network_rows = rewrite_rows_with_unit_label(
        load_optional_rows(REQUEST_STRUCTURE_UNIT_CSVS["metric3"]["network"]),
        ad_unit_name_map,
    )
    metric3_placement_rows = rewrite_rows_with_unit_label(
        load_optional_rows(REQUEST_STRUCTURE_UNIT_CSVS["metric3"]["placement"]),
        ad_unit_name_map,
    )
    metric4_network_rows = rewrite_rows_with_unit_label(
        load_optional_rows(REQUEST_STRUCTURE_UNIT_CSVS["metric4"]["network"]),
        ad_unit_name_map,
    )
    metric4_placement_rows = rewrite_rows_with_unit_label(
        load_optional_rows(REQUEST_STRUCTURE_UNIT_CSVS["metric4"]["placement"]),
        ad_unit_name_map,
    )
    all_rows = (
        metric1_network_rows
        + metric1_placement_rows
        + metric1_rank_rows
        + metric2_network_rows
        + metric2_placement_rows
        + metric3_network_rows
        + metric3_placement_rows
        + metric4_network_rows
        + metric4_placement_rows
    )
    unit_options_by_combo: dict[str, list[str]] = {}
    for product in sorted({str(row.get("product") or "") for row in all_rows if str(row.get("product") or "")}):
        for ad_format in sorted(
            {str(row.get("ad_format") or "") for row in all_rows if str(row.get("product") or "") == product}
        ):
            combo_key = build_combo_key(product, ad_format)
            unit_options_by_combo[combo_key] = sorted(
                {
                    normalize_country(row)
                    for row in all_rows
                    if str(row.get("product") or "") == product
                    and str(row.get("ad_format") or "") == ad_format
                    and normalize_country(row)
                }
            )
    return {
        "groups": GROUP_LABELS,
        "products": sorted({str(row.get("product") or "") for row in all_rows if str(row.get("product") or "")}),
        "ad_formats": sorted({str(row.get("ad_format") or "") for row in all_rows if str(row.get("ad_format") or "")}),
        "units": sorted({normalize_country(row) for row in all_rows if normalize_country(row)}),
        "unit_options_by_combo": unit_options_by_combo,
        "metrics": {
            "metric1": build_request_structure_metric1(
                metric1_network_rows,
                metric1_placement_rows,
                metric1_rank_rows,
            ),
            "metric2": build_request_structure_metric2(metric2_network_rows, metric2_placement_rows),
            "metric3": build_request_structure_metric3(metric3_network_rows, metric3_placement_rows),
            "metric4": build_request_structure_metric4(metric4_network_rows, metric4_placement_rows),
        },
    }


def build_coverage_analysis_payload() -> dict[str, Any]:
    metric1_rows = load_rows(COVERAGE_CSVS["metric1"])
    metric2_rows = load_rows(COVERAGE_CSVS["metric2"])
    metric3_rows = load_rows(COVERAGE_CSVS["metric3"])
    metric4_rows = load_rows(COVERAGE_CSVS["metric4"])
    return {
        "groups": GROUP_LABELS,
        "products": sorted({row["product"] for row in metric1_rows}),
        "ad_formats": sorted({row["ad_format"] for row in metric1_rows}),
        "metrics": {
            "metric1": build_coverage_metric1(metric1_rows),
            "metric2": build_coverage_metric2(metric2_rows),
            "metric3": build_coverage_metric3(metric3_rows),
            "metric4": build_coverage_metric4(metric4_rows),
        },
    }


def build_success_mapping_metric(
    rows: list[dict[str, Any]],
    *,
    count_field: str,
    title: str,
    desc: list[str],
) -> dict[str, Any]:
    combos: dict[str, Any] = {}
    for product in sorted({str(row.get("product") or "") for row in rows}):
        for ad_format in sorted({str(row.get("ad_format") or "") for row in rows if str(row.get("product") or "") == product}):
            combo_rows = [
                row for row in rows
                if str(row.get("product") or "") == product and str(row.get("ad_format") or "") == ad_format
            ]
            count_options = sort_num_str({str(row.get(count_field) or "") for row in combo_rows if str(row.get(count_field) or "")})
            cnt_map: dict[str, Any] = {}
            for count_value in count_options:
                count_rows = [
                    row for row in combo_rows
                    if str(row.get(count_field) or "") == count_value
                ]
                target_order = sorted(
                    {str(row.get("success_target") or "").strip() for row in count_rows if str(row.get("success_target") or "").strip()},
                    key=lambda target: (
                        -max(
                            float(row.get("share") or 0)
                            for row in count_rows
                            if str(row.get("success_target") or "").strip() == target
                        ),
                        -sum(
                            float(row.get("request_pv") or 0)
                            for row in count_rows
                            if str(row.get("success_target") or "").strip() == target
                        ),
                        target.lower(),
                    ),
                )
                rows_payload = []
                for target in target_order:
                    groups = {}
                    for group_key in (GROUP_A, GROUP_B):
                        current = next(
                            (
                                row for row in count_rows
                                if str(row.get("success_target") or "").strip() == target
                                and str(row.get("experiment_group") or "").strip() == group_key
                            ),
                            None,
                        )
                        groups[group_key] = {
                            "request_pv": float((current or {}).get("request_pv") or 0),
                            "denominator_request_pv": float((current or {}).get("denominator_request_pv") or 0),
                            "share": float((current or {}).get("share") or 0),
                        }
                    rows_payload.append({"success_target": target, "groups": groups})
                cnt_map[count_value] = {"rows": rows_payload}
            if count_options:
                combos[f"{product}__{ad_format}"] = {
                    "product": product,
                    "ad_format": ad_format,
                    "view": {
                        "count_label": count_field,
                        "count_options": count_options,
                        "cnt_map": cnt_map,
                    },
                }
    return {"title": title, "desc": desc, "combos": combos}


def build_success_mapping_payload(
    network_rows: list[dict[str, Any]],
    placement_rows: list[dict[str, Any]],
) -> dict[str, Any]:
    network_metric = build_success_mapping_metric(
        network_rows,
        count_field="network_cnt",
        title="成功 network 分布",
        desc=SUCCESS_MAPPING_TEXT["network"],
    )
    placement_metric = build_success_mapping_metric(
        placement_rows,
        count_field="placement_cnt",
        title="成功 placement 分布",
        desc=SUCCESS_MAPPING_TEXT["placement"],
    )
    combo_keys = sorted(set(network_metric["combos"]) | set(placement_metric["combos"]))
    combos = {}
    for combo_key in combo_keys:
        network_combo = network_metric["combos"].get(combo_key)
        placement_combo = placement_metric["combos"].get(combo_key)
        product = (network_combo or placement_combo or {}).get("product", "")
        ad_format = (network_combo or placement_combo or {}).get("ad_format", "")
        combos[combo_key] = {
            "product": product,
            "ad_format": ad_format,
            "network_view": (network_combo or {}).get("view"),
            "placement_view": (placement_combo or {}).get("view"),
        }
    return {
        "title": "成功 network / placement 分布",
        "desc": SUCCESS_MAPPING_HERO_TEXT,
        "combos": combos,
        "network_metric": network_metric,
        "placement_metric": placement_metric,
    }


def build_success_mapping_dashboard_payload() -> dict[str, Any]:
    network_rows = load_optional_rows(SUCCESS_NETWORK_BY_NETWORK_CNT_CSV)
    placement_rows = load_optional_rows(SUCCESS_PLACEMENT_BY_PLACEMENT_CNT_CSV)
    payload = build_success_mapping_payload(network_rows, placement_rows)
    all_rows = network_rows + placement_rows
    return {
        "title": payload["title"],
        "desc": payload["desc"],
        "groups": GROUP_LABELS,
        "products": sorted({str(row.get("product") or "") for row in all_rows if str(row.get("product") or "")}),
        "ad_formats": sorted({str(row.get("ad_format") or "") for row in all_rows if str(row.get("ad_format") or "")}),
        "metrics": {
            "network": payload["network_metric"],
            "placement": payload["placement_metric"],
        },
    }


def resolve_dashboard_page_title(page_key: str, page_title: str | None = None) -> str:
    resolved = (page_title or "").strip()
    if resolved:
        return resolved
    return DASHBOARD_PAGE_TITLES.get(page_key, "")


def validate_generated_html_text(
    html_text: str,
    output_label: str,
    *,
    required_strings: list[str] | tuple[str, ...] | None = None,
) -> None:
    if "???" in html_text:
        raise ValueError(f"{output_label} 包含 '???'，疑似中文在生成链路中损坏。")
    if "\ufffd" in html_text:
        raise ValueError(f"{output_label} 包含 Unicode replacement char，疑似编码损坏。")
    for required in required_strings or []:
        if required and required not in html_text:
            raise ValueError(f"{output_label} 缺少预期文本：{required}")


def write_validated_html(
    path: Path,
    html_text: str,
    *,
    required_strings: list[str] | tuple[str, ...] | None = None,
) -> None:
    path.write_text(html_text, encoding="utf-8")
    raw = path.read_bytes()
    if raw.startswith(b"\xef\xbb\xbf"):
        raise ValueError(f"{path} 生成了 UTF-8 BOM。")
    roundtrip = raw.decode("utf-8")
    validate_generated_html_text(roundtrip, str(path), required_strings=required_strings)


def build_entry_html() -> str:
    return """<!doctype html>
<html lang="zh-CN">
<head>
<meta charset="UTF-8" />
<meta name="viewport" content="width=device-width, initial-scale=1.0" />
<title>AB 请求结构看板入口</title>
<style>
body{margin:0;font-family:"Microsoft YaHei",sans-serif;background:#f6f1e9;color:#203040}
.page{max-width:980px;margin:0 auto;padding:40px 20px 60px}
.hero h1{margin:0 0 10px;font-size:34px}
.hero p{margin:0;color:#667788;line-height:1.7}
.grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(280px,1fr));gap:16px;margin-top:28px}
.card{display:block;padding:22px;border-radius:24px;border:1px solid rgba(32,48,64,.12);background:#fff;color:inherit;text-decoration:none}
.card h2{margin:0 0 8px;font-size:24px}
.card p{margin:0;color:#667788;line-height:1.7}
</style>
</head>
<body>
<div class="page">
  <section class="hero">
    <h1>AB 请求结构看板</h1>
    <p>本入口页将请求结构、bidding network status、胜利渠道命中率、覆盖率分析与成功映射拆成独立 HTML，分别打开即可查看。</p>
  </section>
  <section class="grid">
    <a class="card" href="ab_request_structure_dashboard.html">
      <h2>请求结构分布</h2>
      <p>查看全量 request 的 total network_cnt、bidding/waterfall 组合和 status 下钻分布。</p>
    </a>
    <a class="card" href="ab_request_structure_unit_dashboard.html">
      <h2>请求结构分布（Unit）</h2>
      <p>查看当前 product + ad_format 下，各 ad unit 的 total network_cnt、placement 结构和 status 下钻分布。</p>
    </a>
    <a class="card" href="ab_coverage_analysis_dashboard.html">
      <h2>覆盖率分析</h2>
      <p>查看 req_index × network_cnt 桶占比，以及 network_type / status 在桶内的请求覆盖率。</p>
    </a>
    <a class="card" href="ab_bidding_network_status_dashboard.html">
      <h2>Bidding Network Status</h2>
      <p>查看 ALL UNIT 与具体 unit 下，type + network 在四状态上的总占比与曲线分布。</p>
    </a>
    <a class="card" href="ab_winning_type_network_status_dashboard.html">
      <h2>胜利渠道状态命中率</h2>
      <p>查看指定胜利 type + network 为唯一 AD_LOADED 时，其他渠道在这些 request 上的状态命中率。</p>
    </a>
    <a class="card" href="ab_success_mapping_dashboard.html">
      <h2>成功 network / placement 分布</h2>
      <p>先按全量 network_cnt 或 placement_cnt 分桶，再看当前桶里不同成功对象与 fail 的占比。</p>
    </a>
  </section>
</div>
</body>
</html>"""


def build_common_script(payload: dict[str, Any]) -> str:
    payload_json = json.dumps(payload, ensure_ascii=False)
    return f"""
const DATA={payload_json};
const GROUP_A='no_is_adx',GROUP_B='have_is_adx',GROUPS=DATA.groups||{{}},STATUS_SORT={{'AD_LOADED':0,'FAILED_TO_LOAD':1,'AD_LOAD_NOT_ATTEMPTED':2,'AD_LOADED+FAILED_TO_LOAD':3,'AD_LOADED+AD_LOAD_NOT_ATTEMPTED':4,'FAILED_TO_LOAD+AD_LOAD_NOT_ATTEMPTED':5,'AD_LOADED+FAILED_TO_LOAD+AD_LOAD_NOT_ATTEMPTED':6}};
const fmtNum=v=>Math.round(Number(v||0)).toLocaleString(),fmtPct=v=>`${{(Number(v||0)*100).toFixed(2)}}%`,byOrder=arr=>[...arr].sort((a,b)=>(STATUS_SORT[a]??99)-(STATUS_SORT[b]??99));
const pageError=document.getElementById('page-error');
function setError(m){{pageError.hidden=!m;pageError.textContent=m||''}}
function explain(desc){{return `<div class="explain"><h4>指标解释</h4><ul>${{(desc||[]).map(v=>`<li>${{v}}</li>`).join('')}}</ul></div>`}}
function colors(n){{const p=['#e6194b','#3cb44b','#ffe119','#4363d8','#f58231','#911eb4','#46f0f0','#f032e6','#bcf60c','#fabebe','#008080','#e6beff','#9a6324','#fffac8','#800000','#aaffc3','#808000','#ffd8b1','#000075','#808080'];return Array.from({{length:n}},(_,i)=>p[i%p.length])}}
function selectorGroup(title,vals,active,cb){{const wrap=document.createElement('div');wrap.className='toolbar-field';const lab=document.createElement('label');lab.textContent=title;const box=document.createElement('div');box.className='selector-group';wrap.appendChild(lab);wrap.appendChild(box);(vals||[]).forEach(v=>{{const b=document.createElement('button');b.type='button';b.className=`selector${{active.includes(v)?' active':''}}`;b.textContent=v;b.onclick=()=>cb(v);box.appendChild(b)}});return wrap}}
function selectField(label,values,current,onChange){{const wrap=document.createElement('div');wrap.className='toolbar-field';const lab=document.createElement('label');lab.textContent=label;const sel=document.createElement('select');sel.innerHTML=(values||[]).map(v=>`<option value="${{v}}">${{v}}</option>`).join('');sel.value=(values||[]).includes(current)?current:((values||[])[0]||'');sel.onchange=e=>onChange(e.target.value);wrap.appendChild(lab);wrap.appendChild(sel);return wrap}}
function combo(metricKey,product,adFormat,country='',successScope=''){{const metric=(DATA.metrics||{{}})[metricKey]||{{}};if(country&&successScope)return (metric.combos||{{}})[`${{product}}__${{adFormat}}__${{country}}__${{successScope}}`]||null;if(country)return (metric.combos||{{}})[`${{product}}__${{adFormat}}__${{country}}`]||null;if(successScope)return (metric.combos||{{}})[`${{product}}__${{adFormat}}__${{successScope}}`]||null;return (metric.combos||{{}})[`${{product}}__${{adFormat}}`]||null}}
function metricConfig(metricKey){{return ((DATA.metrics||{{}})[metricKey]||null)}}
function countryOptions(product,adFormat){{return ((DATA.country_options_by_combo||{{}})[`${{product}}__${{adFormat}}`]||[])}}
function unitOptions(product,adFormat){{return ((DATA.unit_options_by_combo||{{}})[`${{product}}__${{adFormat}}`]||[])}}
function buildOption(keys,points,peerPoints,groupLabel,peerLabel,chartMode='share',axisMax=null){{
  const isCoverage=chartMode==='coverage';
  const ratioLabel=isCoverage?'覆盖率':'占比';
  const deltaLabel=isCoverage?'覆盖率差值':'占比差值';
  const denominatorLabel=isCoverage?'桶内请求':'当前轮次总请求';
  const axisTitle=String(points[0]?.axis_label||'维度');
  const palette=colors(keys.length);
  const series=keys.map((key)=>{{
    const data=points.map((point,index)=>{{
      const cur=point.series[key]||{{pv_count:0,share:0,denominator_pv:0}};
      const peer=(peerPoints[index]&&peerPoints[index].series[key])||{{pv_count:0,share:0,denominator_pv:0}};
      return {{value:Number((cur.share||0).toFixed(6)),pv:cur.pv_count,share:cur.share,peerPv:peer.pv_count,peerShare:peer.share,denominatorPv:cur.denominator_pv,peerDenominatorPv:peer.denominator_pv,key,groupLabel,peerLabel}};
    }});
    return {{name:key,type:'line',stack:'total',areaStyle:{{opacity:0.75}},lineStyle:{{width:1,opacity:0}},symbol:'none',emphasis:{{focus:'series',areaStyle:{{opacity:0.95}}}},data}};
  }});
  const yAxis={{type:'value',min:0,axisLabel:{{formatter:v=>`${{Math.round(v*100)}}%`,color:'#667788',fontSize:11}},splitLine:{{lineStyle:{{color:'rgba(31,49,64,0.07)'}}}}}};
  if(!isCoverage)yAxis.max=1;
  if(isCoverage&&axisMax!==null)yAxis.max=axisMax;
  const zoomEnd=Math.min(100,Math.floor(4000/Math.max(1,points.length)));
  return {{
    color:palette,
    backgroundColor:'rgba(252,250,246,0.95)',
    grid:{{left:54,right:12,top:30,bottom:70}},
    dataZoom:[{{type:'inside'}},{{type:'slider',bottom:30,height:16,end:zoomEnd,showDetail:false,borderColor:'transparent',backgroundColor:'rgba(31,49,64,0.04)',fillerColor:'rgba(15,118,110,0.15)',handleStyle:{{color:'#0f766e'}}}}],
    legend:{{show:true,type:'scroll',bottom:0,itemHeight:10,itemWidth:14,textStyle:{{fontSize:11,color:'#667788'}}}},
    tooltip:{{
      trigger:'axis',
      axisPointer:{{type:'line',lineStyle:{{color:'rgba(32,48,64,0.2)'}}}},
      backgroundColor:'rgba(255,255,255,0.96)',
      borderColor:'rgba(31,49,64,0.14)',
      textStyle:{{fontSize:12,color:'#1f3140'}},
      formatter(params){{
        if(!params||!params.length)return '';
        let active=params[0];
        if(typeof window._current_mouse_y === 'number'){{
          const mouseY = window._current_mouse_y;
          let acc = 0;
          for(let i=0; i<params.length; i++){{acc += Number(params[i].data.share||0); if(mouseY <= acc && Number(params[i].data.share||0)>0){{active = params[i]; break;}}}}
        }}else{{active=params.slice().sort((a,b)=>Number(b.data.share||0)-Number(a.data.share||0))[0];}}
        const d=active.data,delta=Number(d.share||0)-Number(d.peerShare||0),sign=delta>0?'+':'';
        const dot=`<span style="display:inline-block;width:8px;height:8px;border-radius:50%;background:${{active.color}};margin-right:5px"></span>`;
        return `<div style="min-width:220px"><div style="margin-bottom:6px;font-weight:600">${{axisTitle}}: ${{active.axisValue}}&nbsp;&nbsp;${{dot}}<b>${{d.key}}</b></div>`+
          `<table style="width:100%;border-collapse:collapse;font-size:12px">`+
          `<tr><td style="color:#667788;padding:2px 6px 2px 0">${{d.groupLabel}} pv</td><td style="text-align:right">${{fmtNum(d.pv)}}</td></tr>`+
          `${{isCoverage?`<tr><td style="color:#667788;padding:2px 6px 2px 0">${{d.groupLabel}} ${{denominatorLabel}}</td><td style="text-align:right">${{fmtNum(d.denominatorPv)}}</td></tr>`:''}}`+
          `<tr><td style="color:#667788;padding:2px 6px 2px 0">${{d.groupLabel}} ${{ratioLabel}}</td><td style="text-align:right;font-weight:600">${{fmtPct(d.share)}}</td></tr>`+
          `<tr><td style="color:#667788;padding:2px 6px 2px 0">${{d.peerLabel}} pv</td><td style="text-align:right">${{fmtNum(d.peerPv)}}</td></tr>`+
          `${{isCoverage?`<tr><td style="color:#667788;padding:2px 6px 2px 0">${{d.peerLabel}} ${{denominatorLabel}}</td><td style="text-align:right">${{fmtNum(d.peerDenominatorPv)}}</td></tr>`:''}}`+
          `<tr><td style="color:#667788;padding:2px 6px 2px 0">${{d.peerLabel}} ${{ratioLabel}}</td><td style="text-align:right">${{fmtPct(d.peerShare)}}</td></tr>`+
          `<tr><td style="color:#667788;padding:2px 6px 2px 0">${{deltaLabel}}</td><td style="text-align:right;color:${{delta>0?'#0f766e':'#e11d48'}}">${{sign}}${{fmtPct(delta)}}</td></tr>`+
          `</table></div>`;
      }}
    }},
    xAxis:{{type:'category',data:points.map(point=>point.axis_value),axisLine:{{lineStyle:{{color:'rgba(31,49,64,0.12)'}}}},axisTick:{{show:false}},axisLabel:{{color:'#667788',fontSize:11,interval:(i,v)=>v%50===0||v===1}},splitLine:{{show:false}}}},
    yAxis,
    series
  }};
}}
function mountPair(host,keys,leftPoints,rightPoints,title,subtitle,chartMode='share',axisMax=null){{
  const uid=Math.random().toString(36).slice(2);
  const box=document.createElement('div');
  box.className='detail-card';
  box.innerHTML=`<div class="detail-top"><h4>${{title}}</h4><p>${{subtitle}}</p></div><div class="chart-pair"><div class="chart-box"><div class="chart-box-head"><strong>${{GROUPS[GROUP_A]}}</strong><span class="muted">左图固定 A 组</span></div><div class="chart" id="lc-${{uid}}"></div></div><div class="chart-box"><div class="chart-box-head"><strong>${{GROUPS[GROUP_B]}}</strong><span class="muted">右图固定 B 组</span></div><div class="chart" id="rc-${{uid}}"></div></div></div>`;
  host.appendChild(box);
  const leftEl=document.getElementById(`lc-${{uid}}`),rightEl=document.getElementById(`rc-${{uid}}`);
  const lc=echarts.init(leftEl),rc=echarts.init(rightEl);
  lc.setOption(buildOption(keys,leftPoints,rightPoints,GROUPS[GROUP_A],GROUPS[GROUP_B],chartMode,axisMax));
  rc.setOption(buildOption(keys,rightPoints,leftPoints,GROUPS[GROUP_B],GROUPS[GROUP_A],chartMode,axisMax));
  const bindZr=(chart)=>{{chart.getZr().on('mousemove', function(e){{const pointInGrid=chart.convertFromPixel({{seriesIndex:0}}, [e.offsetX, e.offsetY]);if(pointInGrid) window._current_mouse_y = pointInGrid[1];}});}};
  bindZr(lc); bindZr(rc);
  new ResizeObserver(()=>{{lc.resize();rc.resize();}}).observe(box);
}}
function metricEmpty(title,desc,product,adFormat,key){{return `<section class="metric"><div class="card"><div class="card-head"><div><h2>${{title}}</h2><div class="muted">${{product}} / ${{adFormat}}</div></div></div>${{explain(desc)}}<div class="panel-wrap"><div class="empty">${{key}} 在当前筛选条件下暂无结果。</div></div></div></section>`}}
function productFormat(){{const product=document.getElementById('product-select'),format=document.getElementById('format-select');product.innerHTML=(DATA.products||[]).map(v=>`<option value="${{v}}">${{v}}</option>`).join('');format.innerHTML=(DATA.ad_formats||[]).map(v=>`<option value="${{v}}">${{v}}</option>`).join('');return {{product,format}}}}
"""


def build_request_structure_page_script() -> str:
    return """
function buildBucketCategories(groupAPoints,groupBPoints){
  const categorySet=new Set();
  [...(groupAPoints||[]),...(groupBPoints||[])].forEach(point=>{
    const bucketKey=String(point?.bucket_key??'');
    if(bucketKey!=='')categorySet.add(bucketKey);
  });
  return [...categorySet].sort((left,right)=>{
    const leftNum=Number(left),rightNum=Number(right);
    const leftIsNumeric=left!==''&&Number.isFinite(leftNum),rightIsNumeric=right!==''&&Number.isFinite(rightNum);
    if(leftIsNumeric&&rightIsNumeric)return leftNum-rightNum;
    if(leftIsNumeric!==rightIsNumeric)return leftIsNumeric?-1:1;
    return left.localeCompare(right,undefined,{numeric:true,sensitivity:'base'});
  });
}
function buildDistributionSeriesData(points,categories){
  const pointMap=new Map();
  (points||[]).forEach(point=>{pointMap.set(String(point?.bucket_key??''),point)});
  return categories.map(bucketKey=>{
    const point=pointMap.get(bucketKey);
    return {value:Number(((point?.share)||0).toFixed(6)),bucketKey,share:Number(point?.share||0),requestPv:Number(point?.request_pv||0),denominatorRequestPv:Number(point?.denominator_request_pv||0)};
  });
}
function buildDistributionCompareOptionWithCategories(groupAPoints,groupBPoints,categories,axisMax=null){
  const basePoints=(groupAPoints&&groupAPoints.length)?groupAPoints:(groupBPoints||[]);
  const axisTitle=String(basePoints[0]?.axis_label||'bucket');
  const groupASeriesData=buildDistributionSeriesData(groupAPoints,categories);
  const groupBSeriesData=buildDistributionSeriesData(groupBPoints,categories);
  const resolvedAxisMax=resolveDistributionAxisMax(axisMax);
  return {
    backgroundColor:'rgba(252,250,246,0.95)',
    grid:{left:54,right:12,top:28,bottom:65},
    legend:{show:true,top:0,textStyle:{fontSize:11,color:'#667788'}},
    tooltip:{
      trigger:'axis',
      axisPointer:{type:'shadow'},
      backgroundColor:'rgba(255,255,255,0.96)',
      borderColor:'rgba(31,49,64,0.14)',
      textStyle:{fontSize:12,color:'#1f3140'},
      formatter(params){
        if(!params||!params.length)return '';
        const a=params.find(item=>item.seriesName===GROUPS[GROUP_A])||{data:{}};
        const b=params.find(item=>item.seriesName===GROUPS[GROUP_B])||{data:{}};
        const bucketKey=a.data.bucketKey||b.data.bucketKey||params[0].axisValue||'';
        const delta=Number((a.data.share||0)-(b.data.share||0));
        const sign=delta>0?'+':'';
        return `<div style="min-width:240px"><div style="margin-bottom:6px;font-weight:600">${axisTitle}: ${bucketKey}</div><table style="width:100%;border-collapse:collapse;font-size:12px"><tr><td style="color:#667788;padding:2px 6px 2px 0">${GROUPS[GROUP_A]} pv</td><td style="text-align:right">${fmtNum(a.data.requestPv||0)}</td></tr><tr><td style="color:#667788;padding:2px 6px 2px 0">${GROUPS[GROUP_A]} 分母</td><td style="text-align:right">${fmtNum(a.data.denominatorRequestPv||0)}</td></tr><tr><td style="color:#667788;padding:2px 6px 2px 0">${GROUPS[GROUP_A]} share</td><td style="text-align:right;font-weight:600">${fmtPct(a.data.share||0)}</td></tr><tr><td style="color:#667788;padding:2px 6px 2px 0">${GROUPS[GROUP_B]} pv</td><td style="text-align:right">${fmtNum(b.data.requestPv||0)}</td></tr><tr><td style="color:#667788;padding:2px 6px 2px 0">${GROUPS[GROUP_B]} 分母</td><td style="text-align:right">${fmtNum(b.data.denominatorRequestPv||0)}</td></tr><tr><td style="color:#667788;padding:2px 6px 2px 0">${GROUPS[GROUP_B]} share</td><td style="text-align:right;font-weight:600">${fmtPct(b.data.share||0)}</td></tr><tr><td style="color:#667788;padding:2px 6px 2px 0">share 差值</td><td style="text-align:right;color:${delta>0?'#0f766e':'#e11d48'}">${sign}${fmtPct(delta)}</td></tr></table></div>`;
      }
    },
    xAxis:{type:'category',data:categories,axisLine:{lineStyle:{color:'rgba(31,49,64,0.12)'}},axisTick:{show:false},axisLabel:{color:'#667788',fontSize:11,interval:0}},
    yAxis:{type:'value',min:0,max:resolvedAxisMax,axisLabel:{formatter:value=>`${Math.round(value*100)}%`,color:'#667788',fontSize:11},splitLine:{lineStyle:{color:'rgba(31,49,64,0.07)'}}},
    series:[
      {name:GROUPS[GROUP_A],type:'bar',barMaxWidth:30,itemStyle:{color:'#0f766e'},data:groupASeriesData},
      {name:GROUPS[GROUP_B],type:'bar',barMaxWidth:30,itemStyle:{color:'#2563eb'},data:groupBSeriesData}
    ]
  };
}
function buildDistributionCompareOption(groupAPoints,groupBPoints,axisMax=null){return buildDistributionCompareOptionWithCategories(groupAPoints,groupBPoints,buildBucketCategories(groupAPoints,groupBPoints),axisMax);}
function bindDistributionHover(chart){chart.getZr().on('mousemove', function(e){const pointInGrid=chart.convertFromPixel({seriesIndex:0}, [e.offsetX, e.offsetY]);if(pointInGrid) window._current_mouse_y = pointInGrid[1];});}
function resolveDistributionAxisMax(axisMax){return axisMax!==null?axisMax:1;}
function mountDistributionCompareChart(host,groupAPoints,groupBPoints,axisMax=null){const chart=echarts.init(host);chart.setOption(buildDistributionCompareOption(groupAPoints,groupBPoints,axisMax));bindDistributionHover(chart);return chart;}
function mountDistributionCompareChartWithCategories(host,groupAPoints,groupBPoints,categories,axisMax=null){const chart=echarts.init(host);chart.setOption(buildDistributionCompareOptionWithCategories(groupAPoints,groupBPoints,categories,axisMax));bindDistributionHover(chart);return chart;}
function createDistributionViewBox(title,subtitle){const box=document.createElement('div');box.className='chart-box';box.innerHTML=`<div class="detail-top"><h4>${title}</h4>${subtitle?`<p>${subtitle}</p>`:''}</div><div class="toolbar"></div><div class="chart-wrap"></div>`;return {box,toolbar:box.querySelector('.toolbar'),chartWrap:box.querySelector('.chart-wrap')};}
function calcMetric1ChartWidth(view){void view;return 0;}
function calcDistributionChartWidth(categories){return Math.max(960,Math.max(1,(categories||[]).length)*88);}
function scrollableDistributionCategories(current){return current.bucket_options||buildBucketCategories((current.groups||{})[GROUP_A]?.points||[],(current.groups||{})[GROUP_B]?.points||[]);}
function appendScrollableDistributionChart(chartWrap,box,current,axisMax=null){const categories=scrollableDistributionCategories(current);const scroll=document.createElement('div');scroll.className='chart-scroll';const chartEl=document.createElement('div');chartEl.className='chart';chartEl.style.width=`${calcDistributionChartWidth(categories)}px`;scroll.appendChild(chartEl);chartWrap.appendChild(scroll);const chart=mountDistributionCompareChartWithCategories(chartEl,(current.groups||{})[GROUP_A]?.points||[],(current.groups||{})[GROUP_B]?.points||[],categories,axisMax);new ResizeObserver(()=>chart.resize()).observe(box);}
function successScopeLabel(successScope){return ({all:'全部',has_success:'有成功',no_success:'无成功'})[successScope]||successScope||'全部';}
function renderMetric1View(host,title,view,axisMax=null,subtitleOverride='',chartWidth=null){const subtitle=subtitleOverride||'';const {box,chartWrap}=createDistributionViewBox(title,subtitle);host.appendChild(box);if(!view||!((view.count_options||[]).length)){chartWrap.innerHTML='<div class="empty">当前口径暂无结果。</div>';return}const scroll=document.createElement('div');scroll.className='chart-scroll';const chartEl=document.createElement('div');chartEl.className='chart';void chartWidth;scroll.appendChild(chartEl);chartWrap.appendChild(scroll);const chart=mountDistributionCompareChart(chartEl,(view.groups||{})[GROUP_A]?.points||[],(view.groups||{})[GROUP_B]?.points||[],axisMax);new ResizeObserver(()=>chart.resize()).observe(box);}
function buildMetric1RankSummaryTag(groupKey,summary){const tag=document.createElement('div');tag.className='condition-tag';tag.innerHTML=`<strong>${GROUPS[groupKey]}</strong>&nbsp;成功 ${fmtNum(summary.success_request_pv||0)} / 总桶 ${fmtNum(summary.bucket_total_request_pv||0)} / 成功率 ${fmtPct(summary.success_rate||0)}`;return tag;}
function renderMetric1RankView(host,title,view){const {box,toolbar,chartWrap}=createDistributionViewBox(title,'图内分母 = 当前 cnt 桶内成功 request pv；右上角显示成功 pv / 当前 cnt 总桶 pv / 成功率。');host.appendChild(box);if(!view||!((view.cnt_options||[]).length)){chartWrap.innerHTML='<div class="empty">当前口径暂无结果。</div>';return}let cntValue=view.default_cnt||view.cnt_options[0]||'';const countLabel=view.cnt_type==='placement'?'placement_cnt（单选）':'network_cnt（单选）';const draw=()=>{const current=(view.bucket_map||{})[cntValue];toolbar.innerHTML='';toolbar.appendChild(selectField(countLabel,view.cnt_options,cntValue,value=>{cntValue=value;draw()}));chartWrap.innerHTML='';if(!current){chartWrap.innerHTML='<div class="empty">当前筛选条件下暂无结果。</div>';return}toolbar.appendChild(buildMetric1RankSummaryTag(GROUP_A,((current.groups||{})[GROUP_A]||{}).summary||{}));toolbar.appendChild(buildMetric1RankSummaryTag(GROUP_B,((current.groups||{})[GROUP_B]||{}).summary||{}));const categories=current.rank_options||buildBucketCategories((current.groups||{})[GROUP_A]?.points||[],(current.groups||{})[GROUP_B]?.points||[]);const scroll=document.createElement('div');scroll.className='chart-scroll';const chartEl=document.createElement('div');chartEl.className='chart';chartEl.style.width=`${calcDistributionChartWidth(categories)}px`;scroll.appendChild(chartEl);chartWrap.appendChild(scroll);const chart=mountDistributionCompareChartWithCategories(chartEl,(current.groups||{})[GROUP_A]?.points||[],(current.groups||{})[GROUP_B]?.points||[],categories,current.axis_max??null);new ResizeObserver(()=>chart.resize()).observe(box)};draw();}
function renderMetric1RankBlock(root,c,subtitle){const metric=metricConfig('metric1');const rankBlock=c.rank_block||{};if(!rankBlock.network_rank_view&&!rankBlock.placement_rank_view)return;const sec=document.createElement('section');sec.className='metric';sec.innerHTML=`<div class="card"><div class="card-head"><div><h2>成功 request 的 success_rank 分布</h2><div class="muted">${subtitle}</div></div></div>${explain(metric.rank_desc||[])}<div class="panel-wrap"></div></div>`;root.appendChild(sec);const pair=document.createElement('div');pair.className='chart-pair';sec.querySelector('.panel-wrap').appendChild(pair);renderMetric1RankView(pair,'network success_rank 分布',rankBlock.network_rank_view);renderMetric1RankView(pair,'placement success_rank 分布',rankBlock.placement_rank_view);}
function renderMetric2View(host,title,view){const {box,toolbar,chartWrap}=createDistributionViewBox(title,'');host.appendChild(box);if(!view||!((view.count_options||[]).length)){chartWrap.innerHTML='<div class="empty">当前口径暂无结果。</div>';return}let countValue=view.count_options[0]||'';const draw=()=>{chartWrap.innerHTML='';const current=(view.cnt_map||{})[countValue];if(!current){chartWrap.innerHTML='<div class="empty">当前筛选条件下暂无结果。</div>';return}const chartEl=document.createElement('div');chartEl.className='chart';chartWrap.appendChild(chartEl);const chart=mountDistributionCompareChart(chartEl,(current.groups||{})[GROUP_A]?.points||[],(current.groups||{})[GROUP_B]?.points||[]);new ResizeObserver(()=>chart.resize()).observe(box)};toolbar.innerHTML='';toolbar.appendChild(selectField(`${view.count_label}（单选）`,view.count_options,countValue,value=>{countValue=value;draw()}));draw();}
function renderMetric3View(host,title,view){const {box,toolbar,chartWrap}=createDistributionViewBox(title,'');host.appendChild(box);if(!view||!((view.count_options||[]).length)){chartWrap.innerHTML='<div class="empty">当前口径暂无结果。</div>';return}let countValue=view.count_options[0]||'',networkType='',typeCount='';const countLabel=view.count_label==='placement_cnt'?'placement_cnt（单选）':'network_cnt（单选）';const typeCountLabel=view.type_count_label==='type_placement_cnt'?'type_placement_cnt（单选）':'type_network_cnt（单选）';const typeOptions=()=>(((view.cnt_map||{})[countValue]||{}).network_type_options||[]);const typeCountOptions=()=>(((((view.cnt_map||{})[countValue]||{}).type_map||{})[networkType]||{}).type_count_options||[]);const normalize=()=>{const currentTypes=typeOptions();networkType=currentTypes.includes(networkType)?networkType:(currentTypes[0]||'');const currentTypeCounts=typeCountOptions();typeCount=currentTypeCounts.includes(typeCount)?typeCount:(currentTypeCounts[0]||'')};const draw=()=>{normalize();chartWrap.innerHTML='';const current=((((((view.cnt_map||{})[countValue]||{}).type_map||{})[networkType]||{}).type_cnt_map||{})[typeCount]);if(!current){chartWrap.innerHTML='<div class="empty">当前筛选条件下暂无结果。</div>';return}const chartEl=document.createElement('div');chartEl.className='chart';chartWrap.appendChild(chartEl);const chart=mountDistributionCompareChart(chartEl,(current.groups||{})[GROUP_A]?.points||[],(current.groups||{})[GROUP_B]?.points||[]);new ResizeObserver(()=>chart.resize()).observe(box)};const drawToolbar=()=>{normalize();toolbar.innerHTML='';toolbar.appendChild(selectField(countLabel,view.count_options,countValue,value=>{countValue=value;drawToolbar();draw()}));toolbar.appendChild(selectField('network_type（单选）',typeOptions(),networkType,value=>{networkType=value;drawToolbar();draw()}));toolbar.appendChild(selectField(typeCountLabel,typeCountOptions(),typeCount,value=>{typeCount=value;draw()}))};drawToolbar();draw();}
function renderRequestStructureDualViews(panel,combo,renderer,sharedAxisMax=null){const card=document.createElement('div');card.className='detail-card';const pair=document.createElement('div');pair.className='chart-pair';card.appendChild(pair);panel.appendChild(card);renderer(pair,'network个数',combo.network_view,sharedAxisMax);renderer(pair,'placement个数',combo.placement_view,sharedAxisMax);}
function renderMetric1(root,c){const metric=metricConfig('metric1');const sec=document.createElement('section');sec.className='metric';sec.innerHTML=`<div class="card"><div class="card-head"><div><h2>${metric.title}</h2><div class="muted">${c.product} / ${c.ad_format} / ${successScopeLabel(c.success_scope)}</div></div></div>${explain(metric.desc)}<div class="panel-wrap"></div></div>`;root.appendChild(sec);const stack=document.createElement('div');stack.className='chart-stack';sec.querySelector('.panel-wrap').appendChild(stack);renderMetric1View(stack,'network个数',c.network_view,c.axis_max??null,'',calcMetric1ChartWidth(c.network_view));renderMetric1View(stack,'placement个数',c.placement_view,c.axis_max??null,'',calcMetric1ChartWidth(c.placement_view));}
function renderMetric2(root,c){const metric=metricConfig('metric2');const sec=document.createElement('section');sec.className='metric';sec.innerHTML=`<div class="card"><div class="card-head"><div><h2>${metric.title}</h2><div class="muted">${c.product} / ${c.ad_format} / ${successScopeLabel(c.success_scope)}</div></div></div>${explain(metric.desc)}<div class="panel-wrap"></div></div>`;root.appendChild(sec);renderRequestStructureDualViews(sec.querySelector('.panel-wrap'),c,renderMetric2View);}
function renderMetric3(root,c){const metric=metricConfig('metric3');const sec=document.createElement('section');sec.className='metric';sec.innerHTML=`<div class="card"><div class="card-head"><div><h2>${metric.title}</h2><div class="muted">${c.product} / ${c.ad_format} / ${successScopeLabel(c.success_scope)}</div></div></div>${explain(metric.desc)}<div class="panel-wrap"></div></div>`;root.appendChild(sec);renderRequestStructureDualViews(sec.querySelector('.panel-wrap'),c,renderMetric3View);}
function heatRange(values){const nums=(values||[]).filter(v=>Number.isFinite(v));if(!nums.length)return {min:0,max:1};return {min:Math.min(...nums),max:Math.max(...nums)}}
function heatStyle(value,range){const min=Number(range.min||0),max=Number(range.max||0);let norm=0;if(max>min){norm=(Number(value||0)-min)/(max-min)}else if(max>0){norm=1}norm=Math.max(0,Math.min(1,norm));const alpha=0.16+norm*0.34;const bg=`rgba(14,116,144,${alpha.toFixed(3)})`;const fg=norm>=0.62?'#f8fafc':'#102a43';return `background:${bg};color:${fg}`;}
function heatTd(share,inner,title,extraClass=''){return `<td class="heat-cell ${extraClass}" style="${heatStyle(share,window.__heat_range__)}" title="${title}">${inner}</td>`}
function renderMetric4DistributionBlock(host,title,view,countLabel,emptyLabel){const {box,toolbar,chartWrap}=createDistributionViewBox(title,'');host.appendChild(box);if(!view||!((view.count_options||[]).length)){chartWrap.innerHTML='<div class="empty">当前口径暂无结果。</div>';return}let countValue=view.count_options[0]||'',structureValue='',statusValue='';const structureOptions=()=>(((view.cnt_map||{})[countValue]||{}).structure_options||[]);const statusOptions=()=>((((((view.cnt_map||{})[countValue]||{}).structure_map||{})[structureValue]||{}).status_options)||[]);const normalize=()=>{const currentStructures=structureOptions();structureValue=currentStructures.includes(structureValue)?structureValue:(currentStructures[0]||'');const currentStatuses=statusOptions();statusValue=currentStatuses.includes(statusValue)?statusValue:(currentStatuses[0]||'')};const draw=()=>{normalize();chartWrap.innerHTML='';const current=((((((view.cnt_map||{})[countValue]||{}).structure_map||{})[structureValue]||{}).status_map||{})[statusValue]);if(!current){chartWrap.innerHTML=`<div class="empty">${emptyLabel}</div>`;return}const scroll=document.createElement('div');scroll.className='chart-scroll';const chartEl=document.createElement('div');chartEl.className='chart';const categories=current.target_options||[];chartEl.style.width=`${calcDistributionChartWidth(categories)}px`;scroll.appendChild(chartEl);chartWrap.appendChild(scroll);const chart=mountDistributionCompareChartWithCategories(chartEl,(current.groups||{})[GROUP_A]?.points||[],(current.groups||{})[GROUP_B]?.points||[],categories);new ResizeObserver(()=>chart.resize()).observe(box)};const drawToolbar=()=>{normalize();toolbar.innerHTML='';toolbar.appendChild(selectField(countLabel,view.count_options,countValue,value=>{countValue=value;drawToolbar();draw()}));toolbar.appendChild(selectField('B/W 结构（单选）',structureOptions(),structureValue,value=>{structureValue=value;drawToolbar();draw()}));toolbar.appendChild(selectField('status（单选）',statusOptions(),statusValue,value=>{statusValue=value;draw()}))};drawToolbar();draw()}
function renderMetric4(root,c){const metric=metricConfig('metric4');const sec=document.createElement('section');sec.className='metric';sec.innerHTML=`<div class="card"><div class="card-head"><div><h2>${metric.title}</h2><div class="muted">${c.product} / ${c.ad_format} / ${successScopeLabel(c.success_scope)}</div></div></div>${explain(metric.desc)}<div class="panel-wrap"></div></div>`;root.appendChild(sec);const panel=sec.querySelector('.panel-wrap');renderMetric4DistributionBlock(panel,'network 渠道分布',c.network_view,'network_cnt（单选）','当前 network 口径下暂无结果。');renderMetric4DistributionBlock(panel,'placement 分布',c.placement_view,'placement_cnt（单选）','当前 placement 口径下暂无结果。')}
function renderMetric5(root,c){const metric=metricConfig('metric5');const sec=document.createElement('section');sec.className='metric';sec.innerHTML=`<div class="card"><div class="card-head"><div><h2>${metric.title}</h2><div class="muted">${c.product} / ${c.ad_format}</div></div></div>${explain(metric.desc)}<div class="panel-wrap"></div></div>`;root.appendChild(sec);const panel=sec.querySelector('.panel-wrap');const rows=c.rows||[];if(!rows.length){panel.innerHTML='<div class="empty">当前筛选条件下暂无结果。</div>';return}const shares=[];rows.forEach(row=>{shares.push(Number((row.groups[GROUP_A]||{}).share||0));shares.push(Number((row.groups[GROUP_B]||{}).share||0))});window.__heat_range__=heatRange(shares);const totals=c.groups||{};const statusCounts={};rows.forEach(row=>{statusCounts[row.status]=(statusCounts[row.status]||0)+1});let headTop='<tr><th rowspan="2" class="status-network-head metric4-status">status</th><th rowspan="2" class="status-network-head metric4-network">network</th><th colspan="2">share</th><th colspan="2">pv</th><th colspan="2">total</th></tr>';let headBottom='<tr><th>A组</th><th>B组</th><th>A组</th><th>B组</th><th>A组</th><th>B组</th></tr>';let body='';const seen={};rows.forEach(row=>{const rowSpan=statusCounts[row.status]||1;const first=!seen[row.status];seen[row.status]=true;const left=row.groups[GROUP_A]||{request_pv:0,share:0};const right=row.groups[GROUP_B]||{request_pv:0,share:0};const leftTotal=((totals[GROUP_A]||{}).denominator_request_pv)||0;const rightTotal=((totals[GROUP_B]||{}).denominator_request_pv)||0;const leftShare=heatTd(left.share,fmtPct(left.share),`${row.status} / ${GROUPS[GROUP_A]} share=${fmtPct(left.share)}`,'metric4-share');const rightShare=heatTd(right.share,fmtPct(right.share),`${row.status} / ${GROUPS[GROUP_B]} share=${fmtPct(right.share)}`,'metric4-share');body+=`<tr class="${first?'group-divider':''}">${first?`<td class="status-cell metric4-status" rowspan="${rowSpan}" title="${row.status}">${row.status}</td>`:''}<td class="network-cell metric4-network" title="${row.network}">${row.network}</td>${leftShare}${rightShare}<td class="metric4-num">${fmtNum(left.request_pv)}</td><td class="metric4-num">${fmtNum(right.request_pv)}</td><td class="metric4-num">${fmtNum(leftTotal)}</td><td class="metric4-num">${fmtNum(rightTotal)}</td></tr>`});panel.innerHTML=`<div class="detail-card table-card"><div class="table-wrap metric4-wrap"><table class="metric-table metric4-table"><thead>${headTop}${headBottom}</thead><tbody>${body}</tbody></table></div></div>`}
function renderMetric6(root,c){const metric=metricConfig('metric6');let nullMode='include_null';const sec=document.createElement('section');sec.className='metric';sec.innerHTML=`<div class="card"><div class="card-head"><div><h2>${metric.title}</h2><div class="muted">${c.product} / ${c.ad_format}</div></div></div>${explain(metric.desc)}<div class="toolbar"></div><div class="panel-wrap"></div></div>`;root.appendChild(sec);const toolbar=sec.querySelector('.toolbar'),panel=sec.querySelector('.panel-wrap');const rows=c.rows||[];if(!rows.length){panel.innerHTML='<div class="empty">当前筛选条件下暂无结果。</div>';return}const allStatusBuckets=metric.status_buckets||[];const realStatusBuckets=allStatusBuckets.filter(status=>status!=='NULL');const pickMetric=(groupPayload,status)=>{const current=(groupPayload?.statuses?.[status])||{request_pv:0,share:0};if(nullMode==='exclude_null'){const denominator=realStatusBuckets.reduce((sum,key)=>sum+Number((groupPayload?.statuses?.[key]||{}).request_pv||0),0);const requestPv=Number(current.request_pv||0);return {request_pv:requestPv,share:denominator?requestPv/denominator:0}}return {request_pv:Number(current.request_pv||0),share:Number(current.share||0)}};const visibleStatuses=()=>nullMode==='exclude_null'?realStatusBuckets:allStatusBuckets;const draw=()=>{const statusBuckets=visibleStatuses();const shares=[];rows.forEach(row=>{statusBuckets.forEach(status=>{shares.push(pickMetric((row.groups||{})[GROUP_A]||{statuses:{}},status).share);shares.push(pickMetric((row.groups||{})[GROUP_B]||{statuses:{}},status).share)})});window.__heat_range__=heatRange(shares);let headTop='<tr><th rowspan="2" class="status-network-head sticky-col sticky-col-1">type</th><th rowspan="2" class="status-network-head sticky-col sticky-col-2">network</th>';statusBuckets.forEach(status=>{headTop+=`<th colspan="2" title="${status}">${status}</th>`});headTop+='</tr>';let headBottom='<tr>';statusBuckets.forEach(()=>{headBottom+=`<th>${GROUPS[GROUP_A]}</th><th>${GROUPS[GROUP_B]}</th>`});headBottom+='</tr>';let body='';rows.forEach(row=>{body+=`<tr><td class="status-cell sticky-col sticky-col-1" title="${row.network_type}">${row.network_type}</td><td class="network-cell sticky-col sticky-col-2" title="${row.network}">${row.network}</td>`;statusBuckets.forEach(status=>{const left=pickMetric((row.groups||{})[GROUP_A]||{statuses:{}},status);const right=pickMetric((row.groups||{})[GROUP_B]||{statuses:{}},status);const leftInner=`<div class="share-main">${fmtPct(left.share)}</div><div class="pv-sub">pv ${fmtNum(left.request_pv)}</div>`;const rightInner=`<div class="share-main">${fmtPct(right.share)}</div><div class="pv-sub">pv ${fmtNum(right.request_pv)}</div>`;body+=heatTd(left.share,leftInner,`${status} / ${GROUPS[GROUP_A]} share=${fmtPct(left.share)} / pv=${fmtNum(left.request_pv)}`,'metric5-value');body+=heatTd(right.share,rightInner,`${status} / ${GROUPS[GROUP_B]} share=${fmtPct(right.share)} / pv=${fmtNum(right.request_pv)}`,'metric5-value')});body+='</tr>'});panel.innerHTML=`<div class="detail-card table-card"><div class="table-wrap metric5-wrap"><table class="metric-table metric5-table"><thead>${headTop}${headBottom}</thead><tbody>${body}</tbody></table></div></div>`};const drawToolbar=()=>{toolbar.innerHTML='';const label=nullMode==='exclude_null'?'不考虑 NULL':'考虑 NULL';toolbar.appendChild(selectField('是否考虑 NULL（单选）',['考虑 NULL','不考虑 NULL'],label,value=>{nullMode=value==='不考虑 NULL'?'exclude_null':'include_null';drawToolbar();draw()}))};drawToolbar();draw()}
function initSuccessScopeSelect(){const select=document.getElementById('success-scope-select');if(!select)return null;const options=(DATA.success_scopes||[]).length?(DATA.success_scopes||[]):['all'];select.innerHTML=options.map(value=>`<option value="${value}">${successScopeLabel(value)}</option>`).join('');return select;}
function render(){setError('');const root=document.getElementById('root'),product=document.getElementById('product-select').value,adFormat=document.getElementById('format-select').value,successScope=(document.getElementById('success-scope-select')||{}).value||'all',m1=metricConfig('metric1'),m2=metricConfig('metric2'),m3=metricConfig('metric3'),m4=metricConfig('metric4'),m5=metricConfig('metric5'),m6=metricConfig('metric6'),c1=combo('metric1',product,adFormat,'',successScope),c2=combo('metric2',product,adFormat,'',successScope),c3=combo('metric3',product,adFormat,'',successScope),c4=combo('metric4',product,adFormat,'',successScope),c5=combo('metric5',product,adFormat),c6=combo('metric6',product,adFormat);root.innerHTML='';if(!product||!adFormat){setError('当前结果缺少 product 或 ad_format。');return}if(c1){renderMetric1(root,c1);renderMetric1RankBlock(root,c1,`${c1.product} / ${c1.ad_format} / ${successScopeLabel(c1.success_scope)}`)}else if(m1)root.insertAdjacentHTML('beforeend',metricEmpty(m1.title,m1.desc,product,adFormat,'metric1'));if(c2)renderMetric2(root,c2);else if(m2)root.insertAdjacentHTML('beforeend',metricEmpty(m2.title,m2.desc,product,adFormat,'metric2'));if(c3)renderMetric3(root,c3);else if(m3)root.insertAdjacentHTML('beforeend',metricEmpty(m3.title,m3.desc,product,adFormat,'metric3'));if(c4)renderMetric4(root,c4);else if(m4)root.insertAdjacentHTML('beforeend',metricEmpty(m4.title,m4.desc,product,adFormat,'metric4'));if(c5)renderMetric5(root,c5);else if(m5)root.insertAdjacentHTML('beforeend',metricEmpty(m5.title,m5.desc,product,adFormat,'metric5'));if(c6)renderMetric6(root,c6);else if(m6)root.insertAdjacentHTML('beforeend',metricEmpty(m6.title,m6.desc,product,adFormat,'metric6'))}
const controls=productFormat(),successSelect=initSuccessScopeSelect();controls.product.onchange=render;controls.format.onchange=render;if(successSelect)successSelect.onchange=render;render();
"""


def build_request_structure_country_page_script() -> str:
    return """
function buildBucketCategories(groupAPoints,groupBPoints){
  const categorySet=new Set();
  [...(groupAPoints||[]),...(groupBPoints||[])].forEach(point=>{
    const bucketKey=String(point?.bucket_key??'');
    if(bucketKey!=='')categorySet.add(bucketKey);
  });
  return [...categorySet].sort((left,right)=>{
    const leftNum=Number(left),rightNum=Number(right);
    const leftIsNumeric=left!==''&&Number.isFinite(leftNum),rightIsNumeric=right!==''&&Number.isFinite(rightNum);
    if(leftIsNumeric&&rightIsNumeric)return leftNum-rightNum;
    if(leftIsNumeric!==rightIsNumeric)return leftIsNumeric?-1:1;
    return left.localeCompare(right,undefined,{numeric:true,sensitivity:'base'});
  });
}
function buildDistributionSeriesData(points,categories){
  const pointMap=new Map();
  (points||[]).forEach(point=>{pointMap.set(String(point?.bucket_key??''),point)});
  return categories.map(bucketKey=>{
    const point=pointMap.get(bucketKey);
    return {value:Number(((point?.share)||0).toFixed(6)),bucketKey,share:Number(point?.share||0),requestPv:Number(point?.request_pv||0),denominatorRequestPv:Number(point?.denominator_request_pv||0)};
  });
}
function buildDistributionCompareOptionWithCategories(groupAPoints,groupBPoints,categories,axisMax=null){
  const basePoints=(groupAPoints&&groupAPoints.length)?groupAPoints:(groupBPoints||[]);
  const axisTitle=String(basePoints[0]?.axis_label||'bucket');
  const groupASeriesData=buildDistributionSeriesData(groupAPoints,categories);
  const groupBSeriesData=buildDistributionSeriesData(groupBPoints,categories);
  const resolvedAxisMax=resolveDistributionAxisMax(axisMax);
  return {
    backgroundColor:'rgba(252,250,246,0.95)',
    grid:{left:54,right:12,top:28,bottom:65},
    legend:{show:true,top:0,textStyle:{fontSize:11,color:'#667788'}},
    tooltip:{
      trigger:'axis',
      axisPointer:{type:'shadow'},
      backgroundColor:'rgba(255,255,255,0.96)',
      borderColor:'rgba(31,49,64,0.14)',
      textStyle:{fontSize:12,color:'#1f3140'},
      formatter(params){
        if(!params||!params.length)return '';
        const a=params.find(item=>item.seriesName===GROUPS[GROUP_A])||{data:{}};
        const b=params.find(item=>item.seriesName===GROUPS[GROUP_B])||{data:{}};
        const bucketKey=a.data.bucketKey||b.data.bucketKey||params[0].axisValue||'';
        const delta=Number((a.data.share||0)-(b.data.share||0));
        const sign=delta>0?'+':'';
        return `<div style="min-width:240px"><div style="margin-bottom:6px;font-weight:600">${axisTitle}: ${bucketKey}</div><table style="width:100%;border-collapse:collapse;font-size:12px"><tr><td style="color:#667788;padding:2px 6px 2px 0">${GROUPS[GROUP_A]} pv</td><td style="text-align:right">${fmtNum(a.data.requestPv||0)}</td></tr><tr><td style="color:#667788;padding:2px 6px 2px 0">${GROUPS[GROUP_A]} 分母</td><td style="text-align:right">${fmtNum(a.data.denominatorRequestPv||0)}</td></tr><tr><td style="color:#667788;padding:2px 6px 2px 0">${GROUPS[GROUP_A]} share</td><td style="text-align:right;font-weight:600">${fmtPct(a.data.share||0)}</td></tr><tr><td style="color:#667788;padding:2px 6px 2px 0">${GROUPS[GROUP_B]} pv</td><td style="text-align:right">${fmtNum(b.data.requestPv||0)}</td></tr><tr><td style="color:#667788;padding:2px 6px 2px 0">${GROUPS[GROUP_B]} 分母</td><td style="text-align:right">${fmtNum(b.data.denominatorRequestPv||0)}</td></tr><tr><td style="color:#667788;padding:2px 6px 2px 0">${GROUPS[GROUP_B]} share</td><td style="text-align:right;font-weight:600">${fmtPct(b.data.share||0)}</td></tr><tr><td style="color:#667788;padding:2px 6px 2px 0">share 差值</td><td style="text-align:right;color:${delta>0?'#0f766e':'#e11d48'}">${sign}${fmtPct(delta)}</td></tr></table></div>`;
      }
    },
    xAxis:{type:'category',data:categories,axisLine:{lineStyle:{color:'rgba(31,49,64,0.12)'}},axisTick:{show:false},axisLabel:{color:'#667788',fontSize:11,interval:0}},
    yAxis:{type:'value',min:0,max:resolvedAxisMax,axisLabel:{formatter:value=>`${Math.round(value*100)}%`,color:'#667788',fontSize:11},splitLine:{lineStyle:{color:'rgba(31,49,64,0.07)'}}},
    series:[
      {name:GROUPS[GROUP_A],type:'bar',barMaxWidth:30,itemStyle:{color:'#0f766e'},data:groupASeriesData},
      {name:GROUPS[GROUP_B],type:'bar',barMaxWidth:30,itemStyle:{color:'#2563eb'},data:groupBSeriesData}
    ]
  };
}
function buildDistributionCompareOption(groupAPoints,groupBPoints,axisMax=null){return buildDistributionCompareOptionWithCategories(groupAPoints,groupBPoints,buildBucketCategories(groupAPoints,groupBPoints),axisMax);}
function bindDistributionHover(chart){chart.getZr().on('mousemove', function(e){const pointInGrid=chart.convertFromPixel({seriesIndex:0}, [e.offsetX, e.offsetY]);if(pointInGrid) window._current_mouse_y = pointInGrid[1];});}
function resolveDistributionAxisMax(axisMax){return axisMax!==null?axisMax:1;}
function mountDistributionCompareChart(host,groupAPoints,groupBPoints,axisMax=null){const chart=echarts.init(host);chart.setOption(buildDistributionCompareOption(groupAPoints,groupBPoints,axisMax));bindDistributionHover(chart);return chart;}
function mountDistributionCompareChartWithCategories(host,groupAPoints,groupBPoints,categories,axisMax=null){const chart=echarts.init(host);chart.setOption(buildDistributionCompareOptionWithCategories(groupAPoints,groupBPoints,categories,axisMax));bindDistributionHover(chart);return chart;}
function createDistributionViewBox(title,subtitle){const box=document.createElement('div');box.className='chart-box';box.innerHTML=`<div class="detail-top"><h4>${title}</h4>${subtitle?`<p>${subtitle}</p>`:''}</div><div class="toolbar"></div><div class="chart-wrap"></div>`;return {box,toolbar:box.querySelector('.toolbar'),chartWrap:box.querySelector('.chart-wrap')};}
function calcMetric1ChartWidth(view){void view;return 0;}
function calcDistributionChartWidth(categories){return Math.max(960,Math.max(1,(categories||[]).length)*88);}
function scrollableDistributionCategories(current){return current.bucket_options||buildBucketCategories((current.groups||{})[GROUP_A]?.points||[],(current.groups||{})[GROUP_B]?.points||[]);}
function appendScrollableDistributionChart(chartWrap,box,current,axisMax=null){const categories=scrollableDistributionCategories(current);const scroll=document.createElement('div');scroll.className='chart-scroll';const chartEl=document.createElement('div');chartEl.className='chart';chartEl.style.width=`${calcDistributionChartWidth(categories)}px`;scroll.appendChild(chartEl);chartWrap.appendChild(scroll);const chart=mountDistributionCompareChartWithCategories(chartEl,(current.groups||{})[GROUP_A]?.points||[],(current.groups||{})[GROUP_B]?.points||[],categories,axisMax);new ResizeObserver(()=>chart.resize()).observe(box);}
function renderMetric1View(host,title,view,axisMax=null,subtitleOverride='',chartWidth=null){const subtitle=subtitleOverride||'';const {box,chartWrap}=createDistributionViewBox(title,subtitle);host.appendChild(box);if(!view||!((view.count_options||[]).length)){chartWrap.innerHTML='<div class="empty">当前口径暂无结果。</div>';return}const scroll=document.createElement('div');scroll.className='chart-scroll';const chartEl=document.createElement('div');chartEl.className='chart';void chartWidth;scroll.appendChild(chartEl);chartWrap.appendChild(scroll);const chart=mountDistributionCompareChart(chartEl,(view.groups||{})[GROUP_A]?.points||[],(view.groups||{})[GROUP_B]?.points||[],axisMax);new ResizeObserver(()=>chart.resize()).observe(box);}
function buildMetric1RankSummaryTag(groupKey,summary){const tag=document.createElement('div');tag.className='condition-tag';tag.innerHTML=`<strong>${GROUPS[groupKey]}</strong>&nbsp;成功 ${fmtNum(summary.success_request_pv||0)} / 总桶 ${fmtNum(summary.bucket_total_request_pv||0)} / 成功率 ${fmtPct(summary.success_rate||0)}`;return tag;}
function renderMetric1RankView(host,title,view){const {box,toolbar,chartWrap}=createDistributionViewBox(title,'图内分母 = 当前 cnt 桶内成功 request pv；右上角显示成功 pv / 当前 cnt 总桶 pv / 成功率。');host.appendChild(box);if(!view||!((view.cnt_options||[]).length)){chartWrap.innerHTML='<div class="empty">当前口径暂无结果。</div>';return}let cntValue=view.default_cnt||view.cnt_options[0]||'';const countLabel=view.cnt_type==='placement'?'placement_cnt（单选）':'network_cnt（单选）';const draw=()=>{const current=(view.bucket_map||{})[cntValue];toolbar.innerHTML='';toolbar.appendChild(selectField(countLabel,view.cnt_options,cntValue,value=>{cntValue=value;draw()}));chartWrap.innerHTML='';if(!current){chartWrap.innerHTML='<div class="empty">当前筛选条件下暂无结果。</div>';return}toolbar.appendChild(buildMetric1RankSummaryTag(GROUP_A,((current.groups||{})[GROUP_A]||{}).summary||{}));toolbar.appendChild(buildMetric1RankSummaryTag(GROUP_B,((current.groups||{})[GROUP_B]||{}).summary||{}));const categories=current.rank_options||buildBucketCategories((current.groups||{})[GROUP_A]?.points||[],(current.groups||{})[GROUP_B]?.points||[]);const scroll=document.createElement('div');scroll.className='chart-scroll';const chartEl=document.createElement('div');chartEl.className='chart';chartEl.style.width=`${calcDistributionChartWidth(categories)}px`;scroll.appendChild(chartEl);chartWrap.appendChild(scroll);const chart=mountDistributionCompareChartWithCategories(chartEl,(current.groups||{})[GROUP_A]?.points||[],(current.groups||{})[GROUP_B]?.points||[],categories,current.axis_max??null);new ResizeObserver(()=>chart.resize()).observe(box)};draw();}
function renderMetric1RankBlock(root,c,subtitle){const metric=metricConfig('metric1');const rankBlock=c.rank_block||{};if(!rankBlock.network_rank_view&&!rankBlock.placement_rank_view)return;const sec=document.createElement('section');sec.className='metric';sec.innerHTML=`<div class="card"><div class="card-head"><div><h2>成功 request 的 success_rank 分布</h2><div class="muted">${subtitle}</div></div></div>${explain(metric.rank_desc||[])}<div class="panel-wrap"></div></div>`;root.appendChild(sec);const pair=document.createElement('div');pair.className='chart-pair';sec.querySelector('.panel-wrap').appendChild(pair);renderMetric1RankView(pair,'network success_rank 分布',rankBlock.network_rank_view);renderMetric1RankView(pair,'placement success_rank 分布',rankBlock.placement_rank_view);}
function renderMetric2View(host,title,view){const {box,toolbar,chartWrap}=createDistributionViewBox(title,'');host.appendChild(box);if(!view||!((view.count_options||[]).length)){chartWrap.innerHTML='<div class="empty">当前口径暂无结果。</div>';return}let countValue=view.count_options[0]||'';const draw=()=>{chartWrap.innerHTML='';const current=(view.cnt_map||{})[countValue];if(!current){chartWrap.innerHTML='<div class="empty">当前筛选条件下暂无结果。</div>';return}const chartEl=document.createElement('div');chartEl.className='chart';chartWrap.appendChild(chartEl);const chart=mountDistributionCompareChart(chartEl,(current.groups||{})[GROUP_A]?.points||[],(current.groups||{})[GROUP_B]?.points||[]);new ResizeObserver(()=>chart.resize()).observe(box)};toolbar.innerHTML='';toolbar.appendChild(selectField(`${view.count_label}（单选）`,view.count_options,countValue,value=>{countValue=value;draw()}));draw();}
function renderMetric3View(host,title,view){const {box,toolbar,chartWrap}=createDistributionViewBox(title,'');host.appendChild(box);if(!view||!((view.count_options||[]).length)){chartWrap.innerHTML='<div class="empty">当前口径暂无结果。</div>';return}let countValue=view.count_options[0]||'',networkType='',typeCount='';const countLabel=view.count_label==='placement_cnt'?'placement_cnt（单选）':'network_cnt（单选）';const typeCountLabel=view.type_count_label==='type_placement_cnt'?'type_placement_cnt（单选）':'type_network_cnt（单选）';const typeOptions=()=>(((view.cnt_map||{})[countValue]||{}).network_type_options||[]);const typeCountOptions=()=>(((((view.cnt_map||{})[countValue]||{}).type_map||{})[networkType]||{}).type_count_options||[]);const normalize=()=>{const currentTypes=typeOptions();networkType=currentTypes.includes(networkType)?networkType:(currentTypes[0]||'');const currentTypeCounts=typeCountOptions();typeCount=currentTypeCounts.includes(typeCount)?typeCount:(currentTypeCounts[0]||'')};const draw=()=>{normalize();chartWrap.innerHTML='';const current=((((((view.cnt_map||{})[countValue]||{}).type_map||{})[networkType]||{}).type_cnt_map||{})[typeCount]);if(!current){chartWrap.innerHTML='<div class="empty">当前筛选条件下暂无结果。</div>';return}const chartEl=document.createElement('div');chartEl.className='chart';chartWrap.appendChild(chartEl);const chart=mountDistributionCompareChart(chartEl,(current.groups||{})[GROUP_A]?.points||[],(current.groups||{})[GROUP_B]?.points||[]);new ResizeObserver(()=>chart.resize()).observe(box)};const drawToolbar=()=>{normalize();toolbar.innerHTML='';toolbar.appendChild(selectField(countLabel,view.count_options,countValue,value=>{countValue=value;drawToolbar();draw()}));toolbar.appendChild(selectField('network_type（单选）',typeOptions(),networkType,value=>{networkType=value;drawToolbar();draw()}));toolbar.appendChild(selectField(typeCountLabel,typeCountOptions(),typeCount,value=>{typeCount=value;draw()}))};drawToolbar();draw();}
function renderRequestStructureDualViews(panel,comboPayload,renderer,sharedAxisMax=null){const card=document.createElement('div');card.className='detail-card';const pair=document.createElement('div');pair.className='chart-pair';card.appendChild(pair);panel.appendChild(card);renderer(pair,'network个数',comboPayload.network_view,sharedAxisMax);renderer(pair,'placement个数',comboPayload.placement_view,sharedAxisMax);}
function renderMetric1(root,c){const metric=metricConfig('metric1');const sec=document.createElement('section');sec.className='metric';sec.innerHTML=`<div class="card"><div class="card-head"><div><h2>${metric.title}</h2><div class="muted">${c.product} / ${c.ad_format} / ${c.country}</div></div></div>${explain(metric.desc)}<div class="panel-wrap"></div></div>`;root.appendChild(sec);const stack=document.createElement('div');stack.className='chart-stack';sec.querySelector('.panel-wrap').appendChild(stack);renderMetric1View(stack,'network个数',c.network_view,c.axis_max??null,'',calcMetric1ChartWidth(c.network_view));renderMetric1View(stack,'placement个数',c.placement_view,c.axis_max??null,'',calcMetric1ChartWidth(c.placement_view));}
function renderMetric2(root,c){const metric=metricConfig('metric2');const sec=document.createElement('section');sec.className='metric';sec.innerHTML=`<div class="card"><div class="card-head"><div><h2>${metric.title}</h2><div class="muted">${c.product} / ${c.ad_format} / ${c.country}</div></div></div>${explain(metric.desc)}<div class="panel-wrap"></div></div>`;root.appendChild(sec);renderRequestStructureDualViews(sec.querySelector('.panel-wrap'),c,renderMetric2View);}
function renderMetric3(root,c){const metric=metricConfig('metric3');const sec=document.createElement('section');sec.className='metric';sec.innerHTML=`<div class="card"><div class="card-head"><div><h2>${metric.title}</h2><div class="muted">${c.product} / ${c.ad_format} / ${c.country}</div></div></div>${explain(metric.desc)}<div class="panel-wrap"></div></div>`;root.appendChild(sec);renderRequestStructureDualViews(sec.querySelector('.panel-wrap'),c,renderMetric3View);}
function heatRange(values){const nums=(values||[]).filter(v=>Number.isFinite(v));if(!nums.length)return {min:0,max:1};return {min:Math.min(...nums),max:Math.max(...nums)}}
function heatStyle(value,range){const min=Number(range.min||0),max=Number(range.max||0);let norm=0;if(max>min){norm=(Number(value||0)-min)/(max-min)}else if(max>0){norm=1}norm=Math.max(0,Math.min(1,norm));const alpha=0.16+norm*0.34;const bg=`rgba(14,116,144,${alpha.toFixed(3)})`;const fg=norm>=0.62?'#f8fafc':'#102a43';return `background:${bg};color:${fg}`;}
function heatTd(share,inner,title,extraClass=''){return `<td class="heat-cell ${extraClass}" style="${heatStyle(share,window.__heat_range__)}" title="${title}">${inner}</td>`}
function renderMetric4DistributionBlock(host,title,view,countLabel,emptyLabel){const {box,toolbar,chartWrap}=createDistributionViewBox(title,'');host.appendChild(box);if(!view||!((view.count_options||[]).length)){chartWrap.innerHTML='<div class="empty">当前口径暂无结果。</div>';return}let countValue=view.count_options[0]||'',structureValue='',statusValue='';const structureOptions=()=>(((view.cnt_map||{})[countValue]||{}).structure_options||[]);const statusOptions=()=>((((((view.cnt_map||{})[countValue]||{}).structure_map||{})[structureValue]||{}).status_options)||[]);const normalize=()=>{const currentStructures=structureOptions();structureValue=currentStructures.includes(structureValue)?structureValue:(currentStructures[0]||'');const currentStatuses=statusOptions();statusValue=currentStatuses.includes(statusValue)?statusValue:(currentStatuses[0]||'')};const draw=()=>{normalize();chartWrap.innerHTML='';const current=((((((view.cnt_map||{})[countValue]||{}).structure_map||{})[structureValue]||{}).status_map||{})[statusValue]);if(!current){chartWrap.innerHTML=`<div class="empty">${emptyLabel}</div>`;return}const scroll=document.createElement('div');scroll.className='chart-scroll';const chartEl=document.createElement('div');chartEl.className='chart';const categories=current.target_options||[];chartEl.style.width=`${calcDistributionChartWidth(categories)}px`;scroll.appendChild(chartEl);chartWrap.appendChild(scroll);const chart=mountDistributionCompareChartWithCategories(chartEl,(current.groups||{})[GROUP_A]?.points||[],(current.groups||{})[GROUP_B]?.points||[],categories);new ResizeObserver(()=>chart.resize()).observe(box)};const drawToolbar=()=>{normalize();toolbar.innerHTML='';toolbar.appendChild(selectField(countLabel,view.count_options,countValue,value=>{countValue=value;drawToolbar();draw()}));toolbar.appendChild(selectField('B/W 结构（单选）',structureOptions(),structureValue,value=>{structureValue=value;drawToolbar();draw()}));toolbar.appendChild(selectField('status（单选）',statusOptions(),statusValue,value=>{statusValue=value;draw()}))};drawToolbar();draw()}
function renderMetric4(root,c){const metric=metricConfig('metric4');const sec=document.createElement('section');sec.className='metric';sec.innerHTML=`<div class="card"><div class="card-head"><div><h2>${metric.title}</h2><div class="muted">${c.product} / ${c.ad_format} / ${c.country}</div></div></div>${explain(metric.desc)}<div class="panel-wrap"></div></div>`;root.appendChild(sec);const panel=sec.querySelector('.panel-wrap');renderMetric4DistributionBlock(panel,'network 渠道分布',c.network_view,'network_cnt（单选）','当前 network 口径下暂无结果。');renderMetric4DistributionBlock(panel,'placement 分布',c.placement_view,'placement_cnt（单选）','当前 placement 口径下暂无结果。')}
function renderCountry(){setError('');const root=document.getElementById('root'),product=document.getElementById('product-select').value,adFormat=document.getElementById('format-select').value,country=document.getElementById('country-select').value,m1=metricConfig('metric1'),m2=metricConfig('metric2'),m3=metricConfig('metric3'),m4=metricConfig('metric4'),c1=combo('metric1',product,adFormat,country),c2=combo('metric2',product,adFormat,country),c3=combo('metric3',product,adFormat,country),c4=combo('metric4',product,adFormat,country);root.innerHTML='';if(!product||!adFormat||!country){setError('当前结果缺少 product、ad_format 或 country。');return}if(c1){renderMetric1(root,c1);renderMetric1RankBlock(root,c1,`${c1.product} / ${c1.ad_format} / ${c1.country}`)}else if(m1)root.insertAdjacentHTML('beforeend',metricEmpty(m1.title,m1.desc,product,adFormat,'metric1'));if(c2)renderMetric2(root,c2);else if(m2)root.insertAdjacentHTML('beforeend',metricEmpty(m2.title,m2.desc,product,adFormat,'metric2'));if(c3)renderMetric3(root,c3);else if(m3)root.insertAdjacentHTML('beforeend',metricEmpty(m3.title,m3.desc,product,adFormat,'metric3'));if(c4)renderMetric4(root,c4);else if(m4)root.insertAdjacentHTML('beforeend',metricEmpty(m4.title,m4.desc,product,adFormat,'metric4'));}
const controls=productFormat();const countrySelect=document.getElementById('country-select');
function syncCountry(){const options=countryOptions(controls.product.value,controls.format.value);countrySelect.innerHTML=(options||[]).map(v=>`<option value="${v}">${v}</option>`).join('');countrySelect.value=(options||[]).includes(countrySelect.value)?countrySelect.value:((options||[])[0]||'');}
controls.product.onchange=()=>{syncCountry();renderCountry();};controls.format.onchange=()=>{syncCountry();renderCountry();};countrySelect.onchange=renderCountry;syncCountry();renderCountry();
"""


def build_request_structure_unit_page_script() -> str:
    return (
        build_request_structure_country_page_script()
        .replace("renderCountry", "renderUnit")
        .replace("syncCountry", "syncUnit")
        .replace("country-select", "unit-select")
        .replace("countryOptions", "unitOptions")
        .replace("countrySelect", "unitSelect")
        .replace("country=document.getElementById('unit-select').value", "unit=document.getElementById('unit-select').value")
        .replace("country=document.getElementById('country-select').value", "unit=document.getElementById('unit-select').value")
        .replace("combo('metric1',product,adFormat,country)", "combo('metric1',product,adFormat,unit)")
        .replace("combo('metric2',product,adFormat,country)", "combo('metric2',product,adFormat,unit)")
        .replace("combo('metric3',product,adFormat,country)", "combo('metric3',product,adFormat,unit)")
        .replace("combo('metric4',product,adFormat,country)", "combo('metric4',product,adFormat,unit)")
        .replace("if(!product||!adFormat||!country)", "if(!product||!adFormat||!unit)")
        .replace("当前结果缺少 product、ad_format 或 country。", "当前结果缺少 product、ad_format 或 unit。")
        .replace("const options=countryOptions(controls.product.value,controls.format.value);", "const options=unitOptions(controls.product.value,controls.format.value);")
    )


def build_coverage_analysis_page_script() -> str:
    return """
function renderMetric1(root,c){const metric=metricConfig('metric1');const sec=document.createElement('section');sec.className='metric';sec.innerHTML=`<div class="card"><div class="card-head"><div><h2>${metric.title}</h2><div class="muted">${c.product} / ${c.ad_format}</div></div></div>${explain(metric.desc)}<div class="panel-wrap"></div></div>`;root.appendChild(sec);mountPair(sec.querySelector('.panel-wrap'),metric.series_keys,c.groups[GROUP_A].points,c.groups[GROUP_B].points,'不同 network_cnt 的整体桶占比','图中每个 N 都表示当前 req_index 下落在该 network_cnt 桶中的请求占比')}
function renderMetric2(root,c){const metric=metricConfig('metric2');let networkCnt=c.network_cnt_options[0]||'';const sec=document.createElement('section');sec.className='metric';sec.innerHTML=`<div class="card"><div class="card-head"><div><h2>${metric.title}</h2><div class="muted">${c.product} / ${c.ad_format}</div></div></div>${explain(metric.desc)}<div class="toolbar"></div><div class="panel-wrap"></div></div>`;root.appendChild(sec);const toolbar=sec.querySelector('.toolbar'),panel=sec.querySelector('.panel-wrap');const draw=()=>{panel.innerHTML='';const cur=(c.cnt_map||{})[networkCnt];if(!cur){panel.innerHTML='<div class="empty">当前 network_cnt 下暂无结果。</div>';return}mountPair(panel,cur.series_keys,cur.groups[GROUP_A].points,cur.groups[GROUP_B].points,'bidding / waterfall 覆盖率',`当前固定 network_cnt=${networkCnt}；分母为当前 req_index + network_cnt 桶的请求总数`,'coverage',cur.axis_max??null)};toolbar.appendChild(selectField('network_cnt（单选）',c.network_cnt_options,networkCnt,value=>{networkCnt=value;draw()}));draw()}
function renderMetric3(root,c){const metric=metricConfig('metric3');let networkCnt=c.network_cnt_options[0]||'';const sec=document.createElement('section');sec.className='metric';sec.innerHTML=`<div class="card"><div class="card-head"><div><h2>${metric.title}</h2><div class="muted">${c.product} / ${c.ad_format}</div></div></div>${explain(metric.desc)}<div class="toolbar"></div><div class="panel-wrap"></div></div>`;root.appendChild(sec);const toolbar=sec.querySelector('.toolbar'),panel=sec.querySelector('.panel-wrap');const draw=()=>{panel.innerHTML='';const cur=(c.cnt_map||{})[networkCnt];if(!cur){panel.innerHTML='<div class="empty">当前 network_cnt 下暂无结果。</div>';return}mountPair(panel,cur.series_keys,cur.groups[GROUP_A].points,cur.groups[GROUP_B].points,'status 覆盖率',`当前固定 network_cnt=${networkCnt}；各状态覆盖率相加可能大于 100%，这是正常现象。`,'coverage',cur.axis_max??null)};toolbar.appendChild(selectField('network_cnt（单选）',c.network_cnt_options,networkCnt,value=>{networkCnt=value;draw()}));draw()}
function renderMetric4(root,c){const metric=metricConfig('metric4');let networkCnt=c.network_cnt_options[0]||'';const sec=document.createElement('section');sec.className='metric';sec.innerHTML=`<div class="card"><div class="card-head"><div><h2>${metric.title}</h2><div class="muted">${c.product} / ${c.ad_format}</div></div></div>${explain(metric.desc)}<div class="toolbar"></div><div class="panel-wrap"></div></div>`;root.appendChild(sec);const toolbar=sec.querySelector('.toolbar'),panel=sec.querySelector('.panel-wrap');const draw=()=>{panel.innerHTML='';const cur=(c.cnt_map||{})[networkCnt];if(!cur){panel.innerHTML='<div class="empty">当前 network_cnt 下暂无结果。</div>';return}(metric.network_types||[]).forEach(type=>{const block=document.createElement('div');block.className='type-panel';const typeLabel=type==='bidding'?'bidding 下的 status 覆盖率':'waterfall 下的 status 覆盖率';const typePayload=(cur.type_map||{})[type];block.innerHTML=`<div class="detail-top"><h4>${typeLabel}</h4><p>当前固定 network_cnt=${networkCnt}；分母为当前 network_type 内的请求总数。</p></div>`;panel.appendChild(block);if(!typePayload){block.innerHTML+=`<div class="empty">${type} 在当前 network_cnt 下暂无结果。</div>`;return}mountPair(block,typePayload.series_keys,typePayload.groups[GROUP_A].points,typePayload.groups[GROUP_B].points,typeLabel,`当前固定 network_cnt=${networkCnt}；各状态覆盖率相加可能大于 100%，这是正常现象。`,'coverage',typePayload.axis_max??null)})};toolbar.appendChild(selectField('network_cnt（单选）',c.network_cnt_options,networkCnt,value=>{networkCnt=value;draw()}));draw()}
function render(){setError('');const root=document.getElementById('root'),product=document.getElementById('product-select').value,adFormat=document.getElementById('format-select').value,m1=metricConfig('metric1'),m2=metricConfig('metric2'),m3=metricConfig('metric3'),m4=metricConfig('metric4'),c1=combo('metric1',product,adFormat),c2=combo('metric2',product,adFormat),c3=combo('metric3',product,adFormat),c4=combo('metric4',product,adFormat);root.innerHTML='';if(!product||!adFormat){setError('当前结果缺少 product 或 ad_format。');return}if(c1)renderMetric1(root,c1);else if(m1)root.insertAdjacentHTML('beforeend',metricEmpty(m1.title,m1.desc,product,adFormat,'metric1'));if(c2)renderMetric2(root,c2);else if(m2)root.insertAdjacentHTML('beforeend',metricEmpty(m2.title,m2.desc,product,adFormat,'metric2'));if(c3)renderMetric3(root,c3);else if(m3)root.insertAdjacentHTML('beforeend',metricEmpty(m3.title,m3.desc,product,adFormat,'metric3'));if(c4)renderMetric4(root,c4);else if(m4)root.insertAdjacentHTML('beforeend',metricEmpty(m4.title,m4.desc,product,adFormat,'metric4'))}
const controls=productFormat();controls.product.onchange=render;controls.format.onchange=render;render();
"""


def build_dashboard_html(
    *,
    page_title: str | None = None,
    metrics: dict[str, Any],
    products: list[str],
    ad_formats: list[str],
    page_key: str,
    countries: list[str] | None = None,
    country_options_by_combo: dict[str, list[str]] | None = None,
    units: list[str] | None = None,
    unit_options_by_combo: dict[str, list[str]] | None = None,
    success_scopes: list[str] | None = None,
) -> str:
    payload = {
        "groups": GROUP_LABELS,
        "products": products,
        "ad_formats": ad_formats,
        "countries": countries or [],
        "country_options_by_combo": country_options_by_combo or {},
        "units": units or [],
        "unit_options_by_combo": unit_options_by_combo or {},
        "success_scopes": success_scopes or [],
        "metrics": metrics,
    }
    resolved_page_title = resolve_dashboard_page_title(page_key, page_title)
    if page_key == "request_structure_country":
        hero_lines = REQUEST_STRUCTURE_COUNTRY_HERO_TEXT
        hero_chip = "country 维度"
        hero_pill = "AB Request Structure Country Dashboard"
        page_script = build_request_structure_country_page_script()
    elif page_key == "request_structure_unit":
        hero_lines = REQUEST_STRUCTURE_UNIT_HERO_TEXT
        hero_chip = "max_unit_id 维度"
        hero_pill = "AB Request Structure Unit Dashboard"
        page_script = build_request_structure_unit_page_script()
    elif page_key == "request_structure":
        hero_lines = REQUEST_STRUCTURE_HERO_TEXT
        hero_chip = "全量 request 口径"
        hero_pill = "AB Request Structure Dashboard"
        page_script = build_request_structure_page_script()
    else:
        hero_lines = COVERAGE_HERO_TEXT
        hero_chip = "req_index 范围 · 1-200"
        hero_pill = "AB Request Structure Dashboard"
        page_script = build_coverage_analysis_page_script()
    hero_lines_html = "".join(f"<p>{line}</p>" for line in hero_lines)
    return f"""<!doctype html>
<html lang="zh-CN">
<head>
<meta charset="UTF-8" />
<meta name="viewport" content="width=device-width, initial-scale=1.0" />
<title>{resolved_page_title}</title>
<style>
body{{margin:0;font-family:"Microsoft YaHei",sans-serif;background:#f6f1e9;color:#203040}}.page{{max-width:1680px;margin:0 auto;padding:20px 16px 40px}}.hero{{display:flex;justify-content:space-between;gap:16px;flex-wrap:wrap;margin-bottom:16px}}.hero h1{{margin:8px 0 6px;font-size:30px}}.hero-copy{{display:grid;gap:4px}}.hero p{{margin:0;color:#667788;line-height:1.6}}.pill,.chip,.selector,.condition-tag{{display:inline-flex;align-items:center;min-height:28px;padding:0 12px;border-radius:999px;border:1px solid rgba(32,48,64,.12);background:rgba(255,255,255,.92)}}.pill{{color:#0f766e;border-color:rgba(15,118,110,.18)}}.condition-tag{{color:#1f3140;background:rgba(240,249,255,.9);border-color:rgba(14,116,144,.2);max-width:100%;min-height:auto;padding:6px 12px;white-space:normal;line-height:1.4;align-items:flex-start}}.controls{{display:grid;grid-template-columns:repeat(auto-fit,minmax(220px,260px));gap:12px;padding:12px 14px;border-radius:18px;border:1px solid rgba(32,48,64,.12);background:rgba(255,255,255,.82);position:sticky;top:10px;z-index:5;backdrop-filter:blur(8px)}}.field label,.toolbar-field label{{display:block;margin-bottom:6px;font-size:12px;color:#667788}}.field select,.toolbar-field select{{width:100%;padding:9px 12px;border-radius:12px;border:1px solid rgba(32,48,64,.12);background:#fff}}.metric{{margin-top:24px;display:grid;gap:12px}}.card{{border:1px solid rgba(32,48,64,.12);border-radius:24px;background:rgba(255,255,255,.82);overflow:hidden}}.card-head{{display:flex;justify-content:space-between;gap:12px;flex-wrap:wrap;padding:16px 18px;border-bottom:1px solid rgba(32,48,64,.12)}}.card-head h2{{margin:2px 0 4px;font-size:24px}}.muted{{color:#667788;font-size:12px}}.toolbar{{display:flex;gap:10px;flex-wrap:wrap;padding:16px 18px 0}}.toolbar-field{{min-width:180px}}.selector-group{{display:flex;gap:8px;flex-wrap:wrap;align-items:center}}.selector{{cursor:pointer;font-size:12px;transition:all .12s ease}}.selector.active{{background:#0f766e;color:#fff;border-color:#0f766e}}.explain{{margin:16px 18px 0;padding:14px 16px;border:1px solid rgba(32,48,64,.12);border-radius:18px;background:rgba(255,255,255,.72);min-width:0}}.explain h4{{margin:0 0 10px;font-size:15px}}.explain ul{{margin:0;padding-left:18px;display:grid;gap:6px;min-width:0}}.explain li{{line-height:1.65;overflow-wrap:anywhere;word-break:break-word}}.panel-wrap{{display:grid;gap:16px;padding:16px 18px 18px}}.detail-card,.type-panel{{border:1px solid rgba(32,48,64,.12);border-radius:20px;background:rgba(255,255,255,.7);padding:14px}}.detail-top{{margin-bottom:12px}}.detail-top h4{{margin:2px 0 0;font-size:18px}}.detail-top p{{margin:6px 0 0;color:#667788;font-size:13px}}.chart-pair{{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:12px}}.chart-stack{{display:grid;gap:12px}}.chart-box{{border-radius:16px;background:rgba(255,255,255,.62);padding:10px 12px 6px}}.chart-box-head{{display:flex;justify-content:space-between;gap:10px;margin-bottom:6px}}.chart-scroll{{overflow-x:auto;padding-bottom:6px;width:100%}}.chart{{width:100%;height:360px}}.table-card{{padding:0;overflow:hidden}}.table-card .table-wrap{{border-top:0}}.table-wrap{{border-top:1px solid rgba(32,48,64,.08)}}.metric-table{{border-collapse:separate;border-spacing:0;background:rgba(255,255,255,.88)}}.metric-table th,.metric-table td{{padding:10px 12px;border-bottom:1px solid rgba(32,48,64,.08);font-size:13px;text-align:right;vertical-align:top}}.metric-table th{{background:rgba(240,244,248,.96);color:#1f3140;font-weight:700;white-space:normal}}.metric-table td:first-child,.metric-table td:nth-child(2),.metric-table th.status-network-head{{text-align:left}}.metric-table .status-cell{{font-weight:700;background:rgba(248,250,252,.96);white-space:normal;overflow-wrap:anywhere}}.metric-table .network-cell{{color:#203040;white-space:normal;overflow-wrap:anywhere}}.metric-table .group-divider td{{border-top:1px solid rgba(32,48,64,.14)}}.heat-cell{{transition:background-color .12s ease,color .12s ease}}.share-main{{font-weight:700;line-height:1.2}}.pv-sub{{margin-top:4px;font-size:11px;opacity:.86;line-height:1.2}}.metric4-wrap{{overflow-x:hidden}}.metric4-table{{width:100%;table-layout:fixed}}.metric4-table thead th{{text-align:center}}.metric4-table th.metric4-status,.metric4-table th.metric4-network{{text-align:left}}.metric4-table .metric4-status{{width:140px;min-width:140px}}.metric4-table .metric4-network{{width:24%;min-width:160px}}.metric4-table .metric4-share{{font-weight:700}}.metric4-table .metric4-num{{white-space:nowrap}}.metric5-wrap{{overflow:auto;max-width:100%}}.metric5-table{{width:max-content;min-width:1320px}}.metric5-table thead th{{position:sticky;top:0;z-index:4}}.metric5-table thead tr:nth-child(2) th{{top:43px;z-index:4}}.sticky-col{{position:sticky;background:rgba(248,250,252,.98)}}.sticky-col-1{{left:0;z-index:5;min-width:110px;max-width:110px}}.sticky-col-2{{left:110px;z-index:5;min-width:220px;max-width:220px}}.metric5-table thead .sticky-col{{z-index:6;background:rgba(240,244,248,.98)}}.metric5-value{{min-width:126px}}.empty,.page-error{{padding:14px;border-radius:14px;border:1px dashed rgba(32,48,64,.12);color:#667788;text-align:center}}.page-error{{border-style:solid;background:rgba(254,226,226,.86);color:#991b1b}}@media (max-width:1100px){{.chart-pair{{grid-template-columns:1fr}}.controls{{position:static}}.toolbar-field{{min-width:140px;flex:1 1 140px}}.metric4-table .metric4-network{{width:auto}}.metric5-table{{min-width:1180px}}}}
</style>
</head>
<body>
<div class="page"><section class="hero"><div><span class="pill">{hero_pill}</span><h1>{resolved_page_title}</h1><div class="hero-copy">{hero_lines_html}</div></div><div><span class="chip">{hero_chip}</span></div></section><section class="controls"><div class="field"><label for="product-select">Product</label><select id="product-select"></select></div><div class="field"><label for="format-select">Ad Format</label><select id="format-select"></select></div>{'<div class="field"><label for="country-select">Country</label><select id="country-select"></select></div>' if page_key == "request_structure_country" else ''}{'<div class="field"><label for="unit-select">Unit</label><select id="unit-select"></select></div>' if page_key == "request_structure_unit" else ''}{'<div class="field"><label for="success-scope-select">Success Scope</label><select id="success-scope-select"></select></div>' if page_key == "request_structure" else ''}</section><div id="page-error" class="page-error" hidden></div><div id="root"></div></div>
<script src="{ASSET_SCRIPT_PATH}"></script>
<script>
{build_common_script(payload)}
{page_script}
</script>
</body>
</html>"""


def build_success_mapping_page_script() -> str:
    return """
function heatRange(values){const nums=(values||[]).filter(v=>Number.isFinite(v));if(!nums.length)return {min:0,max:1};return {min:Math.min(...nums),max:Math.max(...nums)}}
function heatStyle(value,range){const min=Number(range.min||0),max=Number(range.max||0);let norm=0;if(max>min){norm=(Number(value||0)-min)/(max-min)}else if(max>0){norm=1}norm=Math.max(0,Math.min(1,norm));const alpha=0.16+norm*0.34;const bg=`rgba(14,116,144,${alpha.toFixed(3)})`;const fg=norm>=0.62?'#f8fafc':'#102a43';return `background:${bg};color:${fg}`;}
function heatTd(share,inner,title,extraClass=''){return `<td class="heat-cell ${extraClass}" style="${heatStyle(share,window.__heat_range__)}" title="${title}">${inner}</td>`}
function renderSuccessMetric(root,metricKey){
  const metric=metricConfig(metricKey);
  const product=document.getElementById('product-select').value,adFormat=document.getElementById('format-select').value;
  const comboPayload=combo(metricKey,product,adFormat);
  const view=comboPayload?.view||null;
  const sec=document.createElement('section');
  sec.className='metric';
  sec.innerHTML=`<div class="card"><div class="card-head"><div><h2>${metric?.title||metricKey}</h2><div class="muted">${product} / ${adFormat}</div></div></div>${explain(metric?.desc||[])}<div class="toolbar"></div><div class="panel-wrap"></div></div>`;
  root.appendChild(sec);
  const toolbar=sec.querySelector('.toolbar'),panel=sec.querySelector('.panel-wrap');
  if(!view||!((view.count_options||[]).length)){panel.innerHTML='<div class="empty">当前筛选条件下暂无结果。</div>';return}
  let countValue=view.count_options[0]||'';
  const draw=()=>{
    panel.innerHTML='';
    const current=(view.cnt_map||{})[countValue];
    if(!current){panel.innerHTML='<div class="empty">当前 cnt 下暂无结果。</div>';return}
    const rows=current.rows||[];
    const shares=[];
    rows.forEach(row=>{shares.push(Number((((row.groups||{})[GROUP_A]||{}).share)||0));shares.push(Number((((row.groups||{})[GROUP_B]||{}).share)||0));});
    window.__heat_range__=heatRange(shares);
    let headTop='<tr><th rowspan="2" class="status-network-head metric4-network">success_target</th><th colspan="2">share</th><th colspan="2">pv</th><th colspan="2">当前 cnt 桶内 total</th></tr>';
    let headBottom='<tr><th>A组</th><th>B组</th><th>A组</th><th>B组</th><th>A组</th><th>B组</th></tr>';
    let body='';
    rows.forEach(row=>{
      const left=(row.groups||{})[GROUP_A]||{request_pv:0,share:0,denominator_request_pv:0};
      const right=(row.groups||{})[GROUP_B]||{request_pv:0,share:0,denominator_request_pv:0};
      const leftInner=`<div class="share-main">${fmtPct(left.share)}</div><div class="pv-sub">pv ${fmtNum(left.request_pv)}</div>`;
      const rightInner=`<div class="share-main">${fmtPct(right.share)}</div><div class="pv-sub">pv ${fmtNum(right.request_pv)}</div>`;
      body+=`<tr><td class="network-cell metric4-network" title="${row.success_target}">${row.success_target}</td>${heatTd(left.share,leftInner,`${row.success_target} / ${GROUPS[GROUP_A]} share=${fmtPct(left.share)} / pv=${fmtNum(left.request_pv)}`,'metric5-value')}${heatTd(right.share,rightInner,`${row.success_target} / ${GROUPS[GROUP_B]} share=${fmtPct(right.share)} / pv=${fmtNum(right.request_pv)}`,'metric5-value')}<td class="metric4-num">${fmtNum(left.request_pv)}</td><td class="metric4-num">${fmtNum(right.request_pv)}</td><td class="metric4-num">${fmtNum(left.denominator_request_pv)}</td><td class="metric4-num">${fmtNum(right.denominator_request_pv)}</td></tr>`;
    });
    panel.innerHTML=`<div class="detail-card"><div class="detail-top"><h4>${metricKey==='network'?'成功 network 分布':'成功 placement 分布'}</h4><p>当前 cnt 桶内，按 success_target 统计占比；无成功则记为 fail。</p></div><div class="detail-card table-card"><div class="table-wrap metric5-wrap"><table class="metric-table metric5-table"><thead>${headTop}${headBottom}</thead><tbody>${body}</tbody></table></div></div></div>`;
  };
  toolbar.innerHTML='';
  const countLabel=view.count_label==='network_cnt'?'network_cnt（单选）':'placement_cnt（单选）';
  toolbar.appendChild(selectField(countLabel,view.count_options,countValue,value=>{countValue=value;draw()}));
  draw();
}
function renderSuccessMappingPage(){
  setError('');
  const root=document.getElementById('root'),product=document.getElementById('product-select').value,adFormat=document.getElementById('format-select').value;
  root.innerHTML='';
  if(!product||!adFormat){setError('当前结果缺少 product 或 ad_format。');return}
  renderSuccessMetric(root,'network');
  renderSuccessMetric(root,'placement');
}
const controls=productFormat();controls.product.onchange=renderSuccessMappingPage;controls.format.onchange=renderSuccessMappingPage;renderSuccessMappingPage();
"""


def build_success_mapping_html(payload: dict[str, Any]) -> str:
    title = payload.get("title", "成功 network / placement 分布")
    desc = payload.get("desc") or []
    hero_lines_html = "".join(f"<p>{line}</p>" for line in desc)
    script_payload = {
        "groups": payload.get("groups", GROUP_LABELS),
        "products": payload.get("products", []),
        "ad_formats": payload.get("ad_formats", []),
        "metrics": payload.get("metrics", {}),
    }
    return f"""<!doctype html>
<html lang="zh-CN">
<head>
<meta charset="UTF-8" />
<meta name="viewport" content="width=device-width, initial-scale=1.0" />
<title>{title}</title>
<style>
body{{margin:0;font-family:"Microsoft YaHei",sans-serif;background:#f6f1e9;color:#203040}}.page{{max-width:1500px;margin:0 auto;padding:20px 16px 40px}}.hero{{display:flex;justify-content:space-between;gap:16px;flex-wrap:wrap;margin-bottom:16px}}.hero h1{{margin:8px 0 6px;font-size:30px}}.hero-copy{{display:grid;gap:4px}}.hero p{{margin:0;color:#667788;line-height:1.6}}.pill,.chip,.selector,.condition-tag{{display:inline-flex;align-items:center;min-height:28px;padding:0 12px;border-radius:999px;border:1px solid rgba(32,48,64,.12);background:rgba(255,255,255,.92)}}.pill{{color:#0f766e;border-color:rgba(15,118,110,.18)}}.controls{{display:grid;grid-template-columns:repeat(auto-fit,minmax(220px,260px));gap:12px;padding:12px 14px;border-radius:18px;border:1px solid rgba(32,48,64,.12);background:rgba(255,255,255,.82);position:sticky;top:10px;z-index:5;backdrop-filter:blur(8px)}}.field label,.toolbar-field label{{display:block;margin-bottom:6px;font-size:12px;color:#667788}}.field select,.toolbar-field select{{width:100%;padding:9px 12px;border-radius:12px;border:1px solid rgba(32,48,64,.12);background:#fff}}.metric{{margin-top:24px;display:grid;gap:12px}}.card{{border:1px solid rgba(32,48,64,.12);border-radius:24px;background:rgba(255,255,255,.82);overflow:hidden}}.card-head{{display:flex;justify-content:space-between;gap:12px;flex-wrap:wrap;padding:16px 18px;border-bottom:1px solid rgba(32,48,64,.12)}}.card-head h2{{margin:2px 0 4px;font-size:24px}}.muted{{color:#667788;font-size:12px}}.toolbar{{display:flex;gap:10px;flex-wrap:wrap;padding:16px 18px 0}}.toolbar-field{{min-width:180px}}.selector-group{{display:flex;gap:8px;flex-wrap:wrap;align-items:center}}.selector{{cursor:pointer;font-size:12px;transition:all .12s ease}}.selector.active{{background:#0f766e;color:#fff;border-color:#0f766e}}.explain{{margin:16px 18px 0;padding:14px 16px;border:1px solid rgba(32,48,64,.12);border-radius:18px;background:rgba(255,255,255,.72);min-width:0}}.explain h4{{margin:0 0 10px;font-size:15px}}.explain ul{{margin:0;padding-left:18px;display:grid;gap:6px;min-width:0}}.explain li{{line-height:1.65;overflow-wrap:anywhere;word-break:break-word}}.panel-wrap{{display:grid;gap:16px;padding:16px 18px 18px}}.detail-card{{border:1px solid rgba(32,48,64,.12);border-radius:20px;background:rgba(255,255,255,.7);padding:14px}}.detail-top{{margin-bottom:12px}}.detail-top h4{{margin:2px 0 0;font-size:18px}}.detail-top p{{margin:6px 0 0;color:#667788;font-size:13px}}.table-card{{padding:0;overflow:hidden}}.table-wrap{{border-top:1px solid rgba(32,48,64,.08)}}.metric-table{{border-collapse:separate;border-spacing:0;background:rgba(255,255,255,.88)}}.metric-table th,.metric-table td{{padding:10px 12px;border-bottom:1px solid rgba(32,48,64,.08);font-size:13px;text-align:right;vertical-align:top}}.metric-table th{{background:rgba(240,244,248,.96);color:#1f3140;font-weight:700;white-space:normal}}.metric-table td:first-child,.metric-table th.status-network-head{{text-align:left}}.network-cell{{color:#203040;white-space:normal;overflow-wrap:anywhere}}.heat-cell{{transition:background-color .12s ease,color .12s ease}}.share-main{{font-weight:700;line-height:1.2}}.pv-sub{{margin-top:4px;font-size:11px;opacity:.86;line-height:1.2}}.metric5-wrap{{overflow:auto;max-width:100%}}.metric5-table{{width:max-content;min-width:1320px}}.metric5-value{{min-width:126px}}.empty,.page-error{{padding:14px;border-radius:14px;border:1px dashed rgba(32,48,64,.12);color:#667788;text-align:center}}.page-error{{border-style:solid;background:rgba(254,226,226,.86);color:#991b1b}}@media (max-width:1100px){{.controls{{position:static}}.toolbar-field{{min-width:140px;flex:1 1 140px}}.metric5-table{{min-width:1080px}}}}
</style>
</head>
<body class="ab-success-mapping">
<div class="page">
  <section class="hero">
    <div>
      <span class="pill">AB Success Mapping Dashboard</span>
      <h1>{title}</h1>
      <div class="hero-copy">{hero_lines_html}</div>
    </div>
  </section>
  <section class="controls">
    <div class="field"><label for="product-select">Product</label><select id="product-select"></select></div>
    <div class="field"><label for="format-select">Ad Format</label><select id="format-select"></select></div>
  </section>
  <div id="page-error" class="page-error" hidden></div>
  <div id="root"></div>
</div>
<script src="{ASSET_SCRIPT_PATH}"></script>
<script>
{build_common_script(script_payload)}
{build_success_mapping_page_script()}
</script>
</body>
</html>"""


def build_null_bidding_page_script(payload: dict[str, Any] | None = None) -> str:
    payload_json = json.dumps(payload or {}, ensure_ascii=False)
    return f"""
const DATA={payload_json};
const GROUP_A='no_is_adx',GROUP_B='have_is_adx',GROUPS=DATA.groups||{{'no_is_adx':'A组（no_is_adx）','have_is_adx':'B组（have_is_adx）'}};
const PLATFORM_ORDER=DATA.platform_order||['android','ios'],FORMAT_ORDER=DATA.format_order||['interstitial','rewarded'];
const STATUS_OPTIONS=DATA.status_options||['NULL','FAILED_TO_LOAD','AD_LOAD_NOT_ATTEMPTED'];
const fmtNum=v=>Math.round(Number(v||0)).toLocaleString(),fmtPct=v=>`${{(Number(v||0)*100).toFixed(2)}}%`;
const colors=n=>{{const p=['#e6194b','#3cb44b','#ffe119','#4363d8','#f58231','#911eb4','#46f0f0','#f032e6','#bcf60c','#9a6324','#008080','#800000','#808000','#000075','#808080'];return Array.from({{length:n}},(_,i)=>p[i%p.length])}};
function shortUnitLabel(unit){{
  const value=String(unit||'').trim();
  const pMatch=value.match(/p\\s*(\\d+)/i);
  if(pMatch)return `P${{pMatch[1]}}`;
  if(/df/i.test(value))return 'DF';
  const parts=value.split(/[ _-]+/).filter(Boolean);
  return parts.length ? parts[parts.length-1] : value;
}}
function unitOption(points,seriesKeys,groupKey,axisMax){{
  const groupLabel=GROUPS[groupKey]||groupKey;
  const palette=colors(seriesKeys.length);
  return {{
    backgroundColor:'rgba(252,250,246,0.95)',
    grid:{{left:58,right:16,top:72,bottom:54}},
    legend:{{show:true,type:'scroll',top:0,itemHeight:10,itemWidth:14,textStyle:{{fontSize:11,color:'#667788'}}}},
    tooltip:{{
      trigger:'axis',
      axisPointer:{{type:'line',lineStyle:{{color:'rgba(32,48,64,0.2)'}}}},
      backgroundColor:'rgba(255,255,255,0.96)',
      borderColor:'rgba(31,49,64,0.14)',
      textStyle:{{fontSize:12,color:'#1f3140'}},
      formatter(params){{
        if(!params||!params.length)return '';
        let active=params[0];
        if(typeof window._current_mouse_y === 'number'){{
          let acc=0;
          for(let i=0;i<params.length;i++){{acc += Number(params[i].data.share||0); if(window._current_mouse_y <= acc && Number(params[i].data.share||0)>0){{active=params[i]; break;}}}}
        }}else{{active=params.slice().sort((a,b)=>Number(b.data.share||0)-Number(a.data.share||0))[0];}}
        const d=active.data||{{}};
        return `<div style="min-width:220px"><div style="margin-bottom:6px;font-weight:600">${{groupLabel}} · ${{shortUnitLabel(d.unit||params[0].axisValue)}} · cnt=${{d.seriesKey||active.seriesName}}</div>`+
          `<div style="display:grid;gap:4px;font-size:12px">`+
          `<div style="display:flex;justify-content:space-between;gap:8px"><span style="color:#667788">占比</span><span style="font-weight:600">${{fmtPct(d.share)}}</span></div>`+
          `<div style="display:flex;justify-content:space-between;gap:8px"><span style="color:#667788">pv</span><span>${{fmtNum(d.request_pv)}}</span></div>`+
          `<div style="display:flex;justify-content:space-between;gap:8px"><span style="color:#667788">cnt</span><span>${{fmtNum(d.bidding_cnt)}}</span></div>`+
          `<div style="display:flex;justify-content:space-between;gap:8px"><span style="color:#667788">总请求量</span><span>${{fmtNum(d.denominator_request_pv)}}</span></div>`+
          `</div></div>`;
      }}
    }},
    color:palette,
    xAxis:{{
      type:'category',
      data:points.map(p=>shortUnitLabel(p.unit)),
      axisLine:{{lineStyle:{{color:'rgba(31,49,64,0.12)'}}}},
      axisTick:{{show:false}},
      axisLabel:{{color:'#667788',fontSize:11,fontWeight:600,interval:0,rotate:0,margin:12}},
    }},
    yAxis:{{
      type:'value',
      min:0,
      max:axisMax||0.1,
      axisLabel:{{formatter:v=>`${{Math.round(v*100)}}%`,color:'#667788',fontSize:11}},
      splitLine:{{lineStyle:{{color:'rgba(31,49,64,0.07)'}}}},
    }},
    series:seriesKeys.map((seriesKey,index)=>({{
      type:'bar',
      name:seriesKey,
      stack:'total',
      barMaxWidth:42,
      itemStyle:{{borderRadius:index===seriesKeys.length-1?[8,8,0,0]:[0,0,0,0]}},
      emphasis:{{focus:'series'}},
      data:points.map(point=>{{
        const current=(point.series||{{}})[seriesKey]||{{share:0,request_pv:0,bidding_cnt:Number(seriesKey||0),denominator_request_pv:0}};
        return {{
          value:Number((current.share||0).toFixed(6)),
          unit:point.unit,
          share:current.share||0,
          request_pv:current.request_pv||0,
          bidding_cnt:current.bidding_cnt||0,
          denominator_request_pv:current.denominator_request_pv||0,
          seriesKey,
        }};
      }}),
    }})),
  }};
}}
function pieOption(items,groupKey){{
  const groupLabel=GROUPS[groupKey]||groupKey;
  return {{
    backgroundColor:'rgba(252,250,246,0.95)',
    tooltip:{{
      trigger:'item',
      backgroundColor:'rgba(255,255,255,0.96)',
      borderColor:'rgba(31,49,64,0.14)',
      textStyle:{{fontSize:12,color:'#1f3140'}},
      formatter(params){{
        const d=params.data||{{}};
        return `<div style="min-width:200px"><div style="margin-bottom:6px;font-weight:600">${{groupLabel}} · ${{shortUnitLabel(d.unit||params.name)}}</div>`+
          `<div style="display:grid;gap:4px;font-size:12px">`+
          `<div style="display:flex;justify-content:space-between;gap:8px"><span style="color:#667788">request占比</span><span style="font-weight:600">${{fmtPct(d.share)}}</span></div>`+
          `<div style="display:flex;justify-content:space-between;gap:8px"><span style="color:#667788">pv</span><span>${{fmtNum(d.request_pv)}}</span></div>`+
          `</div></div>`;
      }}
    }},
    legend:{{show:false}},
    series:[{{
      type:'pie',
      radius:['46%','72%'],
      center:['50%','52%'],
      minAngle:2,
      avoidLabelOverlap:true,
      label:{{
        show:true,
        formatter:params=>`${{shortUnitLabel(params.data.unit||params.name)}}\\n${{fmtPct(params.data.share)}}`,
        color:'#556677',
        fontSize:11,
      }},
      labelLine:{{length:10,length2:8,lineStyle:{{color:'rgba(32,48,64,0.18)'}}}},
      itemStyle:{{borderColor:'rgba(255,255,255,0.96)',borderWidth:2}},
      data:items.map(item=>({{
        name:shortUnitLabel(item.unit),
        value:Number((item.request_pv||0).toFixed(6)),
        unit:item.unit,
        share:item.share||0,
        request_pv:item.request_pv||0,
      }})),
    }}],
  }};
}}
function mountUnitChart(host,points,seriesKeys,groupKey,axisMax){{
  const scroll=document.createElement('div');
  scroll.className='chart-scroll';
  const chartEl=document.createElement('div');
  chartEl.className='chart null-unit-chart';
  chartEl.style.width=`${{Math.max(720, (points.length||1)*92)}}px`;
  scroll.appendChild(chartEl);
  host.appendChild(scroll);
  const chart=echarts.init(chartEl);
  chart.setOption(unitOption(points,seriesKeys,groupKey,axisMax));
  chart.getZr().on('mousemove', function(e){{
    const pointInGrid=chart.convertFromPixel({{seriesIndex:0}}, [e.offsetX, e.offsetY]);
    if(pointInGrid) window._current_mouse_y = pointInGrid[1];
  }});
  new ResizeObserver(()=>chart.resize()).observe(scroll);
}}
function mountPieChart(host,items,groupKey){{
  const chartEl=document.createElement('div');
  chartEl.className='chart unit-pie-chart';
  host.appendChild(chartEl);
  const chart=echarts.init(chartEl);
  chart.setOption(pieOption(items,groupKey));
  new ResizeObserver(()=>chart.resize()).observe(host);
}}
function renderStatusControl(){{
  const select=document.getElementById('status-select');
  if(!select)return null;
  select.innerHTML=STATUS_OPTIONS.map(status=>`<option value="${{status}}">${{status}}</option>`).join('');
  return select;
}}
function renderGroupRow(host,groupKey,groupPayload,seriesKeys,axisMax){{
  const row=document.createElement('div');
  row.className='chart-row';
  const chartCard=document.createElement('div');
  chartCard.className='chart-box';
  chartCard.innerHTML=`<div class="chart-box-head"><strong>${{GROUPS[groupKey]}}</strong><span class="muted">左侧看 cnt 分布，右侧看 unit request 占比</span></div>`;
  row.appendChild(chartCard);
  mountUnitChart(chartCard,groupPayload.points||[],seriesKeys,groupKey,axisMax);
  const pieCard=document.createElement('div');
  pieCard.className='chart-box pie-box';
  pieCard.innerHTML=`<div class="chart-box-head"><strong>${{GROUPS[groupKey]}}</strong><span class="muted">各 unit request 占比</span></div>`;
  row.appendChild(pieCard);
  const piePayload=groupPayload.pie||{{items:[],total_request_pv:0}};
  if((piePayload.items||[]).length){{
    mountPieChart(pieCard,piePayload.items||[],groupKey);
  }}else{{
    pieCard.insertAdjacentHTML('beforeend','<div class="empty">当前组暂无可展示的 unit request 占比。</div>');
  }}
  host.appendChild(row);
}}
function renderFormatCard(host,platformLabel,formatKey,formatPayload,statusKey){{
  const sec=document.createElement('section');
  sec.className='card null-format-card';
  sec.innerHTML=`<div class="card-head"><div><h2>${{formatPayload.label}}</h2><div class="muted">${{platformLabel}} · ${{statusKey}}</div></div></div>`;
  const body=document.createElement('div');
  body.className='panel-wrap';
  sec.appendChild(body);
  const statusPayload=((formatPayload.status_map||{{}})[statusKey])||{{empty:true,series_keys:[],axis_max:0.1,groups:{{[GROUP_A]:{{points:[]}},[GROUP_B]:{{points:[]}}}}}};
  if(statusPayload.empty){{
    body.innerHTML=`<div class="empty">${{platformLabel}} 的 ${{formatKey}} 在 ${{statusKey}} 下当前无结果。</div>`;
    host.appendChild(sec);
    return;
  }}
  const axisMax=Math.max(Number(statusPayload.axis_max||0),0.1);
  const seriesKeys=statusPayload.series_keys||[];
  renderGroupRow(body,GROUP_A,statusPayload.groups[GROUP_A]||{{points:[],pie:{{items:[],total_request_pv:0}}}},seriesKeys,axisMax);
  renderGroupRow(body,GROUP_B,statusPayload.groups[GROUP_B]||{{points:[],pie:{{items:[],total_request_pv:0}}}},seriesKeys,axisMax);
  host.appendChild(sec);
}}
function renderPlatformSection(root,platformKey,statusKey){{
  const platform=(DATA.platforms||{{}})[platformKey];
  if(!platform)return;
  const section=document.createElement('section');
  section.className='metric';
  section.innerHTML=`<div class="card platform-card"><div class="card-head"><div><h2>${{platform.label}}</h2></div></div><div class="panel-wrap null-format-stack"></div></div>`;
  root.appendChild(section);
  const stack=section.querySelector('.null-format-stack');
  FORMAT_ORDER.forEach(formatKey=>renderFormatCard(stack,platform.label,formatKey,(platform.formats||{{}})[formatKey]||{{label:formatKey,status_map:{{}}}},statusKey));
}}
function renderNullBiddingPage(statusKey){{
  const root=document.getElementById('root');
  root.innerHTML='';
  PLATFORM_ORDER.forEach(platformKey=>renderPlatformSection(root,platformKey,statusKey));
}}
let currentStatus=STATUS_OPTIONS[0]||'NULL';
const statusSelect=renderStatusControl();
if(statusSelect){{
  statusSelect.value=STATUS_OPTIONS.includes(currentStatus)?currentStatus:(STATUS_OPTIONS[0]||'NULL');
  statusSelect.onchange=e=>{{
    currentStatus=e.target.value;
    renderNullBiddingPage(currentStatus);
  }};
}}
renderNullBiddingPage(currentStatus);
"""


def build_null_bidding_html(payload: dict[str, Any]) -> str:
    hero_lines_html = "".join(f"<p>{line}</p>" for line in payload.get("desc", []))
    return f"""<!doctype html>
<html lang="zh-CN">
<head>
<meta charset="UTF-8" />
<meta name="viewport" content="width=device-width, initial-scale=1.0" />
<title>{payload.get("title", "Null Bidding Unit Share")}</title>
<style>
body{{margin:0;font-family:"Microsoft YaHei",sans-serif;background:#f6f1e9;color:#203040}}.page{{max-width:1680px;margin:0 auto;padding:20px 16px 40px}}.hero{{display:flex;justify-content:space-between;gap:16px;flex-wrap:wrap;margin-bottom:16px}}.hero h1{{margin:8px 0 6px;font-size:30px}}.hero-copy{{display:grid;gap:4px}}.hero p{{margin:0;color:#667788;line-height:1.6}}.pill{{display:inline-flex;align-items:center;min-height:28px;padding:0 12px;border-radius:999px;border:1px solid rgba(32,48,64,.12);background:rgba(255,255,255,.92);color:#0f766e;border-color:rgba(15,118,110,.18)}}.controls{{display:grid;grid-template-columns:repeat(auto-fit,minmax(220px,260px));gap:12px;padding:12px 14px;border-radius:18px;border:1px solid rgba(32,48,64,.12);background:rgba(255,255,255,.82);position:sticky;top:10px;z-index:5;backdrop-filter:blur(8px);margin-bottom:16px}}.field label{{display:block;margin-bottom:6px;font-size:12px;color:#667788}}.field select{{width:100%;padding:9px 12px;border-radius:12px;border:1px solid rgba(32,48,64,.12);background:#fff}}.metric{{margin-top:24px;display:grid;gap:12px}}.card{{border:1px solid rgba(32,48,64,.12);border-radius:24px;background:rgba(255,255,255,.82);overflow:hidden}}.card-head{{display:flex;justify-content:space-between;gap:12px;flex-wrap:wrap;padding:16px 18px;border-bottom:1px solid rgba(32,48,64,.12)}}.card-head h2{{margin:2px 0 4px;font-size:24px;text-transform:none}}.muted{{color:#667788;font-size:12px}}.panel-wrap{{display:grid;gap:16px;padding:16px 18px 18px}}.null-format-stack{{grid-template-columns:1fr;align-items:start}}.chart-row{{display:grid;grid-template-columns:minmax(0,1fr) 330px;gap:14px;align-items:start}}.chart-box{{border-radius:16px;background:rgba(255,255,255,.62);padding:12px 14px 12px}}.pie-box{{min-height:380px}}.chart-box-head{{display:flex;justify-content:space-between;gap:10px;margin-bottom:10px}}.chart{{height:320px}}.chart-scroll{{overflow-x:auto;padding-bottom:8px;width:100%}}.unit-pie-chart{{width:100%;min-width:280px}}.empty{{padding:14px;border-radius:14px;border:1px dashed rgba(32,48,64,.12);color:#667788;text-align:center}}.null-format-card .panel-wrap{{padding-top:14px}}@media (max-width:1100px){{.null-format-stack{{grid-template-columns:1fr}}.chart-row{{grid-template-columns:1fr}}.controls{{position:static}}}}
</style>
</head>
<body>
<div class="page">
  <section class="hero">
    <div>
      <span class="pill">AB Null Bidding Dashboard</span>
      <h1>{payload.get("title", "Null Bidding Unit Share")}</h1>
      <div class="hero-copy">{hero_lines_html}</div>
    </div>
  </section>
  <section class="controls"><div class="field"><label for="status-select">Status</label><select id="status-select"></select></div></section>
  <div id="root"></div>
</div>
<script src="{ASSET_SCRIPT_PATH}"></script>
<script>
{build_null_bidding_page_script(payload)}
</script>
</body>
</html>"""


def build_bidding_network_status_page_script(payload: dict[str, Any] | None = None) -> str:
    payload_json = json.dumps(payload or {}, ensure_ascii=False)
    return f"""
const DATA={payload_json};
const GROUP_A='no_is_adx',GROUP_B='have_is_adx',GROUPS=DATA.groups||{{'no_is_adx':'A组（no_is_adx）','have_is_adx':'B组（have_is_adx）'}};
const PLATFORM_ORDER=DATA.platform_order||['android','ios'],FORMAT_ORDER=DATA.format_order||['interstitial','rewarded'];
const NETWORK_TYPE_ORDER=DATA.network_type_order||['bidding','waterfall'];
const NETWORK_TYPE_LABELS={{'bidding':'bidding','waterfall':'waterfall'}};
const STATUS_ORDER=DATA.status_order||['NULL','AD_LOADED','FAILED_TO_LOAD','AD_LOAD_NOT_ATTEMPTED'];
const STATUS_COLORS={{'NULL':'#111827','AD_LOADED':'#2563eb','FAILED_TO_LOAD':'#dc2626','AD_LOAD_NOT_ATTEMPTED':'#059669'}};
const GROUP_STYLE={{[GROUP_A]:'solid',[GROUP_B]:'dashed'}};
const fmtNum=v=>Math.round(Number(v||0)).toLocaleString(),fmtPct=v=>`${{(Number(v||0)*100).toFixed(2)}}%`;
function splitNetworkWords(network){{
  return String(network||'')
    .trim()
    .replace(/([a-z0-9])([A-Z])/g,'$1 $2')
    .replace(/[_-]+/g,' ')
    .split(/\\s+/)
    .filter(Boolean);
}}
function formatNetworkAxisLabel(network){{
  const words=splitNetworkWords(network);
  if(!words.length)return '';
  const lines=[];
  let current='';
  words.forEach(word=>{{
    const next=current ? `${{current}} ${{word}}` : word;
    if(next.length <= 12){{
      current=next;
      return;
    }}
    if(current)lines.push(current);
    current=word;
  }});
  if(current)lines.push(current);
  return lines.slice(0,3).join('\\n');
}}
function calculateVisibleNetworkCount(containerWidth, networkCount){{
  const width=Math.max(Number(containerWidth||0), 320);
  const minPerNetwork = width >= 1500 ? 94 : width >= 1100 ? 106 : width >= 820 ? 118 : 132;
  return Math.max(1, Math.floor(width / minPerNetwork), 1);
}}
function shortUnitLabel(unit){{
  const value=String(unit||'').trim();
  const pMatch=value.match(/p\\s*(\\d+)/i);
  if(pMatch)return `P${{pMatch[1]}}`;
  if(/df/i.test(value))return 'DF';
  const parts=value.split(/[ _-]+/).filter(Boolean);
  return parts.length ? parts[parts.length-1] : value;
}}
function networkChartOption(block, visibleCount){{
  const networkCount=(block.networks||[]).length;
  const effectiveVisibleCount=Math.min(Math.max(visibleCount||networkCount||1, 1), Math.max(networkCount, 1));
  const showAll = networkCount <= effectiveVisibleCount;
  const series=[];
  (STATUS_ORDER||[]).forEach(status=>{{
    [GROUP_A,GROUP_B].forEach(group=>{{
      const points=((block.groups||{{}})[group]||{{series:{{}}}}).series?.[status]||[];
      series.push({{
        name:`${{status}} · ${{GROUPS[group]||group}}`,
        type:'line',
        smooth:false,
        connectNulls:false,
        showSymbol:true,
        symbol:'circle',
        symbolSize:7,
        lineStyle:{{width:3,type:GROUP_STYLE[group]}},
        itemStyle:{{color:STATUS_COLORS[status]}},
        color:STATUS_COLORS[status],
        data:points.map(point=>({{
          value:Number((point.share||0).toFixed(6)),
          network:point.network,
          status_bucket:status,
          experiment_group:group,
          request_pv:point.request_pv||0,
          denominator_request_pv:point.denominator_request_pv||0,
          share:point.share||0,
        }})),
      }});
    }});
  }});
  return {{
    backgroundColor:'rgba(252,250,246,0.95)',
    color:STATUS_ORDER.map(status=>STATUS_COLORS[status]),
    grid:{{left:64,right:20,top:82,bottom:86,containLabel:true}},
    legend:{{type:'scroll',top:0,itemWidth:14,itemHeight:10,textStyle:{{fontSize:11,color:'#667788'}}}},
    tooltip:{{
      trigger:'axis',
      axisPointer:{{type:'line',lineStyle:{{color:'rgba(32,48,64,0.18)'}}}},
      backgroundColor:'rgba(255,255,255,0.96)',
      borderColor:'rgba(31,49,64,0.14)',
      textStyle:{{fontSize:12,color:'#1f3140'}},
      formatter(params){{
        if(!params||!params.length)return '';
        const lines=params
          .filter(item=>Number(item.data?.share||0)>0)
          .map(item=>`<div style="display:flex;justify-content:space-between;gap:10px"><span><span style="display:inline-block;width:10px;height:10px;border-radius:50%;background:${{item.color}};margin-right:6px"></span>${{item.data.status_bucket}} · ${{GROUPS[item.data.experiment_group]||item.data.experiment_group}}</span><span>${{fmtPct(item.data.share)}} / pv ${{fmtNum(item.data.request_pv)}}</span></div>`)
          .join('');
        const active=params.find(item=>Number(item.data?.denominator_request_pv||0)>0);
        return `<div style="min-width:280px"><div style="margin-bottom:6px;font-weight:600">${{params[0].axisValue}}</div>`+
          `<div style="margin-bottom:6px;color:#667788">总请求量：${{fmtNum(active?.data?.denominator_request_pv||0)}}</div>`+
          `${{lines||'<div style="color:#667788">当前 network 暂无命中数据</div>'}}</div>`;
      }}
    }},
    dataZoom:[
      {{
        type:'inside',
        xAxisIndex:0,
        filterMode:'none',
        startValue:0,
        endValue:effectiveVisibleCount-1,
        zoomLock:true,
        disabled:showAll,
      }},
      {{
        type:'slider',
        xAxisIndex:0,
        filterMode:'none',
        bottom:14,
        height:18,
        startValue:0,
        endValue:effectiveVisibleCount-1,
        showDetail:false,
        show:!showAll,
        brushSelect:false,
        moveHandleSize:0,
        borderColor:'rgba(31,49,64,0.12)',
        fillerColor:'rgba(15,118,110,0.16)',
        backgroundColor:'rgba(255,255,255,0.72)',
        dataBackground:{{lineStyle:{{color:'rgba(102,119,136,0.45)'}},areaStyle:{{color:'rgba(203,213,225,0.45)'}}}},
      }},
    ],
    xAxis:{{
      type:'category',
      data:block.networks||[],
      axisLine:{{lineStyle:{{color:'rgba(31,49,64,0.12)'}}}},
      axisTick:{{show:false}},
      axisLabel:{{color:'#667788',fontSize:11,rotate:0,interval:0,margin:10,lineHeight:14,hideOverlap:false,formatter:value=>formatNetworkAxisLabel(value)}},
    }},
    yAxis:{{
      type:'value',
      min:0,
      max:Math.max(Number(block.axis_max||0),0.1),
      axisLabel:{{formatter:value=>`${{Math.round(value*100)}}%`,color:'#667788',fontSize:11}},
      splitLine:{{lineStyle:{{color:'rgba(31,49,64,0.07)'}}}},
    }},
    series,
  }};
}}
function mountNetworkStatusChart(host,block){{
  const scroll=document.createElement('div');
  scroll.className='chart-scroll';
  const chartEl=document.createElement('div');
  chartEl.className='chart network-status-chart';
  chartEl.style.width='100%';
  scroll.appendChild(chartEl);
  host.appendChild(scroll);
  const chart=echarts.init(chartEl);
  const applyResponsiveOption = ()=>{{
    const visibleCount=calculateVisibleNetworkCount(scroll.clientWidth, (block.networks||[]).length);
    chart.setOption(networkChartOption(block, visibleCount), true);
    chart.resize();
  }};
  applyResponsiveOption();
  chart.getZr().on('mousemove', function(e){{
    const pointInGrid=chart.convertFromPixel({{seriesIndex:0}}, [e.offsetX, e.offsetY]);
    if(pointInGrid) window._current_mouse_y = pointInGrid[1];
  }});
  new ResizeObserver(()=>applyResponsiveOption()).observe(scroll);
}}
function renderUnitSelector(formatPayload,currentUnit,onChange){{
  const field=document.createElement('div');
  field.className='field network-status-unit-field';
  field.innerHTML=`<label>Unit</label><select class="network-status-unit-select"></select>`;
  const select=field.querySelector('select');
  const options=formatPayload.unit_options||[];
  if(!options.length){{
    select.innerHTML='<option value="">暂无可选 unit</option>';
    select.disabled=true;
    return field;
  }}
  select.innerHTML=options.map(item=>`<option value="${{item.value}}">${{item.label}}</option>`).join('');
  select.value=currentUnit||formatPayload.default_unit||options[0].value;
  select.onchange=e=>onChange(e.target.value);
  return field;
}}
function renderFormatBlockBody(body,platformLabel,formatKey,formatPayload,unitId){{
  body.innerHTML='';
  const unitPayload=(formatPayload.unit_map||{{}})[unitId];
  if(!unitPayload){{
    body.innerHTML=`<div class="empty">${{platformLabel}} 的 ${{formatKey}} 在当前 unit 下暂无结果。</div>`;
    return;
  }}
  const stack=document.createElement('div');
  stack.className='network-type-stack';
  body.appendChild(stack);
  NETWORK_TYPE_ORDER.forEach(typeKey=>renderNetworkTypeBlock(stack,unitPayload,typeKey));
}}
function renderNetworkTypeBlock(host,unitPayload,typeKey){{
  const block=((unitPayload.network_types||{{}})[typeKey])||{{label:typeKey,networks:[],groups:{{}},axis_max:0.1,status_order:STATUS_ORDER,empty:true}};
  const chartBox=document.createElement('div');
  chartBox.className='chart-box network-type-box';
  chartBox.innerHTML=`<div class="chart-box-head"><strong>${{NETWORK_TYPE_LABELS[typeKey]||typeKey}}</strong><span class="muted">${{unitPayload.label}} · 颜色表示 status，实线/虚线表示组别</span></div>`;
  host.appendChild(chartBox);
  if(block.empty || !(block.networks||[]).length){{
    chartBox.innerHTML += `<div class="empty">${{NETWORK_TYPE_LABELS[typeKey]||typeKey}} 在当前 unit 下暂无结果。</div>`;
    return;
  }}
  mountNetworkStatusChart(chartBox,block);
}}
function renderFormatBlock(host,platformLabel,formatKey,formatPayload){{
  const card=document.createElement('section');
  card.className='card network-status-card';
  const cardHead=document.createElement('div');
  cardHead.className='card-head';
  cardHead.innerHTML=`<div><h2>${{formatPayload.label}}</h2><div class="muted">${{platformLabel}}</div></div>`;
  card.appendChild(cardHead);
  const body=document.createElement('div');
  body.className='panel-wrap';
  card.appendChild(body);
  let currentUnit=formatPayload.default_unit||'';
  cardHead.appendChild(renderUnitSelector(formatPayload,currentUnit,(nextUnit)=>{{
    currentUnit=nextUnit;
    renderFormatBlockBody(body,platformLabel,formatKey,formatPayload,currentUnit);
  }}));
  renderFormatBlockBody(body,platformLabel,formatKey,formatPayload,currentUnit);
  host.appendChild(card);
}}
function renderPlatformSection(root,platformKey){{
  const platform=(DATA.platforms||{{}})[platformKey];
  if(!platform)return;
  const section=document.createElement('section');
  section.className='metric';
  section.innerHTML=`<div class="card"><div class="card-head"><div><h2>${{platform.label}}</h2></div></div><div class="panel-wrap network-status-grid"></div></div>`;
  root.appendChild(section);
  const grid=section.querySelector('.network-status-grid');
  FORMAT_ORDER.forEach(formatKey=>renderFormatBlock(grid,platform.label,formatKey,(platform.formats||{{}})[formatKey]||{{label:formatKey,unit_map:{{}},unit_options:[],default_unit:''}}));
}}
function renderBiddingNetworkStatusPage(){{
  const root=document.getElementById('root');
  root.innerHTML='';
  PLATFORM_ORDER.forEach(platformKey=>renderPlatformSection(root,platformKey));
}}
renderBiddingNetworkStatusPage();
"""


def build_bidding_network_status_html(payload: dict[str, Any]) -> str:
    hero_lines_html = "".join(f"<p>{line}</p>" for line in payload.get("desc", []))
    return f"""<!doctype html>
<html lang="zh-CN">
<head>
<meta charset="UTF-8" />
<meta name="viewport" content="width=device-width, initial-scale=1.0" />
<title>{payload.get("title", "Bidding Network Status Share")}</title>
<style>
body{{margin:0;font-family:"Microsoft YaHei",sans-serif;background:#f6f1e9;color:#203040}}.page{{max-width:1720px;margin:0 auto;padding:20px 16px 40px}}.hero{{display:flex;justify-content:space-between;gap:16px;flex-wrap:wrap;margin-bottom:16px}}.hero h1{{margin:8px 0 6px;font-size:30px}}.hero-copy{{display:grid;gap:4px}}.hero p{{margin:0;color:#667788;line-height:1.6}}.pill{{display:inline-flex;align-items:center;min-height:28px;padding:0 12px;border-radius:999px;border:1px solid rgba(32,48,64,.12);background:rgba(255,255,255,.92);color:#0f766e;border-color:rgba(15,118,110,.18)}}.field label{{display:block;margin-bottom:6px;font-size:12px;color:#667788}}.field select{{width:100%;padding:9px 12px;border-radius:12px;border:1px solid rgba(32,48,64,.12);background:#fff}}.metric{{margin-top:24px;display:grid;gap:12px}}.card{{border:1px solid rgba(32,48,64,.12);border-radius:24px;background:rgba(255,255,255,.82);overflow:hidden}}.card-head{{display:flex;justify-content:space-between;gap:12px;flex-wrap:wrap;padding:16px 18px;border-bottom:1px solid rgba(32,48,64,.12)}}.card-head h2{{margin:2px 0 4px;font-size:24px;text-transform:none}}.muted{{color:#667788;font-size:12px}}.panel-wrap{{display:grid;gap:16px;padding:16px 18px 18px}}.network-status-grid{{grid-template-columns:1fr;align-items:start}}.network-status-unit-field{{min-width:280px;max-width:420px;flex:1}}.network-type-stack{{display:grid;gap:16px}}.chart-box{{border-radius:16px;background:rgba(255,255,255,.62);padding:12px 14px 12px}}.network-type-box{{border:1px solid rgba(32,48,64,.08)}}.chart-box-head{{display:flex;justify-content:space-between;gap:10px;margin-bottom:10px}}.chart{{height:400px}}.chart-scroll{{overflow:hidden;padding-bottom:6px;width:100%}}.empty{{padding:14px;border-radius:14px;border:1px dashed rgba(32,48,64,.12);color:#667788;text-align:center}}
</style>
</head>
<body>
<div class="page">
  <section class="hero">
    <div>
      <span class="pill">AB Bidding Network Status Dashboard</span>
      <h1>{payload.get("title", "Bidding Network Status Share")}</h1>
      <div class="hero-copy">{hero_lines_html}</div>
    </div>
  </section>
  <div id="root"></div>
</div>
<script src="{ASSET_SCRIPT_PATH}"></script>
<script>
{build_bidding_network_status_page_script(payload)}
</script>
</body>
</html>"""


def build_winning_type_network_status_payload(rows: list[dict[str, Any]]) -> dict[str, Any]:
    unit_labels: dict[str, str] = {ALL_UNIT_OPTION_VALUE: "ALL UNIT"}
    winner_type_options_by_combo: dict[str, set[str]] = defaultdict(set)
    winner_network_options_by_combo: dict[str, set[str]] = defaultdict(set)
    denominator_by_unit: dict[tuple[str, str, str, str, str, str], float] = {}
    request_counts: dict[tuple[str, str, str, str, str, str, str, str, str], float] = defaultdict(float)

    for row in rows:
        experiment_group = str(row.get("experiment_group") or "").strip()
        product = str(row.get("product") or "").strip()
        ad_format = str(row.get("ad_format") or "").strip().lower()
        winner_network_type = str(row.get("winner_network_type") or "").strip().lower()
        winner_network = str(row.get("winner_network") or "").strip()
        unit_id = str(row.get("max_unit_id") or "").strip()
        network_type = str(row.get("network_type") or "").strip().lower()
        network = str(row.get("network") or "").strip()
        status_bucket = str(row.get("status_bucket") or "").strip().upper()
        if experiment_group not in (GROUP_A, GROUP_B):
            continue
        if ad_format not in BIDDING_NETWORK_STATUS_FORMAT_ORDER:
            continue
        if winner_network_type not in TYPE_OPTIONS or network_type not in TYPE_OPTIONS:
            continue
        if not product or not winner_network or not unit_id or not network:
            continue
        allowed_statuses = WINNING_BIDDING_STATUS_ORDER if network_type == "bidding" else WINNING_WATERFALL_STATUS_ORDER
        if status_bucket not in allowed_statuses:
            continue
        unit_label = str(row.get("ad_unit_name") or unit_id).strip()
        unit_labels[unit_id] = unit_label
        winner_type_options_by_combo[f"{product}__{ad_format}"].add(winner_network_type)
        winner_network_options_by_combo[f"{product}__{ad_format}__{winner_network_type}"].add(winner_network)
        denominator_key = (product, ad_format, winner_network_type, winner_network, unit_id, experiment_group)
        denominator_by_unit[denominator_key] = float(row.get("denominator_request_pv") or 0)
        count_key = (
            product,
            ad_format,
            winner_network_type,
            winner_network,
            unit_id,
            experiment_group,
            network_type,
            network,
            status_bucket,
        )
        request_counts[count_key] += float(row.get("request_pv") or 0)

    for (product, ad_format, winner_network_type, winner_network, unit_id, experiment_group), denominator in list(
        denominator_by_unit.items()
    ):
        all_unit_key = (product, ad_format, winner_network_type, winner_network, ALL_UNIT_OPTION_VALUE, experiment_group)
        denominator_by_unit[all_unit_key] = denominator_by_unit.get(all_unit_key, 0.0) + denominator

    for key, request_pv in list(request_counts.items()):
        product, ad_format, winner_network_type, winner_network, _, experiment_group, network_type, network, status_bucket = key
        all_unit_key = (
            product,
            ad_format,
            winner_network_type,
            winner_network,
            ALL_UNIT_OPTION_VALUE,
            experiment_group,
            network_type,
            network,
            status_bucket,
        )
        request_counts[all_unit_key] += request_pv

    combos: dict[str, Any] = {}
    combo_dimensions = sorted(
        {
            (product, ad_format, winner_network_type, winner_network)
            for product, ad_format, winner_network_type, winner_network, _, _ in denominator_by_unit
        }
    )
    for product, ad_format, winner_network_type, winner_network in combo_dimensions:
        combo_key = f"{product}__{ad_format}__{winner_network_type}__{winner_network}"
        current_units = sorted(
            {
                unit_id
                for p, a, wt, wn, unit_id, _ in denominator_by_unit
                if (p, a, wt, wn) == (product, ad_format, winner_network_type, winner_network)
            },
            key=lambda current_unit_id: (-1, "") if current_unit_id == ALL_UNIT_OPTION_VALUE else unit_sort_key(unit_labels.get(current_unit_id, current_unit_id)),
        )
        unit_map: dict[str, Any] = {}
        for unit_id in current_units:
            network_types: dict[str, Any] = {}
            for network_type in TYPE_OPTIONS:
                status_order = (
                    WINNING_BIDDING_STATUS_ORDER if network_type == "bidding" else WINNING_WATERFALL_STATUS_ORDER
                )
                current_networks = sorted(
                    {
                        network
                        for p, a, wt, wn, u, _, nt, network, _ in request_counts
                        if (p, a, wt, wn, u, nt) == (product, ad_format, winner_network_type, winner_network, unit_id, network_type)
                    },
                    key=str.lower,
                )
                rows_payload = []
                for network in current_networks:
                    groups_payload: dict[str, Any] = {}
                    for experiment_group in (GROUP_A, GROUP_B):
                        denominator = denominator_by_unit.get(
                            (product, ad_format, winner_network_type, winner_network, unit_id, experiment_group),
                            0.0,
                        )
                        statuses_payload = {}
                        for status_bucket in status_order:
                            request_pv = request_counts.get(
                                (
                                    product,
                                    ad_format,
                                    winner_network_type,
                                    winner_network,
                                    unit_id,
                                    experiment_group,
                                    network_type,
                                    network,
                                    status_bucket,
                                ),
                                0.0,
                            )
                            statuses_payload[status_bucket] = {
                                "request_pv": request_pv,
                                "share": (request_pv / denominator) if denominator else 0.0,
                            }
                        groups_payload[experiment_group] = {"statuses": statuses_payload}
                    fallback_statuses = groups_payload[GROUP_A]["statuses"] if groups_payload.get(GROUP_A) else {}
                    rows_payload.append(
                        {
                            "network": network,
                            "groups": groups_payload,
                            "statuses": fallback_statuses,
                        }
                    )
                network_types[network_type] = {
                    "rows": rows_payload,
                    "status_order": status_order,
                }
            unit_map[unit_id] = {
                "label": unit_labels.get(unit_id, unit_id),
                "groups": {
                    GROUP_A: {
                        "denominator_request_pv": denominator_by_unit.get(
                            (product, ad_format, winner_network_type, winner_network, unit_id, GROUP_A),
                            0.0,
                        )
                    },
                    GROUP_B: {
                        "denominator_request_pv": denominator_by_unit.get(
                            (product, ad_format, winner_network_type, winner_network, unit_id, GROUP_B),
                            0.0,
                        )
                    },
                },
                "bidding_status_order": WINNING_BIDDING_STATUS_ORDER,
                "waterfall_status_order": WINNING_WATERFALL_STATUS_ORDER,
                "network_types": network_types,
            }
        combos[combo_key] = {
            "product": product,
            "ad_format": ad_format,
            "winner_network_type": winner_network_type,
            "winner_network": winner_network,
            "unit_options": [{"value": unit_id, "label": unit_map[unit_id]["label"]} for unit_id in current_units],
            "unit_map": unit_map,
        }

    return {
        "title": "胜利渠道下其他渠道状态命中率",
        "desc": WINNING_TYPE_NETWORK_STATUS_TEXT,
        "groups": GROUP_LABELS,
        "products": sorted({str(row.get("product") or "") for row in rows if str(row.get("product") or "")}),
        "ad_formats": sorted({str(row.get("ad_format") or "") for row in rows if str(row.get("ad_format") or "")}),
        "winner_type_options_by_combo": {
            key: sorted(values)
            for key, values in winner_type_options_by_combo.items()
        },
        "winner_network_options_by_combo": {
            key: sorted(values)
            for key, values in winner_network_options_by_combo.items()
        },
        "combos": combos,
    }


def build_winning_type_network_status_dashboard_payload(
    rows: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    rows = list(rows) if rows is not None else (
        load_rows(WINNING_TYPE_NETWORK_STATUS_CSV) if WINNING_TYPE_NETWORK_STATUS_CSV.exists() else []
    )
    ad_unit_name_map = load_ad_unit_name_map()
    for row in rows:
        max_unit_id = str(row.get("max_unit_id") or "").strip()
        if max_unit_id and not str(row.get("ad_unit_name") or "").strip() and max_unit_id in ad_unit_name_map:
            row["ad_unit_name"] = ad_unit_name_map[max_unit_id]
    return build_winning_type_network_status_payload(rows)


def build_winning_type_network_status_page_script(payload: dict[str, Any] | None = None) -> str:
    payload_json = json.dumps(payload or {}, ensure_ascii=False)
    return f"""
const DATA={payload_json};
const GROUP_A='no_is_adx',GROUP_B='have_is_adx',GROUPS=DATA.groups||{{'no_is_adx':'A 组','have_is_adx':'B 组'}};
const ALL_UNIT='{ALL_UNIT_OPTION_VALUE}';
const ALL_UNIT_LABEL='ALL UNIT';
const fmtNum=v=>Math.round(Number(v||0)).toLocaleString(),fmtPct=v=>`${{(Number(v||0)*100).toFixed(2)}}%`;
function comboKey(product,adFormat,winnerType,winnerNetwork){{return `${{product}}__${{adFormat}}__${{winnerType}}__${{winnerNetwork}}`;}}
function comboPayload(product,adFormat,winnerType,winnerNetwork){{return (DATA.combos||{{}})[comboKey(product,adFormat,winnerType,winnerNetwork)]||null;}}
function winnerTypeOptions(product,adFormat){{return (DATA.winner_type_options_by_combo||{{}})[`${{product}}__${{adFormat}}`]||[];}}
function winnerNetworkOptions(product,adFormat,winnerType){{return (DATA.winner_network_options_by_combo||{{}})[`${{product}}__${{adFormat}}__${{winnerType}}`]||[];}}
function setOptions(select,values,current){{select.innerHTML=(values||[]).map(v=>`<option value="${{v}}">${{v}}</option>`).join('');if((values||[]).includes(current))select.value=current;else if((values||[]).length)select.value=values[0];}}
function tableBlock(title,statusOrder,rows,unitPayload){{
  const block=document.createElement('div');
  block.className='detail-card';
  block.innerHTML=`<div class="detail-top"><h4>${{title}}</h4><p>${{unitPayload.label}}；分母是当前胜利 request 总数，share 表示命中率。</p></div>`;
  if(!(rows||[]).length){{block.innerHTML+=`<div class="empty">当前筛选条件下暂无结果。</div>`;return block;}}
  let headTop='<tr><th rowspan="2" class="status-network-head sticky-col sticky-col-1">network</th>';
  (statusOrder||[]).forEach(status=>{{headTop+=`<th colspan="2">${{status}}</th>`}});
  headTop+='<th colspan="2">total</th></tr>';
  let headBottom='<tr>';
  (statusOrder||[]).forEach(()=>{{headBottom+=`<th>${{GROUPS[GROUP_A]}}</th><th>${{GROUPS[GROUP_B]}}</th>`}});
  headBottom+=`<th>${{GROUPS[GROUP_A]}}</th><th>${{GROUPS[GROUP_B]}}</th></tr>`;
  let body='';
  (rows||[]).forEach(row=>{{
    body+=`<tr><td class="network-cell sticky-col sticky-col-1">${{row.network}}</td>`;
    (statusOrder||[]).forEach(status=>{{
      const left=((row.groups||{{}})[GROUP_A]||{{statuses:{{}}}}).statuses?.[status]||{{request_pv:0,share:0}};
      const right=((row.groups||{{}})[GROUP_B]||{{statuses:{{}}}}).statuses?.[status]||{{request_pv:0,share:0}};
      body+=`<td><div class="share-main">${{fmtPct(left.share)}}</div><div class="pv-sub">pv ${{fmtNum(left.request_pv)}}</div></td>`;
      body+=`<td><div class="share-main">${{fmtPct(right.share)}}</div><div class="pv-sub">pv ${{fmtNum(right.request_pv)}}</div></td>`;
    }});
    body+=`<td class="metric4-num">${{fmtNum(((unitPayload.groups||{{}})[GROUP_A]||{{}}).denominator_request_pv||0)}}</td>`;
    body+=`<td class="metric4-num">${{fmtNum(((unitPayload.groups||{{}})[GROUP_B]||{{}}).denominator_request_pv||0)}}</td></tr>`;
  }});
  block.innerHTML+=`<div class="table-card"><div class="table-wrap metric5-wrap"><table class="metric-table metric5-table"><thead>${{headTop}}${{headBottom}}</thead><tbody>${{body}}</tbody></table></div></div>`;
  return block;
}}
function render(){{
  const root=document.getElementById('root');
  const product=document.getElementById('product-select');
  const adFormat=document.getElementById('format-select');
  const winnerType=document.getElementById('winner-type-select');
  const winnerNetwork=document.getElementById('winner-network-select');
  const unit=document.getElementById('unit-select');
  const productValue=product.value,adFormatValue=adFormat.value;
  const typeValues=winnerTypeOptions(productValue,adFormatValue);
  setOptions(winnerType,typeValues,winnerType.value);
  const networkValues=winnerNetworkOptions(productValue,adFormatValue,winnerType.value);
  setOptions(winnerNetwork,networkValues,winnerNetwork.value);
  const combo=comboPayload(productValue,adFormatValue,winnerType.value,winnerNetwork.value);
  const units=(combo?.unit_options||[]).map(item=>item.value);
  unit.innerHTML=(combo?.unit_options||[]).map(item=>`<option value="${{item.value}}">${{item.label}}</option>`).join('');
  if(units.includes(unit.value)){{unit.value=unit.value;}}else if(units.length){{unit.value=units[0];}}
  root.innerHTML='';
  if(!combo){{root.innerHTML='<div class="empty">当前筛选条件下暂无结果。</div>';return;}}
  const unitPayload=(combo.unit_map||{{}})[unit.value];
  if(!unitPayload){{root.innerHTML='<div class="empty">当前 unit 下暂无结果。</div>';return;}}
  const section=document.createElement('section');
  section.className='metric';
  section.innerHTML=`<div class="card"><div class="card-head"><div><h2>${{DATA.title||'胜利渠道下其他渠道状态命中率'}}</h2><div class="muted">${{combo.product}} / ${{combo.ad_format}} / winner = ${{combo.winner_network_type}} + ${{combo.winner_network}}</div></div></div><div class="panel-wrap"></div></div>`;
  root.appendChild(section);
  const panel=section.querySelector('.panel-wrap');
  panel.appendChild(tableBlock('其他 bidding 渠道状态命中率',unitPayload.bidding_status_order,(unitPayload.network_types?.bidding||{{rows:[]}}).rows||[],unitPayload));
  panel.appendChild(tableBlock('其他 waterfall 渠道状态命中率',unitPayload.waterfall_status_order,(unitPayload.network_types?.waterfall||{{rows:[]}}).rows||[],unitPayload));
}}
const product=document.getElementById('product-select');
const adFormat=document.getElementById('format-select');
const winnerType=document.getElementById('winner-type-select');
const winnerNetwork=document.getElementById('winner-network-select');
const unit=document.getElementById('unit-select');
setOptions(product,DATA.products||[],product.value);
setOptions(adFormat,DATA.ad_formats||[],adFormat.value);
[product,adFormat,winnerType,winnerNetwork,unit].forEach(node=>{{node.onchange=render;}});
render();
"""


def build_winning_type_network_status_html(payload: dict[str, Any]) -> str:
    hero_lines_html = "".join(f"<p>{line}</p>" for line in payload.get("desc", []))
    return f"""<!doctype html>
<html lang="zh-CN">
<head>
<meta charset="UTF-8" />
<meta name="viewport" content="width=device-width, initial-scale=1.0" />
<title>{payload.get("title", "胜利渠道下其他渠道状态命中率")}</title>
<style>
body{{margin:0;font-family:"Microsoft YaHei",sans-serif;background:#f6f1e9;color:#203040}}.page{{max-width:1500px;margin:0 auto;padding:20px 16px 40px}}.hero{{display:flex;justify-content:space-between;gap:16px;flex-wrap:wrap;margin-bottom:16px}}.hero h1{{margin:8px 0 6px;font-size:30px}}.hero-copy{{display:grid;gap:4px}}.hero p{{margin:0;color:#667788;line-height:1.6}}.pill,.chip{{display:inline-flex;align-items:center;min-height:28px;padding:0 12px;border-radius:999px;border:1px solid rgba(32,48,64,.12);background:rgba(255,255,255,.92)}}.pill{{color:#0f766e;border-color:rgba(15,118,110,.18)}}.controls{{display:grid;grid-template-columns:repeat(auto-fit,minmax(220px,260px));gap:12px;padding:12px 14px;border-radius:18px;border:1px solid rgba(32,48,64,.12);background:rgba(255,255,255,.82);position:sticky;top:10px;z-index:5;backdrop-filter:blur(8px)}}.field label{{display:block;margin-bottom:6px;font-size:12px;color:#667788}}.field select{{width:100%;padding:9px 12px;border-radius:12px;border:1px solid rgba(32,48,64,.12);background:#fff}}.metric{{margin-top:24px;display:grid;gap:12px}}.card{{border:1px solid rgba(32,48,64,.12);border-radius:24px;background:rgba(255,255,255,.82);overflow:hidden}}.card-head{{display:flex;justify-content:space-between;gap:12px;flex-wrap:wrap;padding:16px 18px;border-bottom:1px solid rgba(32,48,64,.12)}}.card-head h2{{margin:2px 0 4px;font-size:24px}}.muted{{color:#667788;font-size:12px}}.panel-wrap{{display:grid;gap:16px;padding:16px 18px 18px}}.detail-card{{border:1px solid rgba(32,48,64,.12);border-radius:20px;background:rgba(255,255,255,.7);padding:14px}}.detail-top{{margin-bottom:12px}}.detail-top h4{{margin:2px 0 0;font-size:18px}}.detail-top p{{margin:6px 0 0;color:#667788;font-size:13px}}.table-card{{padding:0;overflow:hidden}}.table-wrap{{overflow:auto;max-width:100%;border-top:1px solid rgba(32,48,64,.08)}}.metric-table{{border-collapse:separate;border-spacing:0;background:rgba(255,255,255,.88)}}.metric-table th,.metric-table td{{padding:10px 12px;border-bottom:1px solid rgba(32,48,64,.08);font-size:13px;text-align:right;vertical-align:top}}.metric-table th{{background:rgba(240,244,248,.96);color:#1f3140;font-weight:700;white-space:normal}}.metric-table td:first-child,.metric-table th.status-network-head{{text-align:left}}.network-cell{{color:#203040;white-space:normal;overflow-wrap:anywhere}}.sticky-col{{position:sticky;background:rgba(248,250,252,.98)}}.sticky-col-1{{left:0;z-index:4;min-width:220px;max-width:220px}}.metric5-table{{width:max-content;min-width:1160px}}.share-main{{font-weight:700;line-height:1.2}}.pv-sub{{margin-top:4px;font-size:11px;opacity:.86;line-height:1.2}}.empty{{padding:14px;border-radius:14px;border:1px dashed rgba(32,48,64,.12);color:#667788;text-align:center}}@media (max-width:1100px){{.controls{{position:static}}.metric5-table{{min-width:1020px}}}}
</style>
</head>
<body>
<div class="page">
  <section class="hero">
    <div>
      <span class="pill">AB Winning Type + Network Dashboard</span>
      <h1>{payload.get("title", "胜利渠道下其他渠道状态命中率")}</h1>
      <div class="hero-copy">{hero_lines_html}</div>
    </div>
  </section>
  <section class="controls">
    <div class="field"><label for="product-select">Product</label><select id="product-select"></select></div>
    <div class="field"><label for="format-select">Ad Format</label><select id="format-select"></select></div>
    <div class="field"><label for="winner-type-select">Winner Type</label><select id="winner-type-select"></select></div>
    <div class="field"><label for="winner-network-select">Winner Network</label><select id="winner-network-select"></select></div>
    <div class="field"><label for="unit-select">Unit</label><select id="unit-select"></select></div>
  </section>
  <div id="root"></div>
</div>
<script>
{build_winning_type_network_status_page_script(payload)}
</script>
</body>
</html>"""


def write_dashboards(only_pages: set[str] | None = None) -> dict[str, Path]:
    def should_write(page_key: str) -> bool:
        return only_pages is None or page_key in only_pages

    request_payload = build_request_structure_payload() if should_write("request_structure") else None
    request_country_payload = build_request_structure_country_payload() if should_write("request_structure_country") else None
    request_unit_payload = build_request_structure_unit_payload() if should_write("request_structure_unit") else None
    coverage_payload = build_coverage_analysis_payload() if should_write("coverage_analysis") else None
    null_bidding_payload = build_null_bidding_dashboard_payload() if should_write("null_bidding") else None
    bidding_network_status_payload = (
        build_bidding_network_status_dashboard_payload() if should_write("bidding_network_status") else None
    )
    winning_type_network_status_payload = (
        build_winning_type_network_status_dashboard_payload() if should_write("winning_type_network_status") else None
    )
    success_mapping_payload = build_success_mapping_dashboard_payload() if should_write("success_mapping") else None
    filled_duration_payload = build_filled_duration_dashboard_payload() if should_write("filled_duration") else None
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    if should_write("entry"):
        write_validated_html(
            ENTRY_HTML,
            build_entry_html(),
            required_strings=["AB 请求结构看板入口", "AB 请求结构看板", "请求结构分布"],
        )
    if should_write("request_structure"):
        assert request_payload is not None
        write_validated_html(
            REQUEST_STRUCTURE_HTML,
            build_dashboard_html(
                metrics=request_payload["metrics"],
                products=request_payload["products"],
                ad_formats=request_payload["ad_formats"],
                success_scopes=request_payload.get("success_scopes", []),
                page_key="request_structure",
            ),
            required_strings=["请求结构分布", REQUEST_STRUCTURE_HERO_TEXT[0], REQUEST_STRUCTURE_TEXT["metric1"][0]],
        )
    if should_write("request_structure_country"):
        assert request_country_payload is not None
        write_validated_html(
            REQUEST_STRUCTURE_COUNTRY_HTML,
            build_dashboard_html(
                metrics=request_country_payload["metrics"],
                products=request_country_payload["products"],
                ad_formats=request_country_payload["ad_formats"],
                countries=request_country_payload["countries"],
                country_options_by_combo=request_country_payload["country_options_by_combo"],
                page_key="request_structure_country",
            ),
            required_strings=["请求结构分布（Country）", REQUEST_STRUCTURE_COUNTRY_HERO_TEXT[0], REQUEST_STRUCTURE_TEXT["metric1"][0]],
        )
    if should_write("request_structure_unit"):
        assert request_unit_payload is not None
        write_validated_html(
            REQUEST_STRUCTURE_UNIT_HTML,
            build_dashboard_html(
                metrics=request_unit_payload["metrics"],
                products=request_unit_payload["products"],
                ad_formats=request_unit_payload["ad_formats"],
                units=request_unit_payload["units"],
                unit_options_by_combo=request_unit_payload["unit_options_by_combo"],
                page_key="request_structure_unit",
            ),
            required_strings=["请求结构分布（Unit）", REQUEST_STRUCTURE_UNIT_HERO_TEXT[0], REQUEST_STRUCTURE_TEXT["metric1"][0]],
        )
    if should_write("coverage_analysis"):
        assert coverage_payload is not None
        write_validated_html(
            COVERAGE_ANALYSIS_HTML,
            build_dashboard_html(
                metrics=coverage_payload["metrics"],
                products=coverage_payload["products"],
                ad_formats=coverage_payload["ad_formats"],
                page_key="coverage_analysis",
            ),
            required_strings=["覆盖率分析", COVERAGE_HERO_TEXT[0], COVERAGE_TEXT["metric1"][0]],
        )
    if should_write("null_bidding"):
        assert null_bidding_payload is not None
        write_validated_html(
            NULL_BIDDING_HTML,
            build_null_bidding_html(null_bidding_payload),
            required_strings=[NULL_BIDDING_TEXT[0], NULL_BIDDING_TEXT[1]],
        )
    if should_write("bidding_network_status"):
        assert bidding_network_status_payload is not None
        write_validated_html(
            BIDDING_NETWORK_STATUS_HTML,
            build_bidding_network_status_html(bidding_network_status_payload),
            required_strings=[BIDDING_NETWORK_STATUS_TEXT[0], BIDDING_NETWORK_STATUS_TEXT[1]],
        )
    if should_write("winning_type_network_status"):
        assert winning_type_network_status_payload is not None
        write_validated_html(
            WINNING_TYPE_NETWORK_STATUS_HTML,
            build_winning_type_network_status_html(winning_type_network_status_payload),
            required_strings=["胜利渠道", WINNING_TYPE_NETWORK_STATUS_TEXT[0]],
        )
    if should_write("success_mapping"):
        assert success_mapping_payload is not None
        write_validated_html(
            SUCCESS_MAPPING_HTML,
            build_success_mapping_html(success_mapping_payload),
            required_strings=["成功 network / placement 分布", SUCCESS_MAPPING_HERO_TEXT[0], SUCCESS_MAPPING_TEXT["network"][0]],
        )
    if should_write("filled_duration"):
        assert filled_duration_payload is not None
        write_validated_html(
            FILLED_DURATION_HTML,
            build_filled_duration_dashboard_html(filled_duration_payload),
            required_strings=["adslog_filled 时长分布", FILLED_DURATION_HERO_TEXT[0], "左闭右开", "B-A GAP"],
        )
    return {
        "entry": ENTRY_HTML,
        "request_structure": REQUEST_STRUCTURE_HTML,
        "request_structure_country": REQUEST_STRUCTURE_COUNTRY_HTML,
        "request_structure_unit": REQUEST_STRUCTURE_UNIT_HTML,
        "coverage_analysis": COVERAGE_ANALYSIS_HTML,
        "null_bidding": NULL_BIDDING_HTML,
        "bidding_network_status": BIDDING_NETWORK_STATUS_HTML,
        "winning_type_network_status": WINNING_TYPE_NETWORK_STATUS_HTML,
        "success_mapping": SUCCESS_MAPPING_HTML,
        "filled_duration": FILLED_DURATION_HTML,
    }


def write_dashboard() -> Path:
    return write_dashboards()["entry"]




def load_ad_unit_name_map() -> dict[str, str]:
    unit_name_map, _ = load_mediation_report_configuration(MEDIATION_REPORT_CSV)
    return unit_name_map


def load_configured_units_by_channel() -> dict[tuple[str, str, str, str], set[str]]:
    _, configured_units_by_channel = load_mediation_report_configuration(MEDIATION_REPORT_CSV)
    return configured_units_by_channel


def _configured_unit_scope_for_platform_format(
    configured_units_by_channel: dict[tuple[str, str, str, str], set[str]],
    platform: str,
    ad_format: str,
    network_type: str,
    network: str,
) -> list[tuple[str, str]]:
    matches: list[tuple[str, str]] = []
    for product, current_format, current_type, current_network in configured_units_by_channel:
        if infer_platform(product) != platform:
            continue
        if (current_format, current_type, current_network) != (ad_format, network_type, network):
            continue
        for unit_id in configured_units_by_channel[(product, current_format, current_type, current_network)]:
            matches.append((product, unit_id))
    return matches


def _safe_float(value: Any) -> float:
    try:
        return float(value or 0)
    except (TypeError, ValueError):
        return 0.0


def build_bidding_network_status_payload(
    rows: list[dict[str, Any]],
    overall_rows: list[dict[str, Any]] | None = None,
    configured_units_by_channel: dict[tuple[str, str, str, str], set[str]] | None = None,
) -> dict[str, Any]:
    del overall_rows
    configured_units_by_channel = configured_units_by_channel or load_configured_units_by_channel()
    grouped_rows: dict[tuple[str, str, str, str, str, str, str, str], dict[str, Any]] = {}
    network_totals: dict[tuple[str, str, str, str, str], float] = defaultdict(float)
    unit_labels: dict[str, str] = {ALL_UNIT_OPTION_VALUE: "ALL UNIT"}
    denominator_by_unit: dict[tuple[str, str, str, str, str], float] = {}
    observed_counts: dict[tuple[str, str, str, str, str, str, str, str], float] = defaultdict(float)

    for row in rows:
        experiment_group = str(row.get("experiment_group") or "").strip()
        product = str(row.get("product") or "").strip()
        ad_format = str(row.get("ad_format") or "").strip().lower()
        network_type = str(row.get("network_type") or "").strip().lower()
        network = str(row.get("network") or "").strip()
        status_bucket = str(row.get("status_bucket") or "").strip().upper()
        unit_id = str(row.get("max_unit_id") or "").strip()
        if experiment_group not in (GROUP_A, GROUP_B):
            continue
        if ad_format not in BIDDING_NETWORK_STATUS_FORMAT_ORDER:
            continue
        if network_type not in BIDDING_NETWORK_STATUS_TYPE_ORDER:
            continue
        if status_bucket not in BIDDING_NETWORK_STATUS_STATUS_ORDER:
            continue
        if not product or not network or not unit_id:
            continue
        platform = infer_platform(product)
        unit_label = str(row.get("ad_unit_name") or unit_id).strip()
        unit_labels[unit_id] = unit_label
        denominator_by_unit[(product, platform, ad_format, unit_id, experiment_group)] = max(
            denominator_by_unit.get((product, platform, ad_format, unit_id, experiment_group), 0.0),
            _safe_float(row.get("denominator_request_pv")),
        )
        observed_counts[(product, platform, ad_format, unit_id, experiment_group, network_type, network, status_bucket)] += _safe_float(row.get("request_pv"))
        grouped_rows[(platform, ad_format, unit_id, experiment_group, network_type, network, status_bucket, unit_label)] = {
            "network": network,
            "share": _safe_float(row.get("share")),
            "request_pv": _safe_float(row.get("request_pv")),
            "denominator_request_pv": _safe_float(row.get("denominator_request_pv")),
            "status_bucket": status_bucket,
            "network_type": network_type,
            "unit_id": unit_id,
            "unit_label": unit_label,
        }

    channel_keys: set[tuple[str, str, str, str]] = set()
    for product, platform, ad_format, unit_id, experiment_group, network_type, network, status_bucket in observed_counts:
        channel_keys.add((platform, ad_format, network_type, network))
    for product, ad_format, network_type, network in configured_units_by_channel:
        channel_keys.add((infer_platform(product), ad_format, network_type, network))

    for platform, ad_format, network_type, network in sorted(channel_keys):
        if ad_format not in BIDDING_NETWORK_STATUS_FORMAT_ORDER or network_type not in BIDDING_NETWORK_STATUS_TYPE_ORDER:
            continue
        scope = _configured_unit_scope_for_platform_format(configured_units_by_channel, platform, ad_format, network_type, network)
        if not scope:
            continue
        for experiment_group in (GROUP_A, GROUP_B):
            denominator_total = 0.0
            status_counts = {status: 0.0 for status in BIDDING_NETWORK_STATUS_STATUS_ORDER}
            for product, unit_id in scope:
                denominator = denominator_by_unit.get((product, platform, ad_format, unit_id, experiment_group), 0.0)
                if denominator <= 0:
                    continue
                denominator_total += denominator
                real_sum = 0.0
                for status_bucket in BIDDING_NETWORK_STATUS_STATUS_ORDER:
                    if status_bucket == "NULL":
                        continue
                    request_pv = observed_counts.get((product, platform, ad_format, unit_id, experiment_group, network_type, network, status_bucket), 0.0)
                    status_counts[status_bucket] += request_pv
                    real_sum += request_pv
                observed_null = observed_counts.get((product, platform, ad_format, unit_id, experiment_group, network_type, network, "NULL"), 0.0)
                status_counts["NULL"] += observed_null
            if denominator_total <= 0 and not any(status_counts.values()):
                continue
            for status_bucket in BIDDING_NETWORK_STATUS_STATUS_ORDER:
                request_pv = status_counts[status_bucket]
                grouped_rows[(platform, ad_format, ALL_UNIT_OPTION_VALUE, experiment_group, network_type, network, status_bucket, "ALL UNIT")] = {
                    "network": network,
                    "share": (request_pv / denominator_total) if denominator_total else 0.0,
                    "request_pv": request_pv,
                    "denominator_request_pv": denominator_total,
                    "status_bucket": status_bucket,
                    "network_type": network_type,
                    "unit_id": ALL_UNIT_OPTION_VALUE,
                    "unit_label": "ALL UNIT",
                }

    for key, payload in grouped_rows.items():
        network_totals[(key[0], key[1], key[2], key[4], key[5])] += float(payload["request_pv"])

    def sort_unit_ids(current_unit_id: str) -> tuple[int, Any]:
        if current_unit_id == ALL_UNIT_OPTION_VALUE:
            return (-1, "")
        return unit_sort_key(unit_labels.get(current_unit_id, current_unit_id))

    platforms: dict[str, Any] = {}
    for platform_key in BIDDING_NETWORK_STATUS_PLATFORM_ORDER:
        formats: dict[str, Any] = {}
        for ad_format in BIDDING_NETWORK_STATUS_FORMAT_ORDER:
            unit_map: dict[str, Any] = {}
            format_units = sorted({key[2] for key in grouped_rows if key[0] == platform_key and key[1] == ad_format}, key=sort_unit_ids)
            for unit_id in format_units:
                unit_label = unit_labels.get(unit_id, unit_id)
                network_types: dict[str, Any] = {}
                axis_max = 0.0
                for network_type in BIDDING_NETWORK_STATUS_TYPE_ORDER:
                    networks = sorted(
                        {key[5] for key in grouped_rows if key[0] == platform_key and key[1] == ad_format and key[2] == unit_id and key[4] == network_type},
                        key=lambda network_name: (-network_totals.get((platform_key, ad_format, unit_id, network_type, network_name), 0.0), network_name.lower()),
                    )
                    if not networks:
                        network_types[network_type] = {"label": network_type, "networks": [], "groups": {}, "axis_max": 0.1, "status_order": BIDDING_NETWORK_STATUS_STATUS_ORDER, "empty": True}
                        continue
                    groups: dict[str, Any] = {}
                    type_axis_max = 0.0
                    for group in (GROUP_A, GROUP_B):
                        series: dict[str, list[dict[str, Any]]] = {}
                        for status_bucket in BIDDING_NETWORK_STATUS_STATUS_ORDER:
                            points = []
                            for network_name in networks:
                                current = grouped_rows.get((platform_key, ad_format, unit_id, group, network_type, network_name, status_bucket, unit_label)) or {
                                    "network": network_name,
                                    "network_type": network_type,
                                    "share": 0.0,
                                    "request_pv": 0.0,
                                    "denominator_request_pv": 0.0,
                                    "status_bucket": status_bucket,
                                    "unit_id": unit_id,
                                    "unit_label": unit_label,
                                }
                                type_axis_max = max(type_axis_max, float(current["share"]))
                                points.append(current)
                            series[status_bucket] = points
                        groups[group] = {"series": series}
                    network_types[network_type] = {"label": network_type, "networks": networks, "groups": groups, "axis_max": type_axis_max if type_axis_max > 0 else 0.1, "status_order": BIDDING_NETWORK_STATUS_STATUS_ORDER, "empty": False}
                    axis_max = max(axis_max, network_types[network_type]["axis_max"])
                if all((network_types.get(nt, {}).get("empty", True) for nt in BIDDING_NETWORK_STATUS_TYPE_ORDER)):
                    continue
                unit_map[unit_id] = {"label": unit_label, "network_types": network_types, "axis_max": axis_max if axis_max > 0 else 0.1, "status_order": BIDDING_NETWORK_STATUS_STATUS_ORDER, "is_all_unit": unit_id == ALL_UNIT_OPTION_VALUE}
            unit_options = [{"value": unit_id, "label": unit_map[unit_id]["label"]} for unit_id in format_units if unit_id in unit_map]
            formats[ad_format] = {"label": ad_format, "unit_map": unit_map, "unit_options": unit_options, "default_unit": unit_options[0]["value"] if unit_options else ""}
        platforms[platform_key] = {"label": NULL_BIDDING_PLATFORM_LABELS[platform_key], "formats": formats}

    return {"title": "Bidding Network Status", "desc": BIDDING_NETWORK_STATUS_TEXT, "groups": GROUP_LABELS, "platform_order": BIDDING_NETWORK_STATUS_PLATFORM_ORDER, "format_order": BIDDING_NETWORK_STATUS_FORMAT_ORDER, "network_type_order": BIDDING_NETWORK_STATUS_TYPE_ORDER, "status_order": BIDDING_NETWORK_STATUS_STATUS_ORDER, "platforms": platforms}


def build_bidding_network_status_dashboard_payload(
    rows: list[dict[str, Any]] | None = None,
    overall_rows: list[dict[str, Any]] | None = None,
    configured_units_by_channel: dict[tuple[str, str, str, str], set[str]] | None = None,
) -> dict[str, Any]:
    del overall_rows
    rows = list(rows) if rows is not None else (load_rows(BIDDING_NETWORK_STATUS_CSV) if BIDDING_NETWORK_STATUS_CSV.exists() else [])
    ad_unit_name_map = load_ad_unit_name_map()
    for row in rows:
        max_unit_id = str(row.get("max_unit_id") or "").strip()
        if max_unit_id and not str(row.get("ad_unit_name") or "").strip() and max_unit_id in ad_unit_name_map:
            row["ad_unit_name"] = ad_unit_name_map[max_unit_id]
    return build_bidding_network_status_payload(rows, configured_units_by_channel=configured_units_by_channel)


def build_winning_type_network_status_payload(
    rows: list[dict[str, Any]],
    configured_units_by_channel: dict[tuple[str, str, str, str], set[str]] | None = None,
) -> dict[str, Any]:
    configured_units_by_channel = configured_units_by_channel or load_configured_units_by_channel()
    unit_labels: dict[str, str] = {ALL_UNIT_OPTION_VALUE: "ALL UNIT"}
    winner_type_options_by_combo: dict[str, set[str]] = defaultdict(set)
    winner_network_options_by_combo: dict[str, set[str]] = defaultdict(set)
    denominator_by_unit: dict[tuple[str, str, str, str, str, str], float] = {}
    request_counts: dict[tuple[str, str, str, str, str, str, str, str, str], float] = defaultdict(float)

    for row in rows:
        experiment_group = str(row.get("experiment_group") or "").strip()
        product = str(row.get("product") or "").strip()
        ad_format = str(row.get("ad_format") or "").strip().lower()
        winner_network_type = str(row.get("winner_network_type") or "").strip().lower()
        winner_network = str(row.get("winner_network") or "").strip()
        unit_id = str(row.get("max_unit_id") or "").strip()
        network_type = str(row.get("network_type") or "").strip().lower()
        network = str(row.get("network") or "").strip()
        status_bucket = str(row.get("status_bucket") or "").strip().upper()
        if experiment_group not in (GROUP_A, GROUP_B):
            continue
        if ad_format not in BIDDING_NETWORK_STATUS_FORMAT_ORDER:
            continue
        if winner_network_type not in TYPE_OPTIONS or network_type not in TYPE_OPTIONS:
            continue
        if not product or not winner_network or not unit_id or not network:
            continue
        unit_label = str(row.get("ad_unit_name") or unit_id).strip()
        unit_labels[unit_id] = unit_label
        winner_type_options_by_combo[f"{product}__{ad_format}"].add(winner_network_type)
        winner_network_options_by_combo[f"{product}__{ad_format}__{winner_network_type}"].add(winner_network)
        denominator_by_unit[(product, ad_format, winner_network_type, winner_network, unit_id, experiment_group)] = max(
            denominator_by_unit.get((product, ad_format, winner_network_type, winner_network, unit_id, experiment_group), 0.0),
            _safe_float(row.get("denominator_request_pv")),
        )
        allowed_statuses = WINNING_BIDDING_STATUS_ORDER if network_type == "bidding" else WINNING_WATERFALL_STATUS_ORDER
        if status_bucket not in allowed_statuses:
            continue
        request_counts[(product, ad_format, winner_network_type, winner_network, unit_id, experiment_group, network_type, network, status_bucket)] += _safe_float(row.get("request_pv"))

    combo_dimensions = sorted({(product, ad_format, winner_network_type, winner_network) for product, ad_format, winner_network_type, winner_network, _, _ in denominator_by_unit})
    channel_denominators: dict[tuple[str, str, str, str, str, str, str], float] = {}
    for product, ad_format, winner_network_type, winner_network in combo_dimensions:
        channel_keys: set[tuple[str, str]] = {
            (network_type, network)
            for p, a, wt, wn, _, _, network_type, network, _ in request_counts
            if (p, a, wt, wn) == (product, ad_format, winner_network_type, winner_network)
        }
        for current_product, current_format, network_type, network in configured_units_by_channel:
            if (current_product, current_format) == (product, ad_format):
                channel_keys.add((network_type, network))
        for network_type, network in sorted(channel_keys):
            scope = configured_units_by_channel.get((product, ad_format, network_type, network), set())
            if not scope:
                continue
            status_order = WINNING_BIDDING_STATUS_ORDER if network_type == "bidding" else WINNING_WATERFALL_STATUS_ORDER
            real_statuses = [status for status in status_order if status != "NULL"]
            for experiment_group in (GROUP_A, GROUP_B):
                denominator_total = 0.0
                status_counts = {status: 0.0 for status in status_order}
                for unit_id in scope:
                    denominator = denominator_by_unit.get((product, ad_format, winner_network_type, winner_network, unit_id, experiment_group), 0.0)
                    if denominator <= 0:
                        continue
                    denominator_total += denominator
                    real_sum = 0.0
                    for status_bucket in real_statuses:
                        request_pv = request_counts.get((product, ad_format, winner_network_type, winner_network, unit_id, experiment_group, network_type, network, status_bucket), 0.0)
                        status_counts[status_bucket] += request_pv
                        real_sum += request_pv
                    if network_type == "bidding":
                        observed_null = request_counts.get((product, ad_format, winner_network_type, winner_network, unit_id, experiment_group, network_type, network, "NULL"), 0.0)
                        status_counts["NULL"] += observed_null
                if denominator_total <= 0 and not any(status_counts.values()):
                    continue
                channel_denominators[(product, ad_format, winner_network_type, winner_network, experiment_group, network_type, network)] = denominator_total
                for status_bucket in status_order:
                    request_counts[(product, ad_format, winner_network_type, winner_network, ALL_UNIT_OPTION_VALUE, experiment_group, network_type, network, status_bucket)] += status_counts[status_bucket]

    combos: dict[str, Any] = {}
    for product, ad_format, winner_network_type, winner_network in combo_dimensions:
        combo_key = f"{product}__{ad_format}__{winner_network_type}__{winner_network}"
        current_units = sorted(
            {
                unit_id
                for p, a, wt, wn, unit_id, _ in denominator_by_unit
                if (p, a, wt, wn) == (product, ad_format, winner_network_type, winner_network)
            } | {ALL_UNIT_OPTION_VALUE},
            key=lambda current_unit_id: (-1, "") if current_unit_id == ALL_UNIT_OPTION_VALUE else unit_sort_key(unit_labels.get(current_unit_id, current_unit_id)),
        )
        unit_map: dict[str, Any] = {}
        for unit_id in current_units:
            network_types: dict[str, Any] = {}
            has_rows = False
            for network_type in TYPE_OPTIONS:
                status_order = WINNING_BIDDING_STATUS_ORDER if network_type == "bidding" else WINNING_WATERFALL_STATUS_ORDER
                current_networks = sorted(
                    {
                        network
                        for p, a, wt, wn, u, _, nt, network, _ in request_counts
                        if (p, a, wt, wn, u, nt) == (product, ad_format, winner_network_type, winner_network, unit_id, network_type)
                    },
                    key=str.lower,
                )
                rows_payload = []
                for network in current_networks:
                    groups_payload: dict[str, Any] = {}
                    for experiment_group in (GROUP_A, GROUP_B):
                        denominator = channel_denominators.get((product, ad_format, winner_network_type, winner_network, experiment_group, network_type, network), 0.0) if unit_id == ALL_UNIT_OPTION_VALUE else denominator_by_unit.get((product, ad_format, winner_network_type, winner_network, unit_id, experiment_group), 0.0)
                        statuses_payload = {}
                        for status_bucket in status_order:
                            request_pv = request_counts.get((product, ad_format, winner_network_type, winner_network, unit_id, experiment_group, network_type, network, status_bucket), 0.0)
                            statuses_payload[status_bucket] = {"request_pv": request_pv, "share": (request_pv / denominator) if denominator else 0.0}
                        groups_payload[experiment_group] = {"statuses": statuses_payload, "denominator_request_pv": denominator}
                    rows_payload.append({"network": network, "groups": groups_payload, "statuses": groups_payload[GROUP_A]["statuses"] if GROUP_A in groups_payload else {}})
                network_types[network_type] = {"rows": rows_payload, "status_order": status_order}
                has_rows = has_rows or bool(rows_payload)
            if not has_rows and unit_id != ALL_UNIT_OPTION_VALUE:
                continue
            combo_denominator_group_a = (
                sum(
                    denominator_by_unit.get((product, ad_format, winner_network_type, winner_network, current_real_unit, GROUP_A), 0.0)
                    for current_real_unit in current_units
                    if current_real_unit != ALL_UNIT_OPTION_VALUE
                )
                if unit_id == ALL_UNIT_OPTION_VALUE
                else denominator_by_unit.get((product, ad_format, winner_network_type, winner_network, unit_id, GROUP_A), 0.0)
            )
            combo_denominator_group_b = (
                sum(
                    denominator_by_unit.get((product, ad_format, winner_network_type, winner_network, current_real_unit, GROUP_B), 0.0)
                    for current_real_unit in current_units
                    if current_real_unit != ALL_UNIT_OPTION_VALUE
                )
                if unit_id == ALL_UNIT_OPTION_VALUE
                else denominator_by_unit.get((product, ad_format, winner_network_type, winner_network, unit_id, GROUP_B), 0.0)
            )
            unit_map[unit_id] = {
                "label": unit_labels.get(unit_id, unit_id),
                "groups": {
                    GROUP_A: {"denominator_request_pv": combo_denominator_group_a},
                    GROUP_B: {"denominator_request_pv": combo_denominator_group_b},
                },
                "bidding_status_order": WINNING_BIDDING_STATUS_ORDER,
                "waterfall_status_order": WINNING_WATERFALL_STATUS_ORDER,
                "network_types": network_types,
                "is_all_unit": unit_id == ALL_UNIT_OPTION_VALUE,
            }
        combos[combo_key] = {"product": product, "ad_format": ad_format, "winner_network_type": winner_network_type, "winner_network": winner_network, "unit_options": [{"value": unit_id, "label": unit_map[unit_id]["label"]} for unit_id in current_units if unit_id in unit_map], "unit_map": unit_map}

    return {"title": "胜利渠道下其他渠道状态命中率", "desc": WINNING_TYPE_NETWORK_STATUS_TEXT, "groups": GROUP_LABELS, "products": sorted({str(row.get("product") or "") for row in rows if str(row.get("product") or "")}), "ad_formats": sorted({str(row.get("ad_format") or "") for row in rows if str(row.get("ad_format") or "")}), "winner_type_options_by_combo": {key: sorted(values) for key, values in winner_type_options_by_combo.items()}, "winner_network_options_by_combo": {key: sorted(values) for key, values in winner_network_options_by_combo.items()}, "combos": combos}


def build_winning_type_network_status_dashboard_payload(
    rows: list[dict[str, Any]] | None = None,
    configured_units_by_channel: dict[tuple[str, str, str, str], set[str]] | None = None,
) -> dict[str, Any]:
    rows = list(rows) if rows is not None else (load_rows(WINNING_TYPE_NETWORK_STATUS_CSV) if WINNING_TYPE_NETWORK_STATUS_CSV.exists() else [])
    ad_unit_name_map = load_ad_unit_name_map()
    for row in rows:
        max_unit_id = str(row.get("max_unit_id") or "").strip()
        if max_unit_id and not str(row.get("ad_unit_name") or "").strip() and max_unit_id in ad_unit_name_map:
            row["ad_unit_name"] = ad_unit_name_map[max_unit_id]
    return build_winning_type_network_status_payload(rows, configured_units_by_channel=configured_units_by_channel)


def build_winning_type_network_status_page_script(payload: dict[str, Any] | None = None) -> str:
    payload_json = json.dumps(payload or {}, ensure_ascii=False)
    return """
const DATA=""" + payload_json + """;
const GROUP_A='no_is_adx',GROUP_B='have_is_adx',GROUPS=DATA.groups||{'no_is_adx':'A 组','have_is_adx':'B 组'};
const ALL_UNIT='""" + ALL_UNIT_OPTION_VALUE + """';
const fmtNum=v=>Math.round(Number(v||0)).toLocaleString(),fmtPct=v=>`${(Number(v||0)*100).toFixed(2)}%`;
const sortState={};
function comboKey(product,adFormat,winnerType,winnerNetwork){return `${product}__${adFormat}__${winnerType}__${winnerNetwork}`;}
function comboPayload(product,adFormat,winnerType,winnerNetwork){return (DATA.combos||{})[comboKey(product,adFormat,winnerType,winnerNetwork)]||null;}
function winnerTypeOptions(product,adFormat){return (DATA.winner_type_options_by_combo||{})[`${product}__${adFormat}`]||[];}
function winnerNetworkOptions(product,adFormat,winnerType){return (DATA.winner_network_options_by_combo||{})[`${product}__${adFormat}__${winnerType}`]||[];}
function setOptions(select,values,current){select.innerHTML=(values||[]).map(v=>`<option value="${v}">${v}</option>`).join('');if((values||[]).includes(current))select.value=current;else if((values||[]).length)select.value=values[0];}
function heatRange(values){const nums=(values||[]).filter(v=>Number.isFinite(v));if(!nums.length)return {min:0,max:1};return {min:Math.min(...nums),max:Math.max(...nums)};}
function heatStyle(value,range){const min=Number(range.min||0),max=Number(range.max||0);let norm=0;if(max>min){norm=(Number(value||0)-min)/(max-min)}else if(max>0){norm=1}norm=Math.max(0,Math.min(1,norm));const alpha=0.14+norm*0.34;const bg=`rgba(14,116,144,${alpha.toFixed(3)})`;const fg=norm>=0.62?'#f8fafc':'#102a43';return `background:${bg};color:${fg}`;}
function nextDirection(dir){if(dir==='desc')return 'asc';if(dir==='asc')return '';return 'desc';}
function columnKey(status,group){return `status:${status}:${group}`;}
function totalKey(group){return `total:${group}`;}
function metricValue(row,key){if(key.startsWith('status:')){const [,status,group]=key.split(':');return Number((((row.groups||{})[group]||{statuses:{}}).statuses?.[status]||{}).share||0);}if(key.startsWith('total:')){const [,group]=key.split(':');return Number((((row.groups||{})[group])||{}).denominator_request_pv||0);}return 0;}
function sortRows(rows,tableKey){const state=sortState[tableKey]||{};if(!state.key||!state.direction)return [...(rows||[])];const sorted=[...(rows||[])].sort((left,right)=>{const delta=metricValue(right,state.key)-metricValue(left,state.key);if(Math.abs(delta)>1e-12)return state.direction==='desc'?delta:-delta;return String(left.network||'').localeCompare(String(right.network||''),undefined,{sensitivity:'base'});});return sorted;}
function buildRanges(rows,statusOrder){const map={};(statusOrder||[]).forEach(status=>{[GROUP_A,GROUP_B].forEach(group=>{const key=columnKey(status,group);map[key]=heatRange((rows||[]).map(row=>metricValue(row,key)));});});[GROUP_A,GROUP_B].forEach(group=>{const key=totalKey(group);map[key]=heatRange((rows||[]).map(row=>metricValue(row,key)));});return map;}
function sortHeader(label,key,tableKey){const state=sortState[tableKey]||{};const direction=state.key===key?state.direction:'';const icon=direction==='desc'?'↓':direction==='asc'?'↑':'↕';return `<th data-sort-key="${key}" class="sortable"><button type="button" class="sort-btn"><span>${label}</span><span class="sort-icon">${icon}</span></button></th>`;}
function tableBlock(title,statusOrder,rows,unitPayload,tableKey){const block=document.createElement('div');block.className='detail-card';const allUnitNote=unitPayload.is_all_unit?'ALL UNIT 仅汇总该渠道已配置的 unit；不同渠道 total pv 可能不同。':'当前 unit 仍沿用该 unit 自身分母。';block.innerHTML=`<div class="detail-top"><h4>${title}</h4><p>${unitPayload.label}；${allUnitNote}</p></div>`;if(!(rows||[]).length){block.innerHTML+=`<div class="empty">当前筛选条件下暂无结果。</div>`;return block;}const sortedRows=sortRows(rows,tableKey);const ranges=buildRanges(sortedRows,statusOrder);let headTop='<tr><th rowspan="2" class="status-network-head sticky-col sticky-col-1">network</th>';(statusOrder||[]).forEach(status=>{headTop+=`<th colspan="2">${status}</th>`});headTop+='<th colspan="2">total</th></tr>';let headBottom='<tr>';(statusOrder||[]).forEach(status=>{headBottom+=sortHeader(GROUPS[GROUP_A],columnKey(status,GROUP_A),tableKey);headBottom+=sortHeader(GROUPS[GROUP_B],columnKey(status,GROUP_B),tableKey);});headBottom+=sortHeader(GROUPS[GROUP_A],totalKey(GROUP_A),tableKey);headBottom+=sortHeader(GROUPS[GROUP_B],totalKey(GROUP_B),tableKey);headBottom+='</tr>';let body='';sortedRows.forEach(row=>{body+=`<tr><td class="network-cell sticky-col sticky-col-1">${row.network}</td>`;(statusOrder||[]).forEach(status=>{const left=((row.groups||{})[GROUP_A]||{statuses:{}}).statuses?.[status]||{request_pv:0,share:0};const right=((row.groups||{})[GROUP_B]||{statuses:{}}).statuses?.[status]||{request_pv:0,share:0};const leftKey=columnKey(status,GROUP_A),rightKey=columnKey(status,GROUP_B);body+=`<td class="heat-cell metric5-value" style="${heatStyle(left.share,ranges[leftKey])}"><div class="share-main">${fmtPct(left.share)}</div><div class="pv-sub">pv ${fmtNum(left.request_pv)}</div></td>`;body+=`<td class="heat-cell metric5-value" style="${heatStyle(right.share,ranges[rightKey])}"><div class="share-main">${fmtPct(right.share)}</div><div class="pv-sub">pv ${fmtNum(right.request_pv)}</div></td>`;});const leftTotal=Number((((row.groups||{})[GROUP_A])||{}).denominator_request_pv||0);const rightTotal=Number((((row.groups||{})[GROUP_B])||{}).denominator_request_pv||0);body+=`<td class="heat-cell metric4-num" style="${heatStyle(leftTotal,ranges[totalKey(GROUP_A)])}">${fmtNum(leftTotal)}</td>`;body+=`<td class="heat-cell metric4-num" style="${heatStyle(rightTotal,ranges[totalKey(GROUP_B)])}">${fmtNum(rightTotal)}</td></tr>`;});block.innerHTML+=`<div class="table-card"><div class="table-wrap metric5-wrap"><table class="metric-table metric5-table"><thead>${headTop}${headBottom}</thead><tbody>${body}</tbody></table></div></div>`;block.querySelectorAll('th[data-sort-key] button').forEach(btn=>{btn.onclick=(event)=>{const key=event.currentTarget.parentElement.dataset.sortKey;const current=sortState[tableKey]||{};const direction=current.key===key?nextDirection(current.direction):'desc';if(direction){sortState[tableKey]={key,direction};}else{delete sortState[tableKey];}render();};});return block;}
function render(){const root=document.getElementById('root');const product=document.getElementById('product-select');const adFormat=document.getElementById('format-select');const winnerType=document.getElementById('winner-type-select');const winnerNetwork=document.getElementById('winner-network-select');const unit=document.getElementById('unit-select');const previousUnitValue=unit.value;const productValue=product.value,adFormatValue=adFormat.value;setOptions(winnerType,winnerTypeOptions(productValue,adFormatValue),winnerType.value);setOptions(winnerNetwork,winnerNetworkOptions(productValue,adFormatValue,winnerType.value),winnerNetwork.value);const combo=comboPayload(productValue,adFormatValue,winnerType.value,winnerNetwork.value);const units=(combo?.unit_options||[]).map(item=>item.value);unit.innerHTML=(combo?.unit_options||[]).map(item=>`<option value="${item.value}">${item.label}</option>`).join('');if(units.includes(previousUnitValue)){unit.value=previousUnitValue;}else if(units.length){unit.value=units[0];}else{unit.value='';}root.innerHTML='';if(!combo){root.innerHTML='<div class="empty">当前筛选条件下暂无结果。</div>';return;}const unitPayload=(combo.unit_map||{})[unit.value];if(!unitPayload){root.innerHTML='<div class="empty">当前 unit 下暂无结果。</div>';return;}const section=document.createElement('section');section.className='metric';const note=unitPayload.is_all_unit?'ALL UNIT = 仅汇总该渠道在 mediation 配置表中已配置的 unit；因此不同渠道 total pv 可能不一致。':'当前 unit 只看该 unit 内的胜利 request。';section.innerHTML=`<div class="card"><div class="card-head"><div><h2>${DATA.title||'胜利渠道下其他渠道状态命中率'}</h2><div class="muted">${combo.product} / ${combo.ad_format} / winner = ${combo.winner_network_type} + ${combo.winner_network}</div><div class="muted">${note}</div></div></div><div class="panel-wrap"></div></div>`;root.appendChild(section);const panel=section.querySelector('.panel-wrap');panel.appendChild(tableBlock('其他 bidding 渠道状态命中率',unitPayload.bidding_status_order,(unitPayload.network_types?.bidding||{rows:[]}).rows||[],unitPayload,'bidding'));panel.appendChild(tableBlock('其他 waterfall 渠道状态命中率',unitPayload.waterfall_status_order,(unitPayload.network_types?.waterfall||{rows:[]}).rows||[],unitPayload,'waterfall'));}
const product=document.getElementById('product-select');const adFormat=document.getElementById('format-select');const winnerType=document.getElementById('winner-type-select');const winnerNetwork=document.getElementById('winner-network-select');const unit=document.getElementById('unit-select');setOptions(product,DATA.products||[],product.value);setOptions(adFormat,DATA.ad_formats||[],adFormat.value);[product,adFormat,winnerType,winnerNetwork,unit].forEach(node=>{node.onchange=render;});render();
"""


def build_winning_type_network_status_html(payload: dict[str, Any]) -> str:
    hero_lines_html = "".join(f"<p>{line}</p>" for line in payload.get("desc", []))
    return f"""<!doctype html>
<html lang="zh-CN">
<head>
<meta charset="UTF-8" />
<meta name="viewport" content="width=device-width, initial-scale=1.0" />
<title>{payload.get('title', '胜利渠道下其他渠道状态命中率')}</title>
<style>
body{{margin:0;font-family:"Microsoft YaHei",sans-serif;background:#f6f1e9;color:#203040}}.page{{max-width:1500px;margin:0 auto;padding:20px 16px 40px}}.hero{{display:flex;justify-content:space-between;gap:16px;flex-wrap:wrap;margin-bottom:16px}}.hero h1{{margin:8px 0 6px;font-size:30px}}.hero-copy{{display:grid;gap:4px}}.hero p{{margin:0;color:#667788;line-height:1.6}}.pill,.chip{{display:inline-flex;align-items:center;min-height:28px;padding:0 12px;border-radius:999px;border:1px solid rgba(32,48,64,.12);background:rgba(255,255,255,.92)}}.pill{{color:#0f766e;border-color:rgba(15,118,110,.18)}}.controls{{display:grid;grid-template-columns:repeat(auto-fit,minmax(220px,260px));gap:12px;padding:12px 14px;border-radius:18px;border:1px solid rgba(32,48,64,.12);background:rgba(255,255,255,.82);position:sticky;top:10px;z-index:5;backdrop-filter:blur(8px)}}.field label{{display:block;margin-bottom:6px;font-size:12px;color:#667788}}.field select{{width:100%;padding:9px 12px;border-radius:12px;border:1px solid rgba(32,48,64,.12);background:#fff}}.metric{{margin-top:24px;display:grid;gap:12px}}.card{{border:1px solid rgba(32,48,64,.12);border-radius:24px;background:rgba(255,255,255,.82);overflow:hidden}}.card-head{{display:flex;justify-content:space-between;gap:12px;flex-wrap:wrap;padding:16px 18px;border-bottom:1px solid rgba(32,48,64,.12)}}.card-head h2{{margin:2px 0 4px;font-size:24px}}.muted{{color:#667788;font-size:12px;margin-top:4px}}.panel-wrap{{display:grid;gap:16px;padding:16px 18px 18px}}.detail-card{{border:1px solid rgba(32,48,64,.12);border-radius:20px;background:rgba(255,255,255,.7);padding:14px}}.detail-top{{margin-bottom:12px}}.detail-top h4{{margin:2px 0 0;font-size:18px}}.detail-top p{{margin:6px 0 0;color:#667788;font-size:13px}}.table-card{{padding:0;overflow:hidden}}.table-wrap{{overflow:auto;max-width:100%;border-top:1px solid rgba(32,48,64,.08)}}.metric-table{{border-collapse:separate;border-spacing:0;background:rgba(255,255,255,.88)}}.metric-table th,.metric-table td{{padding:10px 12px;border-bottom:1px solid rgba(32,48,64,.08);font-size:13px;text-align:right;vertical-align:top}}.metric-table th{{background:rgba(240,244,248,.96);color:#1f3140;font-weight:700;white-space:normal}}.metric-table td:first-child,.metric-table th.status-network-head{{text-align:left}}.network-cell{{color:#203040;white-space:normal;overflow-wrap:anywhere}}.sticky-col{{position:sticky;background:rgba(248,250,252,.98)}}.sticky-col-1{{left:0;z-index:4;min-width:220px;max-width:220px}}.metric5-table{{width:max-content;min-width:1080px}}.share-main{{font-weight:700;line-height:1.2}}.pv-sub{{margin-top:4px;font-size:11px;opacity:.86;line-height:1.2}}.heat-cell{{transition:background-color .12s ease,color .12s ease}}.sort-btn{{display:inline-flex;gap:6px;align-items:center;border:0;background:transparent;padding:0;color:inherit;font:inherit;cursor:pointer}}.sort-icon{{font-size:11px;color:#667788}}.sortable{{text-align:center!important}}.metric4-num{{white-space:nowrap}}.empty{{padding:14px;border-radius:14px;border:1px dashed rgba(32,48,64,.12);color:#667788;text-align:center}}@media (max-width:1100px){{.controls{{position:static}}.metric5-table{{min-width:980px}}}}
</style>
</head>
<body>
<div class="page">
  <section class="hero">
    <div>
      <span class="pill">AB Winning Type + Network Dashboard</span>
      <h1>{payload.get('title', '胜利渠道下其他渠道状态命中率')}</h1>
      <div class="hero-copy">{hero_lines_html}</div>
    </div>
  </section>
  <section class="controls">
    <div class="field"><label for="product-select">Product</label><select id="product-select"></select></div>
    <div class="field"><label for="format-select">Ad Format</label><select id="format-select"></select></div>
    <div class="field"><label for="winner-type-select">Winner Type</label><select id="winner-type-select"></select></div>
    <div class="field"><label for="winner-network-select">Winner Network</label><select id="winner-network-select"></select></div>
    <div class="field"><label for="unit-select">Unit</label><select id="unit-select"></select></div>
  </section>
  <div id="root"></div>
</div>
<script>{build_winning_type_network_status_page_script(payload)}</script>
</body>
</html>"""


FILLED_DURATION_BUCKET_OPTIONS_BY_PLATFORM = {
    "ios": ["-1", "0-0.5", "0.5-1", "1-2", "2-3", "3-5", "5-8", "8-12", "12-20", "20-40", "40+"],
    "android": ["-1", "0-1", "1-3", "3-5", "5-8", "8-12", "12-20", "20-30", "30-60", "60-120", "120+"],
}
FILLED_DURATION_BLOCK_SPECS = [
    {
        "block_key": "android_interstitial",
        "platform": "android",
        "product": "screw_puzzle",
        "ad_format": "interstitial",
        "title": "Android interstitial",
    },
    {
        "block_key": "android_rewarded",
        "platform": "android",
        "product": "screw_puzzle",
        "ad_format": "rewarded",
        "title": "Android rewarded",
    },
    {
        "block_key": "ios_interstitial",
        "platform": "ios",
        "product": "ios_screw_puzzle",
        "ad_format": "interstitial",
        "title": "iOS interstitial",
    },
    {
        "block_key": "ios_rewarded",
        "platform": "ios",
        "product": "ios_screw_puzzle",
        "ad_format": "rewarded",
        "title": "iOS rewarded",
    },
]
ISADX_LATENCY_BUCKET_OPTIONS = [
    "<0",
    "[0,0.01)",
    "[0.01,0.02)",
    "[0.02,0.03)",
    "[0.03,0.05)",
    "[0.05,0.08)",
    "[0.08,0.10)",
    "[0.10,0.15)",
    "[0.15,0.20)",
    "[0.20,0.30)",
    "[0.30,0.50)",
    "[0.50,1.00)",
    "[1,2)",
    "[2,5)",
    "[5,10)",
    "[10,30)",
    "30+",
]
ISADX_LATENCY_STATUS_ORDER = ["success", "fail"]
ISADX_LATENCY_BLOCK_SPECS = [
    {
        "block_key": "android_interstitial",
        "platform": "android",
        "ad_format": "interstitial",
        "title": "Android interstitial",
    },
    {
        "block_key": "android_rewarded",
        "platform": "android",
        "ad_format": "rewarded",
        "title": "Android rewarded",
    },
    {
        "block_key": "ios_interstitial",
        "platform": "ios",
        "ad_format": "interstitial",
        "title": "iOS interstitial",
    },
    {
        "block_key": "ios_rewarded",
        "platform": "ios",
        "ad_format": "rewarded",
        "title": "iOS rewarded",
    },
]


def _float_or_zero(value: Any) -> float:
    text = str(value or "").strip()
    if not text:
        return 0.0
    return float(text)


def bucket_filled_duration_label(platform: str, duration_sec: float) -> str:
    if platform not in FILLED_DURATION_BUCKET_OPTIONS_BY_PLATFORM:
        raise ValueError(f"未知平台: {platform}")
    if duration_sec < 0:
        return "-1"
    for bucket_key in FILLED_DURATION_BUCKET_OPTIONS_BY_PLATFORM[platform]:
        if bucket_key == "-1":
            continue
        if bucket_key.endswith("+"):
            threshold = float(bucket_key[:-1])
            if duration_sec >= threshold:
                return bucket_key
            continue
        left_text, right_text = bucket_key.split("-", 1)
        left = float(left_text)
        right = float(right_text)
        if left <= duration_sec < right:
            return bucket_key
    return FILLED_DURATION_BUCKET_OPTIONS_BY_PLATFORM[platform][-1]


def _filled_duration_interval_text(bucket_key: str) -> str:
    bucket = str(bucket_key).strip()
    if bucket == "-1":
        return "<0s"
    if bucket.endswith("+"):
        return f"[{bucket[:-1]}, +inf)"
    left_text, right_text = bucket.split("-", 1)
    return f"[{left_text}, {right_text})"


def _filled_duration_platform(product: str) -> str:
    lowered = str(product).strip().lower()
    return "ios" if lowered.startswith("ios") else "android"


def bucket_isadx_latency_label(latency_sec: float) -> str:
    if latency_sec < 0:
        return "<0"
    if latency_sec < 0.01:
        return "[0,0.01)"
    if latency_sec < 0.02:
        return "[0.01,0.02)"
    if latency_sec < 0.03:
        return "[0.02,0.03)"
    if latency_sec < 0.05:
        return "[0.03,0.05)"
    if latency_sec < 0.08:
        return "[0.05,0.08)"
    if latency_sec < 0.10:
        return "[0.08,0.10)"
    if latency_sec < 0.15:
        return "[0.10,0.15)"
    if latency_sec < 0.20:
        return "[0.15,0.20)"
    if latency_sec < 0.30:
        return "[0.20,0.30)"
    if latency_sec < 0.50:
        return "[0.30,0.50)"
    if latency_sec < 1.00:
        return "[0.50,1.00)"
    if latency_sec < 2.00:
        return "[1,2)"
    if latency_sec < 5.00:
        return "[2,5)"
    if latency_sec < 10.00:
        return "[5,10)"
    if latency_sec < 30.00:
        return "[10,30)"
    return "30+"


def _isadx_latency_interval_text(bucket_key: str) -> str:
    bucket = str(bucket_key).strip()
    if bucket == "<0":
        return "<0s"
    if bucket == "30+":
        return "[30, +inf)"
    return bucket


def build_filled_duration_dashboard_payload(
    rows: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    source_rows = rows if rows is not None else load_optional_rows(FILLED_DURATION_CSV)
    ad_unit_name_map = load_ad_unit_name_map() if rows is None else {}
    blocks: list[dict[str, Any]] = []
    block_map: dict[str, Any] = {}
    for spec in FILLED_DURATION_BLOCK_SPECS:
        block = {
            **spec,
            "bucket_options": list(FILLED_DURATION_BUCKET_OPTIONS_BY_PLATFORM[spec["platform"]]),
            "unit_options": [],
            "default_unit": "",
            "unit_map": {},
        }
        blocks.append(block)
        block_map[spec["block_key"]] = block

    for row in source_rows:
        experiment_group = str(row.get("experiment_group") or "").strip()
        if experiment_group not in (GROUP_A, GROUP_B):
            continue
        product = str(row.get("product") or "").strip()
        ad_format = str(row.get("ad_format") or "").strip().lower()
        max_unit_id = str(row.get("max_unit_id") or "").strip()
        if not product or ad_format not in ("interstitial", "rewarded") or not max_unit_id:
            continue
        platform = _filled_duration_platform(product)
        unit_label = str(row.get("ad_unit_name") or ad_unit_name_map.get(max_unit_id, max_unit_id)).strip()
        if not unit_label:
            unit_label = max_unit_id
        block_key = f"{platform}_{ad_format}"
        if block_key not in block_map:
            continue
        block = block_map[block_key]
        bucket_key = bucket_filled_duration_label(platform, _float_or_zero(row.get("duration_sec_2dp")))
        filled_pv = _float_or_zero(row.get("filled_pv"))
        denominator_filled_pv = _float_or_zero(row.get("denominator_filled_pv"))
        unit_payload = block["unit_map"].setdefault(
            unit_label,
            {
                "unit": unit_label,
                "max_unit_id": max_unit_id,
                "groups": {
                    GROUP_A: {"bucket_map": {}},
                    GROUP_B: {"bucket_map": {}},
                },
                "total_filled_pv": 0.0,
            },
        )
        unit_payload["total_filled_pv"] += filled_pv
        group_bucket_map = unit_payload["groups"][experiment_group]["bucket_map"]
        bucket_payload = group_bucket_map.setdefault(
            bucket_key,
            {
                "bucket_key": bucket_key,
                "interval_text": _filled_duration_interval_text(bucket_key),
                "filled_pv": 0.0,
                "denominator_filled_pv": 0.0,
                "share": 0.0,
            },
        )
        bucket_payload["filled_pv"] += filled_pv
        bucket_payload["denominator_filled_pv"] = max(bucket_payload["denominator_filled_pv"], denominator_filled_pv)
    for block in blocks:
        block["unit_options"] = sorted(block["unit_map"].keys(), key=unit_sort_key)
        if block["unit_options"]:
            block["default_unit"] = max(
                block["unit_options"],
                key=lambda unit: (
                    float(block["unit_map"][unit]["total_filled_pv"]),
                    unit_sort_key(unit),
                ),
            )
        for group_key in (GROUP_A, GROUP_B):
            for unit_payload in block["unit_map"].values():
                bucket_map = unit_payload["groups"][group_key]["bucket_map"]
                denominator = max(
                    (
                        float(payload.get("denominator_filled_pv") or 0.0)
                        for payload in bucket_map.values()
                    ),
                    default=0.0,
                )
                for bucket_key in block["bucket_options"]:
                    payload = bucket_map.setdefault(
                        bucket_key,
                        {
                            "bucket_key": bucket_key,
                            "interval_text": _filled_duration_interval_text(bucket_key),
                            "filled_pv": 0.0,
                            "denominator_filled_pv": denominator,
                            "share": 0.0,
                        },
                    )
                    payload["denominator_filled_pv"] = max(float(payload.get("denominator_filled_pv") or 0.0), denominator)
                    current_denominator = float(payload["denominator_filled_pv"] or 0.0)
                    payload["share"] = (float(payload["filled_pv"] or 0.0) / current_denominator) if current_denominator else 0.0

    return {
        "groups": GROUP_LABELS,
        "blocks": blocks,
        "block_map": block_map,
    }


def build_filled_duration_page_script() -> str:
    return """
const BLOCKS=DATA.blocks||[];
const GROUP_A='no_is_adx',GROUP_B='have_is_adx',GROUPS=DATA.groups||{};
const fmtNum=v=>Math.round(Number(v||0)).toLocaleString(),fmtPct=v=>`${(Number(v||0)*100).toFixed(2)}%`;
const pageError=document.getElementById('page-error');
function setError(message){
  if(!pageError) return;
  pageError.hidden=!message;
  pageError.textContent=message||'';
}
const blockState=Object.fromEntries(BLOCKS.map(block=>[block.block_key, block.default_unit||'']));
function filledDurationIntervalText(bucketKey){
  if(bucketKey === '-1') return '<0s';
  if(String(bucketKey||'').endsWith('+')) return `[${String(bucketKey).slice(0, -1)}, +inf)`;
  const parts=String(bucketKey||'').split('-');
  if(parts.length !== 2) return String(bucketKey||'');
  return `[${parts[0]}, ${parts[1]})`;
}
function buildFilledDurationSeries(groupPayload,bucketOptions){
  const bucketMap=(groupPayload||{}).bucket_map||{};
  return (bucketOptions||[]).map(bucketKey=>{
    const current=bucketMap[bucketKey]||{filled_pv:0,share:0,denominator_filled_pv:0};
    return {
      value:Number((current.share||0).toFixed(6)),
      bucketKey,
      intervalText:current.interval_text||filledDurationIntervalText(bucketKey),
      filledPv:Number(current.filled_pv||0),
      denominatorFilledPv:Number(current.denominator_filled_pv||0),
      share:Number(current.share||0)
    };
  });
}
function resolvePlatformAxisMax(platform){
  const relatedBlocks=BLOCKS.filter(block=>block.platform===platform);
  let maxShare=0;
  relatedBlocks.forEach(block=>{
    const unitPayload=(block.unit_map||{})[blockState[block.block_key]];
    if(!unitPayload) return;
    [GROUP_A,GROUP_B].forEach(groupKey=>{
      (block.bucket_options||[]).forEach(bucketKey=>{
        const current=(((unitPayload.groups||{})[groupKey]||{bucket_map:{}}).bucket_map||{})[bucketKey]||{share:0};
        maxShare=Math.max(maxShare, Number(current.share||0));
      });
    });
  });
  return maxShare>0 ? Math.min(1, Math.max(0.05, maxShare*1.1)) : 0.05;
}
function buildFilledDurationOption(block,unitPayload,axisMax){
  const bucketOptions=block.bucket_options||[];
  const groupAData=buildFilledDurationSeries((unitPayload.groups||{})[GROUP_A],bucketOptions);
  const groupBData=buildFilledDurationSeries((unitPayload.groups||{})[GROUP_B],bucketOptions);
  const gapData=bucketOptions.map((bucketKey, index)=>{
    const a=groupAData[index]||{share:0};
    const b=groupBData[index]||{share:0};
    return {
      value:(Number(b.share||0)-Number(a.share||0))*100,
      bucketKey,
      shareGap:(Number(b.share||0)-Number(a.share||0))
    };
  });
  return {
    backgroundColor:'rgba(252,250,246,0.95)',
    grid:{left:54,right:54,top:28,bottom:65},
    legend:{show:true,top:0,textStyle:{fontSize:11,color:'#667788'}},
    tooltip:{
      trigger:'axis',
      axisPointer:{type:'shadow'},
      backgroundColor:'rgba(255,255,255,0.96)',
      borderColor:'rgba(31,49,64,0.14)',
      textStyle:{fontSize:12,color:'#1f3140'},
      formatter(params){
        if(!params||!params.length)return '';
        const a=params.find(item=>item.seriesName===GROUPS[GROUP_A])||{data:{}};
        const b=params.find(item=>item.seriesName===GROUPS[GROUP_B])||{data:{}};
        const bucketKey=a.data.bucketKey||b.data.bucketKey||params[0].axisValue||'';
        const intervalText=a.data.intervalText||b.data.intervalText||filledDurationIntervalText(bucketKey);
        const gap=(Number(b.data.share||0)-Number(a.data.share||0));
        const sign=gap>0?'+':'';
        const gapColor=gap>=0?'#0f766e':'#e11d48';
        return `<div style="min-width:240px"><div style="margin-bottom:6px;font-weight:600">时长区间 ${bucketKey} · ${intervalText}</div><table style="width:100%;border-collapse:collapse;font-size:12px"><tr><td style="color:#667788;padding:2px 6px 2px 0">${GROUPS[GROUP_A]} pv</td><td style="text-align:right">${fmtNum(a.data.filledPv||0)}</td></tr><tr><td style="color:#667788;padding:2px 6px 2px 0">${GROUPS[GROUP_A]} 占比</td><td style="text-align:right;font-weight:600">${fmtPct(a.data.share||0)}</td></tr><tr><td style="color:#667788;padding:2px 6px 2px 0">${GROUPS[GROUP_B]} pv</td><td style="text-align:right">${fmtNum(b.data.filledPv||0)}</td></tr><tr><td style="color:#667788;padding:2px 6px 2px 0">${GROUPS[GROUP_B]} 占比</td><td style="text-align:right;font-weight:600">${fmtPct(b.data.share||0)}</td></tr><tr><td style="color:#667788;padding:2px 6px 2px 0">B-A GAP</td><td style="text-align:right;color:${gapColor};font-weight:600">${sign}${fmtPct(gap)}</td></tr></table></div>`;
      }
    },
    xAxis:{type:'category',data:bucketOptions,axisLine:{lineStyle:{color:'rgba(31,49,64,0.12)'}},axisTick:{show:false},axisLabel:{color:'#667788',fontSize:11,interval:0}},
    yAxis:[
      {type:'value',min:0,max:axisMax,axisLabel:{formatter:value=>`${(value*100).toFixed(1)}%`,color:'#667788',fontSize:11},splitLine:{lineStyle:{color:'rgba(31,49,64,0.07)'}}},
      {type:'value',position:'right',axisLabel:{formatter:value=>`${value.toFixed(1)}pp`,color:'#7c3aed',fontSize:11},splitLine:{show:false}}
    ],
    series:[
      {name:GROUPS[GROUP_A],type:'bar',barMaxWidth:30,itemStyle:{color:'#0f766e'},data:groupAData},
      {name:GROUPS[GROUP_B],type:'bar',barMaxWidth:30,itemStyle:{color:'#2563eb'},data:groupBData},
      {name:'B-A GAP',type:'line',yAxisIndex:1,symbol:'circle',symbolSize:6,smooth:false,lineStyle:{width:2,color:'#7c3aed'},itemStyle:{color:'#7c3aed'},data:gapData}
    ]
  };
}
function renderBlock(block,axisMax){
  const selectedUnit=blockState[block.block_key];
  const unitPayload=(block.unit_map||{})[selectedUnit];
  const sec=document.createElement('section');
  sec.className='metric';
  if(!unitPayload){
    sec.innerHTML=`<div class="card"><div class="card-head"><div><h2>${block.title}</h2><div class="muted">${block.platform} / ${block.ad_format}</div></div></div><div class="panel-wrap"><div class="detail-card"><div class="detail-top"><h4>${block.title}</h4><p>当前块内暂无可选 unit。</p></div><div class="empty">当前筛选条件下暂无结果。</div></div></div></div>`;
    return sec;
  }
  const options=(block.unit_options||[]).map(unit=>`<option value="${unit}">${unit}</option>`).join('');
  sec.innerHTML=`<div class="card"><div class="card-head"><div><h2>${block.title}</h2><div class="muted">同平台共用纵轴上限；当前上限 ${(axisMax*100).toFixed(2)}%</div></div></div><div class="toolbar"><div class="toolbar-field"><label for="unit-${block.block_key}">Unit</label><select id="unit-${block.block_key}" class="block-unit-select">${options}</select></div></div><div class="panel-wrap"><div class="detail-card"><div class="detail-top"><h4>${block.title}</h4><p>当前平台使用独立分桶；常规区间均为左闭右开。-1 表示 &lt;0s。</p></div><div class="chart-scroll"><div id="chart-${block.block_key}" class="chart"></div></div></div></div></div>`;
  const select=sec.querySelector(`#unit-${block.block_key}`);
  select.value=selectedUnit;
  select.onchange=(event)=>{blockState[block.block_key]=event.target.value; renderFilledDurationPage();};
  return sec;
}
function renderFilledDurationPage(){
  setError('');
  const root=document.getElementById('root');
  root.innerHTML='';
  BLOCKS.forEach(block=>{
    const axisMax=resolvePlatformAxisMax(block.platform);
    const sec=renderBlock(block, axisMax);
    root.appendChild(sec);
    const unitPayload=(block.unit_map||{})[blockState[block.block_key]];
    if(!unitPayload) return;
    const chartEl=sec.querySelector(`#chart-${block.block_key}`);
    const chart=echarts.init(chartEl);
    chart.setOption(buildFilledDurationOption(block, unitPayload, axisMax));
    new ResizeObserver(()=>chart.resize()).observe(sec);
  });
}
renderFilledDurationPage();
"""


def build_filled_duration_dashboard_html(payload: dict[str, Any]) -> str:
    title = "adslog_filled 时长分布"
    desc = FILLED_DURATION_HERO_TEXT + [
        "当前页面固定展示四块：Android interstitial、Android rewarded、iOS interstitial、iOS rewarded。",
        "每块只保留自己的 unit selector；Android 两块共用一个纵轴上限，iOS 两块共用一个纵轴上限。",
    ]
    hero_lines_html = "".join(f"<p>{line}</p>" for line in desc)
    script_payload = payload
    return f"""<!doctype html>
<html lang="zh-CN">
<head>
<meta charset="UTF-8" />
<meta name="viewport" content="width=device-width, initial-scale=1.0" />
<title>{title}</title>
<style>
body{{margin:0;font-family:"Microsoft YaHei",sans-serif;background:#f6f1e9;color:#203040}}.page{{max-width:1500px;margin:0 auto;padding:20px 16px 40px}}.hero{{display:flex;justify-content:space-between;gap:16px;flex-wrap:wrap;margin-bottom:16px}}.hero h1{{margin:8px 0 6px;font-size:30px}}.hero-copy{{display:grid;gap:4px}}.hero p{{margin:0;color:#667788;line-height:1.6}}.pill,.chip{{display:inline-flex;align-items:center;min-height:28px;padding:0 12px;border-radius:999px;border:1px solid rgba(32,48,64,.12);background:rgba(255,255,255,.92)}}.pill{{color:#0f766e;border-color:rgba(15,118,110,.18)}}.controls{{display:grid;grid-template-columns:repeat(auto-fit,minmax(220px,260px));gap:12px;padding:12px 14px;border-radius:18px;border:1px solid rgba(32,48,64,.12);background:rgba(255,255,255,.82);position:sticky;top:10px;z-index:5;backdrop-filter:blur(8px)}}.field label,.toolbar-field label{{display:block;margin-bottom:6px;font-size:12px;color:#667788}}.field select,.toolbar-field select{{width:100%;padding:9px 12px;border-radius:12px;border:1px solid rgba(32,48,64,.12);background:#fff}}.metric{{margin-top:24px;display:grid;gap:12px}}.card{{border:1px solid rgba(32,48,64,.12);border-radius:24px;background:rgba(255,255,255,.82);overflow:hidden}}.card-head{{display:flex;justify-content:space-between;gap:12px;flex-wrap:wrap;padding:16px 18px;border-bottom:1px solid rgba(32,48,64,.12)}}.card-head h2{{margin:2px 0 4px;font-size:24px}}.muted{{color:#667788;font-size:12px}}.explain{{margin:16px 18px 0;padding:14px 16px;border:1px solid rgba(32,48,64,.12);border-radius:18px;background:rgba(255,255,255,.72);min-width:0}}.explain h4{{margin:0 0 10px;font-size:15px}}.explain ul{{margin:0;padding-left:18px;display:grid;gap:6px;min-width:0}}.explain li{{line-height:1.65;overflow-wrap:anywhere;word-break:break-word}}.panel-wrap{{display:grid;gap:16px;padding:16px 18px 18px}}.detail-card{{border:1px solid rgba(32,48,64,.12);border-radius:20px;background:rgba(255,255,255,.7);padding:14px}}.detail-top{{margin-bottom:12px}}.detail-top h4{{margin:2px 0 0;font-size:18px}}.detail-top p{{margin:6px 0 0;color:#667788;font-size:13px}}.chart-scroll{{overflow-x:auto;padding-bottom:6px;width:100%}}.chart{{width:100%;height:360px}}.empty,.page-error{{padding:14px;border-radius:14px;border:1px dashed rgba(32,48,64,.12);color:#667788;text-align:center}}.page-error{{border-style:solid;background:rgba(254,226,226,.86);color:#991b1b}}@media (max-width:1100px){{.controls{{position:static}}}}
</style>
</head>
<body class="ab-filled-duration">
<div class="page">
  <section class="hero">
    <div>
      <span class="pill">AB Filled Duration Dashboard</span>
      <h1>{title}</h1>
      <div class="hero-copy">{hero_lines_html}</div>
    </div>
    <div><span class="chip">四块固定布局</span></div>
  </section>
  <div id="page-error" class="page-error" hidden></div>
  <div id="root"></div>
</div>
<script src="{ASSET_SCRIPT_PATH}"></script>
<script>
const DATA={json.dumps(script_payload, ensure_ascii=False)};
{build_filled_duration_page_script()}
</script>
</body>
</html>"""


def build_isadx_latency_dashboard_payload(
    rows: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    source_rows = rows if rows is not None else load_optional_rows(ISADX_LATENCY_CSV)
    ad_unit_name_map = load_ad_unit_name_map() if rows is None else {}
    blocks: list[dict[str, Any]] = []
    block_map: dict[str, Any] = {}
    for spec in ISADX_LATENCY_BLOCK_SPECS:
        block = {
            **spec,
            "bucket_options": list(ISADX_LATENCY_BUCKET_OPTIONS),
            "status_order": list(ISADX_LATENCY_STATUS_ORDER),
            "unit_options": [],
            "default_unit": "",
            "unit_map": {},
        }
        blocks.append(block)
        block_map[spec["block_key"]] = block

    for row in source_rows:
        experiment_group = str(row.get("experiment_group") or "").strip()
        request_status = str(row.get("request_status") or "").strip()
        product = str(row.get("product") or "").strip()
        ad_format = str(row.get("ad_format") or "").strip().lower()
        max_unit_id = str(row.get("max_unit_id") or "").strip()
        if experiment_group not in (GROUP_A, GROUP_B):
            continue
        if request_status not in ISADX_LATENCY_STATUS_ORDER:
            continue
        if not product or ad_format not in ("interstitial", "rewarded") or not max_unit_id:
            continue
        block_key = f"{infer_platform(product)}_{ad_format}"
        if block_key not in block_map:
            continue
        block = block_map[block_key]
        unit_label = str(row.get("ad_unit_name") or ad_unit_name_map.get(max_unit_id, max_unit_id)).strip() or max_unit_id
        bucket_key = bucket_isadx_latency_label(_float_or_zero(row.get("latency_bucket_raw")))
        request_pv = _float_or_zero(row.get("request_pv"))
        denominator_request_pv = _float_or_zero(row.get("denominator_request_pv"))
        unit_payload = block["unit_map"].setdefault(
            unit_label,
            {
                "unit": unit_label,
                "max_unit_id": max_unit_id,
                "total_request_pv": 0.0,
                "status_map": {
                    status_key: {
                        "groups": {
                            GROUP_A: {"bucket_map": {}},
                            GROUP_B: {"bucket_map": {}},
                        }
                    }
                    for status_key in ISADX_LATENCY_STATUS_ORDER
                },
            },
        )
        unit_payload["total_request_pv"] += request_pv
        group_bucket_map = unit_payload["status_map"][request_status]["groups"][experiment_group]["bucket_map"]
        bucket_payload = group_bucket_map.setdefault(
            bucket_key,
            {
                "bucket_key": bucket_key,
                "interval_text": _isadx_latency_interval_text(bucket_key),
                "request_pv": 0.0,
                "denominator_request_pv": 0.0,
                "share": 0.0,
                "gap_share": 0.0,
            },
        )
        bucket_payload["request_pv"] += request_pv
        bucket_payload["denominator_request_pv"] = max(
            float(bucket_payload.get("denominator_request_pv") or 0.0),
            denominator_request_pv,
        )

    for block in blocks:
        block["unit_options"] = sorted(block["unit_map"].keys(), key=unit_sort_key)
        if block["unit_options"]:
            block["default_unit"] = max(
                block["unit_options"],
                key=lambda unit: (
                    float(block["unit_map"][unit]["total_request_pv"]),
                    unit_sort_key(unit),
                ),
            )
        for unit_payload in block["unit_map"].values():
            for status_key in ISADX_LATENCY_STATUS_ORDER:
                status_payload = unit_payload["status_map"][status_key]
                for group_key in (GROUP_A, GROUP_B):
                    bucket_map = status_payload["groups"][group_key]["bucket_map"]
                    denominator = max(
                        (float(payload.get("denominator_request_pv") or 0.0) for payload in bucket_map.values()),
                        default=0.0,
                    )
                    for bucket_key in ISADX_LATENCY_BUCKET_OPTIONS:
                        payload = bucket_map.setdefault(
                            bucket_key,
                            {
                                "bucket_key": bucket_key,
                                "interval_text": _isadx_latency_interval_text(bucket_key),
                                "request_pv": 0.0,
                                "denominator_request_pv": denominator,
                                "share": 0.0,
                                "gap_share": 0.0,
                            },
                        )
                        payload["denominator_request_pv"] = max(
                            float(payload.get("denominator_request_pv") or 0.0),
                            denominator,
                        )
                        current_denominator = float(payload["denominator_request_pv"] or 0.0)
                        payload["share"] = (
                            float(payload["request_pv"] or 0.0) / current_denominator if current_denominator else 0.0
                        )
                a_bucket_map = status_payload["groups"][GROUP_A]["bucket_map"]
                b_bucket_map = status_payload["groups"][GROUP_B]["bucket_map"]
                for bucket_key in ISADX_LATENCY_BUCKET_OPTIONS:
                    gap_share = float(b_bucket_map[bucket_key]["share"]) - float(a_bucket_map[bucket_key]["share"])
                    a_bucket_map[bucket_key]["gap_share"] = gap_share
                    b_bucket_map[bucket_key]["gap_share"] = gap_share

    return {
        "groups": GROUP_LABELS,
        "blocks": blocks,
        "block_map": block_map,
    }


def build_isadx_latency_page_script() -> str:
    return """
const BLOCKS=DATA.blocks||[];
const GROUP_A='no_is_adx',GROUP_B='have_is_adx',GROUPS=DATA.groups||{};
const BAR_A='#1d4ed8',BAR_B='#f97316',GAP='#7c3aed',ZERO='#dc2626';
const fmtNum=v=>Math.round(Number(v||0)).toLocaleString(),fmtPct=v=>`${(Number(v||0)*100).toFixed(2)}%`;
const pageError=document.getElementById('page-error');
function setError(message){if(!pageError)return;pageError.hidden=!message;pageError.textContent=message||'';}
const blockState={};
BLOCKS.forEach(block=>{blockState[block.block_key]=block.default_unit||((block.unit_options||[])[0]||'');});
function statusTitle(status){return status==='success'?'success':'fail';}
function buildStatusOption(block,unitPayload,status){
  const statusPayload=((unitPayload.status_map||{})[status])||{groups:{}};
  const groupA=((statusPayload.groups||{})[GROUP_A]||{}).bucket_map||{};
  const groupB=((statusPayload.groups||{})[GROUP_B]||{}).bucket_map||{};
  const keys=block.bucket_options||[];
  const noSeries=keys.map(key=>({value:Number((groupA[key]||{}).share||0),meta:groupA[key]||{}}));
  const haveSeries=keys.map(key=>({value:Number((groupB[key]||{}).share||0),meta:groupB[key]||{}}));
  const gapSeries=keys.map(key=>Number((groupB[key]||{}).gap_share||0));
  return {
    animation:false,
    color:[BAR_A,BAR_B,GAP],
    grid:{left:56,right:56,top:34,bottom:72},
    legend:{bottom:0,itemWidth:14,itemHeight:10,textStyle:{fontSize:11,color:'#667788'}},
    tooltip:{
      trigger:'axis',
      axisPointer:{type:'line',lineStyle:{color:'rgba(32,48,64,0.18)'}},
      backgroundColor:'rgba(255,255,255,0.97)',
      borderColor:'rgba(31,49,64,0.14)',
      textStyle:{fontSize:12,color:'#1f3140'},
      formatter(params){
        const idx=(params&&params.length)?params[0].dataIndex:0;
        const a=(noSeries[idx]||{}).meta||{},b=(haveSeries[idx]||{}).meta||{},gap=gapSeries[idx]||0;
        const sign=gap>0?'+':'';
        return `<div style="min-width:220px"><div style="margin-bottom:6px;font-weight:600">${keys[idx]||''}</div>`
          + `<div>${GROUPS[GROUP_A]} request_pv: ${fmtNum(a.request_pv||0)}</div>`
          + `<div>${GROUPS[GROUP_A]} 占比: ${fmtPct(a.share||0)}</div>`
          + `<div>${GROUPS[GROUP_B]} request_pv: ${fmtNum(b.request_pv||0)}</div>`
          + `<div>${GROUPS[GROUP_B]} 占比: ${fmtPct(b.share||0)}</div>`
          + `<div style="color:${gap>=0?ZERO:'#0f766e'};font-weight:700">B-A GAP: ${sign}${fmtPct(gap)}</div></div>`;
      }
    },
    xAxis:{
      type:'category',
      data:keys,
      axisLabel:{color:'#667788',fontSize:11,interval:0,rotate:28},
      axisLine:{lineStyle:{color:'rgba(31,49,64,0.12)'}},
      axisTick:{show:false}
    },
    yAxis:[
      {
        type:'value',
        name:'占比',
        axisLabel:{color:'#667788',formatter:value=>`${(value*100).toFixed(0)}%`},
        axisLine:{show:false},
        splitLine:{show:true,lineStyle:{color:'rgba(31,49,64,0.10)',width:1}}
      },
      {
        type:'value',
        name:'B-A GAP',
        axisLabel:{color:'#667788',formatter:value=>`${(value*100).toFixed(0)}%`},
        axisLine:{show:false},
        splitLine:{show:false}
      }
    ],
    series:[
      {name:GROUPS[GROUP_A],type:'bar',barMaxWidth:28,data:noSeries},
      {name:GROUPS[GROUP_B],type:'bar',barMaxWidth:28,data:haveSeries},
      {name:'B-A GAP',type:'line',yAxisIndex:1,symbol:'circle',symbolSize:6,lineStyle:{width:2,color:GAP},itemStyle:{color:GAP},data:gapSeries,
       markLine:{symbol:['none','none'],silent:true,label:{show:false},lineStyle:{color:ZERO,width:2},data:[{yAxis:0}]}}
    ]
  };
}
function renderStatusSection(host,block,unitPayload,status){
  const wrap=document.createElement('div');
  wrap.className='detail-card';
  wrap.innerHTML=`<div class="detail-top"><h4>${statusTitle(status)}</h4><p>柱状图展示 A/B 占比，折线展示 B-A GAP；展示桶仅用于当前页面。</p></div><div class="chart-scroll"><div id="chart-${block.block_key}-${status}" class="chart"></div></div>`;
  host.appendChild(wrap);
  const chartEl=wrap.querySelector('.chart');
  chartEl.style.width=`${Math.max(1120, (block.bucket_options||[]).length*82)}px`;
  const chart=echarts.init(chartEl);
  chart.setOption(buildStatusOption(block,unitPayload,status));
  new ResizeObserver(()=>chart.resize()).observe(wrap);
}
function renderBlock(block){
  const selectedUnit=blockState[block.block_key];
  const unitPayload=(block.unit_map||{})[selectedUnit];
  const sec=document.createElement('section');
  sec.className='metric';
  if(!unitPayload){
    sec.innerHTML=`<div class="card"><div class="card-head"><div><h2>${block.title}</h2><div class="muted">${block.platform} / ${block.ad_format}</div></div></div><div class="panel-wrap"><div class="empty">当前块内暂无可选 unit。</div></div></div>`;
    return sec;
  }
  const options=(block.unit_options||[]).map(unit=>`<option value="${unit}">${unit}</option>`).join('');
  sec.innerHTML=`<div class="card"><div class="card-head"><div><h2>${block.title}</h2><div class="muted">双轴图：左轴占比，右轴 B-A GAP；0 基线已单独高亮。</div></div></div><div class="toolbar"><div class="toolbar-field"><label for="unit-${block.block_key}">Unit</label><select id="unit-${block.block_key}" class="block-unit-select">${options}</select></div></div><div class="panel-wrap" id="panel-${block.block_key}"></div></div>`;
  const select=sec.querySelector(`#unit-${block.block_key}`);
  select.value=selectedUnit;
  select.onchange=(event)=>{blockState[block.block_key]=event.target.value; renderIsadxLatencyPage();};
  const panel=sec.querySelector(`#panel-${block.block_key}`);
  (block.status_order||[]).forEach(status=>renderStatusSection(panel,block,unitPayload,status));
  return sec;
}
function renderIsadxLatencyPage(){
  setError('');
  const root=document.getElementById('root');
  root.innerHTML='';
  BLOCKS.forEach(block=>root.appendChild(renderBlock(block)));
}
renderIsadxLatencyPage();
"""


def build_isadx_latency_dashboard_html(payload: dict[str, Any]) -> str:
    title = "IsAdx latency 分布差异"
    desc = ISADX_LATENCY_HERO_TEXT + [
        "当前页面固定展示四块：Android interstitial、Android rewarded、iOS interstitial、iOS rewarded。",
        "每块保留自己的 unit selector；同页同时展示 success 和 fail。",
    ]
    hero_lines_html = "".join(f"<p>{line}</p>" for line in desc)
    script_payload = payload
    return f"""<!doctype html>
<html lang="zh-CN">
<head>
<meta charset="UTF-8" />
<meta name="viewport" content="width=device-width, initial-scale=1.0" />
<title>{title}</title>
<style>
body{{margin:0;font-family:"Microsoft YaHei",sans-serif;background:#f6f1e9;color:#203040}}.page{{max-width:1500px;margin:0 auto;padding:20px 16px 40px;box-sizing:border-box}}.hero{{display:flex;justify-content:space-between;gap:16px;flex-wrap:wrap;margin-bottom:16px}}.hero h1{{margin:8px 0 6px;font-size:30px}}.hero-copy{{display:grid;gap:4px}}.hero p{{margin:0;color:#667788;line-height:1.6}}.pill,.chip{{display:inline-flex;align-items:center;min-height:28px;padding:0 12px;border-radius:999px;border:1px solid rgba(32,48,64,.12);background:rgba(255,255,255,.92)}}.pill{{color:#0f766e;border-color:rgba(15,118,110,.18)}}.metric{{margin-top:24px;display:grid;gap:12px}}.card{{border:1px solid rgba(32,48,64,.12);border-radius:24px;background:rgba(255,255,255,.82);overflow:hidden;max-width:100%}}.card-head{{display:flex;justify-content:space-between;gap:12px;flex-wrap:wrap;padding:16px 18px;border-bottom:1px solid rgba(32,48,64,.12)}}.card-head h2{{margin:2px 0 4px;font-size:24px}}.muted{{color:#667788;font-size:12px}}.toolbar{{display:flex;gap:10px;flex-wrap:wrap;padding:16px 18px 0}}.toolbar-field{{min-width:220px}}.toolbar-field label{{display:block;margin-bottom:6px;font-size:12px;color:#667788}}.toolbar-field select{{width:100%;padding:9px 12px;border-radius:12px;border:1px solid rgba(32,48,64,.12);background:#fff}}.panel-wrap{{display:grid;gap:16px;padding:16px 18px 18px}}.detail-card{{border:1px solid rgba(32,48,64,.12);border-radius:20px;background:rgba(255,255,255,.7);padding:14px;max-width:100%}}.detail-top{{margin-bottom:12px}}.detail-top h4{{margin:2px 0 0;font-size:18px;text-transform:none}}.detail-top p{{margin:6px 0 0;color:#667788;font-size:13px}}.chart-scroll{{overflow-x:auto;padding-bottom:6px;width:100%;max-width:100%}}.chart{{height:360px;min-width:1120px}}.empty,.page-error{{padding:14px;border-radius:14px;border:1px dashed rgba(32,48,64,.12);color:#667788;text-align:center}}.page-error{{border-style:solid;background:rgba(254,226,226,.86);color:#991b1b}}@media (max-width:1100px){{.chart{{height:320px}}}}
</style>
</head>
<body class="ab-isadx-latency">
<div class="page">
  <section class="hero">
    <div>
      <span class="pill">AB IsAdx Latency Dashboard</span>
      <h1>{title}</h1>
      <div class="hero-copy">{hero_lines_html}</div>
    </div>
    <div><span class="chip">双轴 + unit 下钻</span></div>
  </section>
  <div id="page-error" class="page-error" hidden></div>
  <div id="root"></div>
</div>
<script src="{ASSET_SCRIPT_PATH}"></script>
<script>
const DATA={json.dumps(script_payload, ensure_ascii=False)};
{build_isadx_latency_page_script()}
</script>
</body>
</html>"""

def main() -> None:
    parser = argparse.ArgumentParser(description="Generate dashboard HTML files with UTF-8 validation.")
    parser.add_argument(
        "--only",
        nargs="+",
        choices=[
            "entry",
            "request_structure",
            "request_structure_country",
            "request_structure_unit",
            "coverage_analysis",
            "null_bidding",
            "bidding_network_status",
            "winning_type_network_status",
            "success_mapping",
            "filled_duration",
        ],
        help="Generate only the selected dashboard pages. Use ASCII page keys only.",
    )
    args = parser.parse_args()
    selected_pages = set(args.only or []) or None
    outputs = write_dashboards(selected_pages)
    if selected_pages is None:
        from validate_bidding_network_status_consistency import build_conclusion, write_validation_report

        validation_rows = write_validation_report()
        print(build_conclusion(validation_rows))
    for key, path in outputs.items():
        if selected_pages is None or key in selected_pages:
            print(f"generated {key}: {path}")


if __name__ == "__main__":
    main()
