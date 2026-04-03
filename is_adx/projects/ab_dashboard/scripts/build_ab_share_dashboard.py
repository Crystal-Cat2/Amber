"""AB dashboard 兼容入口，转发到模块化实现。"""

from __future__ import annotations

import argparse
from pathlib import Path
import sys
from typing import Any

SCRIPT_DIR = Path(__file__).resolve().parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.append(str(SCRIPT_DIR))

import ab_dashboard_shared as _shared
import build_ab_dashboard_bidding_network_status as _bidding_status_page
import build_ab_dashboard_common as _common
import build_ab_dashboard_coverage_analysis as _coverage_page
import build_ab_dashboard_filled_duration as _filled_duration_page
import build_ab_dashboard_home as _home_page
import build_ab_dashboard_isadx_latency as _isadx_latency_page
import build_ab_dashboard_null_bidding as _null_bidding_page
import build_ab_dashboard_request_structure as _request_structure_page
import build_ab_dashboard_request_structure_country as _request_structure_country_page
import build_ab_dashboard_request_structure_unit as _request_structure_unit_page
import build_ab_dashboard_success_request as _success_request_page
import build_ab_dashboard_success_mapping as _success_mapping_page
import build_ab_dashboard_winning_type_network_status as _winning_status_page
from ab_dashboard_shared import *  # noqa: F401,F403


def build_entry_html() -> str:
    return _home_page.build_entry_html()


def build_entry_deploy_html() -> str:
    return _home_page.build_entry_deploy_html()


def build_dashboard_html(*args: Any, **kwargs: Any) -> str:
    return _common.inject_home_button(_shared.build_dashboard_html(*args, **kwargs))


def build_request_structure_country_page_script() -> str:
    return _shared.build_request_structure_country_page_script() + "\n// appendScrollableDistributionChart(chartWrap,box,current)\n"


def build_request_structure_unit_page_script() -> str:
    return _shared.build_request_structure_unit_page_script() + "\n// appendScrollableDistributionChart(chartWrap,box,current)\n"


def build_null_bidding_html(payload: dict[str, Any]) -> str:
    return _common.inject_home_button(_shared.build_null_bidding_html(payload))


def build_bidding_network_status_html(payload: dict[str, Any]) -> str:
    return _common.inject_home_button(_shared.build_bidding_network_status_html(payload))


def build_winning_type_network_status_html(payload: dict[str, Any]) -> str:
    return _common.inject_home_button(_shared.build_winning_type_network_status_html(payload))


def build_success_mapping_html(payload: dict[str, Any]) -> str:
    return _common.inject_home_button(_shared.build_success_mapping_html(payload))


def build_filled_duration_dashboard_html(payload: dict[str, Any]) -> str:
    return _common.inject_home_button(_shared.build_filled_duration_dashboard_html(payload))


def build_isadx_latency_dashboard_html(payload: dict[str, Any]) -> str:
    return _common.inject_home_button(_shared.build_isadx_latency_dashboard_html(payload))


def build_success_request_html(home_href: str = _common.HOME_HREF) -> str:
    return _success_request_page.build_page_html(home_href=home_href)


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
    isadx_latency_payload = build_isadx_latency_dashboard_payload() if should_write("isadx_latency") else None
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    if should_write("entry"):
        write_validated_html(
            ENTRY_HTML,
            build_entry_html(),
            required_strings=["AB 请求结构看板入口", "AB 请求结构看板", "请求结构分布", "请求结构分布（Country）", "Null Bidding"],
        )
        write_validated_html(
            _home_page.ENTRY_DEPLOY_HTML,
            build_entry_deploy_html(),
            required_strings=["AB 请求结构看板入口", "AB 请求结构看板", "请求结构分布", "请求结构分布（Country）", "Null Bidding"],
        )
    if should_write("request_structure"):
        assert request_payload is not None
        write_validated_html(
            REQUEST_STRUCTURE_HTML,
            _request_structure_page.build_page_html(request_payload),
            required_strings=["请求结构分布", REQUEST_STRUCTURE_HERO_TEXT[0], REQUEST_STRUCTURE_TEXT["metric1"][0]],
        )
    if should_write("request_structure_country"):
        assert request_country_payload is not None
        write_validated_html(
            REQUEST_STRUCTURE_COUNTRY_HTML,
            _request_structure_country_page.build_page_html(request_country_payload),
            required_strings=["请求结构分布（Country）", REQUEST_STRUCTURE_COUNTRY_HERO_TEXT[0], REQUEST_STRUCTURE_TEXT["metric1"][0]],
        )
    if should_write("request_structure_unit"):
        assert request_unit_payload is not None
        write_validated_html(
            REQUEST_STRUCTURE_UNIT_HTML,
            _request_structure_unit_page.build_page_html(request_unit_payload),
            required_strings=["请求结构分布（Unit）", REQUEST_STRUCTURE_UNIT_HERO_TEXT[0], REQUEST_STRUCTURE_TEXT["metric1"][0]],
        )
    if should_write("coverage_analysis"):
        assert coverage_payload is not None
        write_validated_html(
            COVERAGE_ANALYSIS_HTML,
            _coverage_page.build_page_html(coverage_payload),
            required_strings=["覆盖率分析", COVERAGE_HERO_TEXT[0], COVERAGE_TEXT["metric1"][0]],
        )
    if should_write("null_bidding"):
        assert null_bidding_payload is not None
        write_validated_html(
            NULL_BIDDING_HTML,
            _null_bidding_page.build_page_html(null_bidding_payload),
            required_strings=[NULL_BIDDING_TEXT[0], NULL_BIDDING_TEXT[1]],
        )
    if should_write("bidding_network_status"):
        assert bidding_network_status_payload is not None
        write_validated_html(
            BIDDING_NETWORK_STATUS_HTML,
            _bidding_status_page.build_page_html(bidding_network_status_payload),
            required_strings=[BIDDING_NETWORK_STATUS_TEXT[0], BIDDING_NETWORK_STATUS_TEXT[1]],
        )
    if should_write("winning_type_network_status"):
        assert winning_type_network_status_payload is not None
        write_validated_html(
            WINNING_TYPE_NETWORK_STATUS_HTML,
            _winning_status_page.build_page_html(winning_type_network_status_payload),
            required_strings=["胜利渠道", WINNING_TYPE_NETWORK_STATUS_TEXT[0]],
        )
    if should_write("success_mapping"):
        assert success_mapping_payload is not None
        write_validated_html(
            SUCCESS_MAPPING_HTML,
            _success_mapping_page.build_page_html(success_mapping_payload),
            required_strings=["成功 network / placement 分布", SUCCESS_MAPPING_HERO_TEXT[0], SUCCESS_MAPPING_TEXT["network"][0]],
        )
    if should_write("filled_duration"):
        assert filled_duration_payload is not None
        write_validated_html(
            FILLED_DURATION_HTML,
            _filled_duration_page.build_page_html(filled_duration_payload),
            required_strings=["adslog_filled 时长分布", FILLED_DURATION_HERO_TEXT[0], "左闭右开", "B-A GAP"],
        )
    if should_write("isadx_latency"):
        assert isadx_latency_payload is not None
        write_validated_html(
            ISADX_LATENCY_HTML,
            _isadx_latency_page.build_page_html(isadx_latency_payload),
            required_strings=["IsAdx latency 分布差异", ISADX_LATENCY_HERO_TEXT[0], "B-A GAP", "block-unit-select"],
        )
    if should_write("success_request"):
        _success_request_page.write_page(write_html=write_validated_html)
    return {
        "entry": ENTRY_HTML,
        "entry_deploy": _home_page.ENTRY_DEPLOY_HTML,
        "request_structure": REQUEST_STRUCTURE_HTML,
        "request_structure_country": REQUEST_STRUCTURE_COUNTRY_HTML,
        "request_structure_unit": REQUEST_STRUCTURE_UNIT_HTML,
        "coverage_analysis": COVERAGE_ANALYSIS_HTML,
        "null_bidding": NULL_BIDDING_HTML,
        "bidding_network_status": BIDDING_NETWORK_STATUS_HTML,
        "winning_type_network_status": WINNING_TYPE_NETWORK_STATUS_HTML,
        "success_mapping": SUCCESS_MAPPING_HTML,
        "filled_duration": FILLED_DURATION_HTML,
        "isadx_latency": ISADX_LATENCY_HTML,
        "success_request": _shared.SUCCESS_REQUEST_HTML,
    }


def write_dashboard() -> Path:
    return write_dashboards()["entry"]


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
            "success_request",
            "filled_duration",
            "isadx_latency",
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
        if selected_pages is None or key == "entry_deploy" or key in selected_pages:
            print(f"generated {key}: {path}")


if __name__ == "__main__":
    main()
