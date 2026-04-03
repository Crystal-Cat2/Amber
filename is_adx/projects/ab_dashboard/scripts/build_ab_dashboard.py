"""AB dashboard 模块化主入口。"""

from __future__ import annotations

import argparse
from pathlib import Path
import sys

SCRIPT_DIR = Path(__file__).resolve().parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.append(str(SCRIPT_DIR))

import ab_dashboard_shared as shared
import build_ab_dashboard_bidding_network_status as bidding_status_page
import build_ab_dashboard_coverage_analysis as coverage_page
import build_ab_dashboard_filled_duration as filled_duration_page
import build_ab_dashboard_home as home_page
import build_ab_dashboard_isadx_latency as isadx_latency_page
import build_ab_dashboard_null_bidding as null_bidding_page
import build_ab_dashboard_request_structure as request_structure_page
import build_ab_dashboard_request_structure_country as request_structure_country_page
import build_ab_dashboard_request_structure_unit as request_structure_unit_page
import build_ab_dashboard_success_request as success_request_page
import build_ab_dashboard_success_mapping as success_mapping_page
import build_ab_dashboard_winning_type_network_status as winning_status_page

PAGE_WRITERS = {
    "request_structure": request_structure_page.write_page,
    "request_structure_country": request_structure_country_page.write_page,
    "request_structure_unit": request_structure_unit_page.write_page,
    "coverage_analysis": coverage_page.write_page,
    "null_bidding": null_bidding_page.write_page,
    "bidding_network_status": bidding_status_page.write_page,
    "winning_type_network_status": winning_status_page.write_page,
    "success_mapping": success_mapping_page.write_page,
    "success_request": success_request_page.write_page,
    "filled_duration": filled_duration_page.write_page,
    "isadx_latency": isadx_latency_page.write_page,
}


def write_dashboards(
    only_pages: set[str] | None = None,
    write_html=shared.write_validated_html,
) -> dict[str, Path]:
    outputs = home_page.write_home_pages(write_html=write_html) if only_pages is None or "entry" in only_pages else {
        "entry": shared.ENTRY_HTML,
        "entry_deploy": home_page.ENTRY_DEPLOY_HTML,
    }
    for page_key, writer in PAGE_WRITERS.items():
        outputs[page_key] = writer(write_html=write_html) if only_pages is None or page_key in only_pages else getattr(shared, f"{page_key.upper()}_HTML", None) or outputs.get(page_key)
    return outputs


def write_dashboard() -> Path:
    return write_dashboards()["entry"]


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate modular AB dashboard HTML files with UTF-8 validation.")
    parser.add_argument(
        "--only",
        nargs="+",
        choices=["entry", *PAGE_WRITERS.keys()],
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
