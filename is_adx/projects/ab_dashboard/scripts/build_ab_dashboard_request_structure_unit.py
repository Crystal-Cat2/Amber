"""请求结构 Unit 页面构建。"""

from __future__ import annotations

from pathlib import Path
import sys

SCRIPT_DIR = Path(__file__).resolve().parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.append(str(SCRIPT_DIR))

import ab_dashboard_shared as shared
from build_ab_dashboard_common import inject_home_button


def build_page_payload() -> dict:
    return shared.build_request_structure_unit_payload()


def build_page_html(payload: dict) -> str:
    return inject_home_button(
        shared.build_dashboard_html(
            metrics=payload["metrics"],
            products=payload["products"],
            ad_formats=payload["ad_formats"],
            units=payload["units"],
            unit_options_by_combo=payload["unit_options_by_combo"],
            page_key="request_structure_unit",
        )
    )


def write_page(write_html=shared.write_validated_html) -> Path:
    payload = build_page_payload()
    write_html(
        shared.REQUEST_STRUCTURE_UNIT_HTML,
        build_page_html(payload),
        required_strings=["请求结构分布（Unit）", shared.REQUEST_STRUCTURE_UNIT_HERO_TEXT[0], shared.REQUEST_STRUCTURE_TEXT["metric1"][0]],
    )
    return shared.REQUEST_STRUCTURE_UNIT_HTML
