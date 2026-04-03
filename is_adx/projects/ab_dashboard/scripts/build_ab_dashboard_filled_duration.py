"""adslog_filled 时长分布页面构建。"""

from __future__ import annotations

from pathlib import Path
import sys

SCRIPT_DIR = Path(__file__).resolve().parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.append(str(SCRIPT_DIR))

import ab_dashboard_shared as shared
from build_ab_dashboard_common import inject_home_button


def build_page_payload() -> dict:
    return shared.build_filled_duration_dashboard_payload()


def build_page_html(payload: dict) -> str:
    return inject_home_button(shared.build_filled_duration_dashboard_html(payload))


def write_page(write_html=shared.write_validated_html) -> Path:
    payload = build_page_payload()
    write_html(
        shared.FILLED_DURATION_HTML,
        build_page_html(payload),
        required_strings=["adslog_filled 时长分布", shared.FILLED_DURATION_HERO_TEXT[0], "左闭右开", "B-A GAP"],
    )
    return shared.FILLED_DURATION_HTML
