"""胜利渠道状态命中率页面构建。"""

from __future__ import annotations

from pathlib import Path
import sys

SCRIPT_DIR = Path(__file__).resolve().parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.append(str(SCRIPT_DIR))

import ab_dashboard_shared as shared
from build_ab_dashboard_common import inject_home_button


def build_page_payload() -> dict:
    return shared.build_winning_type_network_status_dashboard_payload()


def build_page_html(payload: dict) -> str:
    return inject_home_button(shared.build_winning_type_network_status_html(payload))


def write_page(write_html=shared.write_validated_html) -> Path:
    payload = build_page_payload()
    write_html(
        shared.WINNING_TYPE_NETWORK_STATUS_HTML,
        build_page_html(payload),
        required_strings=["胜利渠道", shared.WINNING_TYPE_NETWORK_STATUS_TEXT[0]],
    )
    return shared.WINNING_TYPE_NETWORK_STATUS_HTML
