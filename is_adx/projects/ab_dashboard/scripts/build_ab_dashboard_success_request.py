"""成功 request 分层分析页面桥接构建。"""

from __future__ import annotations

from pathlib import Path
import sys

SCRIPT_DIR = Path(__file__).resolve().parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.append(str(SCRIPT_DIR))

SUCCESS_SCRIPT_DIR = SCRIPT_DIR.parents[1] / "success_request_dashboard" / "scripts"
if str(SUCCESS_SCRIPT_DIR) not in sys.path:
    sys.path.append(str(SUCCESS_SCRIPT_DIR))

import ab_dashboard_shared as shared
from build_ab_dashboard_common import inject_home_button
import build_success_request_dashboard as success_dashboard

DEPLOY_HTML = shared.OUTPUT_DIR / "ab_success_request_dashboard.deploy.html"


def load_source_html() -> str:
    if success_dashboard.DASHBOARD_HTML.exists():
        return success_dashboard.DASHBOARD_HTML.read_text(encoding="utf-8")
    payload = success_dashboard.build_payload()
    return success_dashboard.render_html(payload)


def build_page_html(home_href: str = "ab_share_dashboard.html") -> str:
    return inject_home_button(load_source_html(), href=home_href)


def write_page(
    write_html=shared.write_validated_html,
    *,
    local_home_href: str = "ab_share_dashboard.html",
    deploy_home_href: str = "ab_share_dashboard.html",
) -> Path:
    write_html(
        shared.SUCCESS_REQUEST_HTML,
        build_page_html(home_href=local_home_href),
        required_strings=["成功 Request 分层分析", "当前 Unit 成功 Request", "Network CNT 分布"],
    )
    write_html(
        DEPLOY_HTML,
        build_page_html(home_href=deploy_home_href),
        required_strings=["成功 Request 分层分析", "当前 Unit 成功 Request", "Network CNT 分布"],
    )
    return shared.SUCCESS_REQUEST_HTML
