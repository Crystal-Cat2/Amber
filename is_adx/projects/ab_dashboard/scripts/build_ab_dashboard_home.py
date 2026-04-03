"""AB dashboard 首页与 deploy 首页生成。"""

from __future__ import annotations

from pathlib import Path
import sys

SCRIPT_DIR = Path(__file__).resolve().parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.append(str(SCRIPT_DIR))

import ab_dashboard_shared as shared

ENTRY_DEPLOY_HTML = shared.OUTPUT_DIR / "ab_share_dashboard.deploy.html"

PAGE_ENTRIES = [
    {
        "key": "request_structure",
        "title": "请求结构分布",
        "description": "查看全量 request 的 total network_cnt、bidding/waterfall 组合和 status 下钻分布。",
        "local_href": "ab_request_structure_dashboard.html",
        "deploy_href": "http://10.0.0.252:11005/81efef46-4ed1-4647-ab3b-699ee7ab8cb0/",
    },
    {
        "key": "request_structure_country",
        "title": "请求结构分布（Country）",
        "description": "按 country 维度查看请求结构前 4 个指标，便于横向对比不同国家差异。",
        "local_href": "ab_request_structure_country_dashboard.html",
        "deploy_href": "http://10.0.0.252:11005/ba8feeab-69bf-470d-939a-2946b6e95a23/",
    },
    {
        "key": "request_structure_unit",
        "title": "请求结构分布（Unit）",
        "description": "查看当前 product + ad_format 下，各 ad unit 的 total network_cnt、placement 结构和 status 下钻分布。",
        "local_href": "ab_request_structure_unit_dashboard.html",
        "deploy_href": "http://10.0.0.252:11005/640dbb85-5790-446f-ada2-ed5e43fd9998/",
    },
    {
        "key": "coverage_analysis",
        "title": "覆盖率分析",
        "description": "查看 req_index × network_cnt 桶占比，以及 network_type / status 在桶内的请求覆盖率。",
        "local_href": "ab_coverage_analysis_dashboard.html",
        "deploy_href": "http://10.0.0.252:11005/c417080e-ae44-40d5-aaba-2e7e45f03988/",
    },
    {
        "key": "null_bidding",
        "title": "Null Bidding",
        "description": "查看各 unit 在 NULL / FAILED_TO_LOAD / AD_LOAD_NOT_ATTEMPTED 下的无效竞价结构分布。",
        "local_href": "ab_null_bidding_dashboard.html",
        "deploy_href": "ab_null_bidding_dashboard.html",
    },
    {
        "key": "bidding_network_status",
        "title": "Bidding Network Status",
        "description": "查看 ALL UNIT 与具体 unit 下，type + network 在四状态上的总占比与曲线分布。",
        "local_href": "ab_bidding_network_status_dashboard.html",
        "deploy_href": "http://10.0.0.252:11005/464eab99-8a22-4b9a-bd19-e19ebb0a062a/",
    },
    {
        "key": "winning_type_network_status",
        "title": "胜利渠道状态命中率",
        "description": "查看指定胜利 type + network 为唯一 AD_LOADED 时，其他渠道在这些 request 上的状态命中率。",
        "local_href": "ab_winning_type_network_status_dashboard.html",
        "deploy_href": "http://10.0.0.252:11005/7628554a-d219-4b4c-b896-8f87b7ea9f0a/",
    },
    {
        "key": "success_mapping",
        "title": "成功 network / placement 分布",
        "description": "先按全量 network_cnt 或 placement_cnt 分桶，再看当前桶里不同成功对象与 fail 的占比。",
        "local_href": "ab_success_mapping_dashboard.html",
        "deploy_href": "http://10.0.0.252:11005/ebc7f396-aaf8-45fd-a60e-6c90ebc01ee6/",
    },
    {
        "key": "success_request",
        "title": "成功 Request 分层分析",
        "description": "按 country、unit 与 cnt 桶查看成功 request 的 eCPM、成功渠道和 success rank 分布。",
        "local_href": "ab_success_request_dashboard.html",
        "deploy_href": "http://10.0.0.252:11005/70181d13-9978-4464-9c62-238b147c6c74/",
    },
    {
        "key": "filled_duration",
        "title": "adslog_filled 时长分布",
        "description": "按 product、ad_format 与 unit 查看 adslog_filled.duration 的默认分布，对比 A/B 组 pv、占比与 B-A GAP。",
        "local_href": "ab_filled_duration_dashboard.html",
        "deploy_href": "http://10.0.0.252:11005/c1d631cc-277d-4c16-b298-f1dfe79b5f97/",
    },
    {
        "key": "isadx_latency",
        "title": "IsAdx latency 分布差异",
        "description": "按 product、ad_format 与 unit 查看 isadx request latency 的分布差异；同页区分 success / fail，并对比 A/B 占比与 B-A GAP。",
        "local_href": "ab_isadx_latency_dashboard.html",
        "deploy_href": "ab_isadx_latency_dashboard.html",
    },
]


def _build_cards(deploy: bool = False) -> str:
    rows = []
    href_key = "deploy_href" if deploy else "local_href"
    for entry in PAGE_ENTRIES:
        href = entry[href_key]
        rows.append(
            f"""    <a class="entry-row" href="{href}">
      <div class="entry-copy">
        <h2>{entry["title"]}</h2>
        <p>{entry["description"]}</p>
      </div>
      <span class="entry-arrow">Open</span>
    </a>"""
        )
    return "\n".join(rows)


def build_entry_html(deploy: bool = False) -> str:
    cards_html = _build_cards(deploy=deploy)
    return f"""<!doctype html>
<html lang="zh-CN">
<head>
<meta charset="UTF-8" />
<meta name="viewport" content="width=device-width, initial-scale=1.0" />
<title>AB 请求结构看板入口</title>
<style>
body{{margin:0;font-family:"Microsoft YaHei",sans-serif;background:#f6f1e9;color:#203040}}
.page{{max-width:1040px;margin:0 auto;padding:40px 20px 60px;box-sizing:border-box}}
.hero{{margin-bottom:28px}}
.hero h1{{margin:0 0 10px;font-size:34px}}
.hero p{{margin:0;color:#667788;line-height:1.7}}
.entry-list{{display:grid;gap:14px;margin-top:28px}}
.entry-row{{display:flex;justify-content:space-between;align-items:center;gap:18px;padding:20px 22px;border-radius:24px;border:1px solid rgba(32,48,64,.12);background:#fff;color:inherit;text-decoration:none;box-sizing:border-box;max-width:100%}}
.entry-copy{{min-width:0}}
.entry-copy h2{{margin:0 0 8px;font-size:24px}}
.entry-copy p{{margin:0;color:#667788;line-height:1.7}}
.entry-arrow{{display:inline-flex;align-items:center;justify-content:center;min-width:78px;min-height:36px;padding:0 14px;border-radius:999px;background:rgba(15,118,110,.1);color:#0f766e;font-size:12px;font-weight:700}}
@media (max-width:720px){{.entry-row{{align-items:flex-start;flex-direction:column}}.entry-arrow{{min-width:0}}}}
</style>
</head>
<body>
<div class="page">
  <section class="hero">
    <h1>AB 请求结构看板</h1>
    <p>本入口页将请求结构、状态分析与成功映射拆成独立 HTML；主入口只负责导航，各业务页各自独立生成。</p>
  </section>
  <section class="entry-list">
{cards_html}
  </section>
</div>
</body>
</html>"""


def build_entry_deploy_html() -> str:
    return build_entry_html(deploy=True)


def write_home_pages(write_html=shared.write_validated_html) -> dict[str, Path]:
    shared.OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    write_html(
        shared.ENTRY_HTML,
        build_entry_html(),
        required_strings=["AB 请求结构看板入口", "AB 请求结构看板", "请求结构分布", "请求结构分布（Country）", "成功 Request 分层分析"],
    )
    write_html(
        ENTRY_DEPLOY_HTML,
        build_entry_deploy_html(),
        required_strings=["AB 请求结构看板入口", "AB 请求结构看板", "请求结构分布", "请求结构分布（Country）", "成功 Request 分层分析"],
    )
    return {"entry": shared.ENTRY_HTML, "entry_deploy": ENTRY_DEPLOY_HTML}
