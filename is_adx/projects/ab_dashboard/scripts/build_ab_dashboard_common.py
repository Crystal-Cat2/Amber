"""AB dashboard 模块化构建的公共辅助函数。"""

from __future__ import annotations

HOME_HREF = "ab_share_dashboard.html"
HOME_LABEL = "返回首页"
HOME_NAV_CSS = """
.hero-actions{display:flex;justify-content:flex-end;align-items:center;flex:1 1 100%;margin-top:12px}
.home-link{display:inline-flex;align-items:center;justify-content:center;min-height:36px;padding:0 16px;border-radius:999px;border:1px solid rgba(32,48,64,.14);background:rgba(255,255,255,.94);color:#1f3140;text-decoration:none;font-size:13px;font-weight:700;box-sizing:border-box}
.home-link:hover{background:#fff}
"""


def inject_home_button(html: str, href: str = HOME_HREF, label: str = HOME_LABEL) -> str:
    """为业务页注入固定返回首页按钮。"""
    button_html = f'<div class="hero-actions"><a class="home-link" href="{href}">{label}</a></div>'
    updated = html
    if ".home-link" not in updated and "</style>" in updated:
        updated = updated.replace("</style>", HOME_NAV_CSS + "\n</style>", 1)
    if button_html not in updated and "</section>" in updated:
        updated = updated.replace("</section>", button_html + "</section>", 1)
    return updated
