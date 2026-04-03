"""
Looker Studio 报表截图工具 - 滚动拼接方式
使用方法：
1. 确保 Chrome 已用 --remote-debugging-port=9222 启动
2. 运行: python looker_screenshot.py
"""
import subprocess
import sys

try:
    from playwright.sync_api import sync_playwright
except ImportError:
    print("正在安装 playwright...")
    subprocess.check_call([sys.executable, "-m", "pip", "install", "playwright"])
    subprocess.check_call([sys.executable, "-m", "playwright", "install", "chromium"])
    from playwright.sync_api import sync_playwright

try:
    from PIL import Image
except ImportError:
    print("正在安装 Pillow...")
    subprocess.check_call([sys.executable, "-m", "pip", "install", "Pillow"])
    from PIL import Image

import os
import io

DESKTOP = os.path.join(os.environ["USERPROFILE"], "Desktop")

REPORTS = [
    {
        "name": "report1",
        "url": "https://lookerstudio.google.com/u/0/reporting/1d949eec-106c-4e46-b837-7ee6d7f356c4/page/p_d6ubr7390c",
    },
    {
        "name": "report2",
        "url": "https://lookerstudio.google.com/reporting/b86598a5-2fa5-400e-ab86-e87e5ba5bd9e/page/p_9muvjtonuc",
    },
]


def scroll_and_stitch(page, filepath):
    """滚动页面并拼接截图"""
    # 找到主滚动容器
    scroll_info = page.evaluate("""() => {
        // Looker Studio 的滚动容器
        const candidates = document.querySelectorAll('*');
        let scrollEl = null;
        let maxScroll = 0;
        for (const el of candidates) {
            if (el.scrollHeight > el.clientHeight + 10 && el.clientHeight > 200) {
                if (el.scrollHeight > maxScroll) {
                    maxScroll = el.scrollHeight;
                    scrollEl = el;
                }
            }
        }
        if (!scrollEl) {
            return { found: false, scrollHeight: document.body.scrollHeight, clientHeight: window.innerHeight };
        }
        // 给滚动容器加个临时 ID
        scrollEl.id = '__screenshot_scroll_target__';
        return {
            found: true,
            scrollHeight: scrollEl.scrollHeight,
            clientHeight: scrollEl.clientHeight,
            tagName: scrollEl.tagName,
            className: scrollEl.className.substring(0, 100),
        };
    }""")

    print(f"  滚动容器: found={scroll_info['found']}, scrollHeight={scroll_info['scrollHeight']}, clientHeight={scroll_info['clientHeight']}")

    scroll_height = scroll_info["scrollHeight"]
    view_height = scroll_info["clientHeight"]
    overlap = 50  # 重叠像素，避免拼接缝隙

    if not scroll_info["found"]:
        # 没找到滚动容器，直接截整个页面
        page.screenshot(path=filepath, full_page=True)
        print(f"  直接截图保存: {filepath}")
        return

    screenshots = []
    scroll_pos = 0
    step = view_height - overlap
    idx = 0

    while scroll_pos < scroll_height:
        # 滚动到指定位置
        page.evaluate(f"""() => {{
            const el = document.getElementById('__screenshot_scroll_target__');
            if (el) el.scrollTop = {scroll_pos};
            else window.scrollTo(0, {scroll_pos});
        }}""")
        page.wait_for_timeout(1000)  # 等待渲染

        # 截取当前视口
        screenshot_bytes = page.screenshot()
        img = Image.open(io.BytesIO(screenshot_bytes))
        screenshots.append(img)
        idx += 1
        print(f"  截取第 {idx} 段 (scrollTop={scroll_pos})...")

        scroll_pos += step
        if scroll_pos >= scroll_height - view_height and scroll_pos < scroll_height:
            # 最后一段，滚到底
            scroll_pos = scroll_height - view_height
            page.evaluate(f"""() => {{
                const el = document.getElementById('__screenshot_scroll_target__');
                if (el) el.scrollTop = {scroll_pos};
                else window.scrollTo(0, {scroll_pos});
            }}""")
            page.wait_for_timeout(1000)
            screenshot_bytes = page.screenshot()
            img = Image.open(io.BytesIO(screenshot_bytes))
            screenshots.append(img)
            idx += 1
            print(f"  截取第 {idx} 段 - 最后 (scrollTop={scroll_pos})...")
            break

    if len(screenshots) == 1:
        screenshots[0].save(filepath)
    else:
        # 拼接所有截图
        # 需要裁剪出滚动容器区域并拼接
        # 简单方案：直接纵向拼接所有截图（会有顶部/底部固定元素重复，但内容完整）
        total_width = screenshots[0].width
        total_height = sum(img.height for img in screenshots)
        stitched = Image.new("RGB", (total_width, total_height))
        y = 0
        for img in screenshots:
            stitched.paste(img, (0, y))
            y += img.height
        stitched.save(filepath, quality=95)

    print(f"  拼接完成，共 {len(screenshots)} 段，已保存: {filepath}")


def main():
    with sync_playwright() as p:
        try:
            browser = p.chromium.connect_over_cdp("http://127.0.0.1:9222")
            print("已连接到 Chrome")
        except Exception as e:
            print(f"无法连接到 Chrome: {e}")
            print()
            print("请先关闭所有 Chrome，然后用以下命令重新启动：")
            print('"C:\\Program Files\\Google\\Chrome\\Application\\chrome.exe" --remote-debugging-port=9222 --user-data-dir="%TEMP%\\chrome_debug"')
            return

        context = browser.contexts[0]
        page = context.new_page()
        page.set_viewport_size({"width": 1920, "height": 1080})

        for report in REPORTS:
            print(f"\n正在打开: {report['name']} ...")
            page.goto(report["url"], wait_until="load", timeout=3600000)
            print("等待页面渲染（30秒）...")
            page.wait_for_timeout(30000)

            filepath = os.path.join(DESKTOP, f"looker_{report['name']}_fullpage.png")
            scroll_and_stitch(page, filepath)

        page.close()
        print("\n全部完成!")


if __name__ == "__main__":
    main()
