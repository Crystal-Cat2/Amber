"""PreToolUse hook: 拦截未经用户确认的 BigQuery 执行命令。

拦截范围：
  1. bq_runtime_cli.py run --sql-file ... （CLI 工具）
  2. bigquery.Client / client.query / mcp__Bigquery 等直接调用 BigQuery 的命令
  3. python script.py 间接调用 BigQuery（读取脚本内容检测 BQ 模式）
"""
import hashlib, json, re, sys
from pathlib import Path

CONFIRM_DIR = Path(__file__).resolve().parents[1] / "sql_confirmed"

# 匹配直接调用 BigQuery 的关键词（不区分大小写）
BQ_DIRECT_PATTERNS = [
    r"bigquery\.Client",
    r"client\.query\s*\(",
    r"\.query\(.*\.result\(\)",
    r"from\s+google\.cloud\s+import\s+bigquery",
]


def deny(msg):
    print(json.dumps({
        "hookSpecificOutput": {"permissionDecision": "deny"},
        "systemMessage": msg,
    }))
    sys.exit(2)


def check_bq_cli(cmd):
    """检查 bq_runtime_cli run 命令，需要 --sql-file 对应的确认标记。"""
    m = (
        re.search(r'--sql-file\s+"([^"]+)"', cmd)
        or re.search(r"--sql-file\s+'([^']+)'", cmd)
        or re.search(r"--sql-file\s+(\S+)", cmd)
    )
    if not m:
        deny("⛔ bq run 被拦截：未找到 --sql-file 参数。")

    sql_path = Path(m.group(1))
    if not sql_path.exists():
        deny(f"⛔ SQL 文件不存在: {sql_path}")

    check_confirmed(sql_path)


def check_bq_direct(cmd):
    """检查内联 BigQuery 调用，提取 open(...).read() 引用的 SQL 文件或拦截内联 SQL。"""
    # 尝试提取 open('xxx.sql') 引用
    sql_files = re.findall(r"open\(['\"]([^'\"]+\.sql)['\"]", cmd)
    if sql_files:
        for sf in sql_files:
            p = Path(sf)
            if not p.exists():
                deny(f"⛔ SQL 文件不存在: {p}")
            check_confirmed(p)
        return

    # 没有引用 SQL 文件 → 内联 SQL，一律拦截
    deny(
        "⛔ BigQuery 执行被拦截：检测到内联 SQL 调用。\n\n"
        "请将 SQL 写入 .sql 文件，让用户确认后再执行。\n"
        "不允许在 python -c 或脚本中直接嵌入 SQL 字符串执行。"
    )


def check_bq_script(script_path, content):
    """检查 python 脚本文件中的 BigQuery 调用，提取 SQL 文件引用或拦截内联 SQL。"""
    sql_files = re.findall(r"open\(['\"]([^'\"]+\.sql)['\"]", content)
    if sql_files:
        for sf in sql_files:
            p = Path(sf)
            if not p.exists():
                deny(f"⛔ SQL 文件不存在: {p}（来自脚本 {script_path}）")
            check_confirmed(p)
        return

    deny(
        f"⛔ BigQuery 执行被拦截：脚本 {script_path} 包含 BQ 调用但未引用 .sql 文件。\n\n"
        "请将 SQL 写入 .sql 文件，让用户确认后再执行。\n"
        "不允许在脚本中直接嵌入 SQL 字符串执行。"
    )


def check_confirmed(sql_path):
    """检查 SQL 文件是否已有用户确认标记。"""
    content = sql_path.read_text(encoding="utf-8").replace("\r\n", "\n").strip()
    digest = hashlib.sha256(content.encode("utf-8")).hexdigest()
    marker = CONFIRM_DIR / f"{digest}.confirmed"

    if marker.exists():
        marker.unlink(missing_ok=True)
        return  # 已确认，放行
    else:
        deny(
            f"⛔ SQL 执行被拦截：用户尚未确认此 SQL。\n\n"
            f"SQL 文件: {sql_path}\n"
            f"SQL digest: sha256:{digest}\n\n"
            f"请先让用户查看 SQL 文件内容/diff 并获得明确确认，"
            f"然后创建确认标记文件 .claude/sql_confirmed/{digest}.confirmed 后重试。"
        )


def main():
    raw = sys.stdin.read()
    try:
        payload = json.loads(raw)
    except Exception:
        return

    tool = payload.get("tool_name", "")

    # MCP BigQuery 工具调用 → 一律拦截，要求走确认流程
    if tool.startswith("mcp__Bigquery"):
        sql_text = payload.get("tool_input", {}).get("sql", "")
        deny(
            "⛔ MCP BigQuery 执行被拦截：需要用户确认。\n\n"
            f"SQL 预览: {sql_text[:200]}...\n\n"
            "请将 SQL 写入 .sql 文件，让用户确认后通过 Bash + bigquery.Client 执行。"
        )

    if tool != "Bash":
        return

    cmd = payload.get("tool_input", {}).get("command", "")

    # 路径 1: bq_runtime_cli run
    if "bq_runtime_cli" in cmd and " run " in cmd:
        check_bq_cli(cmd)
        return

    # 路径 2: 直接调用 BigQuery（python -c / 脚本内）
    for pat in BQ_DIRECT_PATTERNS:
        if re.search(pat, cmd):
            check_bq_direct(cmd)
            return

    # 路径 3: python 执行脚本文件（间接 BQ 调用）
    py_match = re.search(r'python[3]?\s+["\']?([^"\'\s]+\.py)["\']?', cmd)
    if py_match:
        script_path = Path(py_match.group(1))
        if script_path.exists():
            script_content = script_path.read_text(encoding="utf-8")
            for pat in BQ_DIRECT_PATTERNS:
                if re.search(pat, script_content):
                    check_bq_script(script_path, script_content)
                    return

    # 其他命令放行


if __name__ == "__main__":
    main()
