"""PreToolUse hook: 拦截未经用户确认的 bq_runtime_cli.py run 命令。"""
import hashlib, json, re, sys
from pathlib import Path

CONFIRM_DIR = Path(__file__).resolve().parents[1] / "sql_confirmed"


def main():
    raw = sys.stdin.read()
    try:
        payload = json.loads(raw)
    except Exception:
        return  # 非 JSON 输入，放行

    tool = payload.get("tool_name", "")
    if tool != "Bash":
        return

    cmd = payload.get("tool_input", {}).get("command", "")
    if "bq_runtime_cli" not in cmd or " run " not in cmd:
        return  # 不是 bq run 命令，放行

    # 提取 --sql-file 参数
    m = (
        re.search(r'--sql-file\s+"([^"]+)"', cmd)
        or re.search(r"--sql-file\s+'([^']+)'", cmd)
        or re.search(r"--sql-file\s+(\S+)", cmd)
    )
    if not m:
        print(json.dumps({
            "hookSpecificOutput": {"permissionDecision": "deny"},
            "systemMessage": "⛔ bq run 被拦截：未找到 --sql-file 参数。"
        }))
        sys.exit(2)

    sql_path = Path(m.group(1))
    if not sql_path.exists():
        print(json.dumps({
            "hookSpecificOutput": {"permissionDecision": "deny"},
            "systemMessage": f"⛔ SQL 文件不存在: {sql_path}"
        }))
        sys.exit(2)

    # 计算 SQL 文件 digest
    content = sql_path.read_text(encoding="utf-8").replace("\r\n", "\n").strip()
    digest = hashlib.sha256(content.encode("utf-8")).hexdigest()
    marker = CONFIRM_DIR / f"{digest}.confirmed"

    if marker.exists():
        # 已确认，放行并清理标记
        marker.unlink(missing_ok=True)
        return  # exit 0 = allow
    else:
        print(json.dumps({
            "hookSpecificOutput": {"permissionDecision": "deny"},
            "systemMessage": (
                f"⛔ SQL 执行被拦截：用户尚未确认此 SQL。\n\n"
                f"SQL 文件: {sql_path}\n"
                f"SQL digest: sha256:{digest}\n\n"
                f"请先让用户查看 SQL 文件内容/diff 并获得明确确认，"
                f"然后创建确认标记文件 .claude/sql_confirmed/{digest}.confirmed 后重试。"
            )
        }))
        sys.exit(2)


if __name__ == "__main__":
    main()
