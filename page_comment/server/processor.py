"""评论处理: 分类 → Claude CLI (--resume) 直接改文件 → 重生成"""
import asyncio
import csv
import hashlib
import json
import logging
import os
import re
import shutil
import subprocess

from config import AMBER_ROOT, CLAUDE_CLI, CLI_TIMEOUT, CLAUDE_CODE_GIT_BASH, CLAUDE_MODEL
from source_registry import resolve_source
import store

log = logging.getLogger("processor")

PAGEDOC_HOST = "10.0.0.54:12005"


def _read_csv_preview(csv_path: str, max_rows: int = 5) -> str:
    """读取 CSV 前几行，返回可读格式"""
    try:
        with open(csv_path, encoding="utf-8-sig") as f:
            reader = csv.reader(f)
            rows = []
            for i, row in enumerate(reader):
                if i >= max_rows + 1:
                    break
                rows.append(row)
        if not rows:
            return "(空文件)"
        header = rows[0]
        data = rows[1:]
        lines = [", ".join(header)]
        for r in data:
            lines.append(", ".join(r))
        return "\n".join(lines)
    except Exception as e:
        return f"(读取失败: {e})"


def _read_script_source(script_path: str) -> str:
    """读取脚本源码"""
    try:
        with open(script_path, encoding="utf-8") as f:
            return f.read()
    except Exception as e:
        return f"(读取失败: {e})"


def _classify_page(page_url: str, source_info: dict | None) -> str:
    """分类页面类型"""
    if source_info and source_info.get("script_path"):
        return "amber_dashboard"
    if PAGEDOC_HOST in page_url:
        return "pagedoc"
    return "generic"


def _extract_csv_from_comment(comment: str, data_dir: str | None) -> list[str]:
    """从评论文本中提取提到的 CSV 文件路径"""
    extra = []
    if not data_dir:
        return extra
    for m in re.finditer(r'[A-Za-z]:[/\\][^\s"\'<>|]+\.csv', comment, re.IGNORECASE):
        p = os.path.normpath(m.group())
        if os.path.exists(p) and p not in extra:
            extra.append(p)
    for m in re.finditer(r'[\w\-]+\.csv', comment, re.IGNORECASE):
        name = m.group()
        p = os.path.join(data_dir, name)
        if os.path.exists(p) and p not in extra:
            extra.append(p)
    return extra


def _list_available_csvs(data_dir: str | None) -> list[str]:
    """列出 data 目录下所有 CSV 文件名"""
    if not data_dir or not os.path.isdir(data_dir):
        return []
    return sorted(f for f in os.listdir(data_dir) if f.lower().endswith(".csv"))


def _collect_context_csvs(source_info: dict, selected: str, comment: str) -> list[str]:
    data_dir = source_info.get("data_dir")
    csv_paths = list(source_info.get("csv_files", []))
    for text in (comment, selected):
        for p in _extract_csv_from_comment(text, data_dir):
            if p not in csv_paths:
                csv_paths.append(p)
    return csv_paths


def _get_modifiable_files(source_info: dict) -> list[str]:
    """收集 CLI 允许修改的文件：脚本 + data 目录下全部 CSV。"""
    files = []
    script_path = source_info.get("script_path")
    if script_path:
        files.append(script_path)

    for csv_path in source_info.get("csv_files", []) or []:
        if csv_path and csv_path not in files and os.path.exists(csv_path):
            files.append(csv_path)

    data_dir = source_info.get("data_dir")
    if data_dir and os.path.isdir(data_dir):
        for name in sorted(os.listdir(data_dir)):
            if not name.lower().endswith(".csv"):
                continue
            csv_path = os.path.join(data_dir, name)
            if csv_path not in files:
                files.append(csv_path)

    return files


def _build_chart_section(chart_info: dict | None) -> str:
    if not chart_info:
        return ""
    return f"""
## 用户点击的图表
图表库: {chart_info.get('library', '未知')}
图表 canvas ID: {chart_info.get('canvas_id', '未知')}
图表元素 ID: {chart_info.get('element_id', '未知')}
图表标题: {chart_info.get('title', '未知')}
图表类型: {chart_info.get('chart_type', '未知')}
图表配置摘要: {chart_info.get('config_summary', '无')}
图表序列摘要: {chart_info.get('series_summary', '无')}
"""


def _build_amber_prompt_cli(data: dict, source_info: dict, is_followup: bool = False) -> str:
    """构建 Amber dashboard 的 Claude CLI prompt.
    首次: 发路径和规则, AI 自己 Read 文件.
    后续: 只发用户指令, AI 已有上下文."""
    selected = data.get("selected_text", "")
    comment = data.get("comment", "")
    chart_info = data.get("chart_info")
    script_path = source_info["script_path"]

    if is_followup:
        parts = []
        if selected:
            if chart_info:
                parts.append(f"用户点击了一个图表:\n{selected}")
                parts.append(_build_chart_section(chart_info))
            else:
                parts.append(f"用户选中了以下文字:\n{selected}")
        parts.append(f"用户评论: {comment}")
        return "\n\n".join(parts)

    # 首次消息：发路径和规则，不发文件内容
    modifiable_files = _get_modifiable_files(source_info)
    modifiable_block = "\n".join(f"- {path}" for path in modifiable_files) if modifiable_files else "- (无)"
    available_csv_list = ", ".join(_list_available_csvs(source_info.get("data_dir"))) or "(无)"

    return f"""你在一个 Amber 本地 dashboard 项目中工作。用户在最终 HTML 页面上提出了修改需求，你需要直接修改工作区中的源文件来完成它。

## 当前选中上下文
{"用户点击了一个图表。" if chart_info else "用户选中了以下文字。"}
{selected}
{_build_chart_section(chart_info)}
## 用户评论
{comment}

## 源生成脚本路径
{script_path}

## data 目录下所有可用 CSV 文件
{available_csv_list}

## 允许直接修改的文件
{modifiable_block}

## 执行要求
1. 先用 Read 工具读取源脚本和相关文件，理解当前实现。
2. 直接修改工作区文件来满足用户需求，不要输出 JSON edits。
3. 优先修改源生成脚本和相关 CSV；不要直接编辑生成后的 HTML。
4. 只能修改"允许直接修改的文件"列表中的文件。
5. 若用户只是提问或无需改动，请不要修改文件，直接回答。
6. 若拿到的是图表摘要而非完整配置，优先根据标题、系列、上下文定位对应的 Chart.js 初始化片段。
7. 完成后请只用中文简要说明：你改了哪些文件、做了什么修改、如果没改文件就说明原因。"""


def _build_generic_prompt_cli(data: dict) -> str:
    return f"""用户在网页上选中了以下文字并发表了评论。

## 选中的文字
{data.get("selected_text", "")}

## 用户评论
{data.get("comment", "")}

## 页面标题
{data.get("page_title", "")}

请直接用中文回答用户。不要输出 JSON，不要修改任何文件。"""


async def _call_claude_cli(prompt: str, cli_session_id: str = None, cwd: str | None = None, status_callback=None) -> tuple[str, str | None]:
    """调用 Claude CLI（流式），支持 --resume 继续会话。
    返回 (result_text, session_id)。
    通过 stream-json 格式逐行读取事件，实时推送 AI 处理进度。
    """
    base_cmd = CLAUDE_CLI if isinstance(CLAUDE_CLI, list) else [CLAUDE_CLI]
    cmd = base_cmd + [
        "-p", "-",
        "--model", CLAUDE_MODEL,
        "--output-format", "stream-json",
        "--verbose",
        "--allowedTools", "Edit,Write,Read",
    ]
    if cli_session_id:
        cmd.extend(["--resume", cli_session_id])

    env = os.environ.copy()
    env["CLAUDE_CODE_GIT_BASH_PATH"] = CLAUDE_CODE_GIT_BASH

    log.info("CLI 调用: %s (session=%s, cwd=%s, prompt=%d chars)", CLAUDE_CLI, cli_session_id or "new", cwd or os.getcwd(), len(prompt))

    creationflags = getattr(subprocess, "CREATE_NO_WINDOW", 0)
    try:
        proc = await asyncio.create_subprocess_exec(
            *cmd,
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            env=env,
            cwd=cwd,
            creationflags=creationflags,
        )
    except FileNotFoundError as e:
        raise RuntimeError(f"Claude CLI 启动失败: {e}")

    proc.stdin.write(prompt.encode("utf-8"))
    await proc.stdin.drain()
    proc.stdin.close()

    result_text = ""
    result_is_error = False
    new_session_id = cli_session_id
    last_text_status_time = 0.0

    async def _send_status(msg: str):
        if status_callback:
            await status_callback("editing", msg)

    async def _read_stream():
        nonlocal result_text, result_is_error, new_session_id, last_text_status_time
        while True:
            line = await proc.stdout.readline()
            if not line:
                break
            line_str = line.decode("utf-8", errors="replace").strip()
            if not line_str:
                continue
            try:
                event = json.loads(line_str)
            except json.JSONDecodeError:
                continue

            etype = event.get("type")

            if etype == "system" and event.get("subtype") == "init":
                sid = event.get("session_id")
                if sid:
                    new_session_id = sid

            elif etype == "assistant":
                blocks = event.get("message", {}).get("content", [])
                for block in blocks:
                    btype = block.get("type")
                    if btype == "thinking":
                        await _send_status("AI 正在思考...")
                    elif btype == "tool_use":
                        tool_name = block.get("name", "")
                        file_path = block.get("input", {}).get("file_path", "")
                        filename = os.path.basename(file_path) if file_path else ""
                        label_map = {"Read": "正在读取", "Edit": "正在编辑", "Write": "正在写入"}
                        label = label_map.get(tool_name)
                        if label and filename:
                            await _send_status(f"{label}: {filename}")
                        elif tool_name:
                            await _send_status(f"正在使用工具: {tool_name}")
                    elif btype == "text":
                        now = asyncio.get_event_loop().time()
                        if now - last_text_status_time > 3:
                            last_text_status_time = now
                            preview = (block.get("text") or "")[:40].replace("\n", " ")
                            if preview:
                                await _send_status(f"AI: {preview}...")

            elif etype == "result":
                result_text = event.get("result", "")
                result_is_error = event.get("is_error", False)
                new_session_id = event.get("session_id", new_session_id)
                duration = event.get("duration_ms", 0)
                log.info("CLI 完成: %dms, session=%s, is_error=%s", duration, new_session_id, result_is_error)

    try:
        await asyncio.wait_for(_read_stream(), timeout=CLI_TIMEOUT)
    except asyncio.TimeoutError:
        proc.kill()
        raise RuntimeError(f"Claude CLI 超时 ({CLI_TIMEOUT}s)")

    await proc.wait()
    stderr_bytes = await proc.stderr.read()
    stderr = stderr_bytes.decode("utf-8", errors="replace").strip()

    if result_is_error:
        error_detail = result_text or stderr or "(无详情)"
        raise RuntimeError(f"Claude CLI 返回错误: {error_detail[-500:]}")
    if proc.returncode != 0 and not result_text:
        raise RuntimeError(f"Claude CLI 失败 (code {proc.returncode}): {stderr[-500:]}")
    if proc.returncode != 0:
        log.warning("CLI 返回非零退出码 %d，但已获取到结果，继续处理 (stderr: %s)", proc.returncode, stderr[-200:])

    return (result_text or "").strip(), new_session_id


def _build_page_key(source_info: dict) -> str:
    """从 source_info 构建 page_key: 脚本相对路径::HTML文件名"""
    script_path = source_info.get("script_path", "")
    html_path = source_info.get("output_html", "")
    try:
        script_rel = os.path.relpath(script_path, AMBER_ROOT).replace("\\", "/")
    except ValueError:
        script_rel = script_path
    html_name = os.path.basename(html_path) if html_path else ""
    return f"{script_rel}::{html_name}"


def _file_digest(file_path: str) -> str | None:
    if not os.path.exists(file_path):
        return None
    digest = hashlib.md5()
    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _snapshot_files(paths: list[str]) -> dict[str, str | None]:
    return {path: _file_digest(path) for path in paths}


def _detect_changes(before: dict[str, str | None], after: dict[str, str | None]) -> list[str]:
    changed = []
    for path in before:
        if before.get(path) != after.get(path):
            changed.append(path)
    return changed


def _backup_files(paths: list[str]) -> dict[str, str]:
    backups = {}
    for file_path in paths:
        if not os.path.exists(file_path):
            continue
        backup_path = file_path + ".pc_backup"
        shutil.copy2(file_path, backup_path)
        backups[file_path] = backup_path
    return backups


def _restore_backups(backups: dict[str, str]) -> None:
    for file_path, backup_path in backups.items():
        if os.path.exists(backup_path):
            shutil.copy2(backup_path, file_path)


def _cleanup_backups(backups: dict[str, str]) -> None:
    for backup_path in backups.values():
        if os.path.exists(backup_path):
            try:
                os.remove(backup_path)
            except OSError:
                pass


def _inject_source_meta(html_path: str, source_info: dict) -> None:
    """在生成的 HTML 中注入 amber:source-script/source-data meta 标签，
    使前端能正确构建 page_key 匹配历史记录。已有标签则跳过。"""
    if not html_path or not os.path.exists(html_path):
        return
    try:
        with open(html_path, encoding="utf-8") as f:
            content = f.read()
    except Exception:
        return

    if 'amber:source-script' in content:
        return

    script_path = source_info.get("script_path", "")
    try:
        script_rel = os.path.relpath(script_path, AMBER_ROOT).replace("\\", "/")
    except ValueError:
        return

    meta_tags = f'  <meta name="amber:source-script" content="{script_rel}">'
    data_dir = source_info.get("data_dir")
    if data_dir:
        try:
            data_rel = os.path.relpath(data_dir, AMBER_ROOT).replace("\\", "/")
            meta_tags += f'\n  <meta name="amber:source-data" content="{data_rel}">'
        except ValueError:
            pass

    # 插入到 <head> 后面或 <html> 后面
    for anchor in ("<head>", "<HEAD>", "<html>", "<HTML>"):
        pos = content.find(anchor)
        if pos >= 0:
            insert_at = pos + len(anchor)
            content = content[:insert_at] + "\n" + meta_tags + "\n" + content[insert_at:]
            break
    else:
        content = meta_tags + "\n" + content

    try:
        with open(html_path, "w", encoding="utf-8") as f:
            f.write(content)
        log.info("已注入 source meta: %s", script_rel)
    except Exception as e:
        log.warning("注入 meta 失败: %s", e)


async def _regenerate(source_info: dict, status_callback) -> bool:
    """运行生成脚本重新生成 HTML"""
    cmd = source_info.get("run_command", [])
    if not cmd:
        return False

    await status_callback("regenerating", f"正在运行 {os.path.basename(cmd[1])}...")
    log.info("运行: %s", " ".join(cmd))

    try:
        creationflags = getattr(subprocess, "CREATE_NO_WINDOW", 0)
        result = await asyncio.to_thread(
            subprocess.run,
            cmd,
            capture_output=True,
            text=True,
            timeout=120,
            cwd=os.path.dirname(cmd[1]),
            creationflags=creationflags,
        )
        if result.returncode != 0:
            log.error("脚本执行失败:\nstdout: %s\nstderr: %s", result.stdout[-500:], result.stderr[-500:])
            return False
        log.info("脚本执行成功")
        _inject_source_meta(source_info.get("output_html"), source_info)
        return True
    except subprocess.TimeoutExpired:
        log.error("脚本执行超时 (120s)")
        return False
    except Exception as e:
        log.error("脚本执行异常: %s", e)
        return False


def _resolve_session(page_key: str, page_url: str, requested_session_id: str | None) -> dict:
    session = None
    if requested_session_id:
        candidate = store.get_session(requested_session_id)
        if candidate and candidate.get("page_key") == page_key:
            session = candidate
    if not session:
        session = store.get_active_session(page_key)
    if not session:
        session = store.create_session(page_key, page_url)
    return session


async def _handle_non_amber(data: dict, status_callback) -> dict:
    await status_callback("processing", "正在回答问题...")
    response_text, _ = await _call_claude_cli(_build_generic_prompt_cli(data), cwd=AMBER_ROOT, status_callback=status_callback)
    return {
        "response": response_text,
        "action": "none",
    }


async def process_comment(data: dict, status_callback) -> dict:
    """
    处理一条评论。

    data: {selected_text, comment, page_url, page_title, page_meta, session_id?, parent_id?}
    status_callback: async (status, message) -> None
    返回: {response: str, action: "reload" | "none", session_id?, message_id?, page_key?}
    """
    page_url = data.get("page_url", "")
    page_meta = data.get("page_meta", {})
    parent_id = data.get("parent_id")
    requested_session_id = data.get("session_id")

    source_info = resolve_source(page_url, page_meta)
    page_type = _classify_page(page_url, source_info)
    log.info("页面类型: %s, URL: %s", page_type, page_url[:80])

    if page_type != "amber_dashboard":
        return await _handle_non_amber(data, status_callback)

    page_key = _build_page_key(source_info)
    project_root = source_info.get("project_root") or os.path.dirname(source_info["script_path"])
    await status_callback("processing", f"已追溯到脚本: {os.path.basename(source_info['script_path'])}")

    session = _resolve_session(page_key, page_url, requested_session_id)

    cli_sid = store.get_thread_id(parent_id) if parent_id else None
    if not cli_sid:
        cli_sid = session.get("cli_session_id")

    is_followup = bool(cli_sid)
    prompt = _build_amber_prompt_cli(data, source_info, is_followup=is_followup)
    log.info("Prompt 模式: %s (%d chars)", "followup" if is_followup else "full", len(prompt))

    user_msg = store.add_message(
        session_id=session["id"],
        role="user",
        content=data.get("comment", ""),
        selected_text=data.get("selected_text"),
        chart_info=json.dumps(data.get("chart_info"), ensure_ascii=False) if data.get("chart_info") else None,
        parent_id=parent_id,
    )

    modifiable_files = _get_modifiable_files(source_info)
    before_snapshot = _snapshot_files(modifiable_files)
    backups = _backup_files(modifiable_files)

    await status_callback("editing", "正在让模型直接修改源文件...")
    response_text, new_cli_sid = await _call_claude_cli(prompt, cli_sid, cwd=project_root, status_callback=status_callback)

    after_snapshot = _snapshot_files(modifiable_files)
    changed_files = _detect_changes(before_snapshot, after_snapshot)

    if new_cli_sid and new_cli_sid != session.get("cli_session_id") and not parent_id:
        store.update_session(session["id"], cli_session_id=new_cli_sid)

    if not session.get("title"):
        store.update_session(session["id"], title=data.get("comment", "")[:50])

    action = "none"
    edit_success = None
    final_response = response_text or "已处理"

    if changed_files:
        edit_success = 1
        if source_info.get("run_command"):
            regenerate_ok = await _regenerate(source_info, status_callback)
            if regenerate_ok:
                action = "reload"
            else:
                _restore_backups(backups)
                edit_success = 0
                final_response = "模型已修改源文件，但重新生成失败，已回滚本次修改。"
        else:
            final_response = response_text or f"已修改 {len(changed_files)} 个文件。"
    else:
        final_response = response_text or "未检测到文件变更。"

    _cleanup_backups(backups)

    store.add_message(
        session_id=session["id"],
        role="assistant",
        content=final_response,
        edits_json=json.dumps(changed_files, ensure_ascii=False) if changed_files else None,
        edit_success=edit_success,
        parent_id=user_msg["id"],
        cli_thread_id=new_cli_sid if parent_id else None,
    )

    return {
        "response": final_response,
        "action": action,
        "session_id": session["id"],
        "message_id": user_msg["id"],
        "page_key": page_key,
        "cli_session_id": new_cli_sid,
    }
