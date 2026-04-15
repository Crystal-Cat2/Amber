"""评论处理: 分类 → CLI (--resume) 直接改文件 → 重生成"""
import asyncio
import base64
import csv
import hashlib
import json
import logging
import os
import re
import shutil
import subprocess
import tempfile
from urllib.parse import urlsplit, urlunsplit

from config import (
    AMBER_ROOT,
    CLAUDE_CLI,
    CODEX_CLI,
    CLI_TIMEOUT,
    CLAUDE_CODE_GIT_BASH,
    CLAUDE_MODEL,
    CLAUDE_ALLOWED_TOOLS,
    DEFAULT_MODEL_PROVIDER,
)
from source_registry import resolve_source
import store

log = logging.getLogger("processor")

PAGEDOC_HOST = "10.0.0.54:12005"
VISUAL_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), ".tmp", "page_comment_visuals")
RUNTIME_TMP_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), ".tmp")


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


def _format_target_context(target_context: dict) -> str:
    if not target_context:
        return ""
    parts = [
        f"page_type: {target_context.get('page_type', 'unknown')}",
        f"target_type: {target_context.get('target_type', 'unknown')}",
    ]
    target_text = target_context.get("target_text")
    if target_text:
        parts.append(f"target_text: {target_text}")
    surrounding_blocks = target_context.get("surrounding_blocks") or []
    if surrounding_blocks:
        parts.append("surrounding_blocks:")
        parts.extend(f"- {block}" for block in surrounding_blocks)
    object_meta = target_context.get("object_meta") or {}
    if object_meta:
        parts.append("object_meta:")
        for key, value in object_meta.items():
            if not value:
                continue
            parts.append(f"- {key}: {value}")
    return "\n".join(parts)


def _format_visual_context(visual_context: dict) -> str:
    if not visual_context:
        return ""
    lines = []
    file_path = visual_context.get("file_path")
    if file_path:
        lines.append(f"- file_path: {file_path}")
    format_name = visual_context.get("format")
    width = visual_context.get("width")
    height = visual_context.get("height")
    if format_name or width or height:
        lines.append(f"- meta: format={format_name or 'unknown'}, size={width or '?'}x{height or '?'}")
    return "\n".join(lines)


def _persist_visual_context(visual_context: dict | None) -> dict:
    if not visual_context or not visual_context.get("data_url"):
        return visual_context or {}
    data_url = str(visual_context.get("data_url", ""))
    match = re.match(r"^data:(image\/[a-zA-Z0-9.+-]+);base64,(.+)$", data_url)
    if not match:
        return visual_context
    mime_type = match.group(1)
    payload = match.group(2)
    ext = ".png" if "png" in mime_type else ".jpg"
    os.makedirs(VISUAL_DIR, exist_ok=True)
    file_path = os.path.join(VISUAL_DIR, f"{hashlib.md5(payload.encode('utf-8')).hexdigest()}{ext}")
    try:
        with open(file_path, "wb") as file_obj:
            file_obj.write(base64.b64decode(payload))
    except Exception:
        return visual_context
    persisted = dict(visual_context)
    persisted["file_path"] = file_path
    persisted.pop("data_url", None)
    persisted["mime_type"] = mime_type
    return persisted


def _build_generic_prompt_cli(data: dict) -> str:
    selected_text = data.get("selected_text", "")
    page_context = data.get("page_context") or {}
    target_context = data.get("target_context") or {}
    visual_context = data.get("visual_context") or {}
    page_context_text = ""
    if not selected_text:
        page_context_text = page_context.get("content", "")
    context_label = "选中的文字" if selected_text else "页面正文摘要"
    context_text = selected_text or page_context_text or "(无)"
    truncation_note = ""
    if page_context_text and page_context.get("truncated"):
        truncation_note = "\n注：页面正文摘要已截断，仅包含当前页面主要可见内容。"

    if target_context.get("page_type") == "feishu_doc":
        target_block = _format_target_context(target_context)
        visual_block = _format_visual_context(visual_context)
        visual_section = ""
        if visual_block:
            visual_section = f"""

## 目标对象视觉上下文
{visual_block}"""

        return f"""用户在飞书页面上就当前命中的对象发起了一条评论。

## 飞书页面命中对象
{target_block}{visual_section}

## 用户评论
{data.get("comment", "")}

## 页面标题
{data.get("page_title", "")}

## 页面地址
{data.get("page_url", "")}

你可以根据上述对象级上下文，自主决定是否使用已有 lark 相关能力处理飞书内容。
请直接用中文回答用户。不要输出 JSON，不要修改任何本地文件。"""

    return f"""用户在网页上就当前页面内容发起了一条评论。

## {context_label}
{context_text}{truncation_note}

## 用户评论
{data.get("comment", "")}

## 页面标题
{data.get("page_title", "")}

## 页面地址
{data.get("page_url", "")}

请直接用中文回答用户。不要输出 JSON，不要修改任何文件。"""


def _normalize_page_url(page_url: str) -> str:
    try:
        parts = urlsplit(page_url)
    except ValueError:
        return page_url.split("#", 1)[0].split("?", 1)[0]
    return urlunsplit((parts.scheme, parts.netloc, parts.path, "", ""))


def _resolve_page_key(page_url: str, page_meta: dict | None, source_info: dict | None) -> str:
    page_meta = page_meta or {}
    if page_meta.get("page_key"):
        return page_meta["page_key"]
    if source_info and source_info.get("script_path"):
        return _build_page_key(source_info)
    return _normalize_page_url(page_url)


def _merge_session_state(session: dict, **updates) -> dict:
    merged = dict(session)
    for key, value in updates.items():
        if value is not None:
            merged[key] = value
    return merged


def _ensure_session_title(session: dict, comment: str) -> dict:
    if session.get("title"):
        return session
    title = (comment or "")[:50]
    if not title:
        return session
    store.update_session(session["id"], title=title)
    return _merge_session_state(session, title=title)


def _normalize_model_provider(model_provider: str | None) -> str:
    return "codex" if str(model_provider or "").strip().lower() == "codex" else "claude"


def _ensure_session_model_provider(session: dict, requested_provider: str | None) -> dict:
    requested = _normalize_model_provider(requested_provider) if requested_provider else None
    current = session.get("model_provider")
    inferred_provider = current
    if not inferred_provider and session.get("cli_session_id"):
        inferred_provider = DEFAULT_MODEL_PROVIDER
    provider = requested or _normalize_model_provider(inferred_provider or DEFAULT_MODEL_PROVIDER)

    updates = {}
    if current != provider:
        updates["model_provider"] = provider
    if requested and current and _normalize_model_provider(current) != requested and session.get("cli_session_id"):
        log.info("切换模型 provider: %s -> %s，清空旧 CLI session %s", current, requested, session.get("cli_session_id"))
        updates["cli_session_id"] = None
    if not updates:
        return session
    store.update_session(session["id"], **updates)
    merged = dict(session)
    merged.update(updates)
    return merged


def _save_cli_session(session: dict, cli_session_id: str | None) -> dict:
    if not cli_session_id:
        log.warning("CLI 未返回 session_id，后续消息将无法续用会话")
        return session
    if session.get("cli_session_id") == cli_session_id:
        return session
    log.info("保存 CLI session: %s -> session %s", cli_session_id, session["id"][:12])
    store.update_session(session["id"], cli_session_id=cli_session_id)
    return _merge_session_state(session, cli_session_id=cli_session_id)


def _build_resume_command(model_provider: str, cli_session_id: str) -> list[str]:
    provider = _normalize_model_provider(model_provider)
    if provider == "codex":
        return list(CODEX_CLI if isinstance(CODEX_CLI, list) else [CODEX_CLI]) + [
            "exec",
            "--json",
            "--full-auto",
            "--skip-git-repo-check",
            "resume",
            cli_session_id,
        ]
    return list(CLAUDE_CLI if isinstance(CLAUDE_CLI, list) else [CLAUDE_CLI]) + ["--resume", cli_session_id]


def _build_cli_error_message(err_msg: str, cli_sid: str | None) -> str:
    err_text = str(err_msg or "")
    err_lower = err_text.lower()
    if cli_sid and (
        "invalid" in err_lower
        or "signature" in err_lower
        or "invalid_request" in err_lower
        or "no conversation found" in err_lower
        or "unknown session" in err_lower
    ):
        return (
            f"无法续用会话 {cli_sid[:12]}...：当前会话已失效或与当前模型/API 配置不兼容。"
            f"请新建空白会话后重试。"
        )
    if "超时" in err_text or "timeout" in err_lower:
        return "处理超时，请稍后重试。"
    if "启动失败" in err_text:
        return "目标 CLI 未找到，请检查安装。"
    return "处理失败，请查看服务端日志了解详情。"


async def _call_cli(prompt: str, cli_session_id: str = None, cwd: str | None = None, status_callback=None, model_provider: str = "claude", interaction_queue: asyncio.Queue | None = None) -> tuple[str, str | None]:
    """调用 CLI（流式），支持 provider 特定 resume 继续会话。
    interaction_queue: 用于接收前端交互回复（AskUserQuestion 等），由 server 层注入。
    """
    provider = _normalize_model_provider(model_provider)
    codex_output_path = None
    if provider == "codex":
        base_cmd = CODEX_CLI if isinstance(CODEX_CLI, list) else [CODEX_CLI]
        os.makedirs(RUNTIME_TMP_DIR, exist_ok=True)
        fd, codex_output_path = tempfile.mkstemp(prefix="codex_last_message_", suffix=".txt", dir=RUNTIME_TMP_DIR)
        os.close(fd)
        if cli_session_id:
            cmd = _build_resume_command(provider, cli_session_id) + [prompt]
        else:
            cmd = list(base_cmd) + [
                "exec",
                "--json",
                "--full-auto",
                "--skip-git-repo-check",
                "--output-last-message",
                codex_output_path,
                prompt,
            ]
    else:
        base_cmd = CLAUDE_CLI if isinstance(CLAUDE_CLI, list) else [CLAUDE_CLI]
        cmd = list(base_cmd) + [
            "-p", "-",
            "--model", CLAUDE_MODEL,
            "--input-format", "stream-json",
            "--output-format", "stream-json",
            "--verbose",
            "--allowedTools", ",".join(CLAUDE_ALLOWED_TOOLS),
        ]
        if cli_session_id:
            cmd.extend(["--resume", cli_session_id])

    env = os.environ.copy()
    env["CLAUDE_CODE_GIT_BASH_PATH"] = CLAUDE_CODE_GIT_BASH

    log.info("CLI 调用: %s (provider=%s, session=%s, cwd=%s, prompt=%d chars)", cmd[:6], provider, cli_session_id or "new", cwd or os.getcwd(), len(prompt))

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
        raise RuntimeError(f"{provider} CLI 启动失败: {e}")

    if provider == "claude":
        # stream-json 输入格式: 发送用户消息
        init_msg = json.dumps({
            "type": "user",
            "message": {
                "role": "user",
                "content": [{"type": "text", "text": prompt}],
            },
        })
        proc.stdin.write((init_msg + "\n").encode("utf-8"))
        await proc.stdin.drain()
        # 不关闭 stdin — 保持开放用于后续交互回复
    elif proc.stdin:
        proc.stdin.close()

    result_text = ""
    result_is_error = False
    new_session_id = cli_session_id
    last_text_status_time = 0.0

    async def _send_status(msg: str):
        if status_callback:
            await status_callback("editing", msg)

    async def _send_interaction(data: dict):
        """发送交互事件（AskUserQuestion 等）到前端"""
        if status_callback:
            await status_callback("interaction", json.dumps(data, ensure_ascii=False))

    async def _write_stdin(payload: str):
        """安全写入 stdin"""
        try:
            if proc.stdin and not proc.stdin.is_closing():
                proc.stdin.write((payload + "\n").encode("utf-8"))
                await proc.stdin.drain()
        except (BrokenPipeError, ConnectionResetError, OSError):
            log.warning("写入 CLI stdin 失败（进程可能已退出）")

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

            if provider == "codex":
                if etype == "task_started":
                    sid = event.get("session_id")
                    if sid:
                        new_session_id = sid
                elif etype in ("agent_message_delta", "agent_message"):
                    chunk = event.get("delta") or event.get("message") or event.get("content") or ""
                    if isinstance(chunk, list):
                        chunk = " ".join(str(item.get("text", "")) for item in chunk if isinstance(item, dict))
                    chunk = str(chunk)
                    if chunk:
                        result_text = (result_text or "") + chunk
                        now = asyncio.get_event_loop().time()
                        if now - last_text_status_time > 3:
                            last_text_status_time = now
                            await _send_status(f"AI: {(result_text or '').strip()}")
                elif etype == "task_complete":
                    result_text = event.get("last_message") or result_text
                    new_session_id = event.get("session_id", new_session_id)
                    result_is_error = bool(event.get("is_error", False))
                    break
                elif etype in ("task_error", "error"):
                    result_is_error = True
                    result_text = event.get("message") or event.get("error") or result_text
                    break
            else:
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
                            tool_input = block.get("input", {})
                            tool_use_id = block.get("id", "")

                            if tool_name == "AskUserQuestion" and interaction_queue:
                                # 转发选项到前端，等待用户回复
                                questions = tool_input.get("questions", [])
                                await _send_interaction({
                                    "interaction_type": "ask_user",
                                    "interaction_id": tool_use_id,
                                    "questions": questions,
                                })
                                try:
                                    response = await asyncio.wait_for(
                                        interaction_queue.get(), timeout=120,
                                    )
                                    # 将用户回复写入 stdin
                                    answer_event = json.dumps({
                                        "type": "user",
                                        "message": {
                                            "role": "user",
                                            "content": [{
                                                "type": "tool_result",
                                                "tool_use_id": tool_use_id,
                                                "content": json.dumps(response, ensure_ascii=False),
                                            }],
                                        },
                                    })
                                    await _write_stdin(answer_event)
                                except asyncio.TimeoutError:
                                    log.warning("交互回复超时 (120s), tool_use_id=%s", tool_use_id)
                            else:
                                file_path = tool_input.get("file_path", "")
                                filename = os.path.basename(file_path) if file_path else ""
                                label_map = {
                                    "Read": "正在读取", "Edit": "正在编辑", "Write": "正在写入",
                                    "Glob": "正在搜索文件", "Grep": "正在搜索内容",
                                    "Bash": "正在执行命令", "Agent": "正在调用子代理",
                                }
                                label = label_map.get(tool_name)
                                if label and filename:
                                    await _send_status(f"{label}: {filename}")
                                elif label:
                                    cmd_preview = tool_input.get("command", "")[:30]
                                    pattern_preview = tool_input.get("pattern", "")[:30]
                                    detail = cmd_preview or pattern_preview or ""
                                    if detail:
                                        await _send_status(f"{label}: {detail}")
                                    else:
                                        await _send_status(label)
                                elif tool_name:
                                    await _send_status(f"正在使用工具: {tool_name}")
                        elif btype == "text":
                            txt = block.get("text") or ""
                            result_text = (result_text or "") + txt
                            now = asyncio.get_event_loop().time()
                            if now - last_text_status_time > 3:
                                last_text_status_time = now
                                if result_text and result_text.strip():
                                    await _send_status(f"AI: {result_text.strip()}")

                elif etype == "result":
                    result_text = event.get("result", "")
                    result_is_error = event.get("is_error", False)
                    new_session_id = event.get("session_id", new_session_id)
                    duration = event.get("duration_ms", 0)
                    log.info("CLI 完成: %dms, session=%s, is_error=%s", duration, new_session_id, result_is_error)
                    break

    try:
        await asyncio.wait_for(_read_stream(), timeout=CLI_TIMEOUT)
    except asyncio.TimeoutError:
        log.warning("%s CLI 超时 (%ds), session=%s", provider, CLI_TIMEOUT, new_session_id or cli_session_id)
        proc.kill()
        try:
            await asyncio.wait_for(proc.wait(), timeout=10)
        except asyncio.TimeoutError:
            log.error("%s CLI 进程在 kill 后仍未退出", provider)
        raise RuntimeError(f"{provider} CLI 超时 ({CLI_TIMEOUT}s)")

    # 关闭 stdin（如果还开着）
    if proc.stdin and not proc.stdin.is_closing():
        proc.stdin.close()

    # proc.wait() 和 stderr 读取也加超时保护
    try:
        await asyncio.wait_for(proc.wait(), timeout=15)
    except asyncio.TimeoutError:
        log.error("%s CLI 进程 wait() 超时，强制终止", provider)
        proc.kill()
        try:
            await asyncio.wait_for(proc.wait(), timeout=5)
        except asyncio.TimeoutError:
            pass

    try:
        stderr_bytes = await asyncio.wait_for(proc.stderr.read(), timeout=5)
        stderr = stderr_bytes.decode("utf-8", errors="replace").strip()
    except asyncio.TimeoutError:
        stderr = ""

    if result_is_error:
        error_detail = result_text or stderr or "(无详情)"
        if codex_output_path and os.path.exists(codex_output_path):
            try:
                os.remove(codex_output_path)
            except OSError:
                pass
        raise RuntimeError(f"{provider} CLI 返回错误: {error_detail[-500:]}")
    if proc.returncode != 0 and not result_text:
        if codex_output_path and os.path.exists(codex_output_path):
            try:
                os.remove(codex_output_path)
            except OSError:
                pass
        raise RuntimeError(f"{provider} CLI 失败 (code {proc.returncode}): {stderr[-500:]}")
    if proc.returncode != 0:
        log.warning("%s CLI 返回非零退出码 %d，但已获取到结果，继续处理 (stderr: %s)", provider, proc.returncode, stderr[-200:])

    if provider == "codex" and not result_text and codex_output_path and os.path.exists(codex_output_path):
        try:
            with open(codex_output_path, encoding="utf-8") as f:
                result_text = f.read().strip()
        except OSError as e:
            log.warning("读取 codex 最终消息失败: %s", e)
        finally:
            try:
                os.remove(codex_output_path)
            except OSError:
                pass
    elif codex_output_path and os.path.exists(codex_output_path):
        try:
            os.remove(codex_output_path)
        except OSError:
            pass

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

    script_path = source_info.get("script_path", "")
    try:
        script_rel = os.path.relpath(script_path, AMBER_ROOT).replace("\\", "/")
    except ValueError:
        return
    page_key = _build_page_key(source_info)

    meta_tags = []
    if 'amber:source-script' not in content:
        meta_tags.append(f'  <meta name="amber:source-script" content="{script_rel}">')
    data_dir = source_info.get("data_dir")
    if data_dir and 'amber:source-data' not in content:
        try:
            data_rel = os.path.relpath(data_dir, AMBER_ROOT).replace("\\", "/")
            meta_tags.append(f'  <meta name="amber:source-data" content="{data_rel}">')
        except ValueError:
            pass
    if 'amber:page-key' not in content:
        meta_tags.append(f'  <meta name="amber:page-key" content="{page_key}">')

    if not meta_tags:
        return

    meta_block = "\n".join(meta_tags)

    # 插入到 <head> 后面或 <html> 后面
    for anchor in ("<head>", "<HEAD>", "<html>", "<HTML>"):
        pos = content.find(anchor)
        if pos >= 0:
            insert_at = pos + len(anchor)
            content = content[:insert_at] + "\n" + meta_block + "\n" + content[insert_at:]
            break
    else:
        content = meta_block + "\n" + content

    try:
        with open(html_path, "w", encoding="utf-8") as f:
            f.write(content)
        log.info("已注入 source meta: %s", page_key)
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


def _resolve_session(page_key: str, page_url: str, requested_session_id: str | None, model_provider: str | None = None) -> dict:
    session = None
    if requested_session_id:
        candidate = store.get_session(requested_session_id)
        if candidate and candidate.get("page_key") == page_key:
            session = candidate
    if not session:
        session = store.get_active_session(page_key)
    if not session:
        session = store.create_session(
            page_key,
            page_url,
            model_provider=_normalize_model_provider(model_provider or DEFAULT_MODEL_PROVIDER),
        )
    return session


async def _handle_non_amber(data: dict, status_callback, interaction_queue: asyncio.Queue | None = None) -> dict:
    await status_callback("processing", "正在回答问题...")
    page_url = data.get("page_url", "")
    page_meta = data.get("page_meta", {})
    requested_session_id = data.get("session_id")
    parent_id = data.get("parent_id")
    requested_provider = data.get("model_provider")
    page_key = _resolve_page_key(page_url, page_meta, None)
    session = _resolve_session(page_key, page_url, requested_session_id, requested_provider)
    previous_provider = session.get("model_provider")
    session = _ensure_session_model_provider(session, requested_provider)
    provider_changed = bool(
        requested_provider
        and previous_provider
        and _normalize_model_provider(previous_provider) != session.get("model_provider")
    )

    cli_sid = None if provider_changed else (store.get_thread_id(parent_id) if parent_id else None)
    if not cli_sid:
        cli_sid = session.get("cli_session_id")

    user_msg = store.add_message(
        session_id=session["id"],
        role="user",
        content=data.get("comment", ""),
        selected_text=data.get("selected_text"),
        chart_info=json.dumps(data.get("chart_info"), ensure_ascii=False) if data.get("chart_info") else None,
        parent_id=parent_id,
    )

    prompt_data = dict(data)
    prompt_data["visual_context"] = _persist_visual_context(data.get("visual_context"))

    try:
        response_text, new_cli_sid = await _call_cli(
            _build_generic_prompt_cli(prompt_data),
            cli_sid,
            cwd=AMBER_ROOT,
            status_callback=status_callback,
            model_provider=session.get("model_provider") or requested_provider or DEFAULT_MODEL_PROVIDER,
            interaction_queue=interaction_queue,
        )
    except RuntimeError as e:
        err_msg = str(e)
        if "超时" in err_msg or "Timeout" in err_msg:
            log.warning("通用页面 CLI 超时: %s", err_msg[:200])
        friendly = _build_cli_error_message(err_msg, cli_sid)
        store.add_message(
            session_id=session["id"],
            role="assistant",
            content=friendly,
            parent_id=user_msg["id"],
        )
        session = _ensure_session_title(session, data.get("comment", ""))
        return {
            "response": friendly,
            "action": "none",
            "session_id": session["id"],
            "message_id": user_msg["id"],
            "page_key": page_key,
            "cli_session_id": session.get("cli_session_id"),
            "session": session,
        }
    session = _save_cli_session(session, new_cli_sid)
    session = _ensure_session_title(session, data.get("comment", ""))

    store.add_message(
        session_id=session["id"],
        role="assistant",
        content=response_text,
        parent_id=user_msg["id"],
        cli_thread_id=new_cli_sid if parent_id else None,
    )
    return {
        "response": response_text,
        "action": "none",
        "session_id": session["id"],
        "message_id": user_msg["id"],
        "page_key": page_key,
        "cli_session_id": session.get("cli_session_id"),
        "session": session,
    }


async def process_comment(data: dict, status_callback, interaction_queue: asyncio.Queue | None = None) -> dict:
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
    requested_provider = data.get("model_provider")

    source_info = resolve_source(page_url, page_meta)
    page_type = _classify_page(page_url, source_info)
    log.info("页面类型: %s, URL: %s", page_type, page_url[:80])

    if page_type != "amber_dashboard":
        return await _handle_non_amber(data, status_callback, interaction_queue)

    page_key = _resolve_page_key(page_url, page_meta, source_info)
    project_root = source_info.get("project_root") or os.path.dirname(source_info["script_path"])
    await status_callback("processing", f"已追溯到脚本: {os.path.basename(source_info['script_path'])}")

    session = _resolve_session(page_key, page_url, requested_session_id, requested_provider)
    previous_provider = session.get("model_provider")
    session = _ensure_session_model_provider(session, requested_provider)
    provider_changed = bool(
        requested_provider
        and previous_provider
        and _normalize_model_provider(previous_provider) != session.get("model_provider")
    )

    # 自动链接创建者会话
    creator_sid = data.get("page_meta", {}).get("creator_session")
    if (
        creator_sid
        and not provider_changed
        and session.get("model_provider") == "claude"
        and not session.get("cli_session_id")
        and session.get("session_type", "normal") == "normal"
    ):
        store.update_session(session["id"],
                             cli_session_id=creator_sid,
                             creator_session_id=creator_sid,
                             session_type="linked")
        session = _merge_session_state(
            session,
            cli_session_id=creator_sid,
            creator_session_id=creator_sid,
            session_type="linked",
        )

    cli_sid = None if provider_changed else (store.get_thread_id(parent_id) if parent_id else None)
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

    await status_callback("editing", "AI 正在思考...")
    try:
        response_text, new_cli_sid = await _call_cli(
            prompt,
            cli_sid,
            cwd=project_root,
            status_callback=status_callback,
            model_provider=session.get("model_provider") or requested_provider or DEFAULT_MODEL_PROVIDER,
            interaction_queue=interaction_queue,
        )
    except RuntimeError as e:
        err_msg = str(e)
        _cleanup_backups(backups)
        if "超时" in err_msg or "Timeout" in err_msg:
            log.warning("Amber dashboard CLI 超时: %s", err_msg[:200])
        elif cli_sid and ("invalid" in err_msg.lower() or "signature" in err_msg.lower() or "no conversation found" in err_msg.lower()):
            log.warning("Resume 失败: %s", err_msg[:200])
        friendly = _build_cli_error_message(err_msg, cli_sid)

        store.add_message(session_id=session["id"], role="assistant", content=friendly,
                          parent_id=user_msg["id"])
        session = _ensure_session_title(session, data.get("comment", ""))
        return {
            "response": friendly,
            "action": "none",
            "session_id": session["id"],
            "message_id": user_msg["id"],
            "page_key": page_key,
            "cli_session_id": session.get("cli_session_id"),
            "session": session,
        }

    after_snapshot = _snapshot_files(modifiable_files)
    changed_files = _detect_changes(before_snapshot, after_snapshot)

    # 始终保存 CLI session ID（确保后续消息能 --resume）
    session = _save_cli_session(session, new_cli_sid)
    session = _ensure_session_title(session, data.get("comment", ""))

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
        "cli_session_id": session.get("cli_session_id"),
        "session": session,
    }
