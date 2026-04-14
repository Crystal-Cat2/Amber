"""PageComment WebSocket 服务器"""
import asyncio
import json
import logging
import logging.handlers
import os
import time
import uuid

import websockets

from config import HOST, PORT

_LOG_DIR = os.path.join(os.path.dirname(__file__), "logs")
os.makedirs(_LOG_DIR, exist_ok=True)

_file_handler = logging.handlers.TimedRotatingFileHandler(
    os.path.join(_LOG_DIR, "server.log"),
    when="midnight",
    backupCount=14,
    encoding="utf-8",
)
_file_handler.setFormatter(logging.Formatter("%(asctime)s %(message)s", datefmt="%H:%M:%S"))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(message)s",
    datefmt="%H:%M:%S",
    handlers=[logging.StreamHandler(), _file_handler],
)
log = logging.getLogger("server")

processor = None
_store = None

def get_processor():
    global processor
    if processor is None:
        from processor import process_comment
        processor = process_comment
    return processor

def get_store():
    global _store
    if _store is None:
        import store
        _store = store
    return _store


CLIENTS = set()

# 缓存最近的处理结果，供客户端重连后轮询 {msg_id: result_dict}
# 保留 5 分钟
_result_cache = {}
_CACHE_TTL = 300

# 交互队列: ws -> asyncio.Queue，用于将前端的交互回复传递给 processor
_active_interaction_queues: dict = {}


def cache_result(msg_id, result):
    _result_cache[msg_id] = {"result": result, "time": time.time()}
    # 清理过期
    now = time.time()
    expired = [k for k, v in _result_cache.items() if now - v["time"] > _CACHE_TTL]
    for k in expired:
        del _result_cache[k]


def get_cached_result(msg_id):
    entry = _result_cache.get(msg_id)
    if entry and time.time() - entry["time"] < _CACHE_TTL:
        return entry["result"]
    return None


async def send_json(ws, data):
    try:
        await ws.send(json.dumps(data, ensure_ascii=False))
    except websockets.ConnectionClosed:
        log.info("发送消息时连接已断开，忽略")


async def handle_message(ws, raw):
    try:
        msg = json.loads(raw)
    except json.JSONDecodeError:
        await send_json(ws, {"type": "error", "message": "无效的 JSON"})
        return

    msg_type = msg.get("type")
    msg_id = msg.get("id", str(uuid.uuid4()))

    if msg_type == "ping":
        await send_json(ws, {"type": "pong", "id": msg_id})
        return

    if msg_type == "interaction_response":
        interaction_id = msg.get("interaction_id", "")
        response = msg.get("response", {})
        queue = _active_interaction_queues.get(ws)
        if queue:
            await queue.put({"interaction_id": interaction_id, **response})
            await send_json(ws, {"type": "interaction_ack", "id": msg_id, "interaction_id": interaction_id})
        else:
            await send_json(ws, {"type": "error", "id": msg_id, "message": "无活跃的交互请求"})
        return

    if msg_type == "comment":
        data = msg.get("data", {})
        log.info("收到评论: %s -> %s", data.get("selected_text", "")[:30], data.get("comment", "")[:50])

        await send_json(ws, {"type": "status", "id": msg_id, "status": "processing", "message": "正在分析评论..."})

        # 创建交互队列，用于将用户的交互回复传递给 processor
        interaction_queue = asyncio.Queue()
        _active_interaction_queues[ws] = interaction_queue

        try:
            proc = get_processor()
            result = await proc(data, lambda status, message: send_json(ws, {
                "type": "status", "id": msg_id, "status": status, "message": message
            }), interaction_queue=interaction_queue)
            result_msg = {
                "type": "result", "id": msg_id,
                "response": result.get("response", ""),
                "action": result.get("action", "none"),
                "session_id": result.get("session_id"),
                "session": result.get("session"),
                "message_id": result.get("message_id"),
                "page_key": result.get("page_key"),
                "cli_session_id": result.get("cli_session_id"),
            }
            # 缓存结果，供断线重连后轮询
            cache_result(msg_id, result_msg)
            await send_json(ws, result_msg)
        except Exception as e:
            log.exception("处理评论失败")
            err_str = str(e)
            if "invalid_request" in err_str or "signature" in err_str:
                friendly = "API 配置与目标会话不兼容，请新建空白会话后重试。"
            elif "超时" in err_str or "Timeout" in err_str:
                friendly = "处理超时，请稍后重试。"
            elif "启动失败" in err_str:
                friendly = "目标 CLI 未找到，请检查安装。"
            else:
                friendly = "处理失败，请查看服务端日志了解详情。"
            error_msg = {
                "type": "result", "id": msg_id,
                "response": friendly,
                "action": "none",
            }
            cache_result(msg_id, error_msg)
            await send_json(ws, error_msg)
        finally:
            _active_interaction_queues.pop(ws, None)
        return

    if msg_type == "poll_result":
        comment_id = msg.get("comment_id", "")
        cached = get_cached_result(comment_id)
        if cached:
            # 用当前轮询请求的 id 回复，同时保留原 comment_id 方便前端对账
            cached_copy = dict(cached)
            cached_copy["id"] = msg_id
            cached_copy["poll_for"] = comment_id
            await send_json(ws, cached_copy)
        else:
            await send_json(ws, {
                "type": "poll_empty",
                "id": msg_id,
                "comment_id": comment_id,
            })
        return

    if msg_type == "get_history":
        page_key = msg.get("page_key", "")
        try:
            s = get_store()
            history = s.get_history(page_key)
            await send_json(ws, {"type": "history", "id": msg_id, "sessions": history})
        except Exception as e:
            log.exception("获取历史失败")
            await send_json(ws, {"type": "error", "id": msg_id, "message": f"获取历史失败: {e}"})
        return

    if msg_type == "new_session":
        page_key = msg.get("page_key", "")
        page_url = msg.get("page_url", "")
        cli_session_id = msg.get("cli_session_id")
        session_type = msg.get("session_type", "normal")
        model_provider = msg.get("model_provider")
        try:
            s = get_store()
            session = s.create_session(page_key, page_url,
                                       cli_session_id=cli_session_id,
                                       session_type=session_type,
                                       model_provider=model_provider)
            await send_json(ws, {"type": "session_created", "id": msg_id, "session": session})
        except Exception as e:
            log.exception("创建会话失败")
            await send_json(ws, {"type": "error", "id": msg_id, "message": f"创建会话失败: {e}"})
        return

    if msg_type == "switch_session":
        session_id = msg.get("session_id", "")
        try:
            s = get_store()
            session = s.get_session(session_id)
            if not session:
                await send_json(ws, {"type": "error", "id": msg_id, "message": "会话不存在"})
                return
            messages = s.get_session_messages(session_id)
            await send_json(ws, {
                "type": "session_switched", "id": msg_id,
                "session": session,
                "messages": messages,
            })
        except Exception as e:
            log.exception("切换会话失败")
            await send_json(ws, {"type": "error", "id": msg_id, "message": f"切换失败: {e}"})
        return

    if msg_type == "fork_session":
        parent_id = msg.get("parent_session_id", "")
        page_key = msg.get("page_key", "")
        page_url = msg.get("page_url", "")
        try:
            s = get_store()
            session = s.fork_session(parent_id, page_key, page_url)
            await send_json(ws, {"type": "session_created", "id": msg_id, "session": session})
        except Exception as e:
            log.exception("Fork 失败")
            await send_json(ws, {"type": "error", "id": msg_id, "message": f"Fork 失败: {e}"})
        return

    if msg_type == "batch_comment":
        data = msg.get("data", {})
        comments = data.get("comments", [])
        items = data.get("items", [])
        if not comments:
            await send_json(ws, {"type": "error", "id": msg_id, "message": "空批次"})
            return

        log.info("收到批量评论: %d 条", len(comments))
        try:
            proc = get_processor()
            results = []
            for i, comment_text in enumerate(comments):
                await send_json(ws, {
                    "type": "batch_progress", "id": msg_id,
                    "total": len(comments), "current": i + 1,
                    "current_comment": comment_text[:50],
                })
                # 用每条自己的 selected_text（如果有 items 数组）
                single_data = {**data, "comment": comment_text}
                single_data.pop("comments", None)
                single_data.pop("items", None)
                if items and i < len(items):
                    single_data["selected_text"] = items[i].get("selected_text", "")
                    if items[i].get("chart_info"):
                        single_data["chart_info"] = items[i]["chart_info"]
                if results:
                    single_data["session_id"] = results[0].get("session_id")
                result = await proc(single_data, lambda s, m, _i=i: send_json(ws, {
                    "type": "status", "id": msg_id,
                    "status": s, "message": f"[{_i+1}/{len(comments)}] {m}",
                }))
                results.append(result)

            batch_result = {
                "type": "batch_result", "id": msg_id,
                "results": results,
                "session_id": results[0].get("session_id") if results else None,
                "session": results[-1].get("session") if results else None,
                "page_key": results[0].get("page_key") if results else None,
                "cli_session_id": results[-1].get("cli_session_id") if results else None,
                "action": "reload" if any(r.get("action") == "reload" for r in results) else "none",
            }
            cache_result(msg_id, batch_result)
            await send_json(ws, batch_result)
        except Exception as e:
            log.exception("批量处理失败")
            error_msg = {
                "type": "batch_result", "id": msg_id,
                "results": [], "response": f"处理失败: {e}",
                "action": "none",
            }
            cache_result(msg_id, error_msg)
            await send_json(ws, error_msg)
        return

    await send_json(ws, {"type": "error", "id": msg_id, "message": f"未知消息类型: {msg_type}"})


async def handler(ws):
    CLIENTS.add(ws)
    remote = ws.remote_address
    log.info("客户端连接: %s", remote)
    try:
        async for message in ws:
            await handle_message(ws, message)
    except websockets.ConnectionClosed:
        pass
    finally:
        CLIENTS.discard(ws)
        log.info("客户端断开: %s", remote)


async def main():
    log.info("PageComment 服务器启动: ws://%s:%d", HOST, PORT)
    async with websockets.serve(handler, HOST, PORT):
        await asyncio.Future()


if __name__ == "__main__":
    asyncio.run(main())
