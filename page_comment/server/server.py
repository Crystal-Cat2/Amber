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

    if msg_type == "comment":
        data = msg.get("data", {})
        log.info("收到评论: %s -> %s", data.get("selected_text", "")[:30], data.get("comment", "")[:50])

        await send_json(ws, {"type": "status", "id": msg_id, "status": "processing", "message": "正在分析评论..."})

        try:
            proc = get_processor()
            result = await proc(data, lambda status, message: send_json(ws, {
                "type": "status", "id": msg_id, "status": status, "message": message
            }))
            result_msg = {
                "type": "result", "id": msg_id,
                "response": result.get("response", ""),
                "action": result.get("action", "none"),
                "session_id": result.get("session_id"),
                "message_id": result.get("message_id"),
                "page_key": result.get("page_key"),
                "cli_session_id": result.get("cli_session_id"),
            }
            # 缓存结果，供断线重连后轮询
            cache_result(msg_id, result_msg)
            await send_json(ws, result_msg)
        except Exception as e:
            log.exception("处理评论失败")
            error_msg = {
                "type": "result", "id": msg_id,
                "response": f"处理失败: {e}",
                "action": "none",
            }
            cache_result(msg_id, error_msg)
            await send_json(ws, error_msg)
        return

    if msg_type == "poll_result":
        comment_id = msg.get("comment_id", "")
        cached = get_cached_result(comment_id)
        if cached:
            # 用当前 msg_id 回复，但内容是缓存的结果
            cached_copy = dict(cached)
            cached_copy["id"] = msg_id
            cached_copy["poll_for"] = comment_id
            await send_json(ws, cached_copy)
        else:
            await send_json(ws, {
                "type": "poll_empty", "id": msg_id,
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
        try:
            s = get_store()
            s.deactivate_sessions(page_key)
            session = s.create_session(page_key, page_url)
            await send_json(ws, {"type": "session_created", "id": msg_id, "session": session})
        except Exception as e:
            log.exception("创建会话失败")
            await send_json(ws, {"type": "error", "id": msg_id, "message": f"创建会话失败: {e}"})
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
