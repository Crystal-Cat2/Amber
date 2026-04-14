import importlib.util
import json
import os
import sys
import unittest


SERVER_DIR = os.path.dirname(__file__)
if SERVER_DIR not in sys.path:
    sys.path.insert(0, SERVER_DIR)

SERVER_FILE = os.path.join(SERVER_DIR, "server.py")
SERVER_SPEC = importlib.util.spec_from_file_location("page_comment_server", SERVER_FILE)
server = importlib.util.module_from_spec(SERVER_SPEC)
assert SERVER_SPEC and SERVER_SPEC.loader
SERVER_SPEC.loader.exec_module(server)


class FakeWebSocket:
    def __init__(self):
        self.sent = []

    async def send(self, payload):
        self.sent.append(json.loads(payload))


class ServerMessageTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        server._result_cache.clear()
        server._active_interaction_queues.clear()

    async def test_poll_result_returns_cached_result_with_poll_for(self):
        ws = FakeWebSocket()
        server.cache_result("comment-1", {
            "type": "result",
            "id": "comment-1",
            "response": "已完成",
            "action": "none",
        })

        await server.handle_message(ws, json.dumps({
            "type": "poll_result",
            "id": "poll-1",
            "comment_id": "comment-1",
        }))

        self.assertEqual(len(ws.sent), 1)
        self.assertEqual(ws.sent[0]["type"], "result")
        self.assertEqual(ws.sent[0]["id"], "poll-1")
        self.assertEqual(ws.sent[0]["poll_for"], "comment-1")
        self.assertEqual(ws.sent[0]["response"], "已完成")

    async def test_poll_result_returns_poll_empty_when_cache_missing(self):
        ws = FakeWebSocket()

        await server.handle_message(ws, json.dumps({
            "type": "poll_result",
            "id": "poll-2",
            "comment_id": "missing-comment",
        }))

        self.assertEqual(len(ws.sent), 1)
        self.assertEqual(ws.sent[0], {
            "type": "poll_empty",
            "id": "poll-2",
            "comment_id": "missing-comment",
        })


if __name__ == "__main__":
    unittest.main()
