/**
 * PageComment Background Service Worker
 * 管理 WebSocket 连接，路由 content script 和服务器之间的消息
 */

const DEFAULT_SERVER = "ws://localhost:18080";
let ws = null;
let reconnectTimer = null;
let reconnectDelay = 1000;
const MAX_RECONNECT_DELAY = 30000;

// 等待响应的回调 {id: {tabId, type}}
const pending = new Map();

// keepalive: 每 20 秒 ping 一次，防止 service worker 被杀
let keepaliveTimer = null;

function startKeepalive() {
  stopKeepalive();
  keepaliveTimer = setInterval(() => {
    if (ws && ws.readyState === WebSocket.OPEN) {
      ws.send(JSON.stringify({ type: "ping" }));
    }
  }, 20000);
}

function stopKeepalive() {
  if (keepaliveTimer) {
    clearInterval(keepaliveTimer);
    keepaliveTimer = null;
  }
}

function getServerUrl() {
  return new Promise((resolve) => {
    chrome.storage.local.get({ serverUrl: DEFAULT_SERVER }, (r) => resolve(r.serverUrl));
  });
}

async function connect() {
  if (ws && ws.readyState === WebSocket.OPEN) return ws;
  if (ws && ws.readyState === WebSocket.CONNECTING) {
    return new Promise((resolve) => {
      const orig = ws.onopen;
      ws.onopen = () => { orig?.(); resolve(ws); };
      const origErr = ws.onerror;
      ws.onerror = () => { origErr?.(); resolve(null); };
    });
  }

  const url = await getServerUrl();
  return new Promise((resolve) => {
    try {
      ws = new WebSocket(url);
    } catch (e) {
      scheduleReconnect();
      resolve(null);
      return;
    }

    ws.onopen = () => {
      console.log("[PageComment] 已连接服务器");
      reconnectDelay = 1000;
      startKeepalive();
      broadcastStatus(true);
      resolve(ws);
    };

    ws.onmessage = (event) => {
      let msg;
      try {
        msg = JSON.parse(event.data);
      } catch {
        return;
      }

      // pong 不需要转发
      if (msg.type === "pong") return;

      const id = msg.id;
      const entry = pending.get(id);
      if (!entry) return;

      chrome.tabs.sendMessage(entry.tabId, msg).catch(() => {});

      // 终态消息：清理 pending
      if (msg.type === "result" || msg.type === "history" || msg.type === "session_created" || msg.type === "error") {
        pending.delete(id);
      }
    };

    ws.onclose = () => {
      console.log("[PageComment] 连接断开");
      ws = null;
      stopKeepalive();
      broadcastStatus(false);
      // 如果还有 pending 请求，立即重连
      if (pending.size > 0) {
        reconnectDelay = 1000;
      }
      scheduleReconnect();
    };

    ws.onerror = () => {
      ws?.close();
      resolve(null);
    };

    setTimeout(() => resolve(null), 3000);
  });
}

function scheduleReconnect() {
  if (reconnectTimer) return;
  reconnectTimer = setTimeout(() => {
    reconnectTimer = null;
    reconnectDelay = Math.min(reconnectDelay * 2, MAX_RECONNECT_DELAY);
    connect();
  }, reconnectDelay);
}

function broadcastStatus(connected) {
  chrome.tabs.query({}, (tabs) => {
    for (const tab of tabs) {
      chrome.tabs.sendMessage(tab.id, { type: "connection_status", connected }).catch(() => {});
    }
  });
}

function generateId() {
  return crypto.randomUUID();
}

// 监听 content script 消息
chrome.runtime.onMessage.addListener((msg, sender, sendResponse) => {
  if (msg.type === "submit_comment") {
    const tabId = sender.tab?.id;

    (async () => {
      const conn = await connect();
      if (!conn || conn.readyState !== WebSocket.OPEN) {
        sendResponse({ ok: false, error: "未连接到服务器" });
        return;
      }

      const id = generateId();
      pending.set(id, { tabId, type: "comment" });

      conn.send(JSON.stringify({
        type: "comment",
        id,
        data: msg.data,
      }));

      sendResponse({ ok: true, id });
    })();

    return true;
  }

  // 通用 WebSocket 转发：get_history, new_session
  if (msg.type === "get_history" || msg.type === "new_session") {
    const tabId = sender.tab?.id;

    (async () => {
      const conn = await connect();
      if (!conn || conn.readyState !== WebSocket.OPEN) {
        sendResponse({ ok: false, error: "未连接到服务器" });
        return;
      }

      const id = generateId();
      pending.set(id, { tabId, type: msg.type });

      conn.send(JSON.stringify({
        type: msg.type,
        id,
        page_key: msg.page_key,
        page_url: msg.page_url,
      }));

      sendResponse({ ok: true, id });
    })();

    return true;
  }

  // 轮询结果（连接断开后重连时用）
  if (msg.type === "poll_result") {
    const tabId = sender.tab?.id;

    (async () => {
      const conn = await connect();
      if (!conn || conn.readyState !== WebSocket.OPEN) {
        sendResponse({ ok: false, error: "未连接到服务器" });
        return;
      }

      const id = generateId();
      pending.set(id, { tabId, type: "poll" });

      conn.send(JSON.stringify({
        type: "poll_result",
        id,
        comment_id: msg.comment_id,
      }));

      sendResponse({ ok: true, id });
    })();

    return true;
  }

  if (msg.type === "get_status") {
    sendResponse({ connected: ws && ws.readyState === WebSocket.OPEN });
    return;
  }

  if (msg.type === "reconnect") {
    reconnectDelay = 1000;
    connect();
    sendResponse({ ok: true });
    return;
  }
});

// 启动时连接
connect();
