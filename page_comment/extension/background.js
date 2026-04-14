/**
 * PageComment Background Service Worker
 * 管理 WebSocket 连接，路由 content script 和服务器之间的消息
 */

const DEFAULT_SERVER = "ws://localhost:18080";
const MAX_RECONNECT_DELAY = 30000;
const TERMINAL_MESSAGE_TYPES = new Set([
  "result",
  "history",
  "session_created",
  "session_switched",
  "batch_result",
  "error",
]);

const pageContexts = new Map();

function normalizePageUrl(urlText) {
  try {
    const parsed = new URL(urlText || "");
    parsed.search = "";
    parsed.hash = "";
    return parsed.toString();
  } catch {
    return String(urlText || "").replace(/[?#].*/, "");
  }
}

function resolvePageIdentity(msg, sender) {
  if (msg?.page_identity) return String(msg.page_identity);
  if (msg?.page_key) return String(msg.page_key);
  if (msg?.data?.page_meta?.page_key) return String(msg.data.page_meta.page_key);
  if (msg?.data?.page_url) return normalizePageUrl(msg.data.page_url);
  if (sender?.tab?.url) return normalizePageUrl(sender.tab.url);
  return "__default__";
}

function createPageContext(pageIdentity) {
  return {
    pageIdentity,
    ws: null,
    reconnectTimer: null,
    reconnectDelay: 1000,
    keepaliveTimer: null,
    pending: new Map(),
    connectPromise: null,
  };
}

function getPageContext(pageIdentity) {
  const key = String(pageIdentity || "__default__");
  if (!pageContexts.has(key)) {
    pageContexts.set(key, createPageContext(key));
  }
  return pageContexts.get(key);
}

function startKeepalive(context) {
  if (globalThis.__PAGE_COMMENT_TEST_MODE) return;
  stopKeepalive(context);
  context.keepaliveTimer = setInterval(() => {
    if (context.ws && context.ws.readyState === WebSocket.OPEN) {
      context.ws.send(JSON.stringify({ type: "ping" }));
    }
  }, 20000);
}

function stopKeepalive(context) {
  if (context.keepaliveTimer) {
    clearInterval(context.keepaliveTimer);
    context.keepaliveTimer = null;
  }
}

function getServerUrl() {
  return new Promise((resolve) => {
    chrome.storage.local.get({ serverUrl: DEFAULT_SERVER }, (result) => resolve(result.serverUrl));
  });
}

function broadcastStatus(pageIdentity, connected) {
  chrome.tabs.query({}, (tabs) => {
    for (const tab of tabs) {
      chrome.tabs.sendMessage(tab.id, {
        type: "connection_status",
        connected,
        page_identity: pageIdentity,
      }).catch(() => {});
    }
  });
}

function scheduleReconnect(context) {
  if (context.reconnectTimer) return;
  context.reconnectTimer = setTimeout(() => {
    context.reconnectTimer = null;
    context.reconnectDelay = Math.min(context.reconnectDelay * 2, MAX_RECONNECT_DELAY);
    connect(context.pageIdentity);
  }, context.reconnectDelay);
}

async function connect(pageIdentity) {
  const context = getPageContext(pageIdentity);

  if (context.ws && context.ws.readyState === WebSocket.OPEN) return context.ws;
  if (context.connectPromise) return context.connectPromise;

  const url = await getServerUrl();
  context.connectPromise = new Promise((resolve) => {
    try {
      context.ws = new WebSocket(url);
    } catch (_error) {
      scheduleReconnect(context);
      context.connectPromise = null;
      resolve(null);
      return;
    }

    context.ws.onopen = () => {
      console.log("[PageComment] 已连接服务器:", context.pageIdentity);
      context.reconnectDelay = 1000;
      startKeepalive(context);
      broadcastStatus(context.pageIdentity, true);
      context.connectPromise = null;
      resolve(context.ws);
    };

    context.ws.onmessage = (event) => {
      let msg;
      try {
        msg = JSON.parse(event.data);
      } catch {
        return;
      }

      if (msg.type === "pong") return;

      const entry = context.pending.get(msg.id);
      if (!entry) return;

      chrome.tabs.sendMessage(entry.tabId, msg).catch(() => {});

      if (TERMINAL_MESSAGE_TYPES.has(msg.type)) {
        context.pending.delete(msg.id);
      }
    };

    context.ws.onclose = () => {
      console.log("[PageComment] 连接断开:", context.pageIdentity);
      context.ws = null;
      context.connectPromise = null;
      stopKeepalive(context);
      broadcastStatus(context.pageIdentity, false);
      if (context.pending.size > 0) {
        context.reconnectDelay = 1000;
      }
      scheduleReconnect(context);
    };

    context.ws.onerror = () => {
      context.ws?.close();
      context.connectPromise = null;
      resolve(null);
    };

    if (!globalThis.__PAGE_COMMENT_TEST_MODE) {
      setTimeout(() => {
        if (context.connectPromise) {
          context.connectPromise = null;
          resolve(null);
        }
      }, 3000);
    }
  });

  return context.connectPromise;
}

function generateId() {
  return crypto.randomUUID();
}

async function sendViaPageContext(message, sender, sendResponse) {
  const tabId = sender.tab?.id;
  const pageIdentity = resolvePageIdentity(message, sender);
  const context = getPageContext(pageIdentity);
  const connection = await connect(pageIdentity);

  if (!connection || connection.readyState !== WebSocket.OPEN) {
    sendResponse({ ok: false, error: "未连接到服务器" });
    return;
  }

  const id = generateId();
  context.pending.set(id, { tabId, type: message.type });

  const payload = { ...message, id };
  connection.send(JSON.stringify(payload));
  sendResponse({ ok: true, id });
}

function isAnyContextConnected() {
  for (const context of pageContexts.values()) {
    if (context.ws && context.ws.readyState === WebSocket.OPEN) return true;
  }
  return false;
}

chrome.runtime.onMessage.addListener((msg, sender, sendResponse) => {
  if (msg.type === "submit_comment") {
    sendViaPageContext({
      type: "comment",
      page_identity: resolvePageIdentity(msg, sender),
      data: msg.data,
    }, sender, sendResponse);
    return true;
  }

  if (msg.type === "submit_interaction_response") {
    sendViaPageContext({
      type: "interaction_response",
      page_identity: resolvePageIdentity(msg, sender),
      interaction_id: msg.interaction_id,
      response: msg.response,
    }, sender, sendResponse);
    return true;
  }

  if (msg.type === "get_history" || msg.type === "new_session"
      || msg.type === "switch_session" || msg.type === "fork_session") {
    const payload = { ...msg, page_identity: resolvePageIdentity(msg, sender) };
    sendViaPageContext(payload, sender, sendResponse);
    return true;
  }

  if (msg.type === "submit_batch_comment") {
    sendViaPageContext({
      type: "batch_comment",
      page_identity: resolvePageIdentity(msg, sender),
      data: msg.data,
    }, sender, sendResponse);
    return true;
  }

  if (msg.type === "poll_result") {
    sendViaPageContext({
      type: "poll_result",
      page_identity: resolvePageIdentity(msg, sender),
      comment_id: msg.comment_id,
    }, sender, sendResponse);
    return true;
  }

  if (msg.type === "capture_target_visual") {
    chrome.tabs.captureVisibleTab(sender.tab?.windowId, { format: "png" }, (dataUrl) => {
      if (chrome.runtime.lastError || !dataUrl) {
        sendResponse({ ok: false, error: chrome.runtime.lastError?.message || "截图失败" });
        return;
      }
      sendResponse({ ok: true, data_url: dataUrl });
    });
    return true;
  }

  if (msg.type === "get_status") {
    const pageIdentity = msg.page_identity ? String(msg.page_identity) : null;
    if (!pageIdentity) {
      sendResponse({ connected: isAnyContextConnected() });
      return;
    }
    const context = pageContexts.get(pageIdentity);
    sendResponse({ connected: !!(context?.ws && context.ws.readyState === WebSocket.OPEN) });
    return;
  }

  if (msg.type === "reconnect") {
    const pageIdentity = msg.page_identity ? String(msg.page_identity) : null;
    if (pageIdentity) {
      const context = getPageContext(pageIdentity);
      context.reconnectDelay = 1000;
      if (context.ws && context.ws.readyState === WebSocket.OPEN) {
        context.ws.close();
      } else {
        connect(pageIdentity);
      }
    } else {
      for (const context of pageContexts.values()) {
        context.reconnectDelay = 1000;
        if (context.ws && context.ws.readyState === WebSocket.OPEN) {
          context.ws.close();
        } else {
          connect(context.pageIdentity);
        }
      }
    }
    sendResponse({ ok: true });
    return;
  }
});

if (!globalThis.__PAGE_COMMENT_TEST_MODE) {
  // 按页面懒连接，避免无意义的全局共享连接。
}
