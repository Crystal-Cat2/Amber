const dot = document.getElementById("dot");
const toggleInput = document.getElementById("enableToggle");
const toggleDesc = document.getElementById("toggleDesc");
const serverUrlInput = document.getElementById("serverUrl");
const modelProviderSelect = document.getElementById("modelProvider");
const saveBtn = document.getElementById("save");
const reconnectBtn = document.getElementById("reconnect");
const settingsToggle = document.getElementById("settingsToggle");
const settingsPanel = document.getElementById("settingsPanel");

// ========== Connection status ==========

function updateStatus(connected) {
  dot.className = "status-dot " + (connected ? "on" : "off");
}

function normalizePageUrl(url) {
  try {
    const parsed = new URL(url || "");
    parsed.search = "";
    parsed.hash = "";
    return parsed.toString();
  } catch {
    return String(url || "").replace(/[?#].*/, "");
  }
}

function getLegacyHostKey(urlText) {
  try {
    const url = new URL(urlText || "");
    if (url.protocol === "file:") {
      const parts = url.pathname.split("/").filter(Boolean);
      return "file://" + parts.slice(0, Math.min(parts.length, 4)).join("/");
    }
    return url.hostname;
  } catch {
    return null;
  }
}

function buildPageEnabledStorageKey(pageIdentity) {
  return "pc_enabled_page:" + pageIdentity;
}

function getCurrentPageContext(callback) {
  chrome.tabs.query({ active: true, currentWindow: true }, (tabs) => {
    const tab = tabs[0];
    if (!tab?.id || !tab.url) {
      callback(null);
      return;
    }
    const fallbackIdentity = normalizePageUrl(tab.url);
    const legacyHostKey = getLegacyHostKey(tab.url);
    chrome.tabs.sendMessage(tab.id, { type: "get_page_identity" }, (response) => {
      if (chrome.runtime.lastError) {
        callback({
          tabId: tab.id,
          pageIdentity: fallbackIdentity,
          enabled: null,
          legacyHostKey,
        });
        return;
      }
      callback({
        tabId: tab.id,
        pageIdentity: response?.page_identity || fallbackIdentity,
        enabled: typeof response?.enabled === "boolean" ? response.enabled : null,
        legacyHostKey,
      });
    });
  });
}

// ========== Enable/disable toggle ==========

getCurrentPageContext((pageContext) => {
  if (!pageContext?.pageIdentity) {
    toggleDesc.textContent = "无法识别当前页面";
    toggleInput.disabled = true;
    updateStatus(false);
    return;
  }

  chrome.runtime.sendMessage({ type: "get_status", page_identity: pageContext.pageIdentity }, (r) => {
    if (chrome.runtime.lastError) { updateStatus(false); return; }
    updateStatus(r?.connected || false);
  });

  const storageKey = buildPageEnabledStorageKey(pageContext.pageIdentity);
  const defaults = { [storageKey]: null };
  if (pageContext.legacyHostKey) {
    defaults["pc_enabled:" + pageContext.legacyHostKey] = true;
  }
  chrome.storage.local.get(defaults, (r) => {
    const enabled = pageContext.enabled ?? r[storageKey] ?? r["pc_enabled:" + pageContext.legacyHostKey] ?? true;
    toggleInput.checked = enabled;
    toggleDesc.textContent = enabled ? "当前页面已启用" : "当前页面已关闭";
  });

  toggleInput.addEventListener("change", () => {
    const enabled = toggleInput.checked;
    chrome.storage.local.set({ [storageKey]: enabled });
    toggleDesc.textContent = enabled ? "当前页面已启用" : "当前页面已关闭";

    // 通知所有已打开的同页面 tab
    chrome.tabs.query({}, (tabs) => {
      for (const tab of tabs) {
        if (!tab.id) continue;
        chrome.tabs.sendMessage(tab.id, {
          type: "toggle_enabled",
          enabled,
          page_identity: pageContext.pageIdentity,
        }).catch(() => {});
      }
    });
  });
});

// ========== Settings fold ==========

settingsToggle.addEventListener("click", () => {
  settingsToggle.classList.toggle("open");
  settingsPanel.classList.toggle("open");
});

// ========== Server URL ==========

chrome.storage.local.get({ serverUrl: "ws://localhost:18080", pc_model: "claude" }, (r) => {
  serverUrlInput.value = r.serverUrl;
  modelProviderSelect.value = r.pc_model || "claude";
});

saveBtn.addEventListener("click", () => {
  const url = serverUrlInput.value.trim();
  const modelProvider = modelProviderSelect.value || "claude";
  if (!url) return;
  chrome.storage.local.set({ serverUrl: url, pc_model: modelProvider }, () => {
    saveBtn.textContent = "已保存";
    setTimeout(() => { saveBtn.textContent = "保存"; }, 1000);
    chrome.runtime.sendMessage({ type: "reconnect" });
    chrome.tabs.query({}, (tabs) => {
      for (const tab of tabs) {
        chrome.tabs.sendMessage(tab.id, {
          type: "settings_updated",
          model_provider: modelProvider,
        }).catch(() => {});
      }
    });
  });
});

reconnectBtn.addEventListener("click", () => {
  getCurrentPageContext((pageContext) => {
    chrome.runtime.sendMessage({ type: "reconnect", page_identity: pageContext?.pageIdentity }, () => {
      reconnectBtn.textContent = "连接中...";
      setTimeout(() => {
        chrome.runtime.sendMessage({ type: "get_status", page_identity: pageContext?.pageIdentity }, (r) => {
          updateStatus(r?.connected || false);
          reconnectBtn.textContent = "重连";
        });
      }, 1500);
    });
  });
});
