const dot = document.getElementById("dot");
const statusText = document.getElementById("statusText");
const serverUrlInput = document.getElementById("serverUrl");
const saveBtn = document.getElementById("save");
const reconnectBtn = document.getElementById("reconnect");

function updateStatus(connected) {
  dot.className = "dot " + (connected ? "on" : "off");
  statusText.textContent = connected ? "已连接" : "未连接";
}

// 加载设置
chrome.storage.local.get({ serverUrl: "ws://localhost:18080" }, (r) => {
  serverUrlInput.value = r.serverUrl;
});

// 检查连接状态
chrome.runtime.sendMessage({ type: "get_status" }, (r) => {
  if (chrome.runtime.lastError) {
    updateStatus(false);
    return;
  }
  updateStatus(r?.connected || false);
});

saveBtn.addEventListener("click", () => {
  const url = serverUrlInput.value.trim();
  if (!url) return;
  chrome.storage.local.set({ serverUrl: url }, () => {
    saveBtn.textContent = "已保存";
    setTimeout(() => { saveBtn.textContent = "保存"; }, 1000);
    // 重连到新地址
    chrome.runtime.sendMessage({ type: "reconnect" });
  });
});

reconnectBtn.addEventListener("click", () => {
  chrome.runtime.sendMessage({ type: "reconnect" }, () => {
    reconnectBtn.textContent = "连接中...";
    setTimeout(() => {
      chrome.runtime.sendMessage({ type: "get_status" }, (r) => {
        updateStatus(r?.connected || false);
        reconnectBtn.textContent = "重连";
      });
    }, 1500);
  });
});
