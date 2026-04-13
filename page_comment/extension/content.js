/**
 * PageComment Content Script
 * 注入评论 UI: 选中文字/点击图表 → 评论按钮 → 评论框 → 提交 → 侧边栏聊天
 */

(function () {
  "use strict";
  if (window.__pageCommentLoaded) return;
  window.__pageCommentLoaded = true;

  const chartUtils = window.PageCommentChartUtils || {};
  const chartInspectQueue = [];
  const chartInspectPending = new Map();
  const hoverScanSelectors = [
    "canvas",
    ".js-plotly-plot",
    "[_echarts_instance_]",
    ".echarts",
    ".highcharts-container svg",
    "[class*='chart'] svg",
    "[class*='plot'] svg",
    "[class*='graph'] svg",
  ].join(", ");

  let commentBtn = null;
  let commentBox = null;
  let currentSelection = "";
  let currentChartInfo = null;
  let bridgeReady = false;
  let skipNextSelectionCleanup = false;

  // 会话相关状态
  let sidebarEl = null;
  let sidebarOpen = false;
  let currentSessionId = null;
  let currentPageKey = null;
  let pendingCommentId = null;

  injectPageBridge();
  markChartTargets();
  window.addEventListener("message", handleBridgeMessage);
  window.addEventListener("load", () => setTimeout(markChartTargets, 0), { once: true });
  setTimeout(markChartTargets, 300);

  if (window.MutationObserver) {
    const observer = new MutationObserver(() => {
      clearTimeout(observer._pageCommentTimer);
      observer._pageCommentTimer = setTimeout(markChartTargets, 100);
    });
    observer.observe(document.documentElement || document.body, { childList: true, subtree: true });
  }

  function getPageMeta() {
    const meta = {};
    const scriptTag = document.querySelector('meta[name="amber:source-script"]');
    const dataTag = document.querySelector('meta[name="amber:source-data"]');
    if (scriptTag) meta.source_script = scriptTag.content;
    if (dataTag) meta.source_data = dataTag.content;
    return meta;
  }

  function injectPageBridge() {
    if (!chrome.runtime?.getURL || document.documentElement?.dataset?.pageCommentBridgeInjected === "1") return;
    const script = document.createElement("script");
    script.src = chrome.runtime.getURL("page_bridge.js");
    script.dataset.pageCommentBridge = "1";
    script.onload = () => script.remove();
    script.onerror = () => script.remove();
    (document.head || document.documentElement).appendChild(script);
    document.documentElement.dataset.pageCommentBridgeInjected = "1";
  }

  function ensureChartKey(chartEl) {
    if (!chartEl?.dataset) return chartEl?.id || "";
    if (!chartEl.dataset.pageCommentChartKey) {
      chartEl.dataset.pageCommentChartKey = chartEl.id || `pc-chart-${crypto.randomUUID()}`;
    }
    return chartEl.dataset.pageCommentChartKey;
  }

  function markChartTargets() {
    const candidates = document.querySelectorAll(hoverScanSelectors);
    for (const candidate of candidates) {
      const chartEl = chartUtils.detectChartElement?.(candidate) || null;
      if (!chartEl || !chartUtils.isInteractiveChartElement?.(chartEl)) continue;
      chartEl.classList?.add("pc-chart-hover-target");
      ensureChartKey(chartEl);
    }
  }

  function requestChartInspect(chartEl) {
    const chartKey = ensureChartKey(chartEl);
    if (!chartKey) return;
    const request = { requestId: crypto.randomUUID(), chartKey };
    chartInspectPending.set(request.requestId, request);
    if (!bridgeReady) {
      chartInspectQueue.push(request);
      injectPageBridge();
      setTimeout(flushChartInspectQueue, 50);
      return;
    }
    postChartInspect(request);
  }

  function postChartInspect(request) {
    window.postMessage({
      source: "page-comment-content",
      type: "inspect-chart",
      requestId: request.requestId,
      chartKey: request.chartKey,
    }, "*");
    setTimeout(() => chartInspectPending.delete(request.requestId), 3000);
  }

  function flushChartInspectQueue() {
    if (!bridgeReady) return;
    while (chartInspectQueue.length > 0) postChartInspect(chartInspectQueue.shift());
  }

  function handleBridgeMessage(event) {
    if (event.source !== window) return;
    const msg = event.data;
    if (!msg || msg.source !== "page-comment-bridge") return;
    if (msg.type === "bridge-ready") {
      bridgeReady = true;
      flushChartInspectQueue();
      markChartTargets();
      return;
    }
    if (msg.type !== "inspect-chart-result" || !msg.requestId) return;
    const request = chartInspectPending.get(msg.requestId);
    chartInspectPending.delete(msg.requestId);
    if (!request || !currentChartInfo || currentChartInfo.element_id !== request.chartKey) return;
    currentChartInfo = chartUtils.mergeChartInfo?.(currentChartInfo, msg.chartInfo || {}) || currentChartInfo;
    refreshCommentSelectionLabel();
  }

  function cleanup() {
    if (commentBtn) { commentBtn.remove(); commentBtn = null; }
    if (commentBox) { commentBox.remove(); commentBox = null; }
  }

  function getSelectionRect() {
    const sel = window.getSelection();
    if (!sel || sel.rangeCount === 0) return null;
    return sel.getRangeAt(0).getBoundingClientRect();
  }

  function getChartLabel() {
    if (!currentChartInfo) return "图表";
    const label = currentChartInfo.title || currentChartInfo.canvas_id || currentChartInfo.element_id || "图表";
    const typeParts = [
      currentChartInfo.library && currentChartInfo.library !== "unknown" ? currentChartInfo.library : "",
      currentChartInfo.chart_type && currentChartInfo.chart_type !== "canvas" ? currentChartInfo.chart_type : "",
    ].filter(Boolean);
    return typeParts.length > 0 ? `📊 ${label} (${typeParts.join(" / ")})` : `📊 ${label}`;
  }

  function refreshCommentSelectionLabel() {
    const selectedDiv = commentBox?.querySelector(".pc-comment-box-selected");
    if (!selectedDiv || !currentChartInfo) return;
    selectedDiv.textContent = getChartLabel();
  }

  function showCommentButton(rect, isChart) {
    cleanup();
    commentBtn = document.createElement("button");
    commentBtn.className = "pc-comment-btn";
    commentBtn.textContent = isChart ? "修改图表" : "评论";
    commentBtn.style.left = (rect.left + window.scrollX + rect.width / 2 - 30) + "px";
    commentBtn.style.top = (rect.bottom + window.scrollY + 4) + "px";
    document.body.appendChild(commentBtn);
    commentBtn.addEventListener("mousedown", (e) => {
      e.preventDefault();
      e.stopPropagation();
      showCommentBox(rect, isChart);
    });
  }

  function showCommentBox(rect, isChart) {
    if (commentBtn) { commentBtn.remove(); commentBtn = null; }
    if (commentBox) { commentBox.remove(); commentBox = null; }

    commentBox = document.createElement("div");
    commentBox.className = "pc-comment-box";
    commentBox.style.left = Math.max(10, rect.left + window.scrollX - 40) + "px";
    commentBox.style.top = (rect.bottom + window.scrollY + 8) + "px";

    const selectedDiv = document.createElement("div");
    selectedDiv.className = "pc-comment-box-selected";
    selectedDiv.textContent = isChart
      ? getChartLabel()
      : (currentSelection.length > 200 ? currentSelection.slice(0, 200) + "..." : currentSelection);

    const textarea = document.createElement("textarea");
    textarea.placeholder = isChart
      ? "修改图表: 如改标题、换折线图、调坐标轴..."
      : "输入评论或修改指令...";

    const actions = document.createElement("div");
    actions.className = "pc-comment-box-actions";

    const cancelBtn = document.createElement("button");
    cancelBtn.className = "pc-btn-cancel";
    cancelBtn.textContent = "取消";
    cancelBtn.addEventListener("click", cleanup);

    const submitBtn = document.createElement("button");
    submitBtn.className = "pc-btn-submit";
    submitBtn.textContent = "提交";
    submitBtn.addEventListener("click", () => {
      const comment = textarea.value.trim();
      if (!comment) return;
      submitBtn.disabled = true;
      submitBtn.textContent = "提交中...";
      submitComment(comment, rect);
    });

    textarea.addEventListener("keydown", (e) => {
      if (e.key === "Enter" && (e.ctrlKey || e.metaKey)) {
        e.preventDefault();
        submitBtn.click();
      }
    });

    actions.appendChild(cancelBtn);
    actions.appendChild(submitBtn);
    commentBox.appendChild(selectedDiv);
    commentBox.appendChild(textarea);
    commentBox.appendChild(actions);
    document.body.appendChild(commentBox);
    textarea.focus();
  }

  // 提交评论
  function submitComment(comment) {
    const data = {
      selected_text: currentSelection,
      comment: comment,
      page_url: window.location.href,
      page_title: document.title,
      page_meta: getPageMeta(),
    };
    if (currentChartInfo) data.chart_info = currentChartInfo;
    if (currentSessionId) data.session_id = currentSessionId;

    chrome.runtime.sendMessage({ type: "submit_comment", data }, (response) => {
      cleanup();
      if (chrome.runtime.lastError || !response?.ok) {
        openSidebarChat();
        appendChatMessage("system", response?.error || "发送失败，服务器未连接");
        return;
      }
      pendingCommentId = response.id;
      openSidebarChat();
      const label = currentChartInfo ? getChartLabel() : (currentSelection ? currentSelection.slice(0, 80) : "");
      appendChatMessage("user", comment, label);
      showTypingIndicator("正在处理...");
      disableChatInput();
    });
  }

  function escapeHtml(s) {
    const d = document.createElement("div");
    d.textContent = s;
    return d.innerHTML;
  }

  // ========== 侧边栏聊天功能 ==========

  function appendChatMessage(role, text, context) {
    const body = sidebarEl?.querySelector(".pc-sidebar-body");
    if (!body) return;
    const msgEl = document.createElement("div");
    msgEl.className = `pc-chat-msg pc-chat-${role}`;
    if (context) {
      const ctxEl = document.createElement("div");
      ctxEl.className = "pc-chat-context";
      ctxEl.textContent = context.length > 100 ? context.slice(0, 100) + "..." : context;
      msgEl.appendChild(ctxEl);
    }
    const contentEl = document.createElement("div");
    contentEl.className = "pc-chat-content";
    contentEl.textContent = text;
    msgEl.appendChild(contentEl);
    body.appendChild(msgEl);
    body.scrollTop = body.scrollHeight;
  }

  function showTypingIndicator(text) {
    const body = sidebarEl?.querySelector(".pc-sidebar-body");
    if (!body) return;
    removeTypingIndicator();
    const el = document.createElement("div");
    el.className = "pc-chat-msg pc-chat-assistant pc-chat-typing";
    el.innerHTML = '<span class="pc-spinner"></span><span class="pc-typing-text">' + escapeHtml(text) + '</span>';
    body.appendChild(el);
    body.scrollTop = body.scrollHeight;
  }

  function updateTypingIndicator(text) {
    const el = sidebarEl?.querySelector(".pc-chat-typing .pc-typing-text");
    if (el) el.textContent = text;
  }

  function removeTypingIndicator() {
    sidebarEl?.querySelector(".pc-chat-typing")?.remove();
  }

  function updateCliSessionId(cliSessionId) {
    if (!sidebarEl || !cliSessionId) return;
    let infoEl = sidebarEl.querySelector(".pc-cli-session-info");
    if (!infoEl) {
      const header = sidebarEl.querySelector(".pc-sidebar-header");
      infoEl = document.createElement("div");
      infoEl.className = "pc-cli-session-info";
      infoEl.addEventListener("click", () => {
        navigator.clipboard.writeText(infoEl.dataset.fullId || "").then(() => {
          const orig = infoEl.textContent;
          infoEl.textContent = "已复制!";
          setTimeout(() => { infoEl.textContent = orig; }, 1000);
        });
      });
      header.parentNode.insertBefore(infoEl, header.nextSibling);
    }
    infoEl.dataset.fullId = cliSessionId;
    infoEl.textContent = "CLI: " + cliSessionId;
    infoEl.title = "点击复制完整 ID";
  }

  // 监听 background 消息
  chrome.runtime.onMessage.addListener((msg) => {
    if (msg.type === "status" && msg.id) {
      if (msg.id === pendingCommentId) {
        updateTypingIndicator(msg.message);
      }
    }
    if (msg.type === "result" && msg.id) {
      // 直接匹配或轮询匹配
      const isMyResult = msg.id === pendingCommentId || msg.poll_for === pendingCommentId;
      if (isMyResult && pendingCommentId) {
        removeTypingIndicator();
        const isError = msg.response?.startsWith("处理失败") || msg.response?.startsWith("部分编辑失败");
        appendChatMessage(isError ? "system" : "assistant", msg.response);
        pendingCommentId = null;
        enableChatInput();
      }

      if (msg.session_id) currentSessionId = msg.session_id;
      if (msg.page_key) {
        currentPageKey = msg.page_key;
        // 缓存 page_key，页面刷新后仍可匹配历史记录
        chrome.storage.local.set({ ["pk:" + location.href.replace(/[?#].*/, "")]: msg.page_key });
      }
      if (msg.cli_session_id) updateCliSessionId(msg.cli_session_id);

      if (msg.action === "reload") {
        appendChatMessage("system", "页面即将刷新...");
        setTimeout(() => window.location.reload(), 1500);
      }
    }

    // 轮询返回空 = 还在处理中，继续轮询
    if (msg.type === "poll_empty" && pendingCommentId) {
      setTimeout(pollForResult, 3000);
    }

    // 连接恢复时，如果有 pending 请求，轮询结果
    if (msg.type === "connection_status" && msg.connected && pendingCommentId) {
      setTimeout(pollForResult, 500);
    }

    if (msg.type === "history") renderHistory(msg.sessions);
    if (msg.type === "session_created") {
      currentSessionId = msg.session?.id;
      clearChatBody();
    }
  });

  function pollForResult() {
    if (!pendingCommentId) return;
    chrome.runtime.sendMessage({
      type: "poll_result",
      comment_id: pendingCommentId,
    });
  }

  // 监听文字选中
  document.addEventListener("mouseup", (e) => {
    if (e.target.closest(".pc-comment-btn, .pc-comment-box, .pc-sidebar")) return;

    setTimeout(() => {
      const sel = window.getSelection();
      const text = sel?.toString().trim();
      const shouldSuppressCleanup = chartUtils.shouldSuppressEmptySelectionCleanup?.({
        text,
        skipNextSelectionCleanup,
      });
      skipNextSelectionCleanup = false;

      if (shouldSuppressCleanup) return;

      if (!text || text.length < 2) {
        if (commentBtn) { commentBtn.remove(); commentBtn = null; }
        return;
      }

      currentSelection = text;
      currentChartInfo = null;
      const rect = getSelectionRect();
      if (rect) showCommentButton(rect, false);
    }, 10);
  });

  // 监听图表点击
  document.addEventListener("click", (e) => {
    if (e.target.closest(".pc-comment-btn, .pc-comment-box, .pc-sidebar")) return;

    const chartEl = chartUtils.detectChartElement?.(e.target) || null;
    if (!chartEl || !chartUtils.isInteractiveChartElement?.(chartEl)) return;

    e.stopPropagation();
    window.getSelection()?.removeAllRanges();

    chartEl.classList?.add("pc-chart-hover-target");
    ensureChartKey(chartEl);

    currentChartInfo = chartUtils.buildFallbackChartInfo?.(chartEl) || {
      library: "unknown",
      canvas_id: chartEl.id || "",
      element_id: chartEl.id || "",
      title: "",
      chart_type: chartEl.tagName ? chartEl.tagName.toLowerCase() : "unknown",
      config_summary: "DOM context only",
      series_summary: "",
      dom_context: chartEl.id || "chart",
    };
    currentSelection = currentChartInfo.dom_context || chartUtils.getChartContext?.(chartEl) || "chart";

    const rect = chartEl.getBoundingClientRect();
    skipNextSelectionCleanup = true;
    showCommentButton(rect, true);
    requestChartInspect(chartEl);
  }, true);

  // 点击其他地方关闭评论框
  document.addEventListener("mousedown", (e) => {
    if (commentBox && !commentBox.contains(e.target) && e.target !== commentBtn) {
      cleanup();
    }
  });

  // ========== 侧边栏：聊天界面 ==========

  function buildPageKey() {
    const meta = getPageMeta();
    if (meta.source_script) {
      const htmlName = location.pathname.split("/").pop() || "";
      return meta.source_script + "::" + htmlName;
    }
    return location.href.replace(/[?#].*/, "");
  }

  function buildPageKeyAsync(callback) {
    const meta = getPageMeta();
    if (meta.source_script) {
      const htmlName = location.pathname.split("/").pop() || "";
      callback(meta.source_script + "::" + htmlName);
      return;
    }
    // 尝试从缓存读取服务端返回的 page_key
    const urlKey = "pk:" + location.href.replace(/[?#].*/, "");
    chrome.storage.local.get(urlKey, (data) => {
      callback(data[urlKey] || location.href.replace(/[?#].*/, ""));
    });
  }

  function createSidebar() {
    if (sidebarEl) return sidebarEl;
    sidebarEl = document.createElement("div");
    sidebarEl.className = "pc-sidebar";
    sidebarEl.innerHTML = `
      <div class="pc-sidebar-header">
        <span class="pc-sidebar-title">评论</span>
        <div class="pc-sidebar-actions">
          <button class="pc-sidebar-new-btn" title="开启新会话">+ 新会话</button>
          <button class="pc-sidebar-close" title="关闭">&times;</button>
        </div>
      </div>
      <div class="pc-sidebar-body"></div>
      <div class="pc-sidebar-input">
        <textarea class="pc-chat-input" placeholder="输入评论或修改指令... (Ctrl+Enter 发送)" rows="2"></textarea>
        <button class="pc-chat-send" title="发送">发送</button>
      </div>
    `;
    document.body.appendChild(sidebarEl);

    sidebarEl.querySelector(".pc-sidebar-close").addEventListener("click", toggleSidebar);
    sidebarEl.querySelector(".pc-sidebar-new-btn").addEventListener("click", requestNewSession);

    const input = sidebarEl.querySelector(".pc-chat-input");
    const sendBtn = sidebarEl.querySelector(".pc-chat-send");
    sendBtn.addEventListener("click", () => sendChatMessage(input));
    input.addEventListener("keydown", (e) => {
      if (e.key === "Enter" && (e.ctrlKey || e.metaKey)) {
        e.preventDefault();
        sendChatMessage(input);
      }
    });

    return sidebarEl;
  }

  function sendChatMessage(input) {
    const text = input.value.trim();
    if (!text || pendingCommentId) return;

    const data = {
      selected_text: "",
      comment: text,
      page_url: window.location.href,
      page_title: document.title,
      page_meta: getPageMeta(),
    };
    if (currentSessionId) data.session_id = currentSessionId;

    input.value = "";
    disableChatInput();

    chrome.runtime.sendMessage({ type: "submit_comment", data }, (response) => {
      if (chrome.runtime.lastError || !response?.ok) {
        appendChatMessage("system", response?.error || "发送失败，服务器未连接");
        enableChatInput();
        return;
      }
      pendingCommentId = response.id;
      appendChatMessage("user", text);
      showTypingIndicator("正在处理...");
    });
  }

  function disableChatInput() {
    const input = sidebarEl?.querySelector(".pc-chat-input");
    const btn = sidebarEl?.querySelector(".pc-chat-send");
    if (input) input.disabled = true;
    if (btn) btn.disabled = true;
  }

  function enableChatInput() {
    const input = sidebarEl?.querySelector(".pc-chat-input");
    const btn = sidebarEl?.querySelector(".pc-chat-send");
    if (input) { input.disabled = false; input.focus(); }
    if (btn) btn.disabled = false;
  }

  function clearChatBody() {
    const body = sidebarEl?.querySelector(".pc-sidebar-body");
    if (body) body.innerHTML = "";
  }

  function createSidebarToggle() {
    const btn = document.createElement("button");
    btn.className = "pc-sidebar-toggle";
    btn.textContent = "\uD83D\uDCAC";
    btn.title = "评论";
    btn.addEventListener("click", toggleSidebar);
    document.body.appendChild(btn);
    return btn;
  }

  function openSidebarChat() {
    if (!sidebarOpen) {
      sidebarOpen = true;
      const sidebar = createSidebar();
      sidebar.classList.add("pc-sidebar-open");
    }
    if (!currentPageKey) {
      buildPageKeyAsync((key) => { currentPageKey = key; });
    }
  }

  function toggleSidebar() {
    sidebarOpen = !sidebarOpen;
    const sidebar = createSidebar();
    sidebar.classList.toggle("pc-sidebar-open", sidebarOpen);
    if (sidebarOpen) {
      buildPageKeyAsync((key) => {
        currentPageKey = key;
        requestHistory();
      });
    }
  }

  function requestHistory() {
    if (!currentPageKey) return;
    chrome.runtime.sendMessage({ type: "get_history", page_key: currentPageKey });
  }

  function requestNewSession() {
    if (!currentPageKey) return;
    currentSessionId = null;
    chrome.runtime.sendMessage({
      type: "new_session",
      page_key: currentPageKey,
      page_url: location.href,
    });
  }

  function renderHistory(sessions) {
    const body = sidebarEl?.querySelector(".pc-sidebar-body");
    if (!body) return;

    // 正在处理中，不覆盖当前聊天
    if (pendingCommentId) return;

    body.innerHTML = "";

    if (!sessions || sessions.length === 0) {
      body.innerHTML = '<div class="pc-sidebar-empty">暂无评论历史，选中文字或点击图表开始评论</div>';
      return;
    }

    const activeSession = sessions.find(s => s.is_active);
    if (activeSession) currentSessionId = activeSession.id;

    for (const session of sessions) {
      const sessionEl = document.createElement("div");
      sessionEl.className = "pc-session-group";
      if (session.is_active) sessionEl.classList.add("pc-session-active");

      // 会话头部：标题 + CLI session ID
      const header = document.createElement("div");
      header.className = "pc-session-header" + (session.is_active ? "" : " pc-session-collapsed");
      const title = session.title || "未命名会话";
      const date = session.created_at ? new Date(session.created_at).toLocaleString("zh-CN") : "";
      header.innerHTML = `<span class="pc-session-title">${escapeHtml(title)}</span><span class="pc-session-date">${date}</span>`;
      if (!session.is_active) {
        header.addEventListener("click", () => sessionEl.classList.toggle("pc-session-expanded"));
      }
      sessionEl.appendChild(header);

      // CLI Session ID 行（可点击复制）
      if (session.cli_session_id) {
        const sidEl = document.createElement("div");
        sidEl.className = "pc-session-cli-id";
        sidEl.textContent = "CLI: " + session.cli_session_id;
        sidEl.title = "点击复制";
        sidEl.addEventListener("click", (e) => {
          e.stopPropagation();
          navigator.clipboard.writeText(session.cli_session_id).then(() => {
            const orig = sidEl.textContent;
            sidEl.textContent = "已复制!";
            setTimeout(() => { sidEl.textContent = orig; }, 1000);
          });
        });
        sessionEl.appendChild(sidEl);
      }

      // 渲染消息为聊天气泡
      const msgsContainer = document.createElement("div");
      msgsContainer.className = "pc-session-messages";
      for (const m of session.messages || []) {
        const msgEl = document.createElement("div");
        msgEl.className = `pc-chat-msg pc-chat-${m.role}`;

        if (m.selected_text) {
          const ctxEl = document.createElement("div");
          ctxEl.className = "pc-chat-context";
          ctxEl.textContent = m.selected_text.length > 100 ? m.selected_text.slice(0, 100) + "..." : m.selected_text;
          msgEl.appendChild(ctxEl);
        }

        const contentEl = document.createElement("div");
        contentEl.className = "pc-chat-content";
        contentEl.textContent = m.content;
        msgEl.appendChild(contentEl);

        if (m.role === "assistant" && m.edits_json) {
          try {
            const edits = JSON.parse(m.edits_json);
            if (edits.length > 0) {
              const badge = document.createElement("span");
              badge.className = "pc-edit-badge";
              badge.textContent = (m.edit_success ? "\u2713" : "\u2717") + ` ${edits.length} 处修改`;
              contentEl.appendChild(badge);
            }
          } catch {}
        }

        msgsContainer.appendChild(msgEl);
      }
      sessionEl.appendChild(msgsContainer);
      body.appendChild(sessionEl);
    }

    body.scrollTop = body.scrollHeight;
  }

  // 页面加载后创建侧边栏切换按钮
  createSidebarToggle();
})();
