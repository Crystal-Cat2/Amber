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
  let currentRange = null;  // 保存页面选区 Range，暂存时用于创建高亮
  let currentChartInfo = null;
  let currentTargetContext = null;
  let currentVisualContext = null;
  let lastContentTarget = null;
  let bridgeReady = false;
  let skipNextSelectionCleanup = false;

  // 会话相关状态
  let sidebarEl = null;
  let sidebarOpen = false;
  let currentSessionId = null;
  let currentPageKey = null;
  let pendingCommentId = null;
  let pendingTimeoutId = null;
  const PENDING_TIMEOUT_MS = 120000; // 2分钟超时
  let allSessions = [];
  let batchMode = false;
  let commentQueue = [];  // [{selected_text, comment, chart_info?, rect?, markerEl?}]
  let draftEditorEl = null;
  let pcEnabled = true;
  let currentModelProvider = "claude";
  let sendPageContextEnabled = false;
  const PAGE_CONTEXT_MAX_CHARS = 6000;

  function getPageIdentity() {
    if (typeof chartUtils.buildPageIdentity === "function") {
      return chartUtils.buildPageIdentity({
        pageMeta: getPageMeta(),
        pageUrl: location.href,
        pathname: location.pathname,
      });
    }
    return location.href;
  }

  function getPageEnabledStorageKey(pageIdentity) {
    if (typeof chartUtils.buildPageEnabledStorageKey === "function") {
      return chartUtils.buildPageEnabledStorageKey(pageIdentity);
    }
    return "pc_enabled_page:" + String(pageIdentity || "");
  }

  function checkEnabledAndInit() {
    const pageIdentity = getPageIdentity();
    if (!pageIdentity) { initUI(); return; }
    const storageKey = getPageEnabledStorageKey(pageIdentity);
    const legacyHostKey = typeof chartUtils.getLegacyHostKey === "function"
      ? chartUtils.getLegacyHostKey(location.href)
      : null;
    const legacyStorageKey = legacyHostKey ? "pc_enabled:" + legacyHostKey : null;
    const defaults = { [storageKey]: null };
    if (legacyStorageKey) defaults[legacyStorageKey] = true;
    chrome.storage.local.get(defaults, (r) => {
      pcEnabled = r[storageKey];
      if (pcEnabled === null || typeof pcEnabled === "undefined") {
        pcEnabled = legacyStorageKey ? !!r[legacyStorageKey] : true;
      }
      initUI();
    });
  }

  function initUI() {
    if (pcEnabled) {
      injectPageBridge();
      markChartTargets();
      createSidebarToggle();
    }
  }

  function setEnabled(enabled) {
    pcEnabled = enabled;
    if (fabBtn) fabBtn.style.display = enabled && !sidebarOpen ? "" : "none";
    if (!enabled) {
      cleanup();
      if (sidebarOpen) toggleSidebar();
    }
  }

  // 监听 popup 的开关消息（注册在最前面，后面还有更多 handler 在同一个 listener）
  chrome.runtime.onMessage.addListener((msg, _sender, sendResponse) => {
    if (msg.type === "toggle_enabled") {
      if (msg.page_identity && msg.page_identity !== getPageIdentity()) return;
      setEnabled(msg.enabled);
    }
    if (msg.type === "settings_updated") {
      if (msg.model_provider) currentModelProvider = msg.model_provider;
      if (typeof msg.send_page_context === "boolean") {
        sendPageContextEnabled = msg.send_page_context;
        updatePageContextToggleUi();
      }
    }
    if (msg.type === "get_page_identity") {
      sendResponse({
        page_identity: getPageIdentity(),
        enabled: pcEnabled,
      });
    }
  });

  chrome.storage.local.get({ pc_model: "claude", pc_send_page_context: false }, (data) => {
    currentModelProvider = data.pc_model || "claude";
    sendPageContextEnabled = !!data.pc_send_page_context;
    updatePageContextToggleUi();
  });

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
    const pageKeyTag = document.querySelector('meta[name="amber:page-key"]');
    const creatorTag = document.querySelector('meta[name="amber:creator-session"]');
    if (scriptTag) meta.source_script = scriptTag.content;
    if (dataTag) meta.source_data = dataTag.content;
    if (pageKeyTag) meta.page_key = pageKeyTag.content;
    if (creatorTag) meta.creator_session = creatorTag.content;
    return meta;
  }

  function normalizePageUrl(url) {
    if (typeof chartUtils.normalizePageUrl === "function") {
      return chartUtils.normalizePageUrl(url || window.location.href);
    }
    return String(url || window.location.href).replace(/[?#].*/, "");
  }

  function cleanPageText(text) {
    return String(text || "")
      .replace(/\r/g, "")
      .replace(/[ \t]+\n/g, "\n")
      .replace(/\n{3,}/g, "\n\n")
      .replace(/[ \t]{2,}/g, " ")
      .trim();
  }

  function getPageContext(selectedText) {
    if (selectedText && selectedText.trim()) {
      return {
        mode: "selected_only",
        content: "",
        truncated: false,
      };
    }

    const roots = Array.from(document.querySelectorAll("main, article, [role='main']"))
      .filter((node) => node && node.innerText && node.innerText.trim());
    const bestRoot = roots.sort((a, b) => (b.innerText || "").length - (a.innerText || "").length)[0] || document.body;
    let sourceText = "";
    if (bestRoot) {
      const clone = bestRoot.cloneNode(true);
      clone.querySelectorAll(".pc-sidebar, .pc-sidebar-toggle, .pc-comment-box, .pc-dialog-overlay, .pc-highlight, .pc-highlight-badge, .pc-comment-btn, .pc-resize-handle")
        .forEach((node) => node.remove());
      sourceText = clone.innerText || clone.textContent || "";
    }
    const cleaned = cleanPageText(sourceText || bestRoot?.innerText || document.body?.innerText || "");
    const truncated = cleaned.length > PAGE_CONTEXT_MAX_CHARS;
    return {
      mode: "visible_main",
      content: truncated ? cleaned.slice(0, PAGE_CONTEXT_MAX_CHARS) : cleaned,
      truncated,
    };
  }

  function isFeishuPage() {
    return typeof chartUtils.isFeishuDocUrl === "function"
      ? chartUtils.isFeishuDocUrl(window.location.href)
      : false;
  }

  function normalizeTargetText(text, limit = 400) {
    return typeof chartUtils.normalizeText === "function"
      ? chartUtils.normalizeText(text || "", limit)
      : String(text || "").trim().slice(0, limit);
  }

  function findClosestElement(node, predicate) {
    let current = node && node.nodeType === Node.ELEMENT_NODE ? node : node?.parentElement || null;
    while (current) {
      if (predicate(current)) return current;
      current = current.parentElement || null;
    }
    return null;
  }

  function findFeishuBlock(node) {
    return findClosestElement(node, (element) => {
      if (!element || !element.innerText) return false;
      const text = normalizeTargetText(element.innerText, 800);
      if (!text) return false;
      const role = element.getAttribute?.("role") || "";
      const tag = (element.tagName || "").toLowerCase();
      const className = String(element.className || "").toLowerCase();
      return tag === "p" || tag === "li" || tag === "figure" || tag === "table"
        || role === "paragraph" || role === "listitem"
        || className.includes("paragraph") || className.includes("block");
    });
  }

  function collectSurroundingBlocks(blockEl, limit = 2) {
    if (!blockEl || !blockEl.parentElement) return [];
    const siblings = Array.from(blockEl.parentElement.children || []);
    const index = siblings.indexOf(blockEl);
    if (index < 0) return [];
    const blocks = [];
    for (let i = Math.max(0, index - limit); i <= Math.min(siblings.length - 1, index + limit); i += 1) {
      if (i === index) continue;
      const text = normalizeTargetText(siblings[i].innerText || siblings[i].textContent || "", 220);
      if (text) blocks.push(text);
    }
    return blocks;
  }

  function extractTableMatrix(tableEl) {
    const rowSelector = "tr, [role='row']";
    const cellSelector = "th, td, [role='gridcell'], [role='columnheader'], [role='rowheader']";
    const rows = Array.from(tableEl?.querySelectorAll?.(rowSelector) || []).map((row) =>
      Array.from(row.querySelectorAll(cellSelector)).map((cell) =>
        normalizeTargetText(cell.innerText || cell.textContent || "", 160)
      )
    ).filter((row) => row.some(Boolean));
    return typeof chartUtils.normalizeFeishuTable === "function"
      ? chartUtils.normalizeFeishuTable(rows)
      : rows;
  }

  function extractObjectTokens(text, element) {
    const raw = [text || "", element?.outerHTML || "", element?.getAttribute?.("href") || "", element?.getAttribute?.("src") || ""]
      .filter(Boolean)
      .join("\n");
    return typeof chartUtils.extractLarkTokens === "function"
      ? chartUtils.extractLarkTokens(raw)
      : [];
  }

  function buildFeishuTargetContext(baseNode, selectedText) {
    if (!isFeishuPage()) return null;
    const node = baseNode && baseNode.nodeType === Node.ELEMENT_NODE ? baseNode : baseNode?.parentElement || lastContentTarget;
    if (!node) return null;

    const tableEl = findClosestElement(node, (element) => {
      const tag = (element.tagName || "").toLowerCase();
      const role = element.getAttribute?.("role") || "";
      const className = String(element.className || "").toLowerCase();
      return tag === "table" || role === "table" || role === "grid" || className.includes("table");
    });
    const imageEl = !tableEl && findClosestElement(node, (element) => {
      const tag = (element.tagName || "").toLowerCase();
      const role = element.getAttribute?.("role") || "";
      const className = String(element.className || "").toLowerCase();
      return tag === "img" || role === "img" || className.includes("image");
    });
    const attachmentEl = !tableEl && !imageEl && findClosestElement(node, (element) => {
      const className = String(element.className || "").toLowerCase();
      const href = String(element.getAttribute?.("href") || "").toLowerCase();
      return className.includes("attachment") || className.includes("file-card") || href.includes("/file/");
    });
    const specialEl = !tableEl && !imageEl && !attachmentEl && findClosestElement(node, (element) => {
      const className = String(element.className || "").toLowerCase();
      const text = normalizeTargetText(element.innerText || "", 80).toLowerCase();
      return className.includes("mindmap") || className.includes("mindnote")
        || className.includes("whiteboard") || className.includes("board")
        || text.includes("思维导图") || text.includes("画板");
    });
    const embedEl = !tableEl && !imageEl && !attachmentEl && !specialEl && findClosestElement(node, (element) => {
      const tag = (element.tagName || "").toLowerCase();
      const href = String(element.getAttribute?.("href") || "").toLowerCase();
      return tag === "iframe" || tag === "object" || tag === "embed"
        || href.includes("/docx/") || href.includes("/wiki/") || href.includes("/sheets/") || href.includes("/base/");
    });

    const targetEl = tableEl || imageEl || attachmentEl || specialEl || embedEl || findFeishuBlock(node) || node;
    const targetType = typeof chartUtils.classifyFeishuTarget === "function"
      ? chartUtils.classifyFeishuTarget({
        typeHint: tableEl ? "table" : imageEl ? "image" : attachmentEl ? "attachment" : specialEl ? normalizeTargetText(specialEl.innerText || "", 40) : embedEl ? "embed" : selectedText ? "text" : "unknown",
        tagName: targetEl.tagName || "",
        className: String(targetEl.className || ""),
        href: targetEl.getAttribute?.("href") || "",
        text: normalizeTargetText(targetEl.innerText || targetEl.textContent || "", 120),
      })
      : "unknown";

    const targetText = normalizeTargetText(
      selectedText
        || targetEl.getAttribute?.("alt")
        || targetEl.getAttribute?.("title")
        || targetEl.innerText
        || targetEl.textContent
        || "",
      400,
    );
    const blockEl = findFeishuBlock(targetEl) || targetEl;
    const objectMeta = {
      tag_name: (targetEl.tagName || "").toLowerCase(),
      class_name: String(targetEl.className || "").slice(0, 200),
      title: targetEl.getAttribute?.("title") || "",
      alt: targetEl.getAttribute?.("alt") || "",
      href: targetEl.getAttribute?.("href") || "",
      src: targetEl.getAttribute?.("src") || "",
      width: targetEl.clientWidth || 0,
      height: targetEl.clientHeight || 0,
      tokens: extractObjectTokens(targetText, targetEl),
    };
    if (tableEl) {
      objectMeta.table = {
        rows: extractTableMatrix(tableEl),
        hit_text: normalizeTargetText(node.innerText || node.textContent || "", 120),
      };
    }

    return {
      page_type: "feishu_doc",
      target_type: targetType,
      target_text: targetText,
      surrounding_blocks: collectSurroundingBlocks(blockEl),
      object_meta: objectMeta,
    };
  }

  function getTargetRect(element) {
    if (!element?.getBoundingClientRect) return null;
    const rect = element.getBoundingClientRect();
    if (!rect || rect.width <= 0 || rect.height <= 0) return null;
    return {
      left: Math.max(0, rect.left),
      top: Math.max(0, rect.top),
      width: Math.max(1, rect.width),
      height: Math.max(1, rect.height),
      devicePixelRatio: window.devicePixelRatio || 1,
    };
  }

  function captureVisualContext(element) {
    const rect = getTargetRect(element);
    if (!rect) return Promise.resolve(null);
    return new Promise((resolve) => {
      chrome.runtime.sendMessage({ type: "capture_target_visual", rect }, async (response) => {
        if (chrome.runtime.lastError || !response?.ok || !response?.data_url) {
          resolve(null);
          return;
        }
        try {
          const image = new Image();
          image.onload = () => {
            const canvas = document.createElement("canvas");
            const maxWidth = 800;
            const scale = rect.width > maxWidth ? maxWidth / rect.width : 1;
            canvas.width = Math.max(1, Math.round(rect.width * scale));
            canvas.height = Math.max(1, Math.round(rect.height * scale));
            const ctx = canvas.getContext("2d");
            if (!ctx) {
              resolve(null);
              return;
            }
            ctx.drawImage(
              image,
              rect.left * rect.devicePixelRatio,
              rect.top * rect.devicePixelRatio,
              rect.width * rect.devicePixelRatio,
              rect.height * rect.devicePixelRatio,
              0,
              0,
              canvas.width,
              canvas.height,
            );
            resolve({
              format: "jpeg",
              width: canvas.width,
              height: canvas.height,
              data_url: canvas.toDataURL("image/jpeg", 0.82),
            });
          };
          image.onerror = () => resolve(null);
          image.src = response.data_url;
        } catch {
          resolve(null);
        }
      });
    });
  }

  async function buildFeishuContexts(selectedText, baseNode) {
    const targetContext = buildFeishuTargetContext(baseNode, selectedText);
    let visualContext = null;
    if (targetContext && targetContext.target_type !== "text" && targetContext.target_type !== "unknown") {
      const targetEl = baseNode && baseNode.nodeType === Node.ELEMENT_NODE ? baseNode : baseNode?.parentElement || lastContentTarget;
      visualContext = await captureVisualContext(targetEl);
    }
    currentTargetContext = targetContext;
    currentVisualContext = visualContext;
    return { targetContext, visualContext };
  }

  function shouldSendPageContext(selectedText) {
    if (typeof chartUtils.shouldSendPageContext === "function") {
      return chartUtils.shouldSendPageContext({ selectedText, sendPageContext: sendPageContextEnabled });
    }
    return !selectedText && !!sendPageContextEnabled;
  }

  function getActiveSessionModelProvider() {
    const currentSession = allSessions.find((session) => session.id === currentSessionId);
    return currentSession?.model_provider || currentModelProvider || "claude";
  }

  function upsertSession(session, messages) {
    if (!session?.id) return null;
    const existing = allSessions.find((item) => item.id === session.id) || {};
    const merged = {
      ...existing,
      ...session,
      messages: messages || existing.messages || [],
    };
    allSessions = [merged, ...allSessions.filter((item) => item.id !== session.id)];
    return merged;
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

    // 保存当前页面选区，暂存时用于创建高亮
    const sel = window.getSelection();
    currentRange = (sel && sel.rangeCount > 0 && !isChart) ? sel.getRangeAt(0).cloneRange() : null;

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

    const queueBtn = document.createElement("button");
    queueBtn.className = "pc-btn-queue";
    queueBtn.textContent = "暂存";
    queueBtn.addEventListener("click", () => {
      const comment = textarea.value.trim();
      if (!comment) return;
      queueComment(currentSelection, comment, currentChartInfo, rect);
      cleanup();
    });

    const submitBtn = document.createElement("button");
    submitBtn.className = "pc-btn-submit";
    submitBtn.textContent = "发送";
    submitBtn.addEventListener("click", () => {
      const comment = textarea.value.trim();
      if (!comment) return;
      submitBtn.disabled = true;
      submitBtn.textContent = "发送中...";
      submitComment(comment, rect);
    });

    textarea.addEventListener("keydown", (e) => {
      if (e.key === "Enter" && !e.ctrlKey && !e.metaKey && !e.shiftKey) {
        e.preventDefault();
        submitBtn.click();
      }
      if (e.key === "s" && (e.ctrlKey || e.metaKey)) {
        e.preventDefault();
        queueBtn.click();
      }
    });

    actions.appendChild(cancelBtn);
    actions.appendChild(queueBtn);
    actions.appendChild(submitBtn);
    commentBox.appendChild(selectedDiv);
    commentBox.appendChild(textarea);
    commentBox.appendChild(actions);
    document.body.appendChild(commentBox);
    textarea.focus();
  }

  // 提交评论
  async function submitComment(comment) {
    const modelProvider = getActiveSessionModelProvider();
    const feishuContexts = isFeishuPage()
      ? await buildFeishuContexts(currentSelection, currentRange?.startContainer || lastContentTarget)
      : { targetContext: null, visualContext: null };
    const data = {
      selected_text: currentSelection,
      comment: comment,
      page_url: window.location.href,
      page_title: document.title,
      page_meta: getPageMeta(),
      model_provider: modelProvider,
    };
    if (shouldSendPageContext(currentSelection)) {
      data.page_context = getPageContext(currentSelection);
    }
    if (currentChartInfo) data.chart_info = currentChartInfo;
    if (feishuContexts.targetContext) data.target_context = feishuContexts.targetContext;
    if (feishuContexts.visualContext) data.visual_context = feishuContexts.visualContext;
    if (currentSessionId) data.session_id = currentSessionId;

    chrome.runtime.sendMessage({ type: "submit_comment", page_identity: getPageIdentity(), data }, (response) => {
      cleanup();
      if (chrome.runtime.lastError || !response?.ok) {
        openSidebarChat();
        appendChatMessage("system", response?.error || "发送失败，服务器未连接");
        return;
      }
      pendingCommentId = response.id;
      startPendingTimeout();
      openSidebarChat();
      const label = currentChartInfo ? getChartLabel() : (currentSelection ? currentSelection.slice(0, 80) : "");
      appendChatMessage("user", comment, label);
      showTypingIndicator("正在处理...", "processing");
      disableChatInput();
    });
  }

  function escapeHtml(s) {
    const d = document.createElement("div");
    d.textContent = s;
    return d.innerHTML;
  }

  function renderMessageContent(contentEl, text, role) {
    if (!contentEl) return;
    if (role === "assistant" && typeof chartUtils.renderMarkdownToHtml === "function") {
      contentEl.innerHTML = chartUtils.renderMarkdownToHtml(text || "");
      contentEl.querySelectorAll("a[href]").forEach((anchor) => {
        anchor.target = "_blank";
        anchor.rel = "noopener noreferrer";
      });
      return;
    }
    contentEl.textContent = text;
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
    renderMessageContent(contentEl, text, role);
    msgEl.appendChild(contentEl);
    body.appendChild(msgEl);
    body.scrollTop = body.scrollHeight;
  }

  function getPendingStatusMeta(status) {
    if (typeof chartUtils.getPendingStatusMeta === "function") {
      return chartUtils.getPendingStatusMeta(status);
    }
    return { label: "处理中", tone: "processing" };
  }

  function renderPendingText(contentEl, text) {
    if (!contentEl) return;
    if (typeof chartUtils.renderMarkdownToHtml === "function") {
      contentEl.innerHTML = chartUtils.renderMarkdownToHtml(text || "");
      contentEl.querySelectorAll("a[href]").forEach((anchor) => {
        anchor.target = "_blank";
        anchor.rel = "noopener noreferrer";
      });
      return;
    }
    contentEl.textContent = text || "";
  }

  function upsertTypingIndicator(status, text) {
    const body = sidebarEl?.querySelector(".pc-sidebar-body");
    if (!body) return;

    let el = body.querySelector(".pc-chat-typing");
    if (!el) {
      el = document.createElement("div");
      el.className = "pc-chat-msg pc-chat-assistant pc-chat-typing";
      el.innerHTML = ''
        + '<div class="pc-typing-header">'
        + '  <span class="pc-typing-status-badge"></span>'
        + '  <button class="pc-typing-cancel" title="取消请求">取消</button>'
        + '</div>'
        + '<div class="pc-typing-text"></div>'
        + '<div class="pc-chat-typing-shimmer"></div>';
      el.querySelector(".pc-typing-cancel").addEventListener("click", cancelPendingRequest);
      body.appendChild(el);
    }

    const meta = getPendingStatusMeta(status);
    el.dataset.pendingStatus = meta.tone;

    const badge = el.querySelector(".pc-typing-status-badge");
    if (badge) {
      badge.className = `pc-typing-status-badge pc-typing-status-${meta.tone}`;
      badge.textContent = meta.label;
    }

    renderPendingText(el.querySelector(".pc-typing-text"), text || meta.label);

    body.scrollTop = body.scrollHeight;
  }

  function showTypingIndicator(text, status = "processing") {
    upsertTypingIndicator(status, text);
  }

  function updateTypingIndicator(text, status = "processing") {
    upsertTypingIndicator(status, text);
  }

  function removeTypingIndicator() {
    sidebarEl?.querySelector(".pc-chat-typing")?.remove();
  }

  function removeInteractionCard() {
    sidebarEl?.querySelector(".pc-interaction")?.remove();
  }

  function renderInteraction(data) {
    const body = sidebarEl?.querySelector(".pc-sidebar-body");
    if (!body) return;

    // 交互阶段：服务端在等用户回复，停止超时倒计时
    clearPendingTimeout();
    showTypingIndicator(
      (typeof chartUtils.getInteractionPendingHint === "function")
        ? chartUtils.getInteractionPendingHint()
        : "AI 需要你的确认，请选择一个选项或直接输入回复。",
      "interaction",
    );
    removeInteractionCard();

    if (data.interaction_type === "ask_user") {
      const container = document.createElement("div");
      container.className = "pc-chat-msg pc-chat-assistant pc-interaction";
      container.dataset.interactionId = data.interaction_id;

      const intro = document.createElement("div");
      intro.className = "pc-interaction-intro";
      intro.textContent = (typeof chartUtils.getInteractionPendingHint === "function")
        ? chartUtils.getInteractionPendingHint()
        : "AI 需要你的确认，请选择一个选项或直接输入回复。";
      container.appendChild(intro);

      const questions = data.questions || [];
      for (const q of questions) {
        const qEl = document.createElement("div");
        qEl.className = "pc-interaction-question";

        const header = document.createElement("div");
        header.className = "pc-interaction-header";
        // 用 markdown 渲染，支持链接显示
        if (typeof chartUtils.renderMarkdownToHtml === "function") {
          header.innerHTML = chartUtils.renderMarkdownToHtml(q.question || "");
          header.querySelectorAll("a[href]").forEach(a => { a.target = "_blank"; a.rel = "noopener noreferrer"; });
        } else {
          header.textContent = q.question || "";
        }
        qEl.appendChild(header);

        const optionsEl = document.createElement("div");
        optionsEl.className = "pc-interaction-options";

        const options = q.options || [];
        for (const opt of options) {
          const btn = document.createElement("button");
          btn.className = "pc-interaction-option";
          // description 也用 markdown 渲染
          let descHtml = "";
          if (opt.description) {
            if (typeof chartUtils.renderMarkdownToHtml === "function") {
              descHtml = '<span class="pc-option-desc">' + chartUtils.renderMarkdownToHtml(opt.description) + '</span>';
            } else {
              descHtml = '<span class="pc-option-desc">' + escapeHtml(opt.description) + '</span>';
            }
          }
          btn.innerHTML =
            '<span class="pc-option-label">' + escapeHtml(opt.label || "") + '</span>' + descHtml;
          // 确保 description 中的链接可点击
          btn.querySelectorAll("a[href]").forEach(a => { a.target = "_blank"; a.rel = "noopener noreferrer"; });
          btn.addEventListener("click", () => {
            // 高亮选中
            optionsEl.querySelectorAll(".pc-interaction-option").forEach(b => b.classList.remove("pc-option-selected"));
            btn.classList.add("pc-option-selected");

            // 发送回复
            chrome.runtime.sendMessage({
              type: "submit_interaction_response",
              page_identity: getPageIdentity(),
              interaction_id: data.interaction_id,
              response: { answers: { [q.question]: opt.label } },
            }, (response) => {
              if (chrome.runtime.lastError || !response?.ok) {
                // 恢复按钮，让用户重试
                optionsEl.querySelectorAll(".pc-interaction-option").forEach(b => { b.disabled = false; });
                removeTypingIndicator();
                appendChatMessage("system", response?.error || "发送回复失败，请重试");
                enableChatInput();
                return;
              }
            });

            // 禁用所有按钮
            optionsEl.querySelectorAll(".pc-interaction-option").forEach(b => { b.disabled = true; });

            // 恢复 typing，重新启动超时
            disableChatInput();
            startPendingTimeout();
            showTypingIndicator("正在继续处理...", "processing");
          });
          optionsEl.appendChild(btn);
        }
        qEl.appendChild(optionsEl);
        container.appendChild(qEl);
      }
      body.appendChild(container);
      body.scrollTop = body.scrollHeight;

      // 交互阶段启用输入框，允许用户打字发送自定义回复
      enableChatInput();
    }
  }

  function updateCliSessionId(cliSessionId, modelProvider) {
    if (!sidebarEl || !cliSessionId) return;
    let infoEl = sidebarEl.querySelector(".pc-cli-session-info");
    if (!infoEl) {
      const header = sidebarEl.querySelector(".pc-sidebar-header");
      infoEl = document.createElement("div");
      infoEl.className = "pc-cli-session-info";
      infoEl.addEventListener("click", () => {
        const cmd = (typeof chartUtils.buildResumeCommand === "function")
          ? chartUtils.buildResumeCommand(infoEl.dataset.provider || currentModelProvider, infoEl.dataset.fullId || "")
          : "claude --resume " + (infoEl.dataset.fullId || "");
        navigator.clipboard.writeText(cmd).then(() => {
          const orig = infoEl.textContent;
          infoEl.textContent = "已复制命令!";
          setTimeout(() => { infoEl.textContent = orig; }, 1000);
        });
      });
      header.parentNode.insertBefore(infoEl, header.nextSibling);
    }
    infoEl.dataset.fullId = cliSessionId;
    infoEl.dataset.provider = modelProvider || "claude";
    const cmd = (typeof chartUtils.buildResumeCommand === "function")
      ? chartUtils.buildResumeCommand(infoEl.dataset.provider, cliSessionId)
      : "claude --resume " + cliSessionId;
    infoEl.textContent = cmd;
    infoEl.title = "点击复制命令";
  }

  function clearCliSessionId() {
    sidebarEl?.querySelector(".pc-cli-session-info")?.remove();
  }

  // 监听 background 消息
  chrome.runtime.onMessage.addListener((msg) => {
    if (msg.type === "connection_status" && msg.page_identity && msg.page_identity !== getPageIdentity()) {
      return;
    }
    if (msg.type === "status" && msg.id) {
      if (msg.id === pendingCommentId) {
        startPendingTimeout(); // 收到状态说明服务端仍在工作，重置超时
        if (msg.status === "interaction") {
          // 交互事件：渲染选项卡片
          try {
            const interactionData = JSON.parse(msg.message);
            renderInteraction(interactionData);
          } catch (e) {
            console.warn("解析交互数据失败", e);
          }
        } else {
          updateTypingIndicator(msg.message, msg.status);
        }
      }
    }
    if (msg.type === "result" && msg.id) {
      // 直接匹配或轮询匹配
      const isMyResult = msg.id === pendingCommentId || msg.poll_for === pendingCommentId;
      if (isMyResult && pendingCommentId) {
        removeTypingIndicator();
        removeInteractionCard();
        const isError = msg.response?.startsWith("处理失败") || msg.response?.startsWith("部分编辑失败");
        appendChatMessage(isError ? "system" : "assistant", msg.response);
        pendingCommentId = null;
        clearPendingTimeout();
        enableChatInput();
      }

      let currentSession = null;
      if (msg.session) {
        currentSession = upsertSession(msg.session);
        currentSessionId = msg.session.id;
        updateSessionSelector();
        renderSessionDropdown();
      } else if (msg.session_id) {
        currentSessionId = msg.session_id;
      }
      if (msg.page_key) {
        currentPageKey = msg.page_key;
        chrome.storage.local.set({ ["pk:" + normalizePageUrl()]: msg.page_key });
      }
      if (currentSession?.cli_session_id || msg.cli_session_id) {
        updateCliSessionId(currentSession?.cli_session_id || msg.cli_session_id, currentSession?.model_provider);
      } else if (currentSession) {
        clearCliSessionId();
      }

      // 会话列表可能变了（新建、标题更新），刷新选择器
      if (msg.session_id && currentPageKey) {
        requestHistory();
      }

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
      if (msg.session) {
        upsertSession(msg.session, msg.session.messages);
        currentSessionId = msg.session.id;
        updateSessionSelector();
        renderSessionDropdown();
        if (msg.session.cli_session_id) updateCliSessionId(msg.session.cli_session_id, msg.session.model_provider);
        else clearCliSessionId();
      }
      // 刷新会话列表
      requestHistory();
    }
    if (msg.type === "session_switched") {
      const session = msg.session ? upsertSession(msg.session, msg.messages || []) : null;
      currentSessionId = msg.session?.id;
      renderSessionMessages(session?.messages || msg.messages || []);
      updateSessionSelector();
      renderSessionDropdown();
      if (msg.session?.cli_session_id) updateCliSessionId(msg.session.cli_session_id, msg.session.model_provider);
      else clearCliSessionId();
    }

    // 批量评论进度
    if (msg.type === "batch_progress" && msg.id === pendingCommentId) {
      const progressMessage = typeof chartUtils.formatBatchProgressMessage === "function"
        ? chartUtils.formatBatchProgressMessage({
          current: msg.current,
          total: msg.total,
          currentComment: msg.current_comment,
        })
        : `正在处理 ${msg.current}/${msg.total}: ${msg.current_comment || ""}`;
      updateTypingIndicator(progressMessage, "processing");
      updateBatchItemStatus(msg.current - 1, "processing");
      if (msg.current > 1) updateBatchItemStatus(msg.current - 2, "done");
    }

    // 批量评论完成
    if (msg.type === "batch_result" && msg.id === pendingCommentId) {
      removeTypingIndicator();
      removeInteractionCard();
      pendingCommentId = null;
      clearPendingTimeout();
      enableChatInput();
      // 标记所有项为完成
      const results = msg.results || [];
      for (let i = 0; i < results.length; i++) {
        updateBatchItemStatus(i, results[i].action === "none" && results[i].response?.startsWith("处理失败") ? "failed" : "done");
      }
      // 显示汇总
      const successCount = results.filter(r => r.action !== "none" || !r.response?.startsWith("处理失败")).length;
      appendChatMessage("system", `批量处理完成: ${successCount}/${results.length} 条成功`);

      let currentSession = null;
      if (msg.session) {
        currentSession = upsertSession(msg.session);
        currentSessionId = msg.session.id;
        updateSessionSelector();
        renderSessionDropdown();
      } else if (msg.session_id) {
        currentSessionId = msg.session_id;
      }
      if (msg.page_key) {
        currentPageKey = msg.page_key;
        chrome.storage.local.set({ ["pk:" + normalizePageUrl()]: msg.page_key });
      }
      if (currentSession?.cli_session_id || msg.cli_session_id) {
        updateCliSessionId(currentSession?.cli_session_id || msg.cli_session_id, currentSession?.model_provider);
      } else if (currentSession) {
        clearCliSessionId();
      }
      if (msg.action === "reload") {
        appendChatMessage("system", "页面即将刷新...");
        setTimeout(() => window.location.reload(), 1500);
      }
    }

    // 服务端错误
    if (msg.type === "error" && msg.id) {
      if (msg.id === pendingCommentId) {
        removeTypingIndicator();
        removeInteractionCard();
        appendChatMessage("system", msg.message || "服务器处理出错");
        pendingCommentId = null;
        clearPendingTimeout();
        enableChatInput();
      }
    }
  });

  function pollForResult() {
    if (!pendingCommentId) return;
    chrome.runtime.sendMessage({
      type: "poll_result",
      page_identity: getPageIdentity(),
      comment_id: pendingCommentId,
    });
  }

  // 监听文字选中
  document.addEventListener("mouseup", (e) => {
    if (!pcEnabled) return;
    if (e.target.closest(".pc-comment-btn, .pc-comment-box, .pc-sidebar")) return;
    lastContentTarget = e.target;

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
      if (isFeishuPage()) {
        const anchorNode = sel?.anchorNode || e.target;
        currentTargetContext = buildFeishuTargetContext(anchorNode, text);
        currentVisualContext = null;
      }
      const rect = getSelectionRect();
      if (rect) showCommentButton(rect, false);
    }, 10);
  });

  // 监听图表点击
  document.addEventListener("click", (e) => {
    if (!pcEnabled) return;
    if (e.target.closest(".pc-comment-btn, .pc-comment-box, .pc-sidebar")) return;
    lastContentTarget = e.target;

    const chartEl = chartUtils.detectChartElement?.(e.target) || null;
    if (!chartEl || !chartUtils.isInteractiveChartElement?.(chartEl)) {
      if (isFeishuPage()) {
        currentTargetContext = buildFeishuTargetContext(e.target, "");
        currentVisualContext = null;
      }
      return;
    }

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
    if (!e.target.closest(".pc-comment-btn, .pc-comment-box, .pc-sidebar")) {
      lastContentTarget = e.target;
    }
    if (commentBox && !commentBox.contains(e.target) && e.target !== commentBtn) {
      cleanup();
    }
    if (draftEditorEl && !draftEditorEl.contains(e.target) && !e.target.closest(".pc-highlight, .pc-highlight-badge")) {
      removeDraftEditor();
    }
  });

  // ========== 侧边栏：聊天界面 ==========

  function buildPageKey() {
    return getPageIdentity();
  }

  function updatePageContextToggleUi() {
    const toggle = sidebarEl?.querySelector(".pc-page-context-toggle");
    const hint = sidebarEl?.querySelector(".pc-page-context-hint");
    if (toggle) toggle.checked = !!sendPageContextEnabled;
    if (hint) hint.textContent = sendPageContextEnabled ? "发送时附带页面摘要" : "发送时不附带页面摘要";
  }

  function buildPageKeyAsync(callback) {
    callback(getPageIdentity());
  }

  function createSidebar() {
    if (sidebarEl) return sidebarEl;
    sidebarEl = document.createElement("div");
    sidebarEl.className = "pc-sidebar";
    sidebarEl.innerHTML = `
      <div class="pc-sidebar-header">
        <span class="pc-sidebar-title">评论</span>
        <div class="pc-sidebar-actions">
          <button class="pc-sidebar-new-btn" title="新建会话">+ 新会话</button>
          <button class="pc-sidebar-close" title="关闭">&times;</button>
        </div>
      </div>
      <div class="pc-session-bar">
        <div class="pc-session-current" tabindex="0">
          <span class="pc-session-current-name">选择会话...</span>
          <span class="pc-session-current-arrow"><svg width="12" height="12" viewBox="0 0 12 12" fill="none" stroke="currentColor" stroke-width="1.5"><path d="M3 4.5L6 7.5L9 4.5"/></svg></span>
        </div>
        <div class="pc-session-dropdown"></div>
      </div>
      <div class="pc-sidebar-body"></div>
      <div class="pc-sidebar-input">
        <label class="pc-page-context-row">
          <input type="checkbox" class="pc-page-context-toggle">
          <span class="pc-page-context-hint">发送时不附带页面摘要</span>
        </label>
        <div class="pc-sidebar-input-main">
          <textarea class="pc-chat-input" placeholder="输入评论或修改指令... (Enter 发送)" rows="2"></textarea>
          <button class="pc-chat-send" title="发送">发送</button>
        </div>
      </div>
    `;
    document.body.appendChild(sidebarEl);

    // 拖拽分隔条
    const resizeHandle = document.createElement("div");
    resizeHandle.className = "pc-resize-handle";
    document.body.appendChild(resizeHandle);
    initResize(resizeHandle, sidebarEl);

    sidebarEl.querySelector(".pc-sidebar-close").addEventListener("click", toggleSidebar);
    sidebarEl.querySelector(".pc-sidebar-new-btn").addEventListener("click", showNewSessionDialog);

    // 会话选择器下拉
    const sessionBar = sidebarEl.querySelector(".pc-session-bar");
    const sessionCurrent = sidebarEl.querySelector(".pc-session-current");
    sessionCurrent.addEventListener("click", () => {
      sessionBar.classList.toggle("pc-dropdown-open");
    });
    document.addEventListener("click", (e) => {
      if (!sessionBar.contains(e.target)) {
        sessionBar.classList.remove("pc-dropdown-open");
      }
    });

    const input = sidebarEl.querySelector(".pc-chat-input");
    const sendBtn = sidebarEl.querySelector(".pc-chat-send");
    const pageContextToggle = sidebarEl.querySelector(".pc-page-context-toggle");
    pageContextToggle.addEventListener("change", () => {
      sendPageContextEnabled = pageContextToggle.checked;
      chrome.storage.local.set({ pc_send_page_context: sendPageContextEnabled });
      updatePageContextToggleUi();
    });
    updatePageContextToggleUi();
    sendBtn.addEventListener("click", () => {
      const text = input.value.trim();
      if (text) sendChatMessage(input);
      else if (commentQueue.length > 0) sendQueuedComments();
    });
    input.addEventListener("keydown", (e) => {
      if (e.key === "Enter" && !e.ctrlKey && !e.metaKey && !e.shiftKey) {
        e.preventDefault();
        const text = input.value.trim();
        if (text) sendChatMessage(input);
        else if (commentQueue.length > 0) sendQueuedComments();
      }
    });

    return sidebarEl;
  }

  function initResize(handle, panel) {
    let startX, startWidth;
    const minWidth = 280, maxRatio = 0.6;

    function updateLayout(width) {
      panel.style.width = width + "px";
      handle.style.right = width + "px";
      document.documentElement.style.marginRight = width + "px";
    }

    handle.addEventListener("mousedown", (e) => {
      e.preventDefault();
      startX = e.clientX;
      startWidth = panel.offsetWidth;
      handle.classList.add("pc-resizing");
      document.addEventListener("mousemove", onMouseMove);
      document.addEventListener("mouseup", onMouseUp);
    });

    function onMouseMove(e) {
      const dx = startX - e.clientX;
      const maxWidth = window.innerWidth * maxRatio;
      const newWidth = Math.max(minWidth, Math.min(maxWidth, startWidth + dx));
      updateLayout(newWidth);
    }

    function onMouseUp() {
      handle.classList.remove("pc-resizing");
      document.removeEventListener("mousemove", onMouseMove);
      document.removeEventListener("mouseup", onMouseUp);
    }
  }

  async function sendChatMessage(input) {
    const text = input.value.trim();
    if (!text) return;

    // 交互阶段：将用户输入作为交互回复发送
    const interactionCard = sidebarEl?.querySelector(".pc-interaction[data-interaction-id]");
    if (pendingCommentId && interactionCard) {
      const interactionId = interactionCard.dataset.interactionId;
      if (interactionId) {
        const questionText = interactionCard.querySelector(".pc-interaction-header")?.textContent || "";
        input.value = "";
        appendChatMessage("user", text);
        disableChatInput();
        // 禁用交互卡片的按钮
        interactionCard.querySelectorAll(".pc-interaction-option").forEach(b => { b.disabled = true; });
        interactionCard.removeAttribute("data-interaction-id"); // 防止重复发送
        chrome.runtime.sendMessage({
          type: "submit_interaction_response",
          page_identity: getPageIdentity(),
          interaction_id: interactionId,
          response: { answers: { [questionText]: text } },
        }, (response) => {
          if (chrome.runtime.lastError || !response?.ok) {
            appendChatMessage("system", response?.error || "发送回复失败，请重试");
            enableChatInput();
            return;
          }
        });
        startPendingTimeout();
        showTypingIndicator("正在继续处理...", "processing");
        return;
      }
    }

    if (pendingCommentId) return; // 非交互阶段仍然阻止

    const modelProvider = getActiveSessionModelProvider();
    const feishuContexts = isFeishuPage()
      ? await buildFeishuContexts("", lastContentTarget)
      : { targetContext: null, visualContext: null };
    const data = {
      selected_text: "",
      comment: text,
      page_url: window.location.href,
      page_title: document.title,
      page_meta: getPageMeta(),
      model_provider: modelProvider,
    };
    if (shouldSendPageContext("")) {
      data.page_context = getPageContext("");
    }
    if (feishuContexts.targetContext) data.target_context = feishuContexts.targetContext;
    if (feishuContexts.visualContext) data.visual_context = feishuContexts.visualContext;
    if (currentSessionId) data.session_id = currentSessionId;

    input.value = "";
    disableChatInput();

    chrome.runtime.sendMessage({ type: "submit_comment", page_identity: getPageIdentity(), data }, (response) => {
      if (chrome.runtime.lastError || !response?.ok) {
        appendChatMessage("system", response?.error || "发送失败，服务器未连接");
        enableChatInput();
        return;
      }
      pendingCommentId = response.id;
      startPendingTimeout();
      appendChatMessage("user", text);
      showTypingIndicator("正在处理...", "processing");
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

  function startPendingTimeout() {
    clearPendingTimeout();
    pendingTimeoutId = setTimeout(() => {
      if (!pendingCommentId) return;
      removeTypingIndicator();
      appendChatMessage("system", "请求超时，请重试");
      pendingCommentId = null;
      enableChatInput();
    }, PENDING_TIMEOUT_MS);
  }

  function clearPendingTimeout() {
    if (pendingTimeoutId) {
      clearTimeout(pendingTimeoutId);
      pendingTimeoutId = null;
    }
  }

  function cancelPendingRequest() {
    if (!pendingCommentId) return;
    removeTypingIndicator();
    clearPendingTimeout();
    appendChatMessage("system", "已取消请求");
    pendingCommentId = null;
    enableChatInput();
  }

  function clearChatBody() {
    const body = sidebarEl?.querySelector(".pc-sidebar-body");
    if (body) body.innerHTML = "";
  }

  function updateSendButton() {
    const btn = sidebarEl?.querySelector(".pc-chat-send");
    if (!btn) return;
    if (commentQueue.length > 0) {
      btn.textContent = `发送队列 (${commentQueue.length})`;
      btn.classList.add("pc-send-queue-mode");
    } else {
      btn.textContent = "发送";
      btn.classList.remove("pc-send-queue-mode");
    }
  }

  let fabBtn = null;

  function createSidebarToggle() {
    fabBtn = document.createElement("button");
    fabBtn.className = "pc-sidebar-toggle";
    fabBtn.innerHTML = '<svg width="22" height="22" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M21 15a2 2 0 0 1-2 2H7l-4 4V5a2 2 0 0 1 2-2h14a2 2 0 0 1 2 2z"/></svg>';
    fabBtn.title = "评论";
    fabBtn.addEventListener("click", toggleSidebar);
    document.body.appendChild(fabBtn);
    return fabBtn;
  }

  function openSidebarChat() {
    if (!sidebarOpen) {
      sidebarOpen = true;
      const sidebar = createSidebar();
      sidebar.classList.add("pc-sidebar-open");
      if (fabBtn) fabBtn.style.display = "none";
      const w = sidebar.offsetWidth || 400;
      document.documentElement.style.marginRight = w + "px";
      const handle = document.querySelector(".pc-resize-handle");
      if (handle) { handle.style.right = w + "px"; handle.style.display = "block"; }
    }
    if (!currentPageKey) {
      buildPageKeyAsync((key) => {
        currentPageKey = key;
        requestHistory();
      });
    } else if (!currentSessionId) {
      requestHistory();
    }
  }

  function toggleSidebar() {
    sidebarOpen = !sidebarOpen;
    const sidebar = createSidebar();
    sidebar.classList.toggle("pc-sidebar-open", sidebarOpen);
    if (fabBtn) fabBtn.style.display = sidebarOpen ? "none" : "";
    const handle = document.querySelector(".pc-resize-handle");
    if (sidebarOpen) {
      const w = sidebar.offsetWidth || 400;
      document.documentElement.style.marginRight = w + "px";
      if (handle) { handle.style.right = w + "px"; handle.style.display = "block"; }
      buildPageKeyAsync((key) => {
        currentPageKey = key;
        requestHistory();
      });
    } else {
      document.documentElement.style.marginRight = "";
      if (handle) handle.style.display = "none";
    }
  }

  function requestHistory() {
    if (!currentPageKey) return;
    chrome.runtime.sendMessage({ type: "get_history", page_identity: getPageIdentity(), page_key: currentPageKey });
  }

  function showNewSessionDialog() {
    if (!sidebarEl) return;
    // 移除已有对话框
    sidebarEl.querySelector(".pc-dialog-overlay")?.remove();

    const meta = getPageMeta();
    const hasCreator = !!meta.creator_session;

    const overlay = document.createElement("div");
    overlay.className = "pc-dialog-overlay";
    overlay.innerHTML = `
      <div class="pc-dialog">
        <div class="pc-dialog-title">新会话</div>
        <div class="pc-dialog-option" data-value="normal">
          <input type="radio" name="pc-new-type" value="normal" checked>
          <div>
            <div class="pc-dialog-option-label">新建空白会话</div>
            <div class="pc-dialog-option-desc">从零开始一个新的对话</div>
          </div>
        </div>
        <div class="pc-dialog-option" data-value="linked">
          <input type="radio" name="pc-new-type" value="linked">
          <div>
            <div class="pc-dialog-option-label">链接 Claude Code 会话</div>
            <div class="pc-dialog-option-desc">续用已有 CLI 会话上下文</div>
          </div>
        </div>
        <input class="pc-dialog-input" placeholder="输入 CLI Session ID..." disabled
               value="${hasCreator ? escapeHtml(meta.creator_session) : ""}">
        <div class="pc-dialog-actions">
          <button class="pc-btn-cancel">取消</button>
          <button class="pc-btn-submit">创建</button>
        </div>
      </div>
    `;

    const radios = overlay.querySelectorAll('input[name="pc-new-type"]');
    const cliInput = overlay.querySelector(".pc-dialog-input");
    radios.forEach(r => r.addEventListener("change", () => {
      cliInput.disabled = r.value !== "linked" || !r.checked;
      if (r.value === "linked" && r.checked) cliInput.focus();
    }));
    overlay.querySelectorAll(".pc-dialog-option").forEach(opt => {
      opt.addEventListener("click", () => {
        const radio = opt.querySelector("input");
        radio.checked = true;
        radio.dispatchEvent(new Event("change"));
      });
    });

    if (hasCreator) {
      const linkedRadio = overlay.querySelector('input[value="linked"]');
      linkedRadio.checked = true;
      cliInput.disabled = false;
    }

    overlay.querySelector(".pc-btn-cancel").addEventListener("click", () => overlay.remove());
    overlay.addEventListener("click", (e) => { if (e.target === overlay) overlay.remove(); });

    overlay.querySelector(".pc-btn-submit").addEventListener("click", () => {
      const type = overlay.querySelector('input[name="pc-new-type"]:checked')?.value || "normal";
      const cliSid = cliInput.value.trim();
      const msg = {
        type: "new_session",
        page_identity: getPageIdentity(),
        page_key: currentPageKey,
        page_url: location.href,
        model_provider: currentModelProvider,
      };
      if (type === "linked" && cliSid) {
        msg.cli_session_id = cliSid;
        msg.session_type = "linked";
      }
      chrome.runtime.sendMessage(msg);
      overlay.remove();
    });

    sidebarEl.appendChild(overlay);
  }

  function switchSession(sessionId) {
    if (sessionId === currentSessionId) return;
    currentSessionId = sessionId;
    chrome.runtime.sendMessage({ type: "switch_session", page_identity: getPageIdentity(), session_id: sessionId });
    // 更新选择器显示
    updateSessionSelector();
    const nextSession = allSessions.find(s => s.id === sessionId);
    if (nextSession?.cli_session_id) updateCliSessionId(nextSession.cli_session_id, nextSession.model_provider);
    else clearCliSessionId();
  }

  function requestForkSession(sessionId) {
    if (!currentPageKey) return;
    chrome.runtime.sendMessage({
      type: "fork_session",
      page_identity: getPageIdentity(),
      parent_session_id: sessionId,
      page_key: currentPageKey,
      page_url: location.href,
    });
  }

  function updateSessionSelector() {
    const nameEl = sidebarEl?.querySelector(".pc-session-current-name");
    if (!nameEl) return;
    const current = allSessions.find(s => s.id === currentSessionId);
    if (current) {
      const prefix = current.session_type === "forked" ? "↳ " : current.session_type === "linked" ? "🔗 " : "";
      nameEl.textContent = prefix + (current.title || "未命名会话");
    } else {
      nameEl.textContent = allSessions.length > 0 ? "选择会话..." : "暂无会话";
    }
  }

  function renderSessionDropdown() {
    const dropdown = sidebarEl?.querySelector(".pc-session-dropdown");
    if (!dropdown) return;
    dropdown.innerHTML = "";
    if (allSessions.length === 0) {
      dropdown.innerHTML = '<div style="padding:12px 16px;color:#999;font-size:12px;text-align:center">暂无会话</div>';
      return;
    }
    for (const session of allSessions) {
      const item = document.createElement("div");
      item.className = "pc-session-item" + (session.id === currentSessionId ? " pc-session-item-active" : "");

      const dot = document.createElement("span");
      dot.className = "pc-session-item-dot";

      const info = document.createElement("div");
      info.className = "pc-session-item-info";

      let nameHtml = "";
      if (session.session_type === "forked") {
        nameHtml += '<span class="pc-session-item-type pc-session-type-forked">Fork</span>';
      } else if (session.session_type === "linked") {
        nameHtml += '<span class="pc-session-item-type pc-session-type-linked">Linked</span>';
      }
      nameHtml += escapeHtml(session.title || "未命名会话");

      const nameDiv = document.createElement("div");
      nameDiv.className = "pc-session-item-name";
      nameDiv.innerHTML = nameHtml;

      const metaDiv = document.createElement("div");
      metaDiv.className = "pc-session-item-meta";
      metaDiv.textContent = session.created_at ? new Date(session.created_at).toLocaleString("zh-CN") : "";

      info.appendChild(nameDiv);
      info.appendChild(metaDiv);

      const forkBtn = document.createElement("button");
      forkBtn.className = "pc-session-item-fork";
      forkBtn.textContent = "Fork";
      forkBtn.title = "基于此会话创建分支";
      forkBtn.addEventListener("click", (e) => {
        e.stopPropagation();
        requestForkSession(session.id);
        sidebarEl?.querySelector(".pc-session-bar")?.classList.remove("pc-dropdown-open");
      });

      item.appendChild(dot);
      item.appendChild(info);
      item.appendChild(forkBtn);
      item.addEventListener("click", () => {
        switchSession(session.id);
        sidebarEl?.querySelector(".pc-session-bar")?.classList.remove("pc-dropdown-open");
      });

      dropdown.appendChild(item);
    }
  }

  function renderSessionMessages(messages) {
    const body = sidebarEl?.querySelector(".pc-sidebar-body");
    if (!body) return;
    body.innerHTML = "";

    if (!messages || messages.length === 0) {
      body.innerHTML = '<div class="pc-sidebar-empty">暂无消息，选中文字或点击图表开始评论</div>';
      return;
    }

    for (const m of messages) {
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
      renderMessageContent(contentEl, m.content, m.role);
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

      body.appendChild(msgEl);
    }

    body.scrollTop = body.scrollHeight;
  }

  function renderHistory(sessions) {
    const body = sidebarEl?.querySelector(".pc-sidebar-body");
    if (!body) return;

    allSessions = sessions || [];

    // 如果没有 currentSessionId，选第一个活跃会话
    if (!currentSessionId && allSessions.length > 0) {
      const active = allSessions.find(s => s.is_active);
      currentSessionId = active ? active.id : allSessions[0].id;
    }

    // 始终更新选择器（即使正在处理中）
    updateSessionSelector();
    renderSessionDropdown();

    // 正在处理中，不覆盖当前聊天消息区
    if (pendingCommentId) return;

    // 渲染当前会话的消息
    const currentSession = allSessions.find(s => s.id === currentSessionId);
    renderSessionMessages(currentSession?.messages || []);

    // 更新 CLI session info
    if (currentSession?.cli_session_id) {
      updateCliSessionId(currentSession.cli_session_id, currentSession.model_provider);
    } else {
      clearCliSessionId();
    }
  }

  // ========== 暂存队列 ==========

  function createDraftId() {
    return `draft-${crypto.randomUUID()}`;
  }

  function buildDraftTooltip(entry) {
    const index = entry?.draft_index || 0;
    const comment = String(entry?.comment || "");
    return `#${index}: ${comment.slice(0, 40)}`;
  }

  function renumberCommentQueue(entries) {
    if (typeof chartUtils.renumberDraftEntries === "function") {
      return chartUtils.renumberDraftEntries(entries);
    }
    return (entries || []).map((entry, index) => ({ ...entry, draft_index: index + 1 }));
  }

  function updateCommentQueue(entries) {
    commentQueue = renumberCommentQueue(entries || []);
    syncDraftMarkers();
    renderQueue();
    updateFabBadge();
    updateSendButton();
  }

  function getDraftById(draftId) {
    return commentQueue.find((entry) => entry.draft_id === draftId) || null;
  }

  function removeDraftEditor() {
    draftEditorEl?.remove();
    draftEditorEl = null;
  }

  function getDraftAnchorRect(anchorEl) {
    if (anchorEl?.getBoundingClientRect) {
      return anchorEl.getBoundingClientRect();
    }
    return null;
  }

  function syncDraftMarkers() {
    for (const entry of commentQueue) {
      for (const el of entry.highlightEls || []) {
        el.dataset.draftId = entry.draft_id;
        if (el.classList.contains("pc-highlight")) {
          el.dataset.queueIndex = entry.draft_index;
          el.title = buildDraftTooltip(entry);
        } else if (el.classList.contains("pc-highlight-badge")) {
          el.textContent = entry.draft_index;
          el.title = buildDraftTooltip(entry);
        }
      }
    }
  }

  function removeDraftHighlights(entry) {
    for (const el of entry?.highlightEls || []) {
      if (el.classList.contains("pc-highlight")) {
        const parent = el.parentNode;
        if (parent) {
          while (el.firstChild) parent.insertBefore(el.firstChild, el);
          parent.removeChild(el);
          parent.normalize();
        }
      } else {
        el.remove();
      }
    }
  }

  function deleteDraft(draftId) {
    const fallback = {
      removed: null,
      entries: renumberCommentQueue(commentQueue.filter((entry) => entry?.draft_id !== draftId)),
    };
    const result = typeof chartUtils.removeDraftById === "function"
      ? chartUtils.removeDraftById(commentQueue, draftId)
      : fallback;
    if (result.removed) {
      removeDraftHighlights(result.removed);
    }
    removeDraftEditor();
    updateCommentQueue(result.entries);
  }

  function saveDraftComment(draftId, nextComment) {
    const normalized = nextComment.trim();
    if (!normalized) return false;
    const nextEntries = typeof chartUtils.updateDraftComment === "function"
      ? chartUtils.updateDraftComment(commentQueue, draftId, normalized)
      : commentQueue.map((entry) => entry.draft_id === draftId ? { ...entry, comment: normalized } : entry);
    removeDraftEditor();
    updateCommentQueue(nextEntries);
    return true;
  }

  function openDraftEditor(draftId, anchorEl) {
    const entry = getDraftById(draftId);
    if (!entry) return;
    removeDraftEditor();

    const rect = getDraftAnchorRect(anchorEl || entry.highlightEls?.[0]);
    draftEditorEl = document.createElement("div");
    draftEditorEl.className = "pc-draft-editor";
    draftEditorEl.innerHTML = `
      <div class="pc-draft-editor-title">编辑待提交评论 #${entry.draft_index}</div>
      <textarea class="pc-draft-editor-input" rows="3"></textarea>
      <div class="pc-draft-editor-actions">
        <button class="pc-draft-editor-delete">删除</button>
        <span class="pc-draft-editor-spacer"></span>
        <button class="pc-draft-editor-cancel">取消</button>
        <button class="pc-draft-editor-save">保存</button>
      </div>
    `;
    document.body.appendChild(draftEditorEl);

    // 阻止编辑器内的事件冒泡到 document mousedown handler
    draftEditorEl.addEventListener("mousedown", (e) => e.stopPropagation());

    const input = draftEditorEl.querySelector(".pc-draft-editor-input");
    const saveBtn = draftEditorEl.querySelector(".pc-draft-editor-save");
    const deleteBtn = draftEditorEl.querySelector(".pc-draft-editor-delete");
    const cancelBtn = draftEditorEl.querySelector(".pc-draft-editor-cancel");
    input.value = entry.comment;

    const editorRect = draftEditorEl.getBoundingClientRect();
    const left = rect
      ? Math.min(window.innerWidth - editorRect.width - 12, Math.max(12, rect.left + window.scrollX))
      : window.scrollX + 24;
    let top = rect
      ? rect.bottom + window.scrollY + 10
      : window.scrollY + 24;
    if (rect && top + editorRect.height > window.scrollY + window.innerHeight - 12) {
      top = Math.max(window.scrollY + 12, rect.top + window.scrollY - editorRect.height - 10);
    }
    draftEditorEl.style.left = `${left}px`;
    draftEditorEl.style.top = `${top}px`;

    function handleSave() {
      saveDraftComment(draftId, input.value);
    }

    saveBtn.addEventListener("click", handleSave);
    deleteBtn.addEventListener("click", () => deleteDraft(draftId));
    cancelBtn.addEventListener("click", removeDraftEditor);
    input.addEventListener("keydown", (e) => {
      if (e.key === "Enter" && !e.ctrlKey && !e.metaKey && !e.shiftKey) {
        e.preventDefault();
        handleSave();
      }
      if (e.key === "Escape") {
        e.preventDefault();
        removeDraftEditor();
      }
    });
    input.focus();
    input.setSelectionRange(input.value.length, input.value.length);
  }

  function queueComment(selectedText, comment, chartInfo, rect) {
    const entry = {
      draft_id: createDraftId(),
      draft_index: commentQueue.length + 1,
      selected_text: selectedText,
      comment: comment,
      chart_info: chartInfo || null,
      anchor_type: chartInfo ? "chart_marker" : "inline_wrap",
      highlightEls: [],
    };

    // 高亮页面上选中的文字（使用 showCommentBox 时保存的 Range）
    if (currentRange && !chartInfo) {
      try {
        const range = currentRange;
        currentRange = null;
        const wrapper = document.createElement("span");
        wrapper.className = "pc-highlight";
        wrapper.dataset.queueIndex = entry.draft_index;
        wrapper.dataset.draftId = entry.draft_id;
        wrapper.title = buildDraftTooltip(entry);
        wrapper.addEventListener("click", (e) => {
          e.stopPropagation();
          openDraftEditor(entry.draft_id, wrapper);
        });
        // 用 surroundContents 包裹（仅单节点有效）
        try {
          range.surroundContents(wrapper);
          entry.highlightEls.push(wrapper);
        } catch {
          // 跨节点选择：在选区起始处放圆角标记
          const mark = document.createElement("span");
          mark.className = "pc-highlight-badge pc-highlight-badge-inline";
          mark.textContent = entry.draft_index;
          mark.dataset.draftId = entry.draft_id;
          entry.anchor_type = "inline_marker";
          mark.title = buildDraftTooltip(entry);
          mark.addEventListener("click", (e) => {
            e.stopPropagation();
            openDraftEditor(entry.draft_id, mark);
          });
          range.collapse(true);
          range.insertNode(mark);
          entry.highlightEls.push(mark);
        }
      } catch (e) {
        // 选区已失效，忽略
      }
    } else if (chartInfo && rect) {
      // 图表：在图表边角加标记
      const mark = document.createElement("span");
      mark.className = "pc-highlight-badge";
      mark.textContent = entry.draft_index;
      mark.dataset.draftId = entry.draft_id;
      mark.title = buildDraftTooltip(entry);
      mark.style.position = "absolute";
      mark.style.left = (rect.right + window.scrollX - 24) + "px";
      mark.style.top = (rect.top + window.scrollY - 2) + "px";
      mark.addEventListener("click", (e) => {
        e.stopPropagation();
        openDraftEditor(entry.draft_id, mark);
      });
      document.body.appendChild(mark);
      entry.highlightEls.push(mark);
    }

    window.getSelection()?.removeAllRanges();
    openSidebarChat();
    updateCommentQueue([...commentQueue, entry]);
  }

  function removeAllHighlights() {
    for (const q of commentQueue) {
      removeDraftHighlights(q);
    }
    removeDraftEditor();
  }

  function editQueueItem(index) {
    if (index < 0 || index >= commentQueue.length) return;
    const entry = commentQueue[index];
    openDraftEditor(entry.draft_id, entry.highlightEls?.[0]);
  }
  function renderQueue() {
    if (!sidebarEl) return;

    // 移除旧的队列区（可能在 sidebar-body 或 sidebar 级别）
    sidebarEl.querySelector(".pc-queue-section")?.remove();

    if (commentQueue.length === 0) return;

    const section = document.createElement("div");
    section.className = "pc-queue-section";

    // 头部
    const header = document.createElement("div");
    header.className = "pc-queue-header";
    header.innerHTML = `<span class="pc-queue-title">待发送队列 (${commentQueue.length}条)</span>`;
    const clearBtn = document.createElement("button");
    clearBtn.className = "pc-queue-clear";
    clearBtn.textContent = "清空";
    clearBtn.addEventListener("click", () => {
      removeAllHighlights();
      updateCommentQueue([]);
    });
    header.appendChild(clearBtn);
    section.appendChild(header);

    // 队列项
    const items = document.createElement("div");
    items.className = "pc-queue-items";
    for (let i = 0; i < commentQueue.length; i++) {
      const q = commentQueue[i];
      const item = document.createElement("div");
      item.className = "pc-queue-item";

      const num = document.createElement("span");
      num.className = "pc-queue-item-num";
      num.textContent = q.draft_index || (i + 1);

      const itemBody = document.createElement("div");
      itemBody.className = "pc-queue-item-body";

      if (q.selected_text) {
        const ctx = document.createElement("div");
        ctx.className = "pc-queue-item-context";
        ctx.textContent = q.selected_text.length > 60 ? q.selected_text.slice(0, 60) + "..." : q.selected_text;
        itemBody.appendChild(ctx);
      }

      const text = document.createElement("div");
      text.className = "pc-queue-item-text";
      text.textContent = q.comment;
      text.addEventListener("click", () => {
        const anchor = q.highlightEls?.[0];
        if (anchor) {
          anchor.scrollIntoView({ behavior: "smooth", block: "center" });
          setTimeout(() => openDraftEditor(q.draft_id, anchor), 300);
        } else {
          openDraftEditor(q.draft_id);
        }
      });
      itemBody.appendChild(text);

      const removeBtn = document.createElement("button");
      removeBtn.className = "pc-queue-item-remove";
      removeBtn.textContent = "\u00d7";
      removeBtn.title = "移除";
      removeBtn.addEventListener("click", () => deleteDraft(q.draft_id));

      item.appendChild(num);
      item.appendChild(itemBody);
      item.appendChild(removeBtn);
      items.appendChild(item);
    }
    section.appendChild(items);

    // 发送按钮
    const footer = document.createElement("div");
    footer.className = "pc-queue-footer";
    const sendBtn = document.createElement("button");
    sendBtn.className = "pc-queue-send";
    sendBtn.textContent = `全部发送 (${commentQueue.length}条)`;
    sendBtn.addEventListener("click", sendQueuedComments);
    footer.appendChild(sendBtn);
    section.appendChild(footer);

    // 插入到输入框上方（sidebar flex 子项，不在滚动区域内）
    const inputArea = sidebarEl.querySelector(".pc-sidebar-input");
    if (inputArea) {
      sidebarEl.insertBefore(section, inputArea);
    }
  }

  function sendQueuedComments() {
    if (commentQueue.length === 0 || pendingCommentId) return;

    // 每条携带自己的 selected_text
    const items = commentQueue.map(q => ({
      comment: q.comment,
      selected_text: q.selected_text || "",
      chart_info: q.chart_info || null,
    }));
    const data = {
      items: items,
      comments: items.map(i => i.comment),
      page_url: window.location.href,
      page_title: document.title,
      page_meta: getPageMeta(),
      model_provider: getActiveSessionModelProvider(),
    };
    if (shouldSendPageContext("")) {
      data.page_context = getPageContext("");
    }
    if (currentSessionId) data.session_id = currentSessionId;

    removeDraftEditor();
    removeAllHighlights();
    const queueCopy = [...commentQueue];
    commentQueue = [];
    renderQueue();
    updateFabBadge();
    updateSendButton();
    disableChatInput();

    chrome.runtime.sendMessage({ type: "submit_batch_comment", page_identity: getPageIdentity(), data }, (response) => {
      if (chrome.runtime.lastError || !response?.ok) {
        appendChatMessage("system", response?.error || "发送失败，服务器未连接");
        commentQueue = queueCopy;
        renderQueue();
        updateFabBadge();
        updateSendButton();
        enableChatInput();
        return;
      }
      pendingCommentId = response.id;
      startPendingTimeout();
      appendBatchList(queueCopy);
      showTypingIndicator("正在处理 1/" + items.length + "...", "processing");
    });
  }

  function appendBatchList(queueItems) {
    const body = sidebarEl?.querySelector(".pc-sidebar-body");
    if (!body) return;
    const list = document.createElement("div");
    list.className = "pc-batch-list";
    for (let i = 0; i < queueItems.length; i++) {
      const item = document.createElement("div");
      item.className = "pc-batch-item";
      item.innerHTML = `<span class="pc-batch-item-num">${i + 1}.</span>`
        + `<span class="pc-batch-item-text">${escapeHtml(queueItems[i].comment)}</span>`
        + `<span class="pc-batch-item-status pc-batch-status-pending">待处理</span>`;
      list.appendChild(item);
    }
    body.appendChild(list);
    body.scrollTop = body.scrollHeight;
  }

  function updateBatchItemStatus(index, status) {
    const list = sidebarEl?.querySelector(".pc-batch-list");
    if (!list) return;
    const items = list.querySelectorAll(".pc-batch-item");
    if (index < 0 || index >= items.length) return;
    const statusEl = items[index].querySelector(".pc-batch-item-status");
    if (!statusEl) return;
    statusEl.className = "pc-batch-item-status";
    const map = {
      processing: ["pc-batch-status-processing", "处理中"],
      done: ["pc-batch-status-done", "已完成"],
      failed: ["pc-batch-status-failed", "失败"],
    };
    const [cls, txt] = map[status] || ["pc-batch-status-pending", "待处理"];
    statusEl.classList.add(cls);
    statusEl.textContent = txt;
  }

  function updateFabBadge() {
    const toggle = document.querySelector(".pc-sidebar-toggle");
    if (!toggle) return;
    let badge = toggle.querySelector(".pc-fab-badge");
    if (commentQueue.length === 0) {
      badge?.remove();
      return;
    }
    if (!badge) {
      badge = document.createElement("span");
      badge.className = "pc-fab-badge";
      toggle.style.position = "relative";
      toggle.appendChild(badge);
    }
    badge.textContent = commentQueue.length;
  }

  // 页面加载后检查启用状态并初始化
  checkEnabledAndInit();
})();
