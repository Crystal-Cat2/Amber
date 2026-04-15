(function (global) {
  "use strict";

  function getClassName(node) {
    if (!node) return "";
    const raw = node.className;
    if (typeof raw === "string") return raw;
    if (raw && typeof raw.baseVal === "string") return raw.baseVal;
    return "";
  }

  function normalizeText(text, limit) {
    const normalized = String(text || "").replace(/\s+/g, " ").trim();
    if (!limit || normalized.length <= limit) return normalized;
    return normalized.slice(0, limit);
  }

  function hasClassFragment(node, fragments) {
    const className = getClassName(node).toLowerCase();
    return fragments.some((fragment) => className.includes(fragment));
  }

  function findClosest(node, predicate) {
    let current = node || null;
    while (current) {
      if (predicate(current)) return current;
      current = current.parentElement || current.parentNode || null;
    }
    return null;
  }

  function isCanvas(node) {
    return node?.tagName === "CANVAS";
  }

  function isSvg(node) {
    return node?.tagName === "SVG";
  }

  function isPlotlyRoot(node) {
    return !!node && hasClassFragment(node, ["js-plotly-plot", "plotly"]);
  }

  function isEchartsRoot(node) {
    if (!node) return false;
    if (typeof node.hasAttribute === "function" && node.hasAttribute("_echarts_instance_")) {
      return true;
    }
    return hasClassFragment(node, ["echart"]);
  }

  function isChartContainer(node) {
    if (!node) return false;
    return isPlotlyRoot(node) || isEchartsRoot(node) || hasClassFragment(node, [
      "chart",
      "plot",
      "graph",
      "highcharts",
      "apexcharts",
    ]);
  }

  function isLikelyChartSvg(node) {
    if (!isSvg(node)) return false;
    if (isChartContainer(node)) return true;
    const owner = findClosest(node.parentElement, (parent) => isChartContainer(parent));
    return !!owner;
  }

  function detectChartElement(target) {
    if (!target || typeof target !== "object") return null;

    const plotlyRoot = findClosest(target, (node) => isPlotlyRoot(node));
    if (plotlyRoot) return plotlyRoot;

    const echartsRoot = findClosest(target, (node) => isEchartsRoot(node));
    if (echartsRoot) return echartsRoot;

    const canvas = findClosest(target, (node) => isCanvas(node));
    if (canvas) return canvas;

    const svg = findClosest(target, (node) => isSvg(node));
    if (svg && isLikelyChartSvg(svg)) {
      return findClosest(svg, (node) => node === svg || isChartContainer(node)) || svg;
    }

    const container = findClosest(target, (node) => {
      if (!isChartContainer(node) || typeof node.querySelector !== "function") return false;
      return !!node.querySelector("canvas, svg");
    });
    return container || null;
  }

  function isCanvasVisuallyBlank(canvas) {
    if (!isCanvas(canvas)) return false;
    const width = Number(canvas.width || 0);
    const height = Number(canvas.height || 0);
    if (width <= 0 || height <= 0) return true;

    try {
      const ctx = canvas.getContext?.("2d");
      if (!ctx?.getImageData) return false;
      const pixels = ctx.getImageData(0, 0, width, height).data;
      for (let i = 3; i < pixels.length; i += 16) {
        if (pixels[i] !== 0) return false;
      }
      return true;
    } catch {
      return false;
    }
  }

  function isInteractiveChartElement(chartEl) {
    if (!chartEl) return false;
    if (isPlotlyRoot(chartEl) || isEchartsRoot(chartEl) || isSvg(chartEl)) return true;

    if (isCanvas(chartEl)) {
      return !isCanvasVisuallyBlank(chartEl);
    }

    const nestedCanvas = chartEl.querySelector?.("canvas") || null;
    if (nestedCanvas) return !isCanvasVisuallyBlank(nestedCanvas);

    const nestedSvg = chartEl.querySelector?.("svg") || null;
    return !!nestedSvg;
  }

  function findSectionRoot(chartEl) {
    const semanticSection = findClosest(chartEl, (node) => {
      if (!node) return false;
      return hasClassFragment(node, ["section", "card"]);
    });
    if (semanticSection) return semanticSection;

    return findClosest(chartEl, (node) => {
      if (!node) return false;
      return hasClassFragment(node, ["section", "card", "chart-wrapper", "chart-container"]) || isChartContainer(node);
    }) || chartEl;
  }

  function findTitle(chartEl) {
    const section = findSectionRoot(chartEl);
    if (!section || typeof section.querySelector !== "function") return "";
    const titleNode = section.querySelector("h1, h2, h3, h4, h5, h6, .title, .chart-title, [data-chart-title]");
    return normalizeText(titleNode?.textContent || "", 160);
  }

  function guessChartLibrary(chartEl) {
    if (!chartEl) return "unknown";
    if (isPlotlyRoot(chartEl) || chartEl.closest?.(".js-plotly-plot")) return "plotly";
    if (isEchartsRoot(chartEl) || chartEl.closest?.("[_echarts_instance_]")) return "echarts";
    if (isSvg(chartEl)) return "svg";
    return "unknown";
  }

  function getElementId(chartEl) {
    if (!chartEl) return "";
    return chartEl.id || chartEl.dataset?.pageCommentChartKey || "";
  }

  function getChartContext(chartEl) {
    const section = findSectionRoot(chartEl);
    if (!section) return "chart";
    return normalizeText(section.textContent || chartEl.id || chartEl.tagName || "chart", 300);
  }

  function buildFallbackChartInfo(chartEl) {
    const canvas = isCanvas(chartEl) ? chartEl : chartEl?.querySelector?.("canvas") || null;
    const elementId = getElementId(chartEl);
    return {
      library: guessChartLibrary(chartEl),
      canvas_id: canvas?.id || "",
      element_id: elementId,
      title: findTitle(chartEl),
      chart_type: chartEl?.tagName ? chartEl.tagName.toLowerCase() : (canvas ? "canvas" : "unknown"),
      config_summary: "DOM context only",
      series_summary: "",
      dom_context: getChartContext(chartEl),
    };
  }

  function mergeChartInfo(baseInfo, nextInfo) {
    const merged = { ...(baseInfo || {}) };
    for (const [key, value] of Object.entries(nextInfo || {})) {
      if (value === null || value === undefined) continue;
      if (typeof value === "string" && !value.trim()) continue;
      merged[key] = value;
    }
    return merged;
  }

  function shouldSuppressEmptySelectionCleanup(state) {
    const text = normalizeText(state?.text || "");
    return !text && !!state?.skipNextSelectionCleanup;
  }

  function renumberDraftEntries(entries) {
    return (entries || []).map((entry, index) => ({
      ...entry,
      draft_index: index + 1,
    }));
  }

  function updateDraftComment(entries, draftId, nextComment) {
    return (entries || []).map((entry) => (
      entry?.draft_id === draftId
        ? { ...entry, comment: nextComment }
        : entry
    ));
  }

  function removeDraftById(entries, draftId) {
    let removed = null;
    const remaining = [];
    for (const entry of entries || []) {
      if (!removed && entry?.draft_id === draftId) {
        removed = entry;
        continue;
      }
      remaining.push(entry);
    }
    return {
      removed,
      entries: renumberDraftEntries(remaining),
    };
  }

  function buildResumeCommand(modelProvider, sessionId) {
    if (modelProvider === "codex") {
      return `codex resume ${sessionId || ""}`.trim();
    }
    return `claude --resume ${sessionId || ""}`.trim();
  }

  function getThinkingStatusMessage(provider) {
    return "AI 正在思考...";
  }

  function resolveModelProvider(selectedProvider, sessionProvider) {
    const normalize = (value) => String(value || "").trim().toLowerCase() === "codex" ? "codex" : "claude";
    if (String(selectedProvider || "").trim()) {
      return normalize(selectedProvider);
    }
    if (String(sessionProvider || "").trim()) {
      return normalize(sessionProvider);
    }
    return "claude";
  }

  function shouldSendPageContext(state) {
    const selectedText = normalizeText(state?.selectedText || "");
    return !selectedText && !!state?.sendPageContext;
  }

  function normalizePageUrl(pageUrl) {
    try {
      const parsed = new URL(pageUrl || "");
      parsed.search = "";
      parsed.hash = "";
      return parsed.toString();
    } catch {
      return String(pageUrl || "").replace(/[?#].*/, "");
    }
  }

  function getLegacyHostKey(pageUrl) {
    try {
      const parsed = new URL(pageUrl || "");
      if (parsed.protocol === "file:") {
        const parts = parsed.pathname.split("/").filter(Boolean);
        return "file://" + parts.slice(0, Math.min(parts.length, 4)).join("/");
      }
      return parsed.hostname;
    } catch {
      return null;
    }
  }

  function buildPageIdentity(state) {
    const pageMeta = state?.pageMeta || {};
    if (pageMeta.page_key) return String(pageMeta.page_key);
    if (pageMeta.source_script) {
      const pathname = String(state?.pathname || "");
      const htmlName = pathname.split("/").pop() || "";
      return `${pageMeta.source_script}::${htmlName}`;
    }
    return normalizePageUrl(state?.pageUrl || "");
  }

  function buildPageEnabledStorageKey(pageIdentity) {
    return `pc_enabled_page:${String(pageIdentity || "")}`;
  }

  function isFeishuDocUrl(pageUrl) {
    try {
      const parsed = new URL(pageUrl || "");
      const host = (parsed.hostname || "").toLowerCase();
      const path = parsed.pathname || "";
      const isFeishuHost = host.includes("feishu.cn") || host.includes("larksuite.com");
      if (!isFeishuHost) return false;
      return /^\/(docx|doc|wiki|sheets)\//.test(path);
    } catch {
      return false;
    }
  }

  function classifyFeishuTarget(candidate) {
    const typeHint = String(candidate?.typeHint || "").toLowerCase();
    const text = String(candidate?.text || "").toLowerCase();
    const className = String(candidate?.className || "").toLowerCase();
    const href = String(candidate?.href || "").toLowerCase();
    const tagName = String(candidate?.tagName || "").toLowerCase();

    if (typeHint === "table" || tagName === "table") return "table";
    if (typeHint === "image" || tagName === "img" || className.includes("image")) return "image";
    if (typeHint === "attachment" || className.includes("attachment") || className.includes("file")) return "attachment";
    if (
      typeHint === "mindmap" ||
      className.includes("mindmap") ||
      className.includes("mindnote") ||
      text.includes("思维导图")
    ) {
      return "mindmap";
    }
    if (
      typeHint === "whiteboard" ||
      className.includes("whiteboard") ||
      className.includes("board") ||
      text.includes("画板")
    ) {
      return "whiteboard";
    }
    if (
      typeHint === "embed" ||
      tagName === "iframe" ||
      href.includes("/docx/") ||
      href.includes("/doc/") ||
      href.includes("/wiki/") ||
      href.includes("/sheets/") ||
      href.includes("/base/")
    ) {
      return "embed";
    }
    if (typeHint === "text" || text) return "text";
    return "unknown";
  }

  function extractLarkTokens(text) {
    const source = String(text || "");
    const matches = [];
    const patterns = [
      /https?:\/\/[^\s"'<>]+\/(docx|doc|wiki|sheets|base)\/([A-Za-z0-9]+)/g,
      /(?:token|obj_token|sheet token|base token)\s*=\s*["']?([A-Za-z0-9_-]{6,})["']?/gi,
      /<(sheet|base|docx|wiki)[^>]*token=["']([^"']+)["']/gi,
    ];
    for (const pattern of patterns) {
      let match;
      while ((match = pattern.exec(source))) {
        if (pattern.source.startsWith("https")) {
          matches.push({ kind: match[1], token: match[2], raw: match[0] });
        } else if (match.length >= 3 && match[1] && match[2] && pattern.source.startsWith("<(")) {
          matches.push({ kind: match[1], token: match[2], raw: match[0] });
        } else {
          matches.push({ kind: "token", token: match[1], raw: match[0] });
        }
      }
    }
    return matches;
  }

  function normalizeFeishuTable(tableMatrix, limitRows = 12, limitCols = 8) {
    return (tableMatrix || [])
      .slice(0, limitRows)
      .map((row) => (row || []).slice(0, limitCols).map((cell) => normalizeText(cell || "", 120)));
  }

  function getPendingStatusMeta(status) {
    switch (String(status || "").trim()) {
      case "interaction":
        return { label: "等待你的回复", tone: "waiting-user" };
      case "processing":
      case "editing":
      case "regenerating":
        return { label: "AI 处理中", tone: "processing" };
      default:
        return { label: "处理中", tone: "processing" };
    }
  }

  function formatBatchProgressMessage(state) {
    const current = Number(state?.current || 0);
    const total = Number(state?.total || 0);
    const currentComment = String(state?.currentComment || "").trim();
    return currentComment
      ? `正在处理 ${current}/${total}: ${currentComment}`
      : `正在处理 ${current}/${total}`;
  }

  function getInteractionPendingHint() {
    return "AI 需要你的确认，请选择一个选项或直接输入回复。";
  }

  function shouldScheduleResultPoll(status) {
    return ["processing", "editing", "regenerating", "interaction"].includes(String(status || "").trim());
  }

  function escapeHtml(text) {
    return String(text || "")
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;")
      .replace(/"/g, "&quot;")
      .replace(/'/g, "&#39;");
  }

  function sanitizeUrl(url) {
    const trimmed = String(url || "").trim();
    if (/^https?:\/\//i.test(trimmed)) return trimmed;
    return "#";
  }

  function renderInlineMarkdown(text) {
    const placeholders = [];
    function store(html) {
      const token = `\u0000${placeholders.length}\u0000`;
      placeholders.push(html);
      return token;
    }

    let html = "";
    const raw = String(text || "");
    for (let i = 0; i < raw.length; i += 1) {
      const char = raw[i];
      if (char === "`") {
        const end = raw.indexOf("`", i + 1);
        if (end > i + 1) {
          html += store(`<code>${escapeHtml(raw.slice(i + 1, end))}</code>`);
          i = end;
          continue;
        }
      }
      // 裸 URL 自动转链接
      if (char === "h" && (raw.startsWith("https://", i) || raw.startsWith("http://", i))) {
        const preceding = raw.slice(Math.max(0, i - 2), i);
        if (!preceding.endsWith("](")) {
          const urlMatch = raw.slice(i).match(/^https?:\/\/[^\s)\]<>"']+/);
          if (urlMatch) {
            const url = urlMatch[0];
            const safeUrl = escapeHtml(sanitizeUrl(url));
            html += store(`<a href="${safeUrl}" target="_blank" rel="noopener noreferrer" class="pc-autolink">${escapeHtml(url)}</a>`);
            i += url.length - 1;
            continue;
          }
        }
      }
      if (char === "[") {
        const labelEnd = raw.indexOf("]", i + 1);
        if (labelEnd > i + 1 && raw[labelEnd + 1] === "(") {
          let depth = 1;
          let cursor = labelEnd + 2;
          while (cursor < raw.length && depth > 0) {
            if (raw[cursor] === "(") depth += 1;
            else if (raw[cursor] === ")") depth -= 1;
            cursor += 1;
          }
          if (depth === 0) {
            const label = raw.slice(i + 1, labelEnd);
            const url = raw.slice(labelEnd + 2, cursor - 1);
            const safeUrl = escapeHtml(sanitizeUrl(url));
            html += store(`<a href="${safeUrl}" target="_blank" rel="noopener noreferrer">${escapeHtml(label)}</a>`);
            i = cursor - 1;
            continue;
          }
        }
      }
      html += escapeHtml(char);
    }
    html = html.replace(/\*\*([^*]+)\*\*/g, "<strong>$1</strong>");
    html = html.replace(/~~([^~]+)~~/g, "<del>$1</del>");
    html = html.replace(/(^|[^\*])\*([^*\n]+)\*(?!\*)/g, "$1<em>$2</em>");
    html = html.replace(/(^|[^_])_([^_\n]+)_(?!_)/g, "$1<em>$2</em>");
    html = html.replace(/\u0000(\d+)\u0000/g, (_match, index) => placeholders[Number(index)] || "");
    return html;
  }

  function isTableSeparator(line) {
    const normalized = String(line || "").trim();
    return /^\|?(\s*:?-{3,}:?\s*\|)+\s*:?-{3,}:?\s*\|?$/.test(normalized);
  }

  function splitTableRow(line) {
    return String(line || "")
      .trim()
      .replace(/^\|/, "")
      .replace(/\|$/, "")
      .split("|")
      .map((cell) => cell.trim());
  }

  function renderTableBlock(lines) {
    if (lines.length < 2 || !isTableSeparator(lines[1])) return null;
    const headers = splitTableRow(lines[0]);
    const rows = lines.slice(2).map(splitTableRow);
    const thead = `<thead><tr>${headers.map((cell) => `<th>${renderInlineMarkdown(cell)}</th>`).join("")}</tr></thead>`;
    const tbody = rows.length
      ? `<tbody>${rows.map((cells) => `<tr>${cells.map((cell) => `<td>${renderInlineMarkdown(cell)}</td>`).join("")}</tr>`).join("")}</tbody>`
      : "";
    return `<table>${thead}${tbody}</table>`;
  }

  function renderCodeFenceBlock(block) {
    const match = String(block || "").match(/^```([^\n`]*)\n([\s\S]*?)\n```$/);
    if (!match) return null;
    const language = match[1].trim().replace(/[^\w-]/g, "");
    const code = escapeHtml(match[2]);
    const classAttr = language ? ` class="language-${language}"` : "";
    return `<pre><code${classAttr}>${code}</code></pre>`;
  }

  function renderListBlock(lines) {
    const ordered = /^\d+\.\s+/.test(lines[0] || "");
    const pattern = ordered ? /^\d+\.\s+/ : /^[-*+]\s+/;
    if (!lines.every((line) => pattern.test(line))) return null;
    const tag = ordered ? "ol" : "ul";
    return `<${tag}>${lines.map((line) => `<li>${renderInlineMarkdown(line.replace(pattern, ""))}</li>`).join("")}</${tag}>`;
  }

  function renderBlockquoteBlock(lines) {
    if (!lines.every((line) => /^\s*>\s?/.test(line))) return null;
    const inner = lines.map((line) => line.replace(/^\s*>\s?/, "")).join("\n");
    return `<blockquote>${renderMarkdownToHtml(inner)}</blockquote>`;
  }

  function renderHeadingBlock(block) {
    const match = String(block || "").match(/^(#{1,4})\s+(.*)$/);
    if (!match) return null;
    const level = match[1].length;
    return `<h${level}>${renderInlineMarkdown(match[2])}</h${level}>`;
  }

  function renderParagraphBlock(block) {
    const lines = String(block || "").split("\n");
    return `<p>${lines.map((line) => renderInlineMarkdown(line)).join("<br>")}</p>`;
  }

  function renderMarkdownToHtml(markdownText) {
    const source = String(markdownText || "").replace(/\r\n?/g, "\n").trim();
    if (!source) return "";

    const blocks = source.split(/\n{2,}/);
    const rendered = [];
    for (const block of blocks) {
      const lines = block.split("\n");
      const codeFence = renderCodeFenceBlock(block);
      if (codeFence) {
        rendered.push(codeFence);
        continue;
      }
      const table = renderTableBlock(lines);
      if (table) {
        rendered.push(table);
        continue;
      }
      const heading = renderHeadingBlock(block);
      if (heading) {
        rendered.push(heading);
        continue;
      }
      const blockquote = renderBlockquoteBlock(lines);
      if (blockquote) {
        rendered.push(blockquote);
        continue;
      }
      const list = renderListBlock(lines);
      if (list) {
        rendered.push(list);
        continue;
      }
      rendered.push(renderParagraphBlock(block));
    }
    return rendered.join("");
  }

  global.PageCommentChartUtils = {
    buildFallbackChartInfo,
    buildPageEnabledStorageKey,
    buildPageIdentity,
    classifyFeishuTarget,
    extractLarkTokens,
    formatBatchProgressMessage,
    detectChartElement,
    findSectionRoot,
    findTitle,
    getThinkingStatusMessage,
    getInteractionPendingHint,
    getLegacyHostKey,
    getPendingStatusMeta,
    getChartContext,
    getElementId,
    isFeishuDocUrl,
    isChartContainer,
    isInteractiveChartElement,
    isLikelyChartSvg,
    buildResumeCommand,
    mergeChartInfo,
    normalizeFeishuTable,
    normalizePageUrl,
    normalizeText,
    resolveModelProvider,
    renderMarkdownToHtml,
    removeDraftById,
    renumberDraftEntries,
    shouldSendPageContext,
    shouldScheduleResultPoll,
    shouldSuppressEmptySelectionCleanup,
    updateDraftComment,
  };
})(globalThis);
