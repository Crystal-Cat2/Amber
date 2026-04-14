const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");
const vm = require("node:vm");

function loadUtils() {
  const filePath = path.join(__dirname, "..", "content_shared.js");
  const source = fs.readFileSync(filePath, "utf8");
  const context = {
    console,
    URL,
    globalThis: {},
  };
  context.global = context;
  vm.createContext(context);
  vm.runInContext(source, context, { filename: filePath });
  return context.globalThis.PageCommentChartUtils;
}

function createElement(tagName, options = {}) {
  const element = {
    tagName: tagName.toUpperCase(),
    id: options.id || "",
    className: options.className || "",
    dataset: { ...(options.dataset || {}) },
    textContent: options.textContent || "",
    parentElement: null,
    children: [],
    appendChild(child) {
      child.parentElement = this;
      this.children.push(child);
      return child;
    },
    closest(selector) {
      let node = this;
      while (node) {
        if (matches(node, selector)) return node;
        node = node.parentElement;
      }
      return null;
    },
    querySelector(selector) {
      for (const child of this.children) {
        if (matches(child, selector)) return child;
        const nested = child.querySelector(selector);
        if (nested) return nested;
      }
      return null;
    },
    querySelectorAll(selector) {
      const results = [];
      for (const child of this.children) {
        if (matches(child, selector)) results.push(child);
        results.push(...child.querySelectorAll(selector));
      }
      return results;
    },
  };
  return element;
}

function matches(element, selector) {
  return selector
    .split(",")
    .map((part) => part.trim())
    .some((part) => matchSingle(element, part));
}

function matchSingle(element, selector) {
  if (!selector) return false;
  if (selector.startsWith(".")) {
    return element.className.split(/\s+/).includes(selector.slice(1));
  }
  if (selector.startsWith("#")) {
    return element.id === selector.slice(1);
  }
  if (selector.startsWith("[")) {
    const attr = selector.slice(1, -1).replace(/[*^$~|]?=.*/, "");
    if (attr.startsWith("class*")) {
      return element.className.includes(selector.match(/"([^"]+)"/)?.[1] || "");
    }
    if (attr.startsWith("id*")) {
      return element.id.includes(selector.match(/"([^"]+)"/)?.[1] || "");
    }
    if (attr.startsWith("data-")) {
      const key = attr
        .slice(5)
        .replace(/-([a-z])/g, (_, c) => c.toUpperCase());
      return Object.prototype.hasOwnProperty.call(element.dataset, key);
    }
    return false;
  }
  if (selector.includes(".")) {
    const [tag, cls] = selector.split(".");
    return (
      element.tagName === tag.toUpperCase() &&
      element.className.split(/\s+/).includes(cls)
    );
  }
  return element.tagName === selector.toUpperCase();
}

function runTest(name, fn) {
  try {
    fn();
    console.log(`PASS ${name}`);
  } catch (error) {
    console.error(`FAIL ${name}`);
    console.error(error);
    process.exitCode = 1;
  }
}

runTest("exports chart utilities", () => {
  const utils = loadUtils();
  assert.equal(typeof utils, "object");
  assert.equal(typeof utils.detectChartElement, "function");
  assert.equal(typeof utils.buildFallbackChartInfo, "function");
  assert.equal(typeof utils.mergeChartInfo, "function");
});

runTest("detectChartElement returns plotly root for nested svg nodes", () => {
  const utils = loadUtils();
  const plotlyRoot = createElement("div", { className: "js-plotly-plot" });
  const svg = plotlyRoot.appendChild(createElement("svg", { className: "main-svg" }));
  const point = svg.appendChild(createElement("path", { className: "point" }));

  const chartEl = utils.detectChartElement(point);
  assert.equal(chartEl, plotlyRoot);
});

runTest("buildFallbackChartInfo works without page-world chart globals", () => {
  const utils = loadUtils();
  const section = createElement("div", { className: "section", textContent: "漏斗数据 Level 1 通过率" });
  section.appendChild(createElement("h2", { textContent: "关卡漏斗数据" }));
  const wrapper = section.appendChild(createElement("div", { className: "chart-container" }));
  const canvas = wrapper.appendChild(createElement("canvas", { id: "funnelChart" }));

  const info = utils.buildFallbackChartInfo(canvas);
  assert.equal(info.library, "unknown");
  assert.equal(info.canvas_id, "funnelChart");
  assert.equal(info.element_id, "funnelChart");
  assert.equal(info.title, "关卡漏斗数据");
  assert.match(info.dom_context, /漏斗数据/);
});

runTest("mergeChartInfo prefers bridge metadata over fallback defaults", () => {
  const utils = loadUtils();
  const merged = utils.mergeChartInfo(
    {
      library: "unknown",
      chart_type: "canvas",
      title: "",
      config_summary: "",
      series_summary: "",
      element_id: "chart-1",
      canvas_id: "chart-1",
      dom_context: "ctx",
    },
    {
      library: "chartjs",
      chart_type: "line",
      title: "通过率",
      config_summary: "axes: y linear",
      series_summary: "通过率",
    }
  );

  assert.equal(merged.library, "chartjs");
  assert.equal(merged.chart_type, "line");
  assert.equal(merged.title, "通过率");
  assert.equal(merged.element_id, "chart-1");
  assert.equal(merged.dom_context, "ctx");
});

runTest("shouldSuppressEmptySelectionCleanup keeps chart button alive for same click gesture", () => {
  const utils = loadUtils();
  assert.equal(
    utils.shouldSuppressEmptySelectionCleanup({
      text: "",
      skipNextSelectionCleanup: true,
    }),
    true
  );
  assert.equal(
    utils.shouldSuppressEmptySelectionCleanup({
      text: "有效选区",
      skipNextSelectionCleanup: true,
    }),
    false
  );
  assert.equal(
    utils.shouldSuppressEmptySelectionCleanup({
      text: "",
      skipNextSelectionCleanup: false,
    }),
    false
  );
});

runTest("isInteractiveChartElement ignores blank canvas but keeps painted canvas", () => {
  const utils = loadUtils();
  const blankCanvas = createElement("canvas", { id: "blankCanvas" });
  blankCanvas.width = 4;
  blankCanvas.height = 4;
  blankCanvas.getContext = () => ({
    getImageData() {
      return { data: new Uint8ClampedArray(4 * 4 * 4) };
    },
  });

  const paintedCanvas = createElement("canvas", { id: "paintedCanvas" });
  paintedCanvas.width = 4;
  paintedCanvas.height = 4;
  paintedCanvas.getContext = () => ({
    getImageData() {
      const data = new Uint8ClampedArray(4 * 4 * 4);
      data[3] = 255;
      return { data };
    },
  });

  assert.equal(utils.isInteractiveChartElement(blankCanvas), false);
  assert.equal(utils.isInteractiveChartElement(paintedCanvas), true);
});

runTest("renumberDraftEntries assigns sequential draft_index values", () => {
  const utils = loadUtils();
  const entries = utils.renumberDraftEntries([
    { draft_id: "d-1", comment: "first", draft_index: 9 },
    { draft_id: "d-2", comment: "second", draft_index: 7 },
    { draft_id: "d-3", comment: "third", draft_index: 5 },
  ]);

  assert.deepEqual(
    entries.map((entry) => ({ draft_id: entry.draft_id, draft_index: entry.draft_index })),
    [
      { draft_id: "d-1", draft_index: 1 },
      { draft_id: "d-2", draft_index: 2 },
      { draft_id: "d-3", draft_index: 3 },
    ]
  );
});

runTest("updateDraftComment updates only the targeted draft", () => {
  const utils = loadUtils();
  const entries = utils.updateDraftComment(
    [
      { draft_id: "d-1", comment: "first" },
      { draft_id: "d-2", comment: "second" },
    ],
    "d-2",
    "updated second"
  );

  assert.deepEqual(
    entries.map((entry) => ({ draft_id: entry.draft_id, comment: entry.comment })),
    [
      { draft_id: "d-1", comment: "first" },
      { draft_id: "d-2", comment: "updated second" },
    ]
  );
});

runTest("removeDraftById removes target draft and renumbers the remainder", () => {
  const utils = loadUtils();
  const result = utils.removeDraftById(
    [
      { draft_id: "d-1", comment: "first", draft_index: 1 },
      { draft_id: "d-2", comment: "second", draft_index: 2 },
      { draft_id: "d-3", comment: "third", draft_index: 3 },
    ],
    "d-2"
  );

  assert.equal(result.removed?.draft_id, "d-2");
  assert.equal(
    JSON.stringify(result.entries.map((entry) => ({ draft_id: entry.draft_id, draft_index: entry.draft_index }))),
    JSON.stringify([
      { draft_id: "d-1", draft_index: 1 },
      { draft_id: "d-3", draft_index: 2 },
    ])
  );
});

runTest("renderMarkdownToHtml supports headings, lists, code fences, links, and tables", () => {
  const utils = loadUtils();
  const html = utils.renderMarkdownToHtml([
    "# 标题",
    "",
    "- 第一项",
    "- 第二项",
    "",
    "```js",
    "const x = 1 < 2;",
    "```",
    "",
    "[示例](https://example.com)",
    "",
    "| A | B |",
    "| --- | --- |",
    "| 1 | 2 |",
  ].join("\n"));

  assert.match(html, /<h1>标题<\/h1>/);
  assert.match(html, /<ul>/);
  assert.match(html, /<pre><code class="language-js">/);
  assert.match(html, /const x = 1 &lt; 2;/);
  assert.match(html, /<a href="https:\/\/example.com"/);
  assert.match(html, /<table>/);
  assert.match(html, /<th>A<\/th>/);
  assert.match(html, /<td>2<\/td>/);
});

runTest("renderMarkdownToHtml escapes raw html and blocks unsafe urls", () => {
  const utils = loadUtils();
  const html = utils.renderMarkdownToHtml('这里有 <script>alert(1)</script> 和 [坏链接](javascript:alert(1))');

  assert.doesNotMatch(html, /<script>/);
  assert.match(html, /&lt;script&gt;alert\(1\)&lt;\/script&gt;/);
  assert.match(html, /<a href="#"[^>]*>坏链接<\/a>/);
});

runTest("buildResumeCommand adapts to provider", () => {
  const utils = loadUtils();
  assert.equal(utils.buildResumeCommand("claude", "sid-1"), "claude --resume sid-1");
  assert.equal(utils.buildResumeCommand("codex", "sid-2"), "codex resume sid-2");
});

runTest("shouldSendPageContext depends on toggle and selection", () => {
  const utils = loadUtils();
  assert.equal(utils.shouldSendPageContext({ selectedText: "", sendPageContext: true }), true);
  assert.equal(utils.shouldSendPageContext({ selectedText: "有选中", sendPageContext: true }), false);
  assert.equal(utils.shouldSendPageContext({ selectedText: "", sendPageContext: false }), false);
});

runTest("getPendingStatusMeta maps statuses to clear labels and tones", () => {
  const utils = loadUtils();

  assert.equal(
    JSON.stringify(utils.getPendingStatusMeta("processing")),
    JSON.stringify({ label: "AI 处理中", tone: "processing" })
  );
  assert.equal(
    JSON.stringify(utils.getPendingStatusMeta("editing")),
    JSON.stringify({ label: "AI 处理中", tone: "processing" })
  );
  assert.equal(
    JSON.stringify(utils.getPendingStatusMeta("regenerating")),
    JSON.stringify({ label: "AI 处理中", tone: "processing" })
  );
  assert.equal(
    JSON.stringify(utils.getPendingStatusMeta("interaction")),
    JSON.stringify({ label: "等待你的回复", tone: "waiting-user" })
  );
  assert.equal(
    JSON.stringify(utils.getPendingStatusMeta("unknown-status")),
    JSON.stringify({ label: "处理中", tone: "processing" })
  );
});

runTest("formatBatchProgressMessage keeps full current comment text", () => {
  const utils = loadUtils();
  const message = utils.formatBatchProgressMessage({
    current: 2,
    total: 5,
    currentComment: "这是一个很长的批量处理评论，不应该被前端状态文案截断显示。",
  });

  assert.equal(
    message,
    "正在处理 2/5: 这是一个很长的批量处理评论，不应该被前端状态文案截断显示。"
  );
});

runTest("getInteractionPendingHint returns explicit waiting copy", () => {
  const utils = loadUtils();
  assert.equal(
    utils.getInteractionPendingHint(),
    "AI 需要你的确认，请选择一个选项或直接输入回复。"
  );
});

runTest("renderMarkdownToHtml auto-links bare URLs", () => {
  const utils = loadUtils();
  const html = utils.renderMarkdownToHtml("请访问 https://example.com/auth?code=abc&state=123 完成授权");
  assert.match(html, /<a href="https:\/\/example\.com\/auth\?code=abc&amp;state=123"/);
  assert.match(html, /target="_blank"/);
  assert.match(html, /class="pc-autolink"/);
});

runTest("renderMarkdownToHtml does not double-link markdown links", () => {
  const utils = loadUtils();
  const html = utils.renderMarkdownToHtml("[点击这里](https://example.com)");
  const linkCount = (html.match(/<a /g) || []).length;
  assert.equal(linkCount, 1);
  assert.doesNotMatch(html, /pc-autolink/);
});

runTest("renderMarkdownToHtml auto-links bare URL next to text", () => {
  const utils = loadUtils();
  const html = utils.renderMarkdownToHtml("链接：http://test.com/path 结束");
  assert.match(html, /<a href="http:\/\/test\.com\/path"/);
  assert.match(html, /class="pc-autolink"/);
});

runTest("buildPageIdentity prefers page_key then source_script then normalized url", () => {
  const utils = loadUtils();

  assert.equal(
    utils.buildPageIdentity({
      pageMeta: { page_key: "amber/report::index.html" },
      pageUrl: "https://example.com/report?from=share#top",
      pathname: "/report",
    }),
    "amber/report::index.html"
  );

  assert.equal(
    utils.buildPageIdentity({
      pageMeta: { source_script: "ad_kill/scripts/run.py" },
      pageUrl: "file:///D:/Work/Amber/ad_kill/outputs/report.html?from=share",
      pathname: "/D:/Work/Amber/ad_kill/outputs/report.html",
    }),
    "ad_kill/scripts/run.py::report.html"
  );

  assert.equal(
    utils.buildPageIdentity({
      pageMeta: {},
      pageUrl: "https://example.com/report?from=share#top",
      pathname: "/report",
    }),
    "https://example.com/report"
  );
});

runTest("buildPageEnabledStorageKey uses page identity", () => {
  const utils = loadUtils();
  assert.equal(
    utils.buildPageEnabledStorageKey("https://example.com/report"),
    "pc_enabled_page:https://example.com/report"
  );
});

runTest("getLegacyHostKey keeps previous host based behavior for migration", () => {
  const utils = loadUtils();
  assert.equal(
    utils.getLegacyHostKey("https://xwbo3y4nxr.feishu.cn/docx/abc?from=share#top"),
    "xwbo3y4nxr.feishu.cn"
  );
  assert.equal(
    utils.getLegacyHostKey("file:///D:/Work/Amber/ad_kill/outputs/report/index.html"),
    "file://D:/Work/Amber/ad_kill"
  );
});

runTest("isFeishuDocUrl detects supported Feishu document urls", () => {
  const utils = loadUtils();
  assert.equal(utils.isFeishuDocUrl("https://xwbo3y4nxr.feishu.cn/docx/AIzedZyZZoX5vtxM3UKcHKsQnyh"), true);
  assert.equal(utils.isFeishuDocUrl("https://xwbo3y4nxr.feishu.cn/wiki/abc123"), true);
  assert.equal(utils.isFeishuDocUrl("https://xwbo3y4nxr.feishu.cn/sheets/shtcn123"), true);
  assert.equal(utils.isFeishuDocUrl("https://example.com/report"), false);
});

runTest("classifyFeishuTarget distinguishes table image embed and mindmap", () => {
  const utils = loadUtils();
  assert.equal(utils.classifyFeishuTarget({ tagName: "table" }), "table");
  assert.equal(utils.classifyFeishuTarget({ tagName: "img" }), "image");
  assert.equal(utils.classifyFeishuTarget({ href: "https://x.feishu.cn/sheets/shtcn123" }), "embed");
  assert.equal(utils.classifyFeishuTarget({ className: "mindnote-card" }), "mindmap");
  assert.equal(utils.classifyFeishuTarget({ text: "普通段落内容" }), "text");
});

runTest("extractLarkTokens finds urls and embedded token markers", () => {
  const utils = loadUtils();
  const matches = utils.extractLarkTokens([
    '参考链接: https://xwbo3y4nxr.feishu.cn/sheets/shtcn123abc',
    '<sheet token="shtcn999xyz" />',
    'obj_token="doxcn777"',
  ].join("\n"));

  assert.ok(matches.some((item) => item.kind === "sheets" && item.token === "shtcn123abc"));
  assert.ok(matches.some((item) => item.kind === "sheet" && item.token === "shtcn999xyz"));
  assert.ok(matches.some((item) => item.token === "doxcn777"));
});

runTest("normalizeFeishuTable trims table size and cell content", () => {
  const utils = loadUtils();
  const normalized = utils.normalizeFeishuTable([
    ["  表头一  ", "表头二"],
    ["很长很长很长很长很长很长很长很长很长很长很长很长很长很长很长很长很长", "值2"],
    ["值3", "值4"],
  ], 2, 2);

  assert.equal(normalized.length, 2);
  assert.equal(normalized[0][0], "表头一");
  assert.ok(normalized[1][0].length <= 120);
});
