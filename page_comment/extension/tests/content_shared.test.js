const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");
const vm = require("node:vm");

function loadUtils() {
  const filePath = path.join(__dirname, "..", "content_shared.js");
  const source = fs.readFileSync(filePath, "utf8");
  const context = {
    console,
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
