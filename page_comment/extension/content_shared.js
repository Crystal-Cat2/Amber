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

  global.PageCommentChartUtils = {
    buildFallbackChartInfo,
    detectChartElement,
    findSectionRoot,
    findTitle,
    getChartContext,
    getElementId,
    isChartContainer,
    isInteractiveChartElement,
    isLikelyChartSvg,
    mergeChartInfo,
    normalizeText,
    shouldSuppressEmptySelectionCleanup,
  };
})(globalThis);
