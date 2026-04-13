(function () {
  "use strict";
  if (window.__pageCommentBridgeLoaded) return;
  window.__pageCommentBridgeLoaded = true;

  function normalizeText(text, limit) {
    const normalized = String(text || "").replace(/\s+/g, " ").trim();
    if (!limit || normalized.length <= limit) return normalized;
    return normalized.slice(0, limit);
  }

  function getTitleFromChartJs(chart) {
    const titleOption = chart?.config?.options?.plugins?.title;
    if (!titleOption?.text) return "";
    return Array.isArray(titleOption.text)
      ? titleOption.text.join(" ")
      : String(titleOption.text);
  }

  function summarizeAxes(scales) {
    if (!scales || typeof scales !== "object") return "";
    return Object.entries(scales)
      .map(([key, value]) => {
        const title = normalizeText(value?.title?.text || "", 80);
        return `${key}: ${value?.type || "linear"}${title ? ` '${title}'` : ""}`;
      })
      .join(", ");
  }

  function summarizeSeries(series, fallbackType) {
    return (series || [])
      .map((item) => {
        const name = normalizeText(item?.name || item?.label || "unnamed", 60);
        const type = item?.type || fallbackType || "unknown";
        return `${name} (${type})`;
      })
      .join("; ");
  }

  function inspectChartJs(chartEl) {
    const canvas = chartEl?.tagName === "CANVAS" ? chartEl : chartEl?.querySelector?.("canvas");
    const chart = window.Chart?.getChart?.(canvas);
    if (!chart) return null;

    const config = chart.config || {};
    const datasets = config.data?.datasets || [];
    const scales = config.options?.scales || {};
    const axisSummary = summarizeAxes(scales);
    const seriesSummary = summarizeSeries(datasets, config.type);
    return {
      library: "chartjs",
      chart_type: config.type || "chartjs",
      title: normalizeText(getTitleFromChartJs(chart), 120),
      config_summary: axisSummary ? `${seriesSummary} | axes: ${axisSummary}` : seriesSummary,
      series_summary: seriesSummary,
      element_id: chartEl?.id || chartEl?.dataset?.pageCommentChartKey || "",
      canvas_id: canvas?.id || "",
    };
  }

  function inspectEcharts(chartEl) {
    const root = chartEl?.hasAttribute?.("_echarts_instance_")
      ? chartEl
      : chartEl?.querySelector?.("[_echarts_instance_]") || chartEl;
    const instance = window.echarts?.getInstanceByDom?.(root);
    if (!instance) return null;

    const option = instance.getOption?.() || {};
    const title = Array.isArray(option.title)
      ? normalizeText(option.title[0]?.text || "", 120)
      : normalizeText(option.title?.text || "", 120);
    const series = option.series || [];
    const chartType = series[0]?.type || "echarts";
    const seriesSummary = summarizeSeries(series, chartType);
    const xAxis = Array.isArray(option.xAxis) ? option.xAxis : option.xAxis ? [option.xAxis] : [];
    const yAxis = Array.isArray(option.yAxis) ? option.yAxis : option.yAxis ? [option.yAxis] : [];
    const axisSummary = [
      ...xAxis.map((axis, index) => `x${index}: ${axis?.type || "category"}${axis?.name ? ` '${normalizeText(axis.name, 60)}'` : ""}`),
      ...yAxis.map((axis, index) => `y${index}: ${axis?.type || "value"}${axis?.name ? ` '${normalizeText(axis.name, 60)}'` : ""}`),
    ].join(", ");

    return {
      library: "echarts",
      chart_type: chartType,
      title,
      config_summary: axisSummary ? `${seriesSummary} | axes: ${axisSummary}` : seriesSummary,
      series_summary: seriesSummary,
      element_id: chartEl?.id || chartEl?.dataset?.pageCommentChartKey || "",
      canvas_id: "",
    };
  }

  function inspectPlotly(chartEl) {
    const root = chartEl?.closest?.(".js-plotly-plot") || chartEl;
    if (!root || !root.classList?.contains("js-plotly-plot")) return null;

    const traces = root.data || root._fullData || [];
    const layout = root.layout || root._fullLayout || {};
    const chartType = traces[0]?.type || "plotly";
    const seriesSummary = summarizeSeries(traces, chartType);
    const layoutTitle = typeof layout.title === "string"
      ? layout.title
      : layout.title?.text;
    const axisSummary = [
      layout.xaxis?.title?.text ? `x: '${normalizeText(layout.xaxis.title.text, 60)}'` : "",
      layout.yaxis?.title?.text ? `y: '${normalizeText(layout.yaxis.title.text, 60)}'` : "",
    ].filter(Boolean).join(", ");

    return {
      library: "plotly",
      chart_type: chartType,
      title: normalizeText(layoutTitle || "", 120),
      config_summary: axisSummary ? `${seriesSummary} | axes: ${axisSummary}` : seriesSummary,
      series_summary: seriesSummary,
      element_id: root.id || root.dataset?.pageCommentChartKey || "",
      canvas_id: "",
    };
  }

  function inspectSvg(chartEl) {
    if (chartEl?.tagName !== "SVG") return null;
    return {
      library: "svg",
      chart_type: "svg",
      title: "",
      config_summary: "svg chart",
      series_summary: "",
      element_id: chartEl.id || chartEl.dataset?.pageCommentChartKey || "",
      canvas_id: "",
    };
  }

  function inspectChart(chartEl) {
    if (!chartEl) return null;
    try {
      return inspectChartJs(chartEl)
        || inspectEcharts(chartEl)
        || inspectPlotly(chartEl)
        || inspectSvg(chartEl)
        || null;
    } catch (error) {
      return {
        library: "unknown",
        chart_type: chartEl.tagName ? chartEl.tagName.toLowerCase() : "unknown",
        title: "",
        config_summary: `inspect error: ${normalizeText(error?.message || "unknown", 120)}`,
        series_summary: "",
        element_id: chartEl.id || chartEl.dataset?.pageCommentChartKey || "",
        canvas_id: chartEl.tagName === "CANVAS" ? chartEl.id || "" : "",
      };
    }
  }

  window.addEventListener("message", (event) => {
    if (event.source !== window) return;
    const msg = event.data;
    if (!msg || msg.source !== "page-comment-content" || msg.type !== "inspect-chart") return;
    const chartKey = msg.chartKey;
    const chartEl = chartKey
      ? document.querySelector(`[data-page-comment-chart-key="${chartKey}"]`)
      : null;
    const chartInfo = inspectChart(chartEl);
    window.postMessage({
      source: "page-comment-bridge",
      type: "inspect-chart-result",
      requestId: msg.requestId,
      chartInfo,
    }, "*");
  });

  window.postMessage({
    source: "page-comment-bridge",
    type: "bridge-ready",
  }, "*");
})();
