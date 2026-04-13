from pathlib import Path


OUTPUT_DIR = Path(__file__).resolve().parent.parent / "outputs"


ECHARTS_HTML = """<!DOCTYPE html>
<html lang="zh-CN">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>PageComment ECharts Test</title>
<script src="https://cdn.jsdelivr.net/npm/echarts@5/dist/echarts.min.js"></script>
<style>
body { font-family: "Segoe UI", sans-serif; background: #f5f6fa; padding: 24px; }
.section { background: #fff; border-radius: 12px; padding: 20px; max-width: 960px; margin: 0 auto; box-shadow: 0 2px 10px rgba(0,0,0,.08); }
.chart-container { height: 420px; }
</style>
</head>
<body>
  <div class="section">
    <h2>渠道 ROI 趋势</h2>
    <p>用于验证 PageComment 对 ECharts 容器的点击识别和 bridge 元数据提取。</p>
    <div id="echartsRoot" class="chart-container"></div>
  </div>
  <script>
    const chart = echarts.init(document.getElementById('echartsRoot'));
    chart.setOption({
      title: { text: '渠道 ROI 趋势' },
      tooltip: { trigger: 'axis' },
      legend: { data: ['ROI', '新增用户'] },
      xAxis: { type: 'category', name: '日期', data: ['04-01', '04-02', '04-03', '04-04'] },
      yAxis: [
        { type: 'value', name: 'ROI (%)' },
        { type: 'value', name: '新增用户' }
      ],
      series: [
        { name: 'ROI', type: 'line', data: [110, 125, 118, 132] },
        { name: '新增用户', type: 'bar', yAxisIndex: 1, data: [1200, 1450, 1380, 1510] }
      ]
    });
  </script>
</body>
</html>
"""


PLOTLY_HTML = """<!DOCTYPE html>
<html lang="zh-CN">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>PageComment Plotly Test</title>
<script src="https://cdn.plot.ly/plotly-2.35.2.min.js"></script>
<style>
body { font-family: "Segoe UI", sans-serif; background: #f5f6fa; padding: 24px; }
.section { background: #fff; border-radius: 12px; padding: 20px; max-width: 960px; margin: 0 auto; box-shadow: 0 2px 10px rgba(0,0,0,.08); }
#plotlyRoot { height: 420px; }
</style>
</head>
<body>
  <div class="section">
    <h2>付费与广告收入对比</h2>
    <p>用于验证 PageComment 对 Plotly 容器的点击识别和 bridge 元数据提取。</p>
    <div id="plotlyRoot"></div>
  </div>
  <script>
    Plotly.newPlot('plotlyRoot', [
      { x: ['周一', '周二', '周三', '周四'], y: [320, 360, 340, 390], type: 'scatter', mode: 'lines+markers', name: '广告收入' },
      { x: ['周一', '周二', '周三', '周四'], y: [180, 210, 205, 230], type: 'bar', name: 'IAP 收入' }
    ], {
      title: { text: '付费与广告收入对比' },
      xaxis: { title: { text: '日期' } },
      yaxis: { title: { text: '收入 ($)' } }
    });
  </script>
</body>
</html>
"""


UNKNOWN_CANVAS_HTML = """<!DOCTYPE html>
<html lang="zh-CN">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>PageComment Unknown Canvas Test</title>
<style>
body { font-family: "Segoe UI", sans-serif; background: #f5f6fa; padding: 24px; }
.section { background: #fff; border-radius: 12px; padding: 20px; max-width: 960px; margin: 0 auto; box-shadow: 0 2px 10px rgba(0,0,0,.08); }
canvas { display: block; width: 100%; height: 320px; border-radius: 12px; background: #fff; }
</style>
</head>
<body>
  <div class="section">
    <h2>未知 Canvas 图表示例</h2>
    <p>用于验证 bridge 未识别图库时，仍能弹出“修改图表”入口。</p>
    <canvas id="unknownCanvas" width="900" height="320"></canvas>
  </div>
  <script>
    const canvas = document.getElementById('unknownCanvas');
    const ctx = canvas.getContext('2d');
    const values = [80, 120, 90, 150];
    const colors = ['#4a6cf7', '#2ecc71', '#f39c12', '#e74c3c'];
    ctx.clearRect(0, 0, canvas.width, canvas.height);
    ctx.font = '14px sans-serif';
    values.forEach((value, index) => {
      const x = 80 + index * 180;
      const y = canvas.height - value - 40;
      ctx.fillStyle = colors[index];
      ctx.fillRect(x, y, 90, value);
      ctx.fillStyle = '#333';
      ctx.fillText(`Q${index + 1}`, x + 30, canvas.height - 10);
    });
  </script>
</body>
</html>
"""


def write_fixture(filename: str, content: str) -> None:
    path = OUTPUT_DIR / filename
    path.write_text(content, encoding="utf-8")
    print(f"wrote {path}")


def main() -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    write_fixture("page_comment_echarts_test_page.html", ECHARTS_HTML)
    write_fixture("page_comment_plotly_test_page.html", PLOTLY_HTML)
    write_fixture("page_comment_unknown_canvas_test_page.html", UNKNOWN_CANVAS_HTML)


if __name__ == "__main__":
    main()
