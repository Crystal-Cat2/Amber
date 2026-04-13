# PageComment

Chrome Extension + 本地服务器，在任意页面上选中文字发表评论，AI 自动处理。

对 Amber 项目的 dashboard，能追溯到源 Python 脚本和 CSV 数据，从源头修改后重新生成 HTML。

## 安装

### 1. 服务器

```bash
cd D:/Work/Amber/page_comment/server
pip install -r requirements.txt
```

创建 `.env` 文件：
```
ANTHROPIC_API_KEY=sk-ant-xxx
```

启动：
```bash
python server.py
```

### 2. Chrome Extension

1. 打开 `chrome://extensions/`
2. 开启「开发者模式」
3. 点击「加载已解压的扩展程序」→ 选择 `page_comment/extension/` 目录
4. 在扩展详情页开启「允许访问文件网址」（用于 file:// 页面）

## 使用

1. 确保服务器在运行（`python server.py`）
2. 在任意页面选中文字 → 出现「评论」按钮
3. 点击按钮 → 输入评论或修改指令 → Ctrl+Enter 或点击「提交」
4. AI 处理后显示结果，如果修改了源文件会自动 reload 页面

## 页面类型处理

| 页面类型 | 处理方式 |
|---------|---------|
| Amber 本地 dashboard (file://) | 追溯到脚本+CSV → 修改源文件 → 重新生成 HTML → reload |
| PageDoc 页面 | AI 回答问题 |
| 其他页面 | AI 回答问题 |
