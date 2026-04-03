# Amber 项目规则

## 文件组织规则

Amber 目录下分两类工作区：**日常任务** 和 **专题分析**。所有文件输出（包括 sql-assistant 产生的文件）都必须遵循以下规则，无需用户每次指定。

### 目录结构

```
Amber/
├── daily_tasks/                # 日常一次性查询任务
│   ├── sql/                    # SQL 查询文件
│   ├── data/                   # csv/json 数据输出
│   ├── reports/                # md 报告/分析笔记
│   └── scripts/                # py 脚本
│
├── {专题名}/                    # 专题分析（如 is_adx、ad_creative_regression）
│   ├── sql/                    # SQL 查询文件
│   ├── data/                   # csv/json 数据输出
│   ├── reports/                # md 报告/分析笔记
│   ├── scripts/                # py 脚本
│   └── projects/               # 子项目（如有），每个子项目保持同样的子目录结构
│       └── {子项目名}/
│           ├── sql/
│           ├── scripts/
│           └── outputs/        # html dashboard 等可视化产出
```

### 基目录选择规则

每次产生文件时，先确定「基目录」：

| 场景 | 基目录 |
|------|--------|
| 日常一次性查询、临时取数 | `daily_tasks/` |
| 围绕某个专题的分析任务 | `{专题名}/`（如 `is_adx/`） |
| 专题下某子项目的工作 | `{专题名}/projects/{子项目名}/` |
| 用户明确指定路径 | 用户指定的路径（最高优先级） |

### 文件路由规则

确定基目录后，按文件类型放入对应子目录：

| 文件类型 | 子目录 | 命名规则 |
|----------|--------|----------|
| `.sql` | `{base}/sql/` | `{product}_{topic}_{date_part}.sql` |
| `.csv` / `.json`（数据） | `{base}/data/` | 语义化命名或 `result_YYYYMMDD_HHMMSS.csv` |
| `.md`（报告/笔记） | `{base}/reports/` | 语义化命名 |
| `.py`（脚本） | `{base}/scripts/` | 语义化命名 |
| `.html`（dashboard） | `{base}/outputs/` | 语义化命名 |

### 命名约定

- **daily_tasks 内的文件**：使用日期前缀 `YYYY-MM-DD_{描述}`，便于按时间定位
- **专题内的文件**：使用语义化命名 `{product}_{topic}_{date_part}`
- 新建专题文件夹统一使用 `snake_case`

## 结果展示

- 涉及查询、统计、汇总、AB 对比或结果表输出的任务时，最终展示结果默认参考 `result-markdown-style` skill。
- 默认遵循：左侧先展示各指标 GAP，右侧再展示原值；原值按"指标 -> 分组/维度"展开。
- 若用户指定输出载体（如 HTML、飞书文档），按指定载体输出；未指定时默认在对话框中直接展示结果。
