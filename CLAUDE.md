# Amber 项目规则

## 项目概况

- 数据分析工作区，主要工具：BigQuery、飞书（Lark）、Python
- 环境：Windows 11 + Git Bash（使用 Unix shell 语法）
- 编辑器：Antigravity（VSCode fork）
- Python 可用，pandas 未安装，数据导出用 csv 模块

## 沟通偏好

- 用中文回复
- 简洁直接，不要冗余总结
- 做了什么改动要简短告知，不要静默操作

## 飞书文档

- 创建文档默认在此文件夹下：https://xwbo3y4nxr.feishu.cn/drive/folder/O2iLfgRoGlcUfbdKVw7cioIRnqd
- 修改文档优先搜索飞书云文档（用 lark-cli），不要搜索本地文件
- 本地存储：code、原始数据（csv/json）、html；飞书存储：文档、报告

## 工作流规则

- Write 工具和 Antigravity 打开互斥，不要同时使用
- 实验操作完成后，主动提醒是否需要同步写入飞书追踪表

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
