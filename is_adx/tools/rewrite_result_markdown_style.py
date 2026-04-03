from pathlib import Path


FILES = {
    Path(r"C:\Users\ASUS\.codex\skills\result-markdown-style\SKILL.md"): """---
name: result-markdown-style
description: 用于数据查询、统计、汇总、AB对比或结果表输出类任务，当最终结果需要在对话、HTML或文档中结构化展示时使用。默认按 GAP 在左、原值在右的方式展示结果，原值按指标分组或业务维度展开。
---

# result-markdown-style

用于数据结果最终展示的默认 skill。它只作用在结果展示阶段，不干预查询、清洗、计算过程。

## 核心规则

- 左侧先展示 GAP。
- 右侧再展示原值。
- 原值部分永远按“指标在外层，分组或其他业务维度在内层”展开。
- 行维度不固定，每次按任务语义决定。

## 输出载体优先级

- 用户明确要求输出到 HTML，就把这套结构写进 HTML。
- 用户明确要求输出到飞书文档或其他文档，就把这套结构写进对应文档。
- 用户明确要求其他输出载体时，按指定载体输出。
- 如果用户没有指定输出载体，默认直接在对话框里展示结果。

## 固定结构

- 左侧是 GAP 区。
- 右侧是原值区。
- GAP 区下永远是具体指标，例如展示、收入、eCPM。
- 原值区下永远是“指标 -> 分组/维度”的展开方式，例如：
  - `展示 -> no_is_adx, have_is_adx`
  - `收入 -> no_is_adx, have_is_adx`
  - `eCPM -> no_is_adx, have_is_adx`

## 行维度规则

- 行维度不固定。
- 行维度通常来自用户最关心的分析对象，常见是 1 到 3 层。
- 常见层级示例：
  - 平台 -> 广告位
  - 平台 -> 广告位 -> 日期
  - 国家 -> 平台 -> 广告位
- 如果同时存在汇总和明细，汇总行放前，明细行放后。

## 参考文件

- 读取 [references/layout-principles.md](references/layout-principles.md) 看固定布局原则。
- 读取 [references/templates-summary.md](references/templates-summary.md) 看汇总型模板。
- 读取 [references/templates-detail.md](references/templates-detail.md) 看汇总加明细模板。
- 读取 [references/templates-row-hierarchy.md](references/templates-row-hierarchy.md) 看行维度如何组织。
- 读取 [references/output-targets.md](references/output-targets.md) 看不同输出载体的处理方式。

## 使用时机

遇到以下任务时，在最终展示结果阶段默认使用本 skill：

- 查询某项数据后需要展示结果
- 统计指标后需要展示结果
- 汇总分析结果后需要展示结果
- 做 AB 对比后需要展示结果
- 需要输出结构化结果表

即使用户没有显式说“请展示”，只要任务本质上是让系统查数据并返回结果，就默认需要按本 skill 组织最终展示。
""",
    Path(r"C:\Users\ASUS\.codex\skills\result-markdown-style\references\layout-principles.md"): """# 布局原则

这些规则在所有结果展示场景里都保持不变。

## 固定列顺序

1. 行标签区
2. GAP 区
3. 原值区

## GAP 区

- GAP 永远在原值左侧。
- GAP 下永远是具体指标。
- 常见顺序：
  - 展示
  - 收入
  - eCPM

## 原值区

- 原值永远按“指标优先”展开。
- 每个指标下面再展开实验分组或业务维度。
- 例如：
  - `展示 -> no_is_adx, have_is_adx`
  - `收入 -> no_is_adx, have_is_adx`
  - `eCPM -> no_is_adx, have_is_adx`

## 不允许变化的部分

- 不能把原值放到 GAP 前面。
- 不能默认按“分组优先、指标在后”展开。
- 不管行维度怎么变，列结构都必须保持“左 GAP、右原值”。
""",
    Path(r"C:\Users\ASUS\.codex\skills\result-markdown-style\references\templates-summary.md"): """# 汇总型模板

适用于时间段汇总、平台汇总、广告位汇总等不需要逐行明细展开的结果。

## 模板 A：单时间段 + 平台分块

适用场景：

- 单个时间段汇总
- AB 对比
- 平台拆分

结构示意：

```markdown
| 时间段 | GAP=have/no-1 |  |  | 指标一 |  | 指标二 |  | 指标三 |  |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
|  | GAP-指标一 | GAP-指标二 | GAP-指标三 | 指标一-分组A | 指标一-分组B | 指标二-分组A | 指标二-分组B | 指标三-分组A | 指标三-分组B |
| 平台A |  |  |  |  |  |  |  |  |  |
| 行一 | ... |
| 行二 | ... |
| 平台B |  |  |  |  |  |  |  |  |  |
| 行一 | ... |
| 行二 | ... |
```

## 模板 B：多时间段上下排列

适用场景：

- 两个或多个时间段
- 同一套指标结构重复展示

结构示意：

```markdown
### 时间段一

[汇总表]

### 时间段二

[汇总表]
```

## 使用说明

- 平台、日期段等块级标签只负责分段，不打乱列结构。
- 所有时间段都保持同一套指标顺序和分组顺序。
""",
    Path(r"C:\Users\ASUS\.codex\skills\result-markdown-style\references\templates-detail.md"): """# 明细型模板

适用于“先给汇总，再展开日期或层级明细”的结果。

## 模板：汇总行 + 明细行

适用场景：

- 广告位汇总后继续挂日期明细
- 国家汇总后继续挂版本明细
- 平台汇总后继续挂渠道明细

结构示意：

```markdown
| 行标签一 | 行标签二 | GAP=have/no-1 |  |  | 指标一 |  | 指标二 |  | 指标三 |  |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 主分组 |  | GAP-指标一 | GAP-指标二 | GAP-指标三 | 指标一-分组A | 指标一-分组B | 指标二-分组A | 指标二-分组B | 指标三-分组A | 指标三-分组B |
| 汇总行 |  | ... |
| 明细行一 | 子维度一 | ... |
| 明细行二 | 子维度二 | ... |
```

## 优先规则

- 先放汇总行，再放该汇总行对应的明细行。
- 明细行和汇总行保持同样的列结构。
- 如果用户想要更像表格软件的多层表头，且载体允许，可以改用 HTML table。
""",
    Path(r"C:\Users\ASUS\.codex\skills\result-markdown-style\references\templates-row-hierarchy.md"): """# 行维度组织规则

行维度不固定，必须根据任务语义决定。

## 选择规则

1. 先放用户最关心的主分析对象。
2. 再放次级拆分维度。
3. 如果用户要看明细，时间通常放在更下一层。
4. 如果同时有汇总和明细，汇总在前，明细在后。

## 常见主分析对象

- 平台
- 广告位
- 国家
- 渠道
- 版本
- 日期段

## 常见层级示例

- `平台 -> 广告位`
- `平台 -> 广告位 -> 日期`
- `国家 -> 平台 -> 广告位`
- `日期段 -> 平台 -> 广告位`

## 有歧义时

- 列结构保持不变。
- 只在“行维度顺序”会明显影响可读性时，最多追问一次。
- 如果拿不到澄清，优先选择最符合用户主比较目标的层级。
""",
    Path(r"C:\Users\ASUS\.codex\skills\result-markdown-style\references\output-targets.md"): """# 输出载体规则

## 对话框

- 这是默认输出载体。
- 用户没有指定其他载体时，直接在对话框里展示结果。
- 先给简短结论，再给结构化结果表。

## HTML

- 用户明确要求写成 HTML 时，保持同样的信息结构。
- HTML 可以使用合并表头。
- 即使 HTML 更灵活，也不能改变“左 GAP、右原值”的结构。

## 文档

- 用户明确要求写到飞书文档或其他文档时，按同样结构写入文档。
- 如果文档系统支持更复杂的表格，只能用于提升可读性，不能改变结构顺序。

## 未指定载体

- 默认认为用户要直接看结果。
- 不能只把结果藏到文件里而不在对话框展示。
""",
    Path(r"D:\Work\Amber\AGENTS.md"): """# Amber 项目规则

## 结果展示

- 涉及查询、统计、汇总、AB 对比或结果表输出的任务时，最终展示结果默认参考 `result-markdown-style` skill。
- 默认遵循：左侧先展示各指标 GAP，右侧再展示原值；原值按“指标 -> 分组/维度”展开。
- 若用户指定输出载体（如 HTML、飞书文档），按指定载体输出；未指定时默认在对话框中直接展示结果。
""",
}


def main() -> None:
    for path, content in FILES.items():
        path.write_text(content, encoding="utf-8", newline="\n")
        print(f"WROTE {path}")


if __name__ == "__main__":
    main()
