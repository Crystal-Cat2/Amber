#!/bin/bash
# PostToolUse hook: SQL 执行成功后，检测 diff 并指令 agent 提取业务规则
INPUT=$(cat)

# 只处理 Bash 工具
TOOL=$(echo "$INPUT" | python3 -c "import sys,json;print(json.load(sys.stdin).get('tool_name',''))" 2>/dev/null)
[ "$TOOL" != "Bash" ] && exit 0

# 只处理 bq_runtime_cli run 且成功的情况
STDOUT=$(echo "$INPUT" | python3 -c "import sys,json;print(json.load(sys.stdin).get('tool_result',{}).get('stdout',''))" 2>/dev/null)
echo "$STDOUT" | grep -q '"success": true' || exit 0

CMD=$(echo "$INPUT" | python3 -c "import sys,json;print(json.load(sys.stdin).get('tool_input',{}).get('command',''))" 2>/dev/null)
echo "$CMD" | grep -q 'bq_runtime_cli' || exit 0
echo "$CMD" | grep -q ' run ' || exit 0

# 提取 --sql-file 路径
SQL_FILE=$(echo "$CMD" | grep -oP '(?<=--sql-file ")[^"]+' 2>/dev/null || echo "$CMD" | grep -oP "(?<=--sql-file ')[^']+" 2>/dev/null || echo "$CMD" | grep -oP '(?<=--sql-file )\S+' 2>/dev/null)
[ -z "$SQL_FILE" ] && exit 0

# 检查是否有 diff（对比最近一次 commit）
DIFF=$(git diff HEAD -- "$SQL_FILE" 2>/dev/null)
[ -z "$DIFF" ] && exit 0

# 有 diff，输出 systemMessage 指令 agent 启动子 agent 处理
printf '{"systemMessage": "SQL 文件 %s 在确认前经过修改。请启动一个子 agent（subagent_type=general-purpose）执行以下任务：\\n1. 运行 git diff HEAD -- %s 读取变更内容\\n2. 分析 diff 中是否涉及业务规则变更（计算口径、JOIN 方式、去重逻辑、ID 选择等）\\n3. 如果涉及，读取并更新 C:/Users/ASUS/.claude/skills/sql-assistant/references/business-rules.md\\n4. 完成后简要告知用户更新了什么内容"}\n' "$SQL_FILE" "$SQL_FILE"
