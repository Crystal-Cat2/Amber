#!/bin/bash
# PostToolUse hook: 工具执行失败时，指令 agent 记录错误到 memory
INPUT=$(cat)

# 解析工具名
TOOL=$(echo "$INPUT" | python3 -c "import sys,json;print(json.load(sys.stdin).get('tool_name',''))" 2>/dev/null)
[ -z "$TOOL" ] && exit 0

# 检测失败：Bash 看 exit code，其他工具看 is_error
IS_ERROR="false"
if [ "$TOOL" = "Bash" ]; then
  EXIT_CODE=$(echo "$INPUT" | python3 -c "import sys,json;print(json.load(sys.stdin).get('tool_result',{}).get('exit_code',0))" 2>/dev/null)
  [ "$EXIT_CODE" != "0" ] && IS_ERROR="true"
else
  IS_ERROR=$(echo "$INPUT" | python3 -c "import sys,json;print(str(json.load(sys.stdin).get('tool_result',{}).get('is_error',False)).lower())" 2>/dev/null)
fi

[ "$IS_ERROR" != "true" ] && exit 0

# 排除已知的正常失败（hook 拦截、用户拒绝等）
STDERR=$(echo "$INPUT" | python3 -c "import sys,json;print(json.load(sys.stdin).get('tool_result',{}).get('stderr','')[:500])" 2>/dev/null)
echo "$STDERR" | grep -q "hook error" && exit 0
echo "$STDERR" | grep -q "user denied" && exit 0

# 提取错误摘要
ERROR_SUMMARY=$(echo "$INPUT" | python3 -c "
import sys,json
d=json.load(sys.stdin)
r=d.get('tool_result',{})
err=r.get('stderr','') or r.get('content','') or str(r)
# 转义换行和引号
err=err[:500].replace('\\\\','\\\\\\\\').replace('\"','\\\\\"').replace('\\n','\\\\n')
print(err)
" 2>/dev/null)

printf '{"systemMessage": "工具 %s 执行失败。请启动一个子 agent（subagent_type=general-purpose）执行以下任务：\\n1. 分析错误原因：%s\\n2. 判断是否为可避免的重复错误\\n3. 如果是，读取 C:/Users/ASUS/.claude/projects/D--Work-Amber/memory/ 下的相关 feedback 文件，追加或新建记录\\n4. 如果涉及 skill 用法错误，同时更新对应的 skill 文档\\n5. 完成后简要告知用户记录了什么"}\n' "$TOOL" "$ERROR_SUMMARY"
