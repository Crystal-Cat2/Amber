#!/bin/bash
# PostToolUse hook: SQL 文件写入后自动 commit
INPUT=$(cat)
FILE=$(echo "$INPUT" | python3 -c "
import sys,json
d=json.load(sys.stdin)
r=d.get('tool_result',{})
# Edit 返回 filePath，Write 也返回 filePath
fp=r.get('filePath','')
if not fp:
    # 从 tool_input 取
    fp=d.get('tool_input',{}).get('file_path','')
print(fp)
" 2>/dev/null)

[ -z "$FILE" ] && exit 0
echo "$FILE" | grep -qi '\.sql$' || exit 0
[ -f "$FILE" ] || exit 0

cd D:/Work/Amber || exit 0
git add "$FILE" 2>/dev/null
git diff --cached --quiet && exit 0
BASENAME=$(basename "$FILE")
git commit -m "sql: update $BASENAME" 2>/dev/null
