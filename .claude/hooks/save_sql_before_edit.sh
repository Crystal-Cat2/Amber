#!/bin/bash
# PreToolUse hook: 确保 SQL 文件有 diff 基线
INPUT=$(cat)
FILE=$(echo "$INPUT" | grep -o '"file_path" *: *"[^"]*"' | sed 's/.*"file_path" *: *"//;s/"$//')

if [ -n "$FILE" ] && echo "$FILE" | grep -qi '\.sql$'; then
  if [ -f "$FILE" ]; then
    if git log --oneline -1 -- "$FILE" > /dev/null 2>&1; then
      # 已有 commit 历史，正常 stage（原有逻辑）
      git add "$FILE"
    else
      # 新文件，先 commit 初版作为 diff 基线
      git add "$FILE"
      git commit -m "baseline: $(basename "$FILE")" -- "$FILE"
    fi
  fi
fi
