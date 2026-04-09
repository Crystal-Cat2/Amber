#!/bin/bash
# PostToolUse hook: open .sql in Antigravity after Write/Edit
# Write: git add to establish baseline; Edit: just open (PreToolUse already staged prev version)

INPUT=$(cat)
FILE=$(echo "$INPUT" | grep -o '"file_path" *: *"[^"]*"' | sed 's/.*"file_path" *: *"//;s/"$//')
TOOL=$(echo "$INPUT" | grep -o '"tool_name" *: *"[^"]*"' | sed 's/.*"tool_name" *: *"//;s/"$//')

if [ -n "$FILE" ] && echo "$FILE" | grep -qi '\.sql$'; then
  if [ "$TOOL" = "Write" ]; then
    git add "$FILE"
  fi
  powershell -Command "& 'D:\Code\Antigravity\bin\antigravity.cmd' --reuse-window '$FILE'"
fi
