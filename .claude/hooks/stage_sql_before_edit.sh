#!/bin/bash
# PreToolUse hook: stage current .sql file before Edit, so post-edit diff shows prev vs new
INPUT=$(cat)
FILE=$(echo "$INPUT" | grep -o '"file_path" *: *"[^"]*"' | sed 's/.*"file_path" *: *"//;s/"$//')

if [ -n "$FILE" ] && echo "$FILE" | grep -qi '\.sql$'; then
  # Only stage if file exists and is tracked or staged
  if [ -f "$FILE" ]; then
    git add "$FILE"
  fi
fi
