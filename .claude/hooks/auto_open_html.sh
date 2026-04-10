#!/bin/bash
# PostToolUse hook: auto-open .html files in browser after Write/Edit
INPUT=$(cat)
FILE=$(echo "$INPUT" | grep -o '"file_path" *: *"[^"]*"' | sed 's/.*"file_path" *: *"//;s/"$//')

if [ -n "$FILE" ] && echo "$FILE" | grep -qi '\.html$'; then
  # Convert to Windows path and open in default browser
  WIN_PATH=$(echo "$FILE" | sed 's|/|\\|g')
  cmd.exe /c start "" "$WIN_PATH" 2>/dev/null
fi
