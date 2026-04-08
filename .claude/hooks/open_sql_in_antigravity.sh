#!/bin/bash
# Hook script: auto-open .sql files in Antigravity after Write/Edit
# Reads hook stdin JSON, extracts file_path, opens if .sql

grep -o '"file_path":"[^"]*"' | sed 's/"file_path":"//;s/"$//' | {
  read -r f
  if [ -n "$f" ] && echo "$f" | grep -qi '\.sql$'; then
    powershell -Command "& 'D:\Code\Antigravity\bin\antigravity.cmd' --reuse-window '$f'"
  fi
}
