"""PageComment 服务器配置"""
import os

PORT = 18080
HOST = "localhost"
AMBER_ROOT = os.environ.get("AMBER_ROOT", "D:/Work/Amber")
CLAUDE_MODEL = "claude-haiku-4-5-20251001"

# Claude CLI 配置
# 直接用 claude.exe 完整路径，避免 cmd /c 弹出黑窗口。
# 如需覆盖，可设置环境变量 CLAUDE_CLI。
CLAUDE_CLI = os.environ.get("CLAUDE_CLI", "").split() or [r"C:\Users\ASUS\.local\bin\claude.exe"]
CLI_TIMEOUT = int(os.environ.get("CLI_TIMEOUT", "180"))
# Windows 上 Claude Code 需要 git-bash
CLAUDE_CODE_GIT_BASH = os.environ.get(
    "CLAUDE_CODE_GIT_BASH_PATH",
    r"D:\Code\Git\bin\bash.exe",
)

# .env 文件加载（简易版，不依赖 python-dotenv）
_env_file = os.path.join(os.path.dirname(__file__), ".env")
if os.path.exists(_env_file):
    with open(_env_file, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                k, v = line.split("=", 1)
                v = v.strip().strip('"').strip("'")
                os.environ.setdefault(k.strip(), v)

# 支持 ANTHROPIC_AUTH_TOKEN (cc-switch) 或 ANTHROPIC_API_KEY
ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_AUTH_TOKEN") or os.environ.get("ANTHROPIC_API_KEY", "")
ANTHROPIC_BASE_URL = os.environ.get("ANTHROPIC_BASE_URL", None)
