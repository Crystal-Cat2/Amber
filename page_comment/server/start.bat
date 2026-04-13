@echo off
:: 后台启动 PageComment 服务器（无窗口）
:: 日志: logs\server.log | 停止: stop.bat

cd /d "%~dp0"

:: 检查端口是否已被占用
netstat -ano | findstr ":18080 " | findstr "LISTENING" >nul 2>&1
if %errorlevel%==0 (
    echo 端口 18080 已被占用，服务器可能已在运行
    exit /b 0
)

:: 用 pythonw 后台启动（无窗口）
start "" /b pythonw server.py
timeout /t 2 /nobreak >nul

:: 验证启动成功
netstat -ano | findstr ":18080 " | findstr "LISTENING" >nul 2>&1
if %errorlevel%==0 (
    echo 服务器已启动
    echo 日志: %~dp0logs\server.log
) else (
    echo 启动失败，检查 logs\server.log
)
