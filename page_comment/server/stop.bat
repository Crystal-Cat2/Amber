@echo off
:: 停止 PageComment 服务器

cd /d "%~dp0"

:: 找监听 18080 的进程并杀掉
set FOUND=0
for /f "tokens=5" %%a in ('netstat -ano ^| findstr ":18080 " ^| findstr "LISTENING"') do (
    taskkill /f /pid %%a >nul 2>&1
    echo 服务器已停止 (PID %%a)
    set FOUND=1
)

if %FOUND%==0 echo 未找到运行中的服务器
