
@echo off
setlocal enabledelayedexpansion

REM Conda 环境名称
set CONDA_ENV=download

REM Conda 安装路径（根据您的安装位置进行调整）
set CONDA_PATH=D:\anaconda3

REM 设置最大重启次数
set MAX_RESTARTS=50
set restart_count=0

REM 激活 Conda 环境
call "%CONDA_PATH%\Scripts\activate.bat" %CONDA_ENV%
if %ERRORLEVEL% neq 0 (
    echo Failed to activate Conda environment. Please check the environment name and Conda installation.
    exit /b 1
)

:restart_loop
if %restart_count% lss %MAX_RESTARTS% (
    set /a restart_count+=1
    echo Starting Python script... (Attempt !restart_count!)
    python climate_data_downloader.py
    if %ERRORLEVEL% equ 0 (
        echo Python script completed successfully.
        goto :end
    ) else (
        echo Python script exited with code %ERRORLEVEL%. Restarting...
        timeout /t 10 /nobreak
        goto :restart_loop
    )
) else (
    echo Maximum restart attempts reached. Please check the script and logs.
)

:end
echo Script execution completed.
pause