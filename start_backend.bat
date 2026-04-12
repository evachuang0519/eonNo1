@echo off
chcp 65001 >nul
title 日照交通服務系統 - Python Backend (Port 8000)
echo.
echo  ================================================
echo   日照交通服務系統  Python Backend
echo   http://localhost:8000
echo  ================================================
echo.

cd /d "%~dp0backend"

:: Check Python
python --version >nul 2>&1
if errorlevel 1 (
    echo [ERROR] Python not found. Please install Python 3.10+
    pause
    exit /b 1
)

:: Check dependencies
python -c "import flask, pg8000, apscheduler" >nul 2>&1
if errorlevel 1 (
    echo [INFO] Installing dependencies...
    pip install flask pg8000 requests apscheduler
)

echo [INFO] Starting backend server...
echo [INFO] Press Ctrl+C to stop
echo.
python app.py
pause
