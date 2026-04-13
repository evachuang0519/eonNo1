@echo off
title 日照接送管理系統
cd /d C:\project\eonNo1
echo.
echo  ╔══════════════════════════════════════════╗
echo  ║       日照接送管理系統  啟動中...        ║
echo  ║       http://localhost:8000              ║
echo  ╚══════════════════════════════════════════╝
echo.
python backend\app.py
pause
