@echo off
cd /d "%~dp0"
title Yemat2 Access Log Viewer
powershell -NoProfile -ExecutionPolicy Bypass -File ".\view_access_log.ps1"
