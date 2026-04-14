@echo off
title MyTime MQTT Listener

cd /d "%~dp0"

:START
echo ==========================================
echo Starting MyTime MQTT Listener...
echo %date% %time%
echo ==========================================

npm run start:mqtt

echo.
echo Listener stopped or crashed.
echo Restarting in 5 seconds...
timeout /t 5 /nobreak >nul

goto START