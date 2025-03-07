@echo off
echo Iniciando servidor...
cd %~dp0

REM Intenta usar Python desde Windows Store (ubicación típica)
set PYTHON_CMD=python
where %PYTHON_CMD% >nul 2>nul
if %ERRORLEVEL% NEQ 0 (
    set PYTHON_CMD=%LOCALAPPDATA%\Microsoft\WindowsApps\python.exe
)

echo Usando Python: %PYTHON_CMD%
%PYTHON_CMD% app.py
pause 