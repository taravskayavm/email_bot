@echo off
chcp 65001 >nul
setlocal enableextensions

set "REPO_DIR=C:\Users\med\Desktop\email_bot"
set "ENV_NAME=emailbot"
set "ACT1=D:\Anaconda\Scripts\activate.bat"
set "ACT2=D:\Anaconda\condabin\conda.bat"
set "LOG_DIR=%REPO_DIR%\logs"
set "ENTRY=%REPO_DIR%\email_bot.py"

if not exist "%REPO_DIR%" (
  echo [ERROR] Project folder not found: %REPO_DIR%
  pause
  exit /b 2
)

if not exist "%LOG_DIR%" mkdir "%LOG_DIR%"

cd /d "%REPO_DIR%"

:: Try to activate conda environment in-current-shell using CALL
if exist "%ACT1%" (
  echo [INFO] Activating via %ACT1%
  call "%ACT1%" %ENV_NAME%
) else if exist "%ACT2%" (
  echo [INFO] Activating via %ACT2%
  call "%ACT2%" activate %ENV_NAME%
) else (
  echo [WARN] activate.bat / conda.bat not found in expected places.
  echo        If conda is in PATH, try: conda activate %ENV_NAME%
)

echo.
where python 2>nul || echo [WARN] python not found in PATH
python --version 2>nul || echo [WARN] python --version failed

if not exist "%ENTRY%" (
  echo [ERROR] Entry file not found: %ENTRY%
  pause
  exit /b 3
)

echo [INFO] Running: python -u "%ENTRY%"
python -u "%ENTRY%" 1>>"%LOG_DIR%\run.out" 2>>"%LOG_DIR%\run.err"
echo [INFO] Process exited with code %ERRORLEVEL%

echo.
echo ===== last 40 lines of logs\run.err =====
powershell -NoProfile -Command "Get-Content -Path '%LOG_DIR%\run.err' -Tail 40 -ErrorAction SilentlyContinue"
echo =========================================
pause
endlocal