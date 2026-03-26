@echo off
REM Forage Intel Daemon - Continuous Runner (Windows)
REM Runs non-stop until Ctrl+C
REM
REM Usage:
REM   run_daemon.bat              - Normal mode
REM   run_daemon.bat fast         - 4x faster
REM   run_daemon.bat aggressive   - 10x faster

cd /d "%~dp0"

echo ============================================
echo   FORAGE INTEL DAEMON - CONTINUOUS MODE
echo   Press Ctrl+C to stop
echo ============================================
echo.

REM Check Python
where python >nul 2>nul
if %errorlevel% neq 0 (
    echo ERROR: Python not found
    exit /b 1
)

REM Install deps
pip show requests >nul 2>nul
if %errorlevel% neq 0 pip install requests

REM Run daemon
if "%1"=="fast" (
    echo Starting FAST mode (4x speed)...
    python intel_daemon.py --fast
) else if "%1"=="aggressive" (
    echo Starting AGGRESSIVE mode (10x speed)...
    python intel_daemon.py --aggressive
) else if "%1"=="slow" (
    echo Starting SLOW mode (0.5x speed)...
    python intel_daemon.py --slow
) else (
    echo Starting NORMAL mode...
    python intel_daemon.py
)
