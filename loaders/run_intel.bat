@echo off
REM Forage Intel Collector - Windows Batch Runner
REM Run: run_intel.bat [all|wikidata|sanctions|sec|worldbank|news|github]

cd /d "%~dp0"

echo ============================================
echo   FORAGE INTEL COLLECTOR
echo ============================================
echo.

REM Check Python
where python >nul 2>nul
if %errorlevel% neq 0 (
    echo ERROR: Python not found. Please install Python 3.8+
    exit /b 1
)

REM Install dependencies if needed
pip show requests >nul 2>nul
if %errorlevel% neq 0 (
    echo Installing dependencies...
    pip install requests
)

REM Run collector
if "%1"=="" (
    echo Running ALL collectors...
    python intel_collector.py --all --output intel_results.json
) else (
    echo Running %1 collector...
    python intel_collector.py --%1 --output intel_results.json
)

echo.
echo Done! Check intel_results.json for details.
pause
