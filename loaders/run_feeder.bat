@echo off
REM Forage Graph Continuous Feeder - Run in background
cd /d "%~dp0"
python continuous_feeder.py > feeder.log 2>&1
echo Feeder started. Check feeder.log for progress.