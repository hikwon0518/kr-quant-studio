@echo off
chcp 65001 >nul 2>&1
title KR Quant Studio

cd /d "%~dp0"

echo [1/3] Checking Python...
python --version >nul 2>&1
if errorlevel 1 (
    echo ERROR: python not found. Install Python 3.14 first.
    pause
    exit /b 1
)

echo [2/3] Installing dependencies...
python -m uv sync --group dev --group data --group ui --group reports --group stats >nul 2>&1
if errorlevel 1 (
    echo ERROR: uv sync failed. Run "pip install uv" first.
    pause
    exit /b 1
)

echo [3/3] Starting Streamlit server...
echo.
echo   Local URL:  http://localhost:8501
echo   Press Ctrl+C to stop.
echo.

python -m uv run streamlit run src/krqs/ui/app.py --server.headless true
pause
