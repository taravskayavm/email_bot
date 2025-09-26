# Helper script to launch the aiogram bot on Windows (Anaconda PowerShell).

param()

Write-Host "Activating environment..." -ForegroundColor Cyan
conda activate emailbot

$env:PYTHONUNBUFFERED = "1"

Write-Host "Starting aiogram bot" -ForegroundColor Green
python -m emailbot.bot
