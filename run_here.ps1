# === EBOT RUN FROM SUBDIR ===
Set-Location -Path $PSScriptRoot
conda activate emailbot
$env:PYTHONPATH="..;$env:PYTHONPATH"
$env:CANDIDATES="bot,services,pipelines,mailer"
python -m dotenv -f .\.env run -- python ..\email_bot.py