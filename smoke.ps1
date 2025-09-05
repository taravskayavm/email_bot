Write-Host "Running gold HTML tests..."
$env:PYTHONWARNINGS = "ignore"
pytest -q tests/test_gold_dataset.py
