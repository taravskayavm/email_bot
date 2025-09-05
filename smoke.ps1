Write-Host "Running gold HTML tests..."
$env:PYTHONWARNINGS = "ignore"
pytest -q tests/test_gold_dataset.py
Write-Host "Running PDF footnote tests..."
pytest -q tests/test_pdf_footnote_singleton.py
