<#
  Cleanup helper for Windows PowerShell
  Usage (Anaconda PowerShell):
    cd /d D:\email_bot
    conda activate emailbot
    .\scripts\cleanup.ps1
#>
param()

Write-Host "Removing common build artifacts (pycache, .pytest_cache, dist, build)..." -ForegroundColor Cyan
$items = @(".\__pycache__", ".\.pytest_cache", ".\dist", ".\build", ".\emailbot.egg-info")
foreach ($it in $items) {
    if (Test-Path $it) {
        try {
            Remove-Item -LiteralPath $it -Recurse -Force -ErrorAction Stop
            Write-Host "Removed $it"
        } catch {
            Write-Warning "Failed to remove $it : $_"
        }
    }
}
Write-Host "Done."
