Param(
  [Parameter(Mandatory=$false)][string]$PdfPath = "",
  [Parameter(Mandatory=$false)][string]$ZipPath = "",
  [Parameter(Mandatory=$false)][string[]]$Expect = @(),
  [switch]$AllowNumeric = $false,
  [switch]$VerboseModules = $false
)

Write-Host "Activating conda env: emailbot"
conda activate emailbot | Out-Null

$repo = Split-Path -Parent $MyInvocation.MyCommand.Path
$repo = Split-Path -Parent $repo  # go to project root
Set-Location $repo

$cmd = "python scripts/smoke_sanity.py"
if ($PdfPath) { $cmd += " --pdf `"$PdfPath`"" }
if ($ZipPath) { $cmd += " --zip `"$ZipPath`"" }
foreach ($e in $Expect) { $cmd += " --expect `"$e`"" }
if ($AllowNumeric) { $cmd += " --allow-numeric" }
if ($VerboseModules) { $cmd += " -v" }

Write-Host "Running: $cmd"
Invoke-Expression $cmd
