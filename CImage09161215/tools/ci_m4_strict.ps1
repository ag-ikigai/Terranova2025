param(
  [string]$InputXlsx = "C:\TerraNova\InputPack\TerraNova_Input_Pack_v10_0.xlsx",
  [string]$OutDir    = "C:\TerraNova\outputs",
  [string]$Currency  = "NAD"
)

$ErrorActionPreference = "Stop"
Write-Host "[M4 STRICT] Start" -ForegroundColor Cyan

# Run M4 from src
.\.venv\Scripts\python.exe -c "from terra_nova.modules.m4_tax.runner import run_m4; run_m4(r'$InputXlsx', r'$OutDir', currency=r'$Currency')"
if ($LASTEXITCODE -ne 0) { throw "M4 run failed" }

# Validate
.\.venv\Scripts\python.exe .\tools\validate_m4.py $OutDir
if ($LASTEXITCODE -ne 0) { throw "M4 validation failed" }

Write-Host "CI M4 STRICT: PASS" -ForegroundColor Green