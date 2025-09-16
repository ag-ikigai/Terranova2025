param(
  [string]$InputXlsx = "C:\TerraNova\InputPack\TerraNova_Input_Pack_v10_0.xlsx",
  [string]$OutDir    = "C:\TerraNova\outputs",
  [string]$Currency  = "NAD"
)

$ErrorActionPreference = "Stop"

Write-Host "[M3 STRICT] Start" -ForegroundColor Cyan

# Run M3 from src
.\.venv\Scripts\python.exe -c "from terra_nova.modules.m3_financing.runner import run_m3; run_m3(r'$InputXlsx', r'$OutDir', currency=r'$Currency')"
if ($LASTEXITCODE -ne 0) { throw "M3 run failed" }

# Validate
.\.venv\Scripts\python.exe .\tools\validate_m3.py $OutDir
if ($LASTEXITCODE -ne 0) { throw "M3 validation failed" }

Write-Host "CI M3 STRICT: PASS" -ForegroundColor Green