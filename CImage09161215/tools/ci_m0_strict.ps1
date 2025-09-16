param(
  [string]$InputXlsx = "C:\TerraNova\InputPack\TerraNova_Input_Pack_v10_0.xlsx",
  [string]$OutDir    = "C:\TerraNova\outputs"
)
$ErrorActionPreference = "Stop"

# Ensure venv present and src is importable
if (-not (Test-Path ".\.venv\Scripts\python.exe")) { throw "venv missing" }
$env:PYTHONPATH="C:\TerraNova\src"
.\.venv\Scripts\Activate.ps1

# 1) Run M0 from main path (no baselines)
.\.venv\Scripts\python.exe -m terra_nova.modules.m0_setup.runner `
  --input $InputXlsx `
  --out   $OutDir
if ($LASTEXITCODE -ne 0) { throw "M0 runner failed with exit code $LASTEXITCODE" }

# 2) Strict validation: outputs must equal Input Pack transforms
.\.venv\Scripts\python.exe .\tools\m0_strict_check.py `
  --input $InputXlsx `
  --out   $OutDir
if ($LASTEXITCODE -ne 0) { throw "M0 STRICT VALIDATION FAILED (see messages above)" }

Write-Host "CI M0 STRICT: PASS" -ForegroundColor Green
