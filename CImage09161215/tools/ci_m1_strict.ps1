# C:\TerraNova\tools\ci_m1_strict.ps1
param(
  [string]$InputXlsx = "C:\TerraNova\InputPack\TerraNova_Input_Pack_v10_0.xlsx",
  [string]$OutDir    = "C:\TerraNova\outputs"
)

$ErrorActionPreference = "Stop"

# Fixed prelude (same pattern as M0 strict)
.\.venv\Scripts\Activate.ps1
$env:PYTHONPATH="C:\TerraNova\src"

# Run M1 from main path (no baselines)
.\.venv\Scripts\python.exe -c "from terra_nova.modules.m1_operational_engines.runner import run_m1; run_m1(r'$InputXlsx', r'$OutDir', currency='NAD')"

# Validate strict contract & logic
.\.venv\Scripts\python.exe .\tools\validate_m1_strict.py --input $InputXlsx --out $OutDir
if ($LASTEXITCODE -ne 0) { throw 'CI M1 STRICT: FAIL' }

Write-Host 'CI M1 STRICT: PASS' -ForegroundColor Green
