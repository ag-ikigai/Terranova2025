<# Terra Nova quick CI smoke (M3→M5 + contracts + smoke tests)
   Run from repo root: powershell -ExecutionPolicy Bypass -File .\tools\ci_smoke.ps1
#>
$ErrorActionPreference = "Stop"
function Die([string]$msg, [int]$code=1) { Write-Host "CI FAILED: $msg" -ForegroundColor Red; exit $code }

$root    = Split-Path -Parent $MyInvocation.MyCommand.Path | Split-Path -Parent
$python  = Join-Path $root ".venv\Scripts\python.exe"; if (!(Test-Path $python)) { $python = "python" }
$out     = Join-Path $root "outputs"; New-Item -ItemType Directory -Force -Path $out | Out-Null

# M3
$env:PYTHONPATH = (Join-Path $root "src") + ";" + (Join-Path $root "baselines\TerraNova_main_up_to_M2\src")
& $python (Join-Path $root "baselines\TerraNova_M3_FinancingEngine_v3.1.0\run.py") run_m3 --input (Join-Path $root "InputPack\TerraNova_Input_Pack_v10_0.xlsx") --out $out --currency NAD
if ($LASTEXITCODE -ne 0) { Die "M3 failed with code $LASTEXITCODE" $LASTEXITCODE }

# M4
& $python (Join-Path $root "baselines\TerraNova_M4_TaxEngine_v4.0.3\run.py") run_m4 --input (Join-Path $root "InputPack\TerraNova_Input_Pack_v10_0.xlsx") --out $out --currency NAD
if ($LASTEXITCODE -ne 0) { Die "M4 failed with code $LASTEXITCODE" $LASTEXITCODE }

# M5 (module)
$env:PYTHONPATH = (Join-Path $root "src")
& $python -c "from terra_nova.modules.m5_cash_flow.runner import run_m5; run_m5(r'$out','NAD')"
if ($LASTEXITCODE -ne 0) { Die "M5 failed with code $LASTEXITCODE" $LASTEXITCODE }

# Contracts (M2)
& $python (Join-Path $root "tools\validate_contracts.py") $out
if ($LASTEXITCODE -ne 0) { Die "Contracts validator failed with code $LASTEXITCODE" $LASTEXITCODE }

# Smoke tests
& $python -m unittest -v tests\smoke\test_pipeline_smoke.py
if ($LASTEXITCODE -ne 0) { Die "Unit tests failed with code $LASTEXITCODE" $LASTEXITCODE }

Write-Host "CI SMOKE PASSED" -ForegroundColor Green
exit 0
