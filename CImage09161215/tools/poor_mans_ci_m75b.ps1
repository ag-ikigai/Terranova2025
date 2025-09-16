param(
  [string]$InputPack = ".\InputPack\TerraNova_Input_Pack_v10_0.xlsx",
  [string]$Currency  = "NAD",
  [double]$FxUsdToNad = 19.20,
  [int]$InjectionMonth = 1
)

$ErrorActionPreference = "Stop"
function Step($name, $cmd) {
  Write-Host "== $name ==" -ForegroundColor Cyan
  & $cmd
  if ($LASTEXITCODE -ne 0) { throw "Step failed: $name" }
}

# 0) Paths
$env:PYTHONPATH="C:\TerraNova\src;C:\TerraNova\baselines\TerraNova_main_up_to_M2\src"

# 1) M3 → M4
Step "M3 (baseline)" { .\.venv\Scripts\python.exe .\baselines\TerraNova_M3_FinancingEngine_v3.1.0\run.py run_m3 --input $InputPack --out ".\outputs" --currency $Currency }
Step "M4 (baseline)" { .\.venv\Scripts\python.exe .\baselines\TerraNova_M4_TaxEngine_v4.0.3\run.py run_m4 --input $InputPack --out ".\outputs" --currency $Currency }

# 2) Enrich TAX_PAYABLE (keeps validator strict & M6 happy)
Step "M4 enrich payable" { .\.venv\Scripts\python.exe .\tools\enrich_tax_payable.py .\outputs }

# 3) M5 → M6 (module runners)
$env:PYTHONPATH="C:\TerraNova\src"
Step "M5 runner" { .\.venv\Scripts\python.exe -c "from terra_nova.modules.m5_cash_flow.runner import run_m5; run_m5(r'.\outputs','$Currency')" }
Step "M6 runner" { .\.venv\Scripts\python.exe -c "from terra_nova.modules.m6_balance_sheet.runner import run_m6; run_m6(r'.\outputs','$Currency')" }

# 4) M7.R1 optimization -> artifacts + top selection
Step "M7.R1 runner" { .\.venv\Scripts\python.exe -c "from terra_nova.modules.m7_optimizer.runner import run_m7_r1; run_m7_r1(r'$InputPack', r'.\outputs', currency='$Currency')" }
Step "M7 freeze selection" { .\.venv\Scripts\python.exe .\tools\m7_freeze_selection.py .\outputs }

# 5) M7.5A wiring (junior schedule injection) – keep your existing call/signature
Step "M7.5A wiring" { .\.venv\Scripts\python.exe -c "from terra_nova.modules.m7_5_wiring.runner import run_m7_5; run_m7_5(r'.\outputs','$Currency', fx_usd_to_nad=$FxUsdToNad, injection_month=$InjectionMonth)" }

# 6) M7.5B rebuild (full NAD/USD statements using FX path)
# If your module path is different, adjust the import below.
$py = @"
try:
    from terra_nova.modules.m7_5b_financials.runner import run_m7_5b
except Exception:
    from terra_nova.modules.m7_5b.runner import run_m7_5b
run_m7_5b(r'.\outputs', '$Currency')
"@
Step "M7.5B rebuild" { .\.venv\Scripts\python.exe -c $py }

# 7) Validators
$env:PYTHONPATH="C:\TerraNova\src"
Step "Contracts validator (M2–M6 strict)" { .\.venv\Scripts\python.exe .\tools\validate_contracts.py .\outputs --strict-tax --include-m6 }
Step "M7.5B validator" { .\.venv\Scripts\python.exe .\tools\validate_m7_5b.py .\outputs }

# 8) Smoke tests
Step "Smoke: pipeline" { .\.venv\Scripts\python.exe -m unittest -v tests\smoke\test_pipeline_smoke.py }
Step "Smoke: M7.R1"     { .\.venv\Scripts\python.exe -m unittest -v tests\smoke\test_m7_smoke.py }
Step "Smoke: M7.5A"     { .\.venv\Scripts\python.exe -m unittest -v tests\smoke\test_m7_5_smoke.py }
Step "Smoke: M7.5B"     { .\.venv\Scripts\python.exe -m unittest -v tests\smoke\test_m7_5b_smoke.py }

Write-Host "`nAll steps completed successfully." -ForegroundColor Green
