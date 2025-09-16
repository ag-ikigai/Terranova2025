# tools\ci_full_m3_to_m7_5b.ps1
param(
  [string]$InputPack = ".\InputPack\TerraNova_Input_Pack_v10_0.xlsx",
  [string]$OutDir    = ".\outputs",
  [string]$Currency  = "NAD",
  [switch]$StopOnError
)

$ErrorActionPreference = "Stop"
$ts = (Get-Date).ToString("yyyyMMdd_HHmm")
Write-Host "== TerraNova CI (M3→M7.5B) :: $ts =="

# Optional: quick backup (fast, excludes .venv/.git)
try {
  if (Test-Path "C:\TerraNova") {
    $bk = "C:\TerraNova_backups\TerraNova_$ts"
    New-Item -ItemType Directory -Force -Path $bk | Out-Null
    robocopy "C:\TerraNova" $bk /MIR /XD .venv __pycache__ .git workspace drops /XF *.pyc *.pyo | Out-Null
    Write-Host "[CI] Backup created at $bk"
  }
} catch { Write-Warning "[CI] Backup step failed: $($_.Exception.Message)" }

# 0) Make sure PYTHONPATH points to src only after M4
$env:PYTHONPATH="C:\TerraNova\src;C:\TerraNova\baselines\TerraNova_main_up_to_M2\src"

# 1) M3 → M4
.\.venv\Scripts\python.exe ".\baselines\TerraNova_M3_FinancingEngine_v3.1.0\run.py" run_m3 --input $InputPack --out $OutDir --currency $Currency
.\.venv\Scripts\python.exe ".\baselines\TerraNova_M4_TaxEngine_v4.0.3\run.py"           run_m4 --input $InputPack --out $OutDir --currency $Currency

# 2) Enrich tax (Tax_Payable)
.\.venv\Scripts\python.exe .\tools\enrich_tax_payable.py $OutDir

# 3) Switch PYTHONPATH to src only (M5+ are source modules)
$env:PYTHONPATH="C:\TerraNova\src"

# 4) M5 → M6
.\.venv\Scripts\python.exe -c "from terra_nova.modules.m5_cash_flow.runner import run_m5; run_m5(r'$OutDir','$Currency')"
.\.venv\Scripts\python.exe -c "from terra_nova.modules.m6_balance_sheet.runner import run_m6; run_m6(r'$OutDir','$Currency')"

# 5) M7.R1 → Freeze
.\.venv\Scripts\python.exe -c "from terra_nova.modules.m7_optimizer.runner import run_m7_r1; run_m7_r1(r'$InputPack', r'$OutDir', currency='$Currency')"
.\.venv\Scripts\python.exe .\tools\m7_freeze_selection.py $OutDir

# 6) M7.5A wiring (NAD, canonical)
.\.venv\Scripts\python.exe -c "from terra_nova.modules.m7_5_wiring.runner import run_m7_5; run_m7_5(r'$OutDir','$Currency', fx_usd_to_nad=19.20, injection_month=1)"

# 7) M7.5B rebuild (Option B behavior is in the runner's logic)
.\.venv\Scripts\python.exe -c "from terra_nova.modules.m7_5b_rebuild.runner import run_m7_5b; run_m7_5b(r'$OutDir','$Currency')"

# 8) Contracts & smoke
.\.venv\Scripts\python.exe .\tools\validate_contracts.py $OutDir --strict-tax --include-m6
.\.venv\Scripts\python.exe .\tools\validate_m7_5b.py $OutDir
.\.venv\Scripts\python.exe -m unittest -v tests\smoke\test_m7_5b_smoke.py

Write-Host "== CI complete =="
