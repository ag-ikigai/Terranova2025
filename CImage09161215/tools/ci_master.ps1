param(
  [string]$InputXlsx       = "C:\TerraNova\InputPack\TerraNova_Input_Pack_v10_0.xlsx",
  [string]$OutDir          = "C:\TerraNova\outputs",
  [string]$Currency        = "NAD",
  [double]$FxUsdToNad      = 19.20,
  [int]$InjectionMonth     = 1
)

$ErrorActionPreference = "Stop"

function Step {
  param([string]$Name, [scriptblock]$Block)
  Write-Host "== $Name ==" -ForegroundColor Cyan
  & $Block
  if ($LASTEXITCODE -ne 0) { throw "Step failed: $Name" }
}

# 0) Preconditions and environment
if (-not (Test-Path ".\.venv\Scripts\python.exe")) { throw "venv missing: .\.venv\Scripts\python.exe" }
if (-not (Test-Path $InputXlsx)) { throw "Input pack not found: $InputXlsx" }
if (-not (Test-Path $OutDir)) { New-Item -ItemType Directory -Force -Path $OutDir | Out-Null }

# Pin src into site-packages once per run (harmless if already present)
Step "Pin src to site-packages" {
  # Note: We ensure internal Python literals use single quotes here too for robustness.
  .\.venv\Scripts\python.exe -c "import site, pathlib; sp=[p for p in site.getsitepackages() if p.endswith('site-packages')][0]; pathlib.Path(sp,'terranova_src.pth').write_text(r'C:\TerraNova\src'); print('PYTHONPATH pinned at', pathlib.Path(sp,'terranova_src.pth'))"
}

# Keep src on path too (some tools read PYTHONPATH)
$env:PYTHONPATH = "C:\TerraNova\src"

# 1) M0..M4 strict (as locked earlier) ---------------------------------------
Step "M0 strict" { powershell -ExecutionPolicy Bypass -File .\tools\ci_m0_strict.ps1 -InputXlsx $InputXlsx -OutDir $OutDir }
Step "M1 strict" { powershell -ExecutionPolicy Bypass -File .\tools\ci_m1_strict.ps1 -InputXlsx $InputXlsx -OutDir $OutDir }
Step "M2 strict" { powershell -ExecutionPolicy Bypass -File .\tools\ci_m2_strict.ps1 -InputXlsx $InputXlsx -OutDir $OutDir }
Step "M3 strict" { powershell -ExecutionPolicy Bypass -File .\tools\ci_m3_strict.ps1 -InputXlsx $InputXlsx -OutDir $OutDir }
Step "M4 strict" { powershell -ExecutionPolicy Bypass -File .\tools\ci_m4_strict.ps1 -InputXlsx $InputXlsx -OutDir $OutDir }

# 2) Enrich TAX_PAYABLE (your existing tool; do not change) -------------------
Step "M4 enrich payable" { .\.venv\Scripts\python.exe .\tools\enrich_tax_payable.py $OutDir }

# 3) M5 and M6 (module runners from src) --------------------------------------
$env:PYTHONPATH = "C:\TerraNova\src"
# Note: These steps use the robust pattern: PowerShell double quotes wrapping Python single quotes.
Step "M5 runner" {
  .\.venv\Scripts\python.exe -c "from terra_nova.modules.m5_cash_flow.runner import run_m5; run_m5(r'$OutDir','$Currency')"
}
Step "M6 runner" {
  .\.venv\Scripts\python.exe -c "from terra_nova.modules.m6_balance_sheet.runner import run_m6; run_m6(r'$OutDir','$Currency')"
}

# 4) M7.R1 optimization and freeze selection ----------------------------------
Step "M7.R1 runner" {
  .\.venv\Scripts\python.exe -c "from terra_nova.modules.m7_optimizer.runner import run_m7_r1; run_m7_r1(r'$InputXlsx', r'$OutDir', currency='$Currency')"
}
Step "M7 freeze selection" { .\.venv\Scripts\python.exe .\tools\m7_freeze_selection.py $OutDir }

# 5) M7.5A wiring --------------------------------------------------------------
Step "M7_5A wiring" {
  .\.venv\Scripts\python.exe -c "from terra_nova.modules.m7_5_wiring.runner import run_m7_5; run_m7_5(r'$OutDir','$Currency', fx_usd_to_nad=$FxUsdToNad, injection_month=$InjectionMonth)"
}

# 6) M7.5B rebuild (with fallback import path) --------------------------------
# FIX 2: Ensure ALL internal Python string literals use single quotes (')
# when using a PowerShell double-quoted here-string (@"..."@).

$py_m75b_script = @"
import sys
try:
    # Prioritize the actual path provided by the user
    from terra_nova.modules.m7_5b_rebuild.runner import run_m7_5b
except ImportError:
    # Fallback paths (kept from original script if module structure varies)
    try:
        from terra_nova.modules.m7_5b_financials.runner import run_m7_5b
    except ImportError:
        try:
            from terra_nova.modules.m7_5b.runner import run_m7_5b
        except ImportError:
             # CRITICAL FIX: Changed f"..." to f'...'
             print(f'[CI ERROR] Could not import M7.5B runner from m7_5b_rebuild, m7_5b_financials, or m7_5b.')
             sys.exit(1)

# Call the function using single quotes for Python string literals
run_m7_5b(r'$OutDir', '$Currency')
"@

# Execute the script stored in the variable
Step "M7_5B rebuild" { .\.venv\Scripts\python.exe -c $py_m75b_script }

# 7) Validators (contracts + optional extra validator) ------------------------
$env:PYTHONPATH = "C:\TerraNova\src"
Step "Contracts validator (M2-M6 strict)" {
  .\.venv\Scripts\python.exe .\tools\validate_contracts.py $OutDir --strict-tax --include-m6
}

# Optional extra validator you will author; runs only if present
if (Test-Path ".\tools\validate_m4_enrich_to_m7_5b.py") {
  Step "Validator: M4-enrich to M7_5B" { .\.venv\Scripts\python.exe .\tools\validate_m4_enrich_to_m7_5b.py $OutDir }
} else {
  Write-Host "Validator: M4-enrich to M7_5B (optional) not found; skipping." -ForegroundColor Yellow
}

# 8) Smoke tests ---------------------------------------------------------------
Step "Smoke: pipeline" { .\.venv\Scripts\python.exe -m unittest -v tests\smoke\test_pipeline_smoke.py }
Step "Smoke: M7.R1"     { .\.venv\Scripts\python.exe -m unittest -v tests\smoke\test_m7_smoke.py }
Step "Smoke: M7_5A"     { .\.venv\Scripts\python.exe -m unittest -v tests\smoke\test_m7_5_smoke.py }
Step "Smoke: M7_5B"     { .\.venv\Scripts\python.exe -m unittest -v tests\smoke\test_m7_5b_smoke.py }

Write-Host ""
Write-Host "MASTER CI (M0..M7_5B) : PASS" -ForegroundColor Green

# -------------------- NEW: M8 --------------------

# --- M8.A (super-verifier) ---------------------------------------------------
# Uses tools\validate_m8a.py (it runs the module AND validates the JSON result).
# We pass the buffer explicitly to satisfy argparse: --buffer <number>
Step "M8.A (run + validate)" {
  & .\.venv\Scripts\python.exe @(
    ".\tools\validate_m8a.py",
    ".\outputs",
    "--buffer", $MinCashBufferNAD000,
    "--input-pack", $InputXlsx,
    "--strict"
  )
}

# Smoke test for M8.A (existing test file)
Step "Smoke: M8.A" {
  .\.venv\Scripts\python.exe -m unittest -v tests\smoke\test_m8a_smoke.py
}


# -----------------------------
# 6) M8.B1..B6 runners
# -----------------------------
function PyTryRun([string]$Title, [string[]]$ModulePaths, [string]$Fn) {
  $code = @"
import importlib, sys, traceback
outdir = r'$OutDir'; currency = r'$Currency'
paths = $(($ModulePaths | ForEach-Object { "'$_'" }) -join ", ")
def _try():
    for mod_name in [$paths]:
        try:
            mod = importlib.import_module(mod_name)
            fn = getattr(mod, '$Fn')
            # prefer simple signature (outdir, currency); tolerate extras via kwargs
            try:
                fn(outdir, currency)
            except TypeError:
                fn(outdir, currency, input_pack_path=r'$InputXlsx')
            print('[OK] {0} via'.format('$Title'), mod_name)
            return 0
        except Exception:
            last = traceback.format_exc()
    print('[FAIL] {0}: runners not resolved'.format('$Title'))
    print(last)
    return 1
sys.exit(_try())
"@
  Step $Title { .\.venv\Scripts\python.exe -c $code }
}

# B1 – base timeseries (names kept generic; we try several)
PyTryRun "M8.B1 runner" @(
  "terra_nova.modules.m8_b1.runner",
  "terra_nova.modules.m8b1.runner",
  "terra_nova.modules.m8_base_timeseries.runner"
) "run_m8b1"

# B2 – promoter scorecard
PyTryRun "M8.B2 runner" @(
  "terra_nova.modules.m8_b2.runner",
  "terra_nova.modules.m8b2.runner",
  "terra_nova.modules.m8_promoter_scorecard.runner"
) "run_m8b2"

# B3 – investor engine
PyTryRun "M8.B3 runner" @(
  "terra_nova.modules.m8_b3.runner",
  "terra_nova.modules.m8b3.runner",
  "terra_nova.modules.m8_investor.runner"
) "run_m8b3"

# B4 – lender pack
PyTryRun "M8.B4 runner" @(
  "terra_nova.modules.m8_b4.runner",
  "terra_nova.modules.m8b4.runner",
  "terra_nova.modules.m8_lender.runner"
) "run_m8b4"

# B5 – benchmarks
PyTryRun "M8.B5 runner" @(
  "terra_nova.modules.m8_b5.runner",
  "terra_nova.modules.m8b5.runner",
  "terra_nova.modules.m8_benchmarks.runner"
) "run_m8b5"

# B6 – IFRS presentation
PyTryRun "M8.B6 runner" @(
  "terra_nova.modules.m8_b6.runner",
  "terra_nova.modules.m8b6.runner",
  "terra_nova.modules.m8_ifrs.runner"
) "run_m8b6"

