param(
  [string]$InputXlsx = "C:\TerraNova\InputPack\TerraNova_Input_Pack_v10_0.xlsx",
  [string]$OutDir    = "C:\TerraNova\outputs",
  [string]$Currency  = "NAD"
)

$ErrorActionPreference = "Stop"
Write-Host "[M2 STRICT] Start"

# --- 1) Run M2 (no logic changes; flexible signature call) ---
$pyRun = @"
import importlib, inspect, sys, traceback
out_dir = r"""$OutDir"""
input_xlsx = r"""$InputXlsx"""
currency = r"""$Currency"""

try:
    m = importlib.import_module('terra_nova.modules.m2_working_capital_pl.runner')
except Exception as e:
    traceback.print_exc()
    sys.exit(2)

fn = getattr(m, 'run_m2', None)
if fn is None:
    print("run_m2 not found in m2_working_capital_pl.runner", file=sys.stderr)
    sys.exit(2)

sig = inspect.signature(fn)
params = list(sig.parameters.keys())

try:
    # Common signatures observed historically:
    #   run_m2(out_dir)
    #   run_m2(out_dir, currency)
    # (older flavors sometimes accepted input_xlsx too; we try gracefully)
    if len(params) == 1:
        fn(out_dir)
    elif len(params) == 2:
        try:
            fn(out_dir, currency)
        except TypeError:
            fn(out_dir)
    elif len(params) >= 3:
        # Best-effort: try (input_xlsx, out_dir, currency)
        try:
            fn(input_xlsx, out_dir, currency)
        except TypeError:
            fn(out_dir)
    else:
        fn(out_dir)
except Exception:
    traceback.print_exc()
    sys.exit(3)

print("[M2][OK] run_m2 executed")
"@

$null = $pyRun | .\.venv\Scripts\python.exe - 
if ($LASTEXITCODE -ne 0) { throw "M2 run failed" }

# --- 2) Validate outputs (accept either schema flavor) ---
$wcPath = Join-Path $OutDir 'm2_working_capital_schedule.parquet'
$plStub1 = Join-Path $OutDir 'm2_profit_and_loss_stub.parquet'
$plStub2 = Join-Path $OutDir 'm2_pl_schedule.parquet'

if (-not (Test-Path $wcPath)) { throw "[M2][ERR] Missing m2_working_capital_schedule.parquet" }

$pyVal = @"
import sys, json
from pathlib import Path
import pandas as pd

out_dir = Path(r"""$OutDir""")
wc_path = out_dir / "m2_working_capital_schedule.parquet"
pl_candidates = [out_dir / "m2_profit_and_loss_stub.parquet", out_dir / "m2_pl_schedule.parquet"]

dfw = pd.read_parquet(wc_path)
if "Month_Index" not in dfw.columns:
    print("[M2][ERR] WC missing Month_Index", file=sys.stderr); sys.exit(2)

stocks = {"AR_Balance_NAD_000","Inventory_Balance_NAD_000","AP_Balance_NAD_000"} & set(dfw.columns)
flows  = {"Change_in_Receivables_NAD_000","Change_in_Inventory_NAD_000","Change_in_Payables_NAD_000","Cash_Flow_from_NWC_Change_NAD_000"} & set(dfw.columns)

if not stocks and not flows:
    print("[M2][ERR] WC has neither stock nor flow columns", file=sys.stderr); sys.exit(2)

flavor = "stocks" if stocks else "flows"

pl_used = None
pl_cols_preview = []
for p in pl_candidates:
    if p.exists():
        pl_used = p
        dpl = pd.read_parquet(p)
        if "Month_Index" not in dpl.columns:
            print("[M2][ERR] PL stub missing Month_Index", file=sys.stderr); sys.exit(2)
        pl_cols_preview = list(dpl.columns)[:40]
        break

report = {
  "wc_rows": int(len(dfw)),
  "wc_flavor": flavor,
  "wc_cols_preview": sorted(dfw.columns.tolist())[:40],
  "pl_used": str(pl_used) if pl_used else None,
  "pl_cols_preview": pl_cols_preview
}
print(json.dumps(report, indent=2))
"@

$report = $pyVal | .\.venv\Scripts\python.exe - 
if ($LASTEXITCODE -ne 0) { throw "M2 validation failed" }

Write-Host "[M2] Validation report: "
Write-Host $report
Write-Host "M2 STRICT: PASS" -ForegroundColor Green
exit 0
