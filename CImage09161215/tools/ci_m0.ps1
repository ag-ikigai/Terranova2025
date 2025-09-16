param(
  [string]$InputXlsx = "C:\TerraNova\InputPack\TerraNova_Input_Pack_v10_0.xlsx",
  [string]$OutDir    = "C:\TerraNova\outputs"
)
$ErrorActionPreference="Stop"

# Ensure venv & src on path (idempotent)
if (-not (Test-Path ".\.venv\Scripts\python.exe")) { throw "venv missing" }
$env:PYTHONPATH="C:\TerraNova\src"
.\.venv\Scripts\Activate.ps1

# --- Run M0 (full export) ---
.\.venv\Scripts\python.exe -m terra_nova.modules.m0_setup.runner `
  --input $InputXlsx `
  --out   $OutDir

# >>> HARDENED EXIT-CODE CHECK <<<
if ($LASTEXITCODE -ne 0) {
  throw "M0 runner failed with exit code $LASTEXITCODE"
}

# --- Contract checks (FX + calendar + opening BS) ---
.\.venv\Scripts\python.exe -c "import pandas as pd; \
fx=pd.read_parquet(r'$OutDir\m0_inputs\FX_Path.parquet'); \
assert list(fx.columns)==['Month_Index','NAD_per_USD'], fx.columns.tolist(); \
assert fx['Month_Index'].min()==1 and fx['Month_Index'].max()==len(fx) and fx['Month_Index'].is_monotonic_increasing, 'Month_Index must be contiguous 1..N'; \
assert (fx['NAD_per_USD']>0).all(), 'NAD_per_USD must be > 0'; \
cal=pd.read_parquet(r'$OutDir\m0_calendar.parquet'); \
assert {'Date','Year','Month','Month_Index'}.issubset(set(cal.columns)), 'Calendar columns missing'; \
obs=pd.read_parquet(r'$OutDir\m0_opening_bs.parquet'); \
assert {'Line_Item','Value_NAD','Notes'}.issubset(set(obs.columns)), 'Opening BS columns missing'; \
print('CI M0: PASS. FX rows=',len(fx),'Calendar rows=',len(cal),'Opening BS rows=',len(obs))"

# --- Optional: show which m0_inputs got refreshed (top 10 by time) ---
Get-ChildItem "$OutDir\m0_inputs\*.parquet" | Sort-Object LastWriteTime -Descending | Select-Object -First 10 FullName, LastWriteTime
