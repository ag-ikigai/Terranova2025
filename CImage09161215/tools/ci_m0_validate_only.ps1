param(
  [string]$OutDir        = "C:\TerraNova\outputs",
  [string]$BackupOutDir  = "C:\TerraNova_backups\TerraNova_20250914_1710\outputs"
)
$ErrorActionPreference = "Stop"

# 0) Ensure venv present
if (-not (Test-Path ".\.venv\Scripts\python.exe")) { throw "venv missing" }

# 1) FX / Calendar / Opening BS — schema & numeric checks (NO writes)
.\.venv\Scripts\python.exe - << 'PY'
import sys, pandas as pd, numpy as np, pathlib as p, json
out=p.Path(r'C:\TerraNova\outputs'); backup=p.Path(r'C:\TerraNova_backups\TerraNova_20250914_1710\outputs')
def read(fp): 
    return pd.read_parquet(fp) if fp.suffix=='.parquet' else pd.read_csv(fp)

def same_numeric(a,b,cols,rtol=0,atol=0):
    for c in cols:
        if not np.allclose(pd.to_numeric(a[c]), pd.to_numeric(b[c]), rtol=rtol, atol=atol, equal_nan=True):
            return False,c
    return True,None

fx_now = read(out/'m0_inputs'/'FX_Path.parquet')
fx_bak = read(backup/'m0_inputs'/'FX_Path.parquet')
assert list(fx_now.columns)==['Month_Index','NAD_per_USD']
assert list(fx_bak.columns)==['Month_Index','NAD_per_USD']
ok,c = same_numeric(fx_now, fx_bak, ['Month_Index','NAD_per_USD'])
print("[FX] match =", ok, "diff_col" , c)

cal_now = read(out/'m0_calendar.parquet')
cal_bak = read(backup/'m0_calendar.parquet')
for req in ['Date','Year','Month','Month_Index']: 
    assert req in cal_now.columns and req in cal_bak.columns
# Date equality at day precision
d_now = pd.to_datetime(cal_now['Date']).dt.normalize()
d_bak = pd.to_datetime(cal_bak['Date']).dt.normalize()
ok_date = d_now.equals(d_bak)
ok_num,c = same_numeric(cal_now, cal_bak, ['Year','Month','Month_Index'])
print("[Calendar] date_match =", ok_date, "numeric_match =", ok_num, "diff_col", c)

obs_now = read(out/'m0_opening_bs.parquet')
obs_bak = read(backup/'m0_opening_bs.parquet')
for req in ['Line_Item','Value_NAD','Notes']: 
    assert req in obs_now.columns and req in obs_bak.columns
ok_items = obs_now['Line_Item'].equals(obs_bak['Line_Item'])
ok_vals,c = same_numeric(obs_now, obs_bak, ['Value_NAD'])
print("[Opening BS] items_match =", ok_items, "values_match =", ok_vals)

if not (ok and ok_date and ok_num and ok_items and ok_vals):
    sys.exit(2)
PY
