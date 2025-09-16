# Terra Nova – MASTER STRICT CI (v1)

## Goal
A single, repeatable CI entrypoint that runs **module-by-module strict validations** against the **current Input Pack** (v10) and frozen artifact names/columns.

## What runs today
1. **M0 strict** — re-generates M0 from the Input Pack using the main path runner and then validates that the three *consumed* artifacts are exactly the deterministic transforms of the Input Pack:
   - outputs/m0_inputs/FX_Path.parquet  (Month_Index, NAD_per_USD)
   - outputs/m0_calendar.parquet        (Date, Year, Month, Month_Index)
   - outputs/m0_opening_bs.parquet      (Line_Item, Value_NAD, Notes)

2. **M1 strict** — validates M1’s four schedules:
   - Revenue: zero in months 1–6; first non-zero at 7; non-negative.
   - CAPEX: exact equality to Input Pack CAPEX_Schedule grouped by Month.
   - Depreciation: zero before first CAPEX month; non-negative; cumulative ≤ total CAPEX.
   - OPEX: non-negative and active after month 6.

## Invocation (VS PowerShell)
```powershell
# From C:\TerraNova
.\.venv\Scripts\Activate.ps1
$env:PYTHONPATH="C:\TerraNova\src"
powershell -ExecutionPolicy Bypass -File .\tools\ci_master.ps1 `
  -InputXlsx "C:\TerraNova\InputPack\TerraNova_Input_Pack_v10_0.xlsx" `
  -OutDir    "C:\TerraNova\outputs"
