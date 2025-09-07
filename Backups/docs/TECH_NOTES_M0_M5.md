# TerraNova M0–M5: Technical Notes (Stabilized)

## Layout (now stable)
- **Main tree**: `C:\TerraNova`
  - `src\terra_nova\modules\m0_setup ... m5_cash_flow` (restored & runnable)
  - `outputs\` ← all module artifacts & smoke/debug reports
  - `baselines\` ← frozen historical drops
  - `tests\smoke\` ← fast runtime checks
  - `run.py` ← single entrypoint (delegates M0–M4 to baselines; M5 uses local runner)

## How it runs
1. **M0…M2**: restored from `baselines\TerraNova_main_up_to_M2`.  
   Produces `m2_pl_schedule.parquet` and `m2_working_capital_schedule.parquet`.
2. **M3 (Financing)**: executed by `baselines\TerraNova_M3_FinancingEngine_v3.1.0\run.py` with  
   `PYTHONPATH="C:\TerraNova\src;C:\TerraNova\baselines\TerraNova_main_up_to_M2\src"`.
3. **M4 (Tax)**: executed by `baselines\TerraNova_M4_TaxEngine_v4.0.3\run.py`.
4. **M5 (Cash Flow)**: local `m5_run_accept_either_v5.py` reads **either** of the following for robustness:
   - PL: `m2_pl_schedule.parquet` (canonical)
   - WC: `m2_working_capital_schedule.parquet` (canonical)
   - If columns like `Net_Profit_After_Tax` or `Depreciation_and_Amortization` are missing,
     M5 derives CFO from available PL lines and WC deltas.
   - Outputs: `m5_cash_flow_statement_final.parquet`, `m5_smoke_report.md`, `m5_debug_dump.json`.

## Why this is stable
- **File‑name contract documented** (see *M2_contract.md*).  
- **Tolerant column mapping** in M5 avoids breakage from minor header drift.  
- **Smoke tests** catch missing or empty artifacts quickly.  
- **Backups** via one‑line `robocopy` ensure easy rollback.

## Normal run commands (PowerShell)
```powershell
# Example: M3 then M4 then M5
$env:PYTHONPATH="C:\TerraNova\src;C:\TerraNova\baselines\TerraNova_main_up_to_M2\src"; .\.venv\Scripts\python.exe "C:\TerraNova\baselines\TerraNova_M3_FinancingEngine_v3.1.0\run.py" run_m3 --input "C:\TerraNova\InputPack\TerraNova_Input_Pack_v10_0.xlsx" --out "C:\TerraNova\outputs" --currency NAD

.\.venv\Scripts\python.exe "C:\TerraNova\baselines\TerraNova_M4_TaxEngine_v4.0.3\run.py" run_m4 --input "C:\TerraNova\InputPack\TerraNova_Input_Pack_v10_0.xlsx" --out "C:\TerraNova\outputs" --currency NAD

$env:PYTHONPATH="C:\TerraNova\src"; .\.venv\Scripts\python.exe .\m5_run_accept_either_v5.py --out "C:\TerraNova\outputs" --currency NAD
