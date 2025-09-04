
# TerraNova — Module 3 (Financing Engine)

This package delivers Module 3 (M3) of the Terra Nova model. It integrates standard loan schedules (annuity, straight, bullet),
revolving credit facilities, and insurance policies. **Business logic and schemas are frozen** and aligned with the
Data Contract for `TerraNova_Input_Pack_v10_0.xlsx`.

## Run modules (VS Code PowerShell, venv + pip)

```powershell
# M0
.\.venv\Scripts\python.exe .\run.py fresh_m0 --input ".\InputPack\TerraNova_Input_Pack_v10_0.xlsx" --out ".\outputs" --currency NAD
# M1
.\.venv\Scripts\python.exe .\run.py run_m1   --input ".\InputPack\TerraNova_Input_Pack_v10_0.xlsx" --out ".\outputs" --currency NAD
# M2
.\.venv\Scripts\python.exe .\run.py run_m2   --input ".\InputPack\TerraNova_Input_Pack_v10_0.xlsx" --out ".\outputs" --currency NAD
# M3 (this module)
.\.venv\Scripts\python.exe .\run.py run_m3   --input ".\InputPack\TerraNova_Input_Pack_v10_0.xlsx" --out ".\outputs" --currency NAD
```

> **Note:** Successful M3 runs **overwrite** the following artifacts in `.\outputs\`:  
> `m3_revolver_schedule.parquet`, `m3_insurance_schedule.parquet`, `m3_finance_index.parquet`, and `m3_smoke_report.md`.

## Run tests (unittest)

```powershell
# Discover all tests explicitly from the ./tests folder
.\.venv\Scripts\python.exe -m unittest discover -s .\tests -p "test*.py"

# Or run the M3 suite only, with verbosity (after tests/__init__.py exists)
.\.venv\Scripts\python.exe -m unittest -v tests.test_m3_full
```

## Requirements (pinned)
See `requirements.txt` for exact versions (Python 3.12.7).


### M3 Note (v3.1.0)
Insurance OFF (v1 stub): `m3_insurance_schedule.parquet` exists with zero values; full insurance modeling will ship in v2 after statements are validated.
