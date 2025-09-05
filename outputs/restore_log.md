# Restore Log (M0..M4)

```
[2025-09-05 10:58:52] Wrote discovery dump: C:\TerraNova\outputs\orchestrator_debug_dump.json
[2025-09-05 10:58:52] Copying m0_setup -> C:\TerraNova\src\terra_nova\modules\m0_setup
[2025-09-05 10:58:52] Copying m1 -> C:\TerraNova\src\terra_nova\modules\m1_operational_engines
[2025-09-05 10:58:52] Copying m2 -> C:\TerraNova\src\terra_nova\modules\m2_working_capital_pl
[2025-09-05 10:58:52] Copying m3 -> C:\TerraNova\src\terra_nova\modules\m3_financing
[2025-09-05 10:58:52] Copying m4 -> C:\TerraNova\src\terra_nova\modules\m4_tax
[2025-09-05 10:58:52] Copied M4 run.py -> C:\TerraNova\run.py
[2025-09-05 10:58:52] Copied README.md from M4 baseline.
[2025-09-05 10:58:52] Copied VERSION.txt from M4 baseline.
[2025-09-05 10:58:52] Wrote post-copy dump: C:\TerraNova\outputs\orchestrator_debug_dump.json
[2025-09-05 10:58:52] [SMOKE] .venv\Scripts\python.exe C:\TerraNova\run.py fresh_m0 --input C:\TerraNova\InputPack\TerraNova_Input_Pack_v10_0.xlsx --out C:\TerraNova\outputs --currency NAD
[2025-09-05 10:58:52] [SMOKE] rc=0
[2025-09-05 10:58:52] [SMOKE] .venv\Scripts\python.exe C:\TerraNova\run.py run_m1 --input C:\TerraNova\InputPack\TerraNova_Input_Pack_v10_0.xlsx --out C:\TerraNova\outputs --currency NAD
[2025-09-05 10:58:53] [SMOKE] rc=0
[2025-09-05 10:58:53] [SMOKE] .venv\Scripts\python.exe C:\TerraNova\run.py run_m2 --input C:\TerraNova\InputPack\TerraNova_Input_Pack_v10_0.xlsx --out C:\TerraNova\outputs --currency NAD
[2025-09-05 10:58:53] [SMOKE] rc=0
[2025-09-05 10:58:53] [SMOKE] .venv\Scripts\python.exe C:\TerraNova\run.py run_m3 --input C:\TerraNova\InputPack\TerraNova_Input_Pack_v10_0.xlsx --out C:\TerraNova\outputs --currency NAD
[2025-09-05 10:58:53] [SMOKE] rc=0
[2025-09-05 10:58:53] [SMOKE] .venv\Scripts\python.exe C:\TerraNova\run.py run_m4 --input C:\TerraNova\InputPack\TerraNova_Input_Pack_v10_0.xlsx --out C:\TerraNova\outputs --currency NAD
[2025-09-05 10:58:54] [SMOKE] rc=0
[2025-09-05 10:58:54] Smoke M0..M4 OK
```
