@echo off
SETLOCAL ENABLEDELAYEDEXPANSION
echo [INFO] Starting TerraNova Pre-Flight Check.
echo [STEP 1/6] Running Module 0: Setup & Validation.
.\.venv\Scripts\python.exe .\run.py fresh_m0 --input ".\InputPack\TerraNova_Input_Pack_v10_0.xlsx" --out ".\outputs" --currency NAD
echo [STEP 2/6] Running Module 1.
.\.venv\Scripts\python.exe .\run.py run_m1   --input ".\InputPack\TerraNova_Input_Pack_v10_0.xlsx" --out ".\outputs" --currency NAD
echo [STEP 3/6] Running Module 2.
.\.venv\Scripts\python.exe .\run.py run_m2   --input ".\InputPack\TerraNova_Input_Pack_v10_0.xlsx" --out ".\outputs" --currency NAD
echo [STEP 4/6] Running Module 3.
.\.venv\Scripts\python.exe .\run.py run_m3   --input ".\InputPack\TerraNova_Input_Pack_v10_0.xlsx" --out ".\outputs" --currency NAD
echo [STEP 5/6] Running Module 4.
.\.venv\Scripts\python.exe .\run.py run_m4   --input ".\InputPack\TerraNova_Input_Pack_v10_0.xlsx" --out ".\outputs" --currency NAD
echo [STEP 6/6] Running Full Test Suite.
.\.venv\Scripts\python.exe -m unittest discover -s .\tests -p "test*.py"
echo [INFO] Pre-Flight Check Complete.
ENDLOCAL
