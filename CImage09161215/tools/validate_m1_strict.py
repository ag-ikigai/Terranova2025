# C:\TerraNova\tools\validate_m1_strict.py
from __future__ import annotations

import argparse
from pathlib import Path
import sys
import pandas as pd
import numpy as np

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", required=True)
    ap.add_argument("--out", required=True)
    args = ap.parse_args()

    ip = Path(args.input)
    out = Path(args.out)

    # Read Input Pack for checks
    xls = pd.ExcelFile(ip)
    sheets = {s: pd.read_excel(ip, sheet_name=s) for s in xls.sheet_names}

    # Horizon
    months = 60
    if "Parameters" in sheets and not sheets["Parameters"].empty and {"Key","Value"}.issubset(sheets["Parameters"].columns):
        sel = sheets["Parameters"].loc[sheets["Parameters"]["Key"]=="HORIZON_MONTHS", "Value"]
        if not sel.empty:
            try: months = int(sel.iloc[0])
            except: months = 60

    # Load artifacts
    rev  = pd.read_parquet(out/"m1_revenue_schedule.parquet")
    opex = pd.read_parquet(out/"m1_opex_schedule.parquet")
    cap  = pd.read_parquet(out/"m1_capex_schedule.parquet")
    dep  = pd.read_parquet(out/"m1_depreciation_schedule.parquet")

    errs = []

    # Column presence
    reqs = [
        (rev,  ["Month_Index","Monthly_Revenue_NAD_000"], "m1_revenue_schedule.parquet"),
        (opex, ["Month_Index","Monthly_OPEX_NAD_000"],    "m1_opex_schedule.parquet"),
        (cap,  ["Month_Index","Monthly_CAPEX_NAD_000"],   "m1_capex_schedule.parquet"),
        (dep,  ["Month_Index","Monthly_Depreciation_NAD_000"], "m1_depreciation_schedule.parquet"),
    ]
    for df, cols, name in reqs:
        missing = [c for c in cols if c not in df.columns]
        if missing:
            errs.append(f"{name} missing columns: {missing}")

    # Month_Index coverage
    for df, _, name in reqs:
        if set(df["Month_Index"].astype(int)) != set(range(1, months+1)):
            errs.append(f"{name} Month_Index must be 1..{months}")

    # Revenue: first non-zero month >= 7
    if (rev["Monthly_Revenue_NAD_000"] > 0).any():
        mfirst = int(rev.loc[rev["Monthly_Revenue_NAD_000"] > 0, "Month_Index"].min())
        if mfirst < 7:
            errs.append(f"Revenue starts before month 7 (got {mfirst})")

    # OPEX yearly reconciliation (sum of months per Year == sum(Yk) in OPEX_Detail)
    if "OPEX_Detail" in sheets and not sheets["OPEX_Detail"].empty:
        od = sheets["OPEX_Detail"]
        years = (months + 11)//12
        for y in range(1, min(5, years)+1):
            ycol = f"Y{y}"
            if ycol in od.columns:
                y_target = pd.to_numeric(od[ycol], errors="coerce").fillna(0.0).sum()
                idx = (opex["Month_Index"].between((y-1)*12+1, y*12))
                y_actual = float(pd.to_numeric(opex.loc[idx, "Monthly_OPEX_NAD_000"], errors="coerce").sum())
                if not np.isclose(y_actual, y_target/12.0*12.0, rtol=0, atol=1e-6):
                    errs.append(f"OPEX Y{y} mismatch: monthly sum {y_actual} vs target {y_target}")
    # CAPEX: sum by month equals Input Pack CAPEX total
    if "CAPEX_Schedule" in sheets and not sheets["CAPEX_Schedule"].empty and "Amount_NAD_000" in sheets["CAPEX_Schedule"].columns:
        total_cap = float(pd.to_numeric(sheets["CAPEX_Schedule"]["Amount_NAD_000"], errors="coerce").fillna(0.0).sum())
        got_cap = float(pd.to_numeric(cap["Monthly_CAPEX_NAD_000"], errors="coerce").sum())
        if not np.isclose(got_cap, total_cap, rtol=0, atol=1e-6):
            errs.append(f"CAPEX sum mismatch: schedule {got_cap} vs input {total_cap}")

    # Depreciation non-negative
    if (pd.to_numeric(dep["Monthly_Depreciation_NAD_000"], errors="coerce") < 0).any():
        errs.append("Negative values in depreciation schedule")

    if errs:
        print("[M1 STRICT] FAIL")
        for e in errs:
            print(" -", e)
        sys.exit(1)

    print("M1 STRICT: PASS")
    sys.exit(0)

if __name__ == "__main__":
    main()
