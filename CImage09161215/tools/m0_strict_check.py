# tools/m0_strict_check.py
from __future__ import annotations
import argparse, sys
from pathlib import Path
import pandas as pd

# Reuse your existing, validated helpers (no business-logic change)
from terra_nova.modules.m0_setup.engine import (
    load_and_validate_input_pack,     # reads Input Pack v10 with pydantic validation
    create_calendar,                  # derives Date/Year/Month/Month_Index from Parameters
    create_opening_balance_sheet,     # computes opening cash from FX (500k USD * month 1 FX)
)

def eq_fx(fx_now: pd.DataFrame, fx_xlsx: pd.DataFrame) -> tuple[bool, str]:
    a = fx_now[["Month_Index","NAD_per_USD"]].copy()
    b = fx_xlsx.rename(columns={"Month":"Month_Index"})[["Month_Index","NAD_per_USD"]].copy()
    a = a.sort_values("Month_Index").reset_index(drop=True)
    b = b.sort_values("Month_Index").reset_index(drop=True)
    a["Month_Index"] = a["Month_Index"].astype("int64")
    b["Month_Index"] = b["Month_Index"].astype("int64")
    a["NAD_per_USD"] = pd.to_numeric(a["NAD_per_USD"])
    b["NAD_per_USD"] = pd.to_numeric(b["NAD_per_USD"])
    if len(a) != len(b): return False, f"FX row count differs: now={len(a)} vs xlsx={len(b)}"
    if not (a["Month_Index"].equals(b["Month_Index"])): return False, "FX Month_Index differs"
    if not (a["NAD_per_USD"].reset_index(drop=True).round(9).equals(b["NAD_per_USD"].reset_index(drop=True).round(9))):
        return False, "FX NAD_per_USD differs"
    return True, "OK"

def eq_calendar(cal_now: pd.DataFrame, cal_exp: pd.DataFrame) -> tuple[bool,str]:
    a = cal_now[["Date","Year","Month","Month_Index"]].copy()
    b = cal_exp[["Date","Year","Month","Month_Index"]].copy()
    a["Date"] = pd.to_datetime(a["Date"]).dt.normalize()
    b["Date"] = pd.to_datetime(b["Date"]).dt.normalize()
    for c in ["Year","Month","Month_Index"]:
        a[c] = pd.to_numeric(a[c])
        b[c] = pd.to_numeric(b[c])
    if len(a)!=len(b): return False, f"Calendar row count differs: now={len(a)} vs exp={len(b)}"
    if not a["Date"].equals(b["Date"]): return False, "Calendar Date differs"
    for c in ["Year","Month","Month_Index"]:
        if not a[c].equals(b[c]): return False, f"Calendar {c} differs"
    return True,"OK"

def eq_opening(obs_now: pd.DataFrame, obs_exp: pd.DataFrame) -> tuple[bool,str]:
    cols = ["Line_Item","Value_NAD","Notes"]
    for df in (obs_now, obs_exp):
        for c in cols:
            if c not in df.columns: return False, f"Opening BS missing column {c}"
    a = obs_now[cols].copy().reset_index(drop=True)
    b = obs_exp[cols].copy().reset_index(drop=True)
    # same rows, same order expected (Cash, Opening Equity-like)
    if len(a)!=len(b): return False, f"Opening BS row count differs: now={len(a)} vs exp={len(b)}"
    if not a["Line_Item"].equals(b["Line_Item"]): return False, "Opening BS Line_Item differs"
    if not pd.to_numeric(a["Value_NAD"]).round(2).equals(pd.to_numeric(b["Value_NAD"]).round(2)):
        return False, "Opening BS Value_NAD differs"
    return True,"OK"

def main():
    ap = argparse.ArgumentParser(description="Strict M0 validator against Input Pack v10.")
    ap.add_argument("--input", required=True, help="Path to TerraNova_Input_Pack_v10_0.xlsx")
    ap.add_argument("--out",    required=True, help="Outputs directory (e.g., .\\outputs)")
    args = ap.parse_args()

    out_dir = Path(args.out)
    fx_now  = pd.read_parquet(out_dir/"m0_inputs"/"FX_Path.parquet")
    cal_now = pd.read_parquet(out_dir/"m0_calendar.parquet")
    obs_now = pd.read_parquet(out_dir/"m0_opening_bs.parquet")

    sheets = load_and_validate_input_pack(Path(args.input))
    fx_xlsx = sheets["FX_Path"]
    cal_exp = create_calendar(sheets["Parameters"])
    obs_exp = create_opening_balance_sheet(sheets["FX_Path"])

    ok_fx,  why_fx  = eq_fx(fx_now, fx_xlsx)
    ok_cal, why_cal = eq_calendar(cal_now, cal_exp)
    ok_obs, why_obs = eq_opening(obs_now, obs_exp)

    print(f"[FX     ] {why_fx}")
    print(f"[Calendar] {why_cal}")
    print(f"[Opening ] {why_obs}")

    if not (ok_fx and ok_cal and ok_obs):
        sys.exit(2)
    print("M0 STRICT: PASS (outputs equal to deterministic transforms of the Input Pack)")

if __name__ == "__main__":
    main()
