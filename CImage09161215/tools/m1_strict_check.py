# tools/m1_strict_check.py
from __future__ import annotations
import argparse, sys
from pathlib import Path
import pandas as pd
import numpy as np

# Reuse M0 helpers (source of truth for InputPack v10 + calendar)
from terra_nova.modules.m0_setup.engine import (
    load_and_validate_input_pack,   # validates v10 workbook
    create_calendar,                # builds Date/Year/Month/Month_Index
)

def _need(path: Path, label: str) -> pd.DataFrame:
    if not path.exists():
        print(f"[ERROR] Missing required file: {path}")
        sys.exit(2)
    try:
        return pd.read_parquet(path)
    except Exception as e:
        print(f"[ERROR] Failed reading {label} at {path}: {e}")
        sys.exit(2)

def _need_cols(df: pd.DataFrame, cols: list[str], label: str):
    missing = [c for c in cols if c not in df.columns]
    if missing:
        print(f"[ERROR] {label} missing columns: {missing}")
        sys.exit(2)

def _assert(cond: bool, msg: str):
    if not cond:
        print(f"[ERROR] {msg}")
        sys.exit(2)

def main():
    ap = argparse.ArgumentParser(description="Strict M1 validator against Input Pack v10.")
    ap.add_argument("--input", required=True, help="Path to TerraNova_Input_Pack_v10_0.xlsx")
    ap.add_argument("--out",    required=True, help="Outputs directory (e.g., .\\outputs)")
    args = ap.parse_args()

    out_dir = Path(args.out)

    # Load outputs
    p_rev = out_dir / "m1_revenue_schedule.parquet"
    p_oxp = out_dir / "m1_opex_schedule.parquet"
    p_cap = out_dir / "m1_capex_schedule.parquet"
    p_dep = out_dir / "m1_depreciation_schedule.parquet"

    rev = _need(p_rev, "m1_revenue_schedule")
    oxp = _need(p_oxp, "m1_opex_schedule")
    cap = _need(p_cap, "m1_capex_schedule")
    dep = _need(p_dep, "m1_depreciation_schedule")

    _need_cols(rev, ["Month_Index","Monthly_Revenue_NAD_000"], "m1_revenue_schedule")
    _need_cols(oxp, ["Month_Index","Monthly_OPEX_NAD_000"], "m1_opex_schedule")
    _need_cols(cap, ["Month_Index","Monthly_CAPEX_NAD_000"], "m1_capex_schedule")
    _need_cols(dep, ["Month_Index","Monthly_Depreciation_NAD_000"], "m1_depreciation_schedule")

    # Load Input Pack via existing M0 loader; derive calendar horizon
    sheets = load_and_validate_input_pack(Path(args.input))
    cal_exp = create_calendar(sheets["Parameters"])
    horizon = int(cal_exp["Month_Index"].max())

    # Month_Index coverage and monotonicity
    for name, df in [("Revenue",rev),("OPEX",oxp),("CAPEX",cap),("Depreciation",dep)]:
        mis = df["Month_Index"]
        _assert(mis.min()==1 and mis.max()==horizon, f"{name}: Month_Index must cover 1..{horizon}")
        _assert(pd.Index(mis).is_monotonic_increasing, f"{name}: Month_Index must be monotonic increasing")

    # 1) Revenue cadence
    first_nz = rev.loc[rev["Monthly_Revenue_NAD_000"]>0, "Month_Index"].min()
    _assert(np.all(rev.loc[rev["Month_Index"]<=6, "Monthly_Revenue_NAD_000"]==0), "Revenue must be zero in months 1..6")
    _assert(first_nz == 7, f"First non-zero revenue month must be 7 (found {first_nz})")
    _assert((rev["Monthly_Revenue_NAD_000"]>=0).all(), "Revenue has negative values")

    # 2) CAPEX exactness vs Input Pack CAPEX_Schedule (both in NAD '000)
    cap_in = sheets.get("CAPEX_Schedule")
    if cap_in is None or cap_in.empty:
        print("[WARN] Input Pack has empty CAPEX_Schedule; skipping CAPEX equality check.")
    else:
        exp = pd.Series(0.0, index=pd.RangeIndex(1, horizon+1), dtype="float64")
        grp = cap_in.groupby("Month", dropna=False)["Amount_NAD_000"].sum()
        for m, v in grp.items():
            if 1 <= int(m) <= horizon:
                exp.iloc[int(m)-1] += float(v)
        have = cap.set_index("Month_Index")["Monthly_CAPEX_NAD_000"].astype(float)
        have = have.reindex(exp.index, fill_value=0.0)
        # Compare at 2-decimal precision to tolerate parquet writer/coercion noise
        diff = (have.round(2) - exp.round(2)).abs()
        if (diff > 0.01).any():
            bad = diff[diff>0.01]
            print("[ERROR] CAPEX differs from Input Pack in these months (>= 0.01 '000 NAD):")
            print(bad.head(10))
            sys.exit(2)

    # 3) Depreciation invariants (engine-agnostic)
    cap_total = float(sheets.get("CAPEX_Schedule", pd.DataFrame({"Amount_NAD_000":[0]}))["Amount_NAD_000"].sum())
    dep_now = dep["Monthly_Depreciation_NAD_000"].astype(float)
    _assert((dep_now>=0).all(), "Depreciation has negative values")
    first_capex_m = int(sheets["CAPEX_Schedule"]["Month"].min()) if "CAPEX_Schedule" in sheets and not sheets["CAPEX_Schedule"].empty else 1
    if first_capex_m > 1:
        pre = dep.loc[dep["Month_Index"]<first_capex_m, "Monthly_Depreciation_NAD_000"].astype(float)
        _assert((pre.abs()<=1e-6).all(), f"Depreciation must be zero before first CAPEX month ({first_capex_m})")
    cum_dep = float(dep_now.sum())
    _assert(cum_dep <= cap_total + 1e-6, f"Cumulative depreciation ({cum_dep:.2f}) exceeds total CAPEX ({cap_total:.2f})")

    # 4) OPEX sanity
    _assert((oxp["Monthly_OPEX_NAD_000"]>=0).all(), "OPEX has negative values")
    _assert(oxp.loc[oxp["Month_Index"]>=7, "Monthly_OPEX_NAD_000"].sum() > 0, "OPEX is zero after month 6")

    print("[M1] Revenue first non-zero month = 7 ✔")
    print(f"[M1] CAPEX matches Input Pack by month ✔ (sum={exp.sum():.2f} '000 NAD)" if cap_in is not None and not cap_in.empty else "[M1] CAPEX check skipped (empty in Input Pack)")
    print(f"[M1] Depreciation cumulative ≤ CAPEX ✔ (cum_dep={cum_dep:.2f} vs cap_total={cap_total:.2f})")
    print("[M1] OPEX non-negative and active after month 6 ✔")
    print("M1 STRICT: PASS")

if __name__ == "__main__":
    main()
