# tools/py/validate_m3_outputs.py
import argparse, sys, json
from pathlib import Path
import pandas as pd
import numpy as np

def find_col(df, options):
    for c in options:
        if c in df.columns:
            return c
    return None

def validate(out_dir):
    od = Path(out_dir)
    cal_path = od / 'm0_calendar.parquet'
    n_months = None
    if cal_path.exists():
        cal = pd.read_parquet(cal_path)
        n_months = int(cal['Month_Index'].max())
        months = set(cal['Month_Index'].astype(int))
    else:
        months = None

    idx_p = od / 'm3_finance_index.parquet'
    sch_p = od / 'm3_financing_schedule.parquet'
    rev_p = od / 'm3_revolver_schedule.parquet'
    ins_p = od / 'm3_insurance_schedule.parquet'

    report = {"variant": None, "rows": None, "checks": {}}

    if sch_p.exists():
        # Schedule flavor
        df = pd.read_parquet(sch_p)
        report["variant"] = "schedule"
        report["rows"] = int(df.shape[0])

        must = {
            "Month_Index": ["Month_Index"],
            "Debt_Begin": ["Debt_Begin_NAD_000"],
            "New_Borrow": ["New_Borrowing_NAD_000"],
            "Repay": ["Principal_Repayment_NAD_000"],
            "Interest": ["Interest_NAD_000"],
            "Debt_End": ["Debt_End_NAD_000"],
            "Equity_In": ["Equity_Infusion_NAD_000"],
        }
        resolved = {k: find_col(df, v) for k, v in must.items()}
        missing = [k for k, v in resolved.items() if v is None]
        if missing:
            raise SystemExit(f"Missing required columns in schedule: {missing}")

        # Invariant: Debt_End = Debt_Begin + New_Borrow − Repay
        lhs = df[resolved["Debt_End"]].astype(float)
        rhs = (df[resolved["Debt_Begin"]].astype(float) +
               df[resolved["New_Borrow"]].astype(float) -
               df[resolved["Repay"]].astype(float))
        resid = (lhs - rhs).abs()
        if not np.all(resid <= 1e-6):
            bad = int((resid > 1e-6).sum())
            raise SystemExit(f"Debt rollforward check failed in {bad} rows (schedule flavor).")

        if months is not None:
            got = set(df[resolved["Month_Index"]].astype(int))
            if got != months:
                raise SystemExit("Month_Index set does not match calendar (schedule flavor).")

    elif idx_p.exists():
        # Index flavor
        dfi = pd.read_parquet(idx_p)
        report["variant"] = "index"
        report["rows"] = int(dfi.shape[0])

        # Resolve synonyms
        col_mi = find_col(dfi, ["Month_Index", "MONTH_INDEX", "month_index", "Period", "Month"])
        col_end = find_col(dfi, ["Debt_Principal_Closing_NAD_000", "Outstanding_Principal_NAD_000",
                                 "Principal_Outstanding_NAD_000", "Debt_End_NAD_000"])
        col_draw = find_col(dfi, ["Principal_Draws_CF_NAD_000", "Debt_Draws_NAD_000"])
        col_repay = find_col(dfi, ["Principal_Repayments_CF_NAD_000", "Debt_Repayments_NAD_000",
                                   "Principal_Repayment_NAD_000"])
        # Optional
        col_int = find_col(dfi, ["Interest_Paid_NAD_000", "Interest_Cash_Outflow_NAD_000", "Interest_NAD_000", "Interest_Expense_NAD_000"])
        col_eq = find_col(dfi, ["Equity_Issued_Cash_NAD_000", "Equity_Cash_In_NAD_000", "Equity_Infusion_NAD_000"])

        for k, v in {"MONTH_INDEX": col_mi, "DEBT_END": col_end, "DRAWS": col_draw, "REPAY": col_repay}.items():
            if v is None:
                raise SystemExit(f"Missing required role in finance index: {k}")

        # Invariant on the index: ΔDebt_End = Draws − Repay (skip month 1 where lag is undefined)
        s_end = dfi[col_end].astype(float)
        s_draw = dfi[col_draw].astype(float)
        s_rep = dfi[col_repay].astype(float)
        resid = (s_end - s_end.shift(1) - s_draw + s_rep).fillna(0.0).abs()
        if not np.all(resid[1:] <= 1e-6):
            bad = int((resid[1:] > 1e-6).sum())
            raise SystemExit(f"Debt delta check failed in {bad} rows (index flavor).")

        if months is not None:
            got = set(dfi[col_mi].astype(int))
            if got != months:
                raise SystemExit("Month_Index set does not match calendar (index flavor).")

        # Back-compat schedule materialization if not present:
        # Debt_Begin = Debt_End − Draws + Repay  (identity from rollforward)
        out = pd.DataFrame({
            "Month_Index": dfi[col_mi].astype(int),
            "Debt_Begin_NAD_000": (s_end - s_draw + s_rep),
            "New_Borrowing_NAD_000": s_draw,
            "Principal_Repayment_NAD_000": s_rep,
            "Interest_NAD_000": dfi[col_int].astype(float) if col_int else 0.0,
            "Debt_End_NAD_000": s_end,
            "Equity_Infusion_NAD_000": dfi[col_eq].astype(float) if col_eq else 0.0,
        })
        out = out.sort_values("Month_Index").reset_index(drop=True)
        out.to_parquet(sch_p)

    else:
        raise SystemExit("Neither m3_finance_index.parquet nor m3_financing_schedule.parquet found.")

    # Revolver & insurance: optional presence check + month coverage if present
    if rev_p.exists():
        dfr = pd.read_parquet(rev_p)
        if months is not None and "Month_Index" in dfr:
            got = set(dfr["Month_Index"].astype(int))
            if got != months:
                raise SystemExit("Month_Index set does not match calendar (revolver).")

    if ins_p.exists():
        dfi = pd.read_parquet(ins_p)
        if months is not None and "Month_Index" in dfi:
            got = set(dfi["Month_Index"].astype(int))
            if got != months:
                raise SystemExit("Month_Index set does not match calendar (insurance).")

    print(json.dumps(report))
    return 0

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", required=True)
    args = ap.parse_args()
    sys.exit(validate(args.out))
