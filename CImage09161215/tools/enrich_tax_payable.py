# tools/enrich_tax_payable.py
import sys, json, argparse
from pathlib import Path
import pandas as pd
import numpy as np

def pick(df, *cands):
    for c in cands:
        if c in df.columns:
            return c
    return None

def main():
    ap = argparse.ArgumentParser(description="Ensure M4 schedule has Tax_Payable (and Tax_Paid if missing).")
    ap.add_argument("outdir", nargs="?", default="./outputs", help="Outputs directory")
    ap.add_argument("--paid-mode", choices=["zero", "expense"], default="zero",
                    help="How to derive Tax_Paid if missing: zero (default) or copy expense with lag.")
    ap.add_argument("--lag", type=int, default=0, help="Lag in months when paid-mode=expense (default 0).")
    args = ap.parse_args()

    outdir = Path(args.outdir)
    sched_pq = outdir / "m4_tax_schedule.parquet"
    summ_pq  = outdir / "m4_tax_summary.parquet"

    if not sched_pq.exists():
        print(f"[M4 enrich] schedule not found: {sched_pq}")
        sys.exit(1)

    df = pd.read_parquet(sched_pq)
    # discover cols
    month_col   = pick(df, "Month_Index", "MONTH_INDEX", "month")
    exp_col     = pick(df, "Tax_Expense_NAD_000", "Tax_Expense")
    paid_col    = pick(df, "Tax_Paid_NAD_000", "Tax_Paid")

    missing = []
    if month_col is None: missing.append("Month_Index*")
    if exp_col   is None: missing.append("Tax_Expense*")

    if missing:
        print(f"[M4 enrich] missing required columns: {missing}")
        print(f"[M4 enrich] schedule columns: {list(df.columns)}")
        sys.exit(1)

    # derive or normalize Tax_Paid
    suffix = "_NAD_000" if exp_col.endswith("_NAD_000") else ""
    target_paid = f"Tax_Paid{suffix}"
    created_paid = False

    if paid_col is None:
        # synthesize Tax_Paid
        if args.paid_mode == "zero":
            df[target_paid] = 0.0
        else:  # expense with lag
            df = df.sort_values(month_col).reset_index(drop=True)
            df[target_paid] = df[exp_col].shift(args.lag, fill_value=0.0)
        created_paid = True
        paid_col = target_paid
    else:
        # if we found 'Tax_Paid' (no suffix) but expense is *_NAD_000, mirror to *_NAD_000 for consistency
        if paid_col == "Tax_Paid" and suffix == "_NAD_000":
            df[target_paid] = df[paid_col].astype(float)
            paid_col = target_paid

    # compute Tax_Payable = cumulative(expense - paid)
    df = df.sort_values(month_col).reset_index(drop=True)
    payable_col = f"Tax_Payable{suffix}"
    df[payable_col] = (df[exp_col].astype(float) - df[paid_col].astype(float)).cumsum()

    # write back schedule
    df.to_parquet(sched_pq, index=False)

    # optionally update summary if present
    summary_info = {}
    if summ_pq.exists():
        summ = pd.read_parquet(summ_pq)
        try:
            # keep single-row summary if that is the shape; otherwise compute agregates
            total_paid = float(df[paid_col].sum())
            eop_payable = float(df[payable_col].iloc[-1])
            # add/update fields (names independent of suffix)
            summ["Tax_Paid_Total"] = total_paid
            summ["Tax_Payable_EOP"] = eop_payable
            summ.to_parquet(summ_pq, index=False)
            summary_info = {"Tax_Paid_Total": total_paid, "Tax_Payable_EOP": eop_payable}
        except Exception:
            pass

    print(json.dumps({
        "status": "ok",
        "schedule": str(sched_pq),
        "used_columns": {"Month_Index": month_col, "Tax_Expense": exp_col, "Tax_Paid": paid_col},
        "created_paid": created_paid,
        "payable_column": payable_col,
        "summary_updates": summary_info
    }, indent=2))

if __name__ == "__main__":
    main()

