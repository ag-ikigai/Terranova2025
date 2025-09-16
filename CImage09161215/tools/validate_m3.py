import sys, json
from pathlib import Path
import pandas as pd

REQUIRED = [
    "Month_Index",
    "Revolver_Open_Balance_NAD_000",
    "Revolver_Draw_NAD_000",
    "Revolver_Repayment_NAD_000",
    "Revolver_Close_Balance_NAD_000",
    "Revolver_Interest_Expense_NAD_000",
]

def main(out_dir: str) -> int:
    out = Path(out_dir)
    rev = pd.read_parquet(out / "m3_revolver_schedule.parquet")
    cal = pd.read_parquet(out / "m0_calendar.parquet")
    missing = [c for c in REQUIRED if c not in rev.columns]
    if missing:
        print(f"[M3][FAIL] m3_revolver_schedule missing columns: {missing}")
        return 2
    # basic identities + alignment
    if len(rev) != len(cal):
        print(f"[M3][FAIL] row count mismatch: rev={len(rev)} cal={len(cal)}")
        return 3
    # close = open + draw - repay
    check = (rev["Revolver_Close_Balance_NAD_000"].round(6) ==
             (rev["Revolver_Open_Balance_NAD_000"] + rev["Revolver_Draw_NAD_000"] - rev["Revolver_Repayment_NAD_000"]).round(6))
    if not bool(check.all()):
        print("[M3][FAIL] closing balance identity failed")
        return 4
    print("[M3][OK] Validation passed.")
    return 0

if __name__ == "__main__":
    sys.exit(main(sys.argv[1] if len(sys.argv) > 1 else "."))