import sys
from pathlib import Path
import pandas as pd

REQ = ["Month_Index","Taxable_Income_NAD_000","Tax_Rate","Tax_Expense_NAD_000","Tax_Paid_NAD_000","Tax_Payable_End_NAD_000"]

def main(out_dir: str) -> int:
    out = Path(out_dir)
    df = pd.read_parquet(out / "m4_tax_schedule.parquet")
    miss = [c for c in REQ if c not in df.columns]
    if miss:
        print(f"[M4][FAIL] m4_tax_schedule missing columns: {miss}")
        return 2
    print("[M4][OK] Validation passed.")
    return 0

if __name__ == "__main__":
    sys.exit(main(sys.argv[1] if len(sys.argv) > 1 else "."))