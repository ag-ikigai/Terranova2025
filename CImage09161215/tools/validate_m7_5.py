### 3) `tools/validate_m7_5.py` — **standalone validator for the wiring artifact**

### This avoids touching your existing `validate_contracts.py`. Drop it in `tools/` and run:
### `.\.venv\Scripts\python.exe .\tools\validate_m7_5.py .\outputs`

### ```python
#!/usr/bin/env python3
import sys, json, argparse, textwrap, os
import pandas as pd

SYN = {
    "MONTH_INDEX": ["Month_Index","MONTH_INDEX"],
    "FX_USD_TO_NAD": ["FX_USD_to_NAD","FX_USD_NAD","FX_USD2NAD"],
    "JUNIOR_EQUITY_IN_NAD_000": ["Junior_Equity_In_NAD_000","Equity_Injection_NAD_000","Junior_Funding_NAD_000"],
    "OPTION": ["Option"],
    "INSTRUMENT": ["Instrument"],
}

def find_col(cols, candidates):
    cols_norm = {c.lower(): c for c in cols}
    for cand in candidates:
        if cand.lower() in cols_norm: return cols_norm[cand.lower()]
    return None

def main():
    ap = argparse.ArgumentParser(description="Validate M7.5 junior financing artifact")
    ap.add_argument("out_dir")
    args = ap.parse_args()

    f_parq = os.path.join(args.out_dir, "m7_5_junior_financing.parquet")
    f_csv  = os.path.join(args.out_dir, "m7_5_junior_financing.csv")
    if not os.path.exists(f_parq) and not os.path.exists(f_csv):
        print("[FAIL] M7.5: artifact not found (parquet/csv missing)"); sys.exit(2)

    df = None
    if os.path.exists(f_parq):
        try:
            df = pd.read_parquet(f_parq)
        except Exception as e:
            print(f"[WARN] could not read parquet: {e}")
    if df is None and os.path.exists(f_csv):
        df = pd.read_csv(f_csv)

    if df is None or df.empty:
        print("[FAIL] M7.5: empty or unreadable"); sys.exit(2)

    cols = list(df.columns)
    missing = []
    month_col = find_col(cols, SYN["MONTH_INDEX"])
    inj_col   = find_col(cols, SYN["JUNIOR_EQUITY_IN_NAD_000"])
    option_c  = find_col(cols, SYN["OPTION"])
    instr_c   = find_col(cols, SYN["INSTRUMENT"])
    fx_c      = find_col(cols, SYN["FX_USD_TO_NAD"])  # optional

    if month_col is None: missing.append("MONTH_INDEX")
    if inj_col   is None: missing.append("JUNIOR_EQUITY_IN_NAD_000")
    if option_c  is None: missing.append("OPTION")
    if instr_c   is None: missing.append("INSTRUMENT")

    if missing:
        print(f"[FAIL] M7.5: missing roles: {missing}\nAvailable columns: {cols}"); sys.exit(2)

    if not (df[inj_col].abs().sum() > 0):
        print("[FAIL] M7.5: injection column present but all zeros"); sys.exit(2)

    print(f"[OK] M7.5: {len(df)} rows; required roles found. "
          f"Month={month_col}, Injection={inj_col}, Option={option_c}, Instrument={instr_c}"
          + (f", FX={fx_c}" if fx_c else ""))
    sys.exit(0)

if __name__ == "__main__":
    main()
