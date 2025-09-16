# tools/m3_validate.py
#!/usr/bin/env python
import argparse, json, sys, os
import pandas as pd

def find_any(cols, candidates):
    for c in candidates:
        if c in cols: return c
    return None

def main():
    p = argparse.ArgumentParser()
    p.add_argument("--out", required=True)
    args = p.parse_args()
    out = args.out

    fi = os.path.join(out, "m3_finance_index.parquet")
    rev = os.path.join(out, "m3_revolver_schedule.parquet")
    ins = os.path.join(out, "m3_insurance_schedule.parquet")

    if not os.path.exists(fi):
        print(f"[M3][ERR] Missing required artifact: {fi}", file=sys.stderr)
        return 3

    # Horizon
    N = None
    cal = os.path.join(out, "m0_calendar.parquet")
    if os.path.exists(cal):
        N = int(pd.read_parquet(cal)["Month_Index"].max())

    # Finance index roles (downstream-facing)
    df = pd.read_parquet(fi)
    cols = df.columns.tolist()
    roles = {
        "MONTH_INDEX": ["Month_Index","MONTH_INDEX","month_index","Period","Month"],
        "DEBT_OUT_PRINCIPAL": ["Debt_Principal_Closing_NAD_000","Outstanding_Principal_NAD_000","Principal_Outstanding_NAD_000"],
        "PRINCIPAL_DRAWS_CF": ["Principal_Draws_CF_NAD_000","Debt_Draws_NAD_000"],
        "PRINCIPAL_REPAY_CF": ["Principal_Repayments_CF_NAD_000","Debt_Repayments_NAD_000"],
    }
    role_map, warns = {}, []
    for role, cands in roles.items():
        hit = find_any(cols, cands)
        if hit is None:
            warns.append(f"Missing role {role} (accepted: {', '.join(cands)})")
        else:
            role_map[role] = hit

    # Month index continuity (soft warn)
    mi = role_map.get("MONTH_INDEX")
    if mi and N is not None:
        s = pd.to_numeric(df[mi], errors="coerce").dropna().astype(int)
        if not (s.min()==1 and s.max()==N and len(s)==N):
            warns.append(f"Month_Index not 1..{N} contiguous (min={s.min()}, max={s.max()}, rows={len(s)})")

    report = {
        "rows": int(len(df)),
        "cols_preview": cols[:12],
        "role_map": role_map
    }

    # Optional artifacts (if present, just show columns for sanity)
    if os.path.exists(rev):
        rc = pd.read_parquet(rev).columns.tolist()
        report["revolver_cols"] = rc[:12]
    if os.path.exists(ins):
        ic = pd.read_parquet(ins).columns.tolist()
        report["insurance_cols"] = ic[:12]

    print(json.dumps(report, indent=2))
    # Surface warnings but do not hard-fail (keeps pipeline moving while we lock M6)
    for w in warns:
        print(f"[M3][WARN] {w}", file=sys.stderr)
    return 0

if __name__ == "__main__":
    sys.exit(main())
