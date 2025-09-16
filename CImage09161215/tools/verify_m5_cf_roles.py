# tools/verify_m5_cf_roles.py
"""
Proof script to assert M5 contains CFO/CFI/CFF roles with the exact names used downstream.
Usage:
  .\.venv\Scripts\python.exe .\tools\verify_m5_cf_roles.py .\outputs
Emits:
  - outputs/m5_cf_roles_report.md
  - outputs/m5_cf_roles_debug.json
"""
from __future__ import annotations
import sys, json
from pathlib import Path
import pandas as pd

REQ = ["Month_Index", "CFO_NAD_000","CFI_NAD_000","CFF_NAD_000"]

def main(outputs: str) -> None:
    out = Path(outputs)
    df = pd.read_parquet(out / "m5_cash_flow_statement_final.parquet")
    ok = all(c in df.columns for c in REQ)
    rep = out / "m5_cf_roles_report.md"
    dbg = out / "m5_cf_roles_debug.json"
    lines = []
    lines.append("== M5.PROOF ==")
    lines.append(f"[OK] Loaded M5 cash flow: {out/'m5_cash_flow_statement_final.parquet'} shape={df.shape}")
    if ok:
        lines.append("[OK] Required roles present -> " + ", ".join(REQ))
    else:
        missing = [c for c in REQ if c not in df.columns]
        lines.append(f"[FAIL] Missing roles: {missing}")
        lines.append("Available (first 40): " + ", ".join(list(df.columns)[:40]))
    rep.write_text("\n".join(lines), encoding="utf-8")
    dbg.write_text(json.dumps({"columns": list(df.columns), "head": df.head(12).to_dict(orient="list")}, indent=2), encoding="utf-8")
    print(lines[0])
    for ln in lines[1:]:
        print(ln)

if __name__ == "__main__":
    main(sys.argv[1] if len(sys.argv) > 1 else ".")
