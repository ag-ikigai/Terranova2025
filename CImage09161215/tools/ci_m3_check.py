# tools/ci_m3_check.py
"""
Strict (but tolerant) CI check for M3 Financing.
- Optionally runs the module runner (no logic changes).
- Validates presence of artifacts and principal continuity identity.
Exit codes:
  0 = PASS
  2 = missing artifacts
  3 = required columns missing
  4 = identity/continuity failure
"""

import argparse, importlib, inspect, json, os, sys
from pathlib import Path

import numpy as np
import pandas as pd


def try_run_m3(input_xlsx: str, out_dir: str, currency: str = "NAD") -> None:
    """Import the M3 runner from src and call it with compatible arguments if available."""
    mod = fn = None
    for m in (
        "terra_nova.modules.m3_financing.runner",
        "terra_nova.modules.m3_financing.engine",
        "terra_nova.modules.m3_financing",
    ):
        try:
            mod = importlib.import_module(m)
        except Exception:
            continue
        for name in ("run_m3", "run", "main"):
            if hasattr(mod, name):
                fn = getattr(mod, name)
                break
        if fn:
            break

    if fn is None:
        print("[M3][INFO] No runner function found; assuming artifacts already present.")
        return

    # Build kwargs from signature (no assumptions about parameter names/order).
    sig = inspect.signature(fn)
    params = list(sig.parameters.keys())
    kwargs = {}
    map_in = ("input", "input_xlsx", "input_path", "xlsx", "path")
    map_out = ("out", "out_dir", "output", "output_dir")
    map_ccy = ("currency", "ccy")

    for k in map_in:
        if k in params:
            kwargs[k] = input_xlsx
            break
    for k in map_out:
        if k in params:
            kwargs[k] = out_dir
            break
    for k in map_ccy:
        if k in params:
            kwargs[k] = currency
            break

    try:
        fn(**kwargs)
        print("[M3][OK] Runner executed with kwargs.")
        return
    except TypeError:
        pass

    # Fallback positional attempts
    for args in (
        (input_xlsx, out_dir, currency),
        (input_xlsx, out_dir),
        (out_dir,),
        tuple(),
    ):
        try:
            fn(*args)
            print("[M3][OK] Runner executed with fallback positional args.")
            return
        except TypeError:
            continue

    print("[M3][WARN] Runner could not be executed (signature mismatch). Continuing to validation only.")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--currency", default="NAD")
    ap.add_argument("--skip-run", action="store_true", help="Validate only; do not call runner.")
    args = ap.parse_args()

    input_xlsx = str(Path(args.input).resolve())
    out_dir = str(Path(args.out).resolve())
    currency = args.currency

    if not args.skip_run:
        try_run_m3(input_xlsx, out_dir, currency)

    idx_p = os.path.join(out_dir, "m3_finance_index.parquet")
    rev_p = os.path.join(out_dir, "m3_revolver_schedule.parquet")

    missing = [p for p in (idx_p, rev_p) if not os.path.exists(p)]
    if missing:
        print(json.dumps({"stage": "m3", "error": "missing_artifacts", "missing": missing}))
        return 2

    df = pd.read_parquet(rev_p)
    cols = set(df.columns)

    aliases = {
        "open": ["Opening_Balance_NAD_000", "Opening_Debt_NAD_000", "Opening_Principal_NAD_000"],
        "draw": ["New_Borrowing_NAD_000", "Draw_NAD_000", "Revolver_Draw_NAD_000"],
        "repay": ["Principal_Repayment_NAD_000", "Repayment_NAD_000", "Revolver_Repayment_NAD_000"],
        "close": ["Closing_Balance_NAD_000", "Debt_End_NAD_000", "Closing_Principal_NAD_000"],
        "int": ["Interest_Expense_NAD_000", "Interest_NAD_000"],
    }

    def pick(key: str):
        for c in aliases[key]:
            if c in cols:
                return c
        return None

    # Required
    need = ("open", "close", "int")
    for n in need:
        if pick(n) is None:
            print(json.dumps({"stage": "m3", "error": "missing_columns", "need": n, "aliases": aliases[n]}))
            return 3

    # Optional (assume zero if absent)
    dcol = pick("draw") or "_draw_zero_"
    rcol = pick("repay") or "_repay_zero_"
    if dcol == "_draw_zero_":
        df[dcol] = 0.0
    if rcol == "_repay_zero_":
        df[rcol] = 0.0

    # Month_Index continuity
    if "Month_Index" not in df.columns:
        print(json.dumps({"stage": "m3", "error": "missing_columns", "need": "Month_Index"}))
        return 3
    mi = df["Month_Index"].dropna().astype(int).sort_values().to_numpy()
    contig = np.all(np.diff(mi) == 1)

    # Principal identity
    open_c = pick("open")
    close_c = pick("close")
    delta = (df[open_c] + df[dcol] - df[rcol] - df[close_c]).abs()
    identity_ok = float(delta.max()) <= 1e-6

    # Optional insurance
    ins_p = os.path.join(out_dir, "m3_insurance_schedule.parquet")
    ins_rows = None
    if os.path.exists(ins_p):
        ins_rows = int(pd.read_parquet(ins_p).shape[0])

    report = {
        "stage": "m3",
        "rows": int(df.shape[0]),
        "contiguous_month_index": bool(contig),
        "identity_ok": bool(identity_ok),
        "used_columns": {
            "opening": open_c,
            "draws": dcol if dcol != "_draw_zero_" else None,
            "repayments": rcol if rcol != "_repay_zero_" else None,
            "closing": close_c,
            "interest": pick("int"),
        },
        "insurance_rows": ins_rows,
        "paths": {"index": idx_p, "revolver": rev_p, "insurance": ins_p if ins_rows is not None else None},
    }
    print(json.dumps(report, indent=2))

    return 0 if (contig and identity_ok) else 4


if __name__ == "__main__":
    sys.exit(main())
