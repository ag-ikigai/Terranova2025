# tools/validate_m7_5b.py
from __future__ import annotations

import argparse
import json
from pathlib import Path

import pandas as pd


def _ok(msg): print(f"[OK] {msg}")
def _warn(msg): print(f"[WARN] {msg}")
def _fail(msg): print(f"[FAIL] {msg}")


REQUIRED_PL = ["Month_Index", "Total_Revenue_NAD_000", "DA_NAD_000", "EBIT_NAD_000", "Interest_NAD_000", "Tax_Expense_NAD_000", "NPAT_NAD_000"]
REQUIRED_CF_MIN = ["Month_Index", "CFO_NAD_000"]
REQUIRED_BS_MIN = ["Month_Index", "Assets_Total_NAD_000", "Liabilities_And_Equity_Total_NAD_000"]


def validate(outputs_dir: str) -> int:
    out = Path(outputs_dir)
    pl_p = out / "m7_5b_profit_and_loss.parquet"
    cf_p = out / "m7_5b_cash_flow.parquet"
    bs_p = out / "m7_5b_balance_sheet.parquet"
    debug_p = out / "m7_5b_debug.json"

    ret = 0
    print("\n== M7.5B ==")

    # Load
    pl = pd.read_parquet(pl_p)
    cf = pd.read_parquet(cf_p)
    bs = pd.read_parquet(bs_p)

    # Minimal role presence (NAD)
    miss_pl = [r for r in REQUIRED_PL if r not in pl.columns]
    if miss_pl:
        _fail(f"PL/minimal: missing {miss_pl}")
        ret = 1
    else:
        _ok(f"PL/minimal: roles present -> { [c.split('_NAD_000')[0] for c in REQUIRED_PL if c.endswith('_NAD_000')] + ['Month_Index'] }")

    miss_cf = [r for r in REQUIRED_CF_MIN if r not in cf.columns]
    if miss_cf:
        _fail(f"CF/minimal: missing {miss_cf}")
        ret = 1
    else:
        _ok(f"CF/minimal: roles present -> { [c.split('_NAD_000')[0] for c in REQUIRED_CF_MIN if c.endswith('_NAD_000')] + ['Month_Index'] }")

    miss_bs = [r for r in REQUIRED_BS_MIN if r not in bs.columns]
    if miss_bs:
        _fail(f"BS/minimal: missing {miss_bs}")
        ret = 1
    else:
        _ok("BS/minimal: roles present -> ['MONTH_INDEX','TOTAL_ASSETS','TOTAL_L_E']")

    # BS tie (NAD)
    if all(c in bs.columns for c in REQUIRED_BS_MIN[1:]):
        tie_ok = (bs["Assets_Total_NAD_000"] - bs["Liabilities_And_Equity_Total_NAD_000"]).abs().max() < 1e-6
        if tie_ok:
            _ok("BS: totals tie -> Assets_Total_NAD_000 = Liabilities_And_Equity_Total_NAD_000")
        else:
            _fail("BS: totals do not tie (NAD)")
            ret = 1

    # Optional Cash link (if both are present)
    cf_close = next((c for c in cf.columns if c.lower().startswith("closing_cash")), None)
    bs_cash = next((c for c in bs.columns if c.lower().startswith("cash_and_cash_equivalents")), None)
    if cf_close and bs_cash:
        link_ok = (cf[cf_close] - bs[bs_cash]).abs().max() < 1e-6
        if link_ok:
            _ok(f"Cash link: {cf_close} equals {bs_cash}")
        else:
            _warn("Cash link: mismatch between CF closing cash and BS cash")
    else:
        _warn("Cash link: skipped (closing cash and/or BS cash not found)")

    # FX debug keys
    if debug_p.exists():
        dbg = json.loads(debug_p.read_text(encoding="utf-8"))
        fx_keys = [k for k in dbg.keys() if k.startswith("fx")]
        if fx_keys:
            _ok(f"FX: debug metadata present -> {sorted(fx_keys)[:4]}{'...' if len(fx_keys)>4 else ''}")
        else:
            _warn(f"FX: no metadata keys starting with 'fx' in {debug_p}")
    else:
        _warn(f"FX: debug json not found: {debug_p}")

    # USD columns presence across statements (expect some)
    def count_usd_cols(p: Path) -> int:
        if not p.exists(): return 0
        df = pd.read_parquet(p)
        return sum(1 for c in df.columns if c.endswith("_USD_000"))

    usd_total = count_usd_cols(pl_p) + count_usd_cols(cf_p) + count_usd_cols(bs_p)
    if usd_total >= 3:
        _ok(f"USD: found {usd_total} _USD_000 columns across statements")
    else:
        _fail(f"USD: expected several _USD_000 columns across statements, found {usd_total}")
        ret = 1

    return ret


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("outputs", help="outputs directory (e.g., .\\outputs)")
    args = ap.parse_args()
    sys_exit = validate(args.outputs)
    raise SystemExit(sys_exit)
