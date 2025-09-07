# m5_run_accept_either_v5.py
#!/usr/bin/env python3
from __future__ import annotations
import argparse, json
from pathlib import Path
from typing import Dict, List, Optional, Tuple
import pandas as pd

def _norm(s: str) -> str:
    return "".join(ch for ch in s.lower() if ch.isalnum())

def _pick_any(cols: List[str], patterns: List[str]) -> Optional[str]:
    norms = {c: _norm(c) for c in cols}
    for pat in patterns:
        t = _norm(pat)
        for c, n in norms.items():
            if t in n:
                return c
    return None

def _read_parquet(p: Path, label: str) -> pd.DataFrame:
    if not p.exists():
        raise FileNotFoundError(f"[{label}] Missing file: {p.name} in {p.parent}")
    try:
        return pd.read_parquet(p)
    except Exception as e:
        raise RuntimeError(f"[{label}] Failed to read {p}: {e}") from e

def _discover_m2(out: Path, debug: Dict) -> Tuple[Path, Path]:
    pl_candidates = ["m2_pl_schedule.parquet", "m2_pl_statement.parquet"]
    wc_candidates = ["m2_working_capital_schedule.parquet", "m2_working_capital.parquet", "m2_nwc_schedule.parquet"]
    pl_path = next((out / n for n in pl_candidates if (out / n).exists()), None)
    wc_path = next((out / n for n in wc_candidates if (out / n).exists()), None)
    if pl_path is None:
        raise FileNotFoundError("[M5/M2] Could not find any M2 P&L parquet in outputs.")
    if wc_path is None:
        raise FileNotFoundError("[M5/M2] Could not find any M2 Working Capital parquet in outputs.")
    debug["m2_pl_path"] = str(pl_path)
    debug["m2_wc_path"] = str(wc_path)
    return pl_path, wc_path

def _map_columns(pl_cols: List[str], wc_cols: List[str]) -> Dict[str, str]:
    month_col = _pick_any(pl_cols, ["Month_Index","MonthIndex","month_index","period_index","period"]) \
             or _pick_any(wc_cols, ["Month_Index","MonthIndex","month_index","period_index","period"])
    if month_col is None:
        raise KeyError("[M5] Could not locate a month/period index column in M2 artifacts.")
    npat_col = _pick_any(pl_cols, ["NPAT","Net_Profit_After_Tax","Profit_After_Tax","Net_Income_After_Tax","NetIncomeAfterTax","PAT","Net_Profit"])
    if npat_col is None:
        raise KeyError("[M5] Could not locate Net Profit After Tax (NPAT) column in M2 P&L.")
    da_col = _pick_any(pl_cols, ["Depreciation_and_Amortization","DepreciationAmortization","DandA","D_A","DA","Depreciation"])
    if da_col is None:
        raise KeyError("[M5] Could not locate Depreciation (or Depreciation & Amortization) column in M2 P&L.")
    nwc_cf_col = _pick_any(wc_cols, ["Cash_Flow_from_NWC_Change","CashFlow_from_NWC_Change","CF_from_NWC_Change","Change_in_NWC_Cash_Flow","NWC_Cash_Flow_Change"])
    if nwc_cf_col is None:
        bal_col = _pick_any(wc_cols, ["NWC_Balance"])
        if bal_col is None:
            raise KeyError("[M5] Could not locate NWC cash-flow column or NWC balance to derive ΔNWC.")
        nwc_cf_col = f"__derived_cf_from_{bal_col}"
    return {"month": month_col, "npat": npat_col, "da": da_col, "nwc_cf": nwc_cf_col}

def _derive_nwc_cf_if_needed(wc: pd.DataFrame, month_col: str, nwc_cf_col: str) -> pd.DataFrame:
    if nwc_cf_col.startswith("__derived_cf_from_"):
        bal_col = nwc_cf_col.replace("__derived_cf_from_", "")
        if bal_col not in wc.columns:
            raise KeyError(f"[M5] Internal error: expected balance column {bal_col} in WC schedule.")
        wc = wc.sort_values(by=month_col).copy()
        wc["__nwc_delta"] = wc[bal_col].diff().fillna(0.0)
        wc["NWC_CF_DERIVED"] = -wc["__nwc_delta"]
    return wc

def _write_json(p: Path, obj: Dict): p.write_text(json.dumps(obj, indent=2))
def _write_smoke(p: Path, lines: List[str]): p.write_text("\n".join(lines))

def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", required=True, help="Outputs folder (contains M2 parquet(s), receives M5 outputs).")
    ap.add_argument("--currency", default="NAD")
    ap.add_argument("--inspect-only", action="store_true")
    args = ap.parse_args()

    out = Path(args.out).resolve()
    out.mkdir(parents=True, exist_ok=True)
    debug = {"args": vars(args), "steps": []}

    pl_path, wc_path = _discover_m2(out, debug); debug["steps"].append("discover_m2")
    pl = _read_parquet(pl_path, "M5/M2-PL")
    wc = _read_parquet(wc_path, "M5/M2-WC")
    debug["m2_pl_cols"] = list(map(str, pl.columns))
    debug["m2_wc_cols"] = list(map(str, wc.columns))

    mapping = _map_columns(debug["m2_pl_cols"], debug["m2_wc_cols"])
    debug["mapping"] = mapping
    _write_json(out / "m5_debug_dump.json", debug)

    if args.inspect_only:
        print(f"[OK] Inspect-only: found PL= {pl_path.name}, WC= {wc_path.name}. Debug -> {out / 'm5_debug_dump.json'}")
        return 0

    month, npat, da, nwc_cf_col = mapping["month"], mapping["npat"], mapping["da"], mapping["nwc_cf"]
    for col in [npat, da]:
        if not pd.api.types.is_numeric_dtype(pl[col]): pl[col] = pd.to_numeric(pl[col], errors="coerce")
    if nwc_cf_col in wc.columns:
        if not pd.api.types.is_numeric_dtype(wc[nwc_cf_col]): wc[nwc_cf_col] = pd.to_numeric(wc[nwc_cf_col], errors="coerce")
    else:
        wc = _derive_nwc_cf_if_needed(wc, month, nwc_cf_col)

    wc_cf_col_effective = nwc_cf_col if nwc_cf_col in wc.columns else "NWC_CF_DERIVED"
    j = pd.merge(pl[[month, npat, da]].copy(), wc[[month, wc_cf_col_effective]].copy(), on=month, how="inner")

    result = j.rename(columns={
        month: "Month_Index",
        npat: "NPAT_NAD_000",
        da: "DandA_NAD_000",
        wc_cf_col_effective: "NWC_CF_NAD_000",
    }).copy()

    result["CFO_NAD_000"] = result["NPAT_NAD_000"].fillna(0.0) + result["DandA_NAD_000"].fillna(0.0) + result["NWC_CF_NAD_000"].fillna(0.0)

    final_path = out / "m5_cash_flow_statement_final.parquet"
    result.to_parquet(final_path, index=False)

    smoke = [
        "# M5 Smoke Report",
        "",
        f"- Inputs: {pl_path.name}, {wc_path.name}",
        f"- Currency: {args.currency} (values appear in *_NAD_000; i.e., thousands of NAD)",
        f"- Rows: {len(result)}",
        f"- Columns: {list(result.columns)}",
        f"- Sum(CFO_NAD_000): {float(result['CFO_NAD_000'].sum()):,.3f}",
        "",
        "Top 5 rows:",
        result.head().to_string(index=False),
    ]
    _write_smoke(out / "m5_smoke_report.md", smoke)
    debug["steps"].append("write_outputs")
    debug["m5_final_path"] = str(final_path)
    debug["m5_cols"] = list(map(str, result.columns))
    _write_json(out / "m5_debug_dump.json", debug)

    print(f"[OK] M5 cash flow computed -> {final_path.name}. Smoke -> {out / 'm5_smoke_report.md'}")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
