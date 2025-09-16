"""
M8.B2 – Promoter Scorecard (from-scratch, IFRS-driven, dual-currency)

Inputs (all under <outputs_dir>):
  - m7_5b_profit_and_loss.parquet
  - m7_5b_balance_sheet.parquet
  - m7_5b_cash_flow.parquet
  - m8b_fx_curve.parquet   # Month_Index + FX column (NAD per USD)

Outputs:
  - m8b2_promoter_scorecard_monthly.parquet
  - m8b2_promoter_scorecard_yearly.parquet
  - m8b2_debug.json
  - m8b2_smoke.md
"""

from __future__ import annotations
import json
from pathlib import Path
from typing import Dict, List
import pandas as pd


FX_COL_SYNONYMS: List[str] = [
    "USD_to_NAD", "USD_NAD", "FX_USD_NAD", "USDtoNAD", "NAD_per_USD", "Rate_USD_to_NAD"
]

# Column synonyms (unit: NAD '000 unless noted)
PL_SYN = {
    "revenue": ["Total_Revenue_NAD_000", "Revenue_NAD_000"],
    "ebitda":  ["EBITDA_NAD_000"],
    "ebit":    ["EBIT_NAD_000", "Operating_Profit_NAD_000"],
    "npat":    ["NPAT_NAD_000", "Net_Profit_After_Tax_NAD_000"],
    "opex":    ["Total_OPEX_NAD_000"]
}
BS_SYN = {
    "cash": ["Cash_and_Cash_Equivalents_NAD_000", "Cash_NAD_000"],
    "ca":   ["Current_Assets_NAD_000"],
    "cl":   ["Current_Liabilities_NAD_000"],
    # Optional helper for Quick Ratio if present anywhere:
    "inv":  ["Inventory_Balance_NAD_000", "Inventory_NAD_000"]
}
CF_SYN = {
    "cfo": ["CFO_NAD_000"],
    "cfi": ["CFI_NAD_000"],
    "cff": ["CFF_NAD_000"],
    "close_cash": ["Closing_Cash_NAD_000"],
}

def _read_parquet(p: Path, what: str) -> pd.DataFrame:
    if not p.exists():
        raise FileNotFoundError(f"[M8.B2][FAIL] Missing {what}: {p}")
    df = pd.read_parquet(p)
    if df.empty:
        raise ValueError(f"[M8.B2][FAIL] Empty {what}: {p}")
    return df

def _pick(df: pd.DataFrame, keys: List[str]) -> str|None:
    for k in keys:
        if k in df.columns:
            return k
    return None

def _fx_col(fx: pd.DataFrame) -> str:
    for c in FX_COL_SYNONYMS:
        if c in fx.columns:
            return c
    # fallback: first numeric col not Month_Index
    nums = [c for c in fx.columns if c != "Month_Index" and pd.api.types.is_numeric_dtype(fx[c])]
    if not nums:
        raise ValueError("[M8.B2][FAIL] Could not detect FX column (NAD per USD).")
    return nums[0]

def _add_usd(df: pd.DataFrame, fx: pd.DataFrame, fx_col: str) -> pd.DataFrame:
    if "Month_Index" not in df.columns:
        return df
    x = df.merge(fx[["Month_Index", fx_col]], on="Month_Index", how="left", validate="m:1")
    # USD twins for *_NAD_000
    nad_cols = [c for c in x.columns if c.endswith("_NAD_000")]
    for c in nad_cols:
        usd = c.replace("_NAD_000", "_USD_000")
        x[usd] = x[c] / x[fx_col]
    return x

def _monthly_yearly(df: pd.DataFrame, metrics: Dict[str, str]) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Return (monthly_df, yearly_df) for requested metrics."""
    out_m = df[["Month_Index"] + list(metrics.values())].copy()
    # Year index from Month_Index (1..12 -> Year 1, 13..24 -> Year 2, etc.)
    out_m["Year_Index"] = ((out_m["Month_Index"] - 1) // 12) + 1

    # Yearly: flows (sum), ratios (mean), stocks (mean of month-end) – here we treat
    # all metrics as either ratio or flow/stock via simple rules by suffix.
    # We define which are ratios explicitly:
    ratio_keys = {"Gross_Margin", "EBITDA_Margin", "Operating_Margin", "Net_Margin"}
    agg: Dict[str, str] = {}
    for k, c in metrics.items():
        if any(k.endswith(s) for s in ["_Ratio", "_Margin"] ) or k in ratio_keys:
            agg[c] = "mean"
        else:
            agg[c] = "sum" if c.endswith(("_NAD_000", "_USD_000")) and ("CFO" in c or "CFI" in c or "CFF" in c or "Revenue" in c) else "mean"

    out_y = out_m.groupby("Year_Index", as_index=False).agg(agg)
    return out_m, out_y

def run_m8B2(outputs_dir: str, currency: str = "NAD", strict: bool = True, diagnostic: bool = True) -> None:
    out = Path(outputs_dir)
    pl = _read_parquet(out / "m7_5b_profit_and_loss.parquet", "M7.5B P&L")
    bs = _read_parquet(out / "m7_5b_balance_sheet.parquet", "M7.5B Balance Sheet")
    cf = _read_parquet(out / "m7_5b_cash_flow.parquet", "M7.5B Cash Flow")
    fx = _read_parquet(out / "m8b_fx_curve.parquet", "FX curve")

    fx_col = _fx_col(fx)

    # Resolve required columns
    rev = _pick(pl, PL_SYN["revenue"])
    ebitda = _pick(pl, PL_SYN["ebitda"])
    ebit = _pick(pl, PL_SYN["ebit"])
    npat = _pick(pl, PL_SYN["npat"])
    opex = _pick(pl, PL_SYN["opex"])

    ca = _pick(bs, BS_SYN["ca"])
    cl = _pick(bs, BS_SYN["cl"])
    cash = _pick(bs, BS_SYN["cash"])
    inv = _pick(bs, BS_SYN["inv"])  # optional

    cfo = _pick(cf, CF_SYN["cfo"])
    cfi = _pick(cf, CF_SYN["cfi"])
    cff = _pick(cf, CF_SYN["cff"])
    close_cash = _pick(cf, CF_SYN["close_cash"])

    required = {
        "P&L:Revenue": rev, "P&L:EBITDA": ebitda, "P&L:EBIT": ebit, "P&L:NPAT": npat, "P&L:OPEX": opex,
        "BS:CA": ca, "BS:CL": cl, "BS:Cash": cash,
        "CF:CFO": cfo, "CF:CFI": cfi, "CF:CFF": cff, "CF:ClosingCash": close_cash
    }
    missing = [k for k, v in required.items() if v is None]
    if missing:
        msg = "[M8.B2][FAIL] Missing required columns (M7.5B IFRS pack should provide these): " + ", ".join(missing)
        if strict:
            raise RuntimeError(msg)
        else:
            print(msg)

    # Base frame – join PL, BS, CF on Month_Index
    for df_ in (pl, bs, cf):
        if "Month_Index" not in df_.columns:
            raise RuntimeError("[M8.B2][FAIL] 'Month_Index' missing in a core artifact.")

    base = pl[["Month_Index", rev, ebitda, ebit, npat, opex]].merge(
        bs[["Month_Index", ca, cl, cash] + ([inv] if inv else [])],
        on="Month_Index", how="left", validate="1:1"
    ).merge(
        cf[["Month_Index", cfo, cfi, cff, close_cash]],
        on="Month_Index", how="left", validate="1:1"
    )

    # KPI calculations (Monthly, NAD)
    eps = 1e-9
    k = pd.DataFrame({"Month_Index": base["Month_Index"]})
    # Liquidity / WC
    k["Current_Ratio"] = (base[ca] / base[cl]).replace([pd.NA, pd.NaT], pd.NA)
    if inv:
        k["Quick_Ratio"] = ((base[ca] - base[inv]) / base[cl]).replace([pd.NA, pd.NaT], pd.NA)
    k["Working_Capital_NAD_000"] = (base[ca] - base[cl])
    # Profitability margins
    k["Gross_Margin"]    = pd.NA  # not modeled explicitly; left for future if COGS available
    k["EBITDA_Margin"]   = (base[ebitda] / (base[rev] + eps))
    k["Operating_Margin"]= (base[ebit] / (base[rev] + eps))
    k["Net_Margin"]      = (base[npat] / (base[rev] + eps))
    # Cash-flow health
    k["CFO_NAD_000"] = base[cfo]
    k["CFI_NAD_000"] = base[cfi]
    k["CFF_NAD_000"] = base[cff]
    # Simple FCF proxy (to firm): CFO + CFI (assuming CFI is mostly CAPEX outflow)
    k["Free_Cash_Flow_NAD_000"] = base[cfo] + base[cfi]
    # Cash runway – operational (months): Cash / max(1, monthly operating outflow)
    monthly_oper_outflow = (-base[cfo]).clip(lower=0.0) + 0.0
    k["Cash_Runway_Months"] = (base[cash] / (monthly_oper_outflow.replace(0.0, eps))).clip(upper=120.0)

    # Merge FX and create USD twins for *_NAD_000 flows/stocks
    k = k.merge(base[["Month_Index", rev, ebitda, ebit, npat, opex, cash]], on="Month_Index", how="left", validate="1:1")
    k = _add_usd(k, fx, fx_col)

    # Build Yearly summary (calendar buckets from Month_Index)
    # Metrics to export (Monthly and Yearly)
    metrics = {
        "Current_Ratio": "Current_Ratio",
        "Quick_Ratio": "Quick_Ratio" if "Quick_Ratio" in k.columns else None,
        "Working_Capital_NAD_000": "Working_Capital_NAD_000",
        "Working_Capital_USD_000": "Working_Capital_USD_000" if "Working_Capital_USD_000" in k.columns else None,
        "EBITDA_Margin": "EBITDA_Margin",
        "Operating_Margin": "Operating_Margin",
        "Net_Margin": "Net_Margin",
        "CFO_NAD_000": "CFO_NAD_000",
        "CFI_NAD_000": "CFI_NAD_000",
        "CFF_NAD_000": "CFF_NAD_000",
        "CFO_USD_000": "CFO_USD_000" if "CFO_USD_000" in k.columns else None,
        "CFI_USD_000": "CFI_USD_000" if "CFI_USD_000" in k.columns else None,
        "CFF_USD_000": "CFF_USD_000" if "CFF_USD_000" in k.columns else None,
        "Free_Cash_Flow_NAD_000": "Free_Cash_Flow_NAD_000",
        "Free_Cash_Flow_USD_000": "Free_Cash_Flow_USD_000" if "Free_Cash_Flow_USD_000" in k.columns else None,
        "Cash_Runway_Months": "Cash_Runway_Months",
    }
    metrics = {k1: v for k1, v in metrics.items() if v is not None}

    # Compose NAD->USD twins for WC/FCF if present
    if "Working_Capital_NAD_000" in k.columns and fx_col in (fx.columns):
        k["Working_Capital_USD_000"] = k["Working_Capital_NAD_000"] / fx[fx_col].reindex(k["Month_Index"]-1).values
    if "Free_Cash_Flow_NAD_000" in k.columns and fx_col in (fx.columns):
        k["Free_Cash_Flow_USD_000"] = k["Free_Cash_Flow_NAD_000"] / fx[fx_col].reindex(k["Month_Index"]-1).values

    monthly, yearly = _monthly_yearly(k, metrics)

    # Emit artifacts
    (out / "m8b2_promoter_scorecard_monthly.parquet").unlink(missing_ok=True)
    (out / "m8b2_promoter_scorecard_yearly.parquet").unlink(missing_ok=True)
    monthly.to_parquet(out / "m8b2_promoter_scorecard_monthly.parquet", index=False)
    yearly.to_parquet(out / "m8b2_promoter_scorecard_yearly.parquet", index=False)

    dbg = {
        "fx_col": fx_col,
        "resolved_columns": {
            "revenue": rev, "ebitda": ebitda, "ebit": ebit, "npat": npat, "opex": opex,
            "cash": cash, "CA": ca, "CL": cl, "Inventory": inv,
            "CFO": cfo, "CFI": cfi, "CFF": cff, "Closing_Cash": close_cash
        },
        "optional_metrics": {
            "Quick_Ratio_included": bool(inv is not None)
        },
        "notes": [
            "Yearly ratios are arithmetic means of monthly ratios.",
            "Free_Cash_Flow ≈ CFO + CFI (negative CFI represents CAPEX outflows).",
            "Cash_Runway_Months uses operational outflow proxy = max(0, -CFO)."
        ]
    }
    with open(out / "m8b2_debug.json", "w", encoding="utf-8") as f:
        json.dump(dbg, f, indent=2)

    with open(out / "m8b2_smoke.md", "w", encoding="utf-8") as f:
        f.write("## M8.B2 Smoke\n")
        f.write(f"- Monthly rows: {len(monthly)}\n- Yearly rows: {len(yearly)}\n")
        f.write(f"- Columns (monthly): {list(monthly.columns)[:12]}...\n")
        f.write(f"- FX column used: {fx_col}\n")

    print("[M8.B2][OK]  Emitted: m8b2_promoter_scorecard_monthly.parquet, m8b2_promoter_scorecard_yearly.parquet")
    print("[M8.B2][OK]  Smoke → m8b2_smoke.md ; Debug → m8b2_debug.json")
