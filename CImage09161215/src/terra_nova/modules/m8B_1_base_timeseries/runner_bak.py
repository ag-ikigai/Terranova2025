# -*- coding: utf-8 -*-
"""
M8B-1 | Base timeseries (calendar + FX + USD twins)

Inputs (all from ./outputs):
- m7_5b_profit_and_loss.parquet
- m7_5b_balance_sheet.parquet
- m7_5b_cash_flow.parquet
- m8b_fx_curve.parquet           (preferred; optional)
  fallbacks: m0_inputs/FX_Path.parquet or FX_Path.parquet (optional)

Outputs (into ./outputs):
- m8b_base_timeseries.parquet    (monthly; NAD + USD twins)
- m8b1_debug.json
- m8b1_smoke.md

Notes:
- We never recompute upstream business logic; we just join, normalize, and add USD
- USD = NAD / FX, where FX is NAD_per_USD or any of the common synonyms.
- We keep running with informative WARNs if some inputs are missing (strict=False).
"""

from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import pandas as pd


FX_COL_SYNONYMS: List[str] = [
    "NAD_per_USD", "USD_to_NAD", "USD_NAD", "FX_USD_NAD", "USDtoNAD",
    "Rate_USD_to_NAD",
]


def _info(msg: str) -> None:
    print(f"[M8.B1][INFO] {msg}")


def _ok(msg: str) -> None:
    print(f"[M8.B1][OK]  {msg}")


def _warn(msg: str, warns: List[str]) -> None:
    w = f"[M8.B1][WARN] {msg}"
    warns.append(w)
    print(w)


def _fail(msg: str) -> None:
    raise RuntimeError(f"[M8.B1][FAIL] {msg}")


def _read_parquet_safe(p: Path, what: str, strict: bool) -> pd.DataFrame:
    if not p.exists():
        if strict:
            _fail(f"Missing {what}: {p}")
        _warn(f"Missing {what}: {p}", warns=[])
        return pd.DataFrame()
    df = pd.read_parquet(p)
    if df.empty and strict:
        _fail(f"Empty {what}: {p}")
    return df


def _resolve_fx_path(outputs: Path) -> Optional[Path]:
    # Preference order
    for cand in [outputs / "m8b_fx_curve.parquet",
                 outputs / "m0_inputs" / "FX_Path.parquet",
                 outputs / "FX_Path.parquet"]:
        if cand.exists():
            return cand
    return None


def _resolve_fx_column(fx: pd.DataFrame) -> Optional[str]:
    if "Month_Index" not in fx.columns:
        return None
    for c in FX_COL_SYNONYMS:
        if c in fx.columns:
            return c
    # Otherwise pick first numeric that is not Month_Index
    for c in fx.columns:
        if c != "Month_Index" and pd.api.types.is_numeric_dtype(fx[c]):
            return c
    return None


def _merge_fx_and_add_usd(df: pd.DataFrame,
                          fx: Optional[pd.DataFrame],
                          fx_col: Optional[str],
                          warns: List[str]) -> pd.DataFrame:
    if "Month_Index" not in df.columns:
        _warn("Cannot add USD twins; missing Month_Index in base frame.", warns)
        return df
    if fx is None or fx_col is None:
        _warn("FX series not available; emitting NAD only.", warns)
        return df

    x = df.merge(fx[["Month_Index", fx_col]], on="Month_Index",
                 how="left", validate="m:1")
    # USD = NAD / FX
    for c in list(x.columns):
        if c.endswith("_NAD_000") and pd.api.types.is_numeric_dtype(x[c]):
            usd_col = c.replace("_NAD_000", "_USD_000")
            x[usd_col] = x[c] / x[fx_col]
    # don't leak the FX column in the wide dataset
    x = x.rename(columns={fx_col: "FX_NAD_per_USD"})
    return x


def _safe_merge(base: pd.DataFrame, add: pd.DataFrame, name: str,
                warns: List[str]) -> pd.DataFrame:
    if add is None or add.empty:
        _warn(f"{name} missing or empty; merge skipped.", warns)
        return base
    if "Month_Index" not in add.columns:
        _warn(f"{name} has no Month_Index; merge skipped.", warns)
        return base
    return base.merge(add, on="Month_Index", how="left", validate="1:1")


def _emit_smoke(out: Path, df: pd.DataFrame, fx_used: Optional[str], warns: List[str]) -> None:
    lines = []
    lines.append("== M8.B1 SMOKE ==\n")
    lines.append(f"Rows: {len(df)} ; Cols: {len(df.columns)}\n")
    cols = ["Month_Index",
            "CFO_NAD_000", "CFI_NAD_000", "CFF_NAD_000", "Closing_Cash_NAD_000",
            "EBITDA_NAD_000", "Total_OPEX_NAD_000",
            "Cash_and_Cash_Equivalents_NAD_000",
            "Current_Assets_NAD_000", "Current_Liabilities_NAD_000",
            "Total_Assets_NAD_000", "Liabilities_and_Equity_Total_NAD_000"]
    have = [c for c in cols if c in df.columns]
    lines.append("Found columns (subset): " + ", ".join(have) + "\n")
    if fx_used:
        lines.append(f"FX source: {fx_used}\n")
    if warns:
        lines.append("\nWARNINGS:\n" + "\n".join(warns) + "\n")

    (out / "m8b1_smoke.md").write_text("".join(lines), encoding="utf-8")


def run_m8B1(outputs_dir: str, currency: str, strict: bool = False, diagnostic: bool = False) -> None:
    out = Path(outputs_dir)
    out.mkdir(parents=True, exist_ok=True)

    _info(f"Starting M8.B1 base timeseries in: {out}")

    warns: List[str] = []

    # Load core M7.5B artifacts
    pl = _read_parquet_safe(out / "m7_5b_profit_and_loss.parquet", "M7.5B P&L", strict)
    bs = _read_parquet_safe(out / "m7_5b_balance_sheet.parquet", "M7.5B Balance Sheet", strict)
    cf = _read_parquet_safe(out / "m7_5b_cash_flow.parquet", "M7.5B Cash Flow", strict)

    if any(df.empty for df in [pl, bs, cf]) and strict:
        _fail("One or more M7.5B artifacts missing/empty under strict mode.")

    # Ensure Month_Index
    for name, df in [("P&L", pl), ("BS", bs), ("CF", cf)]:
        if not df.empty and "Month_Index" not in df.columns:
            _fail(f"{name} lacks Month_Index.")

    base = pd.DataFrame()
    if "Month_Index" in cf.columns:
        base = cf[["Month_Index"]].drop_duplicates().copy()
    elif "Month_Index" in pl.columns:
        base = pl[["Month_Index"]].drop_duplicates().copy()
    else:
        base = bs[["Month_Index"]].drop_duplicates().copy()

    # Merge all series 1:1 on Month_Index
    base = _safe_merge(base, cf, "CF", warns)
    base = _safe_merge(base, pl, "PL", warns)
    base = _safe_merge(base, bs, "BS", warns)

    # Add derived calendar helpers if there is no calendar table
    if "Calendar_Year" not in base.columns:
        if "Month_Index" in base.columns:
            base["Calendar_Year"] = ((base["Month_Index"] - 1) // 12) + 1
            base["Calendar_Quarter"] = ((base["Month_Index"] - 1) % 12) // 3 + 1
            _ok("Calendar helpers (Calendar_Year, Calendar_Quarter) synthesized from Month_Index.")
        else:
            _warn("Month_Index missing; cannot synthesize calendar helpers.", warns)

    # FX resolution and USD twins
    fx_path = _resolve_fx_path(out)
    fx_used = None
    fx_df, fx_col = None, None
    if fx_path is not None:
        fx_used = str(fx_path)
        fx_df = pd.read_parquet(fx_path)
        fx_col = _resolve_fx_column(fx_df)
        if fx_col is None:
            _warn("FX column not detected; USD twins omitted.", warns)
    else:
        _warn("No FX file found (m8b_fx_curve or FX_Path). USD twins omitted.", warns)

    base = _merge_fx_and_add_usd(base, fx_df, fx_col, warns)

    # Emit outputs
    out_file = out / "m8b_base_timeseries.parquet"
    base.to_parquet(out_file, index=False)
    _ok(f"Emitted: {out_file.name}")

    debug = {
        "rows": int(len(base)),
        "cols": int(len(base.columns)),
        "fx_source": fx_used,
        "fx_column": fx_col,
        "nad_cols": [c for c in base.columns if c.endswith("_NAD_000")],
        "usd_cols": [c for c in base.columns if c.endswith("_USD_000")],
        "warnings": warns,
    }
    (out / "m8b1_debug.json").write_text(json.dumps(debug, indent=2), encoding="utf-8")
    _ok("Debug → m8b1_debug.json")

    _emit_smoke(out, base, fx_used, warns)
    _ok("Smoke → m8b1_smoke.md")


if __name__ == "__main__":
    # CLI convenience
    # Example:
    #   python -c "from terra_nova.modules.m8B_1_base_timeseries.runner import run_m8B1; run_m8B1(r'.\outputs','NAD', strict=True)"
    run_m8B1(sys.argv[1] if len(sys.argv) > 1 else r".\outputs", "NAD")
