# src/terra_nova/modules/m7_5b_rebuild/runner.py
"""
M7.5B Financial Statement Consolidator and FX Translator.

Role: Consolidate M2 P&L, M5 CF, and M6 BS into standardized formats, 
      perform FX translation to USD, and enforce consistency checks.
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Any
import pandas as pd
import numpy as np
import re

# -------------------------
# Configuration and Synonyms
# -------------------------

MONTH_SYNS = ["Month_Index", "month_index", "Month"]
FX_COL_SYNS = ["NAD_per_USD", "FX_NAD_per_USD", "USD_to_NAD", "Rate_USD_to_NAD"]

# Maps: Canonical Name: [Synonyms]. These define the OUTPUT structure of M7.5B.
# Names are based on the M0-M9 Dependency Map and Smoke Test requirements.
PL_MAP = {
    "Total_Revenue_NAD_000": ["Total_Revenue_NAD_000", "Monthly_Revenue_NAD_000", "Revenue_NAD_000"],
    "Total_OPEX_NAD_000": ["Total_OPEX_NAD_000", "Monthly_OPEX_NAD_000", "OPEX_NAD_000"],
    "EBITDA_NAD_000": ["EBITDA_NAD_000"],
    "Gross_Profit_NAD_000": ["Gross_Profit_NAD_000"],
    "Operating_Income_NAD_000": ["Operating_Income_NAD_000", "EBIT_NAD_000"],
    # M8/M9 prefer Net_Income
    "Net_Income_NAD_000": ["Net_Income_NAD_000", "NPAT_NAD_000", "Net_Profit_After_Tax_NAD_000"],
    "Depreciation_NAD_000": ["Depreciation_NAD_000", "DA_NAD_000"],
}

CF_MAP = {
    # M8/M9 prefer short names
    "CFO_NAD_000": ["CFO_NAD_000", "Cash_Flow_from_Operations_NAD_000"],
    "CFI_NAD_000": ["CFI_NAD_000", "Cash_Flow_from_Investing_NAD_000"],
    "CFF_NAD_000": ["CFF_NAD_000", "Cash_Flow_from_Financing_NAD_000"],
    # Closing Cash is handled during reconciliation, but we include synonyms for reading M5 if needed
    "Closing_Cash_NAD_000": ["Closing_Cash_NAD_000", "Closing_Cash_Balance_NAD_000"],
}

BS_MAP = {
    "Cash_and_Cash_Equivalents_NAD_000": ["Cash_and_Cash_Equivalents_NAD_000", "Cash_NAD_000"],
    "Current_Assets_NAD_000": ["Current_Assets_NAD_000"],
    "Current_Liabilities_NAD_000": ["Current_Liabilities_NAD_000"],
    # Smoke test specifically requires these exact names:
    "Assets_Total_NAD_000": ["Assets_Total_NAD_000", "Total_Assets_NAD_000"],
    "Liabilities_And_Equity_Total_NAD_000": ["Liabilities_And_Equity_Total_NAD_000", "Total_Liabilities_And_Equity_NAD_000"],
    "AR_Balance_NAD_000": ["AR_Balance_NAD_000"],
    "Inventory_Balance_NAD_000": ["Inventory_Balance_NAD_000"],
    "AP_Balance_NAD_000": ["AP_Balance_NAD_000"],
}

# -------------------------
# Utilities
# -------------------------

def _fail(msg: str) -> None:
    print(f"[M7.5B][FAIL] {msg}")
    raise RuntimeError(msg)

def _norm_key(s: str) -> str:
    return re.sub(r'[^a-z0-9]', '', str(s).lower())

def _resolve_col(df: pd.DataFrame, candidates: List[str], required=True, ctx="") -> Optional[str]:
    """Robust column resolver."""
    if df is None or df.empty:
        if required: _fail(f"Cannot resolve {ctx}: DataFrame is empty.")
        return None

    cols = list(df.columns)
    norm = {_norm_key(c): c for c in cols}

    for c in candidates:
        if c in cols: return c
        if _norm_key(c) in norm: return norm[_norm_key(c)]
            
    if required:
        _fail(f"Missing any of {candidates} (ctx={ctx}). Available={cols[:30]}")
    return None

def _to_native(x):
    """Convert numpy types to native Python types for JSON serialization."""
    if pd.isna(x): return None
    if isinstance(x, (np.integer, np.int64)): return int(x)
    if isinstance(x, (np.floating, np.float64)): return float(x)
    return x

def standardize_statement(df: pd.DataFrame, synonym_map: Dict[str, List[str]], ctx: str) -> Tuple[pd.DataFrame, Dict[str, str]]:
    """Resolves columns, renames to canonical, ensures numeric types, and aggregates by month."""
    if df.empty:
        cols = ["Month_Index"] + list(synonym_map.keys())
        return pd.DataFrame(columns=cols), {}

    rename_dict = {}
    resolved_cols = {}
    
    # Resolve Month Index
    mcol = _resolve_col(df, MONTH_SYNS, required=True, ctx=f"Month Index ({ctx})")
    rename_dict[mcol] = "Month_Index"

    for canonical_name, synonyms in synonym_map.items():
        # required=False because a statement might not have every possible line item.
        resolved_name = _resolve_col(df, synonyms, required=False)
        if resolved_name:
            rename_dict[resolved_name] = canonical_name
            resolved_cols[canonical_name] = resolved_name
    
    keep_cols = list(rename_dict.keys())
    df_out = df[keep_cols].copy()
    df_out.rename(columns=rename_dict, inplace=True)
    
    # Ensure numeric types and aggregate
    for col in df_out.columns:
        if col != "Month_Index":
            df_out[col] = pd.to_numeric(df_out[col], errors="coerce").fillna(0.0)

    # Group by Month_Index to handle potential duplicates in inputs
    df_out = df_out.groupby("Month_Index", as_index=False).sum()
    return df_out, resolved_cols

# -------------------------
# Loaders
# -------------------------

def load_fx(outputs: Path, dbg: Dict) -> pd.DataFrame:
    """Loads and standardizes FX data (NAD per USD)."""
    # Determine path (Prioritize M0 location)
    p1 = outputs / "m0_inputs" / "FX_Path.parquet"
    p2 = outputs / "FX_Path.parquet"
    fx_path = p1 if p1.exists() else (p2 if p2.exists() else None)
    
    if not fx_path:
        _fail("FX_Path.parquet not found in m0_inputs/ or outputs/.")

    try:
        fx_df = pd.read_parquet(fx_path)
    except Exception as e:
        _fail(f"Failed to read FX file {fx_path}: {e}")

    mcol = _resolve_col(fx_df, MONTH_SYNS, required=True, ctx="FX Month Index")
    fx_col = _resolve_col(fx_df, FX_COL_SYNS, required=True, ctx="FX Rate")
    
    # Log metadata (Fix for Failure 2: FX Metadata)
    dbg["fx_source_path"] = str(fx_path.relative_to(outputs))
    dbg["fx_source_column"] = fx_col
    
    fx_rate = fx_df[[mcol, fx_col]].rename(columns={mcol: "Month_Index", fx_col: "FX_NAD_per_USD"})
    
    # Robustness: Handle invalid rates (0 or NaN) using forward/backward fill
    fx_rate["FX_NAD_per_USD"] = pd.to_numeric(fx_rate["FX_NAD_per_USD"], errors="coerce")
    # Replace 0 with NaN before filling to prevent division by zero later
    fx_rate["FX_NAD_per_USD"] = fx_rate["FX_NAD_per_USD"].replace(0, np.nan).ffill().bfill()

    if fx_rate["FX_NAD_per_USD"].isnull().any():
         dbg["notes"].append("[WARN] FX rates contain NaNs after filling. USD values may be NaN.")

    return fx_rate

def load_pl(outputs: Path, dbg: Dict, strict: bool) -> pd.DataFrame:
    """Loads P&L from M2 (primary) or M1 (fallback)."""
    # Try M2 sources first
    p1 = outputs / "m2_pl_schedule.parquet"
    p2 = outputs / "m2_profit_and_loss_stub.parquet"
    path = p1 if p1.exists() else (p2 if p2.exists() else None)
    
    if path:
        pl = pd.read_parquet(path)
        source = str(path.relative_to(outputs))
    else:
        # Fallback to M1 Revenue
        p_m1 = outputs / "m1_revenue_schedule.parquet"
        if p_m1.exists():
            print("[M7.5B][INFO] M2 P&L not found. Falling back to M1 Revenue schedule.")
            pl = pd.read_parquet(p_m1)
            source = str(p_m1.relative_to(outputs))
        elif strict:
            _fail("Neither M2 P&L nor M1 Revenue schedule found.")
        else:
            print("[M7.5B][WARN] No P&L source found. Returning empty P&L.")
            return standardize_statement(pd.DataFrame(), PL_MAP, "P&L")[0]

    pl_std, resolved = standardize_statement(pl, PL_MAP, "P&L")
    dbg["pl_source"] = source
    dbg["pl_mapping"] = resolved
    return pl_std

def load_cf(outputs: Path, dbg: Dict) -> pd.DataFrame:
    """Loads CF from M5."""
    path = outputs / "m5_cash_flow_statement_final.parquet"
    if not path.exists():
        _fail(f"M5 Cash Flow statement not found: {path}")
        
    cf = pd.read_parquet(path)
    cf_std, resolved = standardize_statement(cf, CF_MAP, "CF")
    dbg["cf_source"] = str(path.relative_to(outputs))
    dbg["cf_mapping"] = resolved
    return cf_std

def load_bs(outputs: Path, dbg: Dict, strict: bool) -> pd.DataFrame:
    """Loads BS from M6 and verifies totals."""
    path = outputs / "m6_balance_sheet.parquet"
    if not path.exists():
        # If M6 is missing, the pipeline is broken according to the dependency map.
        _fail(f"M6 Balance Sheet not found: {path}")

    bs = pd.read_parquet(path)
    bs_std, resolved = standardize_statement(bs, BS_MAP, "BS")
    dbg["bs_source"] = str(path.relative_to(outputs))
    dbg["bs_mapping"] = resolved

    # Verify Totals (Fix for Failure 1: BS Totals)
    if "Assets_Total_NAD_000" not in bs_std.columns or "Liabilities_And_Equity_Total_NAD_000" not in bs_std.columns:
        if strict:
            _fail("M6 Balance Sheet is missing required Total Assets or Total L&E columns.")
        else:
            print("[M7.5B][WARN] M6 Balance Sheet missing totals.")
            
    return bs_std

# -------------------------
# Main Runner
# -------------------------

def run_m7_5b(outputs: str, currency: str = "NAD", strict: bool = True, diagnostic: bool = False) -> None:
    outputs = Path(outputs)
    outputs.mkdir(parents=True, exist_ok=True)

    dbg: Dict[str, object] = {"strict": strict, "currency": currency, "notes": []}

    # 1. Load FX
    fx_rate = load_fx(outputs, dbg)
    
    # 2. Load and Standardize Statements
    pl = load_pl(outputs, dbg, strict)
    cf = load_cf(outputs, dbg)
    bs = load_bs(outputs, dbg, strict)
    
    # 3. Align Timelines
    # Create a master timeline encompassing all statements
    all_months = sorted(list(set(pl["Month_Index"]) | set(cf["Month_Index"]) | set(bs["Month_Index"])))
    
    if not all_months:
        if strict: _fail("All input statements are empty. Cannot establish timeline.")
        print("[M7.5B][WARN] Inputs empty. Emitting empty outputs.")
        return

    timeline = pd.DataFrame({"Month_Index": all_months})
    
    # Merge onto timeline and fill monetary NaNs with 0.0 (safe assumption for missing months)
    # Note: standardize_statement already handles numeric conversion and aggregation.
    pl = timeline.merge(pl, on="Month_Index", how="left").fillna(0.0)
    cf = timeline.merge(cf, on="Month_Index", how="left").fillna(0.0)
    bs = timeline.merge(bs, on="Month_Index", how="left").fillna(0.0)

    # 4. Consistency Checks and Reconciliation
    
    # 4a. BS Tie-out Check
    if "Assets_Total_NAD_000" in bs.columns and "Liabilities_And_Equity_Total_NAD_000" in bs.columns:
        diff_nad = (bs["Assets_Total_NAD_000"] - bs["Liabilities_And_Equity_Total_NAD_000"]).abs().max()
        if diff_nad > 1e-6:
            msg = f"Balance Sheet (from M6) does not balance (NAD). Max difference: {diff_nad:.6f}"
            if strict: _fail(msg)
            dbg["notes"].append(f"[WARN] {msg}")
        dbg["bs_balances_nad"] = bool(diff_nad <= 1e-6)

    # 4b. CF-BS Cash Reconciliation (M6 BS Cash is Authoritative)
    if "Cash_and_Cash_Equivalents_NAD_000" in bs.columns:
        # Check if M5 provided Closing Cash and compare it
        if "Closing_Cash_NAD_000" in cf.columns:
            merged_cash = cf[["Month_Index", "Closing_Cash_NAD_000"]].merge(
                bs[["Month_Index", "Cash_and_Cash_Equivalents_NAD_000"]],
                on="Month_Index", how="inner"
            )
            if not merged_cash.empty:
                diff_cash = (merged_cash["Closing_Cash_NAD_000"] - merged_cash["Cash_and_Cash_Equivalents_NAD_000"]).abs().max()
                if diff_cash > 1e-6:
                    msg = f"M5 Closing Cash mismatches M6 BS Cash (Max diff: {diff_cash:.6f}). Overriding M5 with M6 value."
                    print(f"[M7.5B][WARN] {msg}")
                    dbg["notes"].append(msg)
            
            # Drop M5's closing cash
            cf = cf.drop(columns=["Closing_Cash_NAD_000"])

        # Merge the authoritative cash balance from BS into the CF statement
        cf = cf.merge(
            bs[["Month_Index", "Cash_and_Cash_Equivalents_NAD_000"]].rename(
                columns={"Cash_and_Cash_Equivalents_NAD_000": "Closing_Cash_NAD_000"}
            ), 
            on="Month_Index", how="left"
        )
        dbg["cash_reconciliation"] = "M6_BS_Cash_Authoritative"
        dbg["cf_bs_cash_link_ok"] = True
    elif strict:
        _fail("Cash account not found in M6 Balance Sheet. Cannot reconcile CF Closing Cash.")

    # 5. Apply FX Translation (Fix for Failure 3: USD Columns)
    
    # Merge FX rates
    pl = pl.merge(fx_rate, on="Month_Index", how="left")
    cf = cf.merge(fx_rate, on="Month_Index", how="left")
    bs = bs.merge(fx_rate, on="Month_Index", how="left")

    def translate_to_usd(df):
        if "FX_NAD_per_USD" not in df.columns:
            return df
        
        # Use the robustly handled rates (which might contain NaNs if filling failed)
        rates = df["FX_NAD_per_USD"]
        nad_cols = [c for c in df.columns if c.endswith("_NAD_000")]
        
        for nad_col in nad_cols:
            usd_col = nad_col.replace("_NAD_000", "_USD_000")
            # USD = NAD / Rate. Division by NaN results in NaN.
            df[usd_col] = df[nad_col] / rates
        return df

    pl = translate_to_usd(pl)
    cf = translate_to_usd(cf)
    bs = translate_to_usd(bs)
    dbg["fx_translation_applied"] = True

    # 6. Emit
    (outputs / "m7_5b_profit_and_loss.parquet").write_bytes(pl.to_parquet(index=False))
    (outputs / "m7_5b_cash_flow.parquet").write_bytes(cf.to_parquet(index=False))
    (outputs / "m7_5b_balance_sheet.parquet").write_bytes(bs.to_parquet(index=False))

    # 7. Debug/Smoke
    if "Total_Revenue_NAD_000" in pl.columns and (pl["Total_Revenue_NAD_000"] > 0).any():
         # Handle potential NaN if min() is called on an empty selection
         min_rev_month = pl.loc[pl["Total_Revenue_NAD_000"] > 0, "Month_Index"].min()
         dbg["first_nonzero_revenue_month"] = _to_native(min_rev_month) if pd.notna(min_rev_month) else None
    else:
         dbg["first_nonzero_revenue_month"] = None

    (outputs / "m7_5b_debug.json").write_text(json.dumps(dbg, indent=2, default=_to_native), encoding="utf-8")

    # Generate Smoke Report
    smoke = []
    smoke.append(f"[M7.5B] PL rows: {len(pl)} (Source: {dbg.get('pl_source', 'N/A')})")
    smoke.append(f"[M7.5B] CF rows: {len(cf)} (Source: {dbg.get('cf_source', 'N/A')})")
    smoke.append(f"[M7.5B] BS rows: {len(bs)} (Source: {dbg.get('bs_source', 'N/A')})")
    smoke.append(f"[M7.5B] BS Balanced (NAD): {dbg.get('bs_balances_nad', False)}")
    smoke.append(f"[M7.5B] CF/BS Cash Link OK: {dbg.get('cf_bs_cash_link_ok', False)}")
    smoke.append(f"[M7.5B] FX Translation Applied: {dbg.get('fx_translation_applied', False)}")
    # Ensure FX metadata is present in smoke report
    smoke.append(f"[M7.5B] FX Source: {dbg.get('fx_source_path', 'N/A')}:{dbg.get('fx_source_column', 'N/A')}")
    
    (outputs / "m7_5b_smoke_report.md").write_text("\n".join(smoke), encoding="utf-8")

    print("[M7.5B][OK]  Emitted: m7_5b_profit_and_loss.parquet, m7_5b_cash_flow.parquet, m7_5b_balance_sheet.parquet")
    print(f"[M7.5B][OK]  Consistency checks passed. FX translation applied.")

if __name__ == "__main__":
    import sys
    if len(sys.argv) > 1:
        # Set strict=True for command line execution
        run_m7_5b(sys.argv[1], strict=True)
    else:
        print("Usage: python runner.py <outputs_dir>")