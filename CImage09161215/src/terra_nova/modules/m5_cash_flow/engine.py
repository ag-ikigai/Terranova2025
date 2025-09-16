# src/terra_nova/modules/m5_cash_flow/engine.py
"""
M5 Cash Flow Statement Assembler (robust and compliant edition)

Design goals
------------
- Produce a monthly cash flow statement including detailed CFO components and the three IFRS buckets.
- Comply with the "stabilized formula": CFO = NPAT + DA + WC_CF - Tax_Paid - Interest_Paid.
- Be robust to column naming drifts upstream (M0..M4).
- Source Interest Paid strictly from M3 to satisfy validation cross-check.

Outputs
-------
- (df) columns matching validator requirements (e.g., Net_Profit_After_Tax_NAD_000, 
  Cash_Flow_from_Operations_NAD_000, CFI_NAD_000, CFF_NAD_000)
- meta dict with verbose resolution details
- smoke dict (small checks)

All amounts in NAD '000.
"""
from __future__ import annotations
from dataclasses import dataclass
from typing import Dict, List, Tuple, Any, Optional
import pandas as pd
import numpy as np
from pathlib import Path
import re

# ---------- utilities ----------

def _print(msg: str) -> None:
    print(msg, flush=True)

def _fail(msg: str) -> None:
    raise RuntimeError(f"[M5][FAIL] {msg}")

def _warn(msg: str) -> None:
    _print(f"[M5][WARN] {msg}")

def _ok(msg: str) -> None:
    _print(f"[M5][OK]  {msg}")

def _info(msg: str) -> None:
    _print(f"[M5][INFO] {msg}")

def _read_parquet(path: Path) -> pd.DataFrame:
    # We rely on the caller (loaders) to handle FileNotFoundError based on 'strict' mode if the file is optional.
    try:
        return pd.read_parquet(path)
    except FileNotFoundError:
        raise # Re-raise FileNotFoundError
    except Exception as e:
        try:
            # engine auto fallback
            return pd.read_parquet(path, engine="pyarrow")
        except Exception as e2:
            _fail(f"Failed to read parquet file {path}. Errors: {e}, {e2}")

# Robust resolver (Enhanced from previous patch)
def _norm_key(s: str) -> str:
    """lowercase + strip non-alphanum to tolerate minor header drift."""
    return re.sub(r'[^a-z0-9]', '', str(s).lower())

def _resolve_col(df, candidates, *, required=True, ctx=""):
    """Robust column resolver: case-insensitive, underscore/spacing/prefix tolerant."""
    if df is None or df.empty:
        if required:
            _fail(f"Cannot resolve {ctx} because input DataFrame is empty or None.")
        return None

    cols = list(df.columns)
    # Exact match
    for c in candidates:
        if c in df.columns:
            return c
    # Case-insensitive exact
    lower = {str(c).lower(): c for c in cols}
    for c in candidates:
        if str(c).lower() in lower:
            return lower[str(c).lower()]
    # Normalized
    norm = {_norm_key(c): c for c in cols}
    for c in candidates:
        key = _norm_key(c)
        if key in norm:
            return norm[key]
            
    # Prefix drift tolerance (common prefixes seen in the pipeline)
    str_candidates = [c for c in candidates if isinstance(c, str)]
    prefix_regex = r'^(revolver_|debt_|capex_|monthly_|tax_)'
    base_cands = [re.sub(prefix_regex, '', c, flags=re.I) for c in str_candidates]
    base_cands_norm = {_norm_key(x) for x in base_cands}
    
    for c in cols:
        if isinstance(c, str):
            c_base = re.sub(prefix_regex, '', c, flags=re.I)
            if _norm_key(c_base) in base_cands_norm:
                return c
                
    if required:
        _fail(f"Missing any of {candidates} (ctx={ctx}). Available={cols[:35]}")
    return None

# ---------- synonym dictionaries ----------

MONTH_SYNS = ["Month_Index", "MONTH_INDEX", "month_index", "Month", "month"]

# M2 P&L
NPAT_SYNS = ["NPAT_NAD_000", "Net_Profit_After_Tax_NAD_000", "NPAT"]
DA_SYNS   = ["DA_NAD_000", "Depreciation_NAD_000", "Depreciation_and_Amortization_NAD_000", "DA"]

# M2 WC
NWC_CF_SYNS = [
    "NWC_CF_NAD_000",
    "WC_Cash_Flow_NAD_000",
    "Change_in_NWC_CF_NAD_000",
    "Working_Capital_CF_NAD_000",
    "Cash_Flow_from_NWC_Change_NAD_000",
]

# M4 Tax
# NEW: Tax Paid
TAX_PAID_SYNS = ["Tax_Paid_NAD_000", "Taxes_Paid_NAD_000", "Income_Tax_Paid_NAD_000"]

# M1 CAPEX / CFI
CFI_SYNS = [
    "CFI_NAD_000",
    "CAPEX_Outflow_NAD_000",
    "Capex_Cash_Outflow_NAD_000",
    "CAPEX_Cash_NAD_000",
    "Capex_NAD_000",
    "CAPEX_NAD_000",
    "Monthly_CAPEX_NAD_000", # Added based on previous run logs
]

# M3 Revolver schedule (CFF)
REV_MONTH_SYNS = MONTH_SYNS
# (Synonyms updated in previous patch)
REV_DRAW_SYNS = [
    "Revolver_Draw_NAD_000", "Revolver_Draws_NAD_000",
    "Draw_NAD_000", "Draws_NAD_000",
    "Principal_Draw_NAD_000", "Principal_Draws_NAD_000",
    "Debt_Draws_NAD_000", "Drawdown", "Drawdowns"
]

REV_REPAY_SYNS = [
    "Revolver_Repayment_NAD_000", "Revolver_Repayments_NAD_000",
    "Principal_Repayment_NAD_000", "Principal_Repay_NAD_000",
    "Debt_Repayment_NAD_000", "Debt_Repayments_NAD_000",
    "Repayment_NAD_000", "Repay_NAD_000", "Repayments_NAD_000",
    "Repayment", "Repay", "Repayments"
]

REV_INT_SYNS = [
    "Interest_Paid_NAD_000", # Prioritize the canonical name
    "Revolver_Interest_Expense_NAD_000",
    "Interest_NAD_000", "Interest_Expense_NAD_000", "InterestPaid_NAD_000",
    "Interest_Accrued"
]

REV_FEES_SYNS = [
    "Fees_NAD_000","Revolver_Fees_NAD_000","Facility_Fees_NAD_000","Commitment_Fee_NAD_000","Admin_Fee_NAD_000"
]

# Opening cash (M0)
CASH_OPEN_SYNS = [
    "Cash_and_Cash_Equivalents_NAD_000","Cash_NAD_000","Cash_Equivalents_NAD_000",
    "Opening_Cash_NAD_000","Cash_and_Cash_Equivalents","Cash"
]
M0_WIDE_VALUE_SYNS = ["Value_NAD_000","Value_NAD","Value"]

# ---------- loaders ----------

# UPDATED: Use _resolve_col.
def _load_m2_pl(outputs: Path) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    # Search for both common names seen in the pipeline logs
    p1 = outputs / "m2_pl_schedule.parquet"
    p2 = outputs / "m2_profit_and_loss_stub.parquet" 
    
    path = p1 if p1.exists() else (p2 if p2.exists() else None)
    if path is None:
        # This file is fundamental; we fail if missing.
        _fail("M2 P&L schedule not found (looked for m2_pl_schedule.parquet, m2_profit_and_loss_stub.parquet)")

    df = _read_parquet(path)
    
    mcol = _resolve_col(df, MONTH_SYNS, required=True, ctx="Month_Index (M2)")
    npat = _resolve_col(df, NPAT_SYNS, required=True, ctx="NPAT (M2)")
    da   = _resolve_col(df, DA_SYNS,   required=True, ctx="DA (M2)")
    
    keep = [mcol, npat, da]
    base = df[keep].copy()
    
    rename_map = {mcol:"Month_Index", npat:"NPAT_NAD_000", da:"DA_NAD_000"}
        
    base.rename(columns=rename_map, inplace=True)
    _ok(f"M2 P&L columns -> month='Month_Index', NPAT='{npat}', DA='{da}'")
    return base, {"pl_path": str(path), "npat_col": npat, "da_col": da}

# UPDATED: Use _resolve_col
def _load_m2_wc(outputs: Path) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    path = outputs / "m2_working_capital_schedule.parquet"
    try:
        df = _read_parquet(path)
    except FileNotFoundError:
        # This file is fundamental; we fail if missing.
        _fail(f"Required artifact not found: {path}")

    mcol = _resolve_col(df, MONTH_SYNS, required=True, ctx="Month_Index (M2 WC)")
    ncc  = _resolve_col(df, NWC_CF_SYNS, required=True, ctx="NWC_CF (M2 WC)")
    
    wc = df[[mcol, ncc]].copy()
    wc.rename(columns={mcol:"Month_Index", ncc:"NWC_CF_NAD_000"}, inplace=True)
    _ok(f"M2 WC columns -> month='Month_Index', NWC_CF='{ncc}'")
    return wc, {"wc_path": str(path), "nwc_cf_col": ncc}

# NEW: Loader for M4 Tax Paid
def _load_m4_tax(outputs: Path, strict: bool) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    path = outputs / "m4_tax_schedule.parquet"

    try:
        df = _read_parquet(path)
    except FileNotFoundError:
        msg = "M4 tax schedule not found (m4_tax_schedule.parquet)."
        if strict:
            _fail(msg)
        _warn(msg + " Defaulting Tax Paid to 0.0.")
        # Return empty DF, handled during merge in assembler
        return pd.DataFrame(columns=["Month_Index", "Tax_Paid_NAD_000"]), {"source": None, "reason": msg}

    mcol = _resolve_col(df, MONTH_SYNS, required=True, ctx="Month_Index (M4)")
    tcol = _resolve_col(df, TAX_PAID_SYNS, required=True, ctx="Tax_Paid (M4)")
    
    tax = df[[mcol, tcol]].copy()
    tax.rename(columns={mcol:"Month_Index", tcol:"Tax_Paid_NAD_000"}, inplace=True)
    _ok(f"M4 Tax columns -> month='Month_Index', Tax_Paid='{tcol}'")
    return tax, {"tax_path": str(path), "tax_paid_col": tcol}

# UPDATED: Use _resolve_col for CFI.
def _load_m1_cfi(outputs: Path, strict: bool) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    path = outputs / "m1_capex_schedule.parquet"

    try:
        df = _read_parquet(path)
    except FileNotFoundError:
        msg = "M1 CAPEX schedule not found (m1_capex_schedule.parquet)."
        if strict:
            _fail(msg)
        _warn(msg + " Defaulting CFI to 0.0.")
        # Return empty DF, handled during merge
        return pd.DataFrame(columns=["Month_Index", "CFI_NAD_000"]), {"source": None, "reason": msg}
    
    mcol = _resolve_col(df, MONTH_SYNS, required=True, ctx="Month_Index (M1)")
    # Use the robust resolver to find the CFI/CAPEX column.
    cfi_col = _resolve_col(df, CFI_SYNS, required=False, ctx="CFI/CAPEX (M1)")
    
    if cfi_col is None:
        if strict:
            _fail("Could not resolve CAPEX/CFI cash column in m1_capex_schedule.parquet.")
        else:
            _warn("CFI column not resolved; defaulting to zeros.")
            # Ensure Month_Index is correctly sourced from the file if present
            return pd.DataFrame({"Month_Index": df[mcol], "CFI_NAD_000": 0.0}), {"cfi_col": None, "sign_flipped": False, "path": str(path)}

    cfi = df[[mcol, cfi_col]].copy()
    cfi.rename(columns={mcol:"Month_Index", cfi_col:"CFI_NAD_000"}, inplace=True)
    
    # Ensure outflow negative (sign flip logic remains the same)
    # Ensure numeric before median calculation
    cfi_series = pd.to_numeric(cfi["CFI_NAD_000"], errors='coerce').fillna(0.0)
    med = float(np.nanmedian(cfi_series.values))
    flipped = False
    if med > 0:
        cfi["CFI_NAD_000"] = -cfi_series # Apply negation to the numeric series
        flipped = True
        _warn(f"Detected mostly positive CAPEX cash ('{cfi_col}') – flipping sign to outflow negative.")
    else:
        cfi["CFI_NAD_000"] = cfi_series # Ensure the column is numeric
        
    _ok(f"CFI sourced from m1_capex_schedule.parquet -> '{cfi_col}'")
    return cfi, {"cfi_col": cfi_col, "sign_flipped": flipped, "path": str(path)}

# UPDATED: Return CFF DataFrame, Interest DataFrame, and Meta Dict.
def _load_m3_revolver(outputs: Path, strict: bool) -> Tuple[pd.DataFrame, pd.DataFrame, Dict[str, Any]]:
    # File loading with fallback
    p1 = outputs / "m3_revolver_schedule.parquet"
    p2 = outputs / "m3_financing_schedule.parquet"

    path = p1 if p1.exists() else (p2 if p2.exists() else None)

    if path is None:
        msg = "M3 revolver schedule not found (looked for m3_revolver_schedule.parquet, m3_financing_schedule.parquet)"
        if strict:
            _fail(msg)
        _warn(msg + ". Defaulting CFF and Interest Paid to 0.0.")
        # Return empty DataFrames for CFF and Interest
        return pd.DataFrame(columns=["Month_Index", "CFF_NAD_000"]), \
               pd.DataFrame(columns=["Month_Index", "Interest_Paid_NAD_000"]), \
               {"source": None, "reason": msg}

    df = _read_parquet(path)
    src = str(path)

    # Resolve columns
    try:
        mcol = _resolve_col(df, REV_MONTH_SYNS, required=True, ctx="Month_Index (M3)")
        dcol = _resolve_col(df, REV_DRAW_SYNS,  required=True, ctx="revolver_draw (M3)")
        rcol = _resolve_col(df, REV_REPAY_SYNS, required=True, ctx="revolver_repay (M3)")
    except RuntimeError:
        preview = list(df.columns)[:35]
        _fail(f"Cannot resolve revolver draw/repay columns. Tried draws={REV_DRAW_SYNS}, repay={REV_REPAY_SYNS}. Available (first 35): {preview}")

    # Optional columns
    fcol = _resolve_col(df, REV_FEES_SYNS, required=False, ctx="revolver_fees (M3)")
    icol = _resolve_col(df, REV_INT_SYNS,   required=False, ctx="revolver_interest (M3)")

    _ok(f"M3 revolver mapping -> draw='{dcol}', repay='{rcol}', "
        f"fees='{fcol or 'N/A'}', interest='{icol or 'N/A'}' (source={src})")

    # Calculate CFF (logic retained from original)
    # We operate on copies to avoid modifying the source dataframe view.
    cff = df[[mcol]].copy()
    cff.rename(columns={mcol:"Month_Index"}, inplace=True)
    cff["CFF_NAD_000"] = 0.0
    
    # Draws (+), repayments (−). Use join based on index (which aligns with df here).
    cff = cff.join(df[[dcol]].rename(columns={dcol:"_draw"}), rsuffix='_m3d')
    cff = cff.join(df[[rcol]].rename(columns={rcol:"_repay"}), rsuffix='_m3r')
    
    cff["_draw"]  = pd.to_numeric(cff["_draw"], errors="coerce").fillna(0.0)
    cff["_repay"] = pd.to_numeric(cff["_repay"], errors="coerce").fillna(0.0)
    # Assuming repayments are positive values that reduce cash flow
    cff["CFF_NAD_000"] = cff["_draw"] - cff["_repay"]
    
    # Fees (−)
    fee_used = False
    if fcol is not None:
        cff = cff.join(df[[fcol]].rename(columns={fcol:"_fees"}), rsuffix='_m3f')
        cff["_fees"] = pd.to_numeric(cff["_fees"], errors="coerce").fillna(0.0)
        # Assuming fees are positive values that reduce cash flow
        cff["CFF_NAD_000"] = cff["CFF_NAD_000"] - cff["_fees"]
        fee_used = True

    _ok("CFF derived from M3 revolver schedule (draws − repayments − fees).")
    cff = cff[["Month_Index","CFF_NAD_000"]]

    # Extract Interest Paid DataFrame
    # Initialize with zeros aligned to the schedule's months
    interest_df = pd.DataFrame({"Month_Index": df[mcol], "Interest_Paid_NAD_000": 0.0})
    
    if icol is not None:
        # If interest column exists, extract it
        interest_data = df[[mcol, icol]].copy()
        interest_data.rename(columns={mcol:"Month_Index", icol:"_interest_val"}, inplace=True)
        interest_data["_interest_val"] = pd.to_numeric(interest_data["_interest_val"], errors="coerce").fillna(0.0)
        
        # Merge back to overwrite the zeros with actual data where available
        # We use an outer merge on Month_Index and combine the columns
        interest_df = interest_df.merge(interest_data, on="Month_Index", how="outer")
        # If _interest_val exists (not NaN), use it, otherwise use the existing Interest_Paid_NAD_000 (which was initialized to 0.0)
        interest_df["Interest_Paid_NAD_000"] = interest_df["_interest_val"].fillna(interest_df["Interest_Paid_NAD_000"])
        interest_df = interest_df[["Month_Index", "Interest_Paid_NAD_000"]]

    meta = {"rev_path": src, "source": src, "month_col": mcol, "draw_col": dcol, "repay_col": rcol, "fee_col": fcol, "interest_col": icol, "fees_included_in_cff": fee_used}
    
    return cff, interest_df, meta

def _load_opening_cash(outputs: Path, base_months: pd.Series, strict: bool) -> Tuple[float, Dict[str, Any]]:
    """
    Read opening cash from M0 if available. (Logic updated to use _resolve_col).
    """
    # Search candidates
    candidates = [outputs / "m0_opening_bs.parquet", outputs / "m0_inputs" / "m0_opening_bs.parquet"]
    path = next((p for p in candidates if p.exists()), None)

    if path is None:
        if strict:
            _fail("m0_opening_bs.parquet not found in outputs/.")
        else:
            # _warn("m0_opening_bs.parquet not found; defaulting opening cash to 0.0 per policy.")
            return 0.0, {"source": None, "policy_default_zero": True}

    try:
        df = _read_parquet(path)
    except Exception:
         _warn(f"Could not read {path.name}; defaulting opening cash to 0.0.")
         return 0.0, {"source": str(path), "policy_default_zero": True, "error": "read_failed"}

    # Case A: wide form with direct cash column
    cash_col = _resolve_col(df, CASH_OPEN_SYNS, required=False, ctx="Opening Cash (M0 Wide)")

    if cash_col:
        mcol = _resolve_col(df, MONTH_SYNS, required=False, ctx="Month Index (M0)")
        if mcol:
            # align to min month
            m0 = int(min(base_months.min(), df[mcol].min()))
            row = df[df[mcol] == m0]
            if not row.empty:
                val = float(pd.to_numeric(row[cash_col], errors="coerce").fillna(0.0).iloc[0])
            else:
                # pick first value if specific month not found
                val = float(pd.to_numeric(df[cash_col], errors="coerce").fillna(0.0).iloc[0])
        else:
            # take first row if no month index
            val = float(pd.to_numeric(df[cash_col], errors="coerce").fillna(0.0).iloc[0])
        _ok(f"Opening cash source: M0:{cash_col}@{str(path.name)} -> {val:,.2f} (NAD '000)")
        return val, {"source": f"M0:{cash_col}", "path": str(path), "policy_default_zero": False}

    # Case B: long form with line items
    line_col = _resolve_col(df, ["Line_Item","Item","Line","Account","LineItem"], required=False)
    val_col = _resolve_col(df, M0_WIDE_VALUE_SYNS + ["Amount_NAD_000","Amount_NAD","Amount"], required=False)

    if line_col and val_col:
        # normalize names using _norm_key
        targets = [_norm_key(c) for c in CASH_OPEN_SYNS]
        df["_key"] = df[line_col].astype(str).map(_norm_key)
        row = df[df["_key"].isin(targets)]
        if not row.empty:
            val = float(pd.to_numeric(row[val_col], errors="coerce").fillna(0.0).iloc[0])
            _ok(f"Opening cash source: M0:{line_col}='{row[line_col].iloc[0]}' -> {val:,.2f} (NAD '000)")
            return val, {"source": f"M0:{line_col} match", "path": str(path), "policy_default_zero": False}

    # Policy default
    # _ok("Opening cash resolved defaulted to 0.0 (no explicit M0 cash; pre‑freeze policy).")
    return 0.0, {"source": None, "path": str(path), "policy_default_zero": True}


# ---------- assembler ----------

# OVERHAULED: Implement stabilized CFO formula and expose components.
def build_cash_flow_statement(outputs_dir: str, currency: str, input_pack_path: Optional[str], strict: bool=False) -> Tuple[pd.DataFrame, Dict[str, Any], Dict[str, Any]]:
    """
    Public entrypoint used by runner.py.
    """
    outputs = Path(outputs_dir)
    
    # 1. Load all components
    # M2 P&L (NPAT, DA)
    base, pl_m = _load_m2_pl(outputs)
    # M2 WC (NWC_CF)
    wc_df, wc_m = _load_m2_wc(outputs)
    # M1 CFI (CAPEX)
    cfi_df, cfi_m = _load_m1_cfi(outputs, strict=strict)
    # M3 CFF and Interest Paid
    cff_df, interest_df, cff_m = _load_m3_revolver(outputs, strict=strict)
    # M4 Tax Paid
    tax_df, tax_m = _load_m4_tax(outputs, strict=strict)

    # 2. Assemble the base dataframe using Month_Index from M2 P&L as the spine
    if base.empty:
        _fail("M2 P&L schedule is empty. Cannot establish master timeline.")

    months = base["Month_Index"].copy()
    months = months.sort_values().unique() # Ensure unique months
    df = pd.DataFrame({"Month_Index": months})

    # Helper to ensure inputs are grouped by month before merging (prevents duplicate rows)
    def prep_merge(component_df):
        if 'Month_Index' not in component_df.columns or component_df.empty:
            return component_df
        # Ensure numeric columns only before sum
        numeric_cols = component_df.select_dtypes(include=np.number).columns.tolist()
        # Keep Month_Index for grouping, but don't sum it if it's numeric
        if 'Month_Index' in numeric_cols:
             numeric_cols.remove('Month_Index')
        
        if not numeric_cols:
            return component_df[['Month_Index']]

        return component_df.groupby('Month_Index', as_index=False)[numeric_cols].sum()

    # Merge all components onto the spine (using 'left' merge based on the P&L timeline)
    df = df.merge(prep_merge(base), on="Month_Index", how="left")
    df = df.merge(prep_merge(wc_df), on="Month_Index", how="left")
    df = df.merge(prep_merge(tax_df), on="Month_Index", how="left")
    df = df.merge(prep_merge(interest_df), on="Month_Index", how="left")
    df = df.merge(prep_merge(cfi_df), on="Month_Index", how="left")
    df = df.merge(prep_merge(cff_df), on="Month_Index", how="left")

    # 3. Ensure numeric types and fill NaNs (if any component missed months or was empty)
    component_cols = [
        "NPAT_NAD_000", "DA_NAD_000", "NWC_CF_NAD_000", 
        "Tax_Paid_NAD_000", "Interest_Paid_NAD_000",
        "CFI_NAD_000", "CFF_NAD_000"
    ]
    for col in component_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0.0)
        elif strict:
             # This should not happen if loaders correctly return DFs with columns even when data is missing
            _fail(f"Missing expected column '{col}' after assembly.")
        else:
            # Ensure column exists if strict=False (e.g. if M4 was missing)
            df[col] = 0.0

    # 4. Calculate CFO using the stabilized formula required by the validator
    # CFO = NPAT + DA + NWC_CF - Tax_Paid - Interest_Paid
    # Assumption: Tax_Paid and Interest_Paid are positive numbers representing outflows.
    df["CFO_NAD_000"] = (
        df["NPAT_NAD_000"] + 
        df["DA_NAD_000"] + 
        df["NWC_CF_NAD_000"] - 
        df["Tax_Paid_NAD_000"] - 
        df["Interest_Paid_NAD_000"]
    )
    _ok("CFO calculated using stabilized formula: NPAT + DA + NWC_CF - Tax_Paid - Interest_Paid.")

    # 5. Rename columns to the specific names required by the validator
    # This satisfies the requirement that caused the crash.
    rename_map_validator = {
        "NPAT_NAD_000": "Net_Profit_After_Tax_NAD_000",
        "DA_NAD_000": "Depreciation_NAD_000",
        "NWC_CF_NAD_000": "WC_Cash_Flow_NAD_000",
        # Interest_Paid_NAD_000 (Matches)
        # Tax_Paid_NAD_000 (Matches)
        "CFO_NAD_000": "Cash_Flow_from_Operations_NAD_000",
        # CFI_NAD_000 and CFF_NAD_000 are kept as is unless validator requires otherwise.
    }
    df.rename(columns=rename_map_validator, inplace=True)
    
    # Ensure final column order required by the validator
    final_order = [
        "Month_Index",
        "Net_Profit_After_Tax_NAD_000",
        "Depreciation_NAD_000",
        "WC_Cash_Flow_NAD_000",
        "Tax_Paid_NAD_000",
        "Interest_Paid_NAD_000",
        "Cash_Flow_from_Operations_NAD_000",
        "CFI_NAD_000",
        "CFF_NAD_000"
    ]
    # Select only the necessary columns
    df = df[final_order]


    # 6. Opening cash (policy default 0 if absent)
    opening_cash, open_m = _load_opening_cash(outputs, df["Month_Index"], strict=strict)

    # 7. Finalizing Metadata and Smoke
    # simple smoke meta (using renamed columns)
    smoke = {
        "months": int(df["Month_Index"].nunique()),
        "cfo_nonzero": bool((df["Cash_Flow_from_Operations_NAD_000"].abs() > 1e-6).any()),
        "cfi_nonzero": bool((df["CFI_NAD_000"].abs() > 1e-6).any()),
        "cff_nonzero": bool((df["CFF_NAD_000"].abs() > 1e-6).any()),
        "opening_cash_nad_000": float(opening_cash),
    }

    # Updated metadata
    meta = {
        "currency": currency,
        "sources": {
            "m2_pl": pl_m, "m2_wc": wc_m, "m1_capex": cfi_m, 
            "m3_revolver": cff_m, "m4_tax": tax_m, "m0_opening_cash": open_m
        },
        "policy": {
            "cfo_formula": "NPAT + DA + NWC_CF - Tax_Paid - Interest_Paid (Stabilized Formula)",
            "interest_source": "M3_Strict", # Interest is sourced exclusively from M3
            "tax_paid_source": "M4",
            "opening_cash_policy_default_zero_if_absent": True,
        },
        "column_mapping_to_validator": rename_map_validator
    }

    return df, meta, smoke

# Compatibility alias (older imports)
def assemble_cash_flow_statement(outputs_dir: str, currency: str, input_pack_path: Optional[str], strict: bool=False):
    return build_cash_flow_statement(outputs_dir, currency, input_pack_path, strict)
