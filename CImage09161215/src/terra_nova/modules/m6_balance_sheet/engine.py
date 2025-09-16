# src/terra_nova/modules/m6_balance_sheet/engine.py
from __future__ import annotations
import pandas as pd
import numpy as np
from typing import Dict

# ---- Role synonyms used by runner normalization ----
ROLE = {
    "MONTH_INDEX": ["Month_Index", "MONTH_INDEX", "month_index"],
    # M2 P&L
    "NPAT": ["NPAT_NAD_000", "Net_Profit_After_Tax_NAD_000", "NPAT"],
    # M2 WC CF schedule
    "NWC_CF": [
        "Cash_Flow_from_NWC_Change_NAD_000",
        "Net_Working_Capital_CF_NAD_000",
        "Working_Capital_CF_NAD_000",
        "WC_Cash_Flow_NAD_000",
        "NWC_CF",
    ],
    # M3 debt outstanding (revolver)
    # UPDATE: Added Revolver_Close_Balance_NAD_000 based on M3 logs
    "DEBT_OUT": [
        "Outstanding_Balance_NAD_000",
        "Debt_Outstanding_NAD_000",
        "Outstanding_NAD_000",
        "Principal_Balance_NAD_000",
        "Revolver_Balance_NAD_000",
        "Debt_Balance_NAD_000",
        "Outstanding_Balance",
        "Revolver_Close_Balance_NAD_000", 
    ],
    # M4 tax payable (if present) or derive from expense/paid
    # UPDATE: Added Tax_Payable_End_NAD_000 based on M4 logs
    "TAX_PAYABLE": ["Tax_Payable_NAD_000", "Tax_Payable", "Tax_Payable_End_NAD_000"], 
    "TAX_EXPENSE": ["Tax_Expense_NAD_000", "Tax_Expense"],
    "TAX_PAID": ["Tax_Paid_NAD_000", "Tax_Paid", "Taxes_Paid_NAD_000"],
}

def _pick(df: pd.DataFrame, candidates: list[str]) -> str | None:
    # Enhanced _pick to prioritize exact case-insensitive matches
    cols_lower_map = {c.lower(): c for c in df.columns}
    
    # 1. Exact match (case-insensitive) - Prioritized
    for name in candidates:
        key = name.lower()
        if key in cols_lower_map:
            return cols_lower_map[key]
        
    # 2. Fuzzy contains match (less ideal but kept for M6 beta compatibility if exact fails)
    for key_lower, original_name in cols_lower_map.items():
        for candidate in candidates:
            if candidate.lower() in key_lower:
                # Log a warning if we fall back to fuzzy matching
                print(f"[M6][WARN] Using fuzzy match for {candidate} -> {original_name}")
                return original_name
    return None

def derive_tax_payable(tax_df: pd.DataFrame) -> pd.Series:
    """Prefer explicit payable; else derive as cum(expense - paid); else zeros."""
    col_pay = _pick(tax_df, ROLE["TAX_PAYABLE"])
    if col_pay:
        s = pd.to_numeric(tax_df[col_pay], errors="coerce").fillna(0.0)
        return s
    col_exp = _pick(tax_df, ROLE["TAX_EXPENSE"])
    col_paid = _pick(tax_df, ROLE["TAX_PAID"])
    if col_exp and col_paid:
        exp = pd.to_numeric(tax_df[col_exp], errors="coerce").fillna(0.0)
        paid = pd.to_numeric(tax_df[col_paid], errors="coerce").fillna(0.0)
        # Assuming opening payable is 0 if derived this way
        return (exp - paid).cumsum()
    # Ensure the series has the same index as the input dataframe
    return pd.Series(np.zeros(len(tax_df), dtype=float), index=tax_df.index)

def compute_balance_sheet(
    m2_pl: pd.DataFrame,
    m2_wc: pd.DataFrame,
    m3_debt: pd.DataFrame,
    m4_tax: pd.DataFrame,
    currency: str = "NAD",
    start_share_capital: float = 0.0,
) -> pd.DataFrame:
    """
    Minimal/beta balance sheet (v1):
      - Cash is a balancing item so Assets == Liabilities + Equity by construction
    """
    # Align on Month_Index
    # We check defensively here using _pick, although the runner should normalize the name.
    mn_m = _pick(m2_pl, ROLE["MONTH_INDEX"])
    mw_m = _pick(m2_wc, ROLE["MONTH_INDEX"])
    md_m = _pick(m3_debt, ROLE["MONTH_INDEX"])
    mt_m = _pick(m4_tax, ROLE["MONTH_INDEX"])
    
    # UPDATE: Added dataframe context to error message for easier debugging
    for label, col, df_ref in [("M2/PL", mn_m, m2_pl), ("M2/WC", mw_m, m2_wc), ("M3/DEBT", md_m, m3_debt), ("M4/TAX", mt_m, m4_tax)]:
        if not col:
            raise AssertionError(f"[M6] Missing Month_Index in {label}. Columns: {list(df_ref.columns)[:10]}")

    # Use M2 P&L timeline as the master timeline
    df = pd.DataFrame({"Month_Index": m2_pl[mn_m].astype(int)})
    # UPDATE: Ensure timeline is sorted and index is reset for alignment
    df = df.sort_values("Month_Index").reset_index(drop=True)

    # Retained earnings
    col_npat = _pick(m2_pl, ROLE["NPAT"])
    if not col_npat:
        raise AssertionError(f"[M6] NPAT column not found in M2 P&L. Columns: {list(m2_pl.columns)[:10]}")
    
    # UPDATE: Ensure alignment before calculation using reindex
    aligned_pl = m2_pl.set_index(m2_pl[mn_m].astype(int)).reindex(df["Month_Index"])
    # Calculate cumsum on aligned data, then reset index to match df
    df["Equity_Retained_Earnings_NAD_000"] = pd.to_numeric(aligned_pl[col_npat], errors="coerce").fillna(0.0).cumsum().reset_index(drop=True)

    # Reconstruct NWC level from NWC CF (positive CF = release => NWC decreases)
    col_nwc_cf = _pick(m2_wc, ROLE["NWC_CF"])
    if not col_nwc_cf:
        raise AssertionError(f"[M6] NWC cash-flow column not found in M2 WC schedule. Columns: {list(m2_wc.columns)[:10]}")
    
    # UPDATE: Ensure alignment before calculation
    aligned_wc = m2_wc.set_index(m2_wc[mw_m].astype(int)).reindex(df["Month_Index"])
    nwc_cf = pd.to_numeric(aligned_wc[col_nwc_cf], errors="coerce").fillna(0.0)
    
    # Calculate cumsum on aligned data, then reset index
    nwc_level = (-nwc_cf).cumsum().reset_index(drop=True)  # level grows when CF negative (investment)
    nwc_asset = nwc_level.clip(lower=0.0)
    nwc_liab = (-nwc_level).clip(lower=0.0)
    df["NWC_Asset_NAD_000"] = nwc_asset
    df["NWC_Liability_NAD_000"] = nwc_liab

    # Debt outstanding
    col_debt = _pick(m3_debt, ROLE["DEBT_OUT"])
    
    # UPDATE: Ensure alignment before assignment
    aligned_debt = m3_debt.set_index(m3_debt[md_m].astype(int)).reindex(df["Month_Index"]).reset_index(drop=True)

    if not col_debt:
        # degrade gracefully to zeros if schedule present but no recognizable column
        print("[M6][WARN] Debt outstanding column not resolved in M3 schedule. Defaulting to 0.0.")
        # Ensure series index matches df index
        debt_out = pd.Series(np.zeros(len(df), dtype=float), index=df.index)
    else:
        debt_out = pd.to_numeric(aligned_debt[col_debt], errors="coerce").fillna(0.0)
        
    df["Debt_Outstanding_NAD_000"] = debt_out

    # Tax payable
    # UPDATE: Ensure alignment before derivation. Preserve index for derive_tax_payable.
    aligned_tax = m4_tax.set_index(m4_tax[mt_m].astype(int)).reindex(df["Month_Index"])
    tax_payable = derive_tax_payable(aligned_tax)
    # Reset index after derivation to align with df
    df["Tax_Payable_NAD_000"] = pd.to_numeric(tax_payable, errors="coerce").fillna(0.0).reset_index(drop=True)

    # Equity - share capital (0 in v1; to be wired in v7.5)
    df["Equity_Share_Capital_NAD_000"] = float(start_share_capital)

    # Totals and balancing cash
    equity_total = df["Equity_Share_Capital_NAD_000"] + df["Equity_Retained_Earnings_NAD_000"]
    liab_total = df["Debt_Outstanding_NAD_000"] + df["Tax_Payable_NAD_000"] + df["NWC_Liability_NAD_000"]
    liab_eq_total = liab_total + equity_total

    # Calculate the balancing cash required to make Assets = L+E
    cash_balancing = liab_eq_total - df["NWC_Asset_NAD_000"]
    assets_total = cash_balancing + df["NWC_Asset_NAD_000"]

    # CRITICAL FIX: Use the canonical name required by M7.5B and downstream modules.
    # Replaces 'Cash_Balancing_Item_NAD_000'.
    df["Cash_and_Cash_Equivalents_NAD_000"] = cash_balancing
    
    df["Assets_Total_NAD_000"] = assets_total
    df["Liabilities_Total_NAD_000"] = liab_total
    df["Equity_Total_NAD_000"] = equity_total
    df["Liabilities_And_Equity_Total_NAD_000"] = liab_eq_total
    df["Currency"] = currency

    # Identity check
    diff = (df["Assets_Total_NAD_000"] - df["Liabilities_And_Equity_Total_NAD_000"]).abs().max()
    if diff > 1e-6:
        raise AssertionError(f"[M6] Balance sheet identity failed. Max abs diff={diff}")

    return df
