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
    "DEBT_OUT": [
        "Outstanding_Balance_NAD_000",
        "Debt_Outstanding_NAD_000",
        "Outstanding_NAD_000",
        "Principal_Balance_NAD_000",
        "Revolver_Balance_NAD_000",
        "Debt_Balance_NAD_000",
        "Outstanding_Balance",
    ],
    # M4 tax payable (if present) or derive from expense/paid
    "TAX_PAYABLE": ["Tax_Payable_NAD_000", "Tax_Payable"],
    "TAX_EXPENSE": ["Tax_Expense_NAD_000", "Tax_Expense"],
    "TAX_PAID": ["Tax_Paid_NAD_000", "Tax_Paid", "Taxes_Paid_NAD_000"],
}

def _pick(df: pd.DataFrame, candidates: list[str]) -> str | None:
    cols = {c.lower(): c for c in df.columns}
    for name in candidates:
        key = name.lower()
        if key in cols:
            return cols[key]
        # fuzzy contains for common labels
        for c in cols:
            if key in c:
                return cols[c]
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
        return (exp - paid).cumsum()
    return pd.Series(np.zeros(len(tax_df), dtype=float))

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
      - Retained earnings = cumulative NPAT (from M2 P&L)
      - Net working capital level is reconstructed from NWC cash-flow deltas: NWC(t) = sum(-NWC_CF)
      - Split NWC into asset/liability buckets for presentation
      - Debt outstanding from M3 (revolver schedule)
      - Tax payable from M4 (explicit or derived)
      - Cash is a balancing item so Assets == Liabilities + Equity by construction

    This will be replaced by true cash (CLOSING_CASH) once CAPEX and Equity wiring lands in v7.5.
    """
    # Align on Month_Index
    mn_m = _pick(m2_pl, ROLE["MONTH_INDEX"])
    mw_m = _pick(m2_wc, ROLE["MONTH_INDEX"])
    md_m = _pick(m3_debt, ROLE["MONTH_INDEX"])
    mt_m = _pick(m4_tax, ROLE["MONTH_INDEX"])
    for label, col in [("M2/PL", mn_m), ("M2/WC", mw_m), ("M3/DEBT", md_m), ("M4/TAX", mt_m)]:
        if not col:
            raise AssertionError(f"[M6] Missing Month_Index in {label}")

    df = pd.DataFrame({"Month_Index": m2_pl[mn_m].astype(int)})
    # Retained earnings
    col_npat = _pick(m2_pl, ROLE["NPAT"])
    if not col_npat:
        raise AssertionError("[M6] NPAT column not found in M2 P&L")
    df["Equity_Retained_Earnings_NAD_000"] = pd.to_numeric(m2_pl[col_npat], errors="coerce").fillna(0.0).cumsum()

    # Reconstruct NWC level from NWC CF (positive CF = release => NWC decreases)
    col_nwc_cf = _pick(m2_wc, ROLE["NWC_CF"])
    if not col_nwc_cf:
        raise AssertionError("[M6] NWC cash-flow column not found in M2 WC schedule")
    nwc_cf = pd.to_numeric(m2_wc[col_nwc_cf], errors="coerce").fillna(0.0)
    nwc_level = (-nwc_cf).cumsum()  # level grows when CF negative (investment)
    nwc_asset = nwc_level.clip(lower=0.0)
    nwc_liab = (-nwc_level).clip(lower=0.0)
    df["NWC_Asset_NAD_000"] = nwc_asset
    df["NWC_Liability_NAD_000"] = nwc_liab

    # Debt outstanding
    col_debt = _pick(m3_debt, ROLE["DEBT_OUT"])
    if not col_debt:
        # degrade gracefully to zeros if schedule present but no recognizable column
        debt_out = pd.Series(np.zeros(len(df), dtype=float))
    else:
        aligned_debt = m3_debt.set_index(m3_debt[md_m].astype(int)).reindex(df["Month_Index"]).reset_index(drop=True)
        debt_out = pd.to_numeric(aligned_debt[col_debt], errors="coerce").fillna(0.0)
    df["Debt_Outstanding_NAD_000"] = debt_out

    # Tax payable
    aligned_tax = m4_tax.set_index(m4_tax[mt_m].astype(int)).reindex(df["Month_Index"]).reset_index(drop=True)
    tax_payable = derive_tax_payable(aligned_tax)
    df["Tax_Payable_NAD_000"] = pd.to_numeric(tax_payable, errors="coerce").fillna(0.0)

    # Equity - share capital (0 in v1; to be wired in v7.5)
    df["Equity_Share_Capital_NAD_000"] = float(start_share_capital)

    # Totals and balancing cash
    equity_total = df["Equity_Share_Capital_NAD_000"] + df["Equity_Retained_Earnings_NAD_000"]
    liab_total = df["Debt_Outstanding_NAD_000"] + df["Tax_Payable_NAD_000"] + df["NWC_Liability_NAD_000"]
    liab_eq_total = liab_total + equity_total

    cash_balancing = liab_eq_total - df["NWC_Asset_NAD_000"]
    assets_total = cash_balancing + df["NWC_Asset_NAD_000"]

    df["Cash_Balancing_Item_NAD_000"] = cash_balancing
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
