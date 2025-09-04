from __future__ import annotations
import pandas as pd
import numpy as np
from typing import Dict

def _calculate_wc_deltas(wc_schedule: pd.DataFrame) -> pd.DataFrame:
    """Calculate monthly working-capital deltas with cash-impact signs.
    Contract: requires Month_Index and *_EOP columns; first-period delta = first EOP.
    """
    if "Month_Index" not in wc_schedule.columns:
        raise KeyError("[M5] WC schedule missing 'Month_Index'.")
    df = wc_schedule.sort_values("Month_Index").copy()

    AR  = "Accounts_Receivable_EOP"
    INV = "Inventory_EOP"
    AP  = "Accounts_Payable_EOP"
    required_cols = [AR, INV, AP]
    missing_cols = [c for c in required_cols if c not in df.columns]
    if missing_cols:
        raise KeyError(f"[M5] Missing required EOP columns in WC schedule: {missing_cols}")

    delta_ar_raw  = df[AR].diff().fillna(df[AR].iloc[0])
    delta_inv_raw = df[INV].diff().fillna(df[INV].iloc[0])
    delta_ap_raw  = df[AP].diff().fillna(df[AP].iloc[0])

    df["Delta_Accounts_Receivable"] = -delta_ar_raw
    df["Delta_Inventory"]           = -delta_inv_raw
    df["Delta_Accounts_Payable"]    =  delta_ap_raw

    return df[[
        "Month_Index",
        "Delta_Accounts_Receivable",
        "Delta_Inventory",
        "Delta_Accounts_Payable",
    ]]

def assemble_cash_flow_statement(
    pl_statement: pd.DataFrame,
    wc_schedule: pd.DataFrame,
    currency: str
) -> Dict[str, pd.DataFrame]:
    """Assemble the monthly cash-flow statement (CFO only in engine).
    CLI will enrich with CFI/CFF using audited M3 artifact per M5.full P4.
    Returns: { "statement": DataFrame }
    """
    required_pl = ["Month_Index", "Net_Profit_After_Tax", "Depreciation_and_Amortization"]
    missing_pl = [c for c in required_pl if c not in pl_statement.columns]
    if missing_pl:
        raise KeyError(f"[M5] Missing required columns in P&L statement: {missing_pl}")
    pl = pl_statement[required_pl].copy()

    wc_deltas = _calculate_wc_deltas(wc_schedule)
    cfs = pd.merge(pl, wc_deltas, on="Month_Index", how="inner")
    if len(cfs) != len(pl):
        raise ValueError("[M5] Time horizon mismatch between P&L and WC schedule.")

    cfs["CFO"] = (
        cfs["Net_Profit_After_Tax"]
        + cfs["Depreciation_and_Amortization"]
        + cfs["Delta_Accounts_Receivable"]
        + cfs["Delta_Inventory"]
        + cfs["Delta_Accounts_Payable"]
    )

    cfs["CFI"] = 0.0
    cfs["CFF"] = 0.0
    cfs["Net_Change_in_Cash"] = cfs["CFO"] + cfs["CFI"] + cfs["CFF"]
    cfs["Currency"] = str(currency)

    final = [
        "Month_Index","Currency",
        "Net_Profit_After_Tax","Depreciation_and_Amortization",
        "Delta_Accounts_Receivable","Delta_Inventory","Delta_Accounts_Payable",
        "CFO","CFI","CFF","Net_Change_in_Cash"
    ]
    return {"statement": cfs[final]}
