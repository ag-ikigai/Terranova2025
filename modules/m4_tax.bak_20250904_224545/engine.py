# src/terra_nova/modules/m4_tax/engine.py
from __future__ import annotations

from typing import Dict, Optional
import pandas as pd

__all__ = ["compute_tax_schedule"]

REQ_CAL_COL = "Month_Index"

def _required_cols(df: pd.DataFrame, cols, name: str) -> None:
    missing = [c for c in cols if c not in df.columns]
    if missing:
        raise ValueError(f"{name} missing columns: {missing}")

def compute_tax_schedule(
    calendar_df: pd.DataFrame,
    pl_statement_df: pd.DataFrame,
    tax_cfg: Optional[pd.DataFrame],
    case_name: str,
    currency: str,
    opening_bs_df: Optional[pd.DataFrame] = None,
) -> Dict[str, pd.DataFrame]:
    """
    Minimal, deterministic M4 v4.0.3 schedule builder (packaging-safe).

    - Single consolidated monthly schedule (no tuple of DFs).
    - Zeroed fallback when tax_cfg is missing/empty.
    - 'Configured' placeholder when tax_cfg exists (logic increments land in M4.4+).
    - Pure function: no file I/O.
    """
    _required_cols(calendar_df, [REQ_CAL_COL], "calendar_df")
    # pl_statement_df is accepted for contract stability; require Month_Index if present
    if pl_statement_df is not None and not pl_statement_df.empty:
        _required_cols(pl_statement_df, [REQ_CAL_COL], "pl_statement_df")

    horizon = int(pd.to_numeric(calendar_df[REQ_CAL_COL], errors="coerce").max())
    idx = pd.Index(range(1, horizon + 1), name=REQ_CAL_COL)

    sched = pd.DataFrame({
        REQ_CAL_COL: idx,
        "Taxable_Profit": 0.0,
        "Loss_CF_BOP": 0.0,
        "Loss_Used": 0.0,
        "Loss_CF_EOP": 0.0,
        "Tax_Expense": 0.0,
        "Notes": ""
    }).reset_index(drop=True)

    zeroed = (tax_cfg is None) or (getattr(tax_cfg, "empty", False) is True) or (len(tax_cfg) == 0)
    mode = "zeroed" if zeroed else "configured"
    if zeroed:
        sched["Notes"] = "ZEROED: missing tax config"
    else:
        # Placeholder: logic will be added in M4.4+
        sched["Notes"] = ""

    summary = pd.DataFrame([{
        "Case_Name": str(case_name),
        "Currency": str(currency),
        "Tax_Expense_Total": float(sched["Tax_Expense"].sum()),
        "Loss_CF_EOP_Total": float(sched["Loss_CF_EOP"].iloc[-1]) if len(sched) else 0.0,
        "Computation_Mode": mode,
    }])

    # Ensure numeric types where appropriate
    for c in ["Taxable_Profit","Loss_CF_BOP","Loss_Used","Loss_CF_EOP","Tax_Expense"]:
        sched[c] = pd.to_numeric(sched[c], errors="coerce").fillna(0.0)

    return {"schedule": sched, "summary": summary}
