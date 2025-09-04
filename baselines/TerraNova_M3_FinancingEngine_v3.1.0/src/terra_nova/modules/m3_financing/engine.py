
from __future__ import annotations
import pandas as pd
import numpy as np
from typing import Dict
__version__ = "3.1.0"

# ----------------------------- utilities -----------------------------

def _annuity_payment(principal: float, rate_m: float, periods: int) -> float:
    """
    Deterministic annuity (mortgage-style) payment.
    Compatible with NumPy >= 1.20 where np.pmt is removed.
    """
    if periods <= 0:
        return float(principal)
    if abs(rate_m) < 1e-12:
        return float(principal) / float(periods)
    return float(principal) * (rate_m) / (1.0 - (1.0 + rate_m) ** (-periods))


# ----------------------------- standard loan schedule -----------------------------

def create_loan_schedule(loan_params: pd.Series, calendar_df: pd.DataFrame) -> pd.DataFrame:
    """
    Build a 60-month (or calendar horizon) amortization schedule for a single loan.
    - Interest accrues on opening balance only (per spec).
    - Amortization starts after draw window + grace: Draw_End_M + Grace_Principal_M + 1 (capped at Tenor).
    - Supports 'annuity', 'straight', 'bullet' amortization types.
    - No capitalization of interest. No FX.
    Units: FULL currency units (not thousands).
    """
    # horizon from calendar
    horizon = int(pd.to_numeric(calendar_df["Month_Index"]).max())

    # Extract and sanitize inputs
    def _ival(name, default=0):
        v = loan_params.get(name, default)
        try:
            return int(v)
        except Exception:
            return int(float(v) if v not in (None, "") else default)

    def _fval(name, default=0.0):
        v = loan_params.get(name, default)
        try:
            return float(v)
        except Exception:
            return float(default)

    principal = _fval("Principal", 0.0)
    rate_pct = _fval("Rate_Pct", 0.0)
    tenor = max(_ival("Tenor_Months", horizon), 0)
    draw_start = max(_ival("Draw_Start_M", 1), 1)
    draw_end_raw = _ival("Draw_End_M", draw_start)
    # clamp draw_end to tenor and horizon (micro-hardening allowed by spec)
    draw_end = max(0, min(draw_end_raw, tenor, horizon))
    grace_prin_m = max(_ival("Grace_Principal_M", 0), 0)
    amort_type = str(loan_params.get("Amort_Type", "annuity")).strip().lower()

    r_m = float(rate_pct) / 100.0 / 12.0

    # Equal monthly draws over the draw window
    draw_months = max(0, draw_end - draw_start + 1) if draw_end >= draw_start else 0
    monthly_draw = (principal / draw_months) if draw_months > 0 else 0.0

    # Amortization start/end (strict per spec)
    amort_start = min(draw_end + grace_prin_m + 1, tenor)
    amort_nper = max(tenor - amort_start + 1, 0)

    df = pd.DataFrame({
        "Month_Index": pd.RangeIndex(1, horizon + 1, 1, name="Month_Index"),
        "Opening_Balance": 0.0,
        "Drawdown": 0.0,
        "Interest_Accrued": 0.0,
        "Principal_Repayment": 0.0,
        "Closing_Balance": 0.0,
    })
    df["Month_Index"] = df["Month_Index"].astype(int)

    annuity_pmt = None
    straight_principal = None
    pv_at_amort_start = None

    for i in range(horizon):
        m = int(i + 1)
        opening = float(df.loc[i - 1, "Closing_Balance"]) if i > 0 else 0.0

        # Draws only during draw window
        draw = monthly_draw if (draw_months > 0 and (draw_start <= m <= draw_end)) else 0.0

        # Interest accrues on opening only, within tenor
        interest = (opening * r_m) if (1 <= m <= tenor) else 0.0

        repay = 0.0
        if amort_nper > 0 and (amort_start <= m <= tenor):
            # initialize PV and policy amounts when amortization begins
            if pv_at_amort_start is None:
                pv_at_amort_start = opening + 0.0  # draw already ended before amort start
            if amort_type == "annuity":
                if annuity_pmt is None:
                    annuity_pmt = _annuity_payment(pv_at_amort_start, r_m, amort_nper)
                repay = max(0.0, annuity_pmt - interest)
                # plug residual at final amort month
                if m == tenor:
                    repay = max(0.0, opening + draw)
            elif amort_type == "straight":
                if straight_principal is None:
                    straight_principal = (pv_at_amort_start / amort_nper) if amort_nper > 0 else 0.0
                repay = straight_principal
                if m == tenor:
                    repay = max(0.0, opening + draw)  # plug residual
            elif amort_type == "bullet":
                repay = 0.0 if m < tenor else max(0.0, opening + draw)
            else:
                # default to annuity if unknown (defensive, but consistent with spec intent)
                if annuity_pmt is None:
                    annuity_pmt = _annuity_payment(pv_at_amort_start, r_m, amort_nper)
                repay = max(0.0, annuity_pmt - interest)
                if m == tenor:
                    repay = max(0.0, opening + draw)

        closing = opening + draw - repay

        df.loc[i, "Opening_Balance"] = opening
        df.loc[i, "Drawdown"] = draw
        df.loc[i, "Interest_Accrued"] = interest
        df.loc[i, "Principal_Repayment"] = repay
        df.loc[i, "Closing_Balance"] = closing

    return df


# ----------------------------- revolver & insurance helpers -----------------------------

def _create_revolver_schedule(row: pd.Series, calendar_df: pd.DataFrame) -> pd.DataFrame:
    """
    Revolver cost placeholder over FULL calendar horizon.
    Interest accrues at 50%% utilization only for months in [Draw_Start_M .. Tenor_Months].
    Balances/draws/repays remain zero per spec.
    """
    principal = float(row["Principal"])
    rate_m    = (float(row["Rate_Pct"]) / 100.0) / 12.0
    tenor     = int(row["Tenor_Months"])
    draw_start= int(row["Draw_Start_M"])
    line_id   = int(row["Line_ID"])
    ccy       = str(row["Currency"])

    df = pd.DataFrame({"Month_Index": calendar_df["Month_Index"].astype(int)})
    df["Line_ID"]          = line_id
    df["Currency"]         = ccy
    df["Opening_Balance"]  = 0.0
    df["Drawdown"]         = 0.0
    df["Repayment"]        = 0.0
    df["Interest_Accrued"] = 0.0
    df["Closing_Balance"]  = 0.0

    active = (df["Month_Index"] >= draw_start) & (df["Month_Index"] <= tenor)
    df.loc[active, "Interest_Accrued"] = 0.5 * principal * rate_m

    return df[["Month_Index","Line_ID","Currency","Opening_Balance","Drawdown","Repayment","Interest_Accrued","Closing_Balance"]]



def _create_insurance_schedule(row: pd.Series, calendar_df: pd.DataFrame) -> pd.DataFrame:
    """
    v1.0 'Insurance OFF' stub:
    Emit a month-by-month schedule with all zeros for cash, expense, and prepaid balances.
    This preserves schema and timing while deferring the real model to a later version.
    """
    horizon = int(calendar_df["Month_Index"].max())
    line_id = int(row["Line_ID"])
    currency = str(row["Currency"])

    idx = pd.Index(range(1, horizon + 1), name="Month_Index")
    df = pd.DataFrame({
        "Line_ID": line_id,
        "Currency": currency,
        "Premium_Cash_Outflow": 0.0,
        "Expense_Recognized": 0.0,
        "Prepaid_BOP": 0.0,
        "Prepaid_EOP": 0.0,
    }, index=idx).reset_index()

    # Keep the column order stable for parquet determinism.
    return df[[
        "Month_Index", "Line_ID", "Currency",
        "Premium_Cash_Outflow", "Expense_Recognized", "Prepaid_BOP", "Prepaid_EOP"
    ]]


def create_financing_schedules(finance_stack_df: pd.DataFrame,
                               pfinance_case_selector_df: pd.DataFrame,
                               calendar_df: pd.DataFrame) -> Dict[str, pd.DataFrame]:
    """
    Processes the selected financing case across all ACTIVE instruments and returns
    a dict with the three M3 artifacts:
      - 'revolver': monthly interest-only cost schedule for revolving facilities
      - 'insurance': monthly premium/expense/prepaid schedule for insurance policies
      - 'index': compact index of all processed rows (all instruments, including standard loans)
    """
    # validate inputs (schema guard)
    req_cols = {"Case_Name","Line_ID","Instrument","Currency","Principal","Rate_Pct","Tenor_Months",
                "Draw_Start_M","Draw_End_M","Grace_Int_M","Grace_Principal_M","Amort_Type","Balloon_Pct",
                "Revolving","Is_Insurance","Premium_Rate_Pct","Secured_By","Active"}
    missing = req_cols - set(finance_stack_df.columns)
    if missing:
        raise KeyError(f"Finance_Stack missing required columns: {sorted(missing)}")
    if "Key" not in pfinance_case_selector_df.columns or "Value" not in pfinance_case_selector_df.columns:
        raise KeyError("PFinance_Case_Selector must contain 'Key' and 'Value' columns.")

    # case selection
    sel = pfinance_case_selector_df.loc[pfinance_case_selector_df["Key"] == "PFinance_Case", "Value"]
    if sel.empty:
        raise KeyError("PFinance_Case_Selector is missing the 'PFinance_Case' row.")
    active_case = str(sel.iloc[0])

    # filter active case rows
    rows = finance_stack_df.copy()
    rows = rows.loc[(rows["Case_Name"] == active_case) & (rows["Active"] == 1)].copy()
    rows = rows.reset_index(drop=True)

    revolver_schedules = []
    insurance_schedules = []

    for _, r in rows.iterrows():
        if int(r.get("Is_Insurance", 0) or 0) == 1:
            insurance_schedules.append(_create_insurance_schedule(r, calendar_df))
        elif int(r.get("Revolving", 0) or 0) == 1:
            revolver_schedules.append(_create_revolver_schedule(r, calendar_df))
        else:
            # standard loan schedule computed (validated by tests) but NOT persisted in M3
            _ = create_loan_schedule(r, calendar_df)

    revolver_df = pd.concat(revolver_schedules, ignore_index=True) if revolver_schedules else pd.DataFrame(
        columns=["Month_Index","Line_ID","Currency","Opening_Balance","Drawdown","Repayment","Interest_Accrued","Closing_Balance"]
    )
    insurance_df = pd.concat(insurance_schedules, ignore_index=True) if insurance_schedules else pd.DataFrame(
        columns=["Month_Index","Line_ID","Currency","Premium_Cash_Outflow","Expense_Recognized","Prepaid_BOP","Prepaid_EOP"]
    )

    index_df = rows[["Case_Name","Line_ID","Instrument","Revolving","Is_Insurance","Currency"]].copy()

    return {"revolver": revolver_df, "insurance": insurance_df, "index": index_df}
