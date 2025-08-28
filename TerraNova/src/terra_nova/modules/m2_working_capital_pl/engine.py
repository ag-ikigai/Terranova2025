
import pandas as pd
import numpy as np

def create_working_capital_schedules(monthly_revenue_df, monthly_opex_df, working_capital_tax_df, calendar_df):
    cal = calendar_df[["Month_Index"]].copy().drop_duplicates().sort_values("Month_Index")

    # Revenue aggregated to Month_Index
    if {"Month_Index","Crop","Monthly_Revenue_NAD_000"}.issubset(monthly_revenue_df.columns):
        rev = (monthly_revenue_df.groupby("Month_Index", as_index=False)["Monthly_Revenue_NAD_000"]
               .sum().rename(columns={"Monthly_Revenue_NAD_000":"Revenue"}))
    else:
        rev = monthly_revenue_df[["Month_Index","Monthly_Revenue_NAD_000"]].rename(
            columns={"Monthly_Revenue_NAD_000":"Revenue"}
        ).copy()
    rev = cal.merge(rev, on="Month_Index", how="left").fillna({"Revenue":0.0})
    rev["Revenue"] = pd.to_numeric(rev["Revenue"], errors="coerce").fillna(0.0)

    # Variable OPEX
    opex = cal.merge(
        monthly_opex_df[["Month_Index","Variable_OPEX_NAD_000"]], on="Month_Index", how="left"
    ).fillna({"Variable_OPEX_NAD_000":0.0})
    opex["Variable_OPEX_NAD_000"] = pd.to_numeric(opex["Variable_OPEX_NAD_000"], errors="coerce").fillna(0.0)

    # Parameters
    wct = (working_capital_tax_df[["Parameter","Value"]]
           .assign(Parameter=lambda d: d["Parameter"].astype(str))
           .assign(Value=lambda d: pd.to_numeric(d["Value"], errors="coerce")))
    def _get(name, default=0.0):
        s = wct.loc[wct["Parameter"]==name,"Value"]
        return float(s.iloc[0]) if not s.empty and pd.notna(s.iloc[0]) else float(default)

    AR_months  = _get("AR_Days_Local", 0.0) / 30.0
    AP_months  = _get("AP_Days", 0.0) / 30.0
    INV_months = _get("Inventory_Days", 0.0) / 30.0

    def trailing(series, months_float):
        if months_float <= 0: return pd.Series(0.0, index=series.index)
        n_full = int(np.floor(months_float)); frac = months_float - n_full
        bal = series * frac
        for k in range(1, n_full+1):
            bal = bal + series.shift(k, fill_value=0.0)
        return bal

    def forward(series, months_float):
        if months_float <= 0: return pd.Series(0.0, index=series.index)
        n_full = int(np.floor(months_float)); frac = months_float - n_full
        bal = series.shift(-1, fill_value=0.0) * frac
        for k in range(2, n_full+2):
            bal = bal + series.shift(-k+1, fill_value=0.0)
        return bal

    AR  = trailing(rev["Revenue"], AR_months)
    INV = forward(opex["Variable_OPEX_NAD_000"], INV_months)
    AP  = trailing(opex["Variable_OPEX_NAD_000"], AP_months)

    NWC   = AR + INV - AP
    delta = NWC - NWC.shift(1, fill_value=0.0)
    cfnwc = -delta

    out = pd.DataFrame({
        "Month_Index": cal["Month_Index"].astype(int),
        "AR_Balance_NAD_000": AR.astype(float),
        "Inventory_Balance_NAD_000": INV.astype(float),
        "AP_Balance_NAD_000": AP.astype(float),
        "NWC_Balance_NAD_000": NWC.astype(float),
        "Cash_Flow_from_NWC_Change_NAD_000": cfnwc.astype(float),
    }).sort_values("Month_Index").reset_index(drop=True)
    return out

def create_pl_statement(monthly_revenue_df, monthly_opex_df, monthly_depreciation_df, working_capital_tax_df, parameters_df):
    def _param(df, key, default=None, cast=float):
        s = df.loc[df.iloc[:,0].astype(str)==key, df.columns[1]]
        if s.empty or pd.isna(s.iloc[0]): return default
        try: return cast(s.iloc[0])
        except Exception: return default

    horizon = int(_param(parameters_df, "HORIZON_MONTHS", 60, int) or 60)
    preop   = int(_param(parameters_df, "PreOp_Months", 0, int) or 0)
    tax_series = working_capital_tax_df.loc[working_capital_tax_df["Parameter"]=="Corporate_Tax_Rate_pct","Value"]
    tax_rt  = float(tax_series.iloc[0]) / 100.0 if not tax_series.empty else 0.0

    df = pd.DataFrame({"Month_Index": range(1, horizon+1)})

    # revenue
    if {"Month_Index","Crop","Monthly_Revenue_NAD_000"}.issubset(monthly_revenue_df.columns):
        rev = (monthly_revenue_df.groupby("Month_Index", as_index=False)["Monthly_Revenue_NAD_000"]
               .sum().rename(columns={"Monthly_Revenue_NAD_000":"Total_Revenue_NAD_000"}))
    else:
        rev = monthly_revenue_df[["Month_Index","Monthly_Revenue_NAD_000"]].rename(
            columns={"Monthly_Revenue_NAD_000":"Total_Revenue_NAD_000"}
        )
    df = df.merge(rev, on="Month_Index", how="left").fillna({"Total_Revenue_NAD_000":0.0})
    if preop>0:
        df.loc[df["Month_Index"]<=preop, "Total_Revenue_NAD_000"] = 0.0

    # opex
    opex = monthly_opex_df[["Month_Index","Variable_OPEX_NAD_000","Fixed_OPEX_NAD_000"]].copy()
    df = df.merge(opex, on="Month_Index", how="left").fillna({"Variable_OPEX_NAD_000":0.0,"Fixed_OPEX_NAD_000":0.0})
    df["Total_OPEX_NAD_000"] = df["Variable_OPEX_NAD_000"] + df["Fixed_OPEX_NAD_000"]

    # depreciation
    dep = monthly_depreciation_df[["Month_Index","Depreciation_NAD_000"]].copy()
    df = df.merge(dep, on="Month_Index", how="left").fillna({"Depreciation_NAD_000":0.0})

    # P&L
    df["EBITDA_NAD_000"] = df["Total_Revenue_NAD_000"] - df["Total_OPEX_NAD_000"]
    df["EBIT_NAD_000"]   = df["EBITDA_NAD_000"] - df["Depreciation_NAD_000"]
    df["Interest_Expense_NAD_000"] = 0.0
    df["PBT_NAD_000"] = df["EBIT_NAD_000"]

    taxes = []
    loss_cf = 0.0
    for _, r in df.sort_values("Month_Index").iterrows():
        taxable = float(r["PBT_NAD_000"]) + loss_cf
        if taxable>0.0:
            tax = taxable * tax_rt
            loss_cf = 0.0
        else:
            tax = 0.0
            loss_cf = taxable
        taxes.append(tax)
    df["Tax_Expense_NAD_000"] = taxes
    df["NPAT_NAD_000"] = df["PBT_NAD_000"] - df["Tax_Expense_NAD_000"]
    return df
