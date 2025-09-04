
import pandas as pd
import numpy as np

def create_capex_and_depreciation_schedules(capex_schedule_df: pd.DataFrame, calendar_df: pd.DataFrame):
    horizon = calendar_df["Month_Index"].max()
    capex_by_month = (capex_schedule_df.groupby("Month")["Amount_NAD_000"].sum().reset_index())
    monthly_capex_df = calendar_df[["Month_Index"]].copy()
    monthly_capex_df = monthly_capex_df.merge(capex_by_month, left_on="Month_Index", right_on="Month", how="left")
    monthly_capex_df.drop(columns=["Month"], inplace=True)
    monthly_capex_df.rename(columns={"Amount_NAD_000": "CAPEX_Outflow_NAD_000"}, inplace=True)
    monthly_capex_df["CAPEX_Outflow_NAD_000"] = monthly_capex_df["CAPEX_Outflow_NAD_000"].fillna(0)
    monthly_depreciation_df = pd.DataFrame({"Month_Index": range(1, horizon + 1), "Depreciation_NAD_000": np.zeros(horizon)})
    for _, row in capex_schedule_df.iterrows():
        life_years = row.get("Depreciation_Life_Yrs", 0)
        amount = row["Amount_NAD_000"]
        start_month = int(row["Month"]) + 1
        if life_years and life_years > 0:
            life_months = int(life_years) * 12
            monthly_dep = amount / life_months
            end_month = min(start_month + life_months - 1, horizon)
            monthly_depreciation_df.loc[(monthly_depreciation_df["Month_Index"] >= start_month) & (monthly_depreciation_df["Month_Index"] <= end_month), "Depreciation_NAD_000"] += monthly_dep
    return monthly_capex_df, monthly_depreciation_df

def create_opex_schedule(opex_detail_df: pd.DataFrame, calendar_df: pd.DataFrame, opex_multiplier: float) -> pd.DataFrame:
    required_cols = {"Category", "Y1", "Y2", "Y3", "Y4", "Y5"}
    if not required_cols.issubset(opex_detail_df.columns):
        raise ValueError(f"opex_detail_df missing required columns: {sorted(required_cols - set(opex_detail_df.columns))}")
    def _annual_total(cat: str) -> pd.Series:
        row = opex_detail_df.loc[opex_detail_df["Category"] == cat, ["Y1", "Y2", "Y3", "Y4", "Y5"]]
        return row.iloc[0].astype(float) if not row.empty else pd.Series(0.0, index=["Y1", "Y2", "Y3", "Y4", "Y5"])
    var_annual = _annual_total("Variable_OPEX_COGS")
    fix_annual = _annual_total("Fixed_OPEX_G_A")
    horizon = int(calendar_df["Month_Index"].max())
    monthly_opex_df = pd.DataFrame({"Month_Index": calendar_df["Month_Index"].astype(int)})
    def _year_key(m: int) -> str:
        y = ((m - 1) // 12) + 1
        return f"Y{min(max(y, 1), 5)}"
    year_keys = monthly_opex_df["Month_Index"].map(_year_key)
    monthly_opex_df["Variable_OPEX_NAD_000"] = (year_keys.map(var_annual.to_dict()).astype(float) / 12.0) * float(opex_multiplier)
    monthly_opex_df["Fixed_OPEX_NAD_000"] = (year_keys.map(fix_annual.to_dict()).astype(float) / 12.0) * float(opex_multiplier)
    return monthly_opex_df

def calculate_steady_state_revenue(revenue_assumptions_df: pd.DataFrame) -> pd.DataFrame:
    cols = ["Crop", "Hectares", "Yield_t_ha", "Price_NAD_per_t", "Cycles_per_year"]
    if not set(cols).issubset(revenue_assumptions_df.columns):
        raise ValueError(f"revenue_assumptions_df missing required columns: {sorted(set(cols) - set(revenue_assumptions_df.columns))}")
    df = revenue_assumptions_df[cols].copy()
    for c in ["Hectares", "Yield_t_ha", "Price_NAD_per_t", "Cycles_per_year"]:
        df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0.0)
    df["Steady_State_Revenue_NAD_000"] = (df["Hectares"] * df["Yield_t_ha"] * df["Price_NAD_per_t"] * df["Cycles_per_year"]) / 1000.0
    return df[["Crop", "Steady_State_Revenue_NAD_000"]]

def apply_ramps_and_scenarios(steady_state_revenue_df: pd.DataFrame, rev_ramp_seasonality_df: pd.DataFrame, price_multiplier: float, yield_multiplier: float) -> pd.DataFrame:
    scenario_factor = float(price_multiplier) * float(yield_multiplier)
    adj = steady_state_revenue_df.copy()
    adj["Adj_Steady_NAD_000"] = adj["Steady_State_Revenue_NAD_000"] * scenario_factor
    rfs = rev_ramp_seasonality_df[["Crop", "Y1_Ramp", "Y2_Ramp", "Y3_Ramp"]].copy()
    df = adj.merge(rfs, on="Crop", how="left").fillna(0.0)
    for i in range(1, 6):
        ramp_col = f"Y{i}_Ramp"
        df[f"Year_{i}"] = df["Adj_Steady_NAD_000"] * (df[ramp_col] if ramp_col in df else 1.0)
    out = df.melt(id_vars=["Crop"], value_vars=[f"Year_{i}" for i in range(1, 6)], var_name="Year", value_name="Adjusted_Annual_Revenue_NAD_000")
    out["Year"] = out["Year"].str.replace("Year_", "").astype(int)
    return out.sort_values(["Crop", "Year"]).reset_index(drop=True)

def distribute_revenue_monthly(adjusted_annual_revenue_df: pd.DataFrame, rev_ramp_seasonality_df: pd.DataFrame, calendar_df: pd.DataFrame) -> pd.DataFrame:
    m_cols = [f"M{i}" for i in range(1, 13)]
    season = rev_ramp_seasonality_df[["Crop"] + m_cols].copy()
    season_long = season.melt(id_vars=["Crop"], value_vars=m_cols, var_name="M", value_name="Seasonality_Weight")
    season_long["Month"] = season_long["M"].str.replace("M", "", regex=False).astype(int)
    expanded = calendar_df.merge(adjusted_annual_revenue_df, on="Year", how="inner")
    expanded = expanded.merge(season_long, on=["Crop", "Month"], how="left").fillna(0.0)
    expanded["Monthly_Revenue_NAD_000"] = expanded["Adjusted_Annual_Revenue_NAD_000"] * expanded["Seasonality_Weight"]
    return expanded[["Month_Index", "Crop", "Monthly_Revenue_NAD_000"]].sort_values(["Crop", "Month_Index"]).reset_index(drop=True)
