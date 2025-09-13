
import pandas as pd
from pathlib import Path
from typing import Dict, Type, Any, List
from pydantic import BaseModel, ValidationError

from .data_contract import (
    ParametersModel, CaseLibraryModel, RevenueAssumptionsModel, RevRampSeasonalityModel,
    OPEXDetailModel, CAPEXScheduleModel, FXPathModel, WorkingCapitalTaxModel,
    FinanceStackScenariosModel, FinanceStackModel, Investor500kOfferGridModel
)

SHEET_MODEL_MAP: Dict[str, Type[BaseModel]] = {
    "Parameters": ParametersModel,
    "Case_Library": CaseLibraryModel,
    "Revenue_Assumptions": RevenueAssumptionsModel,
    "Rev_Ramp_Seasonality": RevRampSeasonalityModel,
    "OPEX_Detail": OPEXDetailModel,
    "CAPEX_Schedule": CAPEXScheduleModel,
    "FX_Path": FXPathModel,
    "Working_Capital_Tax": WorkingCapitalTaxModel,
    "Financing_Stack_Scenarios": FinanceStackScenariosModel,
    "Finance_Stack": FinanceStackModel,
    "Investor_500k_Offer_Grid": Investor500kOfferGridModel,
}

def _nan_to_none(d: Dict[str, Any]) -> Dict[str, Any]:
    out = {}
    for k, v in d.items():
        try:
            if pd.isna(v):
                out[k] = None
            else:
                out[k] = v
        except Exception:
            out[k] = v
    return out

def load_and_validate_input_pack(file_path: Path) -> Dict[str, pd.DataFrame]:
    file_path = Path(file_path)
    if not file_path.exists():
        raise FileNotFoundError(f"Error: Input file not found at {file_path}")
    try:
        sheets = pd.read_excel(file_path, sheet_name=None, engine="openpyxl")
    except Exception as exc:
        raise RuntimeError(f"Failed to read Excel workbook at {file_path}: {exc}") from exc

    validation_errors: List[str] = []

    for sheet_name, df in sheets.items():
        if sheet_name == "Notes_for_Use":
            continue
        model = SHEET_MODEL_MAP.get(sheet_name)
        if not model:
            continue
        if df is None or df.empty:
            continue
        for idx, row in df.iterrows():
            row_dict = _nan_to_none(row.to_dict())
            try:
                model(**row_dict)
            except ValidationError as e:
                validation_errors.append(
                    f"Validation Error in sheet '{sheet_name}', row {idx + 2}:\n  Data: {row_dict}\n  Errors: {e}\n"
                )

    if validation_errors:
        header = f"Input data validation failed with {len(validation_errors)} error(s):"
        raise ValueError(header + "\n\n" + "\n".join(validation_errors))

    return sheets

def create_calendar(parameters_df: pd.DataFrame) -> pd.DataFrame:
    if not {"Key", "Value"}.issubset(parameters_df.columns):
        raise ValueError("Parameters sheet must have 'Key' and 'Value' columns.")
    p = parameters_df.set_index("Key")["Value"]
    start_date = pd.to_datetime(p["START_DATE"])
    horizon_months = int(p["HORIZON_MONTHS"])
    rng = pd.date_range(start=start_date, periods=horizon_months, freq="ME")  # month-end
    cal = pd.DataFrame({"Date": rng})
    cal["Year"] = cal["Date"].dt.year
    cal["Month"] = cal["Date"].dt.month
    cal["Month_Index"] = range(1, horizon_months + 1)
    return cal[["Date", "Year", "Month", "Month_Index"]]

def create_opening_balance_sheet(fx_path_df: pd.DataFrame) -> pd.DataFrame:
    usd_investment = 500000
    if not {"Month", "NAD_per_USD"}.issubset(fx_path_df.columns):
        raise ValueError("FX_Path sheet must have 'Month' and 'NAD_per_USD' columns.")
    first_rate = float(fx_path_df.loc[fx_path_df["Month"] == 1, "NAD_per_USD"].iloc[0])
    nad_value = usd_investment * first_rate
    out = pd.DataFrame([
        {"Line_Item": "Cash", "Value_NAD": nad_value, "Notes": "Initial seed capital"},
        {"Line_Item": "Opening Equity-like", "Value_NAD": nad_value, "Notes": "Subject to reclassification per final instrument"},
    ])
    return out
