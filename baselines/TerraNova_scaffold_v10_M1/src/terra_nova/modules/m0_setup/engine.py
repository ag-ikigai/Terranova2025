
import pandas as pd
from pathlib import Path
from typing import Dict, Type, Any, List
import numpy as np
from pydantic import BaseModel, ValidationError

from .data_contract import (
    ParametersModel, CaseLibraryModel, RevenueAssumptionsModel,
    RevRampSeasonalityModel, OPEXDetailModel, CAPEXScheduleModel,
    FXPathModel, WorkingCapitalTaxModel, FinanceStackScenariosModel,
    FinanceStackModel, Investor500kOfferGridModel
)

SHEET_MODEL_MAP: Dict[str, Type[BaseModel]] = {
    "Parameters": ParametersModel, "Case_Library": CaseLibraryModel,
    "Revenue_Assumptions": RevenueAssumptionsModel, "Rev_Ramp_Seasonality": RevRampSeasonalityModel,
    "OPEX_Detail": OPEXDetailModel, "CAPEX_Schedule": CAPEXScheduleModel,
    "FX_Path": FXPathModel, "Working_Capital_Tax": WorkingCapitalTaxModel,
    "Financing_Stack_Scenarios": FinanceStackScenariosModel, "Finance_Stack": FinanceStackModel,
    "Investor_500k_Offer_Grid": Investor500kOfferGridModel,
}

def _nan_to_none(d: Dict[str, Any]) -> Dict[str, Any]:
    out = {}
    for k, v in d.items():
        if isinstance(v, float) and (pd.isna(v) or v is np.nan):
            out[k] = None
        else:
            out[k] = v
    return out

def load_and_validate_input_pack(file_path: Path) -> Dict[str, pd.DataFrame]:
    file_path = Path(file_path)
    if not file_path.exists():
        raise FileNotFoundError(f"Error: Input file not found at {file_path}")

    try:
        sheets: Dict[str, pd.DataFrame] = pd.read_excel(file_path, sheet_name=None, engine="openpyxl")
    except Exception as exc:
        raise RuntimeError(f"Failed to read Excel workbook at {file_path}: {exc}") from exc

    validation_errors: List[str] = []

    for sheet_name, df in sheets.items():
        if sheet_name == "Notes_for_Use":
            continue
        model = SHEET_MODEL_MAP.get(sheet_name)
        if model is None:
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
        raise ValueError(f"Input data validation failed with {len(validation_errors)} error(s):\n\n" + "\n".join(validation_errors))

    return sheets

def create_calendar(parameters_df: pd.DataFrame) -> pd.DataFrame:
    def _get_param(df: pd.DataFrame, key: str):
        sel = df.loc[df["Key"] == key, "Value"]
        if sel.empty:
            m = df.assign(_k=df["Key"].astype(str).str.strip().str.lower())
            sel = m.loc[m["_k"] == key.strip().lower(), "Value"]
        if sel.empty:
            raise KeyError(f"Missing parameter '{key}' in Parameters sheet")
        return sel.iloc[0]

    start_raw = _get_param(parameters_df, "START_DATE")
    horizon_raw = _get_param(parameters_df, "HORIZON_MONTHS")

    start_date = pd.to_datetime(start_raw)
    horizon_months = int(horizon_raw)

    rng = pd.date_range(start=start_date, periods=horizon_months, freq="ME")
    cal = pd.DataFrame({
        "Date": rng,
        "Year": rng.year,
        "Month": rng.month,
        "Month_Index": range(1, horizon_months + 1),
    })
    return cal
