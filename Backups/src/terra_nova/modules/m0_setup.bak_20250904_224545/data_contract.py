
# The definitive data_contract.py for InputPack_v10.xlsx
from typing import Optional
from pydantic import BaseModel, Field

class ParametersModel(BaseModel):
    Key: str
    Value: str | int | float

class CaseLibraryModel(BaseModel):
    Case_Name: str
    Price_Mult: float
    Yield_Mult: float
    OPEX_Mult: float
    Ramp_Y1: float
    Ramp_Y2: float
    Ramp_Y3: float

class RevenueAssumptionsModel(BaseModel):
    Crop: str
    Hectares: int
    Yield_t_ha: float
    Price_NAD_per_kg: float
    Price_NAD_per_t: float
    Cycles_per_year: int
    Planting_Window_mm: str
    Harvest_Window_mm: str

class RevRampSeasonalityModel(BaseModel):
    Crop: str
    Y1_Ramp: float
    Y2_Ramp: float
    Y3_Ramp: float
    M1: float; M2: float; M3: float; M4: float; M5: float; M6: float
    M7: float; M8: float; M9: float; M10: float; M11: float; M12: float

class OPEXDetailModel(BaseModel):
    Category: str
    Y1: float; Y2: float; Y3: float; Y4: float; Y5: float

class CAPEXScheduleModel(BaseModel):
    Item: str
    Month: int = Field(ge=1)
    Amount_NAD_000: float
    Class: str
    Depreciation_Life_Yrs: int

class FXPathModel(BaseModel):
    Month: int = Field(ge=1)
    NAD_per_USD: float

class WorkingCapitalTaxModel(BaseModel):
    Parameter: str
    Value: str | int | float

class FinanceStackScenariosModel(BaseModel):
    Case: str
    Total_NAD_000: int
    Equity_NAD_000: int
    Debt_NAD_000: int
    Grants_NAD_000: int
    RevShare_preRefi_pct: float
    Refi_Month: int
    Target_DSCR_at_Refi: float

class FinanceStackModel(BaseModel):
    Case_Name: str
    Line_ID: int
    Instrument: str
    Currency: str
    Principal: int
    Rate_Pct: float
    Tenor_Months: int
    Draw_Start_M: int
    Draw_End_M: int
    Grace_Int_M: int
    Grace_Principal_M: int
    Amort_Type: str
    Balloon_Pct: float
    Revolving: int
    Is_Insurance: int
    Premium_Rate_Pct: float
    Secured_By: str
    Active: int

class Investor500kOfferGridModel(BaseModel):
    Option: str
    Instrument: str
    Ticket_USD: int
    Valuation_Cap_NAD: Optional[float] = None
    Discount_pct: Optional[float] = None
    RevShare_preRefi_pct: Optional[float] = None
    Min_IRR_Floor_pct: Optional[float] = None
    Conversion_Terms: Optional[str] = None
    Exit_Refi_Multiple: float
