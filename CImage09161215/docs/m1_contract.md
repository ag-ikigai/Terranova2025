# M1 Contract – Operational Engines (v1, frozen)

**Artifacts (outputs):**
- `outputs/m1_revenue_schedule.parquet`
- `outputs/m1_opex_schedule.parquet`
- `outputs/m1_capex_schedule.parquet`
- `outputs/m1_depreciation_schedule.parquet`
- `outputs/m1_smoke_report.md` (aux)
- `outputs/m1_debug.json` (aux)

**Required columns:**
- m1_revenue_schedule.parquet:
  - `Month_Index:int`, `Crop:str` (may be present), `Monthly_Revenue_NAD_000:float`
- m1_opex_schedule.parquet:
  - `Month_Index:int`, `Monthly_OPEX_NAD_000:float`
- m1_capex_schedule.parquet:
  - `Month_Index:int`, `Monthly_CAPEX_NAD_000:float`
- m1_depreciation_schedule.parquet:
  - `Month_Index:int`, `Monthly_Depreciation_NAD_000:float`

**Invariants:**
- Month_Index is 1..HORIZON_MONTHS (Parameters sheet).
- Revenue Year‑1 rule: Months 1–6 = 0; Months 7–12 carry full Y1 ramp (re-weighted).
- OPEX_Detail (Y1..Y5) reconciles to the monthly OPEX sums by year.
- CAPEX monthly sum equals Input Pack CAPEX total.
- Depreciation is straight‑line, life > 0 only; non‑negative.

**Units:**
All monetary outputs in NAD thousands (`*_NAD_000`), consistent with downstream M2/M5/M7.5B.  
Do not rename these columns. (See global “do not rename” list.) 
