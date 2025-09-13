# Module 2 (M2) Artifact Contract – v1.0 (NAD, thousands)

**Granularity:** Monthly, one row per period.  
**Units:** NAD in thousands (`_NAD_000`).  
**Currency:** controlled by CLI flag (e.g., `--currency NAD`).  
**Date/Index:** `Month_Index` is a positive, strictly increasing integer (1..N).

---

## 1) Profit & Loss schedule (primary)  
**Canonical file:** `outputs/m2_pl_schedule.parquet`  
**Accepted alternate:** `outputs/m2_pl_statement.parquet`

**Required columns (exact names):**
- `Month_Index` *(int)*
- `Revenue_NAD_000` *(float)*
- `COGS_NAD_000` *(float)*
- `Gross_Profit_NAD_000` *(float)*
- `Opex_NAD_000` *(float)*
- `EBITDA_NAD_000` *(float)*
- `Depreciation_and_Amortization_NAD_000` *(float)*
- `EBIT_NAD_000` *(float)*
- `Interest_Expense_NAD_000` *(float)*
- `Pre_Tax_Profit_NAD_000` *(float)*
- `Tax_Expense_NAD_000` *(float)*
- `Net_Profit_After_Tax_NAD_000` *(float)*

**Sign conventions:**  
All lines are reported with **positive magnitudes** in their economic direction. i.e., `COGS_NAD_000`, `Opex_NAD_000`, `Interest_Expense_NAD_000`, `Tax_Expense_NAD_000` are **positive** expense amounts; totals (e.g., `EBITDA_NAD_000`) are **positive** if profitable.  
`Net_Profit_After_Tax_NAD_000` (NPAT) may be negative in loss months.

---

## 2) Working capital schedule  
**Canonical file:** `outputs/m2_working_capital_schedule.parquet`  
**Accepted alternate:** `outputs/m2_working_capital.parquet`

**Required columns (exact names):**
- `Month_Index` *(int)*
- `AR_Balance_NAD_000` *(float)*
- `Inventory_Balance_NAD_000` *(float)*
- `AP_Balance_NAD_000` *(float)*
- `NWC_Balance_NAD_000` *(float)*
- `Cash_Flow_from_NWC_Change_NAD_000` *(float)*

**Sign conventions:**  
`Cash_Flow_from_NWC_Change_NAD_000` is **positive for cash inflow**, **negative for cash outflow** (e.g., inventory build is usually negative cash flow).

---

## 3) Compatibility guarantees for M5
M5 computes:
- `NPAT_NAD_000` ← `Net_Profit_After_Tax_NAD_000`
- `DandA_NAD_000` ← `Depreciation_and_Amortization_NAD_000`
- `NWC_CF_NAD_000` ← `Cash_Flow_from_NWC_Change_NAD_000`
- `CFO_NAD_000` = `NPAT_NAD_000` + `DandA_NAD_000` + `NWC_CF_NAD_000`

Any schema change requires updating this contract and the M5 mapper+tests.
