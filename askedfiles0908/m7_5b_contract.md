# Module 7.5B — Rebuild Financial Statements with Junior Capital (Contract)

**Purpose.** Reclassify the frozen USD 500k junior capital at opening (Month 0/1) per selected option, subordinate junior payouts to senior debt, and output rebuilt statements in NAD and USD using m0 FX path.

## Inputs (must exist)

- `outputs/m7_selected_offer.json`
  - Required keys: `Option`, `Instrument`, optional terms: `RevShare_preRefi_pct`, `Min_IRR_Floor_pct`, `Conversion_Terms`, `Exit_Refi_Multiple`.
- `outputs/m7_5_junior_financing.parquet`
  - Required roles (synonyms allowed):
    - `MONTH_INDEX` → Month index column (`Month_Index`, `MONTH_INDEX`).
    - `JUNIOR_IN_NAD_000` → junior amount in NAD ‘000 (`Cash_In_NAD_000`, `Injection_NAD_000`, `Amount_NAD_000`, `NAD_000`).
    - Optional `JUNIOR_OUT_NAD_000` for prefilled payouts (rare).
- `outputs/m0_inputs/FX_Path.parquet`
  - Required roles:
    - `MONTH_INDEX`
    - `FX_USD_TO_NAD` (`USD_to_NAD`, `NAD_per_USD`, `FX_Rate`).
- Baselines:
  - `outputs/m2_pl_schedule.parquet` (roles: `Total_Revenue_NAD_000`, `Interest_Expense_NAD_000`, `PBT_NAD_000`, `Tax_Expense_NAD_000`, `NPAT_NAD_000`, `MONTH_INDEX`).
  - `outputs/m3_revolver_schedule.parquet` (optional; roles: `Interest_Paid_NAD_000`, `Principal_Paid_NAD_000`).
  - `outputs/m5_cash_flow_statement_final.parquet` (role: `CFO_NAD_000`).
  - `outputs/m6_balance_sheet.parquet` (roles: `Cash_NAD_000` (or `Cash_and_Cash_Equivalents_NAD_000`), `Assets_Total_NAD_000`, `Liabilities_And_Equity_Total_NAD_000`).

## Outputs

- `outputs/m7_5b_profit_and_loss.parquet`
  - Columns (NAD ‘000; plus `_USD_000` counterparts):
    - `Month_Index`, `Total_Revenue_NAD_000`, `EBIT_NAD_000` (from M2), `Interest_Expense_NAD_000` (baseline),
      **`Junior_Interest_Expense_NAD_000`**, `PBT_Rebuilt_NAD_000`,
      `Tax_Expense_Rebuilt_NAD_000`, `NPAT_Rebuilt_NAD_000`.
- `outputs/m7_5b_cash_flow.parquet`
  - Columns (NAD ‘000; plus `_USD_000`):
    - `Month_Index`, `CFO_NAD_000` (from M5),
      **`CFF_Junior_In_NAD_000`**, **`CFF_Junior_Out_NAD_000`**, `CFF_Junior_Net_NAD_000`,
      `Net_CF_Impact_From_Junior_NAD_000`.
- `outputs/m7_5b_balance_sheet.parquet`
  - Columns (NAD ‘000; plus `_USD_000`):
    - `Month_Index`, `Cash_NAD_000_Rebuilt`,
      **`Junior_Debt_EOP_NAD_000`**, **`Junior_Equity_Reclass_NAD_000`** (cumulative opening equity reclass),
      `Assets_Total_NAD_000_Rebuilt`, `Liabilities_And_Equity_Total_NAD_000_Rebuilt`.
  - **Identity:** `Assets_Total_NAD_000_Rebuilt == Liabilities_And_Equity_Total_NAD_000_Rebuilt` (within 1e‑6).
- `outputs/m7_5b_debug.json` — metadata: instrument, ETR used, FX source, subordination notes.
- `outputs/m7_5b_smoke_report.md` — short report for CI logs.

## Rules

- Opening USD 500k already sits in opening equity/cash; **M7.5B does not inject fresh cash**. It **reclassifies** opening equity → junior debt (for convertibles) or keeps as equity (SAFE / Pref).
- Junior payouts (rev‑share, dividends) are **subordinated**: we cap them by residual cash after CFO and senior‑debt service when that schedule is available, otherwise capped by CFO.
- USD views use `FX_Path.parquet` by **dividing** NAD by `USD_to_NAD` rate for each month.
