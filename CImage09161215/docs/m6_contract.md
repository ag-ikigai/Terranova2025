# Module 6 (M6) – Balance Sheet Contract (v1, beta)

**Purpose.** Produce a monthly balance sheet consistent with M2–M5 outputs. In v1, **Cash** is a *balancing item*; after “7.5 Wiring” (CAPEX & Equity), Cash will be the true closing balance.

## Inputs (discovered in `./outputs/`)
- **M2 P&L:** `m2_pl_schedule.parquet`
  - Roles: `MONTH_INDEX`, `NPAT` (e.g., `NPAT_NAD_000`)
- **M2 Working Capital schedule:** `m2_working_capital_schedule.parquet`
  - Role: `NWC_CF` (e.g., `Cash_Flow_from_NWC_Change_NAD_000`)  
  - Convention: Positive CF = release (NWC decreases)
- **M3 Debt schedule:** `m3_revolver_schedule.parquet`
  - Role: `DEBT_OUT` (e.g., `Outstanding_Balance_NAD_000`)
- **M4 Tax schedule:** `m4_tax_schedule.parquet`
  - Roles: prefer `TAX_PAYABLE`; else derive from `TAX_EXPENSE` and `TAX_PAID`

## Output
`m6_balance_sheet.parquet` with columns (NAD ‘000):
- `Month_Index` (int)
- `Currency` (str, e.g., "NAD")
- `Cash_Balancing_Item_NAD_000`
- `NWC_Asset_NAD_000`
- `NWC_Liability_NAD_000`
- `Debt_Outstanding_NAD_000`
- `Tax_Payable_NAD_000`
- `Equity_Share_Capital_NAD_000` (0.0 in v1)
- `Equity_Retained_Earnings_NAD_000` (cum(NPAT))
- `Equity_Total_NAD_000`
- `Liabilities_Total_NAD_000`
- `Assets_Total_NAD_000`
- `Liabilities_And_Equity_Total_NAD_000`

**Identity:** `Assets_Total == Liabilities_And_Equity_Total` (within 1e-6).

## Evolution (v7.5)
- Replace `Cash_Balancing_Item` with true `Closing_Cash` once CAPEX (CFI) and Equity/Financing (CFF) are wired.
- Add `PPE_Gross`, `Accum_Dep`, `PPE_Net`, and explicit Equity injections if/when scheduled in M3.

### Joins & alignment
- All inputs and the M6 output use `Month_Index` as a 1..N continuous index aligned to M2/M3/M4. Gaps are an error.

### Units & sign conventions
- All monetary fields are NAD in thousands (`*_NAD_000`).
- `Equity_Share_Capital_NAD_000` is 0.0 in v1; will be populated after “7.5 Wiring”.
- `Equity_Retained_Earnings_NAD_000 = cumulative sum of NPAT` from M2.

### Identity & tolerance
- `Assets_Total_NAD_000 == Liabilities_And_Equity_Total_NAD_000` must hold within `1e-6`. Any row violating this is a hard error.
- In v1, `Cash_Balancing_Item_NAD_000` is derived to enforce the identity. After 7.5, this will be replaced with the true `Closing_Cash_NAD_000`.

### Forward-compatibility (v7.5)
- When CFI/CFF and equity injections are wired, M6 will consume `Net_Change_in_Cash`/`Closing_Cash` and add PPE (gross/accum/net). The column names will be frozen in the M6 contract v2.
