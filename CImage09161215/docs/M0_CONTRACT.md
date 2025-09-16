# M0 Contract (Consumed Artifacts Only)

**Scope:** Freeze only the three outputs actually consumed by downstream modules. All filenames and column names are **frozen**.

## 1) outputs/m0_inputs/FX_Path.parquet
**Columns (required):**
- Month_Index:int  (1..N, continuous)
- NAD_per_USD:float  (NAD per 1 USD)

**Derivation:** From Input Pack sheet `FX_Path` (rename Month→Month_Index; same values).
**Consumers:** M8.B1 (FX merge), USD twins anywhere Month_Index is the join key.

## 2) outputs/m0_calendar.parquet
**Columns (required):**
- Date:datetime (month-end)
- Year:int
- Month:int (1..12)
- Month_Index:int (1..N)

**Derivation:** `create_calendar(Parameters)` using `START_DATE` and `HORIZON_MONTHS`.

## 3) outputs/m0_opening_bs.parquet
**Columns (required):**
- Line_Item:str  (rows: `Cash`, `Opening Equity-like`)
- Value_NAD:float
- Notes:str

**Derivation:** `create_opening_balance_sheet(FX_Path)` (500,000 USD × Month==1 `NAD_per_USD`).

## Acceptance (CI)
1. FX: exact numeric equality to Input Pack sheet after Month→Month_Index mapping.
2. Calendar: exact equality to calendar derived from Parameters.
3. Opening: exact equality to `create_opening_balance_sheet(FX_Path)`.

If any check fails, CI must stop with a non-zero exit code.
