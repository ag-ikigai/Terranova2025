# M3 Contract (Financing Engine) – src rebuild v1

**Artifacts**
- `outputs/m3_revolver_schedule.parquet`
- `outputs/m3_finance_index.parquet`
- `outputs/m3_insurance_schedule.parquet` (placeholder)
- `outputs/m3_smoke_report.md`

**Required columns – m3_revolver_schedule.parquet**
- `Month_Index`
- `Revolver_Open_Balance_NAD_000`
- `Revolver_Draw_NAD_000`
- `Revolver_Repayment_NAD_000`
- `Revolver_Close_Balance_NAD_000`
- `Revolver_Interest_Expense_NAD_000`

**Identities**
- `Close = Open + Draw − Repayment` per month.
- Interest is calculated on opening balance at a monthly rate discovered from `m0_inputs/Finance_Stack.parquet` or defaults to 12% APR (1% monthly).

**Semantics**
- Currency = NAD thousands.
- Month_Index is continuous and matches M0 calendar.

**Versioning**
- Column names are frozen. Any change must update this doc and all downstream readers.