# M3 Data Contract — Financing Engine (inputs for M6)

**Artifacts (in `./outputs/`):**
- `m3_finance_index.parquet` (REQUIRED) – canonical monthly totals for financing.
- `m3_revolver_schedule.parquet` (OPTIONAL) – if revolver exists; zeros otherwise.
- `m3_insurance_schedule.parquet` (OPTIONAL) – non-B/S critical; retained for completeness.

**Units:** amounts expressed in thousands of NAD (`*_NAD_000`).  
**Time:** monthly, aligned on `Month_Index` (1..N).

## Roles (what M6 needs)

| Role                         | Purpose (M6)                         | Required? | Allowed Column Names (synonyms)                                        |
|-----------------------------|--------------------------------------|-----------|-------------------------------------------------------------------------|
| `MONTH_INDEX`               | join key across modules              | Yes       | `Month_Index`, `MONTH_INDEX`, `month_index`, `Period`, `Month`          |
| `DEBT_OUT_PRINCIPAL`        | total debt closing balance (liab)    | Yes       | `Debt_Principal_Closing_NAD_000`, `Outstanding_Principal_NAD_000`, `Principal_Outstanding_NAD_000` |
| `PRINCIPAL_DRAWS_CF`        | CF from debt draws (fin CF)          | Yes       | `Principal_Draws_CF_NAD_000`, `Debt_Draws_NAD_000`                      |
| `PRINCIPAL_REPAY_CF`        | CF from principal repayments (fin CF)| Yes       | `Principal_Repayments_CF_NAD_000`, `Debt_Repayments_NAD_000`            |
| `INTEREST_EXPENSE`          | P&L bridge / optional M6 check       | Optional  | `Interest_Expense_NAD_000`, `Interest_NAD_000`                          |
| `INTEREST_PAID_CF`          | cash paid interest (fin CF)          | Optional  | `Interest_Paid_NAD_000`, `Interest_Cash_Outflow_NAD_000`                |
| `EQUITY_CASH_IN`            | CF from equity issues                | Optional  | `Equity_Issued_Cash_NAD_000`, `Equity_Cash_In_NAD_000`                  |

**Notes**
- If an instrument type doesn’t exist (e.g., no revolver), the artifact still exists with zeros or the totals are reflected in `m3_finance_index.parquet`.
- M6 will only **read** the roles above; additional columns are allowed and ignored.
