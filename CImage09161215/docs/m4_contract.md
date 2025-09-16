# M4 Data Contract — Tax Engine (inputs for M6)

**Artifacts (in `./outputs/`):**
- `m4_tax_schedule.parquet` (REQUIRED)
- `m4_tax_summary.parquet` (OPTIONAL)

**Units:** thousands of NAD (`*_NAD_000`).  
**Time:** monthly, aligned on `Month_Index`.

## Roles (what M6 needs)

| Role               | Purpose (M6)                              | Required? | Allowed Column Names (synonyms)                                 |
|--------------------|-------------------------------------------|-----------|------------------------------------------------------------------|
| `MONTH_INDEX`      | join key                                  | Yes       | `Month_Index`, `MONTH_INDEX`, `month_index`, `Period`, `Month`   |
| `TAX_EXPENSE`      | P&L bridge                                | Yes       | `Tax_Expense_NAD_000`, `Income_Tax_Expense_NAD_000`              |
| `TAX_PAID_CF`      | cash paid taxes (operating CF sub-check)  | Yes       | `Tax_Paid_NAD_000`, `Income_Tax_Paid_NAD_000`                    |
| `TAX_PAYABLE_BAL`  | balance sheet liability                    | Yes       | `Tax_Payable_Balance_NAD_000`, `Income_Tax_Payable_NAD_000`      |
| `ETR`              | effective tax rate (debug/analytics)       | Optional  | `Effective_Tax_Rate`, `ETR`                                      |

**Notes**
- `m4_tax_summary.parquet` can include yearly totals; M6 doesn’t consume it.
- M6 consumes the **schedule** roles above to set the tax liability line and to reconcile CFO sub-components.
