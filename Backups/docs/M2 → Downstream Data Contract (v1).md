# M2 → Downstream Data Contract (v1)

## Canonical artifacts (written by M2 to `.\outputs`)
1. `m2_pl_schedule.parquet`  
2. `m2_working_capital_schedule.parquet`

Per our re‑audit of module instructions, the WC artifact must carry the canonical name `m2_working_capital_schedule.parquet`. Aliases like `m2_working_capital.parquet` are **not** permitted in future packaging (tolerance only in M5 runner). :contentReference[oaicite:0]{index=0}

---

## Common requirements
- **Indexing**: one row per period (monthly).  
- **Period field**: either `Period` (preferred, string `YYYY‑MM`) or a pandas datetime column.  
- **Currency**: include a column named `Currency` with ISO code (e.g., `NAD`).  
- All monetary columns must be **period amounts** (not cumulative), except *_EOP balances in WC.

---

## 1) `m2_pl_schedule.parquet`

### Canonical fields (preferred names)
- `Period` (string `YYYY-MM`)  
- `Currency` (string)  
- `Revenue` (number)  
- `COGS` (number)  
- `Operating_Expenses` (number)  
- `Depreciation_and_Amortization` (number, may be 0 if pre‑capex)  
- `Income_Tax_Expense` (number, may be 0)  
- `Net_Profit_After_Tax` (number)

### Accepted synonyms (M5 runner maps automatically)
- `Revenue`: `Total_Revenue`, `Sales`
- `COGS`: `Cost_of_Goods_Sold`
- `Operating_Expenses`: `OPEX`, `Operating_Costs`
- `Depreciation_and_Amortization`: `Depreciation`, `D&A`
- `Income_Tax_Expense`: `Taxes`, `Tax_Expense`
- `Net_Profit_After_Tax`: `Net_Income`, `Profit_After_Tax`

> **Note:** If `Depreciation_and_Amortization` or `Net_Profit_After_Tax` are missing, M5 derives CFO using available line items and WC deltas (documented in Technical Notes below).

---

## 2) `m2_working_capital_schedule.parquet`

### Canonical fields (EOP = end of period balances)
- `Accounts_Receivable_EOP` (number)
- `Inventory_EOP` (number)
- `Accounts_Payable_EOP` (number)
- Optional: `Other_WC_EOP` (number)

### Accepted synonyms (M5 runner maps automatically)
- `Accounts_Receivable_EOP`: `AR_EOP`, any column containing `receiv`+`eop`
- `Inventory_EOP`: `Inv_EOP`, any column containing `invent`+`eop`
- `Accounts_Payable_EOP`: `AP_EOP`, any column containing `payable`+`eop`

> M5 uses **period‑to‑period deltas**:  
> `ΔWC = (ΔAR + ΔInventory − ΔAP [+ ΔOther])`.  
> Signs are handled to **reduce CFO when assets increase** and **increase CFO when liabilities increase**.

---

## Contract validation
A quick validator should assert:
- both parquet files exist
- have ≥ 1 row
- have `Period` and `Currency` (PL), and EOP columns (WC)
- the union of canonical names and synonyms is sufficient for M5’s mappings

See `tests\smoke\test_pipeline_smoke.py` for the minimal presence check.
