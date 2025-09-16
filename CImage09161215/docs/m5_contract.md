# Module 5 (M5) Artifact Contract – v1

**Purpose.** M5 computes **Cash Flow from Operations (CFO)** from M2 outputs and emits a monthly cash‑flow schedule used by M6 (Balance Sheet).

---

## Inputs (from M2, required)

Location: `./outputs/`

### A. P&L schedule (required)
- **Canonical filename (preferred):** `m2_pl_schedule.parquet`
- **Accepted legacy name (tolerated):** `m2_profit_and_loss_schedule.parquet`

**Required roles (column synonyms)**  
M5 must be able to map these roles from the P&L file. Column names may be any of the synonyms below:

| Role         | Synonyms accepted (case‑sensitive)                                     | Units |
|--------------|-------------------------------------------------------------------------|-------|
| `MONTH_INDEX`| `Month_Index`, `MONTH_INDEX`                                            | index |
| `NPAT`       | `NPAT_NAD_000`, `Net_Profit_After_Tax_NAD_000`, `NPAT`, `Net_Profit_After_Tax` | NAD’000 |
| `DA`         | `Depreciation_NAD_000`, `Depreciation_and_Amortization_NAD_000`, `Depreciation`, `Depreciation_and_Amortization`, `DepreciationAmortization`, `DandA`, `DA` | NAD’000 |

### B. Working Capital schedule (required)
- **Canonical filename (preferred):** `m2_working_capital_schedule.parquet`
- **Accepted legacy name (tolerated):** `m2_working_capital_pl.parquet`

**Required roles (column synonyms)**

| Role         | Synonyms accepted (case‑sensitive)                                                                 | Units |
|--------------|-----------------------------------------------------------------------------------------------------|-------|
| `MONTH_INDEX`| `Month_Index`, `MONTH_INDEX`                                                                        | index |
| `NWC_CF`     | `Cash_Flow_from_NWC_Change_NAD_000`, `Net_Working_Capital_CF_NAD_000`, `Working_Capital_CF_NAD_000`, `WC_Cash_Flow_NAD_000`, `Cash_Flow_from_NWC_Change`, `Net_Working_Capital_CF`, `Working_Capital_CF`, `WC_Cash_Flow` | NAD’000 |

**Typing & alignment**
- `MONTH_INDEX` is integer and **must** align across both files (same number of rows and month values).
- Monetary columns are numeric; **units are thousands of NAD** (NAD’000).

---

## Outputs (emitted by M5)

Location: `./outputs/`

1) **Parquet:** `m5_cash_flow_statement_final.parquet` (required)  
   **Required columns**
   - `Month_Index` (int)
   - `NPAT_NAD_000` (float)
   - `Depreciation_NAD_000` (float)  ← mapped from `DA`
   - `Cash_Flow_from_NWC_Change_NAD_000` (float)  ← mapped from `NWC_CF` (sign already consistent with cash flow)
   - `CFO_NAD_000` (float) = `NPAT_NAD_000 + Depreciation_NAD_000 + Cash_Flow_from_NWC_Change_NAD_000`

   **Invariants**
   - No NaNs in required columns (M5 fills missing with zeros and logs it).
   - `CFO_NAD_000` **reconciles** within 1e‑6 to the sum of components per month.
   - Row count and `Month_Index` equal to inputs’ `Month_Index`.

2) **Markdown:** `m5_smoke_report.md` (required)  
   - Brief pass/fail notes and basic stats.

3) **JSON (debug):** `m5_debug_dump.json` (optional)  
   - Machine‑readable trace: resolved input paths, resolved role→column mapping, counts.

---

## Breaking‑change policy

- If a **new synonym** is introduced upstream (M2), **runner must be updated** to recognize it **and** this contract must be amended.
- **Filenames** are canonical above. Legacy names are tolerated **for backward compatibility**; new drops should adopt canonical names.

---

## Quick validation (human)

- Ensure `./outputs/m2_pl_schedule.parquet` & `./outputs/m2_working_capital_schedule.parquet` exist.
- Ensure `./outputs/m5_cash_flow_statement_final.parquet` exists and includes the five required columns.
- Confirm `CFO_NAD_000` equals `NPAT + DA + NWC_CF` for a few months spot‑checks.
