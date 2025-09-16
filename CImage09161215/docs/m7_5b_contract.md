# Module 7.5B — Rebuild & IFRS Aggregator
**Artifact Contract – v1.2 (2025‑09‑11)**

## Purpose
M7.5B rebuilds the Month‑7.5B financials by:
1) Rolling closing cash in CF and linking it to BS cash,
2) Applying the subordination gate per the frozen instrument choice,
3) Adding an **IFRS aggregator** that emits EBITDA, Total OPEX and IAS‑1 current‑classification fields,
4) (If FX is available) creating **USD twins** for *_NAD_000 columns.

> All financial calculations end by M8.Bn; M9 is display/notes only.

---

## Inputs (files in `outputs/` unless otherwise noted)

| Artifact | Required? | Key columns (synonyms accepted) | Notes |
|---|---|---|---|
| **M5 cash flow**: `m5_cash_flow_statement_final.parquet` | Yes | `Month_Index`; `CFO_NAD_000`, `CFI_NAD_000`, `CFF_NAD_000` | CFO/CFI/CFF consumed as‑is. |
| **M2 working capital**: `m2_working_capital_schedule.parquet` | Yes | `Month_Index`; `AR_Balance_NAD_000` (or `AR_*`), `Inventory_Balance_NAD_000` (or `Inventory_*`), `AP_Balance_NAD_000` (or `AP_*`) | Used to build Current Assets/Liabilities. |
| **M3 revolver**: `m3_revolver_schedule.parquet` | Yes | `Month_Index`; `Closing_Balance` (or `Revolver_*_Balance`) | If a **current portion** column is missing, we treat `Closing_Balance` as current (short‑term revolver policy). |
| **M0 opening BS**: `m0_opening_bs.parquet` | Yes | Must contain row with `Line_Item='Cash'` and a numeric value | Path is `outputs\m0_opening_bs.parquet` (not `outputs\m0_inputs`). |
| **Freeze selection**: `m7_selected_offer.json` | No | `option`, `instrument`, `classification` | If absent, we default via a shim to `option="A_SAFE"`, `instrument="SAFE"`, `classification="equity_like"` and log it. |
| **FX curve (preferred)**: `m8b_fx_curve.parquet` | No | `Month_Index`, `NAD_per_USD` | If present, used to emit USD twins. |
| **FX path (fallback)**: `FX_Path.parquet` under `outputs\m0_inputs\` or directly under `outputs\` | No | `Month_Index` plus one numeric FX column among: `USD_to_NAD`, `USD_NAD`, `FX_USD_NAD`, `USDtoNAD`, `NAD_per_USD`, `Rate_USD_to_NAD` | Path/column resolution matches runner logic. :contentReference[oaicite:0]{index=0} |

**Conventions**
- Periodicity: **Monthly**; `Month_Index` is 1‑based, contiguous.
- Units: all *_NAD_000 / *_USD_000 are in **thousands**.
- Currency: **NAD** primary; **USD** derived as `USD = NAD / FX` (FX = NAD per USD).

---

## Outputs (all under `outputs\`)

### 1) `m7_5b_cash_flow.parquet`
Required columns:
- `Month_Index`
- `CFO_NAD_000`, `CFI_NAD_000`, `CFF_NAD_000`
- `Closing_Cash_NAD_000` (exactly equals BS `Cash_and_Cash_Equivalents_NAD_000`)

Optional columns (emitted when available):
- `CFF_JUNIOR_NAD_000` (junior net cash flow after subordination gate)

**FX twins (if FX present):** `CFO_USD_000`, `CFI_USD_000`, `CFF_USD_000`, `Closing_Cash_USD_000`, etc.

### 2) `m7_5b_profit_and_loss.parquet`
Required (post‑IFRS aggregator):
- `Month_Index`
- `EBITDA_NAD_000`  
- `Total_OPEX_NAD_000`  *(= `Fixed_OPEX + Variable_OPEX` when both exist; else IFRS fallback, see explainer)*

Optional (carried through if present in upstream):
- `Total_Revenue_NAD_000`, `Depreciation_NAD_000`, `EBIT_NAD_000`, `Interest_Expense_NAD_000`, `Tax_Expense_NAD_000`, `NPAT_NAD_000`, etc.

**FX twins (if FX present):** `EBITDA_USD_000`, `Total_OPEX_USD_000`, and USD copies of any *_NAD_000 present.

### 3) `m7_5b_balance_sheet.parquet`
Required (post‑IFRS aggregator):
- `Month_Index`
- `Cash_and_Cash_Equivalents_NAD_000`  *(rolled from CF closing cash — exact link)*
- `Current_Assets_NAD_000`
- `Current_Liabilities_NAD_000`
- `Total_Assets_NAD_000`
- `Liabilities_and_Equity_Total_NAD_000`

**How CA/CL are built (IAS 1 basis)**
- `Current_Assets_NAD_000` = Cash (from CF→BS link) + A/R + Inventory (+ Prepaids/other current if modeled later).  
- `Current_Liabilities_NAD_000` = A/P + **Revolver current portion**.  
  - If a revolver current column is missing, we **treat `Closing_Balance` as current** (short‑term revolver policy, logged).

**FX twins (if FX present):** USD versions for all *_NAD_000.

### 4) Diagnostics
- `m7_5b_debug.json` — complete provenance: which inputs/columns were picked, subordination totals, IFRS aggregator decisions, FX path & FX column used, USD twin count, and any warnings that were downgraded to info after resolution.
- `m7_5b_smoke_report.md` — human‑readable summary and “quick checks”.

---

## Selection & Subordination Gate
- If `m7_selected_offer.json` is missing or malformed, the **shim** defaults to the SAFE as equity‑like and logs the fallback choice.
- The **hard subordination** gate enforces that junior cash cannot drive closing cash negative (buffer configurable). Any junior draws/injections are logged in `m7_5b_debug.json`.

---

## Warnings & Failure Modes
- **Revolver current portion not found** → we log a warning and use `Closing_Balance` as current (policy).
- **BS totals missing** → aggregator computes totals; if insufficient inputs, we log and keep totals absent (does not break M8.Bn).
- **FX**: if no FX is found, we **emit NAD only** and log the absence (M8.Bn must tolerate absence of USD columns).
- **Schema mismatches**: missing core roles (e.g., CFO/CFI/CFF) raise in strict mode.

---

## Quick Smoke (PowerShell / one‑liner)
```powershell
$env:PYTHONPATH="C:\TerraNova\src"; .\.venv\Scripts\python.exe -c "import pandas as pd, pathlib; p=pathlib.Path(r'.\outputs'); cf=pd.read_parquet(p/'m7_5b_cash_flow.parquet'); pl=pd.read_parquet(p/'m7_5b_profit_and_loss.parquet'); bs=pd.read_parquet(p/'m7_5b_balance_sheet.parquet'); cols=lambda d,n:[c for c in n if c in d.columns]; print('[CF]',cols(cf,['Month_Index','Closing_Cash_NAD_000','CFO_NAD_000','CFI_NAD_000','CFF_NAD_000','Closing_Cash_USD_000','CFO_USD_000'])); print('[PL]',cols(pl,['Month_Index','EBITDA_NAD_000','Total_OPEX_NAD_000','EBITDA_USD_000'])); print('[BS]',cols(bs,['Month_Index','Cash_and_Cash_Equivalents_NAD_000','Current_Assets_NAD_000','Current_Liabilities_NAD_000','Total_Assets_NAD_000','Liabilities_and_Equity_Total_NAD_000','Current_Assets_USD_000','Current_Liabilities_USD_000']))"
