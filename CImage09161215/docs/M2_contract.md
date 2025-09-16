# Module 2 — Working Capital & P&L Stub (Contract) • v1

## Artifacts (filenames)
- `outputs/m2_working_capital_schedule.parquet` (required)
- One of:
  - `outputs/m2_profit_and_loss_stub.parquet` (preferred), or
  - `outputs/m2_pl_schedule.parquet` (accepted)

## Keys
- `Month_Index` (int) — primary monthly join key, continuous 1..N.

## Working Capital schema — accepted flavors

**A) Stocks flavor (preferred by BS/M7.5B)**
- Required columns (any subset ≥ 1 is allowed, but at least one of these families must be present overall):
  - `AR_Balance_NAD_000`
  - `Inventory_Balance_NAD_000`
  - `AP_Balance_NAD_000`

**B) Flows flavor (accepted by M5 via mapping)**
- Any of:
  - `Change_in_Receivables_NAD_000`
  - `Change_in_Inventory_NAD_000`
  - `Change_in_Payables_NAD_000`
  - `Cash_Flow_from_NWC_Change_NAD_000`

**CI rule:** at least one **flavor** (A or B) must exist; `Month_Index` must exist; row count > 0.

## P&L stub (tolerant)
- Must include `Month_Index`.
- Presence of typical lines is preferred (e.g., `Total_Revenue_NAD_000`, `EBITDA_NAD_000`, `Total_OPEX_NAD_000`), but CI allows variants; M5 contains tolerant mapping.

## Invariants
- Monetary suffixes in thousands: `_NAD_000` (USD twins are added downstream).
- 60-month horizon (current project default).

## Provenance
- WC policies from Input Pack sheet `Working_Capital_Tax` (DSO, DIO, DPO, COGS%).  
- Revenue cadence from M1.  

