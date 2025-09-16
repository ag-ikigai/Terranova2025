# Module 7 (R1) — Investor Offer Ranking — Contract v1

**Purpose.** Rank a fixed set of investor offers (from InputPack sheet `Investor_500k_Offer_Grid`) using a deterministic, explainable scoring model. Emit a sortable table and a debug JSON. Human then freezes the chosen offer via `m7_selected_offer.json` (outside this module).

## Inputs (read-only)
- Excel: `InputPack/TerraNova_Input_Pack_v10_0.xlsx`, sheet `Investor_500k_Offer_Grid`.
  Required columns (case-sensitive):
  - `Option` (string, unique code, e.g., `A_SAFE`)
  - `Instrument` (string)
  - `Ticket_USD` (number)
  - `Valuation_Cap_NAD` (number or NA)
  - `Discount_pct` (number or NA)
  - `RevShare_preRefi_pct` (number or NA)
  - `Min_IRR_Floor_pct` (number or NA)
  - `Conversion_Terms` (string, free text)
  - `Exit_Refi_Multiple` (number or NA)

## Artifacts (in `outputs/`)
- **Table** `m7_r1_scores.parquet` + `m7_r1_scores.csv`
  Columns (exact):
  - `Rank` (int, 1 is best)
  - `Option`, `Instrument`
  - `Ticket_USD`
  - `Valuation_Cap_NAD`, `Discount_pct`, `RevShare_preRefi_pct`, `Min_IRR_Floor_pct`, `Exit_Refi_Multiple`
  - `Score_Valuation`, `Score_Discount`, `Score_RevShare`, `Score_PrefIRR`, `Score_ExitMultiple`
  - `Total_Score_0_100` (float)
  - `Selected` (bool; True for the top row)
- **Debug** `m7_r1_debug.json`
  - includes: `weights` (per-dimension weights), `normalized_fields` (min-max scales), `selected_option`
- **Report** `m7_r1_smoke_report.md` (human-readable snapshot)

## Roles (downstream)
- `M7_SELECTED_OPTION`: resolved via `m7_selected_offer.json` (created by the human freezer)
  - schema: `{ Option, Instrument, Rank, Total_Score_0_100, Ticket_USD, ... }`

## Validation Rules
- `m7_r1_scores.parquet` exists, non-empty, has all required columns.
- Exactly one row has `Selected == True`.
- `m7_r1_debug.json` exists and contains `weights` and `selected_option`.

