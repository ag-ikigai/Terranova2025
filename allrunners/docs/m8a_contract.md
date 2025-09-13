# M8.A — Super‑Verifier & Super‑Smoke (M0..M7.5B)

**Purpose.** Provide an institutional‑grade gate that the integrated statements obey IAS 1 integrity and the project finance waterfall logic:
- BS identity holds monthly: **Assets = Liabilities + Equity** (NAD '000).
- CF cash identity holds monthly: **Opening + CFO + CFI + CFF = Closing** (NAD '000).
- CF closing cash **links exactly** to BS cash if both are present.
- FX translations follow the policy used in M7.5B: **AVG FX for PL/CF** and **EOM FX for BS** (USD = NAD / FX).  
- Junior **subordination**: junior outflows must not reduce closing cash below buffer; priority belongs to senior service and tax.
- **Crop area sanity**: Input Pack Revenue_Assumptions sum to **65 ha** total.

**Inputs (read‑only)**:  
`outputs/m7_5b_profit_and_loss.parquet`, `outputs/m7_5b_cash_flow.parquet`, `outputs/m7_5b_balance_sheet.parquet`, `outputs/m0_inputs/FX_Path.parquet` (or `outputs/FX_Path.parquet`), optional `outputs/m5_cash_flow_statement_final.parquet`, `outputs/m6_balance_sheet.parquet`, `outputs/m4_tax_schedule.parquet`, `outputs/m7_selected_offer.json`, `outputs/m7_5_junior_financing.parquet`, and optional `InputPack/TerraNova_Input_Pack_v10_0.xlsx` (for the 65 ha check).

**Outputs**:  
`outputs/m8a_super_verifier_report.md`, `outputs/m8a_super_verifier.json`.

**Standards & references.** IAS 1 requires a complete set of financial statements and prescribes cash flow presentation (IAS 1. Objective; Structure & Content; .111). FX translation policy and junior treatment aligned with M7.5B contract and subordination memo.  
