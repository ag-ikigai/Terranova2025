
---

## 📄 `modules/m7_5b_rebuild/m7_5b_ifrs_explainer.md` (updated)

```markdown
# IFRS Explainer — What M7.5B Emits and Why (v1.2)

This note explains how the IFRS aggregator in M7.5B derives fields to support **IAS 1** presentation and **IAS 7** cash‑flow classification, using only data already produced upstream (no new inputs).

---

## 1) P&L Additions

### EBITDA_NAD_000
Priority of sources:
1. Use `EBITDA_NAD_000` if provided upstream (rare in our stack).
2. Else compute **EBIT + Depreciation** where both are available.
3. If EBIT is missing, fallback to **NPAT + Tax_Expense + Interest_Expense + Depreciation**.  
All decisions are logged in `m7_5b_debug.json`.

### Total_OPEX_NAD_000
Priority of sources:
1. If both `Fixed_OPEX_NAD_000` and `Variable_OPEX_NAD_000` exist → **Total_OPEX = Fixed + Variable**.
2. Else if `Total_OPEX_NAD_000` exists upstream → carry it.
3. Else IFRS‑consistent fallback:  
   `Total_OPEX ≈ Revenue - (EBIT + Depreciation + Interest + Tax + other below‑EBIT items not modeled)`  
   (We consciously avoid including finance costs and taxes in OPEX.)

**USD twins:** If FX is present, `_USD_000` copies are created for all *_NAD_000 columns by dividing by the monthly **NAD per USD** rate.

---

## 2) Balance Sheet Additions (IAS 1 current/non‑current)

### Cash_and_Cash_Equivalents_NAD_000
Exactly equals CF `Closing_Cash_NAD_000` (one‑to‑one audit trail).

### Current_Assets_NAD_000
`Cash_and_Cash_Equivalents + Accounts_Receivable + Inventory (+ Prepaids/other current if modeled later).`

### Current_Liabilities_NAD_000
`Accounts_Payable + Revolver current portion (+ other current accruals if modeled later).`

> **Revolver:** if an explicit **current portion** is not provided by M3, we treat the `Closing_Balance` of the revolver as a current liability (short‑term revolver assumption). This is a policy choice that is disclosed and logged.

### Totals
- `Total_Assets_NAD_000` and `Liabilities_and_Equity_Total_NAD_000` are carried from upstream when present.  
- If missing, M7.5B will compute totals where possible; if inputs are insufficient, totals may be left absent (but CA/CL are still emitted for M8.Bn).

**USD twins:** USD versions of all *_NAD_000 fields are emitted when FX is available.

---

## 3) Cash‑Flow Classification (IAS 7)
- **CFO** includes cash generated from operations and working‑capital effects.
- **CFI** includes CAPEX and other investing flows.
- **CFF** includes debt/equity proceeds and repayments, excluding interest by design (interest is mapped to P&L finance costs and captured in CFO via our upstream conventions).
- `Closing_Cash_NAD_000` ties to BS cash; the runner validates the link each run.

---

## 4) Disclosures & Policies Logged
`m7_5b_debug.json` captures:
- Which EBITDA/OPEX formula branch was chosen.
- How CA/CL were assembled (which WC columns used).
- Revolver current‑portion policy branch.
- FX: path examined, FX column name chosen, number of USD twins generated.
- Subordination gate settings and any junior cash injections blocked.

---

## 5) Known Limitations & Future Hooks
- Biological assets (IAS 41) and crop‑level inventories are not broken out yet; when modeled, they can be slotted into current/non‑current buckets without changing M8/M9 interfaces.
- If the project adds **prepaids**, **accruals**, or **short‑term portions** of long‑term debt, M7.5B will include them in CA/CL as soon as upstream schedules emit the corresponding columns.

