M0-M9_Dependency_Map.md


## 1) End‑to‑end dependency (DAG) — current repo

```
Input Pack (xlsx)
   └─ M0  → m0_opening_bs.parquet
         → m0_inputs/FX_Path.parquet
          \
           ├─ M1  → m1_revenue_schedule.parquet   (currently EMPTY)
           └─ M2  → m2_pl_schedule.parquet
                 → m2_working_capital_schedule.parquet
                    |
                    ├─ M3 (baseline)  → financing schedules (used by M5)
                    ├─ M4 (baseline)  → tax schedules (used by M5)
                    └─ M5  → m5_cash_flow_statement_final.parquet
                          \
                           └─ M6  → m6_balance_sheet.parquet
                                  \
                                   └─ M7.R1 (ranker)  → m7_r1_scores.csv/.parquet
                                        └─ M7.Human (freezer) → m7_selected_offer.json
                                              \
                                               └─ M7.5B (rebuild) → m7_5b_profit_and_loss.parquet
                                                                  → m7_5b_cash_flow.parquet
                                                                  → m7_5b_balance_sheet.parquet
                                                                  → m8b_fx_curve.parquet (FX normalized for M8)
                                                                        \
                                                                         ├─ M8.B1 → m8b_base_timeseries.parquet
                                                                         ├─ M8.B2 → m8b2_promoter_scorecard_monthly.parquet
                                                                         │         → m8b2_promoter_scorecard_yearly.parquet
                                                                         ├─ M8.B3 → m8b_investor_metrics_selected.parquet
                                                                         │         (also reads m8b_gate_valuations.json, m7_selected_offer.json)
                                                                         ├─ M8.B4 → m8b4_lender_metrics_monthly.parquet
                                                                         │         → m8b4_lender_metrics_yearly.parquet
                                                                         ├─ M8.B5 → m8b_benchmarks.values.parquet, m8b_benchmarks.catalog.json
                                                                         └─ M8.B6 → m8b_ifrs_statements.parquet
                                                                                   → m8b_ifrs_mapping.json
                                                                                   → m8b_ifrs_notes.json
                                                                                          \
                                                                                           ├─ M9.0 Pack → m9_0_pack.xlsx (+ CSVs)
                                                                                           │             → m9_manifest.json
                                                                                           └─ M9.5 App  (reads m9_manifest.json + above)
```

---

## 2) Module‑by‑module contracts (files & columns)

Below, **bold** = confirmed from current debug/smoke/manifest files; *italics* = to confirm when we open the underlying parquet (not critical to the revenue bug).

### M9.0 (Pack & Export)

**Inputs (filenames as currently read):**

* **m7\_5b\_profit\_and\_loss.parquet**, **m7\_5b\_balance\_sheet.parquet**, **m7\_5b\_cash\_flow\.parquet**
* **m8b\_base\_timeseries.parquet**, **m8b\_fx\_curve.parquet**
* **m8b2\_promoter\_scorecard\_monthly.parquet**, **m8b2\_promoter\_scorecard\_yearly.parquet**
* **m8b\_investor\_metrics\_selected.parquet**
* **m8b4\_lender\_metrics\_monthly.parquet**, **m8b4\_lender\_metrics\_yearly.parquet**
* **m8b\_benchmarks.values.parquet**, **m8b\_benchmarks.catalog.json**
* **m8b\_ifrs\_statements.parquet**, **m8b\_ifrs\_mapping.json**, **m8b\_ifrs\_notes.json**
* **m9\_manifest.json** (existing/overwritten by M9.0)

**Outputs:**

* **m9\_0\_pack.xlsx** (11 sheets mirroring inputs), **CSV exports for each sheet**, **m9\_manifest.json** (confirmed content)

**Column expectations used by M9.0 (as exported today):**

* **P\&L**: must include **Total\_Revenue\_NAD\_000** (currently all zeros), **Total\_OPEX\_NAD\_000**, **EBITDA\_NAD\_000**, **Operating\_Income\_NAD\_000**, **Net\_Income\_NAD\_000**, and **Month\_Index**.
* **CF**: **CFO\_NAD\_000**, **CFI\_NAD\_000**, **CFF\_NAD\_000**, **Closing\_Cash\_NAD\_000**, **Month\_Index**.
* **BS**: **Cash\_and\_Cash\_Equivalents\_NAD\_000**, **Current\_Assets\_NAD\_000**, **Current\_Liabilities\_NAD\_000**, **Total\_Assets\_NAD\_000**, **Liabilities\_and\_Equity\_Total\_NAD\_000**, **Month\_Index**.
* **Base timeseries**: **Month\_Index**, **Calendar\_Year**, **Calendar\_Quarter**.
* **FX curve**: **Month\_Index**, **NAD\_per\_USD**.
* **Promoter scorecard** (monthly & yearly): see M8.B2 below.
* **Investor selected**: **Instrument**, **Gate**, **Metric**, **Value**, **Units**, **Currency**.
* **Lender metrics**: see M8.B4 below.
* **IFRS statements**: long format (**Statement**, **IFRS\_Line\_Item**/*Item*, **Month\_Index**, **Currency**, **Value\_000** – *confirm*), + mapping/notes JSON.

> ✅ Cross‑checked against the **existing manifest** which lists those exact filenames and dashboard sections (Hero, Promoter, Investor, Lender, IFRS).

---

### M8.B6 (IFRS presentation)

**Inputs:** M7.5B PL/BS/CF; M8.B1 FX calendar.

**Outputs:**

* **m8b\_ifrs\_statements.parquet** (long format statements in NAD & USD)
* **m8b\_ifrs\_mapping.json** (canonical → IFRS‑18 headings)
* **m8b\_ifrs\_notes.json** (policies & note stubs)

**Key columns expected in the statements parquet:**

* **Statement** (PL / BS / CF), **IFRS\_Line\_Item** (or **Line\_Item**), **Month\_Index**, **Currency**, **Value\_000** (*confirm exact name of the numeric field*).

**Downstream:** M9.0, M9.5.

---

### M8.B5 (Benchmarks)

**Outputs:**

* **m8b\_benchmarks.catalog.json** (metric → definition, ranges, sources, audience, unit)
* **m8b\_benchmarks.values.parquet** (values by `region`, `enterprise`, `scale`, `metric_id`, `value`)

**Downstream:** M9.0 (for display/ranges), M9.5.

---

### M8.B4 (Lender pack)

**Inputs:** M7.5B CF/PL/BS, discount rate (optional), maintenance CAPEX (optional).

**Outputs:**

* **m8b4\_lender\_metrics\_monthly.parquet**
* **m8b4\_lender\_metrics\_yearly.parquet**

**Columns (by metric):**

* **DSCR**, **ICR**, **LLCR**, **PLCR**, **LTV**, **Debt\_to\_Assets\_pct**, **Debt\_to\_Equity\_pct**, **Equity\_pct**, **CFADS\_v1\_NAD\_000**, **CFADS\_v2\_NAD\_000** (v2 = post‑maintenance CAPEX; currently same as v1), **Month\_Index**, **Currency** (NAD & USD), plus covenant highlights in metadata. (*Confirm exact spellings when we reopen the parquet; the smoke report confirms these are present by label.*)

**Downstream:** M9.0, M9.5.

---

### M8.B3 (Investor engine – selected instrument)

**Inputs:**

* **m7\_selected\_offer.json** (frozen instrument & terms)
* **m8b\_gate\_valuations.json** (gate EVs) — today: `{"M36_EV_NAD_000": 40000, ...}`
* **m7\_5\_junior\_financing.parquet** (*cash‑in schedule; used if present*)
* FX context (from M8.B1/7.5B)

**Outputs:**

* **m8b\_investor\_metrics\_selected.parquet** in **long/tidy** form:

  | Instrument | Gate | Metric | Value | Units | Currency |
  | ---------- | ---- | ------ | ----- | ----- | -------- |

  **Metrics** (confirmed in smoke): **Invested\_Capital**, **Distributions\_Cumulative**, **MOIC**, **IRR**, **DPI**, **RVPI**, **TVPI**, **Ownership\_Effective**, **Cash\_Runway\_to\_Gate**, **Implied\_Valuation**.

**Notes (today):**

* Ticket derives from **m7\_selected\_offer.json**: key **Ticket\_USD** (500k) plus **FX=18.5** → **9,250.00 NAD ’000**; injection detected at **Month\_Index=1**.
* **Gate EV** read from **m8b\_gate\_valuations.json**; if missing, **Month‑36 capped at 40,000 NAD ’000** (base case).
* Ownership is computed as **Ticket / Gate36\_EV** if equity‑like SAFE (approx).

**Downstream:** M9.0, M9.5.

---

### M8.B2 (Promoter scorecard)

**Inputs:** M7.5B PL/BS/CF + FX.

**Outputs:**

* **m8b2\_promoter\_scorecard\_monthly.parquet**
* **m8b2\_promoter\_scorecard\_yearly.parquet**

**Columns added (confirmed from debug):**

* **NAD:**
  **Gross\_Margin\_pct**, **EBITDA\_Margin\_pct**, **Operating\_Margin\_pct**, **Net\_Margin\_pct**,
  **Working\_Capital\_NAD\_000** (= **Current\_Assets\_NAD\_000 − Current\_Liabilities\_NAD\_000**),
  **Cash\_Runway\_Months**,
  **CFO\_NAD\_000**, **CFI\_NAD\_000**, **CFF\_NAD\_000**, **Free\_Cash\_Flow\_NAD\_000**,
  plus passthroughs **EBITDA\_NAD\_000**, **Total\_OPEX\_NAD\_000**, **Current\_Assets\_NAD\_000**, **Current\_Liabilities\_NAD\_000**.

* **USD:** same set with **\_USD\_000** suffix where applicable.

**Cadences:** **monthly** and **yearly** (yearly = sums for flows; simple arithmetic means for ratios; means of month‑end for stocks).

**Downstream:** M9.0, M9.5.

---

### M8.B1 (Base time engine)

**Inputs:** Month index from upstream artifacts; FX curve.

**Outputs:**

* **m8b\_base\_timeseries.parquet** with **Month\_Index**, **Calendar\_Year**, **Calendar\_Quarter** (confirmed).
* **m8b\_fx\_curve.parquet** (**Month\_Index**, **NAD\_per\_USD**) is also present and used throughout.

**Downstream:** All M8.Bn, M9.

---

### M7.5B (Rebuild / aggregation with IFRS helpers)

**Inputs (confirmed from debug):**

* **m7\_selected\_offer.json** (the frozen choice)
* **m7\_5\_junior\_financing.parquet** (if present)
* **m6\_balance\_sheet.parquet**
* **m5\_cash\_flow\_statement\_final.parquet**
* **m2\_pl\_schedule.parquet**
* **m0\_opening\_bs.parquet**
* **m0\_inputs/FX\_Path.parquet** (if present; otherwise the pipeline uses **m8b\_fx\_curve.parquet** later)

**Outputs (confirmed):**

* **m7\_5b\_profit\_and\_loss.parquet**
* **m7\_5b\_cash\_flow\.parquet**
* **m7\_5b\_balance\_sheet.parquet**

**Columns created/required:**

* **PL:** **Total\_Revenue\_NAD\_000** (currently 0 across), **Total\_OPEX\_NAD\_000**, **EBITDA\_NAD\_000**, **Gross\_Profit\_NAD\_000**, **Operating\_Income\_NAD\_000**, **Net\_Income\_NAD\_000**, **Month\_Index**.
* **CF:** **CFO\_NAD\_000**, **CFI\_NAD\_000**, **CFF\_NAD\_000**, **Closing\_Cash\_NAD\_000**, **Month\_Index**.
* **BS:** **Cash\_and\_Cash\_Equivalents\_NAD\_000** (sourced from CF Closing Cash), **Current\_Assets\_NAD\_000**, **Current\_Liabilities\_NAD\_000**, **Total\_Assets\_NAD\_000**, **Liabilities\_and\_Equity\_Total\_NAD\_000**, *AR\_Balance\_NAD\_000* (confirm), *Inventory\_Balance\_NAD\_000* (confirm), **Month\_Index**.

> 🔎 **Important:** M7.5B builds PL off **M2 PL**; that’s why **revenue = 0** end‑to‑end right now: your **M1 revenue schedule is empty → M2 PL has zero revenue → M7.5B PL copies that zero.**

---

### M6 (Balance Sheet)

**Inputs:** M5 outputs + opening balances (and WC from M2).

**Output:** **m6\_balance\_sheet.parquet** with (at minimum)
**Cash\_and\_Cash\_Equivalents\_NAD\_000**, **Current\_Assets\_NAD\_000**, **Current\_Liabilities\_NAD\_000**, **Total\_Assets\_NAD\_000**, **Liabilities\_and\_Equity\_Total\_NAD\_000**, *AR\_Balance\_NAD\_000*, *Inventory\_Balance\_NAD\_000*, **Month\_Index**.
(These are the fields M7.5B later uses and augments.)

---

### M5 (Cash Flow)

**Inputs:** M3/M4 outputs + schedules, opening cash.

**Output:** **m5\_cash\_flow\_statement\_final.parquet** with
**CFO\_NAD\_000**, **CFI\_NAD\_000**, **CFF\_NAD\_000**, **Closing\_Cash\_NAD\_000**, **Month\_Index** (plus optional **CFF\_JUNIOR\_NAD\_000** if you wire that split).

---

### M4 (Tax engine – baseline)

**Outputs consumed by M5:** tax payable/receivable series (names are not read directly by M7.5B, but affect **CFO\_NAD\_000** through M5).

---

### M3 (Financing engine – baseline)

**Outputs consumed by M5:** loan schedules used to compute **CFI/CFF** (interest, principal) and CFADS later in M8.B4. (Names not read directly by M7.5B.)

---

### M2 (PL & Working Capital from Ops assumptions)

**Inputs:**

* Input Pack sheets **Revenue\_Assumptions** and **Rev\_Ramp\_Seasonality** (and **Opex** sheets).
* **m1\_revenue\_schedule.parquet** (if M2 expects pre‑built series; your current M7.5B debug shows it reads **m2\_pl\_schedule.parquet**, so M2 is the owner of revenue aggregation).

**Outputs:**

* **m2\_pl\_schedule.parquet** with at least **Total\_Revenue\_NAD\_000**, **Total\_OPEX\_NAD\_000**, **Gross\_Profit\_NAD\_000**, **Operating\_Income\_NAD\_000**, **Net\_Income\_NAD\_000**, **Month\_Index**.
* **m2\_working\_capital\_schedule.parquet** with **AR\_Balance\_NAD\_000**, **Inventory\_Balance\_NAD\_000**, and **Month\_Index** (your sample shows non‑zero AR+Inventory, so M2 is running, but its PL segment is inheriting **zero revenue** from M1 or from its own aggregation path).

**Downstream:** M5 (indirect), M6 (indirect), **M7.5B** (direct for PL).

---

### M1 (Revenue schedule from crop/ramp/seasonality)

**Inputs:** Input Pack **Revenue\_Assumptions**, **Rev\_Ramp\_Seasonality**.

**Output today:** **m1\_revenue\_schedule.parquet** with **Monthly\_Revenue\_NAD\_000** and **Month\_Index**.
➡️ **Current state:** the parquet exists **but has zero rows** (shape (0, 3)) and thus no month totals. That’s the root of the revenue issue.

---

### M0 (Opening balances & FX)

**Outputs:**

* **m0\_opening\_bs.parquet** (at least opening **Cash\_NAD\_000** → rolled into M5/M6 and then into M7.5B).
* **m0\_inputs/FX\_Path.parquet** with **Month\_Index**, **NAD\_per\_USD** (M7.5B/M8.B1 read this; M8.B1 also emits **m8b\_fx\_curve.parquet** for downstream simplicity).

---

## 3) Naming policy (what **must not** change)

* **Time index:** `Month_Index` (1‑based) everywhere.
* **Currency‑amount columns:** `*_NAD_000` and `*_USD_000` (monetary values in ‘000).
* **Core PL names:** `Total_Revenue_NAD_000`, `Total_OPEX_NAD_000`, `EBITDA_NAD_000`, `Gross_Profit_NAD_000`, `Operating_Income_NAD_000`, `Net_Income_NAD_000`.
* **Core CF names:** `CFO_NAD_000`, `CFI_NAD_000`, `CFF_NAD_000`, `Closing_Cash_NAD_000`.
* **Core BS names:** `Cash_and_Cash_Equivalents_NAD_000`, `Current_Assets_NAD_000`, `Current_Liabilities_NAD_000`, `Total_Assets_NAD_000`, `Liabilities_and_Equity_Total_NAD_000`, `AR_Balance_NAD_000`, `Inventory_Balance_NAD_000`.
* **FX series:** `NAD_per_USD`.
* **Investor outputs:** long format with columns `Instrument`, `Gate`, `Metric`, `Value`, `Units`, `Currency`.
* **Benchmarks:** JSON catalog + parquet values keyed by `metric_id` and context dimensions.
* **IFRS:** long statements with `Statement`, `IFRS_Line_Item` (or `Line_Item`), `Month_Index`, `Currency`, and value column (name to confirm; keep it stable).

> If any upstream repair (M0–M2) changes these labels, **M7.5B, all M8.Bn and M9 will fail**. Let’s keep them **exactly** as above.

---

## 4) Immediate implications for the revenue bug

* **Why you see zero revenue everywhere:**
  **M1 revenue schedule is empty** → M2’s PL aggregation has **Total\_Revenue\_NAD\_000 = 0** → M7.5B copies that into its PL → all M8.Bn & M9 inherit zeros.

* **Fix path (no renames):**

  1. Rebuild **M1** to emit **m1\_revenue\_schedule.parquet** with `Month_Index` and `Monthly_Revenue_NAD_000` computed from **Revenue\_Assumptions** × **Rev\_Ramp\_Seasonality** (ramp by year, monthly seasonality, hectares × yields × prices × cycles).
  2. Make sure **M2** reads either that M1 table or directly the same sheets to populate **m2\_pl\_schedule.parquet.Total\_Revenue\_NAD\_000** (keeping the exact column name).
  3. Re‑run **M7.5B** and then **M8.B1–B6** and **M9.0**; revenue should appear from \~Month 7 onward, approaching steady‑state around Year 2 as per your seasonality/ramp assumptions.

---

## 5) CI additions (lightweight but strict)

For each artifact below, add a gate that fails fast if the essential columns are missing or if obvious invariants don’t hold:

* **M1 → m1\_revenue\_schedule.parquet**

  * must have `Month_Index`, `Monthly_Revenue_NAD_000`
  * assert **(nz months ≥ 1)** and **max month ≥ 24**

* **M2 → m2\_pl\_schedule.parquet**

  * must have **Total\_Revenue\_NAD\_000** and **Total\_OPEX\_NAD\_000**
  * assert **sum(Total\_Revenue) > 0**

* **M7.5B PL/CF/BS**

  * assert `CFO+CFI+CFF` change reconciles to `Closing_Cash` deltas
  * assert `Total_Assets == Liabilities_and_Equity_Total` (within 1e‑6)

* **M8.B2/M8.B4**

  * presence of published metric columns; ratios in \[−500%, +500%] sanity

* **M8.B6**

  * IFRC pack: all three files present; statements have both **PL** and **BS** rows

* **M9.0**

  * sheets count == expected; manifest round‑trips