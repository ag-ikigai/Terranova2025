# Terra Nova PF — IFRRS_RULES.md

> **Disclaimer:** This document operationalizes parts of IFRS 9/15/16 for software behavior. It is **not** legal or accounting advice. Confirm policies with qualified professionals.

## Scope
- **IFRS 9 — Financial Instruments**: classification/measurement, impairment (ECL), derecognition.
- **IFRS 15 — Revenue from Contracts with Customers**: five‑step model, variable consideration, significant financing component.
- **IFRS 16 — Leases**: initial measurement, subsequent measurement, remeasurement, presentation.

## Common Definitions
- **EIR (Effective Interest Rate):** the rate that exactly discounts estimated future cashflows through the expected life of the instrument to the gross carrying amount at initial recognition.
- **ECL (Expected Credit Loss):** probability‑weighted present value of credit losses (default events over lifetime or next 12 months).

---

## IFRS 9: Classification & Measurement (Operational Rules)

**Classification decision (pseudo):**
1. Assess business model: Hold to collect (HTC), Hold to collect and sell (HTCS), or Other.
2. Assess SPPI (Solely Payments of Principal and Interest).
3. Map to category:
   - HTC + SPPI → **Amortized Cost**  
   - HTCS + SPPI → **FVOCI**  
   - Else → **FVTPL**

**Measurement rules:**
- **Amortized Cost**: recognize using **EIR** method; fees/costs included in EIR.
- **FVOCI**: OCI for fair value changes; EIR on amortized cost base for P&L interest.
- **FVTPL**: fair value changes in profit or loss.

### Acceptance Test A (Amortized Cost via EIR)
**Given:**
- Price at t0 = 97.000 (per 100.000 notional), annual coupon 5.000 paid annually, maturity 3 years, day count ACT/365, no fees.  
**Then:**
- Compute EIR `r` such that PV(coupons + principal) = 97.000.
- Year‑1 interest = EIR × opening carrying amount.
- Provide schedule with `opening, interest, coupon, closing`.

*(Implement as unit test: `tests/finance/test_eir_schedule.py::test_three_year_bond_example`)*

### Acceptance Test B (12‑month ECL, Stage 1)
**Given:** EAD = 100.000, PD_12m = 2.0%, LGD = 45%, discount rate = EIR, default at period end.  
**Then:** 12‑month ECL ≈ 0.900 (before discounting).  
*(Unit test: `tests/finance/test_ecl.py::test_stage1_ecl_point_estimate`)*

---

## IFRS 15: Revenue (Operational Rules)

**Five steps (software hooks):**
1. Identify contract(s) with a customer.
2. Identify performance obligations (POBs).
3. Determine transaction price (TP), including variable consideration constraints.
4. Allocate TP to POBs based on standalone selling prices.
5. Recognize revenue when (or as) the entity satisfies a POB.

### Acceptance Test C (Variable Consideration — Expected Value)
**Given:** TP fixed = 95, bonus 10 with 40% probability; single POB satisfied over time (straight line over 5 periods).  
**Then:** Expected TP = 99.  
- Recognize 99/5 per period unless constraint triggers limiting recognition (set threshold in policy).  
*(Unit test: `tests/revenue/test_variable_consideration.py::test_expected_value_allocation`)*

---

## IFRS 16: Leases (Operational Rules)

**Initial recognition:**
- Lease liability = PV of lease payments not paid at commencement, discounted at the lease’s incremental borrowing rate (IBR) unless implicit rate known.
- ROU asset = Lease liability + initial direct costs + prepaid lease payments − lease incentives received.

### Acceptance Test D (Simple Lease PV & Schedule)
**Given:** Payments 10 at end of each year for 3 years; IBR = 6%.  
**Then:**
- Initial liability = PV(10,10,10 @ 6%) ≈ 26.730.
- Year‑1 interest = 26.730 × 6% ≈ 1.604; closing = 26.730 + 1.604 − 10 = 18.334.
*(Unit test: `tests/leases/test_ifrs16.py::test_basic_pv_and_schedule`)*

---

## Policy Hooks & References
- Rounding: specify banker's vs commercial; centralize in `finance/rounding.py`.
- Day count: use `DOMAIN_MODEL.md` conventions.
- Materiality thresholds and constraint parameters are set in `CONFIG` and documented in `DATA_DICTIONARY.md` (Config section).
