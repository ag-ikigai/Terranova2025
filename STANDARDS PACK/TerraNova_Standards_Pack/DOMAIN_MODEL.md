# Terra Nova PF — DOMAIN_MODEL.md

This document defines the core entities, value objects, invariants, and conventions.

## Entities & Value Objects

- **Instrument**: a financial instrument (loan, receivable, debt security, lease, derivative).
  - Fields: `instrument_id`, `type`, `currency`, `start_date`, `maturity_date`, `rate_type`, `spread_bps?`, `day_count`, `payment_frequency`, `amortization`, `initial_fair_value`.
- **CashFlow**: dated amount linked to an instrument.
  - Fields: `instrument_id`, `flow_date`, `amount`, `type` (principal/interest/fee/lease_payment).
- **Counterparty**: legal entity; contains risk attributes (rating, sector).
- **Contract**: bundle of obligations/rights; may hold IFRS policy hooks and options.
- **FXRate** (Value Object): `base_ccy`, `quote_ccy`, `date`, `rate`; rule: one source of truth per date.
- **Calendar** (Value Object): business day rules and holidays.
- **LedgerEntry**: accounting entry with debit/credit, account code, amount, currency, timestamp, source.

## Identifiers & Conventions
- IDs: ULID/UUIDv7 strings; never recycle; immutable.
- Dates: ISO 8601; timezone-naive dates for accounting cutoffs; UTC datetimes for events.
- Currency: ISO 4217 (uppercase).
- Amounts: `Decimal`; store monetary values in major units; rounding rule resides in `finance/rounding.py`.

## Day-Count & Calendars
- Supported day-count: ACT/365F, ACT/360, 30E/360, 30/360 US.
- Payment frequencies: monthly/quarterly/semiannual/annual; aligned with business-day adjustments.
- Calendar strategy: `Following`, `ModifiedFollowing`, `Preceding`; configured per instrument type.

## Invariants
- A `CashFlow` must reference an existing `Instrument`.
- `maturity_date >= start_date`.
- Currency of all cashflows equals instrument currency unless explicitly hedged/converted.
- Interest calculations must use the instrument’s `day_count` and `payment_frequency`.
- Each ledger entry: `sum(debits) == sum(credits)` per posting.

## State Machines (illustrative)
- Instrument lifecycle: `Draft → Active → Suspended? → Matured/Terminated`.
- Lease lifecycle (IFRS 16): `Recognition → Remeasurement? → Modification? → End`.
- Transitions must be recorded with timestamps and reasons.

## Domain Services (examples)
- **EIR Pricing**: compute effective interest rate and amortized cost schedules (IFRS 9).
- **Revenue Recognition**: allocation and recognition rules (IFRS 15).
- **Lease Accounting**: PV of lease payments, interest accrual, ROU amortization (IFRS 16).

## Data Contracts
- IO boundaries validate payloads against schemas (Pandera/Pydantic). Any breach raises `DataValidationError`.
- External data (market, ratings) must include `source`, `as_of`, and `licence` fields.
