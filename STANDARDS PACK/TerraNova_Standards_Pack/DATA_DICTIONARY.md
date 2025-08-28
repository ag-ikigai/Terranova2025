# Terra Nova PF — DATA_DICTIONARY.md

This document defines core datasets, columns, types, null policy, constraints, and units.
All datasets must be validated at IO boundaries (Pandera/Pydantic).

## Conventions
- Types use Python typing + storage hints (e.g., `Decimal(18,4)`).
- `NOT NULL` means required. Optional fields explicitly marked.
- Currency: ISO 4217; Date: ISO 8601 `YYYY-MM-DD`.

---

## Table: instruments
| Column            | Type            | Null | Constraints                              | Units     | Example      |
|-------------------|-----------------|------|------------------------------------------|-----------|--------------|
| instrument_id     | str (ULID)      | NO   | unique                                    | —         | 01H9Z...     |
| type              | enum            | NO   | {'loan','bond','receivable','lease'}     | —         | loan         |
| currency          | str(3)          | NO   | ISO 4217                                  | —         | USD          |
| start_date        | date            | NO   | start_date ≤ maturity_date                | —         | 2025-01-01   |
| maturity_date     | date            | NO   | —                                         | —         | 2028-12-31   |
| day_count         | enum            | NO   | {'ACT/365F','ACT/360','30E/360','30/360'} | —         | ACT/365F     |
| payment_frequency | enum            | NO   | {'M','Q','S','A'}                         | —         | A            |
| rate_type         | enum            | NO   | {'fixed','float'}                         | —         | fixed        |
| spread_bps        | int             | YES  | only if `rate_type = float`               | bps       | 125          |
| initial_fair_value| Decimal(18,4)   | YES  | ≥ 0                                       | currency  | 97000.0000   |

## Table: cashflows
| Column        | Type           | Null | Constraints                        | Units    | Example    |
|---------------|----------------|------|------------------------------------|----------|------------|
| instrument_id | str (ULID)     | NO   | FK → instruments.instrument_id     | —        | 01H9Z...   |
| flow_date     | date           | NO   | ≥ instrument.start_date            | —        | 2026-12-31 |
| amount        | Decimal(18,4)  | NO   | can be negative/positive           | currency | 5000.0000  |
| kind          | enum           | NO   | {'principal','interest','fee'}     | —        | interest   |

## Table: leases
| Column            | Type           | Null | Constraints                         | Units | Example |
|-------------------|----------------|------|-------------------------------------|-------|---------|
| lease_id          | str (ULID)     | NO   | unique                              | —     | 01HA... |
| instrument_id     | str (ULID)     | NO   | FK → instruments.instrument_id      | —     | 01H9Z.. |
| ib_rate           | Decimal(6,4)   | NO   | 0 < r < 1                           | ratio | 0.0600  |
| payment_amount    | Decimal(18,4)  | NO   | > 0                                 | ccy   | 10.0000 |
| payment_timing    | enum           | NO   | {'advance','arrears'}               | —     | arrears |
| term_periods      | int            | NO   | ≥ 1                                 | —     | 36      |

## Table: revenue_contracts
| Column               | Type           | Null | Constraints                           | Units | Example |
|----------------------|----------------|------|---------------------------------------|-------|---------|
| contract_id          | str (ULID)     | NO   | unique                                | —     | 01HB... |
| customer_id          | str (ULID)     | NO   | FK → counterparties.counterparty_id   | —     | 01HC... |
| transaction_price    | Decimal(18,4)  | NO   | ≥ 0                                   | ccy   | 100.0000|
| variable_component   | Decimal(18,4)  | YES  | ≥ 0                                   | ccy   | 10.0000 |
| recognition_method   | enum           | NO   | {'over_time','point_in_time'}         | —     | over_time |

## Table: counterparties
| Column          | Type       | Null | Constraints      | Units | Example        |
|-----------------|------------|------|------------------|-------|----------------|
| counterparty_id | str (ULID) | NO   | unique           | —     | 01HC...        |
| name            | str        | NO   | —                | —     | ACME Holdings  |
| rating          | str        | YES  | agency code set  | —     | BBB+           |
| sector          | str        | YES  | controlled vocab | —     | Utilities      |

## Table: fx_rates
| Column     | Type          | Null | Constraints             | Units | Example |
|------------|---------------|------|-------------------------|-------|---------|
| base_ccy   | str(3)        | NO   | ISO 4217                | —     | USD     |
| quote_ccy  | str(3)        | NO   | ISO 4217                | —     | EUR     |
| date       | date          | NO   | unique (base,quote,date)| —     | 2025-06-30 |
| rate       | Decimal(18,8) | NO   | > 0                     | —     | 0.92350000 |

## Config (application settings)
Document all configuration keys here with defaults and ranges. Example:

| Key                                  | Type            | Default | Range/Constraint           | Description |
|--------------------------------------|-----------------|---------|----------------------------|-------------|
| FINANCE_ROUNDING_MODE                | str             | BANKERS | {'BANKERS','HALF_UP'}      | Global rounding rule |
| IFRS_ECL_DEFAULT_DISCOUNT_TO_EIR     | bool            | true    | —                          | Use EIR for discounting ECL |
| IFRS15_VARIABLE_CONSIDERATION_LIMIT  | Decimal(6,4)    | 0.20    | 0..1                       | Cap for constraint as proportion |
