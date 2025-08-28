# Terra Nova PF — TEST_PLAN.md

This plan defines the testing strategy, required tests per module, and the **Definition of Done** for tickets.

## Test Pyramid
- **Unit tests** (vast majority): pure functions/services with mocks for IO.
- **Integration tests**: IO boundaries (DB/files/HTTP), schema validation, config.
- **Contract tests**: external providers (market data) with recorded fixtures.
- **Property-based tests** (Hypothesis) for numeric invariants.
- **End-to-end smoke**: CLI or pipeline happy path on a tiny fixture dataset.

## Coverage & Gates
- Line coverage ≥ 90% overall, and ≥ 80% per package.
- Mutation testing (optional) for finance algorithms.
- CI gates: Black/Ruff/isort/MyPy + pytest must pass.

## Required Tests by Module (examples)
### finance/eir.py
- Compute EIR for fixed coupon instruments (happy path, non-convergence edge case).
- Validate monotonicity and convergence; rounding policy honored.

### finance/ifrs16.py
- Present value of lease payments; schedule generation; remeasurement scenario.
- Reconcile interest expense vs. carrying value change.

### revenue/ifrs15.py
- Allocation of transaction price to POBs; variable consideration constraint.

### data/schemas.py
- Pandera schemas for all tables in `DATA_DICTIONARY.md`; reject malformed rows.

## Acceptance Tests (mirror IFRS_RULES.md)
- A: Amortized cost schedule via EIR (IFRS 9).
- B: Stage-1 ECL point estimate (IFRS 9).
- C: Variable consideration expected value (IFRS 15).
- D: Basic lease PV & schedule (IFRS 16).

Each acceptance test:
- Lives under `tests/acceptance/test_*.py`.
- Includes a short narrative citing the IFRS_RULES.md section.
- Emits a CSV/JSON artifact for manual review when `--capture=tee-sys`.

## Test Data & Fixtures
- All fixtures anonymized; versions tracked.
- Golden files stored under `tests/golden/` with schema versioning.

## Performance Benchmarks (if applicable)
- Micro-benchmarks for hot paths using `pytest-benchmark`.
- Document thresholds; fail PRs if performance regresses > 10% on stable hardware.

## Definition of Done (for any ticket)
- [ ] Unit + integration tests added/updated; acceptance tests touched if relevant.
- [ ] Code follows `CODING_STANDARDS.md`; errors use taxonomy; logs are structured.
- [ ] Any design tradeoff recorded as ADR.
- [ ] CHANGELOG entry drafted.
