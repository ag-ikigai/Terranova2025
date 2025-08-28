# Terra Nova PF — CODING_STANDARDS.md

This document standardizes style, tooling, and conventions for Python code in Terra Nova PF.

## Language & Dependencies
- Python ≥ 3.11; prefer `dataclasses` and `typing` (PEP 484/604/695).
- Money/time/rates use `decimal.Decimal` and `datetime`/`dateutil`. Avoid floats for financial values.
- Dataframes: prefer Pandas; consider Polars for large, columnar workloads; use Pandera schemas.

## Formatting, Linting, Types
- **Black** for formatting (default config).
- **Ruff** for linting (include `E,W,F,I,N,B,UP,PLE,PLR,PLW` rulesets; keep line length 100).
- **isort** for imports (profile `black`).
- **MyPy**: treat `disallow_untyped_defs = True`, `warn_return_any = True`, `strict_optional = True`.
- CI must run all tools; PRs fail on violations.

## Naming & Structure
- Modules: `snake_case.py`; Classes: `PascalCase`; functions/vars: `snake_case`.
- Package layout:
  ```
  terra_nova/
    domain/         # entities, value objects, services
    data/           # IO, schemas, repositories
    finance/        # valuation, EIR, IFRS computations
    cli/            # command-line entry points
    utils/          # shared helpers (small, stable)
    tests/          # pytest suite mirrors package structure
  ```
- One public responsibility per module. Keep files < 500 lines where possible.

## Docstrings (Google style)
Every public function/class has a docstring. Example:

```python
def effective_interest_rate(cashflows: list[Decimal], price: Decimal, guess: Decimal | None = None) -> Decimal:
    """Compute the internal rate of return (EIR) that prices cashflows to `price`.

    Args:
        cashflows: Signed amounts by period (positive = inflow to holder).
        price: Present value (purchase price) as a Decimal.
        guess: Optional initial guess for root-finding.

    Returns:
        Decimal: The EIR per period.

    Raises:
        CalculationError: If convergence fails or inputs violate DOMAIN_MODEL.md rules.
    """
```

## Error Taxonomy
Create a base error and derive specific ones:

```python
class TerraNovaError(Exception): ...
class DataValidationError(TerraNovaError): ...
class IFRSRuleViolation(TerraNovaError): ...
class CalculationError(TerraNovaError): ...
class ExternalServiceError(TerraNovaError): ...
```

Rules:
- Validate inputs at boundaries; raise `DataValidationError` with field names.
- Domain rule breaches → `IFRSRuleViolation` with file/section citation.
- Numeric failures (non-convergence, overflow) → `CalculationError`.

## Logging
- Use `logging` with structured JSON handlers in production.
- Log schema fields: `ts, level, module, function, message, correlation_id, instrument_id?, contract_id?`.
- Never log PII; mask IDs if needed.
- Example:
```python
logger.info("eir_converged", extra={"instrument_id": iid, "iterations": n, "eir": str(rate)})
```

## Git Strategy & Commits
- Trunk-based development; short-lived feature branches; frequent merges to `main`.
- **Conventional Commits**:
  - `feat: add IFRS 16 lease amortization schedule`
  - `fix: correct rounding in EIR Newton step`
  - `docs: update DOMAIN_MODEL.md for day-count`
- Each PR includes: purpose, approach, tests, performance notes, and references to ADRs/IFRS sections.

## DataFrame Best Practices
- Define schemas with Pandera; validate at IO boundaries.
- Avoid `.apply` on row-wise operations; prefer vectorized ops.
- Explicit dtypes; avoid implicit object dtype; category for enums.
- Index must be meaningful or reset to default; do not rely on accidental ordering.

## Configuration
- Use pydantic `BaseSettings` or similar; only read environment at process start.
- All config keys documented in `DATA_DICTIONARY.md` (Config section).

## Performance & Precision
- Use `Decimal` context with explicit rounding (BANKERS or project‑specific) and precision configured centrally.
- Document algorithmic complexity where non-trivial; include micro-benchmarks for hot paths.
