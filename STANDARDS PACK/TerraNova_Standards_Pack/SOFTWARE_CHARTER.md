# Terra Nova PF — SOFTWARE_CHARTER.md

**Purpose.** Establish non‑negotiable principles for software quality, safety, and maintainability for the Terra Nova PF – Full DD & Financial Model Build.

## Core Principles

1. **Readability > cleverness.** Code must be self‑documenting, idiomatic Python, and easy to review.
2. **Tests‑first.** Add/extend tests *before* implementation (TDD where practical). No code merges without passing tests.
3. **Types everywhere.** All functions, methods, and public variables use explicit type hints. Money and rates use `Decimal` (no float).
4. **Deterministic & reproducible.** Pure functions where reasonable; no hidden I/O; same inputs → same outputs.
5. **Security & PII.** No credentials or PII in code, logs, diffs, or test fixtures. Use environment variables and secrets managers.
6. **Logging & observability.** Structured logs for all modules at INFO level; DEBUG only for local dev. Errors must include context.
7. **Error handling.** Fail fast with clear error taxonomy; never silently swallow exceptions.
8. **Single Source of Truth.** Domain and IFRS rules live in project files; code cites those sections by name.
9. **Small, reviewed changes.** Prefer minimal diffs with clear commit messages and ADRs for design decisions.
10. **Performance with evidence.** Optimize only with measured data; include complexity notes in PRs.

## Toolchain

- Python ≥ 3.11
- **Format**: Black; **Lint**: Ruff; **Imports**: isort; **Types**: MyPy (strict-ish); **Tests**: pytest; **Coverage** ≥ 90%
- Optional: Pandera for DataFrame schema validation; Pydantic for config & data contracts.
- Pre-commit hooks run all above tools.

## Secrets & Data Policy

- Secrets from environment or the chosen secrets manager; never hard‑coded or committed.
- Use ISO standards where applicable (ISO 4217 for currency codes, ISO 8601 for dates).
- Local sample datasets must be fully anonymized and documented in `DATA_DICTIONARY.md`.

## Documentation

- API docs via docstrings (Google style). Design docs in ADRs. High‑level overviews in `DOMAIN_MODEL.md`.
- Diagrams (Mermaid/PlantUML) may be embedded in markdown files adjacent to code.

## Definition of Done (DoD)

- [ ] Task acceptance criteria satisfied.
- [ ] Tests added/updated (happy path + ≥2 edge cases); coverage unchanged or higher.
- [ ] Type hints and docstrings present; no TODOs left.
- [ ] Black/Ruff/isort/MyPy all green.
- [ ] Logging and error handling demonstrate the taxonomy.
- [ ] For any design choice, an ADR is written/updated.
- [ ] CHANGELOG entry proposed.
