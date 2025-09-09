# Module 1 (M1) Artifact Contract – stub v0

**Purpose.** M1 computes operational schedules (yields, OPEX frames, etc.). **As of M0–M5 stable**, M1 has **no direct downstream dependency** enforced by contracts; M2 computes Working Capital and P&L independently from InputPack + drivers.

This stub exists to:
- Reserve canonical locations/filenames should M6–M9 later require M1 artifacts.
- Prevent accidental filename drift in future commits.

## (Reserved) Location and names

- Location (if/when emitted): `./outputs/`
- Suggested names (TBD if needed later):
  - `m1_operational_summary.parquet`
  - `m1_smoke_report.md`

## Policy

- Until a downstream module **consumes** an M1 artifact, **no schema is frozen**.
- When M6–M9 design first references any M1 column, we will promote this stub to a full contract (v1) with explicit role mappings, units, and invariants.
