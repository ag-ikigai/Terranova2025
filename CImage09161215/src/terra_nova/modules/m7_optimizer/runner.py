# SPDX-License-Identifier: MIT
"""
Terra Nova — Module 7 (R1): Coarse ranking of investor offers.

Reads the Input Pack sheet `Investor_500k_Offer_Grid`, normalizes sponsor-
friendly metrics, computes a weighted score for each row, chooses exactly
one offer (via OR-Tools CP-SAT if available, otherwise pure-Python argmax),
and persists:

  - outputs/m7_r1_scores.csv
  - outputs/m7_r1_scores.parquet
  - outputs/m7_r1_smoke_report.md
  - outputs/m7_r1_debug.json

Design goals:
- Keep existing M0–M6 stable; M7.R1 reads only the offer grid.
- Outputs satisfy test expectations in tests/smoke/test_m7_smoke.py:
  * CSV/Parquet exist, columns include: Option, Instrument, Rank,
    Total_Score_0_100, Selected
  * Exactly one row has Selected == "yes"
  * Debug JSON includes "weights" and "selected_option"
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Dict, Optional

import numpy as np
import pandas as pd

# Optional: use our thin CP-SAT adapter if present (preferred path).
try:
    # Must live alongside this runner: src/terra_nova/modules/m7_optimizer/solver_adapter.py
    from .solver_adapter import M7Model  # type: ignore
except Exception:  # pragma: no cover - fallback only
    M7Model = None  # sentinel -> fallback to argmax


# ------------------------
# Scoring configuration
# ------------------------
# Weights reflect "sponsor-friendly" preferences:
# - Lower Valuation Cap is better
# - Higher Discount is better
# - Lower RevShare is better
# - Lower IRR floor is better
# - Lower Exit multiple is better (less buyout burden)
DEFAULT_WEIGHTS: Dict[str, float] = {
    "Valuation_Cap_NAD": 1.0,
    "Discount_pct": 1.0,
    "RevShare_preRefi_pct": 1.0,
    "Min_IRR_Floor_pct": 1.0,
    "Exit_Refi_Multiple": 0.5,
}

REQUIRED_COLS = [
    "Option",
    "Instrument",
    "Ticket_USD",
    "Valuation_Cap_NAD",
    "Discount_pct",
    "RevShare_preRefi_pct",
    "Min_IRR_Floor_pct",
    "Conversion_Terms",
    "Exit_Refi_Multiple",
]


# ------------------------
# Helpers
# ------------------------
def _as_num(s: pd.Series) -> pd.Series:
    """Coerce to numeric without raising; keep NaN if not convertible."""
    return pd.to_numeric(s, errors="coerce")


def _minmax01(x: pd.Series) -> pd.Series:
    """Safe min-max normalization to [0,1]; constant series -> 0."""
    x = _as_num(x)
    mn = x.min(skipna=True)
    mx = x.max(skipna=True)
    denom = (mx - mn)
    if pd.isna(mn) or pd.isna(mx) or float(denom) == 0.0:
        return pd.Series(np.zeros(len(x), dtype=float), index=x.index)
    return (x - mn) / denom


def _normalize_better_low(x: pd.Series) -> pd.Series:
    """
    Normalize where SMALLER is better: score = 1 - minmax(x_worstfill)
    Missing values are pessimistically filled with the column max (worst).
    """
    x = _as_num(x)
    x_filled = x.fillna(x.max(skipna=True))
    return 1.0 - _minmax01(x_filled)


def _normalize_better_high(x: pd.Series) -> pd.Series:
    """
    Normalize where LARGER is better: score = minmax(x_bestfill)
    Missing values are pessimistically filled with the column min (worst).
    """
    x = _as_num(x)
    x_filled = x.fillna(x.min(skipna=True))
    return _minmax01(x_filled)


def _df_to_markdown(df: pd.DataFrame) -> str:
    """Tiny Markdown table builder (fallback if pandas.to_markdown/tabulate missing)."""
    cols = list(df.columns)
    lines = []
    # header
    lines.append("| " + " | ".join(cols) + " |")
    lines.append("| " + " | ".join(["---"] * len(cols)) + " |")
    # rows
    for _, row in df.iterrows():
        cells = []
        for c in cols:
            val = row[c]
            if isinstance(val, float):
                # keep concise but readable
                cells.append(f"{val:.2f}")
            else:
                cells.append(str(val))
        lines.append("| " + " | ".join(cells) + " |")
    return "\n".join(lines)


# ------------------------
# Core scoring
# ------------------------
def _build_scores(df: pd.DataFrame, weights: Dict[str, float]) -> pd.DataFrame:
    """
    Return a dataframe with per-offer component scores and Total_Score_0_100.
    Ensures required columns exist (adds with NaN if missing).
    """
    df = df.copy()

    # Ensure columns exist; if missing in the sheet, add empty ones
    for c in REQUIRED_COLS:
        if c not in df.columns:
            df[c] = np.nan

    # Normalize components
    cap = _normalize_better_low(df["Valuation_Cap_NAD"])
    disc = _normalize_better_high(df["Discount_pct"])
    rs = _normalize_better_low(df["RevShare_preRefi_pct"])
    irr = _normalize_better_low(df["Min_IRR_Floor_pct"])
    exitm = _normalize_better_low(df["Exit_Refi_Multiple"])

    # Weights (rescaled defensively to sum to 1.0 if they don't already)
    w = {k: float(weights.get(k, 0.0)) for k in DEFAULT_WEIGHTS.keys()}
    w_sum = sum(w.values()) or 1.0
    w = {k: v / w_sum for k, v in w.items()}

    # Weighted total in [0,1], then scale to 0..100
    total01 = (
        w["Valuation_Cap_NAD"] * cap
        + w["Discount_pct"] * disc
        + w["RevShare_preRefi_pct"] * rs
        + w["Min_IRR_Floor_pct"] * irr
        + w["Exit_Refi_Multiple"] * exitm
    )
    total = (total01 * 100.0).astype(float)

    # Keep original fields + component columns
    out = df[REQUIRED_COLS].copy()
    out["Score_Cap"] = (cap * 100).round(2)
    out["Score_Discount"] = (disc * 100).round(2)
    out["Score_RevShare"] = (rs * 100).round(2)
    out["Score_IRRFloor"] = (irr * 100).round(2)
    out["Score_ExitMult"] = (exitm * 100).round(2)
    out["Total_Score_0_100"] = total.round(2)

    # Make sure Option/Instrument are strings (avoid NaN in MD)
    out["Option"] = out["Option"].fillna("").astype(str)
    out["Instrument"] = out["Instrument"].fillna("").astype(str)

    return out


def _solve_choose_one(scores: pd.DataFrame) -> int:
    """
    Choose exactly one row maximizing Total_Score_0_100.
    Returns a *positional* index (0..n-1).
    """
    # Fallback if adapter is missing
    if M7Model is None:
        # pick argmax safely
        pos = int(scores["Total_Score_0_100"].fillna(-1e9).to_numpy().argmax())
        return pos

    # CP-SAT model (tiny)
    model = M7Model()
    n = len(scores)
    x = [model.bool_var(f"x_{i}") for i in range(n)]
    # exactly one
    model.add(sum(x) == 1)

    # integerized objective
    ints = (scores["Total_Score_0_100"].fillna(0.0) * 1000).round().astype(int).tolist()
    model.maximize(sum(x[i] * ints[i] for i in range(n)))

    solver, status, _vals = model.solve(seconds=5)

    chosen = None
    for i in range(n):
        if solver.Value(x[i]) == 1:
            chosen = i
            break

    if chosen is None:
        # Fallback to Python argmax if solver didn’t decide
        chosen = int(scores["Total_Score_0_100"].fillna(-1e9).to_numpy().argmax())
    return chosen


# ------------------------
# Public API
# ------------------------
def run_m7_r1(
    input_pack_xlsx: str,
    out_dir: str,
    currency: str = "NAD",
    weights: Optional[Dict[str, float]] = None,
) -> None:
    """
    Stage R1 (coarse ranking). See module docstring for details.
    """
    weights = weights or DEFAULT_WEIGHTS
    out = Path(out_dir)
    out.mkdir(parents=True, exist_ok=True)

    # 1) Ingest the grid (drop fully empty rows)
    df = pd.read_excel(input_pack_xlsx, sheet_name="Investor_500k_Offer_Grid")
    df = df.dropna(how="all").copy()

    # 2) Score & select
    scored = _build_scores(df, weights=weights)
    chosen_pos = _solve_choose_one(scored)

    # tag selection (string 'yes'/'no' to satisfy smoke test expectation)
    scored = scored.reset_index(drop=True)
    selected_flags = np.array(["no"] * len(scored), dtype=object)
    if 0 <= chosen_pos < len(scored):
        selected_flags[chosen_pos] = "yes"
    scored["Selected"] = selected_flags

    # Rank (desc by score, then Option asc for stability)
    scored = scored.sort_values(
        ["Total_Score_0_100", "Option"],
        ascending=[False, True],
        kind="mergesort",
    ).reset_index(drop=True)
    scored["Rank"] = np.arange(1, len(scored) + 1)

    # 3) Persist artifacts
    # CSV preferred by smoke tests to avoid parquet engine assumptions.
    scored.to_csv(out / "m7_r1_scores.csv", index=False)
    try:
        scored.to_parquet(out / "m7_r1_scores.parquet", index=False)
    except Exception:
        # Optional; CSV is enough for tests
        pass

    # Debug JSON (keys expected by tests include 'weights' and 'selected_option')
    selected_row = scored.iloc[0] if len(scored) else None
    dbg = {
        "weights": weights,
        "n_offers": int(len(scored)),
        "selected_option": (str(selected_row["Option"]) if selected_row is not None else None),
        "selected_instrument": (str(selected_row["Instrument"]) if selected_row is not None else None),
        "selected_score": (float(selected_row["Total_Score_0_100"]) if selected_row is not None else None),
        "columns": list(scored.columns),
        "currency_context": currency,
    }
    (out / "m7_r1_debug.json").write_text(json.dumps(dbg, indent=2), encoding="utf-8")

    # Smoke MD with top-6
    md_lines = [
        "# M7.R1 — Coarse Ranking (OR-Tools)",
    ]
    if selected_row is not None:
        md_lines.append(
            f"- **Selected**: {selected_row['Option']} — {selected_row['Instrument']}"
        )
        md_lines.append(
            f"- Score: **{selected_row['Total_Score_0_100']}** (Rank 1 of {len(scored)})"
        )
    md_lines.append("\n## Top 6 (by score)\n")
    top_cols = [
        "Rank",
        "Option",
        "Instrument",
        "Total_Score_0_100",
        "Valuation_Cap_NAD",
        "Discount_pct",
        "RevShare_preRefi_pct",
        "Min_IRR_Floor_pct",
        "Exit_Refi_Multiple",
        "Selected",
    ]
    subset = [c for c in top_cols if c in scored.columns]
    head6 = scored[subset].head(6)

    # Prefer pandas markdown if available; otherwise fallback
    try:
        table_md = head6.to_markdown(index=False)  # requires tabulate
    except Exception:
        table_md = _df_to_markdown(head6)
    md_lines.append(table_md)

    (out / "m7_r1_smoke_report.md").write_text("\n".join(md_lines), encoding="utf-8")

    # Console banner (mirrors previous modules)
    sel_code = dbg["selected_option"] or "(n/a)"
    sel_instr = dbg["selected_instrument"] or ""
    print(
        f"[OK] M7.R1 ranked {len(scored)} offer(s). Selected -> {sel_code} ({sel_instr})."
    )
    print(
        f"Scores -> {out/'m7_r1_scores.parquet'} ; Debug -> {out/'m7_r1_debug.json'} ; Smoke -> {out/'m7_r1_smoke_report.md'}"
    )


# ------------------------
# CLI (optional)
# ------------------------
def _cli() -> None:  # pragma: no cover
    import argparse

    p = argparse.ArgumentParser(prog="terra-nova-m7-r1")
    p.add_argument("input", help="Path to Input Pack Excel")
    p.add_argument("out", help="Output directory (e.g., ./outputs)")
    p.add_argument("--currency", default="NAD")
    args = p.parse_args()

    run_m7_r1(args.input, args.out, currency=args.currency)


if __name__ == "__main__":  # pragma: no cover
    _cli()
