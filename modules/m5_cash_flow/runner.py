# src/terra_nova/modules/m5_cash_flow/runner.py
from __future__ import annotations

import json
import os
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List, Tuple

import pandas as pd

# --------------------------------------------
# Helpers
# --------------------------------------------

_CCY_SUFFIX = re.compile(r"_(USD|NAD|EUR|ZAR)(?:_?(\d{3}))?$", re.IGNORECASE)

def _canon(name: str) -> str:
    """Normalize a column name: strip currency suffix, collapse non-alnum to _, lowercase."""
    base = _CCY_SUFFIX.sub("", name)
    base = re.sub(r"[^A-Za-z0-9]+", "_", base).strip("_").lower()
    return base

def _find_first(df: pd.DataFrame, candidates: Iterable[str]) -> str | None:
    """Return the FIRST df column whose canonicalized name matches any canonicalized candidate."""
    if df is None or df.empty:
        return None
    # Precompute canonical map {canon: original}
    canon_to_original: Dict[str, str] = {}
    for col in df.columns:
        canon_to_original.setdefault(_canon(col), col)
    for c in candidates:
        ccan = _canon(c)
        if ccan in canon_to_original:
            return canon_to_original[ccan]
    return None

def _normalize_columns(df: pd.DataFrame, roles: Dict[str, List[str]], ctx: str) -> Dict[str, str]:
    """
    Resolve required 'roles' -> actual df columns using robust synonyms and currency-suffix tolerance.
    roles: {"ROLE": ["synonym1", "synonym2", ...]}
    Returns {"ROLE": "Actual_Column_Name"} or raises AssertionError.
    """
    result: Dict[str, str] = {}
    missing: Dict[str, List[str]] = {}
    for role, cand in roles.items():
        col = _find_first(df, cand)
        if col is None:
            missing[role] = cand
        else:
            result[role] = col
    if missing:
        available = list(df.columns)
        raise AssertionError(
            f"[{ctx}] Missing required roles for M5: {list(missing.keys())}\n"
            f"Looked for any of (normalized names): {json.dumps(missing, indent=2)}\n"
            f"Available columns: {available}"
        )
    return result

@dataclass
class M2LocateResult:
    pl_path: Path
    wc_path: Path

def _locate_m2_files(out_dir: Path) -> M2LocateResult:
    """Find M2 P&L and Working Capital schedules in outputs."""
    pl_candidates = [
        "m2_pl_schedule.parquet",
        "m2_profit_and_loss_schedule.parquet",
    ]
    wc_candidates = [
        "m2_working_capital_schedule.parquet",
        "m2_wc_schedule.parquet",
    ]
    pl_path = next((out_dir / n for n in pl_candidates if (out_dir / n).exists()), None)
    wc_path = next((out_dir / n for n in wc_candidates if (out_dir / n).exists()), None)
    if pl_path is None:
        raise FileNotFoundError("[M5] Could not find M2 P&L schedule parquet in outputs.")
    if wc_path is None:
        raise FileNotFoundError("[M5] Could not find M2 Working Capital schedule parquet in outputs.")
    return M2LocateResult(pl_path=pl_path, wc_path=wc_path)

# --------------------------------------------
# Contracts (roles we need from M2 for M5)
# --------------------------------------------

REQ_PL_ROLES = {
    # Depreciation & amortization
    "DA": [
        "Depreciation_and_Amortization",
        "DepreciationAmortization",
        "Depreciation",
        "DandA",
        "DA",
        "Depreciation_NAD_000",  # tolerant
    ],
    # Net profit after tax
    "NPAT": [
        "Net_Profit_After_Tax",
        "NPAT",
        "NPAT_NAD_000",  # tolerant
    ],
    # Period
    "MONTH_INDEX": [
        "Month_Index",
        "MONTH_INDEX",
        "Month",
        "Period",
    ],
}

REQ_WC_ROLES = {
    # Net working capital cash flow (sign convention per contract)
    "NWC_CF": [
        "Cash_Flow_from_NWC_Change",
        "Net_Working_Capital_CF",
        "Working_Capital_CF",
        "WC_Cash_Flow",
        "Cash_Flow_from_NWC_Change_NAD_000",  # tolerant
    ],
    "MONTH_INDEX": [
        "Month_Index",
        "MONTH_INDEX",
        "Month",
        "Period",
    ],
}

# --------------------------------------------
# Public entrypoint
# --------------------------------------------

def run_m5(out_dir: str | os.PathLike, currency: str, inspect_only: bool = False) -> None:
    """
    Compute M5 cash flow (operating section) from M2 outputs.
    Writes:
      - m5_cash_flow_statement_final.parquet
      - m5_smoke_report.md
      - m5_debug_dump.json (always)
    """
    out = Path(out_dir).resolve()
    out.mkdir(parents=True, exist_ok=True)

    # Locate inputs
    loc = _locate_m2_files(out)
    df_pl = pd.read_parquet(loc.pl_path)
    df_wc = pd.read_parquet(loc.wc_path)

    # Resolve roles
    map_pl = _normalize_columns(df_pl, REQ_PL_ROLES, "M5/PL")
    map_wc = _normalize_columns(df_wc, REQ_WC_ROLES, "M5/WC")

    # Build debug dump early (helps when inspect_only)
    debug = {
        "inputs": {
            "pl_path": str(loc.pl_path),
            "wc_path": str(loc.wc_path),
        },
        "resolved_columns": {
            "pl": map_pl,
            "wc": map_wc,
        },
        "currency": currency,
    }
    (out / "m5_debug_dump.json").write_text(json.dumps(debug, indent=2), encoding="utf-8")

    if inspect_only:
        print(f"[OK] Inspect-only: found PL= {loc.pl_path.name}, WC= {loc.wc_path.name}. "
              f"Debug -> {out / 'm5_debug_dump.json'}")
        return

    # Pull series using resolved columns (keep original column names to preserve suffix)
    s_month = df_pl[map_pl["MONTH_INDEX"]]
    s_npat  = pd.to_numeric(df_pl[map_pl["NPAT"]], errors="coerce").fillna(0.0)
    s_da    = pd.to_numeric(df_pl[map_pl["DA"]],   errors="coerce").fillna(0.0)

    # Align WC by month (left join on month index)
    df_wc_small = df_wc[[map_wc["MONTH_INDEX"], map_wc["NWC_CF"]]].copy()
    df_wc_small.columns = ["Month_Index_wc", "NWC_CF"]

    df = pd.DataFrame({
        "Month_Index": s_month,
        "NPAT": s_npat,
        "DA":   s_da,
    }).merge(df_wc_small, left_on="Month_Index", right_on="Month_Index_wc", how="left")
    df.drop(columns=["Month_Index_wc"], inplace=True)
    df["NWC_CF"] = pd.to_numeric(df["NWC_CF"], errors="coerce").fillna(0.0)

    # Simple CFO bridge (sign convention: NWC_CF already signed as cash flow)
    df["CFO"] = df["NPAT"] + df["DA"] + df["NWC_CF"]

    # Name outputs with currency suffix to match repo convention
    # (Assumes M2 inputs are in *_NAD_000 units; we keep units in column names where appropriate.)
    df.rename(columns={
        "NPAT":  f"NPAT_{currency}_000",
        "DA":    f"DA_{currency}_000",
        "NWC_CF":f"NWC_CF_{currency}_000",
        "CFO":   f"CFO_{currency}_000",
    }, inplace=True)

    # Persist
    out_path = out / "m5_cash_flow_statement_final.parquet"
    df.to_parquet(out_path, index=False)

    # Smoke report
    smoke = [
        "# M5 Smoke Test Report",
        "",
        f"- Rows: {len(df)}",
        f"- Columns: {', '.join(df.columns)}",
        f"- Wrote: {out_path}",
    ]
    (out / "m5_smoke_report.md").write_text("\n".join(smoke), encoding="utf-8")

    print(f"[OK] M5 cash flow computed -> {out_path.name}. Smoke -> {out / 'm5_smoke_report.md'}")
