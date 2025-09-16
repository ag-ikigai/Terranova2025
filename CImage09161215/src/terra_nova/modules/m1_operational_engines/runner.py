# C:\TerraNova\src\terra_nova\modules\m1_operational_engines\runner.py
from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Dict

import pandas as pd

from .engine import (
    load_and_validate_input_pack,
    build_revenue_schedule,
    build_opex_schedule,
    build_capex_schedule,
    build_depreciation_schedule,
    _ensure_dir,
    _norm_objects_for_parquet,
)

def _first_nonzero_month(df: pd.DataFrame, col: str) -> int | None:
    s = pd.to_numeric(df[col], errors="coerce").fillna(0.0)
    pos = s[s > 0].index
    return int(df.loc[pos.min(), "Month_Index"]) if len(pos) else None


def run_m1(
    input_pack_path: str | Path,
    out_dir: str | Path,
    currency: str = "NAD",
    months: int | None = None,
    diagnostic: bool = False,
) -> Dict[str, int]:
    """
    Emits all M1 artifacts (canonical filenames & columns):
      - m1_revenue_schedule.parquet  (Month_Index, Crop, Monthly_Revenue_NAD_000)
      - m1_opex_schedule.parquet     (Month_Index, Monthly_OPEX_NAD_000)
      - m1_capex_schedule.parquet    (Month_Index, Monthly_CAPEX_NAD_000)
      - m1_depreciation_schedule.parquet (Month_Index, Monthly_Depreciation_NAD_000)
    """
    ip = Path(input_pack_path)
    out = Path(out_dir)
    _ensure_dir(out)

    sheets = load_and_validate_input_pack(ip)  # validated per v10 contract
    if months is None:
        # Try to read horizon from Parameters; default to 60.
        months = 60
        if "Parameters" in sheets and not sheets["Parameters"].empty:
            p = sheets["Parameters"]
            if {"Key", "Value"}.issubset(p.columns):
                sel = p.loc[p["Key"] == "HORIZON_MONTHS", "Value"]
                if not sel.empty:
                    try:
                        months = int(sel.iloc[0])
                    except Exception:
                        months = 60

    # Build tables
    rev_df = build_revenue_schedule(sheets, months)
    opex_df = build_opex_schedule(sheets, months)
    capex_df = build_capex_schedule(sheets, months)
    dep_df = build_depreciation_schedule(sheets, months)

    # Write parquet (normalize objects for Arrow)
    wrote: Dict[str, int] = {}
    (_norm_objects_for_parquet(rev_df)).to_parquet(out / "m1_revenue_schedule.parquet", index=False)
    wrote["m1_revenue_schedule.parquet"] = len(rev_df)

    (_norm_objects_for_parquet(opex_df)).to_parquet(out / "m1_opex_schedule.parquet", index=False)
    wrote["m1_opex_schedule.parquet"] = len(opex_df)

    (_norm_objects_for_parquet(capex_df)).to_parquet(out / "m1_capex_schedule.parquet", index=False)
    wrote["m1_capex_schedule.parquet"] = len(capex_df)

    (_norm_objects_for_parquet(dep_df)).to_parquet(out / "m1_depreciation_schedule.parquet", index=False)
    wrote["m1_depreciation_schedule.parquet"] = len(dep_df)

    # Smoke report (keeps your existing pattern)
    lines = ["# M1 Smoke Report", ""]
    fnz = _first_nonzero_month(rev_df, "Monthly_Revenue_NAD_000")
    lines.append(f"- First non‑zero revenue month (total): {fnz}")
    lines.append(f"- Horizon months: {months}")
    (out / "m1_smoke_report.md").write_text("\n".join(lines), encoding="utf-8")

    # Debug payload
    debug = {
        "input_pack": str(ip),
        "months": months,
        "first_nonzero_revenue_month": fnz,
        "row_counts": wrote,
    }
    (out / "m1_debug.json").write_text(json.dumps(debug, indent=2), encoding="utf-8")

    return wrote


def _cli():
    ap = argparse.ArgumentParser(description="Run M1 (revenue+opex+capex+depreciation) from main path.")
    ap.add_argument("--input", required=True, help="Path to TerraNova_Input_Pack_v10_0.xlsx")
    ap.add_argument("--out", required=True, help="Output directory (e.g., .\\outputs)")
    ap.add_argument("--currency", default="NAD")
    ap.add_argument("--months", type=int, default=None)
    ap.add_argument("--diagnostic", action="store_true")
    args = ap.parse_args()
    wrote = run_m1(args.input, args.out, currency=args.currency, months=args.months, diagnostic=args.diagnostic)
    print("[M1] Wrote:", json.dumps(wrote, indent=2))


if __name__ == "__main__":
    _cli()
