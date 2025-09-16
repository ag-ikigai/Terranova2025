from __future__ import annotations
import argparse, json
from datetime import datetime
from pathlib import Path
import pandas as pd

# Use existing engine helpers (no business-logic changes)
from .engine import (
    load_and_validate_input_pack,   # validates the v10 Input Pack
    create_calendar,                # builds calendar from Parameters
    create_opening_balance_sheet,   # builds opening cash from FX_Path
)

def _normalize_object_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Make heterogeneous object columns parquet-safe:
    - bytes/bytearray -> utf-8 strings
    - object dtype -> pandas StringDtype()
    Note: numeric/datetime columns remain unchanged.
    """
    out = df.copy()
    for col in out.columns:
        if pd.api.types.is_object_dtype(out[col].dtype):
            out[col] = out[col].map(
                lambda x: x.decode("utf-8", "ignore") if isinstance(x, (bytes, bytearray)) else x
            )
            out[col] = out[col].astype("string")
    return out

def run_m0(input_pack: str, out_dir: str) -> dict:
    """
    Full M0 export (no business-logic changes):
      1) Load & validate Input Pack (v10).
      2) Export EVERY sheet to outputs/m0_inputs/<Sheet>.parquet (columns preserved).
         Special FX case: export with Month -> Month_Index for downstream joins.
      3) Write m0_calendar.parquet using Parameters.
      4) Write m0_opening_bs.parquet using FX_Path.
      5) Emit smoke report + run log.
    """
    in_path = Path(input_pack)
    out_base = Path(out_dir)
    m0_inputs = out_base / "m0_inputs"
    m0_inputs.mkdir(parents=True, exist_ok=True)

    sheets = load_and_validate_input_pack(in_path)  # existing engine validation

    written: dict[str, int] = {}

    # 1) Export every sheet to outputs/m0_inputs
    for sheet_name, df in sheets.items():
        if df is None or df.empty:
            continue

        if sheet_name == "FX_Path":
            # Export FX with Month_Index for downstream joins (names frozen)
            fx = df.loc[:, ["Month", "NAD_per_USD"]].copy()
            fx.rename(columns={"Month": "Month_Index"}, inplace=True)
            fx = fx.astype({"Month_Index": "int64", "NAD_per_USD": "float64"})
            tgt = m0_inputs / "FX_Path.parquet"
            fx.to_parquet(tgt)
            written["m0_inputs/FX_Path.parquet"] = len(fx)
        else:
            # Normalize object columns so pyarrow doesn't choke on mixed types
            safe_df = _normalize_object_columns(df)
            tgt = m0_inputs / f"{sheet_name}.parquet"
            safe_df.to_parquet(tgt)
            written[f"m0_inputs/{sheet_name}.parquet"] = len(df)

    # 2) Calendar (Parameters -> m0_calendar.parquet)
    if "Parameters" in sheets and not sheets["Parameters"].empty:
        cal = create_calendar(sheets["Parameters"])
        cal_file = out_base / "m0_calendar.parquet"
        cal.to_parquet(cal_file)
        written["m0_calendar.parquet"] = len(cal)

    # 3) Opening Balance Sheet (FX_Path -> m0_opening_bs.parquet)
    if "FX_Path" in sheets and not sheets["FX_Path"].empty:
        obs = create_opening_balance_sheet(sheets["FX_Path"])
        obs_file = out_base / "m0_opening_bs.parquet"
        obs.to_parquet(obs_file)
        written["m0_opening_bs.parquet"] = len(obs)

    # 4) Smoke report
    smoke = out_base / "m0_smoke_report.md"
    lines = [f"# M0 Smoke Report — {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}", ""]
    for k, v in sorted(written.items()):
        lines.append(f"- {k}: {v} rows")
    smoke.write_text("\n".join(lines), encoding="utf-8")

    # 5) Run log
    run_log = out_base / "m0_run_log.json"
    run_log.write_text(
        json.dumps(
            {
                "started_at": datetime.now().isoformat(timespec="seconds"),
                "input_pack": str(in_path),
                "outputs_dir": str(out_base),
                "written": written,
            },
            indent=2,
        ),
        encoding="utf-8",
    )

    return written

def _cli():
    ap = argparse.ArgumentParser(description="Run M0 (full export) from main path.")
    ap.add_argument("--input", required=True, help="Path to TerraNova_Input_Pack_v10_0.xlsx")
    ap.add_argument("--out", required=True, help="Output directory (e.g., .\\outputs)")
    args = ap.parse_args()
    wrote = run_m0(args.input, args.out)
    print("[M0] Wrote:", json.dumps(wrote, indent=2))

if __name__ == "__main__":
    _cli()
