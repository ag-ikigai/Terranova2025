
#!/usr/bin/env python
import argparse
from pathlib import Path
import re
import sys
import pandas as pd

from src.terra_nova.modules.m0_setup.engine import (
    load_and_validate_input_pack,
    create_calendar,
    create_opening_balance_sheet,
)

def _arrow_safe_strings(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    for c in out.columns:
        if pd.api.types.is_object_dtype(out[c]):
            out[c] = out[c].astype("string")
    return out

def _write_parquet(df: pd.DataFrame, path: Path) -> None:
    df2 = _arrow_safe_strings(df)
    try:
        df2.to_parquet(path, index=False)
    except Exception as exc:
        raise RuntimeError(f"Failed to write parquet at {path}. Hint: install pyarrow. Original error: {exc}") from exc

def _sanitize_filename(name: str) -> str:
    s = re.sub(r"[\\/:*?\"<>|]+", "_", name)
    return f"{s}.parquet"

def main():
    parser = argparse.ArgumentParser(description="Terra Nova — M0 CLI (v10 Input Pack)")
    sub = parser.add_subparsers(dest="cmd", required=True)
    p = sub.add_parser("fresh_m0", help="Run Module 0 (fresh outputs)")
    p.add_argument("--input", required=True, help="Path to TerraNova_Input_Pack_v10_0.xlsx")
    p.add_argument("--out", required=True, help="Output folder (e.g., C:\\TerraNova\\outputs)")
    p.add_argument("--currency", required=True, choices=["NAD", "USD"], help="Presentation currency")

    args = parser.parse_args()
    input_xlsx = Path(args.input)
    out_dir = Path(args.out)
    out_dir.mkdir(parents=True, exist_ok=True)

    sheets = load_and_validate_input_pack(input_xlsx)

    parameters_df = sheets.get("Parameters")
    if parameters_df is None:
        raise KeyError("Missing 'Parameters' sheet.")
    calendar_df = create_calendar(parameters_df)

    fx_df = sheets.get("FX_Path")
    if fx_df is None:
        raise KeyError("Missing 'FX_Path' sheet.")
    opening_bs_df = create_opening_balance_sheet(fx_df)

    m0_inputs_dir = out_dir / "m0_inputs"
    m0_inputs_dir.mkdir(parents=True, exist_ok=True)
    for sheet_name, df in sheets.items():
        fname = _sanitize_filename(sheet_name)
        _write_parquet(df, m0_inputs_dir / fname)

    _write_parquet(calendar_df, out_dir / "m0_calendar.parquet")
    _write_parquet(opening_bs_df, out_dir / "m0_opening_bs.parquet")

    smoke_path = out_dir / "m0_smoke_report.md"
    inputs_written = sorted([p.name for p in m0_inputs_dir.glob("*.parquet")])
    smoke_lines = [
        "# M0 Smoke Test Report",
        f"- Input pack: {input_xlsx}",
        f"- Outputs folder: {out_dir}",
        f"- Parquet files in m0_inputs: {len(inputs_written)}",
        "## Files",
        *(f"- {name}" for name in inputs_written),
        "## Artifacts",
        f"- m0_calendar.parquet: {(out_dir / 'm0_calendar.parquet').exists()}",
        f"- m0_opening_bs.parquet: {(out_dir / 'm0_opening_bs.parquet').exists()}",
        "M0 completed successfully.",
    ]
    smoke_path.write_text("\n".join(smoke_lines), encoding="utf-8")
    print("M0 finished. See smoke report at:", smoke_path)

if __name__ == "__main__":
    sys.exit(main())
