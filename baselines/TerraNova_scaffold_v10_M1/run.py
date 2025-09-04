
#!/usr/bin/env python
import argparse
from pathlib import Path
import re
import sys
import pandas as pd

from src.terra_nova.modules.m0_setup.engine import (
    load_and_validate_input_pack,
    create_calendar,
)
from src.terra_nova.modules.m1_operational_engines.engine import (
    create_capex_and_depreciation_schedules,
    create_opex_schedule,
    calculate_steady_state_revenue,
    apply_ramps_and_scenarios,
    distribute_revenue_monthly,
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
        raise RuntimeError(
            f"Failed to write parquet at {path}. Hint: install pyarrow. Original error: {exc}"
        ) from exc

def _sanitize_filename(name: str) -> str:
    s = re.sub(r"[\\/:\*\?\"<>\|]+", "_", name)
    s = s.strip()
    return f"{s}.parquet"

def main():
    parser = argparse.ArgumentParser(description="Terra Nova — M0/M1 CLI (v10 Input Pack)")
    sub = parser.add_subparsers(dest="cmd", required=True)

    # fresh_m0
    p0 = sub.add_parser("fresh_m0", help="Run Module 0 (validate + calendar) and dump inputs")
    p0.add_argument("--input", required=True, help="Path to TerraNova_Input_Pack_v10_0.xlsx")
    p0.add_argument("--out", required=True, help="Output folder (e.g., C:\\TerraNova\\outputs)")
    p0.add_argument("--currency", required=True, choices=["NAD", "USD"], help="Presentation currency")

    # run_m1
    p1 = sub.add_parser("run_m1", help="Run Module 1 (CAPEX/OPEX/Revenue) end-to-end (runs M0 first)")
    p1.add_argument("--input", required=True, help="Path to TerraNova_Input_Pack_v10_0.xlsx")
    p1.add_argument("--out", required=True, help="Output folder (e.g., C:\\TerraNova\\outputs)")
    p1.add_argument("--currency", required=True, choices=["NAD", "USD"], help="Presentation currency")

    args = parser.parse_args()
    input_xlsx = Path(args.input)
    out_dir = Path(args.out)
    out_dir.mkdir(parents=True, exist_ok=True)

    if args.cmd == "fresh_m0":
        sheets = load_and_validate_input_pack(input_xlsx)
        parameters_df = sheets.get("Parameters")
        if parameters_df is None:
            raise KeyError("Missing 'Parameters' sheet.")
        calendar_df = create_calendar(parameters_df)

        # Dump validated input sheets
        m0_inputs_dir = out_dir / "m0_inputs"
        m0_inputs_dir.mkdir(parents=True, exist_ok=True)
        for sheet_name, df in sheets.items():
            fname = _sanitize_filename(sheet_name)
            _write_parquet(df, m0_inputs_dir / fname)

        # Save calendar
        _write_parquet(calendar_df, out_dir / "m0_calendar.parquet")

        # Smoke report
        smoke_path = out_dir / "m0_smoke_report.md"
        inputs_written = sorted(p.name for p in m0_inputs_dir.glob("*.parquet"))
        smoke_lines = [
            "# M0 Smoke Test Report",
            f"- Input pack: {input_xlsx}",
            f"- Outputs folder: {out_dir}",
            f"- Parquet files in m0_inputs: {len(inputs_written)}",
            "## Files",
            *[f"- {name}" for name in inputs_written],
            "",
            "## Artifacts",
            f"- m0_calendar.parquet: {(out_dir / 'm0_calendar.parquet').exists()}",
            "M0 completed successfully.",
        ]
        smoke_path.write_text("\n".join(smoke_lines), encoding="utf-8")
        print("M0 finished. See smoke report at:", smoke_path)
        sys.exit(0)

    elif args.cmd == "run_m1":
        # 1) M0: load, validate, build calendar
        sheets = load_and_validate_input_pack(input_xlsx)
        parameters_df = sheets.get("Parameters")
        if parameters_df is None:
            raise KeyError("Missing 'Parameters' sheet.")
        calendar_df = create_calendar(parameters_df).copy()

        # Ensure Year & Month columns exist for monthly mapping
        if not {"Year", "Month"}.issubset(calendar_df.columns):
            calendar_df["Year"] = ((calendar_df["Month_Index"] - 1) // 12) + 1
            calendar_df["Month"] = ((calendar_df["Month_Index"] - 1) % 12) + 1

        # 2) Select active operational case & multipliers
        case_selector = sheets.get("Case_Selector")
        case_library = sheets.get("Case_Library")
        if case_selector is None or case_library is None:
            raise KeyError("Missing 'Case_Selector' or 'Case_Library' sheet.")

        default_case_row = case_selector.loc[case_selector["Key"] == "Default_Case", "Value"]
        if default_case_row.empty:
            raise ValueError("Default_Case not found in Case_Selector.")
        selected_case = str(default_case_row.iloc[0])

        row = case_library.loc[case_library["Case_Name"] == selected_case]
        if row.empty:
            raise ValueError(f"Selected case '{selected_case}' not found in Case_Library.")

        price_mult = float(row["Price_Mult"].iloc[0])
        yield_mult = float(row["Yield_Mult"].iloc[0])
        opex_mult  = float(row["OPEX_Mult"].iloc[0])

        # 3) Execute M1 engines
        capex_schedule_df        = sheets.get("CAPEX_Schedule")
        opex_detail_df           = sheets.get("OPEX_Detail")
        revenue_assumptions_df   = sheets.get("Revenue_Assumptions")
        rev_ramp_seasonality_df  = sheets.get("Rev_Ramp_Seasonality")

        if any(df is None for df in [capex_schedule_df, opex_detail_df, revenue_assumptions_df, rev_ramp_seasonality_df]):
            raise KeyError("One or more required sheets are missing (CAPEX_Schedule, OPEX_Detail, Revenue_Assumptions, Rev_Ramp_Seasonality).")

        # CAPEX + Depreciation
        m1_capex_df, m1_dep_df = create_capex_and_depreciation_schedules(capex_schedule_df, calendar_df)

        # OPEX (apply multiplier)
        m1_opex_df = create_opex_schedule(opex_detail_df, calendar_df, opex_mult)

        # Revenue steady → ramps → monthly
        steady_df = calculate_steady_state_revenue(revenue_assumptions_df)
        adj_annual_df = apply_ramps_and_scenarios(steady_df, rev_ramp_seasonality_df, price_mult, yield_mult)
        m1_revenue_df = distribute_revenue_monthly(adj_annual_df, rev_ramp_seasonality_df, calendar_df)

        # 4) Save outputs
        _write_parquet(calendar_df, out_dir / "m0_calendar.parquet")
        _write_parquet(m1_capex_df,  out_dir / "m1_capex_schedule.parquet")
        _write_parquet(m1_dep_df,    out_dir / "m1_depreciation_schedule.parquet")
        _write_parquet(m1_opex_df,   out_dir / "m1_opex_schedule.parquet")
        _write_parquet(m1_revenue_df,out_dir / "m1_revenue_schedule.parquet")

        # Update / create smoke report
        smoke_path = out_dir / "m0_smoke_report.md"
        lines = []
        if smoke_path.exists():
            lines = smoke_path.read_text(encoding="utf-8").splitlines()
        lines += [
            "",
            "## Module 1 Artifacts",
            f"- m1_capex_schedule.parquet: {(out_dir / 'm1_capex_schedule.parquet').exists()}",
            f"- m1_depreciation_schedule.parquet: {(out_dir / 'm1_depreciation_schedule.parquet').exists()}",
            f"- m1_opex_schedule.parquet: {(out_dir / 'm1_opex_schedule.parquet').exists()}",
            f"- m1_revenue_schedule.parquet: {(out_dir / 'm1_revenue_schedule.parquet').exists()}",
            "M1 completed successfully.",
        ]
        smoke_path.write_text("\n".join(lines), encoding="utf-8")
        print("M1 finished. See smoke report at:", smoke_path)

    else:
        parser.error(f"Unknown command: {args.cmd}")

if __name__ == "__main__":
    sys.exit(main())
