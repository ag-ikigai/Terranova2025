
#!/usr/bin/env python
import argparse
from pathlib import Path
import re
import sys
import pandas as pd

# M0 Imports
from src.terra_nova.modules.m0_setup.engine import (
    load_and_validate_input_pack,
    create_calendar,
    create_opening_balance_sheet,
)
# M1 Imports
from src.terra_nova.modules.m1_operational_engines.engine import (
    create_capex_and_depreciation_schedules,
    create_opex_schedule,
    calculate_steady_state_revenue,
    apply_ramps_and_scenarios,
    distribute_revenue_monthly,
)
# M2 Imports
from src.terra_nova.modules.m2_working_capital_pl.engine import (
    create_working_capital_schedules,
    create_pl_statement,
)

# --- Helper Functions ---
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
    s = re.sub(r'[\/:\*\?\"<>\|]+', "_", name)
    return f"{s.strip()}.parquet"

# --- Main CLI ---
def main():
    parser = argparse.ArgumentParser(description="Terra Nova Financial Simulator")
    sub = parser.add_subparsers(dest="cmd", required=True)

    # --- Command Definitions ---
    p0 = sub.add_parser("fresh_m0", help="Run Module 0 (Setup & Validation)")
    p1 = sub.add_parser("run_m1", help="Run Modules 0-1 (Operational Engines)")
    p2 = sub.add_parser("run_m2", help="Run Modules 0-2 (WC & P&L)")

    for p in [p0, p1, p2]:
        p.add_argument("--input", required=True, help="Path to TerraNova_Input_Pack_v10_0.xlsx")
        p.add_argument("--out", required=True, help="Output folder")
        p.add_argument("--currency", required=True, choices=["NAD", "USD"], help="Presentation currency")

    args = parser.parse_args()
    input_xlsx = Path(args.input)
    out_dir = Path(args.out)
    out_dir.mkdir(parents=True, exist_ok=True)

    # --- M0 Pipeline ---
    sheets = load_and_validate_input_pack(input_xlsx)
    parameters_df = sheets.get("Parameters")
    if parameters_df is None: raise KeyError("Missing 'Parameters' sheet.")
    calendar_df = create_calendar(parameters_df)

    if args.cmd == 'fresh_m0':
        m0_inputs_dir = out_dir / "m0_inputs"
        m0_inputs_dir.mkdir(parents=True, exist_ok=True)
        for sheet_name, df in sheets.items():
            _write_parquet(df, m0_inputs_dir / _sanitize_filename(sheet_name))

        fx_df = sheets.get("FX_Path")
        if fx_df is None: raise KeyError("Missing 'FX_Path' sheet.")
        opening_bs_df = create_opening_balance_sheet(fx_df)

        _write_parquet(calendar_df, out_dir / "m0_calendar.parquet")
        _write_parquet(opening_bs_df, out_dir / "m0_opening_bs.parquet")

        smoke_path = out_dir / "m0_smoke_report.md"
        smoke_path.write_text(f"# M0 Smoke Test Report\n\n- Run successful for command: {args.cmd}\n- Input pack: {input_xlsx}\n- Outputs folder: {out_dir}\n\nM0 completed successfully.", encoding="utf-8")
        print(f"M0 finished. See smoke report at: {smoke_path}")
        return 0

    # --- M1 Pipeline ---
    case_selector_df = sheets.get("Case_Selector")
    active_case_name = case_selector_df.loc[case_selector_df['Key'] == 'Default_Case', 'Value'].iloc[0]
    case_library_df = sheets.get("Case_Library")
    active_case = case_library_df[case_library_df['Case_Name'] == active_case_name].iloc[0]

    price_mult = active_case['Price_Mult']
    yield_mult = active_case['Yield_Mult']
    opex_mult = active_case['OPEX_Mult']

    m1_capex_df, m1_dep_df = create_capex_and_depreciation_schedules(sheets.get("CAPEX_Schedule"), calendar_df)
    m1_opex_df = create_opex_schedule(sheets.get("OPEX_Detail"), calendar_df, opex_mult)
    steady_state_rev_df = calculate_steady_state_revenue(sheets.get("Revenue_Assumptions"))
    adj_annual_df = apply_ramps_and_scenarios(steady_state_rev_df, sheets.get("Rev_Ramp_Seasonality"), price_mult, yield_mult)
    m1_revenue_df = distribute_revenue_monthly(adj_annual_df, sheets.get("Rev_Ramp_Seasonality"), calendar_df)

    if args.cmd == 'run_m1':
        _write_parquet(m1_capex_df, out_dir / "m1_capex_schedule.parquet")
        _write_parquet(m1_dep_df, out_dir / "m1_depreciation_schedule.parquet")
        _write_parquet(m1_opex_df, out_dir / "m1_opex_schedule.parquet")
        _write_parquet(m1_revenue_df, out_dir / "m1_revenue_schedule.parquet")

        smoke_path = out_dir / "m1_smoke_report.md"
        smoke_path.write_text(f"# M1 Smoke Test Report\n\n- Run successful for command: {args.cmd}\n\nM1 completed successfully.", encoding="utf-8")
        print(f"M1 finished. See smoke report at: {smoke_path}")
        return 0

    # --- M2 Pipeline ---
    if args.cmd == 'run_m2':
        working_capital_tax_df = sheets.get("Working_Capital_Tax")
        m2_wc_df = create_working_capital_schedules(m1_revenue_df, m1_opex_df, working_capital_tax_df, calendar_df)
        m2_pl_df = create_pl_statement(m1_revenue_df, m1_opex_df, m1_dep_df, working_capital_tax_df, parameters_df)

        _write_parquet(m2_wc_df, out_dir / "m2_working_capital_schedule.parquet")
        _write_parquet(m2_pl_df, out_dir / "m2_pl_schedule.parquet")

        smoke_path = out_dir / "m2_smoke_report.md"
        smoke_path.write_text(f"# M2 Smoke Test Report\n\n- Run successful for command: {args.cmd}\n\nM2 completed successfully.", encoding="utf-8")
        print(f"M2 finished. See smoke report at: {smoke_path}")
        return 0

if __name__ == "__main__":
    sys.exit(main())
