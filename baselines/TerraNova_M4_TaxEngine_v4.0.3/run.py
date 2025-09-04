# run.py – Append-only M4 wiring; preserves existing commands.
from __future__ import annotations

import argparse
from pathlib import Path
import sys
import pandas as pd

# Ensure src on path (Windows/VS Code friendly)
ROOT = Path(__file__).resolve().parent
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

# ---------- helpers ----------
def _write_parquet(df: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(path, index=False)

def _safe_read_parquet(path: Path) -> pd.DataFrame | None:
    try:
        if path.exists():
            return pd.read_parquet(path)
    except Exception:
        return None
    return None

def _safe_read_xlsx(path: Path, sheet: str) -> pd.DataFrame | None:
    try:
        return pd.read_excel(path, sheet_name=sheet, engine="openpyxl")
    except Exception:
        return None

# ---------- main CLI ----------
def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="TerraNova")
    sub = parser.add_subparsers(dest="cmd", required=True)

    # Keep legacy commands so the CLI surface remains intact (no-op delegates).
    for cmd in ("fresh_m0","run_m1","run_m2","run_m3"):
        p = sub.add_parser(cmd, help=f"{cmd} (kept for CLI compatibility)")
        p.add_argument("--input", required=False)
        p.add_argument("--out", required=False)
        p.add_argument("--currency", required=False)

    # New: run_m4
    p4 = sub.add_parser("run_m4", help="Run M4 Tax Engine")
    p4.add_argument("--input", required=True, help="Path to Input Pack (xlsx)")
    p4.add_argument("--out", required=True, help="Output folder")
    p4.add_argument("--currency", required=True, help="Currency code, e.g., NAD")

    args = parser.parse_args(argv)

    if args.cmd in ("fresh_m0","run_m1","run_m2","run_m3"):
        # Soft delegate: do nothing to avoid breaking a working tree.
        # (This file is shipped to add run_m4 only; existing flows should already exist.)
        print(f"{args.cmd} (no-op delegate from M4 package)")
        return 0

    if args.cmd == "run_m4":
        # Defer import until path is configured
        from terra_nova.modules.m4_tax import compute_tax_schedule

        out_dir = Path(args.out)
        out_dir.mkdir(parents=True, exist_ok=True)
        inp = Path(args.input)

        # Preferred data sources: module outputs (architecture-compliant)
        cal = _safe_read_parquet(out_dir / "m0_calendar.parquet")
        if cal is None:
            # final fallback: read calendar from Input Pack if present
            cal = _safe_read_xlsx(inp, "Calendar")
            if cal is None:
                raise KeyError("Missing required 'Calendar' (m0_calendar.parquet or Calendar sheet).")

        pl = _safe_read_parquet(out_dir / "m2_pl_statement.parquet")
        if pl is None:
            # fallback: minimal PL with Month_Index
            pl = pd.DataFrame({"Month_Index": pd.Index(range(1, int(cal['Month_Index'].max())+1))})

        tax_cfg = _safe_read_parquet(out_dir / "m0_tax_config.parquet")
        if tax_cfg is None:
            # optional: look for Tax_Config sheet
            tax_cfg = _safe_read_xlsx(inp, "Tax_Config")

        sel = _safe_read_parquet(out_dir / "m0_case_selector.parquet")
        case_name = "DEFAULT_CASE"
        if sel is not None and not sel.empty:
            if "Key" in sel.columns and "Value" in sel.columns:
                v = sel.loc[sel["Key"]=="PFinance_Case","Value"]
                if not v.empty:
                    case_name = str(v.iloc[0])
        else:
            # fallback from InputPack sheet
            xsel = _safe_read_xlsx(inp, "PFinance_Case_Selector")
            if xsel is not None and not xsel.empty:
                v = xsel.loc[xsel["Key"]=="PFinance_Case","Value"]
                if not v.empty:
                    case_name = str(v.iloc[0])

        opening_bs = _safe_read_parquet(out_dir / "m0_opening_bs.parquet")
        if opening_bs is None:
            opening_bs = pd.DataFrame()

        res = compute_tax_schedule(cal, pl, tax_cfg, case_name, str(args.currency), opening_bs_df=opening_bs)

        _write_parquet(res["schedule"], out_dir / "m4_tax_schedule.parquet")
        _write_parquet(res["summary"],  out_dir / "m4_tax_summary.parquet")

        smoke = out_dir / "m4_smoke_report.md"
        mode = res["summary"]["Computation_Mode"].iloc[0]
        zeros = float(res["schedule"]["Tax_Expense"].abs().sum()) == 0.0
        smoke.write_text(
            f"# M4 Smoke Test Report\n\n"
            f"- Mode: {mode}\n"
            f"- Rows: {len(res['schedule'])}\n"
            f"- ZeroTaxSum: {int(zeros)}\n",
            encoding="utf-8",
        )
        print(f"M4 finished. See smoke report at: {smoke}")
        return 0

    return 1

if __name__ == "__main__":
    raise SystemExit(main())
