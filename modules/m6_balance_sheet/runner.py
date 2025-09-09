from __future__ import annotations
import json, os
from pathlib import Path
import pandas as pd
from .engine import compute_balance_sheet, ROLE, _pick

def _read_parquet(p: Path) -> pd.DataFrame:
    return pd.read_parquet(p)

def _find_one(out_dir: Path, names: list[str]) -> Path | None:
    for n in names:
        p = out_dir / n
        if p.exists():
            return p
    return None

def _discover_artifacts(out_dir: Path) -> dict:
    """Find required upstream artifacts with our current filenames."""
    cand = {
        "m2_pl": ["m2_pl_schedule.parquet"],
        "m2_wc": ["m2_working_capital_schedule.parquet"],
        "m3_debt": ["m3_revolver_schedule.parquet"],
        "m4_tax": ["m4_tax_schedule.parquet"],
        # optional M5 (not used for v1 calc, included in debug)
        "m5_cfo": ["m5_cash_flow_statement_final.parquet"],
    }
    found = {}
    for k, lst in cand.items():
        p = _find_one(out_dir, lst)
        if p:
            found[k] = p
    if not all(k in found for k in ["m2_pl", "m2_wc", "m3_debt", "m4_tax"]):
        missing = [k for k in ["m2_pl","m2_wc","m3_debt","m4_tax"] if k not in found]
        raise FileNotFoundError(f"[M6] Missing required upstream artifacts: {missing}")
    return {k: str(v) for k, v in found.items()}

def _normalize_month_index(df: pd.DataFrame) -> pd.DataFrame:
    mi = _pick(df, ROLE["MONTH_INDEX"])
    if not mi:
        raise AssertionError("[M6] Month_Index not found")
    if mi != "Month_Index":
        df = df.rename(columns={mi: "Month_Index"})
    df["Month_Index"] = df["Month_Index"].astype(int)
    return df

def run_m6(out_dir: str, currency: str, inspect_only: bool = False, start_share_capital: float = 0.0) -> None:
    """
    Build M6 balance sheet (beta). Outputs:
      - outputs/m6_balance_sheet.parquet
      - outputs/m6_smoke_report.md
      - outputs/m6_debug_dump.json
    """
    out = Path(out_dir).resolve()
    out.mkdir(parents=True, exist_ok=True)

    found = _discover_artifacts(out)
    dbg = {"found": found, "currency": currency, "notes": "M6 beta: cash is balancing item; wiring in v7.5"}

    if inspect_only:
        (out / "m6_debug_dump.json").write_text(json.dumps(dbg, indent=2))
        print(f"[OK] Inspect-only: found all inputs. Debug -> {out / 'm6_debug_dump.json'}")
        return

    # Load
    m2_pl = _normalize_month_index(_read_parquet(Path(found["m2_pl"])))
    m2_wc = _normalize_month_index(_read_parquet(Path(found["m2_wc"])))
    m3_debt = _normalize_month_index(_read_parquet(Path(found["m3_debt"])))
    m4_tax = _normalize_month_index(_read_parquet(Path(found["m4_tax"])))
    if "m5_cfo" in found:
        try:
            m5_cfo = _normalize_month_index(_read_parquet(Path(found["m5_cfo"])))
            dbg["m5_cfo_columns"] = list(m5_cfo.columns)
        except Exception as e:
            dbg["m5_cfo_error"] = str(e)

    # Compute
    bs = compute_balance_sheet(m2_pl, m2_wc, m3_debt, m4_tax, currency, start_share_capital)

    # Write artifacts
    out_file = out / "m6_balance_sheet.parquet"
    bs.to_parquet(out_file, index=False)

    # Smoke
    ident_diff = (bs["Assets_Total_NAD_000"] - bs["Liabilities_And_Equity_Total_NAD_000"]).abs().max()
    smoke = [
        "# M6 Smoke Report",
        f"- rows: {len(bs)}",
        f"- identity max abs diff: {ident_diff:.6f}",
        "- columns: " + ", ".join(bs.columns),
        "",
        "Notes:",
        "- Cash is a balancing item in v1; will be replaced by true cash once CAPEX & Equity wiring (v7.5) lands.",
    ]
    (out / "m6_smoke_report.md").write_text("\n".join(smoke))

    # Debug
    dbg["out_file"] = str(out_file)
    dbg["columns"] = list(bs.columns)
    (out / "m6_debug_dump.json").write_text(json.dumps(dbg, indent=2))

    print(f"[OK] M6 balance sheet -> {out_file}. Smoke -> {out / 'm6_smoke_report.md'}")
