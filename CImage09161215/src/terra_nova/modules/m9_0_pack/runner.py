# src/terra_nova/modules/m9_0_pack/runner.py
from __future__ import annotations

import json
from pathlib import Path
from typing import Dict, List, Tuple

import pandas as pd

# ---------- tiny logger helpers ----------
def _p(msg: str) -> None:
    print(msg, flush=True)

def _ok(msg: str) -> None:
    _p(f"[M9.0][OK]  {msg}")

def _info(msg: str) -> None:
    _p(f"[M9.0][INFO] {msg}")

def _warn(msg: str) -> None:
    _p(f"[M9.0][WARN] {msg}")

def _fail(msg: str) -> None:
    raise RuntimeError(f"[M9.0][FAIL] {msg}")

# ---------- io helpers ----------
def _read_parquet_safe(p: Path, what: str) -> pd.DataFrame:
    if not p.exists():
        _fail(f"Missing {what}: {p}")
    df = pd.read_parquet(p)
    if df.empty:
        _fail(f"Empty {what}: {p}")
    return df

def _read_json_safe(p: Path, what: str):
    if not p.exists():
        _fail(f"Missing {what}: {p}")
    with p.open("r", encoding="utf-8") as f:
        return json.load(f)

def _ensure_dir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)

# ---------- excel export (robust engine handling) ----------
def _choose_excel_engine() -> Tuple[str, str]:
    """
    Prefer XlsxWriter for formatting; fall back to openpyxl if not installed.
    Returns (engine_name, note).
    """
    try:
        import xlsxwriter  # noqa: F401
        return "xlsxwriter", "xlsxwriter"
    except Exception:
        try:
            import openpyxl  # noqa: F401
            return "openpyxl", "openpyxl"
        except Exception:
            return "", "none"

def _estimate_col_widths(df: pd.DataFrame, max_width: int = 50) -> List[int]:
    # crude but effective; avoids huge widths
    widths: List[int] = []
    for c in df.columns:
        s = df[c].astype(str)
        w = max(10, min(max_width, int(s.str.len().quantile(0.80)) + 2))
        widths.append(w)
    return widths

def _export_excel(path: Path, sheets: Dict[str, pd.DataFrame]) -> Tuple[bool, str]:
    engine, note = _choose_excel_engine()
    if not engine:
        return False, "No Excel engine found (install xlsxwriter or openpyxl)."

    try:
        with pd.ExcelWriter(path, engine=engine) as writer:
            for sheet_name, df in sheets.items():
                # Excel sheet name limit (31); trim safely
                safe_name = (sheet_name[:31]) if len(sheet_name) > 31 else sheet_name
                df.to_excel(writer, index=False, sheet_name=safe_name)

                # Only format when using XlsxWriter
                if engine == "xlsxwriter":
                    ws = writer.sheets[safe_name]
                    widths = _estimate_col_widths(df)
                    for idx, w in enumerate(widths):
                        ws.set_column(idx, idx, w)
                    # bold header
                    workbook = writer.book
                    hdr_fmt = workbook.add_format({"bold": True})
                    ws.set_row(0, None, hdr_fmt)
        return True, f"engine={note}"
    except Exception as e:
        return False, f"{type(e).__name__}: {e}"

def _export_csv(dir_path: Path, sheets: Dict[str, pd.DataFrame]) -> None:
    for sheet_name, df in sheets.items():
        out = dir_path / f"{sheet_name}.csv"
        df.to_csv(out, index=False)
        _ok(f"CSV → {out.name}")

# ---------- main ----------
def run_m9_0(outputs_dir: str, base_currency: str = "NAD",
            strict: bool = True, export_excel: bool = True,
            export_csv: bool = True) -> None:
    out = Path(outputs_dir)
    _info(f"Starting M9.0 pack in: {out}")

    # ---- Load everything we already produced upstream (names as in your logs) ----
    # M7.5B
    pl = _read_parquet_safe(out / "m7_5b_profit_and_loss.parquet", "M7.5B P&L")
    _ok(f"Loaded M7.5B P&L ({len(pl)} rows).")
    bs = _read_parquet_safe(out / "m7_5b_balance_sheet.parquet", "M7.5B BS")
    _ok(f"Loaded M7.5B BS ({len(bs)} rows).")
    cf = _read_parquet_safe(out / "m7_5b_cash_flow.parquet", "M7.5B CF")
    _ok(f"Loaded M7.5B CF ({len(cf)} rows).")

    # M8.B1
    b1 = _read_parquet_safe(out / "m8b_base_timeseries.parquet", "M8.B1 base timeseries")
    _ok(f"Loaded M8.B1 base timeseries ({len(b1)} rows).")
    fxp = out / "m8b_fx_curve.parquet"
    if fxp.exists():
        fx = pd.read_parquet(fxp)
        _ok(f"Loaded M8.B1 FX curve ({len(fx)} rows).")
    else:
        fx = pd.DataFrame()
        _warn("FX curve not found (optional).")

    # M8.B2
    prom_m = _read_parquet_safe(out / "m8b2_promoter_scorecard_monthly.parquet", "M8.B2 promoter monthly")
    _ok(f"Loaded M8.B2 promoter monthly ({len(prom_m)} rows).")
    prom_y = _read_parquet_safe(out / "m8b2_promoter_scorecard_yearly.parquet", "M8.B2 promoter yearly")
    _ok(f"Loaded M8.B2 promoter yearly ({len(prom_y)} rows).")

    # M8.B3
    inv_sel = _read_parquet_safe(out / "m8b_investor_metrics_selected.parquet", "M8.B3 investor metrics (selected instrument)")
    _ok(f"Loaded M8.B3 investor metrics (selected instrument) ({len(inv_sel)} rows).")
    terms_path = out / "m8b_terms.json"
    if terms_path.exists():
        terms = _read_json_safe(terms_path, "M8.B3 terms")
        _ok("Loaded M8.B3 terms (instrument selection + FX context).")
    else:
        terms = {}
        _warn("M8.B3 terms not found (optional).")

    # M8.B4
    lend_m = _read_parquet_safe(out / "m8b4_lender_metrics_monthly.parquet", "M8.B4 lender monthly")
    _ok(f"Loaded M8.B4 lender monthly ({len(lend_m)} rows).")
    lend_y = _read_parquet_safe(out / "m8b4_lender_metrics_yearly.parquet", "M8.B4 lender yearly")
    _ok(f"Loaded M8.B4 lender yearly ({len(lend_y)} rows).")
    dbg4 = out / "m8b4_debug.json"
    if dbg4.exists():
        _ok("Loaded M8.B4 debug.")
    else:
        _warn("M8.B4 debug not found (optional).")

    # M8.B5
    bench_vals = _read_parquet_safe(out / "m8b_benchmarks.values.parquet", "M8.B5 benchmark values")
    _ok(f"Loaded M8.B5 benchmark values ({len(bench_vals)} rows).")
    bench_cat = _read_json_safe(out / "m8b_benchmarks.catalog.json", "M8.B5 benchmark catalog")
    _ok("Loaded M8.B5 benchmark catalog.")

    # M8.B6
    ifrs = _read_parquet_safe(out / "m8b_ifrs_statements.parquet", "M8.B6 IFRS statements")
    _ok(f"Loaded M8.B6 IFRS statements ({len(ifrs)} rows).")
    ifrs_map = _read_json_safe(out / "m8b_ifrs_mapping.json", "M8.B6 IFRS mapping")
    _ok("Loaded M8.B6 IFRS mapping.")
    ifrs_notes = _read_json_safe(out / "m8b_ifrs_notes.json", "M8.B6 IFRS notes")
    _ok("Loaded M8.B6 IFRS notes.")

    # Existing manifest (optional)
    manifest_path = out / "m9_manifest.json"
    manifest = {}
    if manifest_path.exists():
        manifest = _read_json_safe(manifest_path, "M9 manifest (existing)")
        _ok("Loaded M9 manifest (existing).")

    # ---- Assemble sheet map for exports ----
    sheets: Dict[str, pd.DataFrame] = {
        "M7_5B_PL": pl,
        "M7_5B_BS": bs,
        "M7_5B_CF": cf,
        "Base_Timeseries": b1,
        "FX_Curve": fx if not fx.empty else pd.DataFrame([{"note": "FX curve not found"}]),
        "Promoter_Monthly": prom_m,
        "Promoter_Yearly": prom_y,
        "Investor_Selected": inv_sel,
        "Lender_Monthly": lend_m,
        "Lender_Yearly": lend_y,
        "Benchmark_Values": bench_vals,
    }
    _ok(f"Will export {len(sheets)} sheets.")

    # ---- Exports ----
    debug: Dict[str, object] = {
        "base_currency": base_currency,
        "row_counts": {k: int(len(v)) for k, v in sheets.items()},
    }

    if export_excel:
        excel_path = out / "m9_0_pack.xlsx"
        success, note = _export_excel(excel_path, sheets)
        if success:
            _ok(f"Excel → {excel_path.name} ({note})")
            debug["excel_engine"] = note
        else:
            _warn(f"Excel export skipped/fell back: {note}")

    if export_csv:
        _export_csv(out, sheets)

    # ---- Manifest (we keep your existing one, only ensure the minimal shape) ----
    manifest.setdefault("branding", {}).setdefault("project_logo", "B1_Terra Nova Project Logo.jpg")
    manifest.setdefault("audiences", ["promoters", "investors", "lenders"])
    manifest.setdefault("currency_toggle", ["NAD", "USD"])
    with manifest_path.open("w", encoding="utf-8") as f:
        json.dump(manifest, f, indent=2)
    _ok("Manifest → m9_manifest.json")

    # ---- Debug + Smoke ----
    debug.update({
        "inputs": {
            "ifrs_rows": int(len(ifrs)),
            "bench_catalog_keys": len(bench_cat) if isinstance(bench_cat, dict) else "n/a",
            "notes_sections": list(ifrs_notes.keys()) if isinstance(ifrs_notes, dict) else [],
        }
    })
    with (out / "m9_0_debug.json").open("w", encoding="utf-8") as f:
        json.dump(debug, f, indent=2)
    _ok("Debug → m9_0_debug.json")

    smoke_lines: List[str] = []
    smoke_lines.append("# M9.0 Smoke")
    for k, v in sheets.items():
        smoke_lines.append(f"- {k}: {len(v)} rows")
    (out / "m9_0_smoke.md").write_text("\n".join(smoke_lines), encoding="utf-8")
    _ok("Smoke → m9_0_smoke.md")
