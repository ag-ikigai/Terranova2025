"""
M8.B1 | Base timeseries engine (calendar helpers + FX canonicalization)

Outputs in <outputs_dir>:
- m8b_base_timeseries.parquet : Month_Index, Calendar_Year, Calendar_Quarter, [NAD_per_USD if FX present]
- m8b1_debug.json             : debug metadata (fx candidates, chosen path, head)
- m8b1_smoke.md               : quick smoke

Behavior:
- Synthesizes Calendar_Year / Calendar_Quarter from Month_Index.
- FX handling:
  * Looks for FX in:
      1) <outputs>/m8b_fx_curve.parquet
      2) <outputs>/m0_inputs/FX_Path.parquet
  * Normalizes FX schema to {'Month_Index','NAD_per_USD'}.
  * Writes normalized copies to BOTH canonical:
      - <outputs>/m8b_fx_curve.parquet
      - <outputs>/m0_inputs/FX_Path.parquet
"""

from __future__ import annotations
from pathlib import Path
import json
import pandas as pd

FX_CANON = "m8b_fx_curve.parquet"
FX_EXPORT_DIR = "m0_inputs"
FX_EXPORT_FILE = "FX_Path.parquet"

def _read_month_index(outputs: Path) -> pd.Series:
    # Prefer PL for Month_Index; CF/BS also contain it
    for f in ["m7_5b_profit_and_loss.parquet", "m7_5b_cash_flow.parquet", "m7_5b_balance_sheet.parquet"]:
        p = outputs / f
        if p.exists():
            df = pd.read_parquet(p)
            if "Month_Index" in df.columns:
                return df["Month_Index"].drop_duplicates().sort_values()
    raise RuntimeError("[M8.B1][FAIL] Could not find Month_Index in M7.5B outputs.")

def _normalize_fx(df: pd.DataFrame, months: pd.Index) -> pd.DataFrame | None:
    if df is None or df.empty:
        return None
    df = df.copy()
    # Column candidates
    month_syns = ["Month_Index", "month_index", "Month", "month", "Index"]
    rate_syns  = ["NAD_per_USD", "FX_NAD_per_USD", "nad_per_usd", "NADperUSD", "NAD_USD", "FX"]
    cm = None
    cr = None
    for c in df.columns:
        if c in month_syns and cm is None:
            cm = c
        if c in rate_syns and cr is None:
            cr = c
    if cr is None:
        return None
    if cm is None:
        # try to synthesize Month_Index by row order
        df = df.rename(columns={cr: "NAD_per_USD"})
        df["Month_Index"] = range(1, len(df) + 1)
    else:
        df = df.rename(columns={cm: "Month_Index", cr: "NAD_per_USD"})
    # keep only needed cols, left-join to known months to align/safe
    df = df[["Month_Index", "NAD_per_USD"]]
    df = pd.merge(pd.DataFrame({"Month_Index": months}), df, on="Month_Index", how="left", validate="1:1")
    return df

def _load_fx(outputs: Path, months: pd.Index, debug: dict, warns: list[str]) -> pd.DataFrame | None:
    candidates = [
        outputs / FX_CANON,
        outputs / FX_EXPORT_DIR / FX_EXPORT_FILE,
    ]
    fx_used = None
    fx_df = None
    for c in candidates:
        if c.exists():
            try:
                df = pd.read_parquet(c)
                fx_df = _normalize_fx(df, months)
                if fx_df is not None:
                    fx_used = str(c)
                    break
            except Exception as e:
                warns.append(f"FX candidate failed to load ({c.name}): {e}")
    debug["fx_candidates"] = [str(x) for x in candidates]
    debug["fx_used"] = fx_used
    if fx_df is None:
        warns.append("FX not found/normalized — proceeding without FX column.")
        return None
    return fx_df

def run_m8B1(outputs_dir: str, currency: str, strict: bool = True, diagnostic: bool = True):
    out = Path(outputs_dir)
    out.mkdir(parents=True, exist_ok=True)
    debug, warns = {}, []

    print(f"[M8.B1][INFO] Starting M8.B1 base timeseries in: {outputs_dir}")

    # 1) Month index & calendar helpers
    months = _read_month_index(out)
    base = pd.DataFrame({"Month_Index": months})
    base["Calendar_Year"] = 1 + ((base["Month_Index"] - 1) // 12)
    base["Calendar_Quarter"] = 1 + ((base["Month_Index"] - 1) % 12) // 3
    print("[M8.B1][OK]  Calendar helpers (Calendar_Year, Calendar_Quarter) synthesized from Month_Index.")

    # 2) FX load & normalization
    fx = _load_fx(out, months, debug, warns)
    if fx is not None:
        base = base.merge(fx, on="Month_Index", how="left", validate="1:1")
        # write canonical + export copy normalized (no more warnings downstream)
        fx_canon = out / FX_CANON
        fx_canon.parent.mkdir(parents=True, exist_ok=True)
        fx.to_parquet(fx_canon, index=False)
        fx_export_dir = out / FX_EXPORT_DIR
        fx_export_dir.mkdir(parents=True, exist_ok=True)
        fx_export = fx_export_dir / FX_EXPORT_FILE
        fx.to_parquet(fx_export, index=False)
        debug["fx_head"] = fx.head(3).to_dict(orient="list")
        debug["fx_written"] = [str(fx_canon), str(fx_export)]
    else:
        debug["fx_head"] = None
        debug["fx_written"] = []

    # 3) Emit artefacts
    base_out = out / "m8b_base_timeseries.parquet"
    base.to_parquet(base_out, index=False)
    print("[M8.B1][OK]  Emitted: m8b_base_timeseries.parquet")

    # Debug & smoke
    dbg_out = out / "m8b1_debug.json"
    with dbg_out.open("w", encoding="utf-8") as f:
        json.dump({"warns": warns, **debug}, f, indent=2)
    print("[M8.B1][OK]  Debug → m8b1_debug.json")

    smoke = out / "m8b1_smoke.md"
    with smoke.open("w", encoding="utf-8") as f:
        f.write(f"## M8.B1 Smoke\n\nRows={len(base)}; Cols={list(base.columns)}\n\nWarns={warns}\n")
    print("[M8.B1][OK]  Smoke → m8b1_smoke.md")

    # If strict, escalate if any critical issues (none now; warnings are allowed)
    if strict and fx is None:
        print("[M8.B1][WARN] FX absent/unnormalized; continuing by design.")

# When invoked as a module via -c import, run_m8B1(...) is called externally.
