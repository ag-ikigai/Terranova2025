import json
import math
import os
from pathlib import Path
import pandas as pd
import numpy as np

# ---- utilities --------------------------------------------------------------

def _read_parquet_maybe(path: Path, required: bool = True) -> pd.DataFrame:
    if path.exists():
        return pd.read_parquet(path)
    if required:
        raise FileNotFoundError(f"Required file missing: {path}")
    # not required -> empty df
    return pd.DataFrame()

def _detect_capex(df: pd.DataFrame) -> pd.Series:
    """
    Try to find a CAPEX flow column in an M1 capex schedule.
    If none found, returns a zero series matching the df length.
    """
    cols = [c for c in df.columns if c.lower() != "month_index"]
    capex_like = [c for c in cols if ("capex" in c.lower()) or ("capital" in c.lower())]
    if capex_like:
        s = df[capex_like].sum(axis=1, numeric_only=True)
        return pd.to_numeric(s, errors="coerce").fillna(0.0)
    # fallback: all zeros
    n = len(df)
    return pd.Series(np.zeros(n), name="CAPEX_NAD_000")

def _get_monthly_rate_from_finance_stack(fin_stack: pd.DataFrame) -> float:
    """
    Try to discover a revolver APR in Finance_Stack or Fin. Stack Scenarios.
    Falls back to 12% APR => 1% monthly.
    """
    if fin_stack is None or fin_stack.empty:
        return 0.12 / 12.0
    df = fin_stack.copy()
    # Normalize column names
    df.columns = [str(c) for c in df.columns]
    lowcols = {c.lower(): c for c in df.columns}
    # Try to find a row that looks like "Revolver"
    name_col = None
    for candidate in ["Instrument", "Name", "Facility", "Line", "Type", "Parameter"]:
        if candidate in df.columns:
            name_col = candidate
            break
    rate_candidates = [c for c in df.columns if "apr" in c.lower() or "rate" in c.lower()]
    if name_col and rate_candidates:
        # Prefer rows that mention 'revolver'
        revolver_rows = df[df[name_col].astype(str).str.lower().str.contains("revolver", na=False)]
        if not revolver_rows.empty:
            for rc in rate_candidates:
                try:
                    val = pd.to_numeric(revolver_rows.iloc[0][rc])
                    if math.isfinite(val) and val > 0 and val < 1.5:  # treat as decimal APR if 0<val<1.5
                        return float(val) / 12.0
                    if math.isfinite(val) and val > 1.5:  # maybe 12 for 12% APR
                        return float(val) / 100.0 / 12.0
                except Exception:
                    pass
    # Also try generic parameter/value shape: parameter=Revolver_APR, value=0.12 or 12
    if set(["Parameter", "Value"]).issubset(df.columns):
        row = df[df["Parameter"].astype(str).str.lower().str.contains("revolver") & 
                 df["Parameter"].astype(str).str.lower().str.contains("apr")]
        if not row.empty:
            v = str(row.iloc[0]["Value"])
            try:
                fl = float(v)
                if fl > 1.5:
                    return fl / 100.0 / 12.0
                if fl > 0:
                    return fl / 12.0
            except Exception:
                pass
    # fallback
    return 0.12 / 12.0

def _safe_num(s):
    try:
        return float(s)
    except Exception:
        return 0.0

# ---- core -------------------------------------------------------------------

def run_m3(input_xlsx: str, out_dir: str, currency: str = "NAD") -> dict:
    """
    Rebuilt M3 that:
      * reads M0/M1/M2 artifacts from `out_dir`,
      * computes a simple revolver schedule to cover (CAPEX - CFO_approx),
      * emits the legacy columns used downstream.
    Artifacts written:
      - m3_revolver_schedule.parquet
      - m3_finance_index.parquet
      - m3_insurance_schedule.parquet (placeholder zeros)
      - m3_smoke_report.md
    Returns a dict with artifact paths and row counts.
    """
    out_path = Path(out_dir)
    out_path.mkdir(parents=True, exist_ok=True)

    # Inputs produced by M0/M1/M2
    cal = _read_parquet_maybe(out_path / "m0_calendar.parquet", required=True)
    m1_capex = _read_parquet_maybe(out_path / "m1_capex_schedule.parquet", required=False)
    wc = _read_parquet_maybe(out_path / "m2_working_capital_schedule.parquet", required=True)
    pl = _read_parquet_maybe(out_path / "m2_profit_and_loss_stub.parquet", required=True)
    fin_stack = None
    try:
        fin_stack = _read_parquet_maybe(out_path / "m0_inputs" / "Finance_Stack.parquet", required=False)
    except Exception:
        fin_stack = pd.DataFrame()

    # Harmonize calendar / month index
    if "Month_Index" not in cal.columns:
        raise ValueError("Calendar missing Month_Index")
    months = cal["Month_Index"].astype(int).tolist()
    n = len(months)

    # CFO approx = NPAT + Depreciation + NWC CF
    npat = pd.to_numeric(pl.get("NPAT_NAD_000", pd.Series([0]*n)), errors="coerce").fillna(0.0)
    dep = pd.to_numeric(pl.get("Depreciation_NAD_000", pd.Series([0]*n)), errors="coerce").fillna(0.0)
    nwc_cf = pd.to_numeric(wc.get("Cash_Flow_from_NWC_Change_NAD_000", pd.Series([0]*n)), errors="coerce").fillna(0.0)
    # If lengths differ, align by Month_Index
    def _align_by_month(df, colname):
        if "Month_Index" in df.columns and colname in df.columns:
            tmp = df[["Month_Index", colname]].copy()
            tmp = tmp.set_index("Month_Index").reindex(months).fillna(0.0)
            return pd.to_numeric(tmp[colname], errors="coerce").fillna(0.0).reset_index(drop=True)
        return pd.Series(np.zeros(n))
    if len(npat) != n:
        npat = _align_by_month(pl.rename(columns={"Month_Index":"Month_Index"}), "NPAT_NAD_000")
    if len(dep) != n:
        dep  = _align_by_month(pl.rename(columns={"Month_Index":"Month_Index"}), "Depreciation_NAD_000")
    if len(nwc_cf) != n:
        nwc_cf = _align_by_month(wc.rename(columns={"Month_Index":"Month_Index"}), "Cash_Flow_from_NWC_Change_NAD_000")

    cfo_approx = npat + dep + nwc_cf

    # CAPEX detection
    capex = pd.Series(np.zeros(n))
    if not m1_capex.empty:
        if "Month_Index" in m1_capex.columns and len(m1_capex) != n:
            # align by month if lens mismatch
            tmp = m1_capex.copy()
            cap = _detect_capex(tmp)
            tmp = pd.DataFrame({"Month_Index": tmp["Month_Index"], "CAPEX": cap})
            tmp = tmp.set_index("Month_Index").reindex(months).fillna(0.0)
            capex = pd.to_numeric(tmp["CAPEX"], errors="coerce").fillna(0.0).reset_index(drop=True)
        else:
            capex = _detect_capex(m1_capex)
            if len(capex) != n:
                # pad/trim
                capex = pd.Series(np.resize(capex.values, n))

    # Monthly interest rate for revolver
    monthly_rate = _get_monthly_rate_from_finance_stack(fin_stack)

    # Build revolver schedule
    records = []
    balance_open = 0.0
    for i, m in enumerate(months):
        interest = balance_open * monthly_rate
        # Need to cover CAPEX minus CFO; interest adds to the cash need
        need = float(capex.iloc[i]) - float(cfo_approx.iloc[i]) + interest
        draw = max(0.0, need)
        repay = 0.0
        if need < 0.0:
            repay = min(balance_open, -need)
        balance_close = balance_open + draw - repay
        records.append({
            "Month_Index": int(m),
            "Revolver_Open_Balance_NAD_000": float(balance_open),
            "Revolver_Draw_NAD_000": float(draw),
            "Revolver_Repayment_NAD_000": float(repay),
            "Revolver_Close_Balance_NAD_000": float(balance_close),
            "Revolver_Interest_Expense_NAD_000": float(interest),
        })
        balance_open = balance_close

    rev = pd.DataFrame.from_records(records)

    # Finance index (metadata)
    fin_index = pd.DataFrame({
        "Key": ["APR_Revolver", "Monthly_Rate_Revolver", "Rows"],
        "Value": [round(monthly_rate * 12.0, 6), round(monthly_rate, 6), len(rev)]
    })

    # Insurance schedule placeholder (keeps downstream happy if referenced)
    ins = pd.DataFrame({
        "Month_Index": months,
        "Insurance_Expense_NAD_000": [0.0] * n
    })

    # Write artifacts
    out_files = {}
    def _w(df: pd.DataFrame, name: str):
        p = out_path / name
        df.to_parquet(p, index=False)
        out_files[name] = len(df)

    _w(rev, "m3_revolver_schedule.parquet")
    _w(fin_index, "m3_finance_index.parquet")
    _w(ins, "m3_insurance_schedule.parquet")

    # Smoke report
    smoke = {
        "rows": len(rev),
        "monthly_rate": monthly_rate,
        "artifacts": out_files,
        "preview_cols": list(rev.columns)[:6]
    }
    (out_path / "m3_smoke_report.md").write_text(
        "# M3 Smoke Report\n\n"
        f"- Rows: {len(rev)}\n"
        f"- Monthly rate (revolver): {monthly_rate:.6f}\n"
        f"- Files: {json.dumps(out_files, indent=2)}\n"
        f"- Columns: {', '.join(rev.columns)}\n",
        encoding="utf-8"
    )

    print("[M3][OK] Emitted:", json.dumps(out_files))
    return out_files

