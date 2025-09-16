# src/terra_nova/modules/m5_cash_flow/engine.py
"""
M5 Cash Flow Statement Assembler (robust edition)

Design goals
------------
- Produce a monthly cash flow statement with the three IFRS buckets:
  CFO_NAD_000 (operating), CFI_NAD_000 (investing), CFF_NAD_000 (financing).
- Be **robust** to column naming drifts upstream (M0..M3) by using synonym sets.
- Avoid "tunnel patches": if an input is truly missing, fail in strict mode,
  but follow **project policy** that pre‑freeze opening cash is 0 when M0 has
  no explicit cash line (documented in M7.5B freeze logic).
- Be chatty: return metadata explaining how each role was resolved.

Inputs (expected in `outputs/`)
-------------------------------
- m2_pl_schedule.parquet (NPAT, DA/Depreciation)
- m2_working_capital_schedule.parquet (NWC cash flow already in CF sign)
- m1_capex_schedule.parquet (CAPEX cash series → CFI)
- m3_revolver_schedule.parquet (draws, repayments, fees → CFF)
- m0_opening_bs.parquet (optional; opening cash; long or wide)

Outputs
-------
- (df) columns: ['Month_Index', 'CFO_NAD_000', 'CFI_NAD_000', 'CFF_NAD_000']
- meta dict with verbose resolution details
- smoke dict (small checks)

All amounts in NAD '000.
"""
from __future__ import annotations
from dataclasses import dataclass
from typing import Dict, List, Tuple, Any, Optional
import pandas as pd
import numpy as np
from pathlib import Path

# ---------- utilities ----------

def _print(msg: str) -> None:
    print(msg, flush=True)

def _fail(msg: str) -> None:
    raise RuntimeError(f"[M5][FAIL] {msg}")

def _warn(msg: str) -> None:
    _print(f"[M5][WARN] {msg}")

def _ok(msg: str) -> None:
    _print(f"[M5][OK]  {msg}")

def _info(msg: str) -> None:
    _print(f"[M5][INFO] {msg}")

def _read_parquet(path: Path) -> pd.DataFrame:
    if not path.exists():
        _fail(f"Required artifact not found: {path}")
    try:
        return pd.read_parquet(path)
    except Exception:
        # engine auto fallback
        return pd.read_parquet(path, engine="pyarrow")

def _syn(df: pd.DataFrame, candidates: List[str], role: str) -> str:
    cols = list(df.columns)
    for c in candidates:
        if c in cols:
            return c
    preview = cols[:35]
    _fail(f"Cannot resolve role '{role}'. Tried {list(candidates)}. Available (first 35): {preview}")
    return ""  # unreachable

def _zero_series(months: pd.Series) -> pd.Series:
    s = pd.Series(0.0, index=months.index)
    s.name = "zero"
    return s

# ---------- synonym dictionaries ----------

MONTH_SYNS = ["Month_Index", "MONTH_INDEX", "month_index", "Month", "month"]

NPAT_SYNS = ["NPAT_NAD_000", "Net_Profit_After_Tax_NAD_000", "NPAT"]
DA_SYNS   = ["DA_NAD_000", "Depreciation_NAD_000", "Depreciation_and_Amortization_NAD_000", "DA"]

NWC_CF_SYNS = [
    "NWC_CF_NAD_000",
    "WC_Cash_Flow_NAD_000",
    "Change_in_NWC_CF_NAD_000",
    "Working_Capital_CF_NAD_000",
    "Cash_Flow_from_NWC_Change_NAD_000",  # seen in logs
]

# CAPEX / CFI
CFI_SYNS = [
    "CFI_NAD_000",
    "CAPEX_Outflow_NAD_000",
    "Capex_Cash_Outflow_NAD_000",
    "CAPEX_Cash_NAD_000",
    "Capex_NAD_000",
    "CAPEX_NAD_000",
]

# Revolver schedule (CFF)
REV_MONTH_SYNS = MONTH_SYNS
REV_DRAW_SYNS  = [
    "Draw_NAD_000","Draws_NAD_000","Revolver_Draw_NAD_000","Revolver_Draws_NAD_000",
    "Principal_Draw_NAD_000","Principal_Draws_NAD_000","Debt_Draws_NAD_000",
    "Drawdown","Drawdowns"  # seen in logs
]
REV_REPAY_SYNS = [
    "Repay_NAD_000","Repayment_NAD_000","Repayments_NAD_000","Principal_Repay_NAD_000",
    "Debt_Repayments_NAD_000","Repayment","Repay","Repayments"  # seen in logs
]
REV_FEES_SYNS = [
    "Fees_NAD_000","Revolver_Fees_NAD_000","Facility_Fees_NAD_000","Commitment_Fee_NAD_000","Admin_Fee_NAD_000"
]
REV_INT_SYNS = ["Interest_NAD_000","Interest_Accrued","Interest_Paid_NAD_000"]

# Opening cash (M0)
CASH_OPEN_SYNS = [
    "Cash_and_Cash_Equivalents_NAD_000","Cash_NAD_000","Cash_Equivalents_NAD_000",
    "Opening_Cash_NAD_000","Cash_and_Cash_Equivalents","Cash"
]
M0_WIDE_VALUE_SYNS = ["Value_NAD_000","Value_NAD","Value"]

# ---------- loaders ----------

def _load_m2_pl(outputs: Path) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    path = outputs / "m2_pl_schedule.parquet"
    df = _read_parquet(path)
    mcol = _syn(df, MONTH_SYNS, "Month_Index")
    npat = _syn(df, NPAT_SYNS, "NPAT")
    da   = _syn(df, DA_SYNS,   "DA/Depreciation")
    base = df[[mcol, npat, da]].copy()
    base.rename(columns={mcol:"Month_Index", npat:"NPAT_NAD_000", da:"DA_NAD_000"}, inplace=True)
    _ok(f"M2 P&L columns -> month='Month_Index', NPAT='{npat}', DA='{da}'")
    return base, {"pl_path": str(path), "npat_col": npat, "da_col": da}

def _load_m2_wc(outputs: Path) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    path = outputs / "m2_working_capital_schedule.parquet"
    df = _read_parquet(path)
    mcol = _syn(df, MONTH_SYNS, "Month_Index")
    ncc  = _syn(df, NWC_CF_SYNS, "NWC_CF_NAD_000")
    wc = df[[mcol, ncc]].copy()
    wc.rename(columns={mcol:"Month_Index", ncc:"NWC_CF_NAD_000"}, inplace=True)
    _ok(f"M2 WC columns -> month='Month_Index', NWC_CF='{ncc}'")
    return wc, {"wc_path": str(path), "nwc_cf_col": ncc}

def _load_m1_cfi(outputs: Path, strict: bool) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    path = outputs / "m1_capex_schedule.parquet"
    df = _read_parquet(path)
    mcol = _syn(df, MONTH_SYNS, "Month_Index")
    # Heuristic: prefer explicit CFI column; otherwise a CAPEX cash sign column
    cfi_col = None
    for c in CFI_SYNS:
        if c in df.columns:
            cfi_col = c
            break
    if cfi_col is None:
        # try to guess: any column with 'CAPEX' and 'NAD' or 'Cash'
        for c in df.columns:
            lc = c.lower()
            if ("capex" in lc or "capex" in c) and ("nad" in lc or "cash" in lc or "outflow" in lc):
                cfi_col = c
                _warn(f"Using '{c}' from m1_capex_schedule.parquet as CAPEX cash column by heuristic.")
                break
    if cfi_col is None:
        if strict:
            _fail("Could not find CAPEX/CFI cash column in m1_capex_schedule.parquet.")
        else:
            _warn("CFI not found; defaulting to zeros.")
            return pd.DataFrame({"Month_Index": df[mcol], "CFI_NAD_000": 0.0}), {"cfi_col": None, "sign_flipped": False, "path": str(path)}
    cfi = df[[mcol, cfi_col]].copy()
    cfi.rename(columns={mcol:"Month_Index", cfi_col:"CFI_NAD_000"}, inplace=True)
    # Ensure outflow negative
    # If the median is positive, flip sign.
    med = float(np.nanmedian(cfi["CFI_NAD_000"].values))
    flipped = False
    if med > 0:
        cfi["CFI_NAD_000"] = -cfi["CFI_NAD_000"]
        flipped = True
        _warn("Detected mostly positive CAPEX cash – flipping sign to outflow negative.")
    _ok(f"CFI sourced from m1_capex_schedule.parquet -> 'CFI_NAD_000'")
    return cfi, {"cfi_col": cfi_col, "sign_flipped": flipped, "path": str(path)}

def _load_m3_revolver(outputs: Path, strict: bool) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    path = outputs / "m3_revolver_schedule.parquet"
    df = _read_parquet(path)
    # Some repos store a single schedule per 'Case_Name'/'Line_ID' – keep only month and numeric legs
    mcol = None
    for c in REV_MONTH_SYNS:
        if c in df.columns:
            mcol = c
            break
    if mcol is None:
        _fail("Could not resolve Month_Index in m3_revolver_schedule.parquet.")
    # pick helpers
    def pick(cands: List[str], role: str) -> Optional[str]:
        for c in cands:
            if c in df.columns:
                return c
        return None
    dcol = pick(REV_DRAW_SYNS,  "Revolver draw column")
    rcol = pick(REV_REPAY_SYNS, "Revolver repayment column")
    fcol = pick(REV_FEES_SYNS,  "Revolver fee column")
    icol = pick(REV_INT_SYNS,   "Revolver interest column")  # not used in CFF

    if dcol is None or rcol is None:
        # Print available columns to aid debugging
        preview = list(df.columns)[:35]
        _fail(f"Cannot resolve revolver draw/repay columns. Tried draws={REV_DRAW_SYNS}, repay={REV_REPAY_SYNS}. Available (first 35): {preview}")

    cff = df[[mcol]].copy()
    cff.rename(columns={mcol:"Month_Index"}, inplace=True)
    cff["CFF_NAD_000"] = 0.0
    cff["CFF_NAD_000"] = cff["CFF_NAD_000"].astype(float)
    # Draws are inflows (+), repayments are outflows (−)
    cff = cff.join(df[[dcol]].rename(columns={dcol:"_draw"})).join(df[[rcol]].rename(columns={rcol:"_repay"}))
    cff["_draw"]  = pd.to_numeric(cff["_draw"], errors="coerce").fillna(0.0)
    cff["_repay"] = pd.to_numeric(cff["_repay"], errors="coerce").fillna(0.0)
    cff["CFF_NAD_000"] = cff["_draw"] - cff["_repay"]
    # fees often outflows; can be included in CFF or CFO – we include in CFF (project policy)
    fee_used = False
    if fcol is not None:
        cff = cff.join(df[[fcol]].rename(columns={fcol:"_fees"}))
        cff["_fees"] = pd.to_numeric(cff["_fees"], errors="coerce").fillna(0.0)
        cff["CFF_NAD_000"] = cff["CFF_NAD_000"] - cff["_fees"]
        fee_used = True
    # Do not include interest (handled via P&L/CFO). If present we show in meta only.
    meta = {"rev_path": str(path), "month_col": mcol, "draw_col": dcol, "repay_col": rcol, "fee_col": fcol, "interest_col_present": icol is not None, "fees_included_in_cff": fee_used}
    _ok("CFF derived from M3 revolver schedule (draws − repayments − fees). Interest excluded by design.")
    cff = cff[["Month_Index","CFF_NAD_000"]]
    return cff, meta

def _load_opening_cash(outputs: Path, base_months: pd.Series, strict: bool) -> Tuple[float, Dict[str, Any]]:
    """
    Read opening cash from M0 if available.
    Supports wide or long form:
      - wide: has a 'Cash_and_Cash_Equivalents_NAD_000' (or synonyms) column
      - long: has 'Line_Item' + one of value columns; row label matches cash synonyms
    If not present, **project policy** is opening cash = 0.0 (pre-freeze).
    """
    # Search in root and in m0_inputs subdir (be tolerant)
    candidates = [outputs / "m0_opening_bs.parquet", outputs / "m0_inputs" / "m0_opening_bs.parquet"]
    path = None
    for p in candidates:
        if p.exists():
            path = p
            break
    if path is None:
        if strict:
            _fail("m0_opening_bs.parquet not found in outputs/. Provide it or set strict=False.")
        else:
            _warn("m0_opening_bs.parquet not found; defaulting opening cash to 0.0 per policy.")
            return 0.0, {"source": None, "policy_default_zero": True}

    df = _read_parquet(path)
    cols = list(df.columns)

    # Case A: wide form with direct cash column
    for c in CASH_OPEN_SYNS:
        if c in cols:
            # If there's a Month_Index, use first month; else take first row
            mcol = None
            for m in MONTH_SYNS:
                if m in cols:
                    mcol = m
                    break
            if mcol:
                # align to min month present in base
                m0 = int(min(base_months.min(), df[mcol].min()))
                # fetch row with that month
                row = df[df[mcol] == m0]
                if not row.empty:
                    val = float(pd.to_numeric(row[c], errors="coerce").fillna(0.0).iloc[0])
                else:
                    # pick first value
                    val = float(pd.to_numeric(df[c], errors="coerce").fillna(0.0).iloc[0])
            else:
                val = float(pd.to_numeric(df[c], errors="coerce").fillna(0.0).iloc[0])
            _ok(f"Opening cash source: M0:{c}@{str(path.name)} -> {val:,.2f} (NAD '000)")
            return val, {"source": f"M0:{c}", "path": str(path), "policy_default_zero": False}

    # Case B: long form with line items
    line_col = None
    for cand in ["Line_Item","Item","Line","Account","LineItem"]:
        if cand in cols:
            line_col = cand
            break
    val_col = None
    for cand in ["Value_NAD_000","Value_NAD","Value","Amount_NAD_000","Amount_NAD","Amount"]:
        if cand in cols:
            val_col = cand
            break
    if line_col and val_col:
        # normalize names
        def norm(s: str) -> str:
            return "".join(ch for ch in s.upper() if ch.isalnum())
        targets = [norm(c) for c in CASH_OPEN_SYNS]
        df["_key"] = df[line_col].astype(str).map(norm)
        row = df[df["_key"].isin(targets)]
        if not row.empty:
            val = float(pd.to_numeric(row[val_col], errors="coerce").fillna(0.0).iloc[0])
            _ok(f"Opening cash source: M0:{line_col}='{row[line_col].iloc[0]}' -> {val:,.2f} (NAD '000)")
            return val, {"source": f"M0:{line_col} match", "path": str(path), "policy_default_zero": False}

    # Policy default
    _ok("Opening cash resolved defaulted to 0.0 (no explicit M0 cash; pre‑freeze policy).")
    return 0.0, {"source": None, "path": str(path), "policy_default_zero": True}

# ---------- assembler ----------

def build_cash_flow_statement(outputs_dir: str, currency: str, input_pack_path: Optional[str], strict: bool=False) -> Tuple[pd.DataFrame, Dict[str, Any], Dict[str, Any]]:
    """
    Public entrypoint used by runner.py
    """
    outputs = Path(outputs_dir)
    # M2 basis
    base, pl_m = _load_m2_pl(outputs)
    wc,   wc_m = _load_m2_wc(outputs)

    # base months
    months = base["Month_Index"].copy()
    months = months.sort_values().reset_index(drop=True)

    # CFO = NPAT + DA + NWC_CF (note: NWC_CF already has CF sign)
    cfo = pd.DataFrame({"Month_Index": months})
    # Join P&L
    tmp = base.merge(wc, on="Month_Index", how="left")
    tmp["DA_NAD_000"] = pd.to_numeric(tmp["DA_NAD_000"], errors="coerce").fillna(0.0)
    tmp["NPAT_NAD_000"] = pd.to_numeric(tmp["NPAT_NAD_000"], errors="coerce").fillna(0.0)
    tmp["NWC_CF_NAD_000"] = pd.to_numeric(tmp["NWC_CF_NAD_000"], errors="coerce").fillna(0.0)
    cfo["CFO_NAD_000"] = tmp["NPAT_NAD_000"] + tmp["DA_NAD_000"] + tmp["NWC_CF_NAD_000"]

    # CFI from M1
    cfi, cfi_m = _load_m1_cfi(outputs, strict=strict)
    cfi = cfi.merge(cfo[["Month_Index"]], on="Month_Index", how="right")
    cfi["CFI_NAD_000"] = pd.to_numeric(cfi["CFI_NAD_000"], errors="coerce").fillna(0.0)

    # CFF from M3 revolver (draws − repayments − fees; interest excluded)
    cff, cff_m = _load_m3_revolver(outputs, strict=strict)
    cff = cff.merge(cfo[["Month_Index"]], on="Month_Index", how="right")
    cff["CFF_NAD_000"] = pd.to_numeric(cff["CFF_NAD_000"], errors="coerce").fillna(0.0)

    # Opening cash (policy default 0 if absent)
    opening_cash, open_m = _load_opening_cash(outputs, cfo["Month_Index"], strict=strict)

    # Assemble final DF
    df = cfo.merge(cfi, on="Month_Index", how="left").merge(cff, on="Month_Index", how="left")
    df[["CFI_NAD_000","CFF_NAD_000"]] = df[["CFI_NAD_000","CFF_NAD_000"]].fillna(0.0)

    # simple smoke meta
    smoke = {
        "months": int(df["Month_Index"].nunique()),
        "cfo_nonzero": bool((df["CFO_NAD_000"].abs() > 0).any()),
        "cfi_nonzero": bool((df["CFI_NAD_000"].abs() > 0).any()),
        "cff_nonzero": bool((df["CFF_NAD_000"].abs() > 0).any()),
        "opening_cash_nad_000": float(opening_cash),
    }

    meta = {
        "currency": currency,
        "sources": {"m2_pl": pl_m, "m2_wc": wc_m, "m1_capex": cfi_m, "m3_revolver": cff_m, "m0_opening_cash": open_m},
        "policy": {
            "interest_excluded_from_cff": True,
            "opening_cash_policy_default_zero_if_absent": True,
        }
    }

    return df, meta, smoke

# Compatibility alias (older imports)
def assemble_cash_flow_statement(outputs_dir: str, currency: str, input_pack_path: Optional[str], strict: bool=False):
    return build_cash_flow_statement(outputs_dir, currency, input_pack_path, strict)

