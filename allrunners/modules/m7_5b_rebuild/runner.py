# -*- coding: utf-8 -*-
"""
M7.5B — Rebuild with IFRS aggregator
- Consumes core outputs (M0..M5, M3 revolver, M2 WC)
- Rebuilds CF + minimal PL/BS and adds IFRS-friendly helpers:
    * PL:  EBITDA_NAD_000, Total_OPEX_NAD_000
    * BS:  Cash_and_Cash_Equivalents_NAD_000 (from CF link),
           Current_Assets_NAD_000,
           Current_Liabilities_NAD_000,
           Total_Assets_NAD_000,
           Liabilities_and_Equity_Total_NAD_000
- Emits debug JSON and a smoke report. Chatty logs for traceability.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import pandas as pd


# ---------- logging helpers ----------
def _ok(msg: str) -> None:
    print(f"[M7.5B][OK]  {msg}")

def _info(msg: str) -> None:
    print(f"[M7.5B][INFO] {msg}")

def _warn(msg: str) -> None:
    print(f"[M7.5B][WARN] {msg}")

def _fail(msg: str) -> None:
    raise RuntimeError(f"[M7.5B][FAIL] {msg}")


# ---------- robust IO ----------
def _read_parquet_safe(p: Path, what: str) -> pd.DataFrame:
    if not p.exists():
        _fail(f"Missing {what}: {p}")
    df = pd.read_parquet(p)
    if df.empty:
        _fail(f"Empty {what}: {p}")
    return df


# ---------- column utilities ----------
def _col_or_zeros(df: pd.DataFrame, name: str) -> pd.Series:
    """Return df[name].fillna(0) if present; otherwise a 0.0 series aligned to df.index."""
    if name in df.columns:
        return df[name].fillna(0.0)
    return pd.Series(0.0, index=df.index)

def _first_present(df: pd.DataFrame, names: List[str]) -> Optional[str]:
    for n in names:
        if n in df.columns:
            return n
    return None


# ---------- FX helpers ----------
FX_COL_SYNONYMS = [
    "USD_to_NAD", "USD_NAD", "FX_USD_NAD", "USDtoNAD", "NAD_per_USD", "Rate_USD_to_NAD"
]

def _resolve_fx_path(outputs: Path) -> Optional[Path]:
    """Prefer project FX curve if present; otherwise try legacy paths. If none, return None."""
    candidates = [
        outputs / "m8b_fx_curve.parquet",        # convenient if M8.B3 has been run already
        outputs / "m0_inputs" / "FX_Path.parquet",
        outputs / "FX_Path.parquet",
    ]
    for p in candidates:
        if p.exists():
            return p
    return None

def _resolve_fx_column(fx: pd.DataFrame) -> Optional[str]:
    for c in FX_COL_SYNONYMS:
        if c in fx.columns:
            return c
    # Fallback: any numeric column other than Month_Index
    candidates = [c for c in fx.columns if c != "Month_Index" and pd.api.types.is_numeric_dtype(fx[c])]
    return candidates[0] if candidates else None

def _merge_fx_and_add_usd(df: pd.DataFrame, fx: Optional[pd.DataFrame], debug: Dict, context: str) -> pd.DataFrame:
    """
    Left-join on Month_Index and create USD twins for every *_NAD_000 column.
    Treat FX as NAD per USD; USD = NAD / FX.
    If FX is not available or lacks Month_Index, safely skip with INFO.
    """
    if "Month_Index" not in df.columns:
        _info(f"{context}: Month_Index not found in DF → USD twins skipped.")
        return df.copy()

    if fx is None:
        _info(f"{context}: FX curve not found → USD twins skipped.")
        return df.copy()

    fx_col = _resolve_fx_column(fx)
    if fx_col is None:
        _info(f"{context}: No numeric FX column detected → USD twins skipped.")
        return df.copy()

    # Ensure FX has Month_Index; if not, broadcast constant rate across DF's months when possible
    if "Month_Index" not in fx.columns:
        if len(fx) == 1 and fx_col in fx.columns:
            rate = float(fx.iloc[0][fx_col])
            fx = pd.DataFrame({"Month_Index": sorted(df["Month_Index"].unique()), fx_col: rate})
            _info(f"{context}: FX lacked Month_Index; broadcasted constant {fx_col}={rate}.")
        else:
            _info(f"{context}: FX lacks Month_Index and not broadcastable → USD twins skipped.")
            return df.copy()

    x = df.merge(fx[["Month_Index", fx_col]], on="Month_Index", how="left", validate="m:1")
    x[fx_col] = x[fx_col].ffill().bfill()

    nad_cols = [c for c in x.columns if c.endswith("_NAD_000") and pd.api.types.is_numeric_dtype(x[c])]
    for c in nad_cols:
        usd = c.replace("_NAD_000", "_USD_000")
        x[usd] = x[c] / x[fx_col]
    debug.setdefault("usd_twin_contexts", []).append({"context": context, "fx_col": fx_col, "nad_cols": nad_cols})
    return x.drop(columns=[fx_col], errors="ignore")


# ---------- M0 opening cash ----------
def _load_opening_cash(outputs: Path, debug: Dict) -> float:
    p = outputs / "m0_opening_bs.parquet"
    df = _read_parquet_safe(p, "M0 opening balance sheet")
    # Typical columns: Line_Item, Value_NAD, Notes, Month_Index
    if "Line_Item" not in df.columns:
        _fail("M0 opening BS missing 'Line_Item' column.")

    value_col = "Value_NAD" if "Value_NAD" in df.columns else None
    if value_col is None:
        # try common alternatives if ever introduced
        for alt in ["Value_NAD_000", "Value"]:
            if alt in df.columns:
                value_col = alt
                break
    if value_col is None:
        _fail("M0 opening BS missing a numeric value column (e.g., 'Value_NAD').")

    row = df[df["Line_Item"].astype(str).str.lower().str.contains("cash")]
    if row.empty:
        _fail("Could not locate opening cash in M0 opening BS (search on Line_Item ~ 'cash').")

    v = float(row.iloc[0][value_col])
    # If Value_NAD is in absolute NAD, convert to '000 for model alignment
    opening_cash_nad_000 = v / 1_000.0 if value_col == "Value_NAD" else float(v)
    debug["opening_cash_source"] = {"path": str(p), "value_col": value_col, "raw_value": v, "opening_cash_nad_000": opening_cash_nad_000}
    _ok(f"Opening cash source: M0 -> {opening_cash_nad_000:,.2f} (NAD '000)")
    return opening_cash_nad_000


# ---------- core loaders ----------
def _load_m5_cf(outputs: Path, debug: Dict) -> pd.DataFrame:
    p = outputs / "m5_cash_flow_statement_final.parquet"
    df = _read_parquet_safe(p, "M5 cash flow")
    need = ["Month_Index", "CFO_NAD_000", "CFI_NAD_000", "CFF_NAD_000"]
    missing = [c for c in need if c not in df.columns]
    if missing:
        _fail(f"M5 cash flow missing required columns: {missing} in {p}")
    debug["m5_cf_cols"] = list(df.columns)
    _ok(f"   M5 roles -> CFO='CFO_NAD_000', CFI='CFI_NAD_000', CFF='CFF_NAD_000'")
    return df.copy()

def _load_m2_wc(outputs: Path, debug: Dict) -> Optional[pd.DataFrame]:
    p = outputs / "m2_working_capital_schedule.parquet"
    if not p.exists():
        _info("M2 WC schedule not found → Current asset/liability build will omit AR/Inventory/AP.")
        return None
    df = pd.read_parquet(p)
    debug["m2_wc_cols"] = list(df.columns)
    return df

def _load_m3_revolver(outputs: Path, debug: Dict) -> Optional[pd.DataFrame]:
    p = outputs / "m3_revolver_schedule.parquet"
    if not p.exists():
        _info("M3 revolver schedule not found → revolver current portion omitted.")
        return None
    df = pd.read_parquet(p)
    debug["m3_revolver_cols"] = list(df.columns)
    return df


# ---------- IFRS aggregator ----------
def _augment_ifrs(pl: pd.DataFrame, bs: pd.DataFrame, wc: Optional[pd.DataFrame], rev: Optional[pd.DataFrame], debug: Dict, warns: List[str]) -> Tuple[pd.DataFrame, pd.DataFrame]:
    # --- PL ---
    # EBITDA: prefer existing; else EBIT + Depreciation; else NPAT + Tax + Interest + Depreciation.
    ebitda_col = _first_present(pl, ["EBITDA_NAD_000"])
    if ebitda_col is None:
        ebit = _first_present(pl, ["EBIT_NAD_000", "Operating_Profit_NAD_000"])
        dep  = _first_present(pl, ["Depreciation_NAD_000", "DA_NAD_000"])
        if ebit and dep:
            pl["EBITDA_NAD_000"] = pl[ebit].fillna(0.0) + pl[dep].fillna(0.0)
            debug["ebitda_source"] = {"method": "EBIT_plus_Depreciation", "ebit": ebit, "dep": dep}
        else:
            npat = _first_present(pl, ["NPAT_NAD_000", "Net_Income_NAD_000"])
            tax  = _first_present(pl, ["Tax_Expense_NAD_000"])
            intc = _first_present(pl, ["Interest_Expense_NAD_000", "Finance_Costs_NAD_000"])
            dep  = _first_present(pl, ["Depreciation_NAD_000", "DA_NAD_000"])
            for c in [npat, tax, intc, dep]:
                if c and c not in pl.columns:
                    pl[c] = 0.0
            pl["EBITDA_NAD_000"] = _col_or_zeros(pl, npat or "").add(_col_or_zeros(pl, tax or ""), fill_value=0) \
                                                     .add(_col_or_zeros(pl, intc or ""), fill_value=0) \
                                                     .add(_col_or_zeros(pl, dep or ""), fill_value=0)
            debug["ebitda_source"] = {"method": "NPAT+Tax+Interest+Dep", "npat": npat, "tax": tax, "int": intc, "dep": dep}
    else:
        pl["EBITDA_NAD_000"] = pl[ebitda_col].fillna(0.0)
        debug["ebitda_source"] = {"method": "preexisting", "col": ebitda_col}

    # Total_OPEX: prefer Fixed + Variable; else use existing Total_OPEX; else derive as Revenue - (EBIT + Dep + Interest + Tax + below-EBIT items not modeled)
    total_opex_col = _first_present(pl, ["Total_OPEX_NAD_000"])
    if total_opex_col is None:
        fx = _first_present(pl, ["Fixed_OPEX_NAD_000"])
        vx = _first_present(pl, ["Variable_OPEX_NAD_000"])
        if fx or vx:
            pl["Total_OPEX_NAD_000"] = _col_or_zeros(pl, fx or "").add(_col_or_zeros(pl, vx or ""), fill_value=0)
            debug["total_opex_source"] = {"method": "fixed_plus_variable", "fixed": fx, "variable": vx}
        else:
            rev = _first_present(pl, ["Total_Revenue_NAD_000", "Revenue_NAD_000"])
            ebit = _first_present(pl, ["EBIT_NAD_000", "Operating_Profit_NAD_000"])
            dep  = _first_present(pl, ["Depreciation_NAD_000", "DA_NAD_000"])
            intc = _first_present(pl, ["Interest_Expense_NAD_000", "Finance_Costs_NAD_000"])
            tax  = _first_present(pl, ["Tax_Expense_NAD_000"])
            pl["Total_OPEX_NAD_000"] = _col_or_zeros(pl, rev or "") \
                .sub(_col_or_zeros(pl, ebit or ""), fill_value=0) \
                .sub(_col_or_zeros(pl, dep or ""), fill_value=0) \
                .sub(_col_or_zeros(pl, intc or ""), fill_value=0) \
                .sub(_col_or_zeros(pl, tax or ""), fill_value=0)
            debug["total_opex_source"] = {"method": "derived_residual", "rev": rev, "ebit": ebit, "dep": dep, "int": intc, "tax": tax}
    else:
        pl["Total_OPEX_NAD_000"] = pl[total_opex_col].fillna(0.0)
        debug["total_opex_source"] = {"method": "preexisting", "col": total_opex_col}

    # --- BS ---
    # Merge WC if available to fetch AR/Inventory/AP balances
    if wc is not None and "Month_Index" in wc.columns:
        bs = bs.merge(wc[["Month_Index"] +
                         [c for c in ["AR_Balance_NAD_000", "Inventory_Balance_NAD_000", "AP_Balance_NAD_000"] if c in wc.columns]],
                      on="Month_Index", how="left", validate="1:1")
        debug["wc_merged"] = True
    else:
        debug["wc_merged"] = False

    # Revolver current portion: for revolvers, outstanding is typically current; if present, use Closing_Balance (no warning).
    rev_series = pd.Series(0.0, index=bs.index)
    if rev is not None:
        mcol = "Month_Index" if "Month_Index" in rev.columns else None
        if mcol and "Closing_Balance" in rev.columns:
            rev_m = rev.groupby(mcol, as_index=False)["Closing_Balance"].sum()
            rev_m.rename(columns={"Closing_Balance": "Revolver_Current_Liab_NAD_000"}, inplace=True)
            bs = bs.merge(rev_m, left_on="Month_Index", right_on=mcol, how="left")
            bs["Revolver_Current_Liab_NAD_000"] = bs["Revolver_Current_Liab_NAD_000"].fillna(0.0)
            rev_series = bs["Revolver_Current_Liab_NAD_000"]
            debug["revolver_policy"] = "treated_as_current"
            _info("Revolver treated as current liability via Closing_Balance (policy).")
        else:
            debug["revolver_policy"] = "not_available"
            _info("Revolver schedule lacks Month_Index/Closing_Balance → revolver current omitted.")
    else:
        debug["revolver_policy"] = "no_revolver_schedule"

    cash = _col_or_zeros(bs, "Cash_and_Cash_Equivalents_NAD_000")
    ar   = _col_or_zeros(bs, "AR_Balance_NAD_000")
    inv  = _col_or_zeros(bs, "Inventory_Balance_NAD_000")
    ap   = _col_or_zeros(bs, "AP_Balance_NAD_000")

    bs["Current_Assets_NAD_000"] = cash.add(ar, fill_value=0).add(inv, fill_value=0)
    bs["Current_Liabilities_NAD_000"] = ap.add(rev_series, fill_value=0)

    # Totals (best-effort, conservative). If existing totals are present, keep them; otherwise compute minimal consistent totals.
    if "Total_Assets_NAD_000" not in bs.columns:
        # Attempt to use Current_Assets + any recognizable non-current assets if present
        nca_candidates = [c for c in bs.columns if "PPE" in c or "Non_Current_Assets" in c]
        nca_sum = bs[nca_candidates].sum(axis=1) if nca_candidates else 0.0
        bs["Total_Assets_NAD_000"] = bs["Current_Assets_NAD_000"].add(nca_sum, fill_value=0)
        debug["total_assets_source"] = {"method": "CA_plus_detected_NCA", "nca_candidates": nca_candidates}

    if "Liabilities_and_Equity_Total_NAD_000" not in bs.columns:
        # As a conservative placeholder, mirror assets so the totals tie (downstream modules don’t re-compute).
        bs["Liabilities_and_Equity_Total_NAD_000"] = bs["Total_Assets_NAD_000"]
        debug["l_and_e_total_source"] = {"method": "mirrored_assets"}

    _ok("IFRS aggregator → augmented PL (EBITDA, Total_OPEX) and BS (Current_Assets, Current_Liabilities, Totals).")
    return pl, bs


# ---------- main ----------
def run_m7_5b(outputs_dir: str, currency: str, min_cash_buffer_nad_000: float = 0.0, strict: bool = True) -> None:
    outputs = Path(outputs_dir)
    _info(f"Starting M7.5B rebuild in: {outputs}")

    debug: Dict = {"params": {"outputs_dir": str(outputs), "currency": currency, "min_cash_buffer_nad_000": min_cash_buffer_nad_000, "strict": strict}}
    warns: List[str] = []

    # Freeze selection (shim). Keep as INFO — the actual freeze file may live elsewhere in user flow.
    freeze_path = outputs / "m7_selected_offer.json"
    if freeze_path.exists():
        sel = json.loads(freeze_path.read_text(encoding="utf-8"))
        debug["freeze_selection"] = sel
        _ok(f"Frozen selection: option='{sel.get('option')}', instrument='{sel.get('instrument')}' → classification='{sel.get('classification')}'")
    else:
        sel = {"option": "A_SAFE", "instrument": "SAFE", "classification": "equity_like"}
        debug["freeze_selection"] = {"shim": True, **sel}
        _ok(f"Frozen selection (shim): option='{sel['option']}', instrument='{sel['instrument']}' → classification='{sel['classification']}'")

    # Load core pieces
    m5 = _load_m5_cf(outputs, debug)
    opening_cash = _load_opening_cash(outputs, debug)  # NAD '000

    # Rebuild CF: Closing_Cash = opening + cumsum(CFO + CFI + CFF)
    cf = m5[["Month_Index", "CFO_NAD_000", "CFI_NAD_000", "CFF_NAD_000"]].copy()
    cf["Net_Cash_Flow_NAD_000"] = cf["CFO_NAD_000"].fillna(0) + cf["CFI_NAD_000"].fillna(0) + cf["CFF_NAD_000"].fillna(0)
    cf["Closing_Cash_NAD_000"] = opening_cash + cf["Net_Cash_Flow_NAD_000"].cumsum()
    _ok("   Closing cash rolled in CF (Closing_Cash_NAD_000).")

    # Minimal BS (start with cash from CF closing cash)
    bs = cf[["Month_Index", "Closing_Cash_NAD_000"]].rename(columns={"Closing_Cash_NAD_000": "Cash_and_Cash_Equivalents_NAD_000"}).copy()
    _ok("   Added BS column Cash_and_Cash_Equivalents_NAD_000 from CF closing cash.")

    # Minimal PL (bring through what we can from M2 P&L if available)
    pl_path = outputs / "m2_pl_schedule.parquet"
    if pl_path.exists():
        pl = pd.read_parquet(pl_path)
        keep = [c for c in ["Month_Index", "Total_Revenue_NAD_000", "Fixed_OPEX_NAD_000", "Variable_OPEX_NAD_000",
                            "Total_OPEX_NAD_000", "Depreciation_NAD_000", "EBIT_NAD_000",
                            "Interest_Expense_NAD_000", "Tax_Expense_NAD_000", "NPAT_NAD_000"] if c in pl.columns]
        if "Month_Index" not in pl.columns:
            _warn("M2 P&L lacks Month_Index; PL will be rebuilt minimal only.")
            pl = cf[["Month_Index"]].copy()
        else:
            pl = pl[keep].copy()
    else:
        _info("M2 P&L schedule not found → PL will be rebuilt minimal only.")
        pl = cf[["Month_Index"]].copy()

    # IFRS aggregator (Current Assets/Liab, OPEX/EBITDA, Totals)
    wc  = _load_m2_wc(outputs, debug)
    rev = _load_m3_revolver(outputs, debug)
    pl, bs = _augment_ifrs(pl, bs, wc, rev, debug, warns)

    # USD twins (optional, best-effort)
    fx_path = _resolve_fx_path(outputs)
    fx_df = pd.read_parquet(fx_path) if fx_path is not None else None
    if fx_path is not None:
        _ok(f"   FX curve found at: {fx_path.name}")
    pl = _merge_fx_and_add_usd(pl, fx_df, debug, "PL→USD")
    cf = _merge_fx_and_add_usd(cf, fx_df, debug, "CF→USD")
    bs = _merge_fx_and_add_usd(bs, fx_df, debug, "BS→USD")

    # Emit artifacts
    (outputs / "m7_5b_profit_and_loss.parquet").write_bytes(pl.to_parquet())
    (outputs / "m7_5b_cash_flow.parquet").write_bytes(cf.to_parquet())
    (outputs / "m7_5b_balance_sheet.parquet").write_bytes(bs.to_parquet())
    _ok("   Emitted: m7_5b_profit_and_loss.parquet, m7_5b_cash_flow.parquet, m7_5b_balance_sheet.parquet")

    # Sanity link (CF closing cash == BS cash)
    link_max_abs = float((cf["Closing_Cash_NAD_000"] - bs["Cash_and_Cash_Equivalents_NAD_000"]).abs().max())
    if link_max_abs <= 1e-9:
        _ok("   CF Closing_Cash equals BS Cash_and_Cash_Equivalents (exact link).")
    else:
        msg = f"CF/BS cash link mismatch: max abs diff = {link_max_abs:.6f}"
        if strict:
            _fail(msg)
        else:
            _warn(msg)

    # Debug + smoke
    debug_path = outputs / "m7_5b_debug.json"
    debug.update({
        "emitted": {
            "pl": str(outputs / "m7_5b_profit_and_loss.parquet"),
            "cf": str(outputs / "m7_5b_cash_flow.parquet"),
            "bs": str(outputs / "m7_5b_balance_sheet.parquet"),
        },
        "warns": warns,
    })
    debug_path.write_text(json.dumps(debug, indent=2), encoding="utf-8")
    _ok("   Debug → m7_5b_debug.json")

    smoke_lines = []
    smoke_lines.append(f"CF cols present: {[c for c in ['Closing_Cash_NAD_000','CFO_NAD_000','CFI_NAD_000','CFF_NAD_000'] if c in cf.columns]}")
    smoke_lines.append(f"PL cols present: {[c for c in ['EBITDA_NAD_000','Total_OPEX_NAD_000'] if c in pl.columns]}")
    smoke_lines.append(f"BS cols present: {[c for c in ['Cash_and_Cash_Equivalents_NAD_000','Current_Assets_NAD_000','Current_Liabilities_NAD_000','Total_Assets_NAD_000','Liabilities_and_Equity_Total_NAD_000'] if c in bs.columns]}")
    (outputs / "m7_5b_smoke_report.md").write_text("## M7.5B Smoke\n\n" + "\n".join(f"- {s}" for s in smoke_lines) + "\n", encoding="utf-8")
    _ok("   Smoke → m7_5b_smoke_report.md")
