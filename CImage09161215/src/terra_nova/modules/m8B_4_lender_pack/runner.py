# src/terra_nova/modules/m8B_4_lender_pack/runner.py
from __future__ import annotations

import json
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd


def _ok(msg: str): print(f"[M8.B4][OK]  {msg}")
def _info(msg: str): print(f"[M8.B4][INFO] {msg}")
def _warn(msg: str): print(f"[M8.B4][WARN] {msg}")
def _fail(msg: str): raise RuntimeError(f"[M8.B4][FAIL] {msg}")

def _read_parquet(p: Path, what: str) -> pd.DataFrame:
    if not p.exists(): _fail(f"Missing {what}: {p}")
    df = pd.read_parquet(p)
    if df.empty: _fail(f"Empty {what}: {p}")
    return df

def _read_parquet_soft(p: Path, what: str) -> Optional[pd.DataFrame]:
    if not p.exists(): 
        _warn(f"{what} not found at {p}; continuing without it.")
        return None
    df = pd.read_parquet(p)
    if df.empty:
        _warn(f"{what} is empty at {p}; continuing without it.")
        return None
    return df

def _month_to_year_index(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    if "Calendar_Year" in out.columns:
        out["Year_Index"] = out["Calendar_Year"]
    else:
        out["Year_Index"] = ((out["Month_Index"] - 1) // 12) + 1
    return out

def _load_base(outputs: Path) -> Dict[str, pd.DataFrame]:
    pl = _read_parquet(outputs / "m7_5b_profit_and_loss.parquet", "M7.5B PL")
    bs = _read_parquet(outputs / "m7_5b_balance_sheet.parquet", "M7.5B BS")
    cf = _read_parquet(outputs / "m7_5b_cash_flow.parquet", "M7.5B CF")
    base = _read_parquet_soft(outputs / "m8b_base_timeseries.parquet", "M8B base timeseries")
    if base is not None:
        for d in (pl, bs, cf):
            d.drop(columns=[c for c in ["Calendar_Year","Calendar_Quarter"] if c in d.columns], inplace=True, errors="ignore")
        pl = pl.merge(base[["Month_Index","Calendar_Year","Calendar_Quarter"]], on="Month_Index", how="left")
        bs = bs.merge(base[["Month_Index","Calendar_Year","Calendar_Quarter"]], on="Month_Index", how="left")
        cf = cf.merge(base[["Month_Index","Calendar_Year","Calendar_Quarter"]], on="Month_Index", how="left")
    else:
        for d in (pl, bs, cf):
            d["Calendar_Year"] = ((d["Month_Index"] - 1) // 12) + 1
            d["Calendar_Quarter"] = ((d["Month_Index"] - 1) // 3) % 4 + 1
    return {"pl": pl, "bs": bs, "cf": cf}

def _load_revolver(outputs: Path) -> Optional[pd.DataFrame]:
    # Try standard location first
    r = _read_parquet_soft(outputs / "m3_revolver_schedule.parquet", "M3 revolver schedule")
    if r is None:
        # Some stacks place it at root outputs; already tried; nothing else to do
        return None
    # Normalize expected columns
    cols = r.columns
    # Required Month_Index
    if "Month_Index" not in cols:
        if "month" in cols: r = r.rename(columns={"month": "Month_Index"})
        else: _fail("M3 revolver schedule lacks 'Month_Index' column.")
    # Interest
    if "Interest_Accrued" not in cols:
        for c in ["Interest","Interest_Paid","Accrued_Interest"]:
            if c in cols: r = r.rename(columns={c: "Interest_Accrued"}); break
    if "Interest_Accrued" not in r.columns:
        _warn("Interest column not found in revolver; assuming zero interest for DSCR.")
        r["Interest_Accrued"] = 0.0
    # Repayment
    if "Repayment" not in r.columns:
        for c in ["Principal_Repayment","Principal_Out","Principal"]:
            if c in r.columns: r = r.rename(columns={c: "Repayment"}); break
    if "Repayment" not in r.columns:
        _warn("Repayment column not found in revolver; assuming zero principal for DSCR.")
        r["Repayment"] = 0.0
    # Closing balance
    if "Closing_Balance" not in r.columns:
        for c in ["Balance","Debt_Closing_Balance","Ending_Balance"]:
            if c in r.columns: r = r.rename(columns={c: "Closing_Balance"}); break
    if "Closing_Balance" not in r.columns:
        _warn("Closing_Balance not found in revolver; DSCR ok, LLCR/PLCR limited.")
        r["Closing_Balance"] = np.nan
    return r[["Month_Index","Repayment","Interest_Accrued","Closing_Balance"]].copy()

def _derive_cfads(pl: pd.DataFrame, cf: pd.DataFrame) -> pd.DataFrame:
    """
    CFADS variants (monthly, NAD '000):
      - CFADS_v1 = CFO + Interest_Expense (assumes interest included in CFO via indirect method).
      - CFADS_v2 = CFADS_v1 - Maintenance_CAPEX (if a maintenance CAPEX column is present upstream; else equals v1).
    We emit both; lenders can choose.
    """
    need = []
    if "CFO_NAD_000" not in cf.columns: need.append("CFO_NAD_000")
    if "Interest_Expense_NAD_000" not in pl.columns: need.append("Interest_Expense_NAD_000")
    if need:
        _warn(f"Missing columns for CFADS derivation: {need}. Falling back to CFADS_v1=CFO.")
    x = cf[["Month_Index","CFO_NAD_000"]].copy()
    interest = pl.get("Interest_Expense_NAD_000", pd.Series(0.0, index=x.index))
    # Align by Month_Index (PL may not be aligned to CF index)
    interest = pl[["Month_Index","Interest_Expense_NAD_000"]].set_index("Month_Index").squeeze() if "Interest_Expense_NAD_000" in pl.columns else None
    x["CFADS_v1_NAD_000"] = x["CFO_NAD_000"] + (interest.reindex(x["Month_Index"]).values if interest is not None else 0.0)
    # Maintenance CAPEX if present anywhere (common names)
    maint_candidates = [
        "Maintenance_CAPEX_NAD_000","Sustaining_CAPEX_NAD_000","Maintenance_Capex_NAD_000"
    ]
    maint = None
    for c in maint_candidates:
        if c in cf.columns: maint = cf.set_index("Month_Index")[c]; break
    if maint is None:
        _warn("Maintenance CAPEX not found; CFADS_v2 equals CFADS_v1.")
        x["CFADS_v2_NAD_000"] = x["CFADS_v1_NAD_000"]
    else:
        x["CFADS_v2_NAD_000"] = x["CFADS_v1_NAD_000"] - maint.reindex(x["Month_Index"]).fillna(0).values
    return x

def _npv(series: pd.Series, r: float) -> float:
    idx = np.arange(1, len(series)+1)
    return float(np.nansum(series.values / np.power(1.0 + r, idx)))

def _annual_to_monthly(r_annual: float) -> float:
    return (1.0 + r_annual) ** (1.0/12.0) - 1.0

def _derive_lender_metrics(pl: pd.DataFrame, bs: pd.DataFrame, cf: pd.DataFrame, revolver: Optional[pd.DataFrame],
                           discount_rate_annual: float) -> pd.DataFrame:
    # Base columns
    base = pd.DataFrame({"Month_Index": cf["Month_Index"].values}).drop_duplicates().sort_values("Month_Index")
    # CFADS variants
    cfads = _derive_cfads(pl, cf)
    base = base.merge(cfads, on="Month_Index", how="left")
    # Debt service from revolver
    if revolver is not None:
        r = revolver.groupby("Month_Index", as_index=False).agg({"Repayment":"sum","Interest_Accrued":"sum","Closing_Balance":"sum"})
        r["Debt_Service_NAD_000"] = r["Repayment"].fillna(0) + r["Interest_Accrued"].fillna(0)
        base = base.merge(r[["Month_Index","Debt_Service_NAD_000","Closing_Balance"]], on="Month_Index", how="left")
    else:
        base["Debt_Service_NAD_000"] = np.nan
        base["Closing_Balance"] = np.nan
        _warn("Revolver schedule missing; DSCR and coverage ratios may be NaN.")
    # DSCRs
    eps = 1e-9
    base["DSCR_v1"] = base["CFADS_v1_NAD_000"] / (base["Debt_Service_NAD_000"].replace(0, np.nan))
    base["DSCR_v2"] = base["CFADS_v2_NAD_000"] / (base["Debt_Service_NAD_000"].replace(0, np.nan))
    # ICR (EBITDA / Interest)
    EBITDA = pl.get("EBITDA_NAD_000")
    INT = pl.get("Interest_Expense_NAD_000")
    if EBITDA is not None and INT is not None:
        z = pl[["Month_Index","EBITDA_NAD_000","Interest_Expense_NAD_000"]].copy()
        z["ICR"] = z["EBITDA_NAD_000"] / z["Interest_Expense_NAD_000"].replace(0, np.nan)
        base = base.merge(z[["Month_Index","ICR"]], on="Month_Index", how="left")
    else:
        _warn("EBITDA or Interest not found in PL; ICR omitted.")
    # LTV proxy (Debt / Total Assets) using BS if available
    if "Total_Assets_NAD_000" in bs.columns:
        b2 = bs[["Month_Index","Total_Assets_NAD_000"]].copy()
        base = base.merge(b2, on="Month_Index", how="left")
        base["LTV_Proxy"] = base["Closing_Balance"] / base["Total_Assets_NAD_000"].replace(0, np.nan)
    else:
        _warn("Total_Assets_NAD_000 not in BS; LTV proxy omitted.")
    # LLCR & PLCR (monthly series)
    r_m = _annual_to_monthly(discount_rate_annual)
    # LLCR: NPV of CFADS over loan life / debt outstanding each month
    if base["Closing_Balance"].notna().any():
        # Determine last month with positive debt
        last_loan_m = int(base.loc[base["Closing_Balance"].fillna(0) > 0, "Month_Index"].max()
                          if (base["Closing_Balance"].fillna(0) > 0).any() else base["Month_Index"].max())
        # compute rolling NPV efficiently
        cf_v1 = base.set_index("Month_Index")["CFADS_v1_NAD_000"].fillna(0)
        cf_v2 = base.set_index("Month_Index")["CFADS_v2_NAD_000"].fillna(0)
        # For each t, NPV of t..last_loan_m
        def rolling_npv(cfser: pd.Series, end_m: int) -> pd.Series:
            # reverse discounting trick
            idx = cfser.index
            right = cfser.loc[idx[(idx>=idx.min()) & (idx<=end_m)]]
            # build cumulative DF from end to start
            k = np.arange(len(right)-1, -1, -1)  # distances from end
            dfs = np.power(1.0 + r_m, k)
            npv_from_start = np.cumsum((right.values / dfs)[::-1])[::-1]  # NPV from each point to end
            return pd.Series(npv_from_start, index=right.index)
        npv_v1 = rolling_npv(cf_v1, last_loan_m).reindex(base["Month_Index"])
        npv_v2 = rolling_npv(cf_v2, last_loan_m).reindex(base["Month_Index"])
        denom = base["Closing_Balance"].replace(0, np.nan)
        base["LLCR_v1"] = npv_v1 / denom
        base["LLCR_v2"] = npv_v2 / denom
        # PLCR: NPV over full model horizon / debt at month t
        npv_full_v1 = _npv(cf_v1.loc[cf_v1.index >= base["Month_Index"].min()], r_m)
        npv_full_v2 = _npv(cf_v2.loc[cf_v2.index >= base["Month_Index"].min()], r_m)
        base["PLCR_v1"] = npv_full_v1 / denom
        base["PLCR_v2"] = npv_full_v2 / denom
    else:
        _warn("Debt outstanding series missing; LLCR/PLCR omitted.")
    return base

def _yearly_agg(df: pd.DataFrame) -> pd.DataFrame:
    x = _month_to_year_index(df)
    keys = ["Year_Index"]
    flows_sum = [
        "CFADS_v1_NAD_000", "CFADS_v2_NAD_000", "Debt_Service_NAD_000"
    ]
    stocks_avg = ["Closing_Balance","Total_Assets_NAD_000"]
    ratios_avg = ["DSCR_v1","DSCR_v2","ICR","LTV_Proxy","LLCR_v1","LLCR_v2","PLCR_v1","PLCR_v2"]
    agg = {**{c:"sum" for c in flows_sum if c in x.columns},
           **{c:"mean" for c in stocks_avg if c in x.columns},
           **{c:"mean" for c in ratios_avg if c in x.columns}}
    y = x.groupby(keys, as_index=False).agg(agg)
    return y

def run_m8B4(outputs_dir: str, currency: str, strict: bool=False, diagnostic: bool=False):
    """
    M8.B4 Lender pack.
    Inputs  : M7.5B PL/BS/CF (parquet), optional M3 revolver schedule parquet.
    Outputs : m8b4_lender_metrics_monthly.parquet, m8b4_lender_metrics_yearly.parquet
              m8b4_debug.json, m8b4_smoke.md
    """
    out = Path(outputs_dir)
    _info(f"Starting M8.B4 lender engine in: {out}")
    dbg = {"inputs":{}, "warnings":[], "params":{}}
    try:
        base = _load_base(out)
        pl, bs, cf = base["pl"], base["bs"], base["cf"]
        dbg["inputs"]["pl_cols"] = list(pl.columns)[:50]
        dbg["inputs"]["bs_cols"] = list(bs.columns)[:50]
        dbg["inputs"]["cf_cols"] = list(cf.columns)[:50]
        # Discount rate
        terms = _read_parquet_soft(out/"m8b_terms.json", "M8B terms")  # may be json not parquet
    except Exception:
        terms = None
    # Read discount from m8b_terms.json if present
    disc_annual = None
    terms_json = out / "m8b_terms.json"
    if terms_json.exists():
        t = json.loads(terms_json.read_text(encoding="utf-8"))
        disc_annual = t.get("llcr_discount_rate_annual", t.get("debt_discount_rate_annual"))
        if disc_annual is not None: _ok(f"Discount rate (annual) from terms: {disc_annual:.4f}")
    if disc_annual is None:
        disc_annual = 0.12
        _warn(f"No discount rate found; defaulting to 12.0% annual.")
    dbg["params"]["discount_rate_annual"] = disc_annual

    # Revolver
    revolver = _load_revolver(out)
    if revolver is None and strict:
        _fail("Revolver schedule required for DSCR/LLCR/PLCR in strict=True.")

    # Build metrics
    monthly = _derive_lender_metrics(pl, bs, cf, revolver, disc_annual)
    yearly = _yearly_agg(monthly)

    # Emit
    (out/"m8b4_lender_metrics_monthly.parquet").write_bytes(monthly.to_parquet())
    (out/"m8b4_lender_metrics_yearly.parquet").write_bytes(yearly.to_parquet())
    _ok("Emitted: m8b4_lender_metrics_monthly.parquet, m8b4_lender_metrics_yearly.parquet")

    # Smoke
    smoke = []
    smoke.append(f"[SMOKE] Monthly shape={monthly.shape} Yearly shape={yearly.shape}")
    for c in ["DSCR_v1","ICR","LLCR_v1","PLCR_v1","Debt_Service_NAD_000"]:
        if c in monthly.columns: smoke.append(f"[SMOKE] {c} notnull={monthly[c].notnull().sum()}")
    (out/"m8b4_smoke.md").write_text("\n".join(smoke), encoding="utf-8")
    _ok("Smoke → m8b4_smoke.md")

    # Debug
    dbg["shapes"] = {"monthly": list(monthly.shape), "yearly": list(yearly.shape)}
    (out/"m8b4_debug.json").write_text(json.dumps(dbg, indent=2), encoding="utf-8")
    _ok("Debug → m8b4_debug.json")

except_block = """
if __name__ == "__main__":
    run_m8B4(r".\\outputs", "NAD", strict=False, diagnostic=False)
"""
