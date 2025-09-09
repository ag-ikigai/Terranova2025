# -*- coding: utf-8 -*-
"""
M7.5B — Rebuild Financial Statements with Junior Capital (NAD + USD views)

- Reclassify opening USD 500k per selected option (equity vs debt-like).
- Subordinate junior payouts to senior debt service.
- Produce rebuilt P&L, CF (junior layer), and BS with USD conversions via FX_Path.
"""

from __future__ import annotations
import json, re, math
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd


# --------- helpers ------------------------------------------------------------------------------

def _read_parquet(p: Path) -> pd.DataFrame:
    if not p.exists():
        raise FileNotFoundError(f"Missing required file: {p}")
    return pd.read_parquet(p)

def _role(df: pd.DataFrame, wanted: List[str]) -> str:
    cols = list(df.columns)
    for w in wanted:
        if w in cols: return w
    # normalized match
    norm = {c.lower().replace(" ", "").replace("-", "_"): c for c in cols}
    for w in wanted:
        k = w.lower().replace(" ", "").replace("-", "_")
        if k in norm: return norm[k]
    raise KeyError(f"Could not resolve role among {wanted}. Available: {cols}")

def _try_role(df: pd.DataFrame, wanted: List[str], default: Optional[str]=None) -> Optional[str]:
    try:
        return _role(df, wanted)
    except KeyError:
        return default

def _ensure_out_dir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)

def _parse_pct_from_text(text: str, keyword: str, default: float) -> float:
    if not text: return default
    # find e.g. "8% PIK" or "8 % pref"
    m = re.search(rf"(\d+(\.\d+)?)\s*%\s*{re.escape(keyword)}", text, re.IGNORECASE)
    if m:
        return float(m.group(1))
    return default

def _first_num(v) -> float:
    try: return float(v)
    except Exception: return 0.0

# --------- policy knobs -------------------------------------------------------------------------

@dataclass
class Policy:
    default_pik_apy_pct: float = 8.0           # for convertible notes if not parsed
    default_pref_apy_pct: float = 8.0          # preferred equity "pref"
    dscr_buffer_nad_000: float = 0.0           # keep >0 if you want to hold buffer before junior
    tax_floor: float = 0.0
    tax_cap: float = 0.35                      # clamp effective tax rate
    tie_tol: float = 1e-6

# --------- main ---------------------------------------------------------------------------------

def run_m7_5b(out_dir: str, currency: str = "NAD",
              policy: Policy = Policy()) -> None:
    """
    Rebuilds statements with junior capital layer.
    """
    out = Path(out_dir)
    _ensure_out_dir(out)

    # --- inputs ---
    sel = json.loads(Path(out, "m7_selected_offer.json").read_text(encoding="utf-8"))
    instrument = str(sel.get("Instrument", "")).strip()
    option = str(sel.get("Option", "")).strip()
    revshare_pct = _first_num(sel.get("RevShare_preRefi_pct"))
    min_irr_floor_pct = _first_num(sel.get("Min_IRR_Floor_pct"))
    conv_terms = str(sel.get("Conversion_Terms", "") or "")

    df_jun = _read_parquet(out / "m7_5_junior_financing.parquet")
    df_pl  = _read_parquet(out / "m2_pl_schedule.parquet")
    df_m5  = _read_parquet(out / "m5_cash_flow_statement_final.parquet")
    df_bs6 = _read_parquet(out / "m6_balance_sheet.parquet")
    fx     = _read_parquet(out / "m0_inputs" / "FX_Path.parquet")
    # optional M3 schedule to compute senior debt service (subordination)
    df_m3 = None
    if (out / "m3_revolver_schedule.parquet").exists():
        df_m3 = pd.read_parquet(out / "m3_revolver_schedule.parquet")

    # roles / synonyms
    r_month = "Month_Index"
    r_fx = _role(fx, ["USD_to_NAD", "NAD_per_USD", "FX_USD_to_NAD", "FX_Rate"])
    r_pl_rev = _role(df_pl, ["Total_Revenue_NAD_000"])
    r_pl_int = _role(df_pl, ["Interest_Expense_NAD_000", "Interest_Expense"])
    r_pl_pbt = _role(df_pl, ["PBT_NAD_000", "Profit_Before_Tax_NAD_000"])
    r_pl_tax = _role(df_pl, ["Tax_Expense_NAD_000", "Tax_Expense"])
    r_pl_npat = _role(df_pl, ["NPAT_NAD_000", "Net_Profit_After_Tax_NAD_000"])

    r_m5_cfo = _role(df_m5, ["CFO_NAD_000","Cash_Flow_from_Operations_NAD_000","Operating_Cash_Flow_NAD_000"])
    r_bs_cash = _role(df_bs6, ["Cash_NAD_000","Cash_and_Cash_Equivalents_NAD_000","Cash_and_Cash_Equivalents"])
    r_bs_atot = _role(df_bs6, ["Assets_Total_NAD_000"])
    r_bs_letot= _role(df_bs6, ["Liabilities_And_Equity_Total_NAD_000"])

    r_j_in = _role(df_jun, ["Cash_In_NAD_000","Injection_NAD_000","Amount_NAD_000","NAD_000"])
    r_j_out = _try_role(df_jun, ["Cash_Out_NAD_000","Payout_NAD_000","Junior_Out_NAD_000"])

    df_pl = df_pl[[r_month, r_pl_rev, r_pl_int, r_pl_pbt, r_pl_tax, r_pl_npat]].copy()
    df_pl.rename(columns={r_pl_rev:"Total_Revenue_NAD_000",
                          r_pl_int:"Interest_Expense_NAD_000",
                          r_pl_pbt:"PBT_NAD_000",
                          r_pl_tax:"Tax_Expense_NAD_000",
                          r_pl_npat:"NPAT_NAD_000"}, inplace=True)

    df_m5 = df_m5[[r_month, r_m5_cfo]].copy().rename(columns={r_m5_cfo:"CFO_NAD_000"})
    df_bs = df_bs6[[r_month, r_bs_cash, r_bs_atot, r_bs_letot]].copy().rename(
                columns={r_bs_cash:"Cash_NAD_000",
                         r_bs_atot:"Assets_Total_NAD_000",
                         r_bs_letot:"Liabilities_And_Equity_Total_NAD_000"})
    fx = fx[[r_month, r_fx]].copy().rename(columns={r_fx:"USD_to_NAD"})

    # align monthly index universe
    for d in (df_pl, df_m5, df_bs, df_jun, fx):
        if r_month not in d.columns:
            raise AssertionError("Missing Month_Index in one input.")
    # left‑join baseline horizon
    base = df_pl[[r_month]].merge(df_m5, on=r_month, how="left").merge(df_bs, on=r_month, how="left").merge(fx, on=r_month, how="left")
    base.sort_values(r_month, inplace=True)
    base.reset_index(drop=True, inplace=True)

    # effective tax rate from M2 (clamped)
    etr_series = pd.Series(0.0, index=base.index)
    mask = df_pl["PBT_NAD_000"] > 1e-9
    etr = 0.0
    if mask.any():
        etr_raw = (df_pl.loc[mask, "Tax_Expense_NAD_000"] / df_pl.loc[mask, "PBT_NAD_000"]).clip(policy.tax_floor, policy.tax_cap)
        etr = float(np.nanmean(etr_raw))
    etr_series[:] = etr

    # junior classification
    instr_lower = instrument.lower()
    is_safe = "safe" in instr_lower
    is_conv = "convertible" in instr_lower
    is_pref = "pref" in instr_lower or "preferred" in instr_lower
    is_rev  = "revshare" in instr_lower or "revenue share" in instr_lower or "rev share" in instr_lower
    # parse terms
    pik_pct = _parse_pct_from_text(conv_terms, "PIK", policy.default_pik_apy_pct) if is_conv else 0.0
    pref_pct = _parse_pct_from_text(conv_terms, "pref", policy.default_pref_apy_pct) if is_pref else 0.0

    # junior injections schedule (we DO NOT move cash here; opening already included in m0/m6)
    jun = df_jun[[r_month, r_j_in]].copy()
    jun.rename(columns={r_j_in:"Junior_In_NAD_000"}, inplace=True)
    if r_j_out:
        jun["Junior_Out_Scheduled_NAD_000"] = df_jun[r_j_out]
    else:
        jun["Junior_Out_Scheduled_NAD_000"] = 0.0

    # opening reclass (equity->debt for convertibles); take earliest injection as proxy for opening
    opening_month = int(jun[r_month].min()) if not jun.empty else int(base[r_month].min())
    opening_inj = float(jun.loc[jun[r_month]==opening_month, "Junior_In_NAD_000"].sum())
    junior_equity_reclass = 0.0
    if is_conv:
        junior_equity_reclass = opening_inj  # move from equity to junior debt at opening

    # --- junior accruals/payouts ---------------------------------------------------------------
    n = len(base)
    arr = lambda: np.zeros(n, dtype=float)

    junior_debt_eop = arr()
    junior_interest_exp = arr()
    revshare_sched = arr()
    revshare_paid = arr()
    pref_accrual = arr()  # equity appropriation (not P&L)
    cff_in = arr()
    cff_out = arr()

    # CFO & senior service for subordination
    cfo = base["CFO_NAD_000"].fillna(0.0).to_numpy()
    senior_service = arr()
    if df_m3 is not None:
        r_int_paid = _try_role(df_m3, ["Interest_Paid_NAD_000","Cash_Interest_Paid_NAD_000","Interest_Paid"])
        r_pri_paid = _try_role(df_m3, ["Principal_Paid_NAD_000","Principal_Repayment_NAD_000","Principal_Paid"])
        if r_int_paid and r_pri_paid:
            svc = df_m3[[r_month, r_int_paid, r_pri_paid]].copy()
            svc.rename(columns={r_int_paid:"IntPaid", r_pri_paid:"PrinPaid"}, inplace=True)
            base = base.merge(svc, on=r_month, how="left")
            senior_service = (base["IntPaid"].fillna(0.0) + base["PrinPaid"].fillna(0.0)).to_numpy()

    # revenue share schedule based on M2 revenue (pre‑refi notion; we do not infer refi month here)
    if revshare_pct > 0.0 or is_rev:
        revshare_sched = (df_pl["Total_Revenue_NAD_000"].fillna(0.0).to_numpy() * (revshare_pct/100.0))

    # preferred equity accrual (appropriation in equity, not P&L)
    if is_pref:
        # outstanding base approximated by opening injection (no amortization here)
        pref_month_rate = (pref_pct/100.0)/12.0
        pref_accrual[:] = opening_inj * pref_month_rate

    # convertible: accrue PIK; recognize P&L finance cost; add to liability EOP
    if is_conv:
        month_rate = (pik_pct/100.0)/12.0
        principal = junior_equity_reclass  # starts from opening reclass
        for i in range(n):
            interest = principal * month_rate
            junior_interest_exp[i] = interest
            principal += interest  # PIK capitalization
            junior_debt_eop[i] = principal

    # subordination: actual junior payout cannot exceed available residual after senior service
    # availability proxy = max(0, CFO - senior service - buffer)
    available = np.maximum(0.0, cfo - senior_service - policy.dscr_buffer_nad_000)
    # scheduled = revshare only (dividends assumed deferred; convertible interest is PIK → no cash)
    scheduled_out = revshare_sched.copy()
    actual_paid = np.minimum(scheduled_out, available)
    revshare_paid[:] = np.where(scheduled_out>0, actual_paid, 0.0)

    # CFF layer (no new cash in; opening capital already in m0/m6)
    cff_in[:] = 0.0
    cff_out[:] = revshare_paid  # if later we allow pref dividends, add here under subordination gate

    # --- rebuild statements (junior layer only affects certain lines) ---------------------------
    # P&L: add junior interest expense, recompute PBT/tax/NPAT with constant ETR
    out_pl = base[[r_month]].copy()
    out_pl["Total_Revenue_NAD_000"] = df_pl["Total_Revenue_NAD_000"]
    out_pl["EBIT_NAD_000"] = (df_pl["Total_Revenue_NAD_000"]
                              - (df_pl["Total_Revenue_NAD_000"] - df_pl["PBT_NAD_000"]
                                 - df_pl["Interest_Expense_NAD_000"]  # reverse-engineer EBIT from M2
                                 - df_pl["Tax_Expense_NAD_000"])).astype(float)
    out_pl["Interest_Expense_NAD_000"] = df_pl["Interest_Expense_NAD_000"]
    out_pl["Junior_Interest_Expense_NAD_000"] = junior_interest_exp
    out_pl["PBT_Rebuilt_NAD_000"] = (df_pl["PBT_NAD_000"] - df_pl["Interest_Expense_NAD_000"]
                                     + out_pl["Interest_Expense_NAD_000"]
                                     - out_pl["Junior_Interest_Expense_NAD_000"])
    # recompute tax on rebuilt PBT
    out_pl["Tax_Expense_Rebuilt_NAD_000"] = np.maximum(0.0, out_pl["PBT_Rebuilt_NAD_000"] * etr_series)
    out_pl["NPAT_Rebuilt_NAD_000"] = out_pl["PBT_Rebuilt_NAD_000"] - out_pl["Tax_Expense_Rebuilt_NAD_000"]

    # CF: junior layer only
    out_cf = base[[r_month]].copy()
    out_cf["CFO_NAD_000"] = base["CFO_NAD_000"].fillna(0.0)
    out_cf["CFF_Junior_In_NAD_000"] = cff_in
    out_cf["CFF_Junior_Out_NAD_000"] = cff_out
    out_cf["CFF_Junior_Net_NAD_000"] = out_cf["CFF_Junior_In_NAD_000"] - out_cf["CFF_Junior_Out_NAD_000"]
    out_cf["Net_CF_Impact_From_Junior_NAD_000"] = out_cf["CFF_Junior_Net_NAD_000"]

    # BS: adjust cash by cumulative junior CFF outflows; add junior liability and equity reclass
    out_bs = base[[r_month]].copy()
    cash_base = base["Cash_NAD_000"].fillna(0.0).to_numpy()
    cash_adj = cash_base + np.cumsum(out_cf["Net_CF_Impact_From_Junior_NAD_000"].to_numpy())
    out_bs["Cash_NAD_000_Rebuilt"] = cash_adj
    out_bs["Junior_Debt_EOP_NAD_000"] = junior_debt_eop
    out_bs["Junior_Equity_Reclass_NAD_000"] = junior_equity_reclass  # constant cum value
    # Keep other assets/liabs unchanged except for junior insertions; recompute totals
    other_assets = base["Assets_Total_NAD_000"].fillna(0.0).to_numpy() - cash_base
    assets_total = other_assets + out_bs["Cash_NAD_000_Rebuilt"].to_numpy()
    other_le = base["Liabilities_And_Equity_Total_NAD_000"].fillna(0.0).to_numpy()
    # we replace opening equity portion with junior debt: liabilities + junior_debt, equity - reclass -> but totals unchanged.
    le_total = other_le + (out_bs["Junior_Debt_EOP_NAD_000"] - 0.0)  # total already included equity; adding liability requires a counter in equity
    # offset equity: reduce equity by the same opening reclass (constant) and retained earnings delta from P&L change
    retained_delta = np.cumsum((out_pl["NPAT_Rebuilt_NAD_000"] - df_pl["NPAT_NAD_000"]).to_numpy())
    le_total = le_total - junior_equity_reclass + retained_delta
    out_bs["Assets_Total_NAD_000_Rebuilt"] = assets_total
    out_bs["Liabilities_And_Equity_Total_NAD_000_Rebuilt"] = le_total

    # tie check
    diff = np.abs(out_bs["Assets_Total_NAD_000_Rebuilt"] - out_bs["Liabilities_And_Equity_Total_NAD_000_Rebuilt"]).max()
    if diff > policy.tie_tol:
        raise AssertionError(f"[M7.5B] Balance sheet does not tie within tolerance. Max diff={diff}")

    # --- USD views via FX path -------------------------------------------------------------------
    def _usd_cols(df: pd.DataFrame, except_cols: List[str]) -> pd.DataFrame:
        merged = df.merge(fx, on=r_month, how="left")
        rate = merged["USD_to_NAD"].replace(0, np.nan)
        out = df.copy()
        for c in df.columns:
            if c in except_cols: continue
            if c.endswith("_NAD_000"):
                out[c.replace("_NAD_000","_USD_000")] = df[c] / rate
            if c.endswith("_NAD_000_Rebuilt"):
                out[c.replace("_NAD_000_Rebuilt","_USD_000_Rebuilt")] = df[c] / rate
        return out

    out_pl = _usd_cols(out_pl, [r_month])
    out_cf = _usd_cols(out_cf, [r_month])
    out_bs = _usd_cols(out_bs, [r_month])

    # --- write artifacts ------------------------------------------------------------------------
    p_pl = out / "m7_5b_profit_and_loss.parquet"
    p_cf = out / "m7_5b_cash_flow.parquet"
    p_bs = out / "m7_5b_balance_sheet.parquet"
    out_pl.to_parquet(p_pl, index=False)
    out_cf.to_parquet(p_cf, index=False)
    out_bs.to_parquet(p_bs, index=False)

    debug = {
        "module": "m7_5b_rebuild",
        "option": option,
        "instrument": instrument,
        "revshare_pct": revshare_pct,
        "pik_pct": pik_pct,
        "pref_pct": pref_pct,
        "junior_equity_reclass_NAD_000": junior_equity_reclass,
        "effective_tax_rate_used": etr,
        "subordination": {
            "has_senior_schedule": df_m3 is not None,
            "dscr_buffer_nad_000": policy.dscr_buffer_nad_000
        },
        "fx_source": "outputs/m0_inputs/FX_Path.parquet",
        "notes": [
            "SAFE/Pref treated as equity. Convertible treated as liability with PIK interest (P&L finance cost).",
            "Revenue-share payouts are modeled as financing cash flows (CFF) and subordinated to senior debt.",
            "Opening USD 500k assumed pre-loaded in M0/M6 cash; no new cash injected by M7.5B."
        ]
    }
    Path(out, "m7_5b_debug.json").write_text(json.dumps(debug, indent=2), encoding="utf-8")

    # smoke report
    md = []
    md.append(f"# M7.5B — Rebuild OK\n")
    md.append(f"- Selected: **{option} / {instrument}**")
    md.append(f"- ETR used: {etr:.2%}")
    md.append(f"- Junior reclass (opening) NAD'000: {junior_equity_reclass:,.1f}")
    md.append(f"- Tie check diff max: {diff:.6f}")
    Path(out, "m7_5b_smoke_report.md").write_text("\n".join(md), encoding="utf-8")

    print(f"[OK] M7.5B rebuilt statements -> {p_pl.name}, {p_cf.name}, {p_bs.name}. Smoke -> {str(out/'m7_5b_smoke_report.md')}")
