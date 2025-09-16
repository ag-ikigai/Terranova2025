# -*- coding: utf-8 -*-
"""
M2 – Working Capital (PL stub + WC schedule)
--------------------------------------------
Reads:
  • outputs/m1_revenue_schedule.parquet  (from M1)
  • Input Pack (Working_Capital_Tax)     [optional, for policies]

Emits:
  • outputs/m2_working_capital_schedule.parquet
  • outputs/m2_profit_and_loss_stub.parquet
  • outputs/m2_debug_dump.json           [diagnostic=True]
  • outputs/m2_smoke_report.md           [diagnostic=True]

Column names are kept backward‑compatible with M5/M6/M7.5B expectations.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Dict, Tuple

import numpy as np
import pandas as pd


# ---------- helpers ----------------------------------------------------------

def _log(msg: str) -> None:
    print(f"[M2]{msg}")

def _read_parquet_safe(p: Path, what: str) -> pd.DataFrame:
    if not p.exists():
        raise FileNotFoundError(f"[M2][FAIL] Missing {what}: {p}")
    df = pd.read_parquet(p)
    if df.empty:
        raise ValueError(f"[M2][FAIL] Empty {what}: {p}")
    return df

def _to_py(obj):
    """Recursively convert numpy/pandas scalars to built‑ins (JSON‑safe)."""
    if isinstance(obj, (np.integer,)):
        return int(obj)
    if isinstance(obj, (np.floating,)):
        return float(obj)
    if isinstance(obj, (np.bool_,)):
        return bool(obj)
    if isinstance(obj, (pd.Timestamp,)):
        return obj.isoformat()
    if isinstance(obj, dict):
        return {k: _to_py(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [_to_py(v) for v in obj]
    return obj

def _load_policies_from_input_pack(ip_path: Path) -> Tuple[float, float, float, float, str]:
    """
    Look for worksheet 'Working_Capital_Tax' and parse AR/INV/AP days and COGS% (0–1 or 0–100).
    Returns (ar_days, inv_days, ap_days, cogs_ratio, source_note).
    """
    if not ip_path or not ip_path.exists():
        # defaults if pack is not supplied
        return 21.0, 20.0, 30.0, 0.60, "defaults"

    xls = pd.ExcelFile(ip_path)
    if "Working_Capital_Tax" not in xls.sheet_names:
        return 21.0, 20.0, 30.0, 0.60, "defaults(no Working_Capital_Tax)"

    df = pd.read_excel(ip_path, sheet_name="Working_Capital_Tax")
    # Flexible header matching
    cols = {c.strip().lower().replace(" ", "_"): c for c in df.columns}
    # Accept common variants
    def _pick(*names) -> str | None:
        for nm in names:
            key = nm.lower().replace(" ", "_")
            if key in cols:
                return cols[key]
        return None

    c_ar   = _pick("AR_Days", "Accounts Receivable Days", "Receivables_Days")
    c_inv  = _pick("Inventory_Days", "INV_Days")
    c_ap   = _pick("AP_Days", "Accounts Payable Days", "Payables_Days")
    c_cogs = _pick("COGS_Percent", "COGS_%", "COGS_pct")

    # If the sheet uses a single row of key/values, take first non‑NA
    def _first_val(col, default):
        if col is None or col not in df.columns:
            return default
        v = df[col].dropna()
        return float(v.iloc[0]) if not v.empty else default

    ar_days  = _first_val(c_ar, 21.0)
    inv_days = _first_val(c_inv, 20.0)
    ap_days  = _first_val(c_ap, 30.0)
    cogs     = _first_val(c_cogs, 60.0)

    # Normalize COGS to 0–1
    cogs_ratio = cogs / 100.0 if cogs > 1.0 else cogs

    return float(ar_days), float(inv_days), float(ap_days), float(cogs_ratio), "Working_Capital_Tax"

# ---------- core -------------------------------------------------------------

def run_m2(outputs: str | Path,
           currency: str = "NAD",
           *,
           strict: bool = True,
           diagnostic: bool = False,
           input_pack: str | Path | None = None) -> None:
    out = Path(outputs)
    out.mkdir(parents=True, exist_ok=True)

    _log(f"[INFO] Starting M2 in: {out.resolve()}")

    # 1) Revenue from M1
    m1_rev_p = out / "m1_revenue_schedule.parquet"
    rev = _read_parquet_safe(m1_rev_p, "M1 revenue")
    # Expect Month_Index + per‑crop revenue columns and 'Monthly_Revenue_NAD_000'
    month_col = "Month_Index" if "Month_Index" in rev.columns else [c for c in rev.columns if "month" in c.lower()][0]
    if "Monthly_Revenue_NAD_000" not in rev.columns:
        # Fallback – sum *_Revenue_NAD_000
        money_cols = [c for c in rev.columns if c.endswith("_NAD_000")]
        rev["Monthly_Revenue_NAD_000"] = rev[money_cols].sum(axis=1)
    s_rev = rev.groupby(month_col, as_index=True)["Monthly_Revenue_NAD_000"].sum()

    _log(f"[OK]  Loaded M1 revenue ({len(rev)} rows).")

    # 2) Policies from Input Pack (if provided)
    ip_path = Path(input_pack) if input_pack else None
    ar_days, inv_days, ap_days, cogs_ratio, source_note = _load_policies_from_input_pack(ip_path)
    _log(f"[OK]  Policies (source={source_note}) -> AR={ar_days:.1f}d, INV={inv_days:.1f}d, AP={ap_days:.1f}d, COGS%={cogs_ratio*100:.2f}%.")

    # 3) Working capital balances
    #    AR = Revenue * AR_days/30
    #    COGS = Revenue * cogs_ratio
    #    INV = COGS * INV_days/30
    #    AP = COGS * AP_days/30
    #    NWC = AR + INV - AP
    #    CF from ΔNWC = -(NWC_t - NWC_{t-1})
    cogs = s_rev * cogs_ratio
    ar   = s_rev * (ar_days / 30.0)
    inv  = cogs  * (inv_days / 30.0)
    ap   = cogs  * (ap_days / 30.0)
    nwc  = ar + inv - ap
    dlt  = nwc.diff().fillna(nwc)  # first month delta = level
    cf_nwc = -dlt

    wc = pd.DataFrame({
        "Month_Index": s_rev.index.astype(int),
        "AR_Balance_NAD_000": ar.values,
        "Inventory_Balance_NAD_000": inv.values,
        "AP_Balance_NAD_000": ap.values,
        "NWC_Balance_NAD_000": nwc.values,
        "Cash_Flow_from_NWC_Change_NAD_000": cf_nwc.values,
    })

    (out / "m2_working_capital_schedule.parquet").write_bytes(wc.to_parquet(index=False))
    _log("[OK]  Emitted: m2_working_capital_schedule.parquet")

    # 4) Tiny PL stub to keep M5 happy (names used by M5)
    #    Depreciation: if M1 depreciation schedule exists, use it; otherwise zero.
    dep_p = out / "m1_depreciation_schedule.parquet"
    if dep_p.exists():
        dep = pd.read_parquet(dep_p)
        dep_m = "Month_Index" if "Month_Index" in dep.columns else [c for c in dep.columns if "month" in c.lower()][0]
        dep_col = "Monthly_Depreciation_NAD_000"
        if dep_col not in dep.columns:
            # best effort: any *_Depreciation_NAD_000
            cand = [c for c in dep.columns if "depreciation" in c.lower() and c.endswith("_NAD_000")]
            dep_col = cand[0] if cand else dep.columns[-1]
        s_dep = dep.groupby(dep_m)[dep_col].sum().reindex(s_rev.index, fill_value=0.0)
    else:
        s_dep = pd.Series(0.0, index=s_rev.index)

    pl_stub = pd.DataFrame({
        "Month_Index": s_rev.index.astype(int),
        "NPAT_NAD_000": 0.0,                      # placeholder
        "Depreciation_NAD_000": s_dep.values,    # used by M5
    })
    (out / "m2_profit_and_loss_stub.parquet").write_bytes(pl_stub.to_parquet(index=False))
    _log("[OK]  Emitted: m2_profit_and_loss_stub.parquet")

    # 5) Diagnostics
    if diagnostic:
        dbg_out: Dict[str, object] = {
            "policies": {
                "source": source_note,
                "AR_days": ar_days,
                "INV_days": inv_days,
                "AP_days": ap_days,
                "COGS_ratio": cogs_ratio,
            },
            "first_12_nwc_cf": wc.set_index("Month_Index")["Cash_Flow_from_NWC_Change_NAD_000"].head(12).to_dict(),
        }
        (out / "m2_debug_dump.json").write_text(json.dumps(_to_py(dbg_out), indent=2), encoding="utf-8")
        smoke = []
        smoke.append("# M2 Smoke")
        smoke.append(f"- months: {int(s_rev.index.min())}..{int(s_rev.index.max())}")
        smoke.append(f"- policies: AR={ar_days:.1f}d, INV={inv_days:.1f}d, AP={ap_days:.1f}d, COGS%={cogs_ratio*100:.2f}%")
        smoke.append("")
        smoke.append("**First 12 months ΔNWC cash flow (NAD '000):**")
        s12 = wc.set_index("Month_Index")["Cash_Flow_from_NWC_Change_NAD_000"].head(12)
        smoke.append(s12.to_string())
        (out / "m2_smoke_report.md").write_text("\n".join(smoke), encoding="utf-8")

    _log("[OK]  Debug/Smoke written.")


if __name__ == "__main__":
    run_m2("./outputs", "NAD", strict=True, diagnostic=True)
