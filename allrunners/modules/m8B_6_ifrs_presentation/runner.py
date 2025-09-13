# src/terra_nova/modules/m8B_6_ifrs_presentation/runner.py
from __future__ import annotations

import json
from pathlib import Path
from typing import Dict, List
import pandas as pd
import numpy as np

def _ok(m): print(f"[M8.B6][OK]  {m}")
def _info(m): print(f"[M8.B6][INFO] {m}")
def _warn(m): print(f"[M8.B6][WARN] {m}")
def _fail(m): raise RuntimeError(f"[M8.B6][FAIL] {m}")

IFRS_PL_MAP = {
    "Revenue": ["Total_Revenue_NAD_000","Revenue_NAD_000"],
    "Operating expenses": ["Total_OPEX_NAD_000"],
    "Depreciation and amortisation": ["Depreciation_NAD_000"],
    "Operating profit (EBIT)": ["EBIT_NAD_000"],
    "Finance costs": ["Interest_Expense_NAD_000"],
    "Income tax expense": ["Tax_Expense_NAD_000"],
    "Profit for the period": ["NPAT_NAD_000"]
}
# Non-GAAP helpful subtotal (shown but flagged as non‑IFRS subtotal)
IFRS_PL_AUX = {"EBITDA (non‑GAAP subtotal)": ["EBITDA_NAD_000"]}

IFRS_BS_MAP = {
    "Cash and cash equivalents": ["Cash_and_Cash_Equivalents_NAD_000"],
    "Current assets": ["Current_Assets_NAD_000"],
    "Current liabilities": ["Current_Liabilities_NAD_000"],
    "Total assets": ["Total_Assets_NAD_000"],
    "Total liabilities and equity": ["Liabilities_and_Equity_Total_NAD_000"]
}

IFRS_CF_MAP = {
    "Cash flows from operating activities": ["CFO_NAD_000"],
    "Cash flows from investing activities": ["CFI_NAD_000"],
    "Cash flows from financing activities": ["CFF_NAD_000"],
    "Closing cash": ["Closing_Cash_NAD_000"]
}

def _read_parquet(p: Path, what: str) -> pd.DataFrame:
    if not p.exists(): _fail(f"Missing {what}: {p}")
    df = pd.read_parquet(p)
    if df.empty: _fail(f"Empty {what}: {p}")
    return df

def _longify(statement: str, df: pd.DataFrame, mapping: Dict[str, List[str]]) -> pd.DataFrame:
    rows = []
    for ifrs_line, candidates in mapping.items():
        col = None
        for c in candidates:
            if c in df.columns: col = c; break
        if col is None:
            _warn(f"{statement} → '{ifrs_line}' not found from {candidates}; line omitted.")
            continue
        tmp = df[["Month_Index", col]].copy()
        tmp["Statement"] = statement
        tmp["IFRS_Line"] = ifrs_line
        tmp["Value_NAD_000"] = tmp[col]
        rows.append(tmp[["Statement","IFRS_Line","Month_Index","Value_NAD_000"]])
    if not rows: return pd.DataFrame(columns=["Statement","IFRS_Line","Month_Index","Value_NAD_000"])
    return pd.concat(rows, ignore_index=True)

def _add_year(df: pd.DataFrame) -> pd.DataFrame:
    x = df.copy()
    x["Year_Index"] = ((x["Month_Index"] - 1)//12)+1
    return x

def run_m8B6(outputs_dir: str, currency: str, strict: bool=False, diagnostic: bool=False):
    """
    Builds IFRS‑presentable long‑form statements + notes scaffolding:
      - m8b_ifrs_statements.parquet  (Statement, IFRS_Line, Month_Index, Year_Index, Currency, Value)
      - m8b_ifrs_mapping.json        (which M7.5B columns mapped where; missing lines)
      - m8b_ifrs_notes.json          (placeholders for M9 narrative)
    """
    out = Path(outputs_dir)
    _info(f"Starting M8.B6 IFRS presentation in: {out}")
    pl = _read_parquet(out/"m7_5b_profit_and_loss.parquet", "M7.5B PL")
    bs = _read_parquet(out/"m7_5b_balance_sheet.parquet", "M7.5B BS")
    cf = _read_parquet(out/"m7_5b_cash_flow.parquet", "M7.5B CF")

    pl_long = _longify("Profit or loss", pl, IFRS_PL_MAP)
    if IFRS_PL_AUX:
        aux = _longify("Profit or loss (auxiliary)", pl, IFRS_PL_AUX)
        pl_long = pd.concat([pl_long, aux], ignore_index=True)

    bs_long = _longify("Balance sheet", bs, IFRS_BS_MAP)
    cf_long = _longify("Statement of cash flows (IAS 7)", cf, IFRS_CF_MAP)

    all_long = pd.concat([pl_long, bs_long, cf_long], ignore_index=True)
    all_long["Currency"] = currency
    all_long = _add_year(all_long)

    # Emit statements
    all_long.to_parquet(out/"m8b_ifrs_statements.parquet")
    _ok("Emitted: m8b_ifrs_statements.parquet")

    # Mapping report (what was found / missing)
    mapping = {"pl":{}, "bs":{}, "cf":{}}
    for k,mp in [("pl",IFRS_PL_MAP|IFRS_PL_AUX), ("bs",IFRS_BS_MAP), ("cf",IFRS_CF_MAP)]:
        mapping[k] = {"lines":[]}
        cols = {"pl":pl.columns,"bs":bs.columns,"cf":cf.columns}[k]
        for ifrs_line, cands in mp.items():
            found = next((c for c in cands if c in cols), None)
            mapping[k]["lines"].append({"ifrs_line": ifrs_line, "from": found, "candidates": cands, "missing": found is None})
    (out/"m8b_ifrs_mapping.json").write_text(json.dumps(mapping, indent=2), encoding="utf-8")
    _ok("Emitted: m8b_ifrs_mapping.json")

    # Notes scaffolding (placeholders for M9 narrative)
    notes = {
      "1_basis_of_preparation": {
        "status":"TBD",
        "hint":"IFRS-18 presentation basis; IAS 1 current/non-current; measurement bases."
      },
      "2_significant_accounting_policies": {
        "status":"TBD",
        "hint":"Revenue recognition, biological assets (if any), depreciation, FX policy (functional/presentation), borrowing costs."
      },
      "3_property_plant_and_equipment": {
        "status":"TBD",
        "hint":"CAPEX roll-forward; useful lives; impairment policy."
      },
      "4_financial_instruments": {
        "status":"TBD",
        "hint":"SAFE/convertible classification rationale; interest rate; liquidity risk; credit risk."
      },
      "5_borrowings_and_covenants": {
        "status":"TBD",
        "hint":"Debt terms (tenor, rate, security); covenant tests (DSCR/LLCR/PLCR) and compliance narrative."
      },
      "6_related_parties_and_management": {"status":"TBD","hint":"If applicable."},
      "7_subsequent_events": {"status":"TBD","hint":"If applicable."}
    }
    (out/"m8b_ifrs_notes.json").write_text(json.dumps(notes, indent=2), encoding="utf-8")
    _ok("Emitted: m8b_ifrs_notes.json")

    # Sanity smoke
    smoke = []
    smoke.append(f"[SMOKE] Rows={len(all_long)} statements; statements={sorted(all_long['Statement'].unique().tolist())}")
    smoke.append("[SMOKE] Tie check (CF closing ↔ BS cash):")
    try:
        # CF link to BS
        cf_close = cf.set_index("Month_Index")["Closing_Cash_NAD_000"]
        bs_cash = bs.set_index("Month_Index")["Cash_and_Cash_Equivalents_NAD_000"]
        diff = (cf_close - bs_cash).abs().max()
        smoke.append(f"  max abs diff={float(diff):.3f}")
    except Exception as e:
        smoke.append(f"  skipped ({e})")
    (out/"m8b6_smoke.md").write_text("\n".join(smoke), encoding="utf-8")
    _ok("Smoke → m8b6_smoke.md")

    # Debug
    dbg = {"pl_cols": list(pl.columns)[:50], "bs_cols": list(bs.columns)[:50], "cf_cols": list(cf.columns)[:50], "rows": len(all_long)}
    (out/"m8b6_debug.json").write_text(json.dumps(dbg, indent=2), encoding="utf-8")
    _ok("Debug → m8b6_debug.json")

if __name__ == "__main__":
    run_m8B6(r".\outputs","NAD", strict=False, diagnostic=False)
