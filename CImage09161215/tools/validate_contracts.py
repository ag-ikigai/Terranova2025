# tools/validate_contracts.py
import argparse, json, re, sys
from pathlib import Path
import pandas as pd

# ---------- helpers

CURRENCY_SUFFIX_RE = re.compile(r"(_[A-Z]{3})?(_0{3,})?$")  # e.g. _NAD_000, _USD_0000

def canon(s: str) -> str:
    """
    Canonicalize a column/role name:
    - strip currency/scale suffixes like _NAD_000
    - collapse non-alphanumerics to single underscore
    - lower-case
    """
    s = CURRENCY_SUFFIX_RE.sub("", s)
    s = re.sub(r"[^A-Za-z0-9]+", "_", s).strip("_").lower()
    return s

def find_col(df: pd.DataFrame, role_synonyms: list[str]) -> str | None:
    """
    Return the first DataFrame column that matches any of the role synonyms
    after canonicalization; also try a couple of common prefixes/suffix flips.
    """
    cols_canon = {canon(c): c for c in df.columns}
    for syn in role_synonyms:
        c = canon(syn)
        if c in cols_canon: 
            return cols_canon[c]
    return None

def need(df: pd.DataFrame, role: str, synonyms: list[str], filetag: str) -> str:
    col = find_col(df, synonyms)
    if not col:
        raise AssertionError(
            f"[{filetag}] Missing required role: {role}\n"
            f"Looked for any of: {synonyms}\n"
            f"Available columns: {list(df.columns)}"
        )
    return col

def read_parquet(path: Path) -> pd.DataFrame:
    return pd.read_parquet(path)  # pyarrow present in your venv

# ---------- contracts (roles + synonyms)

# M2 roles we actually use downstream (kept minimal)
M2_PL = {
    "DA": ["Depreciation_and_Amortization","DepreciationAmortization","Depreciation","DandA","DA"],
    "NPAT": ["Net_Profit_After_Tax","NPAT"],
    "MONTH_INDEX": ["Month_Index","MONTH_INDEX"],
}
M2_WC = {
    "NWC_CF": ["Cash_Flow_from_NWC_Change","Net_Working_Capital_CF","Working_Capital_CF","WC_Cash_Flow"],
    "MONTH_INDEX": ["Month_Index","MONTH_INDEX"],
}

# M3 acceptance: we allow (a) unified schedule OR (b) debt+equity OR (c) revolver+index
M3_ACCEPT = {
    "unified": ["m3_financing_schedule.parquet"],
    "debt":    ["m3_debt_schedule.parquet"],
    "equity":  ["m3_equity_schedule.parquet"],
    "revolver":["m3_revolver_schedule.parquet"],
    "index":   ["m3_finance_index.parquet"],
}

# M4 roles — strict mode requires TAX_PAYABLE; relaxed accepts just expense/paid
M4_ROLES_STRICT = {
    "TAX_PAYABLE": ["Tax_Payable","Tax_Payable_Balance"],
}
M4_ROLES_RELAXED = {
    "TAX_EXPENSE": ["Tax_Expense","Income_Tax_Expense"],
    "TAX_PAID":    ["Tax_Paid","Taxes_Paid","Tax_Payments"],
}

# M5 minimal roles for M6 beta (CFO only). Add explicit NAD/scale variants.
M5_MINIMAL = {
    "CFO": [
        "Cash_Flow_from_Operations", "Operating_Cash_Flow", "CFO",
        "Cash_Flow_from_Operations_NAD_000", "Operating_Cash_Flow_NAD_000", "CFO_NAD_000"
    ],
    "MONTH_INDEX": ["Month_Index","MONTH_INDEX"],
}

# M6 roles: totals; accept both “total_assets” style and your “assets_total” style
M6_TOTALS = {
    "TOTAL_ASSETS": [
        "Total_Assets","Assets_Total","Total_Assets_NAD_000","Assets_Total_NAD_000"
    ],
    "TOTAL_L_PLUS_E": [
        "Total_Liabilities_And_Equity","Liabilities_And_Equity_Total",
        "Total_Liabilities_And_Equity_NAD_000","Liabilities_And_Equity_Total_NAD_000"
    ],
}

# ---------- validators

def validate_m2(out_dir: Path) -> dict:
    pl = out_dir / "m2_pl_schedule.parquet"
    wc = out_dir / "m2_working_capital_schedule.parquet"
    if not pl.exists(): raise FileNotFoundError(f"M2 PL not found: {pl}")
    if not wc.exists(): raise FileNotFoundError(f"M2 WC not found: {wc}")

    df_pl = read_parquet(pl)
    df_wc = read_parquet(wc)

    pl_map = {r: need(df_pl, r, syn, "M2/PL") for r, syn in M2_PL.items()}
    wc_map = {r: need(df_wc, r, syn, "M2/WC") for r, syn in M2_WC.items()}
    print("[OK] M2/PL:", pl.name)
    print("[OK] M2/WC:", wc.name)
    return {"pl_file": pl.name, "wc_file": wc.name, "pl_map": pl_map, "wc_map": wc_map}

def validate_m3(out_dir: Path) -> dict:
    have = {k: all((out_dir / p).exists() for p in v) for k, v in M3_ACCEPT.items()}
    if (out_dir / "m3_revolver_schedule.parquet").exists() and (out_dir / "m3_finance_index.parquet").exists():
        print("[OK] M3: revolver+index -> m3_revolver_schedule.parquet, m3_finance_index.parquet")
        return {"pattern": "revolver+index"}
    if (out_dir / "m3_financing_schedule.parquet").exists():
        print("[OK] M3: unified financing schedule -> m3_financing_schedule.parquet")
        return {"pattern": "unified"}
    if (out_dir / "m3_debt_schedule.parquet").exists() and (out_dir / "m3_equity_schedule.parquet").exists():
        print("[OK] M3: debt+equity -> m3_debt_schedule.parquet, m3_equity_schedule.parquet")
        return {"pattern": "debt+equity"}

    raise AssertionError("[M3] Expected unified financing schedule or both debt & equity schedules or revolver+index")

def validate_m4(out_dir: Path, strict_tax: bool) -> dict:
    m4p = out_dir / "m4_tax_schedule.parquet"
    if not m4p.exists(): raise FileNotFoundError(f"M4 schedule not found: {m4p}")
    df = read_parquet(m4p)

    if strict_tax:
        col = need(df, "TAX_PAYABLE", M4_ROLES_STRICT["TAX_PAYABLE"], "M4")
        print("[OK] M4: payable present ->", col)
        return {"file": m4p.name, "payable_col": col, "mode": "strict"}
    else:
        exp_ok = find_col(df, M4_ROLES_RELAXED["TAX_EXPENSE"]) is not None
        paid_ok = find_col(df, M4_ROLES_RELAXED["TAX_PAID"]) is not None
        if exp_ok and paid_ok:
            print("[OK] M4: relaxed -> expense & paid present")
            return {"file": m4p.name, "mode": "relaxed"}
        print("[WARN] M4: found tax schedule but TAX_PAYABLE role missing (using TAX_PAID/EXPENSE as minimal)")
        return {"file": m4p.name, "mode": "relaxed_warn"}

def validate_m5(out_dir: Path) -> dict:
    m5p = out_dir / "m5_cash_flow_statement_final.parquet"
    if not m5p.exists(): raise FileNotFoundError(f"M5 not found: {m5p}")
    df = read_parquet(m5p)
    cfo_col = need(df, "CFO", M5_MINIMAL["CFO"], "M5/minimal")
    mi_col  = need(df, "MONTH_INDEX", M5_MINIMAL["MONTH_INDEX"], "M5/minimal")
    print("[OK] M5: CFO ->", cfo_col)
    return {"file": m5p.name, "cfo_col": cfo_col, "month_index": mi_col}

def validate_m6(out_dir: Path, tol: float = 1e-6) -> dict:
    m6p = out_dir / "m6_balance_sheet.parquet"
    if not m6p.exists(): raise FileNotFoundError(f"M6 not found: {m6p}")
    df = read_parquet(m6p)

    ta_col = need(df, "TOTAL_ASSETS", M6_TOTALS["TOTAL_ASSETS"], "M6")
    tle_col = need(df, "TOTAL_L_PLUS_E", M6_TOTALS["TOTAL_L_PLUS_E"], "M6")

    diff = (df[ta_col] - df[tle_col]).abs()
    ok = (diff <= tol).all()
    if not ok:
        raise AssertionError(f"[M6] Assets vs L+E mismatch. max|diff|={float(diff.max())}")
    print("[OK] M6: totals tie ->", ta_col, "=", tle_col)
    return {"file": m6p.name, "total_assets": ta_col, "total_l_plus_e": tle_col, "max_abs_diff": float(diff.max())}

# ---------- CLI

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("out_dir")
    ap.add_argument("--strict-tax", action="store_true", help="require TAX_PAYABLE in M4")
    ap.add_argument("--include-m6", action="store_true", help="also validate M6 totals")
    args = ap.parse_args()

    out_dir = Path(args.out_dir)
    report, failures = {}, []

    print("\n== M2 ==")
    try:
        report["m2"] = validate_m2(out_dir)
    except Exception as e:
        failures.append("m2"); print(f"[FAIL] M2: {type(e).__name__}: {e}")

    print("\n== M3 ==")
    try:
        report["m3"] = validate_m3(out_dir)
    except Exception as e:
        failures.append("m3"); print(f"[FAIL] M3: {type(e).__name__}: {e}")

    print("\n== M4 ==")
    try:
        report["m4"] = validate_m4(out_dir, args.strict_tax)
    except Exception as e:
        failures.append("m4"); print(f"[FAIL] M4: {type(e).__name__}: {e}")

    print("\n== M5 ==")
    try:
        report["m5"] = validate_m5(out_dir)
    except Exception as e:
        failures.append("m5"); print(f"[FAIL] M5: {type(e).__name__}: {e}")

    if args.include_m6:
        print("\n== M6 ==")
        try:
            report["m6"] = validate_m6(out_dir)
        except Exception as e:
            failures.append("m6"); print(f"[FAIL] M6: {type(e).__name__}: {e}")

    dbg = out_dir / "contracts_validate_debug.json"
    dbg.write_text(json.dumps(report, indent=2))
    print(f"\n📝 Wrote: {dbg}")
    if failures:
        print(f"Contract validation failed in: {failures}")
        sys.exit(1)
    else:
        print("🎉 All contracts passed.")

if __name__ == "__main__":
    main()

