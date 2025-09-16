# tools/validate_m4_enrich_to_m7_5b.py
import sys, json, math, pathlib
import pandas as pd

OUTDIR = pathlib.Path(sys.argv[1]) if len(sys.argv) > 1 else pathlib.Path(r'.\outputs')
RTOL = 1e-9
ATOL = 1e-6  # values are in thousands; this tolerance is tiny

def fail(msg, extra=None):
    print(f"[FAIL] {msg}")
    if extra is not None:
        print(json.dumps(extra, indent=2, ensure_ascii=False))
    sys.exit(1)

def ok(msg):
    print(f"[OK] {msg}")

def rd(path):
    p = OUTDIR / path
    if not p.exists():
        fail(f"Missing artifact: {p}")
    return p

def read_parquet_any(*candidates):
    for name in candidates:
        p = OUTDIR / name
        if p.exists():
            return pd.read_parquet(p), p
    fail(f"None of the candidate files exist: {candidates}")

def require_cols(df, cols, where):
    missing = [c for c in cols if c not in df.columns]
    if missing:
        fail(f"[{where}] missing columns: {missing}")

def contiguity_check(df, col="Month_Index"):
    m = pd.Index(df[col].astype(int).tolist())
    expect = pd.Index(range(int(m.min()), int(m.max())+1))
    if not m.equals(expect):
        fail(f"Non-contiguous {col}.", {"found": m.tolist()[:10], "expected_head": expect.tolist()[:10]})

def series_close(a, b, label, rtol=RTOL, atol=ATOL):
    a = pd.to_numeric(a).fillna(0.0)
    b = pd.to_numeric(b).fillna(0.0)
    diff = (a - b).abs()
    tol  = atol + rtol * b.abs()
    bad = diff > tol
    if bad.any():
        i = bad.idxmax()
        raise AssertionError(f"{label} mismatch at idx {i}: got={a[i]} expect={b[i]} diff={diff[i]} tol={tol[i]}")

def main():
    summary = {"outdir": str(OUTDIR), "checks": []}

    # M4 schedule (required)
    m4, m4_path = read_parquet_any("m4_tax_schedule.parquet")
    require_cols(m4, [
        "Month_Index", "Taxable_Income_NAD_000", "Tax_Rate",
        "Tax_Expense_NAD_000", "Tax_Paid_NAD_000", "Tax_Payable_End_NAD_000"
    ], "M4")
    m4 = m4.sort_values("Month_Index").reset_index(drop=True)
    contiguity_check(m4, "Month_Index")
    summary["m4_rows"] = len(m4)

    # Payable roll-forward
    try:
        # Compute expected payable_end from t-1
        pe = pd.to_numeric(m4["Tax_Payable_End_NAD_000"]).fillna(0.0)
        te = pd.to_numeric(m4["Tax_Expense_NAD_000"]).fillna(0.0)
        tp = pd.to_numeric(m4["Tax_Paid_NAD_000"]).fillna(0.0)
        exp = pe.shift(1).fillna(pe.iloc[0]) + te - tp
        series_close(pe, exp, "M4 payable roll-forward")
        summary["m4_rollforward_ok"] = True
        ok("M4 tax payable roll-forward OK")
    except AssertionError as e:
        fail("M4 tax payable roll-forward failed", {"detail": str(e)})

    # M5 CFO (required)
    m5, m5_path = read_parquet_any("m5_cash_flow_statement_final.parquet")
    require_cols(m5, [
        "Month_Index",
        "Net_Profit_After_Tax_NAD_000",
        "Depreciation_NAD_000",
        "WC_Cash_Flow_NAD_000",
        "Interest_Paid_NAD_000",
        "Tax_Paid_NAD_000",
        "Cash_Flow_from_Operations_NAD_000"
    ], "M5")
    m5 = m5.sort_values("Month_Index").reset_index(drop=True)
    contiguity_check(m5, "Month_Index")
    summary["m5_rows"] = len(m5)

    # CFO identity check (per stabilized formula)
    npat = pd.to_numeric(m5["Net_Profit_After_Tax_NAD_000"])
    da   = pd.to_numeric(m5["Depreciation_NAD_000"])
    nwc  = pd.to_numeric(m5["WC_Cash_Flow_NAD_000"])
    intp = pd.to_numeric(m5["Interest_Paid_NAD_000"])
    taxp = pd.to_numeric(m5["Tax_Paid_NAD_000"])
    cfo_expect = npat + da + nwc - taxp - intp
    try:
        series_close(m5["Cash_Flow_from_Operations_NAD_000"], cfo_expect, "M5 CFO identity")
        summary["m5_cfo_identity_ok"] = True
        ok("M5 CFO identity OK")
    except AssertionError as e:
        fail("M5 CFO identity failed", {"detail": str(e)})

    # Optional: M3 monthly interest cross-check (if any schedule with interest exists)
    # Try common candidates and interest column variants.
    candidates = [
        ("m3_financing_schedule.parquet",     ["Interest_NAD_000","Interest_Expense_NAD_000"]),
        ("m3_revolver_schedule.parquet",      ["Interest_NAD_000","Interest_Expense_NAD_000"]),
        ("m3_insurance_schedule.parquet",     ["Interest_NAD_000","Interest_Expense_NAD_000"]),
    ]
    matched = False
    for fname, icolset in candidates:
        p = OUTDIR / fname
        if not p.exists():
            continue
        df = pd.read_parquet(p).sort_values("Month_Index")
        if "Month_Index" not in df.columns:
            continue
        for icol in icolset:
            if icol in df.columns:
                # align by Month_Index
                df2 = df[["Month_Index", icol]].merge(
                    m5[["Month_Index", "Interest_Paid_NAD_000"]],
                    on="Month_Index", how="inner"
                )
                try:
                    series_close(df2[icol], df2["Interest_Paid_NAD_000"], f"M3 vs M5 interest ({fname})")
                    summary["m3_interest_check"] = {"file": fname, "column": icol, "ok": True}
                    ok(f"M3 interest matches M5 ({fname}:{icol})")
                    matched = True
                    break
                except AssertionError as e:
                    fail("M3 vs M5 interest mismatch", {"file": fname, "column": icol, "detail": str(e)})
        if matched:
            break
    if not matched:
        summary["m3_interest_check"] = "skipped (no schedule with interest found)"
        print("[INFO] Skipped M3 interest cross-check (no usable interest column present).")

    # Done
    print(json.dumps(summary, indent=2, ensure_ascii=False))
    ok("Validator (M4-enrich → M5 → M7.5B context) PASS")

if __name__ == "__main__":
    main()
