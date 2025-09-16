#!/usr/bin/env python
import argparse, json, sys, os
from pathlib import Path

import pandas as pd

REQ = {
    "b1": ["m8b_base_timeseries.parquet"],
    "b2": ["m8b2_promoter_scorecard_monthly.parquet", "m8b2_promoter_scorecard_yearly.parquet"],
    "b3": ["m8b_investor_metrics_selected.parquet", "m8b_terms.json"],
    "b4": ["m8b4_lender_metrics_monthly.parquet", "m8b4_lender_metrics_yearly.parquet"],
    "b5": ["m8b_benchmarks.values.parquet", "m8b_benchmarks.catalog.json"],
    "b6": ["m8b_ifrs_statements.parquet", "m8b_ifrs_mapping.json", "m8b_ifrs_notes.json"],
}

def must_exist(p: Path):
    if not p.exists():
        raise FileNotFoundError(str(p))

def read_parquet(p: Path) -> pd.DataFrame:
    return pd.read_parquet(p)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("outputs")
    ap.add_argument("--strict", action="store_true")
    args = ap.parse_args()

    outdir = Path(args.outputs)
    dbg = {"outdir": str(outdir), "checks": []}
    ok_all = True

    # B1
    try:
        p = outdir / "m8b_base_timeseries.parquet"
        must_exist(p)
        df = read_parquet(p)
        required_cols = {"Month_Index", "Calendar_Year", "Calendar_Quarter"}
        missing = [c for c in required_cols if c not in df.columns]
        if missing:
            raise AssertionError(f"B1 missing columns: {missing}")
        if args.strict and len(df) == 0:
            raise AssertionError("B1 has zero rows")
        dbg["checks"].append({"b1_rows": len(df)})
    except Exception as e:
        ok_all = False
        dbg["b1_error"] = str(e)

    # B2
    try:
        p_m = outdir / "m8b2_promoter_scorecard_monthly.parquet"
        p_y = outdir / "m8b2_promoter_scorecard_yearly.parquet"
        must_exist(p_m); must_exist(p_y)
        dm = read_parquet(p_m); dy = read_parquet(p_y)
        req_cols = {"Metric_ID", "Value"}
        if any(c not in dm.columns for c in req_cols):
            raise AssertionError("B2 monthly missing one of required columns {Metric_ID, Value}")
        if args.strict and (len(dm) == 0 or len(dy) == 0):
            raise AssertionError("B2 monthly/yearly are empty")
        dbg["checks"].append({"b2_monthly_rows": len(dm), "b2_yearly_rows": len(dy)})
    except Exception as e:
        ok_all = False
        dbg["b2_error"] = str(e)

    # B3
    try:
        p = outdir / "m8b_investor_metrics_selected.parquet"
        t = outdir / "m8b_terms.json"
        must_exist(p); must_exist(t)
        df = read_parquet(p)
        if args.strict and len(df) == 0:
            raise AssertionError("B3 investor metrics empty")
        dbg["checks"].append({"b3_rows": len(df)})
    except Exception as e:
        ok_all = False
        dbg["b3_error"] = str(e)

    # B4
    try:
        pm = outdir / "m8b4_lender_metrics_monthly.parquet"
        py = outdir / "m8b4_lender_metrics_yearly.parquet"
        must_exist(pm); must_exist(py)
        dm = read_parquet(pm); dy = read_parquet(py)
        req_any = {"DSCR", "ICR", "LLCR", "PLCR"}
        if args.strict and not any(c in dm.columns for c in req_any):
            raise AssertionError("B4 monthly has none of the expected coverage columns (DSCR/ICR/LLCR/PLCR)")
        dbg["checks"].append({"b4_monthly_rows": len(dm), "b4_yearly_rows": len(dy)})
    except Exception as e:
        ok_all = False
        dbg["b4_error"] = str(e)

    # B5
    try:
        pv = outdir / "m8b_benchmarks.values.parquet"
        pc = outdir / "m8b_benchmarks.catalog.json"
        must_exist(pv); must_exist(pc)
        dv = read_parquet(pv)
        cat = json.loads(pc.read_text())
        if args.strict and len(dv) == 0:
            raise AssertionError("B5 values parquet empty")
        if args.strict and not isinstance(cat, dict):
            raise AssertionError("B5 catalog JSON is not an object")
        dbg["checks"].append({"b5_values_rows": len(dv), "b5_catalog_keys": len(cat)})
    except Exception as e:
        ok_all = False
        dbg["b5_error"] = str(e)

    # B6
    try:
        ps = outdir / "m8b_ifrs_statements.parquet"
        pm = outdir / "m8b_ifrs_mapping.json"
        pn = outdir / "m8b_ifrs_notes.json"
        must_exist(ps); must_exist(pm); must_exist(pn)
        ds = read_parquet(ps)
        req_cols = {"Statement", "Month_Index", "Line_Item"}
        missing = [c for c in req_cols if c not in ds.columns]
        if missing:
            raise AssertionError(f"B6 missing columns: {missing}")
        if args.strict:
            required_stmts = {"PL", "BS", "CF"}
            if not required_stmts.issubset(set(ds["Statement"].unique())):
                raise AssertionError("B6 missing one of statements {PL, BS, CF}")
        dbg["checks"].append({"b6_rows": len(ds)})
    except Exception as e:
        ok_all = False
        dbg["b6_error"] = str(e)

    dbg_path = Path(args.outputs) / "m8bn_validate_debug.json"
    dbg_path.write_text(json.dumps(dbg, indent=2))
    if ok_all:
        print("[OK] M8.Bn validator PASS")
        sys.exit(0)
    else:
        print("[FAIL] M8.Bn validator FAIL. See", dbg_path)
        sys.exit(1)

if __name__ == "__main__":
    main()
