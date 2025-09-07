import argparse
import json
import sys
from pathlib import Path
from typing import Dict, List, Tuple

try:
    import pandas as pd  # used only when we want to peek at a file's columns
except Exception:
    pd = None  # validator still works without pandas (it will skip column peeks)

OK = "[OK]"
WARN = "[WARN]"
FAIL = "[FAIL]"

# --------- Helper utilities ---------

def _glob_one(out_dir: Path, patterns: List[str]) -> List[Path]:
    hits = []
    for pat in patterns:
        hits.extend(out_dir.glob(pat))
        hits.extend(out_dir.glob(pat.upper()))
        hits.extend(out_dir.glob(pat.lower()))
    # de-dup while preserving order
    seen = set()
    uniq = []
    for p in hits:
        if p.name not in seen:
            uniq.append(p)
            seen.add(p.name)
    return uniq

def _nonempty_parquet(p: Path) -> bool:
    if not p.exists() or p.stat().st_size == 0:
        return False
    if pd is None:
        return True  # can't peek, assume file present = pass size check
    try:
        # read only first row to be safe/fast
        df = pd.read_parquet(p, columns=None)
        return len(df) > 0
    except Exception:
        return False

def _peek_columns(p: Path) -> List[str]:
    if pd is None:
        return []
    try:
        df = pd.read_parquet(p)
        return list(df.columns)
    except Exception:
        return []

def _print(msg: str):
    sys.stdout.write(msg + "\n")
    sys.stdout.flush()

# --------- M2 contract (frozen) ---------

# Role synonyms (normalized) for M2
M2_PL_ROLES = {
    "DA": ["Depreciation_and_Amortization", "DepreciationAmortization", "Depreciation", "DandA", "DA", "Depreciation_NAD_000"],
    "NPAT": ["Net_Profit_After_Tax", "NPAT", "NPAT_NAD_000"],
    "MONTH_INDEX": ["Month_Index", "MONTH_INDEX"],
}
M2_WC_ROLES = {
    "NWC_CF": ["Cash_Flow_from_NWC_Change", "Net_Working_Capital_CF", "Working_Capital_CF", "WC_Cash_Flow", "Cash_Flow_from_NWC_Change_NAD_000"],
    "MONTH_INDEX": ["Month_Index", "MONTH_INDEX"],
}

def _resolve_roles(cols: List[str], role_map: Dict[str, List[str]]) -> Tuple[bool, Dict[str, str], List[str]]:
    norm = {c: c for c in cols}
    missing = []
    chosen = {}
    lower = {c.lower(): c for c in cols}
    for role, syns in role_map.items():
        found = None
        for s in syns:
            # case-insensitive match but prefer exact if present
            if s in cols:
                found = s
                break
            if s.lower() in lower:
                found = lower[s.lower()]
                break
        if not found:
            missing.append(role)
        else:
            chosen[role] = found
    return (len(missing) == 0, chosen, missing)

def validate_m2(out_dir: Path, dbg: Dict) -> bool:
    ok = True
    _print("\n== M2 ==")

    pl_hits = _glob_one(out_dir, ["m2_*pl*schedule*.parquet"])
    wc_hits = _glob_one(out_dir, ["m2_*working*capital*schedule*.parquet"])
    dbg["M2"] = {"PL": [p.name for p in pl_hits], "WC": [p.name for p in wc_hits]}

    if not pl_hits:
        _print(f"{FAIL} M2/PL: not found")
        ok = False
    else:
        pl = pl_hits[0]
        if not _nonempty_parquet(pl):
            _print(f"{FAIL} M2/PL: empty or unreadable ({pl.name})")
            ok = False
        else:
            cols = _peek_columns(pl)
            good, chosen, missing = _resolve_roles(cols, M2_PL_ROLES)
            if good:
                _print(f"{OK} M2/PL: {pl.name}")
            else:
                _print(f"{FAIL} M2/PL: missing roles {missing} ({pl.name})")
                ok = False
            dbg["M2"]["PL_roles"] = {"chosen": chosen, "missing": missing, "columns": cols}

    if not wc_hits:
        _print(f"{FAIL} M2/WC: not found")
        ok = False
    else:
        wc = wc_hits[0]
        if not _nonempty_parquet(wc):
            _print(f"{FAIL} M2/WC: empty or unreadable ({wc.name})")
            ok = False
        else:
            cols = _peek_columns(wc)
            good, chosen, missing = _resolve_roles(cols, M2_WC_ROLES)
            if good:
                _print(f"{OK} M2/WC: {wc.name}")
            else:
                _print(f"{FAIL} M2/WC: missing roles {missing} ({wc.name})")
                ok = False
            dbg["M2"]["WC_roles"] = {"chosen": chosen, "missing": missing, "columns": cols}

    return ok

# --------- M3 contract (pattern-based acceptance) ---------

def validate_m3(out_dir: Path, dbg: Dict) -> bool:
    """
    Accept exactly one of:
      (A) unified financing schedule: m3_*financ*schedule*.parquet
      (B) both debt & equity schedules: m3_*debt*schedule*.parquet  AND  m3_*equity*schedule*.parquet
      (C) revolver + finance index: m3_*revolver*schedule*.parquet AND m3_*finance*index*.parquet
    We only verify presence and non-empty; column-level validation can come later per module contract.
    """
    _print("\n== M3 ==")
    ok = True

    unified = _glob_one(out_dir, ["m3_*financ*schedule*.parquet"])
    debt = _glob_one(out_dir, ["m3_*debt*schedule*.parquet"])
    equity = _glob_one(out_dir, ["m3_*equity*schedule*.parquet"])
    revolver = _glob_one(out_dir, ["m3_*revolver*schedule*.parquet"])
    f_index = _glob_one(out_dir, ["m3_*finance*index*.parquet", "m3_*financ*index*.parquet"])

    dbg["M3"] = {
        "unified": [p.name for p in unified],
        "debt": [p.name for p in debt],
        "equity": [p.name for p in equity],
        "revolver": [p.name for p in revolver],
        "finance_index": [p.name for p in f_index],
    }

    # Pattern A
    if unified:
        f = unified[0]
        if _nonempty_parquet(f):
            _print(f"{OK} M3: unified financing schedule ({f.name})")
            return True
        _print(f"{FAIL} M3: unified present but empty/unreadable ({f.name})")
        return False

    # Pattern B
    if debt and equity:
        f1, f2 = debt[0], equity[0]
        good = _nonempty_parquet(f1) and _nonempty_parquet(f2)
        if good:
            _print(f"{OK} M3: debt+equity schedules ({f1.name}, {f2.name})")
            return True
        _print(f"{FAIL} M3: debt/equity present but empty/unreadable")
        return False

    # Pattern C
    if revolver and f_index:
        f1, f2 = revolver[0], f_index[0]
        good = _nonempty_parquet(f1) and _nonempty_parquet(f2)
        if good:
            _print(f"{OK} M3: revolver+index ({f1.name}, {f2.name})")
            return True
        _print(f"{FAIL} M3: revolver/index present but empty/unreadable")
        return False

    _print(f"{FAIL} M3: Expected unified financing schedule or both debt & equity schedules or revolver+index")
    return False

# --------- M4 contract (minimal for M6 beta) ---------

def validate_m4(out_dir: Path, dbg: Dict) -> bool:
    """
    Minimal requirement for M6 beta:
      - presence of tax schedule file and it's non-empty.
      - If TAX_PAYABLE column absent, we WARN (we can work with TAX_EXPENSE and TAX_PAID).
    """
    _print("\n== M4 ==")
    hits = _glob_one(out_dir, ["m4_*tax*schedule*.parquet"])
    dbg["M4"] = {"tax": [p.name for p in hits]}
    if not hits:
        _print(f"{FAIL} M4: tax schedule not found")
        return False
    f = hits[0]
    if not _nonempty_parquet(f):
        _print(f"{FAIL} M4: tax schedule empty/unreadable ({f.name})")
        return False

    cols = _peek_columns(f)
    has_payable = any(c.lower() in ("tax_payable", "taxes_payable") for c in cols)
    if has_payable:
        _print(f"{OK} M4: tax schedule ({f.name})")
        return True
    _print(f"{WARN} M4: found tax schedule but TAX_PAYABLE role missing (using TAX_PAID/EXPENSE as minimal)")
    return True

# --------- M5 contract (minimal for M6 beta) ---------

def validate_m5(out_dir: Path, dbg: Dict) -> bool:
    """
    Minimal requirement for M6 beta:
      - presence of M5 final cash flow file and it's non-empty.
      - If NET_CF/CLOSING_CASH missing, WARN (M6 can still proceed with CFO).
    """
    _print("\n== M5 ==")
    hits = _glob_one(out_dir, ["m5_*cash*flow*final*.parquet", "m5_*cash_flow_statement*.parquet"])
    dbg["M5"] = {"cashflow": [p.name for p in hits]}
    if not hits:
        _print(f"{FAIL} M5: cash flow statement not found")
        return False
    f = hits[0]
    if not _nonempty_parquet(f):
        _print(f"{FAIL} M5: cash flow statement empty/unreadable ({f.name})")
        return False

    cols = _peek_columns(f)
    has_net_cf = any(c.lower() in ("net_cash_flow", "net_cf", "net_change_in_cash") for c in cols)
    has_close = any(c.lower() in ("closing_cash", "end_cash", "cash_end", "ending_cash_balance") for c in cols)
    if has_net_cf or has_close:
        _print(f"{OK} M5: {f.name}")
        return True
    _print(f"{WARN} M5: CFO present but no NET_CF / CLOSING_CASH (ok for M6 beta; will require NET_CF later)")
    return True

# --------- Entrypoint ---------

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("out_dir", help="Path to outputs directory (e.g., .\\outputs)")
    args = ap.parse_args()

    out_dir = Path(args.out_dir).resolve()
    debug = {"out_dir": str(out_dir)}

    all_ok = True
    all_ok &= validate_m2(out_dir, debug)
    all_ok &= validate_m3(out_dir, debug)
    all_ok &= validate_m4(out_dir, debug)
    all_ok &= validate_m5(out_dir, debug)

    # Write debug JSON
    dbg_path = out_dir / "contracts_validate_debug.json"
    try:
        dbg_path.write_text(json.dumps(debug, indent=2))
        _print(f"\n📝 Wrote: {dbg_path}")
    except Exception:
        _print("\n[WARN] Could not write contracts_validate_debug.json")

    sys.exit(0 if all_ok else 1)

if __name__ == "__main__":
    main()
