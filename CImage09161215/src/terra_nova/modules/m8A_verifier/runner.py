# src/terra_nova/modules/m8A_verifier/runner.py
from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Optional, Tuple

import pandas as pd


# ---------- chatty console helpers ----------
def _info(msg: str) -> None: print(f"[M8.A][INFO] {msg}")
def _ok(msg: str)   -> None: print(f"[M8.A][OK]  {msg}")
def _warn(msg: str) -> None: print(f"[M8.A][WARN] {msg}")
def _fail(msg: str) -> None: raise RuntimeError(f"[M8.A][FAIL] {msg}")


# ---------- synonyms (case-insensitive matching) ----------
MONTH_SYNS: tuple[str, ...] = ("Month_Index", "MONTH_INDEX", "month_index", "Month")
CFO_SYNS: tuple[str, ...] = (
    "CFO_NAD_000",
    "Cash_Flow_from_Operations_NAD_000",
    "Cash_Flow_From_Operations_NAD_000",
    "CashFlow_from_Operations_NAD_000",
    "CFO", "CFO_NAD", "CFO_NAD000", "Cash_Flow_Operations_NAD_000",
)
CF_CLOSING_CASH_SYNS: tuple[str, ...] = (
    "Closing_Cash_NAD_000", "Cash_EOP_NAD_000", "Cash_End_NAD_000"
)
BS_CASH_SYNS: tuple[str, ...] = (
    "Cash_and_Cash_Equivalents_NAD_000",
    "Cash_And_Cash_Equivalents_NAD_000",
    "Cash_EOP_NAD_000",
)
BS_ASSETS_TOTAL_SYNS: tuple[str, ...] = ("Assets_Total_NAD_000",)
BS_L_E_TOTAL_SYNS: tuple[str, ...] = ("Liabilities_And_Equity_Total_NAD_000",)

# ---------- io helpers ----------
def _read_parquet(p: Path, what: str) -> pd.DataFrame:
    if not p.exists():
        _fail(f"Missing {what}: {p}")
    df = pd.read_parquet(p)
    if df.empty:
        _fail(f"Empty {what}: {p}")
    return df

def _syn(df: pd.DataFrame, candidates: Iterable[str], role: str, *, strict: bool = True) -> str:
    cols = {c.lower(): c for c in df.columns}
    tried = list(candidates)
    # 1) exact case-insensitive
    for c in tried:
        if c.lower() in cols:
            return cols[c.lower()]
    # 2) heuristic for CFO: any col containing 'cfo' and ending with NAD_000
    if role.upper().startswith("CFO"):
        for c in df.columns:
            cl = c.lower()
            if "cfo" in cl and (cl.endswith("_nad_000") or cl == "cfo"):
                return c
    # 3) heuristic for Month_Index
    if role == "Month_Index":
        for c in df.columns:
            if c.lower() in ("month", "monthindex", "idx", "month_id"):
                return c
    # 4) last resort: raise or warn
    preview = list(df.columns)[:12]
    if strict:
        _fail(f"Cannot resolve role '{role}'. Tried {tried}. Available: {preview}")
    else:
        _warn(f"Could not resolve '{role}'. Tried {tried}. Available: {preview}")
    # return first candidate (won't be used if strict=False and caller guards)
    return tried[0]

def _left_join(a: pd.DataFrame, b: pd.DataFrame, key: str) -> pd.DataFrame:
    return a.merge(b, on=key, how="left", validate="m:1")

@dataclass
class Inputs:
    out_dir: Path
    currency: str
    strict: bool
    diagnostic: bool
    input_pack_path: Optional[Path]


def run_m8A(out_dir: str,
            currency: str,
            *,
            input_pack_path: str | None = None,
            strict: bool = True,
            diagnostic: bool = False) -> None:
    """
    M8.A — Super‑verification of the composed pipeline up to and including M7.5B.
    - Validates BS identity and CF→BS cash link
    - Flags negative closing cash (subordination/waterfall guardrail)
    - Compares M5 CFO vs. M7.5B CFO (role‑resolved by synonyms)
    - Checks crop area (65 ha) from InputPack (Revenue_Assumptions)

    Emits:
      outputs/m8a_verification_report.md
      outputs/m8a_debug.json
    """
    args = Inputs(
        out_dir=Path(out_dir),
        currency=currency,
        strict=strict,
        diagnostic=diagnostic,
        input_pack_path=Path(input_pack_path) if input_pack_path else None,
    )

    _info(f"Starting M8.A super‑verification in: {args.out_dir.resolve()}")

    # ---------- load core M7.5B artifacts ----------
    pl_p = args.out_dir / "m7_5b_profit_and_loss.parquet"
    cf_p = args.out_dir / "m7_5b_cash_flow.parquet"
    bs_p = args.out_dir / "m7_5b_balance_sheet.parquet"
    dbg_p = args.out_dir / "m7_5b_debug.json"

    pl = _read_parquet(pl_p, "M7.5B P&L")
    cf = _read_parquet(cf_p, "M7.5B Cash Flow")
    bs = _read_parquet(bs_p, "M7.5B Balance Sheet")
    _ok("Core M7.5B artifacts present.")

    # ---------- resolve keys & totals ----------
    mcol_cf = _syn(cf, MONTH_SYNS, "Month_Index", strict=args.strict)
    mcol_bs = _syn(bs, MONTH_SYNS, "Month_Index", strict=args.strict)
    assets_col = _syn(bs, BS_ASSETS_TOTAL_SYNS, "Assets_Total", strict=args.strict)
    le_col     = _syn(bs, BS_L_E_TOTAL_SYNS, "Liabilities_And_Equity_Total", strict=args.strict)

    # BS tie check
    max_abs_diff = float((bs[assets_col] - bs[le_col]).abs().max())
    if max_abs_diff <= 1.0:
        _ok(f"BS totals tie (max abs diff {max_abs_diff:.3f} ≤ 1.0).")
    else:
        _fail(f"BS totals do not tie (max abs diff {max_abs_diff:.3f} > 1.0).")

    # ---------- closing cash vs BS cash link ----------
    cf_close_c = _syn(cf, CF_CLOSING_CASH_SYNS, "Closing_Cash_NAD_000", strict=False)
    bs_cash_c  = _syn(bs, BS_CASH_SYNS, "Cash_and_Cash_Equivalents_NAD_000", strict=False)
    if cf_close_c in cf.columns and bs_cash_c in bs.columns:
        j = _left_join(cf[[mcol_cf, cf_close_c]], bs[[mcol_bs, bs_cash_c]].rename(columns={mcol_bs: mcol_cf}), mcol_cf)
        max_cash_diff = float((j[cf_close_c] - j[bs_cash_c]).abs().max())
        if max_cash_diff < 1e-6:
            _ok(f"Cash link OK: CF {cf_close_c} equals BS {bs_cash_c} (max diff {max_cash_diff:.3f}).")
        else:
            _warn(f"Cash link mismatch (max diff {max_cash_diff:.3f}).")
    else:
        _warn("Cash link skipped (closing cash and/or BS cash not found).")

    # ---------- negative cash flag (waterfall/subordination sanity) ----------
    if cf_close_c in cf.columns:
        min_cash = float(cf[cf_close_c].min())
        if min_cash < 0.0:
            _warn(f"Negative closing cash detected (min {min_cash:.2f}). Junior must be subordinated to senior. See waterfall note.")
    else:
        _warn("Cannot scan for negative closing cash (closing cash not present).")

    # ---------- CFO: M5 vs M7.5B (robust synonyms) ----------
    m5_p = args.out_dir / "m5_cash_flow_statement_final.parquet"
    m5 = _read_parquet(m5_p, "M5 CFO source")
    mcol_m5 = _syn(m5, MONTH_SYNS, "Month_Index", strict=args.strict)

    cfo_c_m8 = _syn(cf, CFO_SYNS, "CFO_NAD_000", strict=args.strict)   # from M7.5B cash flow
    cfo_c_m5 = _syn(m5, CFO_SYNS, "CFO_NAD_000", strict=args.strict)   # from M5

    if args.diagnostic:
        _info(f"CFO column (M7.5B CF) -> '{cfo_c_m8}' ; (M5) -> '{cfo_c_m5}'")

    j = _left_join(cf[[mcol_cf, cfo_c_m8]].rename(columns={mcol_cf: "Month_Index"}),
                   m5[[mcol_m5, cfo_c_m5]].rename(columns={mcol_m5: "Month_Index", cfo_c_m5: "M5_CFO"}),
                   "Month_Index").rename(columns={cfo_c_m8: "M75B_CFO"})

    cfo_diff = float((j["M75B_CFO"] - j["M5_CFO"]).abs().max())
    _ok(f"CFO comparison: |M5 − M7.5B| max diff = {cfo_diff:.6f}")

    # ---------- crop area (65ha) check from InputPack ----------
    total_ha: Optional[float] = None
    if args.input_pack_path and Path(args.input_pack_path).exists():
        try:
            # Light touch: only read Revenue_Assumptions sheet (no schema churn).
            import openpyxl  # noqa: F401 (ensure available)
            df_rev = pd.read_excel(args.input_pack_path, sheet_name="Revenue_Assumptions")
            # look for an area column
            area_col = None
            for cand in ("Area_ha", "Area", "Hectares", "Area (ha)"):
                if cand in df_rev.columns:
                    area_col = cand
                    break
            if area_col:
                total_ha = float(df_rev[area_col].replace({None: 0}).fillna(0).sum())
                if abs(total_ha - 65.0) < 1e-6:
                    _ok("Total crop area = 65 ha (as expected).")
                else:
                    _warn(f"Total crop area = {total_ha:g} ha (expected 65 ha). Check InputPack Revenue_Assumptions.")
            else:
                _warn("Could not find crop area column in Revenue_Assumptions.")
        except Exception as e:
            _warn(f"Could not read InputPack Revenue_Assumptions for area check: {e}")
    else:
        _warn("InputPack path not provided or not found; skipping crop area check.")

    # ---------- FX debug presence ----------
    fx_meta_ok = False
    if dbg_p.exists():
        try:
            dbg = json.loads(dbg_p.read_text(encoding="utf-8"))
            fx_keys = [k for k in dbg if k.startswith("fx")]
            if fx_keys:
                fx_meta_ok = True
                _ok("FX debug metadata found in M7.5B debug file.")
            else:
                _warn("No FX* keys in M7.5B debug file.")
        except Exception as e:
            _warn(f"Could not parse M7.5B debug json: {e}")
    else:
        _warn("M7.5B debug json not found.")

    # ---------- emit verification report & debug ----------
    rep_p = args.out_dir / "m8a_verification_report.md"
    dbg8_p = args.out_dir / "m8a_debug.json"

    lines = [
        "# M8.A — Super‑verification report",
        "",
        f"- BS tie max abs diff: {max_abs_diff:.3f}",
        f"- Cash link columns: CF '{cf_close_c}' ↔ BS '{bs_cash_c}'",
        f"- CFO columns: M7.5B '{cfo_c_m8}' vs M5 '{cfo_c_m5}' ; max diff {cfo_diff:.6f}",
        f"- Crop area (ha): {total_ha if total_ha is not None else 'n/a'} (target 65)",
        f"- FX meta present: {fx_meta_ok}",
    ]
    rep_p.write_text("\n".join(lines), encoding="utf-8")

    dbg_out = {
        "bs_tie_max_abs_diff": max_abs_diff,
        "cash_link_cf_col": cf_close_c,
        "cash_link_bs_col": bs_cash_c,
        "cfo_m75b_col": cfo_c_m8,
        "cfo_m5_col": cfo_c_m5,
        "cfo_diff_max_abs": cfo_diff,
        "crop_area_total_ha": total_ha,
        "fx_debug_present": fx_meta_ok,
        "diagnostic": args.diagnostic,
        "strict": args.strict,
    }
    dbg8_p.write_text(json.dumps(dbg_out, indent=2), encoding="utf-8")

    _ok(f"Report → {rep_p.name}")
    _ok(f"Debug  → {dbg8_p.name}")
