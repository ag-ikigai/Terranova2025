# -*- coding: utf-8 -*-
"""
M8.B3 – Investor engine (drop-in runner)

Reads:
  - outputs/m7_selected_offer.json              -> selected Option/Instrument + terms (cap/discount/ticket if present)
  - outputs/m7_r1_scores.parquet|csv           -> fallback for Ticket_USD per Option
  - outputs/m7_5_junior_financing.parquet|csv  -> fallback for ticket (sum of junior equity cash-ins) and injection month
  - outputs/m8b_gate_valuations.json           -> gate EVs at M24/M36/M42/M48 (NAD '000)
  - outputs/m8b_fx_curve.parquet               -> NAD_per_USD (preferred), else outputs/m0_inputs/FX_Path.parquet

Emits:
  - outputs/m8b_investor_metrics_selected.parquet  (one row per gate)
  - outputs/m8b3_debug.json
  - outputs/m8b3_smoke.md

Notes:
  * Treat FX as NAD per USD; USD = NAD / FX.
  * Ownership for SAFE/Convertible is approximated using the most conservative valuation basis:
      denom = min( post_money_cap , discounted_gate_price , gate_36_EV ) if available,
              else first present among these.
    For post-money vs pre-money detection, we look for key hints and document the choice in debug.

This runner is intentionally verbose and robust to schema variances.
"""
from __future__ import annotations

import json
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Any

import numpy as np
import pandas as pd

# --------------------------
# Config & synonyms
# --------------------------

FX_COL_SYNS = [
    "NAD_per_USD", "USD_to_NAD", "USD_NAD", "FX_USD_NAD", "USDtoNAD", "Rate_USD_to_NAD"
]

TICKET_USD_KEYS = [
    "Ticket_USD", "TicketUSD", "Ticket_USD_Amount", "Ticket_USD_value", "Ticket_Usd"
]
TICKET_NAD_000_KEYS = [
    "Ticket_NAD_000", "Ticket_NAD000", "Ticket_NAD_k", "Ticket_NAD_thousands"
]
CAP_KEYS_NAD_000 = [
    "Cap_NAD_000", "Post_Money_Cap_NAD_000", "Pre_Money_Cap_NAD_000"
]
CAP_KEYS_USD = [
    "Cap_USD", "Post_Money_Cap_USD", "Pre_Money_Cap_USD"
]
DISCOUNT_KEYS = [
    "Discount", "Discount_Pct", "Discount_%", "DiscountPercent"
]

JUNIOR_INFLOW_SYNS = [
    "Junior_Equity_In_NAD_000", "Junior_In_NAD_000", "Junior_Invest_NAD_000",
    "Junior_Cash_In_NAD_000", "CFF_JUNIOR_NAD_000"  # last is opportunistic if present in CF
]

GATE_KEYS = {
    24: ["M24", "Month_24", "Gate_24", "24"],
    36: ["M36", "Month_36", "Gate_36", "36"],
    42: ["M42", "Month_42", "Gate_42", "42"],
    48: ["M48", "Month_48", "Gate_48", "48"],
}

DEFAULT_M36_CAP_NAD_000 = 40_000.0  # NAD '000


@dataclass
class DebugBag:
    option: Optional[str] = None
    instrument: Optional[str] = None
    offer_source_keys: List[str] = None
    fx_used: Optional[float] = None  # NAD per USD
    fx_source: Optional[str] = None
    gate_vals_nad_000: Dict[str, float] = None
    ticket_nad_000: Optional[float] = None
    ticket_usd: Optional[float] = None
    ticket_month_index: Optional[int] = None
    ticket_source: Optional[str] = None
    cap_nad_000: Optional[float] = None
    cap_source: Optional[str] = None
    discount_fraction: Optional[float] = None
    ownership_fraction: Optional[float] = None
    ownership_basis: Optional[str] = None
    warnings: List[str] = None
    notes: List[str] = None


# --------------------------
# Small utils
# --------------------------

def _read_json(p: Path) -> Dict[str, Any]:
    if not p.exists():
        return {}
    with p.open("r", encoding="utf-8") as f:
        return json.load(f)

def _read_any_table(p: Path) -> Optional[pd.DataFrame]:
    if not p.exists():
        return None
    if p.suffix.lower() == ".parquet":
        return pd.read_parquet(p)
    if p.suffix.lower() == ".csv":
        return pd.read_csv(p)
    return None

def _flatten(d: Any) -> Dict[str, Any]:
    """
    Flatten a potentially nested dict of the selected offer so we can search keys robustly.
    """
    flat = {}
    def rec(prefix, obj):
        if isinstance(obj, dict):
            for k, v in obj.items():
                rec(f"{prefix}{k.lower()}.", v)
        else:
            flat[prefix[:-1]] = obj
    if isinstance(d, dict):
        for k, v in d.items():
            if isinstance(v, (dict, list)):
                # store also top-level
                pass
        rec("", d)
    return flat

def _pick_first(d: Dict[str, Any], keys: List[str]) -> Optional[Any]:
    for k in keys:
        for key in (k, k.lower(), k.upper()):
            if key in d:
                return d[key]
        # also search in flattened form
    return None

def _extract_selected_offer(raw: Dict[str, Any]) -> Tuple[Optional[str], Optional[str], Dict[str, Any], List[str]]:
    """
    Try a few shapes:
      { "Option": "A_SAFE", "Instrument": "SAFE", ... }
      { "selected": { "Option": "...", "Instrument": "...", "terms": {...} } }
      { "selection": {...} }
    """
    seen_keys = list(raw.keys())
    # direct
    option = None
    instrument = None
    offer_dict = raw.copy()

    # common nests
    for nest in ["selected", "selection", "offer", "chosen"]:
        if nest in raw and isinstance(raw[nest], dict):
            offer_dict = {**raw, **raw[nest]}
            seen_keys += list(raw[nest].keys())

    # try to pick
    for k in ["Option", "option", "OPTION"]:
        if k in offer_dict:
            option = str(offer_dict[k])
            break
    for k in ["Instrument", "instrument", "INSTRUMENT"]:
        if k in offer_dict:
            instrument = str(offer_dict[k])
            break
    return option, instrument, offer_dict, sorted(set(seen_keys))

def _resolve_fx(outputs: Path, dbg: DebugBag) -> float:
    """
    Return NAD per USD (float). Prefer m8b_fx_curve.parquet.
    Fallback to outputs/m0_inputs/FX_Path.parquet or outputs/FX_Path.parquet.
    If a time-series, take Month_Index==1 or the first row.
    """
    # preferred
    fx_p = outputs / "m8b_fx_curve.parquet"
    if fx_p.exists():
        d = pd.read_parquet(fx_p)
        col = None
        for c in FX_COL_SYNS:
            if c in d.columns:
                col = c
                break
        if col is None:
            # pick first numeric except Month_Index
            num = [c for c in d.columns if c != "Month_Index" and pd.api.types.is_numeric_dtype(d[c])]
            if num:
                col = num[0]
        if col is not None:
            if "Month_Index" in d.columns:
                row = d.sort_values("Month_Index").iloc[0]
                fx = float(row[col])
            else:
                fx = float(d.iloc[0][col])
            dbg.fx_used = fx
            dbg.fx_source = str(fx_p)
            return fx

    # fallbacks
    for fp in [outputs / "m0_inputs" / "FX_Path.parquet", outputs / "FX_Path.parquet"]:
        if fp.exists():
            d = pd.read_parquet(fp)
            col = None
            for c in FX_COL_SYNS:
                if c in d.columns:
                    col = c
                    break
            if col is None:
                num = [c for c in d.columns if c != "Month_Index" and pd.api.types.is_numeric_dtype(d[c])]
                if num:
                    col = num[0]
            if col is not None:
                if "Month_Index" in d.columns:
                    row = d.sort_values("Month_Index").iloc[0]
                    fx = float(row[col])
                else:
                    fx = float(d.iloc[0][col])
                dbg.fx_used = fx
                dbg.fx_source = str(fp)
                return fx

    # last resort default (shouldn’t happen in your stack)
    fx = 18.50
    dbg.fx_used = fx
    dbg.fx_source = "default(18.50)"
    dbg.warnings.append("[M8.B3] FX source not found; defaulting to 18.50 NAD/USD.")
    return fx

def _load_gate_vals(outputs: Path, dbg: DebugBag) -> Dict[int, float]:
    p = outputs / "m8b_gate_valuations.json"
    gates: Dict[int, float] = {}
    if p.exists():
        raw = _read_json(p)
        # accept { "M24": 32000, "M36": 40000, ... } or lists/dicts
        def find_val(rawd, keys):
            for k in keys:
                if k in rawd:
                    return rawd[k]
            return None
        for m, keys in GATE_KEYS.items():
            v = find_val(raw, keys)
            if v is not None:
                gates[m] = float(v)
    # default for 36 if absent
    if 36 not in gates:
        gates[36] = DEFAULT_M36_CAP_NAD_000
        dbg.notes.append("[M8.B3] Gate Month‑36 defaulted to NAD 40,000,000 ('000 units) per policy.")
    dbg.gate_vals_nad_000 = {f"M{m}": gates[m] for m in sorted(gates)}
    return gates

def _coerce_number(x) -> Optional[float]:
    try:
        if x is None:
            return None
        if isinstance(x, str) and not x.strip():
            return None
        return float(x)
    except Exception:
        return None

def _derive_ticket(outputs: Path, offer: Dict[str, Any], option: Optional[str], fx: float, dbg: DebugBag) -> Tuple[Optional[float], Optional[float], Optional[int], str]:
    """
    Returns (ticket_nad_000, ticket_usd, ticket_month_index, source)
    """
    flat = {}
    # flatten offer for robust key search
    def _walk(prefix, obj):
        if isinstance(obj, dict):
            for k, v in obj.items():
                _walk(prefix + [k], v)
        else:
            flat["/".join([*prefix])] = obj
    if isinstance(offer, dict):
        _walk([], offer)

    # 1) from selected offer JSON
    # NAD '000 directly?
    for k in TICKET_NAD_000_KEYS:
        for key in [k, k.lower(), k.upper()]:
            for full in list(flat.keys()):
                if full.split("/")[-1] == key:
                    val = _coerce_number(flat[full])
                    if val and val > 0:
                        return val, None, 1, f"m7_selected_offer.json:{full}"
    # USD?
    for k in TICKET_USD_KEYS:
        for key in [k, k.lower(), k.upper()]:
            for full in list(flat.keys()):
                if full.split("/")[-1] == key:
                    usd = _coerce_number(flat[full])
                    if usd and usd > 0:
                        nad_000 = usd * fx / 1000.0
                        return nad_000, usd, 1, f"m7_selected_offer.json:{full}"

    # 2) from ranked grid (m7_r1_scores)
    for fname in ["m7_r1_scores.parquet", "m7_r1_scores.csv"]:
        p = outputs / fname
        df = _read_any_table(p)
        if df is not None and not df.empty:
            # Try filter by Option
            df2 = df.copy()
            if option is not None:
                for c in df2.columns:
                    if c.lower() in ("option", "option_id", "offer_id"):
                        df2 = df2[df2[c].astype(str) == str(option)]
                        break
            # pick Ticket_USD if present; else Ticket_NAD_000
            usd_col = None
            for c in df2.columns:
                if c in TICKET_USD_KEYS or c.lower() == "ticket_usd":
                    usd_col = c
                    break
            if usd_col and not df2.empty:
                usd = _coerce_number(df2.iloc[0][usd_col])
                if usd and usd > 0:
                    nad_000 = usd * fx / 1000.0
                    return nad_000, usd, 1, f"{fname}:{usd_col}"
            nad_col = None
            for c in df2.columns:
                if c in TICKET_NAD_000_KEYS or c.lower() == "ticket_nad_000":
                    nad_col = c; break
            if nad_col and not df2.empty:
                nad_000 = _coerce_number(df2.iloc[0][nad_col])
                if nad_000 and nad_000 > 0:
                    return nad_000, None, 1, f"{fname}:{nad_col}"

    # 3) from junior financing schedule (sum of positive inflows), also get injection month
    for fname in ["m7_5_junior_financing.parquet", "m7_5_junior_financing.csv"]:
        p = outputs / fname
        df = _read_any_table(p)
        if df is not None and not df.empty:
            use_col = None
            for c in JUNIOR_INFLOW_SYNS:
                if c in df.columns:
                    use_col = c; break
            if use_col is None:
                # pick first column that looks like junior in
                use_col = next((c for c in df.columns if "junior" in c.lower() and "nad" in c.lower()), None)
            if use_col is not None:
                s = df[use_col].fillna(0.0).astype(float)
                pos = s[s > 0]
                ticket_nad_000 = float(pos.sum()) if not pos.empty else float(s.clip(lower=0).sum())
                # injection month = first positive occurrence or Month_Index==1 if missing
                mi_col = "Month_Index" if "Month_Index" in df.columns else None
                inj_month = int(df.loc[s > 0, mi_col].iloc[0]) if (mi_col and (s > 0).any()) else 1
                if ticket_nad_000 > 0:
                    return ticket_nad_000, None, inj_month, f"{fname}:{use_col}"

    # none found
    return None, None, None, "not_found"

def _cap_and_discount(offer: Dict[str, Any], fx: float, dbg: DebugBag) -> Tuple[Optional[float], Optional[float], str]:
    """
    Returns (cap_nad_000, discount_fraction, source_note)
    """
    flat = {}
    def _walk(prefix, obj):
        if isinstance(obj, dict):
            for k, v in obj.items():
                _walk(prefix + [k], v)
        else:
            flat["/".join([*prefix])] = obj
    if isinstance(offer, dict):
        _walk([], offer)

    # Cap in NAD '000 first
    for k in CAP_KEYS_NAD_000:
        for key in [k, k.lower(), k.upper()]:
            for full in list(flat.keys()):
                if full.split("/")[-1] == key:
                    val = _coerce_number(flat[full])
                    if val and val > 0:
                        return val, None, f"cap:{full}(NAD '000)"

    # Cap in USD -> convert
    for k in CAP_KEYS_USD:
        for key in [k, k.lower(), k.upper()]:
            for full in list(flat.keys()):
                if full.split("/")[-1] == key:
                    usd = _coerce_number(flat[full])
                    if usd and usd > 0:
                        cap_nad_000 = usd * fx / 1000.0
                        return cap_nad_000, None, f"cap:{full}(USD→NAD '000)"

    # Discount %
    disc = None
    for k in DISCOUNT_KEYS:
        for key in [k, k.lower(), k.upper()]:
            for full in list(flat.keys()):
                if full.split("/")[-1] == key:
                    v = _coerce_number(flat[full])
                    if v is not None:
                        disc = float(v) / 100.0 if v > 1.0 else float(v)
                        # keep searching cap too, but record discount
                        break

    return None, disc, "cap:not_found"

def _ownership(ticket_nad_000: Optional[float],
               cap_nad_000: Optional[float],
               gate36_nad_000: Optional[float],
               discount_fraction: Optional[float],
               dbg: DebugBag) -> Tuple[Optional[float], Optional[str]]:
    """
    Conservative equity-like ownership approximation:
        basis candidates = { post_money_cap, discounted_gate_36, gate_36 }
        pick the minimum positive basis and compute ticket / basis.
    """
    if not ticket_nad_000 or ticket_nad_000 <= 0:
        return None, None

    candidates: List[Tuple[str, float]] = []

    # post-money cap (if we can infer it's post-money)
    if cap_nad_000 and cap_nad_000 > 0:
        candidates.append(("post_money_cap_nad_000", cap_nad_000))

    # discounted gate price (apply discount to EV proxy)
    if gate36_nad_000 and gate36_nad_000 > 0 and discount_fraction and discount_fraction > 0:
        candidates.append(("gate36_discounted", gate36_nad_000 * (1.0 - discount_fraction)))

    # raw gate36 EV as a denominator
    if gate36_nad_000 and gate36_nad_000 > 0:
        candidates.append(("gate36_ev", gate36_nad_000))

    candidates = [(name, val) for name, val in candidates if val and val > 0]
    if not candidates:
        return None, None

    basis_name, basis = min(candidates, key=lambda x: x[1])
    own = max(0.0, min(1.0, ticket_nad_000 / basis))
    return own, basis_name

def _irr_bisection(cashflows: List[float], max_iter: int = 200, tol: float = 1e-8) -> Optional[float]:
    """
    IRR for equally spaced periods (monthly). Returns monthly IRR.
    """
    if not cashflows or all(abs(x) < 1e-12 for x in cashflows):
        return None
    # If all same sign -> undefined
    if all(x >= 0 for x in cashflows) or all(x <= 0 for x in cashflows):
        return None

    def npv(rate: float) -> float:
        return sum(cf / ((1.0 + rate) ** t) for t, cf in enumerate(cashflows))

    # bracket
    lo, hi = -0.9999, 10.0
    f_lo, f_hi = npv(lo), npv(hi)
    # Expand hi if needed
    tries = 0
    while f_lo * f_hi > 0 and tries < 10:
        hi *= 2.0
        f_hi = npv(hi)
        tries += 1
    if f_lo * f_hi > 0:
        return None

    for _ in range(max_iter):
        mid = (lo + hi) / 2.0
        f_mid = npv(mid)
        if abs(f_mid) < tol:
            return mid
        if f_lo * f_mid < 0:
            hi, f_hi = mid, f_mid
        else:
            lo, f_lo = mid, f_mid
    return (lo + hi) / 2.0

def _build_metrics(option: str,
                   instrument: str,
                   gates: Dict[int, float],
                   ticket_nad_000: float,
                   ticket_month: int,
                   ownership: Optional[float]) -> pd.DataFrame:
    records = []
    for m in sorted(gates.keys()):
        ev = gates[m]
        payout = None if ownership is None else ownership * ev
        moic = None
        irr_m = None
        irr_a = None
        if payout is not None and ticket_nad_000 and ticket_nad_000 > 0:
            moic = payout / ticket_nad_000 if ticket_nad_000 > 0 else None
            # cashflows timeline: at ticket_month negative ticket; at m payout
            # Move t0 to ticket_month for convenience -> length = (m - ticket_month) + 1
            span = max(0, int(m) - int(ticket_month))
            cfs = [0.0] * (span + 1)
            cfs[0] = -float(ticket_nad_000)
            cfs[-1] = float(payout)
            irr_m = _irr_bisection(cfs)
            if irr_m is not None:
                irr_a = (1.0 + irr_m) ** 12 - 1.0
        records.append({
            "Option": option,
            "Instrument": instrument,
            "Gate_Month": m,
            "Gate_Label": f"M{m}",
            "Ticket_NAD_000": ticket_nad_000,
            "Ownership_Fraction": ownership,
            "EV_NAD_000": ev,
            "Payout_NAD_000": payout,
            "MOIC_x": moic,
            "IRR_Monthly": irr_m,
            "IRR_Annualized": irr_a,
        })
    return pd.DataFrame.from_records(records)


# --------------------------
# Main entry
# --------------------------

def run_m8B3(outputs_dir: str, currency: str, strict: bool = False, diagnostic: bool = False) -> None:
    out = Path(outputs_dir)
    dbg = DebugBag(offer_source_keys=[], warnings=[], notes=[])
    print(f"[M8.B3][INFO] Starting M8.B3 investor engine in: {outputs_dir}")

    # 1) Selected offer
    sel_path = out / "m7_selected_offer.json"
    sel_raw = _read_json(sel_path)
    option, instrument, offer, seen = _extract_selected_offer(sel_raw)
    dbg.option, dbg.instrument, dbg.offer_source_keys = option, instrument, seen
    if option:
        print(f"[M8.B3][OK]  Selected offer -> Option='{option}', Instrument='{instrument}'.")
    else:
        msg = "[M8.B3][WARN] Selected offer not found; proceeding with defaults."
        print(msg); dbg.warnings.append(msg)
        option = option or "UNKNOWN"
        instrument = instrument or "UNKNOWN"

    # 2) FX (NAD per USD)
    fx = _resolve_fx(out, dbg)

    # 3) Gates
    gates = _load_gate_vals(out, dbg)
    if gates:
        desc = ", ".join([f"M{m}={int(gates[m]):,}" for m in sorted(gates)])
        print(f"[M8.B3][OK]  Gate valuations loaded → {desc} (NAD '000).")
    else:
        msg = "[M8.B3][WARN] No gate valuations found; only Month‑36 default will be available."
        print(msg); dbg.warnings.append(msg)

    # 4) Ticket (investment) and injection month
    ticket_nad_000, ticket_usd, ticket_month, t_src = _derive_ticket(out, offer, option, fx, dbg)
    dbg.ticket_nad_000, dbg.ticket_usd, dbg.ticket_month_index, dbg.ticket_source = ticket_nad_000, ticket_usd, ticket_month, t_src
    if ticket_nad_000 and ticket_nad_000 > 0:
        if ticket_usd:
            print(f"[M8.B3][OK]  Ticket derived from USD {ticket_usd:,.0f} using FX={dbg.fx_used:0.4f} -> {ticket_nad_000:,.2f} NAD '000.")
        else:
            print(f"[M8.B3][OK]  Ticket found -> {ticket_nad_000:,.2f} NAD '000 (source: {t_src}).")
        if ticket_month:
            print(f"[M8.B3][OK]  Injection month detected -> Month_Index={ticket_month}.")
        else:
            ticket_month = 1
    else:
        msg = "[M8.B3][WARN] No investment amount found from selected offer/rank/junior schedule; metrics may be partial."
        print(msg); dbg.warnings.append(msg)
        ticket_nad_000 = ticket_nad_000 or 0.0
        ticket_month = ticket_month or 1

    # 5) Cap & discount
    cap_nad_000, disc, cap_src = _cap_and_discount(offer, fx, dbg)
    dbg.cap_nad_000, dbg.cap_source, dbg.discount_fraction = cap_nad_000, cap_src, disc

    # 6) Ownership approximation (equity-like)
    gate36 = gates.get(36, None)
    own, own_basis = _ownership(ticket_nad_000, cap_nad_000, gate36, disc, dbg)
    dbg.ownership_fraction, dbg.ownership_basis = own, own_basis
    if own is not None:
        print(f"[M8.B3][OK]  Ownership approximation: {own:0.6f} (basis={own_basis}).")
    else:
        msg = "[M8.B3][WARN] Could not infer ownership fraction (no cap/discount/gate basis). Metrics limited to cashflows where defined."
        print(msg); dbg.warnings.append(msg)

    # 7) Build metrics rows
    metrics = _build_metrics(option, instrument, gates, ticket_nad_000, ticket_month, own)

    # 8) Emit artifacts
    out_metrics = out / "m8b_investor_metrics_selected.parquet"
    metrics.to_parquet(out_metrics, index=False)
    print(f"[M8.B3][OK]  Emitted: {out_metrics.name}")

    out_debug = out / "m8b3_debug.json"
    with out_debug.open("w", encoding="utf-8") as f:
        json.dump(asdict(dbg), f, indent=2, ensure_ascii=False)
    print("[M8.B3][OK]  Debug → m8b3_debug.json")

    # Smoke
    lines = []
    lines.append("== M8.B3 SMOKE ==\n")
    lines.append(f"* Option/Instrument: {option} / {instrument}\n")
    lines.append(f"* Ticket_NAD_000: {ticket_nad_000:,.2f} | Ticket_USD: {'' if ticket_usd is None else f'{ticket_usd:,.0f}'} | FX used: {dbg.fx_used} ({dbg.fx_source})\n")
    lines.append(f"* Gates: {dbg.gate_vals_nad_000}\n")
    lines.append(f"* Ownership: {'' if own is None else f'{own:0.6f}'}  (basis={own_basis})\n")
    if not metrics.empty:
        head = metrics.head().to_string(index=False)
        lines.append("\n-- metrics head --\n")
        lines.append(head + "\n")
    if dbg.warnings:
        lines.append("\n-- warnings --\n")
        lines.extend([f"- {w}\n" for w in dbg.warnings])
    (out / "m8b3_smoke.md").write_text("".join(lines), encoding="utf-8")
    print("[M8.B3][OK]  Smoke → m8b3_smoke.md")


# Allow direct CLI use via: python -c "from terra_nova.modules.m8B_3_investor_engine.runner import run_m8B3; run_m8B3(r'.\outputs','NAD', strict=True, diagnostic=True)"
if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("outputs", help="Path to outputs directory")
    ap.add_argument("currency", help="Base currency code (e.g., 'NAD')")
    ap.add_argument("--strict", action="store_true", default=False)
    ap.add_argument("--diagnostic", action="store_true", default=False)
    args = ap.parse_args()
    run_m8B3(args.outputs, args.currency, strict=args.strict, diagnostic=args.diagnostic)
