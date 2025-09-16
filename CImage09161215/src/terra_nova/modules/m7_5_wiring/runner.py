import json
from pathlib import Path
from typing import Optional, Dict, Any

import pandas as pd


def _pretty(val) -> str:
    if isinstance(val, float):
        return f"{val:,.2f}"
    return str(val)


def run_m7_5(
    out_dir: str,
    currency: str = "NAD",
    fx_usd_to_nad: Optional[float] = None,
    injection_month: int = 1,
    write_csv: bool = True,
) -> Dict[str, Any]:
    """
    Stage A wiring of the selected junior instrument:
    - Reads outputs/m7_selected_offer.json (created by tools/m7_freeze_selection.py)
    - Converts Ticket_USD to NAD (if currency == NAD) using fx_usd_to_nad if provided, else default 19.00
    - Emits outputs/m7_5_junior_financing.parquet (+ optional CSV) with a single injection row:
        Month_Index, Option, Instrument, FX_USD_to_NAD, Junior_Equity_In_NAD_000
    - Writes outputs/m7_5_debug.json and a simple smoke report markdown.

    NOTE: This is “7.5A” — a clean schedule for downstream wiring.
          It does NOT alter M6 in-place. “7.5B” will consume this schedule to
          post PPE/cash/equity deltas in the formal BS pipeline.
    """
    out = Path(out_dir)
    out.mkdir(parents=True, exist_ok=True)

    sel_path = out / "m7_selected_offer.json"
    if not sel_path.exists():
        raise FileNotFoundError(
            f"Missing {sel_path}. Run the freezer first: tools/m7_freeze_selection.py .\\outputs"
        )

    sel = json.loads(sel_path.read_text(encoding="utf-8"))

    option = sel.get("Option")
    instrument = sel.get("Instrument")
    ticket_usd = sel.get("Ticket_USD")

    if ticket_usd is None:
        raise ValueError("m7_selected_offer.json missing 'Ticket_USD'.")

    # FX logic
    if currency.upper() == "NAD":
        fx = 19.00 if fx_usd_to_nad is None else float(fx_usd_to_nad)
        ticket_nad = ticket_usd * fx
        ticket_nad_000 = round(ticket_nad / 1_000.0, 3)
        fx_note = f"USD→NAD @ {fx:,.4f}"
    else:
        # If model currency is USD, no conversion needed
        fx = 1.0
        ticket_nad_000 = round(ticket_usd / 1_000.0, 3)
        fx_note = "Currency USD: no FX applied"

    # Build schedule (single-row injection)
    df = pd.DataFrame(
        [
            dict(
                Month_Index=int(injection_month),
                Option=str(option),
                Instrument=str(instrument),
                FX_USD_to_NAD=float(fx),
                Junior_Equity_In_NAD_000=float(ticket_nad_000),
            )
        ]
    )

    # Write artifacts
    fin_parquet = out / "m7_5_junior_financing.parquet"
    df.to_parquet(fin_parquet, index=False)

    fin_csv = None
    if write_csv:
        fin_csv = out / "m7_5_junior_financing.csv"
        df.to_csv(fin_csv, index=False)

    debug = {
        "status": "ok",
        "inputs": {
            "selection_file": str(sel_path),
            "currency": currency,
            "injection_month": injection_month,
        },
        "selection": sel,
        "fx_note": fx_note,
        "outputs": {
            "financing_schedule_parquet": str(fin_parquet),
            "financing_schedule_csv": str(fin_csv) if fin_csv else None,
        },
        "preview": df.head(10).to_dict(orient="records"),
    }
    (out / "m7_5_debug.json").write_text(json.dumps(debug, indent=2), encoding="utf-8")

    # Simple smoke report
    md = []
    md.append("# M7.5 (A) — Junior financing schedule")
    md.append("")
    md.append(f"- Selected: **{option}** / **{instrument}**")
    md.append(f"- Ticket: **USD {_pretty(ticket_usd)}** → **NAD ‘000 {_pretty(ticket_nad_000)}** ({fx_note})")
    md.append(f"- Injection @ Month_Index **{injection_month}**")
    md.append("")
    md.append("## Artifacts")
    md.append(f"- Schedule (parquet): `{fin_parquet}`")
    if fin_csv:
        md.append(f"- Schedule (csv): `{fin_csv}`")
    md.append(f"- Debug JSON: `{out / 'm7_5_debug.json'}`")
    (out / "m7_5_smoke_report.md").write_text("\n".join(md), encoding="utf-8")

    print(
        f"[OK] M7.5 wiring emitted -> {fin_parquet}. "
        f"Smoke -> {out / 'm7_5_smoke_report.md'}"
    )
    return debug
