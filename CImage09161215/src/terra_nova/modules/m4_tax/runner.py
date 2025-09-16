import json
from pathlib import Path
import pandas as pd
import numpy as np

def _read_parquet(path: Path, required=True) -> pd.DataFrame:
    if path.exists():
        return pd.read_parquet(path)
    if required:
        raise FileNotFoundError(f"Missing {path}")
    return pd.DataFrame()

def _tax_rate_from_wct(df: pd.DataFrame) -> float:
    # Try Working_Capital_Tax parquet for a tax rate number.
    if df is None or df.empty:
        return 0.30
    cols = df.columns
    if set(["Parameter", "Value"]).issubset(cols):
        # look for 'tax' and 'rate' in Parameter
        row = df[df["Parameter"].astype(str).str.lower().str.contains("tax") &
                 df["Parameter"].astype(str).str.lower().str.contains("rate")]
        if not row.empty:
            v = str(row.iloc[0]["Value"]).strip().replace("%","")
            try:
                fl = float(v)
                if fl > 1.5:
                    return fl/100.0
                elif 0 < fl <= 1.5:
                    return fl
            except Exception:
                pass
    return 0.30

def run_m4(input_xlsx: str, out_dir: str, currency: str = "NAD") -> dict:
    """
    Minimal accrual+cash tax schedule compatible with downstream M5.
    Uses NPAT from M2 stub and a tax rate from Working_Capital_Tax (fallback 30%).
    Tax paid same-period to keep payable flat at zero (deterministic).
    Emits:
      - m4_tax_schedule.parquet
      - m4_tax_summary.parquet
      - m4_smoke_report.md
    """
    out = Path(out_dir)
    out.mkdir(parents=True, exist_ok=True)
    cal = _read_parquet(out / "m0_calendar.parquet", required=True)
    pl = _read_parquet(out / "m2_profit_and_loss_stub.parquet", required=True)
    wct = _read_parquet(out / "m0_inputs" / "Working_Capital_Tax.parquet", required=False)

    rate = _tax_rate_from_wct(wct)
    months = cal["Month_Index"].astype(int).tolist()
    n = len(months)

    # Align NPAT to calendar
    def _align(df, cname):
        if "Month_Index" in df.columns and cname in df.columns:
            tmp = df[["Month_Index", cname]].copy().set_index("Month_Index").reindex(months).fillna(0.0)
            return pd.to_numeric(tmp[cname], errors="coerce").fillna(0.0).reset_index(drop=True)
        return pd.Series(np.zeros(n))

    npat = _align(pl, "NPAT_NAD_000")

    # Simple taxable income approximation:
    # If NPAT>0, approximate PBT = NPAT / (1-rate), else 0
    taxable = np.where(npat > 0, npat / max(rate and 1-rate or 0.70, 0.000001), 0.0)
    tax_exp = taxable * rate
    tax_paid = tax_exp.copy()  # same period cash tax for now (keeps payable=0)
    tax_payable_end = np.zeros(n)

    tax = pd.DataFrame({
        "Month_Index": months,
        "Taxable_Income_NAD_000": taxable,
        "Tax_Rate": [rate]*n,
        "Tax_Expense_NAD_000": tax_exp,
        "Tax_Paid_NAD_000": tax_paid,
        "Tax_Payable_End_NAD_000": tax_payable_end
    })

    # simple summary
    summ = pd.DataFrame({
        "Key": ["Tax_Rate", "Months", "Total_Tax_Expense_NAD_000", "Total_Tax_Paid_NAD_000"],
        "Value": [rate, n, float(tax_exp.sum()), float(tax_paid.sum())]
    })

    tax.to_parquet(out / "m4_tax_schedule.parquet", index=False)
    summ.to_parquet(out / "m4_tax_summary.parquet", index=False)
    (out / "m4_smoke_report.md").write_text(
        "# M4 Smoke Report\n\n"
        f"- Tax rate used: {rate:.6f}\n"
        f"- Months: {n}\n"
        f"- Total Expense: {tax_exp.sum():.3f}\n"
        f"- Total Paid: {tax_paid.sum():.3f}\n",
        encoding="utf-8"
    )
    print("[M4][OK] Emitted m4_tax_schedule.parquet, m4_tax_summary.parquet")
    return {"rows": len(tax)}