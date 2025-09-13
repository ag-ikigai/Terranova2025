# src/terra_nova/modules/m8B_5_benchmarks/runner.py
from __future__ import annotations

import json
from pathlib import Path
import pandas as pd

def _ok(m): print(f"[M8.B5][OK]  {m}")
def _info(m): print(f"[M8.B5][INFO] {m}")

# Minimal catalog for metrics we compute in M8.B2/M8.B4/M8.B3
CATALOG = [
  # metric_id, audience, title, unit, better_is, green, yellow, red, source
  ("current_ratio","promoter","Current Ratio","x","higher",1.5,1.25,1.0,"Ag-lending practice"),
  ("quick_ratio","promoter","Quick Ratio","x","higher",1.2,1.0,0.8,"Ag-lending practice"),
  ("operating_expense_ratio","promoter","Operating Expense Ratio","%","lower",65,75,85,"Farm scorecards"),
  ("roa","promoter","Return on Assets","%","higher",7,5,0,"Farm scorecards"),
  ("roe","promoter","Return on Equity","%","higher",12,10,0,"Farm scorecards"),
  ("dscr","lender","Debt Service Coverage Ratio","x","higher",1.50,1.30,1.10,"Project finance"),
  ("icr","lender","Interest Coverage Ratio","x","higher",3.0,2.0,1.5,"Lender heuristics"),
  ("llcr","lender","Loan Life Coverage Ratio","x","higher",1.6,1.3,1.1,"Project finance"),
  ("plcr","lender","Project Life Coverage Ratio","x","higher",1.6,1.3,1.1,"Project finance"),
  ("moic_gate","investor","MOIC @ Gate","x","higher",2.0,1.5,1.0,"Private markets"),
  ("irr_gate","investor","IRR @ Gate","%","higher",25,15,10,"Private markets"),
]

def run_m8B5(outputs_dir: str, currency: str, strict: bool=False, diagnostic: bool=False):
    """
    Emits:
      - m8b_benchmarks.catalog.json  (metric metadata + thresholds)
      - m8b_benchmarks.values.parquet (static region/enterprise defaults for M9 display)
    """
    out = Path(outputs_dir)
    _info(f"Starting M8.B5 benchmarks in: {out}")

    catalog = []
    for (mid,aud,title,unit,better,green,yellow,red,src) in CATALOG:
        catalog.append({
            "metric_id": mid,
            "audience": aud,
            "title": title,
            "unit": unit,
            "better_is": better,
            "thresholds": {"green": green, "yellow": yellow, "red": red},
            "source_note": src
        })
    (out/"m8b_benchmarks.catalog.json").write_text(json.dumps(catalog, indent=2), encoding="utf-8")
    _ok("Emitted: m8b_benchmarks.catalog.json")

    # Default values table (so M9 can show strips/legends even before user customizes)
    df = pd.DataFrame({
        "region": ["Namibia"],
        "enterprise": ["Mixed_Horticulture_70ha"],
        "currency_context": [currency],
        "note": ["Default thresholds for display; customize via this file if needed."]
    })
    df.to_parquet(out/"m8b_benchmarks.values.parquet")
    _ok("Emitted: m8b_benchmarks.values.parquet")

    # Quick smoke
    smoke = "[SMOKE] Catalog count={} ; Values rows={}".format(len(catalog), len(df))
    (out/"m8b5_smoke.md").write_text(smoke, encoding="utf-8"); _ok("Smoke → m8b5_smoke.md")
    (out/"m8b5_debug.json").write_text(json.dumps({"count": len(catalog)}, indent=2), encoding="utf-8"); _ok("Debug → m8b5_debug.json")

if __name__ == "__main__":
    run_m8B5(r".\outputs","NAD", strict=False, diagnostic=False)
