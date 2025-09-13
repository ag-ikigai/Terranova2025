# src/terra_nova/modules/m5_cash_flow/runner.py
"""
Entrypoint to assemble the M5 cash flow statement (robust edition).
Usage (PowerShell):
  $env:PYTHONPATH="C:\TerraNova\src"; .\.venv\Scripts\python.exe -c "from terra_nova.modules.m5_cash_flow.runner import run_m5; run_m5(r'.\outputs','NAD', strict=$true)"
"""
from __future__ import annotations
from pathlib import Path
import json
import pandas as pd

from .engine import build_cash_flow_statement as _build, assemble_cash_flow_statement as _assemble

def run_m5(outputs_dir: str, currency: str, input_pack_path: str=None, strict: bool=False) -> None:
    out = Path(outputs_dir)
    out.mkdir(parents=True, exist_ok=True)
    print("[M5][INFO] Starting M5 assembler in:", str(out))
    df, meta, smoke = _assemble(str(out), currency, input_pack_path=input_pack_path, strict=strict)

    # emit
    cf_path = out / "m5_cash_flow_statement_final.parquet"
    debug_path = out / "m5_debug_dump.json"
    smoke_path = out / "m5_smoke_report.md"

    # save parquet
    try:
        df.to_parquet(cf_path, index=False)
    except Exception:
        df.to_parquet(cf_path, index=False, engine="pyarrow")

    # debug json (pretty)
    with open(debug_path, "w", encoding="utf-8") as f:
        json.dump({"meta": meta, "preview": df.head(12).to_dict(orient="list")}, f, indent=2)

    # smoke report
    lines = []
    lines.append("== M5 – Cash Flow Statement (robust) ==")
    lines.append(f"[OK] Shape: {df.shape}")
    lines.append(f"[OK] Columns: {list(df.columns)}")
    lines.append(f"[OK] Opening cash (policy): {meta['sources']['m0_opening_cash']['source'] or 'DEFAULT 0.0'} -> {smoke['opening_cash_nad_000']:,.2f} (NAD '000)")
    lines.append(f"[OK] Non‑zero flags: CFO={smoke['cfo_nonzero']}, CFI={smoke['cfi_nonzero']}, CFF={smoke['cff_nonzero']}")
    lines.append("")
    lines.append("Sources:")
    for k,v in meta["sources"].items():
        lines.append(f"- {k}: {v}")
    with open(smoke_path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))

    print("[M5][OK]  Emitted:", cf_path.name)
    print("[M5][OK]  Debug ->", debug_path.name)
    print("[M5][OK]  Smoke ->", smoke_path.name)
