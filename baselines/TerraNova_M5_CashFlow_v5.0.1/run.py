from __future__ import annotations
import argparse
from pathlib import Path
import sys
import pandas as pd
import numpy as np

# Ensure src on path (Windows/VS Code friendly)
ROOT = Path(__file__).resolve().parent
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

def main() -> int:
    parser = argparse.ArgumentParser(prog="run.py", description="Terra Nova CLI")
    sub = parser.add_subparsers(dest="cmd", required=True)

    # --- M5 parser (append-only wiring) ---
    p5 = sub.add_parser("run_m5", help="Run M5 Cash Flow Statement (final artifact)")
    p5.add_argument("--out", required=True, help="Output folder")
    p5.add_argument("--currency", required=True, help="Currency code, e.g., NAD")

    args = parser.parse_args()

    if args.cmd == "run_m5":
        out = Path(args.out); out.mkdir(parents=True, exist_ok=True)
        curr = str(args.currency).strip()

        # --- Upstream M2 artifacts (CFO baseline) ---
        pl_path = out / "m2_pl_statement.parquet"
        wc_path = out / "m2_working_capital_schedule.parquet"
        if not pl_path.exists():
            raise FileNotFoundError(f"[M5] Missing {pl_path.name} in {out}. Run M2 first.")
        if not wc_path.exists():
            raise FileNotFoundError(f"[M5] Missing {wc_path.name} in {out}. Run M2 first.")
        m2_pl = pd.read_parquet(pl_path)
        m2_wc = pd.read_parquet(wc_path)

        # --- Engine: audited CFO (no signature change; positional call) ---
        from terra_nova.modules.m5_cash_flow import assemble_cash_flow_statement
        res = assemble_cash_flow_statement(m2_pl, m2_wc, curr)
        cfs = res["statement"].copy()

        # --- Enrich with CFI/CFF from canonical M3 output ---
        m3_path = out / "m3_financing_engine_outputs.parquet"
        if not m3_path.exists():
            raise FileNotFoundError(f"[M5] Missing {m3_path.name} in {out}. Run M3 first.")
        m3 = pd.read_parquet(m3_path)
        req_cols = [
            "Month_Index",
            "Total_CAPEX_Outflow",
            "Total_Drawdown",
            "Total_Principal_Repayment",
            "Equity_Injection",
        ]
        miss = [c for c in req_cols if c not in m3.columns]
        if miss:
            raise KeyError(f"[M5] Missing required columns in {m3_path.name}: {miss}")
        m3 = m3[req_cols].sort_values("Month_Index")
        cfs = cfs.merge(m3, on="Month_Index", how="left")

        # CFI / CFF and identity
        cfs["CFI"] = cfs["Total_CAPEX_Outflow"].fillna(0.0)
        cfs["CFF"] = (
            cfs["Total_Drawdown"].fillna(0.0)
            - cfs["Total_Principal_Repayment"].fillna(0.0)
            + cfs["Equity_Injection"].fillna(0.0)
        )
        cfs["Net_Change_in_Cash"] = cfs["CFO"] + cfs["CFI"] + cfs["CFF"]

        # Final schema (drop helpers)
        cfs = cfs[[
            "Month_Index","Currency",
            "Net_Profit_After_Tax","Depreciation_and_Amortization",
            "Delta_Accounts_Receivable","Delta_Inventory","Delta_Accounts_Payable",
            "CFO","CFI","CFF","Net_Change_in_Cash"
        ]]

        final = out / "m5_cash_flow_statement_final.parquet"
        cfs.to_parquet(final, index=False)

        smoke = out / "m5_smoke_report.md"
        identity_ok = np.allclose(
            cfs["Net_Change_in_Cash"].to_numpy(),
            (cfs["CFO"] + cfs["CFI"] + cfs["CFF"]).to_numpy(),
            atol=1e-9
        )
        smoke.write_text(
            "# M5 Smoke Report\n\n"
            f"- Rows: {len(cfs)}\n"
            f"- Final Artifact: {final.name}\n"
            f"- Identity (ΔCash=CFO+CFI+CFF): {'PASS' if identity_ok else 'FAIL'}\n",
            encoding="utf-8",
        )
        print(f"[M5] Finished. Wrote {final}")
        return 0

    # If other commands are present in the user's original run.py, they remain untouched there.
    # This minimal runner only wires M5 per packaging instructions.
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
