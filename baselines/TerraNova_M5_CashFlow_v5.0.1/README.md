# Terra Nova – Module 5 (Cash Flow Statement)

## Run M5 (final artifact)

```powershell
# VS Code PowerShell terminal, from project root:
$OUT = ".\outputs"
$CURR = "NAD"

# M5 (assumes M0–M4 already ran and wrote canonical M2/M3 artifacts into $OUT)
.\.venv\Scripts\python.exe .\run.py run_m5 --out $OUT --currency $CURR
```

**Expected outputs**

- `outputs/m5_cash_flow_statement_final.parquet`
- `outputs/m5_smoke_report.md`

ΔCash identity is enforced in the CLI branch: `ΔCash = CFO + CFI + CFF`.
CFI/CFF are sourced from `outputs/m3_financing_engine_outputs.parquet`:
- `CFI = Total_CAPEX_Outflow`
- `CFF = Total_Drawdown - Total_Principal_Repayment + Equity_Injection`
