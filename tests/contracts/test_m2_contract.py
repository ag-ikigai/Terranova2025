import unittest, pandas as pd
from pathlib import Path

PL_REQ = [
    "Month_Index","Revenue_NAD_000","COGS_NAD_000","Gross_Profit_NAD_000",
    "Opex_NAD_000","EBITDA_NAD_000","Depreciation_and_Amortization_NAD_000",
    "EBIT_NAD_000","Interest_Expense_NAD_000","Pre_Tax_Profit_NAD_000",
    "Tax_Expense_NAD_000","Net_Profit_After_Tax_NAD_000",
]
WC_REQ = [
    "Month_Index","AR_Balance_NAD_000","Inventory_Balance_NAD_000",
    "AP_Balance_NAD_000","NWC_Balance_NAD_000","Cash_Flow_from_NWC_Change_NAD_000",
]

def _pick(out_dir: Path, a: str, b: str) -> Path:
    pa, pb = out_dir/a, out_dir/b
    return pa if pa.exists() else pb

class TestM2Contract(unittest.TestCase):
    def test_columns_and_index(self):
        out = Path("outputs")
        pl = pd.read_parquet(_pick(out,"m2_pl_schedule.parquet","m2_pl_statement.parquet"))
        wc = pd.read_parquet(_pick(out,"m2_working_capital_schedule.parquet","m2_working_capital.parquet"))

        self.assertTrue(set(PL_REQ).issubset(pl.columns))
        self.assertTrue(set(WC_REQ).issubset(wc.columns))
        self.assertTrue(pd.api.types.is_integer_dtype(pl["Month_Index"]))
        self.assertTrue(pd.api.types.is_integer_dtype(wc["Month_Index"]))
        self.assertTrue(pl["Month_Index"].is_monotonic_increasing)
        self.assertTrue(wc["Month_Index"].is_monotonic_increasing)
