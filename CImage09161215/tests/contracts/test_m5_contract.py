import unittest, pandas as pd
from pathlib import Path

class TestM5Contract(unittest.TestCase):
    def test_cfo_identity(self):
        out = Path("outputs")
        cf = pd.read_parquet(out/"m5_cash_flow_statement_final.parquet")
        # required columns
        for c in ["Month_Index","NPAT_NAD_000","DandA_NAD_000","NWC_CF_NAD_000","CFO_NAD_000"]:
            self.assertIn(c, cf.columns)
        # identity check
        chk = (cf["NPAT_NAD_000"] + cf["DandA_NAD_000"] + cf["NWC_CF_NAD_000"] - cf["CFO_NAD_000"]).abs()
        self.assertLess(chk.max(), 1e-6)
