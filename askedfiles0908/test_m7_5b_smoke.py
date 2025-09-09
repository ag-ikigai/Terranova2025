import json
from pathlib import Path
import unittest
import pandas as pd
import numpy as np

OUT = Path("outputs")

class TestM75BSmoke(unittest.TestCase):

    def test_artifacts_exist(self):
        self.assertTrue((OUT/"m7_5b_profit_and_loss.parquet").exists())
        self.assertTrue((OUT/"m7_5b_cash_flow.parquet").exists())
        self.assertTrue((OUT/"m7_5b_balance_sheet.parquet").exists())
        self.assertTrue((OUT/"m7_5b_debug.json").exists())
        self.assertTrue((OUT/"m7_5b_smoke_report.md").exists())

    def test_bs_ties(self):
        bs = pd.read_parquet(OUT/"m7_5b_balance_sheet.parquet")
        a = "Assets_Total_NAD_000_Rebuilt"
        l = "Liabilities_And_Equity_Total_NAD_000_Rebuilt"
        self.assertIn(a, bs.columns)
        self.assertIn(l, bs.columns)
        self.assertLessEqual(float(np.abs(bs[a]-bs[l]).max()), 1e-6)

    def test_usd_columns_present(self):
        pl = pd.read_parquet(OUT/"m7_5b_profit_and_loss.parquet")
        cf = pd.read_parquet(OUT/"m7_5b_cash_flow.parquet")
        bs = pd.read_parquet(OUT/"m7_5b_balance_sheet.parquet")
        self.assertTrue(any(c.endswith("_USD_000") or c.endswith("_USD_000_Rebuilt") for c in pl.columns))
        self.assertTrue(any(c.endswith("_USD_000") for c in cf.columns))
        self.assertTrue(any(c.endswith("_USD_000") for c in bs.columns))

    def test_subordination_guard(self):
        # paid junior outflows must not exceed CFO on the month (weak check)
        cf = pd.read_parquet(OUT/"m7_5b_cash_flow.parquet")
        self.assertIn("CFO_NAD_000", cf.columns)
        self.assertIn("CFF_Junior_Out_NAD_000", cf.columns)
        self.assertTrue(((cf["CFF_Junior_Out_NAD_000"] <= cf["CFO_NAD_000"] + 1e-6) | (cf["CFF_Junior_Out_NAD_000"].isna())).all())
