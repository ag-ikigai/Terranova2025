import os, unittest, pandas as pd

OUT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "outputs"))

class TestM6Smoke(unittest.TestCase):
    def test_m6_artifacts_exist(self):
        p = os.path.join(OUT, "m6_balance_sheet.parquet")
        self.assertTrue(os.path.exists(p), "m6_balance_sheet.parquet not found")
        df = pd.read_parquet(p)
        self.assertGreater(len(df), 0)
        self.assertIn("Assets_Total_NAD_000", df.columns)
        self.assertIn("Liabilities_And_Equity_Total_NAD_000", df.columns)

    def test_identity_holds(self):
        df = pd.read_parquet(os.path.join(OUT, "m6_balance_sheet.parquet"))
        diff = (df["Assets_Total_NAD_000"] - df["Liabilities_And_Equity_Total_NAD_000"]).abs().max()
        self.assertLessEqual(diff, 1e-6)

if __name__ == "__main__":
    unittest.main()
