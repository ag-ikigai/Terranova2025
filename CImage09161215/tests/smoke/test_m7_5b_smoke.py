# tests/smoke/test_m7_5b_smoke.py
import json
from pathlib import Path

import pandas as pd
import unittest


OUT = Path("./outputs")


class TestM75BSmoke(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.pl_p = OUT / "m7_5b_profit_and_loss.parquet"
        cls.cf_p = OUT / "m7_5b_cash_flow.parquet"
        cls.bs_p = OUT / "m7_5b_balance_sheet.parquet"
        cls.debug_p = OUT / "m7_5b_debug.json"

        for p in [cls.pl_p, cls.cf_p, cls.bs_p]:
            assert p.exists(), f"Missing: {p}"

        cls.pl = pd.read_parquet(cls.pl_p)
        cls.cf = pd.read_parquet(cls.cf_p)
        cls.bs = pd.read_parquet(cls.bs_p)

    def test_artifacts_exist(self):
        self.assertTrue(self.pl_p.exists())
        self.assertTrue(self.cf_p.exists())
        self.assertTrue(self.bs_p.exists())

    def test_bs_totals_tie(self):
        self.assertIn("Assets_Total_NAD_000", self.bs.columns)
        self.assertIn("Liabilities_And_Equity_Total_NAD_000", self.bs.columns)
        diff = (self.bs["Assets_Total_NAD_000"] - self.bs["Liabilities_And_Equity_Total_NAD_000"]).abs().max()
        self.assertLess(diff, 1e-6, "BS totals do not tie in NAD")

    def test_cf_links_to_bs_cash_when_available(self):
        cf_close = next((c for c in self.cf.columns if c.lower().startswith("closing_cash")), None)
        bs_cash = next((c for c in self.bs.columns if c.lower().startswith("cash_and_cash_equivalents")), None)
        if cf_close and bs_cash:
            diff = (self.cf[cf_close] - self.bs[bs_cash]).abs().max()
            self.assertLess(diff, 1e-6, "Closing cash in CF != Cash on BS")
        else:
            self.skipTest("Closing cash and/or BS cash not present – link check skipped.")

    def test_usd_columns_present(self):
        total = 0
        for df in (self.pl, self.cf, self.bs):
            total += sum(1 for c in df.columns if c.endswith("_USD_000"))
        self.assertGreaterEqual(total, 3, "Expecting a few _USD_000 columns across the statements")

    def test_debug_fx_source_present(self):
        self.assertTrue(self.debug_p.exists(), "Missing m7_5b_debug.json")
        dbg = json.loads(self.debug_p.read_text(encoding="utf-8"))
        fx_keys = [k for k in dbg.keys() if k.startswith("fx")]
        self.assertTrue(len(fx_keys) > 0, "No FX metadata keys found in m7_5b_debug.json")


if __name__ == "__main__":
    unittest.main()
