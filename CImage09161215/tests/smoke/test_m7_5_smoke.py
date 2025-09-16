import json
from pathlib import Path

import pandas as pd
import unittest


class TestM75Smoke(unittest.TestCase):
    OUT = Path("outputs")

    def test_artifacts_exist(self):
        self.assertTrue((self.OUT / "m7_5_junior_financing.parquet").exists())
        self.assertTrue((self.OUT / "m7_5_debug.json").exists())
        self.assertTrue((self.OUT / "m7_5_smoke_report.md").exists())

    def test_financing_has_injection(self):
        df = pd.read_parquet(self.OUT / "m7_5_junior_financing.parquet")
        self.assertIn("Junior_Equity_In_NAD_000", df.columns)
        self.assertGreater(df["Junior_Equity_In_NAD_000"].sum(), 0.0)

    def test_debug_has_fx_note(self):
        dbg = json.loads((self.OUT / "m7_5_debug.json").read_text(encoding="utf-8"))
        self.assertEqual(dbg["status"], "ok")
        self.assertIn("fx_note", dbg)
