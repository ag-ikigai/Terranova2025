# --- path bootstrap (must stay at the top) ---
from pathlib import Path
import sys
ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))
# ---------------------------------------------

import unittest
import pandas as pd
import numpy as np
from terra_nova.modules.m5_cash_flow import assemble_cash_flow_statement

class TestM5Full(unittest.TestCase):
    def setUp(self):
        self.pl = pd.DataFrame({
            "Month_Index": [1,2,3],
            "Net_Profit_After_Tax": [100.0, 150.0, 80.0],
            "Depreciation_and_Amortization": [10.0, 10.0, 10.0],
        })
        self.wc = pd.DataFrame({
            "Month_Index": [1,2,3],
            "Accounts_Receivable_EOP": [50.0, 70.0, 60.0],  # raw deltas: +50, +20, -10 -> cash: -50, -20, +10
            "Inventory_EOP":           [30.0, 30.0, 40.0],  # raw deltas: +30, 0, +10  -> cash: -30,  0, -10
            "Accounts_Payable_EOP":    [40.0, 50.0, 45.0],  # raw deltas: +40, +10, -5 -> cash: +40, +10,  -5
        })
        self.curr = "NAD"

    def test_engine_cfo_and_signs(self):
        cfs = assemble_cash_flow_statement(self.pl, self.wc, self.curr)["statement"]
        # first-month deltas equal first EOP with cash signs
        self.assertAlmostEqual(cfs.loc[cfs.Month_Index==1, "Delta_Accounts_Receivable"].iloc[0], -50.0)
        self.assertAlmostEqual(cfs.loc[cfs.Month_Index==1, "Delta_Inventory"].iloc[0], -30.0)
        self.assertAlmostEqual(cfs.loc[cfs.Month_Index==1, "Delta_Accounts_Payable"].iloc[0], 40.0)
        # CFO checks
        self.assertAlmostEqual(cfs.loc[cfs.Month_Index==1, "CFO"].iloc[0], 70.0)   # 100+10-50-30+40
        self.assertAlmostEqual(cfs.loc[cfs.Month_Index==2, "CFO"].iloc[0], 150.0)  # 150+10-20+0+10
        self.assertAlmostEqual(cfs.loc[cfs.Month_Index==3, "CFO"].iloc[0], 85.0)   # 80+10+10-10-5
        # identity with placeholder CFI/CFF
        self.assertTrue(np.allclose(cfs["Net_Change_in_Cash"], cfs["CFO"] + cfs["CFI"] + cfs["CFF"], atol=1e-9))

    def test_schema_and_currency(self):
        cfs = assemble_cash_flow_statement(self.pl, self.wc, self.curr)["statement"]
        expected = [
            "Month_Index","Currency",
            "Net_Profit_After_Tax","Depreciation_and_Amortization",
            "Delta_Accounts_Receivable","Delta_Inventory","Delta_Accounts_Payable",
            "CFO","CFI","CFF","Net_Change_in_Cash"
        ]
        self.assertEqual(expected, list(cfs.columns))
        self.assertTrue((cfs["Currency"] == self.curr).all())

    def test_artifact_name_convention_optional(self):
        # optional naming check; skip if OUT not present
        out = ROOT / "outputs"
        if not (out / "m5_cash_flow_statement_final.parquet").exists():
            self.skipTest("Final artifact not present in test environment.")
        # if present, file should be non-empty parquet
        import pandas as pd
        df = pd.read_parquet(out / "m5_cash_flow_statement_final.parquet")
        self.assertGreater(len(df), 0)

if __name__ == "__main__":
    unittest.main()
