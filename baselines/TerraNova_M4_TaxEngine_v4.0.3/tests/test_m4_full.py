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
from terra_nova.modules.m4_tax import compute_tax_schedule

class TestM4FullPackaging(unittest.TestCase):
    def setUp(self):
        self.calendar = pd.DataFrame({"Month_Index": range(1, 61)})
        self.pl = pd.DataFrame({"Month_Index": range(1, 61)})  # minimal P&L statement

    def test_zeroed_when_config_missing(self):
        out = compute_tax_schedule(self.calendar, self.pl, None, "CaseX", "NAD", opening_bs_df=pd.DataFrame())
        sched, smry = out["schedule"], out["summary"]
        self.assertEqual(len(sched), 60)
        self.assertTrue((sched["Tax_Expense"].abs() < 1e-12).all())
        self.assertEqual(smry["Computation_Mode"].iloc[0], "zeroed")

    def test_configured_placeholder(self):
        cfg = pd.DataFrame({"Key": ["Example"], "Value": [1]})
        out = compute_tax_schedule(self.calendar, self.pl, cfg, "CaseX", "NAD", opening_bs_df=pd.DataFrame())
        sched, smry = out["schedule"], out["summary"]
        self.assertEqual(len(sched), 60)
        self.assertTrue((sched["Tax_Expense"].abs() < 1e-12).all())
        self.assertEqual(smry["Computation_Mode"].iloc[0], "configured")

if __name__ == "__main__":
    unittest.main()
