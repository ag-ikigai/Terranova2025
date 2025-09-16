import unittest
import pandas as pd
from terra_nova.modules.m6_balance_sheet.engine import compute_balance_sheet

class TestM6Engine(unittest.TestCase):
    def test_compute_balance_sheet_minimal(self):
        # 3 months synthetic
        m2_pl = pd.DataFrame({
            "Month_Index": [1,2,3],
            "NPAT_NAD_000": [10.0, -2.0, 5.0],
        })
        # Positive CF => release => NWC level decreases; start at 0
        m2_wc = pd.DataFrame({
            "Month_Index": [1,2,3],
            "Cash_Flow_from_NWC_Change_NAD_000": [3.0, -4.0, 1.0],  # levels: -3, +1, 0 -> asset/liab split
        })
        m3_debt = pd.DataFrame({
            "Month_Index": [1,2,3],
            "Outstanding_Balance_NAD_000": [100.0, 90.0, 80.0],
        })
        m4_tax = pd.DataFrame({
            "Month_Index": [1,2,3],
            "Tax_Payable_NAD_000": [0.0, 2.0, 1.0],
        })

        bs = compute_balance_sheet(m2_pl, m2_wc, m3_debt, m4_tax, "NAD")

        self.assertEqual(list(bs["Month_Index"]), [1,2,3])
        # Retained earnings cumulative: 10, 8, 13
        self.assertListEqual(list(bs["Equity_Retained_Earnings_NAD_000"].round(6)), [10.0, 8.0, 13.0])
        # Identity
        diff = (bs["Assets_Total_NAD_000"] - bs["Liabilities_And_Equity_Total_NAD_000"]).abs().max()
        self.assertLessEqual(diff, 1e-6)

if __name__ == "__main__":
    unittest.main()
