import unittest
from pathlib import Path
import sys
import pandas as pd

# Ensure src/ is on sys.path for direct test runs
ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from terra_nova.modules.m3_financing.engine import create_financing_schedules

class TestM3Full(unittest.TestCase):
    def setUp(self):
        # Standard 60-month calendar horizon for tests (1..60)
        self.calendar_df = pd.DataFrame({"Month_Index": range(1, 61)})

    def _stack_base_row(self, **overrides):
        base = dict(
            Case_Name="CaseX", Line_ID=1, Instrument="TermLoan", Currency="NAD",
            Principal=1000.0, Rate_Pct=12.0, Tenor_Months=36,
            Draw_Start_M=1, Draw_End_M=3, Grace_Int_M=0, Grace_Principal_M=0,
            Amort_Type="annuity", Balloon_Pct=0.0, Revolving=0, Is_Insurance=0,
            Premium_Rate_Pct=0.0, Secured_By="None", Active=1
        )
        base.update(overrides)
        return base

    def test_revolver_window(self):
        """
        Horizon-aware check: Interest accrues only for months m in [Draw_Start_M .. Tenor_Months].
        Outside that window (including months beyond horizon), interest is 0.00.
        """
        # Arrange: Revolver 2,000 @ 12%, draw_start=7, tenor=36
        stack = pd.DataFrame([self._stack_base_row(
            Line_ID=10, Instrument="WC_Revolver",
            Principal=2000, Rate_Pct=12.0,
            Draw_Start_M=7, Tenor_Months=36,
            Revolving=1, Is_Insurance=0
        )])
        sel = pd.DataFrame([{"Key": "PFinance_Case", "Value": "CaseX"}])

        out = create_financing_schedules(stack, sel, self.calendar_df)
        rev = out["revolver"].loc[out["revolver"]["Line_ID"] == 10].copy()

        # Horizon & expected series
        horizon = int(self.calendar_df["Month_Index"].max())
        rate_m = 0.12 / 12.0
        util = 0.5 * 2000.0
        expected = []
        for m in range(1, horizon + 1):
            exp = util * rate_m if (7 <= m <= 36) else 0.0
            expected.append(round(exp, 2))

        # Actual (aligned to months present only)
        actual = rev.set_index("Month_Index")["Interest_Accrued"].reindex(
            range(1, horizon + 1), fill_value=0.0
        ).round(2).tolist()

        self.assertEqual(actual, expected)

    @unittest.skip("Insurance temporarily disabled in v1 per brief; schedule is zeros.")
    def test_insurance_cash_expense_prepaid(self):
        """
        Insurance OFF (v1 stub): all zero cash/expense/prepaid across the full horizon.
        Schema and timing preserved; values are zeros by design.
        """
        cal = pd.DataFrame({"Month_Index": range(1, 61)})
        sel = pd.DataFrame([{"Key": "PFinance_Case", "Value": "CaseX"}])

        stack = pd.DataFrame([dict(
            Case_Name="CaseX", Line_ID=20, Instrument="Parametric_DroughtCover", Currency="NAD",
            Principal=10_000, Rate_Pct=0.0, Tenor_Months=36,
            Draw_Start_M=1, Draw_End_M=1, Grace_Int_M=0, Grace_Principal_M=0,
            Amort_Type="bullet", Balloon_Pct=0.0, Revolving=0, Is_Insurance=1,
            Premium_Rate_Pct=2.0, Secured_By="None", Active=1
        )])

        out = create_financing_schedules(stack, sel, cal)
        df = out["insurance"].loc[out["insurance"]["Line_ID"] == 20].copy()

        expected_cols = {
            "Month_Index", "Line_ID", "Currency",
            "Premium_Cash_Outflow", "Expense_Recognized", "Prepaid_BOP", "Prepaid_EOP"
        }
        assert set(df.columns) >= expected_cols
        assert df.shape[0] == 60  # full horizon rows for this single line

        # All zeros by design (Insurance OFF v1)
        assert float(df["Premium_Cash_Outflow"].sum()) == 0.0
        assert float(df["Expense_Recognized"].sum()) == 0.0
        assert float(df["Prepaid_BOP"].sum()) == 0.0
        assert float(df["Prepaid_EOP"].sum()) == 0.0
