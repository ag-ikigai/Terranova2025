
# --- path bootstrap (must stay at the top of the file) ---
from pathlib import Path
import sys
ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))
# ---------------------------------------------------------

import unittest
import pandas as pd
from terra_nova.modules.m3_financing.engine import (
    create_financing_schedules,
    create_loan_schedule,
)

class TestM3Full(unittest.TestCase):

    def setUp(self):
        # 60-month calendar
        self.calendar_df = pd.DataFrame({"Month_Index": list(range(1, 61))})

    # ----------------- helpers -----------------
    def _stack_base_row(self, **over):
        base = dict(
            Case_Name="CaseX",
            Line_ID=1,
            Instrument="Loan_A",
            Currency="NAD",
            Principal=12_000,
            Rate_Pct=12.0,
            Tenor_Months=12,
            Draw_Start_M=1,
            Draw_End_M=1,
            Grace_Int_M=0,
            Grace_Principal_M=0,
            Amort_Type="annuity",
            Balloon_Pct=0.0,
            Revolving=0,
            Is_Insurance=0,
            Premium_Rate_Pct=0.0,
            Secured_By="",
            Active=1,
        )
        base.update(over)
        return base

    # 1) Selector correctness
    def test_selector_correctness(self):
        rows = [
            self._stack_base_row(Case_Name="CaseX", Line_ID=1, Revolving=1, Instrument="WC_Revolver"),
            self._stack_base_row(Case_Name="CaseY", Line_ID=2, Revolving=1, Instrument="WC_Revolver"),
            self._stack_base_row(Case_Name="CaseX", Line_ID=3, Is_Insurance=1, Instrument="Parametric_DroughtCover"),
            self._stack_base_row(Case_Name="CaseX", Line_ID=4, Instrument="Loan_B", Revolving=0, Is_Insurance=0),
            self._stack_base_row(Case_Name="CaseX", Line_ID=5, Active=0, Instrument="Inactive_Loan"),
        ]
        stack = pd.DataFrame(rows)
        sel = pd.DataFrame([{"Key": "PFinance_Case", "Value": "CaseX"}])
        out = create_financing_schedules(stack, sel, self.calendar_df)
        idx = out["index"]
        self.assertTrue(set(idx["Line_ID"]) == {1, 3, 4})  # Active rows in CaseX
        self.assertTrue((out["revolver"]["Line_ID"] == 1).all() if not out["revolver"].empty else True)
        self.assertTrue((out["insurance"]["Line_ID"] == 3).all() if not out["insurance"].empty else True)

    # 2) Loan — annuity clears by tenor
    def test_loan_annuity_clears(self):
        row = pd.Series(self._stack_base_row(Amort_Type="annuity", Principal=12_000, Tenor_Months=12))
        sched = create_loan_schedule(row, self.calendar_df)
        cb = float(sched.loc[sched["Month_Index"] == 12, "Closing_Balance"].iloc[0])
        self.assertAlmostEqual(cb, 0.0, places=2)

    # 3) Loan — straight equal principal (inside window) and clears by tenor
    def test_loan_straight(self):
        row = pd.Series(self._stack_base_row(Amort_Type="straight", Principal=12_000, Draw_End_M=1, Tenor_Months=12, Grace_Principal_M=0))
        sched = create_loan_schedule(row, self.calendar_df)
        amort_start = int(row["Draw_End_M"]) + int(row["Grace_Principal_M"]) + 1
        first = float(sched.loc[sched["Month_Index"] == amort_start, "Principal_Repayment"].iloc[0])
        second = float(sched.loc[sched["Month_Index"] == amort_start + 1, "Principal_Repayment"].iloc[0])
        self.assertAlmostEqual(first, second, places=2)
        last_cb = float(sched.loc[sched["Month_Index"] == int(row["Tenor_Months"]), "Closing_Balance"].iloc[0])
        self.assertAlmostEqual(last_cb, 0.0, places=2)

    # 4) Loan — bullet repays at tenor
    def test_loan_bullet(self):
        row = pd.Series(self._stack_base_row(Amort_Type="bullet", Principal=12_000, Tenor_Months=12))
        sched = create_loan_schedule(row, self.calendar_df)
        pre = float(sched.loc[sched["Month_Index"] == 11, "Principal_Repayment"].iloc[0])
        fin = float(sched.loc[sched["Month_Index"] == 12, "Principal_Repayment"].iloc[0])
        self.assertAlmostEqual(pre, 0.0, places=2)
        self.assertGreater(fin, 0.0)

    # 5) Interest on opening only
    def test_interest_on_opening_only(self):
        row = pd.Series(self._stack_base_row(Amort_Type="annuity", Principal=12_000, Draw_End_M=1, Tenor_Months=12))
        sched = create_loan_schedule(row, self.calendar_df)
        # Month 1 opening is 0; draw happens in month 1; interest should be 0.00
        self.assertAlmostEqual(float(sched.loc[sched["Month_Index"] == 1, "Interest_Accrued"].iloc[0]), 0.00, places=2)

    # 6) Amort starts after draw + grace
    def test_amort_starts_after_draw_and_grace(self):
        row = pd.Series(self._stack_base_row(Amort_Type="straight", Principal=12_000, Draw_End_M=3, Grace_Principal_M=2, Tenor_Months=12))
        sched = create_loan_schedule(row, self.calendar_df)
        self.assertAlmostEqual(float(sched.loc[sched["Month_Index"] == 5, "Principal_Repayment"].iloc[0]), 0.0, places=2)
        self.assertGreater(float(sched.loc[sched["Month_Index"] == 6, "Principal_Repayment"].iloc[0]), 0.0)

    # 7) Revolver window
    def test_revolver_window(self):
        stack = pd.DataFrame([self._stack_base_row(Line_ID=10, Instrument="WC_Revolver",
                                                   Principal=2_000, Rate_Pct=12.0,
                                                   Tenor_Months=36, Draw_Start_M=7, Revolving=1)])
        sel = pd.DataFrame([{"Key": "PFinance_Case", "Value": "CaseX"}])
        out = create_financing_schedules(stack, sel, self.calendar_df)
        df = out["revolver"]
        # months 7..36: interest = 2000 * 0.5 * (0.12/12) = 10.00
        win = df[(df["Month_Index"] >= 7) & (df["Month_Index"] <= 36)]
        self.assertTrue((win["Interest_Accrued"].round(2) == 10.00).all())
        pre = df[df["Month_Index"] < 7]["Interest_Accrued"].sum()
        post = df[df["Month_Index"] > 36]["Interest_Accrued"].sum()
        self.assertAlmostEqual(pre + post, 0.0, places=2)

    # 8) Insurance logic — cash/expense/prepaid
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
    df = out["insurance"]

    # Schema
    expected_cols = {
        "Month_Index", "Line_ID", "Currency",
        "Premium_Cash_Outflow", "Expense_Recognized", "Prepaid_BOP", "Prepaid_EOP"
    }
    assert set(df.columns) >= expected_cols

    # Horizon coverage (60 rows for this single line)
    assert df.shape[0] == 60

    # All zeros by design
    assert float(df["Premium_Cash_Outflow"].sum()) == 0.0
    assert float(df["Expense_Recognized"].sum()) == 0.0
    assert float(df["Prepaid_BOP"].sum()) == 0.0
    assert float(df["Prepaid_EOP"].sum()) == 0.0

    def test_insurance_zero_principal(self):
        stack = pd.DataFrame([self._stack_base_row(Line_ID=30, Instrument="Parametric_DroughtCover",
                                                   Principal=0, Premium_Rate_Pct=2.0,
                                                   Tenor_Months=24, Revolving=0, Is_Insurance=1)])
        sel = pd.DataFrame([{"Key": "PFinance_Case", "Value": "CaseX"}])
        out = create_financing_schedules(stack, sel, self.calendar_df)
        df = out["insurance"]
        self.assertTrue((df["Premium_Cash_Outflow"].abs() < 1e-9).all())
        self.assertTrue((df["Expense_Recognized"].abs() < 1e-9).all())
        self.assertTrue((df["Prepaid_EOP"].abs() < 1e-9).all())

    # 10) Index integrity & determinism
    def test_index_integrity_and_determinism(self):
        rows = [
            self._stack_base_row(Line_ID=101, Revolving=1, Instrument="WC_Revolver"),
            self._stack_base_row(Line_ID=102, Is_Insurance=1, Instrument="Parametric_DroughtCover"),
            self._stack_base_row(Line_ID=103, Instrument="Loan_C", Revolving=0, Is_Insurance=0),
        ]
        stack = pd.DataFrame(rows)
        sel = pd.DataFrame([{"Key": "PFinance_Case", "Value": "CaseX"}])
        out1 = create_financing_schedules(stack, sel, self.calendar_df)
        out2 = create_financing_schedules(stack, sel, self.calendar_df)
        self.assertTrue(set(out1["index"]["Line_ID"]) == set(out2["index"]["Line_ID"]) == {101,102,103})
        self.assertEqual(len(out1["revolver"]), len(out2["revolver"]))
        self.assertEqual(len(out1["insurance"]), len(out2["insurance"]))


if __name__ == "__main__":
    unittest.main()
