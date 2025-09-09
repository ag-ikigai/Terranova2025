import json
import os
import unittest
from pathlib import Path

class TestM7R1Smoke(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # project root = .../tests/smoke/../../
        cls.ROOT = Path(__file__).resolve().parents[2]
        cls.OUT = cls.ROOT / "outputs"

        cls.scores_csv = cls.OUT / "m7_r1_scores.csv"
        cls.scores_parquet = cls.OUT / "m7_r1_scores.parquet"
        cls.debug_json = cls.OUT / "m7_r1_debug.json"
        cls.report_md = cls.OUT / "m7_r1_smoke_report.md"

    def test_artifacts_exist(self):
        # At least one of CSV/Parquet must exist; CSV is the fallback check
        self.assertTrue(
            self.scores_csv.exists() or self.scores_parquet.exists(),
            "M7.R1 scores file missing (csv or parquet).",
        )
        self.assertTrue(self.debug_json.exists(), "M7.R1 debug json missing.")
        self.assertTrue(self.report_md.exists(), "M7.R1 smoke report missing.")

    def test_scores_has_expected_columns_and_rows(self):
        import pandas as pd

        # Prefer CSV: avoids optional parquet engines in CI
        if self.scores_csv.exists():
            df = pd.read_csv(self.scores_csv)
        else:
            # Only if you’re sure pyarrow/fastparquet is available
            import pandas as pd
            df = pd.read_parquet(self.scores_parquet)

        required = {"Option", "Instrument", "Rank", "Total_Score_0_100", "Selected"}
        self.assertTrue(required.issubset(set(df.columns)),
                        f"Missing columns in scores: {required - set(df.columns)}")
        self.assertGreater(len(df), 0, "Scores dataframe is empty.")
        self.assertTrue(df["Rank"].min() == 1, "Top option Rank should start at 1.")
        self.assertTrue(df["Rank"].is_monotonic_increasing,
                        "Scores not sorted by Rank ascending.")

    def test_one_option_is_selected(self):
        import pandas as pd

        df = pd.read_csv(self.scores_csv) if self.scores_csv.exists() else None
        if df is None:
            import pandas as pd
            df = pd.read_parquet(self.scores_parquet)

        selected = df["Selected"].astype(str).str.lower().eq("yes").sum()
        self.assertEqual(selected, 1, f"Expected exactly one Selected=='yes', got {selected}.")

    def test_debug_json_has_weights_and_choice(self):
        dbg = json.loads(self.debug_json.read_text(encoding="utf-8"))
        self.assertIn("weights", dbg, "Debug JSON: missing 'weights'.")
        self.assertIn("selected_option", dbg, "Debug JSON: missing 'selected_option'.")
        self.assertTrue(str(dbg["selected_option"]).strip(), "Selected option should be non-empty.")

if __name__ == "__main__":
    unittest.main()
