import unittest, os, json, pandas as pd

OUT = os.path.abspath(os.environ.get("TN_OUT", r"C:\TerraNova\outputs"))

REQUIRED_MD = [
    "m0_smoke_report.md", "m1_smoke_report.md", "m2_smoke_report.md",
    "m3_smoke_report.md", "m4_smoke_report.md", "m5_smoke_report.md"
]
REQUIRED_PARQUET = [
    "m2_pl_schedule.parquet", "m2_working_capital_schedule.parquet",
    "m4_tax_schedule.parquet", "m4_tax_summary.parquet",
    "m5_cash_flow_statement_final.parquet"
]
OPTIONAL_JSON = ["orchestrator_debug_dump.json", "m5_debug_dump.json"]

def _exists(path): return os.path.exists(os.path.join(OUT, path))

class TestArtifactsExist(unittest.TestCase):
    def test_md_reports_exist(self):
        missing = [p for p in REQUIRED_MD if not _exists(p)]
        self.assertFalse(missing, f"Missing smoke reports: {missing}")

    def test_parquet_exist_and_nonempty(self):
        missing = [p for p in REQUIRED_PARQUET if not _exists(p)]
        self.assertFalse(missing, f"Missing parquet artifacts: {missing}")
        for p in REQUIRED_PARQUET:
            df = pd.read_parquet(os.path.join(OUT, p))
            self.assertGreater(len(df), 0, f"{p} has no rows")

    def test_optional_debug_json_if_present(self):
        for p in OPTIONAL_JSON:
            full = os.path.join(OUT, p)
            if os.path.exists(full):
                with open(full, "r", encoding="utf-8") as fh:
                    json.load(fh)  # must be valid json

if __name__ == "__main__":
    unittest.main(verbosity=2)
