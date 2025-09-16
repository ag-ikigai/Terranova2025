# tests/smoke/test_m8a_smoke.py
from __future__ import annotations

import json
from pathlib import Path
import unittest

OUT = Path("./outputs")

class TestM8ASmoke(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.report_md = OUT / "m8a_super_verifier_report.md"
        cls.report_js = OUT / "m8a_super_verifier.json"
        assert cls.report_md.exists(), f"Missing: {cls.report_md}"
        assert cls.report_js.exists(), f"Missing: {cls.report_js}"
        cls.data = json.loads(cls.report_js.read_text(encoding="utf-8"))

    def test_artifacts_exist(self):
        self.assertTrue(self.report_md.exists())
        self.assertTrue(self.report_js.exists())

    def test_overall_status_present(self):
        self.assertIn("overall_status", self.data)
        self.assertIn(self.data["overall_status"], ["OK", "WARN", "FAIL"])

    def test_fx_metadata_present(self):
        self.assertIn("fx_used_avg_col", self.data)
        self.assertIn("fx_used_eom_col", self.data)

if __name__ == "__main__":
    unittest.main()
