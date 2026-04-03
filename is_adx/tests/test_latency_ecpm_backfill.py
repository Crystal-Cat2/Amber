"""Latency ecpm 回填与 filled 匹配校验回归测试。"""

from __future__ import annotations

import json
from pathlib import Path
import unittest


REPO_ROOT = Path(__file__).resolve().parents[1]
PROJECT_DIR = REPO_ROOT / "projects" / "latency_analysis"
SQL_DIR = PROJECT_DIR / "sql"
SCRIPT_DIR = PROJECT_DIR / "scripts"
OUTPUT_DIR = PROJECT_DIR / "outputs"
BACKFILL_SQL_PATH = REPO_ROOT / "outputs" / "backfill_ecpm_to_latency_detail.sql"
VALIDATION_SQL_PATH = SQL_DIR / "loaded_latency_filled_match_validation.sql"
SCRIPT_PATH = SCRIPT_DIR / "run_loaded_latency_filled_match_validation.py"


def load_runner_module():
    """按需加载本地 runner，避免文件缺失时中断测试发现。"""
    import importlib.util

    spec = importlib.util.spec_from_file_location("run_loaded_latency_filled_match_validation", SCRIPT_PATH)
    module = importlib.util.module_from_spec(spec)
    assert spec is not None and spec.loader is not None
    spec.loader.exec_module(module)
    return module


class LatencyEcpmBackfillTests(unittest.TestCase):
    """校验 ecpm 回填 SQL、验证 SQL 与本地 runner 的关键行为。"""

    def test_backfill_sql_exists_and_limits_updates_to_loaded_latency_rows(self) -> None:
        """回填 SQL 应新增 ecpm 列，并只给 AD_LOADED 的 latency 行写值。"""
        self.assertTrue(BACKFILL_SQL_PATH.exists())

        sql_text = BACKFILL_SQL_PATH.read_text(encoding="utf-8")

        self.assertIn("ADD COLUMN IF NOT EXISTS ecpm FLOAT64", sql_text)
        self.assertIn("event_name = 'adslog_filled'", sql_text)
        self.assertIn("filled_value", sql_text)
        self.assertIn("status = 'AD_LOADED'", sql_text)
        self.assertIn("latency.product = filled.product", sql_text)
        self.assertIn("latency.user_pseudo_id = filled.user_pseudo_id", sql_text)
        self.assertIn("latency.request_id = filled.request_id", sql_text)
        self.assertIn("'com.takeoffbolts.screw.puzzle' AS product", sql_text)
        self.assertIn("'ios.takeoffbolts.screw.puzzle' AS product", sql_text)
        self.assertNotIn("SET ecpm = NULL", sql_text)

    def test_validation_sql_exists_and_uses_loaded_request_as_denominator(self) -> None:
        """验证 SQL 的分母应是 loaded request，而不是全部 latency request。"""
        self.assertTrue(VALIDATION_SQL_PATH.exists())

        sql_text = VALIDATION_SQL_PATH.read_text(encoding="utf-8")

        self.assertIn("status = 'AD_LOADED'", sql_text)
        self.assertIn("loaded_latency_requests", sql_text)
        self.assertIn("matched_filled_request_cnt", sql_text)
        self.assertIn("unmatched_request_cnt", sql_text)
        self.assertIn("matched_ratio", sql_text)
        self.assertIn("non_null_filled_value_ratio", sql_text)
        self.assertIn("product = filled.product", sql_text)
        self.assertIn("user_pseudo_id = filled.user_pseudo_id", sql_text)
        self.assertIn("request_id = filled.request_id", sql_text)
        self.assertNotIn("AD_LOAD_NOT_ATTEMPTED", sql_text)

    def test_runner_writes_summary_json_and_flags_ratio_below_expectation(self) -> None:
        """本地 runner 应落盘 JSON 摘要，并标记低于 99% 的结果。"""
        self.assertTrue(SCRIPT_PATH.exists())

        runner = load_runner_module()
        rows = [
            {
                "product": "screw_puzzle",
                "ad_format": "rewarded",
                "loaded_request_cnt": 100,
                "matched_filled_request_cnt": 98,
                "unmatched_request_cnt": 2,
                "matched_ratio": 0.98,
                "loaded_row_cnt": 120,
                "loaded_row_with_ecpm_source_cnt": 118,
                "non_null_filled_value_ratio": 0.9833,
            },
            {
                "product": "ios.takeoffbolts.screw.puzzle",
                "ad_format": "interstitial",
                "loaded_request_cnt": 100,
                "matched_filled_request_cnt": 100,
                "unmatched_request_cnt": 0,
                "matched_ratio": 1.0,
                "loaded_row_cnt": 110,
                "loaded_row_with_ecpm_source_cnt": 110,
                "non_null_filled_value_ratio": 1.0,
            },
        ]
        audit_rows = [
            {
                "duplicate_filled_key_cnt": 3,
                "duplicate_filled_value_conflict_cnt": 1,
            }
        ]

        output_dir = REPO_ROOT / "tmp_test" / "latency_ecpm_runner_test"
        output_dir.mkdir(parents=True, exist_ok=True)
        summary_path = output_dir / "summary.json"
        if summary_path.exists():
            summary_path.unlink()

        summary = runner.build_summary(rows, audit_rows)
        runner.write_summary_json(summary_path, summary)

        saved = json.loads(summary_path.read_text(encoding="utf-8"))

        self.assertEqual(saved["row_count"], 2)
        self.assertTrue(saved["below_expectation"])
        self.assertEqual(saved["threshold"], 0.99)
        self.assertEqual(saved["below_expectation_rows"][0]["product"], "screw_puzzle")
        self.assertEqual(saved["audit"]["duplicate_filled_key_cnt"], 3)
        self.assertEqual(saved["audit"]["duplicate_filled_value_conflict_cnt"], 1)


if __name__ == "__main__":
    unittest.main()
