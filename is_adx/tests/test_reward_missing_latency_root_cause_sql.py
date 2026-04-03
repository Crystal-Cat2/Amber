"""Reward request 缺失 latency 原因归因 SQL 回归测试。"""

from __future__ import annotations

from pathlib import Path
import unittest


REPO_ROOT = Path(__file__).resolve().parents[1]
SQL_PATH = REPO_ROOT / "outputs" / "reward_missing_latency_root_cause_summary.sql"


class RewardMissingLatencyRootCauseSqlTests(unittest.TestCase):
    """校验纯 Hudi reward 缺失 latency 归因 SQL 的关键口径。"""

    def test_sql_exists_and_uses_pure_hudi_root_cause_logic(self) -> None:
        """SQL 应只基于 Hudi 事件，并显式包含 duplicate -> error -> unknown 优先级。"""
        self.assertTrue(SQL_PATH.exists())

        sql_text = SQL_PATH.read_text(encoding="utf-8")

        self.assertIn("adslog_request", sql_text)
        self.assertIn("adslog_filled", sql_text)
        self.assertIn("adslog_load_latency", sql_text)
        self.assertIn("adslog_error", sql_text)
        self.assertIn("rewarded", sql_text)
        self.assertIn("LAG(f.filled_request_id)", sql_text)
        self.assertIn("LAG(f.filled_ts)", sql_text)
        self.assertIn("TIMESTAMP_DIFF", sql_text)
        self.assertIn("<= 50", sql_text)
        self.assertIn("current_has_filled", sql_text)
        self.assertIn("prev_filled_has_latency", sql_text)
        self.assertIn("duplicate_retained_first", sql_text)
        self.assertIn("adslog_error", sql_text)
        self.assertIn("unknown", sql_text)
        self.assertIn("missing_latency_ratio", sql_text)
        self.assertNotIn("commercial-adx.lmh.isadx_adslog_latency_detail", sql_text)


if __name__ == "__main__":
    unittest.main()
