"""Reward request 全量事件分类 SQL 回归测试。"""

from __future__ import annotations

from pathlib import Path
import unittest


REPO_ROOT = Path(__file__).resolve().parents[1]
SQL_PATH = REPO_ROOT / "outputs" / "reward_request_event_latency_classification.sql"


class RewardRequestEventLatencyClassificationSqlTests(unittest.TestCase):
    """校验全量 reward request 一级/二级分类 SQL 的关键口径。"""

    def test_sql_exists_and_uses_primary_and_latency_classes(self) -> None:
        """SQL 应输出 filled/error/neither 与 with/without latency 组合桶。"""
        self.assertTrue(SQL_PATH.exists())

        sql_text = SQL_PATH.read_text(encoding="utf-8")

        self.assertIn("adslog_request", sql_text)
        self.assertIn("adslog_filled", sql_text)
        self.assertIn("adslog_error", sql_text)
        self.assertIn("adslog_load_latency", sql_text)
        self.assertIn("rewarded", sql_text)
        self.assertIn("primary_class", sql_text)
        self.assertIn("latency_class", sql_text)
        self.assertIn("'filled'", sql_text)
        self.assertIn("'error'", sql_text)
        self.assertIn("'neither'", sql_text)
        self.assertIn("'with_latency'", sql_text)
        self.assertIn("'without_latency'", sql_text)
        self.assertIn("ratio_in_total_request", sql_text)
        self.assertIn("ratio_in_primary_class", sql_text)
        self.assertNotIn("duplicate_retained_first", sql_text)
        self.assertNotIn("root_cause", sql_text)
        self.assertNotIn("commercial-adx.lmh.isadx_adslog_latency_detail", sql_text)


if __name__ == "__main__":
    unittest.main()
