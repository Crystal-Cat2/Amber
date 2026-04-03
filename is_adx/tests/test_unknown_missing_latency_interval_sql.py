"""Unknown 缺失 latency 前序间隔分布 SQL 回归测试。"""

from __future__ import annotations

from pathlib import Path
import unittest


REPO_ROOT = Path(__file__).resolve().parents[1]
SQL_PATH = REPO_ROOT / "outputs" / "unknown_missing_latency_prev_interval_distribution.sql"


class UnknownMissingLatencyIntervalSqlTests(unittest.TestCase):
    """校验 unknown 样本前序间隔分布 SQL 的关键口径。"""

    def test_sql_exists_and_uses_full_reward_sequence(self) -> None:
        """SQL 应基于全量 reward request 序列计算 unknown 样本的前序间隔。"""
        self.assertTrue(SQL_PATH.exists())

        sql_text = SQL_PATH.read_text(encoding="utf-8")

        self.assertIn("root_cause = 'unknown'", sql_text)
        self.assertIn("adslog_request", sql_text)
        self.assertIn("adslog_filled", sql_text)
        self.assertIn("adslog_load_latency", sql_text)
        self.assertIn("adslog_error", sql_text)
        self.assertIn("rewarded", sql_text)
        self.assertIn("LAG(f.filled_request_id)", sql_text)
        self.assertIn("LAG(f.filled_ts)", sql_text)
        self.assertIn("current_has_filled", sql_text)
        self.assertIn("prev_filled_has_latency", sql_text)
        self.assertIn("TIMESTAMP_DIFF", sql_text)
        self.assertIn("ROUND(diff_ms, 3)", sql_text)
        self.assertIn("0-10ms", sql_text)
        self.assertIn("10-20ms", sql_text)
        self.assertIn("20-50ms", sql_text)
        self.assertIn("50-100ms", sql_text)
        self.assertIn("100-500ms", sql_text)
        self.assertIn("500ms+", sql_text)
        self.assertNotIn("commercial-adx.lmh.isadx_adslog_latency_detail", sql_text)


if __name__ == "__main__":
    unittest.main()
