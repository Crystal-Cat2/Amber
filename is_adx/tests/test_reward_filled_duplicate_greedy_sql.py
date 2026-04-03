"""Reward filled 贪心重复标记 SQL 回归测试。"""

from __future__ import annotations

from pathlib import Path
import unittest


REPO_ROOT = Path(__file__).resolve().parents[1]
SUMMARY_SQL_PATH = REPO_ROOT / "outputs" / "reward_filled_duplicate_greedy_summary.sql"
AUDIT_SQL_PATH = REPO_ROOT / "outputs" / "reward_filled_duplicate_greedy_audit.sql"


class RewardFilledDuplicateGreedySqlTests(unittest.TestCase):
    """校验 reward filled 按锚点 50ms 窗口做贪心重复标记的关键口径。"""

    def test_summary_sql_exists_and_uses_greedy_filled_window_logic(self) -> None:
        """汇总 SQL 应只基于 filled 事件，并按锚点窗口贪心标记 duplicate。"""
        self.assertTrue(SUMMARY_SQL_PATH.exists())

        sql_text = SUMMARY_SQL_PATH.read_text(encoding="utf-8")

        self.assertIn("adslog_filled", sql_text)
        self.assertIn("rewarded", sql_text)
        self.assertIn("user_pseudo_id", sql_text)
        self.assertIn("max_unit_id", sql_text)
        self.assertIn("CREATE TEMP FUNCTION", sql_text)
        self.assertIn("mark_greedy_filled_events", sql_text)
        self.assertIn("ordered_filled_events", sql_text)
        self.assertIn("greedy_marked_filled_events", sql_text)
        self.assertIn("anchor_filled_ts", sql_text)
        self.assertIn("is_duplicate", sql_text)
        self.assertIn("duplicate_filled_event_cnt", sql_text)
        self.assertIn("anchor_filled_event_cnt", sql_text)
        self.assertIn("50000", sql_text)
        self.assertNotIn("adslog_request", sql_text)
        self.assertNotIn("adslog_load_latency", sql_text)
        self.assertNotIn("user_key", sql_text)
        self.assertNotIn("overall", sql_text)

    def test_audit_sql_exists_and_contains_anchor_assignment_fields(self) -> None:
        """审计 SQL 应输出当前 filled 及其所属锚点信息。"""
        self.assertTrue(AUDIT_SQL_PATH.exists())

        sql_text = AUDIT_SQL_PATH.read_text(encoding="utf-8")

        self.assertIn("current_request_id", sql_text)
        self.assertIn("current_filled_ts", sql_text)
        self.assertIn("anchor_request_id", sql_text)
        self.assertIn("anchor_filled_ts", sql_text)
        self.assertIn("diff_ms", sql_text)
        self.assertIn("is_duplicate", sql_text)
        self.assertNotIn("adslog_load_latency", sql_text)


if __name__ == "__main__":
    unittest.main()
