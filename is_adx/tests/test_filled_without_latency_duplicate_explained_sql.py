"""filled 无 latency 样本重复归因 SQL 回归测试。"""

from __future__ import annotations

from pathlib import Path
import unittest


REPO_ROOT = Path(__file__).resolve().parents[1]
SUMMARY_SQL_PATH = REPO_ROOT / "outputs" / "filled_without_latency_duplicate_explained_summary.sql"
AUDIT_SQL_PATH = REPO_ROOT / "outputs" / "filled_without_latency_duplicate_explained_audit.sql"


class FilledWithoutLatencyDuplicateExplainedSqlTests(unittest.TestCase):
    """校验 filled 无 latency 样本按前序 filled 归因的关键口径。"""

    def test_summary_sql_exists_and_uses_raw_filled_exists_logic(self) -> None:
        """汇总 SQL 应基于 raw filled 事件，用前序 50ms 窗口存在性做重复归因。"""
        self.assertTrue(SUMMARY_SQL_PATH.exists())

        sql_text = SUMMARY_SQL_PATH.read_text(encoding="utf-8")

        self.assertIn("adslog_filled", sql_text)
        self.assertIn("adslog_load_latency", sql_text)
        self.assertIn("rewarded", sql_text)
        self.assertIn("user_pseudo_id", sql_text)
        self.assertIn("max_unit_id", sql_text)
        self.assertIn("raw_reward_filled_events", sql_text)
        self.assertIn("filled_without_latency_events", sql_text)
        self.assertIn("duplicate_explained_event_cnt", sql_text)
        self.assertIn("remaining_unexplained_event_cnt", sql_text)
        self.assertIn("EXISTS", sql_text)
        self.assertIn("TIMESTAMP_DIFF", sql_text)
        self.assertIn("<= 50000", sql_text)
        self.assertNotIn("adslog_request", sql_text)
        self.assertNotIn("user_key", sql_text)
        self.assertNotIn("overall", sql_text)
        self.assertNotIn("duplicate_retained_first", sql_text)
        self.assertNotIn("commercial-adx.lmh.isadx_adslog_latency_detail", sql_text)

    def test_audit_sql_exists_and_contains_match_detail_fields(self) -> None:
        """审计 SQL 应输出当前 filled 与代表性前序 filled 的匹配明细。"""
        self.assertTrue(AUDIT_SQL_PATH.exists())

        sql_text = AUDIT_SQL_PATH.read_text(encoding="utf-8")

        self.assertNotIn("adslog_request", sql_text)
        self.assertNotIn("user_key", sql_text)
        self.assertIn("current_request_id", sql_text)
        self.assertIn("prev_request_id", sql_text)
        self.assertIn("current_filled_ts", sql_text)
        self.assertIn("prev_filled_ts", sql_text)
        self.assertIn("prev_latency_ts", sql_text)
        self.assertIn("diff_ms", sql_text)
        self.assertIn("max_unit_id", sql_text)


if __name__ == "__main__":
    unittest.main()
