"""display_failed 渠道分析回归测试。"""

from __future__ import annotations

import importlib.util
from pathlib import Path
import unittest


REPO_ROOT = Path(__file__).resolve().parents[1]
PROJECT_DIR = REPO_ROOT / "projects" / "reach_rate_analysis"
SCRIPT_PATH = PROJECT_DIR / "scripts" / "run_display_failed_channel_analysis.py"
EVENT_SQL_PATH = PROJECT_DIR / "sql" / "display_failed_channel_event_summary.sql"
REASON_SQL_PATH = PROJECT_DIR / "sql" / "display_failed_channel_reason_distribution.sql"

spec = importlib.util.spec_from_file_location("run_display_failed_channel_analysis", SCRIPT_PATH)
analysis = importlib.util.module_from_spec(spec)
assert spec is not None and spec.loader is not None
spec.loader.exec_module(analysis)


class DisplayFailedChannelAnalysisTests(unittest.TestCase):
    """校验 SQL 结构与中文摘要的核心行为。"""

    def test_project_files_exist(self) -> None:
        self.assertTrue(PROJECT_DIR.exists())
        self.assertTrue(SCRIPT_PATH.exists())
        self.assertTrue(EVENT_SQL_PATH.exists())
        self.assertTrue(REASON_SQL_PATH.exists())

    def test_event_sql_outputs_channel_funnel_with_trigger_show_impression(self) -> None:
        sql_text = EVENT_SQL_PATH.read_text(encoding="utf-8")

        self.assertIn("'screw_puzzle' AS product", sql_text)
        self.assertIn("'ios_screw_puzzle' AS product", sql_text)
        self.assertIn("lib_isx_group", sql_text)
        self.assertIn("user_id", sql_text)
        self.assertIn("MIN(event_timestamp) AS min_timestamp", sql_text)
        self.assertIn("MAX(event_timestamp) AS max_timestamp", sql_text)
        self.assertIn("e.event_timestamp >= u.min_timestamp", sql_text)
        self.assertIn("e.event_timestamp <= u.max_timestamp", sql_text)
        self.assertIn("event_date BETWEEN '2025-09-18' AND '2026-01-03'", sql_text)
        self.assertNotIn("DATE(TIMESTAMP_MICROS(e.event_timestamp), 'UTC') BETWEEN '2025-09-18' AND '2026-01-03'", sql_text)
        self.assertIn("trigger_pv", sql_text)
        self.assertIn("show_pv", sql_text)
        self.assertIn("impression_pv", sql_text)
        self.assertIn("display_failed_pv", sql_text)
        self.assertIn("display_failed_total_pv", sql_text)
        self.assertIn("show_rate", sql_text)
        self.assertIn("impression_rate", sql_text)
        self.assertIn("SAFE_DIVIDE(c.show_pv, c.trigger_pv)", sql_text)
        self.assertIn("SAFE_DIVIDE(c.impression_pv, c.show_pv)", sql_text)
        self.assertIn("interstitial_ad_trigger", sql_text)
        self.assertIn("reward_ad_trigger", sql_text)
        self.assertIn("reward_ad_display_faile", sql_text)
        self.assertNotIn("LEAD(", sql_text)

    def test_reason_sql_uses_err_msg_and_reason_share_in_network(self) -> None:
        sql_text = REASON_SQL_PATH.read_text(encoding="utf-8")

        self.assertIn("err_msg", sql_text)
        self.assertIn("error_massage", sql_text)
        self.assertIn("__NO_ERR_MSG__", sql_text)
        self.assertIn("display_failed_pv", sql_text)
        self.assertIn("reason_share_in_network", sql_text)
        self.assertIn("event_date BETWEEN '2025-09-18' AND '2026-01-03'", sql_text)
        self.assertNotIn("DATE(TIMESTAMP_MICROS(e.event_timestamp), 'UTC') BETWEEN '2025-09-18' AND '2026-01-03'", sql_text)
        self.assertIn("MAX(event_timestamp) AS max_timestamp", sql_text)
        self.assertNotIn("LEAD(", sql_text)

    def test_build_markdown_summary_mentions_channel_funnel_and_reason_columns(self) -> None:
        markdown = analysis.build_markdown_summary(
            event_rows=[
                {
                    "product": "screw_puzzle",
                    "experiment_group": "no_is_adx",
                    "ad_format": "interstitial",
                    "network_name": "Facebook",
                    "trigger_pv": 80,
                    "show_rate": 0.625,
                    "impression_rate": 0.9,
                    "display_failed_pv": 12,
                    "display_failed_share": 0.4,
                    "show_pv": 50,
                    "impression_pv": 45,
                    "display_failed_total_pv": 30,
                }
            ],
            reason_rows=[
                {
                    "product": "screw_puzzle",
                    "experiment_group": "no_is_adx",
                    "ad_format": "interstitial",
                    "network_name": "Facebook",
                    "failure_reason": "renderer error",
                    "display_failed_pv": 12,
                    "reason_pv": 8,
                    "reason_share_in_network": 0.6667,
                }
            ],
        )

        self.assertIn("display_failed 渠道分析说明", markdown)
        self.assertIn("trigger_pv", markdown)
        self.assertIn("show_pv", markdown)
        self.assertIn("impression_pv", markdown)
        self.assertIn("show_rate", markdown)
        self.assertIn("impression_rate", markdown)
        self.assertIn("Facebook", markdown)
        self.assertIn("renderer error", markdown)


if __name__ == "__main__":
    unittest.main()
