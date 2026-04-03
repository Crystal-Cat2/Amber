"""整体日级比例看板回归测试。"""

from __future__ import annotations

import importlib.util
from pathlib import Path
import unittest


REPO_ROOT = Path(__file__).resolve().parents[1]
PROJECT_DIR = REPO_ROOT / "projects" / "reach_rate_analysis"
SCRIPT_PATH = PROJECT_DIR / "scripts" / "build_overall_daily_ratio_dashboard.py"
SQL_TEMPLATE_PATH = PROJECT_DIR / "sql" / "overall_daily_ratio_dashboard.sql"
CHANNEL_SQL_TEMPLATE_PATH = PROJECT_DIR / "sql" / "overall_daily_ratio_channel_dashboard.sql"

spec = importlib.util.spec_from_file_location("build_overall_daily_ratio_dashboard", SCRIPT_PATH)
dashboard = importlib.util.module_from_spec(spec)
assert spec is not None and spec.loader is not None
spec.loader.exec_module(dashboard)


class OverallDailyRatioDashboardTests(unittest.TestCase):
    """校验旧整体页的 SQL 和 HTML 输出骨架。"""

    def test_script_file_exists(self) -> None:
        self.assertTrue(SCRIPT_PATH.exists())
        self.assertTrue(SQL_TEMPLATE_PATH.exists())
        self.assertTrue(CHANNEL_SQL_TEMPLATE_PATH.exists())

    def test_sql_contains_hudi_and_max_sources(self) -> None:
        sql_text = dashboard.build_sql("2025-08-01", "2026-03-28")

        self.assertIn("FROM `transferred.hudi_ods.screw_puzzle`", sql_text)
        self.assertIn("FROM `transferred.hudi_ods.ios_screw_puzzle`", sql_text)
        self.assertIn("FROM `gpdata-224001.applovin_max.screw_puzzle_*`", sql_text)
        self.assertIn("FROM `gpdata-224001.applovin_max.ios_screw_puzzle_*`", sql_text)
        self.assertIn("event_date BETWEEN '2025-08-01' AND '2026-03-28'", sql_text)
        self.assertIn("DATE(max_rows.`Date`, 'UTC')", sql_text)
        self.assertIn("max_impression_pv", sql_text)
        self.assertIn("hudi_max_rate", sql_text)
        self.assertNotIn("experiment_group", sql_text)
        self.assertNotIn("network_name", sql_text)
        self.assertNotIn("{{ start_date }}", sql_text)
        self.assertNotIn("{{ table_suffix_start }}", sql_text)

    def test_channel_sql_contains_mapping_rules(self) -> None:
        sql_text = dashboard.build_channel_sql("2025-08-01", "2026-03-28")

        self.assertIn("AppLovin", sql_text)
        self.assertIn("APPLOVIN_NETWORK", sql_text)
        self.assertIn("Pangle", sql_text)
        self.assertIn("TIKTOK_BIDDING", sql_text)
        self.assertIn("Unity Ads", sql_text)
        self.assertIn("UNITY_BIDDING", sql_text)
        self.assertIn("Chartboost", sql_text)
        self.assertIn("CHARTBOOST_BIDDING", sql_text)
        self.assertIn("CHARTBOOST_NETWORK", sql_text)
        self.assertIn("Google AdMob", sql_text)
        self.assertIn("ADMOB_BIDDING", sql_text)
        self.assertIn("ADMOB_NETWORK", sql_text)
        self.assertIn("Liftoff Monetize", sql_text)
        self.assertIn("VUNGLE_BIDDING", sql_text)
        self.assertIn("DT Exchange", sql_text)
        self.assertIn("FYBER_BIDDING", sql_text)
        self.assertIn("IsAdxCustomAdapter", sql_text)
        self.assertIn("Liftoff_custom", sql_text)
        self.assertIn("TpAdxCustomAdapter", sql_text)
        self.assertIn("MaticooCustomAdapter", sql_text)
        self.assertIn("CUSTOM_NETWORK_SDK", sql_text)
        self.assertIn("UNMAPPED", sql_text)
        self.assertNotIn("{{ start_date }}", sql_text)
        self.assertNotIn("{{ table_suffix_start }}", sql_text)
        self.assertNotIn("THEN max_rows.Network", sql_text)
        self.assertNotIn(
            "THEN (SELECT value.string_value FROM UNNEST(event_params.array) WHERE key = 'network_name')",
            sql_text,
        )

    def test_build_channel_summary_rows_combines_channels(self) -> None:
        rows = [
            {
                "product": "screw_puzzle",
                "event_date": "2025-08-01",
                "ad_format": "interstitial",
                "mapped_channel": "Google AdMob",
                "hudi_impression_pv": 10,
                "max_impression_pv": 6,
                "hudi_max_rate": 10 / 6,
            },
            {
                "product": "screw_puzzle",
                "event_date": "2025-08-02",
                "ad_format": "interstitial",
                "mapped_channel": "Google AdMob",
                "hudi_impression_pv": 20,
                "max_impression_pv": 24,
                "hudi_max_rate": 20 / 24,
            },
        ]

        summary_rows = dashboard.build_channel_summary_rows(rows)
        self.assertEqual(1, len(summary_rows))
        self.assertEqual("Google AdMob", summary_rows[0]["mapped_channel"])
        self.assertEqual(30, summary_rows[0]["hudi_impression_pv"])
        self.assertEqual(30, summary_rows[0]["max_impression_pv"])
        self.assertEqual(1.0, summary_rows[0]["hudi_max_rate"])
        self.assertEqual(0, summary_rows[0]["impression_delta_pv"])

    def test_build_html_contains_third_ratio_line(self) -> None:
        html = dashboard.build_html(
            overall_rows=[
                {
                    "product": "screw_puzzle",
                    "event_date": "2025-08-01",
                    "ad_format": "interstitial",
                    "show_pv": 100,
                    "impression_pv": 95,
                    "display_failed_pv": 3,
                    "max_impression_pv": 97,
                    "impression_show_rate": 0.95,
                    "impression_plus_failed_show_rate": 0.98,
                    "hudi_max_rate": 95 / 97,
                }
            ],
            channel_daily_rows=[
                {
                    "product": "screw_puzzle",
                    "event_date": "2025-08-01",
                    "ad_format": "interstitial",
                    "mapped_channel": "Google AdMob",
                    "hudi_impression_pv": 95,
                    "max_impression_pv": 97,
                    "hudi_max_rate": 95 / 97,
                }
            ],
            start_date="2025-08-01",
            end_date="2026-03-28",
        )

        self.assertIn("2025-08-01", html)
        self.assertIn("2026-03-28", html)
        self.assertIn("android-interstitial-chart", html)
        self.assertIn("ios-rewarded-chart", html)
        self.assertIn("../../ab_dashboard/assets/echarts.min.js", html)
        self.assertIn("hudi/max", html)
        self.assertIn("max_impression_pv", html)
        self.assertIn("channel-selector", html)
        self.assertIn("渠道汇总", html)
        self.assertIn("渠道映射说明", html)
        self.assertIn("Google AdMob", html)
        self.assertIn("mapped_channel", html)
        self.assertIn("toFixed(1)", html)
        self.assertIn("max: 1", html)
        self.assertNotIn("markArea", html)


if __name__ == "__main__":
    unittest.main()
