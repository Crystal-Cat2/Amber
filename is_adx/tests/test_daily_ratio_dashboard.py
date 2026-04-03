"""AB 日级比例看板回归测试。"""

from __future__ import annotations

import importlib.util
from pathlib import Path
import unittest


REPO_ROOT = Path(__file__).resolve().parents[1]
PROJECT_DIR = REPO_ROOT / "projects" / "reach_rate_analysis"
SCRIPT_PATH = PROJECT_DIR / "scripts" / "build_daily_ratio_dashboard.py"
SQL_DIR = PROJECT_DIR / "sql"

spec = importlib.util.spec_from_file_location("build_daily_ratio_dashboard", SCRIPT_PATH)
dashboard = importlib.util.module_from_spec(spec)
assert spec is not None and spec.loader is not None
spec.loader.exec_module(dashboard)


class DailyRatioDashboardTests(unittest.TestCase):
    """校验 AB 日级看板的 SQL 与 HTML 输出骨架。"""

    def test_script_file_exists(self) -> None:
        self.assertTrue(PROJECT_DIR.exists())
        self.assertTrue(SCRIPT_PATH.exists())
        self.assertTrue((SQL_DIR / "ab_daily_ratio_overall.sql").exists())
        self.assertTrue((SQL_DIR / "ab_daily_ratio_channel_daily.sql").exists())
        self.assertTrue((SQL_DIR / "ab_daily_ratio_error_daily.sql").exists())

    def test_overall_sql_uses_same_day_min_timestamp_logic_and_external_template(self) -> None:
        sql_text = dashboard.build_overall_daily_sql("2025-09-18", "2026-03-28")

        self.assertIn("FROM `transferred.hudi_ods.screw_puzzle`", sql_text)
        self.assertIn("FROM `transferred.hudi_ods.ios_screw_puzzle`", sql_text)
        self.assertIn("FROM `gpdata-224001.applovin_max.screw_puzzle_*`", sql_text)
        self.assertIn("FROM `gpdata-224001.applovin_max.ios_screw_puzzle_*`", sql_text)
        self.assertIn("event_date BETWEEN '2025-09-18' AND '2026-03-28'", sql_text)
        self.assertIn("DATE(TIMESTAMP_MICROS(e.event_timestamp), 'UTC') = u.event_date", sql_text)
        self.assertIn("e.event_timestamp >= u.min_timestamp", sql_text)
        self.assertNotIn("e.event_timestamp <= u.max_timestamp", sql_text)
        self.assertNotIn("LEAD(", sql_text)
        self.assertIn("experiment_group", sql_text)
        self.assertIn("display_failed_pv", sql_text)
        self.assertIn("impression_plus_failed_show_rate", sql_text)
        self.assertIn("max_impression_pv", sql_text)
        self.assertIn("hudi_max_rate", sql_text)
        self.assertNotIn("{{ start_date }}", sql_text)
        self.assertNotIn("{{ end_date }}", sql_text)
        self.assertNotIn("{{ table_suffix_start }}", sql_text)
        self.assertNotIn("{{ table_suffix_end }}", sql_text)

    def test_channel_and_error_sqls_cover_required_dimensions(self) -> None:
        channel_sql = dashboard.build_channel_daily_sql("2025-09-18", "2026-03-28")
        error_sql = dashboard.build_error_daily_sql("2025-09-18", "2026-03-28")

        self.assertIn("network_name", channel_sql)
        self.assertIn("event_date", channel_sql)
        self.assertIn("experiment_group", channel_sql)
        self.assertIn("display_failed_pv", channel_sql)
        self.assertIn("failure_reason", error_sql)
        self.assertIn("reason_pv", error_sql)
        self.assertIn("DATE(TIMESTAMP_MICROS(e.event_timestamp), 'UTC') = u.event_date", error_sql)
        self.assertNotIn("e.event_timestamp <= u.max_timestamp", error_sql)
        self.assertIn("err_msg", error_sql)
        self.assertIn("error_massage", error_sql)
        self.assertNotIn("{{ start_date }}", channel_sql)
        self.assertNotIn("{{ end_date }}", error_sql)

    def test_build_payload_prefers_real_error_reason_and_humanizes_missing_reason(self) -> None:
        payload = dashboard.build_payload(
            overall_rows=[
                {
                    "product": "screw_puzzle",
                    "event_date": "2025-09-18",
                    "ad_format": "interstitial",
                    "experiment_group": "no_is_adx",
                    "show_pv": 100,
                    "impression_pv": 95,
                    "display_failed_pv": 3,
                    "max_impression_pv": 97,
                    "impression_show_rate": 0.95,
                    "impression_plus_failed_show_rate": 0.98,
                    "hudi_max_rate": 95 / 97,
                },
                {
                    "product": "screw_puzzle",
                    "event_date": "2025-09-18",
                    "ad_format": "interstitial",
                    "experiment_group": "have_is_adx",
                    "show_pv": 120,
                    "impression_pv": 110,
                    "display_failed_pv": 5,
                    "max_impression_pv": 118,
                    "impression_show_rate": 0.9167,
                    "impression_plus_failed_show_rate": 0.9583,
                    "hudi_max_rate": 110 / 118,
                },
            ],
            channel_daily_rows=[
                {
                    "product": "screw_puzzle",
                    "event_date": "2025-09-18",
                    "ad_format": "interstitial",
                    "experiment_group": "no_is_adx",
                    "network_name": "Unity Ads",
                    "show_pv": 40,
                    "impression_pv": 38,
                    "display_failed_pv": 4,
                    "impression_show_rate": 0.95,
                    "impression_plus_failed_show_rate": 1.0,
                },
                {
                    "product": "screw_puzzle",
                    "event_date": "2025-09-18",
                    "ad_format": "interstitial",
                    "experiment_group": "have_is_adx",
                    "network_name": "Unity Ads",
                    "show_pv": 48,
                    "impression_pv": 44,
                    "display_failed_pv": 4,
                    "impression_show_rate": 0.9167,
                    "impression_plus_failed_show_rate": 1.0,
                },
            ],
            error_rows=[
                {
                    "product": "screw_puzzle",
                    "event_date": "2025-09-18",
                    "ad_format": "interstitial",
                    "experiment_group": "no_is_adx",
                    "network_name": "Unity Ads",
                    "failure_reason": "__NO_ERR_MSG__",
                    "reason_pv": 5,
                },
                {
                    "product": "screw_puzzle",
                    "event_date": "2025-09-18",
                    "ad_format": "interstitial",
                    "experiment_group": "no_is_adx",
                    "network_name": "Unity Ads",
                    "failure_reason": "Video failed to start",
                    "reason_pv": 3,
                },
            ],
            start_date="2025-09-18",
            end_date="2026-03-28",
        )

        section = payload["sections"][0]
        overall_a = section["overall"]["no_is_adx"][0]
        overall_b = section["overall"]["have_is_adx"][0]
        channel_a = section["channel_daily"]["Unity Ads"]["no_is_adx"][0]
        channel_b = section["channel_daily"]["Unity Ads"]["have_is_adx"][0]
        error_summary = section["error_summary"]["Unity Ads"][0]
        error_daily_a = section["error_daily"]["Unity Ads"]["Video failed to start"]["no_is_adx"][0]
        error_daily_b = section["error_daily"]["Unity Ads"]["Video failed to start"]["have_is_adx"][0]

        self.assertEqual(section["default_error_by_channel"]["Unity Ads"], "Video failed to start")
        self.assertIn("未上报错误信息", section["error_options_by_channel"]["Unity Ads"])
        self.assertNotIn("__NO_ERR_MSG__", section["error_options_by_channel"]["Unity Ads"])
        self.assertAlmostEqual(overall_a["impression_show_gap_vs_a_pp"], -3.33, places=2)
        self.assertAlmostEqual(overall_b["impression_show_gap_vs_a_pp"], -3.33, places=2)
        self.assertAlmostEqual(overall_a["impression_plus_failed_gap_vs_a_pp"], -2.17, places=2)
        self.assertAlmostEqual(overall_b["impression_plus_failed_gap_vs_a_pp"], -2.17, places=2)
        self.assertAlmostEqual(overall_a["hudi_max_gap_vs_a_pp"], -4.72, places=2)
        self.assertAlmostEqual(overall_b["hudi_max_gap_vs_a_pp"], -4.72, places=2)
        self.assertAlmostEqual(channel_a["impression_show_gap_vs_a_pp"], -3.33, places=2)
        self.assertAlmostEqual(channel_b["impression_show_gap_vs_a_pp"], -3.33, places=2)
        self.assertAlmostEqual(channel_a["impression_plus_failed_gap_vs_a_pp"], 0.0, places=2)
        self.assertAlmostEqual(channel_b["impression_plus_failed_gap_vs_a_pp"], 0.0, places=2)
        self.assertEqual(error_summary["no_is_adx"]["reason_pv"], 3)
        self.assertEqual(error_summary["have_is_adx"]["reason_pv"], 0)
        self.assertAlmostEqual(error_summary["no_is_adx"]["share_in_network"], 3 / 8, places=6)
        self.assertAlmostEqual(error_summary["have_is_adx"]["share_in_network"], 0.0, places=6)
        self.assertAlmostEqual(error_summary["gap_vs_a_pp"], -37.5, places=2)
        self.assertAlmostEqual(error_daily_a["share_in_group"], 3 / 8, places=6)
        self.assertAlmostEqual(error_daily_b["share_in_group"], 0.0, places=6)
        self.assertAlmostEqual(error_daily_a["gap_vs_a_pp"], -37.5, places=2)
        self.assertAlmostEqual(error_daily_b["gap_vs_a_pp"], -37.5, places=2)

    def test_build_html_contains_combined_rate_chart_and_error_sections(self) -> None:
        html = dashboard.build_html(
            overall_rows=[
                {
                    "product": "screw_puzzle",
                    "event_date": "2025-09-18",
                    "ad_format": "interstitial",
                    "experiment_group": "no_is_adx",
                    "show_pv": 100,
                    "impression_pv": 95,
                    "display_failed_pv": 3,
                    "max_impression_pv": 97,
                    "impression_show_rate": 0.95,
                    "impression_plus_failed_show_rate": 0.98,
                    "hudi_max_rate": 95 / 97,
                },
                {
                    "product": "screw_puzzle",
                    "event_date": "2025-09-18",
                    "ad_format": "interstitial",
                    "experiment_group": "have_is_adx",
                    "show_pv": 120,
                    "impression_pv": 110,
                    "display_failed_pv": 5,
                    "max_impression_pv": 118,
                    "impression_show_rate": 0.9167,
                    "impression_plus_failed_show_rate": 0.9583,
                    "hudi_max_rate": 110 / 118,
                },
            ],
            channel_daily_rows=[
                {
                    "product": "screw_puzzle",
                    "event_date": "2025-09-18",
                    "ad_format": "interstitial",
                    "experiment_group": "no_is_adx",
                    "network_name": "Unity Ads",
                    "show_pv": 40,
                    "impression_pv": 38,
                    "display_failed_pv": 1,
                    "impression_show_rate": 0.95,
                    "impression_plus_failed_show_rate": 0.975,
                },
                {
                    "product": "screw_puzzle",
                    "event_date": "2025-09-18",
                    "ad_format": "interstitial",
                    "experiment_group": "have_is_adx",
                    "network_name": "Unity Ads",
                    "show_pv": 48,
                    "impression_pv": 44,
                    "display_failed_pv": 2,
                    "impression_show_rate": 0.9167,
                    "impression_plus_failed_show_rate": 0.9583,
                },
            ],
            error_rows=[
                {
                    "product": "screw_puzzle",
                    "event_date": "2025-09-18",
                    "ad_format": "interstitial",
                    "experiment_group": "no_is_adx",
                    "network_name": "Unity Ads",
                    "failure_reason": "Video failed to start",
                    "reason_pv": 1,
                },
                {
                    "product": "screw_puzzle",
                    "event_date": "2025-09-18",
                    "ad_format": "interstitial",
                    "experiment_group": "have_is_adx",
                    "network_name": "Unity Ads",
                    "failure_reason": "Video failed to start",
                    "reason_pv": 2,
                },
            ],
            start_date="2025-09-18",
            end_date="2026-03-28",
        )

        self.assertIn("2025-09-18", html)
        self.assertIn("2026-03-28", html)
        self.assertIn("android-interstitial-chart", html)
        self.assertIn("ios-interstitial-chart", html)
        self.assertIn("ios-rewarded-chart", html)
        self.assertIn("channel-selector", html)
        self.assertIn("error-selector", html)
        self.assertIn("A 组实线，B 组虚线", html)
        self.assertNotIn("左 A 右 B", html)
        self.assertIn("../../ab_dashboard/assets/echarts.min.js", html)
        self.assertIn("toFixed(1)", html)
        self.assertIn("max: 1", html)
        self.assertNotIn("markArea", html)
        self.assertIn("Video failed to start", html)
        self.assertIn("hudi/max", html)
        self.assertIn("lineStyle", html)
        self.assertIn("dashed", html)
        self.assertIn("error-summary-chart", html)
        self.assertIn("error-daily-chart", html)
        self.assertIn("B-A GAP", html)
        self.assertIn("gap_vs_a_pp", html)
        self.assertIn("share_in_group", html)
        self.assertIn("value: item.no_is_adx.share_in_network", html)
        self.assertIn("value: item.have_is_adx.share_in_network", html)
        self.assertIn("point.share_in_group", html)
        self.assertNotIn("pv=${params.data.meta.reason_pv.toLocaleString()}\\n", html)
        self.assertNotIn("position: 'right'", html)
        self.assertNotIn("error-summary-a-chart", html)
        self.assertNotIn("error-daily-a-chart", html)
        self.assertNotIn("渠道 A 组 error 汇总分布", html)
        self.assertNotIn("选中 error 的 A 组分天趋势", html)


if __name__ == "__main__":
    unittest.main()
