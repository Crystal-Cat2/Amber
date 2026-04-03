"""Facebook latency 对比报告迁移回归测试。"""

from __future__ import annotations

import importlib.util
import tempfile
from pathlib import Path
import unittest


REPO_ROOT = Path(__file__).resolve().parents[1]
PROJECT_DIR = REPO_ROOT / "projects" / "latency_analysis"
SCRIPT_PATH = PROJECT_DIR / "scripts" / "build_facebook_latency_compare_report.py"
SQL_PATH = PROJECT_DIR / "sql" / "facebook_latency_compare.sql"

spec = importlib.util.spec_from_file_location("build_facebook_latency_compare_report", SCRIPT_PATH)
report = importlib.util.module_from_spec(spec)
assert spec is not None and spec.loader is not None
spec.loader.exec_module(report)


class FacebookLatencyCompareReportTests(unittest.TestCase):
    """覆盖 Facebook 第三部分独立报告的核心行为。"""

    def test_facebook_files_move_under_latency_analysis(self) -> None:
        """Facebook 分析应并入 latency_analysis 项目目录。"""
        self.assertTrue(PROJECT_DIR.exists())
        self.assertTrue(SCRIPT_PATH.exists())
        self.assertTrue(SQL_PATH.exists())
        self.assertTrue((PROJECT_DIR / "outputs" / "facebook.csv").exists())
        self.assertTrue((PROJECT_DIR / "outputs" / "facebook_latency_compare.csv").exists())
        self.assertTrue((PROJECT_DIR / "docs" / "facebook_latency_compare_report.md").exists())

    def test_load_facebook_backend_rows_maps_platform_and_format(self) -> None:
        """后台 CSV 应按平台与广告格式正确映射并聚合，且不再补版本列。"""
        csv_text = '''Date,Property ID,Property,App ID,App name,Platform,Delivery method,Display format,Bidding requests,Bidding responses,Bid rate,Win rate,Requests,Filled requests,Fill rate,Impressions,Impression rate,Clicks,CTR,Effective CPM,Revenue
2026-01-05,"=""1""","=""demo""","=""1""","=""demo""",android,bidding,banner,100,0,0,0,10,10,1,0,0,0,0,0,0
2026-01-05,"=""1""","=""demo""","=""1""","=""demo""",android,bidding,interstitial,200,0,0,0,20,20,1,0,0,0,0,0,0
2026-01-06,"=""1""","=""demo""","=""1""","=""demo""",ios,bidding,rewarded_video,300,0,0,0,30,30,1,0,0,0,0,0,0
2026-01-07,"=""1""","=""demo""","=""1""","=""demo""",ios,bidding,interstitial,400,0,0,0,40,40,1,0,0,0,0,0,0
2026-01-04,"=""1""","=""demo""","=""1""","=""demo""",android,bidding,banner,999,0,0,0,999,999,1,0,0,0,0,0,0
'''
        with tempfile.TemporaryDirectory() as temp_dir:
            csv_path = Path(temp_dir) / "facebook.csv"
            csv_path.write_text(csv_text, encoding="utf-8")

            rows = report.load_facebook_backend_rows(csv_path)

        row_index = {(row["product"], row["ad_format"]): row for row in rows}
        self.assertEqual(row_index[("screw_puzzle", "banner")]["bidding_requests"], 100)
        self.assertEqual(row_index[("screw_puzzle", "banner")]["requests"], 10)
        self.assertEqual(row_index[("screw_puzzle", "interstitial")]["bidding_requests"], 200)
        self.assertEqual(row_index[("ios_screw_puzzle", "interstitial")]["requests"], 40)
        self.assertEqual(row_index[("ios_screw_puzzle", "rewarded")]["bidding_requests"], 300)
        self.assertNotIn("target_version", row_index[("screw_puzzle", "banner")])

    def test_build_markdown_only_contains_facebook_part_three(self) -> None:
        """最终 Markdown 只应保留 Facebook 第三部分与四张对比表，且不再展示版本列。"""
        sql_sections = {"facebook_backend_compare": "SELECT 1 AS demo"}
        markdown = report.build_markdown(
            facebook_rows=[
                {
                    "product": "screw_puzzle",
                    "ad_format": "banner",
                    "facebook_started_cnt": 60,
                    "facebook_total_minus_not_started_cnt": 90,
                }
            ],
            backend_rows=[
                {
                    "product": "screw_puzzle",
                    "ad_format": "banner",
                    "bidding_requests": 100,
                    "requests": 80,
                }
            ],
            sql_sections=sql_sections,
        )

        self.assertIn("# Facebook latency 对比报告", markdown)
        self.assertIn("## 第三部分：Facebook 对比", markdown)
        self.assertIn("facebook_started_cnt vs Facebook Bidding requests", markdown)
        self.assertIn("facebook_total_minus_not_started_cnt vs Facebook Requests", markdown)
        self.assertNotIn("## 第一部分", markdown)
        self.assertNotIn("network_status_breakdown", markdown)
        self.assertNotIn("online_only", markdown)
        self.assertNotIn("target_version", markdown)

    def test_sql_uses_global_total_minus_facebook_not_started(self) -> None:
        """SQL 必须去掉版本限制和版本维度，但保留总数减 Facebook -1 的口径。"""
        sql_text = SQL_PATH.read_text(encoding="utf-8")

        self.assertIn("COUNT(*) AS latency_total_cnt", sql_text)
        self.assertIn("COUNTIF(fill_status_code = '-1') AS facebook_not_started_cnt", sql_text)
        self.assertIn("t.latency_total_cnt - COALESCE(n.facebook_not_started_cnt, 0) AS facebook_total_minus_not_started_cnt", sql_text)
        self.assertIn("STARTS_WITH(ep.key, 'Facebook_')", sql_text)
        self.assertNotIn("network_status_group", sql_text)
        self.assertNotIn("online_only", sql_text)
        self.assertNotIn("app_info.version =", sql_text)
        self.assertNotIn("target_version", sql_text)


if __name__ == "__main__":
    unittest.main()
