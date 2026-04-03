"""AB filled 时长分布页回归测试。"""

from __future__ import annotations

import importlib.util
from pathlib import Path
import unittest


REPO_ROOT = Path(__file__).resolve().parents[1]
PROJECT_DIR = REPO_ROOT / "projects" / "ab_dashboard"
SHARED_SCRIPT = PROJECT_DIR / "scripts" / "ab_dashboard_shared.py"
COMPAT_SCRIPT = PROJECT_DIR / "scripts" / "build_ab_share_dashboard.py"
HOME_SCRIPT = PROJECT_DIR / "scripts" / "build_ab_dashboard_home.py"
MAIN_SCRIPT = PROJECT_DIR / "scripts" / "build_ab_dashboard.py"
PAGE_SCRIPT = PROJECT_DIR / "scripts" / "build_ab_dashboard_filled_duration.py"
SQL_PATH = PROJECT_DIR / "sql" / "adslog_filled_duration_distribution_by_unit.sql"


def _load_module(module_name: str, path: Path):
    spec = importlib.util.spec_from_file_location(module_name, path)
    module = importlib.util.module_from_spec(spec)
    assert spec is not None and spec.loader is not None
    spec.loader.exec_module(module)
    return module


shared = _load_module("ab_dashboard_shared_for_filled_duration", SHARED_SCRIPT)
dashboard = _load_module("build_ab_share_dashboard_for_filled_duration", COMPAT_SCRIPT)
home = _load_module("build_ab_dashboard_home_for_filled_duration", HOME_SCRIPT)
main = _load_module("build_ab_dashboard_main_for_filled_duration", MAIN_SCRIPT)


class AbFilledDurationDashboardTests(unittest.TestCase):
    """校验新页面的 SQL、平台分桶与四块 HTML 骨架。"""

    def test_page_files_are_registered(self) -> None:
        """新页面应接入脚本入口、主生成器和首页导航。"""
        self.assertTrue(PAGE_SCRIPT.exists())
        self.assertTrue(SQL_PATH.exists())
        self.assertIn("filled_duration", main.PAGE_WRITERS)
        entry_html = home.build_entry_html()
        self.assertIn("adslog_filled 时长分布", entry_html)
        self.assertIn("ab_filled_duration_dashboard.html", entry_html)

    def test_sql_uses_hudi_filled_duration_with_raw_second_grain(self) -> None:
        """SQL 应只输出 0.01 秒原始粒度，不直接产出最终展示桶。"""
        sql_text = SQL_PATH.read_text(encoding="utf-8")
        self.assertIn("event_name = 'adslog_filled'", sql_text)
        self.assertIn("transferred.hudi_ods.screw_puzzle", sql_text)
        self.assertIn("transferred.hudi_ods.ios_screw_puzzle", sql_text)
        self.assertIn("key = 'duration'", sql_text)
        self.assertIn("ROUND(", sql_text)
        self.assertIn("/ 1000.0", sql_text)
        self.assertIn("AS duration_sec_2dp", sql_text)
        self.assertIn("AS denominator_filled_pv", sql_text)
        self.assertIn("'max_unit_id', 'unit_id', 'sdk_unit_id'", sql_text)
        self.assertIn("MIN(event_timestamp) AS group_start_ts", sql_text)
        self.assertIn("MAX(event_timestamp) AS group_end_ts", sql_text)
        self.assertIn("BETWEEN g.group_start_ts AND g.group_end_ts", sql_text)
        self.assertNotIn("LEAD(event_timestamp)", sql_text)
        self.assertNotIn("30+", sql_text)
        self.assertNotIn("27-30", sql_text)
        self.assertNotIn("bucket", sql_text.lower())

    def test_platform_bucket_functions_are_left_closed_right_open(self) -> None:
        """iOS 与 Android 应使用两套不同桶规则，且常规桶都是左闭右开。"""
        self.assertEqual(shared.bucket_filled_duration_label("ios", -0.01), "-1")
        self.assertEqual(shared.bucket_filled_duration_label("ios", 0.00), "0-0.5")
        self.assertEqual(shared.bucket_filled_duration_label("ios", 0.50), "0.5-1")
        self.assertEqual(shared.bucket_filled_duration_label("ios", 4.99), "3-5")
        self.assertEqual(shared.bucket_filled_duration_label("ios", 40.00), "40+")

        self.assertEqual(shared.bucket_filled_duration_label("android", -0.01), "-1")
        self.assertEqual(shared.bucket_filled_duration_label("android", 0.00), "0-1")
        self.assertEqual(shared.bucket_filled_duration_label("android", 1.00), "1-3")
        self.assertEqual(shared.bucket_filled_duration_label("android", 59.99), "30-60")
        self.assertEqual(shared.bucket_filled_duration_label("android", 120.00), "120+")

    def test_payload_builds_four_fixed_blocks_with_block_level_units(self) -> None:
        """payload 应固定产出四块，并为每块提供自己的 unit 选项和平台桶。"""
        rows = [
            {
                "experiment_group": "no_is_adx",
                "product": "screw_puzzle",
                "ad_format": "interstitial",
                "max_unit_id": "u1",
                "ad_unit_name": "Unit 1",
                "duration_sec_2dp": "2.99",
                "filled_pv": "4",
                "denominator_filled_pv": "10",
            },
            {
                "experiment_group": "have_is_adx",
                "product": "screw_puzzle",
                "ad_format": "interstitial",
                "max_unit_id": "u1",
                "ad_unit_name": "Unit 1",
                "duration_sec_2dp": "3.00",
                "filled_pv": "6",
                "denominator_filled_pv": "12",
            },
            {
                "experiment_group": "have_is_adx",
                "product": "screw_puzzle",
                "ad_format": "interstitial",
                "max_unit_id": "u1",
                "ad_unit_name": "Unit 1",
                "duration_sec_2dp": "-0.25",
                "filled_pv": "1",
                "denominator_filled_pv": "12",
            },
            {
                "experiment_group": "have_is_adx",
                "product": "ios_screw_puzzle",
                "ad_format": "rewarded",
                "max_unit_id": "u2",
                "ad_unit_name": "Unit 2",
                "duration_sec_2dp": "0.75",
                "filled_pv": "8",
                "denominator_filled_pv": "20",
            },
        ]

        payload = shared.build_filled_duration_dashboard_payload(rows)
        self.assertEqual([block["block_key"] for block in payload["blocks"]], [
            "android_interstitial",
            "android_rewarded",
            "ios_interstitial",
            "ios_rewarded",
        ])

        android_inter = payload["block_map"]["android_interstitial"]
        self.assertEqual(android_inter["unit_options"], ["Unit 1"])
        self.assertEqual(android_inter["default_unit"], "Unit 1")
        self.assertEqual(android_inter["bucket_options"][:4], ["-1", "0-1", "1-3", "3-5"])
        combo = android_inter["unit_map"]["Unit 1"]
        group_a = combo["groups"]["no_is_adx"]["bucket_map"]["1-3"]
        self.assertEqual(group_a["filled_pv"], 4.0)
        self.assertEqual(group_a["denominator_filled_pv"], 10.0)
        self.assertAlmostEqual(group_a["share"], 0.4)

        group_b_negative = combo["groups"]["have_is_adx"]["bucket_map"]["-1"]
        self.assertEqual(group_b_negative["filled_pv"], 1.0)
        self.assertAlmostEqual(group_b_negative["share"], 1.0 / 12.0)

        group_b = combo["groups"]["have_is_adx"]["bucket_map"]["3-5"]
        self.assertEqual(group_b["filled_pv"], 6.0)
        self.assertEqual(group_b["denominator_filled_pv"], 12.0)
        self.assertAlmostEqual(group_b["share"], 0.5)

        ios_rewarded = payload["block_map"]["ios_rewarded"]
        self.assertEqual(ios_rewarded["unit_options"], ["Unit 2"])
        self.assertEqual(ios_rewarded["bucket_options"][:5], ["-1", "0-0.5", "0.5-1", "1-2", "2-3"])
        ios_combo = ios_rewarded["unit_map"]["Unit 2"]
        self.assertEqual(ios_combo["groups"]["have_is_adx"]["bucket_map"]["0.5-1"]["filled_pv"], 8.0)

    def test_html_contains_four_blocks_and_block_level_selectors(self) -> None:
        """页面 HTML 应改成四块固定布局，不再保留全局 product/ad_format selector。"""
        html = shared.build_filled_duration_dashboard_html(
            {
                "groups": shared.GROUP_LABELS,
                "blocks": [
                    {"block_key": "android_interstitial", "title": "Android interstitial", "unit_options": ["Unit 1"]},
                    {"block_key": "android_rewarded", "title": "Android rewarded", "unit_options": ["Unit 2"]},
                    {"block_key": "ios_interstitial", "title": "iOS interstitial", "unit_options": ["Unit 3"]},
                    {"block_key": "ios_rewarded", "title": "iOS rewarded", "unit_options": ["Unit 4"]},
                ],
                "block_map": {},
            }
        )
        self.assertIn("adslog_filled 时长分布", html)
        self.assertIn("Android interstitial", html)
        self.assertIn("Android rewarded", html)
        self.assertIn("iOS interstitial", html)
        self.assertIn("iOS rewarded", html)
        self.assertIn("block-unit-select", html)
        self.assertIn("function filledDurationIntervalText", html)
        self.assertIn("function setError", html)
        self.assertIn("name:'B-A GAP'", html)
        self.assertIn("type:'line'", html)
        self.assertIn("yAxisIndex:1", html)
        self.assertNotIn('label for="product-select"', html)
        self.assertNotIn('label for="format-select"', html)
        self.assertIn("左闭右开", html)
        self.assertIn("同平台共用纵轴上限", html)
        self.assertIn("B-A GAP", html)
        self.assertIn("#0f766e", html)
        self.assertIn("#e11d48", html)


if __name__ == "__main__":
    unittest.main()
