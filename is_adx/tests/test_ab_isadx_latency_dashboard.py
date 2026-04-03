"""AB isadx latency 分布差异页回归测试。"""

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
PAGE_SCRIPT = PROJECT_DIR / "scripts" / "build_ab_dashboard_isadx_latency.py"
SQL_PATH = PROJECT_DIR / "sql" / "isadx_latency_distribution_by_unit.sql"


def _load_module(module_name: str, path: Path):
    spec = importlib.util.spec_from_file_location(module_name, path)
    module = importlib.util.module_from_spec(spec)
    assert spec is not None and spec.loader is not None
    spec.loader.exec_module(module)
    return module


shared = _load_module("ab_dashboard_shared_for_isadx_latency", SHARED_SCRIPT)
dashboard = _load_module("build_ab_share_dashboard_for_isadx_latency", COMPAT_SCRIPT)
home = _load_module("build_ab_dashboard_home_for_isadx_latency", HOME_SCRIPT)
main = _load_module("build_ab_dashboard_main_for_isadx_latency", MAIN_SCRIPT)


class AbIsadxLatencyDashboardTests(unittest.TestCase):
    """校验 isadx latency 分布差异页的注册、分桶与 HTML 骨架。"""

    def test_page_files_are_registered(self) -> None:
        self.assertTrue(PAGE_SCRIPT.exists())
        self.assertTrue(SQL_PATH.exists())
        self.assertIn("isadx_latency", main.PAGE_WRITERS)
        entry_html = home.build_entry_html()
        self.assertIn("IsAdx latency 分布差异", entry_html)
        self.assertIn("ab_isadx_latency_dashboard.html", entry_html)

    def test_sql_outputs_raw_latency_bucket_with_unit_status_and_ab_groups(self) -> None:
        sql_text = SQL_PATH.read_text(encoding="utf-8")
        self.assertIn("`commercial-adx.lmh.isadx_adslog_latency_detail`", sql_text)
        self.assertIn("network = 'IsAdxCustomAdapter'", sql_text)
        self.assertIn("max_unit_id", sql_text)
        self.assertIn("request_status", sql_text)
        self.assertIn("experiment_group", sql_text)
        self.assertIn("latency_bucket_raw", sql_text)
        self.assertIn("denominator_request_pv", sql_text)
        self.assertIn("SAFE_DIVIDE(", sql_text)
        self.assertIn("AS share", sql_text)
        self.assertNotIn("30+", sql_text)

    def test_display_bucket_function_collapses_raw_buckets_into_visual_ranges(self) -> None:
        self.assertEqual(shared.bucket_isadx_latency_label(-0.01), "<0")
        self.assertEqual(shared.bucket_isadx_latency_label(0.00), "[0,0.01)")
        self.assertEqual(shared.bucket_isadx_latency_label(0.01), "[0.01,0.02)")
        self.assertEqual(shared.bucket_isadx_latency_label(0.03), "[0.03,0.05)")
        self.assertEqual(shared.bucket_isadx_latency_label(0.08), "[0.08,0.10)")
        self.assertEqual(shared.bucket_isadx_latency_label(0.10), "[0.10,0.15)")
        self.assertEqual(shared.bucket_isadx_latency_label(0.50), "[0.50,1.00)")
        self.assertEqual(shared.bucket_isadx_latency_label(5.00), "[5,10)")
        self.assertEqual(shared.bucket_isadx_latency_label(10.00), "[10,30)")
        self.assertEqual(shared.bucket_isadx_latency_label(30.01), "30+")

    def test_payload_builds_four_blocks_with_unit_options_and_both_status_sections(self) -> None:
        rows = [
            {
                "product": "com.takeoffbolts.screw.puzzle",
                "ad_format": "interstitial",
                "max_unit_id": "u1",
                "experiment_group": "no_is_adx",
                "request_status": "success",
                "latency_bucket_raw": "0.00",
                "request_pv": "10",
                "denominator_request_pv": "100",
                "share": "0.10",
            },
            {
                "product": "com.takeoffbolts.screw.puzzle",
                "ad_format": "interstitial",
                "max_unit_id": "u1",
                "experiment_group": "have_is_adx",
                "request_status": "success",
                "latency_bucket_raw": "0.01",
                "request_pv": "15",
                "denominator_request_pv": "100",
                "share": "0.15",
            },
            {
                "product": "com.takeoffbolts.screw.puzzle",
                "ad_format": "interstitial",
                "max_unit_id": "u1",
                "experiment_group": "no_is_adx",
                "request_status": "fail",
                "latency_bucket_raw": "0.10",
                "request_pv": "5",
                "denominator_request_pv": "40",
                "share": "0.125",
            },
            {
                "product": "com.takeoffbolts.screw.puzzle",
                "ad_format": "interstitial",
                "max_unit_id": "u1",
                "experiment_group": "have_is_adx",
                "request_status": "fail",
                "latency_bucket_raw": "0.12",
                "request_pv": "8",
                "denominator_request_pv": "40",
                "share": "0.20",
            },
            {
                "product": "ios.takeoffbolts.screw.puzzle",
                "ad_format": "rewarded",
                "max_unit_id": "u2",
                "experiment_group": "have_is_adx",
                "request_status": "success",
                "latency_bucket_raw": "0.50",
                "request_pv": "12",
                "denominator_request_pv": "60",
                "share": "0.20",
            },
        ]

        payload = shared.build_isadx_latency_dashboard_payload(rows)
        self.assertEqual([block["block_key"] for block in payload["blocks"]], [
            "android_interstitial",
            "android_rewarded",
            "ios_interstitial",
            "ios_rewarded",
        ])

        android_inter = payload["block_map"]["android_interstitial"]
        self.assertEqual(android_inter["unit_options"], ["u1"])
        self.assertEqual(android_inter["default_unit"], "u1")
        self.assertIn("success", android_inter["status_order"])
        self.assertIn("fail", android_inter["status_order"])
        combo = android_inter["unit_map"]["u1"]
        success_bucket = combo["status_map"]["success"]["groups"][shared.GROUP_B]["bucket_map"]["[0.01,0.02)"]
        self.assertAlmostEqual(success_bucket["share"], 0.15)
        fail_bucket = combo["status_map"]["fail"]["groups"][shared.GROUP_B]["bucket_map"]["[0.10,0.15)"]
        self.assertAlmostEqual(fail_bucket["share"], 0.20)
        self.assertAlmostEqual(fail_bucket["gap_share"], 0.075)

        ios_rewarded = payload["block_map"]["ios_rewarded"]
        self.assertEqual(ios_rewarded["unit_options"], ["u2"])
        ios_combo = ios_rewarded["unit_map"]["u2"]
        self.assertEqual(
            ios_combo["status_map"]["success"]["groups"][shared.GROUP_B]["bucket_map"]["[0.50,1.00)"]["request_pv"],
            12.0,
        )

    def test_html_contains_dual_axis_gap_line_colored_zero_baseline_and_block_level_selectors(self) -> None:
        html = shared.build_isadx_latency_dashboard_html(
            {
                "groups": shared.GROUP_LABELS,
                "blocks": [
                    {"block_key": "android_interstitial", "title": "Android interstitial", "unit_options": ["u1"], "status_order": ["success", "fail"]},
                    {"block_key": "android_rewarded", "title": "Android rewarded", "unit_options": ["u2"], "status_order": ["success", "fail"]},
                    {"block_key": "ios_interstitial", "title": "iOS interstitial", "unit_options": ["u3"], "status_order": ["success", "fail"]},
                    {"block_key": "ios_rewarded", "title": "iOS rewarded", "unit_options": ["u4"], "status_order": ["success", "fail"]},
                ],
                "block_map": {},
            }
        )
        self.assertIn("IsAdx latency 分布差异", html)
        self.assertIn("block-unit-select", html)
        self.assertIn("success", html)
        self.assertIn("fail", html)
        self.assertIn("name:'B-A GAP'", html)
        self.assertIn("yAxisIndex:1", html)
        self.assertIn("splitLine", html)
        self.assertIn("#dc2626", html)
        self.assertIn("#1d4ed8", html)
        self.assertIn("#f97316", html)
        self.assertIn("#7c3aed", html)
        self.assertIn("chart-scroll", html)
        self.assertNotIn('label for="product-select"', html)
        self.assertNotIn('label for="format-select"', html)


if __name__ == "__main__":
    unittest.main()
